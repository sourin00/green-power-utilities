#!/usr/bin/env python3
"""
Household data ingestion module for UCI Electric Power Consumption dataset
"""

import logging
import zipfile
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Optional, Dict, Any

import numpy as np
import pandas as pd
import requests
from sqlalchemy import text

from config.settings import DatabaseConfig, IngestionConfig
from ingestion.base import BaseIngestionPipeline
from database.models import HOUSEHOLD_COLUMN_MAPPING
from monitoring.logging_utils import LogContext, log_dataframe_info

logger = logging.getLogger(__name__)


class HouseholdIngestionPipeline(BaseIngestionPipeline):
    """Handles ingestion of household electric power consumption data"""

    def __init__(self, db_config: DatabaseConfig, ingestion_config: IngestionConfig):
        super().__init__(db_config, ingestion_config)
        self.uci_url = "https://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip"
        self.household_id = 'uci_france_001'

    def ingest_data(self) -> int:
        """Main ingestion method for household data"""
        logger.info("Starting household data ingestion")

        # Try to find local files first
        local_data = self._find_local_household_files()
        if local_data is not None:
            return self._process_household_dataframe(local_data)

        # Otherwise, download from UCI
        downloaded_data = self._download_uci_household_data()
        if downloaded_data is not None:
            return self._process_household_dataframe(downloaded_data)

        logger.warning("No household data available for ingestion")
        return 0

    def process_batch_files(self) -> int:
        """Process household data files from local directory"""
        logger.info("Processing household batch files")

        records_processed = 0
        records_inserted = 0

        # Define input and archive directories
        input_dir = Path("data/raw")
        archive_dir = Path("data/archive")
        archive_dir.mkdir(parents=True, exist_ok=True)

        # Look for UCI household data files
        household_files = list(input_dir.glob("household_power_consumption.txt")) + \
                          list(input_dir.glob("household_power_consumption.csv")) + \
                          list(input_dir.glob("Individual_household_electric_power_consumption.txt"))

        if not household_files:
            logger.info("No local household files found, attempting download")
            household_data = self._download_uci_household_data()
            if household_data is not None:
                return self._process_household_dataframe(household_data)
            return 0

        # Process existing files
        for file_path in household_files:
            logger.info(f"Processing household file: {file_path}")

            try:
                household_data = self._load_uci_household_file(file_path)
                if household_data is not None and not household_data.empty:
                    # Process in chunks
                    chunk_size = 10000
                    total_chunks = len(household_data) // chunk_size + 1

                    for i in range(total_chunks):
                        start_idx = i * chunk_size
                        end_idx = min((i + 1) * chunk_size, len(household_data))
                        chunk = household_data.iloc[start_idx:end_idx].copy()

                        if not chunk.empty:
                            inserted = self._process_household_dataframe(chunk)
                            records_inserted += inserted
                            records_processed += len(chunk)

                            if i % 10 == 0:
                                logger.info(
                                    f"Processed {i + 1}/{total_chunks} chunks, {records_inserted} records inserted")

                    # Archive processed file
                    archive_path = archive_dir / f"{file_path.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{file_path.suffix}"
                    file_path.rename(archive_path)
                    logger.info(f"Archived file to: {archive_path}")

            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}")
                continue

        return records_inserted

    def process_historical_data(self, start_date: datetime, end_date: datetime) -> int:
        """Process historical household data for a date range"""
        logger.info(f"Processing historical household data from {start_date} to {end_date}")

        # Download full dataset
        household_data = self._download_uci_household_data()
        if household_data is None:
            logger.warning("Could not download UCI household data")
            return 0

        # Filter to date range
        mask = (household_data.index >= start_date) & (household_data.index <= end_date)
        filtered_data = household_data[mask]

        if filtered_data.empty:
            logger.warning("No household data in specified date range")
            return 0

        logger.info(f"Found {len(filtered_data)} records in date range")
        return self._process_household_dataframe(filtered_data)

    def validate_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate household data before insertion"""
        initial_count = len(data)

        # Remove rows with invalid timestamps
        data = data[data.index.notna()]

        # Remove obvious outliers
        if 'global_active_power' in data.columns:
            data = data[(data['global_active_power'] >= 0) & (data['global_active_power'] <= 20)]

        if 'voltage' in data.columns:
            data = data[(data['voltage'] >= 200) & (data['voltage'] <= 260)]

        # Calculate data quality score
        required_columns = ['global_active_power', 'voltage', 'global_intensity']
        null_counts = data[required_columns].isnull().sum(axis=1)
        data['data_quality_score'] = 1.0 - (null_counts / len(required_columns))

        final_count = len(data)
        if final_count < initial_count:
            logger.info(f"Validation removed {initial_count - final_count} invalid records")

        return data

    def _find_local_household_files(self) -> Optional[pd.DataFrame]:
        """Find and load local household data files"""
        input_dir = Path("data/raw")

        search_patterns = [
            "household_power_consumption.txt",
            "household_power_consumption.csv",
            "Individual_household_electric_power_consumption.txt"
        ]

        for pattern in search_patterns:
            files = list(input_dir.glob(pattern))
            if files:
                logger.info(f"Found local file: {files[0]}")
                return self._load_uci_household_file(files[0])

        return None

    def _download_uci_household_data(self) -> Optional[pd.DataFrame]:
        """Download UCI household power consumption dataset"""
        try:
            # Try using ucimlrepo if available
            try:
                from ucimlrepo import fetch_ucirepo
                logger.info("Downloading UCI dataset using ucimlrepo...")

                dataset = fetch_ucirepo(id=235)
                data = dataset.data.features

                # Process datetime columns
                if 'Date' in data.columns and 'Time' in data.columns:
                    data['datetime'] = pd.to_datetime(data['Date'] + ' ' + data['Time'],
                                                      format='%d/%m/%Y %H:%M:%S', errors='coerce')
                    data = data.drop(['Date', 'Time'], axis=1)
                    data.set_index('datetime', inplace=True)

                logger.info(f"Downloaded UCI dataset: {data.shape}")
                return data

            except ImportError:
                logger.warning("ucimlrepo not available, trying direct download...")

            # Direct download from UCI repository
            logger.info(f"Downloading from: {self.uci_url}")

            with LogContext("UCI dataset download", logger):
                response = requests.get(self.uci_url, timeout=300)
                response.raise_for_status()

                # Extract and load the data
                with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                    data_files = [f for f in zip_file.namelist() if f.endswith('.txt') and 'household' in f.lower()]

                    if data_files:
                        with zip_file.open(data_files[0]) as data_file:
                            data = pd.read_csv(data_file, sep=';', encoding='utf-8', low_memory=False)
                            data = self._clean_uci_data(data)
                            logger.info(f"Downloaded and processed UCI dataset: {data.shape}")
                            return data

        except Exception as e:
            logger.error(f"Failed to download UCI dataset: {e}")
            return None

    def _load_uci_household_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Load UCI household data from local file"""
        try:
            logger.info(f"Loading household data from: {file_path}")

            # Try different separators and encodings
            separators = [';', ',', '\t']
            encodings = ['utf-8', 'iso-8859-1', 'latin1']

            data = None
            for sep in separators:
                for encoding in encodings:
                    try:
                        data = pd.read_csv(file_path, sep=sep, encoding=encoding, low_memory=False)
                        if data.shape[1] >= 7:  # UCI dataset should have at least 7-9 columns
                            logger.info(f"Successfully loaded with separator '{sep}' and encoding '{encoding}'")
                            break
                    except Exception:
                        continue
                if data is not None and data.shape[1] >= 7:
                    break

            if data is None or data.shape[1] < 7:
                logger.error(f"Could not load data from {file_path}")
                return None

            # Clean and process the data
            data = self._clean_uci_data(data)
            return data

        except Exception as e:
            logger.error(f"Error loading household file {file_path}: {e}")
            return None

    def _clean_uci_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Clean and preprocess UCI household data"""
        try:
            # Rename columns to match our schema
            for old_name, new_name in HOUSEHOLD_COLUMN_MAPPING.items():
                if old_name in data.columns:
                    data.rename(columns={old_name: new_name}, inplace=True)

            # Create datetime index
            if 'Date' in data.columns and 'Time' in data.columns:
                data['datetime'] = pd.to_datetime(data['Date'] + ' ' + data['Time'],
                                                  format='%d/%m/%Y %H:%M:%S', errors='coerce')
                data = data.drop(['Date', 'Time'], axis=1)
                data.set_index('datetime', inplace=True)

            # Convert '?' to NaN and then to numeric
            numeric_columns = ['global_active_power', 'global_reactive_power', 'voltage',
                               'global_intensity', 'sub_metering_1', 'sub_metering_2', 'sub_metering_3']

            for col in numeric_columns:
                if col in data.columns:
                    data[col] = pd.to_numeric(data[col].replace('?', np.nan), errors='coerce')

            # Add required fields
            data['household_id'] = self.household_id
            data['data_quality_score'] = 1.0
            data['source_file'] = 'uci_dataset'

            # Calculate other consumption
            if all(col in data.columns for col in
                   ['global_active_power', 'sub_metering_1', 'sub_metering_2', 'sub_metering_3']):
                data['calculated_other_consumption'] = (
                        data['global_active_power'] * 1000 / 60 -  # Convert to Wh
                        data['sub_metering_1'] -
                        data['sub_metering_2'] -
                        data['sub_metering_3']
                )

            # Remove rows with invalid timestamps
            data = data.dropna(subset=['datetime'] if 'datetime' in data.columns else [])

            # Validate data
            data = self.validate_data(data)

            log_dataframe_info(data, "Cleaned UCI data", logger)
            return data

        except Exception as e:
            logger.error(f"Error cleaning UCI data: {e}")
            return data

    def _process_household_dataframe(self, df: pd.DataFrame) -> int:
        """Process and insert household data DataFrame"""
        if df.empty:
            return 0

        try:
            # Ensure we have the required columns
            required_columns = ['household_id', 'global_active_power', 'global_reactive_power',
                                'voltage', 'global_intensity', 'sub_metering_1', 'sub_metering_2',
                                'sub_metering_3', 'calculated_other_consumption', 'data_quality_score',
                                'source_file']

            # Add missing columns with default values
            for col in required_columns:
                if col not in df.columns:
                    if col == 'household_id':
                        df[col] = self.household_id
                    elif col == 'data_quality_score':
                        df[col] = 1.0
                    elif col == 'source_file':
                        df[col] = 'processed_batch'
                    else:
                        df[col] = np.nan

            # Reset index to make timestamp a column
            if df.index.name == 'datetime' or 'datetime' in str(type(df.index)):
                df = df.reset_index()
                df.rename(columns={'datetime': 'timestamp'}, inplace=True)

            # Ensure timestamp column exists
            if 'timestamp' not in df.columns:
                logger.error("No timestamp column found in household data")
                return 0

            # Convert timestamp to timezone-aware
            if not pd.api.types.is_datetime64tz_dtype(df['timestamp']):
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                if df['timestamp'].dt.tz is None:
                    df['timestamp'] = df['timestamp'].dt.tz_localize('Europe/Paris')

            # Connect if not connected
            if not self.db_connection.test_connection():
                self.connect()

            # Use upsert to handle duplicates
            inserted_count = self._upsert_household_data(df)
            return inserted_count

        except Exception as e:
            logger.error(f"Error processing household dataframe: {e}")
            return 0

    def _upsert_household_data(self, df: pd.DataFrame) -> int:
        """Upsert household data with conflict resolution"""
        try:
            # First, try to insert directly
            df.to_sql(
                'consumption',
                self.db_connection.engine,
                schema='household',
                if_exists='append',
                index=False,
                method='multi',
                chunksize=self.ingestion_config.batch_size
            )

            logger.info(f"Inserted {len(df)} household records")
            return len(df)

        except Exception as e:
            # Handle duplicate key errors with upsert
            if 'duplicate key' in str(e).lower() or 'unique constraint' in str(e).lower():
                logger.info(f"Handling duplicates with upsert for {len(df)} records")
                return self._upsert_household_data_manually(df)
            else:
                logger.error(f"Household data insertion failed: {e}")
                return 0

    def _upsert_household_data_manually(self, df: pd.DataFrame) -> int:
        """Manual upsert for household data with ON CONFLICT"""
        try:
            upsert_sql = """
                         INSERT INTO household.consumption
                         (timestamp, household_id, global_active_power, global_reactive_power, voltage,
                          global_intensity, sub_metering_1, sub_metering_2, sub_metering_3,
                          calculated_other_consumption, data_quality_score, source_file)
                         VALUES (%(timestamp)s, %(household_id)s, %(global_active_power)s, %(global_reactive_power)s,
                                 %(voltage)s, %(global_intensity)s, %(sub_metering_1)s, %(sub_metering_2)s,
                                 %(sub_metering_3)s, %(calculated_other_consumption)s, %(data_quality_score)s, \
                                 %(source_file)s) ON CONFLICT (timestamp, household_id) 
                DO \
                         UPDATE SET
                             global_active_power = EXCLUDED.global_active_power, \
                             global_reactive_power = EXCLUDED.global_reactive_power, \
                             voltage = EXCLUDED.voltage, \
                             global_intensity = EXCLUDED.global_intensity, \
                             sub_metering_1 = EXCLUDED.sub_metering_1, \
                             sub_metering_2 = EXCLUDED.sub_metering_2, \
                             sub_metering_3 = EXCLUDED.sub_metering_3, \
                             calculated_other_consumption = EXCLUDED.calculated_other_consumption, \
                             data_quality_score = EXCLUDED.data_quality_score, \
                             ingestion_timestamp = NOW() \
                         """

            # Convert DataFrame to records
            records = df.to_dict('records')
            inserted_count = 0

            # Process in smaller batches
            batch_size = 100
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                self.db_connection.execute_many(upsert_sql, batch)
                inserted_count += len(batch)

                if i % 1000 == 0:  # Log progress
                    logger.info(f"Upserted {i + len(batch)}/{len(records)} household records")

            logger.info(f"Successfully upserted {inserted_count} household records")
            return inserted_count

        except Exception as e:
            logger.error(f"Manual upsert failed: {e}")
            return 0