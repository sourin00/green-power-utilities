#!/usr/bin/env python3
"""
Grid operations data ingestion module for Open Power System Data and ENTSO-E
"""

import gzip
import io
import logging
import time
import zlib
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from typing import Optional, Dict, Any

import numpy as np
import pandas as pd
import requests

from config.settings import DatabaseConfig, IngestionConfig
from ingestion.base import BaseIngestionPipeline
from database.models import OPSD_COLUMN_MAPPING
from monitoring.logging_utils import LogContext

logger = logging.getLogger(__name__)


class GridIngestionPipeline(BaseIngestionPipeline):
    """Handles ingestion of grid operations data"""

    def __init__(self, db_config: DatabaseConfig, ingestion_config: IngestionConfig):
        super().__init__(db_config, ingestion_config)
        self.opsd_urls = [
            "https://data.open-power-system-data.org/time_series/latest/time_series_60min_singleindex.csv",
            "https://data.open-power-system-data.org/time_series/2020-10-06/time_series_60min_singleindex.csv"
        ]
        self.supported_countries = ['FR', 'DE', 'ES']

    def ingest_data(self) -> int:
        """Main ingestion method for grid data"""
        return self.ingest_batch_data()

    def ingest_batch_data(self) -> int:
        """Batch ingestion of grid data"""
        logger.info("Starting batch grid data ingestion")

        # Connect if not connected
        if not self.db_connection.test_connection():
            self.connect()

        records_inserted = 0

        # Try to fetch real OPSD data first
        grid_data = self._fetch_opsd_data()  # Remove the URL parameter

        if grid_data is None or grid_data.empty:
            # Fallback to ENTSO-E API if available
            logger.info("Trying ENTSO-E API...")
            grid_data = self._fetch_entsoe_data()

        if grid_data is None or grid_data.empty:
            # Final fallback to synthetic data for demo
            logger.warning("Could not fetch real grid data, generating synthetic data")
            end_time = datetime.now()
            start_time = end_time - timedelta(days=1)
            grid_data = self._generate_sample_grid_data(start_time, end_time)

        if not grid_data.empty:
            grid_data = self.validate_data(grid_data)
            records_inserted = self._insert_grid_data(grid_data)

        logger.info(f"Batch grid ingestion completed: {records_inserted} records inserted")
        return records_inserted

    def process_historical_data(self, start_date: datetime, end_date: datetime) -> int:
        """Process historical grid data for a date range"""
        logger.info(f"Processing historical grid data from {start_date} to {end_date}")

        # Connect if not connected
        if not self.db_connection.test_connection():
            self.connect()

        # Try to fetch OPSD data
        grid_data = self._fetch_opsd_data()

        if grid_data is not None and not grid_data.empty:
            # Ensure timezone consistency for date filtering
            if pd.api.types.is_datetime64tz_dtype(grid_data['timestamp']):
                # If grid_data has timezone info, ensure start_date and end_date are timezone-aware
                if start_date.tzinfo is None:
                    start_date = start_date.replace(tzinfo=pd.Timestamp.now().tz)
                if end_date.tzinfo is None:
                    end_date = end_date.replace(tzinfo=pd.Timestamp.now().tz)
            else:
                # If grid_data is timezone-naive, ensure start_date and end_date are also timezone-naive
                if start_date.tzinfo is not None:
                    start_date = start_date.replace(tzinfo=None)
                if end_date.tzinfo is not None:
                    end_date = end_date.replace(tzinfo=None)

            # Filter to date range
            mask = (grid_data['timestamp'] >= start_date) & (grid_data['timestamp'] <= end_date)
            filtered_data = grid_data[mask]

            if not filtered_data.empty:
                filtered_data = self.validate_data(filtered_data)
                records_inserted = self._insert_grid_data(filtered_data)
                logger.info(f"Processed {records_inserted} historical grid records")
                return records_inserted
            else:
                logger.warning("No grid data in specified date range")

        # If no real data available, generate synthetic data
        logger.info("Generating synthetic historical grid data")
        grid_data = self._generate_sample_grid_data(start_date, end_date)

        if not grid_data.empty:
            grid_data = self.validate_data(grid_data)
            records_inserted = self._insert_grid_data(grid_data)
            return records_inserted

        return 0

    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean grid data"""
        if df.empty:
            return df

        original_count = len(df)

        # Required columns for the database schema
        required_columns = [
            'timestamp', 'country_code', 'region_code', 'load_actual_mw',
            'wind_offshore_generation_actual_mw'  # Ensure this is in required columns
        ]

        # Check for missing required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"Missing required columns: {missing_columns}")
            # Add missing columns with default values
            for col in missing_columns:
                if 'mw' in col.lower():
                    df[col] = 0.0  # Default to 0 for power values
                else:
                    df[col] = None

        # Remove rows with invalid timestamps
        df = df.dropna(subset=['timestamp'])

        # Ensure numeric columns are properly typed
        numeric_columns = [col for col in df.columns if 'mw' in col.lower() or 'eur' in col.lower()]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # Remove completely invalid rows
        df = df.dropna(subset=['country_code', 'region_code'])

        removed_count = original_count - len(df)
        if removed_count > 0:
            logger.info(f"Validation removed {removed_count} invalid records")

        return df

    def _fetch_opsd_data(self) -> Optional[pd.DataFrame]:
        """Fetch OPSD data from configured URLs with improved error handling"""
        import time
        import io

        for url in self.opsd_urls:
            try:
                logger.info(f"Fetching OPSD data from: {url}")
                logger.info(f"Starting OPSD data fetch")
                start_time = time.time()

                # Try different approaches for data fetching
                response = requests.get(url, timeout=30, stream=True)
                response.raise_for_status()

                # Check content type and handle accordingly
                content_type = response.headers.get('content-type', '').lower()

                try:
                    # First try reading as compressed
                    data = pd.read_csv(io.BytesIO(response.content), compression='gzip')
                except (OSError, pd.errors.EmptyDataError, gzip.BadGzipFile):
                    # If that fails, try as regular CSV
                    try:
                        data = pd.read_csv(io.StringIO(response.text))
                    except Exception:
                        # Finally try with different encoding
                        try:
                            data = pd.read_csv(io.BytesIO(response.content), encoding='utf-8')
                        except Exception:
                            logger.warning(f"Error processing OPSD data from {url}: Could not parse as CSV")
                            continue

                fetch_time = time.time() - start_time
                logger.info(f"Successfully fetched OPSD data in {fetch_time:.2f} seconds: {data.shape}")

                # Add debugging
                self._debug_opsd_structure(data, url)

                # Process the data
                processed_data = self._process_opsd_data(data)
                if not processed_data.empty:
                    return processed_data
                else:
                    logger.warning(f"No data after processing from {url}")
                    continue

            except Exception as e:
                fetch_time = time.time() - start_time if 'start_time' in locals() else 0
                logger.error(f"Failed OPSD data fetch after {fetch_time:.2f} seconds: {e}")
                logger.warning(f"Error processing OPSD data from {url}: {e}")
                continue

        logger.warning("Could not fetch OPSD data from any configured URL")
        return None

    def _process_opsd_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process and clean OPSD data with improved column handling"""
        try:
            if data.empty:
                return data

            logger.info(f"Processing OPSD data with columns: {list(data.columns[:10])}...")  # Log first 10 columns

            # Handle timestamp column
            timestamp_cols = ['timestamp', 'utc_timestamp', 'Time (UTC)']
            timestamp_col = None
            for col in timestamp_cols:
                if col in data.columns:
                    timestamp_col = col
                    break

            if timestamp_col:
                data['timestamp'] = pd.to_datetime(data[timestamp_col])
                if timestamp_col != 'timestamp':
                    data.drop(timestamp_col, axis=1, inplace=True)
            else:
                # Use index if it's a datetime index
                if isinstance(data.index, pd.DatetimeIndex):
                    data['timestamp'] = data.index
                else:
                    logger.error("No timestamp column found in OPSD data")
                    return pd.DataFrame()

            # Ensure timezone-naive timestamps for consistency
            if pd.api.types.is_datetime64tz_dtype(data['timestamp']):
                data['timestamp'] = data['timestamp'].dt.tz_convert('UTC').dt.tz_localize(None)

            # Process data by extracting country information from column names
            # OPSD data typically has columns like 'DE_load_actual_entsoe_transparency'
            processed_data = []

            # Extract country codes from column names
            countries = set()
            for col in data.columns:
                if '_' in col and len(col.split('_')[0]) == 2:
                    country = col.split('_')[0]
                    if country.upper() in ['FR', 'DE', 'ES', 'IT', 'NL', 'BE', 'AT', 'CH', 'PL']:
                        countries.add(country.upper())

            logger.info(f"Found countries in OPSD data: {countries}")

            if not countries:
                logger.warning("No country data found in OPSD columns")
                return pd.DataFrame()

            for country in countries:
                country_data = data[['timestamp']].copy()
                country_data['country_code'] = country
                country_data['region_code'] = country
                country_data['source'] = 'Open Power System Data'

                # Map columns based on naming patterns
                column_mappings = {
                    f'{country.lower()}_load_actual_entsoe_transparency': 'load_actual_mw',
                    f'{country.lower()}_load_forecast_entsoe_transparency': 'load_forecast_mw',
                    f'{country.lower()}_solar_generation_actual': 'solar_generation_actual_mw',
                    f'{country.lower()}_wind_onshore_generation_actual': 'wind_onshore_generation_actual_mw',
                    f'{country.lower()}_wind_offshore_generation_actual': 'wind_offshore_generation_actual_mw',
                    f'{country.lower()}_hydro_generation_actual': 'hydro_generation_actual_mw',
                    f'{country.lower()}_nuclear_generation_actual': 'nuclear_generation_actual_mw',
                    f'{country.lower()}_fossil_gas_generation_actual': 'fossil_generation_actual_mw',
                    f'{country.lower()}_fossil_coal_generation_actual': 'fossil_coal_generation_actual_mw',
                    f'{country.lower()}_price_day_ahead': 'price_day_ahead_eur_mwh'
                }

                # Find and map available columns
                mapped_any = False
                for opsd_col, our_col in column_mappings.items():
                    # Try exact match first
                    if opsd_col in data.columns:
                        country_data[our_col] = pd.to_numeric(data[opsd_col], errors='coerce')
                        mapped_any = True
                    else:
                        # Try fuzzy matching
                        for col in data.columns:
                            if (country.lower() in col.lower() and
                                    any(key_word in col.lower() for key_word in opsd_col.split('_')[1:])):
                                country_data[our_col] = pd.to_numeric(data[col], errors='coerce')
                                mapped_any = True
                                break

                # If no columns mapped, try a more flexible approach
                if not mapped_any:
                    logger.info(f"Trying flexible column mapping for {country}")
                    for col in data.columns:
                        if col.upper().startswith(country.upper() + '_'):
                            col_lower = col.lower()
                            if 'load' in col_lower and 'actual' in col_lower:
                                country_data['load_actual_mw'] = pd.to_numeric(data[col], errors='coerce')
                                mapped_any = True
                            elif 'solar' in col_lower:
                                country_data['solar_generation_actual_mw'] = pd.to_numeric(data[col], errors='coerce')
                                mapped_any = True
                            elif 'wind' in col_lower and 'onshore' in col_lower:
                                country_data['wind_onshore_generation_actual_mw'] = pd.to_numeric(data[col],
                                                                                                  errors='coerce')
                                mapped_any = True
                            elif 'wind' in col_lower and 'offshore' in col_lower:
                                country_data['wind_offshore_generation_actual_mw'] = pd.to_numeric(data[col],
                                                                                                   errors='coerce')
                                mapped_any = True
                            elif 'hydro' in col_lower:
                                country_data['hydro_generation_actual_mw'] = pd.to_numeric(data[col], errors='coerce')
                                mapped_any = True
                            elif 'nuclear' in col_lower:
                                country_data['nuclear_generation_actual_mw'] = pd.to_numeric(data[col], errors='coerce')
                                mapped_any = True
                            elif 'price' in col_lower:
                                country_data['price_day_ahead_eur_mwh'] = pd.to_numeric(data[col], errors='coerce')
                                mapped_any = True

                if mapped_any:
                    # Add default values for missing columns
                    required_cols = [
                        'load_actual_mw', 'load_forecast_mw', 'solar_generation_actual_mw',
                        'wind_onshore_generation_actual_mw', 'wind_offshore_generation_actual_mw',
                        'hydro_generation_actual_mw', 'nuclear_generation_actual_mw',
                        'fossil_generation_actual_mw', 'other_renewable_generation_mw',
                        'total_generation_mw', 'net_import_export_mw', 'price_day_ahead_eur_mwh'
                    ]

                    for col in required_cols:
                        if col not in country_data.columns:
                            country_data[col] = 0.0

                    # Calculate total generation
                    generation_cols = [
                        'solar_generation_actual_mw', 'wind_onshore_generation_actual_mw',
                        'wind_offshore_generation_actual_mw', 'hydro_generation_actual_mw',
                        'nuclear_generation_actual_mw', 'fossil_generation_actual_mw',
                        'other_renewable_generation_mw'
                    ]

                    available_gen_cols = [col for col in generation_cols if col in country_data.columns]
                    if available_gen_cols:
                        country_data['total_generation_mw'] = country_data[available_gen_cols].sum(axis=1)

                    # Calculate net import/export
                    if 'load_actual_mw' in country_data.columns and 'total_generation_mw' in country_data.columns:
                        country_data['net_import_export_mw'] = country_data['total_generation_mw'] - country_data[
                            'load_actual_mw']

                    # Remove rows with all NaN values in data columns
                    data_cols = [col for col in country_data.columns if
                                 col not in ['timestamp', 'country_code', 'region_code', 'source']]
                    country_data = country_data.dropna(subset=data_cols, how='all')

                    if not country_data.empty:
                        processed_data.append(country_data)
                        logger.info(f"Processed {len(country_data)} records for {country}")
                    else:
                        logger.warning(f"No valid data after processing for {country}")
                else:
                    logger.warning(f"Could not map any columns for {country}")

            if processed_data:
                final_data = pd.concat(processed_data, ignore_index=True)

                # Ensure timestamps are timezone-naive for database insertion
                final_data['timestamp'] = pd.to_datetime(final_data['timestamp'])
                if pd.api.types.is_datetime64tz_dtype(final_data['timestamp']):
                    final_data['timestamp'] = final_data['timestamp'].dt.tz_localize(None)

                logger.info(
                    f"Successfully processed OPSD data: {final_data.shape[0]} records for {len(processed_data)} countries")
                return final_data

            logger.warning("No data could be processed from OPSD dataset")
            return pd.DataFrame()

        except Exception as e:
            logger.error(f"Error processing OPSD data: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return pd.DataFrame()

    def _debug_opsd_structure(self, data: pd.DataFrame, url: str) -> None:
        """Debug method to understand OPSD data structure"""
        logger.info(f"=== OPSD Data Structure Debug for {url} ===")
        logger.info(f"Shape: {data.shape}")
        logger.info(f"Index type: {type(data.index)}")
        logger.info(f"First 10 columns: {list(data.columns[:10])}")

        # Look for country-specific columns
        country_cols = {}
        for col in data.columns:
            if '_' in col:
                parts = col.split('_')
                if len(parts[0]) == 2 and parts[0].upper() in ['FR', 'DE', 'ES', 'IT', 'NL', 'BE']:
                    country = parts[0].upper()
                    if country not in country_cols:
                        country_cols[country] = []
                    country_cols[country].append(col)

        logger.info(f"Country columns found: {dict(list(country_cols.items())[:3])}")  # First 3 countries

        # Sample first few rows
        logger.info(f"First 3 rows:\n{data.head(3)}")
        logger.info("=== End Debug ===")


    def _fetch_entsoe_data(self) -> Optional[pd.DataFrame]:
        """Fetch data from ENTSO-E Transparency Platform"""
        try:
            # Try using entsoe-py library if available
            try:
                from entsoe import EntsoePandasClient

                # Would need API token from configuration
                api_token = None  # TODO: Get from config
                if not api_token:
                    logger.info("ENTSO-E API token not configured")
                    return None

                client = EntsoePandasClient(api_key=api_token)

                # Fetch data for supported countries
                # This is a placeholder - actual implementation would fetch real data
                logger.info("ENTSO-E API requires proper implementation with valid token")
                return None

            except ImportError:
                logger.info("entsoe-py library not available")
                return None

        except Exception as e:
            logger.error(f"ENTSO-E data fetch failed: {e}")
            return None

    def _generate_sample_grid_data(self, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """Generate synthetic grid data with all required fields"""
        try:
            countries = ['FR', 'DE', 'ES']
            data = []

            # Base load values for different countries
            base_loads = {'FR': 50000, 'DE': 60000, 'ES': 35000}

            current_time = start_time
            while current_time < end_time:
                for country in countries:
                    base_load = base_loads[country]

                    # Time-based factors
                    hour = current_time.hour
                    day_of_week = current_time.weekday()

                    # Daily and weekly load patterns
                    daily_factor = 0.8 + 0.4 * (1 + np.cos(2 * np.pi * (hour - 19) / 24))
                    weekly_factor = 0.9 if day_of_week < 5 else 0.7

                    # Generate load values
                    load = base_load * daily_factor * weekly_factor
                    load += np.random.normal(0, base_load * 0.05)

                    # Generate renewable energy values (time and weather dependent)
                    solar = max(0, np.random.normal(3000, 1000)) if 6 <= hour <= 18 else 0
                    wind_onshore = max(0, np.random.exponential(8000))
                    wind_offshore = max(0, np.random.exponential(4000))  # Add offshore wind
                    hydro = base_load * 0.1 + np.random.normal(0, 500)
                    nuclear = base_load * 0.7 + np.random.normal(0, 1000)
                    fossil = max(0, load - (solar + wind_onshore + wind_offshore + hydro + nuclear))
                    other_renewable = max(0, np.random.normal(1000, 300))

                    total_generation = solar + wind_onshore + wind_offshore + hydro + nuclear + fossil + other_renewable

                    record = {
                        'timestamp': current_time,
                        'country_code': country,
                        'region_code': country,
                        'load_actual_mw': max(0, load),
                        'load_forecast_mw': max(0, load * (1 + np.random.normal(0, 0.02))),
                        'solar_generation_actual_mw': solar,
                        'wind_onshore_generation_actual_mw': wind_onshore,
                        'wind_offshore_generation_actual_mw': wind_offshore,  # Ensure this field is included
                        'hydro_generation_actual_mw': hydro,
                        'nuclear_generation_actual_mw': nuclear,
                        'fossil_generation_actual_mw': fossil,
                        'other_renewable_generation_mw': other_renewable,
                        'total_generation_mw': total_generation,
                        'net_import_export_mw': total_generation - load,
                        'price_day_ahead_eur_mwh': 50 + np.random.normal(0, 20),
                        'source': 'Synthetic Generator'
                    }

                    data.append(record)

                current_time += timedelta(hours=1)

            df = pd.DataFrame(data)
            logger.info(f"Generated synthetic grid data: {len(df)} records for {len(countries)} countries")
            return df

        except Exception as e:
            logger.error(f"Error generating synthetic grid data: {e}")
            return pd.DataFrame()

    def _insert_grid_data(self, df: pd.DataFrame) -> int:
        """Insert grid data with upsert capability"""
        if df.empty:
            return 0

        try:
            # Try direct insert first
            df.to_sql(
                'operations',
                self.db_connection.engine,
                schema='grid',
                if_exists='append',
                index=False,
                method='multi',
                chunksize=self.ingestion_config.batch_size
            )

            logger.info(f"Inserted {len(df)} grid records")
            return len(df)

        except Exception as e:
            # Handle duplicates with upsert
            if 'duplicate key' in str(e).lower() or 'unique constraint' in str(e).lower():
                logger.info(f"Handling grid data duplicates with upsert for {len(df)} records")
                return self._upsert_grid_data(df)
            else:
                logger.error(f"Grid data insertion failed: {e}")
                return 0

    def _upsert_grid_data(self, df: pd.DataFrame) -> int:
        """Upsert grid data with conflict resolution"""
        try:
            # In grid.py, change from %%(parameter)s to :parameter format
            upsert_sql = """
                         INSERT INTO grid.operations 
                         (timestamp, country_code, region_code, load_actual_mw, load_forecast_mw,
                          solar_generation_actual_mw, wind_onshore_generation_actual_mw,
                          wind_offshore_generation_actual_mw, hydro_generation_actual_mw,
                          nuclear_generation_actual_mw, fossil_generation_actual_mw,
                          other_renewable_generation_mw, total_generation_mw, net_import_export_mw,
                          price_day_ahead_eur_mwh, source)
                         VALUES (:timestamp, :country_code, :region_code, :load_actual_mw, :load_forecast_mw,
                                 :solar_generation_actual_mw, :wind_onshore_generation_actual_mw,
                                 :wind_offshore_generation_actual_mw, :hydro_generation_actual_mw,
                                 :nuclear_generation_actual_mw, :fossil_generation_actual_mw,
                                 :other_renewable_generation_mw, :total_generation_mw, :net_import_export_mw,
                                 :price_day_ahead_eur_mwh, \
                                 :source) ON CONFLICT (timestamp, country_code, region_code) DO \
                         UPDATE SET
                             load_actual_mw = EXCLUDED.load_actual_mw, \
                             load_forecast_mw = EXCLUDED.load_forecast_mw, \
                             solar_generation_actual_mw = EXCLUDED.solar_generation_actual_mw, \
                             wind_onshore_generation_actual_mw = EXCLUDED.wind_onshore_generation_actual_mw, \
                             wind_offshore_generation_actual_mw = EXCLUDED.wind_offshore_generation_actual_mw, \
                             hydro_generation_actual_mw = EXCLUDED.hydro_generation_actual_mw, \
                             nuclear_generation_actual_mw = EXCLUDED.nuclear_generation_actual_mw, \
                             fossil_generation_actual_mw = EXCLUDED.fossil_generation_actual_mw, \
                             other_renewable_generation_mw = EXCLUDED.other_renewable_generation_mw, \
                             total_generation_mw = EXCLUDED.total_generation_mw, \
                             net_import_export_mw = EXCLUDED.net_import_export_mw, \
                             price_day_ahead_eur_mwh = EXCLUDED.price_day_ahead_eur_mwh, \
                             ingestion_timestamp = NOW() \
                         """

            records = df.to_dict('records')
            inserted_count = 0

            batch_size = 200
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                self.db_connection.execute_many(upsert_sql, batch)
                inserted_count += len(batch)

            logger.info(f"Successfully upserted {inserted_count} grid records")
            return inserted_count

        except Exception as e:
            logger.error(f"Grid data upsert failed: {e}")
            return 0