#!/usr/bin/env python3
"""
Weather data ingestion module for Open-Meteo API
"""

import logging
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests

from config.settings import DatabaseConfig, IngestionConfig, WEATHER_LOCATIONS
from database.models import WEATHER_VARIABLE_MAPPING, WEATHER_VALIDATION_RANGES
from ingestion.base import BaseIngestionPipeline
from monitoring.logging_utils import LogContext

logger = logging.getLogger(__name__)


class WeatherIngestionPipeline(BaseIngestionPipeline):
    """Handles ingestion of weather data from Open-Meteo API"""

    def __init__(self, db_config: DatabaseConfig, ingestion_config: IngestionConfig):
        super().__init__(db_config, ingestion_config)
        self.weather_locations = WEATHER_LOCATIONS
        self.forecast_url = "https://api.open-meteo.com/v1/forecast"
        self.archive_url = "https://archive-api.open-meteo.com/v1/era5"

    def ingest_data(self) -> int:
        """Main ingestion method for weather data"""
        return self.ingest_batch_data()

    def ingest_batch_data(self) -> int:
        """Batch ingestion of weather data for the last 24 hours"""
        logger.info("Starting batch weather data ingestion")

        end_time = datetime.now()
        start_time = end_time - timedelta(days=1)

        total_records_inserted = 0

        # Connect if not connected
        if not self.db_connection.test_connection():
            self.connect()

        for location_name, location_info in self.weather_locations.items():
            try:
                with LogContext(f"Weather ingestion for {location_name}", logger):
                    weather_data = self._fetch_weather_data(
                        location_info['lat'],
                        location_info['lon'],
                        start_time,
                        end_time,
                        location_info['location_id']
                    )

                    if not weather_data.empty:
                        weather_data = self.validate_data(weather_data)
                        inserted = self._insert_weather_data(weather_data)
                        total_records_inserted += inserted
                        logger.info(f"Inserted {inserted} records for {location_name}")
                    else:
                        logger.warning(f"No weather data retrieved for {location_name}")

                    # Respect API rate limits
                    time.sleep(self.ingestion_config.weather_api_delay)

            except Exception as e:
                logger.error(f"Failed to ingest weather data for {location_name}: {e}")
                continue

        logger.info(f"Batch weather ingestion completed: {total_records_inserted} total records inserted")
        return total_records_inserted

    def process_historical_data(self, start_date: datetime, end_date: datetime) -> int:
        """Process historical weather data for a date range"""
        logger.info(f"Processing historical weather data from {start_date} to {end_date}")

        total_records_inserted = 0

        # Connect if not connected
        if not self.db_connection.test_connection():
            self.connect()

        # Process in chunks to avoid API limits
        current_date = start_date
        chunk_days = 30  # Process 30 days at a time

        while current_date < end_date:
            chunk_end = min(current_date + timedelta(days=chunk_days), end_date)

            logger.info(f"Processing weather data chunk: {current_date} to {chunk_end}")

            for location_name, location_info in self.weather_locations.items():
                try:
                    weather_data = self._fetch_weather_data(
                        location_info['lat'],
                        location_info['lon'],
                        current_date,
                        chunk_end,
                        location_info['location_id']
                    )

                    if not weather_data.empty:
                        weather_data = self.validate_data(weather_data)
                        inserted = self._insert_weather_data(weather_data)
                        total_records_inserted += inserted

                    # Respect API rate limits
                    time.sleep(self.ingestion_config.weather_api_delay)

                except Exception as e:
                    logger.error(f"Failed to process historical weather data for {location_name}: {e}")
                    continue

            current_date = chunk_end + timedelta(hours=1)

        logger.info(f"Historical weather processing completed: {total_records_inserted} total records")
        return total_records_inserted

    def validate_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate weather data"""
        if data.empty:
            return data

        initial_count = len(data)

        # Apply range validation
        for variable, (min_val, max_val) in WEATHER_VALIDATION_RANGES.items():
            if variable in data.columns:
                # Mark out-of-range values as NaN
                invalid_mask = (data[variable] < min_val) | (data[variable] > max_val)
                invalid_count = invalid_mask.sum()

                if invalid_count > 0:
                    logger.warning(f"Found {invalid_count} out-of-range values for {variable}")
                    data.loc[invalid_mask, variable] = np.nan

        # Remove rows where all weather data is missing
        weather_cols = [col for col in data.columns if
                        col not in ['timestamp', 'location_id', 'latitude', 'longitude', 'data_provider']]
        data = data.dropna(subset=weather_cols, how='all')

        # Interpolate missing values
        for col in weather_cols:
            if col in data.columns:
                data[col] = data[col].ffill().bfill()

        final_count = len(data)
        if final_count < initial_count:
            logger.info(f"Validation removed {initial_count - final_count} invalid weather records")

        return data

    def _fetch_weather_data(self, lat: float, lon: float, start_time: datetime,
                            end_time: datetime, location_id: str) -> pd.DataFrame:
        """Fetch weather data from Open-Meteo API"""
        try:
            # Determine which API to use based on date
            now = datetime.now()
            if end_time <= now - timedelta(days=5):
                # Use historical archive API
                base_url = self.archive_url
            else:
                # Use forecast API with historical capabilities
                base_url = self.forecast_url

            params = {
                'latitude': lat,
                'longitude': lon,
                'start_date': start_time.strftime('%Y-%m-%d'),
                'end_date': end_time.strftime('%Y-%m-%d'),
                'hourly': [
                    'temperature_2m', 'relative_humidity_2m', 'dew_point_2m',
                    'apparent_temperature', 'rain', 'shortwave_radiation',
                    'wind_speed_10m', 'wind_direction_10m', 'wind_gusts_10m',
                    'cloud_cover', 'surface_pressure', 'visibility'
                ],
                'timezone': 'UTC',
                'format': 'json'
            }

            # Add model parameter for archive API
            if 'archive-api' in base_url:
                params['models'] = 'era5'

            logger.info(f"Fetching weather data from {base_url} for location {location_id}")

            response = requests.get(base_url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            # Check if we got valid data
            if 'hourly' not in data or 'time' not in data['hourly']:
                logger.error(f"Invalid weather API response for {location_id}")
                return pd.DataFrame()

            # Process the response
            weather_records = []
            times = data['hourly']['time']
            hourly_data = data['hourly']

            for i, time_str in enumerate(times):
                try:
                    record = {
                        'timestamp': pd.to_datetime(time_str),
                        'location_id': location_id,
                        'latitude': lat,
                        'longitude': lon,
                        'data_provider': 'Open-Meteo ERA5' if 'archive-api' in base_url else 'Open-Meteo Forecast'
                    }

                    # Map API variables to our schema
                    for api_var, db_var in WEATHER_VARIABLE_MAPPING.items():
                        if api_var in hourly_data and i < len(hourly_data[api_var]):
                            value = hourly_data[api_var][i]
                            record[db_var] = value if value is not None else np.nan
                        else:
                            record[db_var] = np.nan

                    weather_records.append(record)

                except Exception as e:
                    logger.warning(f"Error processing weather record {i}: {e}")
                    continue

            if weather_records:
                df = pd.DataFrame(weather_records)

                # Ensure timezone awareness
                if not pd.api.types.is_datetime64tz_dtype(df['timestamp']):
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    if df['timestamp'].dt.tz is None:
                        df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')

                logger.info(f"Successfully fetched {len(df)} weather records for {location_id}")
                return df
            else:
                logger.warning(f"No valid weather records processed for {location_id}")
                return pd.DataFrame()

        except requests.exceptions.RequestException as e:
            logger.error(f"Weather API request failed for {location_id}: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Weather data fetch failed for {location_id}: {e}")
            return pd.DataFrame()

    def _insert_weather_data(self, df: pd.DataFrame) -> int:
        """Insert weather data with upsert capability"""
        if df.empty:
            return 0

        try:
            # Try direct insert first
            df.to_sql(
                'observations',
                self.db_connection.engine,
                schema='weather',
                if_exists='append',
                index=False,
                method='multi',
                chunksize=self.ingestion_config.batch_size
            )

            logger.info(f"Inserted {len(df)} weather records")
            return len(df)

        except Exception as e:
            # Handle duplicates with upsert
            if 'duplicate key' in str(e).lower() or 'unique constraint' in str(e).lower():
                logger.info(f"Handling weather data duplicates with upsert for {len(df)} records")
                return self._upsert_weather_data(df)
            else:
                logger.error(f"Weather data insertion failed: {e}")
                return 0

    def _upsert_weather_data(self, df: pd.DataFrame) -> int:
        """Upsert weather data with conflict resolution"""
        try:
            upsert_sql = """
                         INSERT INTO weather.observations
                         (timestamp, location_id, latitude, longitude, temperature_2m_c, relative_humidity_2m_pct,
                          dew_point_2m_c, apparent_temperature_c, rain_mm, shortwave_radiation_w_m2,
                          wind_speed_10m_kmh, wind_direction_10m_deg, wind_gusts_10m_kmh, cloud_cover_pct,
                          surface_pressure_hpa, visibility_m, data_provider)
                         VALUES (:timestamp, :location_id, :latitude, :longitude, :temperature_2m_c,
                                 :relative_humidity_2m_pct, :dew_point_2m_c, :apparent_temperature_c,
                                 :rain_mm, :shortwave_radiation_w_m2, :wind_speed_10m_kmh,
                                 :wind_direction_10m_deg, :wind_gusts_10m_kmh, :cloud_cover_pct,
                                 :surface_pressure_hpa, :visibility_m, \
                                 :data_provider) ON CONFLICT (timestamp, location_id) DO \
                         UPDATE SET
                             temperature_2m_c = EXCLUDED.temperature_2m_c, \
                             relative_humidity_2m_pct = EXCLUDED.relative_humidity_2m_pct, \
                             dew_point_2m_c = EXCLUDED.dew_point_2m_c, \
                             apparent_temperature_c = EXCLUDED.apparent_temperature_c, \
                             rain_mm = EXCLUDED.rain_mm, \
                             shortwave_radiation_w_m2 = EXCLUDED.shortwave_radiation_w_m2, \
                             wind_speed_10m_kmh = EXCLUDED.wind_speed_10m_kmh, \
                             wind_direction_10m_deg = EXCLUDED.wind_direction_10m_deg, \
                             wind_gusts_10m_kmh = EXCLUDED.wind_gusts_10m_kmh, \
                             cloud_cover_pct = EXCLUDED.cloud_cover_pct, \
                             surface_pressure_hpa = EXCLUDED.surface_pressure_hpa, \
                             visibility_m = EXCLUDED.visibility_m, \
                             ingestion_timestamp = NOW() \
                         """

            records = df.to_dict('records')
            inserted_count = 0

            batch_size = 500
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                self.db_connection.execute_many(upsert_sql, batch)
                inserted_count += len(batch)

            logger.info(f"Successfully upserted {inserted_count} weather records")
            return inserted_count

        except Exception as e:
            logger.error(f"Weather data upsert failed: {e}")
            return 0

    def get_data_gaps(self, location_id: str, days_back: int = 30) -> pd.DataFrame:
        """
            Identify
            gaps in weather
            data
            for a location"""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)

            # Query existing data
            query = """
                    SELECT
                    timestamp
                    FROM
                    weather.observations
                    WHERE
                    location_id =:location_id
                    AND
                    timestamp >=: start_date
                    AND
                    timestamp <=: end_date
                
                
                ORDER
                BY
                timestamp
                """

            result = self.db_connection.execute_query(query, {
                'location_id': location_id,
                'start_date': start_date,
                'end_date': end_date
            })

            existing_timestamps = pd.DataFrame(result.fetchall(), columns=['timestamp'])

            if existing_timestamps.empty:
                # No data exists, entire range is a gap
                return pd.DataFrame({
                    'gap_start': [start_date],
                    'gap_end': [end_date],
                    'gap_hours': [(end_date - start_date).total_seconds() / 3600]
                })

            # Convert to datetime index
            existing_timestamps['timestamp'] = pd.to_datetime(existing_timestamps['timestamp'])
            existing_timestamps.set_index('timestamp', inplace=True)

            # Create expected hourly range
            expected_range = pd.date_range(start=start_date, end=end_date, freq='H')

            # Find missing timestamps
            missing_timestamps = expected_range.difference(existing_timestamps.index)

            if len(missing_timestamps) == 0:
                return pd.DataFrame()  # No gaps

            # Group consecutive missing timestamps into gaps
            gaps = []
            gap_start = missing_timestamps[0]
            prev_timestamp = missing_timestamps[0]

            for timestamp in missing_timestamps[1:]:
                if (timestamp - prev_timestamp).total_seconds() > 3600:  # More than 1 hour gap
                    gaps.append({
                        'gap_start': gap_start,
                        'gap_end': prev_timestamp,
                        'gap_hours': (prev_timestamp - gap_start).total_seconds() / 3600 + 1
                    })
                    gap_start = timestamp
                prev_timestamp = timestamp

            # Add the last gap
            gaps.append({
                'gap_start': gap_start,
                'gap_end': prev_timestamp,
                'gap_hours': (prev_timestamp - gap_start).total_seconds() / 3600 + 1
            })

            return pd.DataFrame(gaps)

        except Exception as e:
            logger.error(f"Failed to identify data gaps: {e}")
            return pd.DataFrame()

    def fill_data_gaps(self, location_id: str, max_gap_days: int = 7) -> int:
        """
        Fill
        gaps in weather
        data
        for a location"""
        logger.info(f"Checking for data gaps in weather data for {location_id}")

        # Find gaps
        gaps_df = self.get_data_gaps(location_id, days_back=90)

        if gaps_df.empty:
            logger.info(f"No data gaps found for {location_id}")
            return 0

        # Get location info
        location_info = None
        for loc_name, loc_data in self.weather_locations.items():
            if loc_data['location_id'] == location_id:
                location_info = loc_data
                break

        if not location_info:
            logger.error(f"Location {location_id} not found in configuration")
            return 0

        total_records_filled = 0

        # Fill gaps that are not too large
        for _, gap in gaps_df.iterrows():
            gap_days = gap['gap_hours'] / 24

            if gap_days <= max_gap_days:
                logger.info(f"Filling gap from {gap['gap_start']} to {gap['gap_end']} ({gap['gap_hours']:.1f} hours)")

                try:
                    weather_data = self._fetch_weather_data(
                        location_info['lat'],
                        location_info['lon'],
                        gap['gap_start'],
                        gap['gap_end'],
                        location_id
                    )

                    if not weather_data.empty:
                        weather_data = self.validate_data(weather_data)
                        inserted = self._insert_weather_data(weather_data)
                        total_records_filled += inserted

                    # Respect API rate limits
                    time.sleep(self.ingestion_config.weather_api_delay)

                except Exception as e:
                    logger.error(f"Failed to fill gap: {e}")
                    continue
            else:
                logger.warning(f"Gap too large to fill: {gap_days:.1f} days")

        logger.info(f"Filled {total_records_filled} weather records for {location_id}")
        return total_records_filled