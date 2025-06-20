#!/usr/bin/env python3
"""
Streaming data ingestion module for real-time data processing
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd

from config.settings import DatabaseConfig
from database.connection import DatabaseConnection
from database.schema import SchemaManager

logger = logging.getLogger(__name__)


class StreamingIngestionManager:
    """Handles real-time streaming data ingestion"""

    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.db_connection = DatabaseConnection(db_config)
        self.batch_buffer = []
        self.buffer_size = 100
        self.flush_interval = 300  # 5 minutes
        self.is_running = False

    async def start_streaming(self):
        """Start streaming data ingestion"""
        logger.info("Starting streaming ingestion")

        if not self.db_connection.connect():
            logger.error("Failed to connect to database")
            return

        self.is_running = True

        # Start streaming tasks
        tasks = [
            self._stream_weather_data(),
            self._stream_grid_data(),
            self._stream_household_data(),
            self._flush_buffer_periodically()
        ]

        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            logger.info("Streaming ingestion stopped by user")
        except Exception as e:
            logger.error(f"Streaming ingestion error: {e}")
        finally:
            self.is_running = False
            await self._flush_buffer()  # Final flush
            self.db_connection.disconnect()

    async def stop_streaming(self):
        """Stop streaming ingestion gracefully"""
        logger.info("Stopping streaming ingestion...")
        self.is_running = False
        await self._flush_buffer()

    async def _stream_weather_data(self):
        """Stream weather data in real-time"""
        locations = [
            {'location_id': 'paris_fr_001', 'lat': 48.8566, 'lon': 2.3522},
            {'location_id': 'berlin_de_001', 'lat': 52.5200, 'lon': 13.4050},
            {'location_id': 'madrid_es_001', 'lat': 40.4168, 'lon': -3.7038}
        ]

        while self.is_running:
            try:
                for location in locations:
                    # Simulate real-time weather data
                    weather_record = {
                        'timestamp': datetime.now(),
                        'location_id': location['location_id'],
                        'latitude': location['lat'],
                        'longitude': location['lon'],
                        'temperature_2m_c': 15 + np.random.normal(0, 5),
                        'relative_humidity_2m_pct': 50 + np.random.normal(0, 20),
                        'wind_speed_10m_kmh': 10 + np.random.exponential(5),
                        'wind_direction_10m_deg': np.random.uniform(0, 360),
                        'cloud_cover_pct': np.random.uniform(0, 100),
                        'surface_pressure_hpa': 1013 + np.random.normal(0, 10),
                        'shortwave_radiation_w_m2': max(0, 500 * np.sin(
                            np.pi * (datetime.now().hour - 6) / 12) * np.random.uniform(0.5, 1.0)),
                        'rain_mm': max(0, np.random.exponential(0.1)),
                        'data_provider': 'Streaming Simulator'
                    }

                    self.batch_buffer.append(('weather', weather_record))

                    if len(self.batch_buffer) >= self.buffer_size:
                        await self._flush_buffer()

                # Wait before next update (weather updates every minute)
                await asyncio.sleep(60)

            except Exception as e:
                logger.error(f"Streaming weather error: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes on error

    async def _stream_grid_data(self):
        """Stream grid data in real-time"""
        countries = ['FR', 'DE', 'ES']

        while self.is_running:
            try:
                for country in countries:
                    # Base load values
                    base_loads = {'FR': 50000, 'DE': 60000, 'ES': 35000}
                    base_load = base_loads[country]

                    # Time-based factors
                    hour = datetime.now().hour
                    daily_factor = 0.8 + 0.4 * (1 + np.cos(2 * np.pi * (hour - 19) / 24))

                    # Generate grid data
                    load = base_load * daily_factor + np.random.normal(0, base_load * 0.05)

                    grid_record = {
                        'timestamp': datetime.now(),
                        'country_code': country,
                        'region_code': country,
                        'load_actual_mw': max(0, load),
                        'load_forecast_mw': max(0, load * (1 + np.random.normal(0, 0.02))),
                        'solar_generation_actual_mw': max(0, np.random.normal(3000, 1000)) if 6 <= hour <= 18 else 0,
                        'wind_onshore_generation_actual_mw': max(0, np.random.exponential(8000)),
                        'nuclear_generation_actual_mw': base_load * 0.7 + np.random.normal(0, 1000),
                        'hydro_generation_actual_mw': base_load * 0.1 + np.random.normal(0, 500),
                        'price_day_ahead_eur_mwh': 50 + np.random.normal(0, 20),
                        'source': 'Streaming Simulator'
                    }

                    # Calculate total generation
                    generation_fields = [
                        'solar_generation_actual_mw', 'wind_onshore_generation_actual_mw',
                        'nuclear_generation_actual_mw', 'hydro_generation_actual_mw'
                    ]
                    grid_record['total_generation_mw'] = sum(grid_record.get(f, 0) for f in generation_fields)

                    self.batch_buffer.append(('grid', grid_record))

                # Wait before next update (grid updates every 15 minutes)
                await asyncio.sleep(900)

            except Exception as e:
                logger.error(f"Streaming grid error: {e}")
                await asyncio.sleep(300)

    async def _stream_household_data(self):
        """Stream household consumption data in real-time"""
        household_id = 'uci_france_001'

        while self.is_running:
            try:
                # Time-based consumption patterns
                hour = datetime.now().hour
                base_consumption = 0.5 + 0.3 * (1 + np.cos(2 * np.pi * (hour - 19) / 24))

                # Generate household consumption data
                global_active_power = base_consumption + np.random.normal(0, 0.2)

                household_record = {
                    'timestamp': datetime.now(),
                    'household_id': household_id,
                    'global_active_power': max(0, global_active_power),
                    'global_reactive_power': max(0, global_active_power * 0.2 + np.random.normal(0, 0.05)),
                    'voltage': 240 + np.random.normal(0, 5),
                    'global_intensity': max(0, global_active_power * 4.2 + np.random.normal(0, 0.5)),
                    'sub_metering_1': max(0, global_active_power * 0.3 * 1000 / 60),
                    'sub_metering_2': max(0, global_active_power * 0.2 * 1000 / 60),
                    'sub_metering_3': max(0, global_active_power * 0.1 * 1000 / 60),
                    'data_quality_score': 1.0,
                    'source_file': 'streaming'
                }

                # Calculate other consumption
                total_sub_metering = (household_record['sub_metering_1'] +
                                      household_record['sub_metering_2'] +
                                      household_record['sub_metering_3'])
                household_record['calculated_other_consumption'] = max(0,
                                                                       household_record[
                                                                           'global_active_power'] * 1000 / 60 - total_sub_metering)

                self.batch_buffer.append(('household', household_record))

                # Wait before next update (household updates every minute)
                await asyncio.sleep(60)

            except Exception as e:
                logger.error(f"Streaming household error: {e}")
                await asyncio.sleep(300)

    async def _flush_buffer_periodically(self):
        """Periodically flush the buffer"""
        while self.is_running:
            await asyncio.sleep(self.flush_interval)
            if self.batch_buffer:
                await self._flush_buffer()

    async def _flush_buffer(self):
        """Flush accumulated records to database"""
        if not self.batch_buffer:
            return

        try:
            # Group records by type
            weather_records = []
            grid_records = []
            household_records = []

            for data_type, record in self.batch_buffer:
                if data_type == 'weather':
                    weather_records.append(record)
                elif data_type == 'grid':
                    grid_records.append(record)
                elif data_type == 'household':
                    household_records.append(record)

            # Insert in batches
            total_inserted = 0

            if weather_records:
                df_weather = pd.DataFrame(weather_records)
                inserted = self._insert_streaming_data(df_weather, 'weather', 'observations')
                total_inserted += inserted

            if grid_records:
                df_grid = pd.DataFrame(grid_records)
                inserted = self._insert_streaming_data(df_grid, 'grid', 'operations')
                total_inserted += inserted

            if household_records:
                df_household = pd.DataFrame(household_records)
                inserted = self._insert_streaming_data(df_household, 'household', 'consumption')
                total_inserted += inserted

            logger.info(f"Flushed {total_inserted} streaming records to database")
            self.batch_buffer.clear()

        except Exception as e:
            logger.error(f"Buffer flush failed: {e}")

    def _insert_streaming_data(self, df: pd.DataFrame, schema: str, table: str) -> int:
        """Insert streaming data into database"""
        try:
            df.to_sql(
                table,
                self.db_connection.engine,
                schema=schema,
                if_exists='append',
                index=False
            )
            return len(df)
        except Exception as e:
            logger.error(f"Failed to insert streaming data to {schema}.{table}: {e}")
            return 0

    async def get_streaming_stats(self) -> Dict[str, Any]:
        """Get streaming statistics"""
        stats = {
            'is_running': self.is_running,
            'buffer_size': len(self.batch_buffer),
            'buffer_types': {}
        }

        # Count records by type in buffer
        for data_type, _ in self.batch_buffer:
            stats['buffer_types'][data_type] = stats['buffer_types'].get(data_type, 0) + 1

        return stats


class StreamingDataSimulator:
    """Simulates real-time data streams for testing"""

    def __init__(self):
        self.is_running = False

    async def simulate_sensor_stream(self, callback, interval: float = 1.0):
        """Simulate a sensor data stream"""
        self.is_running = True

        while self.is_running:
            try:
                # Generate sensor data
                sensor_data = {
                    'timestamp': datetime.now(),
                    'sensor_id': f'sensor_{np.random.randint(1, 10)}',
                    'value': np.random.normal(50, 10),
                    'unit': 'units',
                    'quality': np.random.choice(['good', 'warning', 'error'], p=[0.9, 0.08, 0.02])
                }

                # Call the callback with the data
                await callback(sensor_data)

                # Wait for next reading
                await asyncio.sleep(interval)

            except Exception as e:
                logger.error(f"Sensor simulation error: {e}")
                await asyncio.sleep(interval * 5)

    def stop(self):
        """Stop the simulation"""
        self.is_running = False


async def demo_streaming():
    """Demo function for streaming ingestion"""
    print("Starting streaming ingestion demo...")
    print("This will simulate real-time data streams.")
    print("Press Ctrl+C to stop.\n")

    # Create database configuration
    from config.settings import DatabaseConfig
    db_config = DatabaseConfig()

    # Create streaming manager
    manager = StreamingIngestionManager(db_config)

    try:
        # Start streaming
        await manager.start_streaming()
    except KeyboardInterrupt:
        print("\nStopping streaming demo...")
        await manager.stop_streaming()
        print("Streaming demo stopped.")


if __name__ == "__main__":
    # Run the demo
    asyncio.run(demo_streaming())