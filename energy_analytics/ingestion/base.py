#!/usr/bin/env python3
"""
Base ingestion pipeline functionality
"""

import json
import logging
import schedule
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import pandas as pd
from sqlalchemy import text

from config.settings import DatabaseConfig, IngestionConfig
from database.connection import DatabaseConnection
from database.schema import SchemaManager
from monitoring.job_tracking import JobTracker

logger = logging.getLogger(__name__)


class BaseIngestionPipeline(ABC):
    """Abstract base class for data ingestion pipelines"""
    
    def __init__(self, db_config: DatabaseConfig, ingestion_config: IngestionConfig):
        self.db_config = db_config
        self.ingestion_config = ingestion_config
        self.db_connection = DatabaseConnection(db_config)
        self.schema_manager = SchemaManager(self.db_connection)
        self.job_tracker = JobTracker(self.db_connection)
        
    @abstractmethod
    def ingest_data(self) -> int:
        """Abstract method for ingesting data. Returns number of records processed."""
        pass
    
    @abstractmethod
    def validate_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Abstract method for validating data before insertion."""
        pass
    
    def connect(self) -> bool:
        """Connect to database"""
        return self.db_connection.connect()
    
    def disconnect(self):
        """Disconnect from database"""
        self.db_connection.disconnect()
    
    def insert_metadata(self, table: str, data: List[tuple], conflict_column: str = None):
        """Generic method to insert metadata with conflict handling"""
        # Implementation will be in specific ingestion classes
        pass


class DataIngestionPipeline:
    """Main data ingestion pipeline orchestrator"""
    
    def __init__(self, db_config: DatabaseConfig, ingestion_config: IngestionConfig):
        self.db_config = db_config
        self.ingestion_config = ingestion_config
        self.db_connection = DatabaseConnection(db_config)
        self.schema_manager = SchemaManager(self.db_connection)
        self.job_tracker = JobTracker(self.db_connection)
        
        # Initialize specific pipelines (will be imported later)
        self.household_pipeline = None
        self.weather_pipeline = None
        self.grid_pipeline = None
        
    def start_pipeline(self):
        """Initialize and start the ingestion pipeline"""
        logger.info("Starting Energy Data Ingestion Pipeline")
        
        # Connect to database
        if not self.db_connection.connect():
            raise RuntimeError("Failed to connect to database")
        
        # Create schema if it doesn't exist
        self.schema_manager.create_database_schema()
        
        # Insert initial metadata
        self._insert_initial_metadata()
        
        # Initialize specific pipelines
        self._initialize_pipelines()
        
        # Schedule batch jobs
        self._schedule_batch_jobs()
        
        logger.info("Pipeline started successfully")
    
    def stop_pipeline(self):
        """Gracefully stop the pipeline"""
        logger.info("Stopping ingestion pipeline")
        schedule.clear()
        self.db_connection.disconnect()
        logger.info("Pipeline stopped")
    
    def _initialize_pipelines(self):
        """Initialize specific ingestion pipelines"""
        # Import here to avoid circular imports
        from ingestion.household import HouseholdIngestionPipeline
        from ingestion.weather import WeatherIngestionPipeline
        from ingestion.grid import GridIngestionPipeline
        
        self.household_pipeline = HouseholdIngestionPipeline(self.db_config, self.ingestion_config)
        self.weather_pipeline = WeatherIngestionPipeline(self.db_config, self.ingestion_config)
        self.grid_pipeline = GridIngestionPipeline(self.db_config, self.ingestion_config)

    def _insert_initial_metadata(self):
        """Insert initial metadata records"""
        try:
            # Insert household metadata
            household_metadata_sql = """
                                     INSERT INTO household.metadata
                                     (household_id, location_name, latitude, longitude, timezone,
                                      installation_date, meter_type, sampling_frequency, data_source)
                                     VALUES (:household_id, :location_name, :latitude, :longitude, :timezone,
                                             :installation_date, :meter_type, :sampling_frequency, \
                                             :data_source) ON CONFLICT (household_id) DO \
                                     UPDATE SET updated_at = NOW() \
                                     """

            household_data = {
                'household_id': 'uci_france_001',
                'location_name': 'Sceaux, France',
                'latitude': 48.8566,
                'longitude': 2.3522,
                'timezone': 'Europe/Paris',
                'installation_date': '2006-12-01',
                'meter_type': 'Smart Meter',
                'sampling_frequency': '1 minute',
                'data_source': 'UCI ML Repository'
            }

            # Insert weather station metadata
            weather_metadata_sql = """
                                   INSERT INTO weather.stations
                                   (location_id, station_name, latitude, longitude, elevation_m,
                                    timezone, country_code, region, station_type, data_provider, active_from)
                                   VALUES (:location_id, :station_name, :latitude, :longitude, :elevation_m,
                                           :timezone, :country_code, :region, :station_type, :data_provider, \
                                           :active_from) ON CONFLICT (location_id) DO \
                                   UPDATE SET created_at = NOW() \
                                   """

            weather_stations = [
                {
                    'location_id': 'paris_fr_001',
                    'station_name': 'Paris Metro Area',
                    'latitude': 48.8566,
                    'longitude': 2.3522,
                    'elevation_m': 35.0,
                    'timezone': 'Europe/Paris',
                    'country_code': 'FR',
                    'region': 'Île-de-France',
                    'station_type': 'reanalysis',
                    'data_provider': 'Open-Meteo ERA5',
                    'active_from': '1940-01-01'
                },
                {
                    'location_id': 'berlin_de_001',
                    'station_name': 'Berlin Metro Area',
                    'latitude': 52.5200,
                    'longitude': 13.4050,
                    'elevation_m': 34.0,
                    'timezone': 'Europe/Berlin',
                    'country_code': 'DE',
                    'region': 'Berlin',
                    'station_type': 'reanalysis',
                    'data_provider': 'Open-Meteo ERA5',
                    'active_from': '1940-01-01'
                },
                {
                    'location_id': 'madrid_es_001',
                    'station_name': 'Madrid Metro Area',
                    'latitude': 40.4168,
                    'longitude': -3.7038,
                    'elevation_m': 650.0,
                    'timezone': 'Europe/Madrid',
                    'country_code': 'ES',
                    'region': 'Madrid',
                    'station_type': 'reanalysis',
                    'data_provider': 'Open-Meteo ERA5',
                    'active_from': '1940-01-01'
                }
            ]

            # Insert grid infrastructure metadata
            grid_metadata_sql = """
                                INSERT INTO grid.infrastructure
                                (region_code, country_code, tso_name, installed_capacity_mw,
                                 grid_frequency_hz, voltage_levels, interconnections)
                                VALUES (:region_code, :country_code, :tso_name, :installed_capacity_mw,
                                        :grid_frequency_hz, :voltage_levels, \
                                        :interconnections) ON CONFLICT (region_code) DO \
                                UPDATE SET last_updated = NOW() \
                                """

            # In the grid metadata insertion, change these lines:
            grid_data = [
                {
                    'region_code': 'FR',
                    'country_code': 'FR',
                    'tso_name': 'RTE (Réseau de Transport d\'Électricité)',
                    'installed_capacity_mw': json.dumps(
                        {'nuclear': 63130, 'hydro': 25500, 'wind': 17380, 'solar': 13067, 'gas': 12500, 'coal': 3000}),
                    'grid_frequency_hz': 50.0,
                    'voltage_levels': [63, 90, 225, 400],  # Change from json.dumps to actual array
                    'interconnections': ['ES', 'BE', 'DE', 'CH', 'IT', 'GB']  # Change from json.dumps to actual array
                },
                {
                    'region_code': 'DE',
                    'country_code': 'DE',
                    'tso_name': 'Amprion GmbH',
                    'installed_capacity_mw': json.dumps(
                        {'wind': 59312, 'solar': 54000, 'coal': 42000, 'gas': 29000, 'nuclear': 8100, 'hydro': 9600}),
                    'grid_frequency_hz': 50.0,
                    'voltage_levels': [110, 220, 380],  # Change from json.dumps to actual array
                    'interconnections': ['FR', 'NL', 'BE', 'LU', 'CH', 'AT', 'CZ', 'PL', 'DK']
                    # Change from json.dumps to actual array
                },
                {
                    'region_code': 'ES',
                    'country_code': 'ES',
                    'tso_name': 'Red Eléctrica de España (REE)',
                    'installed_capacity_mw': json.dumps(
                        {'wind': 27446, 'hydro': 20300, 'solar': 15000, 'nuclear': 7000, 'gas': 25000, 'coal': 9500}),
                    'grid_frequency_hz': 50.0,
                    'voltage_levels': [132, 220, 400],  # Change from json.dumps to actual array
                    'interconnections': ['FR', 'PT', 'MA']  # Change from json.dumps to actual array
                }
            ]

            # Execute insertions
            self.db_connection.execute_transaction(household_metadata_sql, household_data)

            for station_data in weather_stations:
                self.db_connection.execute_transaction(weather_metadata_sql, station_data)

            for grid_info in grid_data:
                self.db_connection.execute_transaction(grid_metadata_sql, grid_info)

            logger.info("Metadata inserted successfully")

        except Exception as e:
            logger.error(f"Metadata insertion failed: {e}")

    def _schedule_batch_jobs(self):
        """Schedule automated batch processing jobs"""
        # Schedule daily weather data ingestion at 02:00
        schedule.every().day.at("02:00").do(self._batch_ingest_weather_data)
        
        # Schedule daily grid data ingestion at 03:00
        schedule.every().day.at("03:00").do(self._batch_ingest_grid_data)
        
        # Schedule weekly household data processing on Sundays at 01:00
        schedule.every().sunday.at("01:00").do(self._batch_process_household_data)
        
        # Schedule daily data quality checks at 05:00
        schedule.every().day.at("05:00").do(self._run_data_quality_checks)
        
        # Schedule weekly cleanup at 04:00 on Sundays
        schedule.every().sunday.at("04:00").do(self._cleanup_old_data)
        
        logger.info("Batch jobs scheduled")
    
    def _batch_ingest_weather_data(self):
        """Batch ingestion of weather data"""
        if self.weather_pipeline:
            job_id = self.job_tracker.start_job("batch_weather_ingestion", "Open-Meteo API")
            try:
                records = self.weather_pipeline.ingest_batch_data()
                self.job_tracker.complete_job(job_id, "completed", records, records)
            except Exception as e:
                self.job_tracker.complete_job(job_id, "failed", 0, 0, str(e))
    
    def _batch_ingest_grid_data(self):
        """Batch ingestion of grid data"""
        if self.grid_pipeline:
            job_id = self.job_tracker.start_job("batch_grid_ingestion", "Open Power System Data")
            try:
                records = self.grid_pipeline.ingest_batch_data()
                self.job_tracker.complete_job(job_id, "completed", records, records)
            except Exception as e:
                self.job_tracker.complete_job(job_id, "failed", 0, 0, str(e))
    
    def _batch_process_household_data(self):
        """Process household consumption data files"""
        if self.household_pipeline:
            job_id = self.job_tracker.start_job("batch_household_processing", "UCI Household Files")
            try:
                records = self.household_pipeline.process_batch_files()
                self.job_tracker.complete_job(job_id, "completed", records, records)
            except Exception as e:
                self.job_tracker.complete_job(job_id, "failed", 0, 0, str(e))
    
    def _run_data_quality_checks(self):
        """Run automated data quality checks"""
        from monitoring.quality_checks import DataQualityChecker
        quality_checker = DataQualityChecker(self.db_connection)
        quality_checker.run_all_checks()
    
    def _cleanup_old_data(self):
        """Clean up old data based on retention policies"""
        from monitoring.job_tracking import cleanup_old_logs
        cleanup_old_logs(self.db_connection)
    
    def run_scheduler(self):
        """Run the job scheduler"""
        logger.info("Starting job scheduler")
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except KeyboardInterrupt:
                logger.info("Scheduler stopped by user")
                break
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                time.sleep(300)  # Wait 5 minutes before retrying

    def process_historical_data(self, start_date: str = None, end_date: str = None):
        """Process historical data for initial database population"""
        logger.info("Starting historical data processing")

        try:
            # Default to last 30 days if no dates provided
            if not end_date:
                end_date = datetime.now().replace(tzinfo=None)  # Ensure timezone-naive
            else:
                end_date = pd.to_datetime(end_date).replace(tzinfo=None)

            if not start_date:
                start_date = (end_date - timedelta(days=30)).replace(tzinfo=None)
            else:
                start_date = pd.to_datetime(start_date).replace(tzinfo=None)

            logger.info(f"Processing historical data from {start_date} to {end_date}")

            # Process each data type
            if self.household_pipeline:
                self.household_pipeline.process_historical_data(start_date, end_date)

            if self.weather_pipeline:
                self.weather_pipeline.process_historical_data(start_date, end_date)

            if self.grid_pipeline:
                self.grid_pipeline.process_historical_data(start_date, end_date)

            logger.info("Historical data processing completed")

        except Exception as e:
            logger.error(f"Historical data processing failed: {e}")
            raise