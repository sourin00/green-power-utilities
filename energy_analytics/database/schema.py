#!/usr/bin/env python3
"""
Database schema creation and management for TimescaleDB
"""

import logging
from typing import List, Dict, Tuple
from sqlalchemy import text

from database.connection import DatabaseConnection
from database.models import SCHEMA_DEFINITIONS, TABLE_DEFINITIONS, INDEX_DEFINITIONS

logger = logging.getLogger(__name__)


class SchemaManager:
    """Manages database schema creation and updates"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        
    def create_database_schema(self) -> bool:
        """Create complete database schema"""
        # Check and install extensions
        if not self.check_and_install_extensions():
            logger.warning("Some extensions could not be installed, continuing anyway")
        
        # Create schemas
        if not self.create_schemas():
            return False
        
        # Create tables
        if not self.create_tables():
            return False
        
        # Create hypertables
        self.create_hypertables()
        
        # Create indexes
        self.create_indexes()
        
        # Create roles
        self.create_roles()
        
        logger.info("Database schema created successfully")
        return True
    
    def check_and_install_extensions(self) -> bool:
        """Check and install required PostgreSQL extensions"""
        required_extensions = ['timescaledb', 'cube', 'earthdistance']
        all_installed = True
        
        for ext in required_extensions:
            if not self.db.check_extension(ext):
                if not self.db.install_extension(ext):
                    all_installed = False
                    if ext in ['cube', 'earthdistance']:
                        logger.warning("Spatial indexing will be disabled")
                    
        return all_installed
    
    def create_schemas(self) -> bool:
        """Create database schemas"""
        try:
            self.db.execute_transaction(SCHEMA_DEFINITIONS)
            logger.info("Created schemas")
            return True
        except Exception as e:
            logger.error(f"Schema creation failed: {e}")
            return False
    
    def create_tables(self) -> bool:
        """Create all database tables"""
        created_count = 0
        
        for i, table_sql in enumerate(TABLE_DEFINITIONS):
            try:
                self.db.execute_transaction(table_sql)
                created_count += 1
                logger.info(f"Created table {i+1}/{len(TABLE_DEFINITIONS)}")
            except Exception as e:
                logger.error(f"Failed to create table {i+1}: {e}")
                continue
        
        return created_count > 0
    
    def create_hypertables(self):
        """Create TimescaleDB hypertables"""
        hypertables = [
            ("household.consumption", "timestamp", "1 day"),
            ("grid.operations", "timestamp", "6 hours"),
            ("weather.observations", "timestamp", "12 hours")
        ]
        
        for table, time_column, chunk_interval in hypertables:
            try:
                self.db.execute_transaction(
                    f"""
                    SELECT create_hypertable('{table}', '{time_column}', 
                                           chunk_time_interval => INTERVAL '{chunk_interval}',
                                           if_not_exists => TRUE)
                    """
                )
                logger.info(f"Created hypertable: {table}")
            except Exception as e:
                logger.warning(f"Failed to create hypertable {table}: {e}")
    
    def create_indexes(self):
        """Create database indexes"""
        for index_sql in INDEX_DEFINITIONS:
            try:
                self.db.execute_transaction(index_sql)
                logger.info("Created index")
            except Exception as e:
                logger.warning(f"Failed to create index: {e}")
        
        # Try to create spatial index if earthdistance is available
        if self.db.check_extension('earthdistance'):
            try:
                self.db.execute_transaction(
                    """CREATE INDEX IF NOT EXISTS idx_weather_observations_spatial
                       ON weather.observations USING GIST (ll_to_earth(latitude, longitude))"""
                )
                logger.info("Created spatial index")
            except Exception as e:
                logger.warning(f"Failed to create spatial index: {e}")
    
    def create_roles(self):
        """Create database roles with PostgreSQL version compatibility"""
        role_creation_sql = """
        DO $$
        BEGIN
            -- Create energy_reader role
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'energy_reader') THEN
                EXECUTE 'CREATE ROLE energy_reader';
            END IF;

            -- Create energy_writer role
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'energy_writer') THEN
                EXECUTE 'CREATE ROLE energy_writer';
            END IF;

            -- Create energy_admin role
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'energy_admin') THEN
                EXECUTE 'CREATE ROLE energy_admin';
            END IF;
        END
        $$ LANGUAGE plpgsql;
        """
        
        permission_sqls = [
            "GRANT USAGE ON SCHEMA household, grid, weather, metadata TO energy_reader",
            "GRANT SELECT ON ALL TABLES IN SCHEMA household, grid, weather, metadata TO energy_reader",
            "GRANT energy_reader TO energy_writer",
            "GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA household, grid, weather TO energy_writer",
            "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA metadata TO energy_writer",
            "GRANT energy_writer TO energy_admin",
            "GRANT CREATE ON SCHEMA household, grid, weather, metadata TO energy_admin"
        ]
        
        try:
            self.db.execute_transaction(role_creation_sql)
            logger.info("Created database roles")
            
            for perm_sql in permission_sqls:
                try:
                    self.db.execute_transaction(perm_sql)
                except Exception as e:
                    logger.warning(f"Failed to grant permission: {e}")
            
            logger.info("Granted role permissions")
        except Exception as e:
            logger.warning(f"Failed to create roles: {e}")
    
    def setup_timescale_policies(self) -> bool:
        """Setup TimescaleDB policies after data is loaded"""
        logger.info("Setting up TimescaleDB policies...")
        
        # Check if hypertables have data before adding policies
        hypertables_with_data = self._get_hypertables_with_data()
        
        if not hypertables_with_data:
            logger.warning("No hypertables with data found. Skipping policy setup.")
            return False
        
        # Setup compression policies
        self._setup_compression_policies(hypertables_with_data)
        
        # Setup retention policies
        self._setup_retention_policies(hypertables_with_data)
        
        # Setup continuous aggregates
        self._setup_continuous_aggregates(hypertables_with_data)
        
        return True
    
    def _get_hypertables_with_data(self) -> List[str]:
        """Get list of hypertables that have data"""
        hypertables_with_data = []
        
        tables_to_check = [
            ('household', 'consumption'),
            ('grid', 'operations'),
            ('weather', 'observations')
        ]
        
        for schema, table in tables_to_check:
            count = self.db.get_table_count(schema, table)
            if count > 0:
                hypertables_with_data.append(table)
                logger.info(f"Hypertable {schema}.{table} has {count:,} records")
            else:
                logger.info(f"Hypertable {schema}.{table} is empty")
        
        return hypertables_with_data
    
    def _setup_compression_policies(self, hypertables: List[str]):
        """Setup compression policies for hypertables with data"""
        compression_intervals = {
            'consumption': '7 days',
            'operations': '7 days',
            'observations': '7 days'
        }
        
        for table in hypertables:
            if table in compression_intervals:
                try:
                    # Enable compression
                    self.db.execute_transaction(
                        f"ALTER TABLE {table} SET (timescaledb.compress = true)"
                    )
                    
                    # Add compression policy
                    self.db.execute_transaction(
                        f"""
                        SELECT add_compression_policy('{table}', 
                                                    INTERVAL '{compression_intervals[table]}',
                                                    if_not_exists => TRUE)
                        """
                    )
                    logger.info(f"Added compression policy for {table}")
                except Exception as e:
                    logger.warning(f"Failed to add compression policy for {table}: {e}")
    
    def _setup_retention_policies(self, hypertables: List[str]):
        """Setup retention policies for hypertables with data"""
        retention_intervals = {
            'consumption': '3 years',
            'operations': '3 years',
            'observations': '3 years'
        }
        
        for table in hypertables:
            if table in retention_intervals:
                try:
                    self.db.execute_transaction(
                        f"""
                        SELECT add_retention_policy('{table}', 
                                                  INTERVAL '{retention_intervals[table]}',
                                                  if_not_exists => TRUE)
                        """
                    )
                    logger.info(f"Added retention policy for {table}")
                except Exception as e:
                    logger.warning(f"Failed to add retention policy for {table}: {e}")
    
    def _setup_continuous_aggregates(self, hypertables: List[str]):
        """Setup continuous aggregates for hypertables with data"""
        if 'consumption' in hypertables:
            self._create_household_daily_aggregate()
        
        if 'operations' in hypertables:
            self._create_grid_hourly_aggregate()
        
        if 'observations' in hypertables:
            self._create_weather_daily_aggregate()
    
    def _create_household_daily_aggregate(self):
        """Create daily household consumption aggregate"""
        try:
            self.db.execute_transaction(
                """
                CREATE MATERIALIZED VIEW IF NOT EXISTS household.daily_consumption
                WITH (timescaledb.continuous) AS
                SELECT 
                    time_bucket('1 day', timestamp) AS day,
                    household_id,
                    AVG(global_active_power) AS avg_active_power_kw,
                    MAX(global_active_power) AS peak_active_power_kw,
                    SUM(sub_metering_1 + sub_metering_2 + sub_metering_3) / 1000.0 AS total_energy_kwh,
                    AVG(voltage) AS avg_voltage_v,
                    COUNT(*) AS measurements_count,
                    AVG(data_quality_score) AS avg_quality_score
                FROM household.consumption
                GROUP BY day, household_id
                """
            )
            
            # Add refresh policy
            self.db.execute_transaction(
                """
                SELECT add_continuous_aggregate_policy('daily_consumption',
                    start_offset => INTERVAL '3 days',
                    end_offset => INTERVAL '1 hour',
                    schedule_interval => INTERVAL '1 hour',
                    if_not_exists => TRUE)
                """
            )
            logger.info("Created household daily consumption aggregate")
        except Exception as e:
            logger.warning(f"Failed to create household daily aggregate: {e}")
    
    def _create_grid_hourly_aggregate(self):
        """Create hourly grid operations aggregate"""
        try:
            self.db.execute_transaction(
                """
                CREATE MATERIALIZED VIEW IF NOT EXISTS grid.hourly_operations
                WITH (timescaledb.continuous) AS
                SELECT 
                    time_bucket('1 hour', timestamp) AS hour,
                    country_code,
                    AVG(load_actual_mw) AS avg_load_mw,
                    MAX(load_actual_mw) AS peak_load_mw,
                    AVG(solar_generation_actual_mw) AS avg_solar_mw,
                    AVG(wind_onshore_generation_actual_mw) AS avg_wind_mw,
                    AVG(total_generation_mw) AS avg_total_generation_mw,
                    AVG(price_day_ahead_eur_mwh) AS avg_price_eur_mwh,
                    COUNT(*) AS data_points
                FROM grid.operations
                GROUP BY hour, country_code
                """
            )
            
            # Add refresh policy
            self.db.execute_transaction(
                """
                SELECT add_continuous_aggregate_policy('hourly_operations',
                    start_offset => INTERVAL '1 day',
                    end_offset => INTERVAL '15 minutes',
                    schedule_interval => INTERVAL '15 minutes',
                    if_not_exists => TRUE)
                """
            )
            logger.info("Created grid hourly operations aggregate")
        except Exception as e:
            logger.warning(f"Failed to create grid hourly aggregate: {e}")
    
    def _create_weather_daily_aggregate(self):
        """Create daily weather summary aggregate"""
        try:
            self.db.execute_transaction(
                """
                CREATE MATERIALIZED VIEW IF NOT EXISTS weather.daily_summary
                WITH (timescaledb.continuous) AS
                SELECT 
                    time_bucket('1 day', timestamp) AS day,
                    location_id,
                    AVG(temperature_2m_c) AS avg_temp_c,
                    MIN(temperature_2m_c) AS min_temp_c,
                    MAX(temperature_2m_c) AS max_temp_c,
                    AVG(relative_humidity_2m_pct) AS avg_humidity_pct,
                    SUM(rain_mm) AS total_rain_mm,
                    AVG(shortwave_radiation_w_m2) AS avg_solar_radiation_w_m2,
                    AVG(wind_speed_10m_kmh) AS avg_wind_speed_kmh,
                    MAX(wind_speed_10m_kmh) AS max_wind_speed_kmh,
                    COUNT(*) AS observations_count
                FROM weather.observations
                GROUP BY day, location_id
                """
            )
            
            # Add refresh policy
            self.db.execute_transaction(
                """
                SELECT add_continuous_aggregate_policy('daily_summary',
                    start_offset => INTERVAL '2 days',
                    end_offset => INTERVAL '30 minutes',
                    schedule_interval => INTERVAL '30 minutes',
                    if_not_exists => TRUE)
                """
            )
            logger.info("Created weather daily summary aggregate")
        except Exception as e:
            logger.warning(f"Failed to create weather daily aggregate: {e}")
    
    def verify_schema(self) -> bool:
        """Verify that the database schema was created correctly"""
        logger.info("Verifying database schema...")
        
        try:
            # Check schemas
            result = self.db.execute_query(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name IN ('household', 'grid', 'weather', 'metadata')
                """
            )
            schemas = [row[0] for row in result]
            logger.info(f"Created schemas: {schemas}")
            
            # Check tables
            result = self.db.execute_query(
                """
                SELECT schemaname, tablename
                FROM pg_tables
                WHERE schemaname IN ('household', 'grid', 'weather', 'metadata')
                ORDER BY schemaname, tablename
                """
            )
            tables = [(row[0], row[1]) for row in result]
            logger.info(f"Created tables: {len(tables)} total")
            for schema, table in tables:
                logger.info(f"  {schema}.{table}")
            
            # Check hypertables
            hypertables = self.db.get_hypertables()
            logger.info(f"Created hypertables: {len(hypertables)} total")
            for table, chunks in hypertables:
                logger.info(f"  {table}: {chunks} chunks")
            
            # Check extensions
            result = self.db.execute_query(
                """
                SELECT extname
                FROM pg_extension
                WHERE extname IN ('timescaledb', 'cube', 'earthdistance')
                """
            )
            extensions = [row[0] for row in result]
            logger.info(f"Installed extensions: {extensions}")
            
            return True
            
        except Exception as e:
            logger.error(f"Schema verification failed: {e}")
            return False