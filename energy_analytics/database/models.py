#!/usr/bin/env python3
"""
Database model definitions and SQL statements
"""

# Schema creation SQL
SCHEMA_DEFINITIONS = """
-- Create schemas for data organization
CREATE SCHEMA IF NOT EXISTS household;
CREATE SCHEMA IF NOT EXISTS grid;
CREATE SCHEMA IF NOT EXISTS weather;
CREATE SCHEMA IF NOT EXISTS metadata;
"""

# Table creation statements
TABLE_DEFINITIONS = [
    # Household consumption table
    """
    CREATE TABLE IF NOT EXISTS household.consumption (
        timestamp TIMESTAMPTZ NOT NULL,
        household_id TEXT NOT NULL DEFAULT 'uci_france_001',
        global_active_power DECIMAL(8,3),
        global_reactive_power DECIMAL(8,3),
        voltage DECIMAL(6,2),
        global_intensity DECIMAL(6,2),
        sub_metering_1 DECIMAL(8,2),
        sub_metering_2 DECIMAL(8,2),
        sub_metering_3 DECIMAL(8,2),
        calculated_other_consumption DECIMAL(8,2),
        data_quality_score DECIMAL(3,2) DEFAULT 1.0,
        ingestion_timestamp TIMESTAMPTZ DEFAULT NOW(),
        source_file TEXT,
        PRIMARY KEY (timestamp, household_id)
    )
    """,
    
    # Household metadata table
    """
    CREATE TABLE IF NOT EXISTS household.metadata (
        household_id TEXT PRIMARY KEY,
        location_name TEXT,
        latitude DECIMAL(10,8),
        longitude DECIMAL(11,8),
        timezone TEXT,
        installation_date DATE,
        meter_type TEXT,
        sampling_frequency INTERVAL,
        data_source TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    
    # Grid operations table
    """
    CREATE TABLE IF NOT EXISTS grid.operations (
        timestamp TIMESTAMPTZ NOT NULL,
        country_code TEXT NOT NULL,
        region_code TEXT,
        load_actual_mw DECIMAL(10,2),
        load_forecast_mw DECIMAL(10,2),
        solar_generation_actual_mw DECIMAL(10,2),
        wind_onshore_generation_actual_mw DECIMAL(10,2),
        wind_offshore_generation_actual_mw DECIMAL(10,2),
        hydro_generation_actual_mw DECIMAL(10,2),
        nuclear_generation_actual_mw DECIMAL(10,2),
        fossil_generation_actual_mw DECIMAL(10,2),
        other_renewable_generation_mw DECIMAL(10,2),
        total_generation_mw DECIMAL(10,2),
        net_import_export_mw DECIMAL(10,2),
        carbon_intensity_g_co2_kwh DECIMAL(6,2),
        price_day_ahead_eur_mwh DECIMAL(8,2),
        data_quality_flags JSONB,
        ingestion_timestamp TIMESTAMPTZ DEFAULT NOW(),
        source TEXT DEFAULT 'ENTSO-E',
        PRIMARY KEY (timestamp, country_code, region_code)
    )
    """,
    
    # Grid infrastructure metadata
    """
    CREATE TABLE IF NOT EXISTS grid.infrastructure (
        region_code TEXT PRIMARY KEY,
        country_code TEXT NOT NULL,
        tso_name TEXT,
        installed_capacity_mw JSONB,
        grid_frequency_hz DECIMAL(4,2) DEFAULT 50.0,
        voltage_levels INTEGER[],
        interconnections TEXT[],
        last_updated TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    
    # Weather observations table
    """
    CREATE TABLE IF NOT EXISTS weather.observations (
        timestamp TIMESTAMPTZ NOT NULL,
        location_id TEXT NOT NULL,
        latitude DECIMAL(10,8),
        longitude DECIMAL(11,8),
        temperature_2m_c DECIMAL(5,2),
        relative_humidity_2m_pct DECIMAL(5,2),
        dew_point_2m_c DECIMAL(5,2),
        apparent_temperature_c DECIMAL(5,2),
        rain_mm DECIMAL(6,2),
        snowfall_mm DECIMAL(6,2),
        shortwave_radiation_w_m2 DECIMAL(7,2),
        wind_speed_10m_kmh DECIMAL(5,2),
        wind_direction_10m_deg DECIMAL(5,2),
        wind_gusts_10m_kmh DECIMAL(5,2),
        cloud_cover_pct DECIMAL(5,2),
        surface_pressure_hpa DECIMAL(7,2),
        visibility_m DECIMAL(8,2),
        weather_code INTEGER,
        data_provider TEXT,
        quality_control_flags JSONB,
        ingestion_timestamp TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (timestamp, location_id)
    )
    """,
    
    # Weather stations metadata
    """
    CREATE TABLE IF NOT EXISTS weather.stations (
        location_id TEXT PRIMARY KEY,
        station_name TEXT,
        latitude DECIMAL(10,8) NOT NULL,
        longitude DECIMAL(11,8) NOT NULL,
        elevation_m DECIMAL(6,1),
        timezone TEXT,
        country_code TEXT,
        region TEXT,
        station_type TEXT,
        data_provider TEXT,
        active_from DATE,
        active_to DATE,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    
    # Data ingestion tracking
    """
    CREATE TABLE IF NOT EXISTS metadata.ingestion_log (
        id SERIAL PRIMARY KEY,
        job_name TEXT NOT NULL,
        data_source TEXT NOT NULL,
        start_time TIMESTAMPTZ NOT NULL,
        end_time TIMESTAMPTZ,
        status TEXT NOT NULL,
        records_processed INTEGER DEFAULT 0,
        records_inserted INTEGER DEFAULT 0,
        records_updated INTEGER DEFAULT 0,
        records_rejected INTEGER DEFAULT 0,
        error_message TEXT,
        processing_duration_seconds INTEGER,
        file_size_bytes BIGINT,
        data_hash TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    
    # Data quality metrics
    """
    CREATE TABLE IF NOT EXISTS metadata.quality_metrics (
        id SERIAL PRIMARY KEY,
        table_name TEXT NOT NULL,
        metric_name TEXT NOT NULL,
        metric_value DECIMAL(5,4),
        measurement_time TIMESTAMPTZ NOT NULL,
        time_period INTERVAL,
        threshold_value DECIMAL(5,4),
        status TEXT,
        details JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """
]

# Index definitions
INDEX_DEFINITIONS = [
    # Household table indexes
    """CREATE INDEX IF NOT EXISTS idx_household_consumption_household_id
       ON household.consumption (household_id, timestamp DESC)""",
    
    """CREATE INDEX IF NOT EXISTS idx_household_consumption_quality
       ON household.consumption (data_quality_score) WHERE data_quality_score < 0.8""",
    
    # Grid table indexes
    """CREATE INDEX IF NOT EXISTS idx_grid_operations_country
       ON grid.operations (country_code, timestamp DESC)""",
    
    """CREATE INDEX IF NOT EXISTS idx_grid_operations_generation
       ON grid.operations (timestamp DESC) WHERE total_generation_mw IS NOT NULL""",
    
    # Weather table indexes
    """CREATE INDEX IF NOT EXISTS idx_weather_observations_location
       ON weather.observations (location_id, timestamp DESC)""",
    
    # Monitoring indexes
    """CREATE INDEX IF NOT EXISTS idx_ingestion_log_status
       ON metadata.ingestion_log (status, start_time DESC)""",
    
    """CREATE INDEX IF NOT EXISTS idx_quality_metrics_table
       ON metadata.quality_metrics (table_name, measurement_time DESC)"""
]

# Column mappings for data processing
HOUSEHOLD_COLUMN_MAPPING = {
    'Date': 'Date',
    'Time': 'Time',
    'Global_active_power': 'global_active_power',
    'Global_reactive_power': 'global_reactive_power',
    'Voltage': 'voltage',
    'Global_intensity': 'global_intensity',
    'Sub_metering_1': 'sub_metering_1',
    'Sub_metering_2': 'sub_metering_2',
    'Sub_metering_3': 'sub_metering_3'
}

WEATHER_VARIABLE_MAPPING = {
    'temperature_2m': 'temperature_2m_c',
    'relative_humidity_2m': 'relative_humidity_2m_pct',
    'dew_point_2m': 'dew_point_2m_c',
    'apparent_temperature': 'apparent_temperature_c',
    'rain': 'rain_mm',
    'shortwave_radiation': 'shortwave_radiation_w_m2',
    'wind_speed_10m': 'wind_speed_10m_kmh',
    'wind_direction_10m': 'wind_direction_10m_deg',
    'wind_gusts_10m': 'wind_gusts_10m_kmh',
    'cloud_cover': 'cloud_cover_pct',
    'surface_pressure': 'surface_pressure_hpa',
    'visibility': 'visibility_m'
}

OPSD_COLUMN_MAPPING = {
    'FR_load_actual_entsoe_transparency': 'load_actual_mw',
    'FR_load_forecast_entsoe_transparency': 'load_forecast_mw',
    'FR_solar_generation_actual': 'solar_generation_actual_mw',
    'FR_wind_onshore_generation_actual': 'wind_onshore_generation_actual_mw',
    'FR_wind_offshore_generation_actual': 'wind_offshore_generation_actual_mw',
    'FR_hydro_generation_actual': 'hydro_generation_actual_mw',
    'FR_nuclear_generation_actual': 'nuclear_generation_actual_mw',
    'FR_fossil_gas_generation_actual': 'fossil_generation_actual_mw',
    'FR_price_day_ahead': 'price_day_ahead_eur_mwh'
}

# Data validation ranges
WEATHER_VALIDATION_RANGES = {
    'temperature_2m_c': (-50, 60),
    'relative_humidity_2m_pct': (0, 100),
    'wind_speed_10m_kmh': (0, 200),
    'surface_pressure_hpa': (800, 1100),
    'rain_mm': (0, 200),
    'shortwave_radiation_w_m2': (0, 1500),
    'cloud_cover_pct': (0, 100),
    'visibility_m': (0, 50000)
}