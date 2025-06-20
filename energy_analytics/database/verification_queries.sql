-- Verification and Analysis Queries for Energy Analytics Pipeline

-- 1. Data Overview and Coverage
SELECT
    'Data Coverage Summary' as report_type,
    '' as metric,
    '' as value
UNION ALL
SELECT
    'Household Data',
    'Total Records',
    COUNT(*)::text
FROM household.consumption
UNION ALL
SELECT
    'Household Data',
    'Date Range',
    MIN(timestamp)::date::text || ' to ' || MAX(timestamp)::date::text
FROM household.consumption
UNION ALL
SELECT
    'Weather Data',
    'Total Records',
    COUNT(*)::text
FROM weather.observations
UNION ALL
SELECT
    'Weather Data',
    'Locations',
    COUNT(DISTINCT location_id)::text
FROM weather.observations
UNION ALL
SELECT
    'Grid Data',
    'Total Records',
    COUNT(*)::text
FROM grid.operations
UNION ALL
SELECT
    'Grid Data',
    'Countries',
    COUNT(DISTINCT country_code)::text
FROM grid.operations;

-- 2. Household Energy Consumption Analysis
SELECT
    DATE_TRUNC('day', timestamp) as day,
    ROUND(AVG(global_active_power), 2) as avg_power_kw,
    ROUND(MAX(global_active_power), 2) as peak_power_kw,
    ROUND(MIN(global_active_power), 2) as min_power_kw,
    ROUND(SUM((sub_metering_1 + sub_metering_2 + sub_metering_3) / 1000.0 / 60), 2) as daily_energy_kwh,
    COUNT(*) as measurements_per_day
FROM household.consumption
GROUP BY day
ORDER BY day DESC
LIMIT 7;

-- 3. Weather Patterns by Location
SELECT
    location_id,
    COUNT(*) as observations,
    ROUND(AVG(temperature_2m_c), 1) as avg_temp_c,
    ROUND(MIN(temperature_2m_c), 1) as min_temp_c,
    ROUND(MAX(temperature_2m_c), 1) as max_temp_c,
    ROUND(AVG(relative_humidity_2m_pct), 1) as avg_humidity_pct,
    ROUND(AVG(wind_speed_10m_kmh), 1) as avg_wind_kmh,
    ROUND(AVG(shortwave_radiation_w_m2), 0) as avg_solar_w_m2
FROM weather.observations
GROUP BY location_id
ORDER BY location_id;

-- 4. Grid Operations by Country
SELECT
    country_code,
    COUNT(*) as data_points,
    ROUND(AVG(load_actual_mw), 0) as avg_demand_mw,
    ROUND(MAX(load_actual_mw), 0) as peak_demand_mw,
    ROUND(AVG(solar_generation_actual_mw), 0) as avg_solar_mw,
    ROUND(AVG(wind_onshore_generation_actual_mw), 0) as avg_wind_mw,
    ROUND(AVG(nuclear_generation_actual_mw), 0) as avg_nuclear_mw,
    ROUND(AVG(price_day_ahead_eur_mwh), 2) as avg_price_eur_mwh
FROM grid.operations
GROUP BY country_code
ORDER BY country_code;

-- 5. Energy vs Weather Correlation Analysis
SELECT
    DATE_TRUNC('hour', h.timestamp) as hour,
    ROUND(AVG(h.global_active_power), 3) as avg_household_power_kw,
    ROUND(AVG(w.temperature_2m_c), 1) as avg_temperature_c,
    ROUND(AVG(w.shortwave_radiation_w_m2), 0) as avg_solar_radiation,
    ROUND(AVG(w.wind_speed_10m_kmh), 1) as avg_wind_speed
FROM household.consumption h
JOIN weather.observations w ON
    DATE_TRUNC('hour', h.timestamp) = w.timestamp
    AND w.location_id = 'paris_fr_001'
WHERE h.timestamp >= NOW() - INTERVAL '48 hours'
GROUP BY hour
ORDER BY hour DESC
LIMIT 24;

-- 6. Daily Energy Generation Mix (Grid Level)
SELECT
    DATE_TRUNC('day', timestamp) as day,
    country_code,
    ROUND(AVG(nuclear_generation_actual_mw), 0) as nuclear_mw,
    ROUND(AVG(hydro_generation_actual_mw), 0) as hydro_mw,
    ROUND(AVG(solar_generation_actual_mw), 0) as solar_mw,
    ROUND(AVG(wind_onshore_generation_actual_mw), 0) as wind_mw,
    ROUND(AVG(fossil_generation_actual_mw), 0) as fossil_mw,
    ROUND(AVG(total_generation_mw), 0) as total_mw,
    ROUND(100.0 * AVG(solar_generation_actual_mw + wind_onshore_generation_actual_mw)
          / NULLIF(AVG(total_generation_mw), 0), 1) as renewable_percentage
FROM grid.operations
GROUP BY day, country_code
ORDER BY day DESC, country_code
LIMIT 21; -- 7 days * 3 countries

-- 7. Peak Demand Analysis
SELECT
    'Peak Demand Analysis' as analysis_type,
    country_code,
    DATE_TRUNC('hour', timestamp) as peak_hour,
    ROUND(load_actual_mw, 0) as peak_demand_mw,
    ROUND(price_day_ahead_eur_mwh, 2) as price_at_peak_eur_mwh
FROM (
    SELECT
        country_code,
        timestamp,
        load_actual_mw,
        price_day_ahead_eur_mwh,
        ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY load_actual_mw DESC) as rn
    FROM grid.operations
) ranked
WHERE rn = 1
ORDER BY country_code;

-- 8. Data Quality Assessment
SELECT
    'Data Quality Assessment' as report_section,
    '' as metric,
    '' as result
UNION ALL
SELECT
    'Household Consumption',
    'Completeness (non-null power readings)',
    ROUND(100.0 * COUNT(global_active_power) / COUNT(*), 1) || '%'
FROM household.consumption
UNION ALL
SELECT
    'Household Consumption',
    'Average Quality Score',
    ROUND(AVG(data_quality_score), 3)::text
FROM household.consumption
UNION ALL
SELECT
    'Weather Observations',
    'Temperature Data Completeness',
    ROUND(100.0 * COUNT(temperature_2m_c) / COUNT(*), 1) || '%'
FROM weather.observations
UNION ALL
SELECT
    'Weather Observations',
    'Solar Radiation Completeness',
    ROUND(100.0 * COUNT(shortwave_radiation_w_m2) / COUNT(*), 1) || '%'
FROM weather.observations
UNION ALL
SELECT
    'Grid Operations',
    'Load Data Completeness',
    ROUND(100.0 * COUNT(load_actual_mw) / COUNT(*), 1) || '%'
FROM grid.operations;

-- 9. Time Series Continuity Check
SELECT
    'Time Series Continuity' as check_type,
    table_name,
    expected_intervals,
    actual_intervals,
    ROUND(100.0 * actual_intervals / expected_intervals, 1) || '%' as completeness
FROM (
    SELECT
        'household.consumption' as table_name,
        EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) / 60 as expected_intervals,
        COUNT(*) as actual_intervals
    FROM household.consumption
    UNION ALL
    SELECT
        'weather.observations',
        EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) / 3600,
        COUNT(*)
    FROM weather.observations
    UNION ALL
    SELECT
        'grid.operations',
        EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) / 3600,
        COUNT(*)
    FROM grid.operations
) continuity_check;

-- 10. Recent Pipeline Performance
SELECT
    job_name,
    data_source,
    start_time,
    CASE
        WHEN end_time IS NULL THEN 'Running'
        ELSE status
    END as status,
    records_processed,
    records_inserted,
    CASE
        WHEN processing_duration_seconds IS NULL THEN 'In Progress'
        ELSE processing_duration_seconds || ' seconds'
    END as processing_time,
    CASE
        WHEN error_message IS NULL THEN 'Success'
        ELSE 'Error: ' || LEFT(error_message, 50) || '...'
    END as result
FROM metadata.ingestion_log
ORDER BY start_time DESC
LIMIT 10;