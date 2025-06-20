# Migration Guide: From Single File to Modular Structure

This guide helps you migrate from the original single-file `energy_ingestion_pipeline.py` to the new modular structure.

## Step-by-Step Migration

### 1. Create Directory Structure

First, create the new directory structure:

```bash
mkdir -p energy_analytics/{config,database,ingestion,data_sources,monitoring,utils,tests}
```

### 2. Create __init__.py Files

Create empty `__init__.py` files in each directory:

```bash
touch energy_analytics/__init__.py
touch energy_analytics/config/__init__.py
touch energy_analytics/database/__init__.py
touch energy_analytics/ingestion/__init__.py
touch energy_analytics/data_sources/__init__.py
touch energy_analytics/monitoring/__init__.py
touch energy_analytics/utils/__init__.py
touch energy_analytics/tests/__init__.py
```

### 3. Copy Module Files

Copy the content from the artifacts to their respective files:

1. **Configuration Files**:
   - `config/settings.py` - Configuration classes
   - Update `config/__init__.py` with exports

2. **Database Files**:
   - `database/connection.py` - Database connection management
   - `database/schema.py` - Schema creation
   - `database/models.py` - Table definitions
   - Update `database/__init__.py` with exports

3. **Base Ingestion**:
   - `ingestion/base.py` - Base pipeline classes
   - Update `ingestion/__init__.py` with exports

4. **Monitoring Files**:
   - `monitoring/logging_utils.py` - Logging setup
   - `monitoring/job_tracking.py` - Job tracking
   - Update `monitoring/__init__.py` with exports

5. **Utility Files**:
   - `utils/cli.py` - CLI utilities
   - Update `utils/__init__.py` with exports

6. **Main Entry Point**:
   - `main.py` - Main execution file

### 4. Create Remaining Ingestion Modules

Create the specific ingestion modules that were referenced but not yet created:

#### ingestion/household.py
```python
# This file should contain:
# - HouseholdIngestionPipeline class
# - Methods from original file:
#   - _download_uci_household_data
#   - _load_uci_household_file
#   - _clean_uci_data
#   - _process_household_dataframe
#   - _upsert_household_data
```

#### ingestion/weather.py
```python
# This file should contain:
# - WeatherIngestionPipeline class
# - Methods from original file:
#   - _fetch_weather_data
#   - _validate_weather_data
#   - _insert_weather_data
#   - _upsert_weather_data
```

#### ingestion/grid.py
```python
# This file should contain:
# - GridIngestionPipeline class
# - Methods from original file:
#   - _fetch_opsd_data
#   - _fetch_alternative_grid_data
#   - _process_opsd_data
#   - _fetch_entsoe_data
#   - _generate_sample_grid_data
#   - _insert_grid_data
#   - _upsert_grid_data
```

#### ingestion/streaming.py
```python
# This file should contain:
# - StreamingIngestionManager class (already in original)
# - All streaming-related methods
```

### 5. Create Additional Utility Modules

#### monitoring/quality_checks.py
```python
# This file should contain:
# - DataQualityChecker class
# - Methods from original file:
#   - _check_data_freshness
#   - _check_data_completeness
#   - _check_data_accuracy
#   - _log_quality_metric
```

#### monitoring/status.py
```python
# This file should contain:
# - PipelineStatusReporter class
# - show_pipeline_status functionality
```

#### utils/test_sources.py
```python
# This file should contain:
# - DataSourceTester class
# - test_data_sources functionality
```

#### utils/helpers.py
```python
# Additional helper functions that don't fit in cli.py
```

### 6. Update Imports

After creating all files, update the imports throughout the codebase. For example:

```python
# Old import
from dataclasses import dataclass

# New imports
from config.settings import DatabaseConfig, IngestionConfig
from database.connection import DatabaseConnection
```

### 7. Install and Test

1. Install the package in development mode:
```bash
pip install -e .
```

2. Run tests to ensure everything works:
```bash
# Test database connection
energy-pipeline test

# Test schema creation
energy-pipeline setup

# Run the pipeline
energy-pipeline run
```

### 8. Benefits After Migration

- **Better Organization**: Code is logically separated by functionality
- **Easier Testing**: Individual modules can be tested in isolation
- **Improved Maintainability**: Changes to one module don't affect others
- **Better Collaboration**: Multiple developers can work on different modules
- **Reusability**: Modules can be imported and used in other projects
- **Scalability**: New features can be added as new modules

### 9. Next Steps

1. Add comprehensive unit tests for each module
2. Add type hints throughout the codebase
3. Add docstrings to all classes and methods
4. Set up continuous integration
5. Add performance monitoring
6. Consider adding async support for better concurrency

## Troubleshooting

### Import Errors
If you get import errors, ensure:
- All `__init__.py` files are in place
- The package is installed with `pip install -e .`
- You're running from the correct directory

### Missing Functionality
If some functionality is missing:
- Check that all methods were properly moved
- Verify all imports are updated
- Ensure database connections are passed correctly

### Configuration Issues
- Make sure `pipeline_config.yaml` is in the correct location
- Verify all configuration values are properly loaded
- Check environment variables if used