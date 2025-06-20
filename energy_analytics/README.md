# Energy Analytics Data Ingestion Pipeline

A comprehensive data ingestion pipeline for energy analytics, designed to collect, process, and store energy consumption, weather, and grid operations data in a TimescaleDB database.

## Features

- **Multi-source Data Ingestion**:
  - UCI Household Electric Power Consumption dataset
  - Open-Meteo Weather API (historical and real-time)
  - Open Power System Data (European grid data)
  - ENTSO-E API support (with authentication)

- **TimescaleDB Integration**:
  - Optimized time-series storage
  - Automatic data partitioning
  - Compression policies
  - Continuous aggregates
  - Data retention management

- **Robust Pipeline Architecture**:
  - Scheduled batch processing
  - Real-time streaming capabilities
  - Data quality monitoring
  - Job tracking and logging
  - Error handling and retry logic

## Project Structure

```
energy_analytics/
├── config/               # Configuration management
├── database/            # Database connection and schema
├── ingestion/           # Data ingestion modules
├── data_sources/        # External data source clients
├── monitoring/          # Monitoring and quality checks
├── utils/               # Utility functions
└── main.py             # Main entry point
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/sourin00/green-power-utilities.git
cd green-power-utilities
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scriptsctivate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Install the package:
```bash
pip install -e .
```

## Configuration

The pipeline uses a YAML configuration file (`pipeline_config.yaml`) that is automatically created on first run. You can modify it to match your environment:

```yaml
database:
  host: localhost
  port: 5432
  database: energy_analytics
  username: postgres
  password: your_password

ingestion:
  batch_size: 1000
  max_retries: 3
  weather_api_delay: 0.1
  data_retention_days: 1095
```

## Database Setup

1. Install PostgreSQL and TimescaleDB:
```bash
# Ubuntu/Debian
sudo apt-get install postgresql postgresql-contrib
# Follow TimescaleDB installation guide for your OS
```

2. Create the database:
```sql
CREATE DATABASE energy_analytics;
CREATE USER energy_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE energy_analytics TO energy_user;
ALTER USER energy_user WITH SUPERUSER;
```

3. Setup the schema:
```bash
energy-pipeline setup
```

## Usage

### Run the Main Pipeline
```bash
energy-pipeline run
```

### Import Historical Data
```bash
energy-pipeline historical 2023-01-01 2023-12-31
```

### Test Data Sources
```bash
energy-pipeline test
```

### Check Pipeline Status
```bash
energy-pipeline status
```

### Run Streaming Demo
```bash
energy-pipeline streaming
```

## Data Sources

### UCI Household Data
- **Source**: UCI Machine Learning Repository
- **Dataset**: Individual household electric power consumption
- **Frequency**: 1-minute intervals
- **Location**: Sceaux, France
- **Auto-download**: Yes

### Weather Data
- **Source**: Open-Meteo API
- **Coverage**: Paris, Berlin, Madrid
- **Variables**: Temperature, humidity, wind, solar radiation, etc.
- **Frequency**: Hourly
- **Historical**: Available via ERA5 reanalysis

### Grid Operations Data
- **Source**: Open Power System Data / ENTSO-E
- **Coverage**: France, Germany, Spain
- **Variables**: Load, generation by source, prices
- **Frequency**: 15-minute to hourly
- **Real-time**: Yes

## Scheduled Jobs

The pipeline automatically schedules the following jobs:

- **02:00 Daily**: Weather data ingestion
- **03:00 Daily**: Grid operations data ingestion
- **01:00 Sunday**: Household data processing
- **05:00 Daily**: Data quality checks
- **04:00 Sunday**: Old data cleanup

## Data Quality Monitoring

The pipeline includes comprehensive data quality checks:

- **Freshness**: Ensures data is up-to-date
- **Completeness**: Checks for missing values
- **Accuracy**: Validates data against business rules
- **Consistency**: Ensures data relationships are maintained

## Development

### Running Tests
```bash
pytest tests/
```

### Code Formatting
```bash
black .
flake8 .
```

### Type Checking
```bash
mypy .
```

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check PostgreSQL is running
   - Verify credentials in config file
   - Ensure TimescaleDB extension is installed

2. **API Rate Limits**
   - Adjust `weather_api_delay` in config
   - Implement backoff strategies

3. **Memory Issues**
   - Reduce `batch_size` in config
   - Process historical data in smaller date ranges

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- UCI Machine Learning Repository for the household consumption dataset
- Open-Meteo for weather data API
- Open Power System Data for grid operations data
- TimescaleDB for time-series database capabilities

## Support

For issues and questions:
- Create an issue in the GitHub repository
- Contact the data team at data-team@greenpower.com

## Roadmap

- [ ] Add support for more data sources
- [ ] Implement machine learning models for forecasting
- [ ] Add real-time dashboard
- [ ] Support for distributed processing
- [ ] Enhanced anomaly detection
- [ ] API for data access