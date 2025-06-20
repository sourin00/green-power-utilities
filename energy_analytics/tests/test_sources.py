#!/usr/bin/env python3
"""
Data source connectivity testing utilities
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import requests
from pathlib import Path

from config.settings import DatabaseConfig, IngestionConfig
from database.connection import DatabaseConnection
from monitoring.logging_utils import LogContext

logger = logging.getLogger(__name__)


class DataSourceTester:
    """Test connectivity and functionality of all data sources"""

    def __init__(self, db_config: DatabaseConfig, ingestion_config: IngestionConfig):
        self.db_config = db_config
        self.ingestion_config = ingestion_config
        self.test_results = {}

    def run_all_tests(self) -> Dict[str, Any]:
        """Run all data source tests"""
        print("\n" + "=" * 60)
        print("DATA SOURCE CONNECTIVITY TESTS")
        print("=" * 60)

        # Test database connection
        self._test_database_connection()

        # Test UCI household data
        self._test_uci_data_source()

        # Test weather API
        self._test_weather_api()

        # Test OPSD data
        self._test_opsd_data()

        # Test ENTSO-E API
        self._test_entsoe_api()

        # Print summary
        self._print_test_summary()

        return self.test_results

    def _test_database_connection(self):
        """Test database connectivity"""
        test_name = "PostgreSQL/TimescaleDB Connection"
        print(f"\n1. Testing {test_name}...")

        try:
            db_connection = DatabaseConnection(self.db_config)

            # Test basic connection
            if db_connection.connect():
                print("   ✅ Database connection successful")

                # Test TimescaleDB extension
                if db_connection.check_extension('timescaledb'):
                    print("   ✅ TimescaleDB extension installed")

                    # Get hypertables info
                    hypertables = db_connection.get_hypertables()
                    if hypertables:
                        print(f"   ✅ Found {len(hypertables)} hypertables")
                        for table, chunks in hypertables[:3]:  # Show first 3
                            print(f"      - {table}: {chunks} chunks")
                    else:
                        print("   ⚠️  No hypertables found (run setup first)")
                else:
                    print("   ❌ TimescaleDB extension not installed")

                # Test table counts
                test_tables = [
                    ('household', 'consumption'),
                    ('weather', 'observations'),
                    ('grid', 'operations')
                ]

                for schema, table in test_tables:
                    if db_connection.table_exists(schema, table):
                        count = db_connection.get_table_count(schema, table)
                        print(f"   📊 {schema}.{table}: {count:,} records")

                db_connection.disconnect()
                self.test_results[test_name] = {'status': 'success', 'message': 'Connected successfully'}

            else:
                print("   ❌ Database connection failed")
                self.test_results[test_name] = {'status': 'failed', 'message': 'Connection failed'}

        except Exception as e:
            print(f"   ❌ Database test failed: {e}")
            self.test_results[test_name] = {'status': 'error', 'message': str(e)}

    def _test_uci_data_source(self):
        """Test UCI household data availability"""
        test_name = "UCI Household Data"
        print(f"\n2. Testing {test_name}...")

        try:
            # Check if local file exists
            local_files = [
                Path("data/raw/household_power_consumption.txt"),
                Path("data/raw/household_power_consumption.csv"),
                Path("data/raw/Individual_household_electric_power_consumption.txt")
            ]

            local_file_found = any(f.exists() for f in local_files)

            if local_file_found:
                print("   ✅ UCI data found locally")
                for f in local_files:
                    if f.exists():
                        size_mb = f.stat().st_size / 1024 / 1024
                        print(f"      - {f.name}: {size_mb:.1f} MB")
            else:
                print("   ℹ️  UCI data not found locally")

            # Test download URL
            uci_url = "https://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip"
            print(f"   Testing download URL: {uci_url}")

            response = requests.head(uci_url, timeout=10, allow_redirects=True)
            if response.status_code == 200:
                content_length = response.headers.get('content-length')
                if content_length:
                    size_mb = int(content_length) / 1024 / 1024
                    print(f"   ✅ UCI dataset available for download ({size_mb:.1f} MB)")
                else:
                    print("   ✅ UCI dataset URL accessible")

                self.test_results[test_name] = {'status': 'success', 'message': 'Dataset available'}
            else:
                print(f"   ⚠️  UCI dataset URL returned status {response.status_code}")
                self.test_results[test_name] = {'status': 'warning', 'message': f'HTTP {response.status_code}'}

        except requests.exceptions.RequestException as e:
            print(f"   ❌ UCI dataset test failed: {e}")
            self.test_results[test_name] = {'status': 'failed', 'message': str(e)}
        except Exception as e:
            print(f"   ❌ UCI test error: {e}")
            self.test_results[test_name] = {'status': 'error', 'message': str(e)}

    def _test_weather_api(self):
        """Test Open-Meteo weather API"""
        test_name = "Open-Meteo Weather API"
        print(f"\n3. Testing {test_name}...")

        try:
            # Test current weather API
            test_lat, test_lon = 48.8566, 2.3522  # Paris
            current_url = "https://api.open-meteo.com/v1/forecast"

            params = {
                'latitude': test_lat,
                'longitude': test_lon,
                'current_weather': True
            }

            print(f"   Testing current weather for Paris ({test_lat}, {test_lon})...")
            response = requests.get(current_url, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if 'current_weather' in data:
                    current = data['current_weather']
                    print(f"   ✅ Current weather API working")
                    print(f"      - Temperature: {current.get('temperature', 'N/A')}°C")
                    print(f"      - Wind Speed: {current.get('windspeed', 'N/A')} km/h")
                else:
                    print("   ⚠️  API responded but no current weather data")

                # Test historical API
                historical_url = "https://archive-api.open-meteo.com/v1/era5"
                hist_params = {
                    'latitude': test_lat,
                    'longitude': test_lon,
                    'start_date': (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d'),
                    'end_date': (datetime.now() - timedelta(days=9)).strftime('%Y-%m-%d'),
                    'hourly': 'temperature_2m'
                }

                hist_response = requests.get(historical_url, params=hist_params, timeout=10)
                if hist_response.status_code == 200:
                    print("   ✅ Historical weather API (ERA5) working")
                else:
                    print(f"   ⚠️  Historical API returned status {hist_response.status_code}")

                self.test_results[test_name] = {'status': 'success', 'message': 'APIs accessible'}

            else:
                print(f"   ❌ Weather API returned status {response.status_code}")
                self.test_results[test_name] = {'status': 'failed', 'message': f'HTTP {response.status_code}'}

        except requests.exceptions.RequestException as e:
            print(f"   ❌ Weather API test failed: {e}")
            self.test_results[test_name] = {'status': 'failed', 'message': str(e)}
        except Exception as e:
            print(f"   ❌ Weather test error: {e}")
            self.test_results[test_name] = {'status': 'error', 'message': str(e)}

    def _test_opsd_data(self):
        """Test Open Power System Data availability"""
        test_name = "Open Power System Data"
        print(f"\n4. Testing {test_name}...")

        try:
            opsd_urls = [
                "https://data.open-power-system-data.org/time_series/latest/time_series_60min_singleindex.csv",
                "https://data.open-power-system-data.org/time_series/2020-10-06/time_series_60min_singleindex.csv"
            ]

            success = False
            for url in opsd_urls:
                print(f"   Testing OPSD URL: {url[:80]}...")

                try:
                    # Just check if URL is accessible
                    response = requests.head(url, timeout=15, allow_redirects=True)
                    if response.status_code == 200:
                        content_length = response.headers.get('content-length')
                        if content_length:
                            size_mb = int(content_length) / 1024 / 1024
                            print(f"   ✅ OPSD data accessible ({size_mb:.1f} MB)")
                        else:
                            print("   ✅ OPSD data URL accessible")
                        success = True
                        break
                    else:
                        print(f"   ⚠️  OPSD URL returned status {response.status_code}")
                except Exception as e:
                    print(f"   ⚠️  Could not access this URL: {e}")
                    continue

            if success:
                self.test_results[test_name] = {'status': 'success', 'message': 'Data accessible'}
            else:
                print("   ⚠️  OPSD data sources not readily accessible")
                print("   ℹ️  This is common due to large file sizes")
                self.test_results[test_name] = {'status': 'warning', 'message': 'Limited accessibility'}

        except Exception as e:
            print(f"   ❌ OPSD test error: {e}")
            self.test_results[test_name] = {'status': 'error', 'message': str(e)}

    def _test_entsoe_api(self):
        """Test ENTSO-E API availability"""
        test_name = "ENTSO-E Transparency Platform"
        print(f"\n5. Testing {test_name}...")

        try:
            # Check if entsoe-py is installed
            try:
                import entsoe
                print("   ✅ entsoe-py library installed")

                # Check for API token
                api_token = None  # Would need to be configured
                if api_token:
                    print("   ✅ ENTSO-E API token configured")
                    self.test_results[test_name] = {'status': 'success', 'message': 'Ready to use'}
                else:
                    print("   ⚠️  ENTSO-E API token not configured")
                    print("   ℹ️  Register at https://transparency.entsoe.eu to get API token")
                    self.test_results[test_name] = {'status': 'warning', 'message': 'Token required'}

            except ImportError:
                print("   ⚠️  entsoe-py library not installed")
                print("   ℹ️  Install with: pip install entsoe-py")
                self.test_results[test_name] = {'status': 'warning', 'message': 'Library not installed'}

        except Exception as e:
            print(f"   ❌ ENTSO-E test error: {e}")
            self.test_results[test_name] = {'status': 'error', 'message': str(e)}

    def _print_test_summary(self):
        """Print test summary"""
        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)

        success_count = sum(1 for r in self.test_results.values() if r['status'] == 'success')
        warning_count = sum(1 for r in self.test_results.values() if r['status'] == 'warning')
        failed_count = sum(1 for r in self.test_results.values() if r['status'] in ['failed', 'error'])

        print(f"\n✅ Successful: {success_count}")
        print(f"⚠️  Warnings: {warning_count}")
        print(f"❌ Failed: {failed_count}")

        print("\nDETAILS:")
        for test_name, result in self.test_results.items():
            status_icon = {
                'success': '✅',
                'warning': '⚠️',
                'failed': '❌',
                'error': '❌'
            }.get(result['status'], '?')

            print(f"{status_icon} {test_name}: {result['message']}")

        print("\n" + "=" * 60)

        if failed_count == 0:
            print("🎉 All critical tests passed! Pipeline is ready to use.")
        elif success_count > 0:
            print("⚠️  Some tests failed, but basic functionality should work.")
        else:
            print("❌ Critical tests failed. Please check your configuration.")

        print("=" * 60)