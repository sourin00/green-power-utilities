#!/usr/bin/env python3
"""
Pipeline status reporting functionality
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any

from config.settings import DatabaseConfig
from database.connection import DatabaseConnection
from monitoring.job_tracking import JobTracker
from utils.helpers import format_duration, format_bytes, format_number

logger = logging.getLogger(__name__)


class PipelineStatusReporter:
    """Reports on pipeline status and health"""

    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.db_connection = DatabaseConnection(db_config)
        self.job_tracker = None

    def show_status(self):
        """Show comprehensive pipeline status"""
        print("\n" + "=" * 70)
        print("ENERGY ANALYTICS PIPELINE STATUS")
        print("=" * 70)
        print(f"Report Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

        # Connect to database
        if not self.db_connection.connect():
            print("\n❌ Cannot connect to database. Please check your configuration.")
            return

        self.job_tracker = JobTracker(self.db_connection)

        try:
            # Show data summary
            self._show_data_summary()

            # Show recent jobs
            self._show_recent_jobs()

            # Show job statistics
            self._show_job_statistics()

            # Show data quality metrics
            self._show_data_quality()

            # Show system health
            self._show_system_health()

        except Exception as e:
            logger.error(f"Error generating status report: {e}")
            print(f"\n❌ Error generating status report: {e}")

        finally:
            self.db_connection.disconnect()

    def _show_data_summary(self):
        """Show summary of data in the database"""
        print("\n📊 DATA SUMMARY")
        print("-" * 50)

        tables = [
            ('household.consumption', 'Household Consumption'),
            ('weather.observations', 'Weather Observations'),
            ('grid.operations', 'Grid Operations'),
            ('metadata.ingestion_log', 'Ingestion Jobs'),
            ('metadata.quality_metrics', 'Quality Metrics')
        ]

        total_records = 0
        for table_name, description in tables:
            try:
                schema, table = table_name.split('.')
                count = self.db_connection.get_table_count(schema, table)
                total_records += count
                print(f"  {description:<25} {format_number(count):>15} records")

                # Get date range for time-series tables
                if table_name in ['household.consumption', 'weather.observations', 'grid.operations']:
                    date_range = self._get_table_date_range(table_name)
                    if date_range:
                        print(f"    └─ Date range: {date_range}")

            except Exception as e:
                print(f"  {description:<25} {'Error':>15}")
                logger.error(f"Error getting count for {table_name}: {e}")

        print(f"\n  {'TOTAL':<25} {format_number(total_records):>15} records")

    def _get_table_date_range(self, table_name: str) -> str:
        """Get date range for a table"""
        try:
            query = f"""
                SELECT 
                    MIN(timestamp)::date as min_date,
                    MAX(timestamp)::date as max_date
                FROM {table_name}
            """
            result = self.db_connection.execute_query(query)
            row = result.fetchone()

            if row and row[0] and row[1]:
                days = (row[1] - row[0]).days + 1
                return f"{row[0]} to {row[1]} ({days} days)"
            return "No data"

        except Exception as e:
            logger.error(f"Error getting date range for {table_name}: {e}")
            return "Unknown"

    def _show_recent_jobs(self):
        """Show recent ingestion jobs"""
        print("\n📋 RECENT JOBS (Last 10)")
        print("-" * 70)

        recent_jobs = self.job_tracker.get_recent_jobs(10)

        if not recent_jobs:
            print("  No recent jobs found")
            return

        # Header
        print(f"  {'Status':<8} {'Job Name':<30} {'Records':<12} {'Duration':<10} {'Time'}")
        print("  " + "-" * 68)

        for job in recent_jobs:
            status_icon = {
                'completed': '✅',
                'failed': '❌',
                'running': '🔄'
            }.get(job['status'], '❓')

            duration = format_duration(job['duration_seconds']) if job['duration_seconds'] else 'N/A'
            time_str = job['start_time'].strftime('%Y-%m-%d %H:%M') if job['start_time'] else 'Unknown'
            records = format_number(job['records_inserted'] or 0)

            print(f"  {status_icon:<8} {job['job_name']:<30} {records:<12} {duration:<10} {time_str}")

            if job['status'] == 'failed' and job['error_message']:
                error_msg = job['error_message'][:60] + '...' if len(job['error_message']) > 60 else job[
                    'error_message']
                print(f"         └─ Error: {error_msg}")

    def _show_job_statistics(self):
        """Show job statistics"""
        print("\n📈 JOB STATISTICS (Last 7 days)")
        print("-" * 50)

        stats = self.job_tracker.get_job_statistics(7)

        if not stats:
            print("  No statistics available")
            return

        print(f"  Total Jobs:        {stats.get('total_jobs', 0)}")
        print(f"  Successful:        {stats.get('successful_jobs', 0)} ({stats.get('success_rate', 0):.1f}%)")
        print(f"  Failed:            {stats.get('failed_jobs', 0)}")
        print(f"  Records Processed: {format_number(stats.get('total_records_processed', 0))}")
        print(f"  Records Inserted:  {format_number(stats.get('total_records_inserted', 0))}")
        print(f"  Avg Duration:      {format_duration(stats.get('avg_duration_seconds', 0))}")

        # Show failed jobs if any
        failed_jobs = self.job_tracker.get_failed_jobs(1)
        if failed_jobs:
            print("\n  ⚠️  Recent Failures:")
            for job in failed_jobs[:3]:  # Show up to 3 failures
                print(f"     - {job['job_name']} at {job['start_time'].strftime('%Y-%m-%d %H:%M')}")
                if job['error_message']:
                    error_msg = job['error_message'][:50] + '...' if len(job['error_message']) > 50 else job[
                        'error_message']
                    print(f"       {error_msg}")

    def _show_data_quality(self):
        """Show data quality metrics"""
        print("\n🔍 DATA QUALITY (Last 24 hours)")
        print("-" * 50)

        try:
            query = """
                    SELECT table_name, \
                           metric_name, \
                           metric_value, \
                           status, \
                           measurement_time
                    FROM metadata.quality_metrics
                    WHERE measurement_time >= NOW() - INTERVAL '24 hours'
                    ORDER BY measurement_time DESC
                        LIMIT 15 \
                    """

            result = self.db_connection.execute_query(query)
            metrics = result.fetchall()

            if not metrics:
                print("  No recent quality metrics found")
                return

            # Group by table
            by_table = {}
            for row in metrics:
                table = row[0]
                if table not in by_table:
                    by_table[table] = []
                by_table[table].append({
                    'metric': row[1],
                    'value': row[2],
                    'status': row[3],
                    'time': row[4]
                })

            for table, table_metrics in by_table.items():
                print(f"\n  {table}:")
                for metric in table_metrics[:3]:  # Show up to 3 metrics per table
                    status_icon = {
                        'good': '✅',
                        'warning': '⚠️',
                        'critical': '❌'
                    }.get(metric['status'], '❓')

                    value_str = f"{metric['value']:.2%}" if metric['value'] else 'N/A'
                    print(f"    {status_icon} {metric['metric']:<25} {value_str:>10}")

        except Exception as e:
            logger.error(f"Error getting quality metrics: {e}")
            print("  Error retrieving quality metrics")

    def _show_system_health(self):
        """Show system health indicators"""
        print("\n💓 SYSTEM HEALTH")
        print("-" * 50)

        try:
            # Check database size
            db_size_query = """
                SELECT pg_database_size(current_database()) as size
            """
            result = self.db_connection.execute_query(db_size_query)
            db_size = result.fetchone()[0]
            print(f"  Database Size:     {format_bytes(db_size)}")

            # Check hypertable chunks
            hypertables = self.db_connection.get_hypertables()
            if hypertables:
                total_chunks = sum(chunks for _, chunks in hypertables)
                print(f"  Hypertables:       {len(hypertables)}")
                print(f"  Total Chunks:      {total_chunks}")

            # Check for recent data
            freshness_checks = [
                ('household.consumption', 'Household Data', 24),
                ('weather.observations', 'Weather Data', 6),
                ('grid.operations', 'Grid Data', 4)
            ]

            print("\n  Data Freshness:")
            for table, name, threshold_hours in freshness_checks:
                freshness = self._check_data_freshness(table)
                if freshness is not None:
                    hours_old = freshness / 3600
                    if hours_old < threshold_hours:
                        icon = '✅'
                    elif hours_old < threshold_hours * 2:
                        icon = '⚠️'
                    else:
                        icon = '❌'

                    if hours_old < 1:
                        age_str = f"{int(freshness / 60)} minutes"
                    elif hours_old < 24:
                        age_str = f"{hours_old:.1f} hours"
                    else:
                        age_str = f"{hours_old / 24:.1f} days"

                    print(f"    {icon} {name:<20} Last update: {age_str} ago")
                else:
                    print(f"    ❓ {name:<20} No data")

        except Exception as e:
            logger.error(f"Error checking system health: {e}")
            print("  Error checking system health")

    def _check_data_freshness(self, table: str) -> float:
        """Check how old the latest data is (in seconds)"""
        try:
            query = f"""
                SELECT EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))
                FROM {table}
            """
            result = self.db_connection.execute_query(query)
            row = result.fetchone()
            return row[0] if row and row[0] else None
        except Exception:
            return None

    def get_quick_stats(self) -> Dict[str, Any]:
        """Get quick statistics for API or monitoring use"""
        if not self.db_connection.connect():
            return {'error': 'Database connection failed'}

        try:
            stats = {
                'timestamp': datetime.now().isoformat(),
                'data_counts': {},
                'recent_jobs': [],
                'health': {}
            }

            # Get record counts
            tables = [
                ('household.consumption', 'household'),
                ('weather.observations', 'weather'),
                ('grid.operations', 'grid')
            ]

            for table_name, key in tables:
                schema, table = table_name.split('.')
                count = self.db_connection.get_table_count(schema, table)
                stats['data_counts'][key] = count

            # Get recent job info
            if self.job_tracker:
                recent = self.job_tracker.get_recent_jobs(5)
                stats['recent_jobs'] = [
                    {
                        'job': job['job_name'],
                        'status': job['status'],
                        'records': job['records_inserted'],
                        'time': job['start_time'].isoformat() if job['start_time'] else None
                    }
                    for job in recent
                ]

            # Get health metrics
            for table_name, key in tables:
                freshness = self._check_data_freshness(table_name)
                stats['health'][f'{key}_freshness_hours'] = freshness / 3600 if freshness else None

            return stats

        except Exception as e:
            logger.error(f"Error getting quick stats: {e}")
            return {'error': str(e)}

        finally:
            self.db_connection.disconnect()