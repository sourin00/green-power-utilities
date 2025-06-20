#!/usr/bin/env python3
"""
Data quality monitoring and checks
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from sqlalchemy import text

from database.connection import DatabaseConnection

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """Performs data quality checks on ingested data"""

    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection

    def run_all_checks(self):
        """Run all data quality checks"""
        logger.info("Running data quality checks")

        try:
            # Check data freshness
            self.check_data_freshness()

            # Check data completeness
            self.check_data_completeness()

            # Check data accuracy
            self.check_data_accuracy()

            # Check data consistency
            self.check_data_consistency()

            # Check for anomalies
            self.check_for_anomalies()

            logger.info("Data quality checks completed")

        except Exception as e:
            logger.error(f"Data quality checks failed: {e}")

    def check_data_freshness(self):
        """Check if data is up-to-date"""
        freshness_checks = [
            {
                'table': 'weather.observations',
                'threshold_hours': 3,
                'metric_name': 'data_freshness'
            },
            {
                'table': 'grid.operations',
                'threshold_hours': 2,
                'metric_name': 'data_freshness'
            },
            {
                'table': 'household.consumption',
                'threshold_hours': 168,  # 7 days for household data
                'metric_name': 'data_freshness'
            }
        ]

        for check in freshness_checks:
            sql = f"""
                SELECT EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))/3600 as hours_old
                FROM {check['table']}
            """

            try:
                result = self.db.execute_query(sql)
                row = result.fetchone()
                hours_old = row[0] if row and row[0] else float('inf')

                # Determine status
                if hours_old <= check['threshold_hours']:
                    status = 'good'
                elif hours_old <= check['threshold_hours'] * 2:
                    status = 'warning'
                else:
                    status = 'critical'

                # Calculate freshness score (1.0 = perfectly fresh, 0.0 = very stale)
                freshness_score = max(0, 1.0 - (hours_old / (check['threshold_hours'] * 24)))

                # Log quality metric
                self._log_quality_metric(
                    check['table'],
                    check['metric_name'],
                    freshness_score,
                    status,
                    {'hours_old': hours_old, 'threshold_hours': check['threshold_hours']}
                )

                logger.info(f"Freshness check for {check['table']}: {hours_old:.1f} hours old (status: {status})")

            except Exception as e:
                logger.error(f"Freshness check failed for {check['table']}: {e}")

    def check_data_completeness(self):
        """Check data completeness"""
        completeness_checks = [
            {
                'table': 'household.consumption',
                'required_columns': ['global_active_power', 'voltage', 'global_intensity'],
                'metric_name': 'completeness'
            },
            {
                'table': 'weather.observations',
                'required_columns': ['temperature_2m_c', 'relative_humidity_2m_pct', 'wind_speed_10m_kmh'],
                'metric_name': 'completeness'
            },
            {
                'table': 'grid.operations',
                'required_columns': ['load_actual_mw', 'total_generation_mw'],
                'metric_name': 'completeness'
            }
        ]

        for check in completeness_checks:
            try:
                # Build SQL to check nulls in required columns
                null_checks = [f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as null_{col}"
                               for col in check['required_columns']]

                sql = f"""
                    SELECT 
                        COUNT(*) as total_records,
                        {', '.join(null_checks)}
                    FROM {check['table']}
                    WHERE timestamp >= NOW() - INTERVAL '24 hours'
                """

                result = self.db.execute_query(sql)
                row = result.fetchone()

                if row and row[0] > 0:  # Has records
                    total_records = row[0]
                    total_nulls = sum(row[i + 1] for i in range(len(check['required_columns'])))
                    total_expected = total_records * len(check['required_columns'])

                    completeness_score = 1.0 - (total_nulls / max(total_expected, 1))

                    # Determine status
                    if completeness_score >= 0.95:
                        status = 'good'
                    elif completeness_score >= 0.90:
                        status = 'warning'
                    else:
                        status = 'critical'

                    # Details for each column
                    details = {
                        'total_records': total_records,
                        'total_nulls': total_nulls,
                        'completeness_percent': completeness_score * 100
                    }

                    for i, col in enumerate(check['required_columns']):
                        details[f'null_{col}'] = row[i + 1]

                    self._log_quality_metric(
                        check['table'],
                        check['metric_name'],
                        completeness_score,
                        status,
                        details
                    )

                    logger.info(f"Completeness check for {check['table']}: {completeness_score:.2%} (status: {status})")

                else:
                    logger.warning(f"No recent data found for completeness check on {check['table']}")

            except Exception as e:
                logger.error(f"Completeness check failed for {check['table']}: {e}")

    def check_data_accuracy(self):
        """Check data accuracy using business rules"""
        accuracy_checks = [
            {
                'table': 'household.consumption',
                'checks': [
                    ('global_active_power', 'BETWEEN 0 AND 10', 'power_range_accuracy'),
                    ('voltage', 'BETWEEN 220 AND 250', 'voltage_range_accuracy')
                ]
            },
            {
                'table': 'weather.observations',
                'checks': [
                    ('temperature_2m_c', 'BETWEEN -40 AND 50', 'temperature_range_accuracy'),
                    ('relative_humidity_2m_pct', 'BETWEEN 0 AND 100', 'humidity_range_accuracy'),
                    ('wind_speed_10m_kmh', 'BETWEEN 0 AND 200', 'wind_speed_range_accuracy')
                ]
            },
            {
                'table': 'grid.operations',
                'checks': [
                    ('load_actual_mw', '> 0', 'positive_load_accuracy'),
                    ('price_day_ahead_eur_mwh', 'BETWEEN -500 AND 3000', 'price_range_accuracy')
                ]
            }
        ]

        for table_check in accuracy_checks:
            table = table_check['table']

            for column, condition, metric_name in table_check['checks']:
                sql = f"""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN {column} {condition} THEN 1 ELSE 0 END) as valid,
                        SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) as nulls
                    FROM {table}
                    WHERE timestamp >= NOW() - INTERVAL '24 hours'
                """

                try:
                    result = self.db.execute_query(sql)
                    row = result.fetchone()

                    if row and row[0] > 0:
                        total = row[0]
                        valid = row[1]
                        nulls = row[2]

                        # Calculate accuracy excluding nulls
                        non_null_total = total - nulls
                        accuracy_score = valid / non_null_total if non_null_total > 0 else 0

                        # Determine status
                        if accuracy_score >= 0.98:
                            status = 'good'
                        elif accuracy_score >= 0.95:
                            status = 'warning'
                        else:
                            status = 'critical'

                        self._log_quality_metric(
                            table,
                            metric_name,
                            accuracy_score,
                            status,
                            {
                                'total_records': total,
                                'valid_records': valid,
                                'null_records': nulls,
                                'condition': f"{column} {condition}"
                            }
                        )

                        logger.info(f"Accuracy check for {table}.{column}: {accuracy_score:.2%} (status: {status})")

                except Exception as e:
                    logger.error(f"Accuracy check failed for {table}.{column}: {e}")

    def check_data_consistency(self):
        """Check data consistency across tables"""
        consistency_checks = [
            {
                'name': 'weather_location_consistency',
                'description': 'Weather observations should have valid location data',
                'sql': """
                       SELECT COUNT(*)                                                       as total,
                              SUM(CASE WHEN w.location_id = s.location_id THEN 1 ELSE 0 END) as consistent
                       FROM weather.observations w
                                LEFT JOIN weather.stations s ON w.location_id = s.location_id
                       WHERE w.timestamp >= NOW() - INTERVAL '24 hours'
                       """
            },
            {
                'name': 'grid_generation_consistency',
                'description': 'Total generation should approximately match sum of components',
                'sql': """
                       SELECT COUNT(*) as total,
                              SUM(CASE
                                      WHEN ABS(
                                                   total_generation_mw -
                                                   COALESCE(solar_generation_actual_mw, 0) -
                                                   COALESCE(wind_onshore_generation_actual_mw, 0) -
                                                   COALESCE(nuclear_generation_actual_mw, 0) -
                                                   COALESCE(hydro_generation_actual_mw, 0) -
                                                   COALESCE(fossil_generation_actual_mw, 0)
                                           ) < total_generation_mw * 0.1
                                          THEN 1
                                      ELSE 0
                                  END) as consistent
                       FROM grid.operations
                       WHERE timestamp >= NOW() - INTERVAL '24 hours'
                         AND total_generation_mw
                           > 0
                       """
            }
        ]

        for check in consistency_checks:
            try:
                result = self.db.execute_query(check['sql'])
                row = result.fetchone()

                if row and row[0] > 0:
                    total = row[0]
                    consistent = row[1]
                    consistency_score = consistent / total

                    # Determine status
                    if consistency_score >= 0.95:
                        status = 'good'
                    elif consistency_score >= 0.90:
                        status = 'warning'
                    else:
                        status = 'critical'

                    self._log_quality_metric(
                        'cross_table',
                        check['name'],
                        consistency_score,
                        status,
                        {
                            'description': check['description'],
                            'total_records': total,
                            'consistent_records': consistent
                        }
                    )

                    logger.info(f"Consistency check '{check['name']}': {consistency_score:.2%} (status: {status})")

            except Exception as e:
                logger.error(f"Consistency check '{check['name']}' failed: {e}")

    def check_for_anomalies(self):
        """Check for anomalies in the data"""
        anomaly_checks = [
            {
                'table': 'household.consumption',
                'metric': 'global_active_power',
                'name': 'power_consumption_anomaly',
                'method': 'zscore',
                'threshold': 3.5
            },
            {
                'table': 'weather.observations',
                'metric': 'temperature_2m_c',
                'name': 'temperature_anomaly',
                'method': 'zscore',
                'threshold': 4.0
            },
            {
                'table': 'grid.operations',
                'metric': 'load_actual_mw',
                'name': 'grid_load_anomaly',
                'method': 'iqr',
                'threshold': 1.5
            }
        ]

        for check in anomaly_checks:
            try:
                if check['method'] == 'zscore':
                    # Z-score method
                    sql = f"""
                        WITH stats AS (
                            SELECT 
                                AVG({check['metric']}) as mean_val,
                                STDDEV({check['metric']}) as stddev_val
                            FROM {check['table']}
                            WHERE timestamp >= NOW() - INTERVAL '7 days'
                            AND {check['metric']} IS NOT NULL
                        ),
                        recent AS (
                            SELECT 
                                {check['metric']},
                                (({check['metric']} - stats.mean_val) / NULLIF(stats.stddev_val, 0)) as zscore
                            FROM {check['table']}
                            CROSS JOIN stats
                            WHERE timestamp >= NOW() - INTERVAL '24 hours'
                            AND {check['metric']} IS NOT NULL
                        )
                        SELECT 
                            COUNT(*) as total,
                            SUM(CASE WHEN ABS(zscore) > {check['threshold']} THEN 1 ELSE 0 END) as anomalies
                        FROM recent
                    """

                elif check['method'] == 'iqr':
                    # Interquartile range method
                    sql = f"""
                        WITH stats AS (
                            SELECT 
                                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {check['metric']}) as q1,
                                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {check['metric']}) as q3
                            FROM {check['table']}
                            WHERE timestamp >= NOW() - INTERVAL '7 days'
                            AND {check['metric']} IS NOT NULL
                        ),
                        recent AS (
                            SELECT 
                                {check['metric']},
                                stats.q1 - {check['threshold']} * (stats.q3 - stats.q1) as lower_bound,
                                stats.q3 + {check['threshold']} * (stats.q3 - stats.q1) as upper_bound
                            FROM {check['table']}
                            CROSS JOIN stats
                            WHERE timestamp >= NOW() - INTERVAL '24 hours'
                            AND {check['metric']} IS NOT NULL
                        )
                        SELECT 
                            COUNT(*) as total,
                            SUM(CASE WHEN {check['metric']} < lower_bound OR {check['metric']} > upper_bound THEN 1 ELSE 0 END) as anomalies
                        FROM recent
                    """

                result = self.db.execute_query(sql)
                row = result.fetchone()

                if row and row[0] > 0:
                    total = row[0]
                    anomalies = row[1]
                    anomaly_rate = anomalies / total

                    # For anomalies, lower is better
                    quality_score = 1.0 - anomaly_rate

                    # Determine status
                    if anomaly_rate <= 0.01:  # Less than 1% anomalies
                        status = 'good'
                    elif anomaly_rate <= 0.05:  # Less than 5% anomalies
                        status = 'warning'
                    else:
                        status = 'critical'

                    self._log_quality_metric(
                        check['table'],
                        check['name'],
                        quality_score,
                        status,
                        {
                            'method': check['method'],
                            'threshold': check['threshold'],
                            'total_records': total,
                            'anomaly_count': anomalies,
                            'anomaly_rate_percent': anomaly_rate * 100
                        }
                    )

                    logger.info(f"Anomaly check '{check['name']}': {anomaly_rate:.2%} anomaly rate (status: {status})")

            except Exception as e:
                logger.error(f"Anomaly check '{check['name']}' failed: {e}")

    def _log_quality_metric(self, table_name: str, metric_name: str, metric_value: float,
                            status: str, details: Dict[str, Any]):
        """Log data quality metric to database"""
        try:
            sql = """
                  INSERT INTO metadata.quality_metrics
                  (table_name, metric_name, metric_value, measurement_time,
                   time_period, threshold_value, status, details)
                  VALUES (:table_name, :metric_name, :metric_value, :measurement_time,
                          :time_period, :threshold_value, :status, :details) \
                  """

            params = {
                'table_name': table_name,
                'metric_name': metric_name,
                'metric_value': metric_value,
                'measurement_time': datetime.now(),
                'time_period': '24 hours',
                'threshold_value': 0.95,  # Default threshold
                'status': status,
                'details': json.dumps(details)
            }

            self.db.execute_transaction(sql, params)

        except Exception as e:
            logger.error(f"Failed to log quality metric: {e}")

    def get_quality_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get summary of recent quality metrics"""
        try:
            sql = """
                  SELECT table_name, \
                         metric_name, \
                         AVG(metric_value) as avg_score, \
                         MIN(metric_value) as min_score, \
                         MAX(metric_value) as max_score, \
                         COUNT(*)          as measurement_count
                  FROM metadata.quality_metrics
                  WHERE measurement_time >= NOW() - INTERVAL ':hours hours'
                  GROUP BY table_name, metric_name
                  ORDER BY table_name, metric_name \
                  """

            result = self.db.execute_query(sql, {'hours': hours})

            summary = {}
            for row in result:
                table = row[0]
                if table not in summary:
                    summary[table] = {}

                summary[table][row[1]] = {
                    'avg_score': float(row[2]) if row[2] else 0,
                    'min_score': float(row[3]) if row[3] else 0,
                    'max_score': float(row[4]) if row[4] else 0,
                    'measurements': row[5]
                }

            return summary

        except Exception as e:
            logger.error(f"Failed to get quality summary: {e}")
            return {}