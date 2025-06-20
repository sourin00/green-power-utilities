#!/usr/bin/env python3
"""
Job tracking and monitoring functionality
"""

import logging
from datetime import datetime
from typing import Optional
from sqlalchemy import text

from database.connection import DatabaseConnection

logger = logging.getLogger(__name__)


class JobTracker:
    """Tracks and monitors data ingestion jobs"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
    
    def start_job(self, job_name: str, data_source: str) -> int:
        """Log job start and return job ID"""
        try:
            sql = """
                INSERT INTO metadata.ingestion_log
                (job_name, data_source, start_time, status)
                VALUES (:job_name, :data_source, :start_time, 'running')
                RETURNING id
            """
            
            result = self.db.execute_transaction(
                sql,
                {
                    "job_name": job_name,
                    "data_source": data_source,
                    "start_time": datetime.now()
                }
            )
            
            job_id = result.fetchone()[0]
            logger.info(f"Started job {job_name} with ID {job_id}")
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to log job start: {e}")
            return -1
    
    def complete_job(
        self,
        job_id: int,
        status: str,
        records_processed: int,
        records_inserted: Optional[int] = None,
        error_message: Optional[str] = None
    ):
        """Log job completion"""
        try:
            if records_inserted is None:
                records_inserted = records_processed
            
            sql = """
                UPDATE metadata.ingestion_log
                SET end_time = :end_time,
                    status = :status,
                    records_processed = :records_processed,
                    records_inserted = :records_inserted,
                    error_message = :error_message,
                    processing_duration_seconds = EXTRACT(EPOCH FROM (:end_time - start_time))
                WHERE id = :job_id
            """
            
            self.db.execute_transaction(
                sql,
                {
                    "end_time": datetime.now(),
                    "status": status,
                    "records_processed": records_processed,
                    "records_inserted": records_inserted,
                    "error_message": error_message,
                    "job_id": job_id
                }
            )
            
            logger.info(
                f"Completed job {job_id} with status {status}, "
                f"processed {records_processed} records"
            )
            
        except Exception as e:
            logger.error(f"Failed to log job completion: {e}")
    
    def get_recent_jobs(self, limit: int = 10) -> list:
        """Get recent job history"""
        try:
            sql = """
                SELECT job_name, data_source, start_time, end_time, status,
                       records_processed, records_inserted, error_message,
                       processing_duration_seconds
                FROM metadata.ingestion_log
                ORDER BY start_time DESC
                LIMIT :limit
            """
            
            result = self.db.execute_query(sql, {"limit": limit})
            
            jobs = []
            for row in result:
                jobs.append({
                    "job_name": row[0],
                    "data_source": row[1],
                    "start_time": row[2],
                    "end_time": row[3],
                    "status": row[4],
                    "records_processed": row[5],
                    "records_inserted": row[6],
                    "error_message": row[7],
                    "duration_seconds": row[8]
                })
            
            return jobs
            
        except Exception as e:
            logger.error(f"Failed to get recent jobs: {e}")
            return []
    
    def get_job_statistics(self, days: int = 7) -> dict:
        """Get job statistics for the last N days"""
        try:
            sql = """
                SELECT 
                    COUNT(*) as total_jobs,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as successful_jobs,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_jobs,
                    SUM(records_processed) as total_records_processed,
                    SUM(records_inserted) as total_records_inserted,
                    AVG(processing_duration_seconds) as avg_duration_seconds
                FROM metadata.ingestion_log
                WHERE start_time >= NOW() - INTERVAL ':days days'
            """
            
            result = self.db.execute_query(sql, {"days": days})
            row = result.fetchone()
            
            if row:
                return {
                    "total_jobs": row[0] or 0,
                    "successful_jobs": row[1] or 0,
                    "failed_jobs": row[2] or 0,
                    "success_rate": (row[1] or 0) / (row[0] or 1) * 100,
                    "total_records_processed": row[3] or 0,
                    "total_records_inserted": row[4] or 0,
                    "avg_duration_seconds": row[5] or 0
                }
            
            return {}
            
        except Exception as e:
            logger.error(f"Failed to get job statistics: {e}")
            return {}
    
    def get_failed_jobs(self, days: int = 1) -> list:
        """Get failed jobs from the last N days"""
        try:
            sql = """
                SELECT job_name, data_source, start_time, error_message
                FROM metadata.ingestion_log
                WHERE status = 'failed'
                  AND start_time >= NOW() - INTERVAL ':days days'
                ORDER BY start_time DESC
            """
            
            result = self.db.execute_query(sql, {"days": days})
            
            failed_jobs = []
            for row in result:
                failed_jobs.append({
                    "job_name": row[0],
                    "data_source": row[1],
                    "start_time": row[2],
                    "error_message": row[3]
                })
            
            return failed_jobs
            
        except Exception as e:
            logger.error(f"Failed to get failed jobs: {e}")
            return []


def cleanup_old_logs(db_connection: DatabaseConnection, retention_days: int = 90):
    """Clean up old ingestion logs"""
    try:
        sql = """
            DELETE FROM metadata.ingestion_log
            WHERE start_time < NOW() - INTERVAL ':days days'
        """
        
        result = db_connection.execute_transaction(sql, {"days": retention_days})
        deleted_count = result.rowcount
        
        logger.info(f"Cleaned up {deleted_count} old ingestion log records")
        
        # Also clean up old quality metrics
        sql = """
            DELETE FROM metadata.quality_metrics
            WHERE measurement_time < NOW() - INTERVAL ':days days'
        """
        
        result = db_connection.execute_transaction(sql, {"days": retention_days * 4})
        deleted_count = result.rowcount
        
        logger.info(f"Cleaned up {deleted_count} old quality metric records")
        
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")