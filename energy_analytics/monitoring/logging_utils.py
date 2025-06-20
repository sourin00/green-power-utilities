#!/usr/bin/env python3
"""
Logging configuration and utilities
"""

import logging
import logging.handlers
from pathlib import Path
from typing import Optional


def setup_logging(
    log_level: str = "INFO",
    log_file: str = "ingestion_pipeline.log",
    max_size_mb: int = 100,
    backup_count: int = 5
) -> logging.Logger:
    """
    Setup logging configuration for the application
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file
        max_size_mb: Maximum size of log file in MB before rotation
        backup_count: Number of backup files to keep
    
    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    log_path = Path(log_file)
    if log_path.parent.name and not log_path.parent.exists():
        log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Remove existing handlers
    logger.handlers = []
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # File handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=max_size_mb * 1024 * 1024,
        backupCount=backup_count
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Log startup message
    logger.info("=" * 60)
    logger.info("Energy Analytics Pipeline - Logging Started")
    logger.info("=" * 60)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for a specific module"""
    return logging.getLogger(name)


class LogContext:
    """Context manager for logging operations with timing"""
    
    def __init__(self, operation: str, logger: Optional[logging.Logger] = None):
        self.operation = operation
        self.logger = logger or logging.getLogger()
        self.start_time = None
        
    def __enter__(self):
        import time
        self.start_time = time.time()
        self.logger.info(f"Starting {self.operation}")
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        import time
        duration = time.time() - self.start_time
        
        if exc_type is None:
            self.logger.info(f"Completed {self.operation} in {duration:.2f} seconds")
        else:
            self.logger.error(
                f"Failed {self.operation} after {duration:.2f} seconds: {exc_val}"
            )


def log_dataframe_info(df, name: str, logger: Optional[logging.Logger] = None):
    """Log information about a pandas DataFrame"""
    if logger is None:
        logger = logging.getLogger()
    
    logger.info(f"{name} DataFrame Info:")
    logger.info(f"  Shape: {df.shape}")
    logger.info(f"  Columns: {list(df.columns)}")
    logger.info(f"  Memory Usage: {df.memory_usage().sum() / 1024**2:.2f} MB")
    logger.info(f"  Null Values: {df.isnull().sum().sum()}")


def log_performance_metrics(
    operation: str,
    records_processed: int,
    duration_seconds: float,
    logger: Optional[logging.Logger] = None
):
    """Log performance metrics for an operation"""
    if logger is None:
        logger = logging.getLogger()
    
    records_per_second = records_processed / duration_seconds if duration_seconds > 0 else 0
    
    logger.info(f"Performance metrics for {operation}:")
    logger.info(f"  Records processed: {records_processed:,}")
    logger.info(f"  Duration: {duration_seconds:.2f} seconds")
    logger.info(f"  Throughput: {records_per_second:,.0f} records/second")