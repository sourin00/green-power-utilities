#!/usr/bin/env python3
"""
General helper functions for the Energy Analytics Pipeline
"""

import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


def format_duration(seconds: float) -> str:
    """
    Format duration in human-readable format
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Human-readable duration string
    """
    if seconds < 0:
        return "Invalid duration"
    
    if seconds < 1:
        return f"{seconds*1000:.0f} ms"
    elif seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        remaining_seconds = seconds % 60
        if remaining_seconds > 0:
            return f"{int(minutes)} min {int(remaining_seconds)} sec"
        return f"{minutes:.1f} minutes"
    elif seconds < 86400:
        hours = seconds / 3600
        remaining_minutes = (seconds % 3600) / 60
        if remaining_minutes > 0:
            return f"{int(hours)} hr {int(remaining_minutes)} min"
        return f"{hours:.1f} hours"
    else:
        days = seconds / 86400
        remaining_hours = (seconds % 86400) / 3600
        if remaining_hours > 0:
            return f"{int(days)} days {int(remaining_hours)} hr"
        return f"{days:.1f} days"


def format_bytes(bytes_size: Union[int, float]) -> str:
    """
    Format bytes in human-readable format
    
    Args:
        bytes_size: Size in bytes
        
    Returns:
        Human-readable size string
    """
    if bytes_size < 0:
        return "Invalid size"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    unit_index = 0
    size = float(bytes_size)
    
    while size >= 1024.0 and unit_index < len(units) - 1:
        size /= 1024.0
        unit_index += 1
    
    if unit_index == 0:  # Bytes
        return f"{int(size)} {units[unit_index]}"
    else:
        return f"{size:.2f} {units[unit_index]}"


def format_number(number: Union[int, float], decimals: int = 0) -> str:
    """
    Format large numbers with thousand separators
    
    Args:
        number: Number to format
        decimals: Number of decimal places
        
    Returns:
        Formatted number string
    """
    if decimals > 0:
        return f"{number:,.{decimals}f}"
    return f"{int(number):,}"


def calculate_rate(count: int, duration_seconds: float) -> float:
    """
    Calculate rate (items per second)
    
    Args:
        count: Number of items
        duration_seconds: Duration in seconds
        
    Returns:
        Rate as items per second
    """
    if duration_seconds <= 0:
        return 0.0
    return count / duration_seconds


def parse_date_range(start_date: str, end_date: str) -> tuple[datetime, datetime]:
    """
    Parse date range strings into datetime objects
    
    Args:
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)
        
    Returns:
        Tuple of (start_datetime, end_datetime)
        
    Raises:
        ValueError: If date format is invalid
    """
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Add time components
        start_dt = start_dt.replace(hour=0, minute=0, second=0)
        end_dt = end_dt.replace(hour=23, minute=59, second=59)
        
        if start_dt > end_dt:
            raise ValueError("Start date must be before end date")
        
        return start_dt, end_dt
    except ValueError as e:
        raise ValueError(f"Invalid date format: {e}. Use YYYY-MM-DD format.")


def generate_date_chunks(
    start_date: datetime, 
    end_date: datetime, 
    chunk_days: int = 30
) -> List[tuple[datetime, datetime]]:
    """
    Split a date range into smaller chunks
    
    Args:
        start_date: Start datetime
        end_date: End datetime
        chunk_days: Days per chunk
        
    Returns:
        List of (chunk_start, chunk_end) tuples
    """
    chunks = []
    current = start_date
    
    while current < end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    
    return chunks


def calculate_file_hash(file_path: Path, algorithm: str = 'sha256') -> str:
    """
    Calculate hash of a file
    
    Args:
        file_path: Path to file
        algorithm: Hash algorithm to use
        
    Returns:
        Hex digest of file hash
    """
    hash_func = hashlib.new(algorithm)
    
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_func.update(chunk)
    
    return hash_func.hexdigest()


def safe_json_dumps(obj: Any, default: Optional[Any] = None) -> str:
    """
    Safely serialize object to JSON string
    
    Args:
        obj: Object to serialize
        default: Default value for non-serializable objects
        
    Returns:
        JSON string
    """
    def json_default(o):
        if isinstance(o, (datetime, timedelta)):
            return o.isoformat()
        elif isinstance(o, Path):
            return str(o)
        elif default is not None:
            return default
        else:
            return str(o)
    
    return json.dumps(obj, default=json_default, indent=2)


def retry_with_backoff(
    func, 
    max_retries: int = 3, 
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0
):
    """
    Retry a function with exponential backoff
    
    Args:
        func: Function to retry
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        backoff_factor: Multiplier for delay after each retry
        
    Returns:
        Function result
        
    Raises:
        Last exception if all retries fail
    """
    import time
    
    delay = initial_delay
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                time.sleep(delay)
                delay *= backoff_factor
            else:
                raise last_exception


def get_memory_usage() -> Dict[str, float]:
    """
    Get current memory usage information
    
    Returns:
        Dictionary with memory usage in MB
    """
    import psutil
    
    memory = psutil.virtual_memory()
    
    return {
        'total_mb': memory.total / 1024 / 1024,
        'available_mb': memory.available / 1024 / 1024,
        'used_mb': memory.used / 1024 / 1024,
        'percent': memory.percent
    }


def ensure_directory(path: Union[str, Path]) -> Path:
    """
    Ensure a directory exists, creating it if necessary
    
    Args:
        path: Directory path
        
    Returns:
        Path object for the directory
    """
    dir_path = Path(path)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def clean_dataframe_column_names(df) -> None:
    """
    Clean DataFrame column names (remove spaces, lowercase)
    
    Args:
        df: Pandas DataFrame (modified in place)
    """
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')


def batch_iterator(items: List[Any], batch_size: int):
    """
    Yield successive batches from a list
    
    Args:
        items: List of items
        batch_size: Size of each batch
        
    Yields:
        Batches of items
    """
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]


def truncate_string(text: str, max_length: int = 80, suffix: str = '...') -> str:
    """
    Truncate a string to a maximum length
    
    Args:
        text: String to truncate
        max_length: Maximum length
        suffix: Suffix to add if truncated
        
    Returns:
        Truncated string
    """
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix
