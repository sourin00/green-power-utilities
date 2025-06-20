#!/usr/bin/env python3
"""
Command-line interface utilities
"""

import argparse
import sys
from datetime import datetime

from .helpers import format_duration, format_bytes


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Energy Analytics Data Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
   run                      # Run the main pipeline
   test                     # Test data source connectivity
   historical 2023-01-01 2023-12-31  # Import historical data
   streaming                # Run streaming demo
   status                   # Show pipeline status
   setup                    # Setup database schema
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Run command
    parser_run = subparsers.add_parser('run', help='Run the main pipeline')
    
    # Test command
    parser_test = subparsers.add_parser('test', help='Test data source connectivity')
    
    # Historical command
    parser_hist = subparsers.add_parser('historical', help='Import historical data')
    parser_hist.add_argument('start_date', type=str, help='Start date (YYYY-MM-DD)')
    parser_hist.add_argument('end_date', type=str, help='End date (YYYY-MM-DD)')
    
    # Streaming command
    parser_stream = subparsers.add_parser('streaming', help='Run streaming ingestion demo')
    
    # Status command
    parser_status = subparsers.add_parser('status', help='Show pipeline status')
    
    # Setup command
    parser_setup = subparsers.add_parser('setup', help='Setup database schema')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Default to 'run' if no command specified
    if not args.command:
        args.command = 'run'
    
    # Validate dates for historical command
    if args.command == 'historical':
        try:
            datetime.strptime(args.start_date, '%Y-%m-%d')
            datetime.strptime(args.end_date, '%Y-%m-%d')
        except ValueError:
            parser.error("Invalid date format. Use YYYY-MM-DD")
    
    return args


def print_banner():
    """Print application banner"""
    banner = """
    ╔═══════════════════════════════════════════════════════════════╗
    ║           Energy Analytics Data Ingestion Pipeline            ║
    ║                  GreenPower Utilities                         ║
    ║                   Enhanced Version                            ║
    ╚═══════════════════════════════════════════════════════════════╝
    """
    print(banner)


def print_pipeline_info():
    """Print pipeline information"""
    info = """
    Pipeline started successfully!
    
    Scheduled Jobs:
    - Daily weather ingestion: 02:00 (Open-Meteo API)
    - Daily grid data ingestion: 03:00 (Open Power System Data)
    - Weekly household processing: Sunday 01:00 (UCI Dataset)
    - Daily quality checks: 05:00
    - Weekly cleanup: Sunday 04:00
    
    Real Data Sources:
    ✅ UCI Household Electric Power Consumption (auto-download)
    ✅ Open-Meteo Weather API (historical + current)
    ✅ Open Power System Data (European grid data)
    ⚠️  ENTSO-E API (requires authentication token)
    
    Data Coverage:
    📊 Household: France (Sceaux) - minute-level consumption
    🌤️  Weather: Paris, Berlin, Madrid - hourly observations
    ⚡ Grid: France, Germany, Spain - 15-minute operations
    
    Manual Data Processing Options:
    1. Process historical data: pipeline.process_historical_data('2020-01-01', '2020-12-31')
    2. Manual household data: pipeline.manual_data_ingestion('household')
    3. Manual weather data: pipeline.manual_data_ingestion('weather', start_date='2024-01-01', end_date='2024-01-31')
    4. Manual grid data: pipeline.manual_data_ingestion('grid')
    """
    print(info)


def confirm_action(message: str) -> bool:
    """Ask for user confirmation"""
    response = input(f"{message} (y/N): ")
    return response.lower() in ['y', 'yes']


def print_progress(current: int, total: int, prefix: str = "Progress"):
    """Print a progress bar"""
    bar_length = 40
    percent = current / total if total > 0 else 0
    filled_length = int(bar_length * percent)
    bar = '█' * filled_length + '-' * (bar_length - filled_length)
    
    sys.stdout.write(f'{prefix}: |{bar}| {percent:.1%} ({current}/{total})')
    sys.stdout.flush()
    
    if current >= total:
        print()  # New line when complete

