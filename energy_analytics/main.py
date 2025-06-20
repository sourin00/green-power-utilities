#!/usr/bin/env python3
"""
Energy Analytics Data Ingestion Pipeline - Main Entry Point
GreenPower Utilities Capstone Project

Main execution file that orchestrates the energy data ingestion pipeline.
"""

import sys
import logging
from datetime import datetime, timedelta

from config.settings import ConfigManager
from ingestion.base import DataIngestionPipeline
from utils.cli import parse_arguments, print_banner, print_pipeline_info
from monitoring.logging_utils import setup_logging

# Setup logging
logger = setup_logging()


def main():
    """Main execution function"""
    print_banner()
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Load configuration
    config_manager = ConfigManager()
    
    # Create default config if it doesn't exist
    if not config_manager.config_path.exists():
        logger.info("Creating default configuration file...")
        config_manager.create_default_config()
    
    # Get configurations
    db_config = config_manager.get_database_config()
    ingestion_config = config_manager.get_ingestion_config()
    
    # Handle different commands
    if args.command == "run":
        run_pipeline(db_config, ingestion_config)
    elif args.command == "test":
        test_data_sources(db_config, ingestion_config)
    elif args.command == "historical":
        run_historical_import(db_config, ingestion_config, args.start_date, args.end_date)
    elif args.command == "streaming":
        run_streaming_demo(db_config)
    elif args.command == "status":
        show_pipeline_status(db_config)
    elif args.command == "setup":
        setup_database(db_config)
    else:
        logger.error(f"Unknown command: {args.command}")
        sys.exit(1)


def run_pipeline(db_config, ingestion_config):
    """Run the main pipeline"""
    # Initialize pipeline
    pipeline = DataIngestionPipeline(db_config, ingestion_config)
    
    try:
        # Start the pipeline
        pipeline.start_pipeline()
        
        print_pipeline_info()
        
        # Offer to process historical data immediately
        user_choice = input("Would you like to process historical data now? (y/N): ")
        if user_choice.lower() in ['y', 'yes']:
            print("Processing last 7 days of historical data...")
            try:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=7)
                pipeline.process_historical_data(
                    start_date.strftime('%Y-%m-%d'), 
                    end_date.strftime('%Y-%m-%d')
                )
                print("✅ Historical data processing completed!")
            except Exception as e:
                print(f"❌ Historical data processing failed: {e}")
        
        print("Press Ctrl+C to stop the pipeline")
        
        # Run scheduler (this will run indefinitely)
        pipeline.run_scheduler()
        
    except KeyboardInterrupt:
        print("Stopping pipeline...")
        pipeline.stop_pipeline()
        print("Pipeline stopped successfully")
    
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        pipeline.stop_pipeline()
        sys.exit(1)


def run_historical_import(db_config, ingestion_config, start_date: str, end_date: str):
    """Utility function to run historical data import"""
    print(f"Running historical import from {start_date} to {end_date}")
    
    pipeline = DataIngestionPipeline(db_config, ingestion_config)
    
    try:
        pipeline.start_pipeline()
        pipeline.process_historical_data(start_date, end_date)
        pipeline.stop_pipeline()
        print("Historical import completed successfully!")
        
    except Exception as e:
        print(f"Historical import failed: {e}")
        pipeline.stop_pipeline()
        sys.exit(1)


def test_data_sources(db_config, ingestion_config):
    """Test connectivity to all data sources"""
    print("Testing data source connectivity...")
    
    from tests import DataSourceTester
    
    tester = DataSourceTester(db_config, ingestion_config)
    tester.run_all_tests()


def run_streaming_demo(db_config):
    """Run streaming ingestion demo"""
    print("Starting streaming ingestion demo...")
    
    from ingestion.streaming import StreamingIngestionManager
    import asyncio
    
    streaming_manager = StreamingIngestionManager(db_config)
    
    try:
        asyncio.run(streaming_manager.start_streaming())
    except KeyboardInterrupt:
        print("Streaming demo stopped")
    except Exception as e:
        print(f"Streaming demo failed: {e}")
        sys.exit(1)


def show_pipeline_status(db_config):
    """Show current pipeline status and statistics"""
    from monitoring.status import PipelineStatusReporter
    
    reporter = PipelineStatusReporter(db_config)
    reporter.show_status()


def setup_database(db_config):
    """Setup database schema"""
    print("Setting up database schema...")
    
    from database.connection import DatabaseConnection
    from database.schema import SchemaManager
    
    db_connection = DatabaseConnection(db_config)
    
    if not db_connection.connect():
        print("❌ Failed to connect to database")
        sys.exit(1)
    
    schema_manager = SchemaManager(db_connection)
    
    if schema_manager.create_database_schema():
        print("✅ Database schema created successfully")
        
        if schema_manager.verify_schema():
            print("✅ Schema verification passed")
        else:
            print("⚠️  Schema verification failed")
    else:
        print("❌ Failed to create database schema")
        sys.exit(1)
    
    db_connection.disconnect()


if __name__ == "__main__":
    main()