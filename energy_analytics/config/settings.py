#!/usr/bin/env python3
"""
Configuration management for Energy Analytics Pipeline
"""

import yaml
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Optional


@dataclass
class DatabaseConfig:
    """Database configuration settings"""
    host: str = "localhost"
    port: int = 5432
    database: str = "energy_analytics"
    username: str = "energy_user"
    password: str = "123qwe"

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'DatabaseConfig':
        """Create DatabaseConfig from dictionary"""
        return cls(**config_dict)


@dataclass
class IngestionConfig:
    """Pipeline configuration settings"""
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay: int = 60  # seconds
    weather_api_delay: float = 0.1  # seconds between requests
    data_retention_days: int = 1095  # 3 years
    enable_streaming: bool = True
    enable_monitoring: bool = True

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'IngestionConfig':
        """Create IngestionConfig from dictionary"""
        return cls(**config_dict)


class ConfigManager:
    """Manages application configuration"""
    
    def __init__(self, config_path: Optional[Path] = None):
        self.config_path = config_path or Path("pipeline_config.yaml")
        self.config_dict = {}
        
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if self.config_path.exists():
            with open(self.config_path, 'r') as f:
                self.config_dict = yaml.safe_load(f)
        return self.config_dict
    
    def save_config(self, config_dict: Dict[str, Any]):
        """Save configuration to YAML file"""
        with open(self.config_path, 'w') as f:
            yaml.dump(config_dict, f, default_flow_style=False)
    
    def get_database_config(self) -> DatabaseConfig:
        """Get database configuration"""
        if not self.config_dict:
            self.load_config()
        
        if 'database' in self.config_dict:
            return DatabaseConfig.from_dict(self.config_dict['database'])
        return DatabaseConfig()
    
    def get_ingestion_config(self) -> IngestionConfig:
        """Get ingestion configuration"""
        if not self.config_dict:
            self.load_config()
        
        if 'ingestion' in self.config_dict:
            return IngestionConfig.from_dict(self.config_dict['ingestion'])
        return IngestionConfig()
    
    def create_default_config(self):
        """Create default configuration file"""
        config = {
            'database': {
                'host': 'localhost',
                'port': 5432,
                'database': 'energy_analytics',
                'username': 'energy_user',
                'password': '123qwe'
            },
            'ingestion': {
                'batch_size': 1000,
                'max_retries': 3,
                'retry_delay': 60,
                'weather_api_delay': 0.1,
                'data_retention_days': 1095,
                'enable_streaming': True,
                'enable_monitoring': True
            },
            'logging': {
                'level': 'INFO',
                'file': 'ingestion_pipeline.log',
                'max_size_mb': 100,
                'backup_count': 5
            },
            'data_sources': {
                'uci_household': {
                    'url': 'https://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip',
                    'household_id': 'uci_france_001'
                },
                'weather_api': {
                    'base_url': 'https://archive-api.open-meteo.com/v1/era5',
                    'forecast_url': 'https://api.open-meteo.com/v1/forecast',
                    'locations': {
                        'paris': {'lat': 48.8566, 'lon': 2.3522, 'location_id': 'paris_fr_001'},
                        'berlin': {'lat': 52.5200, 'lon': 13.4050, 'location_id': 'berlin_de_001'},
                        'madrid': {'lat': 40.4168, 'lon': -3.7038, 'location_id': 'madrid_es_001'}
                    }
                },
                'grid_data': {
                    'opsd_url': 'https://data.open-power-system-data.org/time_series/latest/time_series_60min_singleindex.csv',
                    'entsoe_api': 'https://web-api.tp.entsoe.eu/api'
                }
            }
        }
        
        self.save_config(config)
        return config


# Weather location configuration
WEATHER_LOCATIONS = {
    'paris_france': {'lat': 48.8566, 'lon': 2.3522, 'location_id': 'paris_fr_001'},
    'berlin_germany': {'lat': 52.5200, 'lon': 13.4050, 'location_id': 'berlin_de_001'},
    'madrid_spain': {'lat': 40.4168, 'lon': -3.7038, 'location_id': 'madrid_es_001'}
}
