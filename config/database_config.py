# config/database_config.py
"""
SQL Server database configuration for pure SQL loading
"""

import pyodbc
from typing import Optional
from utils.logger import get_logger
from config.settings import Config

logger = get_logger(__name__)

class DatabaseConfig:
    """SQL Server configuration and connection management"""
    
    def __init__(self, config: Config):
        self.config = config
        self.trusted_connection = 'yes'
    
    def get_connection_string(self, database: Optional[str] = None) -> str:
        """Get SQL Server connection string"""
        db = database or self.config.SQL_DATABASE
        return (
            f'DRIVER={self.config.SQL_DRIVER};'
            f'SERVER={self.config.SQL_SERVER};'
            f'DATABASE={db};'
            f'Trusted_Connection={self.trusted_connection};'
        )
    
    def create_connection(self, database: Optional[str] = None):
        """Create and return database connection"""
        try:
            conn = pyodbc.connect(self.get_connection_string(database))
            return conn
        except pyodbc.Error as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test database connection to master"""
        try:
            conn = self.create_connection('master')
            conn.close()
            return True
        except pyodbc.Error as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def database_exists(self) -> bool:
        """Check if the configured database exists"""
        try:
            conn = self.create_connection('master')
            cursor = conn.cursor()
            cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{self.config.SQL_DATABASE}'")
            exists = cursor.fetchone() is not None
            conn.close()
            return exists
        except pyodbc.Error as e:
            logger.error(f"Error checking if database '{self.config.SQL_DATABASE}' exists: {e}")
            return False

    def create_database(self) -> None:
        """Create the configured database if it doesn't exist"""
        if not self.database_exists():
            try:
                conn = self.create_connection('master')
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute(f"CREATE DATABASE {self.config.SQL_DATABASE}")
                conn.close()
                logger.info(f"Database '{self.config.SQL_DATABASE}' created successfully.")
            except pyodbc.Error as e:
                logger.error(f"Error creating database '{self.config.SQL_DATABASE}': {e}")
                raise