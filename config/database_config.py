# config/database_config.py
"""
SQL Server database configuration for pure SQL loading
"""

import pyodbc
from typing import Optional

class DatabaseConfig:
    """SQL Server configuration and connection management"""
    
    def __init__(self):
        self.server = 'localhost\\SQLEXPRESS'
        self.database = 'master'  # Connect to master first to check/create database
        self.driver = '{ODBC Driver 17 for SQL Server}'
        self.trusted_connection = 'yes'
    
    def get_connection_string(self, database: Optional[str] = None) -> str:
        """Get SQL Server connection string"""
        db = database or self.database
        return (
            f'DRIVER={self.driver};'
            f'SERVER={self.server};'
            f'DATABASE={db};'
            f'Trusted_Connection={self.trusted_connection};'
        )
    
    def create_connection(self, database: Optional[str] = None):
        """Create and return database connection"""
        try:
            conn = pyodbc.connect(self.get_connection_string(database))
            return conn
        except pyodbc.Error as e:
            print(f"❌ Database connection failed: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test database connection to master"""
        try:
            conn = self.create_connection('master')
            conn.close()
            return True
        except:
            return False
    
    def database_exists(self) -> bool:
        """Check if AirbnbDataWarehouse database exists"""
        try:
            conn = self.create_connection('master')
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sys.databases WHERE name = 'AirbnbDataWarehouse'")
            exists = cursor.fetchone() is not None
            conn.close()
            return exists
        except:
            return False

    def create_database(self) -> None:
        """Create the AirbnbDataWarehouse database if it doesn't exist"""
        if not self.database_exists():
            try:
                conn = self.create_connection('master')
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute("CREATE DATABASE AirbnbDataWarehouse")
                conn.close()
                print("✅ Database 'AirbnbDataWarehouse' created successfully.")
            except Exception as e:
                print(f"❌ Error creating database: {e}")
                raise