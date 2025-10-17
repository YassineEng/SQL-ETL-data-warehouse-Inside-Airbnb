# config/settings.py
"""
Configuration settings for Airbnb Data Warehouse ETL Pipeline
"""

import os
from pathlib import Path
from dataclasses import dataclass

@dataclass
class Config:
    """Application configuration settings"""
    
    def __init__(self):
        # Base directories
        self.BASE_DIR = Path(__file__).parent.parent
        self.RAW_DATA_FOLDER = Path(r"D:\Projects\Web-scraping-and-dataset-download---Airbnb-Insights\data\airbnb_insights_data")
        self.CLEANED_DATA_FOLDER = self.BASE_DIR / "data" / "cleaned_data"
        self.SQL_DIR = self.BASE_DIR / "sql"
        self.MODULES_DIR = self.BASE_DIR / "modules"
        self.CONFIG_DIR = self.BASE_DIR / "config"
        self.LOGS_DIR = self.BASE_DIR / "logs"
        
        # Ensure directories exist
        self.RAW_DATA_FOLDER.mkdir(parents=True, exist_ok=True)
        self.CLEANED_DATA_FOLDER.mkdir(parents=True, exist_ok=True)
        self.LOGS_DIR.mkdir(parents=True, exist_ok=True)
        
        # File patterns
        self.LISTINGS_PATTERN = "*listings*.csv*"
        self.CALENDAR_PATTERN = "*calendar*.csv*"
        self.REVIEWS_PATTERN = "*reviews*.csv*"
        
        # Data processing settings
        self.CHUNK_SIZE = 10000
        self.SAMPLE_SIZE = 1000
        
        # Logging
        self.LOG_LEVEL = "INFO"
        self.LOG_FILE = self.BASE_DIR / "etl_pipeline.log"
        
        # SQL Server settings
        self.SQL_SERVER = os.getenv('SQL_SERVER', 'localhost\\SQLEXPRESS')
        self.SQL_DATABASE = os.getenv('SQL_DATABASE', 'AirbnbDataWarehouse')  # ← This is the correct attribute name
        self.SQL_DRIVER = os.getenv('SQL_DRIVER', '{ODBC Driver 17 for SQL Server}')

        # Spark settings
        self.SPARK_APP_NAME = "AirbnbDataWarehouse"
        self.SPARK_MASTER = "local[*]"
        self.SPARK_CONFIG = {
            "spark.driver.memory": "4g",
            "spark.executor.memory": "4g",
            "spark.sql.shuffle.partitions": "8",
        }
    
    def get_data_files(self, file_type: str = 'raw') -> list[Path]:
        """
        Get data files from the specified folder (raw or cleaned).

        Args:
            file_type (str): 'raw' or 'cleaned'. Defaults to 'raw'.

        Returns:
            list[Path]: A list of file paths.
        """
        if file_type == 'raw':
            folder = self.RAW_DATA_FOLDER
            patterns = [self.LISTINGS_PATTERN, self.CALENDAR_PATTERN, self.REVIEWS_PATTERN]
        elif file_type == 'cleaned':
            folder = self.CLEANED_DATA_FOLDER
            patterns = ["*.csv.gz"]
        else:
            return []

        files = []
        for pattern in patterns:
            files.extend(folder.glob(pattern))
        return files

    def get_cleaned_data_files(self) -> list[Path]:
        """Get all cleaned data files."""
        return self.get_data_files(file_type='cleaned')



    def validate_paths(self) -> bool:
        """Validate that essential directories exist."""
        print("🔍 Validating configuration paths...")
        paths_to_check = {
            "Raw data folder": self.RAW_DATA_FOLDER,
            "Cleaned data folder": self.CLEANED_DATA_FOLDER,
            "SQL script directory": self.SQL_DIR,
        }
        
        all_paths_valid = True
        for name, path in paths_to_check.items():
            if not path.exists():
                print(f"❌ ERROR: {name} not found at: {path}")
                all_paths_valid = False
            else:
                print(f"✅ {name} found.")
        
        return all_paths_valid
    
    def __str__(self) -> str:
        """String representation of configuration"""
        return f"""
Airbnb Data Warehouse Configuration:
-----------------------------------
Base Directory: {self.BASE_DIR}
Raw Data Folder: {self.RAW_DATA_FOLDER}
Cleaned Data Folder: {self.CLEANED_DATA_FOLDER}
SQL Directory: {self.SQL_DIR}
SQL Server: {self.SQL_SERVER}
SQL Database: {self.SQL_DATABASE}  # ← Fixed: changed self.AirbnbDataWarehouse to self.SQL_DATABASE
"""