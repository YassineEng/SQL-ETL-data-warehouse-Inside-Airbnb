# modules/spark_manager.py
"""
Spark session manager for the Airbnb ETL pipeline
Handles Spark session creation and configuration
"""

from pyspark.sql import SparkSession
from config.settings import Config


class SparkSessionManager:
    """
    Manages Spark session lifecycle and configuration
    """
    
    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.spark = None
    
    def start_session(self) -> SparkSession:
        """
        Start and configure Spark session
        
        Returns:
            Configured SparkSession
        """
        if self.spark is not None:
            return self.spark
        
        try:
            builder = SparkSession.builder \
                .appName(self.config.SPARK_APP_NAME) \
                .master(self.config.SPARK_MASTER)
            
            # Add additional configurations
            for key, value in self.config.SPARK_CONFIG.items():
                builder = builder.config(key, value)
            
            # Enable Arrow optimization for Pandas compatibility
            builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
            
            self.spark = builder.getOrCreate()
            
            # Set log level to WARN to reduce verbosity
            self.spark.sparkContext.setLogLevel("WARN")
            
            print("✅ Spark session started successfully")
            return self.spark
            
        except Exception as e:
            print(f"❌ Failed to start Spark session: {e}")
            raise
    
    def stop_session(self):
        """Stop the Spark session"""
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
            print("✅ Spark session stopped")
    
    def get_session(self) -> SparkSession:
        """
        Get current Spark session, start if not exists
        
        Returns:
            Active SparkSession
        """
        if self.spark is None:
            return self.start_session()
        return self.spark
    
    def __enter__(self):
        """Context manager entry"""
        return self.get_session()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop_session()