# utils/logger.py
"""
Logging configuration for the Airbnb ETL Pipeline
"""

import logging
import sys
from datetime import datetime
import os


def setup_logging(log_level: str = "INFO", log_to_file: bool = True):
    """
    Setup logging configuration for the ETL pipeline
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file: Whether to log to file in addition to console
    """
    # Create logs directory if it doesn't exist
    logs_dir = "logs"
    os.makedirs(logs_dir, exist_ok=True)
    
    # Configure logging
    log_handlers = [logging.StreamHandler(sys.stdout)]
    
    if log_to_file:
        log_file = os.path.join(
            logs_dir, 
            f"etl_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        log_handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=log_handlers
    )
    
    # Reduce verbosity for some noisy loggers
    logging.getLogger('py4j').setLevel(logging.WARNING)
    logging.getLogger('pyspark').setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the given name
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)