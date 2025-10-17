# utils/logger.py
"""
Logging configuration for the Airbnb ETL Pipeline
"""

import logging
import sys
from datetime import datetime
import os


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the given name
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)

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
    
    # Configure sys.stdout for UTF-8 on Windows if not already set
    if sys.platform == "win32" and sys.stdout.encoding != 'utf-8':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', line_buffering=True)

    # Configure logging
    # Detailed formatter for file logs
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # Simpler formatter for console output
    console_formatter = logging.Formatter('%(message)s')

    log_handlers = []

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    log_handlers.append(console_handler)
    
    if log_to_file:
        log_file = os.path.join(
            logs_dir, 
            f"etl_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(file_formatter)
        log_handlers.append(file_handler)
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        handlers=log_handlers
    )
    
    # Reduce verbosity for some noisy loggers
    logging.getLogger('py4j').setLevel(logging.WARNING)
    logging.getLogger('pyspark').setLevel(logging.WARNING)