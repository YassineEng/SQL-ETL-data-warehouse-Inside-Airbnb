# utils/utility.py
"""
Utility functions for the Airbnb ETL Pipeline
Common helper functions used across multiple modules
"""

import os
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
import glob


class DataValidationError(Exception):
    """Custom exception for data validation errors"""
    pass


def get_file_size_mb(file_path: str) -> float:
    """
    Get file size in megabytes
    
    Args:
        file_path: Path to the file
        
    Returns:
        File size in MB
    """
    return os.path.getsize(file_path) / (1024 * 1024)


def validate_dataframe(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """
    Validate that dataframe contains all required columns
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        Boolean indicating if validation passed
        
    Raises:
        DataValidationError: If required columns are missing
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise DataValidationError(
            f"DataFrame missing required columns: {missing_columns}"
        )
    
    return True


def safe_read_csv(file_path: str, **kwargs) -> pd.DataFrame:
    """
    Safely read CSV file with error handling
    
    Args:
        file_path: Path to CSV file
        **kwargs: Additional arguments for pd.read_csv
        
    Returns:
        Loaded DataFrame
        
    Raises:
        FileNotFoundError: If file doesn't exist
        pd.errors.EmptyDataError: If file is empty
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    try:
        return pd.read_csv(file_path, **kwargs)
    except pd.errors.EmptyDataError:
        raise pd.errors.EmptyDataError(f"File is empty: {file_path}")


def find_files_by_pattern(pattern: str) -> List[str]:
    """
    Find files matching a pattern with error handling
    
    Args:
        pattern: Glob pattern to search for
        
    Returns:
        List of matching file paths
    """
    try:
        return glob.glob(pattern)
    except Exception as e:
        print(f"Error searching for pattern {pattern}: {e}")
        return []


def format_memory_usage(memory_bytes: int) -> str:
    """
    Format memory usage in human-readable format
    
    Args:
        memory_bytes: Memory usage in bytes
        
    Returns:
        Formatted memory string (e.g., "150.5 MB")
    """
    for unit in ['B', 'KB', 'MB', 'GB']:
        if memory_bytes < 1024.0:
            return f"{memory_bytes:.1f} {unit}"
        memory_bytes /= 1024.0
    return f"{memory_bytes:.1f} TB"


def calculate_missing_percentage(df: pd.DataFrame) -> Dict[str, float]:
    """
    Calculate percentage of missing values for each column
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Dictionary with column names and missing percentages
    """
    total_rows = len(df)
    if total_rows == 0:
        return {}
    
    missing_percentages = {}
    for column in df.columns:
        missing_count = df[column].isna().sum()
        missing_percentages[column] = (missing_count / total_rows) * 100
    
    return missing_percentages


def create_timestamp() -> str:
    """
    Create a standardized timestamp string
    
    Returns:
        Timestamp string in format YYYYMMDD_HHMMSS
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def validate_directory(path: str, create_if_missing: bool = True) -> bool:
    """
    Validate that directory exists and is accessible
    
    Args:
        path: Directory path to validate
        create_if_missing: Whether to create directory if it doesn't exist
        
    Returns:
        Boolean indicating if directory is valid
    """
    try:
        if not os.path.exists(path):
            if create_if_missing:
                os.makedirs(path, exist_ok=True)
                print(f"✅ Created directory: {path}")
            else:
                return False
        
        # Check if we have write permissions
        test_file = os.path.join(path, ".write_test")
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        
        return True
    except Exception as e:
        print(f"❌ Directory validation failed for {path}: {e}")
        return False


def get_data_type_summary(df: pd.DataFrame) -> Dict[str, int]:
    """
    Get summary of data types in DataFrame
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Dictionary with data type counts
    """
    dtype_counts = {}
    for dtype in df.dtypes:
        dtype_str = str(dtype)
        dtype_counts[dtype_str] = dtype_counts.get(dtype_str, 0) + 1
    
    return dtype_counts


def print_progress(current: int, total: int, prefix: str = ""):
    """
    Print progress information
    
    Args:
        current: Current progress
        total: Total items
        prefix: Prefix text for progress message
    """
    percentage = (current / total) * 100 if total > 0 else 0
    print(f"\r{prefix} Progress: {current}/{total} ({percentage:.1f}%)", end="")
    if current == total:
        print()  # New line when complete