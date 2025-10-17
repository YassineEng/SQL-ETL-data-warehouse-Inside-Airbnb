import pandas as pd
import os
from utils.logger import get_logger

logger = get_logger(__name__)

class DataValidator:
    def __init__(self):
        pass

    def validate_and_fix_calendar_data(self, file_path: str) -> str:
        """
        Validates and fixes calendar data in a CSV file for SQL Server compatibility.
        Specifically converts 't'/'f' in 'available' column to '1'/'0'.
        """
        try:
            df = pd.read_csv(file_path, sep='|', engine='python')

            # Convert 'available' column from 't'/'f' to '1'/'0'
            if 'available' in df.columns:
                df['available'] = df['available'].astype(str).str.lower().map({'t': 1, 'f': 0}).fillna(0).astype(int)
                logger.info(f"   ✅ Converted 'available' column to 1/0 in {os.path.basename(file_path)}")
            
            # Overwrite the original temporary file with the fixed data
            df.to_csv(file_path, sep='|', index=False)
            return file_path

        except Exception as e:
            logger.error(f"   ❌ Error validating/fixing calendar data in {os.path.basename(file_path)}: {e}", exc_info=True)
            raise

