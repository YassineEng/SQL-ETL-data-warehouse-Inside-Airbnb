
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.database_config import DatabaseConfig
from config.settings import Config
from utils.logger import get_logger, setup_logging

logger = get_logger(__name__)

if __name__ == '__main__':
    setup_logging()
    config = Config()
    db = DatabaseConfig(config)
    conn = db.create_connection(database='AirbnbDataWarehouse')
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT MIN(full_date), MAX(full_date) FROM dim_dates")
        min_date, max_date = cursor.fetchone()

        if min_date and max_date:
            logger.info(f"✅ Date span in dim_dates: From {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
        else:
            logger.info("ℹ️  dim_dates table is empty or contains no dates.")

    except Exception as e:
        logger.error(f"❌ Error retrieving date span: {e}")
    finally:
        conn.close()
