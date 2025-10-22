
import sys
import os
import pyodbc

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import Config
from config.database_config import DatabaseConfig
from utils.logger import get_logger

logger = get_logger(__name__)

def check_listing_data(listing_id: int):
    """
    Checks the host_country, property_country, host_country_corrected, and is_local_host for a given listing_id.
    """
    conn = None
    try:
        config = Config()
        db_config = DatabaseConfig(config)
        
        print("Connecting to the database...")
        conn = db_config.create_connection()
        cursor = conn.cursor()
        print("Database connection successful.")

        query = f"""
        SELECT
            listing_id,
            host_country,
            property_country,
            host_country_corrected,
            is_local_host
        FROM dim_listings
        WHERE listing_id = {listing_id};
        """
        
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            print(f"Listing ID: {result[0]}")
            print(f"Host Country: '{result[1]}'")
            print(f"Property Country: '{result[2]}'")
            print(f"Host Country Corrected: '{result[3]}'")
            print(f"Is Local Host: {result[4]}")
        else:
            print(f"No data found for listing_id: {listing_id}")

    except pyodbc.Error as e:
        logger.error(f"Database error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    target_listing_id = 11785  # The listing_id to check
    check_listing_data(target_listing_id)
