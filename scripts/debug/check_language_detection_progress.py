
import sys
import os
import pyodbc

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import Config
from config.database_config import DatabaseConfig
from utils.logger import get_logger

logger = get_logger(__name__)

def check_progress():
    """
    Checks the progress of language detection by counting processed and remaining reviews.
    """
    conn = None
    try:
        config = Config()
        db_config = DatabaseConfig(config)
        
        print("Connecting to the database...")
        conn = db_config.create_connection()
        cursor = conn.cursor()
        print("Database connection successful.")

        # Count total reviews
        cursor.execute("SELECT COUNT(*) FROM fact_reviews")
        total_reviews = cursor.fetchone()[0]

        # Count processed reviews (where review_lang is not NULL and not empty)
        cursor.execute("SELECT COUNT(*) FROM fact_reviews WHERE review_lang IS NOT NULL AND review_lang != ''")
        processed_reviews = cursor.fetchone()[0]

        # Count remaining reviews (where review_lang is NULL or empty)
        cursor.execute("SELECT COUNT(*) FROM fact_reviews WHERE review_lang IS NULL OR review_lang = ''")
        remaining_reviews = cursor.fetchone()[0]

        print(f"\n--- Language Detection Progress ---")
        print(f"Total Reviews: {total_reviews}")
        print(f"Reviews Processed: {processed_reviews}")
        print(f"Reviews Remaining: {remaining_reviews}")
        print(f"-----------------------------------")

    except pyodbc.Error as e:
        logger.error(f"Database error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    check_progress()
