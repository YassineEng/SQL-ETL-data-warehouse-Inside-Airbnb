
import sys
import os
import pyodbc
from langdetect import detect, DetectorFactory, LangDetectException

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import Config
from config.database_config import DatabaseConfig
from utils.logger import get_logger, setup_logging

logger = get_logger(__name__)

# Set seed for reproducibility (optional, but good for langdetect)
DetectorFactory.seed = 0

def add_review_lang_column(cursor):
    """
    Adds the 'review_lang' column to the fact_reviews table if it doesn't exist.
    """
    column_name = "review_lang"
    table_name = "fact_reviews"

    logger.info(f"Checking for '{column_name}' column in '{table_name}' table...")
    cursor.execute(f"""
        SELECT *
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = '{column_name}'
    """)
    column_exists = cursor.fetchone()

    if not column_exists:
        logger.info(f"'{column_name}' column not found. Adding it to the {table_name} table...")
        cursor.execute(f"ALTER TABLE {table_name} ADD {column_name} NVARCHAR(10)")
        cursor.connection.commit() # Commit the DDL change immediately
        logger.info(f"Column '{column_name}' added successfully to {table_name}.")
    else:
        logger.info(f"'{column_name}' column already exists in {table_name}.")

def detect_and_update_language(cursor, batch_size=1000):
    """
    Detects the language of review comments and updates the 'review_lang' column in batches
    using a temporary staging table for optimized updates.
    """
    logger.info("Starting language detection and update process...")

    # Create a temporary staging table
    temp_table_name = "#TempReviewLangUpdates"
    cursor.execute(f"IF OBJECT_ID('tempdb..{temp_table_name}') IS NOT NULL DROP TABLE {temp_table_name}")
    cursor.execute(f"CREATE TABLE {temp_table_name} (review_id BIGINT PRIMARY KEY, detected_lang NVARCHAR(10))")
    cursor.connection.commit()
    logger.info(f"Temporary staging table '{temp_table_name}' created.")

    # Fetch all reviews where language is not yet detected or is NULL, and comments are not empty
    select_query = "SELECT review_id, comments FROM fact_reviews WHERE (review_lang IS NULL OR review_lang = '') AND comments IS NOT NULL AND LEN(comments) > 0"
    cursor.execute(select_query)
    reviews_to_process = cursor.fetchall() # Fetch all results into memory

    if not reviews_to_process:
        logger.info("No new reviews to process for language detection.")
        # Drop the temporary staging table before exiting
        cursor.execute(f"DROP TABLE {temp_table_name}")
        cursor.connection.commit()
        logger.info(f"Temporary staging table '{temp_table_name}' dropped.")
        return

    logger.info(f"Found {len(reviews_to_process)} reviews to process.")

    updates_buffer = []
    processed_count = 0

    for review_id, comment in reviews_to_process: # Iterate through in-memory list
        detected_lang = 'und' # Default to undetermined
        log_message = ""

        if comment and comment.strip():
            # Use only a portion of the comment for detection
            snippet = comment[:100]
            try:
                detected_lang = detect(snippet)
                log_message = f"Review_id {review_id}: language detected successfully as '{detected_lang}'."
            except LangDetectException as e:
                log_message = f"Review_id {review_id}: detection failed ({e})."
            except Exception as e:
                log_message = f"Review_id {review_id}: an unexpected error occurred during detection ({e})."
        else:
            log_message = f"Review_id {review_id}: review is missing (empty comment)."

        updates_buffer.append((review_id, detected_lang))
        logger.info(log_message) # Log each detection result

        processed_count += 1
        if processed_count % 100 == 0: # Log progress every 100 reviews
            logger.info(f"Processed {processed_count} reviews...")

        # Perform batch update if buffer reaches batch_size
        if len(updates_buffer) >= batch_size:
            # Prepare values for bulk insert into temporary table
            insert_values = ", ".join([f"({rid}, '{lang}')" for rid, lang in updates_buffer])
            insert_temp_query = f"INSERT INTO {temp_table_name} (review_id, detected_lang) VALUES {insert_values}"
            cursor.execute(insert_temp_query)
            cursor.connection.commit()
            logger.info(f"Inserted {len(updates_buffer)} records into temporary table.")

            # Perform UPDATE from temporary table
            update_main_query = f"""
            UPDATE fr
            SET fr.review_lang = tru.detected_lang
            FROM fact_reviews fr
            INNER JOIN {temp_table_name} tru ON fr.review_id = tru.review_id;
            """
            cursor.execute(update_main_query)
            cursor.connection.commit()
            logger.info(f"Updated {cursor.rowcount} records in fact_reviews from temporary table.")

            # Clear temporary table for next batch
            cursor.execute(f"TRUNCATE TABLE {temp_table_name}")
            cursor.connection.commit()
            logger.info(f"Truncated temporary table '{temp_table_name}'.")

            updates_buffer = [] # Clear buffer for next batch

    # Process any remaining updates in the buffer
    if updates_buffer:
        # Prepare values for bulk insert into temporary table
        insert_values = ", ".join([f"({rid}, '{lang}')" for rid, lang in updates_buffer])
        insert_temp_query = f"INSERT INTO {temp_table_name} (review_id, detected_lang) VALUES {insert_values}"
        cursor.execute(insert_temp_query)
        cursor.connection.commit()
        logger.info(f"Inserted {len(updates_buffer)} records into temporary table.")

        # Perform UPDATE from temporary table
        update_main_query = f"""
        UPDATE fr
        SET fr.review_lang = tru.detected_lang
        FROM fact_reviews fr
        INNER JOIN {temp_table_name} tru ON fr.review_id = tru.review_id;
        """
        cursor.execute(update_main_query)
        cursor.connection.commit()
        logger.info(f"Updated {cursor.rowcount} records in fact_reviews from temporary table.")

        # Clear temporary table for next batch
        cursor.execute(f"TRUNCATE TABLE {temp_table_name}")
        cursor.connection.commit()
        logger.info(f"Truncated temporary table '{temp_table_name}'.")

    # Drop the temporary staging table
    cursor.execute(f"DROP TABLE {temp_table_name}")
    cursor.connection.commit()
    logger.info(f"Temporary staging table '{temp_table_name}' dropped.")

    logger.info(f"Language detection and update complete. Total reviews processed: {processed_count}.")

def main():
    """
    Main function to perform language detection on fact_reviews comments.
    """
    # Setup logging to console
    setup_logging() # Call setup_logging here

    conn = None
    try:
        config = Config()
        db_config = DatabaseConfig(config)

        logger.info("Connecting to the database...")
        conn = db_config.create_connection()
        cursor = conn.cursor()
        logger.info("Database connection successful.")

        add_review_lang_column(cursor)
        detect_and_update_language(cursor)

        # No need for conn.commit() here as batches are committed inside detect_and_update_language
        logger.info("All operations completed.")

    except pyodbc.Error as e:
        logger.error(f"Database error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == '__main__':
    main()


def main():
    """
    Main function to perform language detection on fact_reviews comments.
    """
    conn = None
    try:
        config = Config()
        db_config = DatabaseConfig(config)

        logger.info("Connecting to the database...")
        conn = db_config.create_connection()
        cursor = conn.cursor()
        logger.info("Database connection successful.")

        add_review_lang_column(cursor)
        detect_and_update_language(cursor)

        # No need for conn.commit() here as batches are committed inside detect_and_update_language
        logger.info("All operations completed.")

    except pyodbc.Error as e:
        logger.error(f"Database error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == '__main__':
    main()
