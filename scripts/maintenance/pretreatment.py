
import sys
import os
import pyodbc

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.settings import Config
from config.database_config import DatabaseConfig
from utils.logger import get_logger

logger = get_logger(__name__)

def get_us_state_abbreviations():
    """Returns a list of US state and territory abbreviations."""
    return [
        'AL', 'AK', 'AS', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA',
        'GU', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA',
        'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC',
        'ND', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT',
        'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    ]

def process_table(cursor, table_name: str, source_column_name: str, target_column_name: str):
    """
    Processes a given table to add and update a new country column based on a source country column.
    """
    logger.info(f"Processing table: {table_name} for column '{source_column_name}' -> '{target_column_name}'")

    # Check if the target_column_name column exists
    logger.info(f"Checking for '{target_column_name}' column in '{table_name}' table...")
    cursor.execute(f"""
        SELECT *
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = '{target_column_name}'
    """)
    column_exists = cursor.fetchone()

    if column_exists:
        logger.info(f"'{target_column_name}' column found. Dropping it from the {table_name} table...")
        cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN {target_column_name}")
        cursor.connection.commit() # Commit the DDL change immediately
        logger.info(f"Column '{target_column_name}' dropped successfully from {table_name}.")

    logger.info(f"Adding '{target_column_name}' column to the {table_name} table...")
    cursor.execute(f"ALTER TABLE {table_name} ADD {target_column_name} NVARCHAR(100)")
    cursor.connection.commit() # Commit the DDL change immediately
    logger.info(f"Column '{target_column_name}' added successfully to {table_name}.")

    # Update the target_column_name column
    logger.info(f"Updating '{target_column_name}' column in {table_name}...")
    us_states = get_us_state_abbreviations()

    # Create a string for the IN clause of the SQL query
    states_in_clause = ", ".join([f"'{state}'" for state in us_states])

    update_query = f"""
    UPDATE {table_name}
    SET {target_column_name} =
        CASE
            WHEN {source_column_name} IN ({states_in_clause}) THEN 'United States'
            ELSE {source_column_name}
        END
    """

    cursor.execute(update_query)
    logger.info(f"{cursor.rowcount} rows updated in {table_name}.")

def populate_is_local_host(cursor):
    """
    Populates the is_local_host column in the dim_listings table.
    """
    logger.info("Populating 'is_local_host' column in 'dim_listings' table...")
    update_query = """
    UPDATE dim_listings
    SET is_local_host = CASE
        WHEN host_country_corrected = property_country THEN 1
        ELSE 0
    END;
    """
    cursor.execute(update_query)
    logger.info(f"{cursor.rowcount} rows updated in 'dim_listings' for 'is_local_host'.")


def main():
    """
    Main function to perform the pretreatment of the dim_hosts and dim_listings tables.
    """
    conn = None
    try:
        config = Config()
        db_config = DatabaseConfig(config)

        logger.info("Connecting to the database...")
        conn = db_config.create_connection()
        cursor = conn.cursor()
        logger.info("Database connection successful.")

        # Process dim_hosts table
        process_table(cursor, "dim_hosts", "host_country", "host_country_corrected")

        # Explicitly drop 'corrected_host_country' from dim_listings if it exists
        logger.info("Checking for 'corrected_host_country' column in 'dim_listings' table to drop if exists...")
        cursor.execute("""
            SELECT *
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'dim_listings' AND COLUMN_NAME = 'corrected_host_country'
        """)
        old_column_exists = cursor.fetchone()
        if old_column_exists:
            logger.info("'corrected_host_country' column found. Dropping it from the dim_listings table...")
            cursor.execute("ALTER TABLE dim_listings DROP COLUMN corrected_host_country")
            conn.commit() # Commit the DDL change immediately
            logger.info("'corrected_host_country' column dropped successfully from dim_listings.")
        else:
            logger.info("'corrected_host_country' column not found in dim_listings. No need to drop.")

        # Process dim_listings table
        process_table(cursor, "dim_listings", "host_country", "host_country_corrected")

        # Populate is_local_host column in dim_listings
        populate_is_local_host(cursor)

        conn.commit()
        logger.info("All changes committed successfully.")

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
