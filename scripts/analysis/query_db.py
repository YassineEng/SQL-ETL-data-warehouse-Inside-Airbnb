import sys
import os
from config.settings import Config
from config.database_config import DatabaseConfig
from utils.logger import setup_logging, get_logger

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

setup_logging()
logger = get_logger(__name__)

def query_database():
    config = Config()
    db_config = DatabaseConfig(config)

    if not db_config.test_connection():
        logger.error("Failed to connect to SQL Server. Please check your configuration.")
        return

    try:
        conn = db_config.create_connection(database=config.SQL_DATABASE)
        cursor = conn.cursor()

        logger.info(f"Connected to database: {config.SQL_DATABASE}")
        logger.info("Executing sample query: Listing tables and row counts...")

        cursor.execute("""
            SELECT
                t.name AS TableName,
                p.rows AS RowCounts
            FROM
                sys.tables t
            INNER JOIN
                sys.partitions p ON t.object_id = p.object_id
            WHERE
                p.index_id < 2 -- 0 for heap, 1 for clustered index
                AND t.is_ms_shipped = 0 -- Exclude system tables
            ORDER BY
                TableName;
        """)

        tables = cursor.fetchall()
        if tables:
            logger.info("\n--- Tables in AirbnbDataWarehouse ---")
            for table in tables:
                logger.info(f"  - {table.TableName}: {table.RowCounts:,} rows")
            logger.info("------------------------------------")
        else:
            logger.info("No user tables found in the database.")

        logger.info("\n--- First 10 rows of dim_hosts ---")
        cursor.execute("SELECT TOP 10 * FROM dim_hosts")
        rows = cursor.fetchall()
        if rows:
            for row in rows:
                logger.info(f"  - {row}")
        else:
            logger.info("No rows found in dim_hosts.")
        logger.info("------------------------------------")

    except Exception as e:
        logger.error(f"An error occurred during database query: {e}", exc_info=True)
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    query_database()
