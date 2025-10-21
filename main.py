# main.py
"""
Airbnb Data Warehouse ETL Pipeline
Main entry point for the ETL process - Updated for SQL Server
"""

import sys
import os
import glob
from typing import Optional

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import Config
from config.database_config import DatabaseConfig  # ← Add this import
from modules.data_analyzer import AirbnbDataAnalyzer
from modules.data_cleaner import AirbnbDataCleaner
from modules.data_loader import AirbnbDataLoader
from utils.logger import setup_logging, get_logger

logger = get_logger(__name__)
from utils.utility import validate_directory, create_timestamp

def main():
    """Main ETL pipeline execution"""
    setup_logging(log_level="DEBUG")
    config = Config()
    
    
    # Validate paths
    if not config.validate_paths():
        print("❌ Configuration validation failed!")
        return
    
    # Check if raw data exists for EDA and cleaning
    raw_files = config.get_data_files()
    if not raw_files:
        print("❌ No raw data files found for EDA and cleaning!")
        print(f"💡 Please ensure your raw CSV files are in: {config.RAW_DATA_FOLDER}")
        return
    
    logger.info("🏠 Airbnb Data Warehouse ETL Pipeline")
    logger.info(f"📅 Started at: {create_timestamp()}")
    logger.info("=" * 50)
    
    db_config = DatabaseConfig(config)

    while True:
        logger.info("\n📊 ETL Pipeline Options:")
        logger.info("1. 🔍 Run EDA Analysis (Extract & Analyze) - Uses RAW data")
        logger.info("2. 🧹 Run Data Cleaning (Transform) - RAW → Cleaned data") 
        logger.info("3. 📥 Run SQL Server Data Loading (Load to Database) - Uses CLEANED data")
        logger.info("4. 🔄 Run Complete ETL Pipeline")
        logger.info("5. 🗃️  Database Management")
        logger.info("6. 🖼️  Create/Update Views")
        logger.info("7. 🚪 Exit")
        
        choice = input("\nEnter your choice (1-7): ").strip()
        
        if choice == '1':
            run_eda_analysis(config)
        elif choice == '2':
            run_data_cleaning(config)
        elif choice == '3':
            run_sql_data_loading(config, db_config)
        elif choice == '4':
            run_complete_etl(config, db_config)
        elif choice == '5':
            run_database_management(config, db_config)
        elif choice == '6':
            run_create_views(config, db_config)
        elif choice == '7':
            logger.info("👋 Exiting ETL Pipeline. Goodbye!")
            break
        else:
            logger.warning("❌ Invalid choice. Please enter 1-7.")

def run_eda_analysis(config: Config):
    """Run Exploratory Data Analysis on raw data"""
    logger.info("\n" + "="*60)
    logger.info("🔍 STARTING EDA ANALYSIS (RAW DATA)")
    logger.info("="*60)
    
    # Check for raw data files
    raw_files = config.get_data_files()
    if not raw_files:
        logger.error("❌ No raw data files found!")
        logger.info(f"💡 Please ensure your raw CSV files are in: {config.RAW_DATA_FOLDER}")
        return
    
    analyzer = AirbnbDataAnalyzer(config)
    analyzer.analyze_all_files()

def run_data_cleaning(config: Config):
    """Run data cleaning and transformation"""
    logger.info("\n" + "="*60)
    logger.info("🧹 STARTING DATA CLEANING & TRANSFORMATION")
    logger.info("="*60)
    
    # Check for raw data files
    raw_files = config.get_data_files()
    if not raw_files:
        logger.error("❌ No raw data files found!")
        logger.info(f"💡 Please ensure your raw CSV files are in: {config.RAW_DATA_FOLDER}")
        return
    
    cleaner = AirbnbDataCleaner(config)
    cleaner.analyze_column_relevance()
    
    response = input("\n🧹 Do you want to create cleaned datasets? (y/n): ")
    if response.lower() == 'y':
        cleaner.create_cleaned_dataset()
        logger.info("\n✅ Data cleaning completed!")
1
def run_data_cleaning_non_interactive(config: Config):
    """Run data cleaning and transformation without user interaction."""
    logger.info("\n" + "="*60)
    logger.info("🧹 STARTING DATA CLEANING & TRANSFORMATION (NON-INTERACTIVE)")
    logger.info("="*60)

    # Check for raw data files
    raw_files = config.get_data_files()
    if not raw_files:
        logger.error("❌ No raw data files found!")
        logger.info(f"💡 Please ensure your raw CSV files are in: {config.RAW_DATA_FOLDER}")
        return

    cleaner = AirbnbDataCleaner(config)
    cleaner.create_cleaned_dataset()
    logger.info("\n✅ Data cleaning completed!")

def run_sql_data_loading(config: Config, db_config: DatabaseConfig):
    """Load cleaned data into SQL Server data warehouse"""
    logger.info("\n" + "="*60)
    logger.info("📥 STARTING SQL SERVER DATA LOADING")
    logger.info("="*60)
    
    # Check if cleaned data exists
    cleaned_files = config.get_cleaned_data_files()
    if not cleaned_files:
        logger.error("❌ No cleaned data files found!")
    loader = AirbnbDataLoader(config, db_config)

    logger.info('\nWhich load phase do you want to run?')
    logger.info('1. Listings')
    logger.info('2. Calendar')
    logger.info('3. Reviews')
    logger.info('4. All (Listings -> Calendar -> Reviews)')
    logger.info('5. Exit (return to main menu)')
    phase = input('Enter 1-5: ').strip()

    if phase == '1':
        conn = db_config.create_connection(database=config.SQL_DATABASE)
        try:
            listings = glob.glob(os.path.join(config.CLEANED_DATA_FOLDER, '*listings*.csv.gz'))
            for f in listings:
                loader._load_listings_data(conn, f)

            logger.info("   ↳ Populating dim_hosts...")
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE dim_hosts;")
            with open('sql/data/02_load_hosts.sql', 'r', encoding='utf-8-sig') as f:
                sql_script = f.read()
            cursor.execute(sql_script)

            # Fetch the distinct hosts count
            distinct_hosts_count = cursor.fetchone()[0]
            logger.info(f"DEBUG: distinct_hosts_count = {distinct_hosts_count}")
            logger.info(f"   INFO: {distinct_hosts_count:,} distinct hosts found in dim_listings.")

            # Move to the next result set to get the inserted hosts count
            cursor.nextset()
            host_count = cursor.fetchone()[0]
            conn.commit()
            logger.info(f"   ✅ dim_hosts populated: {host_count:,} hosts added.")
        finally:
            conn.close()
    elif phase == '2':
        conn = db_config.create_connection(database=config.SQL_DATABASE)
        try:
            logger.info("   Clearing existing data from fact_calendar...")
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE fact_calendar;")
            conn.commit()
            logger.info("   ✅ fact_calendar cleared successfully.")

            calendars = glob.glob(os.path.join(config.CLEANED_DATA_FOLDER, '*calendar*.csv.gz'))
            for f in calendars:
                loader._load_calendar_data(conn, f)
        finally:
            conn.close()
    elif phase == '3':
        conn = db_config.create_connection(database=config.SQL_DATABASE)
        try:
            logger.info("   Clearing existing data from fact_reviews...")
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE fact_reviews;")
            conn.commit()
            logger.info("   ✅ fact_reviews cleared successfully.")

            reviews = glob.glob(os.path.join(config.CLEANED_DATA_FOLDER, '*reviews*.csv.gz'))
            for f in reviews:
                loader._load_reviews_data(conn, f)
        finally:
            conn.close()
    elif phase == '4':
        loader.load_to_warehouse()
    elif phase == '5':
        logger.info('↩️  Returning to main menu without running SQL load')
        return
    else:
        logger.warning('Invalid choice, aborting SQL load')

def run_complete_etl(config: Config, db_config: DatabaseConfig):
    """Run the complete ETL pipeline"""
    logger.info("\n" + "="*60)
    logger.info("🔄 STARTING COMPLETE ETL PIPELINE")
    logger.info("="*60)
    
    # Extract & Analyze
    logger.info("\n📊 STEP 1: EDA ANALYSIS (RAW DATA)")
    raw_files = config.get_data_files()
    if not raw_files:
        logger.error("❌ No raw data files found!")
        return
    
    analyzer = AirbnbDataAnalyzer(config)
    analyzer.analyze_all_files()
    
    # Transform
    logger.info("\n🔄 STEP 2: DATA CLEANING & TRANSFORMATION")
    cleaner = AirbnbDataCleaner(config)
    cleaner.create_cleaned_dataset()
    
    # Load to SQL Server
    logger.info("\n📥 STEP 3: SQL SERVER DATA LOADING (CLEANED DATA)")
    
    # Check if cleaned data exists and update SQL scripts
    cleaned_files = config.get_cleaned_data_files()
    if not cleaned_files:
        logger.error("❌ No cleaned data files found after cleaning step!")
        return
    
    loader = AirbnbDataLoader(config, db_config)
    loader.load_to_warehouse()
    
    logger.info("\n✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")

def run_create_views(config: Config, db_config: DatabaseConfig):
    """Create or update the database views"""
    logger.info("\n" + "="*60)
    logger.info("🖼️  CREATING/UPDATING DATABASE VIEWS")
    logger.info("="*60)

    if not db_config.database_exists():
        logger.warning(f"❌ Database '{config.SQL_DATABASE}' does not exist")
        return

    try:
        conn = db_config.create_connection(config.SQL_DATABASE)
        loader = AirbnbDataLoader(config, db_config)
        loader.create_views(conn)
        logger.info("✅ Views created/updated successfully.")
    except Exception as e:
        logger.error(f"❌ Error creating views: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()


def run_database_management(config: Config, db_config: DatabaseConfig):
    """Database management operations"""
    logger.info("\n" + "="*60)
    logger.info("🗃️  DATABASE MANAGEMENT")
    logger.info("="*60)
    
    logger.info("\n📊 Database Operations:")
    logger.info("1. 🔍 Test Database Connection")
    logger.info("2. 📋 Check Database Status")
    logger.info("3. 🗑️  Reset Database (Drop & Recreate)")
    logger.info("4. 📈 View Database Statistics")
    logger.info("5. ↩️  Back to Main Menu")
    
    choice = input("\nEnter your choice (1-5): ").strip()
    
    if choice == '1':
        test_database_connection(db_config)
    elif choice == '2':
        check_database_status(db_config, config)
    elif choice == '3':
        reset_database(db_config, config)
    elif choice == '4':
        view_database_stats(db_config, config)
    elif choice == '5':
        return
    else:
        logger.warning("❌ Invalid choice.")

def test_database_connection(db_config: DatabaseConfig):
    """Test SQL Server connection"""
    logger.info("\n🔌 Testing Database Connection...")
    if db_config.test_connection():
        logger.info("✅ Database connection successful!")
    else:
        logger.error("❌ Database connection failed!")
        logger.info("\n🔧 Troubleshooting tips:")
        logger.info("• Ensure SQL Server Express is running")
        logger.info("• Verify ODBC Driver 17 is installed")
        logger.info("• Check if the server name is correct")
        logger.info("• Ensure Windows Authentication is enabled")

def check_database_status(db_config: DatabaseConfig, config: Config):
    """Check if data warehouse database exists and its status"""
    logger.info("\n📋 Checking Database Status...")
    
    if not db_config.test_connection():
        logger.error("❌ Cannot connect to SQL Server")
        return
    
    try:
        conn = db_config.create_connection('master')
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(f"SELECT name, state_desc FROM sys.databases WHERE name = '{config.SQL_DATABASE}'")
        db_info = cursor.fetchone()
        
        if db_info:
            logger.info(f"✅ Database '{db_info[0]}' exists - Status: {db_info[1]}")
            
            # Check tables
            conn_airbnb = db_config.create_connection(config.SQL_DATABASE)
            cursor_airbnb = conn_airbnb.cursor()
            
            cursor_airbnb.execute("""
                SELECT 
                    TABLE_NAME, 
                    TABLE_TYPE
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """)
            tables = cursor_airbnb.fetchall()
            
            logger.info(f"\n📊 Tables in database: {len(tables)}")
            for table in tables:
                cursor_airbnb.execute(f"SELECT COUNT(*) FROM {table[0]}")
                count = cursor_airbnb.fetchone()[0]
                logger.info(f"   • {table[0]}: {count:,} records")
            
            conn_airbnb.close()
        else:
            logger.warning(f"❌ Database '{config.SQL_DATABASE}' does not exist")
            logger.info("💡 Run 'SQL Server Data Loading' to create it")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Error checking database status: {e}")

def reset_database(db_config: DatabaseConfig, config: Config, interactive: bool = True):
    """Reset the entire database"""
    if interactive:
        logger.warning("\n⚠️  RESET DATABASE")
        logger.warning(f"This will DROP and RECREATE the entire {config.SQL_DATABASE}!")
        
        confirmation = input("Type 'YES' to confirm: ").strip()
        if confirmation != 'YES':
            logger.info("❌ Reset cancelled")
            return

    try:
        conn = db_config.create_connection('master')
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute(f"""
            IF EXISTS (SELECT name FROM sys.databases WHERE name = '{config.SQL_DATABASE}')
            BEGIN
                ALTER DATABASE {config.SQL_DATABASE} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE {config.SQL_DATABASE};
            END
        """)

        # Verify that the database was dropped
        cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{config.SQL_DATABASE}'")
        if cursor.fetchone() is not None:
            logger.error(f"❌ Failed to drop database '{config.SQL_DATABASE}'. It still exists.")
            conn.close()
            return

        logger.info("✅ Database dropped successfully.")
        conn.close()

        # Recreate database
        db_config.create_database()
        logger.info("✅ Database recreated successfully.")

        # Re-apply the schema
        logger.info("Applying schema to the new database...")
        conn_airbnb = db_config.create_connection(config.SQL_DATABASE)
        loader = AirbnbDataLoader(config, db_config)
        loader._execute_schema_scripts(conn_airbnb)
        conn_airbnb.close()
        logger.info("✅ Schema applied successfully.")

    except Exception as e:
        logger.error(f"❌ Error resetting database: {e}")

def reset_database_non_interactive(db_config: DatabaseConfig, config: Config):
    """Reset the entire database without user interaction."""
    reset_database(db_config, config, interactive=False)

def view_database_stats(db_config: DatabaseConfig, config: Config):
    """View database statistics and sizes"""
    logger.info("\n📈 Database Statistics")
    
    if not db_config.database_exists():
        logger.warning(f"❌ Database '{config.SQL_DATABASE}' does not exist")
        return
    
    try:
        conn = db_config.create_connection(config.SQL_DATABASE)
        cursor = conn.cursor()
        
        # Database size
        cursor.execute("""
            SELECT 
                DB_NAME() AS DatabaseName,
                SUM(size * 8.0 / 1024) AS SizeMB
            FROM sys.database_files
            WHERE type = 0  -- ROWS data files only
        """)
        db_size = cursor.fetchone()
        logger.info(f"💾 Database Size: {db_size[1]:.2f} MB")
        
        # Table row counts
        logger.info("\n📊 Table Statistics:")
        cursor.execute("""
            SELECT 
                t.name AS TableName,
                p.rows AS RowCounts,
                SUM(a.total_pages) * 8 AS TotalSpaceKB
            FROM sys.tables t
            INNER JOIN sys.indexes i ON t.object_id = i.object_id
            INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
            INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
            WHERE t.name NOT LIKE 'dt%' AND i.object_id > 255 AND i.index_id <= 1
            GROUP BY t.name, p.rows
            ORDER BY p.rows DESC
        """)
        
        tables = cursor.fetchall()
        for table in tables:
            logger.info(f"   • {table[0]}: {table[1]:,} rows ({table[2]/1024:.1f} MB)")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Error viewing database stats: {e}")

def run_sql_data_loading_non_interactive(config: Config, db_config: DatabaseConfig):
    """Load cleaned data into SQL Server data warehouse without user interaction."""
    logger.info("\n" + "="*60)
    logger.info("📥 STARTING SQL SERVER DATA LOADING (NON-INTERACTIVE)")
    logger.info("="*60)

    # Check if cleaned data exists
    cleaned_files = config.get_cleaned_data_files()
    if not cleaned_files:
        logger.error("❌ No cleaned data files found!")
        return

    loader = AirbnbDataLoader(config, db_config)
    loader.load_to_warehouse()

if __name__ == "__main__":
    main()