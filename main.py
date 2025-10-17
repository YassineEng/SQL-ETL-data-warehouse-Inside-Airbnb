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
from utils.logger import setup_logging
from utils.utility import validate_directory, create_timestamp

def main():
    """Main ETL pipeline execution"""
    setup_logging()
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
    
    print("🏠 Airbnb Data Warehouse ETL Pipeline")
    print(f"📅 Started at: {create_timestamp()}")
    print("=" * 50)
    
    while True:
        print("\n📊 ETL Pipeline Options:")
        print("1. 🔍 Run EDA Analysis (Extract & Analyze) - Uses RAW data")
        print("2. 🧹 Run Data Cleaning (Transform) - RAW → Cleaned data") 
        print("3. 📥 Run SQL Server Data Loading (Load to Database) - Uses CLEANED data")
        print("4. 🔄 Run Complete ETL Pipeline")
        print("5. 🗃️  Database Management")
        print("6. 🚪 Exit")
        
        choice = input("\nEnter your choice (1-6): ").strip()
        
        if choice == '1':
            run_eda_analysis(config)
        elif choice == '2':
            run_data_cleaning(config)
        elif choice == '3':
            run_sql_data_loading(config)
        elif choice == '4':
            run_complete_etl(config)
        elif choice == '5':
            run_database_management(config)
        elif choice == '6':
            print("👋 Exiting ETL Pipeline. Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please enter 1-6.")

def run_eda_analysis(config: Config):
    """Run Exploratory Data Analysis on raw data"""
    print("\n" + "="*60)
    print("🔍 STARTING EDA ANALYSIS (RAW DATA)")
    print("="*60)
    
    # Check for raw data files
    raw_files = config.get_data_files()
    if not raw_files:
        print("❌ No raw data files found!")
        print(f"💡 Please ensure your raw CSV files are in: {config.RAW_DATA_FOLDER}")
        return
    
    analyzer = AirbnbDataAnalyzer(config)
    analyzer.analyze_all_files()

def run_data_cleaning(config: Config):
    """Run data cleaning and transformation"""
    print("\n" + "="*60)
    print("🧹 STARTING DATA CLEANING & TRANSFORMATION")
    print("="*60)
    
    # Check for raw data files
    raw_files = config.get_data_files()
    if not raw_files:
        print("❌ No raw data files found!")
        print(f"💡 Please ensure your raw CSV files are in: {config.RAW_DATA_FOLDER}")
        return
    
    cleaner = AirbnbDataCleaner(config)
    cleaner.analyze_column_relevance()
    
    response = input("\n🧹 Do you want to create cleaned datasets? (y/n): ")
    if response.lower() == 'y':
        cleaner.create_cleaned_dataset()
        print("\n✅ Data cleaning completed!")

def run_sql_data_loading(config: Config):
    """Load cleaned data into SQL Server data warehouse"""
    print("\n" + "="*60)
    print("📥 STARTING SQL SERVER DATA LOADING")
    print("="*60)
    
    # Check if cleaned data exists
    cleaned_files = config.get_cleaned_data_files()
    if not cleaned_files:
        print("❌ No cleaned data files found!")
        print("💡 Please run data cleaning first (Option 2)")
        return
    
    loader = AirbnbDataLoader(config)

    print('\nWhich load phase do you want to run?')
    print('1. Listings')
    print('2. Calendar')
    print('3. Reviews')
    print('4. All (Listings -> Calendar -> Reviews)')
    print('5. Exit (return to main menu)')
    phase = input('Enter 1-5: ').strip()

    if phase == '1':
        conn = loader.db_config.create_connection(database='AirbnbDataWarehouse')
        try:
            listings = glob.glob(os.path.join(config.CLEANED_DATA_FOLDER, '*listings*.csv.gz'))
            for f in listings:
                loader._load_listings_data(conn, f)
        finally:
            conn.close()
    elif phase == '2':
        conn = loader.db_config.create_connection(database='AirbnbDataWarehouse')
        try:
            print("   Clearing existing data from fact_calendar...")
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE fact_calendar;")
            conn.commit()
            print("   ✅ fact_calendar cleared successfully.")

            calendars = glob.glob(os.path.join(config.CLEANED_DATA_FOLDER, '*calendar*.csv.gz'))
            for f in calendars:
                loader._load_calendar_data(conn, f)
        finally:
            conn.close()
    elif phase == '3':
        conn = loader.db_config.create_connection(database='AirbnbDataWarehouse')
        try:
            print("   Clearing existing data from fact_reviews...")
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE fact_reviews;")
            conn.commit()
            print("   ✅ fact_reviews cleared successfully.")

            reviews = glob.glob(os.path.join(config.CLEANED_DATA_FOLDER, '*reviews*.csv.gz'))
            for f in reviews:
                loader._load_reviews_data(conn, f)
        finally:
            conn.close()
    elif phase == '4':
        loader.load_to_warehouse()
    elif phase == '5':
        print('↩️  Returning to main menu without running SQL load')
        return
    else:
        print('Invalid choice, aborting SQL load')

def run_complete_etl(config: Config):
    """Run the complete ETL pipeline"""
    print("\n" + "="*60)
    print("🔄 STARTING COMPLETE ETL PIPELINE")
    print("="*60)
    
    # Extract & Analyze
    print("\n📊 STEP 1: EDA ANALYSIS (RAW DATA)")
    raw_files = config.get_data_files()
    if not raw_files:
        print("❌ No raw data files found!")
        return
    
    analyzer = AirbnbDataAnalyzer(config)
    analyzer.analyze_all_files()
    
    # Transform
    print("\n🔄 STEP 2: DATA CLEANING & TRANSFORMATION")
    cleaner = AirbnbDataCleaner(config)
    cleaner.create_cleaned_dataset()
    
    # Load to SQL Server
    print("\n📥 STEP 3: SQL SERVER DATA LOADING (CLEANED DATA)")
    
    # Check if cleaned data exists and update SQL scripts
    cleaned_files = config.get_cleaned_data_files()
    if not cleaned_files:
        print("❌ No cleaned data files found after cleaning step!")
        return
    
    loader = AirbnbDataLoader(config)
    loader.load_to_warehouse()
    
    print("\n✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")

def run_database_management(config: Config):
    """Database management operations"""
    print("\n" + "="*60)
    print("🗃️  DATABASE MANAGEMENT")
    print("="*60)
    
    # Remove the import from here since it's now at the top
    db_config = DatabaseConfig()
    
    print("\n📊 Database Operations:")
    print("1. 🔍 Test Database Connection")
    print("2. 📋 Check Database Status")
    print("3. 🗑️  Reset Database (Drop & Recreate)")
    print("4. 📈 View Database Statistics")
    print("5. ↩️  Back to Main Menu")
    
    choice = input("\nEnter your choice (1-5): ").strip()
    
    if choice == '1':
        test_database_connection(db_config)
    elif choice == '2':
        check_database_status(db_config)
    elif choice == '3':
        reset_database(db_config)
    elif choice == '4':
        view_database_stats(db_config)
    elif choice == '5':
        return
    else:
        print("❌ Invalid choice.")

def test_database_connection(db_config: DatabaseConfig):
    """Test SQL Server connection"""
    print("\n🔌 Testing Database Connection...")
    if db_config.test_connection():
        print("✅ Database connection successful!")
    else:
        print("❌ Database connection failed!")
        print("\n🔧 Troubleshooting tips:")
        print("• Ensure SQL Server Express is running")
        print("• Verify ODBC Driver 17 is installed")
        print("• Check if the server name is correct")
        print("• Ensure Windows Authentication is enabled")

def check_database_status(db_config: DatabaseConfig):
    """Check if data warehouse database exists and its status"""
    print("\n📋 Checking Database Status...")
    
    if not db_config.test_connection():
        print("❌ Cannot connect to SQL Server")
        return
    
    try:
        conn = db_config.create_connection('master')
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT name, state_desc FROM sys.databases WHERE name = 'AirbnbDataWarehouse'")
        db_info = cursor.fetchone()
        
        if db_info:
            print(f"✅ Database '{db_info[0]}' exists - Status: {db_info[1]}")
            
            # Check tables
            conn_airbnb = db_config.create_connection('AirbnbDataWarehouse')
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
            
            print(f"\n📊 Tables in database: {len(tables)}")
            for table in tables:
                cursor_airbnb.execute(f"SELECT COUNT(*) FROM {table[0]}")
                count = cursor_airbnb.fetchone()[0]
                print(f"   • {table[0]}: {count:,} records")
            
            conn_airbnb.close()
        else:
            print("❌ Database 'AirbnbDataWarehouse' does not exist")
            print("💡 Run 'SQL Server Data Loading' to create it")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error checking database status: {e}")

def reset_database(db_config: DatabaseConfig):
    """Reset the entire database"""
    print("\n⚠️  RESET DATABASE")
    print("This will DROP and RECREATE the entire AirbnbDataWarehouse!")
    
    confirmation = input("Type 'YES' to confirm: ").strip()
    if confirmation != 'YES':
        print("❌ Reset cancelled")
        return
    
    try:
        conn = db_config.create_connection('master')
        # ALTER DATABASE and DROP DATABASE cannot run inside a user transaction.
        # Enable autocommit so these statements execute immediately.
        try:
            conn.autocommit = True
        except Exception:
            # some drivers may use a different attribute name; ignore if not available
            pass
        cursor = conn.cursor()

        # Execute drop if exists
        try:
            cursor.execute("""
                IF EXISTS (SELECT name FROM sys.databases WHERE name = 'AirbnbDataWarehouse')
                BEGIN
                    ALTER DATABASE AirbnbDataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE AirbnbDataWarehouse;
                    PRINT '✅ Database dropped successfully';
                END
                ELSE
                BEGIN
                    PRINT 'ℹ️  Database does not exist';
                END
            """)
        except Exception as inner_e:
            print(f"❌ Error executing DROP/ALTER statements: {inner_e}")
            try:
                conn.close()
            except Exception:
                pass
            return

        try:
            conn.close()
        except Exception:
            pass

        # Recreate database using DatabaseConfig helper (it sets autocommit internally)
        try:
            db_config.create_database()
            print("✅ Database reset completed! Database recreated.")
            
            # Re-apply the schema
            print("Applying schema to the new database...")
            conn_airbnb = db_config.create_connection('AirbnbDataWarehouse')
            loader = AirbnbDataLoader(Config())
            loader._execute_schema_scripts(conn_airbnb)
            conn_airbnb.close()
            print("✅ Schema applied successfully.")

        except Exception as e:
            print(f"❌ Error recreating database or applying schema: {e}")
            print("💡 The database was dropped; please recreate it manually or retry the reset.")

    except Exception as e:
        print(f"❌ Error resetting database: {e}")

def view_database_stats(db_config: DatabaseConfig):
    """View database statistics and sizes"""
    print("\n📈 Database Statistics")
    
    if not db_config.database_exists():
        print("❌ Database 'AirbnbDataWarehouse' does not exist")
        return
    
    try:
        conn = db_config.create_connection('AirbnbDataWarehouse')
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
        print(f"💾 Database Size: {db_size[1]:.2f} MB")
        
        # Table row counts
        print("\n📊 Table Statistics:")
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
            print(f"   • {table[0]}: {table[1]:,} rows ({table[2]/1024:.1f} MB)")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error viewing database stats: {e}")

if __name__ == "__main__":
    main()