
import sys
import os
from config.settings import Config
from modules.data_loader import AirbnbDataLoader

print("--- Running Isolated Schema Creation Test ---")
config = Config()
loader = AirbnbDataLoader(config)

# 1. Ensure database exists
print("1. Ensuring database exists...")
loader.db_config.create_database()

# 2. Connect to the database
print("2. Connecting to AirbnbDataWarehouse...")
conn = loader.db_config.create_connection(database='AirbnbDataWarehouse')

# 3. Execute the schema creation script
print("3. Executing 02_create_tables.sql...")
try:
    loader._execute_sql_file(conn, 'sql/schema/02_create_tables.sql')
    print("--- Schema script executed. Checking for tables... ---")
    
    # 4. Verify table creation
    cursor = conn.cursor()
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'dim_listings_staging'")
    result = cursor.fetchone()
    if result:
        print(f"✅ SUCCESS: 'dim_listings_staging' table was created.")
    else:
        print(f"❌ FAILURE: 'dim_listings_staging' table was NOT created.")
finally:
    conn.close()
    print("--- Test Finished ---")
