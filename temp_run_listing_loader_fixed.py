
import sys
import os
import glob
from config.settings import Config
from modules.data_loader import AirbnbDataLoader

config = Config()
loader = AirbnbDataLoader(config)

# Create database and schema
loader.db_config.create_database()
conn = loader.db_config.create_connection(database='AirbnbDataWarehouse')
loader._execute_sql_file(conn, 'sql/data/00_prepare_tables.sql')
loader._execute_schema_scripts(conn)

# Load listings data
try:
    listings = glob.glob(os.path.join(config.CLEANED_DATA_FOLDER, '*listings*.csv.gz'))
    for f in listings:
        loader._load_listings_data(conn, f)
finally:
    conn.close()
