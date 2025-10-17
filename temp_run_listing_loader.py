
import sys
import os
import glob
from config.settings import Config
from modules.data_loader import AirbnbDataLoader

config = Config()
loader = AirbnbDataLoader(config)
conn = loader.db_config.create_connection(database='AirbnbDataWarehouse')
try:
    listings = glob.glob(os.path.join(config.CLEANED_DATA_FOLDER, '*listings*.csv.gz'))
    for f in listings:
        loader._load_listings_data(conn, f)
finally:
    conn.close()
