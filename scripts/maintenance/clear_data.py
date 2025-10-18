
from pathlib import Path
import sys
import datetime
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig
from config.settings import Config

if __name__ == '__main__':
    config = Config()
    db = DatabaseConfig(config)
    conn = db.create_connection(database='AirbnbDataWarehouse')
    cur = conn.cursor()

    try:
        print('Truncating fact_calendar...')
        cur.execute('TRUNCATE TABLE fact_calendar')
        conn.commit()
        print('Truncated fact_calendar')

        print('Truncating dim_listings_staging_archive...')
        cur.execute('TRUNCATE TABLE dim_listings_staging_archive')
        conn.commit()
        print('Truncated dim_listings_staging_archive')

        print('Truncating dim_listings_staging...')
        cur.execute('TRUNCATE TABLE dim_listings_staging')
        conn.commit()
        print('Truncated dim_listings_staging')

    except Exception as e:
        print('Error during truncate:', e)
        conn.rollback()
    finally:
        conn.close()
