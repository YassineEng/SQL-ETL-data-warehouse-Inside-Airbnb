
import sys
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))

from config.database_config import DatabaseConfig
from config.settings import Config

if __name__ == '__main__':
    config = Config()
    db = DatabaseConfig(config)
    conn = db.create_connection(database='AirbnbDataWarehouse')
    conn.autocommit = True
    cur = conn.cursor()

    try:
        print('Shrinking data file...')
        cur.execute("DBCC SHRINKFILE (N'AirbnbDataWarehouse', 1024)")
        print('Data file shrink operation completed.')

        print('Shrinking log file...')
        cur.execute("DBCC SHRINKFILE (N'AirbnbDataWarehouse_log', 0, TRUNCATEONLY)")
        print('Log file shrink operation completed.')

    except Exception as e:
        print('Error during database shrink:', e)

    finally:
        conn.close()
