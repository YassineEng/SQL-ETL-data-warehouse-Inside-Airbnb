from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))

from config.settings import Config

# load loader module dynamically like run_one_listing does
import importlib.util
spec = importlib.util.spec_from_file_location('data_loader_mod', str(Path('modules') / 'data_loader.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

AirbnbDataLoader = getattr(module, 'AirbnbDataLoader')

if __name__ == '__main__':
    config = Config()
    loader = AirbnbDataLoader(config)
    conn = loader.db_config.create_connection(database='AirbnbDataWarehouse')
    try:
        # pick one calendar file
        files = list(Path(config.CLEANED_DATA_FOLDER).glob('*calendar*.csv*'))
        if not files:
            print('No calendar files found in cleaned_data')
        else:
            target = files[0]
            print('Selected calendar file:', target)
            loader._load_calendar_data(conn, str(target))
    finally:
        conn.close()
