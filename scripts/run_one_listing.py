# scripts/run_one_listing.py
import sys
from pathlib import Path
import importlib.util
import importlib.machinery

# Ensure project root is on sys.path for config imports
PROJECT_ROOT = Path('.').resolve()
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import Config


def load_data_loader_module() -> object:
    """Dynamically load modules/data_loader.py without importing the 'modules' package.

    This avoids executing modules/__init__.py which imports pyspark.
    """
    loader_path = PROJECT_ROOT / 'modules' / 'data_loader.py'
    # Provide lightweight stubs for pyspark when not available so we can import the loader for quick tests
    import types
    if 'pyspark' not in sys.modules:
        sys.modules['pyspark'] = types.ModuleType('pyspark')
    if 'pyspark.sql' not in sys.modules:
        sys.modules['pyspark.sql'] = types.ModuleType('pyspark.sql')
        sys.modules['pyspark.sql'].SparkSession = lambda *a, **k: None

    spec = importlib.util.spec_from_file_location('data_loader_mod', str(loader_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


if __name__ == '__main__':
    config = Config()
    data_loader_mod = load_data_loader_module()
    AirbnbDataLoader = getattr(data_loader_mod, 'AirbnbDataLoader')

    # prefer an Ireland file if available
    files = list(Path(config.CLEANED_DATA_FOLDER).glob('*listings*.csv*'))
    target = None
    for f in files:
        if 'Ireland' in f.name:
            target = f
            break
    if target is None and files:
        target = files[0]

    print('Selected file:', target)
    if target:
        loader = AirbnbDataLoader(config)
        conn = loader.db_config.create_connection(database='AirbnbDataWarehouse')
        try:
            loader._load_listings_data(conn, str(target))
        except Exception as e:
            print('Error during single-file load:', e)
        finally:
            conn.close()
    else:
        print('No listings file found in cleaned_data')
