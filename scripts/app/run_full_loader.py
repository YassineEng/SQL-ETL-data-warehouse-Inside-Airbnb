import sys
import os

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from main import run_sql_data_loading
from config.settings import Config
from config.database_config import DatabaseConfig

if __name__ == '__main__':
    cfg = Config()
    db_cfg = DatabaseConfig(cfg)
    run_sql_data_loading(cfg, db_cfg)
