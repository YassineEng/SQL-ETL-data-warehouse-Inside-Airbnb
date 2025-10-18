
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from main import run_sql_data_loading_non_interactive
from config.settings import Config
from config.database_config import DatabaseConfig

if __name__ == '__main__':
    config = Config()
    db_config = DatabaseConfig(config)
    run_sql_data_loading_non_interactive(config, db_config)
