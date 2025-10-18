
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from main import reset_database_non_interactive
from config.settings import Config
from config.database_config import DatabaseConfig

if __name__ == '__main__':
    config = Config()
    db_config = DatabaseConfig(config)
    reset_database_non_interactive(db_config, config)
