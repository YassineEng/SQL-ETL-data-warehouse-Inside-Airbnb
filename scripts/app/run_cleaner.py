
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from main import run_data_cleaning_non_interactive
from config.settings import Config

if __name__ == '__main__':
    config = Config()
    run_data_cleaning_non_interactive(config)
