from main import run_sql_data_loading
from config.settings import Config

if __name__ == '__main__':
    cfg = Config()
    run_sql_data_loading(cfg)
