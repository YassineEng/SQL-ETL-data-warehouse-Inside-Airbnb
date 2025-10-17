from config.settings import Config
from modules.data_loader import AirbnbDataLoader

if __name__ == '__main__':
    cfg = Config()
    loader = AirbnbDataLoader(cfg)
    loader.load_to_warehouse()
