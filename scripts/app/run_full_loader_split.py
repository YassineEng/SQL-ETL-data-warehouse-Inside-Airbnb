import argparse
from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))
from config.settings import Config
import glob

# Dynamically import the loader to avoid top-level side effects
import importlib.util
spec = importlib.util.spec_from_file_location('data_loader_mod', 'modules/data_loader.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
AirbnbDataLoader = getattr(module, 'AirbnbDataLoader')


def list_files(patterns):
    cfg = Config()
    files = []
    for p in patterns:
        files.extend(sorted(list(Path(cfg.CLEANED_DATA_FOLDER).glob(p))))
    return files


def main():
    parser = argparse.ArgumentParser(description='Run full loader in split phases (listings, calendar, reviews)')
    parser.add_argument('--listings', action='store_true')
    parser.add_argument('--calendar', action='store_true')
    parser.add_argument('--reviews', action='store_true')
    parser.add_argument('--all', action='store_true')
    parser.add_argument('--run', action='store_true', help='Actually execute the phases. Without --run this is a dry-run')
    args = parser.parse_args()

    cfg = Config()
    loader = AirbnbDataLoader(cfg)

    if args.all or args.listings:
        listings = list_files(['*listings*.csv*'])
        print('\nListings files to process:')
        for f in listings:
            print(' -', f)
        if args.run:
            print('\n>>> Running Listings phase')
            conn = loader.db_config.create_connection(database='AirbnbDataWarehouse')
            try:
                for f in listings:
                    loader._load_listings_data(conn, str(f))
            finally:
                conn.close()

    if args.all or args.calendar:
        calendars = list_files(['*calendar*.csv*'])
        print('\nCalendar files to process:')
        for f in calendars:
            print(' -', f)
        if args.run:
            print('\n>>> Running Calendar phase')
            conn = loader.db_config.create_connection(database='AirbnbDataWarehouse')
            try:
                for f in calendars:
                    loader._load_calendar_data(conn, str(f))
            finally:
                conn.close()

    if args.all or args.reviews:
        reviews = list_files(['*reviews*.csv*'])
        print('\nReviews files to process:')
        for f in reviews:
            print(' -', f)
        if args.run:
            print('\n>>> Running Reviews phase')
            conn = loader.db_config.create_connection(database='AirbnbDataWarehouse')
            try:
                for f in reviews:
                    loader._load_reviews_data(conn, str(f))
            finally:
                conn.close()

    if not (args.listings or args.calendar or args.reviews or args.all):
        parser.print_help()


if __name__ == '__main__':
    main()
