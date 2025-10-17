from pathlib import Path
import sys
import gzip
import pandas as pd
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig
from config.settings import Config

if __name__ == '__main__':
    cfg = Config()
    files = list(Path(cfg.CLEANED_DATA_FOLDER).glob('*calendar*.csv*'))
    if not files:
        print('No cleaned calendar files found')
        sys.exit(0)
    f = files[0]
    print('Checking file:', f)
    df = pd.read_csv(gzip.open(f, 'rt', encoding='utf-8'), sep='|', engine='python')
    print('Rows in file:', len(df))

    db = DatabaseConfig()
    conn = db.create_connection(database='AirbnbDataWarehouse')
    cur = conn.cursor()

    # get existing listing ids
    cur.execute('SELECT listing_id FROM dim_listings')
    listing_ids = set(r[0] for r in cur.fetchall())

    # get existing full_date values
    cur.execute('SELECT full_date FROM dim_dates')
    full_dates = set(r[0] for r in cur.fetchall())

    def parse_date(s):
        try:
            return pd.to_datetime(s, dayfirst=False).date()
        except Exception:
            try:
                return pd.to_datetime(s, dayfirst=True).date()
            except Exception:
                return None

    df['listing_id_parsed'] = pd.to_numeric(df['listing_id'], errors='coerce').astype('Int64')
    df['date_parsed'] = df['date'].apply(parse_date)

    total = len(df)
    has_listing = df['listing_id_parsed'].apply(lambda x: x in listing_ids if pd.notna(x) else False).sum()
    has_date = df['date_parsed'].apply(lambda x: x in full_dates if pd.notna(x) else False).sum()
    both = df.apply(lambda r: (r['listing_id_parsed'] in listing_ids if pd.notna(r['listing_id_parsed']) else False) and (r['date_parsed'] in full_dates if pd.notna(r['date_parsed']) else False), axis=1).sum()

    print(f'total rows: {total}')
    print(f'listing_id exists in dim_listings: {has_listing}')
    print(f'date exists in dim_dates: {has_date}')
    print(f'rows where BOTH listing and date exist (would insert): {both}')

    # show some examples of missing cases
    missing_listing = df[df['listing_id_parsed'].apply(lambda x: not (x in listing_ids if pd.notna(x) else False))]
    print('\nExamples of rows with missing listing_id (first 10):')
    print(missing_listing.head(10)[['listing_id','date']])

    missing_date = df[df['date_parsed'].apply(lambda x: not (x in full_dates if pd.notna(x) else False))]
    print('\nExamples of rows with missing date (first 10):')
    print(missing_date.head(10)[['listing_id','date']])

    conn.close()
