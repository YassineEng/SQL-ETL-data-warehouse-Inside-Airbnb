from pathlib import Path
import gzip
import pandas as pd
import sys
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig
from config.settings import Config

cfg = Config()
samples = list(Path(cfg.CLEANED_DATA_FOLDER).glob('sample_calendar_*.csv.gz'))
if not samples:
    print('No sample calendar file found. Run scripts/run_sample_calendar.py first.')
    sys.exit(1)
sample = samples[0]
print('Using sample:', sample)

df = pd.read_csv(gzip.open(sample, 'rt', encoding='utf-8'), sep='|', engine='python')
min_date = pd.to_datetime(df['date']).min().date()
max_date = pd.to_datetime(df['date']).max().date()
listing_ids = sorted(df['listing_id'].unique())[:10]

print('Sample date range:', min_date, '->', max_date)
print('Example listing ids (first 10):', listing_ids)

db = DatabaseConfig()
conn = db.create_connection(database='AirbnbDataWarehouse')
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM fact_calendar')
print('Total fact_calendar rows:', cur.fetchone()[0])
cur.execute('SELECT COUNT(*) FROM fact_calendar WHERE week_start_date <= ? AND week_end_date >= ?', (max_date, min_date))
print('Rows in fact_calendar within sample date range:', cur.fetchone()[0])
# check if a sample listing appears
lid = int(listing_ids[0])
cur.execute('SELECT COUNT(*) FROM fact_calendar WHERE listing_id = ?', (lid,))
print(f'Rows for listing {lid}:', cur.fetchone()[0])
conn.close()
