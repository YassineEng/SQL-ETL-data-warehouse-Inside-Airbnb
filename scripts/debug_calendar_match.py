from pathlib import Path
import sys
import gzip
import pandas as pd
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig
from config.settings import Config

cfg = Config()
# find the sample gz file created earlier
samples = list(Path(cfg.CLEANED_DATA_FOLDER).glob('sample_calendar_*.csv.gz'))
if not samples:
    print('No sample calendar gz found. Run scripts/run_sample_calendar.py first.')
    sys.exit(1)

sample_gz = samples[0]
print('Using sample gz:', sample_gz)

# read into pandas
df = pd.read_csv(gzip.open(sample_gz, 'rt', encoding='utf-8'), sep='|', engine='python')
print('Sample rows read:', len(df))

# ensure columns
expected = ['listing_id','date','available','price']
for c in expected:
    if c not in df.columns:
        print('Missing column in sample:', c)
        sys.exit(1)

# connect and create debug staging table
db = DatabaseConfig()
conn = db.create_connection(database='AirbnbDataWarehouse')
cur = conn.cursor()

cur.execute('''
IF OBJECT_ID('calendar_debug_staging', 'U') IS NULL
CREATE TABLE calendar_debug_staging (
    listing_id NVARCHAR(4000),
    date NVARCHAR(100),
    available NVARCHAR(10),
    price NVARCHAR(50)
)
''')
conn.commit()
cur.execute('TRUNCATE TABLE calendar_debug_staging')
conn.commit()

# insert rows in batches
insert_sql = 'INSERT INTO calendar_debug_staging (listing_id, date, available, price) VALUES (?, ?, ?, ?)'
batch = []
for _, r in df.iterrows():
    batch.append((str(r['listing_id']) if not pd.isna(r['listing_id']) else None,
                  str(r['date']) if not pd.isna(r['date']) else None,
                  str(r['available']) if not pd.isna(r['available']) else None,
                  str(r['price']) if not pd.isna(r['price']) else None))
    if len(batch) >= 500:
        cur.fast_executemany = True
        cur.executemany(insert_sql, batch)
        conn.commit()
        batch = []
if batch:
    cur.executemany(insert_sql, batch)
    conn.commit()

print('Inserted sample rows into calendar_debug_staging')

# run checks
queries = {
    'total': 'SELECT COUNT(*) FROM calendar_debug_staging',
    'listing_id_numeric': "SELECT COUNT(*) FROM calendar_debug_staging WHERE TRY_CAST(listing_id AS BIGINT) IS NOT NULL",
    'listing_id_exists_in_dim_listings': "SELECT COUNT(*) FROM calendar_debug_staging c WHERE TRY_CAST(c.listing_id AS BIGINT) IN (SELECT listing_id FROM dim_listings)",
    'date_parsable': "SELECT COUNT(*) FROM calendar_debug_staging WHERE TRY_CAST(CONVERT(DATE, date) AS DATE) IS NOT NULL",
    'date_exists_in_dim_dates': "SELECT COUNT(*) FROM calendar_debug_staging c WHERE CONVERT(DATE, c.date) IN (SELECT full_date FROM dim_dates)",
    'both_match': "SELECT COUNT(*) FROM calendar_debug_staging c JOIN dim_listings l ON TRY_CAST(c.listing_id AS BIGINT) = l.listing_id JOIN dim_dates d ON CONVERT(DATE, c.date) = d.full_date",
    'missing_listing_samples': "SELECT TOP 20 listing_id, date, available, price FROM calendar_debug_staging c WHERE TRY_CAST(c.listing_id AS BIGINT) NOT IN (SELECT listing_id FROM dim_listings)",
    'missing_date_samples': "SELECT TOP 20 listing_id, date, available, price FROM calendar_debug_staging c WHERE CONVERT(DATE, c.date) NOT IN (SELECT full_date FROM dim_dates)"
}

for name, q in queries.items():
    try:
        cur.execute(q)
        rows = cur.fetchall()
        print(f'-- {name}: {rows if rows else []}')
    except Exception as e:
        print(f'Error running {name}: {e}')

conn.close()
