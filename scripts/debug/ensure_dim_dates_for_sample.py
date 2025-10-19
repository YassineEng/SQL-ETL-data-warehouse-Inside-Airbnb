from pathlib import Path
import sys
from datetime import date, timedelta
import calendar
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig
from config.settings import Config

cfg = Config()
db = DatabaseConfig()
conn = db.create_connection(database='AirbnbDataWarehouse')
cur = conn.cursor()

# get min and max date from the debug staging
cur.execute("SELECT MIN(CONVERT(date, date)) AS min_date, MAX(CONVERT(date, date)) AS max_date FROM calendar_debug_staging")
row = cur.fetchone()
if not row or row[0] is None:
    print('No dates found in calendar_debug_staging; aborting')
    sys.exit(1)
min_date = row[0]
max_date = row[1]
print('Sample date range:', min_date, '->', max_date)

# collect existing dates
cur.execute('SELECT full_date FROM dim_dates WHERE full_date BETWEEN ? AND ?', (min_date, max_date))
existing = {r[0] for r in cur.fetchall()}

# generate all dates in range
all_dates = []
cur_date = min_date
while cur_date <= max_date:
    if cur_date not in existing:
        all_dates.append(cur_date)
    cur_date = cur_date + timedelta(days=1)

print('Missing dates to insert:', len(all_dates))
if all_dates:
    insert_sql = 'INSERT INTO dim_dates (full_date, year, quarter, month, month_name, day, day_name, is_weekend) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
    params = []
    for d in all_dates:
        y = d.year
        m = d.month
        q = (m - 1) // 3 + 1
        month_name = calendar.month_name[m]
        day = d.day
        day_name = calendar.day_name[d.weekday()]
        is_weekend = 1 if d.weekday() >= 5 else 0
        params.append((d, y, q, m, month_name, day, day_name, is_weekend))
    # batch insert
    cur.fast_executemany = True
    cur.executemany(insert_sql, params)
    conn.commit()
    print('Inserted missing dates:', len(all_dates))

# report counts after insertion
cur.execute('SELECT COUNT(*) FROM dim_dates WHERE full_date BETWEEN ? AND ?', (min_date, max_date))
print('dim_dates count in range:', cur.fetchone()[0])

# how many rows would match the calendar join now?
cur.execute('''
SELECT COUNT(*) FROM calendar_debug_staging c
JOIN dim_listings l ON TRY_CAST(c.listing_id AS BIGINT) = l.listing_id
JOIN dim_dates d ON CONVERT(DATE, c.date) = d.full_date
''')
match_count = cur.fetchone()[0]
print('Rows that match both dim_listings and dim_dates:', match_count)

if match_count > 0:
    print('Inserting matched rows into fact_calendar (sample)')
    insert_calendar = '''
    INSERT INTO fact_calendar (listing_id, week_start_date, week_end_date, avg_price_per_week, available_days_per_week)
    SELECT
      TRY_CAST(c.listing_id AS BIGINT) AS listing_id,
      DATEADD(wk, DATEDIFF(wk, 0, CONVERT(DATE, c.date)), 0) AS week_start_date,
      DATEADD(wk, DATEDIFF(wk, 0, CONVERT(DATE, c.date)), 6) AS week_end_date,
      AVG(TRY_CAST(REPLACE(REPLACE(LTRIM(RTRIM(c.price)), '$', ''), ',', '') AS DECIMAL(10,2))) AS avg_price_per_week,
      SUM(CASE WHEN LOWER(LTRIM(RTRIM(c.available))) IN ('t','true','1') THEN 1 ELSE 0 END) AS available_days_per_week
    FROM calendar_debug_staging c
    JOIN dim_listings l ON TRY_CAST(c.listing_id AS BIGINT) = l.listing_id
    GROUP BY
      TRY_CAST(c.listing_id AS BIGINT),
      DATEADD(wk, DATEDIFF(wk, 0, CONVERT(DATE, c.date)), 0)
    '''
    cur.execute(insert_calendar)
    conn.commit()
    print('Inserted into fact_calendar, rows:', cur.rowcount)

conn.close()
