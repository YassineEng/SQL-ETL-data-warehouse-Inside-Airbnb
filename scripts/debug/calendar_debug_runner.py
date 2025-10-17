from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig

# find a sample calendar file generated earlier
cfg_mod = __import__('config.settings', fromlist=['Config'])
Config = cfg_mod.Config
cfg = Config()
cleaned = list(Path(cfg.CLEANED_DATA_FOLDER).glob('sample_calendar*.csv.gz'))
if not cleaned:
    print('No sample calendar file found; please run scripts/run_sample_calendar.py first')
    sys.exit(1)
sample = cleaned[0]
print('Using sample file:', sample)

with open('sql/data/04_load_calendar.sql', 'r', encoding='utf-8') as f:
    sql_script = f.read()
# BULK INSERT needs a path SQL Server can access; use absolute path with backslashes
sql_script = sql_script.replace('{{CALENDAR_FILE_PATH}}', str(sample).replace('\\','\\\\'))

# Connect and execute, printing each result set
db = DatabaseConfig()
conn = db.create_connection(database='AirbnbDataWarehouse')
cur = conn.cursor()
try:
    cur.execute(sql_script)
    # iterate over result sets produced by the batch
    set_index = 0
    while True:
        try:
            rows = cur.fetchall()
            print(f'-- Result set {set_index}: {len(rows)} rows')
            if rows:
                # print up to 10 rows
                for r in rows[:10]:
                    print(r)
        except Exception as e:
            # no rows or not a SELECT
            try:
                cnt = cur.rowcount
                print(f'-- Result set {set_index}: rowcount={cnt}')
            except Exception:
                print(f'-- Result set {set_index}: (no fetchable rows)')
        set_index += 1
        if not cur.nextset():
            break
    conn.commit()
except Exception as e:
    print('Error executing calendar SQL:', e)
    conn.rollback()
finally:
    conn.close()
