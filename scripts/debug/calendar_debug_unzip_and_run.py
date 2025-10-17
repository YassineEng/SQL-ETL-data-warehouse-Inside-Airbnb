from pathlib import Path
import sys
import gzip
import shutil
import tempfile
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig
from config.settings import Config

cfg = Config()
# pick the sample file created earlier
samples = list(Path(cfg.CLEANED_DATA_FOLDER).glob('sample_calendar_*.csv.gz'))
if not samples:
    print('No sample calendar file found. Run scripts/run_sample_calendar.py first.')
    sys.exit(1)
sample = samples[0]
print('Using sample gz:', sample)

# create a temp csv uncompressed (BULK INSERT needs plain file path)
tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
tmp_name = tmp.name
tmp.close()
print('Writing uncompressed CSV to:', tmp_name)
with gzip.open(sample, 'rb') as f_in, open(tmp_name, 'wb') as f_out:
    shutil.copyfileobj(f_in, f_out)

# read SQL and replace path
with open('sql/data/04_load_calendar.sql', 'r', encoding='utf-8') as f:
    sql_script = f.read()
# Replace placeholder with double-backslash escaped path for SQL
sql_script = sql_script.replace('{{CALENDAR_FILE_PATH}}', tmp_name.replace('\\','\\\\'))

# execute the SQL and capture debug SELECT output
db = DatabaseConfig()
conn = db.create_connection(database='AirbnbDataWarehouse')
cur = conn.cursor()
try:
    cur.execute(sql_script)
    set_index = 0
    while True:
        try:
            rows = cur.fetchall()
            print(f'-- Result set {set_index}: {len(rows)} rows')
            for r in rows[:10]:
                print(r)
        except Exception as e:
            try:
                rc = cur.rowcount
                print(f'-- Result set {set_index}: rowcount={rc}')
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
    # keep the tmp csv for inspection; print path
    print('Left temp CSV at:', tmp_name)
