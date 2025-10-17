from pathlib import Path
import gzip
import shutil
import tempfile
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
print('Using sample gz:', sample)

# Uncompress and copy to logs (stable path)
with gzip.open(sample, 'rb') as f_in:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    tmp_name = tmp.name
    tmp.close()
    with open(tmp_name, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

stable_path = Path(cfg.LOGS_DIR) / f'calendar_temp_for_sql_debug_check_{Path(tmp_name).stem}.csv'
shutil.copyfile(tmp_name, stable_path)
print('Stable CSV path:', stable_path)

# Read SQL and substitute path (escape backslashes)
sql = open('sql/data/04_load_calendar.sql','r',encoding='utf-8').read()
sql = sql.replace('{{CALENDAR_FILE_PATH}}', str(stable_path).replace('\\','\\\\'))
# Append checks to capture @@ROWCOUNT and final count
sql += '\nSELECT @@ROWCOUNT AS insert_affected_rows;\nSELECT COUNT(*) AS total_fact_calendar_rows;\n'

# Execute and print result sets
db = DatabaseConfig()
conn = db.create_connection(database='AirbnbDataWarehouse')
cur = conn.cursor()
try:
    cur.execute(sql)
    set_idx = 0
    while True:
        try:
            rows = cur.fetchall()
            print(f'-- Result set {set_idx}: {len(rows)} rows')
            for r in rows[:10]:
                print(r)
        except Exception:
            try:
                rc = cur.rowcount
                print(f'-- Result set {set_idx}: rowcount={rc}')
            except Exception:
                print(f'-- Result set {set_idx}: (no fetchable rows)')
        set_idx += 1
        if not cur.nextset():
            break
    conn.commit()
except Exception as e:
    print('Error executing SQL:', e)
    conn.rollback()
finally:
    conn.close()
    print('Left stable CSV at:', stable_path)
    try:
        Path(tmp_name).unlink()
    except Exception:
        pass
