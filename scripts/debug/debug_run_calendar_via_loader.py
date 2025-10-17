from pathlib import Path
import gzip
import shutil
import tempfile
import sys
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig
from config.settings import Config

# load the loader class
import importlib.util
spec = importlib.util.spec_from_file_location('data_loader_mod', 'modules/data_loader.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
AirbnbDataLoader = getattr(module, 'AirbnbDataLoader')

cfg = Config()
# pick sample
samples = list(Path(cfg.CLEANED_DATA_FOLDER).glob('sample_calendar_*.csv.gz'))
if not samples:
    print('No sample found')
    sys.exit(1)
sample = samples[0]
print('Sample:', sample)

# create temp csv
with gzip.open(sample, 'rb') as f_in:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    tmp_name = tmp.name
    tmp.close()
    with open(tmp_name, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

loader = AirbnbDataLoader(cfg)
conn = loader.db_config.create_connection(database='AirbnbDataWarehouse')
try:
    print('Calling ensure_dim_dates_for_file...')
    loader._ensure_dim_dates_for_file(conn, tmp_name)
    print('Done ensure_dim_dates_for_file')
    # show counts in dim_dates
    cur = conn.cursor()
    cur.execute('SELECT MIN(full_date), MAX(full_date), COUNT(*) FROM dim_dates')
    print('dim_dates min/max/count:', cur.fetchone())

    # Now run the calendar SQL with the tmp path copied to logs
    import shutil, os
    stable_path = os.path.join(cfg.LOGS_DIR, f'calendar_debug_via_loader_{Path(tmp_name).stem}.csv')
    shutil.copyfile(tmp_name, stable_path)
    print('Stable CSV:', stable_path)

    sql = open('sql/data/04_load_calendar.sql','r',encoding='utf-8').read()
    sql = sql.replace('{{CALENDAR_FILE_PATH}}', stable_path.replace('\\','\\\\'))
    # append checks
    sql += '\nSELECT @@ROWCOUNT AS insert_affected_rows; SELECT COUNT(*) AS total_fact_calendar_rows;\n'

    cur.execute(sql)
    set_idx = 0
    while True:
        try:
            rows = cur.fetchall()
            print(f'-- Result set {set_idx}: {len(rows)} rows')
            for r in rows[:5]:
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
finally:
    conn.close()
    try:
        Path(tmp_name).unlink()
    except Exception:
        pass
