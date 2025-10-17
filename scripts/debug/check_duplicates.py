from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig

q = '''
SELECT listing_id, COUNT(*) as cnt
FROM dim_listings_staging
GROUP BY listing_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
'''

if __name__ == '__main__':
    db = DatabaseConfig()
    conn = db.create_connection(database='AirbnbDataWarehouse')
    cur = conn.cursor()
    cur.execute(q)
    rows = cur.fetchmany(20)
    print('dup sample rows:', len(rows))
    for r in rows:
        print(r)
    conn.close()
