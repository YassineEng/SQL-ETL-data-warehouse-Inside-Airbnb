from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig

sqls = {
    'convertible_total': "SELECT COUNT(*) FROM dim_listings_staging WHERE TRY_CAST(listing_id AS BIGINT) IS NOT NULL",
    'distinct_convertible': "SELECT COUNT(DISTINCT TRY_CAST(listing_id AS BIGINT)) FROM dim_listings_staging WHERE TRY_CAST(listing_id AS BIGINT) IS NOT NULL",
    'existing_ids': "SELECT COUNT(DISTINCT listing_id) FROM dim_listings",
    'distinct_new_ids': "SELECT COUNT(*) FROM (SELECT DISTINCT TRY_CAST(listing_id AS BIGINT) AS id FROM dim_listings_staging WHERE TRY_CAST(listing_id AS BIGINT) IS NOT NULL) AS ids WHERE id NOT IN (SELECT listing_id FROM dim_listings)",
    'example_new_ids': "SELECT TOP 20 DISTINCT TRY_CAST(listing_id AS BIGINT) AS id FROM dim_listings_staging WHERE TRY_CAST(listing_id AS BIGINT) IS NOT NULL AND TRY_CAST(listing_id AS BIGINT) NOT IN (SELECT listing_id FROM dim_listings) ORDER BY id"
}

if __name__ == '__main__':
    db = DatabaseConfig()
    conn = db.create_connection(database='AirbnbDataWarehouse')
    cur = conn.cursor()
    print('Move preview')
    for k, q in sqls.items():
        try:
            cur.execute(q)
            rows = cur.fetchall()
            print(f"{k}: {rows}")
        except Exception as e:
            print(f"Error {k}: {e}")
    conn.close()
