from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))

from config.database_config import DatabaseConfig

q = {
    'total_staged': "SELECT COUNT(*) FROM dim_listings_staging",
    'staged_convertible': "SELECT COUNT(*) FROM dim_listings_staging WHERE TRY_CAST(listing_id AS BIGINT) IS NOT NULL",
    'staged_nonconvertible': "SELECT COUNT(*) FROM dim_listings_staging WHERE TRY_CAST(listing_id AS BIGINT) IS NULL AND listing_id IS NOT NULL",
    'top_nonconvertible': "SELECT TOP 50 listing_id FROM dim_listings_staging WHERE TRY_CAST(listing_id AS BIGINT) IS NULL AND listing_id IS NOT NULL",
    'top_long_ids': "SELECT TOP 50 listing_id, LEN(listing_id) as len FROM dim_listings_staging WHERE listing_id IS NOT NULL ORDER BY LEN(listing_id) DESC",
    'map_count': "SELECT COUNT(*) FROM dim_listing_id_map",
    'map_examples': "SELECT TOP 20 mapping_id, listing_id, listing_raw_id, part1, part2, part3 FROM dim_listing_id_map ORDER BY created_date DESC"
}

if __name__ == '__main__':
    db = DatabaseConfig()
    conn = db.create_connection(database='AirbnbDataWarehouse')
    cur = conn.cursor()
    print('\nDATABASE STAGING + MAPPING INSPECTION')
    print('---------------------------------')
    for name, sql in q.items():
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            if len(rows) == 1 and len(rows[0]) == 1:
                print(f"{name}: {rows[0][0]}")
            else:
                print(f"{name} (rows={len(rows)}):")
                for r in rows[:20]:
                    print('  ', r)
        except Exception as e:
            print(f"Error running {name}: {e}")
    conn.close()
