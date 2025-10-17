from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))

from config.database_config import DatabaseConfig

if __name__ == '__main__':
    db = DatabaseConfig()
    conn = db.create_connection(database='AirbnbDataWarehouse')
    cur = conn.cursor()

    print('Tables in database:')
    cur.execute("SELECT name FROM sys.tables ORDER BY name")
    for r in cur.fetchall():
        print(' -', r[0])

    def count(table):
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            return cur.fetchone()[0]
        except Exception as e:
            return f'ERROR: {e}'

    tables_to_check = ['dim_listings', 'dim_listings_staging', 'dim_listing_id_map']
    for t in tables_to_check:
        print(f"{t}: {count(t)}")

    print('\nTop 10 staging rows (listing_id, LEN(listing_id))')
    try:
        cur.execute("SELECT TOP 10 listing_id, LEN(listing_id) FROM dim_listings_staging ORDER BY LEN(listing_id) DESC")
        for r in cur.fetchall():
            print('  ', r)
    except Exception as e:
        print('Error fetching staging rows:', e)

    print('\nTop 10 dim_listings rows (if present)')
    try:
        cur.execute("SELECT TOP 10 listing_id, host_id, price FROM dim_listings")
        for r in cur.fetchall():
            print('  ', r)
    except Exception as e:
        print('Error fetching dim_listings rows:', e)

    conn.close()
