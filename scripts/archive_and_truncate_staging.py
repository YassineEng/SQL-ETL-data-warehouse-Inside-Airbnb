from pathlib import Path
import sys
import datetime
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig

if __name__ == '__main__':
    ts = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    archive_table = f"dim_listings_staging_archive_{ts}"
    csv_path = Path('logs') / f"dim_listings_staging_{ts}.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    db = DatabaseConfig()
    conn = db.create_connection(database='AirbnbDataWarehouse')
    cur = conn.cursor()

    try:
        # create archive table from staging
        cur.execute(f"SELECT TOP 0 * INTO {archive_table} FROM dim_listings_staging")
        conn.commit()
        print('Created archive table:', archive_table)

        # insert current staging into archive table
        cur.execute(f"INSERT INTO {archive_table} SELECT * FROM dim_listings_staging")
        conn.commit()
        print('Copied rows into archive table')

        # export CSV via Python rather than using server tools
        cur.execute("SELECT listing_id, host_id, host_name, host_city, host_country, property_country, property_city, property_neighbourhood, price, number_of_reviews, review_scores_rating, calculated_host_listings_count, is_local_host FROM dim_listings_staging")
        rows = cur.fetchall()
        with open(csv_path, 'w', encoding='utf-8') as f:
            f.write('listing_id,host_id,host_name,host_city,host_country,property_country,property_city,property_neighbourhood,price,number_of_reviews,review_scores_rating,calculated_host_listings_count,is_local_host\n')
            for r in rows:
                f.write(','.join([str(x) if x is not None else '' for x in r]) + '\n')
        print('Exported CSV to', csv_path)

        # finally, truncate staging
        cur.execute('TRUNCATE TABLE dim_listings_staging')
        conn.commit()
        print('Truncated dim_listings_staging')

    except Exception as e:
        print('Error during archive+truncate:', e)
        conn.rollback()
    finally:
        conn.close()
