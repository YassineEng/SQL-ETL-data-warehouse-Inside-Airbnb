from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig

select_sql = '''
SELECT
    TRY_CAST(listing_id AS BIGINT) AS listing_id,
    TRY_CAST(host_id AS BIGINT) AS host_id,
    host_name,
    host_city,
    host_country,
    property_country,
    property_city,
    property_neighbourhood,
    TRY_CAST(REPLACE(REPLACE(price, '$', ''), ',', '') AS DECIMAL(10,2)) AS price,
    TRY_CAST(number_of_reviews AS BIGINT) AS number_of_reviews,
    TRY_CAST(review_scores_rating AS DECIMAL(3,2)) AS review_scores_rating,
    TRY_CAST(calculated_host_listings_count AS BIGINT) AS calculated_host_listings_count,
    CASE WHEN is_local_host = 'True' THEN 1 WHEN is_local_host = 'False' THEN 0 ELSE NULL END AS is_local_host
FROM dim_listings_staging
WHERE TRY_CAST(listing_id AS BIGINT) IS NOT NULL;
'''

if __name__ == '__main__':
    db = DatabaseConfig()
    conn = db.create_connection(database='AirbnbDataWarehouse')
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM (" + select_sql + ") AS s")
        print('select_count:', cur.fetchone()[0])
    except Exception as e:
        print('Error counting select:', e)
    try:
        cur.execute(select_sql)
        rows = cur.fetchmany(10)
        print('sample rows:')
        for r in rows:
            print(r)
    except Exception as e:
        print('Error fetching sample rows:', e)
    conn.close()
