from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig

insert_listings = '''
INSERT INTO dim_listings (
    listing_id, host_id, host_name, host_city, host_country,
    property_country, property_city, property_neighbourhood,
    price, number_of_reviews, review_scores_rating, calculated_host_listings_count, is_local_host
)
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
    CASE WHEN is_local_host = 'True' THEN 1 WHEN is_local_host = 'False' THEN 0 ELSE NULL END
FROM dim_listings_staging
WHERE TRY_CAST(listing_id AS BIGINT) IS NOT NULL
  AND TRY_CAST(listing_id AS BIGINT) NOT IN (SELECT listing_id FROM dim_listings);
'''

insert_map = '''
INSERT INTO dim_listing_id_map (listing_id, listing_raw_id, part1, part2, part3)
SELECT
    TRY_CAST(listing_id AS BIGINT) AS listing_id,
    listing_id AS listing_raw_id,
    LEFT(listing_id, 6) AS part1,
    SUBSTRING(listing_id, 7, 6) AS part2,
    SUBSTRING(listing_id, 13, 6) AS part3
FROM dim_listings_staging;
'''

if __name__ == '__main__':
    db = DatabaseConfig()
    conn = db.create_connection(database='AirbnbDataWarehouse')
    cur = conn.cursor()
    try:
        print('Running INSERT into dim_listings...')
        try:
            cur.execute(insert_listings)
            cur.execute('SELECT @@ROWCOUNT')
            r = cur.fetchone()[0]
            print('Inserted into dim_listings rows=', r)
            conn.commit()
        except Exception as e:
            print('Error inserting into dim_listings:', e)
            conn.rollback()

        print('Running INSERT into dim_listing_id_map...')
        try:
            cur.execute(insert_map)
            cur.execute('SELECT @@ROWCOUNT')
            r2 = cur.fetchone()[0]
            print('Inserted into dim_listing_id_map rows=', r2)
            conn.commit()
        except Exception as e:
            print('Error inserting into dim_listing_id_map:', e)
            conn.rollback()

    finally:
        conn.close()
