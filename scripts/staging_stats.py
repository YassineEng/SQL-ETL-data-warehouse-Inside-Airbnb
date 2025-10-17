from config.database_config import DatabaseConfig

def run():
    db = DatabaseConfig()
    conn = db.create_connection('AirbnbDataWarehouse')
    cur = conn.cursor()
    checks = [
        ("MAX listing_id as bigint", "SELECT MAX(TRY_CAST(listing_id AS BIGINT)) FROM dim_listings_staging"),
        ("MAX host_id as bigint", "SELECT MAX(TRY_CAST(host_id AS BIGINT)) FROM dim_listings_staging"),
        ("MAX number_of_reviews as bigint", "SELECT MAX(TRY_CAST(number_of_reviews AS BIGINT)) FROM dim_listings_staging"),
        ("MAX calculated_host_listings_count as bigint", "SELECT MAX(TRY_CAST(calculated_host_listings_count AS BIGINT)) FROM dim_listings_staging"),
    ("Count number_of_reviews not CASTable to BIGINT", "SELECT COUNT(*) FROM dim_listings_staging WHERE TRY_CAST(number_of_reviews AS BIGINT) IS NULL AND number_of_reviews IS NOT NULL"),
    ("Count calculated_host_listings_count not CASTable to BIGINT", "SELECT COUNT(*) FROM dim_listings_staging WHERE TRY_CAST(calculated_host_listings_count AS BIGINT) IS NULL AND calculated_host_listings_count IS NOT NULL"),
    ]
    for title, q in checks:
        try:
            cur.execute(q)
            print(title + ':', cur.fetchone()[0])
        except Exception as e:
            print(title + ' -> ERROR:', e)
    conn.close()

if __name__ == '__main__':
    run()
