from config.database_config import DatabaseConfig

def run():
    db = DatabaseConfig()
    conn = db.create_connection('AirbnbDataWarehouse')
    cur = conn.cursor()

    checks = [
        ("Max listing_id (as bigint)", "SELECT MAX(TRY_CAST(listing_id AS BIGINT)) FROM dim_listings_staging"),
        ("Max host_id (as bigint)", "SELECT MAX(TRY_CAST(host_id AS BIGINT)) FROM dim_listings_staging"),
        ("Max number_of_reviews (as bigint)", "SELECT MAX(TRY_CAST(number_of_reviews AS BIGINT)) FROM dim_listings_staging"),
        ("Max calculated_host_listings_count (as bigint)", "SELECT MAX(TRY_CAST(calculated_host_listings_count AS BIGINT)) FROM dim_listings_staging"),
        ("Rows where host_id is not numeric", "SELECT TOP 10 host_id FROM dim_listings_staging WHERE TRY_CAST(host_id AS BIGINT) IS NULL AND host_id IS NOT NULL"),
        ("Rows where number_of_reviews is not numeric", "SELECT TOP 10 number_of_reviews FROM dim_listings_staging WHERE TRY_CAST(number_of_reviews AS BIGINT) IS NULL AND number_of_reviews IS NOT NULL"),
        ("Rows where calculated_host_listings_count is not numeric", "SELECT TOP 10 calculated_host_listings_count FROM dim_listings_staging WHERE TRY_CAST(calculated_host_listings_count AS BIGINT) IS NULL AND calculated_host_listings_count IS NOT NULL"),
    ]

    for title, q in checks:
        try:
            cur.execute(q)
            rows = cur.fetchall()
            print(title + ":")
            for r in rows:
                print('   ', r)
        except Exception as e:
            print(title + ' -> ERROR:', e)

    conn.close()

if __name__ == '__main__':
    run()
