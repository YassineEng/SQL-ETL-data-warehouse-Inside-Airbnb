from config.database_config import DatabaseConfig

def run():
    conn = DatabaseConfig().create_connection('AirbnbDataWarehouse')
    cur = conn.cursor()
    queries = [
    ("number_of_reviews non-numeric examples", "SELECT TOP 20 number_of_reviews FROM dim_listings_staging WHERE TRY_CAST(number_of_reviews AS BIGINT) IS NULL AND number_of_reviews IS NOT NULL"),
    ("number_of_reviews huge examples", "SELECT TOP 20 number_of_reviews FROM dim_listings_staging WHERE TRY_CAST(number_of_reviews AS BIGINT) > 100000"),
    ("calculated_host_listings_count non-numeric", "SELECT TOP 20 calculated_host_listings_count FROM dim_listings_staging WHERE TRY_CAST(calculated_host_listings_count AS BIGINT) IS NULL AND calculated_host_listings_count IS NOT NULL"),
    ("calculated_host_listings_count huge", "SELECT TOP 20 calculated_host_listings_count FROM dim_listings_staging WHERE TRY_CAST(calculated_host_listings_count AS BIGINT) > 10000"),
    ]
    for title,q in queries:
        print('---', title)
        try:
            cur.execute(q)
            for r in cur.fetchall():
                print(r)
        except Exception as e:
            print('ERR', e)
    conn.close()

if __name__ == '__main__':
    run()
