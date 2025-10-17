from config.database_config import DatabaseConfig

def run():
    db = DatabaseConfig()
    conn = db.create_connection('AirbnbDataWarehouse')
    cur = conn.cursor()
    cur.execute("SELECT TOP 50 listing_id, host_id, host_name, price, number_of_reviews, calculated_host_listings_count FROM dim_listings_staging")
    rows = cur.fetchall()
    for i, r in enumerate(rows):
        print(i+1, r)
    conn.close()

if __name__ == '__main__':
    run()
