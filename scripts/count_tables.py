from config.database_config import DatabaseConfig

def run():
    db = DatabaseConfig()
    conn = db.create_connection('AirbnbDataWarehouse')
    cur = conn.cursor()
    tables = ['dim_listings', 'dim_listings_staging', 'dim_hosts', 'dim_dates', 'fact_calendar', 'fact_reviews']
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            print(f"{t}:", cur.fetchone()[0])
        except Exception as e:
            print(f"{t}: ERROR ->", e)
    conn.close()

if __name__ == '__main__':
    run()
