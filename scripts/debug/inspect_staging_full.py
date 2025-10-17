from config.database_config import DatabaseConfig

def run():
    db = DatabaseConfig()
    conn = db.create_connection('AirbnbDataWarehouse')
    cur = conn.cursor()
    cur.execute("SELECT TOP 1 * FROM dim_listings_staging")
    row = cur.fetchone()
    desc = [d[0] for d in cur.description]
    print('Columns:', desc)
    print('Row:', row)
    conn.close()

if __name__ == '__main__':
    run()
