from config.database_config import DatabaseConfig

def run():
    db = DatabaseConfig()
    conn = db.create_connection('AirbnbDataWarehouse')
    cur = conn.cursor()
    cur.execute("SELECT COLUMN_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='dim_listings_staging' ORDER BY ORDINAL_POSITION")
    for r in cur.fetchall():
        print(r)
    conn.close()

if __name__ == '__main__':
    run()
