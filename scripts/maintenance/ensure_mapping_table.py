from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig

create_sql = '''
IF OBJECT_ID('dim_listing_id_map', 'U') IS NULL
BEGIN
    CREATE TABLE dim_listing_id_map (
        mapping_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        listing_id BIGINT NULL,
        listing_raw_id NVARCHAR(4000),
        part1 NVARCHAR(255),
        part2 NVARCHAR(255),
        part3 NVARCHAR(255),
        created_date DATETIME2 DEFAULT GETDATE()
    );
END
'''

if __name__ == '__main__':
    db = DatabaseConfig()
    conn = db.create_connection(database='AirbnbDataWarehouse')
    cur = conn.cursor()
    cur.execute(create_sql)
    conn.commit()
    print('ensure_mapping_table: done')
    conn.close()
