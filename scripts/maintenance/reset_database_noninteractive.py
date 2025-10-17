from config.database_config import DatabaseConfig

if __name__ == '__main__':
    db = DatabaseConfig()
    try:
        conn = db.create_connection('master')
        try:
            conn.autocommit = True
        except Exception:
            pass
        cur = conn.cursor()
        print('Dropping database if it exists...')
        try:
            cur.execute("""
                IF EXISTS (SELECT name FROM sys.databases WHERE name = 'AirbnbDataWarehouse')
                BEGIN
                    ALTER DATABASE AirbnbDataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE AirbnbDataWarehouse;
                    PRINT '✅ Database dropped successfully';
                END
                ELSE
                BEGIN
                    PRINT 'ℹ️  Database does not exist';
                END
            """)
        except Exception as e:
            print('Error dropping database:', e)
            try:
                conn.close()
            except Exception:
                pass
            raise
        try:
            conn.close()
        except Exception:
            pass
        print('Recreating database...')
        db.create_database()
        print('Done: AirbnbDataWarehouse recreated')
    except Exception as e:
        print('Failed to reset database:', e)
        raise
