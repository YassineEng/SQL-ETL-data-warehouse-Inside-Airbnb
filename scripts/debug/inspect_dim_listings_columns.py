from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig

SQL = {
    'columns': """
SELECT c.name, t.name AS type_name, c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
JOIN sys.objects o ON c.object_id = o.object_id
WHERE o.name = 'dim_listings'
ORDER BY c.column_id
""",

    'computed': """
SELECT name, definition FROM sys.computed_columns cc
JOIN sys.objects o ON cc.object_id = o.object_id
WHERE o.name = 'dim_listings'
""",

    'checks': """
SELECT name, definition FROM sys.check_constraints c
JOIN sys.objects o ON c.parent_object_id = o.object_id
WHERE o.name = 'dim_listings'
""",

    'triggers': """
SELECT t.name, m.definition FROM sys.triggers t
LEFT JOIN sys.sql_modules m ON t.object_id = m.object_id
JOIN sys.objects o ON t.parent_id = o.object_id
WHERE o.name = 'dim_listings'
"""
}

if __name__ == '__main__':
    db = DatabaseConfig()
    conn = db.create_connection(database='AirbnbDataWarehouse')
    cur = conn.cursor()
    for k, q in SQL.items():
        print(f"\n-- {k} --")
        try:
            cur.execute(q)
            rows = cur.fetchall()
            for r in rows:
                print(r)
            if not rows:
                print('(none)')
        except Exception as e:
            print(f"Error running {k}: {e}")
    conn.close()
