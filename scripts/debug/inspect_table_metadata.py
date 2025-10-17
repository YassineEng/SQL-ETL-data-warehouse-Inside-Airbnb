from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig

q_triggers = """
SELECT t.name, o.name AS table_name
FROM sys.triggers t
JOIN sys.objects o ON t.parent_id = o.object_id
WHERE o.name = 'dim_listings'
"""

q_constraints = """
SELECT c.name, c.type_desc
FROM sys.check_constraints c
JOIN sys.objects o ON c.parent_object_id = o.object_id
WHERE o.name = 'dim_listings'
"""

q_computed = """
SELECT name, definition
FROM sys.computed_columns cc
JOIN sys.columns c ON cc.object_id = c.object_id AND cc.name = c.name
JOIN sys.objects o ON cc.object_id = o.object_id
WHERE o.name = 'dim_listings'
"""

q_triggers_all = """
SELECT name FROM sys.triggers
"""

if __name__ == '__main__':
    db = DatabaseConfig()
    conn = db.create_connection(database='AirbnbDataWarehouse')
    cur = conn.cursor()
    for name, q in [('triggers_on_dim_listings', q_triggers), ('constraints', q_constraints), ('computed', q_computed), ('all_triggers', q_triggers_all)]:
        try:
            cur.execute(q)
            rows = cur.fetchall()
            print(f"{name}: {rows}")
        except Exception as e:
            print(f"Error {name}: {e}")
    conn.close()
