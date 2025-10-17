from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig

q = """
SELECT fk.name AS fk_name,
       sch_parent.name AS parent_schema,
       parent.name AS parent_table,
       col_parent.name AS parent_column,
       sch_ref.name AS ref_schema,
       ref.name AS ref_table,
       col_ref.name AS ref_column
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
JOIN sys.tables parent ON fkc.parent_object_id = parent.object_id
JOIN sys.columns col_parent ON fkc.parent_object_id = col_parent.object_id AND fkc.parent_column_id = col_parent.column_id
JOIN sys.tables ref ON fkc.referenced_object_id = ref.object_id
JOIN sys.columns col_ref ON fkc.referenced_object_id = col_ref.object_id AND fkc.referenced_column_id = col_ref.column_id
JOIN sys.schemas sch_parent ON parent.schema_id = sch_parent.schema_id
JOIN sys.schemas sch_ref ON ref.schema_id = sch_ref.schema_id
WHERE ref.name = 'dim_listings'
"""

if __name__ == '__main__':
    db = DatabaseConfig()
    conn = db.create_connection(database='AirbnbDataWarehouse')
    cur = conn.cursor()
    cur.execute(q)
    rows = cur.fetchall()
    print('Foreign keys referencing dim_listings:')
    for r in rows:
        print(r)
    if not rows:
        print('(none)')
    conn.close()
