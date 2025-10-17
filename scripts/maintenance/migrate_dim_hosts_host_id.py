from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig

DB = 'AirbnbDataWarehouse'

fetch_fks_q = """
SELECT fk.name AS fk_name,
       sch_parent.name AS parent_schema,
       parent.name AS parent_table,
       col_parent.name AS parent_column
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
JOIN sys.tables parent ON fkc.parent_object_id = parent.object_id
JOIN sys.columns col_parent ON fkc.parent_object_id = col_parent.object_id AND fkc.parent_column_id = col_parent.column_id
JOIN sys.tables ref ON fkc.referenced_object_id = ref.object_id
JOIN sys.schemas sch_parent ON parent.schema_id = sch_parent.schema_id
WHERE ref.name = 'dim_hosts'
"""

fetch_pk_q = """
SELECT kc.name
FROM sys.key_constraints kc
JOIN sys.tables t ON kc.parent_object_id = t.object_id
WHERE t.name = 'dim_hosts' AND kc.type = 'PK'
"""

if __name__ == '__main__':
    db = DatabaseConfig()
    conn = db.create_connection(database=DB)
    cur = conn.cursor()

    print('Fetching FKs referencing dim_hosts...')
    cur.execute(fetch_fks_q)
    fks = cur.fetchall()
    print('FKs:', fks)

    print('Fetching PK name for dim_hosts...')
    cur.execute(fetch_pk_q)
    pk_rows = cur.fetchall()
    pk_name = pk_rows[0][0] if pk_rows else None
    print('PK name:', pk_name)

    # Drop FKs
    for fk in fks:
        fk_name = fk[0]
        parent_table = fk[2]
        try:
            sql = f"ALTER TABLE [{parent_table}] DROP CONSTRAINT [{fk_name}]"
            print('Dropping FK:', sql)
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            print('Failed dropping FK', fk_name, e)

    # Drop PK on dim_hosts
    if pk_name:
        try:
            cur.execute(f"ALTER TABLE [dim_hosts] DROP CONSTRAINT [{pk_name}]")
            conn.commit()
            print('Dropped PK', pk_name)
        except Exception as e:
            print('Failed dropping PK', e)

    # Alter host_id to BIGINT
    try:
        cur.execute("ALTER TABLE dim_hosts ALTER COLUMN host_id BIGINT NOT NULL")
        conn.commit()
        print('Altered dim_hosts.host_id to BIGINT')
    except Exception as e:
        print('Failed to alter dim_hosts.host_id:', e)

    # Recreate PK
    try:
        new_pk = pk_name if pk_name else 'PK_dim_hosts_host_id'
        cur.execute(f"ALTER TABLE dim_hosts ADD CONSTRAINT [{new_pk}] PRIMARY KEY (host_id)")
        conn.commit()
        print('Recreated PK', new_pk)
    except Exception as e:
        print('Failed recreate PK', e)

    # Recreate FKs
    for fk in fks:
        fk_name, parent_schema, parent_table, parent_column = fk
        try:
            sql = f"ALTER TABLE [{parent_table}] WITH CHECK ADD CONSTRAINT [{fk_name}] FOREIGN KEY([{parent_column}]) REFERENCES [dim_hosts]([host_id])"
            print('Recreating FK:', sql)
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            print('Failed recreate FK', fk_name, e)

    conn.close()
    print('Migration complete')
