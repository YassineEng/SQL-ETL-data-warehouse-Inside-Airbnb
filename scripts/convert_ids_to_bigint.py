from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig

# This script will:
# - find and drop foreign keys referencing dim_listings
# - drop primary key on dim_listings
# - alter listing_id and related columns to BIGINT
# - alter host_id on dim_hosts to BIGINT
# - recreate primary and foreign keys

DB = 'AirbnbDataWarehouse'

def fetchall(cur, q):
    cur.execute(q)
    return cur.fetchall()

if __name__ == '__main__':
    db = DatabaseConfig()
    conn = db.create_connection(database=DB)
    cur = conn.cursor()

    # 1) Foreign keys referencing dim_listings
    q_fks = """
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
    fks = fetchall(cur, q_fks)
    print('Found FKs referencing dim_listings:', fks)

    q_fks_hosts = """
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
    WHERE ref.name = 'dim_hosts'
    """
    fks_hosts = fetchall(cur, q_fks_hosts)
    print('Found FKs referencing dim_hosts:', fks_hosts)

    all_fks = fks + fks_hosts

    # 2) PK on dim_listings and dim_hosts
    q_pk = """
    SELECT kc.name
    FROM sys.key_constraints kc
    JOIN sys.tables t ON kc.parent_object_id = t.object_id
    WHERE t.name = 'dim_listings' AND kc.type = 'PK'
    """
    pk_rows = fetchall(cur, q_pk)
    pk_name = pk_rows[0][0] if pk_rows else None
    print('dim_listings PK:', pk_name)

    q_pk_hosts = """
    SELECT kc.name
    FROM sys.key_constraints kc
    JOIN sys.tables t ON kc.parent_object_id = t.object_id
    WHERE t.name = 'dim_hosts' AND kc.type = 'PK'
    """
    pk_hosts_rows = fetchall(cur, q_pk_hosts)
    pk_hosts_name = pk_hosts_rows[0][0] if pk_hosts_rows else None
    print('dim_hosts PK:', pk_hosts_name)

    # 3) Drop FKs
    for fk in all_fks:
        fk_name = fk[0]
        parent_table = fk[2]
        try:
            sql = f"ALTER TABLE [{parent_table}] DROP CONSTRAINT [{fk_name}]"
            print('Dropping FK:', sql)
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            print('Failed dropping FK', fk_name, e)

    # 4) Drop PK on dim_listings and dim_hosts
    if pk_name:
        try:
            cur.execute(f"ALTER TABLE [dim_listings] DROP CONSTRAINT [{pk_name}]")
            conn.commit()
            print('Dropped PK', pk_name)
        except Exception as e:
            print('Failed dropping PK on dim_listings', e)

    if pk_hosts_name:
        try:
            cur.execute(f"ALTER TABLE [dim_hosts] DROP CONSTRAINT [{pk_hosts_name}]")
            conn.commit()
            print('Dropped PK', pk_hosts_name)
        except Exception as e:
            print('Failed dropping PK on dim_hosts', e)

    # 5) Alter columns to BIGINT
    alters = [
        "ALTER TABLE dim_listings ALTER COLUMN listing_id BIGINT NOT NULL",
        "ALTER TABLE dim_listings ALTER COLUMN host_id BIGINT NULL",
        "ALTER TABLE dim_listings ALTER COLUMN number_of_reviews BIGINT NULL",
        "ALTER TABLE dim_listings ALTER COLUMN calculated_host_listings_count BIGINT NULL",
        "ALTER TABLE dim_hosts ALTER COLUMN host_id BIGINT NOT NULL",
        "ALTER TABLE fact_calendar ALTER COLUMN listing_id BIGINT NULL",
        "ALTER TABLE fact_reviews ALTER COLUMN listing_id BIGINT NULL",
    ]

    for a in alters:
        try:
            print('Altering:', a)
            cur.execute(a)
            conn.commit()
        except Exception as e:
            print('Failed alter:', a, e)

    # 6) Recreate PK on dim_listings and dim_hosts
    try:
        new_pk = pk_name if pk_name else 'PK_dim_listings_listing_id'
        cur.execute(f"ALTER TABLE dim_listings ADD CONSTRAINT [{new_pk}] PRIMARY KEY (listing_id)")
        conn.commit()
        print('Recreated PK', new_pk)
    except Exception as e:
        print('Failed recreate PK on dim_listings', e)

    try:
        new_pk_hosts = pk_hosts_name if pk_hosts_name else 'PK_dim_hosts_host_id'
        cur.execute(f"ALTER TABLE dim_hosts ADD CONSTRAINT [{new_pk_hosts}] PRIMARY KEY (host_id)")
        conn.commit()
        print('Recreated PK', new_pk_hosts)
    except Exception as e:
        print('Failed recreate PK on dim_hosts', e)

    # 7) Recreate FKs
    for fk in all_fks:
        fk_name, parent_schema, parent_table, parent_column, ref_schema, ref_table, ref_column = fk
        try:
            sql = f"ALTER TABLE [{parent_table}] WITH CHECK ADD CONSTRAINT [{fk_name}] FOREIGN KEY([{parent_column}]) REFERENCES [{ref_table}]([{ref_column}])"
            print('Recreating FK:', sql)
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            print('Failed recreate FK', fk_name, e)

    conn.close()
    print('Done')