
import sys
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))

from config.database_config import DatabaseConfig
from config.settings import Config

def get_database_size(cursor):
    """Gets the total size of the database."""
    cursor.execute("""
        SELECT
            DB_NAME() AS DbName,
            SUM(size) * 8 / 1024 AS DbSizeInMB
        FROM
            sys.database_files;
    """)
    return cursor.fetchone()

def get_table_sizes(cursor):
    """Gets the size of each table in the database."""
    cursor.execute("""
        SELECT
            t.name AS TableName,
            s.name AS SchemaName,
            p.rows AS RowCounts,
            SUM(a.total_pages) * 8 AS TotalSpaceKB,
            SUM(a.used_pages) * 8 AS UsedSpaceKB,
            (SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS UnusedSpaceKB
        FROM
            sys.tables t
        INNER JOIN
            sys.indexes i ON t.object_id = i.object_id
        INNER JOIN
            sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
        INNER JOIN
            sys.allocation_units a ON p.partition_id = a.container_id
        LEFT OUTER JOIN
            sys.schemas s ON t.schema_id = s.schema_id
        WHERE
            t.name NOT LIKE 'sys%' AND t.name NOT LIKE 'dt%'
        GROUP BY
            t.name, s.name, p.rows
        ORDER BY
            TotalSpaceKB DESC;
    """)
    return cursor.fetchall()

def get_database_files(cursor):
    """Gets information about the database files."""
    cursor.execute("""
        SELECT
            name AS FileName,
            size * 8 / 1024 AS SizeMB,
            max_size,
            growth
        FROM
            sys.database_files;
    """)
    return cursor.fetchall()

if __name__ == '__main__':
    config = Config()
    db = DatabaseConfig(config)
    conn = db.create_connection(database='AirbnbDataWarehouse')
    cur = conn.cursor()

    db_size = get_database_size(cur)
    print(f"Database '{db_size[0]}' size: {db_size[1]} MB")

    print("\n--- Database Files ---")
    db_files = get_database_files(cur)
    print(f"{'File Name':<30} {'Size (MB)':>15} {'Max Size':>15} {'Growth':>15}")
    print("-" * 80)
    for row in db_files:
        print(f"{row[0]:<30} {row[1]:>15} {row[2]:>15} {row[3]:>15}")

    print("\n--- Table Sizes ---")
    table_sizes = get_table_sizes(cur)
    print(f"{'Table':<30} {'Schema':<10} {'Rows':>10} {'Total Space (KB)':>20} {'Used Space (KB)':>20} {'Unused Space (KB)':>22}")
    print("-" * 120)
    for row in table_sizes:
        print(f"{row[0]:<30} {row[1]:<10} {row[2]:>10} {row[3]:>20} {row[4]:>20} {row[5]:>22}")

    conn.close()
