from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))
from config.database_config import DatabaseConfig
from config.settings import Config
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("sql_file", help="Path to the SQL file to execute")
    args = parser.parse_args()

    config = Config()
    db = DatabaseConfig(config)
    conn = db.create_connection(database='AirbnbDataWarehouse')
    cur = conn.cursor()

    with open(args.sql_file, 'r', encoding='utf-8') as f:
        sql_script = f.read()

    # Split the script by 'GO' and execute each batch
    sql_commands = sql_script.split('GO')

    for command in sql_commands:
        if command.strip():  # Ensure the command is not empty
            try:
                cur.execute(command)
                conn.commit()
            except Exception as e:
                print(f"Error executing command: {command.strip()}\nError: {e}")
                conn.rollback()
                raise

    print('Applied schema script')
    conn.close()