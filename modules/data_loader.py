"""modules.data_loader

Stable data loader that avoids client-side integer coercion by staging
all fields as NVARCHAR in SQL Server, then moving data server-side using
TRY_CAST(... AS BIGINT/DECIMAL). This prevents arithmetic overflow errors
caused by very large numeric-like ID strings and ODBC/pyodbc parameter
type inference.

Key behaviors:
- Read cleaned CSV.GZ files from the configured folder.
- Sanitize fields and insert as strings into dim_listings_staging.
- Run a server-side transaction to TRY_CAST and insert into dim_listings
  and dim_listing_id_map; TRUNCATE staging on success.
- Persist any individual staging insert failures to logs/listings_skipped_rows.csv
  for offline inspection.
"""

import os
import glob
import gzip
import numpy as np
import tempfile
from typing import Optional
import shutil
from datetime import date, timedelta
import calendar

import pandas as pd
import pyodbc

from config.database_config import DatabaseConfig
from config.settings import Config
from modules.data_validator import DataValidator
from utils.logger import get_logger

logger = get_logger(__name__)


class AirbnbDataLoader:
    def __init__(self, config: Config, db_config: DatabaseConfig):
        self.config = config
        self.db_config = db_config
        self.data_validator = DataValidator()
        self.consecutive_errors = 0

    def _is_connection_error(self, e: Exception) -> bool:
        return "Communication link failure" in str(e) or "Shared Memory Provider" in str(e)

    def _reconnect(self, conn) -> pyodbc.Connection:
        logger.info("   Re-establishing database connection...")
        if conn:
            try:
                conn.close()
            except Exception as e:
                logger.warning(f"   ⚠️ Could not close existing connection: {e}")
        
        conn = self.db_config.create_connection(database=self.config.SQL_DATABASE)
        logger.info("   ✅ Connection re-established.")
        return conn

    def load_to_warehouse(self):
        logger.info("📥 Starting SQL Server Data Warehouse Loading...")
        cleaned_files = glob.glob(os.path.join(self.config.CLEANED_DATA_FOLDER, "*.csv.gz"))
        if not cleaned_files:
            logger.error("❌ No cleaned data found. Please run Data Cleaning first (Option 2).")
            return

        if not self.db_config.test_connection():
            logger.error("❌ Cannot connect to SQL Server.")
            return

        # create DB/schema if needed
        self.db_config.create_database()
        conn = self.db_config.create_connection(database=self.config.SQL_DATABASE)
        try:
            # prepare tables and views
            self._execute_sql_file(conn, 'sql/data/00_prepare_tables.sql')
            self._execute_schema_scripts(conn)
            self._load_data_with_dynamic_paths(conn)
            self.create_views(conn)
            self._show_database_statistics(conn)
            logger.info("\n✅ SQL Data Warehouse loading completed!")
        except Exception as e:
            logger.error(f"❌ Error during data loading: {e}")
        finally:
            conn.close()

    def _load_data_with_dynamic_paths(self, conn):
        logger.info("   Clearing existing data from tables...")
        cursor = conn.cursor()
        try:
            # Delete from tables in the correct order to respect foreign keys
            cursor.execute("DELETE FROM fact_reviews;")
            cursor.execute("DELETE FROM fact_calendar;")
            cursor.execute("DELETE FROM dim_listing_id_map;")
            cursor.execute("DELETE FROM dim_listings;")
            cursor.execute("DELETE FROM dim_hosts;")
            
            conn.commit()
            logger.info("   ✅ Tables cleared successfully.")
        except Exception as e:
            logger.error(f"   ❌ Error clearing tables: {e}")
            conn.rollback()
            return

        listings_files = glob.glob(os.path.join(self.config.CLEANED_DATA_FOLDER, "*listings*.csv.gz"))
        calendar_files = glob.glob(os.path.join(self.config.CLEANED_DATA_FOLDER, "*calendar*.csv.gz"))
        reviews_files = glob.glob(os.path.join(self.config.CLEANED_DATA_FOLDER, "*reviews*.csv.gz"))

        if not listings_files:
            logger.error("❌ No cleaned listings files found")
            return

        for file_path in listings_files:
            self._load_listings_data(conn, file_path)
            print(f"DEBUG: Completed processing file: {file_path}")

        print("DEBUG: Listings files loop completed.")
        print("DEBUG: Reached dim_hosts population logic.")

        for file_path in calendar_files:
            self._load_calendar_data(conn, file_path)

        for file_path in reviews_files:
            self._load_reviews_data(conn, file_path)

    def _load_listings_data(self, conn, file_path: str):
        logger.info(f"   ↳ Loading listings file: {os.path.basename(file_path)}")
        temp_file_path: Optional[str] = None
        cursor = conn.cursor()

        try:
            df = pd.read_csv(gzip.open(file_path, 'rt', encoding='utf-8'), sep='|', engine='python')

            # Ensures minimal required columns exist
            expected_cols = [
                'id', 'host_id', 'host_name', 'host_city', 'host_country',
                'property_country', 'property_city', 'property_neighbourhood',
                'latitude', 'longitude',
                'price', 'number_of_reviews', 'review_scores_rating',
                'calculated_host_listings_count', 'is_local_host'
            ]
            for c in expected_cols:
                if c not in df.columns:
                    df[c] = None

            def sanitize_str(val, maxlen=4000):
                if pd.isna(val):
                    return None
                s = str(val).strip()
                return s[:maxlen] if s else None

            def sanitize_numstr(val):
                if pd.isna(val):
                    return None
                s = str(val).strip()
                s = s.replace(',', '')
                if s.endswith('.0'):
                    s = s[:-2]
                return s if s != '' else None

            def sanitize_price(val):
                if pd.isna(val):
                    return None
                s = str(val).strip()
                return s.replace('$', '').replace(',', '')

            def norm_bool(v):
                if pd.isna(v):
                    return None
                sv = str(v).strip()
                return 'True' if sv.lower() in ('true', '1', 't', 'y', 'yes') else (
                    'False' if sv.lower() in ('false', '0', 'f', 'n', 'no') else sv
                )

            rows = []
            for _, r in df.iterrows():
                rows.append((
                    sanitize_str(r.get('id')),
                    sanitize_numstr(r.get('host_id')),
                    sanitize_str(r.get('host_name'), 255),
                    sanitize_str(r.get('host_city'), 255),
                    sanitize_str(r.get('host_country'), 100),
                    sanitize_str(r.get('property_country'), 100),
                    sanitize_str(r.get('property_city'), 255),
                    sanitize_str(r.get('property_neighbourhood'), 255),
                    sanitize_numstr(r.get('latitude')),
                    sanitize_numstr(r.get('longitude')),
                    sanitize_price(r.get('price')),
                    sanitize_numstr(r.get('number_of_reviews')),
                    sanitize_numstr(r.get('review_scores_rating')),
                    sanitize_numstr(r.get('calculated_host_listings_count')),
                    norm_bool(r.get('is_local_host'))
                ))

            # Best-effort: clear staging
            try:
                cursor.execute("TRUNCATE TABLE dim_listings_staging;")
                conn.commit()
            except Exception:
                pass

            insert_staging_sql = (
                "INSERT INTO dim_listings_staging (listing_id, host_id, host_name, host_city, host_country, property_country, property_city, property_neighbourhood, latitude, longitude, price, number_of_reviews, review_scores_rating, calculated_host_listings_count, is_local_host) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )

            cursor.fast_executemany = True
            batch_size = 500
            staging_inserted = 0
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]
                try:
                    cursor.executemany(insert_staging_sql, batch)
                    conn.commit()
                    staging_inserted += len(batch)
                except Exception as be:
                    logger.warning(f"   ⚠️ Staging batch insert failed: {be}")
                    for j, single in enumerate(batch):
                        try:
                            cursor.execute(insert_staging_sql, single)
                            conn.commit()
                            staging_inserted += 1
                        except Exception as se:
                            errlog = os.path.join(self.config.LOGS_DIR, 'listings_skipped_rows.csv')
                            with open(errlog, 'a', encoding='utf-8') as ef:
                                ef.write(','.join([str(x) if x is not None else '' for x in single]) + '\n')

            logger.info(f"   INFO: staged {staging_inserted} rows into dim_listings_staging")
            logger.info("DEBUG: About to count valid listings in staging.")

            # Check valid listing_ids in staging before MERGE
            cursor.execute("SELECT COUNT(*) FROM dim_listings_staging WHERE TRY_CAST(listing_id AS BIGINT) IS NOT NULL")
            valid_listings_in_staging = cursor.fetchone()[0]
            logger.info(f"   INFO: {valid_listings_in_staging} valid listing_ids found in dim_listings_staging for MERGE.")

            # Add this to check valid host_ids in staging
            cursor.execute("SELECT COUNT(*) FROM dim_listings_staging WHERE TRY_CAST(host_id AS BIGINT) IS NOT NULL")
            valid_hosts_in_staging = cursor.fetchone()[0]
            logger.info(f"   INFO: {valid_hosts_in_staging} valid host_ids found in dim_listings_staging for MERGE.")

            move_sql = '''
SET NOCOUNT ON;
BEGIN TRY
    BEGIN TRANSACTION;

    -- Declare a table variable to store the output of the MERGE statement
    DECLARE @merge_summary TABLE(action VARCHAR(10));

    -- Upsert listings: merge staging values into dim_listings (update existing, insert new)
    MERGE INTO dim_listings AS target
    USING (
        SELECT
            TRY_CAST(listing_id AS BIGINT) AS listing_id,
            TRY_CAST(host_id AS BIGINT) AS host_id,
            host_name,
            host_city,
            host_country,
            property_country,
            property_city,
            property_neighbourhood,
            TRY_CAST(latitude AS DECIMAL(9,6)) AS latitude,
            TRY_CAST(longitude AS DECIMAL(9,6)) AS longitude,
            TRY_CAST(REPLACE(REPLACE(price, '$', ''), ',', '') AS DECIMAL(10,2)) AS price,
            TRY_CAST(number_of_reviews AS BIGINT) AS number_of_reviews,
            TRY_CAST(review_scores_rating AS DECIMAL(5,2)) AS review_scores_rating,
            TRY_CAST(calculated_host_listings_count AS BIGINT) AS calculated_host_listings_count
        FROM dim_listings_staging
        WHERE TRY_CAST(listing_id AS BIGINT) IS NOT NULL
    ) AS src
    ON target.listing_id = src.listing_id
    WHEN MATCHED THEN
        UPDATE SET
            host_id = src.host_id,
            host_name = src.host_name,
            host_city = src.host_city,
            host_country = src.host_country,
            property_country = src.property_country,
            property_city = src.property_city,
            property_neighbourhood = src.property_neighbourhood,
            latitude = src.latitude,
            longitude = src.longitude,
            price = src.price,
            number_of_reviews = src.number_of_reviews,
            review_scores_rating = src.review_scores_rating,
            calculated_host_listings_count = src.calculated_host_listings_count
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (listing_id, host_id, host_name, host_city, host_country, property_country, property_city, property_neighbourhood, latitude, longitude, price, number_of_reviews, review_scores_rating, calculated_host_listings_count)
        VALUES (src.listing_id, src.host_id, src.host_name, src.host_city, src.host_country, src.property_country, src.property_city, src.property_neighbourhood, src.latitude, src.longitude, src.price, src.number_of_reviews, src.review_scores_rating, src.calculated_host_listings_count)
    OUTPUT $action INTO @merge_summary;

    -- Always insert mapping rows for every staging row so raw IDs are preserved.
    INSERT INTO dim_listing_id_map (listing_id, listing_raw_id, part1, part2, part3)
    SELECT
        TRY_CAST(listing_id AS BIGINT) AS listing_id,
        listing_id AS listing_raw_id,
        LEFT(listing_id, 6) AS part1,
        SUBSTRING(listing_id, 7, 6) AS part2,
        SUBSTRING(listing_id, 13, 6) AS part3
    FROM dim_listings_staging;

    -- Truncate staging after archiving
    TRUNCATE TABLE dim_listings_staging;

    COMMIT TRANSACTION;

    -- Return the summary of actions
    SELECT action, COUNT(*) as action_count
    FROM @merge_summary
    GROUP BY action;

    -- Add this line to check total rows in dim_listings
    SELECT COUNT(*) AS total_dim_listings_rows FROM dim_listings;

    -- Add this line to check non-NULL host_ids in dim_listings
    SELECT COUNT(host_id) AS non_null_host_ids_in_dim_listings FROM dim_listings WHERE host_id IS NOT NULL;

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    THROW;
END CATCH
'''

            try:
                cursor.execute(move_sql)

                # Fetch the summary from the SELECT statement
                summary = cursor.fetchall()

                # Move to the next result set to get the total dim_listings count
                cursor.nextset()
                total_dim_listings_rows = cursor.fetchone()[0]

                # Move to the next result set to get the non-NULL host_ids count
                cursor.nextset()
                non_null_host_ids_in_dim_listings = cursor.fetchone()[0]

                conn.commit()
            except Exception as e:
                logger.error(f"   ❌ Error moving staging -> dim_listings: {e}")
                conn.rollback()
                return
            
            # Process the summary
            inserted_count = 0
            updated_count = 0
            if summary:
                for row in summary:
                    action = row[0]
                    count = row[1]
                    if action == 'INSERT':
                        inserted_count = count
                    elif action == 'UPDATE':
                        updated_count = count
            
            logger.info(f"   ✅ Loaded: {os.path.basename(file_path)} - Listings added: {inserted_count:,}, Listings updated: {updated_count:,}")
            logger.info(f"   INFO: Total rows in dim_listings after merge: {total_dim_listings_rows:,}")
            logger.info(f"   INFO: Non-NULL host_ids in dim_listings after merge: {non_null_host_ids_in_dim_listings:,}")

        except Exception as e:
            logger.error(f"   ❌ Error processing file {file_path}: {e}")
            self.consecutive_errors += 1

    def _load_calendar_data(self, conn, file_path: str):
        logger.info(f"   ↳ Loading calendar file: {os.path.basename(file_path)}")
        temp_file_path: Optional[str] = None
        cursor = conn.cursor()

        try:
            # Create a temporary file to store the uncompressed calendar data
            with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as temp_file:
                temp_file_path = temp_file.name
                with gzip.open(file_path, 'rb') as f_in:
                    shutil.copyfileobj(f_in, temp_file)
            
            # Read the SQL script
            with open('sql/data/04_load_calendar.sql', 'r', encoding='utf-8-sig') as f:
                sql_script = f.read()

            # Replace the placeholder with the actual file path
            sql_script = sql_script.replace('{{CALENDAR_FILE_PATH}}', temp_file_path.replace('\\', '\\\\'))

            # Execute the SQL script
            cursor.execute(sql_script)

            # Fetch the inserted row count
            inserted_count = 0
            try:
                while True:
                    desc = cursor.description
                    if desc and len(desc) == 1 and desc[0][0].lower() == 'inserted_calendar_rows':
                        row = cursor.fetchone()
                        if row:
                            inserted_count = int(row[0])
                        break
                    if not cursor.nextset():
                        break
            except Exception as e:
                logger.warning(f"   ⚠️ Could not retrieve exact insert count for calendar: {e}")

            conn.commit()
            logger.info(f"   ✅ Loaded: {os.path.basename(file_path)} - Calendar records added: {inserted_count:,}")

        except Exception as e:
            logger.error(f"   ❌ Error processing calendar file {file_path}: {e}")
            self.consecutive_errors += 1
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            # Clean up the temporary file
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except Exception as e:
                    logger.warning(f"   ⚠️ Could not remove temp file {temp_file_path}: {e}")

    def _load_reviews_data(self, conn, file_path: str):
        logger.info(f"   ↳ Loading reviews file: {os.path.basename(file_path)}")
        temp_file_path: Optional[str] = None
        
        try:
            df = pd.read_csv(gzip.open(file_path, 'rt', encoding='utf-8'), sep='|', engine='python')

            original_rows = len(df)
            if original_rows > 200000:
                capped_rows = int(original_rows * 0.8)
                df = df.sample(n=capped_rows, random_state=42)
                logger.info(f"   ⚠️ Capped reviews from {original_rows:,} to {len(df):,} (80%) for file: {os.path.basename(file_path)}")

            df['listing_id'] = pd.to_numeric(df['listing_id'], errors='coerce').astype('Int64')
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
            df['reviewer_name'] = df['reviewer_name'].str.slice(0, 255)
            df['comments'] = df['comments'].str.slice(0, 4000)

            temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8', newline='')
            temp_file_path = temp_file.name
            df.to_csv(temp_file_path, sep='|', index=False, header=False, na_rep='')
            temp_file.close()

            with open('sql/data/05_load_reviews.sql', 'r', encoding='utf-8-sig') as f:
                sql_script = f.read()
            sql_script = 'SET NOCOUNT ON;\n' + sql_script.replace('{{REVIEWS_FILE_PATH}}', temp_file_path.replace('\\', '\\\\'))

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    cursor = conn.cursor()
                    cursor.execute(sql_script)
                    inserted_count = 0
                    while True:
                        desc = cursor.description
                        if desc and len(desc) == 1 and desc[0][0].lower() == 'inserted_review_rows':
                            row = cursor.fetchone()
                            if row:
                                inserted_count = int(row[0])
                            break
                        if not cursor.nextset():
                            break
                    conn.commit()
                    logger.info(f"   ✅ Loaded: {os.path.basename(file_path)} - New reviews added: {inserted_count:,}")
                    break  # Success, exit retry loop
                except Exception as e:
                    if self._is_connection_error(e) and attempt < max_retries - 1:
                        logger.warning(f"   ⚠️ Connection error on attempt {attempt + 1} for {os.path.basename(file_path)}. Retrying...")
                        conn = self._reconnect(conn)
                    else:
                        logger.warning(f"   ⚠️ Attempt {attempt + 1} failed for {os.path.basename(file_path)}: {e}")
                        conn.rollback()
                        if attempt + 1 == max_retries:
                            logger.error(f"   ❌ All attempts failed for {os.path.basename(file_path)}.")
                            raise
                        else:
                            logger.info(f"   Retrying...")
        except Exception as e:
            logger.error(f"   ❌ Error loading reviews: {e}")
            conn.rollback()
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                os.remove(temp_file_path)

    def _ensure_dim_dates(self, conn, min_date: date, max_date: date):
        logger.info(f"   ↳ Ensuring dim_dates for range: {min_date} -> {max_date}")
        cursor = conn.cursor()
        try:
            # collect existing dates
            cursor.execute('SELECT full_date FROM dim_dates WHERE full_date BETWEEN ? AND ?', (min_date, max_date))
            existing = {r[0] for r in cursor.fetchall()}

            # generate all dates in range
            all_dates = []
            cur_date = min_date
            while cur_date <= max_date:
                if cur_date not in existing:
                    all_dates.append(cur_date)
                cur_date = cur_date + timedelta(days=1)

            if all_dates:
                insert_sql = 'INSERT INTO dim_dates (full_date, year, quarter, month, month_name, day, day_name, is_weekend) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
                params = []
                for d in all_dates:
                    y = d.year
                    m = d.month
                    q = (m - 1) // 3 + 1
                    month_name = calendar.month_name[m]
                    day = d.day
                    day_name = calendar.day_name[d.weekday()]
                    is_weekend = 1 if d.weekday() >= 5 else 0
                    params.append((d, y, q, m, month_name, day, day_name, is_weekend))
                
                cursor.fast_executemany = True
                cursor.executemany(insert_sql, params)
                conn.commit()
                logger.info(f"   ✅ Inserted missing dates: {len(all_dates)}")
            else:
                logger.info("   ✅ No missing dates to insert.")

        except Exception as e:
            logger.error(f"   ❌ Error ensuring dim_dates: {e}")
            conn.rollback()

    def _execute_schema_scripts(self, conn):
        schema_files = ['sql/schema/01_drop_tables.sql', 'sql/schema/02_create_tables.sql']
        for s in schema_files:
            self._execute_sql_file(conn, s)

    def create_views(self, conn):
        self._execute_sql_file(conn, 'sql/schema/03_create_views.sql')

    def _execute_sql_file(self, conn, script_path: str):
        if not os.path.exists(script_path):
            logger.warning(f"   ⚠️  SQL script not found: {script_path}")
            return
        try:
            with open(script_path, 'r', encoding='utf-8-sig') as f:
                sql_script = f.read()

            # Remove BOM, if it exists
            if sql_script.startswith('\ufeff'):
                sql_script = sql_script[1:]

            # Split the script by 'GO' to handle batches
            go_batches = sql_script.split('GO')

            cursor = conn.cursor()
            for batch in go_batches:
                if not batch.strip():
                    continue
                # Further split each batch by ';' for individual statements
                statements = [statement for statement in batch.split(';') if statement.strip()]
                for statement in statements:
                    try:
                        cursor.execute(statement)
                    except Exception as e:
                        # Ignore errors on DROP statements
                        if "DROP TABLE" in statement.upper() or "DROP VIEW" in statement.upper():
                            logger.warning(f"   ⚠️  Ignoring error on DROP statement: {e}")
                        else:
                            raise e

            conn.commit()
            logger.info(f"   ✅ Executed: {os.path.basename(script_path)}")
        except Exception as e:
            logger.error(f"   ❌ Error executing {script_path}: {e}")
            conn.rollback()
            raise e

    def _show_database_statistics(self, conn):
        try:
            cursor = conn.cursor()
            tables = ['dim_listings','dim_hosts','dim_dates','fact_calendar','fact_reviews']
            for t in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {t}")
                logger.info(f"{t}: {cursor.fetchone()[0]}")
        except Exception as e:
            logger.error(f"Error showing stats: {e}")
            # no-op: logging the exception above is sufficient