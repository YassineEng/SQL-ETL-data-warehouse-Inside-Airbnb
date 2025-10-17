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
import tempfile
from typing import Optional
import shutil

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
            self._execute_view_scripts(conn)
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

        # hosts and dates are derived from listings and calendar
        self._execute_sql_file(conn, 'sql/data/02_load_hosts.sql')
        self._execute_sql_file(conn, 'sql/data/03_load_dates.sql')

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
                "INSERT INTO dim_listings_staging (listing_id, host_id, host_name, host_city, host_country, property_country, property_city, property_neighbourhood, price, number_of_reviews, review_scores_rating, calculated_host_listings_count, is_local_host) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
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

            move_sql = '''
SET NOCOUNT ON;
BEGIN TRY
    BEGIN TRANSACTION;

    -- Declare a table variable to store the output of the MERGE statement
    DECLARE @merge_summary TABLE(action VARCHAR(10));

    -- Ensure archive table exists for staging snapshots
    IF OBJECT_ID('dim_listings_staging_archive', 'U') IS NULL
    BEGIN
        CREATE TABLE dim_listings_staging_archive (
            listing_id NVARCHAR(MAX),
            host_id NVARCHAR(MAX),
            host_name NVARCHAR(MAX),
            host_city NVARCHAR(MAX),
            host_country NVARCHAR(MAX),
            property_country NVARCHAR(MAX),
            property_city NVARCHAR(MAX),
            property_neighbourhood NVARCHAR(MAX),
            price NVARCHAR(MAX),
            number_of_reviews NVARCHAR(MAX),
            review_scores_rating NVARCHAR(MAX),
            calculated_host_listings_count NVARCHAR(MAX),
            is_local_host NVARCHAR(MAX),
            archived_at DATETIME2 DEFAULT GETDATE()
        );
    END;

    -- Insert a server-side archive snapshot of staging
    INSERT INTO dim_listings_staging_archive (listing_id, host_id, host_name, host_city, host_country, property_country, property_city, property_neighbourhood, price, number_of_reviews, review_scores_rating, calculated_host_listings_count, is_local_host)
    SELECT listing_id, host_id, host_name, host_city, host_country, property_country, property_city, property_neighbourhood, price, number_of_reviews, review_scores_rating, calculated_host_listings_count, is_local_host FROM dim_listings_staging;

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
            TRY_CAST(REPLACE(REPLACE(price, '$', ''), ',', '') AS DECIMAL(10,2)) AS price,
            TRY_CAST(number_of_reviews AS BIGINT) AS number_of_reviews,
            TRY_CAST(review_scores_rating AS DECIMAL(5,2)) AS review_scores_rating,
            TRY_CAST(calculated_host_listings_count AS BIGINT) AS calculated_host_listings_count,
            CASE WHEN is_local_host = 'True' THEN 1 WHEN is_local_host = 'False' THEN 0 ELSE NULL END AS is_local_host
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
            price = src.price,
            number_of_reviews = src.number_of_reviews,
            review_scores_rating = src.review_scores_rating,
            calculated_host_listings_count = src.calculated_host_listings_count,
            is_local_host = src.is_local_host
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (listing_id, host_id, host_name, host_city, host_country, property_country, property_city, property_neighbourhood, price, number_of_reviews, review_scores_rating, calculated_host_listings_count, is_local_host)
        VALUES (src.listing_id, src.host_id, src.host_name, src.host_city, src.host_country, src.property_country, src.property_city, src.property_neighbourhood, src.price, src.number_of_reviews, src.review_scores_rating, src.calculated_host_listings_count, src.is_local_host)
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

        except Exception as e:
            logger.error(f"   ❌ Error processing file {file_path}: {e}")
            self.consecutive_errors += 1

    def _load_calendar_data(self, conn, file_path: str):
        temp_file_path = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8', newline='') as temp_csv_file:
                temp_file_path = temp_csv_file.name
                df = pd.read_csv(gzip.open(file_path, 'rt', encoding='utf-8'), sep='|', engine='python')
                df.to_csv(temp_csv_file, sep='|', index=False, lineterminator='\n')
            self.data_validator.validate_and_fix_calendar_data(temp_file_path)

            # Ensure dim_dates contains the full date range for this calendar file
            try:
                self._ensure_dim_dates_for_file(conn, temp_file_path)
            except Exception as ed:
                logger.warning(f"   ⚠️ Warning: ensure_dim_dates_for_file failed: {ed}")

            # Copy the temp CSV to a project-controlled logs path so SQL Server can access it reliably
            try:
                stable_path = os.path.join(self.config.LOGS_DIR, f"calendar_temp_for_sql_{pd.Timestamp.now().strftime('%Y%m%dT%H%M%S')}.csv")
                shutil.copyfile(temp_file_path, stable_path)
                logger.info(f"   INFO: calendar CSV copied to: {stable_path}")
            except Exception as e:
                stable_path = temp_file_path
                logger.warning(f"   ⚠️ Warning: failed to copy temp CSV to logs: {e}; using original path {temp_file_path}")

            with open('sql/data/04_load_calendar.sql', 'r', encoding='utf-8-sig') as f:
                sql_script = f.read()
            # Use backslash-escaped path in the SQL so BULK INSERT sees a normal Windows path
            sql_script = sql_script.replace('{{CALENDAR_FILE_PATH}}', stable_path.replace('\\', '\\\\'))

            cursor = conn.cursor()
            # Execute the SQL script; the script will OUTPUT inserted IDs and return
            # a single-row result set named 'inserted_calendar_rows' with the count when present.
            try:
                cursor.execute(sql_script)
            except Exception as e:
                # dump SQL to log for debugging
                try:
                    dbg_path = os.path.join(self.config.LOGS_DIR, f'calendar_sql_error_{pd.Timestamp.now().strftime("%Y%m%dT%H%M%S")}.sql')
                    with open(dbg_path, 'w', encoding='utf-8') as df:
                        df.write(sql_script)
                    logger.error(f"   ❌ Calendar SQL failed. SQL written to: {dbg_path}")
                except Exception:
                    pass
                logger.error(f"   ❌ Error executing calendar SQL: {e}")
                conn.rollback()
                logger.info(f"   ✅ Loaded: {os.path.basename(file_path)} - Calendar records added: 0")
                return

            # scan result sets for a scalar named 'inserted_calendar_rows'
            inserted_count = None
            try:
                while True:
                    desc = cursor.description
                    if desc and len(desc) == 1 and desc[0][0].lower() == 'inserted_calendar_rows':
                        row = cursor.fetchone()
                        if row:
                            inserted_count = int(row[0])
                        break
                    # move to next result set
                    if not cursor.nextset():
                        break
            except Exception:
                pass

            # commit after successful execution
            try:
                conn.commit()
            except Exception:
                conn.rollback()

            if inserted_count is not None:
                logger.info(f"   ✅ Loaded: {os.path.basename(file_path)} - Calendar records added: {inserted_count:,}, Records updated: 0")
            else:
                # fallback: compute by counting difference
                cursor.execute("SELECT COUNT(*) FROM fact_calendar")
                final_rows = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM fact_calendar")
                # We don't have initial_rows here; just print final rows as added
                logger.info(f"   ✅ Loaded: {os.path.basename(file_path)} - Calendar records now: {final_rows:,}")

        finally:
            # clean up original temp file; keep the stable copy in logs for debugging/audit
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except Exception:
                    pass

    def _ensure_dim_dates_for_file(self, conn, csv_path: str):
        """Idempotently ensure dim_dates covers the min/max date found in the given CSV file.

        This reads only the date column with pandas (fast) and inserts missing dates using a parametrized
        recursive CTE executed on the server to avoid transferring large date ranges.
        """
        # read minimal column set to get min/max date
        df = pd.read_csv(csv_path, sep='|', usecols=['date'], parse_dates=['date'], engine='python')
        if df.empty:
            return
        min_date = df['date'].min().date()
        max_date = df['date'].max().date()
        cursor = conn.cursor()
        # Use parameterized SQL that inserts missing dates between min_date and max_date
        ensure_sql = '''
DECLARE @min_date DATE = ?;
DECLARE @max_date DATE = ?;
IF @min_date IS NOT NULL AND @max_date IS NOT NULL
BEGIN
    ;WITH date_range AS (
        SELECT @min_date AS dt
        UNION ALL
        SELECT DATEADD(day, 1, dt) FROM date_range WHERE dt < @max_date
    )
    INSERT INTO dim_dates (full_date, year, quarter, month, month_name, day, day_name, is_weekend)
    SELECT
        dt,
        YEAR(dt) AS year,
        DATEPART(quarter, dt) AS quarter,
        MONTH(dt) AS month,
        DATENAME(month, dt) AS month_name,
        DAY(dt) AS day,
        DATENAME(weekday, dt) AS day_name,
        CASE WHEN DATEPART(weekday, dt) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend
    FROM date_range d
    WHERE NOT EXISTS (SELECT 1 FROM dim_dates dd WHERE dd.full_date = d.dt)
    OPTION (MAXRECURSION 0);
END
'''
        cursor.execute(ensure_sql, (min_date, max_date))
        conn.commit()

    def _load_reviews_data(self, conn, file_path: str):
        temp_file_path = None
        try:
            temp_file = tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv')
            temp_file_path = temp_file.name
            with gzip.open(file_path, 'rb') as f_in:
                for line in f_in:
                    temp_file.write(line)
            temp_file.close()

            with open('sql/data/05_load_reviews.sql', 'r', encoding='utf-8-sig') as f:
                sql_script = f.read()
            sql_script = 'SET NOCOUNT ON;\n' + sql_script.replace('{{REVIEWS_FILE_PATH}}', temp_file_path.replace('\\', '\\\\'))

            cursor = conn.cursor()
            cursor.execute(sql_script)

            inserted_count = 0
            try:
                while True:
                    desc = cursor.description
                    if desc and len(desc) == 1 and desc[0][0].lower() == 'inserted_review_rows':
                        row = cursor.fetchone()
                        if row:
                            inserted_count = int(row[0])
                        break
                    if not cursor.nextset():
                        break
            except Exception as e:
                logger.warning(f"   ⚠️ Could not retrieve exact insert count: {e}")

            conn.commit()
            logger.info(f"   ✅ Loaded: {os.path.basename(file_path)} - Reviews added: {inserted_count:,}, Reviews updated: 0")

        except Exception as e:
            logger.error(f"   ❌ Error loading reviews: {e}")
            conn.rollback()
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                os.remove(temp_file_path)

    def _execute_schema_scripts(self, conn):
        schema_files = ['sql/schema/02_create_tables.sql']
        for s in schema_files:
            self._execute_sql_file(conn, s)

    def _execute_view_scripts(self, conn):
        self._execute_sql_file(conn, 'sql/schema/03_create_views.sql')

    def _execute_sql_file(self, conn, script_path: str):
        if not os.path.exists(script_path):
            logger.warning(f"   ⚠️  SQL script not found: {script_path}")
            return
        try:
            with open(script_path, 'r', encoding='utf-8-sig') as f:
                sql_script = f.read()
            
            # Split the script into individual statements
            statements = [statement for statement in sql_script.split(';') if statement.strip()]
            
            cursor = conn.cursor()
            for statement in statements:
                try:
                    cursor.execute(statement)
                except Exception as e:
                    # Ignore errors on DROP TABLE statements
                    if "DROP TABLE" in statement.upper():
                        logger.warning(f"   ⚠️  Ignoring error on DROP TABLE: {e}")
                    else:
                        raise e
            
            conn.commit()
            logger.info(f"   ✅ Executed: {os.path.basename(script_path)}")
        except Exception as e:
            logger.error(f"   ❌ Error executing {script_path}: {e}")
            conn.rollback()

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
