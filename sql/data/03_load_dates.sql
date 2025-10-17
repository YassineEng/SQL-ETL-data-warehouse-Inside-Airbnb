-- sql/data/03_load_dates.sql

USE AirbnbDataWarehouse;

-- Drop foreign key constraints referencing dim_dates
IF OBJECT_ID('FK_fact_calendar_date_id', 'F') IS NOT NULL ALTER TABLE fact_calendar DROP CONSTRAINT FK_fact_calendar_date_id;
IF OBJECT_ID('FK_fact_reviews_date_id', 'F') IS NOT NULL ALTER TABLE fact_reviews DROP CONSTRAINT FK_fact_reviews_date_id;

-- Clear existing data
TRUNCATE TABLE dim_dates;

-- Generate date dimension for 2020-2030
WITH DateRange AS (
    SELECT CAST('2020-01-01' AS DATE) as DateValue
    UNION ALL
    SELECT DATEADD(DAY, 1, DateValue)
    FROM DateRange
    WHERE DateValue < '2030-12-31'
)
INSERT INTO dim_dates (full_date, year, quarter, month, month_name, day, day_name, is_weekend)
SELECT 
    DateValue,
    YEAR(DateValue),
    DATEPART(QUARTER, DateValue),
    MONTH(DateValue),
    DATENAME(MONTH, DateValue),
    DAY(DateValue),
    DATENAME(WEEKDAY, DateValue),
    CASE WHEN DATENAME(WEEKDAY, DateValue) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END
FROM DateRange
OPTION (MAXRECURSION 4000);

-- Re-create foreign key constraints referencing dim_dates
ALTER TABLE fact_calendar ADD CONSTRAINT FK_fact_calendar_date_id FOREIGN KEY (date_id) REFERENCES dim_dates(date_id);
ALTER TABLE fact_reviews ADD CONSTRAINT FK_fact_reviews_date_id FOREIGN KEY (date_id) REFERENCES dim_dates(date_id);

PRINT 'âœ… Date dimension populated successfully';
