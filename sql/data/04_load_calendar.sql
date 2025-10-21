USE AirbnbDataWarehouse;

ALTER TABLE fact_calendar NOCHECK CONSTRAINT ALL;

CREATE TABLE #temp_calendar (
    listing_id BIGINT,
    date NVARCHAR(50),
    available NVARCHAR(10),
    price NVARCHAR(50)
);

BULK INSERT #temp_calendar
FROM '{{CALENDAR_FILE_PATH}}'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '|',
    ROWTERMINATOR = '0x0a'
);


IF OBJECT_ID('tempdb..#inserted_calendar_ids') IS NOT NULL DROP TABLE #inserted_calendar_ids;
CREATE TABLE #inserted_calendar_ids (listing_id BIGINT);

INSERT INTO fact_calendar (listing_id, week_start_date, week_end_date, avg_price_per_week, available_days_per_week)
OUTPUT inserted.listing_id INTO #inserted_calendar_ids -- Outputting listing_id as calendar_id is not available in fact_calendar
SELECT 
    c.listing_id,
    DATEADD(wk, DATEDIFF(wk, 0, CONVERT(DATE, c.date)), 0) AS week_start_date,
    DATEADD(wk, DATEDIFF(wk, 0, CONVERT(DATE, c.date)), 6) AS week_end_date,
    AVG(TRY_CAST(
        REPLACE(
            REPLACE(
                LTRIM(RTRIM(REPLACE(c.price, CHAR(13), ''))),
            '$', ''),
        ',', '') AS DECIMAL(10,2)
    )) AS avg_price_per_week,
    SUM(CASE
        WHEN LOWER(LTRIM(RTRIM(c.available))) IN ('t','true','1') THEN 1
        ELSE 0
    END) AS available_days_per_week
FROM #temp_calendar c
INNER JOIN dim_listings l ON c.listing_id = l.listing_id
GROUP BY 
    c.listing_id,
    DATEADD(wk, DATEDIFF(wk, 0, CONVERT(DATE, c.date)), 0),
    DATEADD(wk, DATEDIFF(wk, 0, CONVERT(DATE, c.date)), 6);

SELECT COUNT(listing_id) AS inserted_calendar_rows FROM #inserted_calendar_ids;

DROP TABLE #temp_calendar;

ALTER TABLE fact_calendar WITH CHECK CHECK CONSTRAINT ALL;



