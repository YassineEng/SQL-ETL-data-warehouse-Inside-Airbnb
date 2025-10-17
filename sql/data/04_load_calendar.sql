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
CREATE TABLE #inserted_calendar_ids (calendar_id BIGINT);

INSERT INTO fact_calendar (listing_id, date_id, available, price)
OUTPUT inserted.calendar_id INTO #inserted_calendar_ids
SELECT 
    c.listing_id,
    d.date_id,
    CASE
        WHEN LOWER(LTRIM(RTRIM(c.available))) IN ('t','true','1') THEN 1
        WHEN LOWER(LTRIM(RTRIM(c.available))) IN ('f','false','0') THEN 0
        ELSE NULL
    END AS available,
    TRY_CAST(
        REPLACE(
            REPLACE(
                LTRIM(RTRIM(REPLACE(c.price, CHAR(13), ''))),
            '$', ''),
        ',', '') AS DECIMAL(10,2)
    ) AS price
FROM #temp_calendar c
INNER JOIN dim_dates d ON CONVERT(DATE, c.date) = d.full_date
INNER JOIN dim_listings l ON c.listing_id = l.listing_id;

SELECT COUNT(*) AS inserted_calendar_rows FROM #inserted_calendar_ids;

DROP TABLE #temp_calendar;

ALTER TABLE fact_calendar WITH CHECK CHECK CONSTRAINT ALL;



