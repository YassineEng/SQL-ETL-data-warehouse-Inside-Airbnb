-- sql/data/05_load_reviews.sql
USE AirbnbDataWarehouse;

-- Disable foreign key constraints
ALTER TABLE fact_reviews NOCHECK CONSTRAINT ALL;

-- Create temporary staging table
CREATE TABLE #temp_reviews (
    listing_id NVARCHAR(MAX),
    id NVARCHAR(MAX),
    date NVARCHAR(50),
    reviewer_id NVARCHAR(MAX),
    reviewer_name NVARCHAR(MAX),
    comments NVARCHAR(MAX)
);

-- Dynamic file path (will be replaced by Python)
-- Perform BULK INSERT directly using the provided file path
BULK INSERT #temp_reviews
FROM '{{REVIEWS_FILE_PATH}}'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '|',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    CODEPAGE = '65001'
);

-- Table to hold the IDs of inserted rows
IF OBJECT_ID('tempdb..#inserted_review_ids') IS NOT NULL DROP TABLE #inserted_review_ids;
CREATE TABLE #inserted_review_ids (review_id BIGINT);

-- Insert into fact table with proper data types and date mapping
;WITH reviews_cte AS (
            SELECT
                TRY_CAST(r.id AS BIGINT) AS id,
                TRY_CAST(r.listing_id AS BIGINT) AS listing_id,
                d.date_id,
                        TRY_CAST(r.reviewer_id AS BIGINT) AS reviewer_id,
                        LEFT(r.reviewer_name, 255) AS reviewer_name,
                        r.comments,                ROW_NUMBER() OVER(PARTITION BY r.id ORDER BY (SELECT NULL)) as rn        FROM #temp_reviews r
        INNER JOIN dim_dates d ON TRY_CONVERT(DATE, r.date) = d.full_date
        INNER JOIN dim_listings l ON TRY_CAST(r.listing_id AS BIGINT) = l.listing_id
    )
    INSERT INTO fact_reviews (review_id, listing_id, date_id, reviewer_id, reviewer_name, comments)
    OUTPUT inserted.review_id INTO #inserted_review_ids
    SELECT
        cte.id,
        cte.listing_id,
        cte.date_id,
        cte.reviewer_id,
        cte.reviewer_name,
        cte.comments
    FROM reviews_cte cte
    LEFT JOIN fact_reviews fr ON cte.id = fr.review_id
    WHERE cte.rn = 1 AND fr.review_id IS NULL;
-- Return the count of inserted rows
SELECT COUNT(*) AS inserted_review_rows FROM #inserted_review_ids;

-- Drop temporary table
DROP TABLE #temp_reviews;

-- Re-enable foreign key constraints
ALTER TABLE fact_reviews WITH CHECK CHECK CONSTRAINT ALL;

