-- sql/data/05_load_reviews.sql
USE AirbnbDataWarehouse;

-- Disable foreign key constraints
ALTER TABLE fact_reviews NOCHECK CONSTRAINT ALL;

-- Create temporary staging table
CREATE TABLE #temp_reviews (
    listing_id BIGINT,
    id BIGINT,
    date NVARCHAR(50),
    reviewer_id BIGINT,
    reviewer_name NVARCHAR(255),
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
INSERT INTO fact_reviews (review_id, listing_id, date_id, reviewer_id, reviewer_name, comments)
OUTPUT inserted.review_id INTO #inserted_review_ids
SELECT 
    r.id,
    r.listing_id,
    d.date_id,
    r.reviewer_id,
    r.reviewer_name,
    r.comments
FROM #temp_reviews r
INNER JOIN dim_dates d ON CONVERT(DATE, r.date) = d.full_date
INNER JOIN dim_listings l ON r.listing_id = l.listing_id;

-- Return the count of inserted rows
SELECT COUNT(*) AS inserted_review_rows FROM #inserted_review_ids;

-- Drop temporary table
DROP TABLE #temp_reviews;

-- Re-enable foreign key constraints
ALTER TABLE fact_reviews WITH CHECK CHECK CONSTRAINT ALL;

PRINT '✅ Reviews data loaded successfully';
