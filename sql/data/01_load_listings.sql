USE AirbnbDataWarehouse;

BEGIN TRY
    BEGIN TRANSACTION;
    
    -- Clear existing data in staging table
    TRUNCATE TABLE dim_listings_staging;

    -- Bulk insert into staging table
    BULK INSERT dim_listings_staging
    FROM '{{LISTINGS_FILE_PATH}}'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = '|',
        ROWTERMINATOR = '0x0a'
    );

    -- Debug: show staging row count
    PRINT 'DEBUG: dim_listings_staging row count:';
    SELECT COUNT(*) AS staging_rows FROM dim_listings_staging;

    -- Insert into dim_listings with data transformations
    INSERT INTO dim_listings (
        listing_id, host_id, host_name, host_city, host_country,
        property_country, property_city, property_neighbourhood,
        price, number_of_reviews, review_scores_rating,
        calculated_host_listings_count, is_local_host
    )
    SELECT
        TRY_CAST(listing_id AS BIGINT),
        TRY_CAST(host_id AS BIGINT),
        host_name,
        host_city,
        host_country,
        property_country,
        property_city,
        property_neighbourhood,
        TRY_CAST(REPLACE(REPLACE(price, '$', ''), ',', '') AS DECIMAL(10,2)),
    TRY_CAST(number_of_reviews AS BIGINT),
        TRY_CAST(review_scores_rating AS DECIMAL(3,2)),
    TRY_CAST(calculated_host_listings_count AS BIGINT),
        CASE WHEN is_local_host = 'True' THEN 1 WHEN is_local_host = 'False' THEN 0 ELSE NULL END
    FROM dim_listings_staging;
    
    -- Clear staging table
    TRUNCATE TABLE dim_listings_staging;

    USE AirbnbDataWarehouse;

    BEGIN TRY
        BEGIN TRANSACTION;
    
        -- Clear existing data in staging table
        TRUNCATE TABLE dim_listings_staging;

        -- Bulk insert into staging table
        BULK INSERT dim_listings_staging
        FROM '{{LISTINGS_FILE_PATH}}'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = '|',
            ROWTERMINATOR = '0x0a'
        );

        -- Debug: show staging row count
        PRINT 'DEBUG: dim_listings_staging row count:';
        SELECT COUNT(*) AS staging_rows FROM dim_listings_staging;

        -- Insert into dim_listings with data transformations
        INSERT INTO dim_listings (
            listing_id, host_id, host_name, host_city, host_country,
            property_country, property_city, property_neighbourhood,
            price, number_of_reviews, review_scores_rating,
            calculated_host_listings_count, is_local_host
        )
        SELECT
            TRY_CAST(listing_id AS BIGINT),
            TRY_CAST(host_id AS BIGINT),
            host_name,
            host_city,
            host_country,
            property_country,
            property_city,
            property_neighbourhood,
            TRY_CAST(REPLACE(REPLACE(price, '$', ''), ',', '') AS DECIMAL(10,2)),
        TRY_CAST(number_of_reviews AS BIGINT),
            TRY_CAST(review_scores_rating AS DECIMAL(3,2)),
        TRY_CAST(calculated_host_listings_count AS BIGINT),
            CASE WHEN is_local_host = 'True' THEN 1 WHEN is_local_host = 'False' THEN 0 ELSE NULL END
        FROM dim_listings_staging;
    
        -- Clear staging table
        TRUNCATE TABLE dim_listings_staging;

        COMMIT TRANSACTION;
        PRINT '✅ Listings data loaded successfully';
    
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        PRINT '❌ Error loading listings data: ' + ERROR_MESSAGE();
        THROW;
    END CATCH