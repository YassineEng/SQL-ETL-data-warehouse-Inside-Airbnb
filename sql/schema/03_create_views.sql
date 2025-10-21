USE AirbnbDataWarehouse;
GO

-- Drop views if they exist
IF OBJECT_ID('dbo.vw_local_foreign_analysis', 'V') IS NOT NULL 
    DROP VIEW dbo.vw_local_foreign_analysis;
IF OBJECT_ID('dbo.vw_neighborhood_performance', 'V') IS NOT NULL 
    DROP VIEW dbo.vw_neighborhood_performance;
IF OBJECT_ID('dbo.vw_host_activity', 'V') IS NOT NULL 
    DROP VIEW dbo.vw_host_activity;
GO

-- View: Local vs Foreign Host Analysis
CREATE VIEW dbo.vw_local_foreign_analysis AS
SELECT 
    property_country,
    property_city,
    latitude,
    longitude,
    is_local_host,
    COUNT(*) as total_listings,
    AVG(price) as avg_price,
    AVG(review_scores_rating) as avg_rating,
    SUM(number_of_reviews) as total_reviews
FROM dbo.dim_listings
GROUP BY property_country, property_city, latitude, longitude, is_local_host;
GO

-- View: Neighborhood Performance
CREATE VIEW dbo.vw_neighborhood_performance AS
SELECT 
    property_country,
    property_city,
    property_neighbourhood,
    latitude,
    longitude,
    COUNT(*) as listing_count,
    AVG(price) as avg_price,
    AVG(review_scores_rating) as avg_rating,
    AVG(number_of_reviews) as avg_reviews
FROM dbo.dim_listings
GROUP BY property_country, property_city, property_neighbourhood, latitude, longitude;
GO

-- View: Host Activity Summary
CREATE VIEW dbo.vw_host_activity AS
SELECT 
    host_country,
    host_city,
    latitude,
    longitude,
    COUNT(DISTINCT host_id) as unique_hosts,
    COUNT(*) as total_listings,
    AVG(price) as avg_price
FROM dbo.dim_listings
GROUP BY host_country, host_city, latitude, longitude;
GO