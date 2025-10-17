-- sql/schema/03_create_views.sql
USE AirbnbDataWarehouse;

-- Drop views if they exist
IF OBJECT_ID('vw_local_foreign_analysis', 'V') IS NOT NULL DROP VIEW vw_local_foreign_analysis;
IF OBJECT_ID('vw_neighborhood_performance', 'V') IS NOT NULL DROP VIEW vw_neighborhood_performance;
IF OBJECT_ID('vw_host_activity', 'V') IS NOT NULL DROP VIEW vw_host_activity;

-- View: Local vs Foreign Host Analysis
CREATE VIEW vw_local_foreign_analysis AS
SELECT 
    property_country,
    property_city,
    is_local_host,
    COUNT(*) as total_listings,
    AVG(price) as avg_price,
    AVG(review_scores_rating) as avg_rating,
    SUM(number_of_reviews) as total_reviews
FROM dim_listings
GROUP BY property_country, property_city, is_local_host;

-- View: Neighborhood Performance
CREATE VIEW vw_neighborhood_performance AS
SELECT 
    property_country,
    property_city, 
    property_neighbourhood,
    COUNT(*) as listing_count,
    AVG(price) as avg_price,
    AVG(review_scores_rating) as avg_rating,
    AVG(number_of_reviews) as avg_reviews
FROM dim_listings
GROUP BY property_country, property_city, property_neighbourhood;

-- View: Host Activity Summary
CREATE VIEW vw_host_activity AS
SELECT 
    host_country,
    host_city,
    COUNT(DISTINCT host_id) as unique_hosts,
    COUNT(*) as total_listings,
    AVG(price) as avg_price
FROM dim_listings
GROUP BY host_country, host_city;

