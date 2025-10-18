-- sql/schema/01_drop_tables.sql
USE AirbnbDataWarehouse;

-- Drop tables in the correct order to avoid foreign key constraints
IF OBJECT_ID('fact_reviews', 'U') IS NOT NULL DROP TABLE fact_reviews;
IF OBJECT_ID('fact_calendar', 'U') IS NOT NULL DROP TABLE fact_calendar;
IF OBJECT_ID('dim_listing_id_map', 'U') IS NOT NULL DROP TABLE dim_listing_id_map;
IF OBJECT_ID('dim_listings', 'U') IS NOT NULL DROP TABLE dim_listings;
IF OBJECT_ID('dim_hosts', 'U') IS NOT NULL DROP TABLE dim_hosts;
IF OBJECT_ID('dim_dates', 'U') IS NOT NULL DROP TABLE dim_dates;
IF OBJECT_ID('dim_listings_staging', 'U') IS NOT NULL DROP TABLE dim_listings_staging;
IF OBJECT_ID('fact_calendar_temp', 'U') IS NOT NULL DROP TABLE fact_calendar_temp;
IF OBJECT_ID('dim_listings_staging_archive', 'U') IS NOT NULL DROP TABLE dim_listings_staging_archive;
