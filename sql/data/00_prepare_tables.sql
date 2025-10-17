-- sql/data/00_prepare_tables.sql
USE AirbnbDataWarehouse;

-- Clear data from referencing tables first to avoid foreign key violations
TRUNCATE TABLE fact_reviews;
TRUNCATE TABLE fact_calendar;

-- Now clear the dimension tables
DELETE FROM dim_listings;
DELETE FROM dim_hosts;