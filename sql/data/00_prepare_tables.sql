-- sql/data/00_prepare_tables.sql
USE AirbnbDataWarehouse;

-- Clear data from referencing tables first to avoid foreign key violations
DELETE FROM fact_reviews;
DELETE FROM fact_calendar;

-- Now clear the dimension tables
DELETE FROM dim_listings;
DELETE FROM dim_hosts;
DELETE FROM dim_dates; -- This might not be necessary if it's a static dimension

PRINT '✅ All staging and dimension tables have been cleared.';