-- sql/data/02_load_hosts.sql
USE AirbnbDataWarehouse;

-- Clear existing data
TRUNCATE TABLE dim_hosts;

-- Extract unique hosts from listings
INSERT INTO dim_hosts (host_id, host_name, host_city, host_country, total_listings)
SELECT 
    host_id,
    host_name,
    host_city, 
    host_country,
    COUNT(*) as total_listings
FROM dim_listings
GROUP BY host_id, host_name, host_city, host_country;

PRINT '✅ Hosts data loaded successfully';