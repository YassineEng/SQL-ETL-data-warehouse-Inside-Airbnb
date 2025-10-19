-- sql/data/02_load_hosts.sql

-- Add this diagnostic query
SELECT COUNT(DISTINCT host_id) AS distinct_hosts_in_dim_listings FROM dim_listings;

-- Extract unique hosts from listings
WITH host_cte AS (
    SELECT
        host_id,
        host_name,
        host_city,
        host_country,
        ROW_NUMBER() OVER(PARTITION BY host_id ORDER BY updated_date DESC) as rn
    FROM dim_listings
    WHERE host_id IS NOT NULL
)
INSERT INTO dim_hosts (host_id, host_name, host_city, host_country, total_listings)
SELECT
    h.host_id,
    h.host_name,
    h.host_city,
    h.host_country,
    l.total_listings
FROM host_cte h
JOIN (
    SELECT
        host_id,
        COUNT(*) as total_listings
    FROM dim_listings
    WHERE host_id IS NOT NULL
    GROUP BY host_id
) l ON h.host_id = l.host_id
WHERE h.rn = 1;

SELECT @@ROWCOUNT AS inserted_hosts_count;