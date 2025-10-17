-- sql/schema/04_create_indexes.sql (new file)
USE AirbnbDataWarehouse;

-- Indexes for better query performance
CREATE INDEX IX_dim_listings_host_id ON dim_listings(host_id);
CREATE INDEX IX_dim_listings_location ON dim_listings(property_country, property_city, property_neighbourhood);
CREATE INDEX IX_dim_listings_price ON dim_listings(price);

CREATE INDEX IX_fact_calendar_listing_date ON fact_calendar(listing_id, date_id);
CREATE INDEX IX_fact_calendar_available ON fact_calendar(available);
CREATE INDEX IX_fact_calendar_price ON fact_calendar(price);

CREATE INDEX IX_fact_reviews_listing_date ON fact_reviews(listing_id, date_id);
CREATE INDEX IX_fact_reviews_reviewer ON fact_reviews(reviewer_id);

CREATE INDEX IX_dim_dates_date ON dim_dates(full_date);
CREATE INDEX IX_dim_dates_year_month ON dim_dates(year, month);

PRINT 'âœ… Performance indexes created successfully';
