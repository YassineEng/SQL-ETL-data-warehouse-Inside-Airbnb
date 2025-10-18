-- sql/schema/02_create_tables.sql
USE AirbnbDataWarehouse;

-- Dimension Table: Listings (Properties)
CREATE TABLE dim_listings (
    listing_id BIGINT PRIMARY KEY,
    host_id BIGINT,
    host_name NVARCHAR(255),
    host_city NVARCHAR(255),
    host_country NVARCHAR(100),
    property_country NVARCHAR(100),
    property_city NVARCHAR(255),
    property_neighbourhood NVARCHAR(255),
    price DECIMAL(10,2),
    number_of_reviews BIGINT,
    review_scores_rating DECIMAL(3,2),
    calculated_host_listings_count BIGINT,
    is_local_host BIT,
    created_date DATETIME2 DEFAULT GETDATE(),
    updated_date DATETIME2 DEFAULT GETDATE()
);

-- Mapping table to preserve original raw listing IDs and optional split components.
-- Use a surrogate primary key so we can store raw IDs even when they do not
-- convert to BIGINT. listing_id is nullable and only populated when the
-- raw value can be converted to an existing dim_listings.listing_id.
CREATE TABLE dim_listing_id_map (
    mapping_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    listing_id BIGINT NULL,
    listing_raw_id NVARCHAR(4000),
    part1 NVARCHAR(255),
    part2 NVARCHAR(255),
    part3 NVARCHAR(255),
    created_date DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_dim_listing_id_map_listing FOREIGN KEY (listing_id) REFERENCES dim_listings(listing_id)
);

-- Staging table for Listings
CREATE TABLE dim_listings_staging (
    listing_id NVARCHAR(MAX),
    host_id NVARCHAR(MAX),
    host_name NVARCHAR(MAX),
    host_city NVARCHAR(MAX),
    host_country NVARCHAR(MAX),
    property_country NVARCHAR(MAX),
    property_city NVARCHAR(MAX),
    property_neighbourhood NVARCHAR(MAX),
    price NVARCHAR(MAX),
    number_of_reviews NVARCHAR(MAX),
    review_scores_rating NVARCHAR(MAX),
    calculated_host_listings_count NVARCHAR(MAX),
    is_local_host NVARCHAR(MAX)
);

-- Temporary staging tables
CREATE TABLE fact_calendar_temp (
    listing_id BIGINT,
    date DATE,
    available VARCHAR(10),
    price VARCHAR(50)
);

CREATE TABLE fact_reviews_temp (
    review_id BIGINT,
    listing_id BIGINT,
    date DATE,
    reviewer_id BIGINT,
    reviewer_name NVARCHAR(255),
    comments TEXT
);

-- Dimension Table: Hosts
CREATE TABLE dim_hosts (
    host_id BIGINT PRIMARY KEY,
    host_name NVARCHAR(255),
    host_city NVARCHAR(255),
    host_country NVARCHAR(100),
    total_listings INT,
    created_date DATETIME2 DEFAULT GETDATE()
);

-- Dimension Table: Dates (for calendar data)
CREATE TABLE dim_dates (
    date_id INT IDENTITY(1,1) PRIMARY KEY,
    full_date DATE UNIQUE,
    year INT,
    quarter INT,
    month INT,
    month_name NVARCHAR(20),
    day INT,
    day_name NVARCHAR(20),
    is_weekend BIT
);

-- Fact Table: Calendar (Daily availability and pricing)
CREATE TABLE fact_calendar (
    calendar_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    listing_id BIGINT,
    date_id INT,
    available BIT,
    price DECIMAL(10,2),
    FOREIGN KEY (listing_id) REFERENCES dim_listings(listing_id),
    FOREIGN KEY (date_id) REFERENCES dim_dates(date_id)
);

-- Fact Table: Reviews
CREATE TABLE fact_reviews (
    review_id BIGINT PRIMARY KEY,
    listing_id BIGINT,
    date_id INT,
    reviewer_id BIGINT,
    reviewer_name NVARCHAR(255),
    comments NVARCHAR(MAX),
    FOREIGN KEY (listing_id) REFERENCES dim_listings(listing_id),
    FOREIGN KEY (date_id) REFERENCES dim_dates(date_id)
);

