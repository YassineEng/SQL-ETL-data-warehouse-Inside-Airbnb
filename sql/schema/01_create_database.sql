-- sql/schema/01_create_database.sql
-- Check if database exists, create if not
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'AirbnbDataWarehouse')
BEGIN
    CREATE DATABASE AirbnbDataWarehouse;
    PRINT 'âœ… Database AirbnbDataWarehouse created';
END
ELSE
BEGIN
    PRINT 'â„¹ï¸  Database AirbnbDataWarehouse already exists';
END

USE AirbnbDataWarehouse;
