-- Initialize DDRS Database
-- This file is executed when the PostgreSQL container starts for the first time

-- Create database if it doesn't exist (though Docker already creates it)
-- CREATE DATABASE IF NOT EXISTS ddrs_db;

-- Grant permissions to the user
GRANT ALL PRIVILEGES ON DATABASE ddrs_db TO ddrs_user;

-- You can add any additional database initialization here
-- For example, creating extensions, additional schemas, etc.

-- Example: Enable UUID extension if needed in the future
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Log initialization
SELECT 'DDRS Database initialized successfully' AS status;
