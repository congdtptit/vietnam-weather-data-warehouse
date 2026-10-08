-- ==============================================================================
-- 01_init_databases.sql: Khởi tạo database Data Warehouse
-- ==============================================================================

-- Tạo database Data Warehouse (weather_dwh) nếu chưa tồn tại
SELECT 'CREATE DATABASE weather_dwh'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'weather_dwh')\gexec

GRANT ALL PRIVILEGES ON DATABASE weather_dwh TO postgres;
