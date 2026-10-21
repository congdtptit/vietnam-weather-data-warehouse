#!/bin/bash
set -e

echo "=== [PostgreSQL Init] Starting Database Initialization ==="

# 1. Tạo database weather_dwh nếu chưa có
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE weather_dwh'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'weather_dwh')\gexec
    GRANT ALL PRIVILEGES ON DATABASE weather_dwh TO $POSTGRES_USER;
EOSQL

# 2. Thực thi Star Schema DDL vào weather_dwh
if [ -f /docker-entrypoint-initdb.d/schema/init_dwh_schema.sql ]; then
    echo "=== [PostgreSQL Init] Applying Star Schema to weather_dwh ==="
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "weather_dwh" -f /docker-entrypoint-initdb.d/schema/init_dwh_schema.sql
    echo "=== [PostgreSQL Init] Star Schema applied successfully ==="
fi

# 3. Apply numbered migrations in order (02_olap_views.sql, 03_station_daily_rain.sql, ...).
# Each is idempotent and is also applied manually to existing databases.
for migration in /docker-entrypoint-initdb.d/schema/[0-9][0-9]_*.sql; do
    [ -f "$migration" ] || continue
    echo "=== [PostgreSQL Init] Applying $(basename "$migration") to weather_dwh ==="
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "weather_dwh" -f "$migration"
done

echo "=== [PostgreSQL Init] Database setup finished! ==="
