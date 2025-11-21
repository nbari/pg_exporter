#!/bin/bash
# Initialize PostgreSQL with SSL configuration and test data

set -e

echo "🔧 Initializing PostgreSQL with SSL configuration..."

# Wait for PostgreSQL to be ready
until pg_isready -U postgres; do
  echo "⏳ Waiting for PostgreSQL to be ready..."
  sleep 1
done

# Create test database and extensions
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Enable pg_stat_statements for testing
    CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

    -- Create test table for connection testing
    CREATE TABLE IF NOT EXISTS ssl_test (
        id SERIAL PRIMARY KEY,
        test_data TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    );

    -- Insert test data
    INSERT INTO ssl_test (test_data) VALUES
        ('Test data 1'),
        ('Test data 2'),
        ('Test data 3');

    -- Display SSL configuration
    SELECT name, setting
    FROM pg_settings
    WHERE name LIKE 'ssl%'
    ORDER BY name;
EOSQL

echo "✅ PostgreSQL initialization complete!"
echo ""
echo "SSL Status:"
psql -U postgres -c "SHOW ssl;"
echo ""
echo "SSL Configuration:"
psql -U postgres -c "SELECT name, setting FROM pg_settings WHERE name LIKE 'ssl%' ORDER BY name;"
