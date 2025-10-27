#!/bin/bash
set -euo pipefail

# Setup local PostgreSQL for testing pg_exporter
# This script ensures pg_stat_statements is properly configured

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-postgres}"
PG_DATABASE="${PG_DATABASE:-postgres}"
VERBOSE=false
SKIP_DATA_GEN=false
USE_PGBENCH=auto # auto, true, or false
PGBENCH_SCALE=10

log_info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

log_debug() {
    if [ "$VERBOSE" = true ]; then
        echo -e "${BLUE}[DEBUG]${NC} $*"
    fi
}

show_help() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Setup and verify local PostgreSQL for pg_exporter testing.

By default, uses pgbench if available, otherwise uses simple test data.

OPTIONS:
    -h, --help              Show this help message
    -v, --verbose           Enable verbose output
    -s, --skip-data         Skip test data generation
    -b, --pgbench           Force use of pgbench (error if not found)
    --no-pgbench            Force simple test data (skip pgbench even if available)
    --pgbench-scale SCALE   pgbench scale factor (default: 10)
    -H, --host HOST         PostgreSQL host (default: localhost)
    -p, --port PORT         PostgreSQL port (default: 5432)
    -U, --user USER         PostgreSQL user (default: postgres)
    -d, --database DB       PostgreSQL database (default: postgres)

ENVIRONMENT VARIABLES:
    PG_HOST                 PostgreSQL host
    PG_PORT                 PostgreSQL port
    PG_USER                 PostgreSQL user
    PG_DATABASE             PostgreSQL database

EXAMPLES:
    # Auto-detect (use pgbench if available, otherwise simple test data)
    $(basename "$0")

    # Force pgbench usage (error if not available)
    $(basename "$0") --pgbench

    # Force simple test data (skip pgbench)
    $(basename "$0") --no-pgbench

    # Custom pgbench scale (scale 50 = ~750MB database)
    $(basename "$0") --pgbench-scale 50

    # Verbose mode
    $(basename "$0") --verbose

    # Custom host and port
    $(basename "$0") -H 127.0.0.1 -p 5433

    # Skip test data generation (just verify setup)
    $(basename "$0") --skip-data

PGBENCH INFO:
    pgbench generates realistic TPC-B-like workload with:
      • Multiple tables (pgbench_accounts, pgbench_branches, etc.)
      • Diverse query patterns (SELECT, UPDATE, INSERT)
      • Realistic transaction load

    Scale factor determines database size:
      • Scale 1  = ~15MB   (100,000 rows)
      • Scale 10 = ~150MB  (1,000,000 rows) [default]
      • Scale 50 = ~750MB  (5,000,000 rows)
      • Scale 100= ~1.5GB  (10,000,000 rows)

    If pgbench is not installed:
      • Debian/Ubuntu: apt-get install postgresql-contrib
      • RHEL/CentOS:   yum install postgresql-contrib

EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
        -h | --help)
            show_help
            exit 0
            ;;
        -v | --verbose)
            VERBOSE=true
            shift
            ;;
        -s | --skip-data)
            SKIP_DATA_GEN=true
            shift
            ;;
        -b | --pgbench)
            USE_PGBENCH=true
            shift
            ;;
        --no-pgbench)
            USE_PGBENCH=false
            shift
            ;;
        --pgbench-scale)
            PGBENCH_SCALE="$2"
            shift 2
            ;;
        -H | --host)
            PG_HOST="$2"
            shift 2
            ;;
        -p | --port)
            PG_PORT="$2"
            shift 2
            ;;
        -U | --user)
            PG_USER="$2"
            shift 2
            ;;
        -d | --database)
            PG_DATABASE="$2"
            shift 2
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
        esac
    done
}

# Execute psql command with connection parameters
run_psql() {
    PGOPTIONS='--client-min-messages=warning' psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" --no-psqlrc "$@"
}

# Check if PostgreSQL is running
check_postgres() {
    log_info "Checking PostgreSQL connection at $PG_HOST:$PG_PORT..."
    log_debug "Using user: $PG_USER, database: $PG_DATABASE"

    if ! run_psql -c "SELECT 1" &>/dev/null; then
        log_error "Cannot connect to PostgreSQL at $PG_HOST:$PG_PORT"
        log_error ""
        log_error "Troubleshooting:"
        log_error "  • Check if PostgreSQL is running: just postgres"
        log_error "  • Verify connection settings: PG_HOST=$PG_HOST PG_PORT=$PG_PORT"
        log_error "  • Check pg_hba.conf allows connections from localhost"
        exit 1
    fi
    log_info "✓ PostgreSQL is running"
}

# Check PostgreSQL version
check_version() {
    log_info "Checking PostgreSQL version..."

    local version
    version=$(run_psql -t -A -c "SHOW server_version;")
    log_info "✓ PostgreSQL version: $version"

    # Extract major version
    local major_version
    major_version=$(echo "$version" | cut -d. -f1)

    if [ "$major_version" -lt 13 ]; then
        log_warn "PostgreSQL version < 13 detected"
        log_warn "Some metrics (wal_bytes) won't be available"
    fi
}

# Verify extension is loaded
verify_extension() {
    log_info "Verifying pg_stat_statements is preloaded..."

    local preload
    preload=$(run_psql -t -A -c "SHOW shared_preload_libraries;")
    log_debug "shared_preload_libraries: $preload"

    if [[ ! "$preload" =~ pg_stat_statements ]]; then
        log_error "pg_stat_statements is NOT in shared_preload_libraries"
        log_error "Current value: $preload"
        log_error ""
        log_error "Fix this by:"
        log_error "  1. Stop PostgreSQL: just stop-containers"
        log_error "  2. Verify db/config/postgres/postgresql.conf contains:"
        log_error "     shared_preload_libraries = 'pg_stat_statements'"
        log_error "  3. Start PostgreSQL: just postgres"
        exit 1
    fi

    log_info "✓ pg_stat_statements is preloaded"
}

# Check pg_stat_statements extension
check_extension() {
    log_info "Checking pg_stat_statements extension..."

    local ext_exists
    ext_exists=$(run_psql -t -A -c \
        "SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'")

    if [ "$ext_exists" = "" ]; then
        log_warn "pg_stat_statements extension not found"
        log_info "Creating extension..."
        run_psql -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;" >/dev/null
        log_info "✓ Extension created"
    else
        log_info "✓ pg_stat_statements extension exists"
    fi
}

# Check if extension is actually working
test_extension() {
    log_info "Testing pg_stat_statements functionality..."

    # Reset stats
    run_psql -c "SELECT pg_stat_statements_reset();" &>/dev/null

    # Generate a test query
    run_psql -c "SELECT 1 AS test_query;" &>/dev/null

    # Check if query was captured
    local query_count
    query_count=$(run_psql -t -A -c \
        "SELECT COUNT(*) FROM pg_stat_statements WHERE query LIKE '%test_query%'")

    if [ "$query_count" -eq "0" ]; then
        log_error "pg_stat_statements is not capturing queries!"
        log_error "Check: SHOW pg_stat_statements.track;"
        log_error "Should be 'all' or 'top'"
        exit 1
    fi

    log_info "✓ pg_stat_statements is capturing queries"
    log_debug "Found $query_count test queries"
}

# Generate test data with pgbench
generate_pgbench_data() {
    log_info "Generating realistic workload with pgbench (scale=$PGBENCH_SCALE)..."

    # Check if pgbench is available
    if ! command -v pgbench &>/dev/null; then
        if [ "$USE_PGBENCH" = "true" ]; then
            # Explicitly requested pgbench but not found
            log_error "pgbench not found in PATH"
            log_error "Install: apt-get install postgresql-contrib (Debian/Ubuntu)"
            log_error "        yum install postgresql-contrib (RHEL/CentOS)"
            log_error "Or use --no-pgbench to skip pgbench and use simple test data"
            exit 1
        else
            # Auto mode: fall back to simple test data
            log_warn "pgbench not found, falling back to simple test data"
            log_debug "Install postgresql-contrib to enable pgbench"
            generate_simple_test_data
            return
        fi
    fi

    # Create pgbench database if it doesn't exist
    local db_exists
    db_exists=$(run_psql -t -A -c "SELECT 1 FROM pg_database WHERE datname = 'pgbench_test'")

    if [ "$db_exists" = "" ]; then
        log_debug "Creating pgbench_test database..."
        run_psql -c "CREATE DATABASE pgbench_test;" >/dev/null
    fi

    # Ensure pg_stat_statements extension exists in pgbench_test
    log_debug "Ensuring pg_stat_statements extension in pgbench_test..."
    PGOPTIONS='--client-min-messages=warning' psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d pgbench_test --no-psqlrc -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;" >/dev/null

    # Initialize pgbench schema
    log_info "Initializing pgbench schema (this may take a moment)..."
    if [ "$VERBOSE" = true ]; then
        pgbench -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -i -s "$PGBENCH_SCALE" pgbench_test
    else
        pgbench -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -i -s "$PGBENCH_SCALE" pgbench_test -q
    fi

    # Reset stats for clean baseline
    PGOPTIONS='--client-min-messages=warning' psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d pgbench_test --no-psqlrc -c "SELECT pg_stat_statements_reset();" >/dev/null

    # Run a short benchmark to generate query stats
    log_info "Running pgbench workload to populate pg_stat_statements..."
    local duration=10
    local clients=5

    if [ "$VERBOSE" = true ]; then
        log_debug "Running: pgbench -c$clients -T$duration (10 seconds)"
        pgbench -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -c "$clients" -T "$duration" pgbench_test
    else
        pgbench -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -c "$clients" -T "$duration" pgbench_test 2>&1 | grep -E "^(tps|number of)"
    fi

    log_info "✓ pgbench data generated"

    # Show pgbench stats
    if [ "$VERBOSE" = true ]; then
        log_debug "pgbench database stats:"
        PGOPTIONS='--client-min-messages=warning' psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d pgbench_test --no-psqlrc -c "
            SELECT
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
            FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
        "
    fi
}

# Generate simple test data
generate_simple_test_data() {
    log_info "Generating simple test data..."

    run_psql -q <<'SQL'
-- Reset stats
SELECT pg_stat_statements_reset();

-- Generate diverse queries
SELECT 1;
SELECT 2;
SELECT NOW();
SELECT COUNT(*) FROM pg_stat_statements;

-- Create test table
DROP TABLE IF EXISTS pg_exporter_test;
CREATE TEMP TABLE pg_exporter_test (id SERIAL PRIMARY KEY, data TEXT);

-- Generate INSERT queries
INSERT INTO pg_exporter_test (data) SELECT 'test_' || i FROM generate_series(1, 100) i;

-- Generate SELECT queries
SELECT * FROM pg_exporter_test WHERE id > 50;
SELECT COUNT(*) FROM pg_exporter_test;

-- Generate UPDATE queries
UPDATE pg_exporter_test SET data = 'updated' WHERE id < 10;

-- Utility statements (these can have NULL query text)
VACUUM;
ANALYZE;
SQL

    log_info "✓ Simple test data generated"
}

# Generate test data
generate_test_data() {
    if [ "$SKIP_DATA_GEN" = true ]; then
        log_info "Skipping test data generation (--skip-data)"
        return
    fi

    # Decide which method to use
    local use_pgbench_final="$USE_PGBENCH"

    if [ "$USE_PGBENCH" = "auto" ]; then
        # Auto-detect: use pgbench if available
        if command -v pgbench &>/dev/null; then
            use_pgbench_final="true"
            log_debug "pgbench detected, using realistic workload"
        else
            use_pgbench_final="false"
            log_debug "pgbench not found, using simple test data"
        fi
    fi

    if [ "$use_pgbench_final" = "true" ]; then
        generate_pgbench_data
    else
        generate_simple_test_data
    fi
}

# Display stats
show_stats() {
    if [ "$SKIP_DATA_GEN" = true ]; then
        return
    fi

    local target_db="$PG_DATABASE"
    if [ "$USE_PGBENCH" = true ]; then
        target_db="pgbench_test"
    fi

    log_info "Current pg_stat_statements statistics (database: $target_db):"
    echo ""

    PGOPTIONS='--client-min-messages=warning' psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$target_db" --no-psqlrc -c "
        SELECT
            COUNT(*) as total_queries,
            COUNT(*) FILTER (WHERE query IS NULL) as null_queries,
            COUNT(*) FILTER (WHERE query IS NOT NULL) as tracked_queries
        FROM pg_stat_statements;
    "

    echo ""
    log_info "Top 5 queries by execution time:"
    echo ""

    PGOPTIONS='--client-min-messages=warning' psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$target_db" --no-psqlrc -c "
        SELECT
            LEFT(query, 60) as query_sample,
            calls,
            ROUND(total_exec_time::numeric, 2) as total_ms
        FROM pg_stat_statements
        WHERE queryid IS NOT NULL
        ORDER BY total_exec_time DESC
        LIMIT 5;
    "
}

# Main execution
main() {
    parse_args "$@"

    log_info "=== pg_exporter Test Database Setup ==="
    if [ "$VERBOSE" = true ]; then
        log_debug "Connection: $PG_USER@$PG_HOST:$PG_PORT/$PG_DATABASE"
    fi
    echo ""

    check_postgres
    check_version
    verify_extension
    check_extension
    test_extension
    generate_test_data
    show_stats

    echo ""
    log_info "=== Setup Complete ==="
    log_info "Database is ready for testing"
    echo ""
    log_info "Next steps:"
    log_info "  • Run tests: just test"
    log_info "  • Or manually: PG_EXPORTER_DSN='postgresql://postgres@localhost/postgres' cargo test"
    log_info "  • Run exporter: cargo run -- --collector.statements -v"
}

main "$@"
