#!/bin/bash
# Git pre-commit hook to ensure test database is properly configured
# Install: cp scripts/pre-commit-hook.sh .git/hooks/pre-commit

set -e

echo "🔍 Running pre-commit checks..."

# Check if we're modifying collector code
if git diff --cached --name-only | grep -q "src/collectors/"; then
    echo "📊 Collector code changed, verifying test database setup..."

    # Check if PostgreSQL is running
    if ! psql -h localhost -U postgres -d postgres -c "SELECT 1" &>/dev/null 2>&1; then
        echo "⚠️  WARNING: PostgreSQL is not running"
        echo "   Tests may fail. Start with: just postgres"
        echo ""
        read -p "Continue anyway? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    else
        # Check pg_stat_statements
        if ! psql -h localhost -U postgres -d postgres -t -c \
            "SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'" 2>/dev/null | grep -q 1; then
            echo "⚠️  WARNING: pg_stat_statements extension not installed"
            echo "   Run: ./scripts/setup-local-test-db.sh"
            echo ""
            read -p "Continue anyway? (y/N): " -n 1 -r
            echo
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                exit 1
            fi
        fi
    fi
fi

# Check for dangerous patterns in code
echo "🔎 Checking for unsafe patterns..."

UNSAFE_PATTERNS=0

# Check for row.get() without try_get
if git diff --cached | grep -E "^\+.*row\.get\(" | grep -v "try_get" | grep -q .; then
    echo "❌ Found unsafe row.get() usage!"
    echo "   Use row.try_get() instead to handle NULL values safely"
    echo ""
    git diff --cached | grep -E "^\+.*row\.get\(" | grep -v "try_get"
    UNSAFE_PATTERNS=1
fi

# Check for SQL without explicit type casts
if git diff --cached | grep -E "^\+.*FROM pg_stat_statements" | grep -q "SELECT.*FROM pg_stat_statements"; then
    if git diff --cached | grep -E "^\+.*FROM pg_stat_statements" | grep -v "::bigint\|::double precision" | grep -q .; then
        echo "⚠️  Warning: Query to pg_stat_statements may need explicit type casts"
        echo "   Consider adding ::bigint or ::double precision to numeric columns"
        UNSAFE_PATTERNS=1
    fi
fi

if [ "$UNSAFE_PATTERNS" -eq 1 ]; then
    echo ""
    echo "See CONTRIBUTING.md for safe patterns"
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo "✅ Pre-commit checks passed"
