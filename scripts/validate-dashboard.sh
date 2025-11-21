#!/bin/bash
# Dashboard validation - ensures metrics in dashboard match exported collectors

DASHBOARD="grafana/dashboard.json"
ERRORS=0

echo "🔍 Dashboard Validation"
echo "======================="
echo ""

# Step 1: Extract all exported metrics
echo "Step 1: Finding exported metrics..."
grep -rh '"pg_[a-z_0-9]*"' src/collectors --include="*.rs" 2>/dev/null |
    grep -oP '"pg_[a-z_0-9]+"' | sed 's/"//g' >/tmp/metrics.txt

# Find files that use namespace("postgres") and extract their metrics with postgres_ prefix
find src/collectors -name "*.rs" -type f -exec grep -l 'namespace("postgres")' {} \; 2>/dev/null | while read -r file; do
    grep -oP '"pg_[a-z_0-9]+"' "$file" | sed 's/"//g' | sed 's/^/postgres_/'
done >>/tmp/metrics.txt

# Also find any metrics already prefixed with postgres_
grep -rh '"postgres_[a-z_0-9]*"' src/collectors --include="*.rs" 2>/dev/null |
    grep -oP '"postgres_[a-z_0-9]+"' | sed 's/"//g' >>/tmp/metrics.txt

sort -u /tmp/metrics.txt -o /tmp/exported.txt
METRIC_COUNT=$(wc -l </tmp/exported.txt)
echo "  Found: $METRIC_COUNT exported metrics"
echo ""

# Step 2: Extract dashboard metrics
echo "Step 2: Finding dashboard metrics..."
jq -r '.panels[].panels[]?.targets[]?.expr, .panels[].targets[]?.expr' "$DASHBOARD" 2>/dev/null |
    grep -v '^null$' | grep -oP '\b(pg_|postgres_)[a-z_0-9]+' | sort -u >/tmp/dashboard.txt

DASH_COUNT=$(wc -l </tmp/dashboard.txt)
echo "  Found: $DASH_COUNT dashboard metrics"
echo ""

# Step 3: Validate metrics
echo "Step 3: Checking for invalid metrics..."
while IFS= read -r metric; do
    # Direct match - use double quotes for variable, escape $ in pattern
    if grep -q "^${metric}"'$' /tmp/exported.txt; then
        continue
    fi

    # Histogram suffixes (_bucket, _sum, _count)
    for suffix in _bucket _sum _count; do
        if [[ "$metric" == *"$suffix" ]]; then
            base="${metric%"$suffix"}"
            if grep -q "^${base}"'$' /tmp/exported.txt; then
                continue 2
            fi
        fi
    done

    echo "  ❌ $metric"
    ERRORS=$((ERRORS + 1))
done </tmp/dashboard.txt

if [ "$ERRORS" -eq 0 ]; then
    echo "  ✅ All dashboard metrics are valid!"
fi
echo ""

# Step 4: JSON validation
echo "Step 4: Validating JSON..."
if jq '.' "$DASHBOARD" >/dev/null 2>&1; then
    echo "  ✅ JSON is valid"
else
    echo "  ❌ JSON is INVALID"
    ERRORS=$((ERRORS + 1))
fi
echo ""

# Step 5: Variable chain
echo "Step 5: Checking variables..."
jq -e '.templating.list[] | select(.name == "job")' "$DASHBOARD" >/dev/null 2>&1 && echo "  ✅ Job variable exists"
INST=$(jq -r '.templating.list[] | select(.name == "instance") | .query.query' "$DASHBOARD" 2>/dev/null)
echo "$INST" | grep -q 'job="\$job"' && echo "  ✅ Instance depends on job"
DB=$(jq -r '.templating.list[] | select(.name == "database") | .query.query' "$DASHBOARD" 2>/dev/null)
echo "$DB" | grep -q 'job="\$job".*instance="\$instance"\|instance="\$instance".*job="\$job"' && echo "  ✅ Database depends on job+instance"

TOTAL=$(jq -r '.panels[].panels[]?.targets[]?.expr, .panels[].targets[]?.expr' "$DASHBOARD" 2>/dev/null | grep -v '^null$' | wc -l)
WITH_JOB=$(jq -r '.panels[].panels[]?.targets[]?.expr, .panels[].targets[]?.expr' "$DASHBOARD" 2>/dev/null | grep -c 'job="\$job"' || echo 0)
echo "  ✅ $WITH_JOB/$TOTAL queries use job filter"
echo ""

rm -f /tmp/metrics.txt /tmp/exported.txt /tmp/dashboard.txt

echo "======================="
if [ "$ERRORS" -gt 0 ]; then
    echo "❌ FAILED ($ERRORS errors)"
    echo ""
    echo "Run this script to validate dashboard before committing."
    exit 1
else
    echo "✅ PASSED - Dashboard is valid!"
    exit 0
fi
