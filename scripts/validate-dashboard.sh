#!/usr/bin/env bash
# Dashboard validation - ensures metrics in dashboard match exported collectors

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
COLLECTORS_DIR="${REPO_ROOT}/src/collectors"
DASHBOARD="${REPO_ROOT}/grafana/dashboard.json"

if [[ ! -d "${COLLECTORS_DIR}" ]]; then
    echo "❌ Collectors directory not found: ${COLLECTORS_DIR}" >&2
    exit 1
fi

if [[ ! -f "${DASHBOARD}" ]]; then
    echo "❌ Dashboard file not found: ${DASHBOARD}" >&2
    exit 1
fi

for cmd in jq grep sort wc find mktemp; do
    if ! command -v "${cmd}" >/dev/null 2>&1; then
        echo "❌ Missing required command: ${cmd}" >&2
        exit 1
    fi
done

TMP_METRICS="$(mktemp)"
TMP_EXPORTED="$(mktemp)"
TMP_DASHBOARD="$(mktemp)"
trap 'rm -f "${TMP_METRICS}" "${TMP_EXPORTED}" "${TMP_DASHBOARD}"' EXIT

ERRORS=0

echo "🔍 Dashboard Validation"
echo "======================="
echo ""

# Step 1: Extract all exported metrics
echo "Step 1: Finding exported metrics..."
grep -rh '"pg_[a-z_0-9]*"' "${COLLECTORS_DIR}" --include="*.rs" 2>/dev/null |
    grep -oE '"pg_[a-z_0-9]+"' | sed 's/"//g' >"${TMP_METRICS}"

# Find files that use namespace("postgres") and extract their metrics with postgres_ prefix
find "${COLLECTORS_DIR}" -name "*.rs" -type f -exec grep -l 'namespace("postgres")' {} \; 2>/dev/null | while read -r file; do
    grep -oE '"pg_[a-z_0-9]+"' "$file" | sed 's/"//g' | sed 's/^/postgres_/'
done >>"${TMP_METRICS}"

# Also find any metrics already prefixed with postgres_
grep -rh '"postgres_[a-z_0-9]*"' "${COLLECTORS_DIR}" --include="*.rs" 2>/dev/null |
    grep -oE '"postgres_[a-z_0-9]+"' | sed 's/"//g' >>"${TMP_METRICS}"

sort -u "${TMP_METRICS}" -o "${TMP_EXPORTED}"
METRIC_COUNT=$(wc -l <"${TMP_EXPORTED}")
echo "  Found: $METRIC_COUNT exported metrics"
echo ""

# Step 2: Extract dashboard metrics
echo "Step 2: Finding dashboard metrics..."
jq -r '.panels[].panels[]?.targets[]?.expr, .panels[].targets[]?.expr' "$DASHBOARD" 2>/dev/null |
    grep -v '^null$' | grep -oE '(pg_|postgres_)[a-z_0-9]+' | sort -u >"${TMP_DASHBOARD}"

DASH_COUNT=$(wc -l <"${TMP_DASHBOARD}")
echo "  Found: $DASH_COUNT dashboard metrics"
echo ""

# Step 3: Validate metrics
echo "Step 3: Checking for invalid metrics..."
while IFS= read -r metric; do
    # Direct match - use double quotes for variable, escape $ in pattern
    if grep -q "^${metric}"'$' "${TMP_EXPORTED}"; then
        continue
    fi

    # Histogram suffixes (_bucket, _sum, _count)
    for suffix in _bucket _sum _count; do
        if [[ "$metric" == *"$suffix" ]]; then
            base="${metric%"$suffix"}"
            if grep -q "^${base}"'$' "${TMP_EXPORTED}"; then
                continue 2
            fi
        fi
    done

    echo "  ❌ $metric"
    ERRORS=$((ERRORS + 1))
done <"${TMP_DASHBOARD}"

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
WITH_JOB=$(jq -r '.panels[].panels[]?.targets[]?.expr, .panels[].targets[]?.expr' "$DASHBOARD" 2>/dev/null | grep -c 'job="\$job"' || true)
WITH_JOB=${WITH_JOB:-0}
echo "  ✅ $WITH_JOB/$TOTAL queries use job filter"
echo ""

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
