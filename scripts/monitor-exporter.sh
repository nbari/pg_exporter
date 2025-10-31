#!/usr/bin/env bash
#
# monitor-exporter.sh - Monitor CPU and memory usage of pg_exporter
#
# Usage:
#   ./scripts/monitor-exporter.sh [OPTIONS]
#
# Options:
#   -i, --interval SECONDS   Monitoring interval (default: 5)
#   -d, --duration SECONDS   Total duration to monitor (default: infinite)
#   -o, --output FILE        Write output to file (default: stdout)
#   -p, --pid PID            Monitor specific PID (default: find pg_exporter)
#   -j, --json               Output in JSON format
#   -h, --help               Show this help message
#
# Examples:
#   # Monitor pg_exporter for 60 seconds, 5-second intervals
#   ./scripts/monitor-exporter.sh -d 60
#
#   # Monitor specific PID, output to file
#   ./scripts/monitor-exporter.sh -p 12345 -o /tmp/pg_exporter_metrics.log
#
#   # Monitor with JSON output for parsing
#   ./scripts/monitor-exporter.sh -j -d 120 -o metrics.json

set -euo pipefail

# Default values
INTERVAL=5
DURATION=0 # 0 = infinite
OUTPUT=""
PID=""
JSON_OUTPUT=false

# Colors for terminal output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
    -i | --interval)
        INTERVAL="$2"
        shift 2
        ;;
    -d | --duration)
        DURATION="$2"
        shift 2
        ;;
    -o | --output)
        OUTPUT="$2"
        shift 2
        ;;
    -p | --pid)
        PID="$2"
        shift 2
        ;;
    -j | --json)
        JSON_OUTPUT=true
        shift
        ;;
    -h | --help)
        grep '^#' "$0" | sed 's/^# \?//' | head -n 20
        exit 0
        ;;
    *)
        echo "Unknown option: $1"
        echo "Use --help for usage information"
        exit 1
        ;;
    esac
done

# Function to find pg_exporter PID
find_pg_exporter_pid() {
    local pids
    pids=$(pgrep -f 'pg_exporter|target.*pg_exporter' 2>/dev/null || true)

    if [[ -z "$pids" ]]; then
        echo "Error: pg_exporter process not found" >&2
        echo "Hint: Start pg_exporter first or specify PID with -p" >&2
        exit 1
    fi

    # If multiple PIDs, take the first one
    echo "$pids" | head -n 1
}

# Get PID if not provided
if [[ -z "$PID" ]]; then
    PID=$(find_pg_exporter_pid)
    echo -e "${GREEN}Found pg_exporter with PID: ${PID}${NC}" >&2
fi

# Verify PID exists
if ! kill -0 "$PID" 2>/dev/null; then
    echo "Error: Process with PID $PID not found or not accessible" >&2
    exit 1
fi

# Get process info
PROCESS_CMD=$(ps -p "$PID" -o cmd= 2>/dev/null || echo "unknown")
echo -e "${BLUE}Monitoring: ${PROCESS_CMD}${NC}" >&2
echo -e "${BLUE}Interval: ${INTERVAL}s, Duration: ${DURATION}s (0=infinite)${NC}" >&2

# Setup output
if [[ -n "$OUTPUT" ]]; then
    # Redirect stdout to file
    exec >"$OUTPUT"
    echo -e "${GREEN}Writing output to: ${OUTPUT}${NC}" >&2
fi

# Print header (non-JSON mode)
if [[ "$JSON_OUTPUT" == "false" ]]; then
    printf "%-20s %8s %8s %10s %10s %10s %10s %8s\n" \
        "TIMESTAMP" "CPU%" "MEM%" "RSS(MB)" "VSZ(MB)" "THREADS" "FD_COUNT" "UPTIME"
    printf "%-20s %8s %8s %10s %10s %10s %10s %8s\n" \
        "---" "---" "---" "---" "---" "---" "---" "---"
fi

# Monitoring loop
START_TIME=$(date +%s)
ITERATION=0

while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))

    # Check duration limit
    if [[ $DURATION -gt 0 ]] && [[ $ELAPSED -ge $DURATION ]]; then
        echo -e "\n${GREEN}Monitoring duration completed${NC}" >&2
        break
    fi

    # Check if process still exists
    if ! kill -0 "$PID" 2>/dev/null; then
        echo -e "\n${RED}Process $PID terminated${NC}" >&2
        break
    fi

    # Get process stats
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

    # CPU and memory usage
    read -r CPU MEM RSS VSZ <<<"$(ps -p "$PID" -o %cpu=,%mem=,rss=,vsz= 2>/dev/null || echo "0 0 0 0")"

    # Convert KB to MB
    RSS_MB=$(awk "BEGIN {printf \"%.2f\", $RSS/1024}")
    VSZ_MB=$(awk "BEGIN {printf \"%.2f\", $VSZ/1024}")

    # Thread count
    THREADS=$(ps -p "$PID" -o nlwp= 2>/dev/null || echo "0")

    # File descriptor count (Linux-specific)
    if [[ -d "/proc/$PID/fd" ]]; then
        FD_COUNT=$(ls -1 /proc/"$PID"/fd 2>/dev/null | wc -l)
    else
        FD_COUNT="N/A"
    fi

    # Process uptime
    PROC_START=$(ps -p "$PID" -o lstart= 2>/dev/null | date -f - +%s)
    UPTIME_SEC=$((CURRENT_TIME - PROC_START))
    UPTIME=$(date -u -d @"$UPTIME_SEC" '+%H:%M:%S' 2>/dev/null || echo "${UPTIME_SEC}s")

    # Output
    if [[ "$JSON_OUTPUT" == "true" ]]; then
        # JSON format
        cat <<EOF
{
  "timestamp": "$TIMESTAMP",
  "pid": $PID,
  "iteration": $ITERATION,
  "cpu_percent": $CPU,
  "memory_percent": $MEM,
  "rss_mb": $RSS_MB,
  "vsz_mb": $VSZ_MB,
  "threads": $THREADS,
  "fd_count": $FD_COUNT,
  "uptime_seconds": $UPTIME_SEC
}
EOF
    else
        # Human-readable format with color coding
        # Color code CPU usage
        CPU_COLOR="$NC"
        if (($(echo "$CPU > 80" | bc -l))); then
            CPU_COLOR="$RED"
        elif (($(echo "$CPU > 50" | bc -l))); then
            CPU_COLOR="$YELLOW"
        fi

        # Color code memory usage
        MEM_COLOR="$NC"
        if (($(echo "$MEM > 80" | bc -l))); then
            MEM_COLOR="$RED"
        elif (($(echo "$MEM > 50" | bc -l))); then
            MEM_COLOR="$YELLOW"
        fi

        printf "%-20s ${CPU_COLOR}%8s${NC} ${MEM_COLOR}%8s${NC} %10s %10s %10s %10s %8s\n" \
            "$TIMESTAMP" "$CPU" "$MEM" "$RSS_MB" "$VSZ_MB" "$THREADS" "$FD_COUNT" "$UPTIME"
    fi

    ITERATION=$((ITERATION + 1))

    # Sleep for interval
    sleep "$INTERVAL"
done

# Print summary (non-JSON mode)
if [[ "$JSON_OUTPUT" == "false" ]]; then
    echo ""
    echo -e "${GREEN}=== Monitoring Summary ===${NC}"
    echo "PID: $PID"
    echo "Total iterations: $ITERATION"
    echo "Total duration: ${ELAPSED}s"
    echo "Process: $PROCESS_CMD"
fi
