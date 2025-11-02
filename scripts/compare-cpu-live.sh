#!/usr/bin/env bash
#
# Live CPU comparison: ps, top, pidstat, and pg_exporter
#
# Compares instantaneous CPU metrics (top/pidstat/exporter)
# ps shows historical average, included for reference only
#
# Purpose: Validate that pg_exporter CPU metrics match system tools
# Usage: ./compare-cpu-live.sh [interval_seconds]
#

set -euo pipefail

# ============================================================================
# COLORS
# ============================================================================
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

# ============================================================================
# CONFIGURATION
# ============================================================================
INTERVAL=${1:-5} # Sample interval in seconds (default: 5)
PORT=9432        # pg_exporter metrics port

# ============================================================================
# HEADER
# ============================================================================
echo -e "${BOLD}🔍 Live CPU Comparison${NC}"
echo -e "${BOLD}======================${NC}"
echo ""
echo "Comparing instantaneous CPU metrics"
echo "Interval: ${INTERVAL} seconds (change with: $0 <seconds>)"
echo ""
echo -e "${CYAN}ℹ  ps shows average since start, others show current usage${NC}"
echo "Press Ctrl+C to stop"
echo ""

# ============================================================================
# FIND RUNNING EXPORTER
# ============================================================================
PID=$(pgrep -f "pg_exporter.*collector.exporter" | head -1 || true)

if [ "$PID" = "" ]; then
    echo -e "${RED}❌ No running pg_exporter with --collector.exporter found${NC}"
    echo ""
    echo -e "${YELLOW}Start one in another terminal:${NC}"
    echo -e "  ${CYAN}just watch${NC}"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓ Found pg_exporter PID: ${PID}${NC}"

# ============================================================================
# CHECK AVAILABLE TOOLS
# ============================================================================
if ! command -v pidstat >/dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  pidstat not found (install: apt install sysstat)${NC}"
    PIDSTAT_AVAILABLE=false
else
    PIDSTAT_AVAILABLE=true
fi

# Get system information
NUM_CORES=$(nproc 2>/dev/null || echo "N/A")
echo -e "${BLUE}ℹ  System has ${NUM_CORES} CPU cores${NC}"
echo ""

# ============================================================================
# PRINT TABLE HEADER
# ============================================================================
printf "${BOLD}%-10s %-8s %-10s %-10s %-10s %-8s %-8s %-10s${NC}\n" \
    "TIME" "PS-AVG" "TOP" "PIDSTAT" "EXPORTER" "PER-CORE" "DELTA" "MATCH"
printf "${BOLD}%-10s %-8s %-10s %-10s %-10s %-8s %-8s %-10s${NC}\n" \
    "---" "---" "---" "---" "---" "---" "---" "---"

# ============================================================================
# STATISTICS TRACKING
# ============================================================================
SAMPLE=0       # Sample counter
SUM_TOP=0      # Running sum for top CPU %
SUM_PIDSTAT=0  # Running sum for pidstat CPU %
SUM_EXPORTER=0 # Running sum for exporter CPU %
COUNT=0        # Number of valid samples

# ============================================================================
# MAIN MONITORING LOOP
# ============================================================================
while true; do
    SAMPLE=$((SAMPLE + 1))
    TIMESTAMP=$(date '+%H:%M:%S')

    # ------------------------------------------------------------------------
    # Check if process is still alive
    # ------------------------------------------------------------------------
    if ! kill -0 "$PID" 2>/dev/null; then
        echo -e "\n${RED}❌ Process $PID no longer exists${NC}"
        exit 1
    fi

    # ------------------------------------------------------------------------
    # SAMPLE 1: ps (historical average - for reference only)
    # Shows: Average CPU since process started
    # ------------------------------------------------------------------------
    PS_CPU=$(ps -p "$PID" -o %cpu= | tr -d ' ' || echo "N/A")

    # ------------------------------------------------------------------------
    # SAMPLE 2: top (instantaneous snapshot)
    # Shows: Current CPU usage (2 iterations for accuracy)
    # ------------------------------------------------------------------------
    TOP_CPU=$(top -b -n 2 -d 0.5 -p "$PID" 2>/dev/null | grep "^ *$PID" | tail -1 | awk '{print $9}' || echo "0")
    if [ "$TOP_CPU" = "" ] || [ "$TOP_CPU" = "N/A" ]; then
        TOP_CPU="0"
    fi

    # ------------------------------------------------------------------------
    # SAMPLE 3: pidstat (instantaneous average over 1 second)
    # Shows: CPU usage averaged over 1-second interval
    # ------------------------------------------------------------------------
    PIDSTAT_CPU="0"
    if [ "$PIDSTAT_AVAILABLE" = true ]; then
        PIDSTAT_OUTPUT=$(pidstat -p "$PID" 1 1 2>/dev/null || echo "")
        if [ "$PIDSTAT_OUTPUT" != "" ]; then
            # Try to get "Average:" line first (most reliable)
            PIDSTAT_CPU=$(echo "$PIDSTAT_OUTPUT" | grep "Average:" | awk '{print $8}' || echo "")
            # If no Average line, try to get data line
            if [ "$PIDSTAT_CPU" = "" ]; then
                PIDSTAT_CPU=$(echo "$PIDSTAT_OUTPUT" | grep "$PID" | grep -v "Average" | tail -1 | awk '{print $8}' || echo "0")
            fi
        fi
        # Ensure we have a valid value
        if [ "$PIDSTAT_CPU" = "" ]; then
            PIDSTAT_CPU="0"
        fi
    fi

    # ------------------------------------------------------------------------
    # SAMPLE 4: pg_exporter metrics (instantaneous from sysinfo)
    # Shows: Current CPU usage from sysinfo library
    # ------------------------------------------------------------------------
    EXPORTER_RAW=$(curl -s http://localhost:"$PORT"/metrics 2>/dev/null |
        grep "^pg_exporter_process_cpu_percent " |
        awk '{print $2}' || echo "0")

    EXPORTER_CORES=$(curl -s http://localhost:"$PORT"/metrics 2>/dev/null |
        grep "^pg_exporter_process_cpu_cores " |
        awk '{print $2}' || echo "1")

    # ------------------------------------------------------------------------
    # Calculate per-core percentage (for dashboard left axis comparison)
    # Formula: raw_cpu_percent / num_cores
    # Example: 124% / 24 cores = 5.17% per-core average
    # ------------------------------------------------------------------------
    EXPORTER_PERCORE=$(awk -v raw="$EXPORTER_RAW" -v cores="$EXPORTER_CORES" \
        'BEGIN {printf "%.2f", raw/cores}')
    EXPORTER_DISPLAY=$(awk -v raw="$EXPORTER_RAW" 'BEGIN {printf "%.2f", raw}')

    # ------------------------------------------------------------------------
    # Calculate DELTA: Compare exporter against reference
    # Reference is average of instantaneous tools (top + pidstat)
    # We skip ps because it shows historical average, not current usage
    # ------------------------------------------------------------------------
    if [ "$PIDSTAT_CPU" != "0" ] && [ "$TOP_CPU" != "0" ]; then
        # Both tools available - use average for more accurate reference
        REFERENCE_CPU=$(awk -v top="$TOP_CPU" -v pid="$PIDSTAT_CPU" \
            'BEGIN {printf "%.2f", (top+pid)/2}')
        REFERENCE_NAME="avg(top+pidstat)"
    elif [ "$TOP_CPU" != "0" ]; then
        # Only top available
        REFERENCE_CPU="$TOP_CPU"
        REFERENCE_NAME="top"
    else
        # Neither available - can't calculate delta
        REFERENCE_CPU="0"
        REFERENCE_NAME="none"
    fi

    # ------------------------------------------------------------------------
    # Calculate delta and match status
    # ------------------------------------------------------------------------
    if [ "$REFERENCE_CPU" != "0" ] && [ "$EXPORTER_DISPLAY" != "0.00" ]; then
        # Calculate signed delta (how much exporter differs from reference)
        DELTA=$(awk -v exporter="$EXPORTER_DISPLAY" -v ref="$REFERENCE_CPU" 'BEGIN {
            diff = exporter - ref
            printf "%+.2f", diff
        }')

        # Calculate absolute delta (for comparison threshold)
        DELTA_ABS=$(awk -v exporter="$EXPORTER_DISPLAY" -v ref="$REFERENCE_CPU" 'BEGIN {
            diff = exporter - ref
            if (diff < 0) diff = -diff
            printf "%.2f", diff
        }')

        # Track for rolling average statistics
        SUM_TOP=$(awk -v sum="$SUM_TOP" -v val="$TOP_CPU" 'BEGIN {printf "%.2f", sum + val}')
        SUM_PIDSTAT=$(awk -v sum="$SUM_PIDSTAT" -v val="$PIDSTAT_CPU" 'BEGIN {printf "%.2f", sum + val}')
        SUM_EXPORTER=$(awk -v sum="$SUM_EXPORTER" -v val="$EXPORTER_DISPLAY" 'BEGIN {printf "%.2f", sum + val}')
        COUNT=$((COUNT + 1))

        # Determine match status based on delta
        # Note: Instantaneous readings vary due to sampling timing
        #   ±5% = Good match (expected variance)
        #   ±10% = Close enough (acceptable)
        #   >10% = Investigate (may indicate issue)
        if (($(awk -v diff="$DELTA_ABS" 'BEGIN {print (diff < 5.0) ? 1 : 0}'))); then
            MATCH_STATUS="${GREEN}✓ Good${NC}"
        elif (($(awk -v diff="$DELTA_ABS" 'BEGIN {print (diff < 10.0) ? 1 : 0}'))); then
            MATCH_STATUS="${YELLOW}~ Close${NC}"
        else
            MATCH_STATUS="${RED}✗ Off${NC}"
        fi
    else
        # No reference available or exporter not reporting
        DELTA="N/A"
        MATCH_STATUS="${BLUE}...${NC}"
    fi

    # ------------------------------------------------------------------------
    # Print current sample row
    # ------------------------------------------------------------------------
    printf "%-10s %-8s %-10s %-10s %-10s %-8s %-8s " \
        "$TIMESTAMP" "$PS_CPU" "$TOP_CPU" "$PIDSTAT_CPU" "$EXPORTER_DISPLAY" "$EXPORTER_PERCORE" "$DELTA"
    echo -e "$MATCH_STATUS"

    # ------------------------------------------------------------------------
    # Every 10 samples, print statistics summary
    # Shows rolling average to smooth out instantaneous variance
    # ------------------------------------------------------------------------
    if [ $((SAMPLE % 10)) -eq 0 ]; then
        if [ "$COUNT" -gt 0 ]; then
            # Calculate averages for last 10 samples
            AVG_TOP=$(awk -v sum="$SUM_TOP" -v count="$COUNT" 'BEGIN {printf "%.2f", sum/count}')
            AVG_PIDSTAT=$(awk -v sum="$SUM_PIDSTAT" -v count="$COUNT" 'BEGIN {printf "%.2f", sum/count}')
            AVG_EXPORTER=$(awk -v sum="$SUM_EXPORTER" -v count="$COUNT" 'BEGIN {printf "%.2f", sum/count}')

            # Calculate average difference (exporter vs reference)
            AVG_DIFF=$(awk -v top="$AVG_TOP" -v pid="$AVG_PIDSTAT" -v exporter="$AVG_EXPORTER" 'BEGIN {
                avg_ref = (top + pid) / 2
                diff = exporter - avg_ref
                if (diff < 0) diff = -diff
                printf "%.2f", diff
            }')

            # Print statistics
            echo ""
            echo -e "${CYAN}📊 Last 10 samples (instantaneous tools):${NC}"
            echo -e "${CYAN}   Top avg=${AVG_TOP}%, Pidstat avg=${AVG_PIDSTAT}%, Exporter avg=${AVG_EXPORTER}%${NC}"
            echo -e "${CYAN}   Average difference: ${AVG_DIFF}% (comparing exporter vs ${REFERENCE_NAME})${NC}"
            echo ""

            # Reset counters for next 10 samples
            SUM_TOP=0
            SUM_PIDSTAT=0
            SUM_EXPORTER=0
            COUNT=0
        fi

        # Reprint header for readability
        printf "${BOLD}%-10s %-8s %-10s %-10s %-10s %-8s %-8s %-10s${NC}\n" \
            "TIME" "PS-AVG" "TOP" "PIDSTAT" "EXPORTER" "PER-CORE" "DELTA" "MATCH"
        printf "${BOLD}%-10s %-8s %-10s %-10s %-10s %-8s %-8s %-10s${NC}\n" \
            "---" "---" "---" "---" "---" "---" "---" "---"
    fi

    # ------------------------------------------------------------------------
    # Wait for next sample interval
    # ------------------------------------------------------------------------
    sleep "$INTERVAL"
done
