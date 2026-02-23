#!/usr/bin/env bash
# run_comparison.sh — Head-to-head benchmarks: Kolkov pure-Go race detector vs TSAN
#
# Runs three configurations:
#   1. BASELINE — Go 1.26.0, no -race
#   2. TSAN     — Go 1.26.0 (CGO_ENABLED=1), -race
#   3. KOLKOV   — go-race toolchain (CGO_ENABLED=0), -race
#
# Prerequisites:
#   - Go 1.26.0 installed (SYSTEM_GO)
#   - go-race toolchain built for Linux (GORACE_BIN)
#   - gcc (for TSAN)
#   - benchstat: go install golang.org/x/perf/cmd/benchstat@latest
#   - /usr/bin/time (GNU time, for RSS measurement)
#
# Usage:
#   bash run_comparison.sh [--count N] [--benchtime T] [--quick]
#   bash run_comparison.sh --system-go /path/to/go --gorace-bin /path/to/go-race/bin/go
#   bash run_comparison.sh --save VERSION    Save current results as a named baseline
#   bash run_comparison.sh --diff V1 V2      Compare two saved baselines with benchstat
#   bash run_comparison.sh --list            List available saved baselines

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results"
BASELINES_DIR="${SCRIPT_DIR}/baselines"

# Defaults — override via flags or environment
GORACE_BIN="${GORACE_BIN:-${SCRIPT_DIR}/../bin/go}"
SYSTEM_GO="${SYSTEM_GO:-$(which go 2>/dev/null || echo "go")}"

COUNT=10
BENCHTIME="1s"
TIMEOUT="10m"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --count)       COUNT="$2"; shift 2 ;;
        --benchtime)   BENCHTIME="$2"; shift 2 ;;
        --quick)       COUNT=3; BENCHTIME="500ms"; shift ;;
        --system-go)   SYSTEM_GO="$2"; shift 2 ;;
        --gorace-bin)  GORACE_BIN="$2"; shift 2 ;;
        --save)
            VERSION="$2"
            mkdir -p "${BASELINES_DIR}"
            for cfg in baseline tsan kolkov; do
                if [[ -f "${RESULTS_DIR}/${cfg}.txt" ]]; then
                    cp "${RESULTS_DIR}/${cfg}.txt" "${BASELINES_DIR}/${VERSION}-${cfg}.txt"
                else
                    echo "WARNING: ${RESULTS_DIR}/${cfg}.txt not found, skipping"
                fi
            done
            echo "Saved baselines as ${VERSION}"
            exit 0
            ;;
        --diff)
            V1="$2"; V2="$3"
            if [[ ! -f "${BASELINES_DIR}/${V1}-kolkov.txt" ]]; then
                echo "ERROR: baseline ${V1} not found in ${BASELINES_DIR}/"
                exit 1
            fi
            if [[ ! -f "${BASELINES_DIR}/${V2}-kolkov.txt" ]]; then
                echo "ERROR: baseline ${V2} not found in ${BASELINES_DIR}/"
                exit 1
            fi
            benchstat "${BASELINES_DIR}/${V1}-kolkov.txt" "${BASELINES_DIR}/${V2}-kolkov.txt"
            exit 0
            ;;
        --list)
            echo "Available baselines:"
            ls -1 "${BASELINES_DIR}"/*-kolkov.txt 2>/dev/null | sed 's|.*/||; s|-kolkov.txt||' || echo "  (none)"
            exit 0
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --count N         Number of benchmark runs (default: 10)"
            echo "  --benchtime T     Duration per benchmark (default: 1s)"
            echo "  --quick           Quick mode: count=3, benchtime=500ms"
            echo "  --system-go PATH  Path to system Go binary (for TSAN)"
            echo "  --gorace-bin PATH Path to go-race binary (for Kolkov)"
            echo ""
            echo "Baseline management:"
            echo "  --save VERSION    Save current results as a named baseline"
            echo "  --diff V1 V2     Compare two saved baselines with benchstat"
            echo "  --list            List available saved baselines"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

BENCH_FLAGS="-bench=. -benchmem -benchtime=${BENCHTIME} -count=${COUNT} -timeout=${TIMEOUT}"

# ---------------------------------------------------------------------------
# Preflight checks
# ---------------------------------------------------------------------------

echo "============================================================"
echo " Head-to-Head Benchmarks: Kolkov vs TSAN"
echo "============================================================"
echo ""

# Verify binaries exist
for bin_label in "SYSTEM_GO:${SYSTEM_GO}" "GORACE_BIN:${GORACE_BIN}"; do
    label="${bin_label%%:*}"
    path="${bin_label#*:}"
    if [[ ! -x "${path}" ]]; then
        echo "ERROR: ${label} not found or not executable: ${path}"
        echo "Use --system-go / --gorace-bin to set paths."
        exit 1
    fi
done

if ! command -v benchstat &>/dev/null; then
    echo "WARNING: benchstat not found. Install: go install golang.org/x/perf/cmd/benchstat@latest"
fi

if ! command -v gcc &>/dev/null; then
    echo "WARNING: gcc not found. TSAN benchmarks may fail."
fi

echo "Config:"
echo "  count       = ${COUNT}"
echo "  benchtime   = ${BENCHTIME}"
echo "  system go   = ${SYSTEM_GO}"
echo "  go-race     = ${GORACE_BIN}"
echo "  results     = ${RESULTS_DIR}"
echo ""

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

mkdir -p "${RESULTS_DIR}"

# ---------------------------------------------------------------------------
# Environment info
# ---------------------------------------------------------------------------

echo "[1/6] Capturing environment info..."

{
    echo "=== Environment ==="
    echo "Date:       $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "OS:         $(uname -s) $(uname -r)"
    if [[ -f /proc/cpuinfo ]]; then
        echo "CPU:        $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)"
    else
        echo "CPU:        unknown"
    fi
    echo "Cores:      $(nproc 2>/dev/null || echo 'unknown')"
    if command -v free &>/dev/null; then
        echo "RAM:        $(free -h | awk '/^Mem:/{print $2}')"
    else
        echo "RAM:        unknown"
    fi
    echo ""
    echo "System Go:  $(${SYSTEM_GO} version)"
    echo "Go-Race:    $(${GORACE_BIN} version)"
    echo "GCC:        $(gcc --version 2>/dev/null | head -1 || echo 'not found')"
    echo ""
    echo "Benchstat:  $(which benchstat 2>/dev/null || echo 'not found')"
    echo "GOARCH:     $(${SYSTEM_GO} env GOARCH)"
    echo "GOOS:       $(${SYSTEM_GO} env GOOS)"
} > "${RESULTS_DIR}/environment.txt" 2>&1

cat "${RESULTS_DIR}/environment.txt"
echo ""

# ---------------------------------------------------------------------------
# Config 1: BASELINE (no -race)
# ---------------------------------------------------------------------------

echo "[2/6] Running BASELINE (no -race)..."
(
    cd "${SCRIPT_DIR}"
    "${SYSTEM_GO}" test ${BENCH_FLAGS} 2>&1
) > "${RESULTS_DIR}/baseline.txt"
echo "  -> $(grep -c '^Benchmark' "${RESULTS_DIR}/baseline.txt") benchmark results saved"

# ---------------------------------------------------------------------------
# Config 2: TSAN (system Go, CGO_ENABLED=1, -race)
# ---------------------------------------------------------------------------

echo "[3/6] Running TSAN (system Go, -race, CGO_ENABLED=1)..."
(
    cd "${SCRIPT_DIR}"
    CGO_ENABLED=1 "${SYSTEM_GO}" test -race ${BENCH_FLAGS} 2>&1
) > "${RESULTS_DIR}/tsan.txt"
echo "  -> $(grep -c '^Benchmark' "${RESULTS_DIR}/tsan.txt") benchmark results saved"

# ---------------------------------------------------------------------------
# Config 3: KOLKOV (go-race toolchain, CGO_ENABLED=0, -race)
# ---------------------------------------------------------------------------

echo "[4/6] Running KOLKOV (go-race toolchain, -race, CGO_ENABLED=0)..."
(
    cd "${SCRIPT_DIR}"
    CGO_ENABLED=0 "${GORACE_BIN}" test -race ${BENCH_FLAGS} 2>&1
) > "${RESULTS_DIR}/kolkov.txt"
echo "  -> $(grep -c '^Benchmark' "${RESULTS_DIR}/kolkov.txt") benchmark results saved"

# ---------------------------------------------------------------------------
# Benchstat comparison
# ---------------------------------------------------------------------------

echo "[5/6] Running benchstat comparisons..."

if command -v benchstat &>/dev/null; then
    # TSAN vs Kolkov (primary comparison)
    benchstat -col "/config" \
        "config=tsan" "${RESULTS_DIR}/tsan.txt" \
        "config=kolkov" "${RESULTS_DIR}/kolkov.txt" \
        > "${RESULTS_DIR}/benchstat_tsan_vs_kolkov.txt" 2>&1 || true

    # All three configs
    benchstat -col "/config" \
        "config=baseline" "${RESULTS_DIR}/baseline.txt" \
        "config=tsan" "${RESULTS_DIR}/tsan.txt" \
        "config=kolkov" "${RESULTS_DIR}/kolkov.txt" \
        > "${RESULTS_DIR}/benchstat_all.txt" 2>&1 || true

    echo "  -> benchstat done"
else
    echo "  -> benchstat not found, skipping statistical comparison"
    echo "(not available)" > "${RESULTS_DIR}/benchstat_tsan_vs_kolkov.txt"
    echo "(not available)" > "${RESULTS_DIR}/benchstat_all.txt"
fi

# ---------------------------------------------------------------------------
# Memory measurement (peak RSS via /usr/bin/time -v)
# ---------------------------------------------------------------------------

echo "[6/6] Measuring peak RSS..."

GNU_TIME=""
if [[ -x /usr/bin/time ]]; then
    GNU_TIME="/usr/bin/time"
elif command -v gtime &>/dev/null; then
    GNU_TIME="gtime"
fi

measure_peak_rss() {
    local label="$1"
    local go_bin="$2"
    shift 2
    local args=("$@")

    if [[ -z "${GNU_TIME}" ]]; then
        echo "  ${label}: /usr/bin/time not available, skipping"
        echo "${label}: N/A" >> "${RESULTS_DIR}/memory_rss.txt"
        return
    fi

    cd "${SCRIPT_DIR}"
    local tmpfile
    tmpfile=$(mktemp)

    # Run benchmark under GNU time to capture peak RSS
    "${GNU_TIME}" -v "${go_bin}" test "${args[@]}" \
        -bench=BenchmarkWorkerPool/g64 \
        -benchtime=3s -count=1 -timeout=2m \
        > /dev/null 2> "${tmpfile}" || true

    local peak_kb
    peak_kb=$(grep "Maximum resident set size" "${tmpfile}" | awk '{print $NF}' || echo 0)
    local peak_mb=$(( peak_kb / 1024 ))
    echo "${label}: ${peak_mb} MB (${peak_kb} KB)" >> "${RESULTS_DIR}/memory_rss.txt"
    echo "  ${label}: ${peak_mb} MB peak RSS"
    rm -f "${tmpfile}"
}

> "${RESULTS_DIR}/memory_rss.txt"

measure_peak_rss "BASELINE" "${SYSTEM_GO}"
measure_peak_rss "TSAN"     "${SYSTEM_GO}" -race
measure_peak_rss "KOLKOV"   "${GORACE_BIN}" -race

# ---------------------------------------------------------------------------
# Generate summary.md
# ---------------------------------------------------------------------------

echo ""
echo "Generating summary.md..."

{
    cat <<'HEADER'
## Benchmark Results: Pure-Go Race Detector vs TSAN

HEADER

    echo '```'
    cat "${RESULTS_DIR}/environment.txt"
    echo '```'
    echo ""

    echo "**Method:** \`go test -bench -count=${COUNT} -benchtime=${BENCHTIME} -benchmem\`, compared with benchstat"
    echo ""

    echo "### TSAN vs Kolkov (benchstat)"
    echo ""
    echo '```'
    cat "${RESULTS_DIR}/benchstat_tsan_vs_kolkov.txt"
    echo '```'
    echo ""

    echo "### All Three Configs (benchstat)"
    echo ""
    echo '```'
    cat "${RESULTS_DIR}/benchstat_all.txt"
    echo '```'
    echo ""

    echo "### Peak RSS (Memory)"
    echo ""
    echo '```'
    cat "${RESULTS_DIR}/memory_rss.txt"
    echo '```'
    echo ""

} > "${RESULTS_DIR}/summary.md"

echo ""
echo "============================================================"
echo " DONE!"
echo "============================================================"
echo ""
echo "Results in: ${RESULTS_DIR}/"
echo "  environment.txt              - hardware/software info"
echo "  baseline.txt                 - raw benchmark (no -race)"
echo "  tsan.txt                     - raw benchmark (TSAN)"
echo "  kolkov.txt                   - raw benchmark (Kolkov)"
echo "  benchstat_tsan_vs_kolkov.txt - statistical comparison"
echo "  benchstat_all.txt            - all three configs"
echo "  memory_rss.txt               - peak RSS per config"
echo "  summary.md                   - formatted for #76786"
echo ""
echo "Next step: review summary.md, then post to golang/go#76786"
