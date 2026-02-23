#!/usr/bin/env bash
set -euo pipefail

RESULTS="/mnt/d/projects/go-race/benchmarks/results"
SCRIPT="/mnt/d/projects/go-race/benchmarks"
SYSTEM_GO="/opt/go1.26.0/bin/go"
GORACE_BIN="/mnt/d/projects/go-race/bin/go"

> "$RESULTS/memory_rss.txt"

measure() {
    local label="$1"
    local go_bin="$2"
    shift 2
    local args=("$@")

    cd "$SCRIPT"
    local tmpfile
    tmpfile=$(mktemp)

    /usr/bin/time -v "$go_bin" test "${args[@]}" \
        -bench=BenchmarkWorkerPool/g64 \
        -benchtime=3s -count=1 -timeout=2m \
        > /dev/null 2> "$tmpfile" || true

    local peak_kb
    peak_kb=$(grep "Maximum resident set size" "$tmpfile" | awk '{print $NF}' || echo 0)
    local peak_mb=$(( peak_kb / 1024 ))
    echo "${label}: ${peak_mb} MB (${peak_kb} KB)" | tee -a "$RESULTS/memory_rss.txt"
    rm -f "$tmpfile"
}

echo "=== Peak RSS Measurement ==="
measure "BASELINE" "$SYSTEM_GO"
measure "TSAN" "$SYSTEM_GO" -race
measure "KOLKOV" "$GORACE_BIN" -race

echo ""
echo "Results saved to $RESULTS/memory_rss.txt"
