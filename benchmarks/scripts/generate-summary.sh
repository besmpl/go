#!/usr/bin/env bash
# generate-summary.sh — Parse raw go test -bench output from 3 configurations
# and generate beautiful markdown comparison tables.
#
# Usage:
#   bash scripts/generate-summary.sh results/baseline.txt results/tsan.txt results/kolkov.txt
#   bash scripts/generate-summary.sh results/baseline.txt results/tsan.txt results/kolkov.txt results/benchstat_tsan_vs_kolkov.txt
#
# Input:  Standard `go test -bench -benchmem` output files
# Output: Markdown tables to stdout (and optionally to $GITHUB_STEP_SUMMARY)
#
# Requires: awk (gawk), bash 4+ (associative arrays)
# Note: Uses awk for ALL floating-point arithmetic (no bc dependency).

set -euo pipefail

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------

BASELINE_FILE="${1:?Usage: $0 baseline.txt tsan.txt kolkov.txt [benchstat.txt]}"
TSAN_FILE="${2:?Usage: $0 baseline.txt tsan.txt kolkov.txt [benchstat.txt]}"
KOLKOV_FILE="${3:?Usage: $0 baseline.txt tsan.txt kolkov.txt [benchstat.txt]}"
BENCHSTAT_FILE="${4:-}"

RESULTS_DIR="$(dirname "${KOLKOV_FILE}")"

# ---------------------------------------------------------------------------
# awk helper: replaces bc for floating-point math
# ---------------------------------------------------------------------------

# awk_calc "expression" — evaluate a floating-point expression, print result
# Example: awk_calc "282.5 / 34.4" -> "8.2122"
awk_calc() {
    awk "BEGIN { printf \"%.4f\", $1 }"
}

# awk_cmp "a OP b" — returns 0 (true) or 1 (false)
# Example: awk_cmp "3.14 > 2.0"
awk_cmp() {
    awk "BEGIN { exit !($1) }"
}

# ---------------------------------------------------------------------------
# Parse benchmark results using awk
# ---------------------------------------------------------------------------

# parse_bench FILE -> lines of: NAME\tns_per_op\tB_per_op\tallocs_per_op\tcount
# Handles ns/op, us/op, ms/op, s/op — normalizes everything to ns.
# Computes median across repeated runs of the same benchmark name.
parse_bench() {
    local file="$1"
    awk '
    /^Benchmark/ {
        name = $1
        # Strip "Benchmark" prefix for cleaner display
        sub(/^Benchmark/, "", name)

        raw_time = $3 + 0
        unit = $4

        # Normalize to nanoseconds
        if (unit == "ns/op") {
            ns = raw_time
        } else if (unit == "µs/op" || unit == "us/op") {
            ns = raw_time * 1000
        } else if (unit == "ms/op") {
            ns = raw_time * 1000000
        } else if (unit == "s/op") {
            ns = raw_time * 1000000000
        } else {
            next
        }

        # Memory: B/op is field before "B/op", allocs/op is field before "allocs/op"
        bop = 0
        allocs = 0
        for (i = 5; i <= NF; i++) {
            if ($(i) == "B/op") bop = $(i-1) + 0
            if ($(i) == "allocs/op") allocs = $(i-1) + 0
        }

        # Accumulate for median calculation
        count[name]++
        idx = count[name]
        values[name, idx] = ns
        bop_values[name, idx] = bop
        alloc_values[name, idx] = allocs

        # Track order of first appearance
        if (count[name] == 1) {
            order_count++
            order[order_count] = name
        }
    }
    END {
        for (oi = 1; oi <= order_count; oi++) {
            name = order[oi]
            c = count[name]

            # Copy to temp arrays for sorting
            for (a = 1; a <= c; a++) {
                sorted[a] = values[name, a]
                sorted_b[a] = bop_values[name, a]
                sorted_a[a] = alloc_values[name, a]
            }

            # Insertion sort by ns value
            for (a = 2; a <= c; a++) {
                key = sorted[a]; kb = sorted_b[a]; ka = sorted_a[a]
                b = a - 1
                while (b > 0 && sorted[b] > key) {
                    sorted[b+1] = sorted[b]
                    sorted_b[b+1] = sorted_b[b]
                    sorted_a[b+1] = sorted_a[b]
                    b--
                }
                sorted[b+1] = key
                sorted_b[b+1] = kb
                sorted_a[b+1] = ka
            }

            # Median
            if (c % 2 == 1) {
                med_ns = sorted[int(c/2)+1]
                med_b = sorted_b[int(c/2)+1]
                med_a = sorted_a[int(c/2)+1]
            } else {
                med_ns = (sorted[c/2] + sorted[c/2+1]) / 2
                med_b = (sorted_b[c/2] + sorted_b[c/2+1]) / 2
                med_a = (sorted_a[c/2] + sorted_a[c/2+1]) / 2
            }

            printf "%s\t%.2f\t%.0f\t%.0f\t%d\n", name, med_ns, med_b, med_a, c
        }
    }
    ' "$file"
}

# ---------------------------------------------------------------------------
# Load data into associative arrays
# ---------------------------------------------------------------------------

declare -A baseline_ns baseline_bop baseline_allocs baseline_count
declare -A tsan_ns tsan_bop tsan_allocs tsan_count
declare -A kolkov_ns kolkov_bop kolkov_allocs kolkov_count

load_data() {
    local -n ns_arr=$1 bop_arr=$2 allocs_arr=$3 cnt_arr=$4
    local file=$5
    while IFS=$'\t' read -r name ns bop allocs cnt; do
        ns_arr["$name"]="$ns"
        bop_arr["$name"]="$bop"
        allocs_arr["$name"]="$allocs"
        cnt_arr["$name"]="$cnt"
    done < <(parse_bench "$file")
}

load_data baseline_ns baseline_bop baseline_allocs baseline_count "$BASELINE_FILE"
load_data tsan_ns tsan_bop tsan_allocs tsan_count "$TSAN_FILE"
load_data kolkov_ns kolkov_bop kolkov_allocs kolkov_count "$KOLKOV_FILE"

# ---------------------------------------------------------------------------
# Collect all benchmark names (union of all three), preserve order from files
# ---------------------------------------------------------------------------

declare -a all_names=()
declare -A seen_names=()

collect_names() {
    local file="$1"
    while IFS=$'\t' read -r name _rest; do
        if [[ -z "${seen_names[$name]:-}" ]]; then
            all_names+=("$name")
            seen_names["$name"]=1
        fi
    done < <(parse_bench "$file")
}

collect_names "$BASELINE_FILE"
collect_names "$TSAN_FILE"
collect_names "$KOLKOV_FILE"

# ---------------------------------------------------------------------------
# Formatting helpers (all use awk, no bc)
# ---------------------------------------------------------------------------

# Format nanoseconds to human-readable with appropriate unit
format_ns() {
    local ns="$1"
    if [[ -z "$ns" || "$ns" == "0" || "$ns" == "0.00" ]]; then
        echo "—"
        return
    fi
    awk -v ns="$ns" 'BEGIN {
        if (ns >= 1000000000)      printf "%.2fs",  ns / 1000000000
        else if (ns >= 1000000)    printf "%.1fms", ns / 1000000
        else if (ns >= 1000)       printf "%.1fus", ns / 1000
        else if (ns >= 100)        printf "%.0fns", ns
        else if (ns >= 10)         printf "%.1fns", ns
        else                       printf "%.2fns", ns
    }'
}

# Format ratio: Kolkov vs TSAN
format_vs_tsan() {
    local tsan_val="$1"
    local kolkov_val="$2"

    if [[ -z "$tsan_val" || "$tsan_val" == "0" || "$tsan_val" == "0.00" \
       || -z "$kolkov_val" || "$kolkov_val" == "0" || "$kolkov_val" == "0.00" ]]; then
        echo "—"
        return
    fi

    awk -v t="$tsan_val" -v k="$kolkov_val" 'BEGIN {
        ratio = k / t
        if (ratio < 0.91) {
            inv = t / k
            printf "**%.1fx faster**", inv
        } else if (ratio < 1.10) {
            printf "~1x"
        } else if (ratio < 10.0) {
            printf "%.1fx slower", ratio
        } else {
            printf "**%.0fx slower**", ratio
        }
    }'
}

# Format ratio: Kolkov vs Baseline
format_vs_baseline() {
    local base_val="$1"
    local kolkov_val="$2"

    if [[ -z "$base_val" || "$base_val" == "0" || "$base_val" == "0.00" \
       || -z "$kolkov_val" || "$kolkov_val" == "0" || "$kolkov_val" == "0.00" ]]; then
        echo "—"
        return
    fi

    awk -v b="$base_val" -v k="$kolkov_val" 'BEGIN {
        ratio = k / b
        if (ratio < 1.10)      printf "~1x"
        else if (ratio >= 10)  printf "**%.0fx**", ratio
        else                   printf "%.1fx", ratio
    }'
}

# Determine winner between TSAN and Kolkov
get_winner() {
    local tsan_val="$1"
    local kolkov_val="$2"

    if [[ -z "$tsan_val" || "$tsan_val" == "0" || "$tsan_val" == "0.00" ]]; then
        if [[ -n "$kolkov_val" && "$kolkov_val" != "0" && "$kolkov_val" != "0.00" ]]; then
            echo "Kolkov"
        else
            echo "—"
        fi
        return
    fi
    if [[ -z "$kolkov_val" || "$kolkov_val" == "0" || "$kolkov_val" == "0.00" ]]; then
        echo "TSAN"
        return
    fi

    awk -v t="$tsan_val" -v k="$kolkov_val" 'BEGIN {
        if (k < t * 0.95)      printf "**Kolkov**"
        else if (t < k * 0.95) printf "TSAN"
        else                   printf "~tie"
    }'
}

# Strip CPU suffix from benchmark name: "RaceRead-4" -> "RaceRead", "MutexContention/g4-4" -> "MutexContention/g4"
display_name() {
    local name="$1"
    # Remove trailing -N (CPU count) from name
    echo "$name" | sed 's/-[0-9]*$//'
}

# ---------------------------------------------------------------------------
# Output buffer — write to stdout and optionally to GITHUB_STEP_SUMMARY
# ---------------------------------------------------------------------------

OUTPUT=""

emit() {
    OUTPUT+="$1"$'\n'
}

# ---------------------------------------------------------------------------
# Platform header
# ---------------------------------------------------------------------------

if [[ -n "${PLATFORM:-}" || -n "${CPU:-}" || -n "${GO_VERSION:-}" ]]; then
    emit "**Platform:** ${PLATFORM:-unknown}"
    emit "**CPU:** ${CPU:-unknown}"
    emit "**Go:** ${GO_VERSION:-unknown}"
    if [[ -n "${COUNT:-}" && -n "${BENCHTIME:-}" ]]; then
        emit "**Method:** \`go test -bench -count=${COUNT} -benchtime=${BENCHTIME} -benchmem\`"
    fi
    emit ""
elif [[ -f "${RESULTS_DIR}/environment.txt" ]]; then
    # Try to extract from environment.txt
    env_cpu=$(grep "^CPU:" "${RESULTS_DIR}/environment.txt" 2>/dev/null | sed 's/^CPU:[[:space:]]*//' || true)
    env_os=$(grep "^OS:" "${RESULTS_DIR}/environment.txt" 2>/dev/null | sed 's/^OS:[[:space:]]*//' || true)
    env_go=$(grep "^System Go:" "${RESULTS_DIR}/environment.txt" 2>/dev/null | sed 's/^System Go:[[:space:]]*//' || true)
    env_cores=$(grep "^Cores:" "${RESULTS_DIR}/environment.txt" 2>/dev/null | sed 's/^Cores:[[:space:]]*//' || true)
    env_ram=$(grep "^RAM:" "${RESULTS_DIR}/environment.txt" 2>/dev/null | sed 's/^RAM:[[:space:]]*//' || true)

    if [[ -n "$env_cpu" || -n "$env_os" ]]; then
        emit "**Platform:** ${env_os}"
        emit "**CPU:** ${env_cpu} (${env_cores} cores, ${env_ram} RAM)"
        emit "**Go:** ${env_go}"
        emit ""
    fi
fi

# ---------------------------------------------------------------------------
# Table 1: Performance Comparison
# ---------------------------------------------------------------------------

emit "### Performance Comparison"
emit ""
emit "| Benchmark | Baseline | TSAN | Kolkov | vs TSAN | vs Baseline | Winner |"
emit "|-----------|----------|------|--------|---------|-------------|--------|"

for name in "${all_names[@]}"; do
    b_ns="${baseline_ns[$name]:-}"
    t_ns="${tsan_ns[$name]:-}"
    k_ns="${kolkov_ns[$name]:-}"

    # Format time values
    b_fmt=$(format_ns "$b_ns")
    t_fmt=$(format_ns "$t_ns")
    k_fmt=$(format_ns "$k_ns")

    # Calculate ratios
    vs_tsan=$(format_vs_tsan "$t_ns" "$k_ns")
    vs_base=$(format_vs_baseline "$b_ns" "$k_ns")

    # Determine winner
    winner=$(get_winner "$t_ns" "$k_ns")

    # Bold the winner's time
    if [[ "$winner" == "**Kolkov**" ]]; then
        k_fmt="**${k_fmt}**"
    elif [[ "$winner" == "TSAN" ]]; then
        t_fmt="**${t_fmt}**"
    fi

    dname=$(display_name "$name")
    emit "| ${dname} | ${b_fmt} | ${t_fmt} | ${k_fmt} | ${vs_tsan} | ${vs_base} | ${winner} |"
done

emit ""

# ---------------------------------------------------------------------------
# Table 2: Memory Comparison
# ---------------------------------------------------------------------------

# Check if there's any non-zero memory data worth showing
has_mem_data=false
for name in "${all_names[@]}"; do
    t_bop="${tsan_bop[$name]:-0}"
    k_bop="${kolkov_bop[$name]:-0}"
    t_allocs="${tsan_allocs[$name]:-0}"
    k_allocs="${kolkov_allocs[$name]:-0}"
    if [[ "$t_bop" != "0" || "$k_bop" != "0" || "$t_allocs" != "0" || "$k_allocs" != "0" ]]; then
        has_mem_data=true
        break
    fi
done

if [[ "$has_mem_data" == "true" ]]; then
    emit "### Memory Comparison"
    emit ""
    emit "| Benchmark | TSAN B/op | Kolkov B/op | TSAN allocs | Kolkov allocs |"
    emit "|-----------|-----------|-------------|-------------|---------------|"

    for name in "${all_names[@]}"; do
        t_bop="${tsan_bop[$name]:-0}"
        k_bop="${kolkov_bop[$name]:-0}"
        t_allocs="${tsan_allocs[$name]:-0}"
        k_allocs="${kolkov_allocs[$name]:-0}"

        # Skip rows where everything is zero
        if [[ "$t_bop" == "0" && "$k_bop" == "0" && "$t_allocs" == "0" && "$k_allocs" == "0" ]]; then
            continue
        fi

        dname=$(display_name "$name")

        # Format B/op with K suffix for large values
        t_bop_fmt=$(awk -v v="$t_bop" 'BEGIN { if (v >= 1024) printf "%.1fK", v/1024; else printf "%d", v }')
        k_bop_fmt=$(awk -v v="$k_bop" 'BEGIN { if (v >= 1024) printf "%.1fK", v/1024; else printf "%d", v }')

        # Bold the smaller (better) value if they differ significantly
        if [[ "$t_bop" != "0" && "$k_bop" != "0" ]]; then
            if awk_cmp "$t_bop < $k_bop * 0.9"; then
                t_bop_fmt="**${t_bop_fmt}**"
            elif awk_cmp "$k_bop < $t_bop * 0.9"; then
                k_bop_fmt="**${k_bop_fmt}**"
            fi
        fi

        emit "| ${dname} | ${t_bop_fmt} | ${k_bop_fmt} | ${t_allocs} | ${k_allocs} |"
    done

    emit ""
fi

# ---------------------------------------------------------------------------
# Table 3: Peak RSS (if memory_rss.txt exists)
# ---------------------------------------------------------------------------

RSS_FILE="${RESULTS_DIR}/memory_rss.txt"
if [[ -f "$RSS_FILE" ]]; then
    emit "### Peak RSS (Memory)"
    emit ""
    emit "| Config | Peak RSS | vs Baseline |"
    emit "|--------|----------|-------------|"

    baseline_rss_kb=0

    while IFS= read -r line; do
        # Parse: "LABEL: NNN MB (NNNNN KB)" or "LABEL: N/A"
        if [[ "$line" =~ ^([A-Z]+):[[:space:]]+([0-9]+)[[:space:]]+MB[[:space:]]+\(([0-9]+)[[:space:]]+KB\) ]]; then
            label="${BASH_REMATCH[1]}"
            mb="${BASH_REMATCH[2]}"
            kb="${BASH_REMATCH[3]}"

            if [[ "$label" == "BASELINE" ]]; then
                baseline_rss_kb="$kb"
                emit "| ${label} | ${mb} MB | — |"
            else
                if [[ "$baseline_rss_kb" -gt 0 ]]; then
                    pct=$(awk -v kb="$kb" -v base="$baseline_rss_kb" 'BEGIN { printf "%.1f", (kb - base) * 100 / base }')
                    emit "| ${label} | ${mb} MB | +${pct}% |"
                else
                    emit "| ${label} | ${mb} MB | — |"
                fi
            fi
        elif [[ "$line" =~ ^([A-Z]+):[[:space:]]+N/A ]]; then
            label="${BASH_REMATCH[1]}"
            emit "| ${label} | N/A | — |"
        fi
    done < "$RSS_FILE"

    emit ""
fi

# ---------------------------------------------------------------------------
# Table 4: Sample counts and data quality
# ---------------------------------------------------------------------------

emit "### Data Quality"
emit ""
emit "| Benchmark | Baseline (n) | TSAN (n) | Kolkov (n) |"
emit "|-----------|-------------|----------|------------|"

for name in "${all_names[@]}"; do
    b_cnt="${baseline_count[$name]:-0}"
    t_cnt="${tsan_count[$name]:-0}"
    k_cnt="${kolkov_count[$name]:-0}"

    dname=$(display_name "$name")

    # Show dash for missing data
    b_cnt_fmt="$b_cnt"
    t_cnt_fmt="$t_cnt"
    k_cnt_fmt="$k_cnt"
    [[ "$b_cnt" == "0" ]] && b_cnt_fmt="—"
    [[ "$t_cnt" == "0" ]] && t_cnt_fmt="—"
    [[ "$k_cnt" == "0" ]] && k_cnt_fmt="—"

    emit "| ${dname} | ${b_cnt_fmt} | ${t_cnt_fmt} | ${k_cnt_fmt} |"
done

emit ""

# ---------------------------------------------------------------------------
# Benchstat section (optional, 4th argument)
# ---------------------------------------------------------------------------

if [[ -n "$BENCHSTAT_FILE" && -f "$BENCHSTAT_FILE" ]]; then
    emit "### Benchstat: TSAN vs Kolkov"
    emit ""
    emit '```'
    # Filter out parsing error lines, only include benchstat output
    while IFS= read -r line; do
        # Skip benchstat parsing errors
        if [[ "$line" =~ ^.*\.txt:[0-9]+:.*parsing ]]; then
            continue
        fi
        emit "$line"
    done < "$BENCHSTAT_FILE"
    emit '```'
    emit ""
fi

# ---------------------------------------------------------------------------
# Legend
# ---------------------------------------------------------------------------

emit "---"
emit ""
emit "**Legend:** Bold time = winner in that row. \"vs TSAN\" compares Kolkov to TSAN (lower is better). \"vs Baseline\" shows total overhead vs no-race."
emit ""
emit "**Ratio formatting:** ~1x = within 10%, **Nx faster** = Kolkov wins, Nx slower = TSAN wins, **bold >10x** = significant gap."
emit ""

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

printf '%s' "$OUTPUT"

# Write to GITHUB_STEP_SUMMARY if available
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    printf '%s' "$OUTPUT" >> "$GITHUB_STEP_SUMMARY"
fi
