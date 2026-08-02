#!/usr/bin/env bash
# Compare baseline, ThreadSanitizer, and pure-Go race configurations built by
# this fork. Benchmark samples and RSS measurements execute the same prebuilt
# binaries; no configuration is rebuilt while evidence is being collected.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
SOURCE_ROOT="$(cd "${SOURCE_ROOT_OVERRIDE:-${SCRIPT_DIR}/..}" && pwd -P)"
RESULTS_DIR="${SCRIPT_DIR}/results"
BASELINES_DIR="${SCRIPT_DIR}/baselines"
ORDER_HELPER="${SCRIPT_DIR}/scripts/benchmark_order.sh"
[[ -r "${ORDER_HELPER}" ]] || { echo "ERROR: benchmark order helper is missing: ${ORDER_HELPER}" >&2; exit 1; }
# shellcheck source=scripts/benchmark_order.sh
source "${ORDER_HELPER}"

FORK_GO="${FORK_GO:-${SOURCE_ROOT}/bin/go}"
COUNT=10
BENCHTIME=1s
TIMEOUT=10m
MAX_RACEREAD_RATIO=1.00
# Matrix gates are intentionally explicit and fail closed.  Keep these values
# in one place so release metadata, local runs, and CI all use the same
# contract.  A ratio is always PureGo latency divided by TSAN latency; the
# contention throughput ratio is the inverse (TSAN latency / PureGo latency).
MAX_MATRIX_GEOMEAN=1.00
MAX_MATRIX_CLASS_RATIO=1.10
MIN_CONTENTION_THROUGHPUT=0.90
MAX_MATRIX_P99_RATIO=1.25
QUICK_MODE=false
ACTION=run
ACTION_ARG1=
ACTION_ARG2=
ACTION_ARG3=
RUN_OPTIONS=false

# Keep the workload list in the harness rather than inferring it from one
# output.  An output which silently omits a named class is invalid evidence.
# The suffix containing GOMAXPROCS is stripped by the parser before matching.
workload_matrix() {
    cat <<'EOF'
RaceRead
RaceReadAlternating
RaceReadCollision
RaceWrite
RaceReadWrite
MutexLockUnlock
RWMutexReadLock
GoroutineStartStop
MutexContention/g1
MutexContention/g4
MutexContention/g16
MutexContention/g64
ChannelPingPong
WaitGroupFanOut/g1
WaitGroupFanOut/g4
WaitGroupFanOut/g16
WaitGroupFanOut/g64
MapReadWrite/g1
MapReadWrite/g4
MapReadWrite/g16
MapReadWrite/g64
ProducerConsumer/buf1
ProducerConsumer/buf16
ProducerConsumer/buf64
WorkerPool/g1
WorkerPool/g4
WorkerPool/g16
WorkerPool/g64
MemoryAllocation
MemoryConcurrent/g4
MemoryConcurrent/g16
EOF
}

WORKLOAD_MATRIX_CSV="$(workload_matrix | paste -sd, -)"

contention_workload() {
    case "$1" in
        MutexContention/*|ChannelPingPong|WaitGroupFanOut/*|MapReadWrite/*|ProducerConsumer/*|WorkerPool/*)
            return 0
            ;;
        *) return 1 ;;
    esac
}

steady_zero_alloc_workload() {
    case "$1" in
        RaceRead|RaceReadAlternating|RaceReadCollision|RaceWrite|RaceReadWrite|MutexLockUnlock|RWMutexReadLock)
            return 0
            ;;
        *) return 1 ;;
    esac
}

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

# Release evidence must come from an exact Git tree. Ignored benchmark output
# and build artifacts are intentionally excluded by Git's porcelain status.
# BENCHMARK_ALLOW_DIRTY_SOURCE_DEVELOPMENT is deliberately limited to quick
# local measurement; it never permits building, standalone receipt validation,
# or saving.
require_clean_source_tree() {
    local status
    command -v git >/dev/null 2>&1 || fail "git is required to verify source-tree cleanliness"
    status="$(git -C "${SOURCE_ROOT}" status --porcelain=v1 --untracked-files=all)" || fail "could not inspect source-tree cleanliness"
    if [[ -z "${status}" ]]; then
        return 0
    fi
    if [[ "${ACTION}" == run && "${QUICK_MODE}" == true && "${BENCHMARK_ALLOW_DIRTY_SOURCE_DEVELOPMENT:-false}" == true ]]; then
        echo "WARNING: measuring a dirty source tree under BENCHMARK_ALLOW_DIRTY_SOURCE_DEVELOPMENT" >&2
        return 0
    fi
    fail "source tree must be clean (including untracked files) before accepting benchmark evidence"
}

require_args() {
    local option="$1" required="$2" available="$3"
    (( available >= required )) || fail "${option} requires ${required} argument(s)"
}

select_action() {
	[[ "${ACTION}" == run ]] || fail "only one non-run action may be used"
	ACTION="$1"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --count)
            require_args "$1" 1 "$(( $# - 1 ))"
            COUNT="$2"
            RUN_OPTIONS=true
            shift 2
            ;;
        --benchtime)
            require_args "$1" 1 "$(( $# - 1 ))"
            BENCHTIME="$2"
            RUN_OPTIONS=true
            shift 2
            ;;
        --max-raceread-ratio)
            require_args "$1" 1 "$(( $# - 1 ))"
            MAX_RACEREAD_RATIO="$2"
            RUN_OPTIONS=true
            shift 2
            ;;
        --max-matrix-geomean)
            require_args "$1" 1 "$(( $# - 1 ))"
            MAX_MATRIX_GEOMEAN="$2"
            RUN_OPTIONS=true
            shift 2
            ;;
        --max-matrix-class-ratio)
            require_args "$1" 1 "$(( $# - 1 ))"
            MAX_MATRIX_CLASS_RATIO="$2"
            RUN_OPTIONS=true
            shift 2
            ;;
        --min-contention-throughput)
            require_args "$1" 1 "$(( $# - 1 ))"
            MIN_CONTENTION_THROUGHPUT="$2"
            RUN_OPTIONS=true
            shift 2
            ;;
        --max-p99-ratio)
            require_args "$1" 1 "$(( $# - 1 ))"
            MAX_MATRIX_P99_RATIO="$2"
            RUN_OPTIONS=true
            shift 2
            ;;
        --quick)
            COUNT=3
            BENCHTIME=500ms
            QUICK_MODE=true
            RUN_OPTIONS=true
            shift
            ;;
        --go)
            require_args "$1" 1 "$(( $# - 1 ))"
            FORK_GO="$2"
            RUN_OPTIONS=true
            shift 2
            ;;
        --save)
            require_args "$1" 1 "$(( $# - 1 ))"
            select_action save
            ACTION_ARG1="$2"
            shift 2
            ;;
        --diff)
            require_args "$1" 2 "$(( $# - 1 ))"
            select_action diff
            ACTION_ARG1="$2"
            ACTION_ARG2="$3"
            shift 3
            ;;
		--validate-pr-comparison)
			require_args "$1" 2 "$(( $# - 1 ))"
			select_action validate-pr-comparison
			ACTION_ARG1="$2"
			ACTION_ARG2="$3"
			shift 3
			;;
		--list)
			select_action list
			shift
			;;
		--validate-release-contract)
			require_args "$1" 1 "$(( $# - 1 ))"
			select_action validate-release-contract
			ACTION_ARG1="$2"
			shift 2
			;;
		--summarize-rss)
			require_args "$1" 2 "$(( $# - 1 ))"
			select_action summarize-rss
			ACTION_ARG1="$2"
			ACTION_ARG2="$3"
			shift 3
			;;
		--build-toolchain)
			require_args "$1" 1 "$(( $# - 1 ))"
			select_action build-toolchain
			ACTION_ARG1="$2"
			shift 2
			;;
		--validate-toolchain-build)
			require_args "$1" 1 "$(( $# - 1 ))"
			select_action validate-toolchain-build
			ACTION_ARG1="$2"
			shift 2
			;;
		--print-workload-matrix)
			select_action print-workload-matrix
			shift
			;;
        --help|-h)
            cat <<'USAGE'
Usage: run_comparison.sh [OPTIONS]

Run options:
  --count N                   Paired samples per configuration (default and
                              release minimum: 10)
  --benchtime T               Time per sample (default and release minimum: 1s)
  --max-raceread-ratio R      Maximum PureGo/TSAN BenchmarkRaceRead median
                              ratio (default and release gate: 1.00)
  --max-matrix-geomean R      Maximum complete-matrix PureGo/TSAN geomean
                              (default and release gate: 1.00)
  --max-matrix-class-ratio R  Maximum named-class PureGo/TSAN ratio
                              (default and release gate: 1.10)
  --min-contention-throughput R
                              Minimum PureGo/TSAN contention throughput ratio
                              (default and release gate: 0.90)
  --max-p99-ratio R           Maximum complete-matrix p99 latency ratio
                              (default and release gate: 1.25)
  --quick                     Development run: count=3, benchtime=500ms;
                              writes QUICK status and cannot be saved
  --go PATH                   This fork's bin/go

Baseline management (cannot be combined with run options):
  --save VERSION              Save the current validated result set
  --diff V1 V2                Compare saved pure-Go result sets
  --list                      List saved result sets

Evidence validation (cannot be combined with run options):
  --validate-release-contract FILE
                              Validate release-contract metadata and exit
  --summarize-rss DIR COUNT   Validate COUNT matched RSS samples in DIR, write
                              paired-delta files, and print key=value medians
  --validate-pr-comparison BASE PR
                              Validate two PureGo aggregate files and write
                              matrix gate evidence in the current directory
  --build-toolchain PATH      Run a fresh CGO_ENABLED=0 src/make.bash and, only
                              after it succeeds, attest this fork's PATH.
                              GOROOT_BOOTSTRAP or BOOTSTRAP_GO may select the
                              bootstrap toolchain.
  --validate-toolchain-build PATH
                              Validate PATH against its recorded build receipt
  --print-workload-matrix     Print the exact ordered benchmark workload and exit
USAGE
            exit 0
            ;;
        *)
            fail "unknown option: $1"
            ;;
    esac
done

resolve_executable() {
    local candidate="$1" dir
    if [[ "${candidate}" != */* ]]; then
        if ! candidate="$(command -v "${candidate}")"; then
            return 1
        fi
    fi
    [[ -n "${candidate}" && -x "${candidate}" ]] || return 1
    dir="$(cd "$(dirname "${candidate}")" && pwd -P)"
    printf '%s/%s\n' "${dir}" "$(basename "${candidate}")"
}

benchmark_manifest() {
    local input="$1" expected="$2" output="$3" tmp
    tmp="$(mktemp "${output}.tmp.XXXXXX")" || return 1
    if ! awk -v expected="${expected}" '
        function finite_decimal(value, allow_zero) {
            if (value !~ /^[0-9]+([.][0-9]+)?$/) return 0
            if ((value + 0) != (value + 0) || (value + 0) >= 1e308) return 0
            return allow_zero ? (value + 0) >= 0 : (value + 0) > 0
        }
        /^Benchmark/ {
            timed = bytes = allocs = 0
            for (i = 2; i <= NF; i++) {
                if ($i == "ns/op" || $i == "us/op" || $i == "µs/op" ||
                    $i == "ms/op" || $i == "s/op") {
                    timed++
                    if (i == 2 || !finite_decimal($(i-1), 0)) {
                        printf "invalid benchmark metric before %s: %s\n", $i, $0 > "/dev/stderr"
                        bad = 1
                    }
                }
                if ($i == "B/op") {
                    bytes++
                    if (i == 2 || !finite_decimal($(i-1), 1)) {
                        printf "invalid allocation metric before %s: %s\n", $i, $0 > "/dev/stderr"
                        bad = 1
                    }
                }
                if ($i == "allocs/op") {
                    allocs++
                    if (i == 2 || !finite_decimal($(i-1), 1)) {
                        printf "invalid allocation metric before %s: %s\n", $i, $0 > "/dev/stderr"
                        bad = 1
                    }
                }
            }
            if (timed != 1) {
                printf "invalid benchmark result line: %s\n", $0 > "/dev/stderr"
                bad = 1
                next
            }
            if (bytes != 1 || allocs != 1) {
                printf "benchmark result has no unique B/op and allocs/op metrics: %s\n", $0 > "/dev/stderr"
                bad = 1
            }
            samples[$1]++
            total++
        }
        END {
            if (total == 0) {
                print "benchmark output contains no samples" > "/dev/stderr"
                exit 1
            }
            for (name in samples) {
                if (expected > 0 && samples[name] != expected) {
                    printf "%s has %d samples; want %d\n", name, samples[name], expected > "/dev/stderr"
                    bad = 1
                }
                if (expected == 0) {
                    if (uniform == 0) uniform = samples[name]
                    if (samples[name] != uniform) {
                        printf "%s has %d samples; other benchmarks have %d\n", name, samples[name], uniform > "/dev/stderr"
                        bad = 1
                    }
                }
                printf "%s\t%d\n", name, samples[name]
            }
            if (bad) exit 1
        }
    ' "${input}" > "${tmp}.unsorted"; then
        rm -f "${tmp}" "${tmp}.unsorted"
        return 1
    fi
    if ! LC_ALL=C sort "${tmp}.unsorted" > "${tmp}"; then
        rm -f "${tmp}" "${tmp}.unsorted"
        return 1
    fi
    rm -f "${tmp}.unsorted" || { rm -f "${tmp}"; return 1; }
    [[ -s "${tmp}" ]] || { rm -f "${tmp}"; return 1; }
    mv "${tmp}" "${output}" || { rm -f "${tmp}"; return 1; }
}

source_fingerprint() {
    local paths manifest path required hash status=0 count=0
    for required in VERSION go.env benchmarks/go.mod; do
        [[ -f "${SOURCE_ROOT}/${required}" && -r "${SOURCE_ROOT}/${required}" ]] || return 1
    done
    paths="$(mktemp "${TMPDIR:-/tmp}/race-source-paths.XXXXXX")" || return 1
    manifest="$(mktemp "${TMPDIR:-/tmp}/race-source-manifest.XXXXXX")" || {
        rm -f "${paths}"
        return 1
    }
    # Start with every tracked or unignored source input. Add all files under
    # src and top-level benchmark Go files below as well: make.bash writes
    # ignored generated Go sources (for example zbootstrap.go), and those are
    # real build inputs even though git status intentionally omits them. Append
    # the required singleton inputs explicitly so ignore rules cannot omit them.
    if ! git -C "${SOURCE_ROOT}" ls-files --cached --others --exclude-standard -z -- \
        src 'benchmarks/*.go' benchmarks/go.mod VERSION go.env > "${paths}.unsorted"; then
        rm -f "${paths}" "${paths}.unsorted" "${manifest}"
        return 1
    fi
    if ! (
        cd "${SOURCE_ROOT}"
        find src -type f -print0
        find benchmarks -maxdepth 1 -type f -name '*.go' -print0
        printf '%s\0' VERSION go.env benchmarks/go.mod
    ) >> "${paths}.unsorted"; then
        rm -f "${paths}" "${paths}.unsorted" "${manifest}"
        return 1
    fi
    if ! LC_ALL=C sort -zu "${paths}.unsorted" > "${paths}"; then
        rm -f "${paths}" "${paths}.unsorted" "${manifest}"
        return 1
    fi
    rm -f "${paths}.unsorted" || {
        rm -f "${paths}" "${manifest}"
        return 1
    }
    while IFS= read -r -d '' path; do
        if [[ -e "${SOURCE_ROOT}/${path}" || -L "${SOURCE_ROOT}/${path}" ]]; then
            hash="$(git -C "${SOURCE_ROOT}" hash-object -- "${path}")" || { status=1; break; }
        elif git -C "${SOURCE_ROOT}" ls-files --error-unmatch -- "${path}" >/dev/null 2>&1; then
            # A tracked deletion is a real source-tree state. Preserve it in
            # the fingerprint instead of trying to hash a nonexistent file.
            hash=deleted
        else
            status=1
            break
        fi
        printf '%s\0%s\0' "${path}" "${hash}" >> "${manifest}" || { status=1; break; }
        count=$(( count + 1 ))
    done < "${paths}"
    rm -f "${paths}" || { rm -f "${manifest}"; return 1; }
    if (( status != 0 || count == 0 )); then
        rm -f "${manifest}"
        return 1
    fi
    git -C "${SOURCE_ROOT}" hash-object "${manifest}"
    status=$?
    rm -f "${manifest}" || return 1
    return "${status}"
}

toolchain_artifact_fingerprint() {
    local go_binary="$1" goroot goos goarch tool_dir include_dir paths manifest path hash
    local status=0 tool_count=0 include_count=0
    goroot="$("${go_binary}" env GOROOT)" || return 1
    goroot="$(cd "${goroot}" && pwd -P)" || return 1
    [[ "${goroot}" == "${SOURCE_ROOT}" ]] || return 1
    goos="$("${go_binary}" env GOOS)" || return 1
    goarch="$("${go_binary}" env GOARCH)" || return 1
    [[ "${goos}" =~ ^[A-Za-z0-9_]+$ && "${goarch}" =~ ^[A-Za-z0-9_]+$ ]] || return 1
    tool_dir="${goroot}/pkg/tool/${goos}_${goarch}"
    include_dir="${goroot}/pkg/include"
    [[ -d "${tool_dir}" && -r "${tool_dir}" && -x "${tool_dir}" ]] || return 1
    [[ -d "${include_dir}" && -r "${include_dir}" && -x "${include_dir}" ]] || return 1

    paths="$(mktemp "${TMPDIR:-/tmp}/race-toolchain-paths.XXXXXX")" || return 1
    manifest="$(mktemp "${TMPDIR:-/tmp}/race-toolchain-manifest.XXXXXX")" || {
        rm -f "${paths}"
        return 1
    }
    if ! (cd "${goroot}" && find "pkg/tool/${goos}_${goarch}" pkg/include -type f -print0 | LC_ALL=C sort -z) > "${paths}"; then
        rm -f "${paths}" "${manifest}"
        return 1
    fi
    while IFS= read -r -d '' path; do
        [[ -f "${goroot}/${path}" && -r "${goroot}/${path}" ]] || { status=1; break; }
        hash="$(git -C "${SOURCE_ROOT}" hash-object -- "${goroot}/${path}")" || { status=1; break; }
        printf '%s\0%s\0' "${path}" "${hash}" >> "${manifest}" || { status=1; break; }
        case "${path}" in
            pkg/tool/${goos}_${goarch}/*) tool_count=$(( tool_count + 1 )) ;;
            pkg/include/*) include_count=$(( include_count + 1 )) ;;
            *) status=1; break ;;
        esac
    done < "${paths}"
    rm -f "${paths}" || { rm -f "${manifest}"; return 1; }
    if (( status != 0 || tool_count == 0 || include_count == 0 )); then
        rm -f "${manifest}"
        return 1
    fi
    git -C "${SOURCE_ROOT}" hash-object "${manifest}"
    status=$?
    rm -f "${manifest}" || return 1
    return "${status}"
}

toolchain_attestation_content() {
    local go_binary="$1" source_hash binary_hash toolchain_hash goroot
    source_hash="$(source_fingerprint)" || return 1
    binary_hash="$(git -C "${SOURCE_ROOT}" hash-object -- "${go_binary}")" || return 1
    toolchain_hash="$(toolchain_artifact_fingerprint "${go_binary}")" || return 1
    goroot="$("${go_binary}" env GOROOT)" || return 1
    goroot="$(cd "${goroot}" && pwd -P)" || return 1
    [[ "${goroot}" == "${SOURCE_ROOT}" ]] || return 1
    printf 'format=2\nsource=%s\nbinary=%s\ntoolchain=%s\ngoroot=%s\n' \
        "${source_hash}" "${binary_hash}" "${toolchain_hash}" "${goroot}"
}

build_and_attest_toolchain() {
    local go_binary="$1" go_dir receipt tmp bootstrap_go bootstrap_goroot
    require_clean_source_tree
    mkdir -p "${SOURCE_ROOT}/bin" || fail "could not create toolchain output directory"
    go_dir="$(cd "$(dirname "${go_binary}")" && pwd -P)" || fail "toolchain output directory does not exist: ${go_binary}"
    go_binary="${go_dir}/$(basename "${go_binary}")"
    [[ "${go_binary}" == "${SOURCE_ROOT}/bin/go" ]] || fail "tool must be this source tree's bin/go: ${go_binary}"
    receipt="${go_binary}.benchmark-attestation"
    rm -f "${receipt}" || fail "could not remove stale toolchain build receipt"

    bootstrap_goroot="${GOROOT_BOOTSTRAP:-}"
    if [[ -z "${bootstrap_goroot}" ]]; then
        bootstrap_go="${BOOTSTRAP_GO:-go}"
        bootstrap_go="$(resolve_executable "${bootstrap_go}")" || fail "bootstrap go not found or not executable: ${bootstrap_go}"
        bootstrap_goroot="$("${bootstrap_go}" env GOROOT)" || fail "could not determine bootstrap GOROOT"
    fi
    bootstrap_goroot="$(cd "${bootstrap_goroot}" && pwd -P)" || fail "bootstrap GOROOT does not exist: ${bootstrap_goroot}"

    echo "Building fresh PureGo toolchain with GOROOT_BOOTSTRAP=${bootstrap_goroot}..."
    if ! (cd "${SOURCE_ROOT}/src" && GOROOT_BOOTSTRAP="${bootstrap_goroot}" CGO_ENABLED=0 ./make.bash); then
        fail "fresh PureGo make.bash failed; no toolchain receipt was written"
    fi
    [[ -x "${go_binary}" ]] || fail "fresh PureGo make.bash did not create ${go_binary}"
    require_clean_source_tree

    tmp="$(mktemp "${receipt}.tmp.XXXXXX")" || fail "could not create toolchain build receipt"
    if ! toolchain_attestation_content "${go_binary}" > "${tmp}"; then
        rm -f "${tmp}"
        fail "could not attest toolchain build"
    fi
    mv "${tmp}" "${receipt}" || { rm -f "${tmp}"; fail "could not install toolchain build receipt"; }
    echo "Recorded toolchain build receipt: ${receipt}"
}

validate_toolchain_build() {
    local go_binary="$1" receipt expected
    require_clean_source_tree
    go_binary="$(resolve_executable "${go_binary}")" || fail "fork go binary not found or not executable: ${go_binary}"
    [[ "${go_binary}" == "${SOURCE_ROOT}/bin/go" ]] || fail "tool must be this source tree's bin/go: ${go_binary}"
    receipt="${go_binary}.benchmark-attestation"
    [[ -s "${receipt}" ]] || fail "toolchain build receipt is missing: ${receipt}; rerun --build-toolchain"
    expected="$(mktemp "${TMPDIR:-/tmp}/race-toolchain-attestation.XXXXXX")" || fail "could not validate toolchain build receipt"
    if ! toolchain_attestation_content "${go_binary}" > "${expected}"; then
        rm -f "${expected}"
        fail "could not compute current toolchain source identity"
    fi
    if ! cmp -s "${receipt}" "${expected}"; then
        rm -f "${expected}"
        fail "toolchain build receipt does not match current source and toolchain artifacts; rerun --build-toolchain"
    fi
    rm -f "${expected}" || fail "could not clean up toolchain receipt validation"
}

validate_benchmark_output() {
    local label="$1" input="$2" expected="$3" manifest="$4"
    [[ -s "${input}" ]] || fail "${label} benchmark output is empty: ${input}"
    if grep -Eq '^FAIL([[:space:]]|$)|^--- FAIL:|WARNING: DATA RACE|fatal error:|runtime: fatal' "${input}"; then
        cat "${input}" >&2
        fail "${label} benchmark output contains a failure, race, or runtime fatal"
    fi
    grep -q '^PASS$' "${input}" || fail "${label} benchmark output has no PASS marker"
    benchmark_manifest "${input}" "${expected}" "${manifest}" || fail "${label} benchmark sample set is invalid"
}

compare_manifests() {
    local left_label="$1" left="$2" right_label="$3" right="$4"
    if ! cmp -s "${left}" "${right}"; then
        echo "Benchmark sample sets differ (${left_label} vs ${right_label}):" >&2
        echo "--- ${left_label}" >&2
        cat "${left}" >&2
        echo "--- ${right_label}" >&2
        cat "${right}" >&2
        fail "benchmark configurations must contain identical sample sets"
    fi
}

# A manifest only proves that rows were repeated consistently.  This second
# check proves that every named workload in the contract is present.  Keeping
# these checks separate makes the error for a truncated or accidentally
# filtered benchmark actionable instead of allowing a smaller geomean to pass.
validate_complete_matrix() {
    local manifest="$1" expected_count="$2" label="$3"
    [[ -r "${manifest}" ]] || fail "${label} benchmark manifest is missing: ${manifest}"
    [[ "${expected_count}" =~ ^[1-9][0-9]*$ ]] || fail "${label} matrix sample count is invalid: ${expected_count}"
    awk -v expected="${WORKLOAD_MATRIX_CSV}" -v want="${expected_count}" '
        function canonical(name) {
            sub(/^Benchmark/, "", name)
            sub(/-[0-9]+$/, "", name)
            return name
        }
        BEGIN {
            n = split(expected, names, ",")
            for (i = 1; i <= n; i++) wanted[names[i]] = 1
        }
        NF != 2 || $1 !~ /^Benchmark/ || $2 !~ /^[1-9][0-9]*$/ {
            bad = 1
            next
        }
        {
            name = canonical($1)
            if (!(name in wanted)) {
                printf "unexpected workload row: %s\n", $0 > "/dev/stderr"
                bad = 1
                next
            }
            if (++rows[name] != 1) {
                printf "duplicate workload row: %s\n", $0 > "/dev/stderr"
                bad = 1
            }
            if (($2 + 0) != (want + 0)) {
                printf "%s has %s samples; want %s\n", name, $2, want > "/dev/stderr"
                bad = 1
            }
        }
        END {
            if (NR == 0) bad = 1
            for (i = 1; i <= n; i++) {
                if (!(names[i] in rows)) {
                    printf "missing workload row: %s\n", names[i] > "/dev/stderr"
                    bad = 1
                }
            }
            if (length(rows) != n) bad = 1
            exit bad
        }
    ' "${manifest}" || fail "${label} benchmark matrix is incomplete or malformed"
}

# Parse raw Go benchmark output into one canonical row per sample.  In
# addition to ns/op this deliberately requires B/op and allocs/op: a run made
# without -benchmem is not valid release evidence even when its timings look
# plausible.  The parser accepts only ordinary finite decimal numbers so
# NaN, Inf, exponents, and unit-swapped fixtures are rejected deterministically.
extract_matrix() {
    local input="$1" output="$2" expected_count="$3" label="$4" tmp
    [[ -r "${input}" ]] || fail "${label} benchmark output is missing: ${input}"
    tmp="$(mktemp "${output}.tmp.XXXXXX")" || fail "could not create ${label} matrix parser output"
    if ! awk -v expected="${WORKLOAD_MATRIX_CSV}" -v want="${expected_count}" '
        function decimal(value, allow_zero) {
            if (value !~ /^[0-9]+([.][0-9]+)?$/) return 0
            number = value + 0
            if (number != number || number >= 1e308) return 0
            return allow_zero ? number >= 0 : number > 0
        }
        function canonical(name) {
            sub(/^Benchmark/, "", name)
            sub(/-[0-9]+$/, "", name)
            return name
        }
        BEGIN {
            n = split(expected, names, ",")
            for (i = 1; i <= n; i++) wanted[names[i]] = 1
            scale["ns/op"] = 1
            scale["us/op"] = 1000
            scale["µs/op"] = 1000
            scale["ms/op"] = 1000000
            scale["s/op"] = 1000000000
        }
        /^Benchmark/ {
            name = canonical($1)
            if (!(name in wanted)) {
                printf "unexpected workload row: %s\n", $0 > "/dev/stderr"
                bad = 1
                next
            }
            timed = bytes = allocs = 0
            for (i = 2; i <= NF; i++) {
                if ($i in scale) {
                    timed++
                    if (i == 2 || !decimal($(i - 1), 0)) {
                        printf "invalid timed metric in %s\n", $0 > "/dev/stderr"
                        bad = 1
                    } else {
                        ns = ($(i - 1) + 0) * scale[$i]
                    }
                }
                if ($i == "B/op") {
                    bytes++
                    if (i == 2 || !decimal($(i - 1), 1)) {
                        printf "invalid B/op metric in %s\n", $0 > "/dev/stderr"
                        bad = 1
                    } else {
                        byte_value = $(i - 1) + 0
                    }
                }
                if ($i == "allocs/op") {
                    allocs++
                    if (i == 2 || !decimal($(i - 1), 1)) {
                        printf "invalid allocs/op metric in %s\n", $0 > "/dev/stderr"
                        bad = 1
                    } else {
                        alloc_value = $(i - 1) + 0
                    }
                }
            }
            if (timed != 1 || bytes != 1 || allocs != 1 || ns != ns || ns <= 0 || ns >= 1e308) {
                printf "benchmark row lacks one unique finite timing/allocation triplet: %s\n", $0 > "/dev/stderr"
                bad = 1
                next
            }
            if (++rows[name] > want) {
                printf "%s has more than %s samples\n", name, want > "/dev/stderr"
                bad = 1
            }
            printf "%s\t%d\t%.17g\t%.17g\t%.17g\n", name, rows[name], ns, byte_value, alloc_value
            total++
        }
        END {
            if (total == 0) bad = 1
            for (i = 1; i <= n; i++) {
                if ((names[i] in rows) == 0 || rows[names[i]] != want) {
                    printf "%s has %d samples; want %s\n", names[i], rows[names[i]] + 0, want > "/dev/stderr"
                    bad = 1
                }
            }
            if (length(rows) != n) bad = 1
            exit bad
        }
    ' "${input}" > "${tmp}"; then
        rm -f "${tmp}"
        fail "${label} benchmark matrix metrics are invalid"
    fi
    [[ -s "${tmp}" ]] || { rm -f "${tmp}"; fail "${label} benchmark matrix metrics are empty"; }
    mv "${tmp}" "${output}" || { rm -f "${tmp}"; fail "could not install ${label} matrix metrics"; }
}

matrix_report() {
    local tsan_matrix="$1" purego_matrix="$2" output="$3" summary="$4" expected_count="$5" label="$6" tmp_report tmp_summary
    tmp_report="$(mktemp "${output}.tmp.XXXXXX")" || fail "could not create ${label} matrix report"
    tmp_summary="$(mktemp "${summary}.tmp.XXXXXX")" || { rm -f "${tmp_report}"; fail "could not create ${label} matrix summary"; }
    if ! awk -v expected="${WORKLOAD_MATRIX_CSV}" -v want="${expected_count}" \
        -v max_geo="${MAX_MATRIX_GEOMEAN}" -v max_class="${MAX_MATRIX_CLASS_RATIO}" \
        -v min_contention="${MIN_CONTENTION_THROUGHPUT}" -v max_p99="${MAX_MATRIX_P99_RATIO}" \
        -v report="${tmp_report}" -v summary="${tmp_summary}" -v label="${label}" '
        function finite(x) { return x == x && x < 1e308 && x > -1e308 }
        function median(a, key, n,    i,j,t) {
            for (i = 1; i <= n; i++) b[i] = a[key, i]
            for (i = 2; i <= n; i++) {
                t = b[i]; j = i - 1
                while (j >= 1 && b[j] > t) { b[j + 1] = b[j]; j-- }
                b[j + 1] = t
            }
            if (n % 2) return b[(n + 1) / 2]
            return (b[n / 2] + b[n / 2 + 1]) / 2
        }
        function p99(a, key, n,    i,j,t,rank) {
            for (i = 1; i <= n; i++) b[i] = a[key, i]
            for (i = 2; i <= n; i++) {
                t = b[i]; j = i - 1
                while (j >= 1 && b[j] > t) { b[j + 1] = b[j]; j-- }
                b[j + 1] = t
            }
            rank = int((99 * n + 99) / 100)
            if (rank < 1) rank = 1
            if (rank > n) rank = n
            return b[rank]
        }
        function expected_name(name,    i) {
            for (i = 1; i <= n; i++) if (names[i] == name) return 1
            return 0
        }
        function contention(name) {
            return name ~ /^MutexContention\// || name == "ChannelPingPong" ||
                name ~ /^WaitGroupFanOut\// || name ~ /^MapReadWrite\// ||
                name ~ /^ProducerConsumer\// || name ~ /^WorkerPool\//
        }
        function steady(name) {
            return name == "RaceRead" || name == "RaceReadAlternating" ||
                name == "RaceReadCollision" || name == "RaceWrite" ||
                name == "RaceReadWrite" || name == "MutexLockUnlock" ||
                name == "RWMutexReadLock"
        }
        BEGIN {
            n = split(expected, names, ",")
            for (i = 1; i <= n; i++) wanted[names[i]] = 1
            print "class\ttsan_median_ns\tpurego_median_ns\tlatency_ratio\ttsan_p99_ns\tpurego_p99_ns\tp99_ratio\ttsan_bytes_per_op\tpurego_bytes_per_op\ttsan_allocs_per_op\tpurego_allocs_per_op\tthroughput_ratio" > report
        }
        FNR == NR {
            if (NF != 5 || !expected_name($1) || $2 !~ /^[1-9][0-9]*$/ || $2 > want) bad = 1
            tcount[$1]++
            tn[$1, tcount[$1]] = $3
            tb[$1, tcount[$1]] = $4
            ta[$1, tcount[$1]] = $5
            next
        }
        {
            if (NF != 5 || !expected_name($1) || $2 !~ /^[1-9][0-9]*$/ || $2 > want) bad = 1
            pcount[$1]++
            pn[$1, pcount[$1]] = $3
            pb[$1, pcount[$1]] = $4
            pa[$1, pcount[$1]] = $5
        }
        END {
            if (bad) exit 1
            geo_sum = 0
            for (i = 1; i <= n; i++) {
                name = names[i]
                if (tcount[name] != want || pcount[name] != want) {
                    printf "%s has incomplete TSAN/PureGo rows\n", name > "/dev/stderr"
                    bad = 1
                    continue
                }
                tm = median(tn, name, tcount[name]); pm = median(pn, name, pcount[name])
                ttail = p99(tn, name, tcount[name]); ptail = p99(pn, name, pcount[name])
                tbytes = median(tb, name, tcount[name]); pbytes = median(pb, name, pcount[name])
                talloc = median(ta, name, tcount[name]); palloc = median(pa, name, pcount[name])
                if (!finite(tm) || !finite(pm) || tm <= 0 || pm <= 0 || !finite(ttail) || !finite(ptail)) { bad = 1; continue }
                ratio = pm / tm
                tail_ratio = ptail / ttail
                throughput = tm / pm
                if (!finite(ratio) || !finite(tail_ratio) || !finite(throughput) || ratio <= 0 || tail_ratio <= 0 || throughput <= 0) { bad = 1; continue }
                if (ratio > max_class) { printf "%s latency ratio %.17g exceeds %.17g\n", name, ratio, max_class > "/dev/stderr"; bad = 1 }
                if (tail_ratio > max_p99) { printf "%s p99 ratio %.17g exceeds %.17g\n", name, tail_ratio, max_p99 > "/dev/stderr"; bad = 1 }
                if (contention(name) && throughput < min_contention) { printf "%s contention throughput %.17g is below %.17g\n", name, throughput, min_contention > "/dev/stderr"; bad = 1 }
                if (steady(name) && (pbytes != 0 || palloc != 0 || tbytes != 0 || talloc != 0)) { printf "%s steady path is not allocation-free\n", name > "/dev/stderr"; bad = 1 }
                geo_sum += log(ratio)
                printf "%s\t%.17g\t%.17g\t%.17g\t%.17g\t%.17g\t%.17g\t%.17g\t%.17g\t%.17g\t%.17g\t%.17g\n", name, tm, pm, ratio, ttail, ptail, tail_ratio, tbytes, pbytes, talloc, palloc, throughput >> report
            }
            geomean = exp(geo_sum / n)
            if (!finite(geomean) || geomean > max_geo) { printf "complete-matrix geomean %.17g exceeds %.17g\n", geomean, max_geo > "/dev/stderr"; bad = 1 }
            printf "format=matrix-v1\nlabel=%s\nclasses=%d\ncomplete_matrix_geomean=%.17g\ncomplete_matrix_geomean_limit=%.17g\nclass_ratio_limit=%.17g\ncontention_throughput_floor=%.17g\np99_ratio_limit=%.17g\n" , label, n, geomean, max_geo, max_class, min_contention, max_p99 > summary
            if (bad) exit 1
        }
    ' "${tsan_matrix}" "${purego_matrix}"; then
        rm -f "${tmp_report}" "${tmp_summary}"
        fail "${label} complete-matrix metrics failed a release gate"
    fi
    mv "${tmp_report}" "${output}" || { rm -f "${tmp_report}" "${tmp_summary}"; fail "could not install ${label} matrix report"; }
    mv "${tmp_summary}" "${summary}" || { rm -f "${tmp_summary}"; fail "could not install ${label} matrix summary"; }
}

validate_rss_values() {
    local input="$1" expected="$2"
    [[ -f "${input}" && -r "${input}" ]] || fail "RSS sample input is missing or unreadable: ${input}"
    awk -v expected="${expected}" '
        NF != 1 || $1 !~ /^[1-9][0-9]*$/ { bad = 1 }
        END { exit bad || NR != expected }
    ' "${input}" || fail "${input} must contain exactly ${expected} single-field positive integer KiB rows"
}

rss_median() {
    local input="$1" expected="$2" kind="$3" sorted result
    case "${kind}" in
        positive)
            awk -v expected="${expected}" '
                NF != 1 || $1 !~ /^[1-9][0-9]*$/ { bad = 1 }
                END { exit bad || NR != expected }
            ' "${input}" || return 1
            ;;
        signed)
            awk -v expected="${expected}" '
                NF != 1 || $1 !~ /^-?[0-9]+$/ { bad = 1 }
                END { exit bad || NR != expected }
            ' "${input}" || return 1
            ;;
        *) return 1 ;;
    esac
    sorted="$(mktemp "${TMPDIR:-/tmp}/race-rss-median.XXXXXX")" || return 1
    if ! LC_ALL=C sort -n "${input}" > "${sorted}"; then
        rm -f "${sorted}"
        return 1
    fi
    if ! result="$(awk -v n="${expected}" '
        NR == int((n + 1) / 2) { left = $1 }
        NR == int((n + 2) / 2) { right = $1 }
        END {
            if (NR != n || left == "" || right == "") exit 1
            printf "%.17g\n", (left + right) / 2
        }
    ' "${sorted}")"; then
        rm -f "${sorted}"
        return 1
    fi
    rm -f "${sorted}" || return 1
    printf '%s\n' "${result}"
}

summarize_rss() {
    local dir="$1" count="$2" config input tmp
    local baseline_median tsan_median purego_median
    local tsan_baseline_median purego_baseline_median purego_tsan_median
    [[ "${count}" =~ ^[1-9][0-9]*$ ]] || fail "RSS sample count must be a positive integer: ${count}"
    [[ -d "${dir}" ]] || fail "RSS results directory does not exist: ${dir}"
    for config in baseline tsan purego; do
        input="${dir}/rss-${config}-kb.txt"
        validate_rss_values "${input}" "${count}"
    done

    tmp="$(mktemp -d "${dir}/.rss-summary.XXXXXX")" || fail "could not create temporary RSS summary directory"
    if ! paste "${dir}/rss-baseline-kb.txt" "${dir}/rss-tsan-kb.txt" "${dir}/rss-purego-kb.txt" | awk \
        -v tsan_baseline="${tmp}/rss-tsan-minus-baseline-kb.txt" \
        -v purego_baseline="${tmp}/rss-purego-minus-baseline-kb.txt" \
        -v purego_tsan="${tmp}/rss-purego-minus-tsan-kb.txt" '
            NF != 3 { bad = 1; next }
            {
                print $2 - $1 > tsan_baseline
                print $3 - $1 > purego_baseline
                print $3 - $2 > purego_tsan
            }
            END { if (bad) exit 1 }
        '; then
        rm -rf "${tmp}"
        fail "could not create matched RSS deltas"
    fi

    baseline_median="$(rss_median "${dir}/rss-baseline-kb.txt" "${count}" positive)" || { rm -rf "${tmp}"; fail "could not compute baseline RSS median"; }
    tsan_median="$(rss_median "${dir}/rss-tsan-kb.txt" "${count}" positive)" || { rm -rf "${tmp}"; fail "could not compute TSAN RSS median"; }
    purego_median="$(rss_median "${dir}/rss-purego-kb.txt" "${count}" positive)" || { rm -rf "${tmp}"; fail "could not compute PureGo RSS median"; }
    tsan_baseline_median="$(rss_median "${tmp}/rss-tsan-minus-baseline-kb.txt" "${count}" signed)" || { rm -rf "${tmp}"; fail "could not compute TSAN-minus-baseline RSS median"; }
    purego_baseline_median="$(rss_median "${tmp}/rss-purego-minus-baseline-kb.txt" "${count}" signed)" || { rm -rf "${tmp}"; fail "could not compute PureGo-minus-baseline RSS median"; }
    purego_tsan_median="$(rss_median "${tmp}/rss-purego-minus-tsan-kb.txt" "${count}" signed)" || { rm -rf "${tmp}"; fail "could not compute PureGo-minus-TSAN RSS median"; }

    for input in rss-tsan-minus-baseline-kb.txt rss-purego-minus-baseline-kb.txt rss-purego-minus-tsan-kb.txt; do
        mv "${tmp}/${input}" "${dir}/${input}" || { rm -rf "${tmp}"; fail "could not install ${input}"; }
    done
    rmdir "${tmp}" || fail "could not clean up temporary RSS summary directory"

    printf '%s\n' \
        "rss_samples=${count}" \
        'rss_workload=BenchmarkMemoryConcurrent/g16' \
        'rss_benchtime=3s' \
        "baseline_median_kb=${baseline_median}" \
        "tsan_median_kb=${tsan_median}" \
        "purego_median_kb=${purego_median}" \
        "tsan_minus_baseline_median_kb=${tsan_baseline_median}" \
        "purego_minus_baseline_median_kb=${purego_baseline_median}" \
        "purego_minus_tsan_median_kb=${purego_tsan_median}"
}

require_benchstat() {
    command -v benchstat >/dev/null 2>&1 || fail "benchstat not found; install golang.org/x/perf/cmd/benchstat"
}

validate_name() {
    [[ "$1" =~ ^[A-Za-z0-9._-]+$ ]] || fail "invalid baseline name: $1"
}

validate_release_metadata() {
    local file="$1" mode count seconds maximum rss_count rss_benchtime matrix_geo matrix_class contention_floor p99_ratio
    [[ -s "${file}" ]] || fail "release evidence metadata is missing: ${file}"
    mode="$(awk -F= '$1 == "mode" { print substr($0, length($1) + 2) }' "${file}")"
    count="$(awk -F= '$1 == "count" { print substr($0, length($1) + 2) }' "${file}")"
    seconds="$(awk -F= '$1 == "benchtime_seconds" { print substr($0, length($1) + 2) }' "${file}")"
    maximum="$(awk -F= '$1 == "max_raceread_ratio" { print substr($0, length($1) + 2) }' "${file}")"
    matrix_geo="$(awk -F= '$1 == "max_matrix_geomean" { print substr($0, length($1) + 2) }' "${file}")"
    matrix_class="$(awk -F= '$1 == "max_matrix_class_ratio" { print substr($0, length($1) + 2) }' "${file}")"
    contention_floor="$(awk -F= '$1 == "min_contention_throughput" { print substr($0, length($1) + 2) }' "${file}")"
    p99_ratio="$(awk -F= '$1 == "max_p99_ratio" { print substr($0, length($1) + 2) }' "${file}")"
    rss_count="$(awk -F= '$1 == "rss_count" { print substr($0, length($1) + 2) }' "${file}")"
    rss_benchtime="$(awk -F= '$1 == "rss_benchtime" { print substr($0, length($1) + 2) }' "${file}")"
    [[ "${mode}" == release ]] || fail "evidence mode is ${mode:-missing}, not release"
    if [[ ! "${count}" =~ ^[1-9][0-9]*$ ]] || (( count < 10 )); then
        fail "release evidence has count=${count:-missing}; want at least 10"
    fi
    [[ "${seconds}" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "release evidence has invalid benchtime_seconds=${seconds:-missing}; want a finite decimal"
    awk -v value="${seconds}" 'BEGIN { exit !(value >= 1) }' || fail "release evidence has benchtime_seconds=${seconds:-missing}; want at least 1"
    [[ "${maximum}" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "release evidence has invalid max_raceread_ratio=${maximum:-missing}; want a finite decimal"
    awk -v value="${maximum}" 'BEGIN { exit !(value > 0 && value <= 1) }' || fail "release evidence has max_raceread_ratio=${maximum:-missing}; want (0,1]"
    [[ "${matrix_geo}" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "release evidence has invalid max_matrix_geomean=${matrix_geo:-missing}; want a finite decimal"
    awk -v value="${matrix_geo}" 'BEGIN { exit !(value > 0 && value <= 1) }' || fail "release evidence has max_matrix_geomean=${matrix_geo:-missing}; want (0,1]"
    [[ "${matrix_class}" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "release evidence has invalid max_matrix_class_ratio=${matrix_class:-missing}; want a finite decimal"
    awk -v value="${matrix_class}" 'BEGIN { exit !(value > 0 && value <= 1.10) }' || fail "release evidence has max_matrix_class_ratio=${matrix_class:-missing}; want (0,1.10]"
    [[ "${contention_floor}" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "release evidence has invalid min_contention_throughput=${contention_floor:-missing}; want a finite decimal"
    awk -v value="${contention_floor}" 'BEGIN { exit !(value > 0 && value <= 0.90) }' || fail "release evidence has min_contention_throughput=${contention_floor:-missing}; want (0,0.90]"
    [[ "${p99_ratio}" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "release evidence has invalid max_p99_ratio=${p99_ratio:-missing}; want a finite decimal"
    awk -v value="${p99_ratio}" 'BEGIN { exit !(value > 0 && value <= 1.25) }' || fail "release evidence has max_p99_ratio=${p99_ratio:-missing}; want (0,1.25]"
    if [[ ! "${rss_count}" =~ ^[1-9][0-9]*$ ]] || (( rss_count < 10 )); then
        fail "release evidence has rss_count=${rss_count:-missing}; want at least 10"
    fi
    [[ "${rss_count}" == "${count}" ]] || fail "release evidence has rss_count=${rss_count}; want count=${count}"
    [[ "${rss_benchtime}" == 3s ]] || fail "release evidence has rss_benchtime=${rss_benchtime:-missing}; want exactly 3s"
}

validate_pr_comparison() {
    local base="$1" pr="$2" work count base_matrix pr_matrix report summary
    [[ -r "${base}" && -r "${pr}" ]] || fail "PR comparison inputs are missing or unreadable"
    work="$(mktemp -d "${TMPDIR:-/tmp}/race-pr-matrix.XXXXXX")" || fail "could not create PR matrix workspace"
    validate_benchmark_output base "${base}" 0 "${work}/base.samples"
    validate_benchmark_output PR "${pr}" 0 "${work}/pr.samples"
    compare_manifests base "${work}/base.samples" PR "${work}/pr.samples"
    count="$(awk 'NR == 1 { print $2; exit }' "${work}/base.samples")"
    [[ "${count}" =~ ^[1-9][0-9]*$ ]] || fail "PR comparison has no benchmark rows"
    (( count >= 10 )) || fail "PR comparison requires at least 10 samples per workload"
    validate_complete_matrix "${work}/base.samples" "${count}" base
    validate_complete_matrix "${work}/pr.samples" "${count}" PR
    extract_matrix "${base}" "${work}/base.tsv" "${count}" base
    extract_matrix "${pr}" "${work}/pr.tsv" "${count}" PR
    report="$(pwd -P)/pr-matrix-ratios.tsv"
    summary="$(pwd -P)/pr-matrix-summary.txt"
    if ! awk -v expected="${WORKLOAD_MATRIX_CSV}" -v want="${count}" -v report="${report}.tmp" -v summary="${summary}.tmp" '
        function median(a,key,n,    i,j,t) {
            for (i=1;i<=n;i++) b[i]=a[key,i]
            for (i=2;i<=n;i++) { t=b[i]; j=i-1; while(j>=1&&b[j]>t){b[j+1]=b[j];j--}; b[j+1]=t }
            return n%2 ? b[(n+1)/2] : (b[n/2]+b[n/2+1])/2
        }
        function p99(a,key,n,    i,j,t,r) {
            for (i=1;i<=n;i++) b[i]=a[key,i]
            for (i=2;i<=n;i++) { t=b[i]; j=i-1; while(j>=1&&b[j]>t){b[j+1]=b[j];j--}; b[j+1]=t }
            r=int((99*n+99)/100); if(r<1)r=1; if(r>n)r=n; return b[r]
        }
        function expected_name(name,    i) { for(i=1;i<=n;i++)if(names[i]==name)return 1; return 0 }
        BEGIN { n=split(expected,names,","); print "class\tbase_median_ns\tpr_median_ns\tpr_base_ratio\tbase_p99_ns\tpr_p99_ns\tp99_ratio" > report }
        FNR==NR { bc[$1]++; bn[$1,bc[$1]]=$3; next }
        { pc[$1]++; pn[$1,pc[$1]]=$3 }
        END {
            for(i=1;i<=n;i++){ name=names[i]; if(bc[name]!=want||pc[name]!=want){bad=1;continue}; bm=median(bn,name,want);pm=median(pn,name,want);bt=p99(bn,name,want);pt=p99(pn,name,want);r=pm/bm;tr=pt/bt;if(!(r>0&&r<1e308&&tr>0&&tr<1e308)){bad=1;continue};if(r>1.05){printf "%s PR/base ratio %.17g exceeds 1.05\n",name,r > "/dev/stderr";bad=1};if(tr>1.25){printf "%s PR/base p99 ratio %.17g exceeds 1.25\n",name,tr > "/dev/stderr";bad=1};printf "%s\t%.17g\t%.17g\t%.17g\t%.17g\t%.17g\t%.17g\n",name,bm,pm,r,bt,pt,tr >> report;geo+=log(r)}
            g=exp(geo/n); if(g>1.05){printf "PR/base complete-matrix geomean %.17g exceeds 1.05\n",g > "/dev/stderr";bad=1};printf "format=pr-matrix-v1\nclasses=%d\ncomplete_matrix_geomean=%.17g\ncomplete_matrix_geomean_limit=1.05\nclass_ratio_limit=1.05\np99_ratio_limit=1.25\n",n,g > summary; if(bad)exit 1
        }
    ' "${work}/base.tsv" "${work}/pr.tsv"; then
        rm -f "${report}.tmp" "${summary}.tmp"
        fail "PR/base complete-matrix regression gate failed"
    fi
    mv "${report}.tmp" "${report}" || fail "could not install PR matrix report"
    mv "${summary}.tmp" "${summary}" || fail "could not install PR matrix summary"
    rm -rf "${work}"
}

if [[ "${ACTION}" != run && "${RUN_OPTIONS}" == true ]]; then
    fail "run options cannot be combined with --${ACTION}"
fi

case "${ACTION}" in
	print-workload-matrix)
		workload_matrix
		exit 0
		;;
	build-toolchain)
		command -v git >/dev/null 2>&1 || fail "git is required to attest the toolchain"
		build_and_attest_toolchain "${ACTION_ARG1}"
		exit 0
		;;
	validate-toolchain-build)
		command -v git >/dev/null 2>&1 || fail "git is required to attest the toolchain"
		validate_toolchain_build "${ACTION_ARG1}"
		echo "Toolchain build receipt is valid"
		exit 0
		;;
	validate-release-contract)
		validate_release_metadata "${ACTION_ARG1}"
		exit 0
		;;
	validate-pr-comparison)
		validate_pr_comparison "${ACTION_ARG1}" "${ACTION_ARG2}"
		echo "Validated PR/base complete-matrix comparison"
		exit 0
		;;
	summarize-rss)
		summarize_rss "${ACTION_ARG1}" "${ACTION_ARG2}"
		exit 0
		;;
	list)
        echo "Available baselines:"
        found=false
        for path in "${BASELINES_DIR}"/*-purego.txt; do
            [[ -e "${path}" ]] || continue
            found=true
            name="$(basename "${path}")"
            echo "  ${name%-purego.txt}"
        done
        [[ "${found}" == true ]] || echo "  (none)"
        exit 0
        ;;
    save)
        require_clean_source_tree
        validate_name "${ACTION_ARG1}"
        [[ -f "${RESULTS_DIR}/status.txt" ]] || fail "no completed comparison status found"
        [[ "$(cat "${RESULTS_DIR}/status.txt")" == PASS ]] || fail "current comparison is not complete"
        validate_release_metadata "${RESULTS_DIR}/release-contract.txt"
        tmp="$(mktemp -d "${TMPDIR:-/tmp}/race-comparison-save.XXXXXX")"
        trap 'rm -rf "${tmp}"' EXIT
        saved_count="$(awk -F= '$1 == "count" { print $2; exit }' "${RESULTS_DIR}/release-contract.txt")"
        for config in baseline tsan purego; do
            validate_benchmark_output "${config}" "${RESULTS_DIR}/${config}.txt" 0 "${tmp}/${config}.samples"
            validate_complete_matrix "${tmp}/${config}.samples" "${saved_count}" "${config}"
            extract_matrix "${RESULTS_DIR}/${config}.txt" "${tmp}/${config}.tsv" "${saved_count}" "${config}"
        done
        matrix_report "${tmp}/tsan.tsv" "${tmp}/purego.tsv" "${tmp}/matrix-ratios.tsv" "${tmp}/matrix-summary.txt" "${saved_count}" saved-evidence
        compare_manifests baseline "${tmp}/baseline.samples" tsan "${tmp}/tsan.samples"
        compare_manifests baseline "${tmp}/baseline.samples" purego "${tmp}/purego.samples"
        awk -v want="${saved_count}" '$2 != want { exit 1 }' "${tmp}/baseline.samples" || fail "saved evidence contains inconsistent sample counts"
        mkdir -p "${BASELINES_DIR}"
        for config in baseline tsan purego; do
            cp "${RESULTS_DIR}/${config}.txt" "${BASELINES_DIR}/${ACTION_ARG1}-${config}.txt"
        done
        echo "Saved validated baselines as ${ACTION_ARG1}"
        exit 0
        ;;
    diff)
        validate_name "${ACTION_ARG1}"
        validate_name "${ACTION_ARG2}"
        require_benchstat
        tmp="$(mktemp -d "${TMPDIR:-/tmp}/race-comparison-diff.XXXXXX")"
        trap 'rm -rf "${tmp}"' EXIT
        left="${BASELINES_DIR}/${ACTION_ARG1}-purego.txt"
        right="${BASELINES_DIR}/${ACTION_ARG2}-purego.txt"
        validate_benchmark_output "${ACTION_ARG1}" "${left}" 0 "${tmp}/left.samples"
        validate_benchmark_output "${ACTION_ARG2}" "${right}" 0 "${tmp}/right.samples"
        compare_manifests "${ACTION_ARG1}" "${tmp}/left.samples" "${ACTION_ARG2}" "${tmp}/right.samples"
        benchstat "${left}" "${right}"
        exit 0
        ;;
esac

[[ "${COUNT}" =~ ^[1-9][0-9]*$ ]] || fail "--count must be a positive integer: ${COUNT}"
[[ "${BENCHTIME}" =~ ^([0-9]+([.][0-9]+)?)(ns|us|µs|ms|s|m|h)$ ]] || fail "invalid --benchtime value: ${BENCHTIME}"
awk -v value="${BENCHTIME}" 'BEGIN { exit !((value + 0) > 0) }' || fail "--benchtime must be positive: ${BENCHTIME}"
BENCHTIME_SECONDS="$(awk -v value="${BENCHTIME}" '
    BEGIN {
        number = value + 0
        unit = value
        sub(/^[0-9]+([.][0-9]+)?/, "", unit)
        scale["ns"] = 1e-9; scale["us"] = 1e-6; scale["µs"] = 1e-6
        scale["ms"] = 1e-3; scale["s"] = 1; scale["m"] = 60; scale["h"] = 3600
        if (!(unit in scale)) exit 1
        printf "%.17g\n", number * scale[unit]
    }
')" || fail "could not normalize --benchtime: ${BENCHTIME}"
[[ "${MAX_RACEREAD_RATIO}" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "--max-raceread-ratio must be a positive number: ${MAX_RACEREAD_RATIO}"
awk -v value="${MAX_RACEREAD_RATIO}" 'BEGIN { exit !(value > 0) }' || fail "--max-raceread-ratio must be positive: ${MAX_RACEREAD_RATIO}"
awk -v value="${MAX_RACEREAD_RATIO}" 'BEGIN { exit !(value <= 1) }' || fail "--max-raceread-ratio cannot exceed the release ceiling 1.00: ${MAX_RACEREAD_RATIO}"
[[ "${MAX_MATRIX_GEOMEAN}" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "--max-matrix-geomean must be a finite decimal: ${MAX_MATRIX_GEOMEAN}"
awk -v value="${MAX_MATRIX_GEOMEAN}" 'BEGIN { exit !(value > 0 && value <= 1) }' || fail "--max-matrix-geomean must be in (0,1]: ${MAX_MATRIX_GEOMEAN}"
[[ "${MAX_MATRIX_CLASS_RATIO}" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "--max-matrix-class-ratio must be a finite decimal: ${MAX_MATRIX_CLASS_RATIO}"
awk -v value="${MAX_MATRIX_CLASS_RATIO}" 'BEGIN { exit !(value > 0 && value <= 1.10) }' || fail "--max-matrix-class-ratio must be in (0,1.10]: ${MAX_MATRIX_CLASS_RATIO}"
[[ "${MIN_CONTENTION_THROUGHPUT}" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "--min-contention-throughput must be a finite decimal: ${MIN_CONTENTION_THROUGHPUT}"
awk -v value="${MIN_CONTENTION_THROUGHPUT}" 'BEGIN { exit !(value > 0 && value <= 0.90) }' || fail "--min-contention-throughput must be in (0,0.90]: ${MIN_CONTENTION_THROUGHPUT}"
[[ "${MAX_MATRIX_P99_RATIO}" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "--max-p99-ratio must be a finite decimal: ${MAX_MATRIX_P99_RATIO}"
awk -v value="${MAX_MATRIX_P99_RATIO}" 'BEGIN { exit !(value > 0 && value <= 1.25) }' || fail "--max-p99-ratio must be in (0,1.25]: ${MAX_MATRIX_P99_RATIO}"
if [[ "${QUICK_MODE}" == false ]]; then
    (( COUNT >= 10 )) || fail "release comparisons require --count at least 10 (use --quick for development)"
    awk -v value="${BENCHTIME_SECONDS}" 'BEGIN { exit !(value >= 1) }' || fail "release comparisons require --benchtime at least 1s (use --quick for development)"
fi

# Once a run request is syntactically valid, an earlier PASS must not survive
# a preflight failure and masquerade as evidence for this request.
if [[ -d "${RESULTS_DIR}" ]]; then
    echo FAIL > "${RESULTS_DIR}/status.txt"
    rm -f "${RESULTS_DIR}/summary.md"
fi

FORK_GO="$(resolve_executable "${FORK_GO}")" || fail "fork go binary not found or not executable: ${FORK_GO}"
require_benchstat
command -v git >/dev/null 2>&1 || fail "git is required to identify the tested revision"
require_clean_source_tree

FORK_GOROOT="$("${FORK_GO}" env GOROOT)"
[[ -d "${FORK_GOROOT}" ]] || fail "fork GOROOT does not exist: ${FORK_GOROOT}"
FORK_GOROOT="$(cd "${FORK_GOROOT}" && pwd -P)"
[[ "${FORK_GOROOT}" == "${SOURCE_ROOT}" ]] || fail "${FORK_GO} uses ${FORK_GOROOT}; want ${SOURCE_ROOT}"
[[ "${FORK_GO}" == "${FORK_GOROOT}/bin/go" ]] || fail "tool must be this GOROOT's bin/go: ${FORK_GO}"
validate_toolchain_build "${FORK_GO}"
FORK_REVISION="$(git -C "${FORK_GOROOT}" rev-parse --verify HEAD)"
[[ "${FORK_REVISION}" =~ ^[0-9a-f]{40}$ ]] || fail "could not identify fork revision"

backend_manifest() {
    local cgo_enabled="$1"
    CGO_ENABLED="${cgo_enabled}" "${FORK_GO}" list -race -f \
        'GoFiles={{join .GoFiles ","}} CgoFiles={{join .CgoFiles ","}} SysoFiles={{join .SysoFiles ","}}' runtime/race
}

purego_backend="$(backend_manifest 0)"
tsan_backend="$(backend_manifest 1)"
[[ "${purego_backend}" == *race_kolkov_import.go* ]] || fail "CGO_ENABLED=0 did not select Kolkov source: ${purego_backend}"
[[ "${purego_backend}" == *'CgoFiles= SysoFiles=' ]] || fail "pure-Go backend includes cgo or TSAN objects: ${purego_backend}"
[[ "${tsan_backend}" != *race_kolkov_import.go* ]] || fail "CGO_ENABLED=1 selected Kolkov source: ${tsan_backend}"
[[ "${tsan_backend}" == *'SysoFiles=race_'*.syso* ]] || fail "CGO_ENABLED=1 did not select a TSAN system object: ${tsan_backend}"

cc="$(CGO_ENABLED=1 "${FORK_GO}" env CC)"
cc_command="${cc%% *}"
command -v "${cc_command}" >/dev/null 2>&1 || fail "TSAN C compiler not found: ${cc}"

GNU_TIME_BIN="${GNU_TIME_BIN:-}"
if [[ -n "${GNU_TIME_BIN}" ]]; then
    GNU_TIME_BIN="$(resolve_executable "${GNU_TIME_BIN}")" || fail "GNU_TIME_BIN is not executable"
else
    for candidate in /usr/bin/time gtime; do
        if resolved="$(resolve_executable "${candidate}" 2>/dev/null)"; then
            if "${resolved}" --version 2>&1 | grep -qi 'GNU time'; then
                GNU_TIME_BIN="${resolved}"
                break
            fi
        fi
    done
fi
[[ -n "${GNU_TIME_BIN}" ]] || fail "GNU time is required for RSS measurement (install gtime on macOS)"
"${GNU_TIME_BIN}" --version 2>&1 | grep -qi 'GNU time' || fail "not a GNU time executable: ${GNU_TIME_BIN}"

if command -v nproc >/dev/null 2>&1; then
    LOGICAL_CPUS="$(nproc)"
elif command -v sysctl >/dev/null 2>&1 && LOGICAL_CPUS="$(sysctl -n hw.logicalcpu 2>/dev/null)"; then
    :
else
    LOGICAL_CPUS="$(getconf _NPROCESSORS_ONLN)"
fi
[[ "${LOGICAL_CPUS}" =~ ^[1-9][0-9]*$ ]] || fail "could not determine logical CPU count: ${LOGICAL_CPUS:-missing}"
BENCH_GOMAXPROCS="${BENCH_GOMAXPROCS:-${GOMAXPROCS:-${LOGICAL_CPUS}}}"
[[ "${BENCH_GOMAXPROCS}" =~ ^[1-9][0-9]*$ ]] || fail "BENCH_GOMAXPROCS must be a positive integer: ${BENCH_GOMAXPROCS}"
if [[ -r /proc/cpuinfo ]]; then
    CPU_MODEL="$(awk -F: '/^(model name|Hardware)[[:space:]]*:/ { sub(/^[[:space:]]*/, "", $2); print $2; exit }' /proc/cpuinfo)"
elif command -v sysctl >/dev/null 2>&1 && CPU_MODEL="$(sysctl -n machdep.cpu.brand_string 2>/dev/null)"; then
    :
else
    CPU_MODEL="$(uname -m)"
fi
[[ -n "${CPU_MODEL}" ]] || CPU_MODEL="$(uname -m)"

rm -rf "${RESULTS_DIR}"
FORK_STATUS="$(git -C "${FORK_GOROOT}" status --porcelain --untracked-files=normal)"
mkdir -p "${RESULTS_DIR}/raw/baseline" "${RESULTS_DIR}/raw/tsan" "${RESULTS_DIR}/raw/purego"
echo RUNNING > "${RESULTS_DIR}/status.txt"
printf '%s\n' "${FORK_STATUS}" > "${RESULTS_DIR}/source-status.txt"
cat > "${RESULTS_DIR}/backend-identity.txt" <<EOF
format=backend-identity-v1
source_revision=${FORK_REVISION}
purego_cgo_enabled=0
purego_race=true
purego_manifest=${purego_backend}
tsan_cgo_enabled=1
tsan_race=true
tsan_manifest=${tsan_backend}
EOF
if [[ -z "${FORK_STATUS}" ]]; then
    SOURCE_STATE=clean
else
    SOURCE_STATE="dirty (including untracked files; see source-status.txt)"
fi
if [[ "${QUICK_MODE}" == true ]]; then
    RUN_MODE=quick
else
    RUN_MODE=release
fi
{
    echo "mode=${RUN_MODE}"
    echo "count=${COUNT}"
    echo "benchtime=${BENCHTIME}"
    echo "benchtime_seconds=${BENCHTIME_SECONDS}"
    echo "max_raceread_ratio=${MAX_RACEREAD_RATIO}"
    echo "max_matrix_geomean=${MAX_MATRIX_GEOMEAN}"
    echo "max_matrix_class_ratio=${MAX_MATRIX_CLASS_RATIO}"
    echo "min_contention_throughput=${MIN_CONTENTION_THROUGHPUT}"
    echo "max_p99_ratio=${MAX_MATRIX_P99_RATIO}"
    echo "rss_count=${COUNT}"
    echo "rss_benchtime=3s"
} > "${RESULTS_DIR}/release-contract.txt"

BASELINE_BIN="${RESULTS_DIR}/benchmark-baseline.test"
TSAN_BIN="${RESULTS_DIR}/benchmark-tsan.test"
PUREGO_BIN="${RESULTS_DIR}/benchmark-purego.test"

on_exit() {
    local status=$?
    trap - EXIT
    if ! rm -f "${BASELINE_BIN}" "${TSAN_BIN}" "${PUREGO_BIN}"; then
        echo "ERROR: failed to remove prebuilt benchmark binaries" >&2
        status=1
    fi
    if (( status == 0 )) && [[ "${QUICK_MODE}" == true ]]; then
        echo QUICK > "${RESULTS_DIR}/status.txt"
    elif (( status == 0 )); then
        echo PASS > "${RESULTS_DIR}/status.txt"
    else
        echo FAIL > "${RESULTS_DIR}/status.txt"
    fi
    exit "${status}"
}
trap on_exit EXIT

build_binary() {
    local label="$1" cgo_enabled="$2" race_enabled="$3" output="$4"
    local args=(test -c -o "${output}")
    [[ "${race_enabled}" == false ]] || args+=(-race)
    args+=(.)
    validate_toolchain_build "${FORK_GO}"
    echo "Building ${label} benchmark binary..."
    (cd "${SCRIPT_DIR}" && CGO_ENABLED="${cgo_enabled}" "${FORK_GO}" "${args[@]}")
    [[ -x "${output}" ]] || fail "${label} benchmark binary was not created"
    validate_toolchain_build "${FORK_GO}"
}

build_binary BASELINE 0 false "${BASELINE_BIN}"
build_binary TSAN 1 true "${TSAN_BIN}"
build_binary PUREGO 0 true "${PUREGO_BIN}"
validate_toolchain_build "${FORK_GO}"

baseline_id="$("${FORK_GO}" tool buildid "${BASELINE_BIN}")"
tsan_id="$("${FORK_GO}" tool buildid "${TSAN_BIN}")"
purego_id="$("${FORK_GO}" tool buildid "${PUREGO_BIN}")"
[[ -n "${baseline_id}" && -n "${tsan_id}" && -n "${purego_id}" ]] || fail "one or more benchmark binaries have no build ID"
[[ "${baseline_id}" != "${tsan_id}" && "${baseline_id}" != "${purego_id}" && "${tsan_id}" != "${purego_id}" ]] || fail "benchmark configurations produced duplicate binaries"
baseline_hash="$(git -C "${SOURCE_ROOT}" hash-object -- "${BASELINE_BIN}")" || fail "could not hash baseline benchmark binary"
tsan_hash="$(git -C "${SOURCE_ROOT}" hash-object -- "${TSAN_BIN}")" || fail "could not hash TSAN benchmark binary"
purego_hash="$(git -C "${SOURCE_ROOT}" hash-object -- "${PUREGO_BIN}")" || fail "could not hash PureGo benchmark binary"

verify_race_binary_symbols() {
    local label="$1" binary="$2" expected="$3" evidence="$4" nm_output
    nm_output="$(mktemp "${TMPDIR:-/tmp}/race-binary-nm.XXXXXX")" || fail "could not inspect ${label} benchmark symbols"
    if ! "${FORK_GO}" tool nm "${binary}" > "${nm_output}"; then
        rm -f "${nm_output}"
        fail "fork go tool nm could not inspect exact ${label} benchmark binary"
    fi
    case "${expected}" in
        purego)
            if grep -Eq '__tsan|runtime/cgo' "${nm_output}"; then
                rm -f "${nm_output}"
                fail "exact PureGo benchmark binary contains TSAN or runtime/cgo symbols"
            fi
            ;;
        tsan)
            if ! grep -q '__tsan' "${nm_output}"; then
                rm -f "${nm_output}"
                fail "exact TSAN benchmark binary contains no __tsan symbols"
            fi
            ;;
        *)
            rm -f "${nm_output}"
            fail "unknown race binary symbol contract: ${expected}"
            ;;
    esac
    {
        printf '%s exact binary: buildid=%s hash=%s\n' "${label}" \
            "$("${FORK_GO}" tool buildid "${binary}")" \
            "$(git -C "${SOURCE_ROOT}" hash-object -- "${binary}")"
        if [[ "${expected}" == purego ]]; then
            echo 'fork go tool nm check: no __tsan or runtime/cgo symbols'
            echo 'Selected race symbols:'
            awk '/runtime[.]race/ { print; if (++found == 40) exit }' "${nm_output}"
        else
            echo 'fork go tool nm check: __tsan symbols present'
            echo 'Selected TSAN symbols:'
            awk '/__tsan/ { print; if (++found == 40) exit }' "${nm_output}"
        fi
    } > "${evidence}"
    rm -f "${nm_output}" || fail "could not clean up ${label} symbol inspection"
}

verify_race_binary_symbols TSAN "${TSAN_BIN}" tsan "${RESULTS_DIR}/symbols-tsan.txt"
verify_race_binary_symbols PureGo "${PUREGO_BIN}" purego "${RESULTS_DIR}/symbols-purego.txt"
require_clean_source_tree

{
    echo "=== Environment ==="
    echo "Date:          $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "OS:            $(uname -s) $(uname -r)"
    echo "CPU:           ${CPU_MODEL}"
    echo "Logical CPUs:  ${LOGICAL_CPUS}"
    echo "GOMAXPROCS:    ${BENCH_GOMAXPROCS} (explicit for benchmark and RSS processes)"
    echo "Fork Go:       $("${FORK_GO}" version)"
    echo "Fork GOROOT:   ${FORK_GOROOT}"
    echo "Toolchain:     source/bin/go/native-tool build receipt verified"
    echo "Source HEAD:   ${FORK_REVISION}"
    echo "Source status: ${SOURCE_STATE}"
    echo "Evidence mode: ${RUN_MODE}"
    echo "Warm-up:      warmup_benchtime=100ms (one validated run per backend and phase; raw evidence only)"
    echo "Sample order: sample_order=all-six-permutations"
    echo "C compiler:    $("${cc_command}" --version | awk 'NR == 1 { print; exit }')"
    echo "GOOS/GOARCH:   $("${FORK_GO}" env GOOS)/$("${FORK_GO}" env GOARCH)"
    echo "Baseline:      CGO_ENABLED=0 race=false buildid=${baseline_id} hash=${baseline_hash}"
    echo "TSAN:          CGO_ENABLED=1 race=true buildid=${tsan_id} hash=${tsan_hash} ${tsan_backend}"
    echo "PureGo:        CGO_ENABLED=0 race=true buildid=${purego_id} hash=${purego_hash} ${purego_backend}"
    echo "Build IDs and content hashes identify the measured binaries; the toolchain receipt attests provenance."
} > "${RESULTS_DIR}/environment.txt"
cat "${RESULTS_DIR}/environment.txt"

: > "${RESULTS_DIR}/baseline.txt"
: > "${RESULTS_DIR}/tsan.txt"
: > "${RESULTS_DIR}/purego.txt"
: > "${RESULTS_DIR}/raceread-tsan-ns.txt"
: > "${RESULTS_DIR}/raceread-purego-ns.txt"
: > "${RESULTS_DIR}/sample-order.txt"

benchmark_binary() {
    case "$1" in
        baseline) printf '%s\n' "${BASELINE_BIN}" ;;
        tsan) printf '%s\n' "${TSAN_BIN}" ;;
        purego) printf '%s\n' "${PUREGO_BIN}" ;;
        *) fail "unknown benchmark configuration: $1" ;;
    esac
}

run_latency_warmup() {
    local label="$1" binary output manifest
    binary="$(benchmark_binary "${label}")"
    output="${RESULTS_DIR}/raw/${label}/warmup-latency.txt"
    manifest="$(mktemp "${TMPDIR:-/tmp}/race-latency-warmup.XXXXXX")" || fail "could not validate ${label} latency warm-up"
    echo "Warming ${label} latency workload for 100ms..."
    if ! GOMAXPROCS="${BENCH_GOMAXPROCS}" "${binary}" -test.run='^$' -test.bench='.' -test.benchmem \
        -test.benchtime=100ms -test.count=1 -test.timeout="${TIMEOUT}" \
        > "${output}" 2>&1; then
        cat "${output}" >&2
        rm -f "${manifest}"
        fail "${label} latency warm-up failed"
    fi
    if ! validate_benchmark_output "${label} latency warm-up" "${output}" 1 "${manifest}"; then
        rm -f "${manifest}"
        return 1
    fi
    rm -f "${manifest}" || fail "could not clean up ${label} latency warm-up manifest"
}

run_sample() {
    local label="$1" sample="$2" binary output aggregate value_file
    binary="$(benchmark_binary "${label}")"
    output="${RESULTS_DIR}/raw/${label}/${sample}.txt"
    aggregate="${RESULTS_DIR}/${label}.txt"
    echo "Running ${label} sample ${sample}/${COUNT}..."
    printf '%s %s\n' "${sample}" "${label}" >> "${RESULTS_DIR}/sample-order.txt"
    if ! GOMAXPROCS="${BENCH_GOMAXPROCS}" "${binary}" -test.run='^$' -test.bench='.' -test.benchmem \
        -test.benchtime="${BENCHTIME}" -test.count=1 -test.timeout="${TIMEOUT}" \
        > "${output}" 2>&1; then
        cat "${output}" >&2
        fail "${label} sample ${sample} failed"
    fi
    validate_benchmark_output "${label} sample ${sample}" "${output}" 1 "${output}.samples"
    cat "${output}" >> "${aggregate}"
    if [[ "${label}" == tsan || "${label}" == purego ]]; then
        value_file="${RESULTS_DIR}/raceread-${label}-ns.txt"
        awk '
            function positive_decimal(value) {
                return value ~ /^[0-9]+([.][0-9]+)?$/ && (value + 0) > 0
            }
            /^BenchmarkRaceRead(-[0-9]+)?[[:space:]]/ {
                for (i = 2; i <= NF; i++) {
                    if ($i == "ns/op") {
                        if (!positive_decimal($(i-1))) {
                            printf "invalid BenchmarkRaceRead ns/op value: %s\n", $(i-1) > "/dev/stderr"
                            bad = 1
                            next
                        }
                        print $(i-1)
                        found++
                    }
                }
            }
            END { if (bad || found != 1) exit 1 }
        ' "${output}" >> "${value_file}" || fail "${label} sample ${sample} has no unique BenchmarkRaceRead ns/op value"
    fi
}

for label in $(comparison_order 1); do
    run_latency_warmup "${label}"
done

for (( sample = 1; sample <= COUNT; sample++ )); do
    sample_name="$(printf '%03d' "${sample}")"
    for label in $(comparison_order "${sample}"); do
        run_sample "${label}" "${sample_name}"
    done
done

for config in baseline tsan purego; do
    validate_benchmark_output "${config}" "${RESULTS_DIR}/${config}.txt" "${COUNT}" "${RESULTS_DIR}/${config}.samples"
    validate_complete_matrix "${RESULTS_DIR}/${config}.samples" "${COUNT}" "${config}"
done
compare_manifests baseline "${RESULTS_DIR}/baseline.samples" tsan "${RESULTS_DIR}/tsan.samples"
compare_manifests baseline "${RESULTS_DIR}/baseline.samples" purego "${RESULTS_DIR}/purego.samples"

extract_matrix "${RESULTS_DIR}/tsan.txt" "${RESULTS_DIR}/matrix-tsan.tsv" "${COUNT}" TSAN
extract_matrix "${RESULTS_DIR}/purego.txt" "${RESULTS_DIR}/matrix-purego.tsv" "${COUNT}" PureGo
extract_matrix "${RESULTS_DIR}/baseline.txt" "${RESULTS_DIR}/matrix-baseline.tsv" "${COUNT}" baseline
matrix_report "${RESULTS_DIR}/matrix-tsan.tsv" "${RESULTS_DIR}/matrix-purego.tsv" \
    "${RESULTS_DIR}/matrix-ratios.tsv" "${RESULTS_DIR}/matrix-summary.txt" "${COUNT}" TSAN-PureGo
cp "${RESULTS_DIR}/matrix-ratios.tsv" "${RESULTS_DIR}/matrix-tsan-purego.tsv" || fail "could not retain TSAN/PureGo matrix report"

median() {
    local input="$1" count sorted
    count="$(wc -l < "${input}" | tr -d '[:space:]')"
    [[ "${count}" == "${COUNT}" ]] || fail "${input} contains ${count} values; want ${COUNT}"
    awk '
        function positive_decimal(value) {
            return value ~ /^[0-9]+([.][0-9]+)?$/ && (value + 0) > 0
        }
        NF != 1 || !positive_decimal($1) { exit 1 }
    ' "${input}" || fail "${input} contains a non-finite or non-positive metric"
    sorted="${input%.txt}.sorted.txt"
    LC_ALL=C sort -n "${input}" > "${sorted}" || fail "could not sort ${input}"
    awk -v n="${count}" '
        NR == int((n + 1) / 2) { left = $1 }
        NR == int((n + 2) / 2) { right = $1 }
        END {
            if (NR != n || left == "" || right == "") exit 1
            printf "%.17g\n", (left + right) / 2
        }
    ' "${sorted}"
}

tsan_median="$(median "${RESULTS_DIR}/raceread-tsan-ns.txt")" || fail "could not compute TSAN BenchmarkRaceRead median"
purego_median="$(median "${RESULTS_DIR}/raceread-purego-ns.txt")" || fail "could not compute PureGo BenchmarkRaceRead median"
raceread_ratio_raw="$(awk -v pure="${purego_median}" -v tsan="${tsan_median}" 'BEGIN { if (tsan <= 0) exit 1; printf "%.17g\n", pure / tsan }')" || fail "could not compute BenchmarkRaceRead median ratio"
[[ "${raceread_ratio_raw}" =~ ^[0-9]+([.][0-9]+)?$ ]] || fail "BenchmarkRaceRead median ratio is not finite and positive: ${raceread_ratio_raw}"
awk -v value="${raceread_ratio_raw}" 'BEGIN { exit !(value > 0) }' || fail "BenchmarkRaceRead median ratio is not finite and positive: ${raceread_ratio_raw}"
raceread_ratio_display="$(awk -v ratio="${raceread_ratio_raw}" 'BEGIN { printf "%.6f\n", ratio }')"
{
    echo "BenchmarkRaceRead TSAN median ns/op: ${tsan_median}"
    echo "BenchmarkRaceRead PureGo median ns/op: ${purego_median}"
    echo "BenchmarkRaceRead PureGo/TSAN ratio: ${raceread_ratio_display}"
    echo "BenchmarkRaceRead PureGo/TSAN ratio (raw gate value): ${raceread_ratio_raw}"
    echo "Engineering target: <=0.90"
    echo "Release gate: <=${MAX_RACEREAD_RATIO}"
} > "${RESULTS_DIR}/raceread_ratio.txt"
cat "${RESULTS_DIR}/raceread_ratio.txt"
awk -v ratio="${raceread_ratio_raw}" -v maximum="${MAX_RACEREAD_RATIO}" 'BEGIN { exit !(ratio <= maximum) }' || fail "BenchmarkRaceRead PureGo/TSAN ratio ${raceread_ratio_raw} exceeds ${MAX_RACEREAD_RATIO}"

echo "Running benchstat comparisons..."
benchstat "tsan=${RESULTS_DIR}/tsan.txt" "purego=${RESULTS_DIR}/purego.txt" > "${RESULTS_DIR}/benchstat_tsan_vs_purego.txt"
benchstat "baseline=${RESULTS_DIR}/baseline.txt" "tsan=${RESULTS_DIR}/tsan.txt" "purego=${RESULTS_DIR}/purego.txt" > "${RESULTS_DIR}/benchstat_all.txt"
# benchstat's text formatter strips the conventional Benchmark prefix in
# current releases; older releases retained it. Accept both spellings while
# requiring the exact top-level RaceRead row rather than a similarly named
# sub-benchmark.
grep -Eq '^[[:space:]]*(Benchmark)?RaceRead(-[0-9]+)?[[:space:]]' "${RESULTS_DIR}/benchstat_tsan_vs_purego.txt" || fail "TSAN/PureGo benchstat output is incomplete"
grep -Eq '^[[:space:]]*(Benchmark)?RaceRead(-[0-9]+)?[[:space:]]' "${RESULTS_DIR}/benchstat_all.txt" || fail "all-configuration benchstat output is incomplete"

measure_peak_rss() {
    local label="$1" sample="$2" binary stdout_file time_file manifest peak_kb
    local aggregate_stdout aggregate_time sample_file
    binary="$(benchmark_binary "${label}")"
    stdout_file="${RESULTS_DIR}/raw/${label}/rss-${sample}.stdout"
    time_file="${RESULTS_DIR}/raw/${label}/rss-${sample}.time"
    manifest="${stdout_file}.samples"
    aggregate_stdout="${RESULTS_DIR}/rss-${label}.stdout"
    aggregate_time="${RESULTS_DIR}/rss-${label}.time"
    sample_file="${RESULTS_DIR}/rss-${label}-kb.txt"
    echo "Running ${label} RSS sample ${sample}/${COUNT}..."
    printf '%s %s\n' "${sample}" "${label}" >> "${RESULTS_DIR}/rss-sample-order.txt"
    if ! GOMAXPROCS="${BENCH_GOMAXPROCS}" "${GNU_TIME_BIN}" -v "${binary}" -test.run='^$' \
        -test.bench='^BenchmarkMemoryConcurrent$/^g16$' -test.benchmem \
        -test.benchtime=3s -test.count=1 -test.timeout=2m \
        > "${stdout_file}" 2> "${time_file}"; then
        cat "${stdout_file}" >&2
        cat "${time_file}" >&2
        fail "${label} RSS sample ${sample} workload failed"
    fi
    if grep -Eq '^FAIL([[:space:]]|$)|^--- FAIL:|WARNING: DATA RACE|fatal error:|runtime: fatal' "${stdout_file}" "${time_file}"; then
        cat "${stdout_file}" >&2
        cat "${time_file}" >&2
        fail "${label} RSS sample ${sample} workload reported a failure"
    fi
    benchmark_manifest "${stdout_file}" 1 "${manifest}" || fail "${label} RSS sample ${sample} benchmark row is invalid"
    awk 'NR != 1 || $1 !~ /^BenchmarkMemoryConcurrent\/g16(-[0-9]+)?$/ || $2 != 1 { bad = 1 } END { exit bad || NR != 1 }' \
        "${manifest}" || fail "${label} RSS sample ${sample} did not produce the exact BenchmarkMemoryConcurrent/g16 row"
    awk '$0 == "PASS" { found++ } END { exit found != 1 }' "${stdout_file}" || fail "${label} RSS sample ${sample} did not produce exactly one PASS marker"
    peak_kb="$(awk -F: '
        $1 ~ /^[[:space:]]*Maximum resident set size \(kbytes\)[[:space:]]*$/ {
            value = $2
            gsub(/[[:space:]]/, "", value)
            found++
        }
        END { if (found == 1) print value }
    ' "${time_file}")"
    [[ "${peak_kb}" =~ ^[1-9][0-9]*$ ]] || fail "invalid ${label} RSS sample ${sample} maximum: ${peak_kb:-missing}"
    cat "${stdout_file}" >> "${aggregate_stdout}" || fail "could not append ${label} RSS stdout"
    cat "${time_file}" >> "${aggregate_time}" || fail "could not append ${label} RSS time output"
    printf '%s\n' "${peak_kb}" >> "${sample_file}" || fail "could not append ${label} RSS sample"
}

run_rss_warmup() {
    local label="$1" binary stdout_file time_file manifest
    binary="$(benchmark_binary "${label}")"
    stdout_file="${RESULTS_DIR}/raw/${label}/warmup-rss.stdout"
    time_file="${RESULTS_DIR}/raw/${label}/warmup-rss.time"
    manifest="$(mktemp "${TMPDIR:-/tmp}/race-rss-warmup.XXXXXX")" || fail "could not validate ${label} RSS warm-up"
    echo "Warming ${label} RSS workload for 100ms..."
    if ! GOMAXPROCS="${BENCH_GOMAXPROCS}" "${GNU_TIME_BIN}" -v "${binary}" -test.run='^$' \
        -test.bench='^BenchmarkMemoryConcurrent$/^g16$' -test.benchmem \
        -test.benchtime=100ms -test.count=1 -test.timeout=2m \
        > "${stdout_file}" 2> "${time_file}"; then
        cat "${stdout_file}" >&2
        cat "${time_file}" >&2
        rm -f "${manifest}"
        fail "${label} RSS warm-up workload failed"
    fi
    if grep -Eq '^FAIL([[:space:]]|$)|^--- FAIL:|WARNING: DATA RACE|fatal error:|runtime: fatal' "${stdout_file}" "${time_file}"; then
        cat "${stdout_file}" >&2
        cat "${time_file}" >&2
        rm -f "${manifest}"
        fail "${label} RSS warm-up workload reported a failure"
    fi
    if ! benchmark_manifest "${stdout_file}" 1 "${manifest}"; then
        rm -f "${manifest}"
        fail "${label} RSS warm-up benchmark row is invalid"
    fi
    awk 'NR != 1 || $1 !~ /^BenchmarkMemoryConcurrent\/g16(-[0-9]+)?$/ || $2 != 1 { bad = 1 } END { exit bad || NR != 1 }' \
        "${manifest}" || { rm -f "${manifest}"; fail "${label} RSS warm-up did not produce the exact BenchmarkMemoryConcurrent/g16 row"; }
    awk '$0 == "PASS" { found++ } END { exit found != 1 }' "${stdout_file}" || {
        rm -f "${manifest}"
        fail "${label} RSS warm-up did not produce exactly one PASS marker"
    }
    rm -f "${manifest}" || fail "could not clean up ${label} RSS warm-up manifest"
}

for file in \
    rss-sample-order.txt \
    rss-baseline.stdout rss-tsan.stdout rss-purego.stdout \
    rss-baseline.time rss-tsan.time rss-purego.time \
    rss-baseline-kb.txt rss-tsan-kb.txt rss-purego-kb.txt \
    rss-tsan-minus-baseline-kb.txt rss-purego-minus-baseline-kb.txt rss-purego-minus-tsan-kb.txt \
    memory_rss.txt; do
    : > "${RESULTS_DIR}/${file}"
done

for label in $(comparison_order 1); do
    run_rss_warmup "${label}"
done

for (( sample = 1; sample <= COUNT; sample++ )); do
    sample_name="$(printf '%03d' "${sample}")"
    for label in $(comparison_order "${sample}"); do
        measure_peak_rss "${label}" "${sample_name}"
    done
done
summarize_rss "${RESULTS_DIR}" "${COUNT}" > "${RESULTS_DIR}/memory_rss.txt"

verify_benchmark_binary() {
    local label="$1" binary="$2" expected="$3" actual
    actual="$(git -C "${SOURCE_ROOT}" hash-object -- "${binary}")" || fail "could not rehash ${label} benchmark binary"
    [[ "${actual}" == "${expected}" ]] || fail "${label} benchmark binary changed while evidence was collected"
}

verify_benchmark_binary baseline "${BASELINE_BIN}" "${baseline_hash}"
verify_benchmark_binary TSAN "${TSAN_BIN}" "${tsan_hash}"
verify_benchmark_binary PureGo "${PUREGO_BIN}" "${purego_hash}"
validate_toolchain_build "${FORK_GO}"
verify_race_binary_symbols TSAN "${TSAN_BIN}" tsan "${RESULTS_DIR}/symbols-tsan.txt"
verify_race_binary_symbols PureGo "${PUREGO_BIN}" purego "${RESULTS_DIR}/symbols-purego.txt"
require_clean_source_tree

{
    echo "## Benchmark Results: Pure-Go Race Detector vs TSAN"
    echo
    echo '```'
    cat "${RESULTS_DIR}/environment.txt"
    echo '```'
    echo
    echo "**Method:** exact prebuilt binaries, ${COUNT} paired samples at ${BENCHTIME}; sample_order=all-six-permutations."
    echo "**Warm-up:** warmup_benchtime=100ms, one validated run per backend before each phase; retained only as raw evidence."
    echo "**Evidence mode:** ${RUN_MODE}. Quick-mode success is recorded as \`QUICK\`, not \`PASS\`."
    echo
    echo '### BenchmarkRaceRead release gate'
    echo '```'
    cat "${RESULTS_DIR}/raceread_ratio.txt"
    echo '```'
    echo
    echo '### TSAN vs PureGo'
    echo '```'
    cat "${RESULTS_DIR}/benchstat_tsan_vs_purego.txt"
    echo '```'
    echo
    echo '### Complete workload matrix gates'
    echo '```'
    cat "${RESULTS_DIR}/matrix-summary.txt"
    cat "${RESULTS_DIR}/matrix-ratios.tsv"
    echo '```'
    echo
    echo '### All configurations'
    echo '```'
    cat "${RESULTS_DIR}/benchstat_all.txt"
    echo '```'
    echo
    echo '### Repeated peak RSS (exact prebuilt binaries)'
    echo '```'
    cat "${RESULTS_DIR}/memory_rss.txt"
    echo '```'
} > "${RESULTS_DIR}/summary.md"

echo "Validated results written to ${RESULTS_DIR}"
