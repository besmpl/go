#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
HARNESS="${SCRIPT_DIR}/../run_comparison.sh"
ORDER_HELPER="${SCRIPT_DIR}/benchmark_order.sh"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/race-comparison-test.XXXXXX")"
trap 'rm -rf "${TMP_ROOT}"' EXIT

case_number=0
FIXTURE=

test_comparison_order() {
    local sample output expected position backend count

    output="$(bash -c 'source "$1"' _ "${ORDER_HELPER}")"
    [[ -z "${output}" ]] || {
        echo 'benchmark_order.sh is not source-safe' >&2
        exit 1
    }

    output="$({
        source "${ORDER_HELPER}"
        for (( sample = 1; sample <= 12; sample++ )); do
            comparison_order "${sample}"
        done
    })"
    expected="$(cat <<'EOF'
baseline tsan purego
tsan purego baseline
purego baseline tsan
baseline purego tsan
purego tsan baseline
tsan baseline purego
baseline tsan purego
tsan purego baseline
purego baseline tsan
baseline purego tsan
purego tsan baseline
tsan baseline purego
EOF
)"
    [[ "${output}" == "${expected}" ]] || {
        echo 'comparison order did not produce the exact first 12 triplets' >&2
        printf '%s\n' "${output}" >&2
        exit 1
    }

    for position in 1 2 3; do
        for backend in baseline tsan purego; do
            count="$(head -n 6 <<<"${output}" | awk -v position="${position}" -v backend="${backend}" '$position == backend { count++ } END { print count + 0 }')"
            [[ "${count}" == 2 ]] || {
                echo "${backend} appeared ${count} times in position ${position}; want 2" >&2
                exit 1
            }
        done
    done

    # Quick mode's three samples place each backend once in every position.
    # The default ten-sample release prefix differs by at most one, so the
    # incomplete second cycle cannot reintroduce a systematic order bias.
    for prefix in 3 10; do
        for position in 1 2 3; do
            local minimum=100 maximum=0
            for backend in baseline tsan purego; do
                count="$(head -n "${prefix}" <<<"${output}" | awk -v position="${position}" -v backend="${backend}" '$position == backend { count++ } END { print count + 0 }')"
                if (( count < minimum )); then
                    minimum=${count}
                fi
                if (( count > maximum )); then
                    maximum=${count}
                fi
            done
            (( maximum - minimum <= 1 )) || {
                echo "first ${prefix} samples are imbalanced in position ${position}: min=${minimum} max=${maximum}" >&2
                exit 1
            }
        done
    done

    for invalid in 0 -1 1.0 text 01; do
        if bash -c 'source "$1"; comparison_order "$2"' _ "${ORDER_HELPER}" "${invalid}" >/dev/null 2>&1; then
            echo "comparison_order accepted invalid sample ${invalid}" >&2
            exit 1
        fi
    done
    if bash -c 'source "$1"; comparison_order' _ "${ORDER_HELPER}" >/dev/null 2>&1 ||
        bash -c 'source "$1"; comparison_order 1 2' _ "${ORDER_HELPER}" >/dev/null 2>&1; then
        echo 'comparison_order accepted an invalid argument count' >&2
        exit 1
    fi
}

make_fixture() {
    local seconds="$1" maximum="$2" fixture output config sample

    case_number=$(( case_number + 1 ))
    fixture="${TMP_ROOT}/case-${case_number}"
    mkdir -p "${fixture}/benchmarks/results" "${fixture}/benchmarks/baselines" "${fixture}/benchmarks/scripts"
    cp "${HARNESS}" "${fixture}/benchmarks/run_comparison.sh"
    cp "${ORDER_HELPER}" "${fixture}/benchmarks/scripts/benchmark_order.sh"

    output="${fixture}/benchmarks/results/sample.txt"
    for (( sample = 1; sample <= 10; sample++ )); do
        printf 'BenchmarkRaceRead-8 %d 10 ns/op\n' "${sample}" >> "${output}"
    done
    printf 'PASS\nok  \tbenchmarks\t0.1s\n' >> "${output}"
    for config in baseline tsan purego; do
        cp "${output}" "${fixture}/benchmarks/results/${config}.txt"
    done

    echo PASS > "${fixture}/benchmarks/results/status.txt"
    cat > "${fixture}/benchmarks/results/release-contract.txt" <<EOF
mode=release
count=10
benchtime=1s
benchtime_seconds=${seconds}
max_raceread_ratio=${maximum}
rss_count=10
rss_benchtime=3s
EOF
    FIXTURE="${fixture}"
}

test_rss_summarizer() {
    local dir output expected

    case_number=$(( case_number + 1 ))
    dir="${TMP_ROOT}/case-${case_number}"
    mkdir -p "${dir}"
    printf '100\n200\n300\n' > "${dir}/rss-baseline-kb.txt"
    printf '110\n180\n330\n' > "${dir}/rss-tsan-kb.txt"
    printf '90\n250\n310\n' > "${dir}/rss-purego-kb.txt"
    output="$(bash "${HARNESS}" --summarize-rss "${dir}" 3)"
    expected="$(cat <<'EOF'
rss_samples=3
rss_workload=BenchmarkMemoryConcurrent/g16
rss_benchtime=3s
baseline_median_kb=200
tsan_median_kb=180
purego_median_kb=250
tsan_minus_baseline_median_kb=10
purego_minus_baseline_median_kb=10
purego_minus_tsan_median_kb=-20
EOF
)"
    [[ "${output}" == "${expected}" ]] || {
        echo 'odd RSS summary did not contain the exact keys and medians' >&2
        printf '%s\n' "${output}" >&2
        exit 1
    }
    [[ "$(cat "${dir}/rss-tsan-minus-baseline-kb.txt")" == $'10\n-20\n30' ]]
    [[ "$(cat "${dir}/rss-purego-minus-baseline-kb.txt")" == $'-10\n50\n10' ]]
    [[ "$(cat "${dir}/rss-purego-minus-tsan-kb.txt")" == $'-20\n70\n-20' ]]

    case_number=$(( case_number + 1 ))
    dir="${TMP_ROOT}/case-${case_number}"
    mkdir -p "${dir}"
    printf '10\n20\n30\n40\n' > "${dir}/rss-baseline-kb.txt"
    printf '5\n25\n35\n55\n' > "${dir}/rss-tsan-kb.txt"
    printf '20\n15\n50\n45\n' > "${dir}/rss-purego-kb.txt"
    output="$(bash "${HARNESS}" --summarize-rss "${dir}" 4)"
    expected="$(cat <<'EOF'
rss_samples=4
rss_workload=BenchmarkMemoryConcurrent/g16
rss_benchtime=3s
baseline_median_kb=25
tsan_median_kb=30
purego_median_kb=32.5
tsan_minus_baseline_median_kb=5
purego_minus_baseline_median_kb=7.5
purego_minus_tsan_median_kb=2.5
EOF
)"
    [[ "${output}" == "${expected}" ]] || {
        echo 'even RSS summary did not contain the exact keys and medians' >&2
        printf '%s\n' "${output}" >&2
        exit 1
    }
    for invalid_count in 0 3.0 text; do
        if output="$(bash "${HARNESS}" --summarize-rss "${dir}" "${invalid_count}" 2>&1)"; then
            echo "RSS summarizer accepted invalid COUNT=${invalid_count}" >&2
            exit 1
        fi
        grep -F 'RSS sample count must be a positive integer' <<<"${output}" >/dev/null
    done
    if output="$(bash "${HARNESS}" --summarize-rss "${dir}" 4 --count 10 2>&1)"; then
        echo 'RSS summarizer accepted a run option' >&2
        exit 1
    fi
    grep -F 'run options cannot be combined with --summarize-rss' <<<"${output}" >/dev/null
}

expect_invalid_rss_summary() {
    local kind="$1" output dir

    case_number=$(( case_number + 1 ))
    dir="${TMP_ROOT}/case-${case_number}"
    mkdir -p "${dir}"
    printf '10\n20\n30\n' > "${dir}/rss-baseline-kb.txt"
    printf '11\n21\n31\n' > "${dir}/rss-tsan-kb.txt"
    printf '12\n22\n32\n' > "${dir}/rss-purego-kb.txt"
    case "${kind}" in
        wrong-count) printf '11\n21\n' > "${dir}/rss-tsan-kb.txt" ;;
        zero) printf '0\n20\n30\n' > "${dir}/rss-baseline-kb.txt" ;;
        decimal) printf '10\n20.5\n30\n' > "${dir}/rss-baseline-kb.txt" ;;
        malformed) printf '10 extra\n20\n30\n' > "${dir}/rss-baseline-kb.txt" ;;
        missing) rm "${dir}/rss-purego-kb.txt" ;;
        *) echo "unknown invalid RSS fixture: ${kind}" >&2; exit 1 ;;
    esac
    if output="$(bash "${HARNESS}" --summarize-rss "${dir}" 3 2>&1)"; then
        echo "RSS summarizer accepted ${kind} input" >&2
        exit 1
    fi
    [[ "${output}" == ERROR:* ]] || {
        echo "RSS summarizer produced no fail-closed diagnostic for ${kind}" >&2
        printf '%s\n' "${output}" >&2
        exit 1
    }
    for file in rss-tsan-minus-baseline-kb.txt rss-purego-minus-baseline-kb.txt rss-purego-minus-tsan-kb.txt; do
        [[ ! -e "${dir}/${file}" ]] || {
            echo "RSS summarizer wrote ${file} for invalid ${kind} input" >&2
            exit 1
        }
    done
}

expect_invalid_rss_metadata() {
    local field="$1" value="$2" diagnostic="$3" fixture output metadata

    make_fixture 1 1
    fixture="${FIXTURE}"
    metadata="${fixture}/benchmarks/results/release-contract.txt"
    if [[ "${value}" == __missing__ ]]; then
        sed "/^${field}=/d" "${metadata}" > "${metadata}.new"
    else
        sed "s/^${field}=.*/${field}=${value}/" "${metadata}" > "${metadata}.new"
    fi
    mv "${metadata}.new" "${metadata}"
    if output="$(bash "${fixture}/benchmarks/run_comparison.sh" --validate-release-contract "${metadata}" 2>&1)"; then
        echo "validator accepted invalid ${field}=${value}" >&2
        exit 1
    fi
    grep -F "${diagnostic}" <<<"${output}" >/dev/null || {
        echo "wrong ${field} validator diagnostic for ${value}:" >&2
        echo "${output}" >&2
        exit 1
    }
    if output="$(bash "${fixture}/benchmarks/run_comparison.sh" --save invalid 2>&1)"; then
        echo "save accepted invalid ${field}=${value}" >&2
        exit 1
    fi
    grep -F "${diagnostic}" <<<"${output}" >/dev/null
}

expect_invalid_metadata() {
    local field="$1" value="$2" seconds=1 maximum=1 fixture output display

    case "${field}" in
        benchtime_seconds) seconds="${value}" ;;
        max_raceread_ratio) maximum="${value}" ;;
        *) echo "unknown metadata field: ${field}" >&2; exit 1 ;;
    esac

	make_fixture "${seconds}" "${maximum}"
	fixture="${FIXTURE}"
	if output="$(bash "${fixture}/benchmarks/run_comparison.sh" --validate-release-contract "${fixture}/benchmarks/results/release-contract.txt" 2>&1)"; then
		echo "validator accepted invalid ${field}=${value}" >&2
		exit 1
	fi
	display="${value:-missing}"
	grep -F "invalid ${field}=${display}; want a finite decimal" <<<"${output}" >/dev/null || {
		echo "wrong validator diagnostic for ${field}=${value}:" >&2
		echo "${output}" >&2
		exit 1
	}
	if output="$(bash "${fixture}/benchmarks/run_comparison.sh" --save invalid 2>&1)"; then
		echo "accepted invalid ${field}=${value}" >&2
		exit 1
	fi
	grep -F "invalid ${field}=${display}; want a finite decimal" <<<"${output}" >/dev/null || {
        echo "wrong diagnostic for ${field}=${value}:" >&2
        echo "${output}" >&2
        exit 1
    }
    if find "${fixture}/benchmarks/baselines" -type f -print -quit | grep -q .; then
        echo "saved a baseline for invalid ${field}=${value}" >&2
        exit 1
    fi
}

expect_invalid_benchmark() {
    local kind="$1" replacement="$2" fixture output

    make_fixture 1 1
    fixture="${FIXTURE}"
    case "${kind}" in
        unit)
            sed '1s/ns\/op/widgets\/op/' "${fixture}/benchmarks/results/purego.txt" > "${fixture}/invalid.txt"
            ;;
        value)
            sed "1s/10 ns\/op/${replacement} ns\/op/" "${fixture}/benchmarks/results/purego.txt" > "${fixture}/invalid.txt"
            ;;
        *) echo "unknown benchmark corruption: ${kind}" >&2; exit 1 ;;
    esac
    mv "${fixture}/invalid.txt" "${fixture}/benchmarks/results/purego.txt"

    if output="$(bash "${fixture}/benchmarks/run_comparison.sh" --save invalid 2>&1)"; then
        echo "accepted invalid benchmark ${kind}=${replacement}" >&2
        exit 1
    fi
    grep -F 'purego benchmark sample set is invalid' <<<"${output}" >/dev/null || {
        echo "wrong invalid benchmark diagnostic for ${kind}=${replacement}:" >&2
        echo "${output}" >&2
        exit 1
    }
}

expect_wrong_sample_count() {
    local fixture output

    make_fixture 1 1
    fixture="${FIXTURE}"
    sed '1d' "${fixture}/benchmarks/results/purego.txt" > "${fixture}/short.txt"
    mv "${fixture}/short.txt" "${fixture}/benchmarks/results/purego.txt"
    if output="$(bash "${fixture}/benchmarks/run_comparison.sh" --save invalid 2>&1)"; then
        echo "accepted mismatched benchmark sample counts" >&2
        exit 1
    fi
    grep -F 'benchmark configurations must contain identical sample sets' <<<"${output}" >/dev/null || {
        echo "wrong mismatched sample diagnostic:" >&2
        echo "${output}" >&2
        exit 1
    }
}

expect_manifest_sort_failure() {
    local fixture output fake_bin

    make_fixture 1 1
    fixture="${FIXTURE}"
    fake_bin="${fixture}/fake-bin"
    mkdir -p "${fake_bin}"
    cat > "${fake_bin}/sort" <<'EOF'
#!/usr/bin/env bash
exit 17
EOF
    chmod +x "${fake_bin}/sort"
    if output="$(PATH="${fake_bin}:${PATH}" bash "${fixture}/benchmarks/run_comparison.sh" --save invalid 2>&1)"; then
        echo "accepted benchmark evidence after manifest sorting failed" >&2
        exit 1
    fi
    grep -F 'benchmark sample set is invalid' <<<"${output}" >/dev/null || {
        echo "wrong manifest-sort failure diagnostic:" >&2
        echo "${output}" >&2
        exit 1
    }
    if find "${fixture}/benchmarks/baselines" -type f -print -quit | grep -q .; then
        echo "saved a baseline after manifest sorting failed" >&2
        exit 1
    fi
}

expect_runtime_fatal() {
    local marker="$1" fixture output

    make_fixture 1 1
    fixture="${FIXTURE}"
    printf '%s\n' "${marker}" >> "${fixture}/benchmarks/results/purego.txt"
    if output="$(bash "${fixture}/benchmarks/run_comparison.sh" --save invalid 2>&1)"; then
        echo "accepted benchmark evidence containing ${marker}" >&2
        exit 1
    fi
    grep -F 'benchmark output contains a failure, race, or runtime fatal' <<<"${output}" >/dev/null || {
        echo "wrong runtime-fatal diagnostic for ${marker}:" >&2
        echo "${output}" >&2
        exit 1
    }
}

test_toolchain_attestation() {
    local fixture output bootstrap

    case_number=$(( case_number + 1 ))
    fixture="${TMP_ROOT}/case-${case_number}"
    bootstrap="${fixture}/bootstrap"
    mkdir -p "${fixture}/benchmarks/scripts" "${fixture}/bin" "${fixture}/src" \
        "${fixture}/pkg/include" "${fixture}/pkg/tool/testos_testarch" "${bootstrap}"
    cp "${HARNESS}" "${fixture}/benchmarks/run_comparison.sh"
    cp "${ORDER_HELPER}" "${fixture}/benchmarks/scripts/benchmark_order.sh"
    printf 'package benchmarks\n' > "${fixture}/benchmarks/race_bench_test.go"
    printf 'module benchmarks\n\ngo 1.24\n' > "${fixture}/benchmarks/go.mod"
    printf 'go1.26-devel_fixture\n' > "${fixture}/VERSION"
    printf '# fixture go environment\n' > "${fixture}/go.env"
    printf 'package runtime\n' > "${fixture}/src/runtime.go"
    cat > "${fixture}/bin/go" <<'EOF'
#!/usr/bin/env bash
if [[ "$1" == env ]]; then
    case "${2:-}" in
        GOROOT) cd "$(dirname "$0")/.." && pwd -P ;;
        GOOS) echo testos ;;
        GOARCH) echo testarch ;;
        *) exit 1 ;;
    esac
    exit 0
fi
exit 1
EOF
    chmod +x "${fixture}/bin/go"
    cat > "${fixture}/src/make.bash" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "${CGO_ENABLED:-}" == 0 ]]
[[ -n "${GOROOT_BOOTSTRAP:-}" && -d "${GOROOT_BOOTSTRAP}" ]]
if [[ "${FAIL_BUILD:-}" == 1 ]]; then
    exit 17
fi
printf 'fresh compiler artifact\n' > ../pkg/tool/testos_testarch/'compile tool'
printf 'fresh generated assembler header\n' > ../pkg/include/textflag.h
printf '# rebuilt by fixture make.bash\n' >> ../bin/go
printf 'make.bash completed\n' > ../make-ran
EOF
    chmod +x "${fixture}/src/make.bash"
    printf 'stale compiler artifact\n' > "${fixture}/pkg/tool/testos_testarch/compile tool"
    git -C "${fixture}" init -q
    git -C "${fixture}" add VERSION go.env benchmarks/go.mod benchmarks/race_bench_test.go src/runtime.go

    GOROOT_BOOTSTRAP="${bootstrap}" bash "${fixture}/benchmarks/run_comparison.sh" --build-toolchain "${fixture}/bin/go" >/dev/null
    [[ -s "${fixture}/make-ran" ]]
    grep -F 'fresh compiler artifact' "${fixture}/pkg/tool/testos_testarch/compile tool" >/dev/null
    bash "${fixture}/benchmarks/run_comparison.sh" --validate-toolchain-build "${fixture}/bin/go" >/dev/null

    printf '// source changed after build\n' >> "${fixture}/src/runtime.go"
    if output="$(bash "${fixture}/benchmarks/run_comparison.sh" --validate-toolchain-build "${fixture}/bin/go" 2>&1)"; then
        echo "accepted a toolchain receipt for changed source" >&2
        exit 1
    fi
    grep -F 'receipt does not match current source and toolchain artifacts' <<<"${output}" >/dev/null

    git -C "${fixture}" checkout -- src/runtime.go
    printf '// module changed after build\n' >> "${fixture}/benchmarks/go.mod"
    if output="$(bash "${fixture}/benchmarks/run_comparison.sh" --validate-toolchain-build "${fixture}/bin/go" 2>&1)"; then
        echo "accepted a toolchain receipt for changed benchmarks/go.mod" >&2
        exit 1
    fi
    grep -F 'receipt does not match current source and toolchain artifacts' <<<"${output}" >/dev/null

    git -C "${fixture}" checkout -- benchmarks/go.mod
    rm -f "${fixture}/benchmarks/go.mod"
    if output="$(bash "${fixture}/benchmarks/run_comparison.sh" --validate-toolchain-build "${fixture}/bin/go" 2>&1)"; then
        echo "accepted a toolchain receipt with a missing benchmarks/go.mod" >&2
        exit 1
    fi
    grep -F 'could not compute current toolchain source identity' <<<"${output}" >/dev/null

    git -C "${fixture}" checkout -- benchmarks/go.mod
    GOROOT_BOOTSTRAP="${bootstrap}" bash "${fixture}/benchmarks/run_comparison.sh" --build-toolchain "${fixture}/bin/go" >/dev/null

	printf 'package runtime\n' > "${fixture}/src/deleted.go"
	git -C "${fixture}" add src/deleted.go
	rm -f "${fixture}/src/deleted.go"
	GOROOT_BOOTSTRAP="${bootstrap}" bash "${fixture}/benchmarks/run_comparison.sh" --build-toolchain "${fixture}/bin/go" >/dev/null
	bash "${fixture}/benchmarks/run_comparison.sh" --validate-toolchain-build "${fixture}/bin/go" >/dev/null
	git -C "${fixture}" checkout -- src/deleted.go
	if output="$(bash "${fixture}/benchmarks/run_comparison.sh" --validate-toolchain-build "${fixture}/bin/go" 2>&1)"; then
		echo "accepted a toolchain receipt after restoring a tracked source deletion" >&2
		exit 1
	fi
	grep -F 'receipt does not match current source and toolchain artifacts' <<<"${output}" >/dev/null
	rm -f "${fixture}/src/deleted.go"

    printf 'changed compiler artifact\n' > "${fixture}/pkg/tool/testos_testarch/compile tool"
    if output="$(bash "${fixture}/benchmarks/run_comparison.sh" --validate-toolchain-build "${fixture}/bin/go" 2>&1)"; then
        echo "accepted a toolchain receipt for a changed compiler artifact" >&2
        exit 1
    fi
    grep -F 'receipt does not match current source and toolchain artifacts' <<<"${output}" >/dev/null

    printf 'fresh compiler artifact\n' > "${fixture}/pkg/tool/testos_testarch/compile tool"
    GOROOT_BOOTSTRAP="${bootstrap}" bash "${fixture}/benchmarks/run_comparison.sh" --build-toolchain "${fixture}/bin/go" >/dev/null
    printf 'changed generated assembler header\n' > "${fixture}/pkg/include/textflag.h"
    if output="$(bash "${fixture}/benchmarks/run_comparison.sh" --validate-toolchain-build "${fixture}/bin/go" 2>&1)"; then
        echo "accepted a toolchain receipt for a changed generated assembler header" >&2
        exit 1
    fi
    grep -F 'receipt does not match current source and toolchain artifacts' <<<"${output}" >/dev/null

    GOROOT_BOOTSTRAP="${bootstrap}" bash "${fixture}/benchmarks/run_comparison.sh" --build-toolchain "${fixture}/bin/go" >/dev/null
    rm -f "${fixture}/pkg/include/textflag.h"
    if output="$(bash "${fixture}/benchmarks/run_comparison.sh" --validate-toolchain-build "${fixture}/bin/go" 2>&1)"; then
        echo "accepted a toolchain receipt with a missing generated assembler header" >&2
        exit 1
    fi
    grep -F 'could not compute current toolchain source identity' <<<"${output}" >/dev/null

    GOROOT_BOOTSTRAP="${bootstrap}" bash "${fixture}/benchmarks/run_comparison.sh" --build-toolchain "${fixture}/bin/go" >/dev/null
    printf 'src/ignored.go\n' > "${fixture}/.gitignore"
    printf 'package runtime\n' > "${fixture}/src/ignored.go"
    GOROOT_BOOTSTRAP="${bootstrap}" bash "${fixture}/benchmarks/run_comparison.sh" --build-toolchain "${fixture}/bin/go" >/dev/null
    printf '// changed ignored build input\n' >> "${fixture}/src/ignored.go"
    if output="$(bash "${fixture}/benchmarks/run_comparison.sh" --validate-toolchain-build "${fixture}/bin/go" 2>&1)"; then
        echo "accepted a toolchain receipt for a changed ignored source input" >&2
        exit 1
    fi
    grep -F 'receipt does not match current source and toolchain artifacts' <<<"${output}" >/dev/null

    rm -f "${fixture}/src/ignored.go"
    GOROOT_BOOTSTRAP="${bootstrap}" bash "${fixture}/benchmarks/run_comparison.sh" --build-toolchain "${fixture}/bin/go" >/dev/null
    printf '# binary changed after build\n' >> "${fixture}/bin/go"
    if output="$(bash "${fixture}/benchmarks/run_comparison.sh" --validate-toolchain-build "${fixture}/bin/go" 2>&1)"; then
        echo "accepted a toolchain receipt for a changed bin/go" >&2
        exit 1
    fi
    grep -F 'receipt does not match current source and toolchain artifacts' <<<"${output}" >/dev/null

    printf 'stale receipt\n' > "${fixture}/bin/go.benchmark-attestation"
    if output="$(FAIL_BUILD=1 GOROOT_BOOTSTRAP="${bootstrap}" bash "${fixture}/benchmarks/run_comparison.sh" --build-toolchain "${fixture}/bin/go" 2>&1)"; then
        echo 'attested a failed fresh PureGo build' >&2
        exit 1
    fi
    grep -F 'fresh PureGo make.bash failed; no toolchain receipt was written' <<<"${output}" >/dev/null
    [[ ! -e "${fixture}/bin/go.benchmark-attestation" ]] || {
        echo 'failed build left a stale toolchain receipt' >&2
        exit 1
    }
}

test_counterbalanced_run() {
    local fixture bootstrap fake_bin trace output expected_order label phase

    case_number=$(( case_number + 1 ))
    fixture="${TMP_ROOT}/case-${case_number}"
    bootstrap="${fixture}/bootstrap"
    fake_bin="${fixture}/fake-bin"
    trace="${fixture}/execution-order.txt"
    mkdir -p "${fixture}/benchmarks/scripts" "${fixture}/bin" "${fixture}/src" \
        "${fixture}/pkg/include" "${fixture}/pkg/tool/testos_testarch" "${bootstrap}" "${fake_bin}"
    cp "${HARNESS}" "${fixture}/benchmarks/run_comparison.sh"
    cp "${ORDER_HELPER}" "${fixture}/benchmarks/scripts/benchmark_order.sh"
    printf 'package benchmarks\n' > "${fixture}/benchmarks/race_bench_test.go"
    printf 'module benchmarks\n\ngo 1.24\n' > "${fixture}/benchmarks/go.mod"
    printf 'go1.26-devel_fixture\n' > "${fixture}/VERSION"
    printf '# fixture go environment\n' > "${fixture}/go.env"
    printf 'package runtime\n' > "${fixture}/src/runtime.go"
    cat > "${fixture}/src/make.bash" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "${CGO_ENABLED:-}" == 0 && -d "${GOROOT_BOOTSTRAP:-}" ]]
printf 'compiler artifact\n' > ../pkg/tool/testos_testarch/compile
printf 'header artifact\n' > ../pkg/include/textflag.h
EOF
    chmod +x "${fixture}/src/make.bash"
    cat > "${fixture}/bin/go" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
case "${1:-}" in
    env)
        case "${2:-}" in
            GOROOT) echo "${root}" ;;
            GOOS) echo testos ;;
            GOARCH) echo testarch ;;
            CC) echo fakecc ;;
            *) exit 1 ;;
        esac
        ;;
    list)
        if [[ "${CGO_ENABLED:-}" == 0 ]]; then
            echo 'GoFiles=race_kolkov_import.go CgoFiles= SysoFiles='
        else
            echo 'GoFiles=race.go CgoFiles= SysoFiles=race_testos_testarch.syso'
        fi
        ;;
    version) echo 'go version go1.26-fixture testos/testarch' ;;
    tool)
        case "${2:-}" in
            buildid) echo "buildid-$(basename "$3")" ;;
            nm)
                if [[ "$3" == *tsan* ]]; then echo '1 T __tsan_read'; else echo '1 T runtime.raceRead'; fi
                ;;
            *) exit 1 ;;
        esac
        ;;
    test)
        output=
        while (( $# )); do
            if [[ "$1" == -o ]]; then output="$2"; shift 2; else shift; fi
        done
        [[ -n "${output}" ]]
        cat > "${output}" <<'BINARY'
#!/usr/bin/env bash
set -euo pipefail
label="$(basename "$0")"
label="${label#benchmark-}"
label="${label%.test}"
phase=latency
benchtime=
for arg in "$@"; do
    case "${arg}" in
        -test.bench=\^BenchmarkMemoryConcurrent*) phase=rss ;;
        -test.benchtime=*) benchtime="${arg#*=}" ;;
    esac
done
printf '%s %s %s\n' "${phase}" "${benchtime}" "${label}" >> "${FAKE_TRACE}"
echo 'goos: testos'
echo 'goarch: testarch'
echo 'pkg: benchmarks'
if [[ "${phase}" == rss ]]; then
    echo 'BenchmarkMemoryConcurrent/g16-8 1 10 ns/op 0 B/op 0 allocs/op'
else
    echo 'BenchmarkRaceRead-8 1 10 ns/op 0 B/op 0 allocs/op'
fi
echo PASS
BINARY
        chmod +x "${output}"
        ;;
    *) exit 1 ;;
esac
EOF
    chmod +x "${fixture}/bin/go"
    cat > "${fake_bin}/fakecc" <<'EOF'
#!/usr/bin/env bash
echo 'fakecc fixture 1.0'
EOF
    cat > "${fake_bin}/benchstat" <<'EOF'
#!/usr/bin/env bash
echo 'RaceRead-8 10ns'
EOF
    cat > "${fake_bin}/gtime" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == --version ]]; then echo 'GNU time fixture'; exit 0; fi
[[ "${1:-}" == -v ]]
shift
status=0
"$@" || status=$?
echo 'Maximum resident set size (kbytes): 100' >&2
exit "${status}"
EOF
    chmod +x "${fake_bin}/fakecc" "${fake_bin}/benchstat" "${fake_bin}/gtime"

    git -C "${fixture}" init -q
    git -C "${fixture}" -c user.name=fixture -c user.email=fixture@example.invalid add .
    git -C "${fixture}" -c user.name=fixture -c user.email=fixture@example.invalid commit -qm fixture
    PATH="${fake_bin}:${PATH}" GOROOT_BOOTSTRAP="${bootstrap}" \
        bash "${fixture}/benchmarks/run_comparison.sh" --build-toolchain "${fixture}/bin/go" >/dev/null
    PATH="${fake_bin}:${PATH}" FAKE_TRACE="${trace}" GNU_TIME_BIN="${fake_bin}/gtime" BENCH_GOMAXPROCS=2 \
        bash "${fixture}/benchmarks/run_comparison.sh" --quick --go "${fixture}/bin/go" >/dev/null

    expected_order="$(cat <<'EOF'
001 baseline
001 tsan
001 purego
002 tsan
002 purego
002 baseline
003 purego
003 baseline
003 tsan
EOF
)"
    [[ "$(cat "${fixture}/benchmarks/results/sample-order.txt")" == "${expected_order}" ]]
    cmp -s "${fixture}/benchmarks/results/sample-order.txt" "${fixture}/benchmarks/results/rss-sample-order.txt" || {
        echo 'latency and RSS phases did not share the counterbalanced schedule' >&2
        exit 1
    }
    [[ "$(grep -c ' 100ms ' "${trace}")" == 6 ]]
    for label in baseline tsan purego; do
        [[ "$(grep -c "^latency 100ms ${label}$" "${trace}")" == 1 ]]
        [[ "$(grep -c "^rss 100ms ${label}$" "${trace}")" == 1 ]]
        [[ -s "${fixture}/benchmarks/results/raw/${label}/warmup-latency.txt" ]]
        [[ -s "${fixture}/benchmarks/results/raw/${label}/warmup-rss.stdout" ]]
        [[ -s "${fixture}/benchmarks/results/raw/${label}/warmup-rss.time" ]]
        [[ ! -e "${fixture}/benchmarks/results/raw/${label}/warmup-latency.txt.samples" ]]
        [[ ! -e "${fixture}/benchmarks/results/raw/${label}/warmup-rss.stdout.samples" ]]
        [[ "$(grep -c '^Benchmark' "${fixture}/benchmarks/results/${label}.txt")" == 3 ]]
        [[ "$(grep -c '^Benchmark' "${fixture}/benchmarks/results/rss-${label}.stdout")" == 3 ]]
        [[ "$(wc -l < "${fixture}/benchmarks/results/rss-${label}-kb.txt" | tr -d '[:space:]')" == 3 ]]
    done
    [[ "$(wc -l < "${fixture}/benchmarks/results/sample-order.txt" | tr -d '[:space:]')" == 9 ]]
    [[ "$(wc -l < "${fixture}/benchmarks/results/rss-sample-order.txt" | tr -d '[:space:]')" == 9 ]]
    grep -F 'warmup_benchtime=100ms' "${fixture}/benchmarks/results/environment.txt" >/dev/null
    grep -F 'sample_order=all-six-permutations' "${fixture}/benchmarks/results/summary.md" >/dev/null
    [[ "$(cat "${fixture}/benchmarks/results/status.txt")" == QUICK ]]
}

test_comparison_order

for value in '' 1junk 1=junk NaN Inf + - +1 -1 . .5 1. 1e0 ' 1' '1 '; do
    expect_invalid_metadata benchtime_seconds "${value}"
    expect_invalid_metadata max_raceread_ratio "${value}"
done

test_rss_summarizer
for kind in wrong-count zero decimal malformed missing; do
    expect_invalid_rss_summary "${kind}"
done
expect_invalid_rss_metadata rss_count 0 'release evidence has rss_count=0; want at least 10'
expect_invalid_rss_metadata rss_count 9 'release evidence has rss_count=9; want at least 10'
expect_invalid_rss_metadata rss_count 10.0 'release evidence has rss_count=10.0; want at least 10'
expect_invalid_rss_metadata rss_count 11 'release evidence has rss_count=11; want count=10'
expect_invalid_rss_metadata rss_count __missing__ 'release evidence has rss_count=missing; want at least 10'
expect_invalid_rss_metadata rss_benchtime 2s 'release evidence has rss_benchtime=2s; want exactly 3s'
expect_invalid_rss_metadata rss_benchtime 3.0 'release evidence has rss_benchtime=3.0; want exactly 3s'
expect_invalid_rss_metadata rss_benchtime __missing__ 'release evidence has rss_benchtime=missing; want exactly 3s'

expect_invalid_benchmark unit widgets/op
for value in NaN Inf text 0 -1; do
    expect_invalid_benchmark value "${value}"
done
expect_wrong_sample_count
expect_manifest_sort_failure
expect_runtime_fatal 'fatal error: fixture crash'
expect_runtime_fatal 'runtime: fatal fixture crash'
test_toolchain_attestation
test_counterbalanced_run

make_fixture 1 1
bash "${FIXTURE}/benchmarks/run_comparison.sh" \
	--validate-release-contract "${FIXTURE}/benchmarks/results/release-contract.txt"

echo "run_comparison validation tests passed"
