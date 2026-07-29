#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
HARNESS="${SCRIPT_DIR}/../run_comparison.sh"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/race-comparison-test.XXXXXX")"
trap 'rm -rf "${TMP_ROOT}"' EXIT

case_number=0
FIXTURE=

make_fixture() {
    local seconds="$1" maximum="$2" fixture output config sample

    case_number=$(( case_number + 1 ))
    fixture="${TMP_ROOT}/case-${case_number}"
    mkdir -p "${fixture}/benchmarks/results" "${fixture}/benchmarks/baselines"
    cp "${HARNESS}" "${fixture}/benchmarks/run_comparison.sh"

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
EOF
    FIXTURE="${fixture}"
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
    mkdir -p "${fixture}/benchmarks" "${fixture}/bin" "${fixture}/src" \
        "${fixture}/pkg/include" "${fixture}/pkg/tool/testos_testarch" "${bootstrap}"
    cp "${HARNESS}" "${fixture}/benchmarks/run_comparison.sh"
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

for value in '' 1junk 1=junk NaN Inf + - +1 -1 . .5 1. 1e0 ' 1' '1 '; do
    expect_invalid_metadata benchtime_seconds "${value}"
    expect_invalid_metadata max_raceread_ratio "${value}"
done

expect_invalid_benchmark unit widgets/op
for value in NaN Inf text 0 -1; do
    expect_invalid_benchmark value "${value}"
done
expect_wrong_sample_count
expect_manifest_sort_failure
expect_runtime_fatal 'fatal error: fixture crash'
expect_runtime_fatal 'runtime: fatal fixture crash'
test_toolchain_attestation

make_fixture 1 1
bash "${FIXTURE}/benchmarks/run_comparison.sh" \
	--validate-release-contract "${FIXTURE}/benchmarks/results/release-contract.txt"

echo "run_comparison validation tests passed"
