#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
HARNESS="${SCRIPT_DIR}/run_soak.sh"
ROOT="$(mktemp -d "${TMPDIR:-/tmp}/race-soak-test.XXXXXX")"
trap 'rm -rf "${ROOT}"' EXIT

make_fixture() {
    local dir="$1" pure_t="$2" tsan_t="$3" pure_p="$4" tsan_p="$5" pure_heap="$6" last_heap="$7" slope_heap="$8" forced="$9" cleanup="${10}"
    local workload_hash
    workload_hash="$(python3 - "${SCRIPT_DIR}/../race_bench_test.go" "${SCRIPT_DIR}/../go.mod" <<'PY'
import hashlib,sys
h=hashlib.sha256()
for p in sys.argv[1:]: h.update(hashlib.sha256(open(p,'rb').read()).hexdigest().encode())
print(h.hexdigest())
PY
)"
    mkdir -p "${dir}"
    cat > "${dir}/identity.env" <<EOF2
format=soak-identity-v1
mode=stable
source_hash=$(printf '%064d' 0 | tr 0 a)
workload_hash=${workload_hash}
purego_hash=$(printf '%064d' 0 | tr 0 c)
tsan_hash=$(printf '%064d' 0 | tr 0 d)
purego_backend=CGO_ENABLED=0 race_kolkov_import.go CgoFiles=
tsan_backend=CGO_ENABLED=1 SysoFiles=race_linux_amd64.syso
EOF2
    for backend in purego tsan; do
        : > "${dir}/${backend}.tsv"
        for q in 1 2 3 4; do
            if [[ "${backend}" == purego ]]; then t="${pure_t}"; p="${pure_p}"; else t="${tsan_t}"; p="${tsan_p}"; fi
            h=1000
            [[ "${q}" == 1 ]] && h="${pure_heap}"
            post="${slope_heap}"
            [[ "${q}" == 4 ]] && post="${last_heap}"
            printf 'backend=%s phase=periodic quarter=%s elapsed_seconds=900 throughput=%s p99_ns=%s heap_inuse=%s\n' "${backend}" "${q}" "${t}" "${p}" "${h}" >> "${dir}/${backend}.tsv"
            printf 'backend=%s phase=quarter quarter=%s elapsed_seconds=900 throughput=%s p99_ns=%s heap_inuse=%s\n' "${backend}" "${q}" "${t}" "${p}" "${h}" >> "${dir}/${backend}.tsv"
            printf 'backend=%s phase=post_gc quarter=%s elapsed_seconds=900 throughput=%s p99_ns=%s heap_inuse=%s forced_gcs=%s cleanup_seconds=%s\n' "${backend}" "${q}" "${t}" "${p}" "${post}" "${forced}" "${cleanup}" >> "${dir}/${backend}.tsv"
        done
    done
}
expect_pass() {
    local dir="$1"; shift
    make_fixture "${dir}" "$@"
    bash "${HARNESS}" --fixture "${dir}" --duration 60m --interval 15m >/dev/null
    grep -F 'status=PASS' "${dir}/summary.txt" >/dev/null
}
expect_fail() {
    local dir="$1"; shift output
    make_fixture "${dir}" 100 100 100 100 1000 1000 1000 2 1
    if [[ "$#" -gt 0 ]]; then "$@"; fi
    if output=$(bash "${HARNESS}" --fixture "${dir}" --duration 60m --interval 15m 2>&1); then
        echo "accepted invalid soak fixture ${dir}" >&2; echo "${output}" >&2; exit 1
    fi
}

# Equality boundaries are accepted: throughput=.90, p99=1.25, heap=1.05,
# cleanup=2 forced GCs/60 seconds. The retained-slope baseline is post-GC.
expect_pass "${ROOT}/equal" 90 100 125 100 1000 1050 1050 2 60

# Every structural rejection is exercised independently.
for kind in missing-periodic missing-quarter duplicate-quarter incomplete-quarter nonfinite race-fatal; do
    dir="${ROOT}/${kind}"
    make_fixture "${dir}" 100 100 100 100 1000 1000 1000 2 1
    case "${kind}" in
        missing-periodic) python3 - "${dir}/purego.tsv" <<'PY'
import sys
p=sys.argv[1]; lines=open(p).readlines(); open(p,'w').writelines(x for x in lines if 'phase=periodic quarter=4' not in x)
PY
            ;;
        missing-quarter) python3 - "${dir}/tsan.tsv" <<'PY'
import sys
p=sys.argv[1]; lines=open(p).readlines(); open(p,'w').writelines(x for x in lines if 'phase=quarter quarter=3' not in x)
PY
            ;;
        duplicate-quarter) python3 - "${dir}/purego.tsv" <<'PY'
import sys
p=sys.argv[1]; lines=open(p).readlines(); open(p,'w').writelines(lines[:5]+[lines[4]]+lines[5:])
PY
            ;;
        incomplete-quarter) python3 - "${dir}/purego.tsv" <<'PY'
import sys
p=sys.argv[1]; open(p,'w').write(open(p).read().replace('phase=quarter quarter=4 elapsed_seconds=900','phase=quarter quarter=4 elapsed_seconds=1').replace('phase=post_gc quarter=4 elapsed_seconds=900','phase=post_gc quarter=4 elapsed_seconds=1'))
PY
            ;;
        nonfinite) python3 - "${dir}/purego.tsv" <<'PY'
import sys
p=sys.argv[1]; open(p,'w').write(open(p).read().replace('throughput=100','throughput=NaN'))
PY
            ;;
        race-fatal) echo 'WARNING: DATA RACE' >> "${dir}/tsan.tsv" ;;
    esac
    if bash "${HARNESS}" --fixture "${dir}" --duration 60m --interval 15m >/dev/null 2>&1; then echo "accepted ${kind}" >&2; exit 1; fi
done

dir="${ROOT}/identity-mismatch"
make_fixture "${dir}" 100 100 100 100 1000 1000 1000 2 1
python3 - "${dir}/identity.env" <<'PY'
import sys
p=sys.argv[1]; text=open(p).read().replace('purego_hash=' + 'c'*64, 'purego_hash=' + 'd'*64); open(p,'w').write(text)
PY
if bash "${HARNESS}" --fixture "${dir}" --duration 60m --interval 15m >/dev/null 2>&1; then echo 'accepted identity mismatch' >&2; exit 1; fi

for kind in throughput p99 heap slope cleanup; do
    dir="${ROOT}/off-${kind}"
    case "${kind}" in
        throughput) make_fixture "${dir}" 89 100 100 100 1000 1000 1000 2 1 ;;
        p99) make_fixture "${dir}" 100 100 126 100 1000 1000 1000 2 1 ;;
        heap) make_fixture "${dir}" 100 100 100 100 1000 1051 1000 2 1 ;;
        slope) make_fixture "${dir}" 100 100 100 100 1000 1010 1000 2 1 ;;
        cleanup) make_fixture "${dir}" 100 100 100 100 1000 1000 1000 3 1 ;;
    esac
    if bash "${HARNESS}" --fixture "${dir}" --duration 60m --interval 15m >/dev/null 2>&1; then echo "accepted off-by-one ${kind}" >&2; exit 1; fi
done


# --evidence is release-only: missing retained source/binaries and fake hashes
# must not be accepted by the synthetic parser path.
for kind in missing-retained fake-hash; do
    dir="${ROOT}/release-${kind}"
    make_fixture "${dir}" 100 100 100 100 1000 1000 1000 2 1
    if [[ "${kind}" == fake-hash ]]; then
        printf 'package main
func main() {}
' > "${dir}/soak.go"
        source_hash="$(shasum -a 256 "${dir}/soak.go" | awk '{print $1}')"
        sed -i.bak "s/^source_hash=.*/source_hash=${source_hash}/" "${dir}/identity.env"
        rm -f "${dir}/identity.env.bak"
        printf purego > "${dir}/purego.soak"
        printf tsan > "${dir}/tsan.soak"
    fi
    if bash "${HARNESS}" --evidence "${dir}" --duration 60m --interval 15m >/dev/null 2>&1; then
        echo "accepted incomplete release evidence ${kind}" >&2
        exit 1
    fi
done

# Bash duration parsing must retain the complete suffix for every supported
# subsecond and whole-second unit.
for unit in ns us 'µs' ms s m h; do
    dir="${ROOT}/duration-${unit}"
    make_fixture "${dir}" 100 100 100 100 1000 1000 1000 2 1
    bash "${HARNESS}" --fixture "${dir}" --allow-short --duration "1${unit}" --interval "0.1${unit}" >/dev/null
done

# Exercise the generated workload itself, not just evidence parsing. The p99
# cases distinguish nearest-rank ceiling from truncation at n=10 and n=101.
# The cleanup test uses a deterministic clock to prove both forced GCs and the
# post-GC stats read are inside the measured interval.
generated="${ROOT}/generated-source"
mkdir -p "${generated}"
(
    set -- --allow-short --duration 4s --interval 1s
    source "${HARNESS}"
    generate_source "${generated}/soak.go"
)
cat > "${generated}/soak_test.go" <<'EOF2'
package main

import (
	"testing"
	"time"
)

func TestPercentileUsesNearestRankCeiling(t *testing.T) {
	ten := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 100}
	if got := percentile(ten); got != 100 {
		t.Fatalf("p99 of 10 samples = %v, want maximum 100", got)
	}
	hundredOne := make([]int64, 101)
	for i := range hundredOne {
		hundredOne[i] = int64(i + 1)
	}
	if got := percentile(hundredOne); got != 100 {
		t.Fatalf("p99 of 101 samples = %v, want rank 100", got)
	}
}

func TestCleanupDurationIncludesForcedGCsAndStatsRead(t *testing.T) {
	now := time.Unix(0, 0)
	gcCalls := 0
	readCalls := 0
	advance := func(d time.Duration) { now = now.Add(d) }
	got := cleanupDuration(
		func() time.Time { return now },
		func() { gcCalls++; advance(2 * time.Second) },
		func() { readCalls++; advance(3 * time.Second) },
	)
	if gcCalls != 2 || readCalls != 1 {
		t.Fatalf("cleanup called GC %d times and stats read %d times, want 2 and 1", gcCalls, readCalls)
	}
	if got != 7*time.Second {
		t.Fatalf("cleanup duration = %v, want 7s including both GCs and stats read", got)
	}
}
EOF2
(cd "${generated}" && GO111MODULE=off go test)

echo 'run_soak validation tests passed'
