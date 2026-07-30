#!/usr/bin/env bash
# Run and validate the long-lived stable/churn detector soak. The executable
# emits a line-oriented evidence stream; this script never infers a missing
# quarter or metric. --fixture is reserved for deterministic parser fixtures.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
BENCHMARK_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd -P)"
SOURCE_ROOT="$(cd "${BENCHMARK_ROOT}/.." && pwd -P)"
RUN_COMPARISON="${BENCHMARK_ROOT}/run_comparison.sh"
GO_BINARY="${SOAK_GO:-${SOURCE_ROOT}/bin/go}"
MODE=stable
DURATION=60m
INTERVAL=15s
OUTPUT_DIR="${SOAK_OUTPUT_DIR:-${BENCHMARK_ROOT}/results/soak-${MODE}}"
OUTPUT_DIR_EXPLICIT=false
EVIDENCE_DIR=
FIXTURE_MODE=false
ALLOW_SHORT="${SOAK_ALLOW_SHORT:-false}"

fail() { echo "ERROR: $*" >&2; exit 1; }
usage() {
    cat <<'USAGE'
Usage: run_soak.sh [OPTIONS]
  --go PATH             attested fork go binary (default: bin/go)
  --mode stable|churn   workload mode (default: stable)
  --duration D          duration, normally exactly 60m (default: 60m)
  --interval D          periodic observation interval (default: 15s)
  --out DIR             retain raw logs and summaries in DIR
  --evidence DIR        validate retained release evidence instead of running
  --fixture DIR         validate synthetic parser fixtures only
  --allow-short         permit a sub-60m duration for local tests

Evidence layout: identity.env, purego.tsv, tsan.tsv. Each TSV is a stream of
key=value rows with phase=periodic, phase=quarter, and phase=post_gc.
USAGE
}
require_arg() { (( $# >= 2 )) || fail "$1 requires one argument"; }
while [[ $# -gt 0 ]]; do
    case "$1" in
        --go) require_arg "$@"; GO_BINARY="$2"; shift 2 ;;
        --mode) require_arg "$@"; MODE="$2"; shift 2 ;;
        --duration) require_arg "$@"; DURATION="$2"; shift 2 ;;
        --interval) require_arg "$@"; INTERVAL="$2"; shift 2 ;;
        --out) require_arg "$@"; OUTPUT_DIR="$2"; OUTPUT_DIR_EXPLICIT=true; shift 2 ;;
        --evidence) require_arg "$@"; [[ -z "${EVIDENCE_DIR}" ]] || fail "only one evidence mode may be selected"; EVIDENCE_DIR="$2"; shift 2 ;;
        --fixture) require_arg "$@"; [[ -z "${EVIDENCE_DIR}" ]] || fail "only one evidence mode may be selected"; EVIDENCE_DIR="$2"; FIXTURE_MODE=true; shift 2 ;;
        --allow-short) ALLOW_SHORT=true; shift ;;
        --help|-h) usage; exit 0 ;;
        *) fail "unknown option: $1" ;;
    esac
done
[[ "${MODE}" == stable || "${MODE}" == churn ]] || fail "--mode must be stable or churn: ${MODE}"
if [[ "${OUTPUT_DIR_EXPLICIT}" != true && -z "${SOAK_OUTPUT_DIR:-}" ]]; then
    OUTPUT_DIR="${BENCHMARK_ROOT}/results/soak-${MODE}"
fi

duration_seconds() {
    local value="$1" number unit
    [[ "${value}" =~ ^([0-9]+([.][0-9]+)?)(ns|us|µs|ms|s|m|h)$ ]] || return 1
    number="${BASH_REMATCH[1]}"
    unit="${BASH_REMATCH[3]}"
    awk -v n="${number}" -v u="${unit}" 'BEGIN { scale["ns"]=1e-9; scale["us"]=1e-6; scale["µs"]=1e-6; scale["ms"]=1e-3; scale["s"]=1; scale["m"]=60; scale["h"]=3600; if (!(u in scale) || n <= 0) exit 1; printf "%.17g\n", n * scale[u] }'
}
DURATION_SECONDS="$(duration_seconds "${DURATION}")" || fail "invalid --duration: ${DURATION}"
INTERVAL_SECONDS="$(duration_seconds "${INTERVAL}")" || fail "invalid --interval: ${INTERVAL}"
if [[ "${ALLOW_SHORT}" != true && "${ALLOW_SHORT}" != 1 ]]; then
    awk -v d="${DURATION_SECONDS}" 'BEGIN { exit !(d >= 3600) }' || fail "release soaks require duration at least 60m (use --allow-short for tests)"
fi
awk -v d="${DURATION_SECONDS}" -v i="${INTERVAL_SECONDS}" 'BEGIN { exit !(d >= 4*i) }' || fail "duration must contain at least four observation intervals"

hash_file() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'; else shasum -a 256 "$1" | awk '{print $1}'; fi; }
workload_identity() {
    local joined
    joined="$(hash_file "${BENCHMARK_ROOT}/race_bench_test.go")$(hash_file "${BENCHMARK_ROOT}/go.mod")"
    printf '%s' "${joined}" | (sha256sum 2>/dev/null || shasum -a 256) | awk '{print $1}'
}
read_key() {
    local key="$1" file="$2" value
    value="$(awk -F= -v key="${key}" '$1 == key { print substr($0, length($1)+2); found++ } END { if (found != 1) exit 1 }' "${file}")" || return 1
    printf '%s\n' "${value}"
}
validate_identity() {
    local dir="$1" strict="$2" identity="$1/identity.env" format mode source_hash workload_hash pure_hash tsan_hash receipt_hash
    [[ -s "${identity}" ]] || fail "soak identity receipt is missing: ${identity}"
    format="$(read_key format "${identity}")" || fail "soak identity format is missing or duplicated"
    if [[ "${strict}" == true ]]; then
        [[ "${format}" == soak-identity-v2 ]] || fail "release soak identity must use format soak-identity-v2"
    else
        [[ "${format}" == soak-identity-v1 || "${format}" == soak-identity-v2 ]] || fail "unsupported soak identity format: ${format}"
    fi
    mode="$(read_key mode "${identity}")" || fail "soak identity mode is missing or duplicated"
    [[ "${mode}" == "${MODE}" ]] || fail "soak identity mode=${mode}; want ${MODE}"
    source_hash="$(read_key source_hash "${identity}")" || fail "soak source identity is missing"
    workload_hash="$(read_key workload_hash "${identity}")" || fail "soak workload identity is missing"
    pure_hash="$(read_key purego_hash "${identity}")" || fail "soak PureGo binary identity is missing"
    tsan_hash="$(read_key tsan_hash "${identity}")" || fail "soak TSAN binary identity is missing"
    [[ "${source_hash}" =~ ^[0-9a-f]{64}$ && "${workload_hash}" =~ ^[0-9a-f]{64}$ ]] || fail "soak source/workload identities are malformed"
    [[ "${workload_hash}" == "$(workload_identity)" ]] || fail "soak workload identity does not match race_bench_test.go/go.mod"
    [[ "${pure_hash}" =~ ^[0-9a-f]{64}$ && "${tsan_hash}" =~ ^[0-9a-f]{64}$ && "${pure_hash}" != "${tsan_hash}" ]] || fail "soak backend binary identities are malformed or equal"
    grep -Eq '^purego_backend=.*CGO_ENABLED=0.*race_kolkov_import\.go.*CgoFiles=$' "${identity}" || fail "soak identity does not attest the PureGo backend"
    grep -Eq '^tsan_backend=.*CGO_ENABLED=1.*SysoFiles=race_[A-Za-z0-9_]+\.syso' "${identity}" || fail "soak identity does not attest the TSAN backend"
    [[ "${strict}" == true ]] || return 0

    for required in soak.go purego.soak tsan.soak toolchain-attestation; do
        [[ -s "${dir}/${required}" ]] || fail "release soak evidence is missing retained ${required}"
    done
    [[ "${source_hash}" == "$(hash_file "${dir}/soak.go")" ]] || fail "soak source identity does not match retained soak.go"
    [[ "${pure_hash}" == "$(hash_file "${dir}/purego.soak")" && "${tsan_hash}" == "$(hash_file "${dir}/tsan.soak")" ]] || fail "soak binary identity does not match retained binaries"
    receipt_hash="$(read_key toolchain_receipt_sha256 "${identity}")" || fail "soak toolchain receipt identity is missing"
    [[ "${receipt_hash}" =~ ^[0-9a-f]{64}$ && "${receipt_hash}" == "$(hash_file "${dir}/toolchain-attestation")" ]] || fail "soak toolchain receipt identity does not match retained receipt"
    [[ -x "${GO_BINARY}" ]] || fail "fork go binary is not executable: ${GO_BINARY}"
    [[ -x "${RUN_COMPARISON}" ]] || fail "toolchain receipt validator is missing: ${RUN_COMPARISON}"
    SOURCE_ROOT_OVERRIDE="${SOURCE_ROOT}" bash "${RUN_COMPARISON}" --validate-toolchain-build "${GO_BINARY}" || fail "fork toolchain attestation is invalid"
    cmp -s "${GO_BINARY}.benchmark-attestation" "${dir}/toolchain-attestation" || fail "retained soak toolchain receipt does not match validated fork receipt"
    "${GO_BINARY}" tool nm "${dir}/purego.soak" > "${dir}/purego.nm" || fail "could not inspect retained PureGo soak binary"
    "${GO_BINARY}" tool nm "${dir}/tsan.soak" > "${dir}/tsan.nm" || fail "could not inspect retained TSAN soak binary"
    ! grep -Eq '__tsan|runtime/cgo' "${dir}/purego.nm" || fail "retained PureGo soak binary contains TSAN/cgo symbols"
    grep -q '__tsan' "${dir}/tsan.nm" || fail "retained TSAN soak binary contains no __tsan symbols"
}

validate_rows() {
    local backend="$1" input="$2" output="$3" identity_backend="$4" tmp
    [[ -s "${input}" ]] || fail "${backend} soak evidence is missing or empty: ${input}"
    if grep -Eiq 'warning:[[:space:]]*data race|fatal error:|runtime:[[:space:]]*fatal|^FAIL([[:space:]]|$)' "${input}"; then fail "${backend} soak evidence contains a race or runtime fatal"; fi
    tmp="$(mktemp "${output}.tmp.XXXXXX")" || fail "could not create ${backend} soak parser output"
    if ! awk -v want_backend="${identity_backend}" -v duration_seconds="${DURATION_SECONDS}" '
        function decimal(v,zero) { if(v !~ /^[0-9]+([.][0-9]+)?$/) return 0; n=v+0; if(n!=n||n>=1e308) return 0; return zero ? n>=0 : n>0 }
        function field(key,    i,p) { for(i=1;i<=NF;i++){ split($i,p,"="); if(p[1]==key)return substr($i,length(key)+2) } return "" }
        {
            if ($0 ~ /^#/ || NF == 0) next
            delete seen
            for (i=1;i<=NF;i++) { split($i,p,"="); if (p[1] !~ /^(backend|phase|quarter|elapsed_seconds|throughput|p99_ns|heap_inuse|forced_gcs|cleanup_seconds)$/ || p[2] == "" || ++seen[p[1]] != 1) { bad=1; next } }
            backend=field("backend"); phase=field("phase"); quarter=field("quarter"); elapsed=field("elapsed_seconds"); throughput=field("throughput"); p99=field("p99_ns"); heap=field("heap_inuse"); forced=field("forced_gcs"); cleanup=field("cleanup_seconds")
            if(backend!=want_backend || (phase!="periodic" && phase!="quarter" && phase!="post_gc") || quarter !~ /^[1-4]$/ || !decimal(elapsed,0) || !decimal(throughput,0) || !decimal(p99,0) || !decimal(heap,0)) { bad=1; next }
            if((phase=="quarter" || phase=="post_gc") && elapsed+0 < duration_seconds/4*0.99) { bad=1; next }
            if(phase=="post_gc" && (!decimal(forced,0)||!decimal(cleanup,1))) { bad=1; next }
            if(phase=="periodic") periodic[quarter]++
            if(phase=="quarter") { quarter_rows[quarter]++; qthroughput[quarter]=throughput+0; qp99[quarter]=p99+0; qheap[quarter]=heap+0 }
            if(phase=="post_gc") { post_rows[quarter]++; pthroughput[quarter]=throughput+0; pp99[quarter]=p99+0; pheap[quarter]=heap+0; forced_gc[quarter]=forced+0; cleanup_s[quarter]=cleanup+0 }
            printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", phase, quarter, elapsed, throughput, p99, heap, forced, cleanup, backend
            total++
        }
        END { if(total==0)bad=1; for(i=1;i<=4;i++) { if(periodic[i]<1 || quarter_rows[i]!=1 || post_rows[i]!=1) bad=1; if(quarter_rows[i]==1 && post_rows[i]==1 && (qthroughput[i]<=0 || pthroughput[i]<=0 || qp99[i]<=0 || pp99[i]<=0 || qheap[i]<=0 || pheap[i]<=0)) bad=1 }; if(bad)exit 1 }
    ' "${input}" > "${tmp}"; then
        rm -f "${tmp}"; fail "${backend} soak evidence has malformed, non-finite, or incomplete quarter rows"
    fi
    mv "${tmp}" "${output}" || { rm -f "${tmp}"; fail "could not install ${backend} normalized soak evidence"; }
}
validate_metrics() {
    local dir="$1" identity_hash="$2" pure="$1/purego.normalized.tsv" tsan="$1/tsan.normalized.tsv" summary="$1/summary.txt" tmp
    tmp="$(mktemp "${summary}.tmp.XXXXXX")" || fail "could not create soak summary"
    if ! awk -v mode="${MODE}" -v duration="${DURATION_SECONDS}" -v identity="${identity_hash}" -v out="${tmp}" '
        FNR==NR { if($1=="quarter"){pt[$2]=$4;pp[$2]=$5;ph[$2]=$6}; if($1=="post_gc"){ptg[$2]=$4;ppg[$2]=$5;phg[$2]=$6;fg[$2]=$7;cl[$2]=$8}; next }
        { if($1=="quarter"){qt[$2]=$4;qp[$2]=$5;qh[$2]=$6}; if($1=="post_gc"){qtg[$2]=$4;qpg[$2]=$5;qhg[$2]=$6;qfg[$2]=$7;qcl[$2]=$8} }
        END {
            tr=pt[4]>0?pt[4]:ptg[4]; pr=qt[4]>0?qt[4]:qtg[4]; tp=pp[4]>0?pp[4]:ppg[4]; pp99=qp[4]>0?qp[4]:qpg[4]; throughput_ratio=pr/tr; p99_ratio=pp99/tp; first_heap=qh[1]; last_heap=qhg[4]; plateau=last_heap/first_heap; slope=((qhg[4]-qhg[1])/qhg[1])/(duration/3600); cleanup=qcl[4]; forced=qfg[4]; bad=!(throughput_ratio>=0.90 && p99_ratio<=1.25 && plateau<=1.05 && slope<0.01 && forced<=2 && cleanup<=60)
            printf "format=soak-summary-v1\nmode=%s\nduration_seconds=%.17g\nquarters=4\nidentity_sha256=%s\npurego_final_throughput=%.17g\ntsan_final_throughput=%.17g\npurego_tsan_throughput_ratio=%.17g\npurego_final_p99_ns=%.17g\ntsan_final_p99_ns=%.17g\npurego_tsan_p99_ratio=%.17g\nfirst_steady_heap_inuse=%s\nlast_quarter_post_gc_heap_inuse=%s\npost_gc_heap_ratio=%.17g\nretained_heap_slope_per_hour=%.17g\ncleanup_forced_gcs=%s\ncleanup_seconds=%s\nthroughput_floor=0.90\np99_ratio_ceiling=1.25\nheap_ratio_ceiling=1.05\nretained_slope_ceiling=0.01\nstatus=%s\n",mode,duration,identity,pr,tr,throughput_ratio,pp99,tp,p99_ratio,first_heap,last_heap,plateau,slope,forced,cleanup,(bad?"FAIL":"PASS") > out
            if(bad)exit 1
        }
    ' "${tsan}" "${pure}"; then rm -f "${tmp}"; fail "${MODE} soak gate failed (throughput/p99/heap slope/cleanup)"; fi
    mv "${tmp}" "${summary}" || { rm -f "${tmp}"; fail "could not install soak summary"; }
}

generate_source() {
    local path="$1"
    cat > "${path}" <<'SOURCE'
package main
import("fmt";"os";"runtime";"sort";"sync";"time")
func cleanupDuration(now func()time.Time,gc func(),readStats func())time.Duration{start:=now();gc();gc();readStats();return now().Sub(start)}
func main(){
 mode:=os.Getenv("SOAK_MODE"); backend:=os.Getenv("SOAK_BACKEND"); duration,_:=time.ParseDuration(os.Getenv("SOAK_DURATION")); interval,_:=time.ParseDuration(os.Getenv("SOAK_INTERVAL")); if duration<=0||interval<=0{panic("invalid soak duration or interval")}
 start:=time.Now(); var mu sync.Mutex; stable:=make([]byte,256); churn:=make(map[int][]byte)
 for quarter:=1;quarter<=4;quarter++ { qstart:=time.Now(); qend:=start.Add(time.Duration(float64(duration)*float64(quarter)/4)); var ops uint64; var samples []int64; next:=qstart.Add(interval)
  for time.Now().Before(qend){ batch:=time.Now(); for i:=0;i<256;i++{mu.Lock(); if mode=="churn"{churn[int(ops)%4096]=make([]byte,256)}else{stable[int(ops)%len(stable)]++};mu.Unlock();ops++}; samples=append(samples,time.Since(batch).Nanoseconds()/256); now:=time.Now(); if now.After(next){var m runtime.MemStats;runtime.ReadMemStats(&m);fmt.Printf("backend=%s phase=periodic quarter=%d elapsed_seconds=%.6f throughput=%.6f p99_ns=%.6f heap_inuse=%d\n",backend,quarter,now.Sub(qstart).Seconds(),float64(ops)/now.Sub(qstart).Seconds(),percentile(samples),m.HeapInuse);next=now.Add(interval)} }
  elapsed:=time.Since(qstart).Seconds();var m runtime.MemStats;runtime.ReadMemStats(&m);fmt.Printf("backend=%s phase=quarter quarter=%d elapsed_seconds=%.6f throughput=%.6f p99_ns=%.6f heap_inuse=%d\n",backend,quarter,elapsed,float64(ops)/elapsed,percentile(samples),m.HeapInuse);cleanup:=cleanupDuration(time.Now,runtime.GC,func(){runtime.ReadMemStats(&m)});fmt.Printf("backend=%s phase=post_gc quarter=%d elapsed_seconds=%.6f throughput=%.6f p99_ns=%.6f heap_inuse=%d forced_gcs=2 cleanup_seconds=%.6f\n",backend,quarter,elapsed,float64(ops)/elapsed,percentile(samples),m.HeapInuse,cleanup.Seconds())
 }
}
func percentile(v []int64)float64{if len(v)==0{return 1};x:=append([]int64(nil),v...);sort.Slice(x,func(i,j int)bool{return x[i]<x[j]});r:=len(x)-len(x)/100;return float64(x[r-1])}
SOURCE
}
run_generated() {
    local dir="$1" source="$1/soak.go" pure_bin="$1/purego.soak" tsan_bin="$1/tsan.soak" go source_hash workload_hash pure_hash tsan_hash
    if [[ "${GO_BINARY}" != */* ]]; then GO_BINARY="$(command -v "${GO_BINARY}" 2>/dev/null || true)"; fi
    go="$(cd "$(dirname "${GO_BINARY}")" && pwd -P)/$(basename "${GO_BINARY}")"
    [[ -x "${go}" ]] || fail "fork go binary is not executable: ${GO_BINARY}"
    [[ -x "${RUN_COMPARISON}" ]] || fail "toolchain receipt validator is missing: ${RUN_COMPARISON}"
    SOURCE_ROOT_OVERRIDE="${SOURCE_ROOT}" bash "${RUN_COMPARISON}" --validate-toolchain-build "${go}" || fail "fork toolchain attestation is invalid"
    generate_source "${source}"; source_hash="$(hash_file "${source}")"
    workload_hash="$(workload_identity)"
    (cd "${dir}" && CGO_ENABLED=0 "${go}" build -race -o "${pure_bin}" soak.go) || fail "PureGo soak binary build failed"
    (cd "${dir}" && CGO_ENABLED=1 "${go}" build -race -o "${tsan_bin}" soak.go) || fail "TSAN soak binary build failed"
    pure_hash="$(hash_file "${pure_bin}")"; tsan_hash="$(hash_file "${tsan_bin}")"; [[ "${pure_hash}" != "${tsan_hash}" ]] || fail "PureGo and TSAN soak binaries are identical"
    "${go}" tool nm "${pure_bin}" > "${dir}/purego.nm" || fail "could not inspect PureGo soak binary"; "${go}" tool nm "${tsan_bin}" > "${dir}/tsan.nm" || fail "could not inspect TSAN soak binary"
    ! grep -Eq '__tsan|runtime/cgo' "${dir}/purego.nm" || fail "PureGo soak binary contains TSAN/cgo symbols"; grep -q '__tsan' "${dir}/tsan.nm" || fail "TSAN soak binary contains no __tsan symbols"
    cp "${go}.benchmark-attestation" "${dir}/toolchain-attestation" || fail "could not retain validated toolchain receipt"
    local receipt_hash
    receipt_hash="$(hash_file "${dir}/toolchain-attestation")"
    local goos goarch
    goos="$("${go}" env GOOS)"; goarch="$("${go}" env GOARCH)"
    [[ "${goos}" =~ ^[A-Za-z0-9_]+$ && "${goarch}" =~ ^[A-Za-z0-9_]+$ ]] || fail "fork go reported malformed GOOS/GOARCH"
    { echo format=soak-identity-v2; echo "mode=${MODE}"; echo "source_hash=${source_hash}"; echo "workload_hash=${workload_hash}"; echo "purego_hash=${pure_hash}"; echo "tsan_hash=${tsan_hash}"; echo "toolchain_receipt_sha256=${receipt_hash}"; echo 'purego_backend=CGO_ENABLED=0 race_kolkov_import.go CgoFiles='; echo "tsan_backend=CGO_ENABLED=1 SysoFiles=race_${goos}_${goarch}.syso"; } > "${dir}/identity.env"
    local backend binary
    for backend in purego tsan; do binary="${dir}/${backend}.soak"; SOAK_MODE="${MODE}" SOAK_BACKEND="${backend}" SOAK_DURATION="${DURATION}" SOAK_INTERVAL="${INTERVAL}" "${binary}" > "${dir}/${backend}.tsv" 2>&1 || fail "${backend} soak process failed"; done
}
main() {
    local dir identity_hash strict=true
    dir="${EVIDENCE_DIR:-${OUTPUT_DIR}}"; mkdir -p "${dir}"
    if [[ -n "${EVIDENCE_DIR}" ]]; then
        [[ "${FIXTURE_MODE}" == true ]] && strict=false
        validate_identity "${dir}" "${strict}"
    else
        run_generated "${dir}"
        validate_identity "${dir}" true
    fi
    validate_rows PureGo "${dir}/purego.tsv" "${dir}/purego.normalized.tsv" purego; validate_rows TSAN "${dir}/tsan.tsv" "${dir}/tsan.normalized.tsv" tsan
    identity_hash="$(hash_file "${dir}/identity.env")"; validate_metrics "${dir}" "${identity_hash}"; echo "Validated ${MODE} soak evidence in ${dir}"
}
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
