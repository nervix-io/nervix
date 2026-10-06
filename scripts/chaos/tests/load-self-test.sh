#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
chaos_dir="$(cd "${script_dir}/.." && pwd)"
# shellcheck source=../tool-images.sh
source "${chaos_dir}/tool-images.sh"

fail() {
    printf 'load self-test failed: %s\n' "$*" >&2
    exit 1
}

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

# The pacing verifier judges the gaps between the create times the broker stored.
verifier="${chaos_dir}/verify-load-pacing.sh"

# Writes COUNT "CREATE_TIME_MS OFFSET" lines whose gaps repeat the GAPS list.
write_timestamps() {
    local output="$1" count="$2"
    shift 2
    awk -v count="${count}" -v gaps="$*" 'BEGIN {
        cycle = split(gaps, gap, " ")
        at = 1791260000000
        for (offset = 0; offset < count; offset++) {
            printf "%d %d\n", at, offset
            at += gap[offset % cycle + 1]
        }
    }' >"${output}"
}

expect_pacing() {
    local case_name="$1" expected_status="$2" timestamps="$3" interval_ms="$4" jq_check="$5"
    local result="${tmp_dir}/${case_name}.json"
    local status=0
    "${verifier}" "${timestamps}" "${interval_ms}" "${result}" >/dev/null 2>&1 || status=$?
    [[ "${status}" -eq "${expected_status}" ]] \
        || fail "${case_name} returned ${status}, expected ${expected_status}"
    jq -e "${jq_check}" "${result}" >/dev/null \
        || fail "${case_name} did not report the expected pacing: $(jq -c . "${result}")"
}

# One record per interval, within the resolution of the clock the load schedules on.
write_timestamps "${tmp_dir}/paced.txt" 200 500 491 509 500 503 497
expect_pacing paced 0 "${tmp_dir}/paced.txt" 500 '
    .verdict == "pass" and .interval_ms == 500 and .records == 200 and .span_ms == 99500
    and .gap_ms == {minimum: 491, median: 500, p95: 509, maximum: 509, mean: 500}
    and .minimum_allowed_gap_ms == 125 and .early_gap_count == 0 and .early_gaps == []
    and .late_gap_count == 0 and .longest_gap == {offset: 3, gap_ms: 509}'

# Bursts fail: nine records arrive together, then nothing for nine intervals.
write_timestamps "${tmp_dir}/blocks.txt" 181 1 0 1 0 0 1 0 1 4496
expect_pacing blocks 1 "${tmp_dir}/blocks.txt" 500 '
    .verdict == "fail" and .records == 181 and .early_gap_count == 160
    and (.early_gaps | length) == 20 and .early_gaps[0] == {offset: 1, gap_ms: 1}
    and .gap_ms.median == 1 and .gap_ms.maximum == 4496 and .late_gap_count == 20
    and .longest_gap == {offset: 9, gap_ms: 4496}'

# A load held at a record boundary, or stalled behind its producer, is late and never bursting.
write_timestamps "${tmp_dir}/held.txt" 40 1000 1000 1000 1000 1000 1000 1000 1000 1000 1000 1000 55400
expect_pacing held 0 "${tmp_dir}/held.txt" 1000 '
    .verdict == "pass" and .early_gap_count == 0 and .late_gap_count == 3
    and .longest_gap == {offset: 12, gap_ms: 55400}'

# Two records a quarter of the interval apart are the closest the verdict accepts.
write_timestamps "${tmp_dir}/quarter.txt" 4 500 125 500
expect_pacing quarter 0 "${tmp_dir}/quarter.txt" 500 \
    '.verdict == "pass" and .gap_ms.minimum == 125 and .minimum_allowed_gap_ms == 125'
write_timestamps "${tmp_dir}/under-quarter.txt" 4 500 124 500
expect_pacing under-quarter 1 "${tmp_dir}/under-quarter.txt" 500 \
    '.verdict == "fail" and .early_gap_count == 1 and .early_gaps == [{offset: 2, gap_ms: 124}]'

# A wall clock stepped backwards between two records reads as a burst and never as a pass.
printf '%s\n' '1791260000500 0' '1791260001000 1' '1791260000990 2' '1791260001490 3' \
    >"${tmp_dir}/stepped.txt"
expect_pacing stepped 1 "${tmp_dir}/stepped.txt" 500 \
    '.verdict == "fail" and .early_gaps == [{offset: 2, gap_ms: -10}]'

# A single record has no gap to judge.
write_timestamps "${tmp_dir}/single.txt" 1 500
expect_pacing single 0 "${tmp_dir}/single.txt" 500 \
    '.verdict == "pass" and .records == 1 and .span_ms == 0 and .gap_ms == null and .longest_gap == null'

# Missing, malformed and incomplete evidence is an error, not a verdict.
: >"${tmp_dir}/empty.txt"
expect_pacing empty 2 "${tmp_dir}/empty.txt" 500 \
    '.verdict == "error" and .category == "invalid_timestamps"'
expect_pacing missing 2 "${tmp_dir}/absent.txt" 500 \
    '.verdict == "error" and .category == "invalid_timestamps"'
# kcat prints -1 for a record the broker stored without a timestamp.
printf '%s\n' '1791260000000 0' '-1 1' >"${tmp_dir}/untimed.txt"
expect_pacing untimed 2 "${tmp_dir}/untimed.txt" 500 \
    '.verdict == "error" and .category == "invalid_timestamps"'
printf '%s\n' '1791260000000 0' '1791260000500 1' '1791260001500 3' >"${tmp_dir}/skipped.txt"
expect_pacing skipped 2 "${tmp_dir}/skipped.txt" 500 \
    '.verdict == "error" and .category == "invalid_timestamps"
     and (.message | contains("consecutive offsets"))'
expect_pacing zero-interval 2 "${tmp_dir}/paced.txt" 0 \
    '.verdict == "error" and .category == "invalid_interval"'
expect_pacing fractional-interval 2 "${tmp_dir}/paced.txt" 0.5 \
    '.verdict == "error" and .category == "invalid_interval"'
status=0
"${verifier}" "${tmp_dir}/paced.txt" 500 >/dev/null 2>&1 || status=$?
[[ "${status}" -eq 2 ]] || fail "the pacing verifier accepted a call without a result path"

# The load script runs as a run starts it: in the pinned kcat image, under that image's shell.
# Its clock, its sleep and its producer are stand-ins, so every schedule below is exact and takes
# no time. The clock is a file bound over /proc/uptime that reports hundredths of a second, as the
# kernel's does; `sleep` and `kcat` move it forward by the time they would have taken. One
# container runs every case in turn, each with its own fixture, traffic directory and clock.
stand_ins="${tmp_dir}/stand-ins"
loads="${tmp_dir}/loads"
mkdir -p "${stand_ins}" "${loads}"
: >"${tmp_dir}/uptime"
cat >"${stand_ins}/advance-clock" <<'EOF'
#!/bin/sh
set -eu
read -r now </opt/chaos/clock/now-ms
now=$((now + $1))
printf '%d\n' "${now}" >/opt/chaos/clock/now-ms
printf '%d.%02d 0.00\n' "$((now / 1000))" "$((now / 10 % 100))" >/opt/chaos/uptime
EOF
cat >"${stand_ins}/sleep" <<'EOF'
#!/bin/sh
set -eu
case "$1" in
    *.*)
        whole="${1%%.*}"
        fraction="${1#*.}000"
        fraction="${fraction%"${fraction#???}"}"
        ;;
    *)
        whole="$1"
        fraction=000
        ;;
esac
slept_ms=$((whole * 1000 + 1${fraction} - 1000))
printf '%s\n' "$1" >>/opt/chaos/clock/sleeps
advance-clock "${slept_ms}"
# A held load polls its hold file every 200 ms. The runner's part is played here: at the listed
# poll it moves the hold boundary, releases the load or stops it. A hold that nothing releases
# would poll for ever, so the hundredth poll stops the load and its case fails on what it produced.
if [ "${slept_ms}" -eq 200 ]; then
    printf 'poll\n' >>/opt/chaos/clock/polls
    poll="$(wc -l </opt/chaos/clock/polls)"
    while read -r at action boundary; do
        [ "${at}" = "${poll}" ] || continue
        case "${action}" in
            move) printf '%s\n' "${boundary}" >"/opt/chaos/traffic/${CHAOS_LOAD_HOLD_FILE:-hold-load}" ;;
            release) rm -f "/opt/chaos/traffic/${CHAOS_LOAD_HOLD_FILE:-hold-load}" ;;
            stop) : >"/opt/chaos/traffic/${CHAOS_LOAD_STOP_FILE:-stop-load}" ;;
        esac
    done </opt/chaos/clock/runner
    if [ "${poll}" -ge 100 ]; then
        : >"/opt/chaos/traffic/${CHAOS_LOAD_STOP_FILE:-stop-load}"
    fi
fi
EOF
cat >"${stand_ins}/kcat" <<'EOF'
#!/bin/sh
set -eu
IFS= read -r record
read -r now </opt/chaos/clock/now-ms
printf '%d\t%s\t%s\n' "${now}" "$*" "${record}" >>/opt/chaos/clock/calls
call="$(wc -l </opt/chaos/clock/calls)"
took="$(sed -n "${call}p" /opt/chaos/clock/call-ms)"
advance-clock "${took:-10}"
if [ "${call}" = "$(cat /opt/chaos/clock/stop-after-call)" ]; then
    : >"/opt/chaos/traffic/${CHAOS_LOAD_STOP_FILE:-stop-load}"
fi
if [ "${call}" = "$(cat /opt/chaos/clock/failing-call)" ]; then
    printf '%s\n' '% Delivery failed for message: Local: Message timed out' >&2
    exit 1
fi
EOF
cat >"${stand_ins}/run-loads" <<'EOF'
#!/bin/sh
set -eu
for load in /opt/chaos/loads/*; do
    ln -sfn "${load}/clock" /opt/chaos/clock
    ln -sfn "${load}/traffic" /opt/chaos/traffic
    ln -sfn "${load}/input.ndjson" /opt/chaos/input.ndjson
    advance-clock 0
    status=0
    # The settings are NAME=VALUE words for the load's environment.
    # shellcheck disable=SC2046
    env $(cat "${load}/settings") /bin/sh /opt/chaos/continuous-load.sh \
        >"${load}/stdout.txt" 2>"${load}/stderr.txt" || status=$?
    printf '%d\n' "${status}" >"${load}/status"
done
EOF
chmod +x "${stand_ins}/advance-clock" "${stand_ins}/sleep" "${stand_ins}/kcat" "${stand_ins}/run-loads"

# Prepares a load case: RECORDS fixture records, a clock that starts at START_MS, and the load's
# INTERVAL_MS with any further NAME=VALUE settings for its environment.
prepare_load() {
    local case_name="$1" records="$2" start_ms="$3" interval_ms="$4"
    shift 4
    load_dir="${loads}/${case_name}"
    mkdir -p "${load_dir}/clock" "${load_dir}/traffic"
    jq -nc --arg run_id "load-${case_name}" --argjson count "${records}" \
        -f "${chaos_dir}/fixtures/generate-baseline.jq" >"${load_dir}/input.ndjson"
    printf '%s\n' "CHAOS_LOAD_INTERVAL_MS=${interval_ms}" "$@" >"${load_dir}/settings"
    printf '%d\n' "${start_ms}" >"${load_dir}/clock/now-ms"
    : >"${load_dir}/clock/calls"
    : >"${load_dir}/clock/sleeps"
    : >"${load_dir}/clock/call-ms"
    : >"${load_dir}/clock/polls"
    : >"${load_dir}/clock/runner"
    printf '0\n' >"${load_dir}/clock/stop-after-call"
    printf '0\n' >"${load_dir}/clock/failing-call"
}

# Reads a finished case: its exit status into load_status and the uptime at which it started each
# kcat call into load_starts.
finish_load() {
    local case_name="$1"
    load_dir="${loads}/${case_name}"
    [[ -s "${load_dir}/status" ]] || fail "the ${case_name} load left no exit status"
    load_status="$(<"${load_dir}/status")"
    mapfile -t load_starts < <(cut -f 1 "${load_dir}/clock/calls")
}

# Every call hands kcat one fixture record, in fixture order, for the load's topic.
expect_produced() {
    local case_name="$1" expected="$2" topic="${3:-chaos_input}"
    [[ "${#load_starts[@]}" -eq "${expected}" ]] \
        || fail "${case_name} started ${#load_starts[@]} kcat calls, expected ${expected}"
    cmp -s <(cut -f 3 "${load_dir}/clock/calls") <(head -n "${expected}" "${load_dir}/input.ndjson") \
        || fail "${case_name} did not hand kcat the fixture records in order, one per call"
    [[ "$(cut -f 2 "${load_dir}/clock/calls" | sort -u)" == "-b broker:9092 -P -t ${topic}" ]] \
        || fail "${case_name} did not produce every record to ${topic} on the run's broker"
}

# A 100 ms interval with calls of 10 ms. The uptime is past 2^32 milliseconds, as on a worker that
# has been up for longer than 49 days.
prepare_load steady 30 40000000123 100
# An interval the clock cannot resolve, with calls of varying length.
prepare_load unresolved 40 7000005 105
printf '%s\n' 13 13 13 27 13 4 13 13 >"${load_dir}/clock/call-ms"
# Calls that return exactly half an interval after their record was due, just past it, and far
# past it.
prepare_load late-return 8 500000 200
printf '%s\n' 10 100 10 110 10 750 10 10 >"${load_dir}/clock/call-ms"
# The stop file appears while the fifth record is produced, or before the first.
prepare_load stopped 20 900000 100
printf '5\n' >"${load_dir}/clock/stop-after-call"
prepare_load stopped-first 20 900000 100
: >"${load_dir}/traffic/stop-load"
# kcat cannot deliver the fourth record.
prepare_load undelivered 20 900000 100
printf '4\n' >"${load_dir}/clock/failing-call"
# The runner holds the load at five records before it starts, moves the boundary to seven and then
# releases the load.
prepare_load held 12 600000 1000
printf '5\n' >"${load_dir}/traffic/hold-load"
printf '%s\n' '10 move 7' '13 release' >"${load_dir}/clock/runner"
# The runner holds the load where it stands and then stops it.
prepare_load held-stopped 12 600000 1000
: >"${load_dir}/traffic/hold-load"
printf '%s\n' '4 stop' >"${load_dir}/clock/runner"
# A load with its own topic, stop file and hold file, beside another load's hold and stop files.
prepare_load named 12 600000 1000 CHAOS_LOAD_TOPIC=chaos_state_input \
    CHAOS_LOAD_STOP_FILE=stop-state-load CHAOS_LOAD_HOLD_FILE=hold-state-load
: >"${load_dir}/traffic/hold-load"
: >"${load_dir}/traffic/stop-load"
printf '2\n' >"${load_dir}/traffic/hold-state-load"
printf '%s\n' '5 release' >"${load_dir}/clock/runner"
printf '6\n' >"${load_dir}/clock/stop-after-call"

docker image inspect "${chaos_kcat_image}" >/dev/null 2>&1 \
    || docker pull --quiet "${chaos_kcat_image}" >/dev/null
docker run --rm --network none \
    --env PATH=/opt/chaos/stand-ins:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    --volume "${stand_ins}:/opt/chaos/stand-ins:ro" \
    --volume "${loads}:/opt/chaos/loads" \
    --volume "${tmp_dir}/uptime:/opt/chaos/uptime" \
    --volume "${tmp_dir}/uptime:/proc/uptime:ro" \
    --volume "${chaos_dir}/continuous-load.sh:/opt/chaos/continuous-load.sh:ro" \
    --entrypoint /bin/sh "${chaos_kcat_image}" /opt/chaos/stand-ins/run-loads \
    || fail 'the load cases could not run in the pinned kcat image'

# The time a call takes does not stretch the interval.
finish_load steady
[[ "${load_status}" -eq 0 ]] || fail "a load that reached the end of its fixture exited ${load_status}"
expect_produced steady 30
for index in "${!load_starts[@]}"; do
    [[ "$((load_starts[index] - load_starts[0]))" -eq "$((index * 100))" ]] \
        || fail "record ${index} left $((load_starts[index] - load_starts[0])) ms after the first, expected $((index * 100))"
done
[[ "$(sort -u "${load_dir}/clock/sleeps")" == '0.090' ]] \
    || fail "a 10 ms call at a 100 ms interval did not leave 90 ms to sleep: $(sort -u "${load_dir}/clock/sleeps" | tr '\n' ' ')"

# An interval the clock cannot resolve still has no drift: every record leaves within one clock
# reading of its due time.
finish_load unresolved
[[ "${load_status}" -eq 0 ]] || fail "the unresolved-interval load exited ${load_status}"
expect_produced unresolved 40
for index in "${!load_starts[@]}"; do
    ((index > 0)) || continue
    late_ms="$((load_starts[index] - (7000000 + index * 105)))"
    ((late_ms >= 0 && late_ms < 10)) \
        || fail "record ${index} left ${late_ms} ms after it was due at a 105 ms interval"
done

# A call that returns within half an interval of its due time keeps the schedule; one that returns
# later restarts it, so the next record leaves a whole interval after the return.
finish_load late-return
[[ "${load_status}" -eq 0 ]] || fail "the late-return load exited ${load_status}"
expect_produced late-return 8
expected_starts=(500000 500200 500400 500600 500910 501110 502060 502260)
[[ "${load_starts[*]}" == "${expected_starts[*]}" ]] \
    || fail "calls returning 100, 110 and 750 ms into a 200 ms interval left at ${load_starts[*]}, expected ${expected_starts[*]}"

# The stop file ends the load before its next record, and the load exits cleanly.
finish_load stopped
[[ "${load_status}" -eq 0 ]] || fail "a stopped load exited ${load_status}"
expect_produced stopped 5
finish_load stopped-first
[[ "${load_status}" -eq 0 && "${#load_starts[@]}" -eq 0 ]] \
    || fail "a load stopped before its first record exited ${load_status} after ${#load_starts[@]} calls"

# A record kcat could not deliver ends the load with a failure that names it.
finish_load undelivered
[[ "${load_status}" -eq 1 ]] || fail "a load whose kcat failed exited ${load_status}, expected 1"
expect_produced undelivered 4
grep -Fxq 'load failure: kcat did not deliver record 4 to chaos_input' "${load_dir}/stderr.txt" \
    || fail "a load whose kcat failed did not name the undelivered record: $(tr '\n' ' ' <"${load_dir}/stderr.txt")"

# A hold file that names a count stops the load once it has produced that many records. A held
# record leaves when the runner releases it, and the schedule restarts from there.
finish_load held
[[ "${load_status}" -eq 0 ]] || fail "the held load exited ${load_status}"
expect_produced held 12
expected_starts=(600000 601000 602000 603000 604000 607000 608010 609610 610620 611620 612620 613620)
[[ "${load_starts[*]}" == "${expected_starts[*]}" ]] \
    || fail "a load held at five and then seven records left at ${load_starts[*]}, expected ${expected_starts[*]}"

# An empty hold file holds the load where it stands, and the stop file ends a held load.
finish_load held-stopped
[[ "${load_status}" -eq 0 && "${#load_starts[@]}" -eq 0 ]] \
    || fail "a load stopped while held exited ${load_status} after ${#load_starts[@]} calls"
[[ "$(wc -l <"${load_dir}/clock/polls")" -eq 4 ]] \
    || fail "a load stopped at its fourth hold poll kept polling: $(wc -l <"${load_dir}/clock/polls") polls"

# A load follows the hold and stop files it was given and no other load's.
finish_load named
[[ "${load_status}" -eq 0 ]] || fail "the load with its own topic and control files exited ${load_status}"
expect_produced named 6 chaos_state_input
expected_starts=(600000 601000 603000 604010 605010 606010)
[[ "${load_starts[*]}" == "${expected_starts[*]}" ]] \
    || fail "a load held by its own hold file left at ${load_starts[*]}, expected ${expected_starts[*]}"

# A scenario that finds its load stopped says why from the load's exit status. The scenario's own
# Docker calls are stand-ins here, so the checks run in a subshell.
(
    # shellcheck source=../rolling-restart-scenario.sh
    source "${chaos_dir}/rolling-restart-scenario.sh"
    scenario=load-self-test
    run_bounded() {
        shift
        "$@"
    }
    owned_service_container() {
        printf '%s\n' "$1"
    }
    container_running() {
        [[ "$1" != "${stopped_service}" ]]
    }
    # The scenario calls this stand-in through run_bounded.
    # shellcheck disable=SC2329
    docker() {
        [[ "$*" == 'inspect --format {{.State.ExitCode}} load' ]] || fail "unexpected Docker call: $*"
        printf '%s\n' "${load_exit_code}"
    }

    stopped_service=none
    load_exit_code=0
    check_support_containers || fail 'running support containers were reported stopped'

    stopped_service=load
    failure_category=product
    status=0
    check_support_containers 2>/dev/null || status=$?
    [[ "${status}" -eq 1 && "${failure_category}" == setup ]] \
        || fail "a load at the end of its fixture returned ${status} with category ${failure_category}"
    message="$(check_support_containers 2>&1 || true)"
    [[ "${message}" == 'load-self-test failure: the load stopped before fault verification: it reached the end of its fixture; increase --records' ]] \
        || fail "a load at the end of its fixture was explained as: ${message}"

    load_exit_code=1
    message="$(check_support_containers 2>&1 || true)"
    [[ "${message}" == 'load-self-test failure: the load stopped before fault verification: kcat could not deliver a record and the load exited 1; its log is in diagnostics/compose.log' ]] \
        || fail "a load whose kcat failed was explained as: ${message}"

    stopped_service=observer
    message="$(check_support_containers 2>&1 || true)"
    [[ "${message}" == 'load-self-test failure: observer stopped during fault traffic' ]] \
        || fail "a stopped observer was explained as: ${message}"
) || exit 1

printf 'load self-test passed\n'
