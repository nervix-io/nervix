#!/usr/bin/env bash
set -euo pipefail

# Self-test of the shared harness of correctness reproductions: the expected-failure option's rules;
# the burst load's record keeping and ledger reconstruction (burst-load.sh); the negative-outcome
# counters (negative-outcomes.sh); the strict verdict over several sources with its counter and
# error-event inputs (verify-strict-delivery.sh); and the judgement of an expected failure
# (expected-failure.sh). Every check runs without Docker or a product image.

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() {
    printf 'correctness harness self-test failed: %s\n' "$*" >&2
    exit 1
}

expect_setup_rejection() {
    local case_name="$1"
    local expected="$2"
    shift 2
    local status=0
    "$@" >"${tmp_dir}/setup-rejection.out" 2>&1 || status=$?
    [[ "${status}" -eq 2 ]] || fail "${case_name} returned ${status}, expected setup error 2"
    grep -Fq -- "${expected}" "${tmp_dir}/setup-rejection.out" \
        || fail "${case_name} did not explain the rejected option: $(cat "${tmp_dir}/setup-rejection.out")"
}

# Runs COMMAND..., which must exit STATUS.
expect_status() {
    local case_name="$1"
    local expected="$2"
    shift 2
    local status=0
    "$@" >"${tmp_dir}/status.out" 2>&1 || status=$?
    [[ "${status}" -eq "${expected}" ]] \
        || fail "${case_name} returned ${status}, expected ${expected}: $(cat "${tmp_dir}/status.out")"
}

# --- The expected-failure option -----------------------------------------------------------------

expect_setup_rejection 'an expected failure needs a scenario that names symptoms' \
    '--expect-failure applies only to a scenario whose verdict names symptoms' \
    "${chaos_dir}/run-baseline.sh" --scenario stateful --image fixture --expect-failure missing-records
expect_setup_rejection 'an expected failure lists no empty symptom' '--expect-failure lists an empty symptom' \
    "${chaos_dir}/run-baseline.sh" --scenario stateful --image fixture --expect-failure missing-records,,error-events

# --- The burst load ------------------------------------------------------------------------------

# Two payloads of a burst: a JSON array of eight records, which an ingestor's codec unfolds, and one
# record of its own.
jq -nc '[range(0; 8) as $sequence | {event_id: "run-\($sequence)", sequence: $sequence}]' >"${tmp_dir}/payloads.ndjson"
jq -nc '{event_id: "run-8", sequence: 8}' >>"${tmp_dir}/payloads.ndjson"
jq -nc '{event_id: "run-9", sequence: 9}' | jq -sc . >>"${tmp_dir}/payloads.ndjson"

# Stand-ins for the controller: a topic whose end offset advances by what the stand-in kcat produced,
# and a kcat that produces, or consumes back, the stand-in topic.
artifact_dir="${tmp_dir}/run"
mkdir -p "${artifact_dir}/traffic" "${artifact_dir}/fixtures"
printf '{"loads":[]}\n' >"${artifact_dir}/manifest.json"
topic_file="${tmp_dir}/topic.ndjson"
: >"${topic_file}"
kcat_takes=all
topic_end_offset() { wc -l <"${topic_file}" | tr -d ' '; }
epoch_ms() { printf '1000\n'; }
update_manifest() {
    local filter="$1"
    shift
    jq "$@" "${filter}" "${artifact_dir}/manifest.json" >"${artifact_dir}/manifest.json.tmp"
    mv "${artifact_dir}/manifest.json.tmp" "${artifact_dir}/manifest.json"
}
kcat() {
    if [[ " $* " == *" -P "* ]]; then
        if [[ "${kcat_takes}" == all ]]; then
            cat >>"${topic_file}"
        else
            head -n 1 >>"${topic_file}"
            return 1
        fi
    else
        cat "${topic_file}"
    fi
}
# shellcheck source=../burst-load.sh
source "${chaos_dir}/burst-load.sh"
burst_load_produce stand-in chaos_stand_in "${tmp_dir}/payloads.ndjson" || fail 'a whole burst was refused'
jq -e '.loads == [{service: "stand-in", topic: "chaos_stand_in", shape: "burst",
                   bursts: "traffic/stand-in-bursts.ndjson"}]' "${artifact_dir}/manifest.json" >/dev/null \
    || fail 'the first burst did not declare a load of shape burst'
jq -e -s 'length == 1 and .[0].payloads == 3 and .[0].records == 10 and .[0].offset_before == 0
          and .[0].offset_after == 3 and .[0].producer_exit == 0' \
    "${artifact_dir}/traffic/stand-in-bursts.ndjson" >/dev/null || fail 'the burst record is wrong'
burst_load_accepted stand-in chaos_stand_in "${tmp_dir}/accepted.ndjson" || fail 'the accepted ledger was refused'
jq -e -s 'length == 10 and map(.sequence) == [range(0; 10)]' "${tmp_dir}/accepted.ndjson" >/dev/null \
    || fail 'the accepted ledger did not unfold every payload in order'
kcat_takes=one
status=0
burst_load_produce stand-in chaos_stand_in "${tmp_dir}/payloads.ndjson" 2>"${tmp_dir}/burst.err" || status=$?
[[ "${status}" -eq 1 ]] || fail 'a burst whose producer failed was accepted'
jq -e -s 'length == 2 and .[1].burst == 2 and .[1].producer_exit == 1' \
    "${artifact_dir}/traffic/stand-in-bursts.ndjson" >/dev/null || fail 'the failed burst was not recorded'
jq -e '.loads | length == 1' "${artifact_dir}/manifest.json" >/dev/null || fail 'a second burst declared the load again'
unset -f kcat topic_end_offset epoch_ms update_manifest

# --- The negative-outcome counters ---------------------------------------------------------------

outcomes="${chaos_dir}/negative-outcomes.sh"
metrics_series() {
    local outcome_failed="$1" shutdown="$2" unconfirmed="$3" unsent="$4"
    local ingestor
    for ingestor in stand_in_acked stand_in_unacked; do
        local labels="connector=\"KAFKA\",domain=\"stand_in\",entity=\"TOPIC\",ingestor=\"${ingestor}\",instance=\"0\""
        printf 'nervix_ingestor_ack_outcomes_total{%s,outcome="acknowledged",physical_node_id="node-1"} 9\n' "${labels}"
        printf 'nervix_ingestor_ack_outcomes_total{%s,outcome="failed",physical_node_id="node-1"} %s\n' "${labels}" "${outcome_failed}"
        printf 'nervix_ingestor_ack_outcomes_total{%s,outcome="timed_out",physical_node_id="node-1"} 0\n' "${labels}"
        printf 'nervix_ingestor_ack_outcomes_total{%s,outcome="shutdown",physical_node_id="node-1"} %s\n' "${labels}" "${shutdown}"
        printf 'nervix_ingestor_transport_acknowledgements_total{%s,operation="reject",outcome="succeeded",physical_node_id="node-1"} 0\n' "${labels}"
        printf 'nervix_ingestor_transport_acknowledgements_total{%s,operation="acknowledge",outcome="succeeded",physical_node_id="node-1"} 4\n' "${labels}"
        printf 'nervix_ingestor_unconfirmed_stop_positions_total{%s,physical_node_id="node-1"} %s\n' "${labels}" "${unconfirmed}"
        # Another domain's ingestor of the same name never counts.
        printf 'nervix_ingestor_ack_outcomes_total{connector="KAFKA",domain="other",entity="TOPIC",ingestor="%s",instance="0",outcome="failed",physical_node_id="node-1"} 7\n' "${ingestor}"
    done
    local emitter_labels='connector="KAFKA",domain="stand_in",emitter="stand_in_emitter",entity="TOPIC",instance="-",physical_node_id="node-2"'
    printf 'nervix_emitter_records_failed_total{%s} 0\n' "${emitter_labels}"
    printf 'nervix_emitter_unsent_records_total{%s} %s\n' "${emitter_labels}" "${unsent}"
    printf 'nervix_emitter_records_rejected_total{%s,code="external"} 0\n' "${emitter_labels}"
    # An emitter whose name extends the selected one's never counts.
    printf 'nervix_emitter_unsent_records_total{connector="KAFKA",domain="stand_in",emitter="stand_in_emitter_extra",entity="TOPIC",instance="-",physical_node_id="node-2"} 5\n'
}
snapshot() {
    local output="$1"
    shift
    "${outcomes}" snapshot --domain stand_in --ingestor stand_in_acked --ingestor stand_in_unacked \
        --emitter stand_in_emitter --output "${output}" "$@"
}
printf '# HELP nervix_build_info a unit not yet started has no counters\nnervix_build_info{version="x"} 1\n' \
    >"${tmp_dir}/metrics-before.txt"
metrics_series 0 0 0 0 >"${tmp_dir}/metrics-clean.txt"
metrics_series 0 1 1 0 >"${tmp_dir}/metrics-cut.txt"
metrics_series 2 0 0 3 >"${tmp_dir}/metrics-failed.txt"
snapshot "${tmp_dir}/outcomes-before.json" "${tmp_dir}/metrics-before.txt"
snapshot "${tmp_dir}/outcomes-clean.json" "${tmp_dir}/metrics-clean.txt"
snapshot "${tmp_dir}/outcomes-cut.json" "${tmp_dir}/metrics-cut.txt"
snapshot "${tmp_dir}/outcomes-failed.json" "${tmp_dir}/metrics-failed.txt"
jq -e '.ingestors.stand_in_acked.ack_failed == {value: 0, series: 0}' "${tmp_dir}/outcomes-before.json" >/dev/null \
    || fail 'a counter that does not exist yet did not read as no series and zero'
jq -e '.ingestors.stand_in_acked.ack_failed == {value: 0, series: 1}
       and .ingestors.stand_in_acked.transport_rejections == {value: 0, series: 1}
       and .emitters.stand_in_emitter.records_unsent == {value: 0, series: 1}' \
    "${tmp_dir}/outcomes-clean.json" >/dev/null || fail 'the snapshot read another domain or entity'
expect_status 'unchanged counters pass' 0 "${outcomes}" compare --before "${tmp_dir}/outcomes-before.json" \
    --after "${tmp_dir}/outcomes-clean.json" --output "${tmp_dir}/compare.json"
expect_status 'a stop that ended waits fails' 1 "${outcomes}" compare --before "${tmp_dir}/outcomes-before.json" \
    --after "${tmp_dir}/outcomes-cut.json" --output "${tmp_dir}/compare.json"
jq -e '.negative_acknowledgements == 4 and .emitter_failures == 0
       and ([.increases[].counter] | unique == ["ack_shutdown", "unconfirmed_stop_positions"])' \
    "${tmp_dir}/compare.json" >/dev/null || fail 'the ended waits were not counted as negative acknowledgements'
expect_status 'failed trees and unsent records fail' 1 "${outcomes}" compare --before "${tmp_dir}/outcomes-clean.json" \
    --after "${tmp_dir}/outcomes-failed.json" --output "${tmp_dir}/compare.json"
jq -e '.negative_acknowledgements == 4 and .emitter_failures == 3' "${tmp_dir}/compare.json" >/dev/null \
    || fail 'failed trees and unsent records were not both counted'
expect_status 'a counter that went back cannot be judged' 2 "${outcomes}" compare \
    --before "${tmp_dir}/outcomes-cut.json" --after "${tmp_dir}/outcomes-clean.json" --output "${tmp_dir}/compare.json"
expect_status 'a counter missing after the run cannot be judged' 2 "${outcomes}" compare \
    --before "${tmp_dir}/outcomes-before.json" --after "${tmp_dir}/outcomes-before.json" --output "${tmp_dir}/compare.json"
printf 'nervix_ingestor_ack_outcomes_total{domain="stand_in",ingestor="stand_in_acked",outcome="failed"} lots\n' \
    >"${tmp_dir}/metrics-unreadable.txt"
expect_status 'an unreadable counter value is refused' 2 snapshot "${tmp_dir}/outcomes-unreadable.json" \
    "${tmp_dir}/metrics-unreadable.txt"

# --- The strict verdict --------------------------------------------------------------------------

strict="${chaos_dir}/verify-strict-delivery.sh"
record() {
    jq -nc --arg source "$1" --argjson sequence "$2" \
        '{event_id: ("run-\($source)-\($sequence)"), source: $source, lane: "deliver",
          branch_name: (if $sequence % 2 == 0 then "alpha" else "beta" end), sequence: $sequence,
          content: ("payload-\($sequence)")}'
}
for source in acked unacked; do
    for sequence in 0 1 2 3; do
        record "${source}" "${sequence}"
    done >"${tmp_dir}/accepted-${source}.ndjson"
done
cat "${tmp_dir}/accepted-acked.ndjson" "${tmp_dir}/accepted-unacked.ndjson" >"${tmp_dir}/output-exact.ndjson"
# The observers attached at 100 and 110 and the window runs from 200 to 900.
for host in nervix-1 nervix-2; do
    {
        printf '100 created subscription '"'"'events_1'"'"' in domain '"'"'chaos_observer'"'"'\n'
        printf '110 listening for events from relay '"'"'chaos_observer_quiet'"'"'. Press Ctrl-C to stop.\n'
    } >"${tmp_dir}/events-${host}.log"
done
cp "${tmp_dir}/events-nervix-1.log" "${tmp_dir}/quiet-nervix-1.log"
cp "${tmp_dir}/events-nervix-2.log" "${tmp_dir}/quiet-nervix-2.log"
printf '{"start_ns": 200, "end_ns": 900}\n' >"${tmp_dir}/window.json"
strict_verdict() {
    local output="$1"
    shift
    "${strict}" --output "${output}" --result "${tmp_dir}/strict.json" \
        --source "acked=${tmp_dir}/accepted-acked.ndjson" --source "unacked=${tmp_dir}/accepted-unacked.ndjson" "$@"
}
events_inputs() {
    printf '%s\n' --events-window "${tmp_dir}/window.json" --events "${tmp_dir}/$1-nervix-1.log" \
        --events "${tmp_dir}/$1-nervix-2.log"
}
mapfile -t quiet < <(events_inputs quiet)
expect_status 'every record exactly once passes' 0 strict_verdict "${tmp_dir}/output-exact.ndjson" \
    --negative-outcomes "${tmp_dir}/outcomes-before.json" "${tmp_dir}/outcomes-clean.json" "${quiet[@]}"
jq -e '.verdict == "pass" and .symptoms == [] and .error_events.complete and .error_events.count == 0' \
    "${tmp_dir}/strict.json" >/dev/null || fail 'the exact delivery did not pass cleanly'

expect_symptom() {
    local case_name="$1"
    local symptom="$2"
    local check="$3"
    shift 3
    expect_status "${case_name}" 1 strict_verdict "$@"
    jq -e --arg symptom "${symptom}" "any(.symptoms[]; .name == \$symptom and (${check}))" \
        "${tmp_dir}/strict.json" >/dev/null \
        || fail "${case_name} did not name ${symptom}: $(jq -c '.symptoms' "${tmp_dir}/strict.json")"
}
# The acknowledged source sent two records again after a restart: replay duplicates are refused.
{ cat "${tmp_dir}/output-exact.ndjson"; record acked 1; record acked 2; } >"${tmp_dir}/output-replayed.ndjson"
expect_symptom 'replay duplicates fail' duplicate-records '.sources == [{source: "acked", count: 2}]' \
    "${tmp_dir}/output-replayed.ndjson"
grep -v '"run-unacked-3"' "${tmp_dir}/output-exact.ndjson" >"${tmp_dir}/output-lost.ndjson"
expect_symptom 'a lost record fails' missing-records '.sources == [{source: "unacked", count: 1}]' \
    "${tmp_dir}/output-lost.ndjson"
sed 's/"payload-2"/"payload-changed"/' "${tmp_dir}/output-exact.ndjson" >"${tmp_dir}/output-changed.ndjson"
expect_symptom 'a changed record fails' changed-records '.sources | length == 2' "${tmp_dir}/output-changed.ndjson"
{ cat "${tmp_dir}/output-exact.ndjson"; record other 9; } >"${tmp_dir}/output-unexpected.ndjson"
expect_symptom 'a record no source accepted fails' unexpected-records '.count == 1' "${tmp_dir}/output-unexpected.ndjson"
expect_symptom 'negative acknowledgements fail' negative-acknowledgements '.count == 4' \
    "${tmp_dir}/output-exact.ndjson" --negative-outcomes "${tmp_dir}/outcomes-before.json" "${tmp_dir}/outcomes-cut.json"
expect_symptom 'emitter failures fail' emitter-failures '.count == 3' \
    "${tmp_dir}/output-exact.ndjson" --negative-outcomes "${tmp_dir}/outcomes-clean.json" "${tmp_dir}/outcomes-failed.json"
printf '500 [events] runtime ERROR: processor branch '"'"'stand_in_forward'"'"' stopped without its clock\n' \
    >>"${tmp_dir}/events-nervix-1.log"
printf '950 [events] runtime ERROR: after the window\n' >>"${tmp_dir}/events-nervix-2.log"
printf '300 [events] membership INFO: node '"'"'node-3'"'"' joined\n' >>"${tmp_dir}/events-nervix-2.log"
mapfile -t noisy < <(events_inputs events)
expect_symptom 'a runtime error event inside the window fails' error-events '.count == 1' \
    "${tmp_dir}/output-exact.ndjson" "${noisy[@]}"
printf '400 observer: the notice follow ended\n' >>"${tmp_dir}/quiet-nervix-2.log"
expect_status 'a quiet window with a gap cannot be judged' 2 strict_verdict "${tmp_dir}/output-exact.ndjson" "${quiet[@]}"
printf '400 observer: the notice follow ended\n' >>"${tmp_dir}/events-nervix-2.log"
expect_symptom 'an error event stands despite a gap elsewhere' error-events '.count == 1' \
    "${tmp_dir}/output-exact.ndjson" "${noisy[@]}"
printf '{"start_ns": 105, "end_ns": 900}\n' >"${tmp_dir}/window.json"
grep -v 'runtime ERROR' "${tmp_dir}/events-nervix-1.log" >"${tmp_dir}/quiet-nervix-1.log"
grep -v 'runtime ERROR' "${tmp_dir}/events-nervix-1.log" >"${tmp_dir}/quiet-nervix-2.log"
expect_status 'an observer attached after the window opened cannot vouch for it' 2 \
    strict_verdict "${tmp_dir}/output-exact.ndjson" "${quiet[@]}"
cp "${tmp_dir}/accepted-acked.ndjson" "${tmp_dir}/accepted-shared.ndjson"
expect_status 'sources sharing an event ID cannot be judged' 2 "${strict}" \
    --output "${tmp_dir}/output-exact.ndjson" --result "${tmp_dir}/strict.json" \
    --source "acked=${tmp_dir}/accepted-acked.ndjson" --source "unacked=${tmp_dir}/accepted-shared.ndjson"
expect_status 'an empty accepted ledger cannot be judged' 2 "${strict}" \
    --output "${tmp_dir}/output-exact.ndjson" --result "${tmp_dir}/strict.json" --source "acked=${tmp_dir}/none.ndjson"

# --- The expected failure ------------------------------------------------------------------------

# shellcheck source=../expected-failure.sh
source "${chaos_dir}/expected-failure.sh"
jq -n '{scenario: "stand-in", symptoms: [{name: "duplicate-records"}, {name: "missing-records"},
        {name: "unexpected-records"}, {name: "error-events"}]}' >"${tmp_dir}/symptoms.json"
expect_outcome() {
    local case_name="$1"
    local outcome="$2"
    shift 2
    local record
    record="$(expected_failure_record "$@")"
    jq -e --arg outcome "${outcome}" '.outcome == $outcome' <<<"${record}" >/dev/null \
        || fail "${case_name}: expected ${outcome}, got ${record}"
}
expect_outcome 'named symptoms of the findings phase reproduce' reproduced \
    1 product 'stand-in product findings' stand-in '' "${tmp_dir}/symptoms.json" \
    duplicate-records missing-records error-events
expect_outcome 'a symptom the verdict did not name is not reproduced' other-failure \
    1 product 'stand-in product findings' stand-in '' "${tmp_dir}/symptoms.json" \
    missing-records negative-acknowledgements
expect_outcome 'a product failure in another phase is not reproduced' other-failure \
    1 product 'external ledger verification' stand-in '' "${tmp_dir}/symptoms.json" missing-records
expect_outcome 'an injection failure is not reproduced' other-failure \
    1 injection 'stand-in: the fault' stand-in '' "${tmp_dir}/symptoms.json" \
    missing-records
expect_outcome 'a passing run is not reproduced' not-reproduced \
    0 product complete stand-in '' "${tmp_dir}/symptoms.json" missing-records
expect_outcome 'an interrupted run is not judged' interrupted \
    130 product 'stand-in product findings' stand-in INT "${tmp_dir}/symptoms.json" missing-records
expect_outcome 'a run without symptoms is not reproduced' other-failure \
    1 product 'stand-in product findings' stand-in '' "${tmp_dir}/absent.json" missing-records
record="$(expected_failure_record 1 product 'stand-in product findings' stand-in '' \
    "${tmp_dir}/symptoms.json" unexpected-records missing-records)"
jq -e '.symptoms == ["missing-records", "unexpected-records"] and .missing == []
       and .observed == ["duplicate-records", "error-events", "missing-records", "unexpected-records"]
       and .failure == {category: "product", phase: "stand-in product findings", exit_code: 1}' \
    <<<"${record}" >/dev/null || fail "the expected-failure record is incomplete: ${record}"

printf 'correctness harness self-test passed\n'
