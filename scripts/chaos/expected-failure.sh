#!/usr/bin/env bash
# Sourced by run-baseline.sh. Owns the expected failure of a run: the record of a red run against
# an image that still has a defect, which a correctness fix's chaos reproduction keeps as its
# evidence that the scenario fails because of the defect. A scenario whose verdict names symptoms
# writes them to results/symptoms.json as {scenario, verdict, symptoms: [{name, ...}]}, and a run
# started with --expect-failure judges that file once everything else about the run is known.

# Prints the expected-failure record of a run as one JSON object, from its exit STATUS, failure
# CATEGORY and final PHASE, its SCENARIO, the SIGNAL that ended it or nothing, its SYMPTOMS file and
# the EXPECTED symptoms. Its outcome is one of:
#
#   reproduced      the scenario's verdict failed as a product failure, in the phase in which the
#                   scenario judges its findings, naming every expected symptom
#   not-reproduced  the run passed, so the image did not show the defect
#   other-failure   the run failed for another reason: a setup, controller, observation, injection or
#                   diagnostic failure, a product failure elsewhere, or one without every expected
#                   symptom
#   interrupted     a signal ended the run before it could tell
#
# A reproduced failure exits the run 0 with status expected-failure. A run that did not reproduce it
# exits nonzero: one that passed fails as unclassified, establishing no product failure, and any
# other keeps its own category and status.
expected_failure_record() {
    local status="$1"
    local category="$2"
    local phase="$3"
    local scenario_name="$4"
    local signal="$5"
    local symptoms_file="$6"
    shift 6
    local observed="[]"
    if [[ -s "${symptoms_file}" ]]; then
        observed="$(jq -c '[.symptoms[].name] | unique' "${symptoms_file}" 2>/dev/null)" || observed="[]"
    fi
    jq -nc --argjson status "${status}" --arg category "${category}" --arg phase "${phase}" \
        --arg findings_phase "${scenario_name} product findings" --arg signal "${signal}" \
        --argjson observed "${observed}" \
        --argjson expected "$(printf '%s\n' "$@" | jq -R . | jq -sc 'map(select(length > 0)) | unique')" '
        ($expected - $observed) as $missing
        | {outcome: (if $signal != "" then "interrupted"
                     elif $status == 0 then "not-reproduced"
                     elif $category == "product" and $phase == $findings_phase and ($missing | length) == 0
                     then "reproduced"
                     else "other-failure" end),
           symptoms: $expected, observed: $observed, missing: $missing,
           failure: (if $status == 0 then null
                     else {category: $category, phase: $phase, exit_code: $status} end),
           symptoms_file: "results/symptoms.json"}'
}
