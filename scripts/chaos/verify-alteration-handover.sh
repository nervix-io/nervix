#!/usr/bin/env bash
set -euo pipefail

# The alteration-handover scenario's verdict: the strict delivery verdict of its two sources
# (verify-strict-delivery.sh), the answer of the alteration that replaced the deduplicator while
# their records were in motion, and what the nodes logged. Every symptom of the strict verdict
# stands, and two more join it:
#
#   alteration-refused  the CLI did not exit 0 with an answer naming the altered deduplicator
#   domain-rebuilt      a node logged that its entity-level schedule apply failed and that it
#                       rebuilt the domain, which ends the domain's tasks without a drain
#
# How many processor branch tasks the nodes aborted at their stop grace is recorded beside the
# verdict and judges nothing: an idle branch whose state the alteration purges loses nothing when it
# is aborted, and a branch that held records shows its loss in the strict verdict. The alteration's
# timing is recorded beside the verdict and judges nothing either.
#
# Exit status: 0 pass, 1 fail, 2 evidence that cannot be judged.

usage() {
    cat >&2 <<'EOF'
usage: verify-alteration-handover.sh --delivery STRICT_RESULT_JSON --alter-answer ANSWER_TXT
           --alter-exit STATUS --deduplicator NAME --node-log LOG [--node-log LOG]...
           [--alter-timing TIMING_JSON] --result RESULT_JSON
EOF
    exit 2
}

delivery=""
answer=""
alter_exit=""
deduplicator=""
node_logs=()
timing=""
result=""
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --delivery) [[ "$#" -ge 2 ]] || usage; delivery="$2"; shift 2 ;;
        --alter-answer) [[ "$#" -ge 2 ]] || usage; answer="$2"; shift 2 ;;
        --alter-exit) [[ "$#" -ge 2 ]] || usage; alter_exit="$2"; shift 2 ;;
        --deduplicator) [[ "$#" -ge 2 ]] || usage; deduplicator="$2"; shift 2 ;;
        --node-log) [[ "$#" -ge 2 ]] || usage; node_logs+=("$2"); shift 2 ;;
        --alter-timing) [[ "$#" -ge 2 ]] || usage; timing="$2"; shift 2 ;;
        --result) [[ "$#" -ge 2 ]] || usage; result="$2"; shift 2 ;;
        *) usage ;;
    esac
done
[[ -n "${delivery}" && -n "${answer}" && -n "${deduplicator}" && -n "${result}" \
    && "${#node_logs[@]}" -gt 0 ]] || usage
[[ "${alter_exit}" =~ ^[0-9]+$ ]] || usage
mkdir -p "$(dirname "${result}")"

invalid() {
    jq -n --arg message "$*" '{verdict: "invalid", message: $message}' >"${result}"
    printf 'alteration-handover evidence cannot be judged: %s\n' "$*" >&2
    exit 2
}

jq -e '(.verdict == "pass" or .verdict == "fail") and (.symptoms | type == "array")' "${delivery}" \
    >/dev/null 2>&1 || invalid "the strict delivery verdict ${delivery} was not reached"
[[ -e "${answer}" ]] || invalid "the alteration answer ${answer} was not recorded"
timing_json=null
if [[ -n "${timing}" ]]; then
    timing_json="$(jq -c '.' "${timing}" 2>/dev/null)" || invalid "the alteration timing ${timing} is not JSON"
fi

acknowledged=false
if ((alter_exit == 0)) && grep -Fq "altered deduplicator '${deduplicator}'" "${answer}"; then
    acknowledged=true
fi

# What each node logged: the rebuilds that stood in for a failed entity swap, and the branch tasks
# aborted at their stop grace.
rebuild_line='entity-level schedule apply failed; rebuilding domain'
abort_line='processor branch task exceeded shutdown grace period; aborting'
logs_json='[]'
log=""
for log in "${node_logs[@]}"; do
    [[ -e "${log}" ]] || invalid "the node log ${log} was not recorded"
    rebuilds="$(grep -cF -- "${rebuild_line}" "${log}" || true)"
    aborted="$(grep -cF -- "${abort_line}" "${log}" || true)"
    first_rebuild="$(grep -F -m 1 -- "${rebuild_line}" "${log}" || true)"
    logs_json="$(jq -c --arg log "${log}" --argjson rebuilds "${rebuilds}" --argjson aborted "${aborted}" \
        --arg first "${first_rebuild}" \
        '. + [{log: $log, rebuilds: $rebuilds, aborted_branches: $aborted,
               first_rebuild: (if $first == "" then null else $first end)}]' <<<"${logs_json}")"
done

jq -n --slurpfile delivery "${delivery}" --rawfile answer "${answer}" --argjson alter_exit "${alter_exit}" \
    --argjson acknowledged "${acknowledged}" --argjson logs "${logs_json}" \
    --argjson timing "${timing_json}" --arg deduplicator "${deduplicator}" \
    --arg delivery_file "${delivery}" '
    $delivery[0] as $strict
    | [$logs[] | select(.rebuilds > 0)] as $rebuilt
    | ($strict.symptoms
       + [if $acknowledged then empty
          else {name: "alteration-refused", exit_code: $alter_exit} end,
          if ($rebuilt | length) > 0
          then {name: "domain-rebuilt", count: ([$rebuilt[].rebuilds] | add),
                logs: [$rebuilt[].log], first: $rebuilt[0].first_rebuild}
          else empty end]) as $symptoms
    | {verdict: (if ($symptoms | length) == 0 then "pass" else "fail" end),
       symptoms: $symptoms,
       alteration: {deduplicator: $deduplicator, exit_code: $alter_exit, answer: $answer,
                    acknowledged: $acknowledged, timing: $timing},
       nodes: $logs,
       aborted_branches: ([$logs[].aborted_branches] | add),
       delivery: $delivery_file}
    ' >"${result}"

verdict="$(jq -r '.verdict' "${result}")"
printf 'alteration-handover verdict: %s\n' "${verdict}"
printf 'alteration answer: %s\n' "$(tr '\n' ' ' <"${answer}")"
jq -r '.symptoms[] | "symptom: \(.name)"' "${result}"
[[ "${verdict}" == pass ]] || exit 1
