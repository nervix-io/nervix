#!/usr/bin/env bash
set -euo pipefail

# The graceful-stop scenario's verdict: the strict delivery verdict of its two sources
# (verify-strict-delivery.sh) and the answer of the STOP that arrived while their records were in
# motion. Every symptom of the strict verdict stands, and the answer adds three:
#
#   stop-not-acknowledged  the CLI did not exit 0 with an answer naming the stopped domain
#   stop-reported-cut      the answer reports work the stop cut: it says the stop was enforced, or
#                          speaks of a cut and names an entity of the graph. The graph holds no kind
#                          a stop still cuts by design, so a graceful stop has nothing to report
#   silent-cut             the stop cut records, which shows as duplicate or missing records, and
#                          its answer reports no cut
#
# The STOP's timing is recorded beside the verdict and judges nothing.
#
# Exit status: 0 pass, 1 fail, 2 evidence that cannot be judged.

usage() {
    cat >&2 <<'EOF'
usage: verify-graceful-stop.sh --delivery STRICT_RESULT_JSON --stop-answer ANSWER_TXT --stop-exit STATUS
           --domain DOMAIN --entity NAME [--entity NAME]... [--stop-timing TIMING_JSON]
           --result RESULT_JSON
EOF
    exit 2
}

delivery=""
answer=""
stop_exit=""
domain=""
entities=()
timing=""
result=""
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --delivery) [[ "$#" -ge 2 ]] || usage; delivery="$2"; shift 2 ;;
        --stop-answer) [[ "$#" -ge 2 ]] || usage; answer="$2"; shift 2 ;;
        --stop-exit) [[ "$#" -ge 2 ]] || usage; stop_exit="$2"; shift 2 ;;
        --domain) [[ "$#" -ge 2 ]] || usage; domain="$2"; shift 2 ;;
        --entity) [[ "$#" -ge 2 ]] || usage; entities+=("$2"); shift 2 ;;
        --stop-timing) [[ "$#" -ge 2 ]] || usage; timing="$2"; shift 2 ;;
        --result) [[ "$#" -ge 2 ]] || usage; result="$2"; shift 2 ;;
        *) usage ;;
    esac
done
[[ -n "${delivery}" && -n "${answer}" && -n "${domain}" && -n "${result}" && "${#entities[@]}" -gt 0 ]] || usage
[[ "${stop_exit}" =~ ^[0-9]+$ ]] || usage
mkdir -p "$(dirname "${result}")"

invalid() {
    jq -n --arg message "$*" '{verdict: "invalid", message: $message}' >"${result}"
    printf 'graceful-stop evidence cannot be judged: %s\n' "$*" >&2
    exit 2
}

jq -e '(.verdict == "pass" or .verdict == "fail") and (.symptoms | type == "array")' "${delivery}" \
    >/dev/null 2>&1 || invalid "the strict delivery verdict ${delivery} was not reached"
[[ -e "${answer}" ]] || invalid "the STOP answer ${answer} was not recorded"
timing_json=null
if [[ -n "${timing}" ]]; then
    timing_json="$(jq -c '.' "${timing}" 2>/dev/null)" || invalid "the STOP timing ${timing} is not JSON"
fi

acknowledged=false
if ((stop_exit == 0)) && grep -Fq "stopped domain '${domain}'" "${answer}"; then
    acknowledged=true
fi
# A cut report names the work a stop cut per node and entity, so it names an entity of the graph.
reports_cut=false
named_entities=()
entity=""
for entity in "${entities[@]}"; do
    if grep -Fqw -- "${entity}" "${answer}"; then
        named_entities+=("${entity}")
    fi
done
if grep -Eqiw 'enforced' "${answer}" \
    || { grep -Eqiw 'cut' "${answer}" && ((${#named_entities[@]} > 0)); }; then
    reports_cut=true
fi

jq -n --slurpfile delivery "${delivery}" --rawfile answer "${answer}" --argjson stop_exit "${stop_exit}" \
    --argjson acknowledged "${acknowledged}" --argjson reports_cut "${reports_cut}" \
    --argjson named "$(printf '%s\n' ${named_entities[@]+"${named_entities[@]}"} | jq -R . | jq -s 'map(select(length > 0))')" \
    --argjson timing "${timing_json}" --arg domain "${domain}" --arg delivery_file "${delivery}" '
    $delivery[0] as $strict
    | ([$strict.symptoms[] | select(.name == "duplicate-records" or .name == "missing-records")]
       | length > 0) as $cut_records
    | ($strict.symptoms
       + [if $acknowledged then empty
          else {name: "stop-not-acknowledged", exit_code: $stop_exit} end,
          if $reports_cut then {name: "stop-reported-cut", entities: $named} else empty end,
          if $cut_records and $acknowledged and ($reports_cut | not)
          then {name: "silent-cut",
                cut: [$strict.symptoms[] | select(.name == "duplicate-records" or .name == "missing-records")]}
          else empty end]) as $symptoms
    | {verdict: (if ($symptoms | length) == 0 then "pass" else "fail" end),
       symptoms: $symptoms,
       stop: {domain: $domain, exit_code: $stop_exit, answer: $answer, acknowledged: $acknowledged,
              reports_cut: $reports_cut, entities_named: $named, timing: $timing},
       delivery: $delivery_file}
    ' >"${result}"

verdict="$(jq -r '.verdict' "${result}")"
printf 'graceful-stop verdict: %s\n' "${verdict}"
printf 'stop answer: %s\n' "$(tr '\n' ' ' <"${answer}")"
jq -r '.symptoms[] | "symptom: \(.name)"' "${result}"
[[ "${verdict}" == pass ]] || exit 1
