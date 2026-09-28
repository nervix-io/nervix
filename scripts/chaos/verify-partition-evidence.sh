#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage:
  verify-partition-evidence.sh plan PLAN_JSON
  verify-partition-evidence.sh rules PLAN_JSON HOST RULES_TXT [--healed]
  verify-partition-evidence.sh links PLAN_JSON OBSERVED_JSON RESULT_JSON [--healed]

A plan names each node's address, the directed node links the fault must block, and the Pumba
rules placed on each node: netem egress targets and iptables ingress sources. `plan` proves that
the placed rules block exactly the intended links. `rules` compares one node's recorded
traffic-control and INPUT state with its planned rules. `links` compares an observed link matrix
with the plan: every intended node link blocked, every other node link open, and every verifier
and broker link open. With --healed, every node must carry no rules and every link must be open.
EOF
}

[[ "$#" -ge 2 ]] || { usage; exit 2; }
mode="$1"
plan_path="$2"
[[ -s "${plan_path}" ]] || { printf 'partition plan is missing: %s\n' "${plan_path}" >&2; exit 2; }

plan_contract='
    (.addresses | type == "object" and length >= 2)
    and (.addresses | to_entries | all(.key | test("^nervix-[1-9]$")))
    and (.addresses | to_entries | all(.value | test("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$")))
    and (.intended_blocked | type == "array")
    and (.rules | type == "object")
    and ((.rules | keys) == (.addresses | keys))
    and (.rules | to_entries | all(.value.netem_targets | type == "array"))
    and (.rules | to_entries | all(.value.iptables_sources | type == "array"))'
jq -e "${plan_contract}" "${plan_path}" >/dev/null 2>&1 \
    || { printf 'partition plan does not match the plan contract: %s\n' "${plan_path}" >&2; exit 2; }

case "${mode}" in
    plan)
        [[ "$#" -eq 2 ]] || { usage; exit 2; }
        if ! jq -e '
            (.addresses | to_entries | map({key: .value, value: .key}) | from_entries) as $host_of
            | ([.rules | to_entries[] | .value.netem_targets[], .value.iptables_sources[]]
               | all($host_of[.] != null)) as $known
            | [.rules | to_entries[] | .key as $from | .value.netem_targets[] | "\($from)>\($host_of[.])"] as $egress
            | [.rules | to_entries[] | .key as $to | .value.iptables_sources[] | "\($host_of[.])>\($to)"] as $ingress
            | $known and ((($egress + $ingress) | unique) == (.intended_blocked | unique))
              and (.rules | to_entries | all(.value.netem_targets | length == (unique | length)))
              and (.rules | to_entries | all(.value.iptables_sources | length == (unique | length)))
        ' "${plan_path}" >/dev/null; then
            printf 'placed rules in %s do not block exactly the intended links\n' "${plan_path}" >&2
            exit 1
        fi
        ;;
    rules)
        [[ "$#" -eq 4 || "$#" -eq 5 ]] || { usage; exit 2; }
        host="$3"
        rules_path="$4"
        healed=false
        if [[ "$#" -eq 5 ]]; then
            [[ "$5" == --healed ]] || { usage; exit 2; }
            healed=true
        fi
        [[ -s "${rules_path}" ]] || { printf 'recorded rules are missing: %s\n' "${rules_path}" >&2; exit 2; }
        jq -e --arg host "${host}" '.rules[$host] != null' "${plan_path}" >/dev/null \
            || { printf 'plan has no rules for %s\n' "${host}" >&2; exit 2; }
        netem_matches=""
        iptables_rules=""
        if [[ "${healed}" != true ]]; then
            # u32 prints the destination match as the address in hexadecimal at offset 16.
            netem_matches="$(jq -r --arg host "${host}" '
                def hex_octet: [(. / 16 | floor), (. % 16)] | map("0123456789abcdef"[.:(. + 1)]) | join("");
                .rules[$host].netem_targets[]
                | "match " + (split(".") | map(tonumber | hex_octet) | join("")) + "/ffffffff at 16"
            ' "${plan_path}" | sort)"
            iptables_rules="$(jq -r --arg host "${host}" '
                .rules[$host].iptables_sources[]
                | "-A INPUT -s \(.)/32 -i eth0 -m statistic --mode random --probability 1.00000000000 -j DROP"
            ' "${plan_path}" | sort)"
        fi
        observed_qdiscs="$(awk '/^# qdisc$/ { s = 1; next } /^# / { s = 0 } s && NF > 0' "${rules_path}")"
        observed_matches="$(awk '/^# filter$/ { s = 1; next } /^# / { s = 0 } s && $1 == "match" { print "match " $2 " at " $4 }' "${rules_path}" | sort)"
        observed_flows="$(awk '/^# filter$/ { s = 1; next } /^# / { s = 0 } s && /flowid 504d:3/' "${rules_path}" | wc -l)"
        observed_filter_lines="$(awk '/^# filter$/ { s = 1; next } /^# / { s = 0 } s && NF > 0' "${rules_path}" | wc -l)"
        observed_rules="$(awk '/^# iptables$/ { s = 1; next } /^# / { s = 0 } s && /^-A /' "${rules_path}" | sort)"
        observed_policy="$(awk '/^# iptables$/ { s = 1; next } /^# / { s = 0 } s && /^-P INPUT /' "${rules_path}")"
        [[ "${observed_policy}" == '-P INPUT ACCEPT' ]] \
            || { printf '%s INPUT policy is not ACCEPT\n' "${host}" >&2; exit 1; }
        if [[ -z "${netem_matches}" ]]; then
            [[ "$(wc -l <<<"${observed_qdiscs}")" -eq 1 ]] \
                && grep -Eq '^qdisc (noqueue|fq|fq_codel|pfifo_fast) 0: root' <<<"${observed_qdiscs}" \
                && [[ "${observed_filter_lines}" -eq 0 ]] \
                || { printf '%s carries a qdisc or filter the plan does not place there\n' "${host}" >&2; exit 1; }
        else
            expected_count="$(wc -l <<<"${netem_matches}")"
            [[ "$(wc -l <<<"${observed_qdiscs}")" -eq 4 ]] \
                && grep -Eq '^qdisc prio 504d: root ' <<<"${observed_qdiscs}" \
                && grep -Eq '^qdisc sfq 504e: parent 504d:1 ' <<<"${observed_qdiscs}" \
                && grep -Eq '^qdisc sfq 504f: parent 504d:2 ' <<<"${observed_qdiscs}" \
                && grep -Eq '^qdisc netem 5050: parent 504d:3 .* loss 100%' <<<"${observed_qdiscs}" \
                || { printf '%s does not carry exactly one owned netem loss topology\n' "${host}" >&2; exit 1; }
            [[ "${observed_flows}" -eq "${expected_count}" && "${observed_matches}" == "${netem_matches}" ]] \
                || { printf '%s netem filters do not match exactly the planned egress targets\n' "${host}" >&2; exit 1; }
        fi
        [[ "${observed_rules}" == "${iptables_rules}" ]] \
            || { printf '%s INPUT rules do not match exactly the planned ingress sources\n' "${host}" >&2; exit 1; }
        ;;
    links)
        [[ "$#" -eq 4 || "$#" -eq 5 ]] || { usage; exit 2; }
        observed_path="$3"
        result_path="$4"
        healed=false
        if [[ "$#" -eq 5 ]]; then
            [[ "$5" == --healed ]] || { usage; exit 2; }
            healed=true
        fi
        jq -e '(.links | type == "array" and length > 0)
            and (.links | all(.from | type == "string"))
            and (.links | all(.to | type == "string"))
            and (.links | all(.sent | type == "number" and . > 0))
            and (.links | all(.received | type == "number" and . >= 0))' \
            "${observed_path}" >/dev/null 2>&1 \
            || { printf 'observed link matrix is invalid: %s\n' "${observed_path}" >&2; exit 2; }
        jq -n \
            --argjson healed "${healed}" \
            --slurpfile plan "${plan_path}" \
            --slurpfile observed "${observed_path}" '
            ($plan[0].addresses | keys) as $hosts
            | (if $healed then [] else $plan[0].intended_blocked end) as $blocked
            | ([$hosts[] as $from | $hosts[] | select(. != $from) as $to
                | {key: "\($from)>\($to)", value: (if ($blocked | index("\($from)>\($to)")) != null then "blocked" else "open" end)}]
               + [$hosts[] | {key: "verifier>\(.)", value: "open"}]
               + [$hosts[] | {key: "\(.)>broker", value: "open"}]
               | from_entries) as $expected
            | ($observed[0].links
               | map({key: "\(.from)>\(.to)", value: {sent, received, state: (if .received > 0 then "open" else "blocked" end)}})
               | from_entries) as $actual
            | ([$expected | to_entries[]
                | select(($actual[.key].state // "unobserved") != .value)
                | {link: .key, expected: .value, observed: ($actual[.key].state // "unobserved"), received: ($actual[.key].received // null)}]) as $mismatches
            | ([$actual | keys[] | select($expected[.] == null)]) as $unexpected
            | {
                verdict: (if ($mismatches | length) == 0 and ($unexpected | length) == 0 then "pass" else "fail" end),
                healed: $healed,
                expected: $expected,
                observed: $actual,
                mismatches: $mismatches,
                unexpected_links: $unexpected
              }' >"${result_path}"
        if [[ "$(jq -r '.verdict' "${result_path}")" != pass ]]; then
            printf 'observed links differ from the %s matrix: %s\n' \
                "$([[ "${healed}" == true ]] && printf healed || printf planned)" \
                "$(jq -c '.mismatches + [.unexpected_links[] | {link: ., expected: "absent"}]' "${result_path}")" >&2
            exit 1
        fi
        ;;
    *)
        usage
        exit 2
        ;;
esac
