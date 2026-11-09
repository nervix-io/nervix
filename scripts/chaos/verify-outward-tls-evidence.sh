#!/usr/bin/env bash
set -euo pipefail

# Judges one observation of the outward TLS policy: a public DESCRIBE CLUSTER transcript and two
# tls-probe.py documents taken after it, one of the web console's HTTPS listeners and one of the
# interconnect listeners. It writes its verdict to --result and exits 1 when the evidence contradicts
# the expected policy or does not cover every named node.

usage() {
    cat >&2 <<'EOF'
usage:
  verify-outward-tls-evidence.sh --policy permits|excludes --description FILE --probe FILE
      --interconnect-probe FILE --node NODE=HOST [--node NODE=HOST]... [--after-revision N]
      --result FILE

The description must hold exactly one committed tls-cipher-suites and one committed
tls-key-exchange-groups line, both at one revision, with every value an exact resolved name.

permits  The committed lists contain TLS13_CHACHA20_POLY1305_SHA256 and X25519, and the probe's
         restricted client was accepted by every named node's console over TLS 1.3 with that suite.
excludes The committed lists contain neither, and every named node's console refused the restricted
         client with a handshake_failure alert.

Whatever the policy, every named node's interconnect, whose algorithms are fixed and never follow
the outward policy, must refuse the restricted client with a handshake_failure alert. Every named
node must also report a running record of both lists equal to the committed ones at the committed
revision, and that revision must exceed --after-revision when it is given. A probe target that
failed for another reason, or a missing node or target, contradicts the expectation.
EOF
}

fail_usage() {
    printf '%s\n' "$*" >&2
    usage
    exit 2
}

policy=""
description=""
probe=""
interconnect_probe=""
after_revision=""
result=""
node_pairs=()
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --policy | --description | --probe | --interconnect-probe | --after-revision | --result | --node)
            [[ "$#" -ge 2 ]] || fail_usage "$1 requires a value"
            case "$1" in
                --policy) policy="$2" ;;
                --description) description="$2" ;;
                --probe) probe="$2" ;;
                --interconnect-probe) interconnect_probe="$2" ;;
                --after-revision) after_revision="$2" ;;
                --result) result="$2" ;;
                --node) node_pairs+=("$2") ;;
            esac
            shift 2
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *) fail_usage "unknown argument: $1" ;;
    esac
done
[[ "${policy}" == permits || "${policy}" == excludes ]] || fail_usage '--policy must be permits or excludes'
[[ -f "${description}" ]] || fail_usage '--description must name a file'
[[ -f "${probe}" ]] || fail_usage '--probe must name a file'
[[ -f "${interconnect_probe}" ]] || fail_usage '--interconnect-probe must name a file'
[[ -n "${result}" ]] || fail_usage '--result is required'
[[ "${#node_pairs[@]}" -gt 0 ]] || fail_usage 'at least one --node NODE=HOST is required'
[[ -z "${after_revision}" || "${after_revision}" =~ ^[0-9]+$ ]] \
    || fail_usage '--after-revision must be a decimal revision'
nodes_json="$(printf '%s\n' "${node_pairs[@]}" | jq -Rsc '
    split("\n")[:-1] | map(capture("^(?<node>[A-Za-z0-9_.-]+)=(?<host>[A-Za-z0-9_.-]+)$")
        // error("every --node must be NODE=HOST, not \(.)"))')" \
    || fail_usage 'every --node must be NODE=HOST'

result_tmp="$(mktemp "${result}.XXXXXX")"
status=0
# DESCRIBE CLUSTER quotes every resolved name with single quotes, which this single-quoted program
# takes as an argument.
jq -Rse \
    --arg quote "'" \
    --arg policy "${policy}" \
    --arg after "${after_revision}" \
    --argjson nodes "${nodes_json}" \
    --slurpfile probe "${probe}" \
    --slurpfile interconnect_probe "${interconnect_probe}" '
    def names:
        ($quote + "[A-Za-z0-9_]+" + $quote) as $name |
        if test("^\\(" + $name + "(, " + $name + ")*\\)$")
        then .[1:-1] | split(", ") | map(.[1:-1])
        else error("not a resolved algorithm list: \(.)") end;
    def lines: split("\n") | map(sub("\r$"; ""));
    def committed($parameter):
        [lines[] | capture("^" + $parameter + " committed=(?<value>.+) revision=(?<revision>[1-9][0-9]*)$")];
    def running($parameter):
        [lines[] | capture("^node=(?<node>[^ ]+) " + $parameter + " running=(?<value>.+) revision=(?<revision>[1-9][0-9]*)$")];
    . as $text |
    ($text | committed("tls-cipher-suites")) as $suites_committed |
    ($text | committed("tls-key-exchange-groups")) as $groups_committed |
    ($text | running("tls-cipher-suites")) as $suites_running |
    ($text | running("tls-key-exchange-groups")) as $groups_running |
    ($probe[0]) as $document |
    ($interconnect_probe[0]) as $interconnect |
    [
        (if ($suites_committed | length) != 1 or ($groups_committed | length) != 1
            then "the description does not hold exactly one committed line for each TLS list" else empty end),
        (if ($suites_committed | length) == 1 and ($groups_committed | length) == 1
                and $suites_committed[0].revision != $groups_committed[0].revision
            then "the committed TLS lists name different revisions" else empty end)
    ] as $shape_problems |
    if ($shape_problems | length) > 0 then
        {verdict: "fail", policy: $policy, problems: $shape_problems}
    else
        ($suites_committed[0].value | names) as $suites |
        ($groups_committed[0].value | names) as $groups |
        $suites_committed[0].revision as $revision |
        ($suites | any(.[]; . == "TLS13_CHACHA20_POLY1305_SHA256")) as $suite_member |
        ($groups | any(.[]; . == "X25519")) as $group_member |
        [$nodes[] | . as $pair |
            ([$suites_running[] | select(.node == $pair.node)]) as $suite_lines |
            ([$groups_running[] | select(.node == $pair.node)]) as $group_lines |
            ([($document.targets // [])[] | select(.host == $pair.host)]) as $targets |
            ([($interconnect.targets // [])[] | select(.host == $pair.host)]) as $interconnect_targets |
            {node: $pair.node, host: $pair.host,
             running_revision: (if ($suite_lines | length) == 1 then $suite_lines[0].revision else null end),
             running_matches: (($suite_lines | length) == 1 and ($group_lines | length) == 1
                 and $suite_lines[0].revision == $revision and $group_lines[0].revision == $revision
                 and ($suite_lines[0].value | names) == $suites
                 and ($group_lines[0].value | names) == $groups),
             probe: (if ($targets | length) == 1 then $targets[0] else null end),
             probe_count: ($targets | length),
             interconnect_probe: (if ($interconnect_targets | length) == 1 then $interconnect_targets[0] else null end),
             interconnect_probe_count: ($interconnect_targets | length)}] as $observed |
        [
            (if $policy == "permits" and ($suite_member | not)
                then "the committed cipher suites do not contain TLS13_CHACHA20_POLY1305_SHA256" else empty end),
            (if $policy == "permits" and ($group_member | not)
                then "the committed key exchange groups do not contain X25519" else empty end),
            (if $policy == "excludes" and $suite_member
                then "the committed cipher suites contain TLS13_CHACHA20_POLY1305_SHA256" else empty end),
            (if $policy == "excludes" and $group_member
                then "the committed key exchange groups contain X25519" else empty end),
            (if $after != "" and (($revision | tonumber) <= ($after | tonumber))
                then "the committed revision \($revision) does not exceed \($after)" else empty end),
            ($document, $interconnect |
                if .client.cipher_suites != ["TLS_CHACHA20_POLY1305_SHA256"]
                    or .client.groups != ["X25519"] or .client.version != "TLSv1.3"
                then "the probe did not record the restricted client offer" else empty end),
            ($observed[] |
                (if .running_matches | not
                    then "\(.node) does not run the committed TLS lists at revision \($revision)" else empty end),
                (if .probe_count != 1
                    then "\(.host) has \(.probe_count) probe results, not one" else empty end),
                (if .probe != null and $policy == "permits"
                        and (.probe.outcome != "accepted" or .probe.version != "TLSv1.3"
                             or .probe.cipher != "TLS_CHACHA20_POLY1305_SHA256")
                    then "\(.host) did not accept the restricted client: \(.probe.outcome)" else empty end),
                (if .probe != null and $policy == "excludes"
                        and (.probe.outcome != "refused" or .probe.reason != "SSLV3_ALERT_HANDSHAKE_FAILURE")
                    then "\(.host) did not refuse the restricted client with a handshake_failure alert: \(.probe.outcome)"
                    else empty end),
                (if .interconnect_probe_count != 1
                    then "\(.host) has \(.interconnect_probe_count) interconnect probe results, not one" else empty end),
                (if .interconnect_probe != null
                        and (.interconnect_probe.outcome != "refused"
                             or .interconnect_probe.reason != "SSLV3_ALERT_HANDSHAKE_FAILURE")
                    then "the interconnect of \(.host) did not refuse the restricted client with a handshake_failure alert: \(.interconnect_probe.outcome)"
                    else empty end))
        ] as $problems |
        {verdict: (if ($problems | length) == 0 then "pass" else "fail" end),
         policy: $policy, revision: $revision,
         committed: {cipher_suites: $suites, key_exchange_groups: $groups},
         nodes: $observed, problems: $problems}
    end
    ' "${description}" >"${result_tmp}" || status=$?
if [[ "${status}" -ne 0 ]]; then
    rm -f "${result_tmp}"
    printf 'outward TLS evidence could not be judged: the description or probe document is malformed\n' >&2
    jq -n --arg policy "${policy}" \
        '{verdict: "fail", policy: $policy, problems: ["the description or probe document is malformed"]}' \
        >"${result}"
    exit 1
fi
mv "${result_tmp}" "${result}"
if [[ "$(jq -r '.verdict' "${result}")" != pass ]]; then
    jq -r '.problems[] | "outward TLS evidence: " + .' "${result}" >&2
    exit 1
fi
