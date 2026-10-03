#!/usr/bin/env bash
set -euo pipefail

fail() {
    printf 'degraded-link evidence: %s\n' "$*" >&2
    exit 1
}

mode="${1:-}"
case "${mode}" in
    rules)
        [[ "$#" -eq 4 ]] || fail 'rules needs profile, inspection and peer address'
        profile="$2" state="$3" address="$4"
        [[ -s "${state}" && "${address}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] \
            || fail 'rule inspection or peer address is invalid'
        qdiscs="$(awk '/^# qdisc$/ {in_section=1;next} /^# / {in_section=0} in_section && NF' "${state}")"
        filters="$(awk '/^# filter$/ {in_section=1;next} /^# / {in_section=0} in_section && NF' "${state}")"
        input="$(awk '/^# iptables$/ {in_section=1;next} /^# / {in_section=0} in_section && NF' "${state}")"
        [[ "${input}" == '-P INPUT ACCEPT' ]] || fail 'the INPUT chain is not empty and ACCEPT'
        if [[ "${profile}" == default ]]; then
            if [[ "$(wc -l <<<"${qdiscs}")" -ne 1 || -n "${filters}" ]] \
                || ! grep -Eq '^qdisc (noqueue|fq|fq_codel|pfifo_fast) 0: root' <<<"${qdiscs}"; then
                fail 'the interface has a nondefault qdisc or filter'
            fi
            exit 0
        fi
        if [[ "$(wc -l <<<"${qdiscs}")" -ne 4 ]] \
            || ! grep -Eq '^qdisc prio 504d: root ' <<<"${qdiscs}" \
            || ! grep -Eq '^qdisc sfq 504e: parent 504d:1 ' <<<"${qdiscs}" \
            || ! grep -Eq '^qdisc sfq 504f: parent 504d:2 ' <<<"${qdiscs}" \
            || ! grep -Eq '^qdisc netem 5050: parent 504d:3 ' <<<"${qdiscs}"; then
            fail 'the interface does not have exactly one Pumba netem topology'
        fi
        case "${profile}" in
            delay) expected=('delay 180ms') ;;
            jitter) expected=('delay 180ms' '100ms') ;;
            random-loss) expected=('loss 30%') ;;
            burst-loss) expected=('loss state' 'p13 20%' 'p31 15%') ;;
            rate-limit) expected=('rate 256Kbit') ;;
            combined) expected=('delay 180ms' 'loss 20%' 'rate 256Kbit') ;;
            *) fail "unknown profile ${profile}" ;;
        esac
        netem="$(grep '^qdisc netem 5050:' <<<"${qdiscs}")"
        for token in "${expected[@]}"; do
            grep -Fqi "${token}" <<<"${netem}" \
                || fail "installed netem is missing ${token}"
        done
        expected_match="$(jq -n -r --arg ip "${address}" '
            $ip | split(".") | map(tonumber | [(. / 16 | floor),(. % 16)]
              | map("0123456789abcdef"[.:(. + 1)]) | join("")) | join("")
          ')"
        if [[ "$(wc -l <<<"${filters}")" -ne 4 ]] \
            || [[ "$(grep -c '^filter ' <<<"${filters}")" -ne 3 ]] \
            || [[ "$(grep -c 'flowid 504d:3' <<<"${filters}")" -ne 1 ]] \
            || ! grep -Fq "match ${expected_match}/ffffffff at 16" <<<"${filters}"; then
            fail 'netem filter does not select only the declared peer address'
        fi
        ;;
    ping)
        [[ "$#" -eq 3 ]] || fail 'ping needs raw ping output and result JSON'
        [[ -s "$2" ]] || fail 'ping output is empty'
        parsed="$(awk '
            BEGIN { previous=-1 }
            /packets transmitted/ { sent=$1; received=$4 }
            /seq=/ {
                for (i=1;i<=NF;i++) if ($i ~ /^seq=/) {
                    split($i,p,"="); seq=p[2]+0
                    gap=seq-previous-1
                    if (gap>longest) longest=gap
                    previous=seq
                }
            }
            /round-trip min\/avg\/max/ {
                split($0,parts," = "); split(parts[2],rtt,"/")
                minimum=rtt[1]; average=rtt[2]; maximum=rtt[3]
            }
            END {
                trailing=sent-previous-1
                if (trailing>longest) longest=trailing
                printf "%d %d %d %.3f %.3f %.3f",sent,received,longest,minimum,average,maximum
            }
        ' "$2")"
        read -r sent received longest minimum average maximum <<<"${parsed}"
        [[ "${sent}" -eq 40 && "${received}" -gt 0 ]] || fail 'ping sent fewer than 40 probes or received none'
        jq -n --argjson sent "${sent}" --argjson received "${received}" \
            --argjson longest_loss_run "${longest}" \
            --argjson min_ms "${minimum}" --argjson avg_ms "${average}" \
            --argjson max_ms "${maximum}" \
            '{sent:$sent,received:$received,lost:($sent-$received),longest_loss_run:$longest_loss_run,min_ms:$min_ms,avg_ms:$avg_ms,max_ms:$max_ms}' \
            >"$3"
        ;;
    effect)
        [[ "$#" -eq 4 || "$#" -eq 6 ]] || fail 'effect needs profile and baseline/fault ping JSON, plus rate JSON for rate profiles'
        profile="$2" baseline="$3" observed="$4"
        [[ -s "${baseline}" && -s "${observed}" ]] || fail 'effect samples are missing'
        jq -n -e --arg profile "${profile}" --slurpfile base "${baseline}" \
            --slurpfile fault "${observed}" '
            $base[0] as $b | $fault[0] as $f
            | if $profile == "healed" then $f.received >= 39 and $f.avg_ms <= ($b.avg_ms + 50)
              elif $profile == "delay" then $f.received >= 35 and $f.avg_ms >= ($b.avg_ms + 100)
              elif $profile == "jitter" then $f.received >= 35 and $f.avg_ms >= ($b.avg_ms + 80)
                   and ($f.max_ms - $f.min_ms) >= 70
              elif $profile == "random-loss" then $f.lost >= 3 and $f.received >= 5
              elif $profile == "burst-loss" then $f.lost >= 3 and $f.received >= 5
                   and $f.longest_loss_run >= 2
              elif $profile == "rate-limit" then $f.received >= 30
              elif $profile == "combined" then $f.avg_ms >= ($b.avg_ms + 80)
                   and $f.lost >= 2 and $f.received >= 5
              else false end
          ' >/dev/null || fail "${profile} did not measurably affect the intended link"
        if [[ "${profile}" == rate-limit || "${profile}" == combined ]]; then
            [[ "$#" -eq 6 && -s "$5" && -s "$6" ]] || fail 'rate evidence is missing'
            jq -n -e --slurpfile base "$5" --slurpfile fault "$6" '
                $fault[0].bytes == $base[0].bytes
                and $fault[0].duration_ms >= ($base[0].duration_ms * 2)
              ' >/dev/null || fail "${profile} did not reduce measured link throughput"
        fi
        printf 'verified %s effect\n' "${profile}"
        ;;
    limits)
        [[ "$#" -eq 4 ]] || fail 'limits needs sample JSON, committed limits JSON and findings NDJSON'
        jq -e '.at_ms > 0 and .source_end >= 0 and .output_end >= 0
            and (.docker_memory | type == "array")' "$2" >/dev/null \
            || fail 'sample does not satisfy the metrics contract'
        jq -nc --slurpfile sample "$2" --slurpfile limits "$3" '
            $sample[0] as $s | $limits[0] as $l
            | [ {metric:"backlog",observed:$s.backlog,limit:$l.max_backlog},
                {metric:"pending_operations",observed:$s.pending_operations,limit:$l.max_pending} ]
              + [$s.docker_memory[] | {metric:("docker_memory_bytes:"+.container),observed:.bytes,limit:$l.max_memory_bytes}]
            | .[] | select(.observed > .limit)
            | . + {at_ms:$s.at_ms,stage:$s.stage,profile:$s.profile,raw:$s.raw}
          ' >>"$4"
        ;;
    recovery)
        [[ "$#" -eq 6 && -s "$2" ]] \
            || fail 'recovery needs samples, profile, baseline rate, rate percent and backlog limit'
        jq -s --arg profile "$3" --argjson baseline "$4" \
            --argjson threshold "$5" --argjson max_backlog "$6" '
            [.[] | select(.stage == "recovery" and .profile == $profile)] | .[-4:] as $w
            | ([range(1; $w|length)
                | select($w[.].output_end > $w[. - 1].output_end
                         and $w[.].backlog <= $max_backlog)] | length) as $progress
            | if ($w | length) < 4 then
                {qualified:false,sample_count:($w|length),positive_progress_intervals:$progress,
                 output_records_per_second:null,backlog:$w[-1].backlog}
              else
                (($w[3].output_end - $w[0].output_end) * 1000 /
                   ($w[3].at_ms - $w[0].at_ms)) as $rate
                | {qualified:($progress == 3 and $rate >= $baseline * $threshold / 100),
                   sample_count:4,positive_progress_intervals:$progress,
                   output_records_per_second:$rate,backlog:$w[-1].backlog}
              end
          ' "$2"
        ;;
    *) fail 'expected rules, ping, effect, limits or recovery' ;;
esac
