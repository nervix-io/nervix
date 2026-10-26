#!/usr/bin/env bash
set -euo pipefail
chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
verify="${chaos_dir}/verify-degraded-evidence.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

# A failure inside a mocked probe command would otherwise land in the log the probe redirects it to.
exec 3>&2
fail() { printf 'degraded evidence self-test: %s\n' "$*" >&3; exit 1; }
must_fail() {
    if "$@" >"${tmp_dir}/rejected.txt" 2>&1; then
        fail "invalid evidence passed: $*"
    fi
}

must_fail "${chaos_dir}/chaos.sh" run degraded-links --image unavailable --profile unsupported
must_fail "${chaos_dir}/chaos.sh" run degraded-links --image unavailable --load-interval-ms 0
must_fail "${chaos_dir}/chaos.sh" run degraded-links --image unavailable --max-backlog 10 --max-recovery-backlog 11
must_fail "${chaos_dir}/chaos.sh" run baseline --image unavailable --profile delay

cat >"${tmp_dir}/rules-default.txt" <<'EOF'
# qdisc
qdisc noqueue 0: root refcnt 2
# filter
# iptables
-P INPUT ACCEPT
EOF
"${verify}" rules default "${tmp_dir}/rules-default.txt" 10.213.1.13
cat >"${tmp_dir}/rules-delay.txt" <<'EOF'
# qdisc
qdisc prio 504d: root refcnt 2 bands 3
qdisc sfq 504e: parent 504d:1 limit 127p
qdisc sfq 504f: parent 504d:2 limit 127p
qdisc netem 5050: parent 504d:3 limit 1000 delay 180ms
# filter
filter parent 504d: protocol ip pref 1 u32 chain 0
filter parent 504d: protocol ip pref 1 u32 chain 0 fh 800: ht divisor 1
filter parent 504d: protocol ip pref 1 u32 chain 0 fh 800::800 order 2048 key ht 800 bkt 0 flowid 504d:3 not_in_hw
  match 0ad5010d/ffffffff at 16
# iptables
-P INPUT ACCEPT
EOF
"${verify}" rules delay "${tmp_dir}/rules-delay.txt" 10.213.1.13
must_fail "${verify}" rules default "${tmp_dir}/rules-delay.txt" 10.213.1.13
must_fail "${verify}" rules delay "${tmp_dir}/rules-delay.txt" 10.213.1.12

{
    printf 'PING 10.213.1.13: 56 data bytes\n'
    for seq in $(seq 0 39); do
        [[ "${seq}" -eq 7 || "${seq}" -eq 8 ]] && continue
        printf '64 bytes from 10.213.1.13: seq=%s ttl=64 time=180.000 ms\n' "${seq}"
    done
    printf '40 packets transmitted, 38 packets received, 5%% packet loss\n'
    printf 'round-trip min/avg/max = 100.000/180.000/260.000 ms\n'
} >"${tmp_dir}/ping.txt"
"${verify}" ping "${tmp_dir}/ping.txt" "${tmp_dir}/ping.json" 40
jq -e '.sent==40 and .received==38 and .lost==2 and .longest_loss_run==2 and .avg_ms==180' \
    "${tmp_dir}/ping.json" >/dev/null || fail 'ping parser did not retain the loss burst'

jq -n '{sent:40,received:40,lost:0,longest_loss_run:0,min_ms:1,avg_ms:2,max_ms:3}' \
    >"${tmp_dir}/baseline.json"
"${verify}" effect jitter "${tmp_dir}/baseline.json" "${tmp_dir}/ping.json" >/dev/null
must_fail "${verify}" effect random-loss "${tmp_dir}/baseline.json" "${tmp_dir}/ping.json"
jq -n '{bytes:262144,received_bytes:262144,duration_ms:1200,clock_resolution_ms:10}' >"${tmp_dir}/rate-baseline.json"
jq -n '{bytes:262144,received_bytes:262144,duration_ms:4200,clock_resolution_ms:10}' >"${tmp_dir}/rate-fault.json"
"${verify}" effect combined "${tmp_dir}/baseline.json" "${tmp_dir}/ping.json" \
    "${tmp_dir}/rate-baseline.json" "${tmp_dir}/rate-fault.json" >/dev/null
must_fail "${verify}" effect combined "${tmp_dir}/baseline.json" "${tmp_dir}/ping.json" \
    "${tmp_dir}/rate-baseline.json" "${tmp_dir}/rate-baseline.json"
jq '.duration_ms = 2410' "${tmp_dir}/rate-fault.json" >"${tmp_dir}/rate-uncertain.json"
must_fail "${verify}" effect combined "${tmp_dir}/baseline.json" "${tmp_dir}/ping.json" \
    "${tmp_dir}/rate-baseline.json" "${tmp_dir}/rate-uncertain.json"
jq '.duration_ms = 2420' "${tmp_dir}/rate-fault.json" >"${tmp_dir}/rate-bound.json"
"${verify}" effect combined "${tmp_dir}/baseline.json" "${tmp_dir}/ping.json" \
    "${tmp_dir}/rate-baseline.json" "${tmp_dir}/rate-bound.json" >/dev/null \
    || fail 'a measured twofold lower bound was rejected'
jq '.received_bytes = 131072' "${tmp_dir}/rate-fault.json" >"${tmp_dir}/rate-partial.json"
must_fail "${verify}" effect combined "${tmp_dir}/baseline.json" "${tmp_dir}/ping.json" \
    "${tmp_dir}/rate-baseline.json" "${tmp_dir}/rate-partial.json"

jq -n '{at_ms:1000,stage:"fault",profile:"delay",source_end:50,output_end:30,
    backlog:20,pending_operations:9,docker_memory:[{container:"nervix-2",bytes:300}]}' \
    >"${tmp_dir}/sample.json"
jq -n '{max_backlog:10,max_pending:8,max_memory_bytes:200}' >"${tmp_dir}/limits.json"
: >"${tmp_dir}/findings.ndjson"
"${verify}" limits "${tmp_dir}/sample.json" "${tmp_dir}/limits.json" "${tmp_dir}/findings.ndjson"
jq -s -e 'length==3 and ([.[].metric]|sort)==["backlog","docker_memory_bytes:nervix-2","pending_operations"]
    and all(.[]; .at_ms==1000 and .profile=="delay")' "${tmp_dir}/findings.ndjson" >/dev/null \
    || fail 'threshold violations lost their metric or action time'

printf '%s\n' \
    '{"stage":"recovery","profile":"rate-limit","at_ms":1000,"output_end":0,"backlog":9}' \
    '{"stage":"recovery","profile":"rate-limit","at_ms":11000,"output_end":18,"backlog":9}' \
    '{"stage":"recovery","profile":"rate-limit","at_ms":21000,"output_end":27,"backlog":9}' \
    '{"stage":"recovery","profile":"rate-limit","at_ms":31000,"output_end":45,"backlog":9}' \
    >"${tmp_dir}/recovery.ndjson"
"${verify}" recovery "${tmp_dir}/recovery.ndjson" rate-limit 2 50 20 \
    | jq -e '.qualified == true and .sample_count == 4
        and .positive_progress_intervals == 3 and .output_records_per_second == 1.5' >/dev/null \
    || fail 'healthy three-interval throughput was rejected because of sample cadence'
head -n 1 "${tmp_dir}/recovery.ndjson" >"${tmp_dir}/recovery-short.ndjson"
"${verify}" recovery "${tmp_dir}/recovery-short.ndjson" rate-limit 2 50 20 \
    | jq -e '.qualified == false and .sample_count == 1
        and .positive_progress_intervals == 0 and .output_records_per_second == null' >/dev/null \
    || fail 'short recovery window lost its sample and interval counts'
printf '%s\n' '{"stage":"recovery","profile":"rate-limit","at_ms":41000,"output_end":45,"backlog":9}' \
    >>"${tmp_dir}/recovery.ndjson"
"${verify}" recovery "${tmp_dir}/recovery.ndjson" rate-limit 2 50 20 \
    | jq -e '.qualified == false' >/dev/null \
    || fail 'a stalled interval passed recovery'
jq 'if .at_ms == 41000 then .output_end = 63 | .backlog = 21 else . end' \
    "${tmp_dir}/recovery.ndjson" >"${tmp_dir}/recovery-over-limit.ndjson"
"${verify}" recovery "${tmp_dir}/recovery-over-limit.ndjson" rate-limit 2 50 20 \
    | jq -e '.qualified == false' >/dev/null \
    || fail 'recovery passed above the committed backlog limit'

# A larger sample keeps the received and lost fractions of the directed-link contract.
# Four replies per forty packets are below the required received fraction at every sample size.
jq -n '{sent:400,received:40,lost:360,longest_loss_run:10,min_ms:1,avg_ms:2,max_ms:3}' \
    >"${tmp_dir}/burst-excess-loss.json"
must_fail "${verify}" effect burst-loss "${tmp_dir}/baseline.json" "${tmp_dir}/burst-excess-loss.json"

# Received and lost limits are inclusive fractions, with the next packet outside each boundary
# rejected. Exercise both supported observation sizes so neither limit becomes an absolute count.
# Each fixture represents one contiguous lost run followed by the remaining replies.
while read -r sent received verdict; do
    sample="${tmp_dir}/burst-boundary-${sent}-${received}.json"
    jq -n --argjson sent "${sent}" --argjson received "${received}" \
        '{sent:$sent,received:$received,lost:($sent-$received),longest_loss_run:($sent-$received),
          min_ms:1,avg_ms:2,max_ms:3}' >"${sample}"
    if [[ "${verdict}" == pass ]]; then
        "${verify}" effect burst-loss "${tmp_dir}/baseline.json" "${sample}" \
            || fail "burst boundary ${received}/${sent} did not qualify"
    else
        must_fail "${verify}" effect burst-loss "${tmp_dir}/baseline.json" "${sample}"
    fi
done <<'EOF'
40 5 pass
40 4 fail
400 50 pass
400 49 fail
40 37 pass
40 38 fail
400 370 pass
400 371 fail
EOF

# Random and combined loss keep their inclusive lost and received fractions at both sample sizes,
# with the next packet outside each boundary rejected. A sample that loses nothing never qualifies
# a loss profile: forty replies in forty is the combined observation that failed a nightly soak.
# Combined loss also carries the latency and throughput effect its verdict requires.
while read -r profile sent received verdict; do
    sample="${tmp_dir}/${profile}-boundary-${sent}-${received}.json"
    jq -n --argjson sent "${sent}" --argjson received "${received}" \
        '{sent:$sent,received:$received,lost:($sent-$received),longest_loss_run:1,
          min_ms:120,avg_ms:200,max_ms:260}' >"${sample}"
    evidence=("${tmp_dir}/baseline.json" "${sample}")
    if [[ "${profile}" == combined ]]; then
        evidence+=("${tmp_dir}/rate-baseline.json" "${tmp_dir}/rate-fault.json")
    fi
    if [[ "${verdict}" == pass ]]; then
        "${verify}" effect "${profile}" "${evidence[@]}" >/dev/null \
            || fail "${profile} boundary ${received}/${sent} did not qualify"
    else
        must_fail "${verify}" effect "${profile}" "${evidence[@]}"
    fi
done <<'EOF'
random-loss 40 37 pass
random-loss 40 38 fail
random-loss 400 370 pass
random-loss 400 371 fail
random-loss 40 5 pass
random-loss 40 4 fail
random-loss 400 50 pass
random-loss 400 49 fail
random-loss 40 40 fail
random-loss 400 400 fail
combined 40 38 pass
combined 40 39 fail
combined 400 380 pass
combined 400 381 fail
combined 40 5 pass
combined 40 4 fail
combined 400 50 pass
combined 400 49 fail
combined 40 40 fail
combined 400 400 fail
EOF

# A correlated-loss sample starts with the four replies seen in the failed soak observation,
# then continues with a qualifying loss distribution. Drive the production sampler, parser and
# effect verifier together: a short prefix must not stand in for the complete burst observation.
(
    # shellcheck source=../link-degradation.sh
    source "${chaos_dir}/link-degradation.sh"
    scenario=degraded-probe-control
    run_id=degraded-probe-control
    script_dir="${chaos_dir}"
    CHAOS_PROBE_IMAGE=probe-control
    expected_probe_count=400
    expected_probe_interval=0.01
    owned_service_container() { printf 'sender-control\n'; }
    node_address() { printf '10.213.1.13\n'; }
    # The probe launches its helper under the launch bound, waits for its sample under the probe's
    # own bound and then reads and removes it; the helper's logs carry the sample this generates.
    run_bounded() {
        local limit="$1"
        shift
        case "$1 $2" in
            'docker run')
                [[ "${limit}" -eq "${link_degradation_launch_seconds}" ]] \
                    || fail 'link probe changed its launch bound'
                local cidfile=""
                # The helper passes everything after its script's -- separator to ping.
                while [[ "$#" -gt 0 && "$1" != -- ]]; do
                    [[ "$1" != --cidfile ]] || cidfile="$2"
                    shift
                done
                [[ "$#" -eq 8 && "$2 $4 $6 $7" == '-c -i -W 1' && "$8" == 10.213.1.13 ]] \
                    || fail 'link probe changed its ping arguments'
                sampled_count="$3"
                sampled_interval="$5"
                [[ "${sampled_count}" -eq "${expected_probe_count}" \
                    && "${sampled_interval}" == "${expected_probe_interval}" ]] \
                    || fail 'link probe changed the profile packet count or interval'
                printf 'helper-control\n' | tee "${cidfile}"
                ;;
            'docker wait')
                [[ "${limit}" -eq 30 ]] || fail 'link probe changed its sample bound'
                printf '0\n'
                ;;
            'docker logs')
                awk -v count="${sampled_count}" -v interval="${sampled_interval}" '
                    BEGIN {
                        if (count * interval > 4) exit 2
                        print "PING 10.213.1.13: 56 data bytes"
                        received = 0
                        for (seq = 0; seq < count; seq++) {
                            if (seq < 40 && seq != 10 && seq != 17 && seq != 26 && seq != 30) continue
                            if (seq >= 40 && seq % 5 < 2) continue
                            printf "64 bytes from 10.213.1.13: seq=%d ttl=64 time=0.060 ms\n", seq
                            received++
                        }
                        printf "%d packets transmitted, %d packets received, %.1f%% packet loss\n", count, received, (count - received) * 100 / count
                        print "round-trip min/avg/max = 0.050/0.060/0.067 ms"
                    }
                '
                ;;
            'docker container') [[ "$3 $4 $5" == 'rm --force helper-control' ]] || fail "unexpected removal: $*" ;;
            *) fail "link probe ran an unexpected command: $*" ;;
        esac
    }
    link_ping "${tmp_dir}/burst-sample.txt" sender receiver burst-loss
    "${verify}" effect burst-loss "${tmp_dir}/baseline.json" "${tmp_dir}/burst-sample.json" \
        >/dev/null || fail 'the complete bounded burst sample did not qualify'
    must_fail "${verify}" ping "${tmp_dir}/ping.txt" "${tmp_dir}/burst-truncated.json" 400
    must_fail "${verify}" ping "${tmp_dir}/burst-sample.txt" "${tmp_dir}/invalid-count.json" 0
    must_fail "${verify}" ping "${tmp_dir}/burst-sample.txt" "${tmp_dir}/invalid-count.json" 401
    must_fail "${verify}" ping "${tmp_dir}/burst-sample.txt" "${tmp_dir}/invalid-count.json" 18446744073709551656
    must_fail "${verify}" effect burst-loss "${tmp_dir}/baseline.json" "${tmp_dir}/baseline.json"
    jq '.longest_loss_run = 1' "${tmp_dir}/burst-sample.json" >"${tmp_dir}/burst-isolated-loss.json"
    must_fail "${verify}" effect burst-loss "${tmp_dir}/baseline.json" "${tmp_dir}/burst-isolated-loss.json"
    # Every loss profile judges its lost floor over the larger sample in the same window.
    for profile in random-loss combined; do
        link_ping "${tmp_dir}/${profile}-sample.txt" sender receiver "${profile}"
        jq -e '.sent == 400' "${tmp_dir}/${profile}-sample.json" >/dev/null \
            || fail "${profile} observation did not retain its packet count"
    done
    expected_probe_count=40
    expected_probe_interval=0.1
    for profile in baseline healed delay jitter rate-limit; do
        link_ping "${tmp_dir}/${profile}-sample.txt" sender receiver "${profile}"
        jq -e '.sent == 40' "${tmp_dir}/${profile}-sample.json" >/dev/null \
            || fail "${profile} observation did not retain its packet count"
    done
    must_fail link_ping "${tmp_dir}/invalid-profile.txt" sender receiver unrecognized
)

printf 'degraded evidence self-test passed\n'
