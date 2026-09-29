#!/usr/bin/env bash
set -euo pipefail
chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
verify="${chaos_dir}/verify-degraded-evidence.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() { printf 'degraded evidence self-test: %s\n' "$*" >&2; exit 1; }
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
"${verify}" ping "${tmp_dir}/ping.txt" "${tmp_dir}/ping.json"
jq -e '.sent==40 and .received==38 and .lost==2 and .longest_loss_run==2 and .avg_ms==180' \
    "${tmp_dir}/ping.json" >/dev/null || fail 'ping parser did not retain the loss burst'

jq -n '{sent:40,received:40,lost:0,longest_loss_run:0,min_ms:1,avg_ms:2,max_ms:3}' \
    >"${tmp_dir}/baseline.json"
"${verify}" effect jitter "${tmp_dir}/baseline.json" "${tmp_dir}/ping.json" >/dev/null
must_fail "${verify}" effect random-loss "${tmp_dir}/baseline.json" "${tmp_dir}/ping.json"
jq -n '{bytes:262144,duration_ms:1200}' >"${tmp_dir}/rate-baseline.json"
jq -n '{bytes:262144,duration_ms:4200}' >"${tmp_dir}/rate-fault.json"
"${verify}" effect combined "${tmp_dir}/baseline.json" "${tmp_dir}/ping.json" \
    "${tmp_dir}/rate-baseline.json" "${tmp_dir}/rate-fault.json" >/dev/null
must_fail "${verify}" effect combined "${tmp_dir}/baseline.json" "${tmp_dir}/ping.json" \
    "${tmp_dir}/rate-baseline.json" "${tmp_dir}/rate-baseline.json"

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

printf 'degraded evidence self-test passed\n'
