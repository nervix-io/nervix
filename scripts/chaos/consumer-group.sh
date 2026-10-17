#!/usr/bin/env bash
# Sourced by the scenarios that judge where Kafka runs the baseline ingestor. Owns reading its
# consumer group's members through the broker's own CLI and attributing each member to a node by the
# fixed address the run assigned that node.

node_address() {
    local variable="CHAOS_NODE_${1##*-}_ADDRESS"
    printf '%s\n' "${!variable}"
}

consumer_group_snapshot() {
    local output="$1"
    broker_admin /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server broker:9092 --group chaos_baseline --describe --members \
        >"${output}.txt" 2>&1 || return 1
    local address_map
    address_map="$(for host in "${node_hosts[@]}"; do printf '%s %s\n' "$(node_address "${host}")" "${host}"; done)"
    awk -v address_map="${address_map}" '
        BEGIN {
            count = split(address_map, entries, "\n")
            for (i = 1; i <= count; i++) {
                split(entries[i], pair, " ")
                host_of[pair[1]] = pair[2]
            }
        }
        $1 == "chaos_baseline" && $3 ~ /^\// {
            address = substr($3, 2)
            host = (address in host_of) ? host_of[address] : "unattributed"
            printf "{\"consumer\":\"%s\",\"address\":\"%s\",\"node_host\":\"%s\",\"partitions\":%d}\n", $2, address, host, $NF
        }
    ' "${output}.txt" | jq -s '{members: .}' >"${output}"
}

# True when Kafka ingestion runs only on OWNER: every source consumer-group member is at OWNER's
# fixed address and one of them holds the partition. No member can then remain at another node.
ingestion_on() {
    local output_dir="$1"
    local owner="$2"
    mkdir -p "${output_dir}"
    consumer_group_snapshot "${output_dir}/consumer-group.json" || return 1
    jq -e --arg owner "nervix-${owner##*-}" '
        (.members | length) > 0
        and (.members | all(.node_host == $owner))
        and ([.members[].partitions] | add) == 1
    ' "${output_dir}/consumer-group.json" >/dev/null
}
