#!/usr/bin/env bash
# Sourced by run-baseline.sh after the external cluster, the baseline graph and the lifetimes graph
# are ready. Drives interleaved concrete branches through a junction that waits for their
# materialized state, a generator, a window of sketches and a relay whose branches rotate through an
# LRU bound; replaces materialized rows with higher, lower and equal timestamps; lets an expiring
# branch expire; holds buffered route output until an alteration force-flushes it; makes an emitter
# fail on a missing topic and recover; holds two table emitters behind a one-connection client pool
# that table locks keep full; composes one fault with that state; and judges every output against
# the external ledgers of what the sources accepted.

# shellcheck source=recovery-scenario.sh
source "${script_dir}/recovery-scenario.sh"
# shellcheck source=node-faults.sh
source "${script_dir}/node-faults.sh"

# The lifetimes entities, by the kind their RELOCATE statement spells. The ingestors run apart from
# the state owner, so a fault on the owner leaves their Kafka consumers in place.
lifetimes_owned_entities=(
    'RELAY chaos_life_profiles' 'RELAY chaos_life_records' 'JUNCTION chaos_life_enrich'
    'RELAY chaos_life_enriched' 'EMITTER chaos_life_enriched_emitter' 'GENERATOR chaos_life_generator'
    'RELAY chaos_life_generated' 'EMITTER chaos_life_generated_emitter' 'WINDOW PROCESSOR chaos_life_window'
    'RELAY chaos_life_windows' 'EMITTER chaos_life_window_emitter' 'REINGESTOR chaos_life_rotate'
    'RELAY chaos_life_rotating' 'EMITTER chaos_life_rotating_emitter'
    'RELAY chaos_life_expiring_profiles' 'RELAY chaos_life_expiring_records'
    'JUNCTION chaos_life_expiring_enrich' 'RELAY chaos_life_expiring_enriched'
    'EMITTER chaos_life_expiring_emitter' 'RELAY chaos_life_pooled'
    'EMITTER chaos_life_pool_first' 'EMITTER chaos_life_pool_second'
)
lifetimes_ingestors=(
    'INGESTOR chaos_life_profile_ingestor' 'INGESTOR chaos_life_ingestor'
    'INGESTOR chaos_life_expiring_profile_ingestor' 'INGESTOR chaos_life_expiring_ingestor'
    'INGESTOR chaos_life_pool_ingestor'
)
lifetimes_output_topics=(chaos_life_enriched_output chaos_life_generated_output chaos_life_window_output
    chaos_life_rotating_output chaos_life_expiring_output)

# The lifetimes fixture and its boundaries, by record number of the life load. Blue publishes no
# profile until the load holds after `lifetimes_parked_until`, so blue's first records park on
# REQUIRED WAIT. Red's profile is then replaced with a higher timestamp, offered a lower and an
# equal one that must not replace it, and replaced again; the load holds after each publication
# until the source acknowledged every record before it.
lifetimes_fixture_records=1000
lifetimes_parked_until=24
lifetimes_replaced_until=40
lifetimes_lower_until=56
lifetimes_equal_until=72
lifetimes_milestone_records=88
# After the fault the run continues for this many records, twenty of each branch.
lifetimes_records_after_recovery=80
# Red's timestamp sequence: the version each publication carries, the second of the base minute
# its updated_at names, and the version the relay must hold after it.
lifetimes_red_versions=(1 2 3 4 5)
lifetimes_red_seconds=(10 20 15 20 30)
lifetimes_red_held=(1 2 2 2 5)
# The expiring branch's records before its expiry and after its recreation.
lifetimes_expiring_before=8
lifetimes_expiring_after=8
# The late pipeline's records, which wait for their output topic, and the buffered pipeline's,
# which a 300-second flush interval holds until an alteration force-flushes them.
lifetimes_late_records=5
lifetimes_flush_records=6
lifetimes_flush_interval_seconds=300
# The pooled pipeline's records: the first batch is written before the table locks, the rest wait
# behind them.
lifetimes_pool_batch=4
lifetimes_pool_records=12

# External budgets, in seconds.
lifetimes_phase_bound=180
lifetimes_drain_bound=180
lifetimes_placement_bound=120
lifetimes_expiry_bound=150
lifetimes_flush_bound=60

lifetimes_fail() {
    recovery_fail "$@"
}

lifetimes_fault_seconds() {
    if [[ "${outage_option_set}" == true ]]; then
        printf '%s\n' "${outage_seconds}"
        return 0
    fi
    case "${fault}" in
        *-partition) printf '45\n' ;;
        *) printf '%s\n' "${outage_seconds}" ;;
    esac
}

# The profile timestamp at SECOND of the run's base minute, in RFC 3339.
lifetimes_profile_time() {
    printf '%s:%02dZ\n' "${lifetimes_base_minute}" "$1"
}

# Publishes one profile record per BRANCH:VERSION:SECOND triple to TOPIC, in order, and records
# them in OUTPUT.ndjson.
lifetimes_publish() {
    local topic="$1"
    local output="$2"
    shift 2
    : >"${output}.ndjson"
    local triple branch version second
    for triple in "$@"; do
        IFS=: read -r branch version second <<<"${triple}"
        jq -nc --arg branch "${branch}" --argjson version "${version}" \
            --arg updated_at "$(lifetimes_profile_time "${second}")" \
            '{branch_name:$branch,profile_version:$version,label:("\($branch)-v\($version)"),updated_at:$updated_at}' \
            >>"${output}.ndjson"
    done
    kcat -b broker:9092 -P -t "${topic}" <"${output}.ndjson" >"${output}.producer.txt" 2>&1 \
        || lifetimes_fail setup "the profile producer did not accept ${output##*/}"
    cat "${output}.ndjson" >>"${artifact_dir}/traffic/${topic}.published.ndjson"
}

# Produces COUNT records of KIND, numbered from FIRST, to TOPIC and appends them to its ledger.
lifetimes_produce() {
    local topic="$1"
    local kind="$2"
    local first="$3"
    local count="$4"
    local batch="${artifact_dir}/lifetimes/${topic}-${first}.ndjson"
    jq -nc --arg run_id "${run_id}" --arg kind "${kind}" --argjson first "${first}" --argjson count "${count}" \
        -f "${script_dir}/fixtures/generate-lifetimes.jq" >"${batch}"
    kcat -b broker:9092 -P -t "${topic}" <"${batch}" >"${batch}.producer.txt" 2>&1 \
        || lifetimes_fail setup "the ${kind} producer did not accept records ${first} to $((first + count - 1))"
}

# Writes the rows `SHOW RELAY RELAY MATERIALIZED STATE` reports through HOST to OUTPUT.json, one
# object per branch with its key and payload.
lifetimes_state() {
    local relay="$1"
    local host="$2"
    local output="$3"
    local previous_host="${cli_host}"
    local status=0
    cli_host="${host}"
    domain_cli_command "SHOW RELAY ${relay} MATERIALIZED STATE;" >"${output}.txt" 2>&1 || status=$?
    cli_host="${previous_host}"
    ((status == 0)) || return 1
    sed -n 's/^key=\({[^}]*}\) payload=\({[^}]*}\) low=.*$/{"key":\1,"payload":\2}/p' "${output}.txt" \
        | jq -s '.' >"${output}.json"
}

# True when RELAY, read through HOST, holds exactly the BRANCH=VERSION pairs given, where a VERSION
# of `-` requires the branch to be absent.
lifetimes_state_is() {
    local relay="$1"
    local host="$2"
    local output="$3"
    shift 3
    lifetimes_state "${relay}" "${host}" "${output}" || return 1
    local pair branch version
    for pair in "$@"; do
        branch="${pair%%=*}"
        version="${pair#*=}"
        if [[ "${version}" == - ]]; then
            jq -e --arg branch "${branch}" 'all(.[]; .key.branch_name != $branch)' "${output}.json" >/dev/null || return 1
        else
            jq -e --arg branch "${branch}" --argjson version "${version}" '
                [.[] | select(.key.branch_name == $branch)] as $rows
                | ($rows | length) == 1 and $rows[0].payload.profile_version == $version
                  and $rows[0].payload.branch_name == $branch
            ' "${output}.json" >/dev/null || return 1
        fi
    done
}

# True when consumer group GROUP has committed TOPIC up to its end offset EXPECTED with no lag.
lifetimes_group_committed() {
    local group="$1"
    local topic="$2"
    local expected="$3"
    local output="$4"
    broker_admin /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker:9092 \
        --group "${group}" --describe >"${output}" 2>&1 || return 1
    awk -v topic="${topic}" -v expected="${expected}" '
        $2 == topic && $3 == "0" {
            found = 1
            if ($4 == expected && $5 == expected && $6 == 0) { committed = 1 }
        }
        END { exit !(found && committed) }
    ' "${output}"
}

lifetimes_source_end() {
    topic_end_offset chaos_life_input
}

# True once the life source holds at least COUNT records.
lifetimes_source_reached() {
    local count="$1"
    local current
    current="$(lifetimes_source_end)" || return 1
    [[ "${current}" =~ ^[0-9]+$ ]] && ((current >= count))
}

# Holds the life load once it has produced COUNT records. The hold file is replaced in one rename,
# so the load never reads a partly written count.
lifetimes_hold_after() {
    local count="$1"
    local hold_file="${artifact_dir}/traffic/hold-life-load"
    printf '%s\n' "${count}" >"${hold_file}.next"
    mv -f "${hold_file}.next" "${hold_file}"
}

lifetimes_load_running() {
    local load_id
    load_id="$(owned_service_container life-load)" || return 1
    if ! container_running "${load_id}"; then
        failure_category=setup
        lifetimes_fail setup "the life load stopped before the run stopped it: $(stopped_load_reason "${load_id}" 'increase its fixture')"
    fi
}

# Moves the life load's hold to COUNT, waits until the source holds COUNT records and its group has
# acknowledged all of them, which every attached route reached before acknowledging.
lifetimes_reach() {
    local count="$1"
    local description="$2"
    local output_dir="$3"
    lifetimes_hold_after "${count}"
    lifetimes_load_running
    wait_for "life source holding ${count} records (${description})" "${lifetimes_phase_bound}" \
        lifetimes_source_reached "${count}" \
        || lifetimes_fail setup "the life load did not reach ${count} records"
    wait_for "life source acknowledged through ${count} (${description})" "${lifetimes_phase_bound}" \
        lifetimes_group_committed chaos_life chaos_life_input "${count}" "${output_dir}/group-${count}.txt" \
        || lifetimes_fail product "the life source was not acknowledged through ${count} (${description})"
    printf '%s\n' "${count}" >>"${output_dir}/boundaries.txt"
}

# Writes OUTPUT_DIR.json, one object naming the owner of every lifetimes entity, read through HOST
# from the dry run of a relocation of all of them, which reports each entity's owner and moves
# nothing. DESCRIBE has no form for a generator, so the dry run is the one read that covers every
# kind.
lifetimes_placement() {
    local host="$1"
    local output_dir="$2"
    mkdir -p "${output_dir}"
    local selection
    selection="$(printf '%s, ' "${lifetimes_owned_entities[@]}" "${lifetimes_ingestors[@]}")"
    local previous_host="${cli_host}"
    local status=0
    cli_host="${host}"
    domain_cli_command "DESCRIBE RELOCATION ${selection%, } ONTO NODE ${node_names[0]} IGNORE PREFERENCES;" \
        >"${output_dir}/describe-relocation.txt" 2>&1 || status=$?
    cli_host="${previous_host}"
    ((status == 0)) || return 1
    local owners
    owners="$(awk '/^- kind=/ {
            name = ""
            owner = ""
            for (field = 2; field <= NF; field++) {
                split($field, pair, "=")
                if (pair[1] == "name") name = pair[2]
                if (pair[1] == "owner") owner = pair[2]
            }
            if (name != "") printf "%s %s\n", name, owner
        }' "${output_dir}/describe-relocation.txt" \
        | jq -R -n '[inputs | split(" ") | {key: .[0], value: .[1]}] | from_entries')" || return 1
    local entity
    for entity in "${lifetimes_owned_entities[@]}" "${lifetimes_ingestors[@]}"; do
        jq -n --arg entity "${entity}" --arg name "${entity##* }" --argjson owners "${owners}" \
            '{key: $entity, value: {owner: ($owners[$name] // "")}}'
    done | jq -s 'from_entries' >"${output_dir}.json"
}

# True when every lifetimes entity is owned by OWNER and every lifetimes ingestor by INGEST_NODE.
lifetimes_placed_on() {
    local owner="$1"
    local ingest_node="$2"
    local output_dir="$3"
    lifetimes_placement "${node_hosts[0]}" "${output_dir}" || return 1
    jq -e --arg owner "${owner}" --arg ingest "${ingest_node}" '
        to_entries | all(.[]; if (.key | startswith("INGESTOR ")) then .value.owner == $ingest
                              else .value.owner == $owner end)
    ' "${output_dir}.json" >/dev/null
}

lifetimes_moved_off() {
    local away="$1"
    local host="$2"
    local output_dir="$3"
    lifetimes_placement "${host}" "${output_dir}" || return 1
    jq -e --arg away "${away}" '
        to_entries | map(select(.key | startswith("INGESTOR ") | not))
        | all(.[]; (.value.owner | test("^node-[0-9]+$")) and .value.owner != $away)
    ' "${output_dir}.json" >/dev/null
}

# Writes DESCRIBE EMITTER EMITTER, read through the first node, which no owner fault targets, to
# OUTPUT.txt and is true when it reports status STATUS.
lifetimes_emitter_status_is() {
    local emitter="$1"
    local status="$2"
    local output="$3"
    domain_cli_command "DESCRIBE EMITTER ${emitter};" >"${output}.txt" 2>&1 || return 1
    grep -Fxq "status: ${status}" "${output}.txt"
}

# Sums nervix_branch_evictions_total for BRANCH and REASON over every node's metrics. A node whose
# metrics cannot be read counts nothing.
lifetimes_evictions() {
    local branch="$1"
    local reason="$2"
    local total=0 host value metrics
    metrics="$(mktemp "${artifact_dir}/.metrics.XXXXXX")"
    for host in "${node_hosts[@]}"; do
        value=0
        if node_metrics "${host}" "${metrics}"; then
            value="$(awk -v branch="${branch}" -v reason="${reason}" '
                /^nervix_branch_evictions_total\{/ && index($0, "branch=\"" branch "\"") && index($0, "reason=\"" reason "\"") {
                    sum += $NF
                }
                END { printf "%d\n", sum }' "${metrics}")"
        fi
        total=$((total + value))
    done
    rm -f "${metrics}"
    printf '%d\n' "${total}"
}

# Prints the end offset of every lifetimes output topic as one JSON object.
lifetimes_output_offsets() {
    local topic offset
    local pairs=()
    for topic in "${lifetimes_output_topics[@]}"; do
        offset="$(topic_end_offset "${topic}")" || return 1
        [[ "${offset}" =~ ^[0-9]+$ ]] || return 1
        pairs+=(--argjson "${topic}" "${offset}")
    done
    jq -nc "${pairs[@]}" '$ARGS.named'
}

# Reads TOPIC from its beginning through its current end into the traffic directory.
lifetimes_read_topic() {
    local topic="$1"
    local case_dir="$2"
    local count
    count="$(topic_end_offset "${topic}")" || return 1
    if ((count > 0)); then
        kcat -q -b broker:9092 -C -t "${topic}" -p 0 -o beginning -c "${count}" \
            >"${artifact_dir}/traffic/${topic}.ndjson" 2>"${case_dir}/${topic}.stderr"
    else
        : >"${artifact_dir}/traffic/${topic}.ndjson"
    fi
    [[ "$(wc -l <"${artifact_dir}/traffic/${topic}.ndjson")" -eq "${count}" ]]
}

# The materialized phase: blue parks until its first profile, red's profile is replaced with a
# higher timestamp, offered a lower and an equal one, and replaced again. Each boundary waits for
# the publication to apply before the load moves on.
lifetimes_materialized_phases() {
    local case_dir="$1"
    local host="${node_hosts[0]}"
    phase 'lifetimes: blue records park until their state arrives'
    wait_for "life source holding ${lifetimes_parked_until} records" "${lifetimes_phase_bound}" \
        lifetimes_source_reached "${lifetimes_parked_until}" \
        || lifetimes_fail setup "the life load did not reach ${lifetimes_parked_until} records"
    lifetimes_state_is chaos_life_profiles "${host}" "${case_dir}/state-before-blue" blue=- \
        || lifetimes_fail product 'blue held materialized state before its first profile'
    # Record 2 is blue's first, and the source commits only below its oldest unacknowledged record,
    # so while blue's records park the committed offset stays below 2.
    local parked_offset
    parked_offset="$(lifetimes_group_offset chaos_life chaos_life_input "${case_dir}/group-parked.txt")" \
        || parked_offset=0
    printf '%s\n' "${parked_offset:-0}" >"${case_dir}/parked-committed-offset.txt"
    ((${parked_offset:-0} < 2)) \
        || recovery_finding "${case_dir}" "the life source committed through ${parked_offset} while blue's first record had no state"
    lifetimes_blue_published_ms="$(epoch_ms)"
    lifetimes_publish chaos_life_profile_input "${case_dir}/profile-blue" "blue:1:10"
    wait_for 'blue profile version 1' 90 \
        lifetimes_state_is chaos_life_profiles "${host}" "${case_dir}/state-blue" blue=1 \
        || lifetimes_fail product 'the materialized relay never held blue profile version 1'
    lifetimes_reach "${lifetimes_parked_until}" 'blue released' "${case_dir}"

    local index boundaries=("${lifetimes_replaced_until}" "${lifetimes_lower_until}" "${lifetimes_equal_until}" "${lifetimes_milestone_records}")
    local profile_messages=4
    for index in 1 2 3 4; do
        local version="${lifetimes_red_versions[${index}]}"
        local second="${lifetimes_red_seconds[${index}]}"
        local held="${lifetimes_red_held[${index}]}"
        phase "lifetimes: red version ${version} at second ${second}, held version ${held}"
        lifetimes_publish chaos_life_profile_input "${case_dir}/profile-red-${version}" "red:${version}:${second}"
        profile_messages=$((profile_messages + 1))
        wait_for "red profile message ${profile_messages} applied" 90 \
            lifetimes_group_committed chaos_life_profiles chaos_life_profile_input "${profile_messages}" \
            "${case_dir}/profile-group-${profile_messages}.txt" \
            || lifetimes_fail product "the profile source was not acknowledged through red version ${version}"
        wait_for "red held at version ${held}" 60 \
            lifetimes_state_is chaos_life_profiles "${host}" "${case_dir}/state-red-${version}" "red=${held}" \
            || lifetimes_fail product "red does not hold version ${held} after version ${version} at second ${second}"
        lifetimes_reach "${boundaries[$((index - 1))]}" "after red version ${version}" "${case_dir}"
    done
}

# The late pipeline: its emitter reports an error while its output topic is missing, then recovers
# once the run provisions the topic, and delivers every record exactly once.
lifetimes_late_topic() {
    local case_dir="$1"
    phase 'lifetimes: an emitter waits for its missing topic'
    lifetimes_produce chaos_life_late_input late 1 "${lifetimes_late_records}"
    wait_for 'the late emitter reports its missing topic' 120 \
        lifetimes_emitter_status_is chaos_life_late_emitter ERROR "${case_dir}/late-error" \
        || lifetimes_fail product 'the late emitter never reported an error for its missing topic'
    broker_admin /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:9092 \
        --create --topic chaos_life_late_output --partitions 1 --replication-factor 1 \
        >"${case_dir}/create-late-topic.txt"
    wait_for 'the late pipeline acknowledged through its records' "${lifetimes_drain_bound}" \
        lifetimes_group_committed chaos_life_late chaos_life_late_input "${lifetimes_late_records}" \
        "${case_dir}/late-group.txt" \
        || lifetimes_fail product 'the late pipeline never delivered its records after the topic appeared'
    wait_for 'the late emitter reports recovery' 120 \
        lifetimes_emitter_status_is chaos_life_late_emitter OK "${case_dir}/late-ok" \
        || lifetimes_fail product 'the late emitter did not return to OK after its topic appeared'
}

# True when TOPIC holds at least COUNT records, recording when it first did into FILE.
lifetimes_topic_reached_at() {
    local topic="$1"
    local count="$2"
    local file="$3"
    local current
    current="$(topic_end_offset "${topic}")" || return 1
    [[ "${current}" =~ ^[0-9]+$ ]] && ((current >= count)) || return 1
    epoch_ms >"${file}"
}

# The buffered pipeline: a 300-second flush interval holds its records until an alteration's hold
# force-flushes them, and a second alteration finds the pipeline idle.
lifetimes_force_flush() {
    local case_dir="$1"
    phase 'lifetimes: an alteration force-flushes buffered route output'
    lifetimes_flush_produced_ms="$(epoch_ms)"
    lifetimes_produce chaos_life_flush_input flush 1 "${lifetimes_flush_records}"
    # The records must reach the buffered route before the alteration holds it.
    lifetimes_flush_admission_observed=true
    wait_for 'buffered records received' 60 \
        lifetimes_ingestor_received chaos_life_flush_ingestor "${lifetimes_flush_records}" \
        || lifetimes_flush_admission_observed=false
    local held_offset
    held_offset="$(topic_end_offset chaos_life_buffered_output)" || held_offset=null
    local started_ms
    started_ms="$(epoch_ms)"
    domain_cli_command 'ALTER KAFKA TOPIC INGESTOR chaos_life_flush_ingestor SET GENERAL ERROR IGNORE;' \
        >"${case_dir}/alter-flush-requested.txt" 2>&1 \
        || lifetimes_fail product 'the force-flushing alteration was not acknowledged'
    local completed_ms
    completed_ms="$(epoch_ms)"
    wait_for 'buffered output force-flushed' "${lifetimes_flush_bound}" \
        lifetimes_topic_reached_at chaos_life_buffered_output "${lifetimes_flush_records}" "${case_dir}/flush-arrived-ms.txt" \
        || recovery_finding "${case_dir}" "the alteration did not force-flush the buffered records within ${lifetimes_flush_bound}s"
    domain_cli_command 'ALTER KAFKA TOPIC INGESTOR chaos_life_flush_ingestor SET GENERAL ERROR LOG;' \
        >"${case_dir}/alter-flush-idle.txt" 2>&1 \
        || lifetimes_fail product 'the alteration of the idle buffered pipeline was not acknowledged'
    jq -n --argjson produced "${lifetimes_flush_produced_ms}" --argjson started "${started_ms}" \
        --argjson completed "${completed_ms}" \
        --argjson arrived "$(cat "${case_dir}/flush-arrived-ms.txt" 2>/dev/null || printf 'null')" \
        --argjson interval "$((lifetimes_flush_interval_seconds * 1000))" \
        --argjson held_offset "${held_offset}" \
        --argjson admission_observed "${lifetimes_flush_admission_observed}" \
        '{produced_ms:$produced, alteration_started_ms:$started, alteration_completed_ms:$completed,
          arrived_ms:$arrived, flush_interval_ms:$interval, output_before_alteration:$held_offset,
          admission_observed:$admission_observed}' >"${case_dir}/force-flush.json"
}

# Prints the committed offset of consumer group GROUP on TOPIC partition 0.
lifetimes_group_offset() {
    local group="$1"
    local topic="$2"
    local output="$3"
    broker_admin /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker:9092 \
        --group "${group}" --describe >"${output}" 2>&1 || return 1
    awk -v topic="${topic}" '$2 == topic && $3 == "0" && $4 ~ /^[0-9]+$/ { print $4; exit }' "${output}"
}

# True when the nodes' metrics count at least COUNT messages INGESTOR received, which shows the
# records reached its buffered route.
lifetimes_ingestor_received() {
    local ingestor="$1"
    local count="$2"
    local total=0 host value metrics
    metrics="$(mktemp "${artifact_dir}/.metrics.XXXXXX")"
    for host in "${node_hosts[@]}"; do
        value=0
        if node_metrics "${host}" "${metrics}"; then
            value="$(awk -v target="${ingestor}" '
                /^nervix_messages_total\{/ && index($0, "target_kind=\"INGESTOR\"") && index($0, "target=\"" target "\"") \
                    && index($0, "direction=\"received\"") { sum += $NF }
                END { printf "%d\n", sum }' "${metrics}")"
        fi
        total=$((total + value))
    done
    rm -f "${metrics}"
    ((total >= count))
}

# True when the nodes count more TTL evictions of the expiring branch than BEFORE, and records the
# count they reached.
lifetimes_expiry_evicted() {
    local before="$1"
    lifetimes_expiry_evictions_after="$(lifetimes_evictions chaos_life_expiring ttl)"
    ((lifetimes_expiry_evictions_after > before))
}

# The expiring branch: its records carry its first profile, its materialized row expires after
# the branch TTL without activity, and nothing restores it before it is recreated.
lifetimes_expiry() {
    local case_dir="$1"
    local host="${node_hosts[0]}"
    phase 'lifetimes: the expiring branch expires'
    lifetimes_publish chaos_life_expiring_profile_input "${case_dir}/expiring-profile-1" "green:1:10"
    wait_for 'expiring green profile version 1' 90 \
        lifetimes_state_is chaos_life_expiring_profiles "${host}" "${case_dir}/expiring-state-1" green=1 \
        || lifetimes_fail product 'the expiring relay never held green profile version 1'
    lifetimes_produce chaos_life_expiring_input expiring 1 "${lifetimes_expiring_before}"
    wait_for 'expiring green records acknowledged' "${lifetimes_phase_bound}" \
        lifetimes_group_committed chaos_life_expiring chaos_life_expiring_input "${lifetimes_expiring_before}" \
        "${case_dir}/expiring-group-before.txt" \
        || lifetimes_fail product 'the expiring records were not acknowledged before the branch expired'
    lifetimes_expiry_evictions_before="$(lifetimes_evictions chaos_life_expiring ttl)"
    wait_for 'expiring green profile expired' "${lifetimes_expiry_bound}" \
        lifetimes_state_is chaos_life_expiring_profiles "${host}" "${case_dir}/expiring-state-expired" green=- \
        || lifetimes_fail product 'the expiring green profile did not expire after its branch TTL'
    # Every entity that holds the branch releases it at its own scan, so the nodes can count its
    # eviction after the row left the relay. The verdict judges the count.
    wait_for 'a TTL eviction of the expiring branch counted' "${lifetimes_expiry_bound}" \
        lifetimes_expiry_evicted "${lifetimes_expiry_evictions_before}" || true
}

# Recreates the expiring branch: its new records park on REQUIRED WAIT until its second profile
# arrives, which they then carry.
lifetimes_recreation() {
    local case_dir="$1"
    local host="${node_hosts[0]}"
    phase 'lifetimes: the expiring branch is recreated'
    lifetimes_state_is chaos_life_expiring_profiles "${host}" "${case_dir}/expiring-state-before-recreation" green=- \
        || recovery_finding "${case_dir}" 'the expired green profile was restored before its branch was recreated'
    lifetimes_produce chaos_life_expiring_input expiring "$((lifetimes_expiring_before + 1))" "${lifetimes_expiring_after}"
    lifetimes_publish chaos_life_expiring_profile_input "${case_dir}/expiring-profile-2" "green:2:40"
    wait_for 'expiring green records acknowledged after recreation' "${lifetimes_phase_bound}" \
        lifetimes_group_committed chaos_life_expiring chaos_life_expiring_input \
        "$((lifetimes_expiring_before + lifetimes_expiring_after))" "${case_dir}/expiring-group-after.txt" \
        || recovery_finding "${case_dir}" 'the recreated expiring branch did not release its parked records'
    lifetimes_state_is chaos_life_expiring_profiles "${host}" "${case_dir}/expiring-state-2" green=2 \
        || recovery_finding "${case_dir}" 'the recreated green profile is not version 2'
}

# Runs one SQL statement against the pooled emitters' database and prints its unaligned answer.
lifetimes_sql() {
    compose exec -T postgres psql -U postgres -h 127.0.0.1 -v ON_ERROR_STOP=1 -At -c "$1" </dev/null
}

# True when each pooled table holds COUNT rows.
lifetimes_pool_tables_hold() {
    local count="$1"
    local table rows
    for table in chaos_life_pool_first chaos_life_pool_second; do
        rows="$(lifetimes_sql "SELECT count(*) FROM ${table}")" || return 1
        [[ "${rows}" == "${count}" ]] || return 1
    done
}

# True when the lock session holds its exclusive lock on both pooled tables.
lifetimes_pool_locked() {
    [[ "$(lifetimes_sql "SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation
        WHERE c.relname IN ('chaos_life_pool_first', 'chaos_life_pool_second')
          AND l.mode = 'AccessExclusiveLock' AND l.granted")" == 2 ]]
}

# True when a pooled insert waits for a table lock while it holds the pool's one connection.
lifetimes_pool_insert_blocked() {
    local count
    count="$(lifetimes_sql "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'
        AND query ILIKE '%chaos_life_pool_%' AND application_name = 'chaos_life_pool'")" || return 1
    [[ "${count}" =~ ^[0-9]+$ ]] && ((count >= 1))
}

# Records the pooled client's connections from each node's address, which the one-connection pool
# of every node bounds by one.
lifetimes_pool_connections() {
    local output="$1"
    lifetimes_sql "SELECT coalesce(json_agg(json_build_object('address', address, 'connections', connections)), '[]')
        FROM (SELECT host(client_addr) AS address, count(*) AS connections FROM pg_stat_activity
              WHERE application_name = 'chaos_life_pool' GROUP BY 1) AS per_address" >"${output}"
}

# True when either pooled emitter, described through the first node, which no owner fault targets,
# reports that it waits for a connection from the full pool, and records which one did into
# OUTPUT.emitter.
lifetimes_pool_waiting() {
    local output="$1"
    local emitter
    for emitter in chaos_life_pool_first chaos_life_pool_second; do
        domain_cli_command "DESCRIBE EMITTER ${emitter};" >"${output}-${emitter}.txt" 2>&1 || continue
        if grep -Fxq 'status: WAITING' "${output}-${emitter}.txt" \
            && grep -Fq "waiting for a connection from client 'chaos_life_pg'" "${output}-${emitter}.txt"; then
            printf '%s\n' "${emitter}" >"${output}.emitter"
            return 0
        fi
    done
    return 1
}

# The pooled pipeline: table locks hold one emitter's insert and with it the pool's one connection,
# so the other emitter waits for the full pool and says so in DESCRIBE, whichever of them borrowed
# first. The locks hold through the fault.
lifetimes_pool_exhaustion() {
    local case_dir="$1"
    phase 'lifetimes: a full pool holds an emitter back'
    lifetimes_produce chaos_life_pool_input pool 1 "${lifetimes_pool_batch}"
    wait_for 'pooled records written to both tables' "${lifetimes_phase_bound}" \
        lifetimes_pool_tables_hold "${lifetimes_pool_batch}" \
        || lifetimes_fail product 'the pooled emitters did not write their first records'
    compose exec -d -T -e PGAPPNAME=chaos_lock postgres psql -U postgres -h 127.0.0.1 -v ON_ERROR_STOP=1 -q -c \
        'BEGIN; LOCK TABLE chaos_life_pool_first, chaos_life_pool_second IN ACCESS EXCLUSIVE MODE;
         SELECT pg_sleep(3600); COMMIT;' </dev/null
    wait_for 'both pooled tables locked' 60 lifetimes_pool_locked \
        || lifetimes_fail setup 'the table locks were not granted'
    lifetimes_produce chaos_life_pool_input pool "$((lifetimes_pool_batch + 1))" \
        "$((lifetimes_pool_records - lifetimes_pool_batch))"
    wait_for 'a pooled insert waiting for its table lock' "${lifetimes_phase_bound}" lifetimes_pool_insert_blocked \
        || lifetimes_fail product 'no pooled insert waited for its table lock'
    wait_for 'a pooled emitter waiting for the full pool' "${lifetimes_phase_bound}" \
        lifetimes_pool_waiting "${case_dir}/pool-waiting" \
        || lifetimes_fail product 'no pooled emitter reported a wait for the full pool'
    lifetimes_pool_connections "${case_dir}/pool-connections-waiting.json" \
        || lifetimes_fail controller 'the pooled connections could not be read'
}

# Ends the table locks once the fault recovered: an emitter reports its wait for the pool again from
# the current owner, then both emitters write every record and return to OK.
lifetimes_pool_recovery() {
    local case_dir="$1"
    phase 'lifetimes: the pool recovers when the table lock ends'
    if [[ "${fault}" != none ]]; then
        wait_for 'a pooled emitter waiting for the full pool after the fault' "${lifetimes_phase_bound}" \
            lifetimes_pool_waiting "${case_dir}/pool-waiting-after-fault" \
            || recovery_finding "${case_dir}" 'no pooled emitter reported its wait for the full pool after the fault'
    fi
    lifetimes_sql "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE application_name = 'chaos_lock'" \
        >"${case_dir}/pool-lock-released.txt" \
        || lifetimes_fail controller 'the table locks could not be ended'
    wait_for 'every pooled record written to both tables' "${lifetimes_drain_bound}" \
        lifetimes_pool_tables_hold "${lifetimes_pool_records}" \
        || recovery_finding "${case_dir}" 'the pooled emitters did not write every record once the locks ended'
    local emitter
    for emitter in chaos_life_pool_first chaos_life_pool_second; do
        wait_for "${emitter} reports OK" 120 \
            lifetimes_emitter_status_is "${emitter}" OK "${case_dir}/pool-ok-${emitter}" \
            || recovery_finding "${case_dir}" "${emitter} did not return to OK after the locks ended"
    done
    local table
    for table in chaos_life_pool_first chaos_life_pool_second; do
        lifetimes_sql "SELECT coalesce(json_agg(json_build_object('event_id', event_id, 'sequence', sequence)
            ORDER BY sequence), '[]') FROM ${table}" >"${artifact_dir}/traffic/${table}.json" \
            || lifetimes_fail controller "the ${table} rows could not be read"
    done
}

# Requires the lifetimes work to leave the faulted owner, read through a survivor, while the fault
# holds. Runs as the faults' DURING hook.
lifetimes_failover() {
    local started_ms
    started_ms="$(epoch_ms)"
    wait_for "lifetimes work off ${lifetimes_target_node}" 120 \
        lifetimes_moved_off "${lifetimes_target_node}" "${lifetimes_survivor}" "${lifetimes_case_dir}/placement-during" \
        || recovery_finding "${lifetimes_case_dir}" "the lifetimes work stayed on ${lifetimes_target_node} during its fault"
    lifetimes_failover_ms=$(($(epoch_ms) - started_ms))
}

lifetimes_before_fault() {
    lifetimes_fault_source="$(lifetimes_source_end)"
    lifetimes_hold_after "${lifetimes_fault_source}"
    rm -f "${artifact_dir}/traffic/hold-life-load"
}

# Waits until every lifetimes output and the life source have drained, then reads every ledger.
lifetimes_final_boundary() {
    local case_dir="$1"
    phase 'lifetimes: final traffic boundary'
    local target=$((lifetimes_recovered_source + lifetimes_records_after_recovery))
    ((target <= lifetimes_fixture_records)) \
        || lifetimes_fail setup "the life fixture of ${lifetimes_fixture_records} records cannot reach ${target} records"
    lifetimes_load_running
    wait_for "life source reached ${target} records" "${lifetimes_phase_bound}" lifetimes_source_reached "${target}" \
        || lifetimes_fail setup 'the life load did not reach its final boundary'
    touch "${artifact_dir}/traffic/stop-life-load"
    local load_id
    load_id="$(owned_service_container life-load)" || return 1
    wait_for 'life load flushed and exited' 40 load_exited_cleanly "${load_id}"
    run_bounded 20 docker logs "${load_id}" >"${artifact_dir}/traffic/life-producer.log" 2>&1
    local life_end
    life_end="$(lifetimes_source_end)"
    [[ "${life_end}" =~ ^[0-9]+$ && "${life_end}" -le "${lifetimes_fixture_records}" ]] \
        || lifetimes_fail product 'the life source boundary exceeded the bounded fixture'
    kcat -q -b broker:9092 -C -t chaos_life_input -p 0 -o beginning -c "${life_end}" \
        >"${artifact_dir}/traffic/life-accepted-input.ndjson" 2>"${case_dir}/life-source.stderr"
    [[ "$(wc -l <"${artifact_dir}/traffic/life-accepted-input.ndjson")" -eq "${life_end}" ]] \
        || lifetimes_fail product 'the life accepted-input ledger did not cover the source boundary'
    record_load_pacing life-load chaos_life_input "${life_end}" "${CHAOS_LIFE_LOAD_INTERVAL_MS}"
    wait_for "life source acknowledged through ${life_end}" "${lifetimes_drain_bound}" \
        lifetimes_group_committed chaos_life chaos_life_input "${life_end}" "${case_dir}/final-life-group.txt" \
        || recovery_finding "${case_dir}" "the life source was not acknowledged through its final boundary ${life_end}"
    local first second
    first="$(lifetimes_output_offsets)" || lifetimes_fail controller 'the lifetimes output offsets could not be read'
    wait_for 'lifetimes outputs stable' 90 lifetimes_outputs_stable "${case_dir}/final-outputs.json" \
        || lifetimes_fail product 'lifetimes outputs did not settle after the source drained'
    local topic
    for topic in "${lifetimes_output_topics[@]}" chaos_life_late_output chaos_life_buffered_output \
        chaos_life_expiring_input; do
        lifetimes_read_topic "${topic}" "${case_dir}" \
            || lifetimes_fail product "the ${topic} ledger did not cover its final boundary"
    done
    lifetimes_final_source="${life_end}"
}

# True when the lifetimes output offsets stayed equal across two reads two seconds apart, which it
# then writes to FILE.
lifetimes_outputs_stable() {
    local output="$1"
    local first second
    first="$(lifetimes_output_offsets)" || return 1
    sleep 2
    second="$(lifetimes_output_offsets)" || return 1
    # The generator keeps emitting while its state exists; its output is read at the final boundary
    # rather than required to stop.
    [[ "$(jq -c 'del(.chaos_life_generated_output)' <<<"${first}")" == "$(jq -c 'del(.chaos_life_generated_output)' <<<"${second}")" ]] \
        || return 1
    printf '%s\n' "${second}" >"${output}"
}

# Runs every lifetimes verdict and records each failure as a product finding.
lifetimes_verify() {
    local case_dir="$1"
    phase 'lifetimes: verdicts'
    jq -n \
        --arg fault "${fault}" \
        --argjson parked_until "${lifetimes_parked_until}" \
        --argjson replaced_until "${lifetimes_replaced_until}" \
        --argjson lower_until "${lifetimes_lower_until}" \
        --argjson equal_until "${lifetimes_equal_until}" \
        --argjson milestone "${lifetimes_milestone_records}" \
        --argjson recovered "${lifetimes_recovered_source}" \
        --argjson expiring_before "${lifetimes_expiring_before}" \
        --argjson expiring_after "${lifetimes_expiring_after}" \
        --argjson late_records "${lifetimes_late_records}" \
        --argjson flush_records "${lifetimes_flush_records}" \
        --argjson pool_records "${lifetimes_pool_records}" \
        --arg pool_waiting "$(cat "${case_dir}/pool-waiting.emitter" 2>/dev/null)" \
        --arg pool_waiting_after_fault "$(cat "${case_dir}/pool-waiting-after-fault.emitter" 2>/dev/null)" \
        --slurpfile pool_connections "${case_dir}/pool-connections-waiting.json" \
        --argjson lru_evictions "$(lifetimes_evictions chaos_life_rotation lru)" \
        --argjson ttl_before "${lifetimes_expiry_evictions_before}" \
        --argjson ttl_after "${lifetimes_expiry_evictions_after}" \
        --slurpfile flush "${case_dir}/force-flush.json" \
        '{fault:$fault,
          boundaries:{parked_until:$parked_until,replaced_until:$replaced_until,lower_until:$lower_until,
                      equal_until:$equal_until,milestone_records:$milestone,recovered_records:$recovered},
          red_versions:[{until:$parked_until,version:1},{until:$replaced_until,version:2},
                        {until:$lower_until,version:2},{until:$equal_until,version:2},
                        {until:$milestone,version:5}],
          red_latest:5, red_superseded:[3,4],
          expiring:{before:$expiring_before,after:$expiring_after,ttl_evictions_before:$ttl_before,
                    ttl_evictions_after:$ttl_after},
          late_records:$late_records, flush_records:$flush_records, lru_evictions:$lru_evictions,
          force_flush:$flush[0],
          pool:{records:$pool_records,
                waiting_emitter:(if $pool_waiting == "" then null else $pool_waiting end),
                waiting_after_fault:(if $pool_waiting_after_fault == "" then null else $pool_waiting_after_fault end),
                connections_while_waiting:$pool_connections[0]}}' >"${case_dir}/boundaries.json"
    local mode status
    for mode in enrich generator window rotation expiring late flush pool; do
        status=0
        "${script_dir}/verify-lifetimes-evidence.sh" "${mode}" \
            --traffic "${artifact_dir}/traffic" \
            --boundaries "${case_dir}/boundaries.json" \
            --result "${case_dir}/verdict-${mode}.json" \
            >"${case_dir}/verdict-${mode}.txt" 2>&1 || status=$?
        if ((status == 1)); then
            recovery_finding "${case_dir}" "$(jq -r '.processor + ": " + (.failures | join("; "))' "${case_dir}/verdict-${mode}.json")"
        elif ((status != 0)); then
            lifetimes_fail controller "the ${mode} verifier could not judge its evidence; see lifetimes/verdict-${mode}.txt"
        fi
    done
}

run_lifetimes() {
    local case_dir="${artifact_dir}/lifetimes"
    mkdir -p "${case_dir}"
    : >"${case_dir}/boundaries.txt"
    recovery_traffic_startup "${case_dir}"
    lifetimes_failover_ms=null
    lifetimes_base_minute="$(date -u +%Y-%m-%dT%H:%M)"
    lifetimes_blue_published_ms=null

    failure_category=product
    phase 'lifetimes: placement'
    local state_owner="${node_names[0]}"
    local ingest_node="${node_names[0]}"
    if [[ "${node_count}" == 3 ]]; then
        wait_for 'settled cluster before lifetimes placement' 120 cluster_settled "${case_dir}"
        local leader
        leader="$(<"${case_dir}/leader.txt")"
        local node
        for node in node-3 node-2 node-1; do
            if [[ "${node}" != "${leader}" ]]; then
                state_owner="${node}"
                break
            fi
        done
        for node in node-1 node-2 node-3; do
            if [[ "${node}" != "${state_owner}" ]]; then
                ingest_node="${node}"
                break
            fi
        done
        local selection
        selection="$(printf '%s, ' "${lifetimes_owned_entities[@]}")"
        domain_cli_command "RELOCATE ${selection%, } ONTO NODE ${state_owner} IGNORE PREFERENCES;" \
            >"${case_dir}/relocate-state.txt" 2>&1
        selection="$(printf '%s, ' "${lifetimes_ingestors[@]}")"
        domain_cli_command "RELOCATE ${selection%, } ONTO NODE ${ingest_node} IGNORE PREFERENCES;" \
            >"${case_dir}/relocate-ingestors.txt" 2>&1
    fi
    wait_for "lifetimes work on ${state_owner}, ingestion on ${ingest_node}" "${lifetimes_placement_bound}" \
        lifetimes_placed_on "${state_owner}" "${ingest_node}" "${case_dir}/placement-before"

    phase 'lifetimes: traffic startup'
    : >"${artifact_dir}/traffic/chaos_life_profile_input.published.ndjson"
    : >"${artifact_dir}/traffic/chaos_life_expiring_profile_input.published.ndjson"
    lifetimes_publish chaos_life_profile_input "${case_dir}/profiles-loaded" "red:1:10" "gold:1:10" "green:1:10"
    wait_for 'red, gold and green profile version 1' 90 \
        lifetimes_state_is chaos_life_profiles "${node_hosts[0]}" "${case_dir}/profiles-loaded" red=1 gold=1 green=1 blue=- \
        || lifetimes_fail product 'the materialized relay never held the loaded profiles'
    lifetimes_hold_after "${lifetimes_parked_until}"
    rm -f "${artifact_dir}/traffic/stop-life-load"
    jq -nc --arg run_id "${run_id}" --arg kind life --argjson first 1 --argjson count "${lifetimes_fixture_records}" \
        -f "${script_dir}/fixtures/generate-lifetimes.jq" >"${CHAOS_LIFE_LOAD_FILE}"
    [[ "$(wc -l <"${CHAOS_LIFE_LOAD_FILE}")" -eq "${lifetimes_fixture_records}" ]] \
        || lifetimes_fail controller 'the life fixture generation was incomplete'
    compose up --detach --no-deps life-load
    wait_for 'life source started' 30 topic_progressed chaos_life_input 0

    lifetimes_materialized_phases "${case_dir}"
    lifetimes_late_topic "${case_dir}"
    lifetimes_force_flush "${case_dir}"
    lifetimes_pool_exhaustion "${case_dir}"
    lifetimes_expiry "${case_dir}"
    check_support_containers
    lifetimes_load_running

    local fault_seconds
    fault_seconds="$(lifetimes_fault_seconds)"
    lifetimes_case_dir="${case_dir}"
    lifetimes_target_node="${state_owner}"
    lifetimes_survivor="${node_hosts[0]}"
    local host
    for host in "${node_hosts[@]}"; do
        if [[ "${host}" != "nervix-${state_owner##*-}" ]]; then
            lifetimes_survivor="${host}"
            break
        fi
    done
    local owner_host="nervix-${state_owner##*-}"
    case "${fault}" in
        none)
            phase 'lifetimes: no fault'
            node_fault_begin lifetimes_before_fault
            ;;
        owner-crash)
            phase "lifetimes: SIGKILL of state owner ${owner_host}"
            node_fault_kill "${case_dir}" "${owner_host}" "${fault_seconds}" lifetimes_before_fault lifetimes_failover
            ;;
        owner-partition)
            phase "lifetimes: isolation of state owner ${owner_host}"
            node_fault_isolate "${case_dir}" "${owner_host}" "${fault_seconds}" lifetimes_before_fault lifetimes_failover
            ;;
        cluster-restart)
            phase "lifetimes: SIGKILL of every node (${node_count})"
            node_fault_restart_cluster "${case_dir}" "${fault_seconds}" lifetimes_before_fault
            ;;
    esac
    if [[ "${fault}" != none ]]; then
        if [[ "${node_count}" == 3 ]]; then
            wait_for 'settled cluster after the lifetimes fault' 180 cluster_settled "${case_dir}" \
                || recovery_finding "${case_dir}" 'the cluster did not settle after the lifetimes fault'
        fi
        node_fault_check_events "${case_dir}"
        wait_for 'red holds its latest version after the fault' 120 \
            lifetimes_state_is chaos_life_profiles "${node_hosts[0]}" "${case_dir}/state-after-fault" red=5 gold=1 green=1 blue=1 \
            || recovery_finding "${case_dir}" 'the materialized relay did not hold every branch at its latest version after the fault'
    fi
    lifetimes_recovered_source="$(lifetimes_source_end)"
    check_support_containers
    lifetimes_recreation "${case_dir}"
    lifetimes_pool_recovery "${case_dir}"

    lifetimes_final_boundary "${case_dir}"
    lifetimes_verify "${case_dir}"
    recovery_final_boundary
    capture_all_metrics "${case_dir}"
    jq -n \
        --arg fault "${fault}" \
        --arg state_owner "${state_owner}" \
        --arg ingest_node "${ingest_node}" \
        --argjson nodes "${node_count}" \
        --argjson fault_seconds "${fault_seconds}" \
        --argjson failover_ms "${lifetimes_failover_ms}" \
        --argjson recovered_records "${lifetimes_recovered_source}" \
        --argjson final_records "${lifetimes_final_source}" \
        --slurpfile boundaries "${case_dir}/boundaries.json" \
        --slurpfile outputs "${case_dir}/final-outputs.json" \
        '{fault: $fault, topology_nodes: $nodes, fault_seconds: (if $fault == "none" then null else $fault_seconds end),
          deployment: {state_owner: $state_owner, ingestion_node: $ingest_node},
          failover_ms: $failover_ms, recovered_records: $recovered_records, final_records: $final_records,
          boundaries: $boundaries[0], outputs: $outputs[0]}' >"${artifact_dir}/results/lifetimes-progress.json"
    local mode
    for mode in enrich generator window rotation expiring late flush pool; do
        jq --slurpfile verdict "${case_dir}/verdict-${mode}.json" --arg mode "${mode}" \
            '.verdicts[$mode] = $verdict[0]' "${artifact_dir}/results/lifetimes-progress.json" \
            >"${artifact_dir}/results/lifetimes-progress.json.next"
        mv "${artifact_dir}/results/lifetimes-progress.json.next" "${artifact_dir}/results/lifetimes-progress.json"
    done
}
