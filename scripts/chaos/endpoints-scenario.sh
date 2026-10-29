#!/usr/bin/env bash
# Sourced by run-baseline.sh after the external cluster, the baseline graph and the endpoint graphs
# are ready. Drives HTTP intake under MODE ACK through every node's listener into two domains that
# share one host and path, withdraws the peer domain while the primary continues and binds it again,
# keeps a WebSocket open across the replacement of its source, composes one fault with continuing
# intake, and judges every route's output against the endpoint client's own ledger of attempts.

# shellcheck source=recovery-scenario.sh
source "${script_dir}/recovery-scenario.sh"
# shellcheck source=node-faults.sh
source "${script_dir}/node-faults.sh"

# The endpoint domains, the route they share and the entities each places. Both domains install
# fixtures/endpoints.plexus; the primary also installs fixtures/endpoints-websocket.plexus.
endpoints_primary=chaos_edge_primary
endpoints_peer=chaos_edge_peer
endpoints_host=chaos-edge.example
endpoints_http_entities=('RELAY chaos_edge_http' 'EMITTER chaos_edge_http_emitter')
endpoints_frame_entities=('RELAY chaos_edge_frames' 'EMITTER chaos_edge_frame_emitter')

# The HTTP records and their phases, by record number. The client holds after each boundary until
# the runner moves it: the peer domain is stopped after `endpoints_withdrawn_after` and started
# again after `endpoints_reshared_after`; the WebSocket phase runs while the client holds after
# `endpoints_websocket_after`; the fault begins once record `endpoints_fault_after` is accepted.
endpoints_http_records=400
endpoints_withdrawn_after=40
endpoints_reshared_after=80
endpoints_websocket_after=120
endpoints_fault_after=160
# Frames the retained WebSocket sends before its source is replaced and the new connection after it,
# with the probe between them.
endpoints_retained_frames=20
endpoints_replacement_frames=20

# External budgets, in seconds.
endpoints_phase_bound=300
endpoints_drain_bound=180
endpoints_placement_bound=120

endpoints_fail() {
    recovery_fail "$@"
}

endpoints_fault_seconds() {
    if [[ "${outage_option_set}" == true ]]; then
        printf '%s\n' "${outage_seconds}"
        return 0
    fi
    case "${fault}" in
        *-partition) printf '45\n' ;;
        *) printf '%s\n' "${outage_seconds}" ;;
    esac
}

# Runs one Plexus command text against DOMAIN.
endpoints_cli() {
    local domain="$1"
    local command_text="$2"
    run_cli "${domain}" "${command_text}"
}

# The HTTP listener of every node, as the endpoint client addresses them.
endpoints_targets() {
    local targets=()
    local host
    for host in "${node_hosts[@]}"; do
        targets+=("${host}:8080")
    done
    local IFS=,
    printf '%s\n' "${targets[*]}"
}

# Prints one JSON object naming the owner of every placed endpoint entity of DOMAIN, read through
# HOST, into OUTPUT_DIR.json.
endpoints_placement() {
    local domain="$1"
    local host="$2"
    local output_dir="$3"
    shift 3
    local entities=("$@")
    local pairs=()
    local entity
    for entity in "${entities[@]}"; do
        pairs+=("describe-${entity// /_}" "DESCRIBE ${entity};")
    done
    admin_cli_batch "${host}" "${domain}" "${output_dir}" 90 "${pairs[@]}" || return 1
    local placements_file
    placements_file="$(mktemp "${output_dir}/.placement.XXXXXX")"
    for entity in "${entities[@]}"; do
        local label="describe-${entity// /_}"
        batch_read_succeeded "${output_dir}" "${label}" || { rm -f "${placements_file}"; return 1; }
        jq -n --arg entity "${entity}" \
            --arg owner "$(awk -F ': ' '$1 == "owner" { print $2; exit }' "${output_dir}/${label}.txt")" \
            '{key: $entity, value: {owner: $owner}}' >>"${placements_file}"
    done
    jq -s 'from_entries' "${placements_file}" >"${output_dir}.json"
    rm -f "${placements_file}"
}

# True when every placed endpoint entity of both domains is owned by OWNER.
endpoints_placed_on() {
    local owner="$1"
    local output_dir="$2"
    endpoints_placement "${endpoints_primary}" "${node_hosts[0]}" "${output_dir}-primary" \
        "${endpoints_http_entities[@]}" "${endpoints_frame_entities[@]}" || return 1
    endpoints_placement "${endpoints_peer}" "${node_hosts[0]}" "${output_dir}-peer" \
        "${endpoints_http_entities[@]}" || return 1
    jq -e -s --arg owner "${owner}" 'map(to_entries[]) | all(.[]; .value.owner == $owner)' \
        "${output_dir}-primary.json" "${output_dir}-peer.json" >/dev/null
}

# True when, read through HOST, every placed endpoint entity of both domains is owned by a node
# other than AWAY.
endpoints_moved_off() {
    local away="$1"
    local host="$2"
    local output_dir="$3"
    endpoints_placement "${endpoints_primary}" "${host}" "${output_dir}-primary" \
        "${endpoints_http_entities[@]}" "${endpoints_frame_entities[@]}" || return 1
    endpoints_placement "${endpoints_peer}" "${host}" "${output_dir}-peer" \
        "${endpoints_http_entities[@]}" || return 1
    jq -e -s --arg away "${away}" '
        map(to_entries[]) | all(.[]; (.value.owner | test("^node-[0-9]+$")) and .value.owner != $away)
    ' "${output_dir}-primary.json" "${output_dir}-peer.json" >/dev/null
}

# The ledgers the endpoint clients write to their logs, as the runner last read them.
endpoints_http_ledger() {
    printf '%s\n' "${artifact_dir}/endpoints/http-attempts.ndjson"
}

endpoints_frames_ledger() {
    printf '%s\n' "${artifact_dir}/endpoints/frames.ndjson"
}

# Reads the ledger the client SERVICE has written to standard output so far into FILE, replacing
# it in one rename. The client writes its diagnostics to standard error, which this discards.
endpoints_read_ledger() {
    local service="$1"
    local file="$2"
    local container_id
    container_id="$(endpoints_client_container "${service}")" || return 1
    run_bounded 20 docker logs "${container_id}" 2>/dev/null >"${file}.next" || return 1
    mv "${file}.next" "${file}"
}

# True once the client finished every attempt of record NUMBER: it was answered with HTTP 200 or
# abandoned at its deadline. The client sends the next record only after that.
endpoints_record_done() {
    local number="$1"
    local ledger
    ledger="$(endpoints_http_ledger)"
    endpoints_read_ledger endpoint-load "${ledger}" || return 1
    [[ -s "${ledger}" ]] || return 1
    jq -e -s --argjson number "${number}" '
        any(.[]; (.kind == "http_attempt" and .sequence == $number and .status == 200)
                 or (.kind == "http_abandoned" and .sequence == $number))
    ' "${ledger}" >/dev/null
}

# Holds the client after record NUMBER. The hold file is replaced in one rename, so the client
# never reads a partly written count.
endpoints_hold_after() {
    local number="$1"
    local hold="${artifact_dir}/endpoints/control/hold-endpoint"
    printf '%s\n' "${number}" >"${hold}.next"
    mv "${hold}.next" "${hold}"
}

endpoints_release() {
    rm -f "${artifact_dir}/endpoints/control/hold-endpoint"
}

# Waits until the client finished record NUMBER, which the hold already names, and requires that it
# sent nothing past it. The client checks its hold before each record, and the hold is in place
# before the client reaches the record, so it waits there.
endpoints_reach() {
    local number="$1"
    local description="$2"
    wait_for "${description}: record ${number} answered" "${endpoints_phase_bound}" \
        endpoints_record_done "${number}" \
        || endpoints_fail product "the endpoint never answered record ${number} with HTTP 200 (${description})"
    jq -e -s --argjson number "${number}" '
        all(.[] | select(.kind == "http_attempt"); .sequence <= $number)
    ' "$(endpoints_http_ledger)" >/dev/null \
        || endpoints_fail controller "the endpoint client sent a record past its hold at ${number}"
}

# Starts or stops DOMAIN and records the answer in FILE.
endpoints_domain_command() {
    local domain="$1"
    local statement="$2"
    local file="$3"
    endpoints_cli "${domain}" "${statement}" >"${file}" 2>&1 \
        || endpoints_fail product "${statement} on ${domain} was not acknowledged; see ${file#"${artifact_dir}/"}"
}

endpoints_client_container() {
    owned_service_container "$1"
}

# True when the client container SERVICE has exited with status zero.
endpoints_client_exited() {
    local service="$1"
    local container_id
    container_id="$(endpoints_client_container "${service}")" || return 1
    load_exited_cleanly "${container_id}"
}

# True when the WebSocket ledger records KIND.
endpoints_frames_recorded() {
    local kind="$1"
    local ledger
    ledger="$(endpoints_frames_ledger)"
    endpoints_read_ledger endpoint-websocket "${ledger}" || return 1
    [[ -s "${ledger}" ]] && jq -e -s --arg kind "${kind}" 'any(.[]; .kind == $kind)' "${ledger}" >/dev/null
}

# True when every topic of the JSON object FILE holds at least its count, read now.
endpoints_outputs_cover() {
    local file="$1"
    local topic expected current
    while IFS=$'\t' read -r topic expected; do
        current="$(topic_end_offset "${topic}")" || return 1
        [[ "${current}" =~ ^[0-9]+$ ]] && ((current >= expected)) || return 1
    done < <(jq -r 'to_entries[] | [.key, (.value | tostring)] | @tsv' "${file}")
}

# Prints the end offset of every endpoint output topic as one JSON object.
endpoints_output_offsets() {
    local topic offset
    local pairs=()
    for topic in chaos_edge_primary_http chaos_edge_peer_http chaos_edge_frames; do
        offset="$(topic_end_offset "${topic}")" || return 1
        [[ "${offset}" =~ ^[0-9]+$ ]] || return 1
        pairs+=("$(jq -nc --arg topic "${topic}" --argjson offset "${offset}" '{key:$topic,value:$offset}')")
    done
    printf '%s\n' "${pairs[@]}" | jq -s 'from_entries'
}

# True when the endpoint output offsets stayed equal across two reads two seconds apart, which it
# then writes to FILE.
endpoints_outputs_stable() {
    local file="$1"
    local first second
    first="$(endpoints_output_offsets)" || return 1
    sleep 2
    second="$(endpoints_output_offsets)" || return 1
    [[ "${first}" == "${second}" ]] || return 1
    printf '%s\n' "${second}" >"${file}"
}

# Requires the endpoint work to leave the faulted owner, read through a survivor, while the fault
# holds. Runs as the faults' DURING hook.
endpoints_failover() {
    local started_ms
    started_ms="$(epoch_ms)"
    wait_for "endpoint work off ${endpoints_target_node}" 120 \
        endpoints_moved_off "${endpoints_target_node}" "${endpoints_survivor}" "${endpoints_case_dir}/placement-during" \
        || recovery_finding "${endpoints_case_dir}" "the endpoint work stayed on ${endpoints_target_node} during its fault"
    endpoints_failover_ms=$(($(epoch_ms) - started_ms))
}

# Records the moment the fault starts and the last record accepted before it. Runs as the faults'
# BEFORE hook.
endpoints_before_fault() {
    endpoints_fault_started_at="$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
}

endpoints_start_clients_http() {
    rm -f "${artifact_dir}/endpoints/control/stop-endpoint"
    jq -nc --arg run_id "${run_id}" --argjson count "${endpoints_http_records}" --arg kind http \
        -f "${script_dir}/fixtures/generate-endpoints.jq" >"${artifact_dir}/endpoints/http-input.ndjson"
    [[ "$(wc -l <"${artifact_dir}/endpoints/http-input.ndjson")" -eq "${endpoints_http_records}" ]] \
        || endpoints_fail controller 'the endpoint fixture generation was incomplete'
    CHAOS_ENDPOINT_TARGETS="$(endpoints_targets)" compose up --detach --no-deps endpoint-load
}

endpoints_start_clients_websocket() {
    local target="$1"
    jq -nc --arg run_id "${run_id}" \
        --argjson count "$((endpoints_retained_frames + 1 + endpoints_replacement_frames))" --arg kind frame \
        -f "${script_dir}/fixtures/generate-endpoints.jq" >"${artifact_dir}/endpoints/frame-input.ndjson"
    CHAOS_ENDPOINT_WEBSOCKET_TARGET="${target}" compose up --detach --no-deps endpoint-websocket
}

# Explains why the WebSocket client never sent its retained frames: an upgrade the endpoint refused
# or answered with the wrong accept key is a product failure, and a client that recorded no opening
# at all is a controller failure.
endpoints_websocket_unopened() {
    local ledger
    ledger="$(endpoints_frames_ledger)"
    endpoints_read_ledger endpoint-websocket "${ledger}" || true
    local websocket_id
    if websocket_id="$(endpoints_client_container endpoint-websocket)"; then
        run_bounded 20 docker logs "${websocket_id}" 1>/dev/null \
            2>"${artifact_dir}/endpoints/endpoint-websocket.stderr" || true
    fi
    local opening=""
    if [[ -s "${ledger}" ]]; then
        opening="$(jq -c -s 'map(select(.kind == "ws_open" and .connection == "retained")) | first // empty' "${ledger}")"
    fi
    if [[ -n "${opening}" ]]; then
        endpoints_fail product "the endpoint did not accept the retained WebSocket upgrade: ${opening}"
    fi
    endpoints_fail controller 'the WebSocket client recorded no opening; see endpoints/endpoint-websocket.stderr'
}

# Waits until the HTTP client finished its input and the outputs hold every record it accepted,
# then reads each output into the traffic directory.
endpoints_final_boundary() {
    local case_dir="$1"
    phase 'endpoints: final intake boundary'
    endpoints_release
    wait_for 'endpoint client reached the end of its input' "${endpoints_phase_bound}" \
        endpoints_client_exited endpoint-load \
        || endpoints_fail product 'the endpoint client did not finish its input'
    endpoints_read_ledger endpoint-load "$(endpoints_http_ledger)" \
        || endpoints_fail controller 'the endpoint client ledger could not be read from its log'
    local load_id
    load_id="$(endpoints_client_container endpoint-load)" || return 1
    run_bounded 20 docker logs "${load_id}" 1>/dev/null 2>"${artifact_dir}/endpoints/endpoint-load.stderr"
    jq -s '
        [.[] | select(.kind == "http_attempt" and .status == 200) | .sequence] | unique | length
    ' "$(endpoints_http_ledger)" >"${case_dir}/accepted-count.txt"
    local accepted withdrawn
    accepted="$(<"${case_dir}/accepted-count.txt")"
    withdrawn=$((endpoints_reshared_after - endpoints_withdrawn_after))
    jq -n --argjson primary "${accepted}" --argjson peer "$((accepted - withdrawn))" \
        --argjson frames "$((endpoints_retained_frames + endpoints_replacement_frames))" \
        '{chaos_edge_primary_http:$primary,chaos_edge_peer_http:$peer,chaos_edge_frames:$frames}' \
        >"${case_dir}/expected-minimum.json"
    wait_for 'endpoint outputs hold every accepted record' "${endpoints_drain_bound}" \
        endpoints_outputs_cover "${case_dir}/expected-minimum.json" \
        || recovery_finding "${case_dir}" 'the endpoint outputs did not reach the accepted records'
    wait_for 'endpoint outputs stable' 90 endpoints_outputs_stable "${case_dir}/final-outputs.json" \
        || endpoints_fail product 'endpoint outputs did not settle after intake ended'
    local topic count
    for topic in chaos_edge_primary_http chaos_edge_peer_http chaos_edge_frames; do
        count="$(jq -r --arg topic "${topic}" '.[$topic]' "${case_dir}/final-outputs.json")"
        if ((count > 0)); then
            kcat -q -b broker:9092 -C -t "${topic}" -p 0 -o beginning -c "${count}" \
                >"${artifact_dir}/traffic/${topic}.ndjson" 2>"${case_dir}/${topic}.stderr"
        else
            : >"${artifact_dir}/traffic/${topic}.ndjson"
        fi
        [[ "$(wc -l <"${artifact_dir}/traffic/${topic}.ndjson")" -eq "${count}" ]] \
            || endpoints_fail product "the ${topic} ledger did not cover its final boundary"
    done
}

# Runs every endpoint verdict and records each failure as a product finding.
endpoints_verify() {
    local case_dir="$1"
    phase 'endpoints: route verdicts'
    jq -n --argjson withdrawn "${endpoints_withdrawn_after}" --argjson reshared "${endpoints_reshared_after}" \
        '{withdrawn_after:$withdrawn,reshared_after:$reshared}' >"${case_dir}/phases.json"
    local verdict status
    for verdict in primary peer; do
        status=0
        "${script_dir}/verify-endpoint-evidence.sh" http --domain "${verdict}" \
            --attempts "$(endpoints_http_ledger)" \
            --input "${artifact_dir}/endpoints/http-input.ndjson" \
            --output "${artifact_dir}/traffic/chaos_edge_${verdict}_http.ndjson" \
            --phases "${case_dir}/phases.json" \
            --result "${case_dir}/verdict-http-${verdict}.json" \
            >"${case_dir}/verdict-http-${verdict}.txt" 2>&1 || status=$?
        if ((status == 1)); then
            recovery_finding "${case_dir}" "$(jq -r '.processor + ": " + (.failures | join("; "))' "${case_dir}/verdict-http-${verdict}.json")"
        elif ((status != 0)); then
            endpoints_fail controller "the http ${verdict} verifier could not judge its evidence; see endpoints/verdict-http-${verdict}.txt"
        fi
    done
    status=0
    "${script_dir}/verify-endpoint-evidence.sh" websocket \
        --frames "$(endpoints_frames_ledger)" \
        --input "${artifact_dir}/endpoints/frame-input.ndjson" \
        --output "${artifact_dir}/traffic/chaos_edge_frames.ndjson" \
        --result "${case_dir}/verdict-websocket.json" \
        >"${case_dir}/verdict-websocket.txt" 2>&1 || status=$?
    if ((status == 1)); then
        recovery_finding "${case_dir}" "$(jq -r '.processor + ": " + (.failures | join("; "))' "${case_dir}/verdict-websocket.json")"
    elif ((status != 0)); then
        endpoints_fail controller 'the websocket verifier could not judge its evidence; see endpoints/verdict-websocket.txt'
    fi
}

run_endpoints() {
    local case_dir="${artifact_dir}/endpoints"
    mkdir -p "${case_dir}/control"
    recovery_traffic_startup "${case_dir}"
    endpoints_failover_ms=null
    endpoints_fault_started_at=""

    failure_category=product
    phase 'endpoints: placement'
    local owner="${node_names[0]}"
    local websocket_host="${node_hosts[0]}"
    if [[ "${node_count}" == 3 ]]; then
        wait_for 'settled cluster before endpoint placement' 120 cluster_settled "${case_dir}"
        local leader
        leader="$(<"${case_dir}/leader.txt")"
        local node
        for node in node-3 node-2 node-1; do
            if [[ "${node}" != "${leader}" ]]; then
                owner="${node}"
                break
            fi
        done
        local selection
        selection="$(printf '%s, ' "${endpoints_http_entities[@]}" "${endpoints_frame_entities[@]}")"
        endpoints_cli "${endpoints_primary}" "RELOCATE ${selection%, } ONTO NODE ${owner} IGNORE PREFERENCES;" \
            >"${case_dir}/relocate-primary.txt" 2>&1
        selection="$(printf '%s, ' "${endpoints_http_entities[@]}")"
        endpoints_cli "${endpoints_peer}" "RELOCATE ${selection%, } ONTO NODE ${owner} IGNORE PREFERENCES;" \
            >"${case_dir}/relocate-peer.txt" 2>&1
        # The WebSocket connects through another node, so its frames cross the interconnect.
        local host
        for host in "${node_hosts[@]}"; do
            if [[ "${host}" != "nervix-${owner##*-}" ]]; then
                websocket_host="${host}"
                break
            fi
        done
    fi
    wait_for "endpoint work on ${owner}" "${endpoints_placement_bound}" \
        endpoints_placed_on "${owner}" "${case_dir}/placement-before"

    phase 'endpoints: shared route'
    endpoints_hold_after "${endpoints_withdrawn_after}"
    endpoints_start_clients_http
    endpoints_reach "${endpoints_withdrawn_after}" 'shared route'

    phase "endpoints: ${endpoints_peer} withdrawn"
    endpoints_domain_command "${endpoints_peer}" 'STOP;' "${case_dir}/stop-peer.txt"
    endpoints_hold_after "${endpoints_reshared_after}"
    endpoints_reach "${endpoints_reshared_after}" 'withdrawn peer route'

    phase "endpoints: ${endpoints_peer} bound again"
    endpoints_domain_command "${endpoints_peer}" 'START;' "${case_dir}/start-peer.txt"
    endpoints_hold_after "${endpoints_websocket_after}"
    endpoints_reach "${endpoints_websocket_after}" 'route shared again'

    phase "endpoints: WebSocket across the replacement of its source on ${websocket_host}"
    endpoints_start_clients_websocket "${websocket_host}:8080"
    if ! wait_for 'retained WebSocket frames sent' 120 endpoints_frames_recorded ws_retained_sent; then
        endpoints_websocket_unopened
    fi
    endpoints_domain_command "${endpoints_primary}" 'STOP;' "${case_dir}/stop-primary.txt"
    endpoints_domain_command "${endpoints_primary}" 'START;' "${case_dir}/start-primary.txt"
    wait_for "endpoint work on ${owner} after the source replacement" "${endpoints_placement_bound}" \
        endpoints_placed_on "${owner}" "${case_dir}/placement-replaced"
    touch "${case_dir}/control/websocket-source-replaced"
    wait_for 'replacement WebSocket frames sent' 180 endpoints_client_exited endpoint-websocket \
        || endpoints_fail product 'the WebSocket client did not finish after the source replacement'
    endpoints_read_ledger endpoint-websocket "$(endpoints_frames_ledger)" \
        || endpoints_fail controller 'the WebSocket client ledger could not be read from its log'
    local websocket_id
    websocket_id="$(endpoints_client_container endpoint-websocket)" || return 1
    run_bounded 20 docker logs "${websocket_id}" 1>/dev/null 2>"${case_dir}/endpoint-websocket.stderr"

    phase 'endpoints: intake through the fault'
    endpoints_hold_after "${endpoints_fault_after}"
    endpoints_reach "${endpoints_fault_after}" 'intake before the fault'
    endpoints_release
    local fault_seconds
    fault_seconds="$(endpoints_fault_seconds)"
    endpoints_case_dir="${case_dir}"
    endpoints_target_node="${owner}"
    endpoints_survivor="${node_hosts[0]}"
    local owner_host="nervix-${owner##*-}"
    for host in "${node_hosts[@]}"; do
        if [[ "${host}" != "${owner_host}" ]]; then
            endpoints_survivor="${host}"
            break
        fi
    done
    case "${fault}" in
        none)
            phase 'endpoints: no fault'
            node_fault_begin endpoints_before_fault
            ;;
        owner-crash)
            phase "endpoints: SIGKILL of endpoint owner ${owner_host}"
            node_fault_kill "${case_dir}" "${owner_host}" "${fault_seconds}" endpoints_before_fault endpoints_failover
            ;;
        owner-partition)
            phase "endpoints: isolation of endpoint owner ${owner_host}"
            node_fault_isolate "${case_dir}" "${owner_host}" "${fault_seconds}" endpoints_before_fault endpoints_failover
            ;;
        cluster-restart)
            phase "endpoints: SIGKILL of every node (${node_count})"
            node_fault_restart_cluster "${case_dir}" "${fault_seconds}" endpoints_before_fault
            ;;
    esac
    if [[ "${fault}" != none ]]; then
        if [[ "${node_count}" == 3 ]]; then
            wait_for 'settled cluster after the endpoint fault' 180 cluster_settled "${case_dir}" \
                || recovery_finding "${case_dir}" 'the cluster did not settle after the endpoint fault'
        fi
        node_fault_check_events "${case_dir}"
    fi
    check_support_containers

    endpoints_final_boundary "${case_dir}"
    endpoints_verify "${case_dir}"
    recovery_final_boundary
    capture_all_metrics "${case_dir}"
    jq -n \
        --arg fault "${fault}" \
        --arg owner "${owner}" \
        --arg websocket_host "${websocket_host}" \
        --argjson nodes "${node_count}" \
        --argjson fault_seconds "${fault_seconds}" \
        --arg fault_started_at "${endpoints_fault_started_at}" \
        --argjson failover_ms "${endpoints_failover_ms}" \
        --argjson records "${endpoints_http_records}" \
        --argjson withdrawn_after "${endpoints_withdrawn_after}" \
        --argjson reshared_after "${endpoints_reshared_after}" \
        --argjson websocket_after "${endpoints_websocket_after}" \
        --argjson fault_after "${endpoints_fault_after}" \
        --slurpfile primary "${case_dir}/verdict-http-primary.json" \
        --slurpfile peer "${case_dir}/verdict-http-peer.json" \
        --slurpfile websocket "${case_dir}/verdict-websocket.json" \
        --slurpfile outputs "${case_dir}/final-outputs.json" \
        '{fault: $fault, topology_nodes: $nodes, fault_seconds: (if $fault == "none" then null else $fault_seconds end),
          placement: {owner: $owner, websocket_connection: $websocket_host},
          domains: {primary: "chaos_edge_primary", peer: "chaos_edge_peer"},
          route: {host: "chaos-edge.example", http_path: "/events", websocket_path: "/stream"},
          phases: {http_records: $records, withdrawn_after: $withdrawn_after, reshared_after: $reshared_after,
                   websocket_after: $websocket_after, fault_after: $fault_after},
          fault_started_at: (if $fault_started_at == "" then null else $fault_started_at end),
          failover_ms: $failover_ms,
          outputs: $outputs[0],
          verdicts: {http_primary: $primary[0], http_peer: $peer[0], websocket: $websocket[0]}}' \
        >"${artifact_dir}/results/endpoints-progress.json"
}
