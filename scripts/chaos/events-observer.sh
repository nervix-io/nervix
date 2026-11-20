#!/usr/bin/env bash
# Sourced by a scenario whose verdict admits no error event. Owns the error-event observers: one
# container of the image under test per node, each following with the packaged CLI the server
# notices that node's sessions receive (observe-events.sh). A notice of kind runtime reports a
# failure of any node, which reaches every session, so each observer witnesses the whole cluster.
# The observers subscribe to a relay of their own quiet domain, which no record reaches and no
# scenario stops, so they print nothing but notices and outlive every change to the domains under
# test. verify-strict-delivery.sh judges what they print inside the scenario's window.

events_observer_domain=chaos_observer
events_observer_relay=chaos_observer_quiet
# Each observer must attach within this bound, in seconds.
events_observer_attach_bound=90

events_observer_name() {
    printf '%s-events-%s\n' "${project_name}" "$1"
}

# Creates the observers' quiet domain and relay and starts the domain, keeping the CLI transcripts
# in CASE_DIR.
events_observer_prepare() {
    local case_dir="$1"
    cli_command "CREATE UNPACED DOMAIN ${events_observer_domain};" \
        >"${case_dir}/observer-domain.txt" 2>&1 || return 1
    run_cli "${events_observer_domain}" "BEGIN;
CREATE SCHEMA ${events_observer_relay} (event_id STRING);
CREATE RELAY ${events_observer_relay} SCHEMA ${events_observer_relay} UNBRANCHED CAPACITY 1;
COMMIT;" >"${case_dir}/observer-relay.txt" 2>&1 || return 1
    run_cli "${events_observer_domain}" 'START;' >"${case_dir}/observer-start.txt" 2>&1
}

# True once the observer of HOST has attached its subscription.
events_observer_attached() {
    local host="$1"
    run_bounded 20 docker logs "$(events_observer_name "${host}")" 2>&1 \
        | grep -Fq 'listening for events from relay'
}

# Starts one observer per node and waits until each has attached. Writes CASE_DIR/events-window.json
# with the moment from which every observer was attached, the start of the window the verdict judges.
events_observer_start() {
    local case_dir="$1"
    local host
    for host in "${node_hosts[@]}"; do
        compose run --detach --no-deps --name "$(events_observer_name "${host}")" \
            -e "CHAOS_EVENTS_HOST=${host}" events-observer >/dev/null || return 1
    done
    for host in "${node_hosts[@]}"; do
        wait_for "error-event observer of ${host} attached" "${events_observer_attach_bound}" \
            events_observer_attached "${host}" || return 1
    done
    jq -n --argjson start_ns "$(date +%s%N)" --arg domain "${events_observer_domain}" \
        --arg relay "${events_observer_relay}" \
        --argjson hosts "$(printf '%s\n' "${node_hosts[@]}" | jq -R . | jq -s .)" \
        '{start_ns: $start_ns, domain: $domain, relay: $relay, observers: $hosts}' \
        >"${case_dir}/events-window.json"
}

# Ends the window, keeps each observer's stamped transcript as CASE_DIR/events-HOST.log and stops the
# observers. The window's end is recorded before the transcripts are read, so it lies inside them.
events_observer_collect() {
    local case_dir="$1"
    local window_tmp
    window_tmp="$(mktemp "${case_dir}/.events-window.XXXXXX")"
    jq --argjson end_ns "$(date +%s%N)" '.end_ns = $end_ns' "${case_dir}/events-window.json" >"${window_tmp}"
    mv "${window_tmp}" "${case_dir}/events-window.json"
    local host
    for host in "${node_hosts[@]}"; do
        run_bounded 30 docker logs "$(events_observer_name "${host}")" \
            >"${case_dir}/events-${host}.log" 2>&1 || return 1
    done
    for host in "${node_hosts[@]}"; do
        run_bounded 30 docker stop -t 5 "$(events_observer_name "${host}")" >/dev/null 2>&1 || true
    done
}
