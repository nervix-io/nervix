#!/usr/bin/env bash
set -eEuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
compose_file="${script_dir}/compose.yaml"
fixture_file="${script_dir}/fixtures/baseline.nspl"
fixture_generator="${script_dir}/fixtures/generate-baseline.jq"
# shellcheck source=tool-images.sh
source "${script_dir}/tool-images.sh"
# shellcheck source=docker-event-recording.sh
source "${script_dir}/docker-event-recording.sh"

usage() {
    cat <<EOF
usage: just chaos run ${scenario} --image IMAGE [options]

Required:
  --image IMAGE          Already-built Nervix image reference or local image ID.

Options:
  --nodes 1|3            Cluster topology (default: 3).
  --records N            Finite fixture size, 1..1000 (default: ${record_count}).
  --artifacts DIR        Artifact root (default: target/chaos).
  --run-id ID            Stable run identifier; generated when omitted.
  --timeout SECONDS      Whole-run bound, 120..3600 (default: ${overall_timeout}).
  --outage-seconds N     Minimum held crash outage, 5..120 (default: ${outage_seconds}).
  pause-resume uses configured 10s/12s Raft election and 15s node detection
  thresholds, with 1s short and 75s failover-length pauses.
  --case CASE            partition-recovery case: all, follower, asymmetric, leader,
                         or quorum-loss (default: ${partition_case}).
  --partition-seconds N  Minimum verified partition window, 20..600 (default: ${partition_seconds}).
  --profile NAME         degraded-links profile: all, delay, jitter, random-loss,
                         burst-loss, rate-limit, combined (default: ${degradation_profile}).
  --load-interval-ms N   Fixed producer interval, 100..5000 (default: ${load_interval_ms}).
  --baseline-seconds N   Independent healthy measurement window, 10..120 (default: ${baseline_seconds}).
  --degrade-seconds N    Hold each fault for 10..120 seconds (default: ${degrade_seconds}).
  --drain-seconds N      Per-profile recovery deadline, 10..180 (default: ${drain_seconds}).
  --max-backlog N        Maximum accepted-minus-emitted records (default: ${max_backlog}).
  --max-recovery-backlog N  Maximum backlog after each heal (default: ${max_recovery_backlog}).
  --max-memory-bytes N   Maximum Docker memory per node (default: ${max_memory_bytes}).
  --max-pending N        Maximum public interconnect pending operations (default: ${max_pending}).
  --min-throughput-pct N Minimum recovered output rate as percent of healthy baseline
                         for three consecutive intervals (default: ${min_throughput_pct}).
  --keep                 Retain labeled Docker resources after diagnostics.
  -h, --help             Show this help.
EOF
}

setup_error() {
    failure_category=setup
    printf 'chaos setup error: %s\n' "$*" >&2
    exit 2
}

image_ref=""
scenario="baseline"
node_count=3
record_count=24
artifact_root="target/chaos"
run_id=""
overall_timeout=900
outage_seconds=8
outage_option_set=false
partition_case=all
partition_seconds=45
partition_option_set=false
degradation_profile=all
degradation_option_set=false
load_interval_ms=750
baseline_seconds=15
degrade_seconds=20
drain_seconds=90
max_backlog=200
max_recovery_backlog=20
max_memory_bytes=1073741824
max_pending=128
min_throughput_pct=50
keep_resources=false

while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --image)
            [[ "$#" -ge 2 ]] || setup_error '--image requires a value'
            image_ref="$2"
            shift 2
            ;;
        --scenario)
            [[ "$#" -ge 2 ]] || setup_error '--scenario requires a value'
            scenario="$2"
            if [[ "${scenario}" == degraded-links ]]; then
                record_count=1000
                overall_timeout=2400
            fi
            shift 2
            ;;
        --nodes)
            [[ "$#" -ge 2 ]] || setup_error '--nodes requires 1 or 3'
            node_count="$2"
            shift 2
            ;;
        --records)
            [[ "$#" -ge 2 ]] || setup_error '--records requires a value'
            record_count="$2"
            shift 2
            ;;
        --artifacts)
            [[ "$#" -ge 2 ]] || setup_error '--artifacts requires a directory'
            artifact_root="$2"
            shift 2
            ;;
        --run-id)
            [[ "$#" -ge 2 ]] || setup_error '--run-id requires a value'
            run_id="$2"
            shift 2
            ;;
        --timeout)
            [[ "$#" -ge 2 ]] || setup_error '--timeout requires seconds'
            overall_timeout="$2"
            shift 2
            ;;
        --outage-seconds)
            [[ "$#" -ge 2 ]] || setup_error '--outage-seconds requires a value'
            outage_seconds="$2"
            outage_option_set=true
            shift 2
            ;;
        --case)
            [[ "$#" -ge 2 ]] || setup_error '--case requires a value'
            partition_case="$2"
            partition_option_set=true
            shift 2
            ;;
        --partition-seconds)
            [[ "$#" -ge 2 ]] || setup_error '--partition-seconds requires a value'
            partition_seconds="$2"
            partition_option_set=true
            shift 2
            ;;
        --profile | --load-interval-ms | --baseline-seconds | --degrade-seconds | --drain-seconds | --max-backlog | --max-recovery-backlog | --max-memory-bytes | --max-pending | --min-throughput-pct)
            [[ "$#" -ge 2 ]] || setup_error "$1 requires a value"
            case "$1" in
                --profile) degradation_profile="$2" ;;
                --load-interval-ms) load_interval_ms="$2" ;;
                --baseline-seconds) baseline_seconds="$2" ;;
                --degrade-seconds) degrade_seconds="$2" ;;
                --drain-seconds) drain_seconds="$2" ;;
                --max-backlog) max_backlog="$2" ;;
                --max-recovery-backlog) max_recovery_backlog="$2" ;;
                --max-memory-bytes) max_memory_bytes="$2" ;;
                --max-pending) max_pending="$2" ;;
                --min-throughput-pct) min_throughput_pct="$2" ;;
            esac
            degradation_option_set=true
            shift 2
            ;;
        --keep)
            keep_resources=true
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            setup_error "unknown ${scenario} argument: $1"
            ;;
    esac
done

[[ -n "${image_ref}" ]] || setup_error '--image is required and must name an already-built Nervix image'
case "${scenario}" in
    baseline | rolling-restart | leader-crash | follower-crash | ingestor-owner-crash | emitter-owner-crash | pause-resume | partition-recovery | degraded-links) ;;
    *) setup_error "unknown scenario: ${scenario}" ;;
esac
[[ "${node_count}" == "1" || "${node_count}" == "3" ]] \
    || setup_error '--nodes must be 1 or 3'
if [[ "${scenario}" != baseline && "${scenario}" != rolling-restart && "${scenario}" != leader-crash && "${node_count}" != 3 ]]; then
    setup_error "${scenario} requires --nodes 3"
fi
if [[ "${scenario}" == pause-resume && "${outage_option_set}" == true ]]; then
    setup_error 'pause-resume selects its own finite durations; --outage-seconds is for crash scenarios'
fi
if [[ "${scenario}" == partition-recovery && "${outage_option_set}" == true ]]; then
    setup_error 'partition-recovery holds partitions, not outages; --outage-seconds is for crash scenarios'
fi
if [[ "${scenario}" != partition-recovery && "${partition_option_set}" == true ]]; then
    setup_error '--case and --partition-seconds apply only to partition-recovery'
fi
if [[ "${scenario}" != degraded-links && "${degradation_option_set}" == true ]]; then
    setup_error 'degradation profiles and thresholds apply only to degraded-links'
fi
case "${degradation_profile}" in
    all | delay | jitter | random-loss | burst-loss | rate-limit | combined) ;;
    *) setup_error "unknown degradation profile: ${degradation_profile}" ;;
esac
for numeric_setting in load_interval_ms baseline_seconds degrade_seconds drain_seconds max_backlog max_recovery_backlog max_memory_bytes max_pending min_throughput_pct; do
    [[ "${!numeric_setting}" =~ ^[0-9]+$ ]] || setup_error "${numeric_setting} must be an integer"
done
((load_interval_ms >= 100 && load_interval_ms <= 5000)) || setup_error '--load-interval-ms must be 100..5000'
((baseline_seconds >= 10 && baseline_seconds <= 120)) || setup_error '--baseline-seconds must be 10..120'
((degrade_seconds >= 10 && degrade_seconds <= 120)) || setup_error '--degrade-seconds must be 10..120'
((drain_seconds >= 10 && drain_seconds <= 180)) || setup_error '--drain-seconds must be 10..180'
((max_backlog >= 1 && max_backlog <= 1000)) || setup_error '--max-backlog must be 1..1000'
((max_recovery_backlog >= 0 && max_recovery_backlog <= max_backlog)) || setup_error '--max-recovery-backlog must be 0..max-backlog'
((max_memory_bytes >= 1048576)) || setup_error '--max-memory-bytes must be at least 1048576'
((max_pending >= 1)) || setup_error '--max-pending must be positive'
((min_throughput_pct >= 1 && min_throughput_pct <= 100)) || setup_error '--min-throughput-pct must be 1..100'
case "${partition_case}" in
    all | follower | asymmetric | leader | quorum-loss) ;;
    *) setup_error "--case must be all, follower, asymmetric, leader, or quorum-loss, not ${partition_case}" ;;
esac
[[ "${partition_seconds}" =~ ^[0-9]+$ ]] \
    || setup_error '--partition-seconds must be an integer from 20 through 600'
((partition_seconds >= 20 && partition_seconds <= 600)) \
    || setup_error '--partition-seconds must be an integer from 20 through 600'
[[ "${record_count}" =~ ^[0-9]+$ ]] \
    || setup_error '--records must be an integer from 1 through 1000'
((record_count >= 1 && record_count <= 1000)) \
    || setup_error '--records must be an integer from 1 through 1000'
[[ "${overall_timeout}" =~ ^[0-9]+$ ]] \
    || setup_error '--timeout must be an integer from 120 through 3600'
((overall_timeout >= 120 && overall_timeout <= 3600)) \
    || setup_error '--timeout must be an integer from 120 through 3600'
[[ "${outage_seconds}" =~ ^[0-9]+$ ]] \
    || setup_error '--outage-seconds must be an integer from 5 through 120'
((outage_seconds >= 5 && outage_seconds <= 120)) \
    || setup_error '--outage-seconds must be an integer from 5 through 120'

if [[ -z "${run_id}" ]]; then
    run_id="run-$(date -u +%Y%m%dt%H%M%Sz)-$$-${RANDOM}"
fi
[[ "${run_id}" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,95}$ ]] \
    || setup_error '--run-id must be 1..96 letters, numbers, dots, underscores, or hyphens'

for command_name in docker jq openssl timeout awk sed grep sort wc date; do
    command -v "${command_name}" >/dev/null 2>&1 \
        || setup_error "required command is unavailable: ${command_name}"
done

mkdir -p "${artifact_root}"
artifact_root="$(cd "${artifact_root}" && pwd)"
artifact_dir="${artifact_root}/${run_id}"
if [[ -e "${artifact_dir}" ]]; then
    setup_error "artifact directory already exists: ${artifact_dir}"
fi
mkdir -p \
    "${artifact_dir}/diagnostics" \
    "${artifact_dir}/fixtures" \
    "${artifact_dir}/public" \
    "${artifact_dir}/results" \
    "${artifact_dir}/tls" \
    "${artifact_dir}/traffic"
chmod 0700 "${artifact_dir}" "${artifact_dir}/tls"

project_suffix="$(tr '[:upper:]_.' '[:lower:]--' <<<"${run_id}" | sed 's/[^a-z0-9-]/-/g; s/--*/-/g; s/^-//; s/-$//')"
project_name="nervix-chaos-${project_suffix}"
cluster_id="chaos-${project_suffix}"
password="nervix-chaos-${RANDOM}-${RANDOM}"
current_phase="preflight"
overall_deadline=$((SECONDS + overall_timeout))
compose_ready=false
image_id=""
image_digest=""
signal_name=""
failure_category="controller"
# The live recording is the run's complete Docker event evidence and is never trimmed; a recording
# that outgrows this bound fails the run instead.
docker_event_bytes_limit=67108864
run_event_recording_covered=false

export CHAOS_RUN_ID="${run_id}"
export CHAOS_CLUSTER_ID="${cluster_id}"
export CHAOS_TLS_DIR="${artifact_dir}/tls"
export CHAOS_PASSWORD="${password}"
export CHAOS_KAFKA_IMAGE="apache/kafka:3.9.1"
export CHAOS_KCAT_IMAGE="edenhill/kcat:1.7.1"
export CHAOS_PROBE_IMAGE="alpine:3.22"
export CHAOS_PUMBA_IMAGE="${chaos_pumba_image}"
export CHAOS_NETTOOLS_IMAGE="${chaos_nettools_image}"
export CHAOS_LOAD_FILE="${artifact_dir}/fixtures/input.ndjson"
export CHAOS_TRAFFIC_DIR="${artifact_dir}/traffic"
export CHAOS_SCRIPT_DIR="${script_dir}"
export CHAOS_NODE_COUNT="${node_count}"
if [[ "${scenario}" == pause-resume ]]; then
    export CHAOS_RAFT_HEARTBEAT_INTERVAL=250ms
    export CHAOS_RAFT_ELECTION_TIMEOUT_MIN=10s
    export CHAOS_RAFT_ELECTION_TIMEOUT_MAX=12s
    export CHAOS_NODE_UNAVAILABILITY_TIMEOUT=15s
    export CHAOS_LOAD_INTERVAL=1.0
fi
if [[ "${scenario}" == partition-recovery ]]; then
    # One record every two seconds keeps the bounded fixture flowing through all four cases.
    export CHAOS_LOAD_INTERVAL=2.0
fi
if [[ "${scenario}" == degraded-links ]]; then
    CHAOS_LOAD_INTERVAL="$(awk -v ms="${load_interval_ms}" 'BEGIN {printf "%.3f", ms / 1000}')"
    export CHAOS_LOAD_INTERVAL
fi

jq -n \
    --arg run_id "${run_id}" \
    --arg project "${project_name}" \
    --arg image "${image_ref}" \
    --arg scenario "${scenario}" \
    --arg started_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --argjson nodes "${node_count}" \
    --argjson records "${record_count}" \
    --argjson timeout_seconds "${overall_timeout}" \
    --argjson outage_seconds "${outage_seconds}" \
    --argjson docker_event_bytes "${docker_event_bytes_limit}" \
    '{
      run_id: $run_id,
      compose_project: $project,
      status: "preflight",
      started_at: $started_at,
      requested_image: $image,
      scenario: $scenario,
      topology_nodes: $nodes,
      fixture_record_limit: $records,
      timeout_seconds: $timeout_seconds,
      outage_seconds: $outage_seconds,
      artifact_limits: {
        fixture_records: 1000,
        compose_log_bytes: 2097152,
        docker_event_bytes: $docker_event_bytes,
        metrics_bytes_per_node: 1048576,
        restart_log_bytes: 2097152,
        observer_log_bytes_per_restart: 1048576,
        compose_log_files_per_container: 2,
        compose_log_bytes_per_file: 2097152
      }
    }' >"${artifact_dir}/manifest.json"

remaining_seconds() {
    local remaining=$((overall_deadline - SECONDS))
    if ((remaining < 0)); then
        remaining=0
    fi
    printf '%d\n' "${remaining}"
}

run_bounded() {
    local requested="$1"
    shift
    local remaining
    remaining="$(remaining_seconds)"
    if ((remaining == 0)); then
        printf 'overall timeout reached during phase %s\n' "${current_phase}" >&2
        return 124
    fi
    local limit="${requested}"
    if ((limit > remaining)); then
        limit="${remaining}"
    fi
    timeout --foreground --kill-after=5s "${limit}s" "$@"
}

phase() {
    current_phase="$1"
    printf '\n==> %s\n' "${current_phase}"
    jq -nc --arg phase "${current_phase}" --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        '{phase: $phase, at: $at}' >>"${artifact_dir}/phases.ndjson"
}

update_manifest() {
    local filter="$1"
    shift
    local tmp_path
    tmp_path="$(mktemp "${artifact_dir}/.manifest.XXXXXX")"
    if jq "$@" "${filter}" "${artifact_dir}/manifest.json" >"${tmp_path}"; then
        mv "${tmp_path}" "${artifact_dir}/manifest.json"
    else
        rm -f "${tmp_path}"
        return 1
    fi
}

compose_args=(--project-name "${project_name}" --file "${compose_file}" --profile tools)
if [[ "${node_count}" == "3" ]]; then
    compose_args+=(--profile three-node)
fi
if [[ "${scenario}" != "baseline" ]]; then
    compose_args+=(--profile rolling)
fi

compose() {
    run_bounded "${compose_call_timeout:-120}" docker compose "${compose_args[@]}" "$@"
}

run_cli() {
    local domain="$1"
    local command_text="$2"
    local cli_output
    cli_output="$(mktemp "${artifact_dir}/.cli.XXXXXX")"
    local cli_status=0
    local domain_args=()
    if [[ -n "${domain}" ]]; then
        domain_args=(--domain "${domain}")
    fi
    compose run --rm --no-deps admin \
        nervix-cli \
        --server "http://${cli_host}:47391" \
        "${domain_args[@]}" \
        --password "${CHAOS_PASSWORD}" \
        --command "${command_text}" >"${cli_output}" 2>&1 || cli_status=$?
    cat "${cli_output}"
    if [[ "${cli_status}" -eq 0 ]] \
        && ! grep -Fq 'Error:' "${cli_output}" \
        && ! grep -Eq '^error:' "${cli_output}"; then
        rm -f "${cli_output}"
        return 0
    fi
    rm -f "${cli_output}"
    if [[ "${cli_status}" -eq 0 ]]; then
        return 1
    fi
    return "${cli_status}"
}

cli_command() {
    local command_text="$1"
    run_cli "" "${command_text}"
}

domain_cli_command() {
    local command_text="$1"
    run_cli chaos_baseline "${command_text}"
}

broker_admin() {
    compose run --rm --no-deps broker-admin "$@"
}

kcat() {
    compose run --rm --no-deps -T kcat "$@"
}

wait_for() {
    local description="$1"
    local seconds="$2"
    shift 2
    local deadline=$((SECONDS + seconds))
    while ((SECONDS < deadline && SECONDS < overall_deadline)); do
        if "$@"; then
            printf 'ready: %s\n' "${description}"
            return 0
        fi
        sleep 1
    done
    printf 'timed out waiting for %s during phase %s\n' "${description}" "${current_phase}" >&2
    return 124
}

trim_file() {
    local path="$1"
    local max_bytes="$2"
    [[ -f "${path}" ]] || return 0
    local size
    size="$(wc -c <"${path}")"
    if ((size <= max_bytes)); then
        return 0
    fi
    local trimmed="${path}.trimmed"
    tail -c "${max_bytes}" "${path}" >"${trimmed}"
    mv "${trimmed}" "${path}"
}

capture_diagnostics() {
    set +e
    mkdir -p "${artifact_dir}/diagnostics"
    if [[ "${compose_ready}" == true ]]; then
        timeout --foreground --kill-after=5s 30s \
            docker compose "${compose_args[@]}" ps --all --format json \
            >"${artifact_dir}/diagnostics/compose-ps.json" 2>&1
        timeout --foreground --kill-after=5s 30s \
            docker compose "${compose_args[@]}" logs --no-color --timestamps --tail 2000 \
            >"${artifact_dir}/diagnostics/compose.log" 2>&1
        trim_file "${artifact_dir}/diagnostics/compose.log" 2097152
    fi

    # The recording is closed only after every heal, so it holds the run's own recovery actions.
    if docker_event_recording_finish diagnostics/docker-events.recording.json \
        "${docker_event_bytes_limit}"; then
        run_event_recording_covered=true
    fi

    mapfile -t owned_containers < <(
        docker container ls --all --quiet \
            --filter "label=io.nervix.chaos.run=${run_id}" 2>/dev/null
    )
    if ((${#owned_containers[@]} > 0)); then
        timeout --foreground --kill-after=5s 20s docker inspect "${owned_containers[@]}" \
            >"${artifact_dir}/diagnostics/containers.json" 2>&1
    else
        printf '[]\n' >"${artifact_dir}/diagnostics/containers.json"
    fi
    set -e
}

remove_private_keys() {
    rm -f \
        "${artifact_dir}/tls/ca-key.pem" \
        "${artifact_dir}/tls/"*-key.pem \
        "${artifact_dir}/tls/"*.csr \
        "${artifact_dir}/tls/"*.ext
}

finish() {
    local status=$?
    trap - EXIT INT TERM HUP
    set +e
    if [[ "${scenario}" == pause-resume ]]; then
        if [[ -n "${pause_injector_pid:-}" ]]; then
            kill "${pause_injector_pid}" 2>/dev/null
            wait "${pause_injector_pid}" 2>/dev/null
        fi
        mapfile -t active_injectors < <(
            docker container ls --quiet \
                --filter "label=io.nervix.chaos.run=${run_id}" \
                --filter label=io.nervix.chaos.role=fault 2>/dev/null
        )
        if ((${#active_injectors[@]} > 0)); then
            timeout --foreground --kill-after=5s 20s docker container rm --force \
                "${active_injectors[@]}" >"${artifact_dir}/diagnostics/pause-injector-heal.txt" 2>&1
        fi
        if [[ -n "${pause_target_id:-}" ]]; then
            timeout --foreground --kill-after=5s 20s docker inspect "${pause_target_id}" \
                >"${artifact_dir}/diagnostics/target-before-unpause.json" 2>&1
            if [[ "$(timeout --foreground --kill-after=5s 20s docker inspect --format '{{.State.Paused}}' "${pause_target_id}" 2>/dev/null)" == true ]]; then
                timeout --foreground --kill-after=5s 20s docker unpause "${pause_target_id}" \
                    >"${artifact_dir}/diagnostics/target-unpause.txt" 2>&1
            fi
        fi
    fi
    if [[ "${scenario}" == partition-recovery || "${scenario}" == degraded-links ]]; then
        local heal_prefix=network
        if [[ "${scenario}" == partition-recovery ]]; then
            heal_prefix=partition
        fi
        # Heal before anything else so neither retained resources nor diagnostics stay partitioned.
        timeout --foreground --kill-after=5s 180s \
            "${script_dir}/network-faults.sh" heal --run-id "${run_id}" \
            --nettools "${CHAOS_NETTOOLS_IMAGE}" \
            --output "${artifact_dir}/diagnostics/${heal_prefix}-heal.txt" \
            >"${artifact_dir}/diagnostics/${heal_prefix}-heal-status.txt" 2>&1 \
            || printf 'network fault healing on exit reported a failure\n' \
                >>"${artifact_dir}/diagnostics/${heal_prefix}-heal-status.txt"
        if [[ -n "${partition_restart_id:-}" && "${partition_restart_started:-true}" != true \
            && "$(timeout --foreground --kill-after=5s 20s docker inspect --format '{{.State.Running}}' "${partition_restart_id}" 2>/dev/null)" == false ]]; then
            timeout --foreground --kill-after=5s 30s docker start "${partition_restart_id}" \
                >"${artifact_dir}/diagnostics/partition-restart-heal.txt" 2>&1
        fi
    fi
    if [[ "${scenario}" == *-crash && -n "${crash_target_id:-}" \
        && "${crash_restarted:-false}" != true ]]; then
        timeout --foreground --kill-after=5s 20s docker inspect "${crash_target_id}" \
            >"${artifact_dir}/diagnostics/target-before-heal.json" 2>&1
        if [[ "$(timeout --foreground --kill-after=5s 20s docker inspect --format '{{.State.Running}}' "${crash_target_id}" 2>/dev/null)" == false ]]; then
            timeout --foreground --kill-after=5s 30s docker start "${crash_target_id}" \
                >"${artifact_dir}/diagnostics/target-heal.txt" 2>&1
        fi
    fi
    capture_diagnostics
    # A run that otherwise passed still fails when its Docker event evidence is incomplete. A run
    # that already failed keeps its own classification, with the recording's bounds as evidence.
    if [[ "${status}" -eq 0 && "${run_event_recording_covered}" != true ]]; then
        status=1
        failure_category=controller
        current_phase='Docker event recording'
        if [[ -s "${artifact_dir}/diagnostics/docker-events.recording.json" ]]; then
            printf 'controller failure: %s; bounds in diagnostics/docker-events.recording.json\n' \
                "$(jq -r '.reason' "${artifact_dir}/diagnostics/docker-events.recording.json")" >&2
        else
            printf '%s\n' 'controller failure: the run has no live Docker event recording' >&2
        fi
    fi

    if [[ "${status}" -ne 0 && ( "${scenario}" == *-crash || "${scenario}" == pause-resume || "${scenario}" == partition-recovery || "${scenario}" == degraded-links ) ]]; then
        local reproducer_image="${image_id:-${image_ref}}"
        if [[ "${image_ref}" == *@sha256:* ]]; then
            reproducer_image="${image_ref}"
        elif [[ -n "${image_digest}" ]]; then
            reproducer_image="${image_digest%%,*}"
        fi
        local reproducer
        if [[ "${scenario}" == pause-resume ]]; then
            reproducer="$(printf 'just chaos run %q --image %q --records %q' \
                "${scenario}" "${reproducer_image}" "${record_count}")"
        elif [[ "${scenario}" == partition-recovery ]]; then
            reproducer="$(printf 'just chaos run %q --image %q --records %q --case %q --partition-seconds %q' \
                "${scenario}" "${reproducer_image}" "${record_count}" "${partition_case}" "${partition_seconds}")"
        elif [[ "${scenario}" == degraded-links ]]; then
            reproducer="$(printf 'just chaos run %q --image %q --records %q --profile %q --load-interval-ms %q --baseline-seconds %q --degrade-seconds %q --drain-seconds %q --max-backlog %q --max-recovery-backlog %q --max-memory-bytes %q --max-pending %q --min-throughput-pct %q' \
                "${scenario}" "${reproducer_image}" "${record_count}" "${degradation_profile}" "${load_interval_ms}" "${baseline_seconds}" "${degrade_seconds}" "${drain_seconds}" "${max_backlog}" "${max_recovery_backlog}" "${max_memory_bytes}" "${max_pending}" "${min_throughput_pct}")"
        else
            reproducer="$(printf 'just chaos run %q --image %q --nodes %q --records %q --outage-seconds %q' \
                "${scenario}" "${reproducer_image}" "${node_count}" "${record_count}" "${outage_seconds}")"
        fi
        local evidence_path
        local evidence_paths=()
        for evidence_path in \
            diagnostics/docker-events.ndjson diagnostics/docker-events.stderr \
            diagnostics/containers.json \
            diagnostics/compose.log crash/fault-command.json crash/pumba.txt \
            crash/kill-events.ndjson crash/all-node-events.ndjson \
            crash/killed.json crash/held.json \
            crash/started.json crash/final.json crash/before-all-nodes.json \
            crash/status-nervix-1.attempt.txt \
            crash/status-nervix-2.attempt.txt crash/status-nervix-3.attempt.txt \
            crash/control-results.json traffic/accepted-input.ndjson \
            diagnostics/target-before-unpause.json diagnostics/target-unpause.txt \
            results/pause-progress.json \
            traffic/observed-output.ndjson results/crash-progress.json \
            results/ledger.json results/ledger.txt; do
            if [[ -s "${artifact_dir}/${evidence_path}" ]]; then
                evidence_paths+=("${evidence_path}")
            fi
        done
        # Every Docker-event window records the bounds of the recording it was read from.
        while IFS= read -r evidence_path; do
            evidence_paths+=("${evidence_path#"${artifact_dir}/"}")
        done < <(find "${artifact_dir}" -type f -name '*.recording.json' -size +0c 2>/dev/null | sort)
        if [[ "${scenario}" == pause-resume ]]; then
            while IFS= read -r evidence_path; do
                evidence_paths+=("${evidence_path#"${artifact_dir}/"}")
            done < <(find "${artifact_dir}/pauses" -type f \
                \( -name 'result.json' -o -name 'duration.json' -o -name 'pause-events.ndjson' \
                -o -name 'all-node-events.ndjson' -o -name 'pumba.txt' \
                -o -name 'fault-command.json' -o -name 'observer.log' \) \
                -size +0c 2>/dev/null)
        fi
        if [[ "${scenario}" == partition-recovery ]]; then
            for evidence_path in results/partition-findings.ndjson results/partition-recovery.json \
                diagnostics/partition-heal.txt diagnostics/network-preflight.txt; do
                if [[ -s "${artifact_dir}/${evidence_path}" ]]; then
                    evidence_paths+=("${evidence_path}")
                fi
            done
            while IFS= read -r evidence_path; do
                evidence_paths+=("${evidence_path#"${artifact_dir}/"}")
            done < <(find "${artifact_dir}/partitions" -type f \
                \( -name 'plan.json' -o -name 'result.json' -o -name 'samples.ndjson' \
                -o -name 'links-*.json' -o -name '*-netem.json' -o -name '*-iptables.json' \
                -o -name '*-netem.log' -o -name '*-iptables.log' -o -name 'control-*.json' \
                -o -name 'consumer-group-*.json' -o -name 'node-events.ndjson' \
                -o -name 'kill-events.ndjson' \
                -o -name 'isolation-boundary.json' -o -name 'observer.log' \) \
                -size +0c 2>/dev/null | sort)
        fi
        if [[ "${scenario}" == degraded-links ]]; then
            for evidence_path in results/degraded-progress.json results/degraded-links.json \
                degraded/samples.ndjson degraded/actions.ndjson \
                degraded/findings.ndjson \
                degraded/baseline.json diagnostics/network-heal.txt; do
                if [[ -s "${artifact_dir}/${evidence_path}" ]]; then
                    evidence_paths+=("${evidence_path}")
                fi
            done
            while IFS= read -r evidence_path; do
                evidence_paths+=("${evidence_path#"${artifact_dir}/"}")
            done < <(find "${artifact_dir}/degraded" -type f \
                \( -name 'effect-verdict.txt' -o -name 'rules-installed.txt' \
                -o -name 'ping-fault.json' -o -name 'rate-fault.json' \
                -o -name 'recovery-deadline.json' \) \
                -size +0c 2>/dev/null | sort)
        fi
        local evidence_json
        evidence_json="$(printf '%s\n' "${evidence_paths[@]}" \
            | jq -Rsc 'split("\n") | map(select(length > 0))')"
        jq -n \
            --arg category "${failure_category}" \
            --arg phase "${current_phase}" \
            --arg reproducer "${reproducer}" \
            --arg image_id "${image_id}" \
            --arg image_reference "${reproducer_image}" \
            --argjson exit_code "${status}" \
            --argjson evidence "${evidence_json}" \
            '{category:$category,phase:$phase,exit_code:$exit_code,image_id:$image_id,image_reference:$image_reference,reproducer:$reproducer,evidence:$evidence}' \
            >"${artifact_dir}/results/finding.json"
    fi

    local cleanup_status=0
    local retained=false
    if [[ "${keep_resources}" == true ]]; then
        retained=true
    else
        timeout --foreground --kill-after=5s 90s \
            "${script_dir}/cleanup.sh" --run-id "${run_id}" --quiet \
            >"${artifact_dir}/diagnostics/cleanup.txt" 2>&1 || cleanup_status=$?
        if [[ "${status}" -eq 0 && "${cleanup_status}" -ne 0 ]]; then
            status=1
            current_phase="cleanup"
        fi
    fi
    remove_private_keys

    local final_status="passed"
    if [[ "${status}" -ne 0 ]]; then
        final_status="failed"
    fi
    if [[ -n "${signal_name}" ]]; then
        final_status="interrupted"
    fi
    local manifest_tmp
    manifest_tmp="$(mktemp "${artifact_dir}/.manifest.final.XXXXXX")"
    jq \
        --arg status "${final_status}" \
        --arg phase "${current_phase}" \
        --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --arg signal "${signal_name}" \
        --argjson exit_code "${status}" \
        --argjson resources_retained "${retained}" \
        '.status = $status
         | .final_phase = $phase
         | .finished_at = $finished_at
         | .exit_code = $exit_code
         | .resources_retained = $resources_retained
         | if $signal == "" then . else .signal = $signal end' \
        "${artifact_dir}/manifest.json" >"${manifest_tmp}" \
        && mv "${manifest_tmp}" "${artifact_dir}/manifest.json"

    printf '\nchaos %s %s (exit %d)\n' "${scenario}" "${final_status}" "${status}"
    printf 'artifacts: %s\n' "${artifact_dir}"
    if [[ "${retained}" == true ]]; then
        printf 'cleanup: just chaos cleanup --run-id %s\n' "${run_id}"
    fi
    exit "${status}"
}

on_signal() {
    local code="$1"
    signal_name="$2"
    current_phase="signal-${signal_name,,}"
    exit "${code}"
}

trap finish EXIT
trap 'on_signal 130 INT' INT
trap 'on_signal 143 TERM' TERM
trap 'on_signal 129 HUP' HUP

cli_host=nervix-1

ensure_tool_image() {
    local tool_image="$1"
    if run_bounded 20 docker image inspect "${tool_image}" >/dev/null 2>&1; then
        return 0
    fi
    printf 'pulling required tool image %s\n' "${tool_image}"
    run_bounded 180 docker pull "${tool_image}"
}

# Starts the run's live Docker event recording before the run creates its first container, so the
# recording holds every event of every run-owned container.
start_run_event_recording() {
    local owned
    owned="$(run_bounded 20 docker container ls --all --quiet \
        --filter "label=io.nervix.chaos.run=${run_id}")" \
        || setup_error 'could not list run-owned containers before recording Docker events'
    [[ -z "${owned}" ]] \
        || setup_error "containers labeled for run ${run_id} already exist; remove them with just chaos cleanup --run-id ${run_id}"
    local start_status=0
    docker_event_recording_start "${artifact_dir}" diagnostics/docker-events.ndjson "${run_id}" \
        "${CHAOS_PROBE_IMAGE}" "$((overall_timeout + 900))" || start_status=$?
    if ((start_status == 3)); then
        setup_error 'the Docker daemon stamps events outside the time this controller measures around them; run chaos against the local Docker daemon'
    fi
    if ((start_status != 0)); then
        failure_category=controller
        printf '%s\n' 'the Docker event subscriber never recorded its start marker; see diagnostics/docker-events.stderr' >&2
        exit 1
    fi
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_manifest '.docker_event_recording = {recording: "diagnostics/docker-events.ndjson", bounds: "diagnostics/docker-events.recording.json", subscriber_filter: $filter, covered_from_ns: $started}' \
        --arg filter "label=io.nervix.chaos.run=${run_id}" \
        --argjson started "${docker_event_recording_started_ns}"
}

# Chooses this run's network away from every Docker network and host route, then fixes the node
# addresses outside the range Docker hands to every other container.
select_run_network() {
    local networks=()
    mapfile -t networks < <(run_bounded 30 docker network ls --quiet)
    local used_file
    used_file="$(mktemp "${artifact_dir}/.used-networks.XXXXXX")"
    if ((${#networks[@]} > 0)); then
        run_bounded 30 docker network inspect \
            --format '{{range .IPAM.Config}}{{println .Subnet}}{{end}}' "${networks[@]}" \
            >>"${used_file}" 2>/dev/null || true
    fi
    if command -v ip >/dev/null 2>&1; then
        ip -4 -o address show 2>/dev/null | awk '{ print $4 }' >>"${used_file}"
        ip -4 route show 2>/dev/null | awk '$1 != "default" { print $1 }' >>"${used_file}"
    fi
    local subnet
    subnet="$("${script_dir}/select-subnet.sh" "${run_id}" <"${used_file}")" \
        || setup_error 'no free 10.213.N.0/24 network is available for this run'
    rm -f "${used_file}"
    local prefix="${subnet%.0/24}"
    export CHAOS_SUBNET="${subnet}"
    export CHAOS_DYNAMIC_RANGE="${prefix}.128/25"
    export CHAOS_NODE_1_ADDRESS="${prefix}.11"
    export CHAOS_NODE_2_ADDRESS="${prefix}.12"
    export CHAOS_NODE_3_ADDRESS="${prefix}.13"
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_manifest '.network = {subnet: $subnet, dynamic_range: $range, node_addresses: {"nervix-1": $one, "nervix-2": $two, "nervix-3": $three}}' \
        --arg subnet "${CHAOS_SUBNET}" --arg range "${CHAOS_DYNAMIC_RANGE}" \
        --arg one "${CHAOS_NODE_1_ADDRESS}" --arg two "${CHAOS_NODE_2_ADDRESS}" \
        --arg three "${CHAOS_NODE_3_ADDRESS}"
}

generate_tls() {
    local tls_dir="${artifact_dir}/tls"
    run_bounded 30 openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 2 \
        -subj "/CN=Nervix Chaos CA ${run_id}" \
        -keyout "${tls_dir}/ca-key.pem" \
        -out "${tls_dir}/ca.pem" \
        >"${artifact_dir}/diagnostics/openssl-ca.txt" 2>&1

    local number
    for number in 1 2 3; do
        local hostname="nervix-${number}"
        local node_id="node-${number}"
        local extension="${tls_dir}/${node_id}.ext"
        cat >"${extension}" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:${hostname},URI:nervix://cluster/${cluster_id}/node/${node_id}
EOF
        run_bounded 30 openssl req -newkey rsa:2048 -sha256 -nodes \
            -subj "/CN=${hostname}" \
            -keyout "${tls_dir}/${node_id}-key.pem" \
            -out "${tls_dir}/${node_id}.csr" \
            >>"${artifact_dir}/diagnostics/openssl-nodes.txt" 2>&1
        run_bounded 30 openssl x509 -req -sha256 -days 2 \
            -in "${tls_dir}/${node_id}.csr" \
            -CA "${tls_dir}/ca.pem" \
            -CAkey "${tls_dir}/ca-key.pem" \
            -CAcreateserial \
            -extfile "${extension}" \
            -out "${tls_dir}/${node_id}.pem" \
            >>"${artifact_dir}/diagnostics/openssl-nodes.txt" 2>&1
    done
    chmod 0644 "${tls_dir}/"*.pem
}

probe_broker() {
    broker_admin /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server broker:9092 --list >/dev/null 2>&1
}

probe_node() {
    local hostname="$1"
    compose run --rm --no-deps probe sh -eu -c "
        nc -z -w 2 \"\$1\" 47391
        nc -z -w 2 \"\$1\" 47395
        wget -q -T 3 -O /dev/null \"http://\$1:9090/livez\"
        wget -q -T 3 -O /dev/null \"http://\$1:9090/readyz\"
        wget -q -T 3 -O /dev/null \"http://\$1:9090/metrics\"
        wget -q -T 3 -O /dev/null \"http://\$1:47420/console/\"
    " -- "${hostname}" >/dev/null 2>&1
}

cluster_status_ready() {
    local attempt="${artifact_dir}/public/cluster-status.attempt.txt"
    if ! cli_command 'SHOW CLUSTER STATUS;' >"${attempt}" 2>&1; then
        return 1
    fi
    local node
    for node in "${node_names[@]}"; do
        grep -Fq "${node}" "${attempt}" || return 1
    done
}

describe_owner_ready() {
    local statement="$1"
    local owner="$2"
    local output="$3"
    domain_cli_command "${statement}" >"${output}" 2>&1 \
        && grep -Fq "owner: ${owner}" "${output}"
}

topic_end_offset() {
    local topic="$1"
    local output
    output="$(broker_admin /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server broker:9092 --topic "${topic}" 2>/dev/null)" || return 1
    awk -F: -v topic="${topic}" '$1 == topic && $2 == "0" { print $3 }' <<<"${output}"
}

consumer_offsets_at_end() {
    local expected_end="$1"
    local attempt="${artifact_dir}/traffic/consumer-group.attempt.txt"
    if ! broker_admin /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server broker:9092 \
        --group chaos_baseline \
        --describe >"${attempt}" 2>&1; then
        return 1
    fi
    awk -v expected="${expected_end}" '
      $2 == "chaos_input" && $3 == "0" {
        found = 1
        if ($4 == expected && $5 == expected && $6 == 0) {
          correct = 1
        }
      }
      END { exit !(found && correct) }
    ' "${attempt}"
}

output_has_all_records() {
    local expected="$1"
    local end_offset
    end_offset="$(topic_end_offset chaos_output)" || return 1
    [[ "${end_offset}" =~ ^[0-9]+$ ]] || return 1
    ((end_offset >= expected))
}

wait_for_stable_output() {
    local minimum="$1"
    local deadline=$((SECONDS + 30))
    local previous=""
    local stable_polls=0
    while ((SECONDS < deadline && SECONDS < overall_deadline)); do
        local current=""
        current="$(topic_end_offset chaos_output)" || true
        if [[ "${current}" =~ ^[0-9]+$ ]] && ((current >= minimum)); then
            if [[ "${current}" == "${previous}" ]]; then
                stable_polls=$((stable_polls + 1))
            else
                stable_polls=0
            fi
            if ((stable_polls >= 2)); then
                printf '%s\n' "${current}"
                return 0
            fi
            previous="${current}"
        fi
        sleep 1
    done
    printf '%s\n' 'output topic did not reach a stable final boundary' >&2
    return 124
}

capture_metrics() {
    local hostname="$1"
    local output="${artifact_dir}/public/metrics-${hostname}.txt"
    compose run --rm --no-deps probe wget -q -T 5 -O - \
        "http://${hostname}:9090/metrics" >"${output}"
    trim_file "${output}" 1048576
}

assert_metric_total() {
    local metrics_path="$1"
    local expected="$2"
    shift 2
    local line
    while IFS= read -r line; do
        [[ "${line}" == nervix_messages_total\{* ]] || continue
        local required
        local matches=true
        for required in "$@"; do
            if [[ "${line}" != *"${required}"* ]]; then
                matches=false
                break
            fi
        done
        if [[ "${matches}" == true && "${line##* }" == "${expected}" ]]; then
            return 0
        fi
    done <"${metrics_path}"
    printf 'metrics evidence in %s did not report total %s for %s\n' \
        "${metrics_path}" "${expected}" "$*" >&2
    return 1
}

traffic_metrics_ready() {
    local node_host
    for node_host in "${node_hosts[@]}"; do
        capture_metrics "${node_host}" >/dev/null 2>&1 || return 1
    done
    assert_metric_total "${artifact_dir}/public/metrics-nervix-1.txt" "${input_end}" \
        'direction="sent"' 'physical_node_id="node-1"' 'relay="chaos_records"' \
        'target="chaos_ingestor"' >/dev/null 2>&1 || return 1
    if [[ "${node_count}" == "3" ]]; then
        assert_metric_total "${artifact_dir}/public/metrics-nervix-2.txt" "${input_end}" \
            'direction="received"' 'physical_node_id="node-2"' 'relay="chaos_records"' \
            'target="chaos_records"' 'target_kind="RELAY"' >/dev/null 2>&1 || return 1
        assert_metric_total "${artifact_dir}/public/metrics-nervix-3.txt" "${input_end}" \
            'direction="received"' 'physical_node_id="node-3"' 'relay="chaos_records"' \
            'target="chaos_emitter"' 'target_kind="EMITTER"' >/dev/null 2>&1 || return 1
    else
        assert_metric_total "${artifact_dir}/public/metrics-nervix-1.txt" "${input_end}" \
            'direction="received"' 'physical_node_id="node-1"' 'relay="chaos_records"' \
            'target="chaos_emitter"' 'target_kind="EMITTER"' >/dev/null 2>&1 || return 1
    fi
}

phase "preflight"
run_bounded 30 docker info >/dev/null \
    || setup_error 'Docker daemon is unavailable'
run_bounded 30 docker compose version >"${artifact_dir}/docker-compose-version.txt"
select_run_network

if ! image_id="$(run_bounded 30 docker image inspect --format '{{.Id}}' "${image_ref}" 2>/dev/null)"; then
    printf 'image is not local; attempting bounded pull: %s\n' "${image_ref}"
    run_bounded 180 docker pull "${image_ref}" \
        >"${artifact_dir}/image-pull.txt" 2>&1 \
        || setup_error "image '${image_ref}' could not be resolved; build or pull it before running chaos"
    image_id="$(run_bounded 30 docker image inspect --format '{{.Id}}' "${image_ref}")" \
        || setup_error "image '${image_ref}' was pulled but cannot be inspected"
fi
[[ "${image_id}" =~ ^sha256:[a-f0-9]{64}$ ]] \
    || setup_error "image '${image_ref}' did not resolve to an immutable local image ID"

image_digest="$(run_bounded 30 docker image inspect \
    --format '{{join .RepoDigests ","}}' "${image_ref}" 2>/dev/null || true)"
export NERVIX_IMAGE="${image_id}"
run_bounded 30 docker run --rm --entrypoint /bin/sh "${image_id}" -eu -c \
    'test -x /usr/local/bin/nervix-server; test -x /usr/local/bin/nervix-cli' \
    || setup_error "image '${image_ref}' does not package executable nervix-server and nervix-cli binaries"

ensure_tool_image "${CHAOS_KAFKA_IMAGE}"
ensure_tool_image "${CHAOS_KCAT_IMAGE}"
ensure_tool_image "${CHAOS_PROBE_IMAGE}"
start_run_event_recording
if [[ "${scenario}" != "baseline" ]]; then
    [[ -S /var/run/docker.sock ]] \
        || setup_error "${scenario} requires a local /var/run/docker.sock for Pumba"
    ensure_tool_image "${CHAOS_PUMBA_IMAGE}"
    pumba_image_id="$(run_bounded 30 docker image inspect --format '{{.Id}}' "${CHAOS_PUMBA_IMAGE}")"
    run_bounded 30 docker run --rm \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --version >"${artifact_dir}/pumba-version.txt"
    pumba_preflight=(stop --time 60 impossible-chaos-preflight-target)
    if [[ "${scenario}" == *-crash ]]; then
        pumba_preflight=(kill --signal SIGKILL impossible-chaos-preflight-target)
    elif [[ "${scenario}" == pause-resume ]]; then
        pumba_preflight=(pause --duration 1s impossible-chaos-preflight-target)
    elif [[ "${scenario}" == partition-recovery || "${scenario}" == degraded-links ]]; then
        pumba_preflight=(netem --duration 1s --target 192.0.2.1 loss --percent 100 impossible-chaos-preflight-target)
    fi
    run_bounded 30 docker run --rm \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --dry-run --label io.nervix.chaos.run="${run_id}" \
        "${pumba_preflight[@]}" \
        >"${artifact_dir}/diagnostics/pumba-docker-preflight.txt" 2>&1 \
        || setup_error 'Pumba cannot access the selected Docker daemon'
fi
if [[ "${scenario}" == partition-recovery || "${scenario}" == degraded-links ]]; then
    ensure_tool_image "${CHAOS_NETTOOLS_IMAGE}"
    nettools_image_id="$(run_bounded 30 docker image inspect --format '{{.Id}}' "${CHAOS_NETTOOLS_IMAGE}")"
    # Pumba's netem and iptables faults must take effect on this worker's kernel and heal on SIGTERM.
    run_bounded 120 "${script_dir}/network-faults.sh" preflight --run-id "${run_id}" \
        --pumba "${pumba_image_id}" --nettools "${CHAOS_NETTOOLS_IMAGE}" \
        --output "${artifact_dir}/diagnostics/network-preflight" \
        >"${artifact_dir}/diagnostics/network-preflight.txt" 2>&1 \
        || setup_error "this Docker worker cannot install and heal netem/iptables faults; see ${artifact_dir}/diagnostics/network-preflight.txt"
fi

update_manifest \
    ".status = \"running\" | .resolved_image_id = \$image_id | .resolved_repo_digests = \$digests" \
    --arg image_id "${image_id}" --arg digests "${image_digest}"
if [[ "${scenario}" != "baseline" ]]; then
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_manifest '.pumba_image_id = $image_id | .pumba_image = $image' \
        --arg image_id "${pumba_image_id}" --arg image "${CHAOS_PUMBA_IMAGE}"
fi
if [[ "${scenario}" == partition-recovery ]]; then
    # shellcheck disable=SC2016
    update_manifest '.nettools_image_id = $image_id | .nettools_image = $image | .partition = {case: $case, minimum_window_seconds: ($seconds | tonumber)}' \
        --arg image_id "${nettools_image_id}" --arg image "${CHAOS_NETTOOLS_IMAGE}" \
        --arg case "${partition_case}" --arg seconds "${partition_seconds}"
fi
if [[ "${scenario}" == degraded-links ]]; then
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_manifest '.nettools_image_id = $image_id | .nettools_image = $image | .degradation = $settings' \
        --arg image_id "${nettools_image_id}" --arg image "${CHAOS_NETTOOLS_IMAGE}" \
        --argjson settings "$(jq -n \
            --arg profile "${degradation_profile}" \
            --argjson load_interval_ms "${load_interval_ms}" \
            --argjson baseline_seconds "${baseline_seconds}" \
            --argjson degrade_seconds "${degrade_seconds}" \
            --argjson drain_seconds "${drain_seconds}" \
            --argjson max_backlog "${max_backlog}" \
            --argjson max_recovery_backlog "${max_recovery_backlog}" \
            --argjson max_memory_bytes "${max_memory_bytes}" \
            --argjson max_pending "${max_pending}" \
            --argjson min_throughput_pct "${min_throughput_pct}" \
            '{profile:$profile,load_interval_ms:$load_interval_ms,baseline_seconds:$baseline_seconds,degrade_seconds:$degrade_seconds,drain_seconds:$drain_seconds,max_backlog:$max_backlog,max_recovery_backlog:$max_recovery_backlog,max_memory_bytes:$max_memory_bytes,max_pending:$max_pending,min_throughput_pct:$min_throughput_pct}')"
fi

phase "verifier self-check"
run_bounded 180 "${script_dir}/tests/self-test.sh" \
    >"${artifact_dir}/results/verifier-self-test.txt" 2>&1

phase "TLS generation"
generate_tls

phase "Compose validation"
compose config --quiet
compose config >"${artifact_dir}/compose.rendered.yaml"
compose_ready=true

phase "broker startup and provisioning"
compose up --detach broker
wait_for "Kafka broker" 90 probe_broker
broker_admin /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server broker:9092 \
    --create --topic chaos_input --partitions 1 --replication-factor 1 \
    >"${artifact_dir}/public/create-input-topic.txt"
broker_admin /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server broker:9092 \
    --create --topic chaos_output --partitions 1 --replication-factor 1 \
    >"${artifact_dir}/public/create-output-topic.txt"
broker_admin /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server broker:9092 --describe --topic chaos_input \
    >"${artifact_dir}/public/input-topic.txt"
broker_admin /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server broker:9092 --describe --topic chaos_output \
    >"${artifact_dir}/public/output-topic.txt"

phase "Nervix startup"
compose up --detach nervix-1
wait_for "nervix-1 readiness and listeners" 120 probe_node nervix-1
node_names=(node-1)
node_hosts=(nervix-1)
if [[ "${node_count}" == "3" ]]; then
    compose up --detach nervix-2 nervix-3
    wait_for "nervix-2 readiness and listeners" 120 probe_node nervix-2
    wait_for "nervix-3 readiness and listeners" 120 probe_node nervix-3
    node_names+=(node-2 node-3)
    node_hosts+=(nervix-2 nervix-3)
fi
wait_for "public cluster status for ${node_count} node(s)" 120 cluster_status_ready
cp "${artifact_dir}/public/cluster-status.attempt.txt" \
    "${artifact_dir}/public/cluster-status.txt"

phase "image identity verification"
mapfile -t node_containers < <(
    docker container ls --quiet \
        --filter "label=io.nervix.chaos.run=${run_id}" \
        --filter 'label=io.nervix.chaos.role=node'
)
[[ "${#node_containers[@]}" -eq "${node_count}" ]] \
    || { printf 'expected %d Nervix containers, found %d\n' "${node_count}" "${#node_containers[@]}" >&2; exit 1; }
for container_id in "${node_containers[@]}"; do
    running_image="$(run_bounded 20 docker inspect --format '{{.Image}}' "${container_id}")"
    [[ "${running_image}" == "${image_id}" ]] \
        || { printf 'container %s uses %s instead of %s\n' "${container_id}" "${running_image}" "${image_id}" >&2; exit 1; }
done

phase "NSPL graph installation"
nspl_fixture="$(<"${fixture_file}")"
cli_command 'CREATE UNPACED DOMAIN chaos_baseline;' \
    >"${artifact_dir}/public/create-domain.txt" 2>&1
domain_cli_command "${nspl_fixture}" \
    >"${artifact_dir}/public/configure-nspl.txt" 2>&1
domain_cli_command 'START;' >"${artifact_dir}/public/start-domain.txt" 2>&1

phase "public placement evidence"
if [[ "${node_count}" == "3" ]]; then
    domain_cli_command \
        'RELOCATE INGESTOR chaos_ingestor ONTO NODE node-1 IGNORE PREFERENCES;' \
        >"${artifact_dir}/public/relocate-ingestor.txt" 2>&1
    domain_cli_command \
        'RELOCATE RELAY chaos_records ONTO NODE node-2 IGNORE PREFERENCES;' \
        >"${artifact_dir}/public/relocate-relay.txt" 2>&1
    domain_cli_command \
        'RELOCATE EMITTER chaos_emitter ONTO NODE node-3 IGNORE PREFERENCES;' \
        >"${artifact_dir}/public/relocate-emitter.txt" 2>&1
    expected_ingestor_owner="node-1"
    expected_relay_owner="node-2"
    expected_emitter_owner="node-3"
else
    expected_ingestor_owner="node-1"
    expected_relay_owner="node-1"
    expected_emitter_owner="node-1"
fi

wait_for "ingestor owner ${expected_ingestor_owner}" 60 \
    describe_owner_ready 'DESCRIBE INGESTOR chaos_ingestor;' \
    "${expected_ingestor_owner}" "${artifact_dir}/public/describe-ingestor.txt"
wait_for "relay owner ${expected_relay_owner}" 60 \
    describe_owner_ready 'DESCRIBE RELAY chaos_records;' \
    "${expected_relay_owner}" "${artifact_dir}/public/describe-relay.txt"
wait_for "emitter owner ${expected_emitter_owner}" 60 \
    describe_owner_ready 'DESCRIBE EMITTER chaos_emitter;' \
    "${expected_emitter_owner}" "${artifact_dir}/public/describe-emitter.txt"
domain_cli_command 'SHOW PLACEMENTS;' >"${artifact_dir}/public/show-placements.txt" 2>&1

jq -n \
    --arg ingestor "${expected_ingestor_owner}" \
    --arg relay "${expected_relay_owner}" \
    --arg emitter "${expected_emitter_owner}" \
    --argjson crosses_nodes "$([[ "${node_count}" == "3" ]] && printf true || printf false)" \
    '{
      ingestor_owner: $ingestor,
      relay_owner: $relay,
      emitter_owner: $emitter,
      crosses_nodes: $crosses_nodes,
      evidence: [
        "public/describe-ingestor.txt",
        "public/describe-relay.txt",
        "public/describe-emitter.txt"
      ]
    }' >"${artifact_dir}/results/remote-path.json"

if [[ "${scenario}" != "baseline" ]]; then
    # Fault scenarios use this setup and the shared final ledger verifier.
    # shellcheck source=rolling-restart-scenario.sh
    source "${script_dir}/rolling-restart-scenario.sh"
    if [[ "${scenario}" == "rolling-restart" ]]; then
        run_rolling_restart
    elif [[ "${scenario}" == "pause-resume" ]]; then
        # shellcheck source=pause-resume-scenario.sh
        source "${script_dir}/pause-resume-scenario.sh"
        run_pause_resume
    elif [[ "${scenario}" == "partition-recovery" ]]; then
        # shellcheck source=partition-scenario.sh
        source "${script_dir}/partition-scenario.sh"
        run_partition_recovery
    elif [[ "${scenario}" == "degraded-links" ]]; then
        # shellcheck source=degraded-links-scenario.sh
        source "${script_dir}/degraded-links-scenario.sh"
        run_degraded_links
    else
        # shellcheck source=crash-scenario.sh
        source "${script_dir}/crash-scenario.sh"
        run_crash
    fi
else
phase "fixture generation and production"
jq -nc \
    --arg run_id "${run_id}" \
    --argjson count "${record_count}" \
    -f "${fixture_generator}" >"${artifact_dir}/fixtures/input.ndjson"
generated_count="$(wc -l <"${artifact_dir}/fixtures/input.ndjson")"
[[ "${generated_count}" -eq "${record_count}" ]] \
    || { printf 'fixture generated %s records, expected %s\n' "${generated_count}" "${record_count}" >&2; exit 1; }

producer_status=0
kcat -b broker:9092 -P -t chaos_input \
    <"${artifact_dir}/fixtures/input.ndjson" \
    >"${artifact_dir}/traffic/producer.stdout" \
    2>"${artifact_dir}/traffic/producer.stderr" || producer_status=$?
jq -n --argjson exit_code "${producer_status}" \
    '{exit_code: $exit_code, accepted_input_is_reconstructed_from_broker: true}' \
    >"${artifact_dir}/traffic/producer-result.json"

input_end="$(topic_end_offset chaos_input)"
[[ "${input_end}" =~ ^[0-9]+$ ]] \
    || { printf '%s\n' 'could not read the source topic final boundary' >&2; exit 1; }
((input_end > 0)) \
    || { printf '%s\n' 'producer left no accepted records in the source topic' >&2; exit 1; }
((input_end <= record_count)) \
    || { printf 'source topic contains %s records, fixture limit was %s\n' "${input_end}" "${record_count}" >&2; exit 1; }

kcat -q -b broker:9092 -C -t chaos_input -p 0 -o beginning -c "${input_end}" \
    >"${artifact_dir}/traffic/accepted-input.ndjson" \
    2>"${artifact_dir}/traffic/source-consumer.stderr"
accepted_count="$(wc -l <"${artifact_dir}/traffic/accepted-input.ndjson")"
[[ "${accepted_count}" -eq "${input_end}" ]] \
    || { printf 'accepted-input ledger has %s records, expected %s\n' "${accepted_count}" "${input_end}" >&2; exit 1; }
fi

phase "offset and output boundaries"
wait_for "Nervix consumer offsets at source boundary ${input_end}" 120 \
    consumer_offsets_at_end "${input_end}"
cp "${artifact_dir}/traffic/consumer-group.attempt.txt" \
    "${artifact_dir}/traffic/consumer-group-final.txt"
output_wait_timed_out=false
if [[ "${scenario}" == *-crash || "${scenario}" == pause-resume || "${scenario}" == partition-recovery || "${scenario}" == degraded-links ]]; then
    output_wait_seconds=120
    if [[ "${scenario}" == degraded-links ]]; then
        output_wait_seconds="${drain_seconds}"
    fi
    if ! wait_for "output topic to contain at least ${input_end} records" "${output_wait_seconds}" \
        output_has_all_records "${input_end}"; then
        output_wait_timed_out=true
    fi
    # Preserve an exact ledger even when the sink stayed short of the accepted boundary.
    output_end="$(wait_for_stable_output 0)"
else
    wait_for "output topic to contain at least ${input_end} records" 120 \
        output_has_all_records "${input_end}"
    output_end="$(wait_for_stable_output "${input_end}")"
fi
[[ "${output_end}" =~ ^[0-9]+$ ]] \
    || { printf '%s\n' 'could not determine the output topic final boundary' >&2; exit 1; }

if ((output_end > 0)); then
    kcat -q -b broker:9092 -C -t chaos_output -p 0 -o beginning -c "${output_end}" \
        >"${artifact_dir}/traffic/observed-output.ndjson" \
        2>"${artifact_dir}/traffic/output-consumer.stderr"
else
    : >"${artifact_dir}/traffic/observed-output.ndjson"
fi
observed_count="$(wc -l <"${artifact_dir}/traffic/observed-output.ndjson")"
[[ "${observed_count}" -eq "${output_end}" ]] \
    || { printf 'observed-output ledger has %s records, expected %s\n' "${observed_count}" "${output_end}" >&2; exit 1; }

phase "external ledger verification"
ledger_args=()
if [[ "${scenario}" == *-crash || "${scenario}" == pause-resume || "${scenario}" == partition-recovery || "${scenario}" == degraded-links ]]; then
    ledger_args+=(--allow-replay-duplicates)
fi
"${script_dir}/verify-ledger.sh" \
    "${artifact_dir}/traffic/accepted-input.ndjson" \
    "${artifact_dir}/traffic/observed-output.ndjson" \
    "${artifact_dir}/results/ledger.json" "${ledger_args[@]}" \
    >"${artifact_dir}/results/ledger.txt"
if [[ "${output_wait_timed_out}" == true ]]; then
    printf 'sink output exceeded the %s-second accepted-input boundary\n' "${output_wait_seconds:-120}" >&2
    exit 124
fi

phase "public diagnostics"
cli_command 'SHOW CLUSTER STATUS;' >"${artifact_dir}/public/cluster-status-final.txt" 2>&1
domain_cli_command 'DESCRIBE INGESTOR chaos_ingestor;' \
    >"${artifact_dir}/public/describe-ingestor-final.txt" 2>&1
domain_cli_command 'DESCRIBE RELAY chaos_records;' \
    >"${artifact_dir}/public/describe-relay-final.txt" 2>&1
domain_cli_command 'DESCRIBE EMITTER chaos_emitter;' \
    >"${artifact_dir}/public/describe-emitter-final.txt" 2>&1
if [[ "${scenario}" == "baseline" ]]; then
    wait_for "public traffic metrics for every path owner" 45 traffic_metrics_ready
    assert_metric_total "${artifact_dir}/public/metrics-nervix-1.txt" "${input_end}" \
        'direction="sent"' 'physical_node_id="node-1"' 'relay="chaos_records"' \
        'target="chaos_ingestor"'
    if [[ "${node_count}" == "3" ]]; then
        assert_metric_total "${artifact_dir}/public/metrics-nervix-2.txt" "${input_end}" \
            'direction="received"' 'physical_node_id="node-2"' 'relay="chaos_records"' \
            'target="chaos_records"' 'target_kind="RELAY"'
        assert_metric_total "${artifact_dir}/public/metrics-nervix-3.txt" "${input_end}" \
            'direction="received"' 'physical_node_id="node-3"' 'relay="chaos_records"' \
            'target="chaos_emitter"' 'target_kind="EMITTER"'
    else
        assert_metric_total "${artifact_dir}/public/metrics-nervix-1.txt" "${input_end}" \
            'direction="received"' 'physical_node_id="node-1"' 'relay="chaos_records"' \
            'target="chaos_emitter"' 'target_kind="EMITTER"'
    fi
fi

if [[ "${scenario}" == "baseline" ]]; then
    remote_path_tmp="$(mktemp "${artifact_dir}/results/.remote-path.XXXXXX")"
    jq \
        --argjson records "${input_end}" \
        --arg source_metrics "public/metrics-nervix-1.txt" \
        --arg relay_metrics "public/metrics-nervix-$([[ "${node_count}" == "3" ]] && printf 2 || printf 1).txt" \
        --arg emitter_metrics "public/metrics-nervix-$([[ "${node_count}" == "3" ]] && printf 3 || printf 1).txt" \
        '.traffic_records = $records
         | .traffic_evidence = [$source_metrics, $relay_metrics, $emitter_metrics]' \
        "${artifact_dir}/results/remote-path.json" >"${remote_path_tmp}"
    mv "${remote_path_tmp}" "${artifact_dir}/results/remote-path.json"
fi
broker_admin /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server broker:9092 --group chaos_baseline --describe \
    >"${artifact_dir}/public/consumer-group-final.txt"

if [[ "${scenario}" == "baseline" ]]; then
    jq -n \
        --arg run_id "${run_id}" \
        --arg image_id "${image_id}" \
        --arg image_digest "${image_digest}" \
        --argjson nodes "${node_count}" \
        --argjson generated_records "${record_count}" \
        --argjson accepted_records "${input_end}" \
        --argjson observed_records "${output_end}" \
        --argjson producer_exit_code "${producer_status}" \
        --argjson remote_path "$([[ "${node_count}" == "3" ]] && printf true || printf false)" \
        '{
          verdict: "pass",
          run_id: $run_id,
          image_id: $image_id,
          image_digest: $image_digest,
          topology_nodes: $nodes,
          generated_records: $generated_records,
          accepted_source_records: $accepted_records,
          observed_output_records: $observed_records,
          producer_exit_code: $producer_exit_code,
          producer_outcome_was_ambiguous: ($producer_exit_code != 0),
          source_offsets_committed: true,
          remote_path_proven: $remote_path,
          ledger: "results/ledger.json",
          placement: "results/remote-path.json"
        }' >"${artifact_dir}/results/baseline.json"
elif [[ "${scenario}" == "rolling-restart" ]]; then
    jq -n \
        --arg run_id "${run_id}" \
        --arg image_id "${image_id}" \
        --arg pumba_image_id "${pumba_image_id}" \
        --argjson nodes "${node_count}" \
        --argjson accepted_records "${input_end}" \
        --argjson observed_records "${output_end}" \
        --slurpfile progress "${artifact_dir}/results/rolling-progress.json" \
        '{verdict:"pass",run_id:$run_id,image_id:$image_id,pumba_image_id:$pumba_image_id,topology_nodes:$nodes,accepted_source_records:$accepted_records,observed_output_records:$observed_records,source_offsets_committed:true,ledger:"results/ledger.json",remote_path:"results/remote-path.json",progress:$progress[0]}' \
        >"${artifact_dir}/results/rolling-restart.json"
elif [[ "${scenario}" == "pause-resume" ]]; then
    jq -n \
        --arg run_id "${run_id}" \
        --arg image_id "${image_id}" \
        --arg pumba_image_id "${pumba_image_id}" \
        --argjson accepted_records "${input_end}" \
        --argjson observed_records "${output_end}" \
        --slurpfile progress "${artifact_dir}/results/pause-progress.json" \
        --slurpfile ledger "${artifact_dir}/results/ledger.json" \
        '{verdict:"pass",run_id:$run_id,image_id:$image_id,pumba_image_id:$pumba_image_id,topology_nodes:3,accepted_source_records:$accepted_records,observed_output_records:$observed_records,replay_duplicates:$ledger[0].duplicate_records,source_offsets_committed:true,ledger:"results/ledger.json",remote_path:"results/remote-path.json",progress:$progress[0]}' \
        >"${artifact_dir}/results/pause-resume.json"
elif [[ "${scenario}" == "partition-recovery" ]]; then
    jq -n \
        --arg run_id "${run_id}" \
        --arg image_id "${image_id}" \
        --arg pumba_image_id "${pumba_image_id}" \
        --arg nettools_image_id "${nettools_image_id}" \
        --arg case "${partition_case}" \
        --argjson accepted_records "${input_end}" \
        --argjson observed_records "${output_end}" \
        --slurpfile progress "${artifact_dir}/results/partition-progress.json" \
        --slurpfile ledger "${artifact_dir}/results/ledger.json" \
        --slurpfile findings <(cat "${artifact_dir}/results/partition-findings.ndjson" 2>/dev/null || true) \
        '{verdict:(if ($findings | length) == 0 then "pass" else "fail" end),run_id:$run_id,image_id:$image_id,pumba_image_id:$pumba_image_id,nettools_image_id:$nettools_image_id,case:$case,topology_nodes:3,accepted_source_records:$accepted_records,observed_output_records:$observed_records,replay_duplicates:$ledger[0].duplicate_records,source_offsets_committed:true,ledger:"results/ledger.json",remote_path:"results/remote-path.json",findings:$findings,progress:$progress[0]}' \
        >"${artifact_dir}/results/partition-recovery.json"
    if [[ "$(jq -r '.verdict' "${artifact_dir}/results/partition-recovery.json")" != pass ]]; then
        failure_category=product
        current_phase="partition product findings"
        printf 'partition-recovery recorded %d product finding(s):\n' \
            "$(jq '.findings | length' "${artifact_dir}/results/partition-recovery.json")" >&2
        jq -r '.findings[] | "- \(.case): \(.message)"' "${artifact_dir}/results/partition-recovery.json" >&2
        exit 1
    fi
elif [[ "${scenario}" == "degraded-links" ]]; then
    jq -n \
        --arg run_id "${run_id}" --arg image_id "${image_id}" \
        --arg pumba_image_id "${pumba_image_id}" --arg nettools_image_id "${nettools_image_id}" \
        --arg profile "${degradation_profile}" \
        --argjson accepted_records "${input_end}" --argjson observed_records "${output_end}" \
        --slurpfile progress "${artifact_dir}/results/degraded-progress.json" \
        --slurpfile ledger "${artifact_dir}/results/ledger.json" \
        --slurpfile findings <(cat "${artifact_dir}/degraded/findings.ndjson" 2>/dev/null || true) \
        '{verdict:(if ($findings|length)==0 then "pass" else "fail" end),run_id:$run_id,
          image_id:$image_id,pumba_image_id:$pumba_image_id,nettools_image_id:$nettools_image_id,
          profile:$profile,topology_nodes:3,accepted_source_records:$accepted_records,
          observed_output_records:$observed_records,replay_duplicates:$ledger[0].duplicate_records,
          source_offsets_committed:true,ledger:"results/ledger.json",progress:$progress[0],findings:$findings}' \
        >"${artifact_dir}/results/degraded-links.json"
    if [[ "$(jq -r '.verdict' "${artifact_dir}/results/degraded-links.json")" != pass ]]; then
        failure_category=product
        current_phase='degraded-links threshold findings'
        jq -r '.findings[] | "\(.profile) \(.stage) \(.metric): \(.observed) > \(.limit) at \(.at_ms)"' \
            "${artifact_dir}/results/degraded-links.json" >&2
        exit 1
    fi
else
    jq -n \
        --arg run_id "${run_id}" \
        --arg scenario "${scenario}" \
        --arg image_id "${image_id}" \
        --arg pumba_image_id "${pumba_image_id}" \
        --argjson nodes "${node_count}" \
        --argjson accepted_records "${input_end}" \
        --argjson observed_records "${output_end}" \
        --slurpfile progress "${artifact_dir}/results/crash-progress.json" \
        --slurpfile ledger "${artifact_dir}/results/ledger.json" \
        '{verdict:"pass",run_id:$run_id,scenario:$scenario,image_id:$image_id,pumba_image_id:$pumba_image_id,topology_nodes:$nodes,accepted_source_records:$accepted_records,observed_output_records:$observed_records,replay_duplicates:$ledger[0].duplicate_records,source_offsets_committed:true,ledger:"results/ledger.json",remote_path:"results/remote-path.json",progress:$progress[0]}' \
        >"${artifact_dir}/results/crash.json"
fi

current_phase="complete"
