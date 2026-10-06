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
  --records N            Finite fixture size, 1..1000; mixed-instability 1..20000 and by default
                         twice its planned timeline plus 15 minutes of records (default: ${record_count}).
  --artifacts DIR        Artifact root (default: target/chaos).
  --run-id ID            Stable run identifier; generated when omitted.
  --timeout SECONDS      Whole-run bound, 120..3600 (default: ${overall_timeout}); mixed-instability
                         from its duration plus 600 through 21600, by default its duration plus the
                         larger of its duration and 1200, at most 21600.
  --outage-seconds N     Minimum held crash outage, 5..120 (default: ${outage_seconds}).
  pause-resume uses configured 10s/12s Raft election and 15s node detection
  thresholds, with 1s short and 75s failover-length pauses.
  --case CASE            partition-recovery case: all, follower, asymmetric, leader,
                         or quorum-loss (default: ${partition_case}).
  --partition-seconds N  Minimum verified partition window, 20..600 (default: ${partition_seconds}).
  --isolation-seconds N  former-owner-restart window in which the restarted former owner stays
                         isolated before startup admission, 20..600 (default: ${isolation_seconds}).
  --fault FAULT          stateful fault: none, owner-crash, owner-pause, owner-partition or
                         cluster-restart; domain-time fault: none, voter-crash, voter-pause,
                         voter-partition, voter-stop or cluster-restart. One node supports none and
                         cluster-restart (default: ${fault}).
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
  --seed N               mixed-instability seed that selects the action plan, 0..2147483646
                         (default: drawn and recorded).
  --duration D           mixed-instability planned fault timeline, as seconds or with an s, m or h
                         suffix, 60s..4h (default: 30m).
  --policy POLICY        mixed-instability quorum policy: preserve-quorum or temporary-quorum-loss
                         (default: preserve-quorum).
  --coverage LIST        mixed-instability FAMILY:ROLE items and quorum-loss the run must exercise
                         (default: every family against the leader and a follower, plus
                         quorum-loss under temporary-quorum-loss).
  --plan FILE            mixed-instability explicit action plan instead of a seeded one.
  --max-memory-bytes, --max-recovery-backlog and --max-pending also bound mixed-instability.
  --keep                 Retain labeled Docker resources after diagnostics.
  -h, --help             Show this help.
EOF
}

setup_error() {
    failure_category=setup
    setup_error_message="$*"
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
isolation_seconds=45
isolation_option_set=false
fault=none
fault_option_set=false
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
mixed_seed=""
mixed_duration=""
mixed_policy=""
mixed_coverage=""
mixed_plan_file=""
# No mixed-instability run, its default bound included, lasts longer than six hours.
mixed_max_timeout=21600
replay_dir=""
records_option_set=false
timeout_option_set=false
mixed_option_set=false
limit_option_set=false
# Every option the command line gave, so a replay can refuse the ones that would change the
# recorded experiment.
given_options=()

while [[ "$#" -gt 0 ]]; do
    given_options+=("$1")
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
            elif [[ "${scenario}" == backup ]]; then
                record_count=1000
                overall_timeout=1800
            elif [[ "${scenario}" == stateful || "${scenario}" == domain-time ]]; then
                record_count=1000
                overall_timeout=2700
            fi
            shift 2
            ;;
        --replay)
            [[ "$#" -ge 2 ]] || setup_error '--replay requires a run directory'
            replay_dir="$2"
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
            records_option_set=true
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
            timeout_option_set=true
            shift 2
            ;;
        --seed | --duration | --policy | --coverage | --plan)
            [[ "$#" -ge 2 ]] || setup_error "$1 requires a value"
            case "$1" in
                --seed) mixed_seed="$2" ;;
                --duration) mixed_duration="$2" ;;
                --policy) mixed_policy="$2" ;;
                --coverage) mixed_coverage="$2" ;;
                --plan) mixed_plan_file="$2" ;;
            esac
            mixed_option_set=true
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
        --isolation-seconds)
            [[ "$#" -ge 2 ]] || setup_error '--isolation-seconds requires a value'
            isolation_seconds="$2"
            isolation_option_set=true
            shift 2
            ;;
        --fault)
            [[ "$#" -ge 2 ]] || setup_error '--fault requires a value'
            fault="$2"
            fault_option_set=true
            shift 2
            ;;
        --profile | --load-interval-ms | --baseline-seconds | --degrade-seconds | --drain-seconds | --max-backlog | --min-throughput-pct)
            [[ "$#" -ge 2 ]] || setup_error "$1 requires a value"
            case "$1" in
                --profile) degradation_profile="$2" ;;
                --load-interval-ms) load_interval_ms="$2" ;;
                --baseline-seconds) baseline_seconds="$2" ;;
                --degrade-seconds) degrade_seconds="$2" ;;
                --drain-seconds) drain_seconds="$2" ;;
                --max-backlog) max_backlog="$2" ;;
                --min-throughput-pct) min_throughput_pct="$2" ;;
            esac
            degradation_option_set=true
            shift 2
            ;;
        --max-recovery-backlog | --max-memory-bytes | --max-pending)
            [[ "$#" -ge 2 ]] || setup_error "$1 requires a value"
            case "$1" in
                --max-recovery-backlog) max_recovery_backlog="$2" ;;
                --max-memory-bytes) max_memory_bytes="$2" ;;
                --max-pending) max_pending="$2" ;;
            esac
            limit_option_set=true
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

for command_name in docker jq openssl timeout awk sed grep sort wc date; do
    command -v "${command_name}" >/dev/null 2>&1 \
        || setup_error "required command is unavailable: ${command_name}"
done

sha256_of() {
    openssl dgst -sha256 -r "$1" | awk '{ print $1 }'
}

# Loads the experiment a mixed-instability run directory recorded: its Nervix and tool image
# identities, deployment, limits, plan and fixtures. A directory that does not record all of them,
# or whose plan or fixtures no longer match the digests its manifest records, is refused before the
# replay creates anything.
load_replay_settings() {
    [[ -d "${replay_dir}" ]] || setup_error "the replay run directory does not exist: ${replay_dir}"
    replay_dir="$(cd "${replay_dir}" && pwd)"
    local manifest="${replay_dir}/manifest.json"
    [[ -s "${manifest}" ]] || setup_error "the replay run directory has no manifest.json: ${replay_dir}"
    jq -e 'type == "object"' "${manifest}" >/dev/null 2>&1 \
        || setup_error "the replay manifest is not a JSON object: ${manifest}"
    [[ "$(jq -r '.scenario' "${manifest}")" == mixed-instability ]] \
        || setup_error "only mixed-instability runs can be replayed; ${replay_dir} recorded $(jq -r '.scenario // "no scenario"' "${manifest}")"
    local missing
    missing="$(jq -r '
        def digest: type == "string" and test("^[a-f0-9]{64}$");
        [ (if (.resolved_image_id | type == "string" and test("^sha256:[a-f0-9]{64}$")) | not then "the resolved Nervix image" else empty end),
          (if (.requested_image | type == "string" and length > 0) | not then "the requested Nervix image" else empty end),
          (if (.resolved_repo_digests | type == "string") | not then "the Nervix repository digests" else empty end),
          (["kafka", "kcat", "probe", "pumba", "nettools"][] as $tool
           | select(((.tool_images[$tool].reference | type == "string" and test("@sha256:[a-f0-9]{64}$"))
                     and (.tool_images[$tool].image_id | type == "string" and test("^sha256:[a-f0-9]{64}$"))) | not)
           | "the \($tool) image"),
          (if .topology_nodes != 3 then "a three-node topology" else empty end),
          (if (.timeout_seconds | type == "number") | not then "its timeout" else empty end),
          (if (.mixed.plan.sha256 | digest) | not then "the action plan digest" else empty end),
          (if (.mixed.limits | type == "object") | not then "the declared limits" else empty end),
          (if (.mixed.deployment.nodes | type == "object") | not then "the deployment" else empty end),
          (if (.mixed.deployment.load_interval_ms | type == "number" and . >= 1 and . == floor) | not
           then "the load interval" else empty end),
          (if ((.fixtures.input.sha256 | digest) and (.fixtures.input.records | type == "number")) | not
           then "the input fixture" else empty end),
          (if (.fixtures.graph.sha256 | digest) | not then "the NSPL graph" else empty end)
        ] | join(", ")' "${manifest}")"
    [[ -z "${missing}" ]] \
        || setup_error "the replay manifest ${manifest} does not record ${missing}, so the experiment cannot be reconstructed"
    local entry
    for entry in 'mixed/plan.json .mixed.plan.sha256' 'fixtures/input.ndjson .fixtures.input.sha256' \
        'fixtures/baseline.nspl .fixtures.graph.sha256'; do
        local path="${entry%% *}"
        local field="${entry#* }"
        [[ -s "${replay_dir}/${path}" ]] || setup_error "the replay run directory lacks its recorded ${path}"
        local recorded actual
        recorded="$(jq -r "${field}" "${manifest}")"
        actual="$(sha256_of "${replay_dir}/${path}")"
        [[ "${actual}" == "${recorded}" ]] \
            || setup_error "${path} in ${replay_dir} no longer matches the digest its manifest records"
    done
    replay_image_id="$(jq -r '.resolved_image_id' "${manifest}")"
    replay_repo_digests="$(jq -r '.resolved_repo_digests' "${manifest}")"
    image_ref="$(jq -r '.requested_image' "${manifest}")"
    node_count=3
    record_count="$(jq -r '.fixtures.input.records' "${manifest}")"
    mixed_duration="$(jq -r '.duration_seconds' "${replay_dir}/mixed/plan.json" 2>/dev/null)" || mixed_duration=""
    [[ "${mixed_duration}" =~ ^[0-9]{1,6}$ ]] \
        || setup_error "the recorded action plan of ${replay_dir} has no whole duration_seconds"
    if [[ "${timeout_option_set}" != true ]]; then
        overall_timeout="$(jq -r '.timeout_seconds' "${manifest}")"
    fi
    max_memory_bytes="$(jq -r '.mixed.limits.max_memory_bytes' "${manifest}")"
    max_recovery_backlog="$(jq -r '.mixed.limits.max_recovery_backlog' "${manifest}")"
    max_pending="$(jq -r '.mixed.limits.max_pending' "${manifest}")"
    # The recorded tool references replace the pins checked in now, and the recorded deployment the
    # Compose defaults.
    chaos_kafka_image="$(jq -r '.tool_images.kafka.reference' "${manifest}")"
    chaos_kcat_image="$(jq -r '.tool_images.kcat.reference' "${manifest}")"
    chaos_probe_image="$(jq -r '.tool_images.probe.reference' "${manifest}")"
    chaos_pumba_image="$(jq -r '.tool_images.pumba.reference' "${manifest}")"
    chaos_nettools_image="$(jq -r '.tool_images.nettools.reference' "${manifest}")"
    local tool
    for tool in kafka kcat probe pumba nettools; do
        replay_tool_image_ids["${tool}"]="$(jq -r --arg tool "${tool}" '.tool_images[$tool].image_id' "${manifest}")"
    done
    local setting value
    while IFS=$'\t' read -r setting value; do
        export "CHAOS_${setting#NERVIX_}=${value}"
    done < <(jq -r '.mixed.deployment.nodes | to_entries[] | "\(.key)\t\(.value)"' "${manifest}")
    replay_load_interval_ms="$(jq -r '.mixed.deployment.load_interval_ms' "${manifest}")"
    replay_of="$(jq -c --arg directory "${replay_dir}" '{run_id, directory: $directory, status, exit_code,
        final_phase, plan_sha256: .mixed.plan.sha256, seed: .mixed.seed}' "${manifest}")"
}

replay_image_id=""
replay_repo_digests=""
replay_load_interval_ms=""
replay_of=null
declare -A replay_tool_image_ids=()
if [[ -n "${replay_dir}" ]]; then
    [[ "${scenario}" == mixed-instability ]] || setup_error 'only mixed-instability runs can be replayed'
    for option in "${given_options[@]}"; do
        case "${option}" in
            --replay | --scenario | --artifacts | --run-id | --timeout | --keep) ;;
            *) setup_error "a replay reuses the experiment its run directory recorded, so ${option} cannot change it" ;;
        esac
    done
    load_replay_settings
fi

[[ -n "${image_ref}" ]] || setup_error '--image is required and must name an already-built Nervix image'
case "${scenario}" in
    baseline | rolling-restart | leader-crash | follower-crash | ingestor-owner-crash | emitter-owner-crash | pause-resume | partition-recovery | degraded-links | backup | stale-follower | former-owner-restart | cluster-restart | stateful | domain-time | mixed-instability) ;;
    *) setup_error "unknown scenario: ${scenario}" ;;
esac
[[ "${node_count}" == "1" || "${node_count}" == "3" ]] \
    || setup_error '--nodes must be 1 or 3'
# The restart, recovery, stateful, domain-time and mixed-instability scenarios share their traffic,
# findings and exit handling.
recovery_scenario=false
case "${scenario}" in
    stale-follower | former-owner-restart | cluster-restart | stateful | domain-time | mixed-instability) recovery_scenario=true ;;
esac
if [[ "${scenario}" != baseline && "${scenario}" != backup && "${scenario}" != rolling-restart && "${scenario}" != leader-crash && "${scenario}" != cluster-restart && "${scenario}" != stateful && "${scenario}" != domain-time && "${node_count}" != 3 ]]; then
    setup_error "${scenario} requires --nodes 3"
fi
case "${scenario}" in
    stateful)
        case "${fault}" in
            none | owner-crash | owner-pause | owner-partition | cluster-restart) ;;
            *) setup_error "--fault for stateful must be none, owner-crash, owner-pause, owner-partition or cluster-restart, not ${fault}" ;;
        esac
        ;;
    domain-time)
        case "${fault}" in
            none | voter-crash | voter-pause | voter-partition | voter-stop | cluster-restart) ;;
            *) setup_error "--fault for domain-time must be none, voter-crash, voter-pause, voter-partition, voter-stop or cluster-restart, not ${fault}" ;;
        esac
        ;;
    *)
        [[ "${fault_option_set}" != true ]] \
            || setup_error '--fault applies only to stateful and domain-time'
        ;;
esac
if [[ "${node_count}" == 1 && "${fault}" != none && "${fault}" != cluster-restart ]]; then
    setup_error "${fault} requires --nodes 3; one node supports none and cluster-restart"
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
if [[ ( "${scenario}" == stale-follower || "${scenario}" == former-owner-restart ) && "${outage_option_set}" == true ]]; then
    setup_error "${scenario} holds its outage until its recovery conditions; --outage-seconds is for crash and cluster-restart scenarios"
fi
if [[ "${scenario}" != former-owner-restart && "${isolation_option_set}" == true ]]; then
    setup_error '--isolation-seconds applies only to former-owner-restart'
fi
[[ "${isolation_seconds}" =~ ^[0-9]+$ ]] \
    || setup_error '--isolation-seconds must be an integer from 20 through 600'
((isolation_seconds >= 20 && isolation_seconds <= 600)) \
    || setup_error '--isolation-seconds must be an integer from 20 through 600'
if [[ "${scenario}" != degraded-links && "${degradation_option_set}" == true ]]; then
    setup_error 'degradation profiles and thresholds apply only to degraded-links'
fi
if [[ "${scenario}" != degraded-links && "${scenario}" != mixed-instability && "${limit_option_set}" == true ]]; then
    setup_error '--max-memory-bytes, --max-recovery-backlog and --max-pending apply only to degraded-links and mixed-instability'
fi
if [[ "${scenario}" != mixed-instability && "${mixed_option_set}" == true ]]; then
    setup_error '--seed, --duration, --policy, --coverage and --plan apply only to mixed-instability'
fi
if [[ "${scenario}" == mixed-instability ]]; then
    [[ "${outage_option_set}" != true ]] \
        || setup_error 'mixed-instability takes every hold from its action plan; --outage-seconds is for crash and recovery scenarios'
    if [[ -n "${mixed_plan_file}" ]]; then
        [[ -z "${mixed_seed}${mixed_duration}${mixed_policy}${mixed_coverage}" ]] \
            || setup_error '--plan carries its own seed, duration, policy and coverage'
        [[ -s "${mixed_plan_file}" ]] || setup_error "the action plan does not exist: ${mixed_plan_file}"
        mixed_duration="$(jq -r '.duration_seconds' "${mixed_plan_file}" 2>/dev/null)" || mixed_duration=""
        [[ "${mixed_duration}" =~ ^[0-9]{1,6}$ ]] \
            || setup_error "the action plan ${mixed_plan_file} records no whole duration_seconds"
    elif [[ -z "${replay_dir}" ]]; then
        mixed_policy="${mixed_policy:-preserve-quorum}"
        [[ "${mixed_policy}" == preserve-quorum || "${mixed_policy}" == temporary-quorum-loss ]] \
            || setup_error "--policy must be preserve-quorum or temporary-quorum-loss, not ${mixed_policy}"
        mixed_duration="${mixed_duration:-30m}"
        if [[ "${mixed_duration}" =~ ^([0-9]{1,6})(s|m|h)?$ ]]; then
            mixed_duration=$((10#${BASH_REMATCH[1]}))
            case "${BASH_REMATCH[2]}" in
                m) mixed_duration=$((mixed_duration * 60)) ;;
                h) mixed_duration=$((mixed_duration * 3600)) ;;
            esac
        else
            setup_error "--duration must be whole seconds, or minutes or hours with an m or h suffix, not ${mixed_duration}"
        fi
        if [[ -z "${mixed_seed}" ]]; then
            mixed_seed=$(((RANDOM << 15 | RANDOM) % 2147483647))
        fi
        if [[ ! "${mixed_seed}" =~ ^[0-9]{1,10}$ ]] || ((10#${mixed_seed} > 2147483646)); then
            setup_error '--seed must be an integer from 0 through 2147483646'
        fi
        mixed_seed=$((10#${mixed_seed}))
        if [[ -z "${mixed_coverage}" ]]; then
            mixed_coverage="$("${script_dir}/mixed-plan.sh" default-coverage --policy "${mixed_policy}")"
        fi
    fi
    mixed_duration=$((10#${mixed_duration}))
    ((mixed_duration >= 60 && mixed_duration <= 14400)) \
        || setup_error "a mixed-instability duration runs from 60 seconds through 4 hours, not ${mixed_duration} seconds"
    if [[ "${timeout_option_set}" != true && -z "${replay_dir}" ]]; then
        overall_timeout=$((mixed_duration + (mixed_duration > 1200 ? mixed_duration : 1200)))
        if ((overall_timeout > mixed_max_timeout)); then
            overall_timeout="${mixed_max_timeout}"
        fi
    fi
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
max_records=1000
min_timeout=120
max_timeout=3600
if [[ "${scenario}" == mixed-instability ]]; then
    max_records=20000
    min_timeout=$((mixed_duration + 600))
    max_timeout="${mixed_max_timeout}"
fi
[[ "${record_count}" =~ ^[0-9]+$ ]] \
    || setup_error "--records must be an integer from 1 through ${max_records}"
((record_count >= 1 && record_count <= max_records)) \
    || setup_error "--records must be an integer from 1 through ${max_records}"
[[ "${overall_timeout}" =~ ^[0-9]+$ ]] \
    || setup_error "--timeout must be an integer from ${min_timeout} through ${max_timeout}"
((overall_timeout >= min_timeout && overall_timeout <= max_timeout)) \
    || setup_error "--timeout must be an integer from ${min_timeout} through ${max_timeout}"
[[ "${outage_seconds}" =~ ^[0-9]+$ ]] \
    || setup_error '--outage-seconds must be an integer from 5 through 120'
((outage_seconds >= 5 && outage_seconds <= 120)) \
    || setup_error '--outage-seconds must be an integer from 5 through 120'

if [[ -z "${run_id}" ]]; then
    run_id="run-$(date -u +%Y%m%dt%H%M%Sz)-$$-${RANDOM}"
fi
[[ "${run_id}" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,95}$ ]] \
    || setup_error '--run-id must be 1..96 letters, numbers, dots, underscores, or hyphens'

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
setup_error_message=""
declare -A tool_image_ids=()
# The live recording is the run's complete Docker event evidence and is never trimmed; a recording
# that outgrows this bound fails the run instead.
docker_event_bytes_limit=67108864
run_event_recording_covered=false
# Every teardown step after the run's timeout is bounded on its own, and together they fit this
# reserve, which the live event subscriber also outlives the timeout by.
teardown_reserve_seconds=900

export CHAOS_RUN_ID="${run_id}"
export CHAOS_CLUSTER_ID="${cluster_id}"
export CHAOS_TLS_DIR="${artifact_dir}/tls"
export CHAOS_PASSWORD="${password}"
export CHAOS_KAFKA_IMAGE="${chaos_kafka_image}"
export CHAOS_KCAT_IMAGE="${chaos_kcat_image}"
export CHAOS_PROBE_IMAGE="${chaos_probe_image}"
export CHAOS_PUMBA_IMAGE="${chaos_pumba_image}"
export CHAOS_NETTOOLS_IMAGE="${chaos_nettools_image}"
export CHAOS_LOAD_FILE="${artifact_dir}/fixtures/input.ndjson"
export CHAOS_TRAFFIC_DIR="${artifact_dir}/traffic"
export CHAOS_SCRIPT_DIR="${script_dir}"
export CHAOS_NODE_COUNT="${node_count}"
if [[ "${scenario}" == pause-resume || "${fault}" == *-pause ]]; then
    export CHAOS_RAFT_HEARTBEAT_INTERVAL=250ms
    export CHAOS_RAFT_ELECTION_TIMEOUT_MIN=10s
    export CHAOS_RAFT_ELECTION_TIMEOUT_MAX=12s
    export CHAOS_NODE_UNAVAILABILITY_TIMEOUT=15s
fi
# The continuous load produces one fixture record per interval.
case "${scenario}" in
    baseline)
        # The baseline produces its whole fixture with one call and starts no continuous load.
        ;;
    degraded-links)
        # --load-interval-ms declares the interval.
        ;;
    partition-recovery)
        # One record every two seconds keeps the bounded fixture flowing through all four cases.
        load_interval_ms=2000
        ;;
    pause-resume | stale-follower | former-owner-restart | cluster-restart | stateful | domain-time)
        # One record a second keeps the bounded fixture flowing through the slowest case.
        load_interval_ms=1000
        ;;
    mixed-instability)
        # One record a second keeps the fixture flowing through a long timeline of disturbances; a
        # replay produces at the interval its run recorded.
        load_interval_ms="${replay_load_interval_ms:-1000}"
        ;;
    *)
        load_interval_ms=500
        ;;
esac
export CHAOS_LOAD_INTERVAL_MS="${load_interval_ms}"
if [[ "${scenario}" == stale-follower ]]; then
    # Ordinary retention options small enough that acknowledged changes made while one follower is
    # offline snapshot and purge the survivors' logs past its position within a bounded run.
    export CHAOS_RAFT_SNAPSHOT_ENTRY_THRESHOLD=64
    export CHAOS_RAFT_COVERED_LOG_ENTRIES_RETAINED=16
fi
if [[ "${scenario}" == stateful ]]; then
    # Ordinary state options: a one-second snapshot interval publishes runtime checkpoints several
    # times inside a held milestone, and on three nodes one state replica lets the loss of an owner
    # promote its replicated state instead of resetting it.
    export CHAOS_STATE_SNAPSHOT_INTERVAL=1s
    if [[ "${node_count}" == 3 ]]; then
        export CHAOS_REPLICA_COUNT=1
    fi
fi
export CHAOS_STATE_LOAD_FILE="${artifact_dir}/fixtures/state-input.ndjson"
export CHAOS_PACED_LOAD_FILE="${artifact_dir}/fixtures/paced-input.ndjson"
# The stateful load produces one record a second and the paced load one every 250 ms.
export CHAOS_STATE_LOAD_INTERVAL_MS=1000
export CHAOS_PACED_LOAD_INTERVAL_MS=250

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
    --argjson fixture_records "${max_records}" \
    '{
      run_id: $run_id,
      compose_project: $project,
      status: "preflight",
      started_at: $started_at,
      requested_image: $image,
      tool_images: {},
      scenario: $scenario,
      topology_nodes: $nodes,
      fixture_record_limit: $records,
      timeout_seconds: $timeout_seconds,
      outage_seconds: $outage_seconds,
      artifact_limits: {
        fixture_records: $fixture_records,
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

# Fixes a mixed-instability experiment before the run alters anything: the validated action plan,
# the fixture records and the NSPL graph, each kept in the run directory with the digest a replay
# checks, and the limits the run judges. A seeded plan is generated here; a supplied or replayed one
# is copied. An invalid plan fails the run as a setup error before any container exists.
prepare_mixed_experiment() {
    local plan="${artifact_dir}/mixed/plan.json"
    mkdir -p "${artifact_dir}/mixed"
    local plan_source
    if [[ -n "${replay_dir}" ]]; then
        cp "${replay_dir}/mixed/plan.json" "${plan}"
        plan_source='replay'
    elif [[ -n "${mixed_plan_file}" ]]; then
        cp "${mixed_plan_file}" "${plan}"
        plan_source='file'
    else
        "${script_dir}/mixed-plan.sh" generate --seed "${mixed_seed}" --duration "${mixed_duration}" \
            --policy "${mixed_policy}" --coverage "${mixed_coverage}" --output "${plan}" \
            2>"${artifact_dir}/mixed/plan-generation.txt" \
            || setup_error "seed ${mixed_seed} selected no plan: $(tail -n 1 "${artifact_dir}/mixed/plan-generation.txt")"
        plan_source='seed'
    fi
    if ! "${script_dir}/mixed-plan.sh" validate --plan "${plan}" \
        --report "${artifact_dir}/mixed/plan-validation.json" 2>"${artifact_dir}/mixed/plan-validation.txt"; then
        setup_error "the action plan is invalid, so no container was altered: $(head -n 1 "${artifact_dir}/mixed/plan-validation.txt"); every reason is in mixed/plan-validation.json"
    fi
    mixed_seed="$(jq -r '.seed // empty' "${plan}")"
    mixed_policy="$(jq -r '.policy' "${plan}")"
    mixed_duration="$(jq -r '.duration_seconds' "${plan}")"
    local plan_end
    plan_end="$(jq -r '.steps[-1].at_seconds + .steps[-1].estimated_seconds' "${plan}")"
    if [[ -n "${replay_dir}" ]]; then
        cp "${replay_dir}/fixtures/input.ndjson" "${artifact_dir}/fixtures/input.ndjson"
        cp "${replay_dir}/fixtures/baseline.nspl" "${artifact_dir}/fixtures/baseline.nspl"
    else
        # The load produces one record a second until the run stops it, so the fixture lasts the
        # planned timeline and fifteen minutes of startup and final boundaries, and by default twice
        # the timeline for steps that outlast their estimates.
        local minimum_records=$((plan_end + 900))
        if [[ "${records_option_set}" == true ]]; then
            ((record_count >= minimum_records)) \
                || setup_error "${record_count} records cannot keep the load running through a plan that ends at ${plan_end}s; use at least ${minimum_records}"
        else
            record_count=$((2 * plan_end + 900))
            if ((record_count > max_records)); then
                record_count="${max_records}"
            fi
        fi
        jq -nc --arg run_id "${run_id}" --argjson count "${record_count}" \
            -f "${fixture_generator}" >"${artifact_dir}/fixtures/input.ndjson"
        cp "${script_dir}/fixtures/baseline.nspl" "${artifact_dir}/fixtures/baseline.nspl"
    fi
    [[ "$(wc -l <"${artifact_dir}/fixtures/input.ndjson")" -eq "${record_count}" ]] \
        || setup_error "the input fixture does not hold ${record_count} records"
    fixture_file="${artifact_dir}/fixtures/baseline.nspl"
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_manifest '.fixture_record_limit = $records
        | .mixed = {seed: (if $seed == "" then null else ($seed | tonumber) end), policy: $policy,
                    duration_seconds: ($duration | tonumber), coverage: $plan[0].coverage,
                    plan: {file: "mixed/plan.json", source: $source, sha256: $plan_sha256,
                           steps: ($plan[0].steps | length), planned_end_seconds: ($plan_end | tonumber)},
                    limits: {max_memory_bytes: $memory, max_recovery_backlog: $backlog, max_pending: $pending}}
        | .fixtures = {input: {file: "fixtures/input.ndjson", sha256: $input_sha256, records: $records},
                       graph: {file: "fixtures/baseline.nspl", sha256: $graph_sha256}}
        | .replay_of = $replay_of' \
        --argjson records "${record_count}" --arg seed "${mixed_seed}" --arg policy "${mixed_policy}" \
        --arg duration "${mixed_duration}" --slurpfile plan "${plan}" --arg source "${plan_source}" \
        --arg plan_sha256 "$(sha256_of "${plan}")" --arg plan_end "${plan_end}" \
        --argjson memory "${max_memory_bytes}" --argjson backlog "${max_recovery_backlog}" \
        --argjson pending "${max_pending}" \
        --arg input_sha256 "$(sha256_of "${artifact_dir}/fixtures/input.ndjson")" \
        --arg graph_sha256 "$(sha256_of "${fixture_file}")" --argjson replay_of "${replay_of}"
    printf 'mixed-instability plan: %s steps, %s policy, seed %s, ending at %ss of a %ss timeline\n' \
        "$(jq '.steps | length' "${plan}")" "${mixed_policy}" "${mixed_seed:-none}" "${plan_end}" "${mixed_duration}"
}

compose_args=(--project-name "${project_name}" --file "${compose_file}" --profile tools)
if [[ "${node_count}" == "3" ]]; then
    compose_args+=(--profile three-node)
fi
if [[ "${scenario}" != "baseline" ]]; then
    compose_args+=(--profile rolling)
fi
if [[ "${scenario}" == stateful ]]; then
    compose_args+=(--profile stateful)
elif [[ "${scenario}" == domain-time ]]; then
    compose_args+=(--profile paced)
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
    local teardown_started="${SECONDS}"
    if [[ "${scenario}" == mixed-instability ]]; then
        # The sampler and every pause the controller still waits for end before anything is healed.
        if declare -F mixed_stop_sampler >/dev/null; then
            mixed_stop_sampler
        fi
        if declare -F mixed_stop_pause_injectors >/dev/null; then
            mixed_stop_pause_injectors
        fi
        if declare -F mixed_summarize_interrupted >/dev/null; then
            mixed_summarize_interrupted
        fi
    fi
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
    if [[ -n "${cluster_restart_sampler_pid:-}" ]]; then
        kill "${cluster_restart_sampler_pid}" 2>/dev/null
        wait "${cluster_restart_sampler_pid}" 2>/dev/null
    fi
    if [[ "${fault}" == *-pause || "${scenario}" == mixed-instability ]]; then
        # A pause fault ends with its injector; one that outlived the controller is removed, and
        # every run-owned node it left paused runs again before anything else is captured.
        mapfile -t active_injectors < <(
            docker container ls --quiet \
                --filter "label=io.nervix.chaos.run=${run_id}" \
                --filter label=io.nervix.chaos.role=fault 2>/dev/null
        )
        if ((${#active_injectors[@]} > 0)); then
            timeout --foreground --kill-after=5s 20s docker container rm --force \
                "${active_injectors[@]}" >"${artifact_dir}/diagnostics/pause-injector-heal.txt" 2>&1
        fi
        local paused_id
        while IFS= read -r paused_id; do
            [[ -n "${paused_id}" ]] || continue
            if [[ "$(timeout --foreground --kill-after=5s 20s docker inspect --format '{{.State.Paused}}' "${paused_id}" 2>/dev/null)" == true ]]; then
                timeout --foreground --kill-after=5s 20s docker unpause "${paused_id}" \
                    >>"${artifact_dir}/diagnostics/target-unpause.txt" 2>&1
            fi
        done < <(docker container ls --quiet --filter "label=io.nervix.chaos.run=${run_id}" \
            --filter label=io.nervix.chaos.role=node 2>/dev/null)
    fi
    if [[ "${scenario}" == partition-recovery || "${scenario}" == degraded-links || "${scenario}" == former-owner-restart || "${fault}" == *-partition || "${scenario}" == mixed-instability ]]; then
        local heal_prefix=network
        if [[ "${scenario}" == partition-recovery ]]; then
            heal_prefix=partition
        elif [[ "${scenario}" == former-owner-restart || "${fault}" == *-partition ]]; then
            heal_prefix=isolation
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
    # A recovery case can end while it holds nodes stopped; start them after every heal so retained
    # resources and diagnostics never leave a node down.
    local stopped_id
    for stopped_id in ${recovery_stopped_ids[@]+"${recovery_stopped_ids[@]}"}; do
        timeout --foreground --kill-after=5s 20s docker inspect "${stopped_id}" \
            >"${artifact_dir}/diagnostics/stopped-node-${stopped_id:0:12}.json" 2>&1
        if [[ "$(timeout --foreground --kill-after=5s 20s docker inspect --format '{{.State.Running}}' "${stopped_id}" 2>/dev/null)" == false ]]; then
            timeout --foreground --kill-after=5s 30s docker start "${stopped_id}" \
                >>"${artifact_dir}/diagnostics/stopped-node-heal.txt" 2>&1
        fi
    done
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
    # The covered recording holds the creation of every run-owned container, so it shows whether
    # each came from the Nervix image or a pinned tool image that the manifest records.
    if [[ "${run_event_recording_covered}" == true ]]; then
        local images_status=0
        "${script_dir}/verify-docker-events.sh" images \
            --recording "${artifact_dir}/diagnostics/docker-events.ndjson" \
            --manifest "${artifact_dir}/manifest.json" \
            --output "${artifact_dir}/results/container-images.json" || images_status=$?
        if [[ "${status}" -eq 0 && "${images_status}" -ne 0 ]]; then
            status=1
            failure_category=controller
            current_phase='container image identity'
            printf '%s\n' 'controller failure: every run-owned container must come from a recorded image; see results/container-images.json' >&2
        fi
    fi

    if [[ "${status}" -ne 0 && ( "${scenario}" == *-crash || "${scenario}" == pause-resume || "${scenario}" == partition-recovery || "${scenario}" == degraded-links || "${recovery_scenario}" == true ) ]]; then
        local reproducer_image="${image_id:-${image_ref}}"
        if [[ "${image_ref}" == *@sha256:* ]]; then
            reproducer_image="${image_ref}"
        elif [[ -n "${image_digest}" ]]; then
            reproducer_image="${image_digest%%,*}"
        fi
        local reproducer
        if [[ "${scenario}" == mixed-instability ]]; then
            # Once the run directory records the deployment, it holds everything a replay reuses.
            # Before then a replay of the run's own source reproduces a failed replay, and a run
            # command reproduces any other run with its plan or with what selects it, together with
            # every other option that shaped it.
            if mixed_replayable; then
                reproducer="$(printf 'just chaos replay %q' "${artifact_dir}")"
            elif [[ -n "${replay_dir}" ]]; then
                reproducer="$(printf 'just chaos replay %q' "${replay_dir}")"
            else
                if [[ -s "${artifact_dir}/mixed/plan.json" ]]; then
                    reproducer="$(printf 'just chaos run mixed-instability --image %q --plan %q' \
                        "${reproducer_image}" "${artifact_dir}/mixed/plan.json")"
                else
                    reproducer="$(printf 'just chaos run mixed-instability --image %q --seed %q --duration %q --policy %q --coverage %q' \
                        "${reproducer_image}" "${mixed_seed}" "${mixed_duration}" "${mixed_policy}" "${mixed_coverage}")"
                fi
                if [[ "${records_option_set}" == true ]]; then
                    reproducer+="$(printf ' --records %q' "${record_count}")"
                fi
                if [[ "${timeout_option_set}" == true ]]; then
                    reproducer+="$(printf ' --timeout %q' "${overall_timeout}")"
                fi
                if [[ "${limit_option_set}" == true ]]; then
                    reproducer+="$(printf ' --max-memory-bytes %q --max-recovery-backlog %q --max-pending %q' \
                        "${max_memory_bytes}" "${max_recovery_backlog}" "${max_pending}")"
                fi
            fi
        elif [[ "${scenario}" == pause-resume ]]; then
            reproducer="$(printf 'just chaos run %q --image %q --records %q' \
                "${scenario}" "${reproducer_image}" "${record_count}")"
        elif [[ "${scenario}" == partition-recovery ]]; then
            reproducer="$(printf 'just chaos run %q --image %q --records %q --case %q --partition-seconds %q' \
                "${scenario}" "${reproducer_image}" "${record_count}" "${partition_case}" "${partition_seconds}")"
        elif [[ "${scenario}" == stale-follower ]]; then
            reproducer="$(printf 'just chaos run %q --image %q --records %q' \
                "${scenario}" "${reproducer_image}" "${record_count}")"
        elif [[ "${scenario}" == former-owner-restart ]]; then
            reproducer="$(printf 'just chaos run %q --image %q --records %q --isolation-seconds %q' \
                "${scenario}" "${reproducer_image}" "${record_count}" "${isolation_seconds}")"
        elif [[ "${scenario}" == stateful || "${scenario}" == domain-time ]]; then
            reproducer="$(printf 'just chaos run %q --image %q --nodes %q --fault %q --records %q' \
                "${scenario}" "${reproducer_image}" "${node_count}" "${fault}" "${record_count}")"
            if [[ "${outage_option_set}" == true ]]; then
                reproducer+="$(printf ' --outage-seconds %q' "${outage_seconds}")"
            fi
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
            results/ledger.json results/ledger.txt results/load-pacing.json \
            results/state-load-pacing.json results/paced-load-pacing.json \
            results/container-images.json; do
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
        if [[ "${recovery_scenario}" == true ]]; then
            for evidence_path in "results/${scenario}-progress.json" results/recovery-findings.ndjson \
                diagnostics/isolation-heal.txt diagnostics/network-preflight.txt \
                diagnostics/stopped-node-heal.txt; do
                if [[ -s "${artifact_dir}/${evidence_path}" ]]; then
                    evidence_paths+=("${evidence_path}")
                fi
            done
            for evidence_path in manifest.json fixtures/input.ndjson fixtures/baseline.nspl \
                mixed/plan-validation.json results/mixed-trace.json results/mixed-resources.json \
                results/mixed-instability.json; do
                if [[ -s "${artifact_dir}/${evidence_path}" ]]; then
                    evidence_paths+=("${evidence_path}")
                fi
            done
            if [[ -d "${artifact_dir}/mixed/steps" ]]; then
                while IFS= read -r evidence_path; do
                    evidence_paths+=("${evidence_path#"${artifact_dir}/"}")
                done < <(find "${artifact_dir}/mixed/steps" -mindepth 2 -maxdepth 2 -type f \
                    \( -name 'result.json' -o -name 'recovered.json' -o -name 'hold-*.json' -o -name 'nodes.json' \) \
                    -size +0c 2>/dev/null | sort)
            fi
            local case_directory
            for case_directory in stale former-owner restart stateful domain-time mixed; do
                [[ -d "${artifact_dir}/${case_directory}" ]] || continue
                while IFS= read -r evidence_path; do
                    evidence_paths+=("${evidence_path#"${artifact_dir}/"}")
                done < <(find "${artifact_dir}/${case_directory}" -maxdepth 1 -type f \
                    \( -name '*.json' -o -name '*.ndjson' -o -name 'pumba*.txt' -o -name '*.log' \) \
                    -size +0c 2>/dev/null | sort)
            done
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
            --argjson tool_images "$(jq -c '.tool_images' "${artifact_dir}/manifest.json")" \
            --argjson exit_code "${status}" \
            --argjson evidence "${evidence_json}" \
            '{category:$category,phase:$phase,exit_code:$exit_code,image_id:$image_id,image_reference:$image_reference,tool_images:$tool_images,reproducer:$reproducer,evidence:$evidence}' \
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
        --arg setup_error "${setup_error_message}" \
        --argjson exit_code "${status}" \
        --argjson resources_retained "${retained}" \
        --argjson teardown_seconds "$((SECONDS - teardown_started))" \
        --argjson teardown_reserve_seconds "${teardown_reserve_seconds}" \
        '.status = $status
         | .final_phase = $phase
         | .finished_at = $finished_at
         | .exit_code = $exit_code
         | .resources_retained = $resources_retained
         | .teardown_seconds = $teardown_seconds
         | .teardown_reserve_seconds = $teardown_reserve_seconds
         | if $signal == "" then . else .signal = $signal end
         | if $setup_error == "" then . else .setup_error = $setup_error end' \
        "${artifact_dir}/manifest.json" >"${manifest_tmp}" \
        && mv "${manifest_tmp}" "${artifact_dir}/manifest.json"

    printf '\nchaos %s %s (exit %d)\n' "${scenario}" "${final_status}" "${status}"
    printf 'artifacts: %s\n' "${artifact_dir}"
    if [[ "${scenario}" == mixed-instability ]] && mixed_replayable; then
        printf 'replay: just chaos replay %s\n' "${artifact_dir}"
    fi
    if [[ "${retained}" == true ]]; then
        printf 'cleanup: just chaos cleanup --run-id %s\n' "${run_id}"
    fi
    exit "${status}"
}

# True once the run's manifest records the deployment, the last part of the experiment a replay needs.
mixed_replayable() {
    jq -e '.mixed.deployment.nodes | type == "object"' "${artifact_dir}/manifest.json" >/dev/null 2>&1
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

# Makes one pinned tool image local and records its reference and image ID under TOOL in the
# manifest. A missing image is pulled once within a bound; its digest-pinned reference cannot pull
# different content, and a pull that fails is a setup failure that names the image.
resolve_tool_image() {
    local tool="$1"
    local reference="$2"
    local pull_log="diagnostics/tool-image-pull-${tool}.txt"
    if ! run_bounded 20 docker image inspect "${reference}" >/dev/null 2>&1; then
        printf 'pulling pinned %s image %s\n' "${tool}" "${reference}"
        run_bounded 180 docker pull "${reference}" >"${artifact_dir}/${pull_log}" 2>&1 \
            || setup_error "pinned ${tool} image '${reference}' could not be pulled; see ${pull_log}"
    fi
    local tool_image_id
    tool_image_id="$(run_bounded 30 docker image inspect --format '{{.Id}}' "${reference}")" \
        || setup_error "pinned ${tool} image '${reference}' is local but cannot be inspected"
    [[ "${tool_image_id}" =~ ^sha256:[a-f0-9]{64}$ ]] \
        || setup_error "pinned ${tool} image '${reference}' did not resolve to an immutable local image ID"
    tool_image_ids["${tool}"]="${tool_image_id}"
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_manifest '.tool_images[$tool] = {reference: $reference, image_id: $image_id}' \
        --arg tool "${tool}" --arg reference "${reference}" --arg image_id "${tool_image_id}"
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
        "${CHAOS_PROBE_IMAGE}" "$((overall_timeout + teardown_reserve_seconds))" || start_status=$?
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

# The probe container's script that requires every listener of the node its first argument names to
# answer.
node_listener_probe="
    nc -z -w 2 \"\$1\" 47391
    nc -z -w 2 \"\$1\" 47395
    wget -q -T 3 -O /dev/null \"http://\$1:9090/livez\"
    wget -q -T 3 -O /dev/null \"http://\$1:9090/metrics\"
    wget -q -T 3 -O /dev/null \"http://\$1:47420/console/\"
"

# Requires every listener of HOSTNAME to answer and its /readyz to report a known leader.
probe_node() {
    local hostname="$1"
    compose run --rm --no-deps probe sh -eu -c "${node_listener_probe}
        wget -q -T 3 -O /dev/null \"http://\$1:9090/readyz\"
    " -- "${hostname}" >/dev/null 2>&1
}

# Requires every listener of HOSTNAME to answer, whatever its /readyz reports: a node can know no
# leader for a moment during an election, which loss on a leader's link can start.
probe_node_listeners() {
    local hostname="$1"
    compose run --rm --no-deps probe sh -eu -c "${node_listener_probe}" -- "${hostname}" >/dev/null 2>&1
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

# Records a continuous load the scenario starts: the Compose SERVICE that runs it, the TOPIC it
# produces to, its interval and where the run will leave its pacing verdict.
manifest_load() {
    local service="$1" topic="$2" interval_ms="$3"
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_manifest '.loads += [{service: $service, topic: $topic, interval_ms: $interval_ms, pacing: ("results/" + $service + "-pacing.json")}]' \
        --arg service "${service}" --arg topic "${topic}" --argjson interval_ms "${interval_ms}"
}

# Reads the create time the broker stored for each of the first COUNT records of TOPIC, in offset
# order, and judges the gaps between them against the INTERVAL_MS of the Compose SERVICE that
# produced them. The evidence is traffic/SERVICE-timestamps.txt and results/SERVICE-pacing.json. A
# verdict that did not pass is kept and applied after the ledger, so a run whose load was not paced
# still keeps its delivery evidence. Evidence that cannot be read is a controller failure at once.
load_pacing_failures=()
record_load_pacing() {
    local service="$1" topic="$2" count="$3" interval_ms="$4"
    local timestamps="${artifact_dir}/traffic/${service}-timestamps.txt"
    local scenario_category="${failure_category}"
    failure_category=controller
    kcat -q -b broker:9092 -C -t "${topic}" -p 0 -o beginning -c "${count}" -f '%T %o\n' \
        >"${timestamps}" 2>"${artifact_dir}/traffic/${service}-timestamps.stderr"
    local timestamp_count
    timestamp_count="$(wc -l <"${timestamps}")"
    if [[ "${timestamp_count}" -ne "${count}" ]]; then
        printf '%s timestamps cover %s records, expected %s\n' "${topic}" "${timestamp_count}" "${count}" >&2
        return 1
    fi
    failure_category="${scenario_category}"
    "${script_dir}/verify-load-pacing.sh" "${timestamps}" "${interval_ms}" \
        "${artifact_dir}/results/${service}-pacing.json" \
        >"${artifact_dir}/results/${service}-pacing.txt" 2>&1 \
        || load_pacing_failures+=("${service}")
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

# A replay starts exactly the Nervix image its run recorded: by that image ID when it is local, or
# pulled through a recorded repository digest that resolves to the same ID.
resolve_recorded_image() {
    if image_id="$(run_bounded 30 docker image inspect --format '{{.Id}}' "${replay_image_id}" 2>/dev/null)" \
        && [[ "${image_id}" == "${replay_image_id}" ]]; then
        return 0
    fi
    local candidates=()
    IFS=, read -r -a candidates <<<"${replay_repo_digests}"
    if [[ "${image_ref}" == *@sha256:* ]]; then
        candidates+=("${image_ref}")
    fi
    local candidate
    for candidate in ${candidates[@]+"${candidates[@]}"}; do
        [[ -n "${candidate}" ]] || continue
        printf 'the recorded image is not local; attempting bounded pull: %s\n' "${candidate}"
        if run_bounded 180 docker pull "${candidate}" >>"${artifact_dir}/image-pull.txt" 2>&1 \
            && image_id="$(run_bounded 30 docker image inspect --format '{{.Id}}' "${candidate}" 2>/dev/null)" \
            && [[ "${image_id}" == "${replay_image_id}" ]]; then
            return 0
        fi
    done
    setup_error "the recorded Nervix image ${replay_image_id} is unavailable: it is not local and no recorded repository digest pulled it"
}

if [[ "${scenario}" == mixed-instability ]]; then
    phase "action plan and fixtures"
    prepare_mixed_experiment
fi

phase "preflight"
run_bounded 30 docker info >/dev/null \
    || setup_error 'Docker daemon is unavailable'
run_bounded 30 docker compose version >"${artifact_dir}/docker-compose-version.txt"
select_run_network

# Every tool image the scenario starts is local and recorded before the run creates a container.
resolve_tool_image kafka "${CHAOS_KAFKA_IMAGE}"
resolve_tool_image kcat "${CHAOS_KCAT_IMAGE}"
resolve_tool_image probe "${CHAOS_PROBE_IMAGE}"
if [[ "${scenario}" != "baseline" && "${scenario}" != backup ]]; then
    resolve_tool_image pumba "${CHAOS_PUMBA_IMAGE}"
    pumba_image_id="${tool_image_ids[pumba]}"
fi
if [[ "${scenario}" == partition-recovery || "${scenario}" == degraded-links || "${scenario}" == former-owner-restart || "${fault}" == *-partition || "${scenario}" == mixed-instability ]]; then
    resolve_tool_image nettools "${CHAOS_NETTOOLS_IMAGE}"
fi
for tool in "${!replay_tool_image_ids[@]}"; do
    [[ "${tool_image_ids[${tool}]:-}" == "${replay_tool_image_ids[${tool}]}" ]] \
        || setup_error "the recorded ${tool} image resolved to ${tool_image_ids[${tool}]:-nothing}, not the recorded ${replay_tool_image_ids[${tool}]}"
done

if [[ -n "${replay_dir}" ]]; then
    resolve_recorded_image
elif ! image_id="$(run_bounded 30 docker image inspect --format '{{.Id}}' "${image_ref}" 2>/dev/null)"; then
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
    --format '{{join .RepoDigests ","}}' "${image_id}" 2>/dev/null || true)"
# The dollars in this jq filter are jq variables, not shell expansion.
# shellcheck disable=SC2016
update_manifest '.resolved_image_id = $image_id | .resolved_repo_digests = $digests' \
    --arg image_id "${image_id}" --arg digests "${image_digest}"
export NERVIX_IMAGE="${image_id}"
run_bounded 30 docker run --rm --entrypoint /bin/sh "${image_id}" -eu -c \
    'test -x /usr/local/bin/nervix-server; test -x /usr/local/bin/nervix-cli' \
    || setup_error "image '${image_ref}' does not package executable nervix-server and nervix-cli binaries"

start_run_event_recording
if [[ "${scenario}" != "baseline" && "${scenario}" != backup ]]; then
    [[ -S /var/run/docker.sock ]] \
        || setup_error "${scenario} requires a local /var/run/docker.sock for Pumba"
    run_bounded 30 docker run --rm \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --version >"${artifact_dir}/pumba-version.txt"
    pumba_preflight=(stop --time 60 impossible-chaos-preflight-target)
    if [[ "${scenario}" == *-crash || "${scenario}" == former-owner-restart || "${scenario}" == cluster-restart \
        || "${fault}" == *-crash || "${fault}" == cluster-restart ]]; then
        pumba_preflight=(kill --signal SIGKILL impossible-chaos-preflight-target)
    elif [[ "${scenario}" == pause-resume || "${fault}" == *-pause ]]; then
        pumba_preflight=(pause --duration 1s impossible-chaos-preflight-target)
    elif [[ "${scenario}" == partition-recovery || "${scenario}" == degraded-links || "${fault}" == *-partition \
        || "${scenario}" == mixed-instability ]]; then
        pumba_preflight=(netem --duration 1s --target 192.0.2.1 loss --percent 100 impossible-chaos-preflight-target)
    fi
    run_bounded 30 docker run --rm \
        --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
        "${pumba_image_id}" --dry-run --label io.nervix.chaos.run="${run_id}" \
        "${pumba_preflight[@]}" \
        >"${artifact_dir}/diagnostics/pumba-docker-preflight.txt" 2>&1 \
        || setup_error 'Pumba cannot access the selected Docker daemon'
fi
if [[ "${scenario}" == partition-recovery || "${scenario}" == degraded-links || "${scenario}" == former-owner-restart || "${fault}" == *-partition || "${scenario}" == mixed-instability ]]; then
    # Pumba's netem and iptables faults must take effect on this worker's kernel and heal on SIGTERM.
    run_bounded 120 "${script_dir}/network-faults.sh" preflight --run-id "${run_id}" \
        --pumba "${pumba_image_id}" --nettools "${CHAOS_NETTOOLS_IMAGE}" \
        --probe "${CHAOS_PROBE_IMAGE}" \
        --output "${artifact_dir}/diagnostics/network-preflight" \
        >"${artifact_dir}/diagnostics/network-preflight.txt" 2>&1 \
        || setup_error "this Docker worker cannot install and heal netem/iptables faults; see ${artifact_dir}/diagnostics/network-preflight.txt"
fi

update_manifest '.status = "running"'
if [[ "${scenario}" != "baseline" && "${scenario}" != backup ]]; then
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_manifest '.pumba_image_id = $image_id | .pumba_image = $image' \
        --arg image_id "${pumba_image_id}" --arg image "${CHAOS_PUMBA_IMAGE}"
fi
if [[ "${scenario}" != baseline ]]; then
    manifest_load load chaos_input "${load_interval_ms}"
fi
if [[ "${scenario}" == stateful ]]; then
    manifest_load state-load chaos_state_input "${CHAOS_STATE_LOAD_INTERVAL_MS}"
elif [[ "${scenario}" == domain-time ]]; then
    manifest_load paced-load chaos_paced_input "${CHAOS_PACED_LOAD_INTERVAL_MS}"
fi
if [[ "${scenario}" == partition-recovery ]]; then
    # shellcheck disable=SC2016
    update_manifest '.partition = {case: $case, minimum_window_seconds: ($seconds | tonumber)}' \
        --arg case "${partition_case}" --arg seconds "${partition_seconds}"
fi
if [[ "${scenario}" == stale-follower ]]; then
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_manifest '.recovery = {snapshot_entry_threshold: $entries, covered_log_entries_retained: $covered}' \
        --argjson entries "${CHAOS_RAFT_SNAPSHOT_ENTRY_THRESHOLD}" \
        --argjson covered "${CHAOS_RAFT_COVERED_LOG_ENTRIES_RETAINED}"
elif [[ "${scenario}" == former-owner-restart ]]; then
    # shellcheck disable=SC2016
    update_manifest '.recovery = {minimum_isolation_seconds: $seconds}' --argjson seconds "${isolation_seconds}"
elif [[ "${scenario}" == cluster-restart ]]; then
    # shellcheck disable=SC2016
    update_manifest '.recovery = {minimum_outage_seconds: $seconds}' --argjson seconds "${outage_seconds}"
elif [[ "${scenario}" == stateful || "${scenario}" == domain-time ]]; then
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_manifest '.fault = $fault | .deployment = {replica_count: ($replicas | tonumber), state_snapshot_interval: $interval}' \
        --arg fault "${fault}" --arg replicas "${CHAOS_REPLICA_COUNT:-0}" \
        --arg interval "${CHAOS_STATE_SNAPSHOT_INTERVAL:-30s}"
fi
if [[ "${scenario}" == degraded-links ]]; then
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_manifest '.degradation = $settings' \
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
# The self-check drives Docker as a scenario does, so the worker's load stretches it: it takes half
# a minute on a quiet worker and took 247 seconds beside three chaos runs on a loaded one. The bound
# only ends a self-check that hangs.
run_bounded 600 "${script_dir}/tests/self-test.sh" \
    >"${artifact_dir}/results/verifier-self-test.txt" 2>&1

phase "TLS generation"
generate_tls

phase "Compose validation"
compose config --quiet
if [[ "${scenario}" == mixed-instability ]]; then
    # The node settings and load interval Compose renders are part of the experiment, so a replay
    # deploys them again and refuses to start when its rendering differs.
    deployment="$(compose config --format json | jq -c '
        {nodes: (.services["nervix-1"].environment
                 | with_entries(select(.key | test("^NERVIX_(RAFT_|NODE_UNAVAILABILITY_TIMEOUT$|REPLICA_COUNT$|STATE_SNAPSHOT_INTERVAL$)")))),
         load_interval_ms: (.services.load.environment.CHAOS_LOAD_INTERVAL_MS | tonumber)}')"
    if [[ -n "${replay_dir}" ]]; then
        jq -e --argjson rendered "${deployment}" '.mixed.deployment == $rendered' \
            "${replay_dir}/manifest.json" >/dev/null \
            || setup_error "this controller renders the deployment ${deployment}, not the one ${replay_dir} recorded"
    fi
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_manifest '.mixed.deployment = $deployment' --argjson deployment "${deployment}"
fi
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
scenario_topics=()
if [[ "${scenario}" == stateful ]]; then
    scenario_topics=(chaos_state_input chaos_profile_input chaos_unique_output chaos_window_output
        chaos_enriched_output chaos_counted_output)
elif [[ "${scenario}" == domain-time ]]; then
    scenario_topics=(chaos_paced_input chaos_paced_output)
fi
for scenario_topic in ${scenario_topics[@]+"${scenario_topics[@]}"}; do
    broker_admin /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server broker:9092 \
        --create --topic "${scenario_topic}" --partitions 1 --replication-factor 1 \
        >"${artifact_dir}/public/create-topic-${scenario_topic}.txt"
done

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
if [[ "${scenario}" == backup ]]; then
    # The backup scenario needs source positions owned by the domain archive. Other chaos
    # scenarios retain their consumer-group boundary.
    [[ "${nspl_fixture}" == *'OFFSET BY CONSUMER GROUP chaos_baseline'* ]] \
        || setup_error 'the backup fixture no longer contains its expected Kafka offset clause'
    nspl_fixture="${nspl_fixture/OFFSET BY CONSUMER GROUP chaos_baseline/OFFSET BY DOMAIN}"
fi
cli_command 'CREATE UNPACED DOMAIN chaos_baseline;' \
    >"${artifact_dir}/public/create-domain.txt" 2>&1
if [[ "${scenario}" == stateful ]]; then
    # The WASM guest is a prebuilt module in WebAssembly text, which the packaged server loads
    # directly; the run uploads exactly that file and records its identity.
    mkdir -p "${artifact_dir}/fixtures/wasm/processors"
    cp "${script_dir}/fixtures/wasm/processors/branch-counter.wat" \
        "${artifact_dir}/fixtures/wasm/processors/branch-counter.wat"
    chmod 0755 "${artifact_dir}/fixtures/wasm" "${artifact_dir}/fixtures/wasm/processors"
    chmod 0644 "${artifact_dir}/fixtures/wasm/processors/branch-counter.wat"
    wasm_fixture_digest="$(openssl dgst -sha256 -r \
        "${artifact_dir}/fixtures/wasm/processors/branch-counter.wat" | awk '{ print $1 }')"
    [[ "${wasm_fixture_digest}" =~ ^[a-f0-9]{64}$ ]] \
        || setup_error 'the WASM fixture digest could not be computed'
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_manifest '.fixtures.wasm = {resource: "chaos_counter_guest", resource_version: 1,
        file: "processors/branch-counter.wat", format: "WebAssembly text", sha256: $digest,
        bytes: ($bytes | tonumber), source: "scripts/chaos/fixtures/wasm/processors/branch-counter.wat"}' \
        --arg digest "${wasm_fixture_digest}" \
        --arg bytes "$(wc -c <"${artifact_dir}/fixtures/wasm/processors/branch-counter.wat")"
    domain_cli_command 'CREATE RESOURCE chaos_counter_guest;' \
        >"${artifact_dir}/public/create-wasm-resource.txt" 2>&1
    compose run --rm --no-deps -v "${artifact_dir}/fixtures/wasm:/chaos-wasm:ro" admin \
        nervix-cli --server "http://${cli_host}:47391" --domain chaos_baseline \
        --password "${CHAOS_PASSWORD}" \
        --command "UPLOAD RESOURCE chaos_counter_guest VERSION '/chaos-wasm';" \
        >"${artifact_dir}/public/upload-wasm-resource.txt" 2>&1
    grep -Fxq 'uploaded resource version 1' "${artifact_dir}/public/upload-wasm-resource.txt" \
        || setup_error 'the packaged CLI did not upload the WASM fixture as resource version 1'
fi
domain_cli_command "${nspl_fixture}" \
    >"${artifact_dir}/public/configure-nspl.txt" 2>&1
if [[ "${scenario}" == stateful ]]; then
    domain_cli_command "$(<"${script_dir}/fixtures/stateful.nspl")" \
        >"${artifact_dir}/public/configure-stateful-nspl.txt" 2>&1
fi
domain_cli_command 'START;' >"${artifact_dir}/public/start-domain.txt" 2>&1
if [[ "${scenario}" == domain-time ]]; then
    cli_command 'CREATE PACED DOMAIN chaos_paced WITH PERIOD 1s SKEW 1s;' \
        >"${artifact_dir}/public/create-paced-domain.txt" 2>&1
    run_cli chaos_paced "$(<"${script_dir}/fixtures/paced.nspl")" \
        >"${artifact_dir}/public/configure-paced-nspl.txt" 2>&1
    run_cli chaos_paced 'START AT NOW TIME RATE 4.0;' \
        >"${artifact_dir}/public/start-paced-domain.txt" 2>&1
fi

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
    elif [[ "${scenario}" == backup ]]; then
        # shellcheck source=backup-scenario.sh
        source "${script_dir}/backup-scenario.sh"
        run_backup_scenario
    elif [[ "${scenario}" == stale-follower ]]; then
        # shellcheck source=stale-follower-scenario.sh
        source "${script_dir}/stale-follower-scenario.sh"
        run_stale_follower
    elif [[ "${scenario}" == former-owner-restart ]]; then
        # shellcheck source=former-owner-scenario.sh
        source "${script_dir}/former-owner-scenario.sh"
        run_former_owner_restart
    elif [[ "${scenario}" == cluster-restart ]]; then
        # shellcheck source=cluster-restart-scenario.sh
        source "${script_dir}/cluster-restart-scenario.sh"
        run_cluster_restart
    elif [[ "${scenario}" == stateful ]]; then
        # shellcheck source=stateful-scenario.sh
        source "${script_dir}/stateful-scenario.sh"
        run_stateful
    elif [[ "${scenario}" == domain-time ]]; then
        # shellcheck source=domain-time-scenario.sh
        source "${script_dir}/domain-time-scenario.sh"
        run_domain_time
    elif [[ "${scenario}" == mixed-instability ]]; then
        # shellcheck source=mixed-scenario.sh
        source "${script_dir}/mixed-scenario.sh"
        run_mixed_instability
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

if [[ "${scenario}" != baseline ]]; then
    phase "load pacing"
    record_load_pacing load chaos_input "${input_end}" "${load_interval_ms}"
fi

phase "offset and output boundaries"
if [[ "${scenario}" != backup ]]; then
    wait_for "Nervix consumer offsets at source boundary ${input_end}" 120 \
        consumer_offsets_at_end "${input_end}"
    cp "${artifact_dir}/traffic/consumer-group.attempt.txt" \
        "${artifact_dir}/traffic/consumer-group-final.txt"
fi
output_wait_timed_out=false
if [[ "${scenario}" == *-crash || "${scenario}" == pause-resume || "${scenario}" == partition-recovery || "${scenario}" == degraded-links || "${scenario}" == backup || "${recovery_scenario}" == true ]]; then
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
if [[ "${scenario}" == *-crash || "${scenario}" == pause-resume || "${scenario}" == partition-recovery || "${scenario}" == degraded-links || "${scenario}" == backup || "${recovery_scenario}" == true ]]; then
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
if [[ "${scenario}" != backup ]]; then
    broker_admin /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server broker:9092 --group chaos_baseline --describe \
        >"${artifact_dir}/public/consumer-group-final.txt"
fi

tool_images_json="$(jq -c '.tool_images' "${artifact_dir}/manifest.json")"
if [[ "${scenario}" == "baseline" ]]; then
    jq -n \
        --arg run_id "${run_id}" \
        --arg image_id "${image_id}" \
        --arg image_digest "${image_digest}" \
        --argjson tool_images "${tool_images_json}" \
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
          tool_images: $tool_images,
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
elif [[ "${scenario}" == backup ]]; then
    jq -n \
        --arg run_id "${run_id}" --arg image_id "${image_id}" \
        --argjson nodes "${node_count}" \
        --argjson accepted_records "${input_end}" --argjson observed_records "${output_end}" \
        --slurpfile progress "${artifact_dir}/results/backup-progress.json" \
        --slurpfile ledger "${artifact_dir}/results/ledger.json" \
        '{verdict:"pass",run_id:$run_id,image_id:$image_id,topology_nodes:$nodes,
          accepted_source_records:$accepted_records,observed_output_records:$observed_records,
          replay_duplicates:$ledger[0].duplicate_records,source_offsets_committed:true,
          ledger:"results/ledger.json",remote_path:"results/remote-path.json",
          backup:"backup/domain.nvxb",progress:$progress[0]}' \
        >"${artifact_dir}/results/backup.json"
elif [[ "${scenario}" == "rolling-restart" ]]; then
    jq -n \
        --arg run_id "${run_id}" \
        --arg image_id "${image_id}" \
        --argjson tool_images "${tool_images_json}" \
        --argjson nodes "${node_count}" \
        --argjson accepted_records "${input_end}" \
        --argjson observed_records "${output_end}" \
        --slurpfile progress "${artifact_dir}/results/rolling-progress.json" \
        '{verdict:"pass",run_id:$run_id,image_id:$image_id,tool_images:$tool_images,topology_nodes:$nodes,accepted_source_records:$accepted_records,observed_output_records:$observed_records,source_offsets_committed:true,ledger:"results/ledger.json",remote_path:"results/remote-path.json",progress:$progress[0]}' \
        >"${artifact_dir}/results/rolling-restart.json"
elif [[ "${scenario}" == "pause-resume" ]]; then
    jq -n \
        --arg run_id "${run_id}" \
        --arg image_id "${image_id}" \
        --argjson tool_images "${tool_images_json}" \
        --argjson accepted_records "${input_end}" \
        --argjson observed_records "${output_end}" \
        --slurpfile progress "${artifact_dir}/results/pause-progress.json" \
        --slurpfile ledger "${artifact_dir}/results/ledger.json" \
        '{verdict:"pass",run_id:$run_id,image_id:$image_id,tool_images:$tool_images,topology_nodes:3,accepted_source_records:$accepted_records,observed_output_records:$observed_records,replay_duplicates:$ledger[0].duplicate_records,source_offsets_committed:true,ledger:"results/ledger.json",remote_path:"results/remote-path.json",progress:$progress[0]}' \
        >"${artifact_dir}/results/pause-resume.json"
elif [[ "${scenario}" == "partition-recovery" ]]; then
    jq -n \
        --arg run_id "${run_id}" \
        --arg image_id "${image_id}" \
        --argjson tool_images "${tool_images_json}" \
        --arg case "${partition_case}" \
        --argjson accepted_records "${input_end}" \
        --argjson observed_records "${output_end}" \
        --slurpfile progress "${artifact_dir}/results/partition-progress.json" \
        --slurpfile ledger "${artifact_dir}/results/ledger.json" \
        --slurpfile findings <(cat "${artifact_dir}/results/partition-findings.ndjson" 2>/dev/null || true) \
        '{verdict:(if ($findings | length) == 0 then "pass" else "fail" end),run_id:$run_id,image_id:$image_id,tool_images:$tool_images,case:$case,topology_nodes:3,accepted_source_records:$accepted_records,observed_output_records:$observed_records,replay_duplicates:$ledger[0].duplicate_records,source_offsets_committed:true,ledger:"results/ledger.json",remote_path:"results/remote-path.json",findings:$findings,progress:$progress[0]}' \
        >"${artifact_dir}/results/partition-recovery.json"
    if [[ "$(jq -r '.verdict' "${artifact_dir}/results/partition-recovery.json")" != pass ]]; then
        failure_category=product
        current_phase="partition product findings"
        printf 'partition-recovery recorded %d product finding(s):\n' \
            "$(jq '.findings | length' "${artifact_dir}/results/partition-recovery.json")" >&2
        jq -r '.findings[] | "- \(.case): \(.message)"' "${artifact_dir}/results/partition-recovery.json" >&2
        exit 1
    fi
elif [[ "${scenario}" == mixed-instability ]]; then
    jq -n \
        --arg run_id "${run_id}" \
        --arg image_id "${image_id}" \
        --argjson tool_images "${tool_images_json}" \
        --argjson accepted_records "${input_end}" \
        --argjson observed_records "${output_end}" \
        --slurpfile manifest "${artifact_dir}/manifest.json" \
        --slurpfile trace "${artifact_dir}/results/mixed-trace.json" \
        --slurpfile resources "${artifact_dir}/results/mixed-resources.json" \
        --slurpfile ledger "${artifact_dir}/results/ledger.json" \
        --slurpfile findings "${artifact_dir}/results/recovery-findings.ndjson" \
        '{verdict: (if $trace[0].verdict != "complete" or $resources[0].verdict == "gapped" then "incomplete"
                    elif ($findings | length) > 0 then "fail" else "pass" end),
          run_id: $run_id, image_id: $image_id, tool_images: $tool_images,
          seed: $manifest[0].mixed.seed, policy: $manifest[0].mixed.policy,
          plan: $manifest[0].mixed.plan, replay_of: $manifest[0].replay_of, topology_nodes: 3,
          coverage: $trace[0].coverage, action_trace: $trace[0].verdict, resources: $resources[0].verdict,
          accepted_source_records: $accepted_records, observed_output_records: $observed_records,
          replay_duplicates: $ledger[0].duplicate_records, source_offsets_committed: true,
          ledger: "results/ledger.json", remote_path: "results/remote-path.json", findings: $findings,
          progress: "results/mixed-instability-progress.json"}' \
        >"${artifact_dir}/results/mixed-instability.json"
    case "$(jq -r '.verdict' "${artifact_dir}/results/mixed-instability.json")" in
        incomplete)
            failure_category=controller
            current_phase='mixed-instability evidence'
            printf 'mixed-instability evidence is incomplete: action trace %s, resource sampling %s; see results/mixed-trace.json and results/mixed-resources.json\n' \
                "$(jq -r '.action_trace' "${artifact_dir}/results/mixed-instability.json")" \
                "$(jq -r '.resources' "${artifact_dir}/results/mixed-instability.json")" >&2
            exit 1
            ;;
        fail)
            failure_category=product
            current_phase='mixed-instability product findings'
            printf 'mixed-instability recorded %d product finding(s):\n' \
                "$(jq '.findings | length' "${artifact_dir}/results/mixed-instability.json")" >&2
            jq -r '.findings[] | "- \(.phase): \(.message)"' "${artifact_dir}/results/mixed-instability.json" >&2
            exit 1
            ;;
    esac
elif [[ "${recovery_scenario}" == true ]]; then
    jq -n \
        --arg run_id "${run_id}" \
        --arg scenario "${scenario}" \
        --arg image_id "${image_id}" \
        --argjson tool_images "${tool_images_json}" \
        --argjson nodes "${node_count}" \
        --argjson accepted_records "${input_end}" \
        --argjson observed_records "${output_end}" \
        --slurpfile progress "${artifact_dir}/results/${scenario}-progress.json" \
        --slurpfile ledger "${artifact_dir}/results/ledger.json" \
        --slurpfile findings "${artifact_dir}/results/recovery-findings.ndjson" \
        '{verdict:(if ($findings | length) == 0 then "pass" else "fail" end),run_id:$run_id,
          scenario:$scenario,image_id:$image_id,tool_images:$tool_images,topology_nodes:$nodes,
          accepted_source_records:$accepted_records,observed_output_records:$observed_records,
          replay_duplicates:$ledger[0].duplicate_records,source_offsets_committed:true,
          ledger:"results/ledger.json",remote_path:"results/remote-path.json",findings:$findings,
          progress:$progress[0]}' \
        >"${artifact_dir}/results/${scenario}.json"
    if [[ "$(jq -r '.verdict' "${artifact_dir}/results/${scenario}.json")" != pass ]]; then
        failure_category=product
        current_phase="${scenario} product findings"
        printf '%s recorded %d product finding(s):\n' "${scenario}" \
            "$(jq '.findings | length' "${artifact_dir}/results/${scenario}.json")" >&2
        jq -r '.findings[] | "- \(.phase): \(.message)"' "${artifact_dir}/results/${scenario}.json" >&2
        exit 1
    fi
elif [[ "${scenario}" == "degraded-links" ]]; then
    jq -n \
        --arg run_id "${run_id}" --arg image_id "${image_id}" \
        --argjson tool_images "${tool_images_json}" \
        --arg profile "${degradation_profile}" \
        --argjson accepted_records "${input_end}" --argjson observed_records "${output_end}" \
        --slurpfile progress "${artifact_dir}/results/degraded-progress.json" \
        --slurpfile ledger "${artifact_dir}/results/ledger.json" \
        --slurpfile findings <(cat "${artifact_dir}/degraded/findings.ndjson" 2>/dev/null || true) \
        '{verdict:(if ($findings|length)==0 then "pass" else "fail" end),run_id:$run_id,
          image_id:$image_id,tool_images:$tool_images,
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
        --argjson tool_images "${tool_images_json}" \
        --argjson nodes "${node_count}" \
        --argjson accepted_records "${input_end}" \
        --argjson observed_records "${output_end}" \
        --slurpfile progress "${artifact_dir}/results/crash-progress.json" \
        --slurpfile ledger "${artifact_dir}/results/ledger.json" \
        '{verdict:"pass",run_id:$run_id,scenario:$scenario,image_id:$image_id,tool_images:$tool_images,topology_nodes:$nodes,accepted_source_records:$accepted_records,observed_output_records:$observed_records,replay_duplicates:$ledger[0].duplicate_records,source_offsets_committed:true,ledger:"results/ledger.json",remote_path:"results/remote-path.json",progress:$progress[0]}' \
        >"${artifact_dir}/results/crash.json"
fi

# A run whose other verdicts passed still fails when a load has no passing pacing verdict: every
# timing the run reports was then measured under a load other than the declared one.
if ((${#load_pacing_failures[@]} > 0)); then
    failure_category=controller
    current_phase='load pacing'
    for unpaced_load in "${load_pacing_failures[@]}"; do
        printf 'controller failure: %s was not verified to keep its records an interval apart; see results/%s-pacing.json\n' \
            "${unpaced_load}" "${unpaced_load}" >&2
    done
    exit 1
fi

current_phase="complete"
