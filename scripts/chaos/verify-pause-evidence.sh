#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 7 || "$#" -gt 11 ]]; then
    printf 'usage: %s before|paused|resumed RUN_ID PROJECT SERVICE IMAGE_ID BEFORE_JSON CURRENT_JSON [EVENTS_NDJSON MIN_MS MAX_MS RESULT_JSON]\n' "$0" >&2
    exit 2
fi

phase="$1"
run_id="$2"
project="$3"
service="$4"
image_id="$5"
before_json="$6"
current_json="$7"
events_path="${8:-}"
min_ms="${9:-}"
max_ms="${10:-}"
result_path="${11:-}"

if [[ ! "${service}" =~ ^nervix-[123]$ ]]; then
    printf 'invalid pause target: %s\n' "${service}" >&2
    exit 2
fi

container_name="/${project}-${service}-1"
volume_name="${project}_node-${service##*-}-data"

identity_matches() {
    local inspection="$1"
    jq -e \
        --arg run_id "${run_id}" \
        --arg project "${project}" \
        --arg service "${service}" \
        --arg image_id "${image_id}" \
        --arg container_name "${container_name}" \
        --arg volume_name "${volume_name}" '
        length == 1 and
        (.[0].Id | test("^[a-f0-9]{64}$")) and
        .[0].Name == $container_name and
        .[0].Config.Labels["io.nervix.chaos.run"] == $run_id and
        .[0].Config.Labels["io.nervix.chaos.role"] == "node" and
        .[0].Config.Labels["io.nervix.chaos.target"] == "true" and
        .[0].Config.Labels["com.docker.compose.project"] == $project and
        .[0].Config.Labels["com.docker.compose.service"] == $service and
        .[0].HostConfig.RestartPolicy.Name == "no" and
        .[0].Image == $image_id and
        (.[0].Mounts | any(.Type == "volume" and .Name == $volume_name)) and
        (.[0].Config.Env | index("NERVIX_RAFT_HEARTBEAT_INTERVAL=250ms") != null) and
        (.[0].Config.Env | index("NERVIX_RAFT_ELECTION_TIMEOUT_MIN=10s") != null) and
        (.[0].Config.Env | index("NERVIX_RAFT_ELECTION_TIMEOUT_MAX=12s") != null) and
        (.[0].Config.Env | index("NERVIX_NODE_UNAVAILABILITY_TIMEOUT=15s") != null)
    ' "${inspection}" >/dev/null
}

if ! identity_matches "${before_json}" || ! identity_matches "${current_json}"; then
    printf 'pause target identity, image, volume, or configured liveness settings mismatch for %s\n' "${service}" >&2
    exit 1
fi

if ! jq -e --slurpfile before "${before_json}" '
    .[0].Id == $before[0][0].Id and
    .[0].State.StartedAt == $before[0][0].State.StartedAt and
    ([.[0].Mounts[] | select(.Type == "volume") | .Name] | sort) ==
    ([$before[0][0].Mounts[] | select(.Type == "volume") | .Name] | sort) and
    .[0].State.Running == true and
    .[0].State.ExitCode == 0 and
    .[0].State.OOMKilled == false
' "${current_json}" >/dev/null; then
    printf 'pause target changed process, volume, or running state for %s\n' "${service}" >&2
    exit 1
fi

case "${phase}" in
    before | resumed)
        jq -e '.[0].State.Paused == false' "${current_json}" >/dev/null \
            || { printf '%s was still paused at %s check\n' "${service}" "${phase}" >&2; exit 1; }
        ;;
    paused)
        jq -e '.[0].State.Paused == true' "${current_json}" >/dev/null \
            || { printf '%s was not paused during the declared fault\n' "${service}" >&2; exit 1; }
        ;;
    *)
        printf 'invalid pause evidence phase: %s\n' "${phase}" >&2
        exit 2
        ;;
esac

if [[ "${phase}" != resumed ]]; then
    exit 0
fi
if [[ "$#" -ne 11 || ! -s "${events_path}" || ! "${min_ms}" =~ ^[0-9]+$ \
    || ! "${max_ms}" =~ ^[0-9]+$ || "${min_ms}" -ge "${max_ms}" ]]; then
    printf 'pause/unpause events and a valid duration window are required for %s\n' "${service}" >&2
    exit 2
fi

if ! jq -s -e \
    --arg id "$(jq -r '.[0].Id' "${before_json}")" \
    --argjson min_ms "${min_ms}" \
    --argjson max_ms "${max_ms}" '
    ([.[] | select(.Action == "pause") | .timeNano] | first) as $start |
    ([.[] | select(.Action == "unpause") | .timeNano] | first) as $end |
    all(.[]; .Type == "container" and .Actor.ID == $id) and
    ([.[] | select(.Action == "pause")] | length == 1) and
    ([.[] | select(.Action == "unpause")] | length == 1) and
    ([.[] | select(.Action == "die" or .Action == "start" or .Action == "stop")] | length == 0) and
    ($start | type == "number") and ($end | type == "number") and
    ($end > $start) and (($end - $start) / 1000000 >= $min_ms) and
    (($end - $start) / 1000000 <= $max_ms)
' "${events_path}" >/dev/null; then
    printf '%s lacked one ordered pause/unpause transition inside %s..%sms\n' \
        "${service}" "${min_ms}" "${max_ms}" >&2
    exit 1
fi

jq -s \
    --argjson min_ms "${min_ms}" \
    --argjson max_ms "${max_ms}" '
    ([.[] | select(.Action == "pause") | .timeNano] | first) as $start |
    ([.[] | select(.Action == "unpause") | .timeNano] | first) as $end |
    {pause_started_ns:$start,pause_ended_ns:$end,actual_pause_ms:(($end - $start) / 1000000),minimum_ms:$min_ms,maximum_ms:$max_ms,pause_verified:true,unpause_verified:true}
' "${events_path}" >"${result_path}"
