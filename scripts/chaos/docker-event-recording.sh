#!/usr/bin/env bash
# Sourced by run-baseline.sh and the chaos self-test. Owns a chaos run's live Docker event
# recording. `docker events --since` replays only the daemon's in-memory buffer of its most recent
# 256 events, shared by every container on the host, and applies label filters afterwards, so a
# replay read after a fault can omit the fault itself. One subscriber therefore records every event
# of the run's labeled containers, from before the run creates its first container until
# diagnostics capture, and every Docker-event check reads its window from that recording.
#
# Labeled marker containers bound the recording. A marker's creation reaches the file only while
# the subscriber is live and has delivered everything the daemon published before it, so each
# window closes with a marker, and verify-docker-events.sh accepts a window only when recorded
# markers bracket it.

docker_event_verifier="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/verify-docker-events.sh"
docker_event_artifact_dir=""
docker_event_recording=""
docker_event_run_id=""
docker_event_marker_image=""
docker_event_recorder_pid=""
docker_event_recorder_exit_code=""
docker_event_recording_started_ns=""
docker_event_marker_id=""

docker_event_recorder_running() {
    [[ -n "${docker_event_recorder_pid}" ]] && kill -0 "${docker_event_recorder_pid}" 2>/dev/null
}

# Creates a labeled marker container and waits up to WAIT_SECONDS until the recording holds its
# creation. Every Docker call is bounded on its own, so markers still close the recording while
# the controller finishes after its run deadline.
docker_event_recording_mark() {
    local purpose="$1"
    local wait_seconds="${2:-20}"
    local recording_path="${docker_event_artifact_dir}/${docker_event_recording}"
    docker_event_marker_id=""
    local marker_id
    marker_id="$(timeout --foreground --kill-after=5s 20s docker container create --pull never \
        --network none \
        --label "io.nervix.chaos.run=${docker_event_run_id}" \
        --label io.nervix.chaos.role=event-marker \
        --label "io.nervix.chaos.event-marker=${purpose}" \
        "${docker_event_marker_image}")" || return 1
    docker_event_marker_id="${marker_id}"
    local recorded=false
    local deadline=$((SECONDS + wait_seconds))
    while ((SECONDS < deadline)); do
        if grep -Fq "${marker_id}" "${recording_path}"; then
            recorded=true
            break
        fi
        if ! docker_event_recorder_running; then
            break
        fi
        sleep 0.05
    done
    if [[ "${recorded}" != true ]] && grep -Fq "${marker_id}" "${recording_path}"; then
        recorded=true
    fi
    # A marker this removal misses still carries the run label, so run cleanup removes it.
    if ! timeout --foreground --kill-after=5s 20s docker container rm "${marker_id}" >/dev/null 2>&1; then
        printf 'could not remove Docker event marker %s\n' "${marker_id}" >&2
    fi
    [[ "${recorded}" == true ]]
}

# Starts the subscriber for RUN_ID, writing RECORDING under ARTIFACT_DIR, and returns once a
# recorded marker proves it live. The subscriber connects a moment after it starts, so a marker it
# missed is replaced until one is recorded. Returns 1 when none is recorded within 30 seconds and 3
# when the daemon stamped the marker outside the interval this controller measured around it: event
# windows are timed on this controller's clock, so they cannot be placed against another one.
docker_event_recording_start() {
    docker_event_artifact_dir="$1"
    docker_event_recording="$2"
    docker_event_run_id="$3"
    docker_event_marker_image="$4"
    local lifetime_seconds="$5"
    local recording_path="${docker_event_artifact_dir}/${docker_event_recording}"
    docker_event_recorder_exit_code=""
    docker_event_recording_started_ns=""
    : >"${recording_path}"
    # The subscriber is bounded so that it ends even if the controller is killed; its own process
    # group keeps a terminal interrupt from ending it before the controller's final marker.
    timeout --kill-after=5s "${lifetime_seconds}s" docker events \
        --filter "label=io.nervix.chaos.run=${docker_event_run_id}" \
        --format '{{json .}}' \
        >"${recording_path}" 2>"${recording_path%.ndjson}.stderr" &
    docker_event_recorder_pid=$!
    local deadline=$((SECONDS + 30))
    while ((SECONDS < deadline)) && docker_event_recorder_running; do
        local requested_ns
        requested_ns="$(date +%s%N)"
        if docker_event_recording_mark start 2; then
            local observed_ns
            observed_ns="$(date +%s%N)"
            if ! jq -e -n \
                --arg id "${docker_event_marker_id}" \
                --argjson requested "${requested_ns}" \
                --argjson observed "${observed_ns}" '
                first(inputs | select(.Action == "create" and .Actor.ID == $id))
                | .timeNano >= $requested and .timeNano <= $observed
            ' "${recording_path}" >/dev/null; then
                return 3
            fi
            docker_event_recording_started_ns="${observed_ns}"
            return 0
        fi
    done
    return 1
}

# Stops the subscriber. One that already exited keeps its exit status as recording evidence.
docker_event_recording_stop() {
    if [[ -z "${docker_event_recorder_pid}" ]]; then
        return 0
    fi
    if docker_event_recorder_running; then
        kill "${docker_event_recorder_pid}" 2>/dev/null
        # This controller stopped the subscriber, so its termination status carries no evidence.
        wait "${docker_event_recorder_pid}" 2>/dev/null || true
    else
        local exit_status=0
        wait "${docker_event_recorder_pid}" 2>/dev/null || exit_status=$?
        docker_event_recorder_exit_code="${exit_status}"
    fi
    docker_event_recorder_pid=""
}

# Runs the verifier from the artifact directory, so every path its bounds record is relative to it.
docker_event_verify_window() {
    local from_ns="$1"
    local to_ns="$2"
    local bounds="$3"
    shift 3
    if [[ -n "${docker_event_recorder_pid}" ]] && ! docker_event_recorder_running; then
        docker_event_recording_stop
    fi
    local exit_code_args=()
    if [[ -n "${docker_event_recorder_exit_code}" ]]; then
        exit_code_args=(--recorder-exit-code "${docker_event_recorder_exit_code}")
    fi
    (
        cd "${docker_event_artifact_dir}" \
            && "${docker_event_verifier}" window \
                --recording "${docker_event_recording}" \
                --from "${from_ns}" --to "${to_ns}" \
                --bounds "${bounds#"${docker_event_artifact_dir}/"}" \
                "$@" "${exit_code_args[@]}"
    )
}

# Closes a window at this instant with a marker and writes the recorded events from FROM_NS through
# the close that the selector matches to OUTPUT, with the recording's bounds beside it in
# OUTPUT's .recording.json. Returns 1 when the recording does not cover the window.
docker_event_window() {
    local from_ns="$1"
    local output="$2"
    shift 2
    local to_ns
    to_ns="$(date +%s%N)"
    local name="${output##*/}"
    if ! docker_event_recording_mark "${name%.ndjson}"; then
        printf 'the Docker event recording did not record the marker closing %s\n' "${name}" >&2
    fi
    docker_event_verify_window "${from_ns}" "${to_ns}" "${output%.ndjson}.recording.json" \
        --output "${output#"${docker_event_artifact_dir}/"}" "$@"
}

# Closes the run's recording with a final marker, stops its subscriber, and verifies that it
# covers the run from its start through this close within MAX_BYTES, writing its bounds to BOUNDS.
# Returns 1 when it does not, and 2 when the recording never started.
docker_event_recording_finish() {
    local bounds="$1"
    local max_bytes="$2"
    if [[ -z "${docker_event_recording_started_ns}" ]]; then
        docker_event_recording_stop
        return 2
    fi
    local to_ns
    to_ns="$(date +%s%N)"
    if ! docker_event_recording_mark end; then
        printf '%s\n' 'the Docker event recording did not record its final marker' >&2
    fi
    docker_event_recording_stop
    docker_event_verify_window "${docker_event_recording_started_ns}" "${to_ns}" "${bounds}" \
        --max-bytes "${max_bytes}"
}
