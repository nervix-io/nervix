#!/usr/bin/env bash
set -euo pipefail

# Self-checks the run's live Docker event recording: the window verifier over recorded fixtures,
# the exact node lifecycle check, the check that every container comes from a recorded image, and
# the recorder against the local daemon, including a fault followed by more daemon events than the
# daemon's 256-event replay buffer holds.

chaos_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
verify="${chaos_dir}/verify-docker-events.sh"
tmp_dir="$(mktemp -d)"
live_run_id="events-self-test-$$-${RANDOM}"
noise_prefix="nervix-chaos-${live_run_id}-noise"

fail() {
    printf 'docker events self-test failed: %s\n' "$*" >&2
    exit 1
}

# shellcheck source=../docker-event-recording.sh
source "${chaos_dir}/docker-event-recording.sh"
# shellcheck source=../tool-images.sh
source "${chaos_dir}/tool-images.sh"

docker_events_self_test_cleanup() {
    docker_event_recording_stop
    "${chaos_dir}/cleanup.sh" --run-id "${live_run_id}" --quiet >/dev/null 2>&1 \
        || printf 'cleanup of %s reported a failure\n' "${live_run_id}" >&2
    local volumes=()
    mapfile -t volumes < <(docker volume ls --quiet --filter "name=${noise_prefix}" 2>/dev/null)
    if ((${#volumes[@]} > 0)); then
        docker volume rm "${volumes[@]}" >/dev/null 2>&1 \
            || printf 'could not remove self-test volumes %s\n' "${volumes[*]}" >&2
    fi
    rm -rf "${tmp_dir}"
}
trap docker_events_self_test_cleanup EXIT

node_id="$(printf 'a%.0s' {1..64})"
other_node_id="$(printf 'b%.0s' {1..64})"
admin_id="$(printf 'c%.0s' {1..64})"
start_marker_id="$(printf 'd%.0s' {1..64})"
inner_marker_id="$(printf 'f%.0s' {1..64})"
end_marker_id="$(printf 'e%.0s' {1..64})"
base_ns=1800000000000000000

# Prints one recorded container event: ID ROLE ACTION OFFSET_MS [ATTRIBUTES_JSON]. Offsets are
# whole milliseconds, far coarser than the precision jq keeps for nanosecond timestamps.
recorded_event() {
    jq -nc \
        --arg id "$1" \
        --arg role "$2" \
        --arg action "$3" \
        --argjson ns "$((base_ns + $4 * 1000000))" \
        --argjson attributes "${5:-{\}}" '
        {Type: "container", Action: $action,
         Actor: {ID: $id,
                 Attributes: ({"io.nervix.chaos.run": "self-test", "io.nervix.chaos.role": $role,
                               name: ("self-test-" + $id[0:12])} + $attributes)},
         scope: "local", time: ($ns / 1000000000 | floor), timeNano: $ns}'
}

# A recording bracketed by two markers. Between the node's SIGKILL and its explicit start it holds
# 300 unrelated container events, more than the daemon's replay buffer keeps, and the marker that
# closed an earlier, shorter window.
recording="${tmp_dir}/recording.ndjson"
{
    recorded_event "${start_marker_id}" event-marker create 0
    recorded_event "${start_marker_id}" event-marker destroy 1
    recorded_event "${admin_id}" admin create 100
    recorded_event "${node_id}" node kill 1100 '{"signal":"9"}'
    recorded_event "${node_id}" node die 1200 '{"exitCode":"137"}'
    recorded_event "${admin_id}" admin start 1300 \
        | jq -c 'range(300) as $offset | .timeNano += $offset * 1000000 | .time = (.timeNano / 1000000000 | floor)'
    recorded_event "${inner_marker_id}" event-marker create 1650
    recorded_event "${inner_marker_id}" event-marker destroy 1660
    recorded_event "${node_id}" node start 1700
    recorded_event "${end_marker_id}" event-marker create 2100
    recorded_event "${end_marker_id}" event-marker destroy 2200
} >"${recording}"
window_from=$((base_ns + 1000 * 1000000))
window_to=$((base_ns + 2000 * 1000000))

"${verify}" window --recording "${recording}" --from "${window_from}" --to "${window_to}" \
    --bounds "${tmp_dir}/nodes.recording.json" --output "${tmp_dir}/nodes.ndjson" --role node \
    || fail 'a window bracketed by recorded markers was not covered'
jq -e --argjson started "${base_ns}" --argjson until "$((base_ns + 2100 * 1000000))" '
    .verdict == "covered" and .window_events == 3 and .recorded_markers == 3
    and .recording_started_ns == $started and .recording_covered_until_ns == $until
    and .selector == {role: "node"}
' "${tmp_dir}/nodes.recording.json" >/dev/null \
    || fail 'covered window bounds did not record the recording and window'
[[ "$(jq -sc 'map(.Action)' "${tmp_dir}/nodes.ndjson")" == '["kill","die","start"]' ]] \
    || fail 'the node window lost an event behind 300 unrelated events'
"${verify}" lifecycle --events "${tmp_dir}/nodes.ndjson" --target "${node_id}" \
    --expect kill:9 --expect die:137 --expect start \
    || fail 'the recorded SIGKILL, exit and restart did not match the planned crash'

"${verify}" window --recording "${recording}" --from "${window_from}" --to "${window_to}" \
    --bounds "${tmp_dir}/target.recording.json" --output "${tmp_dir}/target.ndjson" \
    --container "${node_id}"
[[ "$(wc -l <"${tmp_dir}/target.ndjson")" -eq 3 ]] || fail 'the container selector missed target events'
"${verify}" window --recording "${recording}" --from "${window_from}" --to "${window_to}" \
    --bounds "${tmp_dir}/all.recording.json" --output "${tmp_dir}/all.ndjson"
[[ "$(wc -l <"${tmp_dir}/all.ndjson")" -eq 303 ]] \
    || fail 'an unselected window did not hold exactly the non-marker events'
"${verify}" window --recording "${recording}" --from "${window_from}" --to "${window_to}" \
    --bounds "${tmp_dir}/run.recording.json" --max-bytes 1048576
jq -e '.verdict == "covered" and .window_events == 303' "${tmp_dir}/run.recording.json" >/dev/null \
    || fail 'an unselected window did not count every non-marker event'

# A window the recording does not cover fails with its bounds and never writes events.
expect_window_verdict() {
    local case_name="$1"
    local verdict="$2"
    shift 2
    local status=0
    printf 'stale\n' >"${tmp_dir}/uncovered.ndjson"
    "${verify}" window "$@" --bounds "${tmp_dir}/uncovered.recording.json" \
        --output "${tmp_dir}/uncovered.ndjson" >"${tmp_dir}/uncovered.txt" 2>&1 || status=$?
    [[ "${status}" -eq 1 ]] || fail "${case_name} returned ${status}, expected 1"
    jq -e --arg verdict "${verdict}" '.verdict == $verdict and .window_events == null' \
        "${tmp_dir}/uncovered.recording.json" >/dev/null \
        || fail "${case_name} was not reported as ${verdict}"
    [[ ! -e "${tmp_dir}/uncovered.ndjson" ]] || fail "${case_name} left window events to check"
    grep -Fq 'does not cover window' "${tmp_dir}/uncovered.txt" \
        || fail "${case_name} did not explain the uncovered window"
}

expect_window_verdict 'window opened before the recording' started-late \
    --recording "${recording}" --from "$((base_ns - 1000000))" --to "${window_to}"
expect_window_verdict 'window closed after the recording' ended-early \
    --recording "${recording}" --from "${window_from}" --to "$((base_ns + 3000 * 1000000))" \
    --recorder-exit-code 1
jq -e '.recorder_exit_code == 1 and (.reason | test("exited with status 1"))' \
    "${tmp_dir}/uncovered.recording.json" >/dev/null \
    || fail 'an early subscriber exit was not reported with the recording bounds'
expect_window_verdict 'absent recording' missing \
    --recording "${tmp_dir}/absent.ndjson" --from "${window_from}" --to "${window_to}"
: >"${tmp_dir}/empty.ndjson"
expect_window_verdict 'empty recording' missing \
    --recording "${tmp_dir}/empty.ndjson" --from "${window_from}" --to "${window_to}"
grep -v '"event-marker"' "${recording}" >"${tmp_dir}/unmarked.ndjson"
expect_window_verdict 'recording without markers' missing \
    --recording "${tmp_dir}/unmarked.ndjson" --from "${window_from}" --to "${window_to}"
{
    cat "${recording}"
    printf '%s\n' '{"Type":"container","Action":'
} >"${tmp_dir}/truncated.ndjson"
expect_window_verdict 'unreadable recording' invalid \
    --recording "${tmp_dir}/truncated.ndjson" --from "${window_from}" --to "${window_to}"
expect_window_verdict 'recording over its bound' exceeded \
    --recording "${recording}" --from "${window_from}" --to "${window_to}" --max-bytes 1024

expect_usage_error() {
    local case_name="$1"
    shift
    local status=0
    "${verify}" "$@" >"${tmp_dir}/usage.txt" 2>&1 || status=$?
    [[ "${status}" -eq 2 ]] || fail "${case_name} returned ${status}, expected usage error 2"
}
expect_usage_error 'reversed window' window --recording "${recording}" \
    --from "${window_to}" --to "${window_from}" --bounds "${tmp_dir}/usage.json"
expect_usage_error 'two selectors' window --recording "${recording}" --from "${window_from}" \
    --to "${window_to}" --bounds "${tmp_dir}/usage.json" --container "${node_id}" --role node
expect_usage_error 'unknown lifecycle event' lifecycle --events "${tmp_dir}/nodes.ndjson" \
    --expect reboot
expect_usage_error 'unknown window option' window --recording "${recording}" --window-size 1
expect_usage_error 'unknown lifecycle option' lifecycle --events "${tmp_dir}/nodes.ndjson" --since 1
expect_usage_error 'non-numeric byte bound' window --recording "${recording}" --from "${window_from}" \
    --to "${window_to}" --bounds "${tmp_dir}/usage.json" --max-bytes lots
expect_usage_error 'non-numeric subscriber status' window --recording "${recording}" \
    --from "${window_from}" --to "${window_to}" --bounds "${tmp_dir}/usage.json" \
    --recorder-exit-code crashed
expect_usage_error 'missing command'
expect_usage_error 'unknown command' replay --recording "${recording}"

expect_lifecycle_failure() {
    local case_name="$1"
    shift
    local status=0
    "${verify}" lifecycle "$@" >"${tmp_dir}/lifecycle.txt" 2>&1 || status=$?
    [[ "${status}" -eq 1 ]] || fail "${case_name} returned ${status}, expected 1"
    grep -Fq 'differ from the planned fault' "${tmp_dir}/lifecycle.txt" \
        || fail "${case_name} did not report the observed lifecycle"
}

# An empty window can never stand in for the planned fault.
: >"${tmp_dir}/no-events.ndjson"
"${verify}" lifecycle --events "${tmp_dir}/no-events.ndjson"
expect_lifecycle_failure 'window without the planned SIGKILL' \
    --events "${tmp_dir}/no-events.ndjson" --target "${node_id}" \
    --expect kill:9 --expect die:137 --expect start
{
    cat "${tmp_dir}/nodes.ndjson"
    recorded_event "${other_node_id}" node die 1800 '{"exitCode":"0"}'
} >"${tmp_dir}/other-node-exit.ndjson"
expect_lifecycle_failure 'another node exited' --events "${tmp_dir}/other-node-exit.ndjson" \
    --target "${node_id}" --expect kill:9 --expect die:137 --expect start
{
    recorded_event "${other_node_id}" node kill 1100 '{"signal":"9"}'
    recorded_event "${other_node_id}" node die 1200 '{"exitCode":"137"}'
    recorded_event "${other_node_id}" node start 1700
} >"${tmp_dir}/wrong-target.ndjson"
expect_lifecycle_failure 'the planned fault hit another node' \
    --events "${tmp_dir}/wrong-target.ndjson" --target "${node_id}" \
    --expect kill:9 --expect die:137 --expect start
expect_lifecycle_failure 'a node restarted during a partition' --events "${tmp_dir}/nodes.ndjson"
{
    recorded_event "${node_id}" node pause 1100
    recorded_event "${node_id}" node unpause 1700
} >"${tmp_dir}/pause.ndjson"
"${verify}" lifecycle --events "${tmp_dir}/pause.ndjson" --target "${node_id}" \
    --expect pause --expect unpause
{
    cat "${tmp_dir}/pause.ndjson"
    recorded_event "${node_id}" node start 1800
} >"${tmp_dir}/pause-restart.ndjson"
expect_lifecycle_failure 'a paused node restarted' --events "${tmp_dir}/pause-restart.ndjson" \
    --target "${node_id}" --expect pause --expect unpause
sed 's/"signal":"9"/"signal":"15"/' "${tmp_dir}/nodes.ndjson" >"${tmp_dir}/sigterm.ndjson"
expect_lifecycle_failure 'the node received SIGTERM' --events "${tmp_dir}/sigterm.ndjson" \
    --target "${node_id}" --expect kill:9 --expect die:137 --expect start

# Every container a run creates, markers included, must come from an image its manifest records:
# the resolved Nervix image ID, or a tool image's pinned reference or image ID.
broker_id="$(printf '9%.0s' {1..64})"
fault_id="$(printf '8%.0s' {1..64})"
link_probe_id="$(printf '7%.0s' {1..64})"
nervix_image_id="sha256:$(printf '1%.0s' {1..64})"
kafka_image_id="sha256:$(printf '2%.0s' {1..64})"
pumba_image_id="sha256:$(printf '3%.0s' {1..64})"
probe_image_id="sha256:$(printf '8%.0s' {1..64})"
kafka_reference="registry.example/chaos/kafka@sha256:$(printf '4%.0s' {1..64})"
probe_reference="registry.example/chaos/probe@sha256:$(printf '5%.0s' {1..64})"
pumba_reference="registry.example/chaos/pumba@sha256:$(printf '6%.0s' {1..64})"
unrecorded_reference="registry.example/chaos/unrecorded@sha256:$(printf '7%.0s' {1..64})"
jq -n \
    --arg nervix "${nervix_image_id}" \
    --arg kafka "${kafka_reference}" --arg kafka_id "${kafka_image_id}" \
    --arg probe "${probe_reference}" --arg probe_id "${probe_image_id}" \
    --arg pumba "${pumba_reference}" --arg pumba_id "${pumba_image_id}" '
    {resolved_image_id: $nervix,
     tool_images: {kafka: {reference: $kafka, image_id: $kafka_id},
                   probe: {reference: $probe, image_id: $probe_id},
                   pumba: {reference: $pumba, image_id: $pumba_id}}}
' >"${tmp_dir}/manifest.json"
image_attributes() {
    jq -nc --arg image "$1" '{image: $image}'
}
{
    recorded_event "${start_marker_id}" event-marker create 0 "$(image_attributes "${probe_reference}")"
    recorded_event "${node_id}" node create 100 "$(image_attributes "${nervix_image_id}")"
    recorded_event "${other_node_id}" node create 110 "$(image_attributes "${nervix_image_id}")"
    recorded_event "${admin_id}" admin create 120 "$(image_attributes "${nervix_image_id}")"
    recorded_event "${broker_id}" broker create 130 "$(image_attributes "${kafka_reference}")"
    recorded_event "${fault_id}" fault create 140 "$(image_attributes "${pumba_image_id}")"
    recorded_event "${node_id}" node start 150 "$(image_attributes "${unrecorded_reference}")"
} >"${tmp_dir}/images.ndjson"
"${verify}" images --recording "${tmp_dir}/images.ndjson" --manifest "${tmp_dir}/manifest.json" \
    --output "${tmp_dir}/images.json" \
    || fail 'containers created from recorded images were reported as unrecorded'
jq -e --arg nervix "${nervix_image_id}" --arg kafka "${kafka_reference}" \
    --arg probe "${probe_reference}" --arg pumba_id "${pumba_image_id}" '
    .verdict == "recorded"
    and (.images | sort_by(.image) | map([.image, .recorded_as, .containers, .roles]))
        == ([[$nervix, "nervix", 3, ["admin", "node"]], [$kafka, "kafka", 1, ["broker"]],
             [$probe, "probe", 1, ["event-marker"]], [$pumba_id, "pumba", 1, ["fault"]]]
            | sort_by(.[0]))
' "${tmp_dir}/images.json" >/dev/null \
    || fail 'the image check did not attribute every created container to its manifest entry'
{
    cat "${tmp_dir}/images.ndjson"
    recorded_event "${link_probe_id}" link-probe create 160 "$(image_attributes "${unrecorded_reference}")"
} >"${tmp_dir}/unrecorded-images.ndjson"
images_status=0
"${verify}" images --recording "${tmp_dir}/unrecorded-images.ndjson" \
    --manifest "${tmp_dir}/manifest.json" --output "${tmp_dir}/unrecorded-images.json" \
    >"${tmp_dir}/unrecorded-images.txt" 2>&1 || images_status=$?
[[ "${images_status}" -eq 1 ]] \
    || fail "a container from an unrecorded image returned ${images_status}, expected 1"
jq -e --arg image "${unrecorded_reference}" '
    .verdict == "unrecorded"
    and ([.images[] | select(.recorded_as == null)] == [{image: $image, recorded_as: null,
                                                         containers: 1, roles: ["link-probe"]}])
' "${tmp_dir}/unrecorded-images.json" >/dev/null \
    || fail 'the image check did not single out the container from an unrecorded image'
grep -Fq "${unrecorded_reference}" "${tmp_dir}/unrecorded-images.txt" \
    || fail 'the image check did not name the unrecorded image'
images_status=0
"${verify}" images --recording "${tmp_dir}/truncated.ndjson" --manifest "${tmp_dir}/manifest.json" \
    --output "${tmp_dir}/truncated-images.json" >"${tmp_dir}/truncated-images.txt" 2>&1 \
    || images_status=$?
[[ "${images_status}" -eq 1 ]] || fail "an unreadable recording returned ${images_status}, expected 1"
grep -Fq 'could not read the recording' "${tmp_dir}/truncated-images.txt" \
    || fail 'the image check did not report an unreadable recording'
expect_usage_error 'images without an output' images --recording "${tmp_dir}/images.ndjson" \
    --manifest "${tmp_dir}/manifest.json"
expect_usage_error 'images without a recording' images --recording "${tmp_dir}/absent.ndjson" \
    --manifest "${tmp_dir}/manifest.json" --output "${tmp_dir}/usage.json"
expect_usage_error 'unknown images option' images --since 1

# A run that never started its recording has no bounds to verify, which the controller reports.
# No recording has started yet in this self-test.
never_started_status=0
docker_event_recording_finish diagnostics/docker-events.recording.json 67108864 \
    || never_started_status=$?
[[ "${never_started_status}" -eq 2 ]] \
    || fail "closing a recording that never started returned ${never_started_status}, expected 2"

# The recorder against the local daemon. A run-labeled node is killed, the daemon then publishes
# 320 unrelated volume events, and the node is started again: the window must still hold the kill.
live_dir="${tmp_dir}/live"
mkdir -p "${live_dir}/diagnostics"
# Markers are created from a local image only, as the runner resolves its pinned probe image first.
docker image inspect "${chaos_probe_image}" >/dev/null 2>&1 \
    || docker pull --quiet "${chaos_probe_image}" >/dev/null
start_status=0
docker_event_recording_start "${live_dir}" diagnostics/docker-events.ndjson "${live_run_id}" \
    "${chaos_probe_image}" 300 || start_status=$?
[[ "${start_status}" -eq 0 ]] || fail "the live recorder did not start (status ${start_status})"
live_node="$(docker run --detach \
    --label "io.nervix.chaos.run=${live_run_id}" \
    --label io.nervix.chaos.role=node "${chaos_probe_image}" sleep 300)"
fault_ns="$(date +%s%N)"
docker kill --signal KILL "${live_node}" >/dev/null
# The positional parameters belong to the inner shell, not this one.
# shellcheck disable=SC2016
seq 1 160 | xargs -P 8 -I '{}' sh -c \
    'docker volume create "$1-$2" >/dev/null && docker volume rm "$1-$2" >/dev/null' \
    noise "${noise_prefix}" '{}'
docker start "${live_node}" >/dev/null
docker_event_window "${fault_ns}" "${live_dir}/node-events.ndjson" --role node \
    || fail 'the live recording did not cover the fault'
"${verify}" lifecycle --events "${live_dir}/node-events.ndjson" --target "${live_node}" \
    --expect kill:9 --expect die:137 --expect start \
    || fail 'the live recording lost the SIGKILL behind 320 unrelated daemon events'
jq -e '.verdict == "covered" and .recording == "diagnostics/docker-events.ndjson"' \
    "${live_dir}/node-events.recording.json" >/dev/null \
    || fail 'live window bounds were not recorded beside the window'
if docker_event_window "$((docker_event_recording_started_ns - 60000000000))" \
    "${live_dir}/before-start.ndjson" --role node 2>/dev/null; then
    fail 'a window opened before the recorder started was accepted'
fi
jq -e '.verdict == "started-late"' "${live_dir}/before-start.recording.json" >/dev/null \
    || fail 'a window opened before the recorder started was not reported as started-late'
docker_event_recording_finish diagnostics/docker-events.recording.json 67108864 \
    || fail 'a recording closed while its subscriber ran did not cover the run'
jq -e '.verdict == "covered" and .window_events >= 3' \
    "${live_dir}/diagnostics/docker-events.recording.json" >/dev/null \
    || fail 'the closed recording did not report its whole-run bounds'
# The daemon records the pinned reference each marker and the node were created from.
jq -n --arg nervix "${nervix_image_id}" --arg probe "${chaos_probe_image}" \
    --arg probe_id "$(docker image inspect --format '{{.Id}}' "${chaos_probe_image}")" '
    {resolved_image_id: $nervix, tool_images: {probe: {reference: $probe, image_id: $probe_id}}}
' >"${live_dir}/manifest.json"
"${verify}" images --recording "${live_dir}/diagnostics/docker-events.ndjson" \
    --manifest "${live_dir}/manifest.json" --output "${live_dir}/container-images.json" \
    || fail 'the live recording attributed a container to an image other than the pinned probe'
jq -e --arg probe "${chaos_probe_image}" '
    .images | length == 1 and .[0].image == $probe and .[0].recorded_as == "probe"
    and .[0].roles == ["event-marker", "node"]
' "${live_dir}/container-images.json" >/dev/null \
    || fail 'the live recording did not attribute every marker and the node to the pinned probe'

# A subscriber that exits early leaves every later window uncovered, with its exit status.
early_dir="${tmp_dir}/early"
mkdir -p "${early_dir}/diagnostics"
start_status=0
docker_event_recording_start "${early_dir}" diagnostics/docker-events.ndjson "${live_run_id}" \
    "${chaos_probe_image}" 300 || start_status=$?
[[ "${start_status}" -eq 0 ]] || fail "the second live recorder did not start (status ${start_status})"
window_open_ns="$(date +%s%N)"
kill "${docker_event_recorder_pid}"
if docker_event_window "${window_open_ns}" "${early_dir}/node-events.ndjson" --role node 2>/dev/null; then
    fail 'a window closed after the subscriber exited was accepted'
fi
jq -e '.verdict == "ended-early" and .recorder_exit_code != null' \
    "${early_dir}/node-events.recording.json" >/dev/null \
    || fail 'an early subscriber exit was not reported as ended-early with its status'
[[ ! -e "${early_dir}/node-events.ndjson" ]] || fail 'an uncovered live window left events to check'
finish_status=0
docker_event_recording_finish diagnostics/docker-events.recording.json 67108864 2>/dev/null \
    || finish_status=$?
[[ "${finish_status}" -eq 1 ]] || fail "closing an ended recording returned ${finish_status}, expected 1"
jq -e '.verdict == "ended-early"' "${early_dir}/diagnostics/docker-events.recording.json" >/dev/null \
    || fail 'the run-level bounds did not report the early exit'

printf 'docker events self-test passed\n'
