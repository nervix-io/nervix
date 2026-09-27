#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 7 || "$#" -gt 8 ]]; then
    printf 'usage: %s before|killed|started|recovered RUN_ID PROJECT SERVICE IMAGE_ID BEFORE_JSON CURRENT_JSON [EVENTS_NDJSON]\n' "$0" >&2
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

if [[ ! "${service}" =~ ^nervix-[123]$ ]]; then
    printf 'invalid target service: %s\n' "${service}" >&2
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
        (.[0].State.StartedAt | type == "string" and length > 0) and
        (.[0].Mounts | any(.Type == "volume" and .Name == $volume_name))
    ' "${inspection}" >/dev/null
}

if ! identity_matches "${before_json}" || ! identity_matches "${current_json}"; then
    printf 'target identity, restart policy, image, or volume mismatch for %s\n' "${service}" >&2
    exit 1
fi

if ! jq -e --slurpfile before "${before_json}" '
    .[0].Id == $before[0][0].Id and
    .[0].Image == $before[0][0].Image and
    ([.[0].Mounts[] | select(.Type == "volume") | .Name] | sort) ==
    ([$before[0][0].Mounts[] | select(.Type == "volume") | .Name] | sort)
' "${current_json}" >/dev/null; then
    printf 'container or volume changed for %s\n' "${service}" >&2
    exit 1
fi

case "${phase}" in
    before | started | recovered)
        if ! jq -e '.[0].State.Running == true' "${current_json}" >/dev/null; then
            printf '%s is not running at %s check\n' "${service}" "${phase}" >&2
            exit 1
        fi
        if [[ "${phase}" == started ]] \
            && ! jq -e --slurpfile before "${before_json}" \
                '.[0].State.StartedAt != $before[0][0].State.StartedAt' \
                "${current_json}" >/dev/null; then
            printf '%s did not start a new process after SIGKILL\n' "${service}" >&2
            exit 1
        fi
        if [[ "${phase}" == recovered ]] \
            && ! jq -e --slurpfile started "${before_json}" \
                '.[0].State.StartedAt == $started[0][0].State.StartedAt' \
                "${current_json}" >/dev/null; then
            printf '%s restarted unexpectedly after the explicit Docker start\n' "${service}" >&2
            exit 1
        fi
        ;;
    killed)
        if ! jq -e '.[0].State.Running == false and .[0].State.ExitCode == 137 and .[0].State.OOMKilled == false' \
            "${current_json}" >/dev/null; then
            printf '%s did not remain stopped after SIGKILL with exit code 137\n' "${service}" >&2
            exit 1
        fi
        if [[ -z "${events_path}" || ! -s "${events_path}" ]]; then
            printf 'Docker kill and die events are required for %s\n' "${service}" >&2
            exit 1
        fi
        if ! jq -s -e --arg id "$(jq -r '.[0].Id' "${before_json}")" '
            all(.[]; .Type == "container" and .Actor.ID == $id)
            and ([.[] | select(.Action == "kill" and .Actor.Attributes.signal == "9")] | length == 1)
            and ([.[] | select(.Action == "die" and .Actor.Attributes.exitCode == "137")] | length == 1)
            and ([.[] | select(.Action == "stop" or .Action == "start")] | length == 0)
        ' "${events_path}" >/dev/null; then
            printf '%s lacks exact SIGKILL and exit-137 Docker events\n' "${service}" >&2
            exit 1
        fi
        ;;
    *)
        printf 'invalid crash evidence phase: %s\n' "${phase}" >&2
        exit 2
        ;;
esac
