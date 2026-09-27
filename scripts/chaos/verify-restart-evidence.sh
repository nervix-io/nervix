#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 7 || "$#" -gt 8 ]]; then
    printf 'usage: %s before|stopped|started RUN_ID PROJECT SERVICE IMAGE_ID BEFORE_JSON CURRENT_JSON [SHUTDOWN_LOG]\n' "$0" >&2
    exit 2
fi

phase="$1"
run_id="$2"
project="$3"
service="$4"
image_id="$5"
before_json="$6"
current_json="$7"
shutdown_log="${8:-}"

if [[ ! "${service}" =~ ^nervix-[123]$ ]]; then
    printf 'invalid target service: %s\n' "${service}" >&2
    exit 2
fi

node_number="${service##*-}"
container_name="/${project}-${service}-1"
volume_name="${project}_node-${node_number}-data"

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
        .[0].Image == $image_id and
        (.[0].Mounts | any(.Type == "volume" and .Name == $volume_name))
    ' "${inspection}" >/dev/null
}

if ! identity_matches "${before_json}" || ! identity_matches "${current_json}"; then
    printf 'target identity, image, or volume mismatch for %s\n' "${service}" >&2
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
    before | started)
        if ! jq -e '.[0].State.Running == true' "${current_json}" >/dev/null; then
            printf '%s is not running at %s check\n' "${service}" "${phase}" >&2
            exit 1
        fi
        ;;
    stopped)
        if [[ -z "${shutdown_log}" ]]; then
            printf 'shutdown log is required for stopped check\n' >&2
            exit 2
        fi
        if ! jq -e '.[0].State.Running == false and .[0].State.ExitCode == 0 and .[0].State.OOMKilled == false' \
            "${current_json}" >/dev/null; then
            printf '%s did not exit cleanly\n' "${service}" >&2
            exit 1
        fi
        for phase_name in admission drain-support terminal-teardown; do
            if ! grep -Fq "shutdown ${phase_name} phase finished" "${shutdown_log}"; then
                printf '%s did not finish shutdown phase %s\n' "${service}" "${phase_name}" >&2
                exit 1
            fi
        done
        if grep -Eq 'outcome=Forced|shutdown deadline expired|repeated termination signal received; abandoning' \
            "${shutdown_log}"; then
            printf '%s reported a forced shutdown\n' "${service}" >&2
            exit 1
        fi
        ;;
    *)
        printf 'invalid restart evidence phase: %s\n' "${phase}" >&2
        exit 2
        ;;
esac
