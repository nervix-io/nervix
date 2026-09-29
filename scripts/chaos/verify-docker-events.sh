#!/usr/bin/env bash
set -euo pipefail

# Verifies windows of a chaos run's live Docker event recording, the node lifecycle a window holds,
# and the images the run's containers were created from. The recording holds, in arrival order,
# every event of the run's labeled containers
# together with the creation of the labeled marker containers the controller places while the
# subscriber runs. A marker reaches the recording only while the subscriber is live and caught up,
# so a window is covered only when one recorded marker precedes its start and another follows its
# end. A replay of the daemon's bounded event buffer, a subscriber that started late and one that
# exited early can each omit events without leaving a visible gap, so coverage is proven by the
# markers and never assumed from an empty or short window.

usage() {
    cat >&2 <<'EOF'
usage:
  verify-docker-events.sh window --recording FILE --from NS --to NS --bounds FILE
      [--output FILE] [--container ID | --role ROLE] [--max-bytes N] [--recorder-exit-code N]
  verify-docker-events.sh lifecycle --events FILE [--target ID] [--expect ACTION[:DETAIL]]...
  verify-docker-events.sh images --recording FILE --manifest FILE --output FILE

window writes the recording's bounds and its verdict for the window to --bounds and exits 1 unless
recorded markers bracket the window. Only a covered window writes --output: every recorded event
from --from through --to, excluding markers, of one container or of one io.nervix.chaos.role.

lifecycle requires the create, start, restart, stop, kill, die, oom, pause, unpause and destroy
events in --events to belong to --target and to be exactly the --expect list, where a kill carries
its signal and a die its exit code, as in kill:9 and die:137. Without --expect none may occur.

images requires every container creation in --recording, markers included, to name an image the
run --manifest records: the resolved Nervix image ID, or a tool image's pinned reference or image
ID. It writes each image with the manifest entry it matches, its container count and its roles to
--output, and exits 1 when any image is not recorded.
EOF
}

window() {
    local recording="" from="" to="" bounds="" output="" container="" role=""
    local max_bytes="" recorder_exit_code=""
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --recording | --from | --to | --bounds | --output | --container | --role | --max-bytes | --recorder-exit-code)
                [[ "$#" -ge 2 ]] || { usage; exit 2; }
                case "$1" in
                    --recording) recording="$2" ;;
                    --from) from="$2" ;;
                    --to) to="$2" ;;
                    --bounds) bounds="$2" ;;
                    --output) output="$2" ;;
                    --container) container="$2" ;;
                    --role) role="$2" ;;
                    --max-bytes) max_bytes="$2" ;;
                    --recorder-exit-code) recorder_exit_code="$2" ;;
                esac
                shift 2
                ;;
            *)
                printf 'unknown window argument: %s\n' "$1" >&2
                usage
                exit 2
                ;;
        esac
    done
    [[ -n "${recording}" && -n "${bounds}" ]] || { usage; exit 2; }
    if [[ ! "${from}" =~ ^[0-9]+$ || ! "${to}" =~ ^[0-9]+$ || "${from}" -gt "${to}" ]]; then
        printf '%s\n' '--from and --to must be nanosecond Unix timestamps with --from <= --to' >&2
        exit 2
    fi
    if [[ -n "${container}" && -n "${role}" ]]; then
        printf '%s\n' '--container and --role select different events; pass one of them' >&2
        exit 2
    fi
    if [[ -n "${max_bytes}" && ! "${max_bytes}" =~ ^[0-9]+$ ]]; then
        printf '%s\n' '--max-bytes must be a byte count' >&2
        exit 2
    fi
    if [[ -n "${recorder_exit_code}" && ! "${recorder_exit_code}" =~ ^[0-9]+$ ]]; then
        printf '%s\n' '--recorder-exit-code must be an exit status' >&2
        exit 2
    fi

    local bytes=0
    if [[ -f "${recording}" ]]; then
        bytes="$(wc -c <"${recording}")"
    fi
    local markers='{"count":0,"first_ns":null,"last_ns":null}'
    local readable=true
    if ((bytes > 0)); then
        markers="$(jq -n -c '
            reduce (inputs
                    | select(.Type == "container" and .Action == "create"
                             and .Actor.Attributes["io.nervix.chaos.role"] == "event-marker")
                    | .timeNano) as $marker
                ({count: 0, first_ns: null, last_ns: null};
                 .count += 1
                 | .first_ns = (if .first_ns == null or $marker < .first_ns then $marker else .first_ns end)
                 | .last_ns = (if .last_ns == null or $marker > .last_ns then $marker else .last_ns end))
        ' "${recording}" 2>/dev/null)" || readable=false
        if [[ "${readable}" != true ]]; then
            markers='{"count":0,"first_ns":null,"last_ns":null}'
        fi
    fi

    local verdict_json
    verdict_json="$(jq -n -c \
        --arg recording "${recording}" \
        --argjson bytes "${bytes}" \
        --argjson readable "${readable}" \
        --argjson markers "${markers}" \
        --argjson from "${from}" \
        --argjson to "${to}" \
        --argjson max_bytes "${max_bytes:-null}" \
        --argjson exit_code "${recorder_exit_code:-null}" \
        --arg container "${container}" \
        --arg role "${role}" '
        (if $bytes == 0 then
            ["missing", "no live recording exists"]
         elif $readable | not then
            ["invalid", "the recording is not newline-delimited Docker event JSON"]
         elif $markers.count == 0 then
            ["missing", "the recording holds no marker, so its subscriber never proved it was live"]
         elif $markers.first_ns > $from then
            ["started-late", "the recording started after the window opened"]
         elif $markers.last_ns < $to then
            ["ended-early", "the recording ended before the window closed"
                + (if $exit_code == null then "" else "; its subscriber exited with status \($exit_code)" end)]
         elif $max_bytes != null and $bytes > $max_bytes then
            ["exceeded", "the recording exceeded its \($max_bytes)-byte bound"]
         else
            ["covered", "recorded markers bracket the window"]
         end) as [$verdict, $reason]
        | {verdict: $verdict,
           reason: $reason,
           recording: $recording,
           recording_bytes: $bytes,
           max_bytes: $max_bytes,
           recorded_markers: $markers.count,
           recording_started_ns: $markers.first_ns,
           recording_covered_until_ns: $markers.last_ns,
           recorder_exit_code: $exit_code,
           window_from_ns: $from,
           window_to_ns: $to,
           selector: (if $container != "" then {container: $container}
                      elif $role != "" then {role: $role}
                      else {} end)}
    ')"

    local verdict
    verdict="$(jq -r '.verdict' <<<"${verdict_json}")"
    if [[ -n "${output}" ]]; then
        rm -f "${output}"
    fi
    if [[ "${verdict}" != covered ]]; then
        jq '. + {window_events: null}' <<<"${verdict_json}" >"${bounds}"
        printf 'the Docker event recording does not cover window %s..%s (%s): %s; its recorded markers span %s..%s\n' \
            "${from}" "${to}" "${verdict}" "$(jq -r '.reason' <<<"${verdict_json}")" \
            "$(jq -r '.recording_started_ns // "nothing"' <<<"${verdict_json}")" \
            "$(jq -r '.recording_covered_until_ns // "nothing"' <<<"${verdict_json}")" >&2
        exit 1
    fi

    local window_events=0
    if [[ -n "${output}" ]]; then
        jq -c \
            --argjson from "${from}" \
            --argjson to "${to}" \
            --arg container "${container}" \
            --arg role "${role}" '
            select(.timeNano >= $from and .timeNano <= $to
                   and .Actor.Attributes["io.nervix.chaos.role"] != "event-marker"
                   and ($container == "" or .Actor.ID == $container)
                   and ($role == "" or .Actor.Attributes["io.nervix.chaos.role"] == $role))
        ' "${recording}" >"${output}"
        window_events="$(wc -l <"${output}")"
    else
        window_events="$(jq -n \
            --argjson from "${from}" \
            --argjson to "${to}" '
            reduce (inputs
                    | select(.timeNano >= $from and .timeNano <= $to
                             and .Actor.Attributes["io.nervix.chaos.role"] != "event-marker")) as $event
                (0; . + 1)
        ' "${recording}")"
    fi
    jq --argjson window_events "${window_events}" '. + {window_events: $window_events}' \
        <<<"${verdict_json}" >"${bounds}"
}

lifecycle() {
    local events="" target=""
    local expected=()
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --events | --target | --expect)
                [[ "$#" -ge 2 ]] || { usage; exit 2; }
                case "$1" in
                    --events) events="$2" ;;
                    --target) target="$2" ;;
                    --expect)
                        if [[ ! "$2" =~ ^(create|start|restart|stop|kill|die|oom|pause|unpause|destroy)(:[0-9A-Za-z]+)?$ ]]; then
                            printf 'invalid expected lifecycle event: %s\n' "$2" >&2
                            exit 2
                        fi
                        expected+=("$2")
                        ;;
                esac
                shift 2
                ;;
            *)
                printf 'unknown lifecycle argument: %s\n' "$1" >&2
                usage
                exit 2
                ;;
        esac
    done
    [[ -f "${events}" ]] || { printf 'events file does not exist: %s\n' "${events}" >&2; exit 2; }

    local expected_json
    expected_json="$(printf '%s\n' "${expected[@]}" | jq -R -s -c 'split("\n") | map(select(length > 0)) | sort')"
    local observed_json
    observed_json="$(jq -s -c '
        [.[]
         | select(.Action == "create" or .Action == "start" or .Action == "restart"
                  or .Action == "stop" or .Action == "kill" or .Action == "die" or .Action == "oom"
                  or .Action == "pause" or .Action == "unpause" or .Action == "destroy")
         | {container: .Actor.ID,
            name: (.Actor.Attributes.name // .Actor.ID),
            event: (.Action
                    + (if .Action == "kill" then ":" + (.Actor.Attributes.signal // "")
                       elif .Action == "die" then ":" + (.Actor.Attributes.exitCode // "")
                       else "" end))}]
    ' "${events}")"
    if ! jq -e -n \
        --argjson observed "${observed_json}" \
        --argjson expected "${expected_json}" \
        --arg target "${target}" '
        ($target == "" or all($observed[]; .container == $target))
        and (([$observed[].event] | sort) == $expected)
    ' >/dev/null; then
        printf 'node lifecycle events differ from the planned fault: expected %s, observed %s\n' \
            "${expected_json}" "$(jq -c 'map(.name + " " + .event)' <<<"${observed_json}")" >&2
        exit 1
    fi
}

images() {
    local recording="" manifest="" output=""
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --recording | --manifest | --output)
                [[ "$#" -ge 2 ]] || { usage; exit 2; }
                case "$1" in
                    --recording) recording="$2" ;;
                    --manifest) manifest="$2" ;;
                    --output) output="$2" ;;
                esac
                shift 2
                ;;
            *)
                printf 'unknown images argument: %s\n' "$1" >&2
                usage
                exit 2
                ;;
        esac
    done
    [[ -n "${output}" ]] || { usage; exit 2; }
    [[ -f "${recording}" ]] || { printf 'recording does not exist: %s\n' "${recording}" >&2; exit 2; }
    [[ -f "${manifest}" ]] || { printf 'manifest does not exist: %s\n' "${manifest}" >&2; exit 2; }

    # The Nervix image is recorded by its resolved ID; a tool image is started by its pinned
    # reference or by its image ID.
    if ! jq -n --slurpfile manifest "${manifest}" '
        $manifest[0] as $run
        | ([{image: $run.resolved_image_id, recorded_as: "nervix"}]
           + [$run.tool_images | to_entries[]
              | {image: .value.reference, recorded_as: .key},
                {image: .value.image_id, recorded_as: .key}]
           | map(select(.image != null))) as $recorded
        | [inputs | select(.Type == "container" and .Action == "create")]
        | group_by(.Actor.Attributes.image)
        | map(.[0].Actor.Attributes.image as $image
              | {image: $image,
                 recorded_as: first(($recorded[] | select(.image == $image) | .recorded_as), null),
                 containers: length,
                 roles: (map(.Actor.Attributes["io.nervix.chaos.role"]) | unique)})
        | {verdict: (if all(.[]; .recorded_as != null) then "recorded" else "unrecorded" end),
           images: .}
    ' "${recording}" >"${output}"; then
        printf 'could not read the recording %s against the manifest %s\n' "${recording}" "${manifest}" >&2
        exit 1
    fi
    if ! jq -e '.verdict == "recorded"' "${output}" >/dev/null; then
        printf 'the run created containers from images its manifest does not record: %s\n' \
            "$(jq -c '[.images[] | select(.recorded_as == null) | {image, roles}]' "${output}")" >&2
        exit 1
    fi
}

command_name="${1:-}"
[[ -n "${command_name}" ]] || { usage; exit 2; }
shift
case "${command_name}" in
    window)
        window "$@"
        ;;
    lifecycle)
        lifecycle "$@"
        ;;
    images)
        images "$@"
        ;;
    -h | --help)
        usage
        ;;
    *)
        printf 'unknown verify-docker-events command: %s\n' "${command_name}" >&2
        usage
        exit 2
        ;;
esac
