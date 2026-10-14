#!/usr/bin/env bash
# Owns the read-only packaged-binary probe and its retained command/container evidence. The
# controller supplies run_bounded, remaining_seconds, the immutable image and run artifact scope.

verify_image_package() {
    : "${artifact_dir:?the controller supplies its artifact directory}"
    : "${image_id:?the controller supplies an immutable image ID}"
    : "${run_id:?the controller supplies its run identity}"
    local probe_dir="${artifact_dir}/diagnostics/image-package"
    local container_name="${run_id}-image-package"
    mkdir -p "${probe_dir}" || return $?
    local command=(docker run --name "${container_name}" --label "io.nervix.chaos.run=${run_id}"
        --label io.nervix.chaos.role=admin --interactive=false --network none --entrypoint /bin/sh
        "${image_id}" -eu -c 'test -x /usr/local/bin/nervix-server; test -x /usr/local/bin/nervix-cli')
    local started_ns started_at remaining effective_seconds=30
    started_at="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    started_ns="$(date +%s%N)"
    remaining="$(remaining_seconds)"
    if ((effective_seconds > remaining)); then effective_seconds="${remaining}"; fi
    jq -n --arg name "${container_name}" --arg image "${image_id}" --arg started_at "${started_at}" \
        --argjson effective_seconds "${effective_seconds}" \
        --argjson command "$(printf '%s\n' "${command[@]}" | jq -Rsc 'split("\n")[:-1]')" \
        '{status:"running",operation:"image-package",container_name:$name,image_id:$image,
          started_at:$started_at,requested_seconds:30,effective_seconds:$effective_seconds,
          command:$command,command_exit_code:null,outcome:null}' >"${probe_dir}/result.json" || return $?
    local command_status=0 inspection_status=0 status=0 outcome container_state=null state_valid=false
    # Removal belongs to final run cleanup, after command and state inspection. Its lifetime must
    # not turn a successful predicate into an unexplained image-package command timeout.
    run_bounded 30 "${command[@]}" </dev/null \
        >"${probe_dir}/stdout.txt" 2>"${probe_dir}/stderr.txt" || command_status=$?
    run_bounded 20 docker inspect "${container_name}" \
        >"${probe_dir}/inspect.json" 2>"${probe_dir}/inspect.stderr" || inspection_status=$?
    if ((inspection_status == 0)); then
        if container_state="$(jq -ec 'if length == 1 and (.[0].State | type == "object")
            then .[0].State else error("invalid probe inspection") end' "${probe_dir}/inspect.json")"; then
            state_valid=true
        else
            container_state=null
        fi
    fi
    if ((command_status != 0)); then
        status="${command_status}"
        outcome='command-failed'
    elif ((inspection_status != 0)); then
        status="${inspection_status}"
        outcome='inspection-failed'
    elif [[ "${state_valid}" != true ]]; then
        status=1
        outcome='inspection-invalid'
    elif jq -e '.Status == "exited" and .ExitCode == 0' <<<"${container_state}" >/dev/null; then
        outcome=passed
    else
        status=1
        outcome='container-refused'
    fi
    local ended_ns result_tmp="${probe_dir}/result.tmp"
    ended_ns="$(date +%s%N)"
    jq --arg outcome "${outcome}" --argjson command_status "${command_status}" \
        --argjson inspection_status "${inspection_status}" --argjson container_state "${container_state}" \
        --argjson elapsed_ns "$((ended_ns - started_ns))" \
        '.status = "complete" | .outcome = $outcome | .command_exit_code = $command_status
         | .inspection_exit_code = $inspection_status | .container_state = $container_state
         | .elapsed_ms = ($elapsed_ns / 1000000)' "${probe_dir}/result.json" >"${result_tmp}" || return $?
    mv "${result_tmp}" "${probe_dir}/result.json" || return $?
    if ((status != 0)); then
        printf 'image-package observation failed: %s (command %s, inspection %s); see diagnostics/image-package\n' \
            "${outcome}" "${command_status}" "${inspection_status}" >&2
    fi
    return "${status}"
}
