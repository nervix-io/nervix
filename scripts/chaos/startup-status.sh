#!/usr/bin/env bash
# Owns publication of the validated public startup observation. The controller supplies its
# artifact scope, remaining_seconds and run_bounded; no product readiness decision is made here.

publish_startup_status() {
    : "${artifact_dir:?the controller supplies its artifact directory}"
    local evidence_dir="${artifact_dir}/diagnostics/startup-status"
    mkdir -p "${evidence_dir}" || return $?
    local command=(bash "${BASH_SOURCE[0]}"
        "${artifact_dir}/public/cluster-status.attempt.txt"
        "${artifact_dir}/public/.cluster-status.publish.txt"
        "${artifact_dir}/public/cluster-status.txt")
    local started_seconds="${SECONDS}" started_at effective_seconds
    started_at="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
    jq -n --arg started_at "${started_at}" \
        --argjson command "$(printf '%s\n' "${command[@]}" | jq -Rsc 'split("\n")[:-1]')" \
        '{operation:"startup-status-publication",status:"running",started_at:$started_at,
          requested_seconds:30,command:$command,command_exit_code:null,outcome:null}' \
        >"${evidence_dir}/result.json" || return $?
    effective_seconds="$(remaining_seconds)"
    if ((effective_seconds > 30)); then effective_seconds=30; fi
    local status=0 command_status=null outcome=passed
    if ((effective_seconds == 0)); then
        status=124
        outcome=overall-deadline
    else
        local native_status=0
        # This tiny transcript needs bytes, not a filesystem clone. Publish atomically only
        # after copying succeeds, with both operations inside the existing run's deadline.
        run_bounded "${effective_seconds}" "${command[@]}" </dev/null \
            >"${evidence_dir}/stdout.txt" 2>"${evidence_dir}/stderr.txt" || native_status=$?
        command_status="${native_status}"
        status="${native_status}"
        if ((status != 0)); then
            outcome='command-failed'
        elif (( $(remaining_seconds) == 0 )); then
            status=124
            outcome=overall-deadline
        fi
    fi
    jq --arg outcome "${outcome}" --argjson command_status "${command_status}" \
        --argjson effective_seconds "${effective_seconds}" \
        --argjson elapsed_seconds "$((SECONDS - started_seconds))" \
        '.status = "complete" | .outcome = $outcome | .command_exit_code = $command_status
         | .effective_seconds = $effective_seconds | .elapsed_seconds = $elapsed_seconds' \
        "${evidence_dir}/result.json" >"${evidence_dir}/result.tmp" || return $?
    mv "${evidence_dir}/result.tmp" "${evidence_dir}/result.json" || return $?
    if ((status != 0)); then
        printf 'startup status publication failed: %s (command %s); see diagnostics/startup-status\n' \
            "${outcome}" "${command_status}" >&2
    fi
    return "${status}"
}

# One bounded child owns both the byte copy and atomic publication. A script also gives tracing
# and coverage a source identity for the actual filesystem operations.
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    set -euo pipefail
    cp --reflink=never -- "${1:?source transcript}" "${2:?staging transcript}"
    mv -- "${2}" "${3:?published transcript}"
fi
