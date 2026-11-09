#!/usr/bin/env bash
# Sourced by the chaos runner, the suite and their self-tests. The verifier self-check,
# tests/self-test.sh, proves the external verifiers on this worker before they judge a run. A
# standalone run executes it itself. A suite executes it once before its first run and hands every
# run its record, which a run adopts only when the check passed on this worker, during this boot,
# for exactly the chaos scripts the run executes.
#
# Whichever controller executes the check owns everything the check leaves. The check leads a
# session of its own, and every part of it that gives Docker resources a run label of its own
# records that label in the check's evidence directory first. Once the check has ended, by itself,
# at its deadline or by its controller's signal, the controller kills whatever is left of the
# session and removes what every recorded label still owns, keeping what it found beside the
# partial records a failed or interrupted check leaves in the same directory.

chaos_verifier_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=sessions.sh
source "${chaos_verifier_dir}/sessions.sh"

# The bound of one verifier self-check. A loaded worker stretches the check to minutes; the bound
# only ends a check that hangs.
# shellcheck disable=SC2034
chaos_verifier_self_check_seconds=600

# Records RUN_ID, an io.nervix.chaos.run label a part of the verifier self-check is about to give
# Docker resources, in run-ids.txt of the evidence directory that tests/self-test.sh exports from
# --evidence as CHAOS_SELF_TEST_EVIDENCE. The part records the label before it creates anything, so
# the check's controller removes what the label owns even when a deadline or a signal ends the part
# before its own cleanup does. A check executed without an evidence directory has no controller,
# and nothing is recorded.
chaos_verifier_register_run() {
    local run_id="$1"
    [[ -n "${CHAOS_SELF_TEST_EVIDENCE:-}" ]] || return 0
    mkdir -p "${CHAOS_SELF_TEST_EVIDENCE}" \
        && printf '%s\n' "${run_id}" >>"${CHAOS_SELF_TEST_EVIDENCE}/run-ids.txt"
}

# The process number and start time of the session leader of the verifier self-check this controller
# executes, set while the check has not been ended.
chaos_verifier_pid=""
chaos_verifier_start_ticks=""

# Starts COMMAND as the verifier self-check, bounded to LIMIT seconds, with --evidence EVIDENCE and
# its console in OUTPUT, in the background and leading a session of its own. Its bound passes a
# deadline's TERM, or the one TERM its controller sends the bound, to the check's process group,
# kills COMMAND five seconds later if it still executes, and exits with COMMAND.
chaos_verifier_start() {
    local limit="$1"
    local evidence="$2"
    local output="$3"
    shift 3
    setsid timeout --kill-after=5s "${limit}s" "$@" --evidence "${evidence}" >"${output}" 2>&1 &
    chaos_verifier_pid=$!
    chaos_verifier_start_ticks="$(process_start_ticks "${chaos_verifier_pid}")"
}

# Ends the verifier self-check chaos_verifier_start started and removes what it left, keeping the
# evidence in EVIDENCE. A check that still executes receives one TERM through its bound. The bound
# exits with the command it started, which ends at once on TERM, while the parts of the check still
# run their own exit work, and a part can leave the check's process group; so once the bound has
# exited, whatever is left of the check's session is killed. Then every run label the check
# recorded is cleaned up. The check counts as ended only after that, so an exit trap that a signal
# runs in between ends it again. Returns 1 when a recorded label still owns something cleanup could
# not remove.
chaos_verifier_end() {
    local evidence="$1"
    [[ -n "${chaos_verifier_pid}" ]] || return 0
    if process_executes "${chaos_verifier_pid}" "${chaos_verifier_start_ticks}"; then
        kill -TERM "${chaos_verifier_pid}" 2>/dev/null || true
    fi
    wait "${chaos_verifier_pid}" 2>/dev/null || true
    local killed
    killed="$(kill_session "${chaos_verifier_pid}" "${chaos_verifier_start_ticks}")"
    if ((killed > 0)); then
        printf 'verifier self-check: killed %d processes left in its session\n' "${killed}"
    fi
    local status=0
    chaos_verifier_reclaim "${evidence}" || status=1
    chaos_verifier_pid=""
    chaos_verifier_start_ticks=""
    return "${status}"
}

# Removes what every run label recorded in EVIDENCE/run-ids.txt still owns, with the chaos cleanup
# beside this file, which keeps what it found under EVIDENCE/cleanup/RUN_ID. A label that owns
# nothing leaves no evidence. Returns 1 when a label's cleanup failed.
chaos_verifier_reclaim() {
    local evidence="$1"
    local register="${evidence}/run-ids.txt"
    [[ -s "${register}" ]] || return 0
    local run_id
    local status=0
    while IFS= read -r run_id; do
        [[ -n "${run_id}" ]] || continue
        timeout --foreground --kill-after=5s 90s "${chaos_verifier_dir}/cleanup.sh" --run-id "${run_id}" \
            --evidence "${evidence}/cleanup/${run_id}" --quiet || status=1
    done < <(LC_ALL=C sort -u "${register}")
    return "${status}"
}

# Forgets the run labels a passing verifier self-check recorded in EVIDENCE when cleanup found
# nothing they still owned, so a passing check leaves no evidence directory. Cleanup evidence keeps
# the record beside it.
chaos_verifier_forget() {
    local evidence="$1"
    [[ ! -e "${evidence}/cleanup" ]] || return 0
    rm -f "${evidence}/run-ids.txt"
    rmdir "${evidence}" 2>/dev/null || true
}

# Prints the identity of the chaos scripts under CHAOS_DIR: the SHA-256 digest of every regular
# file's path and content digest, in byte order of the paths.
chaos_scripts_identity() {
    local chaos_dir="$1"
    local listing
    listing="$(cd "${chaos_dir}" && find . -type f -print0 | LC_ALL=C sort -z | xargs -0 -r openssl dgst -sha256 -r)" \
        || return 1
    [[ -n "${listing}" ]] || return 1
    printf '%s\n' "${listing}" | openssl dgst -sha256 -r | awk '{ print $1 }'
}

# Prints the identity of this boot of this worker, which the kernel draws anew at every boot.
chaos_boot_identity() {
    local boot_id
    boot_id="$(cat /proc/sys/kernel/random/boot_id 2>/dev/null)" || return 1
    [[ -n "${boot_id}" ]] || return 1
    printf '%s\n' "${boot_id}"
}

# Judges RECORD, the verifier self-check record a suite wrote, for a run of the chaos scripts under
# CHAOS_DIR on this worker. Prints why the record does not apply and returns 1, or prints nothing
# and returns 0. The record names its output beside it, and binds a passing check to the scripts it
# checked and the boot it ran on.
chaos_verifier_record_refusal() {
    local record="$1"
    local chaos_dir="$2"
    if [[ ! -s "${record}" ]]; then
        printf 'it does not exist'
        return 1
    fi
    if ! jq -e 'type == "object"' "${record}" >/dev/null 2>&1; then
        printf 'it is not a JSON object'
        return 1
    fi
    local missing
    missing="$(jq -r '
        [ (if (.status | type) != "string" then "status" else empty end),
          (if (.exit_code | type) != "number" then "exit_code" else empty end),
          (if (.started_at | type) != "string" then "started_at" else empty end),
          (if (.finished_at | type) != "string" then "finished_at" else empty end),
          (if (.duration_seconds | type) != "number" then "duration_seconds" else empty end),
          (if (.bound_seconds | type) != "number" then "bound_seconds" else empty end),
          (if (.output | type) != "string" then "output" else empty end),
          (if (.output_sha256 | type) != "string" then "output_sha256" else empty end),
          (if (.scripts_sha256 | type) != "string" then "scripts_sha256" else empty end),
          (if (.boot_id | type) != "string" then "boot_id" else empty end) ]
        | join(", ")' "${record}")"
    if [[ -n "${missing}" ]]; then
        printf 'it does not record %s' "${missing}"
        return 1
    fi
    local status exit_code
    status="$(jq -r '.status' "${record}")"
    exit_code="$(jq -r '.exit_code' "${record}")"
    if [[ "${status}" != passed || "${exit_code}" != 0 ]]; then
        printf 'it records status %s, exit code %s' "${status}" "${exit_code}"
        return 1
    fi
    local scripts
    scripts="$(jq -r '.scripts_sha256' "${record}")"
    if [[ "${scripts}" != "$(chaos_scripts_identity "${chaos_dir}")" ]]; then
        printf 'it checked chaos scripts %s, not these' "${scripts}"
        return 1
    fi
    local boot current_boot
    boot="$(jq -r '.boot_id' "${record}")"
    current_boot="$(chaos_boot_identity)" || current_boot=""
    if [[ -z "${current_boot}" || "${boot}" != "${current_boot}" ]]; then
        printf 'it ran on boot %s of a worker, not this one' "${boot}"
        return 1
    fi
    local output
    output="$(jq -r '.output' "${record}")"
    if [[ "${output}" == */* || "${output}" == . || "${output}" == .. || -z "${output}" ]]; then
        printf 'its output must be a file beside it, not %s' "${output}"
        return 1
    fi
    local output_path
    output_path="$(dirname "${record}")/${output}"
    if [[ ! -f "${output_path}" ]]; then
        printf 'its output %s is missing' "${output}"
        return 1
    fi
    if [[ "$(openssl dgst -sha256 -r "${output_path}" | awk '{ print $1 }')" != "$(jq -r '.output_sha256' "${record}")" ]]; then
        printf 'its output %s no longer has the digest it records' "${output}"
        return 1
    fi
    return 0
}
