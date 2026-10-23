#!/usr/bin/env bash
# Sourced by the chaos runner, the suite and their self-tests. The verifier self-check,
# tests/self-test.sh, proves the external verifiers on this worker before they judge a run. A
# standalone run executes it itself. A suite executes it once before its first run and hands every
# run its record, which a run adopts only when the check passed on this worker, during this boot,
# for exactly the chaos scripts the run executes.

# The bound of one verifier self-check. A loaded worker stretches the check to minutes; the bound
# only ends a check that hangs.
# shellcheck disable=SC2034
chaos_verifier_self_check_seconds=600

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
