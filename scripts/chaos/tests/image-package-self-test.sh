#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
chaos_dir="$(cd "${script_dir}/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fail() {
    printf 'image package self-test failed: %s\n' "$*" >&2
    exit 1
}

# shellcheck source=../image-package-check.sh
source "${chaos_dir}/image-package-check.sh"
image_id="sha256:$(printf 'a%.0s' {1..64})"
run_id='image-package-control'
remaining_seconds() { printf '1000\n'; }
probe_status=124
inspect_status=0
inspect_invalid=false
container_exit=0
run_bounded() {
    local bound="$1"
    shift
    if [[ "$1" == docker && "$2" == inspect ]]; then
        [[ "${bound}" -eq 20 && "$3" == "${run_id}-image-package" ]] || return 73
        if ((inspect_status != 0)); then return "${inspect_status}"; fi
        if [[ "${inspect_invalid}" == true ]]; then printf '[]\n'; return 0; fi
        jq -nc --argjson status "${container_exit}" '[{State:{Status:"exited",ExitCode:$status}}]'
        return 0
    fi
    [[ "${bound}" -eq 30 && "$1" == docker && "$2" == run ]] || return 73
    local argument _input
    for argument in "$@"; do
        [[ "${argument}" != --rm ]] || return 73
    done
    if IFS= read -r _input; then return 74; fi
    [[ "$*" == *'--interactive=false'* && "$*" == *'--network none'* \
        && "$*" == *"io.nervix.chaos.run=${run_id}"* && "$*" == *'io.nervix.chaos.role=admin'* ]] || return 73
    printf 'image probe command outcome %s\n' "${probe_status}" >&2
    return "${probe_status}"
}

begin_case() {
    artifact_dir="${tmp_dir}/$1"
    mkdir -p "${artifact_dir}/diagnostics"
}

begin_case refused
status=0
verify_image_package 2>"${artifact_dir}/error.txt" || status=$?
[[ "${status}" -eq 124 ]] || fail 'the refused preflight lost its original outcome'
jq -e '.command_exit_code == 124 and .outcome == "command-failed" and .operation == "image-package"' \
    "${artifact_dir}/diagnostics/image-package/result.json" >/dev/null \
    || fail 'the native preflight outcome was discarded or called missing executables'
grep -q 'image probe command outcome 124' "${artifact_dir}/diagnostics/image-package/stderr.txt" \
    || fail 'the raw command refusal was lost'

begin_case passed
probe_status=0
verify_image_package || fail 'a completed probe did not pass'
jq -e '.outcome == "passed" and .command_exit_code == 0 and .inspection_exit_code == 0
    and .container_state.ExitCode == 0 and .effective_seconds == 30' \
    "${artifact_dir}/diagnostics/image-package/result.json" >/dev/null || fail 'successful helper state was lost'

begin_case inspection-refused
inspect_status=75
status=0
verify_image_package 2>"${artifact_dir}/error.txt" || status=$?
[[ "${status}" -eq 75 ]] || fail 'an inspection refusal passed or changed its status'
jq -e '.outcome == "inspection-failed" and .inspection_exit_code == 75' \
    "${artifact_dir}/diagnostics/image-package/result.json" >/dev/null || fail 'the inspection refusal was lost'

begin_case container-refused
inspect_status=0
container_exit=1
status=0
verify_image_package 2>"${artifact_dir}/error.txt" || status=$?
[[ "${status}" -eq 1 ]] || fail 'a failed helper predicate passed'
jq -e '.outcome == "container-refused" and .container_state.ExitCode == 1' \
    "${artifact_dir}/diagnostics/image-package/result.json" >/dev/null || fail 'the predicate failure was lost'

begin_case inspection-invalid
container_exit=0
inspect_invalid=true
status=0
verify_image_package 2>"${artifact_dir}/error.txt" || status=$?
[[ "${status}" -eq 1 ]] || fail 'invalid inspection evidence passed'
jq -e '.outcome == "inspection-invalid" and .inspection_exit_code == 0 and .container_state == null' \
    "${artifact_dir}/diagnostics/image-package/result.json" >/dev/null || fail 'invalid inspection changed the native command status'

printf 'image package controls passed\n'
