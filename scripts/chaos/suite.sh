#!/usr/bin/env bash
# Runs the suites of `just chaos suite`: named selections from suites.json, each entry executed as
# the `just chaos run` command it lists, one after another, against one already-built image. Every
# run keeps its own verdict and evidence in its own directory; the suite adds suite.json and
# summary.md beside them. CI runs one shard of a suite per job with exactly these commands.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
suites_file="${script_dir}/suites.json"
report_program="${script_dir}/suite-report.jq"
# A run's teardown after its own --timeout is bounded on its own and fits this reserve, the
# runner's teardown_reserve_seconds; a shard's step budget keeps it for the last run.
teardown_reserve_minutes=15

usage() {
    cat <<'EOF'
Usage:
  just chaos suite list
  just chaos suite SUITE --image IMAGE [--shard N] [--entry ID]... [--artifacts DIR] [--suite-id ID]
  just chaos suite shards SUITE
  just chaos suite cleanup SUITE_DIRECTORY
  just chaos suite report [--expect-shards N] SUITE_JSON...

SUITE names a suite of suites.json, such as smoke or soak. Each entry runs as its own
`just chaos run` command with the entry's budget as --timeout, and every entry runs even after one
fails. --shard runs one shard of the suite and --entry the named entries; both may be combined.
The suite writes ARTIFACTS/SUITE_ID/suite.json, summary.md, one directory per run and one console
log per entry under logs/. It exits 0 when every entry passed, 1 when any failed or did not run,
2 for a setup error before any run, and 128 plus the signal number when it was interrupted.

shards prints the CI matrix of a suite: each shard's entries, its execution budget, the step
budget that adds one run teardown reserve, and the job budget that adds the CI reserve for evidence,
cleanup and upload.

cleanup ends what a suite left behind once its controller has stopped: for every run of the suite
it captures bounded evidence of the Docker resources still carrying the run's label and removes
them, then records the entries that never reported a verdict as interrupted.

report renders the summary of one or more suite.json files, such as every shard of a suite, and
exits nonzero when any of them did not pass or fewer than --expect-shards were given.
EOF
}

suite_error() {
    printf 'chaos suite setup error: %s\n' "$*" >&2
    exit 2
}

for command_name in jq timeout date awk; do
    command -v "${command_name}" >/dev/null 2>&1 \
        || suite_error "required command is unavailable: ${command_name}"
done

now() {
    date -u +%Y-%m-%dT%H:%M:%SZ
}

# Refuses a definitions file that would give a suite an ambiguous selection, an unbounded run or an
# option the suite sets itself.
validate_definitions() {
    [[ -s "${suites_file}" ]] || suite_error "the suite definitions do not exist: ${suites_file}"
    jq -e 'type == "object"' "${suites_file}" >/dev/null 2>&1 \
        || suite_error "the suite definitions are not a JSON object: ${suites_file}"
    local problems
    problems="$(jq -r '
        def whole: type == "number" and . == floor;
        def entry_problems($suite):
            . as $entry
            | ($entry.id | tostring) as $id
            | (if ($entry.id | type == "string" and test("^[a-z0-9][a-z0-9-]{0,39}$")) | not
               then "\($suite): entry id \($id) must be 1 to 40 lowercase letters, digits or hyphens" else empty end),
              (if ($entry.shard | whole and . >= 1) | not
               then "\($suite)/\($id): shard must be a positive integer" else empty end),
              (if ($entry.budget_minutes | whole and . >= 2 and . <= 360) | not
               then "\($suite)/\($id): budget_minutes must be an integer from 2 through 360" else empty end),
              (if ($entry.run | type == "array" and length > 0 and all(.[]; type == "string")) | not
               then "\($suite)/\($id): run must list the arguments of just chaos run" else empty end),
              (if ($entry.run | type == "array")
                  and any($entry.run[]; IN("--image", "--artifacts", "--run-id", "--timeout", "--keep"))
               then "\($suite)/\($id): the suite sets --image, --artifacts, --run-id and --timeout itself and never keeps resources" else empty end),
              (if ($entry | has("note")) and ($entry.note | type != "string")
               then "\($suite)/\($id): note must be a string" else empty end),
              (($entry | keys) - ["id", "shard", "budget_minutes", "run", "note"] | .[]
               | "\($suite)/\($id): unknown field \(.)");
        [ (if (.ci_reserve_minutes | whole and . >= 1) | not
           then "ci_reserve_minutes must be a positive integer" else empty end),
          (if (.suites | type == "object" and length > 0) | not
           then "suites must define at least one suite" else empty end),
          ((keys) - ["ci_reserve_minutes", "suites"] | .[] | "unknown field \(.)"),
          (if .suites | type == "object" then .suites | to_entries[] else empty end
           | .key as $suite | .value as $definition
           | (if ($suite | test("^[a-z][a-z0-9-]{0,23}$")) and ($suite | IN("list", "shards", "cleanup", "report") | not)
              then empty
              else "suite name \($suite) must be 1 to 24 lowercase letters, digits or hyphens and not a suite command" end),
             (if ($definition.description | type == "string" and length > 0) | not
              then "\($suite): description must be a non-empty string" else empty end),
             (($definition | keys) - ["description", "entries"] | .[] | "\($suite): unknown field \(.)"),
             (if ($definition.entries | type == "array" and length > 0) | not
              then "\($suite): entries must list at least one entry"
              else
                ($definition.entries[] | entry_problems($suite)),
                ([$definition.entries[].id] | group_by(.)[] | select(length > 1)
                 | "\($suite): entry id \(.[0]) appears more than once"),
                ([$definition.entries[].shard] as $shards
                 | if all($shards[]; whole and . >= 1)
                      and ($shards | unique) != [range(1; ($shards | unique | length) + 1)]
                   then "\($suite): shards must be numbered from 1 without gaps" else empty end)
              end))
        ] | .[]' "${suites_file}")"
    if [[ -n "${problems}" ]]; then
        printf 'chaos suite setup error: the suite definitions in %s are invalid:\n' "${suites_file}" >&2
        printf '  %s\n' "${problems}" >&2
        exit 2
    fi
}

require_suite() {
    local suite="$1"
    jq -e --arg suite "${suite}" '.suites | has($suite)' "${suites_file}" >/dev/null \
        || suite_error "unknown chaos suite: ${suite}; the suites are $(jq -r '.suites | keys | join(", ")' "${suites_file}")"
}

list_suites() {
    jq -r '
        def command: "just chaos run " + (.run | map(if test("^[A-Za-z0-9_./:=@-]+$") then . else @sh end) | join(" "));
        .suites | to_entries[] | .key as $suite | .value
        | ([.entries[].shard] | unique) as $shards
        | "\($suite)  \(.description)",
          "  \($shards | length) shard(s), \([.entries[].budget_minutes] | add) budget minutes",
          ($shards[] as $shard
           | "  shard \($shard), \([.entries[] | select(.shard == $shard) | .budget_minutes] | add) budget minutes",
             (.entries[] | select(.shard == $shard)
              | "    \(.id)  \(.budget_minutes) min  \(command)",
                (if .note then "      \(.note)" else empty end))),
          ""' "${suites_file}"
}

print_shards() {
    local suite="$1"
    require_suite "${suite}"
    jq -c --arg suite "${suite}" --argjson teardown "${teardown_reserve_minutes}" '
        .ci_reserve_minutes as $reserve
        | .suites[$suite].entries
        | group_by(.shard)
        | map({shard: .[0].shard,
               entries: map(.id),
               execution_minutes: (map(.budget_minutes) | add)}
              | .step_minutes = .execution_minutes + $teardown
              | .job_minutes = .step_minutes + $reserve)' "${suites_file}"
}

# The suite record is rewritten whole through a temporary file, so a reader never sees half of it.
update_record() {
    local filter="$1"
    shift
    local tmp_path
    tmp_path="$(mktemp "${suite_dir}/.suite.XXXXXX")"
    if jq "$@" "${filter}" "${suite_dir}/suite.json" >"${tmp_path}"; then
        mv "${tmp_path}" "${suite_dir}/suite.json"
    else
        rm -f "${tmp_path}"
        return 1
    fi
}

render_summary() {
    jq -r -f "${report_program}" --slurp "${suite_dir}/suite.json" >"${suite_dir}/summary.md"
}

# Prints PATH when it holds a JSON object, and otherwise /dev/null, which jq slurps as no record.
optional_record() {
    local path="$1"
    if [[ -s "${path}" ]] && jq -e 'type == "object"' "${path}" >/dev/null 2>&1; then
        printf '%s\n' "${path}"
    else
        printf '%s\n' /dev/null
    fi
}

# Reads what run RUN_ID recorded in its directory and returns the entry's verdict as JSON. EXIT is
# the command's exit status, or "unknown" when the suite ended before the command reported one.
# SUITE_INTERRUPTED is true when a signal ended the suite while this run was executing.
entry_verdict() {
    local run_id="$1"
    local exit_status="$2"
    local suite_interrupted="$3"
    local suite_image_id="$4"
    local run_dir="${suite_dir}/${run_id}"
    local manifest finding ledger resources
    manifest="$(optional_record "${run_dir}/manifest.json")"
    finding="$(optional_record "${run_dir}/results/finding.json")"
    ledger="$(optional_record "${run_dir}/results/ledger.json")"
    resources="$(optional_record "${run_dir}/results/mixed-resources.json")"
    # The scenario's own result file, which embeds its progress record or names it.
    local result=/dev/null progress=/dev/null
    if [[ "${manifest}" != /dev/null ]]; then
        local scenario result_name
        scenario="$(jq -r '.scenario // ""' "${manifest}")"
        result_name="${scenario}"
        if [[ "${scenario}" == *-crash ]]; then
            result_name=crash
        fi
        if [[ -n "${result_name}" ]]; then
            result="$(optional_record "${run_dir}/results/${result_name}.json")"
        fi
        if [[ "${result}" != /dev/null ]]; then
            local progress_path
            progress_path="$(jq -r 'if (.progress | type) == "string" then .progress else "" end' "${result}")"
            if [[ -n "${progress_path}" ]]; then
                progress="$(optional_record "${run_dir}/${progress_path}")"
            fi
        fi
    fi
    local results_json='[]'
    if [[ -d "${run_dir}/results" ]]; then
        results_json="$(find "${run_dir}/results" -maxdepth 1 -type f -name '*.json' -size +0c -printf '%f\n' 2>/dev/null \
            | sort | jq -Rsc --arg run_id "${run_id}" 'split("\n") | map(select(length > 0) | "\($run_id)/results/\(.)")')"
    fi
    # Each optional record is read only when it exists; a missing one is null, never a default.
    jq -n \
        --arg run_id "${run_id}" \
        --arg exit "${exit_status}" \
        --argjson suite_interrupted "${suite_interrupted}" \
        --arg suite_image_id "${suite_image_id}" \
        --argjson results "${results_json}" \
        --slurpfile manifest_input "${manifest}" \
        --slurpfile finding_input "${finding}" \
        --slurpfile ledger_input "${ledger}" \
        --slurpfile result_input "${result}" \
        --slurpfile progress_input "${progress}" \
        --slurpfile resources_input "${resources}" '
        ($manifest_input[0] // null) as $manifest
        | ($finding_input[0] // null) as $finding
        | ($ledger_input[0] // null) as $ledger
        | ($result_input[0] // null) as $result
        | (if ($result.progress | type) == "object" then $result.progress else ($progress_input[0] // null) end) as $progress
        | ($resources_input[0] // null) as $resources
        | (if $exit == "unknown" then null else ($exit | tonumber) end) as $command_exit
        | ($manifest.exit_code // null) as $recorded_exit
        | ($command_exit // $recorded_exit) as $exit_code
        | (if $manifest == null and $command_exit == 2 then
             {status: "failed", category: "setup",
              reason: "the runner refused the entry before it created a run directory; its console log names the reason"}
           elif $manifest == null then
             {status: (if $command_exit == null or $suite_interrupted then "interrupted" else "failed" end),
              category: "controller",
              reason: "the run left no manifest.json, so it has no recorded verdict"}
           elif $command_exit != null and $recorded_exit != null and $command_exit != $recorded_exit
                and ($suite_interrupted | not) then
             {status: "failed", category: "controller",
              reason: "the run recorded exit status \($recorded_exit), but its command exited \($command_exit)"}
           elif $exit_code == null then
             {status: "interrupted", category: null,
              reason: "the suite ended before the run reported its exit status"}
           elif $exit_code == 0 and $manifest.status != "passed" then
             {status: "failed", category: "controller",
              reason: "the run exited 0, but its manifest records status \($manifest.status)"}
           elif $exit_code == 0 and $suite_image_id != "" and $manifest.resolved_image_id != $suite_image_id then
             {status: "failed", category: "controller",
              reason: "the run resolved image \($manifest.resolved_image_id), not the suite image \($suite_image_id)"}
           elif $exit_code == 0 then
             {status: "passed", category: null, reason: null}
           elif $suite_interrupted then
             {status: "interrupted", category: null,
              reason: "a signal ended the suite while the run executed"}
           elif $exit_code == 2 and ($manifest.setup_error // "") != "" then
             {status: "failed", category: "setup", reason: $manifest.setup_error}
           elif $manifest.status == "interrupted" then
             {status: "interrupted", category: null,
              reason: "a signal ended the run"}
           else
             {status: "failed",
              category: ($finding.category // null),
              reason: null}
           end) as $verdict
        | $verdict + {
            exit_code: $exit_code,
            run_status: ($manifest.status // null),
            final_phase: ($manifest.final_phase // null),
            timeout_seconds: ($manifest.timeout_seconds // null),
            teardown_seconds: ($manifest.teardown_seconds // null),
            image_id: ($manifest.resolved_image_id // null),
            seed: ($manifest.mixed.seed // null),
            policy: ($manifest.mixed.policy // null),
            reproducer: ($finding.reproducer // null),
            finding: (if $finding == null then null else "\($run_id)/results/finding.json" end),
            ledger: (if $ledger == null then null else
                       {verdict: $ledger.verdict,
                        expected: $ledger.expected_records,
                        observed: $ledger.observed_records,
                        duplicates: $ledger.duplicate_records,
                        missing: ($ledger.missing_ids | length),
                        unexpected: ($ledger.unexpected_ids | length),
                        incorrect: ($ledger.incorrect_content | length)} end),
            recovery: (if $progress == null then null else
                         ([ ($progress | to_entries[]
                             | select(.key | IN("election_ms", "placement_ms", "listener_recovery_ms",
                                                "settled_recovery_ms", "delivery_resume_ms"))
                             | select(.value | type == "number")),
                            ($progress.recovery_ms // {} | to_entries[]
                             | select(.value | type == "object" and (.max | type == "number"))
                             | {key: "\(.key)_max_ms", value: .value.max}) ]
                          | if length == 0 then null else from_entries end) end),
            resources: (if $resources == null then null else
                          ($resources.summary // {})
                          | {max_node_memory_bytes: ([.nodes[]?.max_memory_bytes | numbers] | max),
                             max_backlog: .max_backlog,
                             longest_output_stall_ms: .longest_output_stall_ms} end),
            results: $results
          }'
}

# Records ENTRY_INDEX's verdict in the suite record from what its run left behind.
record_entry() {
    local entry_index="$1"
    local exit_status="$2"
    local suite_interrupted="$3"
    local run_id finished_at duration verdict suite_image_id started_epoch
    run_id="$(jq -r --argjson index "${entry_index}" '.entries[$index].run_id' "${suite_dir}/suite.json")"
    suite_image_id="$(jq -r '.image.id // ""' "${suite_dir}/suite.json")"
    started_epoch="$(jq -r --argjson index "${entry_index}" '.entries[$index].started_epoch // empty' "${suite_dir}/suite.json")"
    finished_at="$(now)"
    duration=null
    if [[ -n "${started_epoch}" && "${exit_status}" != unknown ]]; then
        duration=$(($(date +%s) - started_epoch))
    fi
    verdict="$(entry_verdict "${run_id}" "${exit_status}" "${suite_interrupted}" "${suite_image_id}")"
    local artifacts=null
    if [[ -d "${suite_dir}/${run_id}" ]]; then
        artifacts="\"${run_id}\""
    fi
    # A run's own reproducer is the most precise one; any other entry repeats its suite command.
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_record '.entries[$index] |= (. + $verdict
            | .artifacts = $artifacts
            | .finished_at = (if $exit == "unknown" then null else $finished_at end)
            | .duration_seconds = $duration
            | .reproducer = (.reproducer // .display))' \
        --argjson index "${entry_index}" --argjson verdict "${verdict}" --argjson artifacts "${artifacts}" \
        --arg exit "${exit_status}" --arg finished_at "${finished_at}" --argjson duration "${duration}"
}

finish_suite() {
    local signal_name="$1"
    local status
    # The suite itself is interrupted only by a signal; an entry interrupted otherwise fails it.
    status="$(jq -r 'if all(.entries[]; .status == "passed") then "passed" else "failed" end' \
        "${suite_dir}/suite.json")"
    if [[ -n "${signal_name}" ]]; then
        status=interrupted
    fi
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_record '.status = $status
        | .finished_at = $finished_at
        | .duration_seconds = ($finished | tonumber) - .started_epoch
        | if $signal == "" then . else .signal = $signal end' \
        --arg status "${status}" --arg finished_at "$(now)" --arg finished "$(date +%s)" \
        --arg signal "${signal_name}"
    render_summary
    printf '\n'
    cat "${suite_dir}/summary.md"
    printf '\nchaos suite %s %s\n' "${suite_name}" "${status}"
    printf 'suite artifacts: %s\n' "${suite_dir}"
}

# A signal ends the run that is executing through its own exit trap, and no later entry starts. The
# run receives one TERM: a second one would end its exit trap while it heals and captures evidence.
suite_signal=""
child_pid=""
child_signalled=false
on_suite_signal() {
    suite_signal="$1"
    if [[ -n "${child_pid}" && "${child_signalled}" == false ]]; then
        child_signalled=true
        kill -TERM "${child_pid}" 2>/dev/null || true
    fi
}

run_suite() {
    suite_name="$1"
    shift
    local image_ref=""
    local artifact_root="target/chaos"
    local suite_id=""
    local shard=""
    local selected_entries=()
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --image)
                [[ "$#" -ge 2 ]] || suite_error '--image requires a value'
                image_ref="$2"
                shift 2
                ;;
            --artifacts)
                [[ "$#" -ge 2 ]] || suite_error '--artifacts requires a directory'
                artifact_root="$2"
                shift 2
                ;;
            --suite-id)
                [[ "$#" -ge 2 ]] || suite_error '--suite-id requires a value'
                suite_id="$2"
                shift 2
                ;;
            --shard)
                [[ "$#" -ge 2 ]] || suite_error '--shard requires a number'
                shard="$2"
                shift 2
                ;;
            --entry)
                [[ "$#" -ge 2 ]] || suite_error '--entry requires an entry id'
                selected_entries+=("$2")
                shift 2
                ;;
            -h | --help)
                usage
                exit 0
                ;;
            *)
                suite_error "unknown suite argument: $1"
                ;;
        esac
    done
    require_suite "${suite_name}"
    [[ -n "${image_ref}" ]] || suite_error '--image is required and must name an already-built Nervix image'
    if [[ -n "${shard}" && ! "${shard}" =~ ^[1-9][0-9]{0,2}$ ]]; then
        suite_error '--shard must be a positive integer'
    fi
    local entries_json
    entries_json="$(printf '%s\n' ${selected_entries[@]+"${selected_entries[@]}"} \
        | jq -Rsc 'split("\n") | map(select(length > 0))')"
    local unknown_entries
    unknown_entries="$(jq -r --arg suite "${suite_name}" --argjson wanted "${entries_json}" '
        [.suites[$suite].entries[].id] as $ids | $wanted - $ids | join(", ")' "${suites_file}")"
    [[ -z "${unknown_entries}" ]] \
        || suite_error "the ${suite_name} suite has no entry ${unknown_entries}"
    local selection
    selection="$(jq -c --arg suite "${suite_name}" --arg shard "${shard}" --argjson wanted "${entries_json}" '
        .suites[$suite].entries
        | map(select(($shard == "" or .shard == ($shard | tonumber))
                     and (($wanted | length) == 0 or (.id | IN($wanted[])))))' "${suites_file}")"
    [[ "$(jq 'length' <<<"${selection}")" -gt 0 ]] \
        || suite_error "the selection names no entry of the ${suite_name} suite${shard:+ in shard ${shard}}"
    if [[ -z "${suite_id}" ]]; then
        suite_id="${suite_name}-$(date -u +%Y%m%dt%H%M%Sz)-$$"
    fi
    # Every run id is the suite id and the entry id, within the runner's 96 characters.
    [[ "${suite_id}" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,47}$ ]] \
        || suite_error '--suite-id must be 1 to 48 letters, numbers, dots, underscores or hyphens'
    for command_name in docker openssl; do
        command -v "${command_name}" >/dev/null 2>&1 \
            || suite_error "required command is unavailable: ${command_name}"
    done

    mkdir -p "${artifact_root}"
    artifact_root="$(cd "${artifact_root}" && pwd)"
    suite_dir="${artifact_root}/${suite_id}"
    [[ ! -e "${suite_dir}" ]] || suite_error "the suite directory already exists: ${suite_dir}"
    mkdir -p "${suite_dir}/logs"

    local started_epoch
    started_epoch="$(date +%s)"
    jq -n \
        --arg suite "${suite_name}" \
        --arg suite_id "${suite_id}" \
        --arg shard "${shard}" \
        --arg description "$(jq -r --arg suite "${suite_name}" '.suites[$suite].description' "${suites_file}")" \
        --arg requested "${image_ref}" \
        --arg started_at "$(now)" \
        --argjson started_epoch "${started_epoch}" \
        --argjson selection "${selection}" \
        '{suite: $suite, suite_id: $suite_id,
          shard: (if $shard == "" then null else ($shard | tonumber) end),
          description: $description, status: "preparing",
          started_at: $started_at, started_epoch: $started_epoch,
          image: {requested: $requested},
          entries: [$selection[] | {id, shard, budget_minutes, note: (.note // null), run,
                                   run_id: "\($suite_id)-\(.id)", status: "pending"}]}' \
        >"${suite_dir}/suite.json"

    trap 'on_suite_signal INT' INT
    trap 'on_suite_signal TERM' TERM
    trap 'on_suite_signal HUP' HUP

    # The suite checks its own verdict logic before it relies on it, as every run checks its
    # verifiers.
    local self_check_status=0
    timeout --kill-after=5s 120s "${script_dir}/tests/suite-self-test.sh" \
        >"${suite_dir}/suite-self-test.txt" 2>&1 || self_check_status=$?
    if [[ "${self_check_status}" -ne 0 ]]; then
        # The dollars in this jq filter are jq variables, not shell expansion.
        # shellcheck disable=SC2016
        update_record '.status = "failed" | .self_check = "failed"
            | .entries |= map(. + {status: "not-started", reason: "the suite self-check failed"})
            | .error = {category: "controller", message: $message}' \
            --arg message "the suite self-check exited ${self_check_status}; see suite-self-test.txt"
        render_summary
        printf 'chaos suite controller failure: the suite self-check exited %d; see %s\n' \
            "${self_check_status}" "${suite_dir}/suite-self-test.txt" >&2
        exit 1
    fi
    update_record '.self_check = "passed"'

    # One immutable identity for every run: a digest reference stays as given, and any other
    # reference is replaced by the local image ID it resolves to now.
    if ! timeout --kill-after=5s 30s docker image inspect "${image_ref}" >/dev/null 2>&1; then
        printf 'pulling %s\n' "${image_ref}"
        if ! timeout --kill-after=10s 600s docker pull "${image_ref}" >"${suite_dir}/image-pull.txt" 2>&1; then
            # The dollars in this jq filter are jq variables, not shell expansion.
            # shellcheck disable=SC2016
            update_record '.status = "failed"
                | .entries |= map(. + {status: "not-started", reason: "the suite image is unavailable"})
                | .error = {category: "setup", message: $message}' \
                --arg message "the image ${image_ref} is neither local nor pullable; see image-pull.txt"
            render_summary
            suite_error "the image ${image_ref} is neither local nor pullable; see ${suite_dir}/image-pull.txt"
        fi
    fi
    local image_id repo_digests run_image
    image_id="$(timeout --kill-after=5s 30s docker image inspect --format '{{.Id}}' "${image_ref}")"
    # The dollars in this Go template are template variables, not shell expansion.
    # shellcheck disable=SC2016
    repo_digests="$(timeout --kill-after=5s 30s docker image inspect \
        --format '{{range $index, $digest := .RepoDigests}}{{if $index}},{{end}}{{$digest}}{{end}}' "${image_ref}")"
    run_image="${image_id}"
    if [[ "${image_ref}" == *@sha256:* ]]; then
        run_image="${image_ref}"
    fi

    local kernel docker_version compose_version cpus memory_bytes
    kernel="$(uname -sr)"
    docker_version="$(timeout --kill-after=5s 30s docker version --format '{{.Server.Version}}' 2>/dev/null || true)"
    compose_version="$(timeout --kill-after=5s 30s docker compose version --short 2>/dev/null || true)"
    cpus="$(getconf _NPROCESSORS_ONLN)"
    memory_bytes="$(awk '/^MemTotal:/ { printf "%d", $2 * 1024 }' /proc/meminfo)"
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_record '.status = "running"
        | .image += {id: $id, repo_digests: $digests, run_reference: $run_image}
        | .worker = {kernel: $kernel, docker: $docker, compose: $compose,
                     cpus: ($cpus | tonumber), memory_bytes: ($memory | tonumber)}
        | .entries |= map(. + {command: (["just", "chaos", "run"] + .run
                                         + ["--image", $run_image, "--timeout", (.budget_minutes * 60 | tostring)])}
                          | .display = (.command | map(if test("^[A-Za-z0-9_./:=@-]+$") then . else @sh end) | join(" ")))' \
        --arg id "${image_id}" --arg digests "${repo_digests}" --arg run_image "${run_image}" \
        --arg kernel "${kernel}" --arg docker "${docker_version}" --arg compose "${compose_version}" \
        --arg cpus "${cpus}" --arg memory "${memory_bytes}"

    local entry_count entry_index
    entry_count="$(jq '.entries | length' "${suite_dir}/suite.json")"
    for ((entry_index = 0; entry_index < entry_count; entry_index++)); do
        if [[ -n "${suite_signal}" ]]; then
            break
        fi
        local entry_id run_id budget_seconds
        entry_id="$(jq -r --argjson index "${entry_index}" '.entries[$index].id' "${suite_dir}/suite.json")"
        run_id="$(jq -r --argjson index "${entry_index}" '.entries[$index].run_id' "${suite_dir}/suite.json")"
        budget_seconds="$(jq -r --argjson index "${entry_index}" '.entries[$index].budget_minutes * 60' "${suite_dir}/suite.json")"
        local run_arguments=()
        mapfile -t run_arguments < <(jq -r --argjson index "${entry_index}" '.entries[$index].run[]' "${suite_dir}/suite.json")
        printf '\n==> chaos suite %s: %s (%d of %d)\n' "${suite_name}" "${entry_id}" \
            "$((entry_index + 1))" "${entry_count}"
        # The dollars in this jq filter are jq variables, not shell expansion.
        # shellcheck disable=SC2016
        update_record '.entries[$index] += {status: "running", started_at: $started_at,
                                            started_epoch: ($epoch | tonumber),
                                            log: "logs/\(.entries[$index].id).log"}' \
            --argjson index "${entry_index}" --arg started_at "$(now)" --arg epoch "$(date +%s)"
        # The run executes in the background so that a signal reaches the suite at once; the suite
        # passes it on, and the run's own exit trap heals, captures and cleans up as on any exit.
        local exit_status=0
        child_signalled=false
        bash "${script_dir}/chaos.sh" run "${run_arguments[@]}" \
            --image "${run_image}" --artifacts "${suite_dir}" --run-id "${run_id}" \
            --timeout "${budget_seconds}" \
            > >(tee "${suite_dir}/logs/${entry_id}.log") 2>&1 &
        child_pid=$!
        # A signal that arrived before the run's process was known still reaches it.
        if [[ -n "${suite_signal}" ]]; then
            on_suite_signal "${suite_signal}"
        fi
        while true; do
            exit_status=0
            wait "${child_pid}" || exit_status=$?
            # A signal interrupts wait while the run still tears down; wait again for its status.
            if ((exit_status <= 128)) || ! kill -0 "${child_pid}" 2>/dev/null; then
                break
            fi
        done
        child_pid=""
        local suite_interrupted=false
        if [[ -n "${suite_signal}" ]]; then
            suite_interrupted=true
        fi
        record_entry "${entry_index}" "${exit_status}" "${suite_interrupted}"
        printf '==> chaos suite %s: %s %s\n' "${suite_name}" "${entry_id}" \
            "$(jq -r --argjson index "${entry_index}" '.entries[$index].status' "${suite_dir}/suite.json")"
    done
    if [[ -n "${suite_signal}" ]]; then
        # The dollars in this jq filter are jq variables, not shell expansion.
        # shellcheck disable=SC2016
        update_record '.entries |= map(if .status == "pending"
            then . + {status: "not-started", reason: "a signal ended the suite before this entry started"}
            else . end)'
    fi
    finish_suite "${suite_signal}"
    case "${suite_signal}" in
        INT) exit 130 ;;
        TERM) exit 143 ;;
        HUP) exit 129 ;;
    esac
    if [[ "$(jq -r '.status' "${suite_dir}/suite.json")" == passed ]]; then
        exit 0
    fi
    exit 1
}

# Ends what a suite left behind after its controller stopped, possibly without its exit traps.
cleanup_suite() {
    [[ "$#" -eq 1 ]] || suite_error 'cleanup requires one suite directory'
    suite_dir="$1"
    if [[ ! -s "${suite_dir}/suite.json" ]]; then
        printf 'chaos suite cleanup: %s holds no suite record, so the suite started no run\n' "${suite_dir}"
        exit 0
    fi
    suite_dir="$(cd "${suite_dir}" && pwd)"
    suite_name="$(jq -r '.suite' "${suite_dir}/suite.json")"
    local cleanup_failed=false
    local cleanups='[]'
    local run_id
    while IFS= read -r run_id; do
        [[ -n "${run_id}" ]] || continue
        local cleanup_status=0
        local evidence_dir="${suite_dir}/cleanup/${run_id}"
        timeout --kill-after=10s 420s "${script_dir}/cleanup.sh" --run-id "${run_id}" \
            --evidence "${evidence_dir}" || cleanup_status=$?
        if [[ "${cleanup_status}" -ne 0 ]]; then
            cleanup_failed=true
        fi
        local cleanup_record=null
        if [[ -s "${evidence_dir}/cleanup.json" ]]; then
            cleanup_record="$(jq -c --arg run_id "${run_id}" '. + {evidence: "cleanup/\($run_id)"}' "${evidence_dir}/cleanup.json")"
        fi
        cleanups="$(jq -c --arg run_id "${run_id}" --argjson status "${cleanup_status}" \
            --argjson record "${cleanup_record}" \
            '. + [{run_id: $run_id, exit_code: $status, leftovers: $record}]' <<<"${cleanups}")"
        # A run ended before its exit trap ran still holds the private keys of its TLS material.
        if [[ -d "${suite_dir}/${run_id}/tls" ]]; then
            rm -f "${suite_dir}/${run_id}/tls/ca-key.pem" "${suite_dir}/${run_id}/tls/"*-key.pem \
                "${suite_dir}/${run_id}/tls/"*.csr "${suite_dir}/${run_id}/tls/"*.ext
        fi
    done < <(jq -r '.entries[] | select(.status != "pending" and .status != "not-started") | .run_id' "${suite_dir}/suite.json")

    local entry_count entry_index
    entry_count="$(jq '.entries | length' "${suite_dir}/suite.json")"
    for ((entry_index = 0; entry_index < entry_count; entry_index++)); do
        # A run that finished before its controller was killed keeps its own recorded verdict.
        if [[ "$(jq -r --argjson index "${entry_index}" '.entries[$index].status' "${suite_dir}/suite.json")" == running ]]; then
            record_entry "${entry_index}" unknown false
        fi
    done
    # The dollars in this jq filter are jq variables, not shell expansion.
    # shellcheck disable=SC2016
    update_record '.entries |= map(if .status == "pending"
            then . + {status: "not-started", reason: "the suite ended before this entry started"}
            else . end)
        | if .status == "preparing" or .status == "running" then .status = "interrupted" else . end
        | .cleanup = {at: $at, runs: $runs}' \
        --arg at "$(now)" --argjson runs "${cleanups}"
    render_summary
    printf 'chaos suite cleanup: %s\n' "${suite_dir}"
    jq -r '.cleanup.runs[] | "  \(.run_id): exit \(.exit_code)\(if .leftovers then ", removed \(.leftovers.containers) containers, \(.leftovers.networks) networks and \(.leftovers.volumes) volumes after capturing \(.leftovers.evidence)" else ", nothing left" end)"' \
        "${suite_dir}/suite.json"
    if [[ "${cleanup_failed}" == true ]]; then
        exit 1
    fi
}

report_suites() {
    local expected=""
    local files=()
    while [[ "$#" -gt 0 ]]; do
        case "$1" in
            --expect-shards)
                [[ "$#" -ge 2 ]] || suite_error '--expect-shards requires a number'
                expected="$2"
                shift 2
                ;;
            *)
                files+=("$1")
                shift
                ;;
        esac
    done
    if [[ -n "${expected}" && ! "${expected}" =~ ^[1-9][0-9]{0,2}$ ]]; then
        suite_error '--expect-shards must be a positive integer'
    fi
    local readable=()
    local file
    for file in ${files[@]+"${files[@]}"}; do
        if [[ -s "${file}" ]] && jq -e 'type == "object" and (.entries | type == "array")' "${file}" >/dev/null 2>&1; then
            readable+=("${file}")
        else
            printf 'chaos suite report: %s is not a suite record\n' "${file}" >&2
        fi
    done
    if ((${#readable[@]} > 0)); then
        jq -r -f "${report_program}" --slurp "${readable[@]}"
    fi
    local failed=false
    if ((${#readable[@]} != ${#files[@]})); then
        failed=true
    fi
    if [[ -n "${expected}" ]] && ((${#readable[@]} != expected)); then
        printf '\n**Missing evidence:** %d of %d shard verdicts are present.\n' "${#readable[@]}" "${expected}"
        failed=true
    fi
    if ((${#readable[@]} > 0)) && ! jq -e -s 'all(.[]; .status == "passed")' "${readable[@]}" >/dev/null; then
        failed=true
    fi
    if ((${#readable[@]} == 0)); then
        failed=true
    fi
    if [[ "${failed}" == true ]]; then
        exit 1
    fi
}

if [[ "$#" -eq 0 ]]; then
    usage >&2
    exit 2
fi
validate_definitions
case "$1" in
    list)
        [[ "$#" -eq 1 ]] || suite_error 'list takes no arguments'
        list_suites
        ;;
    shards)
        [[ "$#" -eq 2 ]] || suite_error 'shards requires one suite'
        print_shards "$2"
        ;;
    cleanup)
        shift
        cleanup_suite "$@"
        ;;
    report)
        shift
        report_suites "$@"
        ;;
    -h | --help | help)
        usage
        ;;
    -*)
        suite_error "a suite or suite command is required before $1"
        ;;
    *)
        run_suite "$@"
        ;;
esac
