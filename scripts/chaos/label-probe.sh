#!/usr/bin/env bash
# Sourced by recovery-scenario.sh. The label probe of the restart and recovery scenarios: labels on
# the baseline domain, the models of its graph, a logical resource and a cluster user, edited
# through the packaged CLI before and during a fault and read back through each node's own public
# route as one typed JSON inspection per owner. A label edit commits with the entity it describes,
# so every node that has applied the same committed revision reports the same complete maps, an
# acknowledged edit is in them, and an uncertain one is either in every node's map or in none. A
# domain archive carries the labels of the domain, its models and its resources and holds no user.

# The cluster user and the logical resource the probe creates and labels. The user's password only
# lets the account exist; nothing signs in with it.
label_probe_user=chaos_label_user
label_probe_resource=chaos_label_resource

# The owners the probe labels, by a file label. `domain` names the domain the probe reads in; every
# other owner keeps its address in whichever domain is read.
label_probe_owners=(domain user schema relay ingestor emitter resource)

# Prints the entity reference of OWNER when it is read in DOMAIN.
label_probe_target() {
    local owner="$1"
    local domain="$2"
    case "${owner}" in
        domain) printf 'DOMAIN %s\n' "${domain}" ;;
        user) printf 'USER %s\n' "${label_probe_user}" ;;
        schema) printf 'SCHEMA chaos_record\n' ;;
        relay) printf 'RELAY chaos_records\n' ;;
        ingestor) printf 'INGESTOR chaos_ingestor\n' ;;
        emitter) printf 'EMITTER chaos_emitter\n' ;;
        resource) printf 'RESOURCE %s\n' "${label_probe_resource}" ;;
        *) return 1 ;;
    esac
}

# Creates the probe's user and resource and labels every owner through HOST, before any fault.
# Each change must be acknowledged, because nothing has disturbed the cluster yet.
label_probe_install() {
    local case_dir="$1"
    local host="$2"
    local outcome
    outcome="$(configuration_change "${case_dir}" chaos_label_user "${host}" chaos_baseline \
        "CREATE USER ${label_probe_user} WITH PASSWORD 'chaos-label-probe' LABELS { 'probe' = 'installed', 'empty' = '' };" \
        "created user '${label_probe_user}'" label-install)"
    [[ "${outcome}" == acknowledged ]] \
        || recovery_fail product "the label probe's user creation was ${outcome}, not acknowledged"
    outcome="$(configuration_change "${case_dir}" chaos_label_resource "${host}" chaos_baseline \
        "CREATE RESOURCE ${label_probe_resource} LABELS { 'probe' = 'installed' };" \
        "created resource '${label_probe_resource}'" label-install)"
    [[ "${outcome}" == acknowledged ]] \
        || recovery_fail product "the label probe's resource creation was ${outcome}, not acknowledged"
    label_probe_edit "${case_dir}" "${host}" probe installed
    jq -s -e '[.[] | select(.kind == "label" and .key == "probe")] | all(.outcome == "acknowledged")' \
        "${case_dir}/changes.ndjson" >/dev/null \
        || recovery_fail product 'a label probe installation edit was not acknowledged'
}

# Sets KEY to VALUE on every probed owner through HOST, one CLI session per edit, and records each
# outcome in CASE_DIR/changes.ndjson with the owner it targets. Acknowledged and uncertain edits
# are judged later through every node.
label_probe_edit() {
    local case_dir="$1"
    local host="$2"
    local key="$3"
    local value="$4"
    local pairs=()
    local owner target
    for owner in "${label_probe_owners[@]}"; do
        target="$(label_probe_target "${owner}" chaos_baseline)"
        pairs+=("label-${key}-${owner}" "SET LABELS ON ${target} { '${key}' = '${value}' };")
    done
    admin_cli_batch "${host}" chaos_baseline "${case_dir}/changes" 120 "${pairs[@]}" || true
    for owner in "${label_probe_owners[@]}"; do
        target="$(label_probe_target "${owner}" chaos_baseline)"
        local name="label-${key}-${owner}"
        jq -nc \
            --arg name "${name}" \
            --arg host "${host}" \
            --arg statement "SET LABELS ON ${target} { '${key}' = '${value}' };" \
            --arg outcome "$(batch_write_outcome "${case_dir}/changes" "${name}" 'updated labels on')" \
            --arg target "${owner}" \
            --arg key "${key}" \
            --arg value "${value}" \
            '{name:$name,entry_host:$host,domain:"chaos_baseline",statement:$statement,outcome:$outcome,
              kind:"label",target:$target,key:$key,value:$value}' \
            >>"${case_dir}/changes.ndjson"
    done
}

# Reads every probed owner's labels through HOST's own public route, in DOMAIN when one is given
# and in the baseline domain otherwise, into OUTPUT_DIR and writes OUTPUT_DIR.json mapping each
# owner to its complete label map, or to null where the read did not answer with one typed
# committed inspection. The map file is written even when the batch itself fails, so a judge reads
# an unanswered owner rather than a missing file. Returns 1 unless every owner answered, so a wait
# repeats the read until the node serves every inspection.
label_probe_read() {
    local host="$1"
    local output_dir="$2"
    local domain="${3:-chaos_baseline}"
    local pairs=()
    local owner
    for owner in "${label_probe_owners[@]}"; do
        pairs+=("labels-${owner}" "SHOW LABELS ON $(label_probe_target "${owner}" "${domain}") FORMAT JSON;")
    done
    local batch_status=0
    admin_cli_batch "${host}" "${domain}" "${output_dir}" 120 "${pairs[@]}" || batch_status=$?
    mkdir -p "${output_dir}"
    local maps
    maps="$(mktemp "${output_dir}/.labels.XXXXXX")"
    for owner in "${label_probe_owners[@]}"; do
        local map=null
        if batch_read_succeeded "${output_dir}" "labels-${owner}"; then
            map="$(jq -c 'select(.view.state == "committed") | .labels' \
                "${output_dir}/labels-${owner}.txt" 2>/dev/null || true)"
            [[ -n "${map}" ]] || map=null
        fi
        jq -nc --arg key "${owner}" --argjson value "${map}" '{key: $key, value: $value}' >>"${maps}"
    done
    jq -s 'from_entries' "${maps}" >"${output_dir}.json"
    rm -f "${maps}"
    ((batch_status == 0)) || return 1
    jq -e 'all(.[]; . != null)' "${output_dir}.json" >/dev/null
}

# Judges the label maps HOSTS each reported, read by label_probe_read into READ_DIR/HOST.json,
# against the edits CASE_DIR/changes.ndjson records, and writes the verdict to OUTPUT. Every host
# must report one map per owner, every host the same map, and that map must hold every acknowledged
# edit. An uncertain edit is in every host's map or in none, which the equality already requires.
# Returns 1 when the verdict is not a pass.
label_probe_judge() {
    local case_dir="$1"
    local read_dir="$2"
    local output="$3"
    shift 3
    local hosts=("$@")
    local reads=()
    local host
    for host in "${hosts[@]}"; do
        reads+=("${read_dir}/${host}.json")
    done
    local hosts_json
    hosts_json="$(printf '%s\n' "${hosts[@]}" | jq -R . | jq -sc .)"
    jq -s \
        --slurpfile changes "${case_dir}/changes.ndjson" \
        --argjson hosts "${hosts_json}" '
        . as $maps
        | ($maps[0] | keys) as $owners
        | [$changes[] | select(.kind == "label" and .outcome == "acknowledged")] as $acknowledged
        | {hosts: $hosts,
           unanswered: [range(0; $maps | length) as $i | $owners[] as $owner
                        | select($maps[$i][$owner] == null) | {host: $hosts[$i], owner: $owner}],
           disagreements: [$owners[] as $owner
                           | select([$maps[][$owner]] | unique | length > 1) | $owner],
           missing: [$acknowledged[] as $edit
                     | select(any($maps[]; .[$edit.target][$edit.key] != $edit.value))
                     | $edit.name],
           acknowledged_edits: ($acknowledged | length),
           uncertain_edits: ([$changes[] | select(.kind == "label" and .outcome == "uncertain")] | length),
           maps: $maps[0]}
        | .passed = ((.unanswered | length) == 0 and (.disagreements | length) == 0
                     and (.missing | length) == 0)' \
        "${reads[@]}" >"${output}"
    jq -e '.passed' "${output}" >/dev/null
}

# Judges a domain backup's label scope from three reads by label_probe_read: SOURCE, the baseline
# domain before the backup; COPY, the domain the archive restored under another name; and USER,
# the baseline read after the restore. DESCRIPTION is the archive's offline description. The copy
# must carry exactly the source's domain, model and resource maps, the archive must be a domain
# archive that holds no user, and the user must keep its own map. Writes the verdict to OUTPUT and
# returns 1 unless it passes.
label_probe_judge_backup_scope() {
    local source="$1"
    local copy="$2"
    local after="$3"
    local description="$4"
    local output="$5"
    jq -n \
        --slurpfile source "${source}" \
        --slurpfile copy "${copy}" \
        --slurpfile after "${after}" \
        --slurpfile description "${description}" '
        ($source[0] | del(.user)) as $scoped
        | {copied: ($copy[0] | del(.user)),
           expected: $scoped,
           scope: $description[0].scope,
           archived_users: $description[0].users,
           user_before: $source[0].user,
           user_after: $after[0].user}
        | .passed = (.copied == .expected and .scope == "domain" and .archived_users == null
                     and .user_before != null and .user_after == .user_before)' \
        >"${output}"
    jq -e '.passed' "${output}" >/dev/null
}

# Backs the baseline domain up through HOST, describes the archive offline, restores it beside the
# baseline as chaos_label_copy, and judges the label scope of the result into CASE_DIR/backup-scope.
# Nothing edits labels meanwhile, so the source read before the backup is the archive's cut.
label_probe_backup_scope() {
    local case_dir="$1"
    local host="$2"
    local scope_dir="${case_dir}/backup-scope"
    local archive_dir="${scope_dir}/archive"
    mkdir -p "${archive_dir}"
    wait_for "the probed owners' labels through ${host} before the backup" 60 \
        label_probe_read "${host}" "${scope_dir}/source" || return 1
    compose run --rm --no-deps --user "$(id -u):$(id -g)" \
        -v "${archive_dir}:/chaos-backup" admin \
        nervix-cli --server "http://${host}:47391" \
        --domain chaos_baseline --password "${CHAOS_PASSWORD}" \
        backup domain chaos_baseline --output /chaos-backup/labels.nvxb \
        --timeout 60s --format json >"${scope_dir}/backup.json" 2>"${scope_dir}/backup.stderr" \
        || return 1
    compose run --rm --no-deps --user "$(id -u):$(id -g)" \
        -v "${archive_dir}:/chaos-backup:ro" admin \
        nervix-cli --command "DESCRIBE BACKUP '/chaos-backup/labels.nvxb' FORMAT JSON;" \
        >"${scope_dir}/description.json" 2>"${scope_dir}/description.stderr" || return 1
    compose run --rm --no-deps --user "$(id -u):$(id -g)" \
        -v "${archive_dir}:/chaos-backup:ro" admin \
        nervix-cli --server "http://${host}:47391" \
        --domain chaos_baseline --password "${CHAOS_PASSWORD}" \
        restore domain chaos_baseline --as chaos_label_copy \
        --input /chaos-backup/labels.nvxb --format json \
        >"${scope_dir}/restore.json" 2>"${scope_dir}/restore.stderr" || return 1
    wait_for "the restored copy's labels through ${host}" 60 \
        label_probe_read "${host}" "${scope_dir}/copy" chaos_label_copy || return 1
    wait_for "the probed owners' labels through ${host} after the restore" 60 \
        label_probe_read "${host}" "${scope_dir}/after" || return 1
    label_probe_judge_backup_scope "${scope_dir}/source.json" "${scope_dir}/copy.json" \
        "${scope_dir}/after.json" "${scope_dir}/description.json" "${scope_dir}/verdict.json"
}
