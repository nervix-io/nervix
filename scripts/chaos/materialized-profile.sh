#!/usr/bin/env bash
# Sourced by the stateful and lifetimes scenarios. Owns their complete materialized-profile read
# through the public JSON envelope; version/absence predicates remain with each scenario.

materialized_profile_read() {
    local relay="$1"
    local host="$2"
    local output="$3"
    local previous_host="${cli_host}"
    local status=0
    # Never let a failed attempt reuse an earlier successful observation.
    rm -f "${output}.json" "${output}.json.next" "${output}.inspection.json" "${output}.reader-error.txt"
    cli_host="${host}"
    domain_cli_command "SHOW RELAY ${relay} MATERIALIZED STATE FORMAT JSON;" >"${output}.txt" 2>&1 || status=$?
    cli_host="${previous_host}"
    ((status == 0)) || return 1
    # Compose's own progress lines precede the pretty JSON document. Keep both the full transcript
    # and exact envelope so a reader refusal can be diagnosed independently of the product rows.
    sed -n '/^{/,$p' "${output}.txt" >"${output}.inspection.json"
    if ! jq -s --arg relay "${relay}" -f "${script_dir}/materialized-profile.jq" \
        "${output}.inspection.json" >"${output}.json.next" 2>"${output}.reader-error.txt"; then
        rm -f "${output}.json.next"
        failure_category=controller
        printf 'materialized profile inspection could not be read: %s\n' "${output}.reader-error.txt" >&2
        # These functions are wait predicates. Returning false would turn unreadable evidence into
        # a product timeout or even a successful branch-absence assertion. End the controller here
        # so its normal exit trap retains the reader error and this exact failure category.
        exit 1
    fi
    mv -f "${output}.json.next" "${output}.json"
}
