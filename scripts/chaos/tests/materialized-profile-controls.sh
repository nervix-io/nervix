#!/usr/bin/env bash
# Sourced by the two verifier self-tests. Exercise their production scenario reader, including
# request identity, the complete observation and the controller's disposition of unreadable state.

check_materialized_profile_reader() {
    profile_reader="$1"
    profile_reply_file="${tmp_dir}/profile-reply.txt"
    profile_cli_status=0
    local relay branches lifetime
    if [[ "${profile_reader}" == stateful ]]; then
        relay=chaos_profiles
        branches='["alpha","beta"]'
        lifetime=false
    else
        relay=chaos_life_profiles
        branches='["gold","green","red"]'
        lifetime=true
    fi

    domain_cli_command() {
        printf '%s\n%s\n' "${cli_host}" "$1" >"${tmp_dir}/profile-command.txt"
        cat "${profile_reply_file}"
        return "${profile_cli_status}"
    }
    profile_read() {
        if [[ "${profile_reader}" == stateful ]]; then
            stateful_profiles_at 1 nervix-2 "${tmp_dir}/profile-read"
        else
            lifetimes_state_is chaos_life_profiles nervix-2 "${tmp_dir}/profile-read" gold=1 green=1 red=1 blue=-
        fi
    }

    local partitions
    for partitions in 1 4; do
        jq -n --arg relay "${relay}" --argjson branches "${branches}" \
            --argjson partitions "${partitions}" --argjson lifetime "${lifetime}" \
            -f "${chaos_dir}/tests/materialized-profile-fixture.jq" >"${tmp_dir}/profile-complete.json"
        # Compose may precede the CLI's pretty JSON with its own container progress lines.
        printf 'Container chaos-admin Starting\nContainer chaos-admin Started\n' >"${profile_reply_file}"
        cat "${tmp_dir}/profile-complete.json" >>"${profile_reply_file}"
        cli_host=nervix-1
        profile_read || fail "${profile_reader} refused a complete ${partitions}-unit profile inspection"
        [[ "${cli_host}" == nervix-1 ]] || fail 'the profile reader did not restore the selected CLI host'
        [[ "$(cat "${tmp_dir}/profile-command.txt")" == "$(printf 'nervix-2\nSHOW RELAY %s MATERIALIZED STATE FORMAT JSON;' "${relay}")" ]] \
            || fail 'the profile reader did not request the selected relay and host as JSON'
        jq -e --argjson branches "${branches}" --argjson lifetime "${lifetime}" '
            (map(.key.branch_name) | sort) == $branches
            and all(.[]; .key == {branch_name: .payload.branch_name} and .payload.profile_version == 1
                and .payload.ingested_at_low_watermark == 1794183779000000001
                and .payload.ingested_at_high_watermark == 1794183779000000002
                and (if $lifetime then .payload.label == "\(.key.branch_name)-v1"
                    and .payload.updated_at == 1794183779000000001 else true end))
        ' "${tmp_dir}/profile-read.json" >/dev/null || fail 'profile fields, branch identity or watermarks changed'
    done

    # A complete empty observation is valid, but cannot satisfy the loaded-profile predicate.
    jq '.counts = {branches:0,rows:0,bytes:0} | .units |= map(.counts = {branches:0,rows:0,bytes:0} | .sections = [])' \
        "${tmp_dir}/profile-complete.json" >"${profile_reply_file}"
    if profile_read; then fail 'an empty observation satisfied the loaded-profile predicate'; fi
    jq -e '. == []' "${tmp_dir}/profile-read.json" >/dev/null || fail 'a complete empty observation was not preserved'
    if [[ "${profile_reader}" == lifetimes ]]; then
        lifetimes_state_is "${relay}" nervix-2 "${tmp_dir}/profile-read" green=- \
            || fail 'a complete empty observation did not establish branch absence'
    fi

    local mutation status
    for mutation in \
        '.continuation = "more-rows"' \
        'del(.continuation)' \
        '.counts.rows += 1' \
        '.units[0].counts.rows += 1' \
        '.units[0].sections[0].omitted_rows = 1' \
        '.units |= .[:-1]' \
        '.units[1].unit = .units[0].unit' \
        '.units[0].sections = []' \
        '.units[0].sections[0].frames[0].rows[0] |= .[:-1]' \
        '.units[0].sections[0].frames[0].rows[0][1].type = "string"' \
        '.units[0].sections[0].frames[0].branch_key = null' \
        '.units[0].sections[0].schema.fields[1].name = "branch_name"' \
        '.units[0].sections[0].schema.fields[1].name = "unexpected_version"' \
        '.entity.name = "another_relay"' \
        '{entity, outcome:"rejected", rejection:"not_ready"}'; do
        jq "${mutation}" "${tmp_dir}/profile-complete.json" >"${profile_reply_file}"
        # A previous good result must not survive an unreadable observation at the same path.
        printf '[{"stale":true}]\n' >"${tmp_dir}/profile-read.json"
        status=0
        (
            trap 'printf "%s\n" "${failure_category:-unset}" >"${tmp_dir}/profile-category.txt"' EXIT
            failure_category=product
            profile_read
            fail 'an unreadable profile observation returned to the product predicate'
        ) >"${tmp_dir}/profile-error.txt" 2>&1 || status=$?
        [[ "${status}" == 1 && "$(cat "${tmp_dir}/profile-category.txt")" == controller ]] \
            || fail "unreadable observation was not a controller failure: ${mutation}"
        [[ ! -e "${tmp_dir}/profile-read.json" ]] || fail 'unreadable state left a usable stale result'
        grep -Fq 'materialized profile inspection' "${tmp_dir}/profile-error.txt" \
            || fail 'an unreadable observation had no reader diagnostic'
    done

    for mutation in truncated missing extra; do
        case "${mutation}" in
            truncated) head -c 100 "${tmp_dir}/profile-complete.json" >"${profile_reply_file}" ;;
            missing) printf 'Container chaos-admin Started\n' >"${profile_reply_file}" ;;
            extra) cat "${tmp_dir}/profile-complete.json" "${tmp_dir}/profile-complete.json" >"${profile_reply_file}" ;;
        esac
        status=0
        (profile_read) >"${tmp_dir}/profile-error.txt" 2>&1 || status=$?
        [[ "${status}" == 1 ]] || fail "${mutation} JSON was accepted"
        grep -Fq 'materialized profile inspection' "${tmp_dir}/profile-error.txt" \
            || fail "${mutation} JSON had no reader diagnostic"
    done

    # A failed transport/CLI attempt remains retryable, restores the host and cannot reuse rows.
    profile_cli_status=1
    printf 'request failed\n' >"${profile_reply_file}"
    printf '[{"stale":true}]\n' >"${tmp_dir}/profile-read.json"
    cli_host=nervix-1
    if profile_read; then fail 'a failed CLI attempt became a successful observation'; fi
    [[ "${cli_host}" == nervix-1 && ! -e "${tmp_dir}/profile-read.json" ]] \
        || fail 'a failed CLI attempt retained its host or stale rows'
    profile_cli_status=0
}
