#!/usr/bin/env bash
# Sourced by a scenario that drives a source with a burst load instead of the paced one. Owns the
# burst load shape: a backlog of payloads produced to a topic with one kcat call, so the payloads
# wait in the broker together and the ingestor reads them at once, and the reconstruction of the
# source's accepted-input ledger from its topic. The paced loads keep one record per interval
# (continuous-load.sh); a burst has no interval and so no pacing verdict. Its records arrive at
# once by design, which is what fills a domain's buffers when something is taken away.
#
# A payload line of a burst fixture is one Kafka message: one record, or a JSON array of records
# that the ingestor's codec unfolds. Every record carries an event_id that no other record of the
# run carries, like every fixture the ledger verifiers judge.

# Produces every line of FIXTURE as one Kafka message to TOPIC with one kcat call, as burst NAME.
# The topic's end offset must advance by exactly the fixture's lines. Each burst appends its record
# to traffic/NAME-bursts.ndjson, and the first burst of NAME declares the load in the manifest as a
# load of shape burst. A producer that fails, or a topic that took another count, fails the burst.
burst_load_produce() {
    local name="$1"
    local topic="$2"
    local fixture="$3"
    local bursts="${artifact_dir}/traffic/${name}-bursts.ndjson"
    local burst=1
    if [[ -s "${bursts}" ]]; then
        burst=$(($(wc -l <"${bursts}") + 1))
    fi
    local payloads records
    payloads="$(wc -l <"${fixture}")"
    records="$(jq -s 'map(if type == "array" then length else 1 end) | add // 0' "${fixture}")" \
        || { printf 'burst %s holds a payload that is not JSON\n' "${name}" >&2; return 1; }
    ((payloads > 0)) || { printf 'burst %s has no payload\n' "${name}" >&2; return 1; }
    local before after
    before="$(topic_end_offset "${topic}")" || return 1
    [[ "${before}" =~ ^[0-9]+$ ]] || { printf 'the end offset of %s could not be read before burst %s\n' "${topic}" "${name}" >&2; return 1; }
    local started_ms ended_ms producer_status=0
    started_ms="$(epoch_ms)"
    kcat -b broker:9092 -P -t "${topic}" <"${fixture}" \
        >"${artifact_dir}/traffic/${name}-burst-${burst}.stdout" \
        2>"${artifact_dir}/traffic/${name}-burst-${burst}.stderr" || producer_status=$?
    ended_ms="$(epoch_ms)"
    after="$(topic_end_offset "${topic}")" || return 1
    [[ "${after}" =~ ^[0-9]+$ ]] || { printf 'the end offset of %s could not be read after burst %s\n' "${topic}" "${name}" >&2; return 1; }
    jq -nc --arg name "${name}" --arg topic "${topic}" --argjson burst "${burst}" \
        --argjson payloads "${payloads}" --argjson records "${records}" \
        --argjson before "${before}" --argjson after "${after}" \
        --argjson producer_exit "${producer_status}" \
        --argjson started_ms "${started_ms}" --argjson ended_ms "${ended_ms}" \
        --arg fixture "${fixture#"${artifact_dir}/"}" \
        '{load: $name, burst: $burst, topic: $topic, fixture: $fixture, payloads: $payloads,
          records: $records, offset_before: $before, offset_after: $after,
          producer_exit: $producer_exit, started_ms: $started_ms, ended_ms: $ended_ms}' >>"${bursts}"
    if ((burst == 1)); then
        # The dollars in this jq filter are jq variables, not shell expansion.
        # shellcheck disable=SC2016
        update_manifest '.loads += [{service: $name, topic: $topic, shape: "burst",
                                     bursts: ("traffic/" + $name + "-bursts.ndjson")}]' \
            --arg name "${name}" --arg topic "${topic}"
    fi
    if ((producer_status != 0)); then
        printf 'burst %s of %s: kcat exited %d; see traffic/%s-burst-%d.stderr\n' \
            "${burst}" "${name}" "${producer_status}" "${name}" "${burst}" >&2
        return 1
    fi
    if ((after - before != payloads)); then
        printf 'burst %s of %s: %s took %d payloads, expected %d\n' \
            "${burst}" "${name}" "${topic}" "$((after - before))" "${payloads}" >&2
        return 1
    fi
}

# Reconstructs the accepted-input ledger of burst load NAME from its TOPIC: every payload the topic
# holds through its current end offset, as the broker stored it in traffic/NAME-payloads.ndjson, and
# every record they hold in OUTPUT, an array payload unfolded in its order. The producer's own report
# is never the ledger: the broker decides what was accepted.
burst_load_accepted() {
    local name="$1"
    local topic="$2"
    local output="$3"
    local end_offset
    end_offset="$(topic_end_offset "${topic}")" || return 1
    [[ "${end_offset}" =~ ^[0-9]+$ ]] || { printf 'the end offset of %s could not be read\n' "${topic}" >&2; return 1; }
    local payloads="${artifact_dir}/traffic/${name}-payloads.ndjson"
    if ((end_offset > 0)); then
        kcat -q -b broker:9092 -C -t "${topic}" -p 0 -o beginning -c "${end_offset}" \
            >"${payloads}" 2>"${artifact_dir}/traffic/${name}-payloads.stderr" || return 1
    else
        : >"${payloads}"
    fi
    [[ "$(wc -l <"${payloads}")" -eq "${end_offset}" ]] \
        || { printf '%s holds %d payloads, but %d were read\n' "${topic}" "${end_offset}" "$(wc -l <"${payloads}")" >&2; return 1; }
    jq -c 'if type == "array" then .[] else . end' "${payloads}" >"${output}" \
        || { printf '%s holds a payload that is not JSON\n' "${topic}" >&2; return 1; }
}
