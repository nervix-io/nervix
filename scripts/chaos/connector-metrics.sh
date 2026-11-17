#!/usr/bin/env bash
# Sourced by the crash controller. The existing hold file separates settled traffic from the
# replacement owner's first input; ordinary continuous-load crash cases do not use this path.

connector_metric_capture() {
    local kind="$1" node="$2" phase_dir="$3"
    mkdir -p "${phase_dir}" || return 1
    local host="nervix-${node##*-}"
    capture_metrics "${host}" || return 1
    cp "${artifact_dir}/public/metrics-${host}.txt" "${phase_dir}/metrics.prom" || return 1
    domain_cli_command "DESCRIBE ${kind^^} chaos_${kind};" \
        >"${phase_dir}/describe.txt" 2>&1
}

connector_metric_zero() {
    local kind="$1" node="$2" phase_dir="$3"
    connector_metric_capture "${kind}" "${node}" "${phase_dir}" || return 1
    run_bounded 20 "${connector_metrics_python}" "${script_dir}/connector_metrics.py" zero \
        --kind "${kind}" --node "${node}" --before "${phase_dir}/metrics.prom" \
        --description "${phase_dir}/describe.txt" --output "${phase_dir}/verdict.json"
}

connector_metric_progress() {
    local kind="$1" node="$2" zero_dir="$3" phase_dir="$4"
    connector_metric_capture "${kind}" "${node}" "${phase_dir}" || return 1
    run_bounded 20 "${connector_metrics_python}" "${script_dir}/connector_metrics.py" progress \
        --kind "${kind}" --node "${node}" --before "${zero_dir}/metrics.prom" \
        --after "${phase_dir}/metrics.prom" --description "${phase_dir}/describe.txt" \
        --output "${phase_dir}/verdict.json"
}

connector_metric_initial() {
    local crash_dir="$1"
    connector_metrics_kind="${scenario%-owner-crash}"
    wait_for 'initial connector owner before its first input' 90 \
        observe_crash_target "${crash_dir}/connector-metrics/initial-role" || return 1
    connector_metrics_initial_node="${target_node}"
    wait_for 'initial connector children registered at zero' 60 \
        connector_metric_zero "${connector_metrics_kind}" "${target_node}" \
        "${crash_dir}/connector-metrics/initial-zero" || return 1
    connector_metrics_prefix=$((record_count / 4))
    ((connector_metrics_prefix > 0)) || connector_metrics_prefix=1
    ((connector_metrics_prefix <= 8)) || connector_metrics_prefix=8
    printf '%s\n' "${connector_metrics_prefix}" >"${artifact_dir}/traffic/hold-load"
}

connector_metric_prefix_settled() {
    local crash_dir="$1"
    wait_for 'qualification prefix delivered' 120 output_has_all_records \
        "${connector_metrics_prefix}" || return 1
    wait_for 'qualification prefix transport-confirmed' 120 consumer_offsets_at_end \
        "${connector_metrics_prefix}" || return 1
    cp "${artifact_dir}/traffic/consumer-group.attempt.txt" \
        "${crash_dir}/connector-metrics/prefix-consumer-group.txt" || return 1
    wait_for 'initial connector counters advanced' 60 connector_metric_progress \
        "${connector_metrics_kind}" "${connector_metrics_initial_node}" \
        "${crash_dir}/connector-metrics/initial-zero" \
        "${crash_dir}/connector-metrics/initial-progress"
}

connector_metric_replacement() {
    local crash_dir="$1"
    local description="${crash_dir}/${connector_metrics_kind}-survivor.attempt.txt"
    connector_metrics_replacement_node="$(owner_from_description "${description}")"
    if [[ ! "${connector_metrics_replacement_node}" =~ ^node-[123]$ \
        || "${connector_metrics_replacement_node}" == "${connector_metrics_initial_node}" ]]; then
        crash_fail product 'connector metrics did not name a replacement physical owner'
        return 1
    fi
    wait_for 'replacement connector children registered at zero before new input' 60 \
        connector_metric_zero "${connector_metrics_kind}" "${connector_metrics_replacement_node}" \
        "${crash_dir}/connector-metrics/replacement-zero" || return 1
    # Let the survivor make progress while retaining one input for delivery after restart.
    printf '%s\n' "$((record_count - 1))" >"${artifact_dir}/traffic/hold-load"
}

connector_metric_restart_input() {
    rm "${artifact_dir}/traffic/hold-load"
}

connector_metric_replacement_progress() {
    local crash_dir="$1"
    wait_for 'replacement connector counters advanced after releasing input' 90 \
        connector_metric_progress "${connector_metrics_kind}" "${connector_metrics_replacement_node}" \
        "${crash_dir}/connector-metrics/replacement-zero" \
        "${crash_dir}/connector-metrics/replacement-progress"
}
