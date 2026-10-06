#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage: cleanup.sh --run-id RUN_ID [--evidence DIR] [--quiet]

Remove only Docker containers, networks, and volumes carrying the exact
io.nervix.chaos.run label for RUN_ID, then confirm that none is left.

--evidence DIR first captures what the run left behind into DIR: the listing
and inspection of every such resource and the last 2 MiB of each container's
log. Nothing is captured, and DIR is not created, when the run left nothing.
EOF
}

run_id=""
quiet=false
evidence_dir=""
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --run-id)
            [[ "$#" -ge 2 ]] || { usage; exit 2; }
            run_id="$2"
            shift 2
            ;;
        --evidence)
            [[ "$#" -ge 2 ]] || { usage; exit 2; }
            evidence_dir="$2"
            shift 2
            ;;
        --quiet)
            quiet=true
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            printf 'unknown cleanup argument: %s\n' "$1" >&2
            usage
            exit 2
            ;;
    esac
done

if [[ -z "${run_id}" ]]; then
    printf '%s\n' '--run-id is required' >&2
    usage
    exit 2
fi
if [[ ! "${run_id}" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,95}$ ]]; then
    printf 'invalid run id: %s\n' "${run_id}" >&2
    exit 2
fi
if ! command -v docker >/dev/null 2>&1; then
    printf '%s\n' 'docker is required for chaos cleanup' >&2
    exit 2
fi
if ! command -v timeout >/dev/null 2>&1; then
    printf '%s\n' 'GNU timeout is required for chaos cleanup' >&2
    exit 2
fi
if [[ -n "${evidence_dir}" ]] && ! command -v jq >/dev/null 2>&1; then
    printf '%s\n' 'jq is required to record chaos cleanup evidence' >&2
    exit 2
fi

label="io.nervix.chaos.run=${run_id}"
containers_output="$(timeout --foreground --kill-after=5s 30s \
    docker container ls --all --quiet --filter "label=${label}")" \
    || { printf '%s\n' 'could not list chaos containers' >&2; exit 1; }
networks_output="$(timeout --foreground --kill-after=5s 30s \
    docker network ls --quiet --filter "label=${label}")" \
    || { printf '%s\n' 'could not list chaos networks' >&2; exit 1; }
volumes_output="$(timeout --foreground --kill-after=5s 30s \
    docker volume ls --quiet --filter "label=${label}")" \
    || { printf '%s\n' 'could not list chaos volumes' >&2; exit 1; }

containers=()
networks=()
volumes=()
if [[ -n "${containers_output}" ]]; then
    mapfile -t containers <<<"${containers_output}"
fi
if [[ -n "${networks_output}" ]]; then
    mapfile -t networks <<<"${networks_output}"
fi
if [[ -n "${volumes_output}" ]]; then
    mapfile -t volumes <<<"${volumes_output}"
fi

status=0
# Pumba runs tc and iptables in sidecars that join a target's network namespace and carry only
# Pumba's own label. One left by an interrupted injector is owned through the container it joined.
sidecars=()
if ((${#containers[@]} > 0)); then
    mapfile -t pumba_sidecars < <(timeout --foreground --kill-after=5s 30s \
        docker container ls --all --quiet --filter label=com.gaiaadm.pumba.skip=true)
    for sidecar_id in "${pumba_sidecars[@]}"; do
        network_mode="$(timeout --foreground --kill-after=5s 20s \
            docker inspect --format '{{.HostConfig.NetworkMode}}' "${sidecar_id}" 2>/dev/null || true)"
        for container_id in "${containers[@]}"; do
            if [[ "${network_mode}" == "container:${container_id}"* ]]; then
                sidecars+=("${sidecar_id}")
                break
            fi
        done
    done
fi

# Captures what the run left behind before anything is removed. Every Docker call is bounded, and
# each container keeps at most the last 2 MiB of its log.
capture_evidence() {
    mkdir -p "${evidence_dir}/logs"
    timeout --foreground --kill-after=5s 30s docker container ls --all --no-trunc \
        --filter "label=${label}" --format '{{json .}}' >"${evidence_dir}/containers.ndjson" 2>&1 || true
    local inspected=(${containers[@]+"${containers[@]}"} ${sidecars[@]+"${sidecars[@]}"})
    if ((${#inspected[@]} > 0)); then
        timeout --foreground --kill-after=5s 30s docker inspect "${inspected[@]}" \
            >"${evidence_dir}/containers.json" 2>&1 || true
        local container_id container_name
        for container_id in "${inspected[@]}"; do
            container_name="$(timeout --foreground --kill-after=5s 20s \
                docker inspect --format '{{.Name}}' "${container_id}" 2>/dev/null || true)"
            container_name="${container_name#/}"
            timeout --foreground --kill-after=5s 30s docker logs --timestamps "${container_id}" 2>&1 \
                | tail -c 2097152 >"${evidence_dir}/logs/${container_name:-${container_id:0:12}}.log" || true
        done
    fi
    if ((${#networks[@]} > 0)); then
        timeout --foreground --kill-after=5s 30s docker network inspect "${networks[@]}" \
            >"${evidence_dir}/networks.json" 2>&1 || true
    fi
    if ((${#volumes[@]} > 0)); then
        timeout --foreground --kill-after=5s 30s docker volume inspect "${volumes[@]}" \
            >"${evidence_dir}/volumes.json" 2>&1 || true
    fi
}

left_behind=$((${#containers[@]} + ${#sidecars[@]} + ${#networks[@]} + ${#volumes[@]}))
if [[ -n "${evidence_dir}" ]] && ((left_behind > 0)); then
    capture_evidence
fi

if ((${#sidecars[@]} > 0)); then
    timeout --foreground --kill-after=5s 60s docker container rm --force "${sidecars[@]}" \
        >/dev/null || status=$?
fi
if ((${#containers[@]} > 0)); then
    for container_id in "${containers[@]}"; do
        paused="$(timeout --foreground --kill-after=5s 20s \
            docker inspect --format '{{.State.Paused}}' "${container_id}")" || status=1
        if [[ "${paused:-}" == true ]]; then
            timeout --foreground --kill-after=5s 20s docker unpause "${container_id}" \
                >/dev/null || status=1
        fi
    done
    timeout --foreground --kill-after=5s 60s docker container rm --force "${containers[@]}" \
        >/dev/null || status=$?
fi
if ((${#networks[@]} > 0)); then
    timeout --foreground --kill-after=5s 30s docker network rm "${networks[@]}" \
        >/dev/null || status=$?
fi
if ((${#volumes[@]} > 0)); then
    timeout --foreground --kill-after=5s 30s docker volume rm "${volumes[@]}" \
        >/dev/null || status=$?
fi

# The removal is confirmed by listing the label again, so a resource that survived it fails the
# cleanup however its removal reported.
remaining_containers=0
remaining_networks=0
remaining_volumes=0
if ((left_behind > 0)); then
    remaining_containers="$(timeout --foreground --kill-after=5s 30s \
        docker container ls --all --quiet --filter "label=${label}" | wc -l)" || status=1
    remaining_networks="$(timeout --foreground --kill-after=5s 30s \
        docker network ls --quiet --filter "label=${label}" | wc -l)" || status=1
    remaining_volumes="$(timeout --foreground --kill-after=5s 30s \
        docker volume ls --quiet --filter "label=${label}" | wc -l)" || status=1
    if ((remaining_containers + remaining_networks + remaining_volumes > 0)); then
        printf 'chaos cleanup left run=%s containers=%d networks=%d volumes=%d\n' \
            "${run_id}" "${remaining_containers}" "${remaining_networks}" "${remaining_volumes}" >&2
        status=1
    fi
fi

if [[ -n "${evidence_dir}" ]] && ((left_behind > 0)); then
    jq -n \
        --arg run_id "${run_id}" \
        --argjson containers "${#containers[@]}" \
        --argjson sidecars "${#sidecars[@]}" \
        --argjson networks "${#networks[@]}" \
        --argjson volumes "${#volumes[@]}" \
        --argjson remaining_containers "${remaining_containers}" \
        --argjson remaining_networks "${remaining_networks}" \
        --argjson remaining_volumes "${remaining_volumes}" \
        --argjson exit_code "${status}" \
        '{run_id: $run_id, containers: $containers, pumba_sidecars: $sidecars, networks: $networks,
          volumes: $volumes,
          remaining: {containers: $remaining_containers, networks: $remaining_networks,
                      volumes: $remaining_volumes},
          exit_code: $exit_code}' >"${evidence_dir}/cleanup.json"
fi

if [[ "${quiet}" != true ]]; then
    printf 'chaos cleanup run=%s containers=%d pumba_sidecars=%d networks=%d volumes=%d\n' \
        "${run_id}" "${#containers[@]}" "${#sidecars[@]}" "${#networks[@]}" "${#volumes[@]}"
fi

exit "${status}"
