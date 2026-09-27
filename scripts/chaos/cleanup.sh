#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage: cleanup.sh --run-id RUN_ID [--quiet]

Remove only Docker containers, networks, and volumes carrying the exact
io.nervix.chaos.run label for RUN_ID.
EOF
}

run_id=""
quiet=false
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --run-id)
            [[ "$#" -ge 2 ]] || { usage; exit 2; }
            run_id="$2"
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
if ((${#containers[@]} > 0)); then
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

if [[ "${quiet}" != true ]]; then
    printf 'chaos cleanup run=%s containers=%d networks=%d volumes=%d\n' \
        "${run_id}" "${#containers[@]}" "${#networks[@]}" "${#volumes[@]}"
fi

exit "${status}"
