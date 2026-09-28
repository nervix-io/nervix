#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    cat <<'EOF'
Usage:
  just chaos list
  just chaos run baseline --image IMAGE [--nodes 1|3] [--records N]
  just chaos run rolling-restart --image IMAGE [--nodes 1|3] [--records N]
  just chaos run leader-crash --image IMAGE [--nodes 1|3] [--records N]
  just chaos run follower-crash|ingestor-owner-crash|emitter-owner-crash --image IMAGE [--records N]
  just chaos run pause-resume --image IMAGE [--records N]
  just chaos cleanup --run-id RUN_ID
  just chaos self-test

Run `just chaos run baseline --help` for all baseline options.
Run `just chaos run rolling-restart --help` for rolling-restart options.
Run `just chaos run pause-resume --help` for pause-resume options.
EOF
}

list_scenarios() {
    cat <<'EOF'
baseline  Kafka-to-Kafka delivery baseline using the supplied Nervix image
          topologies: one-node (--nodes 1), three-node (--nodes 3, default)
rolling-restart  Graceful Pumba stops and Docker restarts under Kafka traffic
                 topologies: one-node (--nodes 1), three-node (--nodes 3, default)
leader-crash  Observed leader SIGKILL, failover, and explicit Docker restart
              topologies: one-node (--nodes 1), three-node (--nodes 3, default)
follower-crash  Observed follower SIGKILL and recovery (three-node)
ingestor-owner-crash  Observed ingestor owner SIGKILL and placement recovery (three-node)
emitter-owner-crash  Observed emitter owner SIGKILL and placement recovery (three-node)
pause-resume  Short and failover-length Pumba pauses of the observed leader and execution owner (three-node)
EOF
}

if [[ "$#" -eq 0 ]]; then
    usage >&2
    exit 2
fi

command_name="$1"
shift
case "${command_name}" in
    list)
        [[ "$#" -eq 0 ]] || { usage >&2; exit 2; }
        list_scenarios
        ;;
    run)
        if [[ "$#" -eq 0 ]]; then
            printf '%s\n' 'a chaos scenario is required' >&2
            usage >&2
            exit 2
        fi
        scenario="$1"
        shift
        case "${scenario}" in
            baseline)
                exec "${script_dir}/run-baseline.sh" "$@"
                ;;
            rolling-restart)
                exec "${script_dir}/run-rolling-restart.sh" "$@"
                ;;
            leader-crash | follower-crash | ingestor-owner-crash | emitter-owner-crash)
                exec "${script_dir}/run-crash.sh" --scenario "${scenario}" "$@"
                ;;
            pause-resume)
                exec "${script_dir}/run-pause-resume.sh" "$@"
                ;;
            *)
                printf 'unknown chaos scenario: %s\n' "${scenario}" >&2
                list_scenarios >&2
                exit 2
                ;;
        esac
        ;;
    cleanup)
        exec "${script_dir}/cleanup.sh" "$@"
        ;;
    self-test)
        [[ "$#" -eq 0 ]] || { usage >&2; exit 2; }
        exec "${script_dir}/tests/self-test.sh"
        ;;
    -h | --help | help)
        usage
        ;;
    *)
        printf 'unknown chaos command: %s\n' "${command_name}" >&2
        usage >&2
        exit 2
        ;;
esac
