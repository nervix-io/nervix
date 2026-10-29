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
  just chaos run partition-recovery --image IMAGE [--case CASE] [--partition-seconds N] [--records N]
  just chaos run degraded-links --image IMAGE [--profile PROFILE] [--records N]
  just chaos run backup --image IMAGE [--nodes 1|3] [--records N]
  just chaos run backup-cycles --image IMAGE [--nodes 1|3] [--records N]
  just chaos run backup-fault --image IMAGE --fault FAULT [--outage-seconds N] [--records N]
  just chaos run restore-fresh-cluster --image IMAGE [--records N]
  just chaos run restore-fault --image IMAGE --fault FAULT [--outage-seconds N] [--records N]
  just chaos run restore-volume --image IMAGE [--records N]
  just chaos run stale-follower --image IMAGE [--records N]
  just chaos run former-owner-restart --image IMAGE [--isolation-seconds N] [--records N]
  just chaos run cluster-restart --image IMAGE [--nodes 1|3] [--outage-seconds N] [--records N]
  just chaos run stateful --image IMAGE [--nodes 1|3] [--fault FAULT] [--outage-seconds N]
  just chaos run endpoints --image IMAGE [--nodes 1|3] [--fault FAULT] [--outage-seconds N]
  just chaos run lifetimes --image IMAGE [--nodes 1|3] [--fault FAULT] [--outage-seconds N]
  just chaos run domain-time --image IMAGE [--nodes 1|3] [--fault FAULT] [--outage-seconds N]
  just chaos run mixed-instability --image IMAGE [--seed N] [--duration 30m] [--policy POLICY]
      [--coverage LIST] [--plan FILE] [--max-memory-bytes N] [--max-recovery-backlog N] [--max-pending N]
  just chaos replay RUN_DIRECTORY [--artifacts DIR] [--run-id ID] [--timeout SECONDS]
      [--verifier-self-check RECORD] [--keep]
  just chaos suite list
  just chaos suite smoke|soak --image IMAGE [--image-kind KIND] [--image-revision REVISION] [--shard N]
      [--entry ID]... [--artifacts DIR] [--suite-id ID]
  just chaos suite shards SUITE
  just chaos suite cleanup [--wait SECONDS] SUITE_DIRECTORY
  just chaos suite report [--expect-shards N] SUITE_JSON...
  just chaos cleanup --run-id RUN_ID [--evidence DIR]
  just chaos self-test

Run `just chaos run baseline --help` for all baseline options.
Run `just chaos run rolling-restart --help` for rolling-restart options.
Run `just chaos run pause-resume --help` for pause-resume options.
Run `just chaos run partition-recovery --help` for partition-recovery options.
Run `just chaos run degraded-links --help` for degradation profiles and thresholds.
Run `just chaos run stateful --help` for stateful, endpoints, lifetimes and domain-time faults.
Run `just chaos run mixed-instability --help` for seeded plans, quorum policies and coverage.
Run `just chaos suite --help` for the smoke and soak suites that CI runs.

An image whose io.nervix.diagnostic.selection label declares a Deloxide selection runs every node as
a diagnostic node, and every run fails unless the deadlock evidence of each process it started
qualifies; see the README's section on diagnostic images.
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
partition-recovery  Verified Pumba network partitions, healing and quorum recovery (three-node)
                    cases: follower, asymmetric, leader, quorum-loss (--case, default all)
degraded-links  Measured delay, jitter, random and burst loss, rate limits, and combined effects
                against one directed cluster link (three-node; --profile, default all)
backup  Quiesced domain backup during acknowledged Kafka traffic, verified offline
        topologies: one-node (--nodes 1), three-node (--nodes 3, default)
backup-cycles  Eight quiesced backups during acknowledged Kafka traffic, each verified offline,
               restored as a new domain and compared, with archived offsets that never move back
               topologies: one-node (--nodes 1), three-node (--nodes 3, default)
backup-fault  One fault against a quiesced backup under acknowledged Kafka traffic: a SIGKILL of the
              state owner or the leader, or an isolation of the state owner, while a delayed sink
              link holds the cut, or a rolling restart while the archive streams, then the domain's
              recovery and a fresh backup restored and compared (three-node)
              faults: owner-crash, leader-crash, owner-partition, rolling-restart (--fault, required)
restore-fresh-cluster  A quiesced backup taken under acknowledged Kafka traffic restored with RESUME
                       into a fresh three-node Compose project with other node names, which must
                       resume consumption at the archived offset while the original domain stays
                       stopped (three-node)
restore-fault  One fault against a restore whose guest saves a delay on the leader's links holds in
               staging: a SIGKILL of the leader, an isolation of the leader from its peers and the
               client, a SIGKILL of a state owner of the target domain, or a SIGKILL of a state owner
               while a failed restore's staging awaits reclamation; then a restore under a new name
               where one failed, the restored domain compared with the archive or, after RESUME,
               judged by its traffic, and staging back to zero on every node (three-node)
               faults: leader-crash, leader-partition, owner-crash, reclamation-owner-crash
               (--fault, required)
restore-volume  Three concurrent restores of an archive whose guest saves exceed the bulk budget,
                with each node's staging, storage and memory sampled every two seconds, every
                restored domain compared with the archive, and staging back to zero (three-node)
stale-follower  Offline follower, survivor log compaction and snapshot catch-up (three-node)
former-owner-restart  Former owner restarted behind peer-side isolation before startup admission (three-node)
cluster-restart  Every node SIGKILLed and started from its own volume
                 topologies: one-node (--nodes 1), three-node (--nodes 3, default)
stateful  Interleaved branches through a deduplicator, a window, materialized relay state and a
          checkpointed WASM processor, held at an observed durability milestone, then one fault
          faults: none (default), owner-crash, owner-pause, owner-partition, cluster-restart
          topologies: one-node (--nodes 1: none, cluster-restart), three-node (--nodes 3, default)
endpoints  HTTP intake under MODE ACK through every node's listener into two domains that share one
           host and path, one domain withdrawn and bound again, a WebSocket kept open across the
           replacement of its source, then one fault while intake continues
           faults: none (default), owner-crash, owner-partition, cluster-restart
           topologies: one-node (--nodes 1: none, cluster-restart), three-node (--nodes 3, default)
lifetimes  Interleaved branches through a junction that waits for materialized state, a generator, a
           window of sketches and an LRU-bounded relay; materialized rows replaced by higher, lower and
           equal timestamps; an expiring branch expired and recreated; buffered route output
           force-flushed by an alteration; an emitter recovering from a missing topic; then one fault
           faults: none (default), owner-crash, owner-partition, cluster-restart
           topologies: one-node (--nodes 1: none, cluster-restart), three-node (--nodes 3, default)
domain-time  A paced domain's clock, authority and logical deadlines followed by independent
             observers through faults of every voter in turn
             faults: none (default), voter-crash, voter-pause, voter-partition, voter-stop,
             cluster-restart
             topologies: one-node (--nodes 1: none, cluster-restart), three-node (--nodes 3, default)
mixed-instability  A seeded, finite plan of restarts, crashes, pauses, partitions and link degradation,
                   some combined, against public roles under a quorum policy, with continuous
                   traffic and resource samples (three-node)
                   policies: preserve-quorum (default), temporary-quorum-loss
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
            partition-recovery)
                exec "${script_dir}/run-partition-recovery.sh" "$@"
                ;;
            degraded-links)
                exec "${script_dir}/run-degraded-links.sh" "$@"
                ;;
            backup)
                exec "${script_dir}/run-backup.sh" "$@"
                ;;
            stale-follower | former-owner-restart | cluster-restart)
                exec "${script_dir}/run-recovery.sh" --scenario "${scenario}" "$@"
                ;;
            stateful | endpoints | lifetimes | domain-time | mixed-instability | backup-cycles | backup-fault | restore-fresh-cluster | restore-fault | restore-volume)
                exec "${script_dir}/run-baseline.sh" --scenario "${scenario}" "$@"
                ;;
            *)
                printf 'unknown chaos scenario: %s\n' "${scenario}" >&2
                list_scenarios >&2
                exit 2
                ;;
        esac
        ;;
    replay)
        if [[ "${1:-}" == -h || "${1:-}" == --help ]]; then
            usage
            exit 0
        fi
        if [[ "$#" -eq 0 || "$1" == -* ]]; then
            printf '%s\n' 'a run directory to replay is required' >&2
            usage >&2
            exit 2
        fi
        run_directory="$1"
        shift
        exec "${script_dir}/run-baseline.sh" --scenario mixed-instability --replay "${run_directory}" "$@"
        ;;
    suite)
        exec "${script_dir}/suite.sh" "$@"
        ;;
    cleanup)
        exec "${script_dir}/cleanup.sh" "$@"
        ;;
    self-test)
        [[ "$#" -eq 0 ]] || { usage >&2; exit 2; }
        "${script_dir}/tests/self-test.sh"
        exec "${script_dir}/tests/network-preflight-self-test.sh"
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
