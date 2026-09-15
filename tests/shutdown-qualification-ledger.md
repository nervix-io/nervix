# Shutdown qualification ledger

This ledger is the acceptance record for [Shutdown 12: Validate graceful termination and SIGKILL
under load](https://app.clickup.com/t/90141361959/86bc00fhb). It names the executable evidence for
every acceptance area and fixes the boundary between a graceful stop, an immediate process death,
and recovery from durable state.

Run the complete public qualification selection with:

```console
just test-scenarios --tags @shutdown_qualification
```

The scenarios use a unique `{{test_id}}`, isolated node databases and reserved ports. Process
scenarios execute the built `nervix-server` binary, deliver Unix signals to its PID, inspect its
real exit status, and put a timeout around every process and load observation.

## Termination and persistence contract

The first `SIGINT` or `SIGTERM` requests the ordered stop-admission, drain-support and
terminal-teardown phases. One monotonic shutdown deadline bounds all three. During drain support,
the terminating incarnation remains available for admitted relay work, acknowledgements,
checkpoints, handoff coordination and schedule activation. Placement excludes that incarnation as
a new destination. Terminal teardown begins only after drain support completes or is abandoned.

A later `SIGINT` or `SIGTERM` and expiry of the process shutdown deadline abandon the remaining
phases. `SIGKILL` gives the process no handler or teardown opportunity. Peers therefore treat all
three forced endings as a crash. A new process incarnation may execute persisted ownership only
after the startup admission fence described below.

The recovery promise stops at a durable boundary:

| State at termination | Graceful shutdown | `SIGKILL` |
| --- | --- | --- |
| Committed control-plane state and published consensus snapshots | Reopens from the committed generation | Reopens from the last fully published generation |
| Runtime-state checkpoints already synced to the node store | Flushed again while runtime tasks stop | Reopen at the last completed periodic snapshot |
| External source offsets and sink commits | Connector acknowledgements and commits finish when drain succeeds | Only the external connector's own delivery and transaction guarantee applies |
| Relay batches, queued payload attempts, suspended work, ACK guards, ACK tokens and ACK maps | Drain tries to resolve them before its deadline | Volatile; they may be lost |

The crash-window rows in the real-process scenario intentionally have no exactly-once assertion.
Its HTTP source is `NO_ACK`, so admitted data that had not crossed the Postgres boundary may be
lost. A Postgres commit whose acknowledgement was interrupted has only the Postgres connector's
documented retry semantics. Exact counts are asserted only for keys whose deduplicator snapshots
were known durable before the kill.

## Acceptance coverage

| Acceptance area | Public or direct evidence |
| --- | --- |
| 1. First, repeated and mixed signals; deadline; real `SIGKILL` under traffic | `cluster/termination_signals.feature` covers first `SIGINT` and `SIGTERM`, repeated same signals, mixed signals, their real exit statuses and absence of terminal teardown after forced exit. `cluster/shutdown_deadline.feature` covers deadline exit. `cluster/process_crash_recovery.feature` proves PID death by `SIGKILL` while bounded HTTP load and Postgres commits continue. |
| 2. Follower, leader, quorum, final node, simultaneous termination, placement and cordon | `cluster/graceful_shutdown.feature` covers persisted operator cordons, exclusion of a terminating incarnation, delayed cross-node ACKs, replacement, no replacement, the last schedulable node and every node terminating together. The scenarios inspect committed owners and bounded stop completion. |
| 3. Delayed ACK, queued work, long cadence, extreme pacing and held upload | `cluster/graceful_shutdown.feature` covers a delayed remote ACK, hour-long processor/emitter cadence, generator buffering, `REQUIRED WAIT`, timeout redelivery and stuck command work. `cluster/shutdown_deadline.feature` holds authenticated uploads at each accepted-stream progress point and runs the same deadline in unpaced, extremely slow and extremely fast domains. |
| 4. Interrupted ownership handoff | Four tagged scenarios in `cluster/relocate.feature` pause after durable preparation or before its response. They restart the destination, lose the response, cancel the coordinator and replace the leader, then prove cleanup by a later exact relocation to the intended owner. |
| 5. Snapshot interruption and recovered runtime state | `cluster/raft_replication.feature` reopens after an interrupted snapshot install. `runtime/deduplicator_replication.feature` restores branch-local suppression after one- and three-node restarts. The real-process crash scenario restores two branch checkpoints from the same database. The direct consensus and runtime recovery tests below cover each storage boundary and schedule rebuild. |
| 6. Unique identities, two branches and external delivery contracts | The real-process scenario sends monotonically unique load IDs through `alpha` and `beta` branch keys from an HTTP `NO_ACK` source to an acknowledging Postgres emitter. The last-schedulable-node scenario uses an externally acknowledged Kafka source, two interleaved branches and a committed consumer-group offset. |
| 7. Isolated former-owner restart fence from Shutdown 11 | `cluster/runtime_startup_admission.feature` restarts the former owner with consensus blocked, proves that its public listener and interconnect can be live while stale persisted ownership cannot route a record, then restores consensus and proves routing through the current owner. |
| 8. Reproducible validation | The tag command above selects the complete public matrix. The direct commands and observed results are recorded below. No scenario waits without a timeout; fault-injection pauses are reached and released through explicit barriers, and teardown releases every armed pause or load task. |

## Real-process crash probe

`SIGKILL during interleaved traffic preserves only checkpoints durable before the crash` uses three
launches of one isolated server:

1. It commits seed keys for `alpha` and `beta`, then uses `SIGTERM` and observes successful terminal
   teardown. This supplies an explicit durable checkpoint baseline.
2. It reopens the same database and ports, proves the seed keys are suppressed, commits a new key
   in each branch, and starts an HTTP load that alternates branches and assigns every request a new
   ID. It waits for 50 admitted requests and an external commit from each branch while the load
   continues, sends `SIGKILL`, and verifies signal 9 as the process's exit cause within ten seconds.
3. It reopens again, submits duplicates before a fresh per-branch barrier, and observes the fresh
   commits. Because each branch preserves order, those barriers prove the duplicate decisions ran.
   Postgres still contains exactly one row for every pre-crash checkpoint key. A final `SIGTERM`
   proves the recovered process can still complete normal terminal teardown.

The fixture retains its temporary database, certificates and six reserved ports across launches.
Its append-only log records every launch, while assertions read only the current launch so the
absence of terminal teardown after `SIGKILL` cannot be satisfied by an earlier clean exit.

## Snapshot and recovery boundaries

The direct consensus regression
`an_interrupted_installation_finishes_on_the_next_start` injects failures at `BeforeCommit` and
`AfterSync` for all three snapshot-publication writes: clearing the receiving generation,
installing its records and publishing `snapshot_installed`. After each failure it closes and
reopens the store, and requires one complete published generation with the expected state, metadata
and section bytes.

`forced_recovery_completion_survives_runtime_restart_and_schedule_rebuild` persists a prepared
handoff, rebuilds the domain, advances the new owner's checkpoint, closes the database, reopens a
new runtime and rebuilds the schedule again. It requires the completed recovery marker and newest
checkpoint to survive. `forced_recovery_replay_preserves_source_offsets_and_branch_processor_state`
does the same classification for a Kafka offset plus two branch-local deduplicator checkpoints and
proves that unrelated entity state is not substituted.

## Former-owner authority fence

The Shutdown 11 scenario is an authority test, not a listener-liveness test. On each process start,
`RuntimeAdmission` asks consensus for an admitted runtime state through a linearizable read and
records the committed log index that proves catch-up. Runtime installation is serialized; the
registry, domains, clock authorities and schedule are applied from that one coherent revision
before the process announces it as prepared. Until this succeeds, a former owner may answer public
and interconnect requests but cannot activate the ownership recorded in its local database.

In the scenario, node 2 restarts while its consensus path is blocked after ownership moved to node
1. An HTTP request accepted by node 2 produces no broker record. Once consensus connectivity is
restored, node 2 observes the current schedule and forwards later traffic to node 1. This is the
fence that prevents crash recovery from reviving an obsolete owner.

## Validation record

Recorded on 15 September 2026 from the task worktree. Cucumber retries were disabled.

| Command | Result |
| --- | --- |
| `just test-scenarios --input tests/features/cluster/process_crash_recovery.feature --concurrency 1 --retry 0` | Pass: 1 scenario and all 38 steps. The process died from signal 9 under live two-branch traffic, reopened twice, retained exact durable-key counts and exited cleanly afterward. The same scenario also passed in the complete tagged run. |
| `just test-scenarios --tags @shutdown_qualification --concurrency 4 --retry 0` | Pass: 8 features, 36 scenarios and all 494 steps, with no skipped or retried failure. |
| `cargo test -p nervix-consensus an_interrupted_installation_finishes_on_the_next_start -- --nocapture` | Pass: the one regression exercised and passed all six operation/boundary reopen cases. |
| `cargo test --features testing --lib forced_recovery_completion_survives_runtime_restart_and_schedule_rebuild -- --nocapture` | Pass: recovered owner and domain rebuild. |
| `cargo test --features testing --lib forced_recovery_replay_preserves_source_offsets_and_branch_processor_state -- --nocapture` | Pass: Kafka and both branch checkpoint classifications. |
| `just validate` | Pass: formatting, all-target all-feature Clippy with warnings denied, protocol lint, web console build, skill publication dry run, documented NSPL parsing and clock-boundary checks. |
| `just ratchet` | Pass: every tracked architecture-debt count remained at or below its baseline. |
