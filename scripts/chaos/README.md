# Cluster chaos runner

The chaos runner exercises a packaged Nervix image entirely through public processes and protocols.
It does not build Nervix or use Cargo, local Nervix binaries, or the Rust scenario harness.

List the scenarios:

```bash
just chaos list
```

Run the three-node baseline against an image that has already been built or published:

```bash
just chaos run baseline --image nervix:debian
```

Run the same workload on one node:

```bash
just chaos run baseline --image nervix:debian --nodes 1
```

Run a quiesced domain backup during continuous acknowledged Kafka traffic:

```bash
just build-chaos-local-image nervix:backup-local nervix:chaos-current
just chaos run backup --image nervix:backup-local
just chaos run backup --image nervix:backup-local --nodes 1

# Or use an already published image:
just chaos run backup --image nervix:debian
just chaos run backup --image nervix:debian --nodes 1
```

The runner first observes healthy source and sink progress, then uses the packaged CLI to create a
quiesced domain archive while load continues. The packaged CLI verifies the archive offline with
`DESCRIBE BACKUP`. It then restores a stopped `chaos_restored` domain through the packaged CLI,
backs up that domain and compares the public checkpoint identities, revisions, branch counts and
exact Kafka positions with the original complete cut. The restored domain stays stopped while
the source traffic continues. The result records the cut's engagement and release times and freeze duration
under `results/backup-progress.json`. After the cut, the runner checks renewed source and sink
progress, stops the producer, and checks the exact accepted-input ledger. Identical replay
duplicates are counted separately in `results/ledger.json`. This run uses domain-owned Kafka
offsets and requires both those offsets and branch lifecycle state in the archive. Restore reports,
both archive descriptions and the compared state inventories stay under `backup/`. This packaged
workload exercises one-node and three-node publication; the large guest-save and post-START
branch-isolation workloads run in the public Cucumber and diagnostic suites.

Run graceful rolling restarts while Kafka traffic is flowing:

```bash
just chaos run rolling-restart --image nervix:debian
just chaos run rolling-restart --image nervix:debian --nodes 1
```

Run abrupt process crashes against the observed public role:

```bash
just chaos run leader-crash --image nervix:debian
just chaos run leader-crash --image nervix:debian --nodes 1
just chaos run follower-crash --image nervix:debian
just chaos run ingestor-owner-crash --image nervix:debian
just chaos run emitter-owner-crash --image nervix:debian
```

Run finite process pauses against the observed leader and a distinct execution owner:

```bash
just chaos run pause-resume --image nervix:debian
```

The three-node pause command runs four cases: a one-second leader pause, a one-second execution-owner
pause, a 75-second execution-owner pause, and a 75-second leader pause. Its Compose deployment
explicitly configures a 250 ms Raft heartbeat, 10–12 second election window, and 15-second node
unavailability timeout. The runner verifies these values in each selected container's Docker
inspection. The short pause is below the configured election and application-health thresholds;
the long pause exceeds them. Gossip uses an adaptive detector, so the short case also checks the
observed leader and placement rather than assuming that those settings alone prevent failover.
The long cases require peer-visible leader/placement failover and sink progress before unpause.

Each case verifies that Pumba's dry run selected the exact observed container and that Docker
inspection showed it paused and then running without a process restart. The target's pause and
unpause events in the run's live Docker event recording supply the actual interval; a short pause
outside 0.8–2 seconds or a long pause outside 15–100 seconds fails the experiment. From the pause
through recovery, the recorded node events must be exactly that pause and unpause. Independent broker load, listener observation, peer-side
public status and control canaries continue around the fault. Short pauses may end before the
listener observer samples the outage; the runner records that limit and still requires the
observed leader and owners to stay stable and every listener to recover. Recovery time is measured
from the Docker unpause event, separately from the pause interval. Final Kafka ledgers require
every accepted record with the correct content and branch, reporting identical replay duplicates
separately. The run retains per-case Docker, public, broker, observer and Pumba evidence under
`pauses/` plus `results/pause-progress.json`.

Immediately before each crash or pause, after the control canary and Pumba dry run, the runner
waits up to 30 seconds for a settled public role read. The read requires the nodes to agree on
leader, Raft term and last log index, then confirms that the selected role still belongs to the
same node. A timeout reports the unmet settled-role condition as an injection failure. If the
settled read shows that the selected leader or owner moved, the run also fails as an injection
failure before the fault.

The follower and execution-owner variants require three nodes. `--outage-seconds N` holds the
selected node down for at least 5–120 seconds (default 8). The controller reads the leader and
ingestor/emitter owners through the packaged CLI, selects the corresponding exact Compose node,
and checks its image, labels, restart policy and persistent volume through Docker inspection. It
rechecks the public role immediately before invoking digest-pinned Pumba with `kill --signal
SIGKILL`. A dry run must resolve only that container. The real kill must yield a signal-9 kill
event and a die event with exit code 137 in the run's live Docker event recording, and a stopped
container throughout the declared outage.
The controller then starts the same container ID explicitly and verifies its original image and
volume. It checks the final process start time against that explicit restart so a second crash
cannot be mistaken for recovery. Automatic restarts are disabled in Compose.

The three-node cases require a caught-up survivor leader, replacement owners for all graph nodes,
and sink progress while the target is down. The one-node case requires listener, cluster and
delivery recovery after restart. Every case measures election, placement, outage, listener and
settlement times in `results/crash-progress.json`; election and placement durations are `null`
when the fault does not require them. Survivor election is bounded by 90 seconds,
owner relocation by 120 seconds, sink progress during the outage by 60 seconds, returning
listeners by 120 seconds, and full settlement by 150 seconds after the explicit restart.
Before and after the fault, acknowledged `CREATE RESOURCE` control canaries must remain visible
through `DESCRIBE RESOURCE`. A canary attempted during the outage records its CLI response as
acknowledged or uncertain and its observed final effect. The input ledger is reconstructed from
Kafka after load stops. The exact-record verifier accepts identical replay duplicates in crash
cases, reports their count separately, and still fails on missing, unexpected, corrupt or
wrong-branch records. From the kill through recovery, the recorded node events must be exactly the
target's kill, its exit and the explicit start, so an unexpected node exit fails the command, as
does a failure to converge.
Crash runs default to 1,000 input records, one every 500 ms; a smaller `--records` value can
exhaust the load before fault verification and then fails as a setup limit. When output remains
short of accepted input after the recovery bound, the controller still saves the available output
and runs the exact ledger verifier to identify the missing IDs.
Failed crash runs retain `results/finding.json` with the phase, failure category, image identity,
and an external reproduction command pinned to a pullable repository digest when available,
together with Pumba, Docker, public status, broker and log evidence.

The rolling scenario uses the same externally provisioned graph and immutable Nervix image as the
baseline. The continuous load writes one unique record every 500 ms, an independent observer probes
every configured listener, and Kafka remains up throughout the rotation. The controller observes
the current leader, stops that node first, then restarts each remaining node once. It invokes a
digest-pinned Pumba 1.2.1 container against the exact labeled Compose container name with a
60-second stop grace, longer than Nervix's default 50-second shutdown deadline. Docker must expose
the local `/var/run/docker.sock` to the Pumba container. The same stopped container is started
through Docker to preserve its image and named volume.

Each stop must produce Docker exit code zero and completed shutdown-phase logs. In a three-node run
the other two nodes are settled, uncordoned voters when each stop begins, so every stopped node has
a live replacement: its log must report `shutdown drain-support phase finished outcome=Completed`,
which shows that it handed its scheduled work to the others through the leader before it exited,
the leader itself included. A drain that was refused, failed or never answered fails the run as a
product finding, because the stopped node's work would then fail over as after a crash. A one-node
run has no replacement, and its drain completes in place. The independent observer must see the
listener outage and restoration; the source broker must advance while the node is stopped. In a three-node run, the survivors must agree on a caught-up leader and the sink
must progress before the stopped node returns. Before the next stop, the controller requires
restored listeners on every node, agreed public leader and membership, caught-up applied Raft
indexes, settled ingestor and emitter state, and output progress. At the end it stops the producer,
reconstructs the accepted-input ledger from Kafka, waits for the consumer group and output
boundaries, and runs the baseline's exact ledger verifier. The rolling default generates at most
1,000 records and allows 20 minutes; `--records`, `--timeout`, and the other baseline options remain
available.

Run verified network partitions, healing and quorum recovery on three nodes:

```bash
just chaos run partition-recovery --image nervix:debian
just chaos run partition-recovery --image nervix:debian --case leader
just chaos run partition-recovery --image nervix:debian --case quorum-loss --partition-seconds 90
```

`--case all`, the default, runs four cases in one cluster: `follower` isolates the observed
follower that owns execution, `asymmetric` drops only the packets the observed leader sends to that
follower, `leader` isolates the observed leader, and `quorum-loss` leaves no two nodes able to
exchange a packet. `--partition-seconds N` (20–600, default 45) sets the minimum verified isolation
window. The deployment keeps the standard 250 ms Raft heartbeat, 1.5–3 second election window and
10-second node unavailability timeout, and the result records them.

Every node has a fixed address inside a per-run `10.213.N.0/24` network, chosen away from existing
Docker networks and host routes; every other container draws its address from the upper half of
that network. A case writes a plan naming the directed node links to block and the Pumba rules that
block exactly them, and checks that the rules imply exactly those links before anything is
installed. An isolation places its rules on the other nodes: netem drops their packets to the
isolated address and iptables drops packets arriving from it, so the isolation still holds while
the isolated container restarts. The asymmetric case places one netem rule on the sending node, and
quorum loss gives each node one netem configuration that covers both of its peers. Only node-to-node
links are ever targeted: the broker, load, listener observer, CLI and probe containers keep their
routes.

Before installing, every node interface must carry only its default qdisc and an empty INPUT chain;
faults are never stacked. One digest-pinned Pumba 1.2.1 container per node and rule kind, using the
pinned nettools image published with it, is dry-run against the exact node container and then
started detached. The runner reads each namespace's qdisc, u32 filters and INPUT rules until they
match the plan exactly. It then proves the effect: for every directed node link, the verifier's
route to each node and each node's route to the broker, it counts the ICMP echo requests the
receiving kernel accepted from one sender at a time. An intended link must accept none and every
other link must accept them, so a successful Pumba exit without effective isolation fails. Healing
sends SIGTERM to every injector, requires exit code zero, requires every namespace back in its
default state, and measures the whole matrix open again. Preflight first proves on the worker that a
Pumba netem and an iptables fault each block a throwaway container's gateway traffic and heal on
SIGTERM.

Each node is observed through its own public status route, and a status only counts if it names
the node that answered. Right after isolation the runner records the highest applied log index any
node reports; a node cut off from every quorum must never apply past it for the whole fault, and in
quorum loss no node may advance its Raft term. The isolated former leader may keep reporting itself
leader: that label is recorded, and the runner instead requires the connected majority to elect a
caught-up leader within 60 seconds, acknowledge a `CREATE RESOURCE` canary within 30 seconds and
apply past the isolation boundary. In the asymmetric case a majority must agree on a leader that
reports itself leader, and a canary through the node outside the one-way link must be acknowledged.
A canary sent to the isolated former leader, or to any node during quorum loss, is bounded at 20
seconds and must not be acknowledged. A canary through an isolated follower may be redirected to the
majority leader, so its outcome is recorded as a cluster observation. Canaries are classified as
acknowledged, refused, or uncertain, including an attempt stopped by its external bound, and after
recovery every node must agree whether each one took effect. Acknowledged effects must exist and
refused ones must not.

In the follower and leader cases the majority must move the isolated node's execution to connected
nodes within 90 seconds. With the follower isolated it must also keep delivering Kafka output. An
isolated node keeps running work it was already admitted to, so the leader case records rather than
requires majority delivery: Kafka can leave the source partition with the isolated former owner's
consumer. Group members are attributed to nodes by their fixed addresses. At the end of every held
fault except quorum loss, work owned by a node whose links stayed healthy must still be where it
was: only the isolated node, or either end of the one-way link, may lose its work. The follower case
also SIGKILLs and explicitly restarts the isolated follower inside the partition, requires the same
container, volume and address, and re-measures the unchanged isolation and its public status route.

A violation that leaves the experiment meaningful, such as a quorum that does not commit, an
unexpected acknowledgement, an applied entry past the isolation boundary, a failover away from a
healthy node, or a missing effect, is recorded in `results/partition-findings.ndjson`, and the run
continues through healing, recovery and the final ledger so it keeps every violation it can observe.
It then exits nonzero with the findings listed. Failures that make later steps meaningless, such as
an ineffective or unremovable fault, no majority leader, or no convergence after healing, stop the
run at once after recording every node's own status.

After healing, all nodes must agree on a caught-up leader with every peer connected and no warnings
within 150 seconds. Kafka membership must converge on the scheduled ingestor owner within 90
seconds, and output must advance within 90 seconds. A canary through the rejoined node must be
acknowledged, and the node events recorded from the start of the case through recovery must be
none at all, or exactly the planned SIGKILL, exit and restart of the isolated follower. The run ends
with the exact ledger verifier, which reports identical replay duplicates separately. Each case
keeps its plan, recorded rules, link matrices, injector commands and logs, status samples,
canaries, consumer group snapshots, node events and timings under `partitions/`, and
`results/partition-progress.json` summarizes them. A failed run retains `results/finding.json` with its failure category and a
reproduction command naming the pinned image, the case and the partition window. A run that exits
or is interrupted heals every run-owned fault before it captures diagnostics, including with
`--keep`, and `just chaos cleanup` also removes Pumba sidecars left joined to run-owned containers.
A partition run defaults to 1,000 input records, one every two seconds, and a 50-minute bound.

Run measured degradation of the directed relay-to-emitter link (`nervix-2` to `nervix-3`):

```bash
just chaos run degraded-links --image nervix:debian
just chaos run degraded-links --image nervix:debian --profile combined
```

`--profile all` runs `delay`, `jitter`, `random-loss`, `burst-loss`, `rate-limit`, and `combined`
sequentially. Delay is 180 ms; jitter adds 100 ms variation; independent loss is 30%; burst loss
uses Pumba's four-state model (`p13=20`, `p31=15`); rate is 256 kbit/s. The combined profile uses
one Pumba `netem combine` configuration with 180 ms delay, 60 ms jitter, 20% loss and a
256 kbit/s rate. Each profile targets only the fixed address of `nervix-3` on `nervix-2`'s egress.
The suite refuses any preexisting qdisc or INPUT rule, checks Pumba's exact selected container,
inspects the single owned netem qdisc and peer filter, requires the broker and every node's
listeners to keep answering while the profile holds, and tests the affected link with ICMP and a
prebuilt Alpine netcat transfer. A node's readiness is not part of that check, because loss on a
leader's link can start an election, during which a node can report no leader for a moment. The
transfer counts all bytes at the receiver and times their arrival. It records a healthy ICMP and
transfer baseline before any fault.
The 262,144-byte transfer has one 90-second deadline shared by the sender and receiver, covering
connection establishment, TCP retransmissions, byte delivery and receiver completion. It uses
the pinned BusyBox netcat's EOF half-close rather than an idle timeout. At 256 kbit/s the payload
alone needs at least 8.192 seconds; packet loss
can add retransmission stalls. The measured duration continues to include connection establishment
and receiver completion, so it is end-to-end transfer throughput, not isolated steady-state link
capacity. A failed transfer remains an `observation` failure and names both nodes and the receiver's
address and port. The adjacent `*.probe.json` records readiness, deadlines, both helper outcomes,
received bytes and cleanup outcomes; `*.log`, `*.server-start.log`, `*.readiness.log`,
`*.server.log`, `*.server.stderr.log` and `*.server-exit.txt` retain raw evidence for stages reached.
Timing out the Docker client also removes its exact sender container. Receiver completion uses
the transfer budget remaining after the sender finishes; helper logs and removal each retain their
twenty-second controller bounds. These bounds do not relax the product's post-heal recovery deadline.
Loss, delay, jitter and rate must also show their expected measured effect; an installed qdisc alone
cannot pass. SIGTERM heals each injector, after which the qdisc and ICMP delay must return to the
healthy state. The exit trap heals all run-owned faults even with `--keep` or an interrupted run.

The workload is the continuous load at a declared interval (750 ms by default), with a
1,000-record fixture. `--load-interval-ms`, `--baseline-seconds`, `--degrade-seconds`, and
`--drain-seconds` set the load and measurement windows. The runner rejects
a fixture too short to maintain that rate through the selected profiles. Before the first fault it
records the independently measured healthy output rate. Repeated samples retain
public metrics from all three nodes, Docker stats, broker source/output boundaries, estimated
backlog, interconnect pending work, relay attempts and resolved admissions, TCP retransmissions in
the affected sender's network namespace, reset/connection-failure retry indicators, and delivery
latency sums/counts. `--max-backlog`, `--max-recovery-backlog`, `--max-memory-bytes`, `--max-pending`,
and `--min-throughput-pct` are committed numeric inputs. A resource violation
records the metric, limit, profile, stage and time in `degraded/findings.ndjson`; the command fails
after final reconciliation. Every healed profile must progress in each of three consecutive
intervals, reach the declared percent of its independent healthy baseline over their full window,
and keep backlog at or below the declared recovery limit before its deadline. Source offsets and
exact branch/content ledgers must reconcile after load stops; identical
replay duplicates are reported separately.
A missed recovery deadline fails immediately with a profile-local `recovery-deadline.json` that
records the baseline, required and observed window, latest backlog, sample timeline and action
timeline. Its finding also distinguishes a short observation window from stalled or slow output.

The run retains `degraded/worker.json`, the exact image and load inputs in `manifest.json`,
`degraded/baseline.json`, profile rule/effect/healing evidence, raw metric and Docker samples,
Docker events, action timelines in `phases.ndjson` and `degraded/actions.ndjson`, and `results/degraded-progress.json` and
`results/degraded-links.json`. Use `--profile` to repeat one effect on the same worker and image.
The command uses only the packaged Nervix CLI and prebuilt broker, traffic, observer, Pumba and
nettools images; the worker needs no Rust toolchain or repository test binary.

### Degraded-link qualification on the selected worker

The qualification worker was Ubuntu 26.04.1, Linux 7.0.0-34-generic, Docker 29.8.1, with 24 CPUs
and 66.7 GB of memory. The supplied Nervix image was
`ghcr.io/nervix-io/nervix@sha256:334379691dd189314f67390c28d00f02c99fcc9117b71e4b86c55b9b42d31baf`.
The default six-profile run used 1,000 available fixture records, one production attempt every
750 ms, a 15-second healthy baseline, 20-second fault windows and 90-second per-profile recovery
deadlines. Its independent healthy output rate was 1.11 records/s. In `degraded-all-final2`, all
six faults were measured and healed, source offsets committed through the 988-record accepted
boundary, and the exact ledger matched 988 outputs with zero replay duplicates. Peak sampled
backlog was 71 records, pending operations 8, and per-node Docker memory 88.6 MB; the declared
limits were 200 records, 128 operations and 1 GiB. The 262,144-byte link transfer took 214 ms
without a fault, 9,268 ms under the rate limit and 16,545 ms under the combined fault. The
affected sender recorded 1,200 additional TCP retransmissions over the run.

An earlier full run on the same image, `degraded-all-final1`, correctly failed its 90-second
burst-loss recovery deadline: after Pumba was stopped, qdiscs returned to default and ICMP
answered 40/40 probes, but source offsets advanced from 414 to 601 while output stayed at 414. The
verifier retains the failed attempt and does not convert it into a pass because another attempt
succeeded. A repeat on the same image, `cc32-repro-all-1`, failed the same way with output held at
397 while the source reached 608.

[Cluster Chaos 32: Resume cross-node delivery after healed burst
loss](https://app.clickup.com/t/86bc95vz6) traced both failures to one record acknowledgement that
`nervix-3` gave up returning to the relay owner `nervix-2` during a loss burst, after the
record's delivery had been admitted. `nervix-2` kept reporting the record alive to the ingestor's
node, so the `ACK SEQUENTIAL` source never reached its `ACK TIMEOUT` and delivered nothing more.
That one lost acknowledgement is all the stall needs; the earlier profiles are not part of it,
although on this image it appeared in two of three full sequences and in none of six
burst-loss-only runs, five of them with 60-second faults. A node now fails a forwarded
acknowledgement its receiver reports nothing about for 15 seconds, and the source redelivers the
record; see [Record Acknowledgements The Receiver Stops
Reporting](../../docs/src/interconnect.md#record-acknowledgements-the-receiver-stops-reporting).
With a server binary built from the fixed source tree layered onto the same image, five
burst-loss-only runs with 60-second faults passed their exact ledgers. In three of them `nervix-3` gave up an acknowledgement and
`nervix-2` failed it 15 seconds later, and each run reported the resulting one or two replay
duplicates separately.

On a worker loaded by unrelated builds, the six profiles can outlast the 1,000-record fixture. In
`cc32-local-fix1-all-1`, every profile through the rate limit recovered and all 1,000 records
reached the output, but the load had stopped before the combined profile's recovery window closed,
and the run failed as a setup limit rather than a product verdict.

In `degraded-limit-negative`, the same worker and image ran one delay profile with a deliberately
impossible 1 MiB per-node memory limit. The exact ledger still matched 179/179 records, while the
command exited nonzero with 30 timestamped memory findings, the source metric samples and the
action timeline. The baseline, lifecycle and partition commands remain separate `just chaos`
entries.

The runs above took their input a kcat read block at a time, eight or nine records every six to
seven seconds, as the [continuous load
qualification](#continuous-load-qualification-on-the-selected-worker) describes. A block moved
every number the recovery verdict reads from the broker's offsets: the healthy output rate,
measured between two or three samples, and each sampled backlog. With the load paced one record
every 750 ms, the six profiles ran on that section's worker and on the image built on main
`9b75a6cb`, each alone with `--drain-seconds 180`, and each passed. The exact ledgers matched 316
to 400 records without a replay duplicate. The healthy output rate read 1.30 to 1.41 records/s in
five runs, against the 1.333 the interval declares, and 1.07 in the run whose two baseline samples
lay 12 seconds apart: a sample reads its offsets up to a few seconds after the time it records.
The sampled healthy backlog was 3 to 9 records. Under the fault it stayed at 4 to 7 records for
delay, random loss and the rate limit and reached 22 under jitter, 92 under burst loss and 82
under the combined profile; the first sample after every heal showed 3 to 8, and output then ran
at 1.26 to 1.36 records/s. No node used more than 83.8 MB or reported more than 8 pending
operations. The 262,144-byte link transfer took 1.1 to 3.5 seconds without a fault on the loaded
worker, 9.6 seconds under the rate limit and 19.5 seconds under the combined fault.

The bursts did not cause the load-dependent verdicts that [Cluster Chaos 37: Keep degraded-links
verdicts independent of worker load](https://app.clickup.com/t/86bc9vuff) owns. Those follow from
the time a sample takes and from the length of the fixture, and the first reproduced with the paced
load. In `cc44-q-degraded-all`, the default six-profile run, the delay and jitter profiles
recovered. A sample then took 31 to 35 seconds, so random-loss recovery completed three samples in
its 90 seconds and failed its deadline, while output advanced by 65, 42 and 54 records from sample
to sample with a sampled backlog of 4 to 10. In the single-profile runs a sample took 13 to 34
seconds, and the fourth recovery sample was taken 70 to 102 seconds after the heal.

Run snapshot catch-up of a follower that stayed offline:

```bash
just chaos run stale-follower --image nervix:debian
```

This three-node deployment sets two ordinary retention options,
`NERVIX_RAFT_SNAPSHOT_ENTRY_THRESHOLD=64` and `NERVIX_RAFT_COVERED_LOG_ENTRIES_RETAINED=16`, and the
runner checks both in every node's Docker inspection; every other scenario runs the product defaults
of 10,000 and 1,000. The runner selects the observed follower that owns execution, acknowledges a
`CREATE RESOURCE` control canary, and stops that follower with a digest-pinned Pumba
`stop --time 60` whose dry run must select exactly its container. The stop must exit with status 0
after every shutdown phase, and because both survivors are live replacements, the follower's
drain-support phase must complete: it hands its work to the survivors through the leader before it
exits. A drain that does not complete is a product finding, which fails the run after the catch-up
checks. The follower's own consensus metrics before the stop and its
`raft transition: state=Shutdown` log line record the last log index it held, and the survivor
leader's `nervix_consensus_log_last_index` once the survivors agree on a leader bounds it from above.

While the follower is offline, the survivors must agree on a caught-up leader within 90 seconds, take
over its execution within 120 seconds and keep delivering output. Through the survivor leader, the
packaged CLI then creates the stopped domain `chaos_stale`, creates the schemas `chaos_stale_kept`
and `chaos_stale_dropped` in it, drops `chaos_stale_dropped`, and relocates the relay onto the
survivor that does not own it; each change must be acknowledged. Rounds of eight `CREATE RESOURCE`
changes follow, run from one administration container with one CLI session per change, until every
survivor's `nervix_consensus_log_purged_index` exceeds that upper bound, so the follower can no longer
catch up from the log. Twelve rounds without that fail the run. Every round's log, snapshot and purge
positions are kept in `stale/compaction.ndjson`.

The runner then starts the same container from its image and volume. Within 120 seconds the
restarted follower's `nervix_consensus_log_snapshot_index` must cover the survivors' purged index and
its own `raft.last_applied` must reach the index the leader had applied at the restart, and the
survivors' `nervix_interconnect_requests_total{operation="snapshot",outcome="answered"}` must rise,
which shows the snapshot transfer itself. Through the restarted node's own public route, which
answers these reads from its applied state, every acknowledged change must then be visible: the
domain line in `SHOW CLUSTER STATUS`, the kept schema's `SHOW CREATE` text, the dropped schema's
absence, the relay's new owner in `DESCRIBE RELAY` and in the node's schedule, every created
resource, and all three voters. A change whose outcome was uncertain must read the same through the
restarted node as through the leader. Finally every listener returns, the cluster settles with every
peer connected within 150 seconds, output advances, a canary through the restarted node is
acknowledged, the node events from the stop through recovery are exactly the follower's `kill` with
signal 15, its exit 0 and the explicit start, the survivors keep their process incarnations, every
node volume keeps its identity, and the exact ledger passes. `stale/applied.json` holds each change
with its outcome and the effect read through each node, and `results/stale-follower-progress.json`
summarizes the policy, bounds, positions and timings.

Run a former owner's restart under peer-side isolation:

```bash
just chaos run former-owner-restart --image nervix:debian
just chaos run former-owner-restart --image nervix:debian --isolation-seconds 90
```

The runner relocates the ingestor, relay and emitter onto the observed follower and requires that node
to hold the source partition, which the Kafka consumer group shows by its fixed address, and to emit
output. After a settled public read confirms that the leader and all three owners are unchanged, it
SIGKILLs that former owner with digest-pinned Pumba. While the former owner is down, the survivors
must take its work over, and an acknowledged `RELOCATE` through the packaged CLI moves each unit to
the survivor that failover did not choose. The consumer group must then run only on the new
scheduled ingestor owner, while the former owner's stored schedule still names it the owner of
everything.

Pumba netem and iptables rules on both survivors then drop the former owner's fixed address in each
direction. The runner verifies the rules installed on the survivors before it starts the stopped
container again from its image and volume at the same address. With the container running, it checks
every node's rules against the plan and counts accepted ICMP echo requests to prove that the former
owner reaches neither peer while the verifier and broker routes stay open. From then until at least
`--isolation-seconds` (20–600, default 45) have passed, it samples the former owner about every five
seconds with no gap over 20 seconds. Every sample must show its session, interconnect, console and
observability listeners and its own status route answering, no Kafka consumer-group member at its
address, no `nervix_messages_total` traffic on its metrics, an applied index that never advances, and
no `runtime execution admitted after linearizable consensus catch-up` line in its log. Its readiness
and the schedule its own status reports are recorded, and the survivors must keep delivering output
during the window.

Healing sends SIGTERM to every injector and requires default rules and an open link matrix. The
former owner must then log runtime admission no earlier than the start of the heal, after having
logged that it waited for linearizable consensus catch-up. Within 150 seconds every node must agree on
a caught-up leader with every peer connected. `DESCRIBE` through the former owner and the schedule in
its own status must report the changed owners, the consumer group must run only on the scheduled
ingestor owner, output must advance, a canary through the former owner must be acknowledged, and the
node events must be exactly the former owner's SIGKILL, exit and explicit start. The pre-admission
samples, admission timing, rules and link matrices are kept under `former-owner/`, with a summary in
`results/former-owner-restart-progress.json`.

Run a whole-cluster process crash and restart:

```bash
just chaos run cluster-restart --image nervix:debian
just chaos run cluster-restart --image nervix:debian --nodes 1
```

After the cluster settles, the runner acknowledges a `CREATE RESOURCE` canary and reads each node's
committed configuration through that node's own route: its domains, its voting membership, the
`SHOW CREATE` text of every model of the baseline graph, and the canary. All nodes must agree. One
digest-pinned Pumba `kill --signal SIGKILL` then names every node container; its dry run must select
exactly those containers, and the live Docker event recording must hold one signal-9 kill and one exit
137 for each. The broker keeps accepting the independent producer's records while no node runs.
After at least `--outage-seconds` (5–120, default 8), one `docker start` starts every original
container, which must keep its image and named volume; the volumes' names, creation times and mount
points are compared before and after. A sampler started before the restart reads every node's own
`SHOW CLUSTER STATUS` several times a second for 40 seconds.

Final accepted-input ledger capture gives the disposable Kafka administration command 60 seconds,
independently of the shorter live-probe budget. Its exit status and bound are retained in
`traffic/source-boundary-query.json`; a failed query is a controller failure and cannot qualify
delivery. This accounts for administration JVM startup under concurrent compiler load while keeping
the source-offset and exact delivery assertions intact.

Listeners must return within 120 seconds, the cluster must settle within 150 seconds and output must
resume within 90 seconds. Ownership is then judged against the voter observation grace of the
control-plane chapter: a leader waits ten seconds from its own process start for gossip to observe
every voter before it fails over the work of a voter it has not heard from. When every node that led
after the restart observed every voter live within ten seconds of its own Docker start event, each
owner must keep its work; otherwise failover is permitted, and the verdict says which applied. Each
node must report the same committed configuration as before the crash, the consumer group must run
only on the scheduled ingestor owner, a canary after the restart must be acknowledged and visible
through every node, each node's events must be exactly its kill, exit and start, and the final exact
ledger shows that every accepted record was delivered from the replayable source.
`results/cluster-restart-progress.json` records the Raft, liveness and retention settings read from
Docker inspection, the fixed ten-second grace, the external bounds, the start times and the ownership
verdict.

These three scenarios record product violations that leave the run meaningful in
`results/recovery-findings.ndjson`, continue through recovery and the final ledger, and then exit
nonzero with the findings listed in their result file. A failure that leaves later steps meaningless
stops the run at once, and `results/finding.json` keeps its category, phase and reproducer. They are
process crash and restart experiments on one host: the host, its page cache and its disks survive,
so they establish nothing about host power loss or storage corruption.

### Restart and recovery qualification on the selected worker

The worker ran Ubuntu 26.04.1, Linux 7.0.0-34-generic, Docker 29.8.1 with Compose 5.5.1, 24 CPUs
and 66.7 GB of memory, shared with other workloads. The supplied image was
`ghcr.io/nervix-io/nervix@sha256:0b39e7ab59ccbf22c8b6293c715affda49948d48c8ef4dc6d73ef89b6867677b`,
the official AMD64 image built from the source tree of main commit `0913fddf`, and every run used
the default 1,000-record fixture produced once a second.

`stale-follower` passed four runs. Each time the stopped follower had held index 48 or 49. After
the five structural changes, one round of eight resources moved the survivors' purged index to 111
and their snapshot to 127. Within 2.9 to 4.3 seconds of its restart the follower held snapshot 127
and had applied past the leader's boundary, the survivor leader had answered five snapshot
requests, and all 14 acknowledged changes then read back through the follower. The ledgers matched
76 to 100 records with no duplicates. In every run the follower's graceful drain request was refused
as unauthenticated, because only the bootstrap node is given the default user's password, as in the
documented Compose deployment. The stop still completed its local drain and exited 0, and failover
moved the follower's work after it stopped.

`former-owner-restart` passed two runs. Ten samples covered 45.0 and 46.4 seconds of verified
isolation with no gap above 5.1 seconds. Throughout them the former owner answered on every
listener, held no consumer-group member, reported no graph messages, applied nothing, and reported
its own stale schedule, which named it the owner of all three units. Its `/readyz` answered ready
the whole time, because its restored Raft state still names a leader it cannot reach. It logged
runtime admission 0.86 and 1.03 seconds after the heal began, the cluster converged 2.7 and 3.4
seconds after the heal, and the ledgers matched 189 and 202 records.

`cluster-restart --nodes 1` passed twice. Listeners returned within 0.8 seconds and the node
settled within 5.5 seconds; output resumed about 34 seconds after the start, once the broker's
45-second session timeout released the killed consumer's partition. The ledgers matched 81 and 88
records.

Three-node `cluster-restart` passed three of four runs. Listeners returned within 2.4 seconds, the
cluster settled within 16.2 seconds and output resumed within 26.5 seconds. The leader observed the
other voters live 1.2 to 1.3 and 1.6 to 1.9 seconds after its own process start, every owner kept
its work, and configuration and ledgers of 109 to 114 records matched. The fourth run,
`cc07-restart-3b`, correctly exited nonzero with one ownership finding while its configuration and
its 108-record ledger matched. node-1 resumed leadership within 40 ms of its process start and, 1.5
seconds after its start, failed the emitter over from node-3 with
`failover found no live replica; moving scheduled node without local replicated state`. Half a
second later its gossip reported node-3 live, 2.15 seconds after node-1 started, well inside the
ten-second grace. The chitchat failure detector declares a node dead until it has two heartbeat
samples, and node-1 first heard of node-3 through node-2's gossip, so the grace did not wait for
it. [Cluster Chaos 39: Keep a restarted voter's work when gossip hears of it before observing it
live](https://app.clickup.com/t/86bcd4hzv) owns the product fix.

With the load paced one record a second, the three scenarios ran again on the worker and image of
the [continuous load qualification](#continuous-load-qualification-on-the-selected-worker), the
official image built on main `9b75a6cb`. `stale-follower` passed. The stopped follower had held
index 55, one round of eight resources moved the survivors' purged index to 111 and their snapshot
to 127, and 8.3 seconds after its restart the follower held snapshot 127 and had applied index 140,
past the leader's 139 at the restart. The survivor leader had answered five snapshot requests, all
14 acknowledged changes read back through the follower, and the ledger matched 333 records. That
image authenticates a drain with the node's own certificate, so the follower's drain completed: it
handed its graph node to the survivors through the leader and released its cordon 67 ms later.

`cluster-restart --nodes 1` passed beside other chaos runs: listeners returned 10.3 seconds after
the start, the node settled within 38.0 seconds, output resumed after 45.2 seconds, and the ledger
matched 217 records. Three-node `cluster-restart` passed one of two runs. In the passing run
listeners returned within 2.9 seconds, the cluster settled within 18.6 seconds, output resumed
within 21.3 seconds, the leader observed the other voters live 1.5 and 1.9 seconds after its own
process start, every owner kept its work, and configuration and the 129-record ledger matched.
`cc44-f-cluster-restart-3` exited nonzero with the ownership finding described above, while its
configuration and its 373-record ledger matched: node-1 failed the emitter over from node-3 4.0
seconds after its own process start, 1.4 seconds before its gossip reported node-3 live. The image
does not contain the fix of Cluster Chaos 39.

`former-owner-restart` passed one of three runs. In the passing run five samples covered 45.9
seconds of verified isolation with no gap above 17.6 seconds. Throughout them the former owner
answered on every listener, held no consumer-group member, reported no graph messages and applied
nothing past index 57. It logged runtime admission 4.5 seconds after the heal began, the cluster
converged 14.9 seconds after the heal, and the ledger matched 330 records. In the other two runs
every sample showed the same inert node and every later check passed, with ledgers of 944 and 542
records, but beside other chaos runs the samples lay up to 43.2 and 23.2 seconds apart where the
runner allows 20, and both runs failed as product findings for that gap alone. [Cluster Chaos 53:
Size the chaos runner's own time budgets and fixtures for a loaded
worker](https://app.clickup.com/t/86bcdkckc) owns that budget.

Run branch-local state through a durability milestone and one fault:

```bash
just chaos run stateful --image nervix:debian
just chaos run stateful --image nervix:debian --nodes 1
just chaos run stateful --image nervix:debian --fault owner-crash
just chaos run stateful --image nervix:debian --fault owner-pause
just chaos run stateful --image nervix:debian --fault owner-partition
just chaos run stateful --image nervix:debian --fault cluster-restart
just chaos run stateful --image nervix:debian --nodes 1 --fault cluster-restart
```

The stateful graph of `fixtures/stateful.nspl` runs in the baseline domain beside the baseline path,
which keeps its own traffic, verdicts and exact ledger. A separate prebuilt kcat container produces
`fixtures/generate-stateful.jq` records to `chaos_state_input`, one record a second, and one kcat
call per record, so a record reaches the broker before the next is read. Two concrete branches,
`alpha` and `beta`, interleave strictly by sequence, and every record carries its branch index. An
`ACK PARALLEL` ingestor with its own consumer group feeds four processors, each with its own
attached Kafka emitter and output topic:

- the deduplicator `chaos_dedup` keys on a deduplication key. Every fourth record of a branch
  repeats the key of a record 3, 23 or 79 positions back, and both branches name keys after the
  same indexes, so equal keys of different branches must never deduplicate each other;
- the detached window processor `chaos_window` aggregates 12 rows of a branch. It reports the
  smallest and largest index, the sum of the sequences, and the sum of 4 to the power of each index
  modulo 24, whose base-4 digits tell which rows the window held and how often;
- the junction `chaos_enrich` reads the branch's row of the materialized relay `chaos_profiles`,
  which a second ingestor fills from `chaos_profile_input`, with a `DEFAULT` that names no branch;
- the WASM processor `chaos_counter` runs `fixtures/wasm/processors/branch-counter.wat`. Each
  branch's guest counts the rows it has processed, saves that count as its state, and emits every
  row with all of its input fields and the count after it.

The WASM guest is a prebuilt module in WebAssembly text, which the packaged server loads directly,
so a run needs no guest toolchain and compiles nothing. The run uploads exactly that file with the
packaged CLI's `UPLOAD RESOURCE` and records its SHA-256 and size under `fixtures.wasm` in
`manifest.json` and in the result. `just chaos-wasm-fixture` regenerates it from the current guest
ABI. The `nervix-wasm` package's `chaos_branch_counter` test, part of `just test`, fails while the
checked-in module differs from what the generator writes, and drives that module through the host
to prove its per-branch counts and its restore from saved state.

The deployment sets two ordinary state options: `NERVIX_STATE_SNAPSHOT_INTERVAL=1s`, and on three
nodes `NERVIX_REPLICA_COUNT=1`, so the loss of an owner promotes a replica of each processor's state
instead of resetting it. A pause fault also deploys the pause scenario's 10–12 second election window
and 15-second unavailability timeout. On three nodes the stateful entities move with one
acknowledged `RELOCATE` onto the observed follower that the faults target, and both stateful
ingestors onto another node, so a fault on the state owner leaves the Kafka consumers in place.

A run first loads profile version 1 for both branches and requires `SHOW RELAY chaos_profiles
MATERIALIZED STATE` to show it before any record is produced, and publishes version 2 after 40
records. At 80 records the runner asks the load to hold, and it holds before its 101st record, when
each branch has two rows in an open window. The milestone requires the stateful consumer group to be
committed through all 100 records, the profile group through its end, every stateful output to stay
unchanged, `DESCRIBE WASM PROCESSOR chaos_counter FORMAT JSON` to report both branch checkpoints
committed at their latest revision and confirmed by every assigned replica, and the materialized
relay to show version 2 for both branches. It then holds the drained state for five configured
snapshot intervals, read from the nodes' Docker inspection, and requires the outputs to stay
unchanged throughout. `stateful/milestone.json` keeps that evidence. The image exposes no
publication milestone of deduplicator, window or materialized relay state, so their durability
verdicts rest on this drained and held boundary rather than on an observed publication, and the
result says so.

The load then resumes for four more records, holds again, and version 3 is published. The runner reads
the fault's boundaries, which takes several broker round trips, while the load holds, and releases it
as the fault begins, so the window rows open at the milestone are still open when the fault strikes.
Records are classified by sequence: before the milestone, the volatile interval from the milestone until the
end of the fault's recovery, and after recovery. `--fault` selects the fault:

- `none` holds no fault and allows no deviation at all;
- `owner-crash` SIGKILLs the state owner with Pumba, requires the survivors to settle and the
  stateful work to move off it while it is down, holds the outage for `--outage-seconds` (default
  8), and starts the same container from its image and volume;
- `owner-pause` pauses the state owner for `--outage-seconds` (default 45, between 16 and 99) and
  requires the failover to happen while it is paused;
- `owner-partition` isolates the state owner with peer-side netem and iptables rules, verified as in
  `partition-recovery`, for at least `--outage-seconds` (default 45);
- `cluster-restart` SIGKILLs every node and starts each from its own volume.

Each fault proves its effect through Pumba's report, Docker inspection and the run's live Docker event
recording, and the node lifecycle events from the fault through recovery must be exactly the planned
ones. Recovery requires a settled, connected cluster, an owner for every stateful entity and
advancing stateful output. The run continues until each branch has seen the duplicates that refer 79
positions back to keys durable at the milestone and at least 48 records after recovery, then stops
the load and waits for the stateful group to commit through the final boundary.

`verify-state-evidence.sh` judges each processor against the accepted-input ledger that the runner
reconstructs from `chaos_state_input`, with three distinct expectations:

- identical replays of a record after a fault are replay duplicates, counted separately; before a
  fault, or in a run without one, a replay is a violation;
- state the milestone made durable must survive: a later duplicate of a key first seen before the
  milestone must be dropped, the two rows each branch held open at the milestone must be aggregated
  exactly once in a window that closes after the fault, a record after the milestone must carry at
  least profile version 2, and every guest count must reach at least its branch index, because
  every acknowledged input is reflected in the checkpoint the guest restores;
- the volatile interval permits what periodic checkpoints and replayed acknowledgements allow: a
  duplicate of a key first seen there may pass, a window row from it may be lost or aggregated
  twice, its profile version may be lost back to version 2, and a guest may count a replayed record
  again, by at most the number of records that interval holds.

Every verdict also requires the records' own content and branch, the first occurrence of every key
of each branch, every record in the enriched and counted outputs, windows of 12 rows of one branch
with the sums they report, guest counts that equal the branch index before the fault and advance by
one per record after recovery, and never the profile default or another branch's profile. Before a
fault every window is 12 consecutive rows. A durability verdict that its fault did not exercise,
such as a run that ended before a duplicate of a milestone key arrived after recovery, fails as
missing evidence. Each processor's verdict is kept in `stateful/verdict-*.json`; a failing one is a
product finding, and the run continues to the baseline ledger before it exits nonzero.
`results/stateful-progress.json` records the deployment, the boundaries, the milestone, the timings
and every verdict.

Run a paced domain's clock and logical deadlines through faults of every voter:

```bash
just chaos run domain-time --image nervix:debian
just chaos run domain-time --image nervix:debian --nodes 1
just chaos run domain-time --image nervix:debian --fault voter-crash
just chaos run domain-time --image nervix:debian --fault voter-pause
just chaos run domain-time --image nervix:debian --fault voter-partition
just chaos run domain-time --image nervix:debian --fault voter-stop
just chaos run domain-time --image nervix:debian --fault cluster-restart
just chaos run domain-time --image nervix:debian --nodes 1 --fault cluster-restart
```

The runner creates the paced domain `chaos_paced` with `PERIOD 1s SKEW 1s`, installs
`fixtures/paced.nspl` and starts it with `START AT NOW TIME RATE 4.0`, so logical time runs four times
faster than physical time. A separate kcat container produces two interleaved branches every 250 ms
to `chaos_paced_input`; the ingestor stamps each record with `TIMESTAMP NOW` and `received_at =
now()`, and a detached window of `WIDTH 8s DURATION` closes on that logical clock and records when it
opened and closed. One independent observer per node, a container of the supplied image, follows
the domain's clock with the packaged CLI's `domain-clock` through that node's session route and stamps
every line with the host's clock. An observer whose follow ends attaches again.

`--fault` selects the faults. A voter fault runs one round per voter, in node order. Before each
round the paced graph moves with one acknowledged `RELOCATE` onto another node, or onto the voter
itself for a graceful stop, and the cluster must be settled and connected:

- `voter-crash` SIGKILLs the voter and starts it again after `--outage-seconds` (default 8);
- `voter-pause` pauses it for `--outage-seconds` (default 30) under the pause deployment;
- `voter-partition` isolates it with peer-side rules for at least `--outage-seconds` (default 30);
- `voter-stop` stops it gracefully with a 60-second grace, requires a completed shutdown whose
  drain-support phase completed, so the voter handed the paced graph to another node through the
  leader before it exited, and starts it again after `--outage-seconds` (default 8). A drain that
  does not complete is a product finding;
- `cluster-restart` SIGKILLs every node and starts each from its own volume;
- `none` observes the healthy domain without a fault.

While a fault holds away from the graph, its windows must keep closing; after each round the cluster
must settle, every observer must tick again, and windows must close again. `verify-clock-evidence.sh`
then judges what the observers recorded. Every attach and every state report after a reconnection must
carry the first report's generation and mapping; no `START` follows the first, so the generation
never changes. Every tick must lie on the mapping's period grid, with its boundary at
`logical origin + (id - 1) * period`, in that generation and no later than the serving node's own
logical time. Tick ids must increase within each attachment. A tick stall longer than two physical seconds on an observer whose
node stayed up must fall inside a fault and end within 60 physical seconds, the authority bound the
result records beside the deployed `NERVIX_NODE_UNAVAILABILITY_TIMEOUT`. From the start of each fault
until recovery, a sampler in one administration container reads every node's own `SHOW CLUSTER
STATUS` several times a second. Once a surviving node no longer counts the faulted voter available,
because its interconnect entry reads `unavailable` or has left the list, another voter must take the
clock over within ten physical seconds. A node becomes unavailable only after its health probes have
failed continuously for the deployed unavailability timeout; a gossip warning alone does not end its
availability, so a short fault may end before any replacement. The replacement bound measures the
portion of each continuous tick stall following that observation while the fault remains held.
Ticks that continued before a later stall remain healthy time in this accounting. Returning voters
can change the eligible authority set and wait for runtime readiness; gaps after healing retain the
sixty-second authority bound. No public command, metric or
info-level log names the clock authority, so a rotation covers every voter: the round whose fault
stalls the ticks on every surviving observer is the one that removed the authority, and a rotation
without such a round fails as missing evidence. Every paced window must close no earlier than eight
logical seconds after it opened, which also shows that a graceful stop never emitted a partial window;
away from a relocation or an outage of its own node, no more than eight logical seconds late. It must
hold rows of one branch, and windows must keep closing on their logical deadlines while an authority
stall withholds ticks. A stall caused by a graceful stop or a cluster restart is exempt, because that
round also takes the graph's own node down, so its windows wait for the graph rather than the clock.
The physical bounds of failover and shutdown therefore stay physical while windows follow logical
time. `domain-time/` keeps each round's fault evidence, the observers'
transcripts, the window output with Kafka timestamps and both verdicts, and
`results/domain-time-progress.json` summarizes them, with each round's availability samples under
its own directory.

### Stateful and domain-time qualification on the selected worker

The worker was the one of the restart qualification above: Ubuntu 26.04.1, Linux 7.0.0-34-generic,
Docker 29.8.1 with Compose 5.5.1, 24 CPUs and 66.7 GB of memory, shared with other workloads. Two
official AMD64 images were used: the earlier
`ghcr.io/nervix-io/nervix@sha256:0b39e7ab59ccbf22c8b6293c715affda49948d48c8ef4dc6d73ef89b6867677b`,
built from main `0913fddf`, and the image of this change,
`ghcr.io/nervix-io/nervix@sha256:56ef9ee5108dbc28c6569268431c434167152b3b70f5cb4451c41c36161fa2bc`,
built on main `9b75a6cb`, which includes the node-authenticated drain. Up to five runs shared the
worker at once. Every stateful run uploaded the 7,261-byte guest with SHA-256
`f8ecaf481938bac803d44aa2ec914f92f34ebfd6a7fa8c2bc5ee38ab7d75377d`.

On the newer image, three-node `stateful` passed with `owner-crash`, `owner-pause`,
`owner-partition` and `cluster-restart`, and `stateful` passed without a fault on one and three
nodes. node-3 owned the stateful entities with one replica, and node-1 ran both stateful ingestors.
Every run held the load at the 100-record milestone and again at 104 records while it read the
fault's boundaries, so each fault began with the source committed through record 104 and the
milestone's window rows still open. The work left the crashed, paused and isolated owner after
17.0, 22.1 and 42.4 seconds, and stateful output advanced again 56.0, 63.5 and 118.9 seconds after
the fault began, and 51.7 seconds after the cluster restart. In every fault run the deduplicator
dropped every later duplicate of a milestone key, the four window rows open at the milestone were
aggregated exactly once, every record after recovery carried profile version 3, and every guest
count equaled its branch index. No record was replayed and no row of the volatile interval was
lost. The runs without a fault matched every output exactly. On the earlier image the same three
owner faults and the run without a fault passed too.

Earlier runs read the fault's boundaries while the load kept running. On the loaded worker that
took about half a minute, and in three runs on the newer image the windows open at the milestone
closed before the fault began. Their window verdicts failed as unverified rather than passing, and
the second hold now keeps those rows open until the fault.

Three of the seven three-node `cluster-restart` runs, two on the earlier image and one on the
newer, lost the state of every processor. In each the leader moved every stateful entity off node-3
about half a second after it resumed leadership, inside the ten-second whole-cluster grace and
before its gossip had heard from either peer; [Cluster Chaos 39: Keep a restarted voter's work when
gossip hears of it before observing it live](https://app.clickup.com/t/86bcd4hzv) owns that. It
promoted its own replicas of the deduplicator and the WASM processor but published every recovery
with recreated state, because it had not yet installed the domain's schedule; [Cluster Chaos 50:
Keep a promoted replica's state when forced recovery is prepared before the destination installs
its schedule](https://app.clickup.com/t/86bcdg8uu) owns that. The verdicts failed as they should:
six later duplicates of milestone keys passed in each run, every record after recovery carried the
profile default, the guest counts started again from zero, and on the earlier image the open window
rows were lost as well.

A restarted window owner restores the windows open at the milestone. These runs used the official
AMD64 image
`ghcr.io/nervix-io/nervix@sha256:a40e5a4eb13c34d0fbd18ed686cb8de8b1edd9b54bc11b39b6afde4507fe66ec`,
built by the nightly Docker build of main `3351e1d5`. That is the first official image to include the
processor branch restore of `365b707b`. The worker ran Ubuntu 26.04.1, Linux 7.0.0-38-generic,
Docker 29.9.0 with Compose 5.6.0, 16 CPUs and 62 GB of memory. Four runs shared it at once.
One-node `stateful --fault cluster-restart` passed five of five runs. Three-node `stateful` passed with
`cluster-restart`, `owner-crash`, `owner-pause` and `owner-partition`; node-3 owned the stateful
entities with one replica. Every window verdict found the four rows open at the milestone and
aggregated all four exactly once after the fault. Eight runs lost only record 104, classified as
volatile, and the `owner-partition` run lost no row. No row was replayed. The deduplicator,
materialized relay and WASM guest verdicts passed in every run.

No node logged `failed to restore processor branches` or `failed to restore processor branch lru
snapshot`, so no restore was even delayed. One-node stateful output advanced again 48.1 to 52.6
seconds after the restart, and three-node output advanced again 49.1 seconds after it. Right after the
three-node restart, node-2 and node-3 each logged a single round of replica catch-up warnings: node-1
did not yet serve the branch lifecycle and branch-aggregated state of its ingestors. All six warnings
fell within the same 0.2 seconds and did not recur.

On the earlier image three-node `domain-time` passed every fault and the run without one. In each
rotation the third round, nervix-3, removed the authority. When that voter crashed, the survivors'
ticks stalled 8.5 and 8.6 seconds and resumed as it returned, 8.9 seconds after the kill. During a
30.9-second pause they stalled 17.0 seconds and resumed 4.4 seconds before the survivors' status
reported nervix-3 unavailable, 21.5 seconds into the pause. Isolated, it stopped their ticks 3.1 and
6.6 seconds into the isolation; both resumed 15.0 seconds in, 0.3 seconds after the survivors
reported it unavailable. A graceful stop of the authority stalled them 9.5 seconds, until shortly
after the voter returned. Throughout each of these stalls the paced windows kept closing on their
logical deadlines, 7 to 16 of them per stall. A whole-cluster restart stalled every observer 10.1 to
10.6 seconds and changed neither the generation nor the mapping. Every tick of every run lay on the
period grid of the first generation. Away from moves and outages, windows closed at most 404 logical
milliseconds after their eight-second width, and never before it.

On the newer image `voter-stop` and `voter-pause` passed as well. Every graceful stop completed its
drain. A graceful stop of the authority still stalled the survivors' ticks 9.7 seconds, until 0.4
seconds after they reported nervix-3 unavailable: the drain hands the graph over but not the clock
authority, which [Cluster Chaos 51: Hand a paced domain's clock authority to another voter during a
graceful stop](https://app.clickup.com/t/86bcdh3cz) proposes to change. The paused authority
stalled them 17.5 seconds, again resolved before the survivors reported it unavailable.

The first three-node `voter-stop` run failed on a verdict defect that has since been corrected. Its
image predates the node-authenticated drain of [Cluster Chaos 40: Drain a stopping follower through
the leader without a user password](https://app.clickup.com/t/86bcd8q49), so node-3's drain request
was refused as unauthenticated and its paced graph failed over only 11.5 seconds after the stop. The
windows verdict blamed the clock for the windows that graph could not close. A stall whose round
takes the graph's own node down is now exempt, and the drain verdict now reports such a refused drain
as a product finding instead. The rerun, in which every drain completed, passed.

One-node `domain-time` passed without a fault and across a whole-cluster restart. The scenarios
delivered before still passed on the earlier image: `baseline` on one and three nodes,
`follower-crash`, and one-node `cluster-restart`. Three-node `baseline` passed on the newer image
too.

Run a seeded experiment of mixed disturbances, and replay one:

```bash
just chaos run mixed-instability --image nervix:debian --seed 42 --duration 30m
just chaos run mixed-instability --image nervix:debian --seed 7 --duration 40m --policy temporary-quorum-loss
just chaos run mixed-instability --image nervix:debian --duration 10m --coverage kill:leader,degrade:follower
just chaos run mixed-instability --image nervix:debian --plan my-plan.json
just chaos replay target/chaos/<run-id>
```

A mixed-instability run executes a finite action plan on three nodes under continuous Kafka traffic.
The plan is a sequence of steps, and each step holds one fault or a combination of faults from the
delivered families: `stop` restarts a node gracefully as in `rolling-restart`, `kill` crashes it as in
the crash scenarios, `pause` pauses it for a second or two or for 20 to 40 seconds as in
`pause-resume`, `partition` isolates a node, drops the packets one node sends another or leaves no two
nodes able to communicate as in `partition-recovery`, and `degrade` applies one of the
`degraded-links` profiles to a directed link. Besides single faults, a step can degrade the link
between the two other nodes around a node's outage, restart a node inside its own isolation, or, under
the temporary-quorum-loss policy, take a second voter out inside the first one's outage, partition
every link, or crash every node.

`--seed N` selects the plan. With the same seed, `--duration` (default 30 minutes), `--policy` and
`--coverage`, the generator in `mixed-plan.sh` selects the same plan on every host, because it draws
from its own arithmetic generator rather than the shell's. It first plans a step for every required
coverage item that no earlier step covers, then adds steps while the planned timeline still fits the
duration, shuffles them, and lays them out with quiet gaps of 10 to 30 seconds. Each step records its
earliest start in the timeline and an estimate of how long it takes; a step starts no earlier than
its offset and no earlier than the end of the step before it, so a slow recovery delays later steps
instead of overlapping them. A seed omitted from the command is drawn and recorded. `--plan FILE`
runs an explicit plan instead; it carries its own seed, duration, policy and coverage, and its
estimates, from which the run sizes its fixture, are held to the generator's. Either way the
run writes the plan to `mixed/plan.json` and validates it before it creates a container, and the plan
stays unchanged while it executes.

A plan never names a concrete node. Each step names a role, `leader`, `follower`, `ingestor-owner`,
`relay-owner`, `emitter-owner` or `cluster`, and its actions name logical references that the step
resolves from public observations when it runs: `target` is the node holding the role, choosing the
follower the step's pick selects in node order; `peer` is the leader when the target is not, and
otherwise the lowest-numbered follower; `other` is the remaining node; `cluster` is every node. Before
the first fault, the step requires two settled public observations in a row to resolve these
references identically. Each observation reads every node's own `SHOW CLUSTER STATUS` and the owners
of the ingestor, relay and emitter through every node, and is settled when every node is connected and
free of warnings and all of them agree on leader, term, log position and owners.

The plan's quorum policy decides which combinations a run may hold. A kill, stop, pause or isolation
takes its node out of a quorum, a quorum-loss partition takes every node, and a degraded link takes
none. One-way loss takes out whichever of its two nodes is not the leader, because the leader keeps
leading and loses either its way to the receiver or the sender's acknowledgements. Validation judges
each step under every reference that can lead it: the target of a `leader` step, the peer of a
`follower` step, and either of them for an owner. `preserve-quorum`, the default, never lets two of
the three voters be out at once; `temporary-quorum-loss` allows it, for at most 120 seconds in a step.
Validation in `mixed-plan.sh validate` refuses a plan whose faults break its policy, whose network
faults overlap, since each interface carries at most one owned netem configuration, whose node fault
targets a node that carries another fault's rules, whose network fault is installed or healed while a
node is out of the link checks, whose holds leave their bounds, whose actions carry a field their
family does not take or an id other than lowercase letters and digits, whose step is estimated shorter
than its longest hold plus what the generator plans beyond the holds of its kind, or whose required
coverage no action plans; `quorum-loss` counts as planned only when two voters are out at once. Such a
run fails with exit status 2 in the phase `action plan and fixtures`, before preflight
and before any container exists, and `mixed/plan-validation.json` lists every reason. While it runs,
each fault is judged again immediately before it is injected, against the current state: the nodes
Docker reports stopped or paused, the link faults the step installed, and the fault itself. A fault
that would break the policy is refused as an injection failure before it alters a container.

Before each step every node container must be running and not paused, no run-owned injector may exist,
every node interface must carry its default state, and the public view must be settled, within 150
seconds. Each fault is injected and verified as in the scenario it comes from: a Pumba dry run that
selects exactly the resolved container, Docker inspection and the live event recording for kills,
stops and pauses, the graceful drain of a stop while live replacements exist, installed rules and the
measured link matrix for partitions, and for degradation installed rules, the broker and every node's
listeners still answering, and a measured ping and transfer effect. A node restarted inside an
isolation must come back still isolated. While the faults
hold, a quorum that can still communicate must agree on a caught-up leader within 90 seconds and
acknowledge a `CREATE RESOURCE` canary through that leader within 30 seconds, or 60 under a
degraded link. Random or burst loss on the one link left between the remaining quorum can stall
commits until the loss heals, so such a step records these checks without requiring them. With no
quorum able to communicate, a canary through every running node is bounded at 20 seconds and none may
be acknowledged, and a node cut off from every quorum may neither apply an entry
past the boundary recorded when the partition was verified nor, in quorum loss, advance its term. A
pause bounds these checks by its own end. Each hold is a minimum: a fault heals once its hold has
passed since it was verified and its checks have finished.

After the last heal of a step, every node must settle within 150 seconds, output must advance within
90 seconds, the source may run at most `--max-recovery-backlog` records (default 20) ahead of the sink
within 120 seconds, a canary through every faulted node must be acknowledged, the public metrics may
report at most `--max-pending` pending interconnect operations (default 128), and each node's Docker
lifecycle events from the step's start must be exactly those of its planned faults. At the end, the
events of the whole fault phase must again be exactly the planned ones, which also covers the quiet
gaps between steps, every canary of the run must be present through every node when it was
acknowledged and absent when it was refused, and the exact ledger must pass with replay duplicates
counted separately.

A background sampler records the source and sink end offsets and every node's Docker state, memory
and CPU every 10 seconds for the whole fault phase in `mixed/samples.ndjson`. A node whose memory
exceeds `--max-memory-bytes` (default 1 GiB) is a product finding, and two samples more than 120
seconds apart fail the run because the continuous observation had a gap.
`results/mixed-resources.json` summarizes each node's first, last and peak memory, the peak backlog,
and the longest source and sink stalls; `results/mixed-instability-progress.json` records every step's
nodes, timings and recovery, with the distribution of settle, delivery and drain times across the
run.

`mixed/actions.ndjson` is the action trace. It records when each step started and ended and how late
it started against its plan, the nodes it selected with the roles each held, and for each action
what it intended, the node, container and roles it met, the conditions its policy check judged — every
node's Docker state, the faults already holding and the voters left out of a quorum — and when it was
started, verified and healed, together with every hold check and every moment two voters were out of
a quorum. At the end
`verify-mixed-evidence.sh trace` requires the trace to be complete and ordered and the plan's coverage
to be met. `--coverage` lists `FAMILY:ROLE` items, where the family is `kill`, `stop`, `pause`,
`partition` or `degrade` and the role one of the step roles, plus `quorum-loss`. The default is every
family against the leader and against a follower, and under temporary-quorum-loss also `quorum-loss`.
An item is covered by a verified action whose node held the role when its step selected it, so an
action can cover several items; `quorum-loss` is covered when a step left two voters out of a quorum.
A plan whose required coverage does not fit the duration is refused as a setup error that names the
duration it needs. An incomplete trace, missing coverage or a gap in the samples fails the run as a
controller failure in `results/mixed-instability.json`; product findings fail it as product failures
after the final ledger. A run that fails inside its fault phase still writes
`results/mixed-trace.json`, whose verdict is then incomplete and names the records it never reached,
and `results/mixed-resources.json` for the samples it took.

The fixture holds twice the planned timeline plus 15 minutes of records, produced one a second, unless
`--records` asks for more, and the run is bounded by its duration plus the larger of that duration and
20 minutes, at most six hours, unless `--timeout` sets a bound from the duration plus 10 minutes
through six hours. Every step of the teardown that follows is bounded on its own, and together they
fit a 15-minute reserve after that bound, which the live event subscriber also outlives it by; the
manifest records the bound as `timeout_seconds`, the reserve as `teardown_reserve_seconds` and the
measured teardown as `teardown_seconds`. The deployment uses the standard 250 ms Raft heartbeat, 1.5
to 3 second election window and 10-second node unavailability timeout, and the manifest records them
as Compose renders them.

`just chaos replay <run-directory>` reconstructs a mixed-instability experiment from what its run
directory recorded. The replay starts the recorded Nervix image by its image ID when the host holds it,
and otherwise through a recorded repository digest, which it pulls within a bound when it is not
local. It starts the tool images by their recorded digest references rather than the pins checked in
now, and each must have its recorded image ID or carry the repository digest its reference pins. An
image ID is the image store's own name for an image: the config digest in Docker's classic store and
the manifest or index digest in the containerd store. A run recorded on a worker whose store is of
the other kind, such as a CI worker, is therefore found through the repository digest, which names
the same content in every store, and the replay's manifest records which of the two matched under
`replay_image_match`. It copies the recorded plan, input fixture and NSPL graph, each of which must still match the
digest the manifest records, deploys the recorded node settings and load interval and refuses to
start when Compose renders anything else, and applies the recorded limits. It runs in a new Compose
project with a new run identifier, network, TLS material and volumes, and resolves every logical node
reference against the new cluster's own public observations. A directory that is not a mixed-instability
run, lacks any of these records, or holds a plan or fixture that no longer matches fails before the
replay creates anything, and an image that is unavailable fails it in preflight with exit status 2,
before the replay records a Docker event. `--artifacts`, `--run-id`, `--timeout` and `--keep` are the
only options a replay accepts. Its manifest names the run it replays under `replay_of`. Once a run's
manifest records its deployment, the last part of the experiment a replay needs, a failure prints the
replay command and records it as the reproducer in `results/finding.json`. A run that fails earlier
records the run command instead, with its plan when it has one and otherwise with the seed, duration,
policy and coverage that select it, together with any `--records`, `--timeout` and limits it was given,
and a replay that fails earlier records the replay of its source.

A replay reproduces the experiment's inputs and its sequence of actions, not its timing. The images,
deployment, workload records and their pacing, plan, steps, roles, logical references, parameters and
policy are identical. Linux scheduling, container start times, Raft elections, failure detection,
Kafka rebalances and the outcomes of random and burst packet loss are not, so the node that holds a
role when a step selects it, how many records the producer has sent by then, how long each recovery
takes and how a step's late start shifts the steps after it can all differ between a run and its
replay. Each run's trace records what it actually met.

### Mixed-instability qualification on the selected worker

The worker was the one of the qualifications above, shared with other workloads at load averages of
9 to 49, and every run used the image of the stateful qualification,
`ghcr.io/nervix-io/nervix@sha256:56ef9ee5108dbc28c6569268431c434167152b3b70f5cb4451c41c36161fa2bc`,
built on main `9b75a6cb`.

Seed 42 under `preserve-quorum` and seed 45 under `temporary-quorum-loss` each selected 14 steps for 30
minutes, and both passed, in 30 minutes 20 seconds and 30 minutes 16 seconds of their 60-minute bound.
Seed 42 paused a follower, the emitter owner and the leader; crashed the leader on its own and while 30%
random loss held between the other two nodes; crashed a follower under the combined degradation between
the other two, and the ingestor owner; stopped the leader and a follower; isolated a follower and the
leader; dropped the packets a follower sent the leader; and degraded the leader's link with random loss
and a follower's with the rate limit. Seed 45 degraded links from the leader, the ingestor owner and a
follower with the rate limit, random loss and delay; isolated the relay owner; crashed a follower inside
its own isolation; paused a follower and crashed the leader inside that pause; crashed every node;
partitioned every link; dropped the packets the leader sent a follower; paused the leader and the
ingestor owner; stopped a follower and the leader; and crashed the leader under a rate limit between the
other two nodes. Every step started within 28 milliseconds of its planned offset, every fault was
verified and healed, and both traces were complete. Through the roles their nodes held, the verified
actions covered 25 and 24 items: in seed 42 every family against the leader, a follower and each owner,
and in seed 45 all of those but a stop of each owner and a partition of the ingestor owner, together
with `quorum-loss`, `kill:cluster` and `partition:cluster`. Without a quorum no canary was
acknowledged, neither through the node left running in the double outage nor through any of the three
in the partition. After every step the cluster settled within 0.8 to 4.4 seconds of the last heal,
output advanced within 2.2 to 21.1 seconds, and the backlog was back within its limit within 4.9 to
27.2 seconds; the slowest followed crashes. The exact ledgers matched 1,726 records with no duplicate
and 1,720 records with one replay duplicate. Docker memory grew from 54 to 59 MB a node to peaks of 65
to 87 MB, the backlog never exceeded 43 records, and consecutive samples lay at most 17.2 seconds apart.

The first run of seed 42 failed its third step, 30% random loss on the leader's link to a follower, as an
injection failure. The loss let that follower's election timer expire, it won the election, and for 0.4
seconds the former leader knew no leader, so its `/readyz` refused the check that the fault left every
node's endpoints answering. That check now requires the listeners, as the degraded-links section
describes, and the rerun above passed the same plan.

Seed 136 under `temporary-quorum-loss` failed as a product failure in its second step, a double outage
of the ingestor owner. The runner crashed the leader nervix-1, crashed nervix-2 six seconds later and
observed the lost quorum as the policy requires. Started again while nervix-1 was still stopped, nervix-2
exited at startup with `failed to start cluster membership … resolving 'nervix-1' failed: the name does
not exist`: Docker's DNS did not resolve the stopped container, and at that revision the node required
its configured bootstrap host even though its recovered Raft members could have found a survivor. [Cluster Chaos
56: Start a restarting voter from its recovered Raft members when its bootstrap host does not
resolve](https://app.clickup.com/t/86bcdk4tn) tracks that failure. An explicit one-step plan, a crash of the leader
with a crash of its peer inside it, reproduced the failure on its first run, within four and a half
minutes. That run, which ended inside its fault phase, still wrote its incomplete trace verdict and its
resource summary. Seed 45's double outage paused its first node, whose name stays resolvable, so it did
not meet the defect.

An explicit plan with one step of every kind ran for 43 minutes under temporary-quorum-loss: a leader
crash, a follower stop, a 25-second pause of the relay owner and a one-second pause of the leader, the
isolation of a follower, one-way loss from the leader, the combined degradation of the emitter owner's
link, a leader crash under burst loss between the survivors, a follower crashed and restarted inside its
isolation, a quorum-loss partition, a follower crash with a pause of the leader inside it, a
whole-cluster crash, and a stop of the ingestor owner and a pause of a follower, each under jitter or
delay between the other two nodes. Every fault was verified and healed, the trace was complete and its
verified actions covered 26 items through the roles their nodes held, and the exact ledger matched
2,476 records with no duplicate. With no quorum able to communicate, every canary stayed unacknowledged
within its bound: through all three nodes in the partition, and through the one node left running in
the double outage, bounded at 9 seconds by the pause. The node restarted inside its isolation answered
its own status route, its peers' rules and the link matrix still matched the plan, and it never applied
past the boundary. After every step the cluster settled within 1.3 seconds of the last heal and output
advanced within 30 seconds; the slowest were the leader crash and the whole-cluster crash, which waited
for the broker to release the killed consumer's partition. Docker memory grew from 55 to 61 MB a node to
at most 81 MB, the backlog never exceeded 41 records, and consecutive samples lay at most 16 seconds
apart. Beyond their holds the steps took 17 to 55 seconds, apart from 106 seconds for the step whose
canary waited out its bound, and the plan estimates are about one and a half times these. The run's one
finding came from an
expectation since corrected: with the leader crashed and the survivors' only link dropping half its
packets in bursts, a canary through the new leader stayed unacknowledged for 60 seconds. Loss on the
last link of a quorum can stall commits until it heals, and such a step now records availability without
requiring it; 0.8 seconds after that heal every node had settled.

A three-step plan, a leader crash, a 20-second delay on a follower's link to the leader and a two-second
pause of a follower, passed with 314 of 314 records, and its replay passed with 303 of 303. The replay
used the same plan and fixture digests, image and tool identities and deployment, and each step
resolved its references to the same nodes, while each step took between 9 seconds less and 3 seconds
more than in the original run. A run of a
two-step plan with the deliberately impossible `--max-memory-bytes 1048576` failed as a product failure,
with one memory finding per node, peaking at 59.7 to 68.4 MB, and `just chaos replay` of its directory
as the reproducer. Its replay applied the recorded limit and failed the same way, with the same three
findings, its own replay command and the same evidence.

A plan holding a double outage under preserve-quorum and two overlapping network faults exited with
status 2 within one second, in the phase `action plan and fixtures`, before preflight: its validation
report named both reasons, and no container, network or Docker event recording existed for the run.
Replays of copies of a run directory failed the same way when the recorded Nervix image was neither
local nor pullable, in preflight within 2 seconds, when the input fixture was missing, and when the plan
no longer matched its digest; and seed 42 with a five-minute duration failed as a setup error that named
the 1,546-second plan its required coverage needed. The refactored fault and link primitives kept the
single scenarios passing: `degraded-links --profile combined`, `stateful` with `owner-pause`,
`owner-crash` and `cluster-restart`, and `domain-time --fault voter-stop` each passed twice, except that
the second three-node `stateful` `cluster-restart` lost the window rows open at the milestone, because
that image's processors failed to restore their branches. Its deduplicator, materialized relay and WASM
guest kept their state. The stateful qualification above shows the restore on a later official image.

The controller resolves the supplied reference to its immutable local image ID before Compose
starts. If the reference is not local, it performs one bounded pull and then resolves the result.
The Compose file has no build directives. Every Nervix node and every disposable administration
container uses the resolved ID. Each run creates its own Compose network in a `10.213.N.0/24` range
that overlaps no existing Docker network or host route. Nodes take the fixed addresses `.11` through
`.13` and every other container an address from the upper half. The manifest records the network
and the node addresses.

Every tool image a run starts is pinned by a registry-qualified digest in `tool-images.sh`: Apache
Kafka 3.9.1 for the broker and topic administration, kcat 1.7.1 for traffic, Alpine 3.22.6 for
probes, observers and event markers, and Pumba 1.2.1 with its nettools helper for faults. Kafka,
Alpine, Pumba and nettools are pinned by multi-architecture index digest; kcat 1.7.1 is published
for linux/amd64 only. The Compose file has no fallback tags. In preflight, before it resolves the
Nervix image and before the run creates any container, the controller makes each tool image the
scenario uses local, pulling a missing one once within a bound, and records its reference and image
ID under `tool_images` in `manifest.json`. Result files and `results/finding.json` carry the same
map. A pinned image that cannot be pulled fails the run in preflight with exit status 2: the setup
error names the image on the console and in the manifest's `setup_error`, and Docker's pull output
is kept in `diagnostics/tool-image-pull-<tool>.txt`. Moving a tool to another release means
replacing its digest in `tool-images.sh` and requalifying the scenarios that start it.

Every scenario except the baseline takes its input from the continuous load, the Compose `load`
service, which runs `continuous-load.sh` in the pinned kcat image. The load produces one fixture
record per interval to `chaos_input` until the runner writes its stop file or the fixture ends, and
every record leaves through its own `kcat -P` call. kcat 1.7.1 reads its standard input in
1,024-byte blocks and produces a record only once the block that holds its newline is full or the
input has ended, so one kcat fed a paced stream delivers about nine fixture records at once and then
nothing for nine intervals. A call that has returned has delivered its record, so no record waits
inside the producer when the load stops or holds. A call that kcat reports as failed ends the load
with a nonzero status and a message that names the record, and the run's next check of the load
fails it. The baseline has no continuous load: it produces its whole fixture with one call. The
stateful and domain-time scenarios start a second load beside the first, `state-load` and
`paced-load`: the same script with its own fixture, topic and stop file.

Records are due one interval apart on the kernel's uptime clock, which the load reads from
`/proc/uptime` to a hundredth of a second. A call that returns within half an interval of its
record's due time keeps that schedule, so the time a call takes does not stretch the interval. A
call that returns later restarts the schedule from its return: the next record leaves a whole
interval after it, and the load never catches up with a burst. Two records are therefore at least
half an interval apart, less the 10 ms the clock resolves. The interval of `load` is 500 ms for
rolling restarts, crashes and backups, two seconds for partitions, `--load-interval-ms` for degraded
links, and one second for every other scenario. `state-load` produces one record a second and
`paced-load` one every 250 ms. `manifest.json` lists each load a run starts under `loads`, with its
service, topic and interval.

After a load stops, the runner reads the create time the broker stored for each of its records into
`traffic/<service>-timestamps.txt`. A producer stamps that time when it hands a record to its
client, so the gaps between consecutive records are the gaps between the load's kcat calls.
`verify-load-pacing.sh` writes them to `results/<service>-pacing.json`: their minimum, median, 95th
percentile, maximum and mean, the number of gaps longer than one and a half intervals, and the
longest gap with its offset. Two records closer than a quarter of the interval fail the run as a
controller failure, after its ledger and results are written, because every timing the run reports
was then measured under a load other than the declared one. A longer gap never fails a run: a call
that waited on the broker makes one record late, a held load is late by its hold, and both are
reported.

### Continuous load qualification on the selected worker

The worker ran Ubuntu 26.04.1, Linux 7.0.0-34-generic, Docker 29.8.2 with Compose 5.6.0, 32 CPUs
and 67.0 GB of memory. Unrelated builds and test suites shared it throughout and held its
one-minute load average between 15 and 85. The supplied image was
`ghcr.io/nervix-io/nervix@sha256:56ef9ee5108dbc28c6569268431c434167152b3b70f5cb4451c41c36161fa2bc`,
the official AMD64 image built on main `9b75a6cb`.

One `kcat -P` call for one record took 10 ms at the median of 2,500 consecutive calls against the
pinned broker, 12 ms at the 99th percentile and 103 ms at the most. At 100 ms, the shortest
interval `--load-interval-ms` accepts, 400 records left 90 to 145 ms apart, with a median of
100 ms and a mean of 100.1 ms. Piped through one kcat process at that interval, 120 records of
that fixture arrived eight or nine at a time: 106 of the 119 gaps were at most 1 ms and the other
13 were 506 to 916 ms.

A call opens a connection, asks the broker for its API versions and the topic's metadata and waits
for the record's acknowledgement, so a broker that answers late makes that record late. With calls
100 ms apart at a load average of 29 to 35, nine of 1,500 took longer than 150 ms, the longest
2.8 seconds: seven waited on the broker and two on the resolution of its name. Such calls are the
gaps over one and a half intervals below. [Cluster Chaos 55: Produce each load record over a kept
broker connection without losing per-record delivery](https://app.clickup.com/t/86bcdkcpu) proposes
a producer that performs those steps once.

Every delivered scenario then ran with its loads paced this way, up to four runs at a time.
`baseline`, which starts no continuous load, matched 24 of 24 records on one and three nodes, and
`mixed-instability` ran a three-minute plan of one leader kill and then the replay of that run.
Every run in the table matched its exact ledger, and every pacing verdict passed. The pause run
delivered one identical replay duplicate, which its ledger reports separately; no other run
replayed a record. The longest gaps of `state-load` are the two holds of the stateful scenario.

| Scenario | Nodes | Load, interval (ms) | Records | Accepted/delivered | Gap minimum, median, 95th percentile, maximum (ms) | Mean gap (ms) | Gaps over 1.5 intervals |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `backup` | 3 | `load`, 500 | 238 | 238/238 | 456, 501, 511, 1,770 | 520.7 | 6 |
| `backup` | 1 | `load`, 500 | 124 | 124/124 | 483, 501, 503, 536 | 500 | 0 |
| `rolling-restart` | 3 | `load`, 500 | 610 | 610/610 | 444, 501, 502, 558 | 500 | 0 |
| `rolling-restart` | 3 | `load`, 500 | 637 | 637/637 | 465, 500, 504, 1,060 | 500.8 | 1 |
| `rolling-restart` | 1 | `load`, 500 | 364 | 364/364 | 471, 501, 502, 1,060 | 501.6 | 1 |
| `leader-crash` | 3 | `load`, 500 | 321 | 321/321 | 442, 501, 502, 556 | 500 | 0 |
| `leader-crash` | 1 | `load`, 500 | 425 | 425/425 | 294, 501, 504, 1,117 | 502.5 | 3 |
| `follower-crash` | 3 | `load`, 500 | 409 | 409/409 | 421, 500, 504, 1,519 | 508.2 | 5 |
| `ingestor-owner-crash` | 3 | `load`, 500 | 497 | 497/497 | 377, 500, 503, 545 | 499.8 | 0 |
| `emitter-owner-crash` | 3 | `load`, 500 | 370 | 370/370 | 358, 501, 507, 790 | 502.3 | 3 |
| `pause-resume` | 3 | `load`, 1,000 | 635 | 635/635, 1 replayed | 960, 1,001, 1,005, 2,838 | 1,007.7 | 5 |
| `partition-recovery --case follower` | 3 | `load`, 2,000 | 250 | 250/250 | 1,905, 2,001, 2,002, 2,079 | 1,999.9 | 0 |
| `partition-recovery --case asymmetric` | 3 | `load`, 2,000 | 195 | 195/195 | 1,989, 2,001, 2,004, 3,063 | 2,005.5 | 1 |
| `partition-recovery --case leader` | 3 | `load`, 2,000 | 181 | 181/181 | 1,927, 2,001, 2,005, 2,079 | 1,999.9 | 0 |
| `partition-recovery --case quorum-loss` | 3 | `load`, 2,000 | 130 | 130/130 | 1,789, 2,001, 2,005, 4,256 | 2,017.5 | 1 |
| `degraded-links --profile delay` | 3 | `load`, 750 | 316 | 316/316 | 724, 751, 754, 1,665 | 755.5 | 2 |
| `degraded-links --profile jitter` | 3 | `load`, 750 | 317 | 317/317 | 692, 751, 753, 1,477 | 755.5 | 3 |
| `degraded-links --profile random-loss` | 3 | `load`, 750 | 400 | 400/400 | 641, 750, 752, 1,576 | 756.9 | 4 |
| `degraded-links --profile burst-loss` | 3 | `load`, 750 | 374 | 374/374 | 662, 751, 754, 1,574 | 754.1 | 2 |
| `degraded-links --profile rate-limit` | 3 | `load`, 750 | 354 | 354/354 | 649, 750, 754, 1,703 | 756.1 | 3 |
| `degraded-links --profile combined` | 3 | `load`, 750 | 400 | 400/400 | 669, 751, 753, 2,366 | 758.5 | 4 |
| `stale-follower` | 3 | `load`, 1,000 | 333 | 333/333 | 800, 1,001, 1,004, 1,901 | 1,008.4 | 4 |
| `former-owner-restart` | 3 | `load`, 1,000 | 330 | 330/330 | 971, 1,001, 1,005, 1,912 | 1,004.3 | 2 |
| `cluster-restart` | 3 | `load`, 1,000 | 129 | 129/129 | 626, 1,001, 1,003, 1,378 | 1,000 | 0 |
| `cluster-restart` | 1 | `load`, 1,000 | 217 | 217/217 | 709, 1,001, 1,004, 1,579 | 1,005.3 | 2 |
| `stateful` | 3 | `load`, 1,000 | 743 | 743/743 | 721, 1,000, 1,006, 2,995 | 1,016.1 | 14 |
| `stateful` | 3 | `state-load`, 1,000 | 289 | — | 960, 1,001, 1,007, 222,432 | 1,974.3 | 6 |
| `stateful --fault owner-crash` | 3 | `load`, 1,000 | 702 | 702/702 | 693, 1,001, 1,005, 2,280 | 1,009.7 | 8 |
| `stateful --fault owner-crash` | 3 | `state-load`, 1,000 | 286 | — | 693, 1,001, 1,005, 168,084 | 1,753.5 | 2 |
| `domain-time` | 3 | `load`, 1,000 | 130 | 130/130 | 963, 1,001, 1,003, 1,040 | 1,000 | 0 |
| `domain-time` | 3 | `paced-load`, 250 | 277 | — | 182, 251, 252, 326 | 250 | 0 |
| `domain-time --fault voter-crash` | 3 | `load`, 1,000 | 198 | 198/198 | 973, 1,001, 1,004, 1,029 | 1,000 | 0 |
| `domain-time --fault voter-crash` | 3 | `paced-load`, 250 | 679 | — | 176, 251, 255, 400 | 250.3 | 1 |
| `mixed-instability --seed 42 --duration 3m --coverage kill:leader` | 3 | `load`, 1,000 | 107 | 107/107 | 907, 1,002, 1,004, 2,131 | 1,015.6 | 2 |
| `replay` of that run | 3 | `load`, 1,000 | 164 | 164/164 | 795, 1,002, 1,008, 2,186 | 1,024.7 | 4 |

Three-node `rolling-restart` passed two of the three runs that reached their verdicts. In
`cc44-f-rolling-3` the second round stopped node-2, which led by then. Its drain moved both of its
graph nodes, but the release of its own cordon, one consensus write with a one-second budget,
timed out, so the node reported `shutdown drain-support phase finished outcome=Abandoned` and the
run failed as a product finding. In the 13 other graceful stops with a replacement the release took
8 to 382 ms. [Cluster Chaos 52: Release a stopping node's drain cordon within its drain budget
instead of abandoning the drain after one second](https://app.clickup.com/t/86bcdkcf1) owns the
fix. The degraded-links, restart and recovery runs are described in their qualification sections
above.

Other runs ended on the runner's own budgets under this worker's load, without a product verdict.
[Cluster Chaos 53: Size the chaos runner's own time budgets and fixtures for a loaded
worker](https://app.clickup.com/t/86bcdkckc) owns them:

- thirteen runs ended in the verifier self-check, which outlasted its 180-second bound beside other
  chaos runs. The bound is now 600 seconds; the self-checks that completed took 34 to 176 seconds;
- both `partition-recovery --case all` runs passed their first three cases without a finding and
  reached the end of the 1,000-record fixture during the fourth case's recovery, 33 to 34 minutes
  after traffic began, so the four cases were qualified one per run;
- one three-node `rolling-restart`, beside three other chaos runs, reached the end of its fixture
  in its second round;
- two `pause-resume` runs ended as injection failures after cases that had passed. In one, Pumba's
  container reached its pause call after the five seconds the runner allows it. In the other,
  Docker held a one-second pause for 3.7 seconds, where a short pause may last 0.8 to 2 seconds.
  The third run passed its four cases.

Docker statistics sampled through one four-case partition run showed every node that led while a
voter was unreachable using 110 to 145% of a CPU until the links healed. [Cluster Chaos 54: Stop a
leader spinning more than one CPU while a voter is unreachable](https://app.clickup.com/t/86bcdkcm8)
owns that.

The host needs Bash, Docker with Compose, GNU `timeout`, OpenSSL, and jq 1.8 or later. The
verifiers use jq 1.8's grammar, which jq 1.7 refuses to compile, so `just chaos run`,
`just chaos replay`, a suite run and `just chaos self-test` stop at once with a setup error naming
an older jq; listing, planning, reporting and cleaning up a suite work with jq 1.7. Kafka administration,
traffic, listener probes, metrics probes, and Nervix administration run in prebuilt containers.
The baseline provisions its Kafka topics explicitly, installs the checked-in NSPL graph with the
image's packaged `nervix-cli`, and reconstructs the accepted-input ledger from Kafka itself. It then
checks the ingestor consumer-group boundary and compares the output ledger by event ID and exact
content. Duplicate, missing, unexpected, and corrupt records are reported independently.

Each run writes bounded evidence under `target/chaos/<run-id>/`: the immutable Nervix and tool
image identities, rendered Compose configuration, phase ledger, public cluster and placement
output, listener and metrics probes, broker offsets, accepted and observed traffic, verifier
reports, logs, Docker events, and container inspection. The default baseline fixture has 24
records and accepts at most 1,000 with `--records`.
Rolling runs additionally retain a Pumba command and version, each target's before/stop/start
inspection, stop and observer logs, measured stop and recovery times, per-restart public status and
metrics, and per-restart offset results. A failed identity, stop, exit, deadline, listener, or
recovery check leaves the manifest and available evidence under that run directory.

Docker events come from one live recording per run, `diagnostics/docker-events.ndjson`. A replay
through `docker events --since` returns only what remains of the daemon's buffer of its most recent
256 events, which every container on the host shares, so no verdict reads one. Before the run
creates its first container, the controller starts a `docker events` subscriber filtered to the
run's label and places a labeled marker container. The recording starts when the marker's creation
appears in it, which proves the subscriber live and the daemon stamping events on the controller's
clock. Each Docker-event check closes its window with another marker and reads that window from the
recording: the crash kill and the node events from the fault through recovery, the pause interval
and the node events around it, and each partition case's node events and restart kill. A window is
accepted only when recorded markers bracket it, and its bounds are kept beside it in a
`.recording.json` file. A recording that is missing, starts after a window opens, or ends before it
closes, including one whose subscriber exited, fails the run as a controller failure with those
bounds as evidence. When diagnostics are captured, after every heal, a final marker closes the
recording. It must then cover the whole run within 64 MiB, and its bounds are kept in
`diagnostics/docker-events.recording.json`. Because the closed recording holds the creation of
every run-owned container, `results/container-images.json` then attributes each one, markers
included, to the resolved Nervix image or to a tool image the manifest records. A container
created from any other image fails a run that otherwise passed as a controller failure.

All owned containers, networks, and volumes carry `io.nervix.chaos.run=<run-id>`. This label also
gives Pumba scenarios an exact target selector; Nervix nodes additionally carry
`io.nervix.chaos.target=true`. Normal exit, failure, timeout, and catchable signals preserve
diagnostics and remove the labeled resources. Pause cleanup first unpauses every run-owned paused
container, including when Pumba fails or the controller receives a supported signal. Partition
runs first stop their injectors and remove any Pumba-owned qdisc or INPUT rule left on a node.
Restart and recovery runs heal a former owner's isolation the same way and then start every node
container they still hold stopped. A mixed-instability run first stops its sampler and every pause it
still waits for, then unpauses every node, heals every network fault and starts every node it holds
stopped. A
controller killed before its trap runs leaves its event subscriber to exit on its own 15 minutes
after the run's timeout. To remove the run's resources in that case, use:

```bash
just chaos cleanup --run-id <run-id>
just chaos cleanup --run-id <run-id> --evidence target/chaos/<run-id>-leftovers
```

`--evidence DIR` first captures what the run left behind: the listing and inspection of every
container, network and volume carrying its label, and the last 2 MiB of each container's log. Only
then does cleanup remove them, and it lists the label again to confirm that nothing survived. A
container that was exiting while its removal ran can still hold the network it was attached to, so
cleanup removes again whatever that listing still finds, in at most three rounds two seconds apart,
and fails only when the last listing still finds something; `cleanup.json` in DIR records what it
removed, the rounds it took and what remained. A run that left nothing gets no evidence directory.
`--keep` retains resources for interactive diagnosis and prints the same cleanup command.

Run the external verifier, pinned tool image and Compose contract checks directly with:

```bash
just chaos self-test
```

`just test-chaos-rate-probe` exercises the production probe with a two-second connect delay,
receiver completion delayed twelve seconds, partial bytes, complete loss, a receiver that does not
listen, a receiver launch failure and a nonzero receiver exit, failed log retrieval and failed
cleanup. It uses only the pinned probe and
nettools images with Docker, and leaves no helper behind. `just coverage-chaos-rate-probe` runs
those controls under kcov and retains its
shell Cobertura report under `target/chaos-rate-probe-coverage/`. These observations test the external
probe; they make no claim about product recovery or model-checker coverage.

`just test-chaos-stateful-verifiers` runs the state, clock and window verdict controls separately.
`just verify-chaos-clock` accepts the clock verifier's arguments for judging retained observer
transcripts, and `just coverage-chaos-stateful-verifiers` retains shell coverage for these controls.

## Diagnostic images

Every scenario and suite also runs against a Deloxide diagnostic image: a test artifact whose server
tracks its thread-blocking locks with the deadlock detector of [Data-Plane
Concurrency](../../docs/src/data-plane-concurrency.md#diagnostic-deadlock-detection), built from the
same revision as a release image and never published as one. Build one, or overlay a locally built
diagnostic server on an available image, and run it like any other image:

```bash
just docker-build-diagnostic deloxide-order 23 nervix:diagnostic
just build-chaos-diagnostic-image nervix:diagnostic-local ghcr.io/nervix-io/nervix:debian-latest deloxide-order
just chaos run leader-crash --image nervix:diagnostic-local
just chaos suite smoke --image nervix:diagnostic-local --image-kind deloxide-order
```

A diagnostic image declares its selection, `deloxide`, `deloxide-order` or `deloxide-stress`, in its
`io.nervix.diagnostic.selection` label, and both kinds of image declare the revision they were
built from in `org.opencontainers.image.revision`. The run reads the label after it resolves the
image, records the selection and revision in `manifest.json`, and requires the image to package
`nervix-deadlock-report` beside the server. It then adds `compose.diagnostic.yaml` to the
deployment: every node records its evidence in `deadlock/node-N/` of the run directory, mounted at
`/var/lib/nervix-deadlock` and named by `NERVIX_DEADLOCK_EVIDENCE`, so the evidence outlives the
node's container and volume. Nothing else of the deployment, the workload, the faults or the
verdicts changes: the fault injection stays external, and no scenario adds a deadlock hook.

Each process a diagnostic node starts writes one evidence file, `deadlock-PID-TIME.rkyv`, when its
diagnostic run starts, and replaces it after each finding. Once the run's live Docker event
recording is closed, the controller freezes every node that still runs, so no process records a
finding after its evidence is read, and qualifies each file with the image's own
`nervix-deadlock-report` in a container without a network whose only mount is the node's evidence
directory, read-only. The tool's inspection and qualification sit beside each file as
`.inspect.txt` and `.qualify.txt`, and `deadlock/qualification.ndjson` lists every file with its
size, the tool's exit status and the selection it records. The nodes run again before cleanup.
`verify-diagnostic-evidence.sh` then writes `results/diagnostic-evidence.json`, which passes only
when:

- each node holds exactly one complete evidence file for each start of its container in the
  recording, and no partially written one;
- every file is valid evidence of the selection the image declares and qualifies: it holds no
  active cycle, no unreviewed potential cycle and no loss;
- no node container ended with status 3, the end of a process whose detector reported an active
  deadlock, or 4, the end of a failed diagnostic execution;
- a node keeps at most 64 files and the run at most 64 MiB of evidence, and the report tool judges
  every file within the run's 300-second qualification budget, inside the teardown reserve.

The result counts the process starts, files, bytes and findings: active, potential, unreviewed and
lost. A run that otherwise passed fails in phase `diagnostic evidence` with the category of its
problems: `diagnostic` for a finding, missing, partial, invalid or unbounded evidence, or another
selection, and `controller` when the recording is incomplete or the tool could not run. An active
deadlock or a failed diagnostic execution also classifies a run that failed otherwise as
`diagnostic`, because the node it ended explains what the run observed after. Every such failure
keeps `results/finding.json` with the evidence files among its evidence. A potential cycle is never
approved here: a correction removes the order from the workload, as in the diagnostic lane.

The evidence is an observation of each process up to its end or the freeze. SIGKILL, a pause and a
graceful stop end a process without a flush barrier, so a finding the detector had not yet delivered
when the process ended is not in its evidence, and the detector observes only Nervix's tracked
thread-blocking locks: Tokio's locks and channels, atomic protocols and network waits keep their
own Shuttle, Loom, Turmoil and Chaos evidence, and a qualifying run proves no untracked path free of
deadlocks. Timings of a diagnostic image are not product performance evidence, and its runs keep the
product's deadlines and the suite's budgets unchanged.

## Suites and CI

A suite is a named selection of runs from `suites.json`, which CI and a developer start with the
same command against an already built image:

```bash
just chaos suite list
just chaos suite smoke --image ghcr.io/nervix-io/nervix@sha256:<digest>
just chaos suite soak --image ghcr.io/nervix-io/nervix@sha256:<digest> --shard 6
just chaos suite soak --image nervix:debian --entry partition-leader --entry stale-follower
just chaos suite smoke --image nervix:diagnostic --image-kind deloxide-order --image-revision "$(git rev-parse HEAD)"
```

Each entry of a suite is one `just chaos run` command, listed with its arguments, and a budget in
minutes that the suite passes to it as `--timeout`, so a run that overruns fails by its own bound
with its own diagnostics. An entry also names its shard, the unit one CI job runs, and may carry a
note. `--shard` runs one shard and `--entry` the named entries; together they select both. The
entries run one after another, every one of them even after another fails, and a suite never
retries a failed run.

The `smoke` suite runs the three-node baseline, a leader crash with its explicit restart, and the
partition and recovery of the follower that owns execution. The `soak` suite runs every delivered
scenario: both topologies of the baseline, backup, rolling-restart, leader-crash and
cluster-restart scenarios, every other crash role, the pause cases, each partition case and each
degradation profile on its own, stale-follower and former-owner-restart, and every fault of the
stateful and domain-time scenarios. Its mixed-instability runs last 30 minutes each: the fixed
seeds 42 under `preserve-quorum` and 45 and 136 under `temporary-quorum-loss`, and two rotating
seeds, one per policy, that every run draws and records. Seed 136 crashes a voter inside the crash
of the bootstrap node, exercising the recovered peer contact path when that bootstrap name no
longer resolves.

Beside what a run needs, a suite needs `setsid` from util-linux and a `tee` that takes `-p`, as the
one of GNU coreutils does, and refuses to start without them; its cleanup reads process state from
`/proc` and `ps`. Before its first run the suite checks its own verdict logic with
`tests/suite-self-test.sh`, which `just chaos self-test` runs too. It resolves the image once: a
digest reference stays as given, and any other reference runs as the local image ID it resolves to,
so every run of the suite uses one immutable image. It records the revision and the diagnostic
selection the image's labels declare. `--image-kind ordinary`, or the selection `deloxide`,
`deloxide-order` or `deloxide-stress`, and `--image-revision REVISION` refuse, as a setup error
before any run, an image whose labels declare another kind or revision, so the suites of a release
image and a diagnostic image show that both came from one revision. On a [diagnostic
image](#diagnostic-images) an entry passes only when its run recorded its nodes as diagnostic nodes
of the image's selection and a passing evidence verdict. A suite writes `ARTIFACTS/SUITE_ID/` (by default
`target/chaos/<suite>-<time>-<pid>/`):

- one run directory per entry, named `<suite-id>-<entry>`, exactly as `just chaos run` writes it;
- `logs/<entry>.log`, the console output of each run;
- `suite.json`, the machine-readable record: the suite and shard, the requested image with its
  resolved image ID, repository digests, revision and diagnostic selection, the worker's kernel, Docker and Compose versions, CPUs
  and memory, and for every entry its command, run ID, verdict, exit status, failure category, final
  phase, duration, timeout and teardown, seed and policy, reproducer, the delivery ledger
  (expected, observed, duplicate, missing, unexpected and incorrect records), the headline recovery
  timings its result records (election, placement, returning listeners, settlement, resumed delivery,
  and the slowest settle, output and drain after a heal of a mixed run), a mixed run's peak node
  memory, backlog and output stall, a diagnostic run's evidence verdict with its process starts,
  files, bytes and findings, and the result files it wrote;
- `summary.md`, the same verdicts rendered for a reader, which the suite also prints at the end;
- `suite.log`, the suite's whole console, which it goes on writing when its caller stops reading.

An entry passes only when its command exits 0 and its manifest records a passing run of the suite's
image. The suite fails an entry as a controller failure when the run left no manifest, when the
manifest's exit status contradicts the command's, or when the run resolved another image. A failed
entry takes the category of its `results/finding.json`, `setup` when the run refused before creating
its directory or recorded a setup error, and is listed as unclassified otherwise, which is the case
of a failed baseline, backup or rolling-restart run until [Cluster Chaos
33](https://app.clickup.com/t/86bc9dkk2) classifies them. An interrupted entry carries no category,
because the category a run held when a signal ended it names no cause. Its reproducer is the run's
own when it recorded one and otherwise the entry's command. The suite exits 0 when every entry
passed, 1 when any failed or did not run, 2 for a setup error before any run, such as an unknown
entry or an image that is neither local nor pullable, and 128 plus the signal's number when a signal
ended it.

A run executes in the background and leads a session of its own, so an interrupt, termination or
hangup reaches the suite at once and reaches the run only through the suite, even when a terminal
signals the suite's whole process group. The suite passes one TERM to the run, whose own exit trap
then heals, captures and cleans up as on any exit, records it as interrupted, and starts no later
entry. The suite's console reaches its caller through a tee that ignores a terminal's interrupts and
hangups and goes on writing `suite.log` when the caller stops reading, so the suite outlives its
console and still records every verdict.

`just chaos suite cleanup [--wait SECONDS] SUITE_DIRECTORY` ends what a suite left behind. The
suite records the process of its controller and of every run it starts. A controller that still
executes receives one TERM, which it passes to its run as on any signal, and a run whose controller
is gone receives one TERM itself. Each has `--wait` seconds, 300 by default, to finish its exit
work; cleanup then kills the controller if it still executes and every process left in the session
of each run. For every run the suite started it runs `just chaos cleanup --evidence` into
`cleanup/<run-id>/`, removes the private keys of any run whose exit trap never ran, keeps the
verdict of a run that finished before its controller ended, records the entries that never reported
an exit status as interrupted and those never started as not-started, records how the controller
ended and which runs cleanup signalled or killed, and renders the summary again.

`just chaos suite shards SUITE` prints the budgets CI applies to each shard. Its execution budget is
the sum of its entries' budgets. Its step budget adds one 15-minute run teardown reserve, the bound
within which a run that reached its own timeout finishes its teardown. Its job budget adds
`ci_reserve_minutes` for the steps that follow the suite: cleanup, the summary and the uploads.
`just chaos suite report [--expect-shards N] SUITE_JSON...` renders one summary of several shards
and fails unless each passed and N of them are present.

The `Chaos` workflow, `.github/workflows/chaos.yaml`, runs a suite on CI against one image of a
named kind and revision. The `Docker Build` workflow calls it twice for each suite it runs: with the
release image its build job published, `ghcr.io/nervix-io/nervix:<build-id>-debian-amd64`, as kind
`ordinary`, and with the diagnostic image its `build-diagnostic` job published from the same
checkout, `ghcr.io/nervix-io/nervix:<build-id>-debian-amd64-deloxide-order`, as kind
`deloxide-order`, both with the revision the build checked out. The diagnostic image is pushed under
that tag only and never enters a published manifest. The diagnostic build runs only when a chaos run
needs it:

- a pull request labeled `chaos` runs the smoke suite against both images built from it;
- a pull request labeled `chaos-soak` runs the soak suite against both images;
- the nightly build of `main` at 02:00 UTC runs the soak suite against both images, and publishes
  neither a manifest nor the book.

A label that builds no image runs its Docker Build in a concurrency group of its own, so a pull
request that adds `chaos` beside other labels in one request keeps the build `chaos` started.

A manual run of the `Chaos` workflow names the suite, an immutable image,
`REPOSITORY@sha256:DIGEST`, its kind and optionally its revision, and builds nothing. The workflow's plan job resolves the image to its
digest, so every shard runs the same image, and reads the shards and their budgets from
`just chaos suite shards`. Each shard then runs on its own GitHub-hosted `ubuntu-24.04` Docker
worker. The repository's Blacksmith workers cannot qualify: their kernel offers none of the queueing
disciplines Pumba installs, so every run there stops in its network preflight or in the network
healing exercise of its verifier self-check. The job loads the `sch_prio`, `sch_sfq` and `sch_netem`
modules and fails when the kernel cannot provide one, installs `just` and `toml`, and installs jq
1.8.1 from its release, checked against the release's SHA-256, because the worker carries Ubuntu's
jq 1.7. It removes every Rust toolchain directory from the `PATH` the suite runs with and fails if
`cargo`, `rustc` or `rustup` is still reachable, pulls the image and logs out of the registry, and
runs
`just chaos suite <suite> --shard <n> --image <digest> --image-kind <kind> --image-revision <revision> --artifacts <dir> --suite-id ci-<run>-<attempt>-<suite>-<n>`,
whose suite ID adds `-diagnostic` before the shard for a diagnostic image.
The suite step ends at the shard's step budget and the job at its job budget. When the step is
cancelled or reaches its budget, the runner sends SIGINT to the step's process, `just`, which does
not pass it on, then SIGTERM 7.5 seconds later, which `just` passes to the suite that its recipe
`exec`s, and 2.5 seconds after that SIGKILL to `just` alone; shortly afterwards it stops reading the
step's output. The suite and its run outlive the step, so the job always runs
`just chaos suite cleanup` next: it waits up to five minutes for the controller to finish its run's
exit work and record it, kills whatever still executes, and captures and removes what any run left
behind as a second witness. It then publishes the shard summary and uploads two artifacts for 14
days: `chaos-verdict-<suite>-<n>-<kind>-<run>-<attempt>` with `suite.json` and `summary.md`, and
`chaos-<suite>-<n>-<kind>-<run>-<attempt>` with the whole suite directory, which holds a diagnostic
run's evidence files. The run attempt is part of each name, so a rerun never replaces the artifacts
of the attempt it repeats, and the kind sits between the shard and the run, so the verdicts of one
kind are never read as another's. The verdict job of each kind publishes `just chaos suite report`
over every shard's verdict of that kind and fails when a shard did not pass or recorded no verdict.

To investigate a CI failure, download the shard's evidence artifact and read its `summary.md`: each
failed entry names its category, phase, reproducer, finding and run directory. A reproducer pins the
image by digest, and `just chaos replay <run-directory>` reconstructs a mixed-instability run from
its downloaded directory with the recorded images.

### Suite qualification on the CI worker

Every shard ran alone on a GitHub-hosted `ubuntu-24.04` worker: Linux 6.17.0-1022-azure, Docker 28.0.4
with Compose 2.38.2, 4 CPUs and 15.6 GiB. Each push of the pull request that introduced the suites
built its own official AMD64 image, and the plan job resolved it to its digest. Two earlier runs set
the worker's requirements:

- on `blacksmith-4vcpu-ubuntu-2404` workers, Linux 6.6.141, Pumba's `tc` exited with status 2 when it
  added the `prio`, `sfq` and `netem` queueing disciplines. Every network scenario stopped in its
  preflight with `Pumba netem installed nothing on the canary`, and every other run in the healing
  exercise of its verifier self-check;
- on `ubuntu-24.04` with the worker's own jq 1.7.1, every run's self-check stopped at a jq compile
  error in the recovery verifier. The job now installs jq 1.8.1, and the scripts refuse an older jq.

The qualification run found a defect of the runner itself: the certificate authority of a run named
the whole run ID in its subject, which holds at most 64 characters, so every run whose ID exceeded
48 characters failed in `TLS generation`. Suite run IDs reach 53 characters in CI. The authority now
names a digest of the run ID, and a baseline with a 96-character run ID, the longest the runner
accepts, passed.

The suites then ran twice there, in Docker Build runs 37497979884 and 37505248763 against
`ghcr.io/nervix-io/nervix@sha256:ab41fb2c40252d04c0e47154cc6117f03660dcf99a1b574b8881f5cc5c005133`
and `ghcr.io/nervix-io/nervix@sha256:0058679f650d69f44028069fba977453f03ef34f118b6d028ed88ef1707c8241`,
both built from the product source of main `8a8a3986`. In the second run the smoke shard passed with
401 seconds of runs in a 7-minute 18-second job, and its verdict came 8 minutes 20 seconds after the
image build finished. The soak's shards took 17 to 36 minutes of runs. A shard whose 30-minute
mixed-instability run reaches its end takes 36 to 38 minutes, and the three shards whose mixed runs
failed early took 17 to 21. The table lists every entry's duration in seconds, with the category of a
failed run, against its budget in minutes. A budget is about two and a half times the duration first
measured on this worker, rounded up and at least three minutes, and 60 minutes, the runner's own
default bound, for a 30-minute mixed run. Entries whose first run stopped in `TLS generation` took
their budget from earlier runs on a developer worker, and the second run confirmed every one of them.

Setting up a shard took under a minute: the checkout 3 seconds, installing `just` and `toml` 14 to 18
seconds, loading the queueing disciplines a second, pulling the image 7 to 14 seconds. After the
suite, cleanup, the summary and both uploads took 3 to 6 seconds, well inside the 10-minute reserve:
the cleanup step's own 8-minute bound, which holds its five-minute wait for a controller that still
executes, and 2 minutes for the summary and the uploads.

| Suite | Entry | Shard | Budget (min) | Run 37497979884 (s) | Run 37505248763 (s) |
| --- | --- | ---: | ---: | ---: | ---: |
| smoke | `baseline-3` | 1 | 4 | 82 | 68 |
| smoke | `leader-crash-3` | 1 | 7 | 143 | 135 |
| smoke | `partition-follower` | 1 | 9 | 205 | 195 |
| soak | `mixed-seed-42` | 1 | 60 | 1811, product | 1283, observation |
| soak | `baseline-1` | 1 | 3 | 62 | 60 |
| soak | `baseline-3` | 1 | 4 | 69 | 64 |
| soak | `cluster-restart-1` | 1 | 6 | 137 | 134 |
| soak | `cluster-restart-3` | 1 | 7 | 166 | 163, product |
| soak | `mixed-seed-45` | 2 | 60 | 1786 | 1784, product |
| soak | `leader-crash-1` | 2 | 6 | 123 | 120 |
| soak | `leader-crash-3` | 2 | 7 | 148 | 144 |
| soak | `follower-crash` | 2 | 5 | 119 | 118 |
| soak | `mixed-seed-136` | 3 | 60 | 343, product | 352, product |
| soak | `ingestor-owner-crash` | 3 | 6 | 125 | 150 |
| soak | `emitter-owner-crash` | 3 | 5 | 101 | 130 |
| soak | `stale-follower` | 3 | 4 | 90 | 114 |
| soak | `former-owner-restart` | 3 | 10 | 239 | 241 |
| soak | `mixed-rotating-preserve` | 4 | 60 | 1813, product | 556, observation |
| soak | `rolling-restart-1` | 4 | 5 | 97 | 96 |
| soak | `rolling-restart-3` | 4 | 7 | 168 | 182 |
| soak | `backup-1` | 4 | 4 | 78 | 80 |
| soak | `backup-3` | 4 | 4 | 87 | 88 |
| soak | `mixed-rotating-temporary` | 5 | 60 | 406, product | 948, product |
| soak | `pause-resume` | 5 | 14 | 327 | 315 |
| soak | `partition-follower` | 6 | 9 | 190 | 217 |
| soak | `partition-asymmetric` | 6 | 7 | 160 | 186 |
| soak | `partition-leader` | 6 | 7 | 168 | 191 |
| soak | `partition-quorum-loss` | 6 | 8 | 169 | 193 |
| soak | `degraded-delay` | 6 | 10 | 205 | 199 |
| soak | `degraded-jitter` | 6 | 10 | 206 | 201 |
| soak | `degraded-random-loss` | 6 | 10 | 202 | 200 |
| soak | `degraded-burst-loss` | 6 | 10 | 213 | 202 |
| soak | `degraded-rate-limit` | 6 | 10 | 210 | 208 |
| soak | `degraded-combined` | 6 | 10 | 220 | 214 |
| soak | `domain-time-1` | 7 | 7 | 148 | 162 |
| soak | `domain-time-1-cluster-restart` | 7 | 8 | — | 180 |
| soak | `domain-time-3` | 7 | 7 | 157 | 158 |
| soak | `domain-time-3-cluster-restart` | 7 | 10 | — | 190 |
| soak | `domain-time-3-voter-crash` | 7 | 12 | — | 202 |
| soak | `domain-time-3-voter-pause` | 7 | 20 | — | 267 |
| soak | `domain-time-3-voter-stop` | 7 | 15 | — | 202 |
| soak | `stateful-1` | 8 | 20 | 474 | 464 |
| soak | `stateful-1-cluster-restart` | 8 | 22 | — | 456 |
| soak | `stateful-3` | 8 | 20 | 472 | 473 |
| soak | `stateful-3-owner-crash` | 8 | 20 | 470 | 472 |
| soak | `stateful-3-owner-pause` | 9 | 20 | 472 | 476 |
| soak | `stateful-3-owner-partition` | 9 | 25 | — | 477 |
| soak | `stateful-3-cluster-restart` | 9 | 22 | — | 470, product |
| soak | `domain-time-3-voter-partition` | 9 | 25 | — | 357 |

Every failure of the two runs is a product defect or a harness limitation with its own task:

- seed 136 under `temporary-quorum-loss` failed in both runs, and so did both rotating
  `temporary-quorum-loss` seeds, 1048352328 and 349485620. Each time a voter restarted during a double
  outage and exited with `resolving 'nervix-1' failed: the name does not exist`, which [Cluster Chaos
  56](https://app.clickup.com/t/86bcdk4tn) tracks;
- seed 42 and the rotating `preserve-quorum` seed 600180860 in the first run, and seed 45 in the
  second, failed because a stopping node's Kafka ingestor stayed quiesced for its ownership handoff
  until the 30-second drain timeout, so the drain was abandoned. [Cluster Chaos
  60](https://app.clickup.com/t/86bcdwr9y) owns that, and Cluster Chaos 52 owns the other cause of the
  same `outcome=Abandoned` record;
- in the second run, seed 42 and the rotating `preserve-quorum` seed 696031281 ended as `observation`
  failures, because the rate probe of a `combined` degradation gave up on a transfer that loss had
  stalled for a second. [Cluster Chaos 61](https://app.clickup.com/t/86bcdxk5q) owns the probe;
- in the second run, the three-node `cluster-restart` and `stateful --fault cluster-restart` moved
  owners within the voter observation grace and lost processor state, which [Cluster Chaos
  39](https://app.clickup.com/t/86bcd4hzv) and [Cluster Chaos 50](https://app.clickup.com/t/86bcdg8uu)
  own.

Seed 45 passed its full 30 minutes in the first run, and every other entry passed in every run that
the TLS defect did not stop.

The temporary qualification entry of the first run, a mixed-instability plan under an impossible
1 MiB memory limit, failed its smoke job as a product failure with one memory finding per node and
printed its replay command. Its run directory, downloaded from the shard's evidence artifact,
replayed on a developer host with Docker 29's containerd image store and failed the same way, with
the same three findings; the replay recorded `replay_image_match` as the repository digest. Before
replays accepted repository digests, the same replay stopped in preflight, because the probe image
ID the CI worker recorded is the config digest of its classic store and differs from the index digest
the containerd store names it by.

Cancellation was qualified against the runner's own sequence. On Linux the runner sends SIGINT to
the step's process, SIGTERM 7.5 seconds later and SIGKILL to that process alone 2.5 seconds after
that, and stops reading the step's output about five seconds after the process ended. The first
cancellation, of the second attempt of run 37505248763 during the smoke suite's leader crash, showed
two defects. The recipe's shell died of the TERM `just` forwarded, so the suite never received a
signal and went on with its run after the step had ended, and the cleanup step raced that run,
failed to remove a container the run was removing and the network it still held, and failed. The
runner's code showed a third: once the runner stops reading, the tee of a run's log dies at its next
write, and the run with it at the run's next one. The recipe now `exec`s the controller, every run
leads its own session, the tees outlive their caller, and cleanup waits for a controller that still
executes. A local reproduction of the sequence against
`just chaos suite smoke --entry leader-crash-3`, interrupted during the failover observations, then
ended as intended: the run received the suite's TERM, healed, captured and tore down in 6 seconds,
after `just` had been killed, and recorded its interruption; the suite wrote its verdict and summary
into `suite.log` after its console had closed; and the cleanup that followed found the controller
still executing, waited 4 seconds for it to finish and found nothing left. In CI, cancelling the
first attempt of run 37527441862 during the smoke suite's leader crash ended inside the runner's ten
seconds: the run recorded its interruption after a one-second teardown and the suite its verdict
before `just` was killed, the cleanup step found nothing left, both artifacts were uploaded, and the
rerun attempt passed with the cancelled attempt listed in its verdict.
