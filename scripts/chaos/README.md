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
Crash runs default to 1,000 paced input records; a smaller `--records` value can exhaust the load
before fault verification and then fails as a setup limit. When output remains short of accepted
input after the recovery bound, the controller still saves the available output and runs the exact
ledger verifier to identify the missing IDs.
Failed crash runs retain `results/finding.json` with the phase, failure category, image identity,
and an external reproduction command pinned to a pullable repository digest when available,
together with Pumba, Docker, public status, broker and log evidence.

The rolling scenario uses the same externally provisioned graph and immutable Nervix image as the
baseline. A separate producer continuously writes unique records, an independent observer probes
every configured listener, and Kafka remains up throughout the rotation. The controller observes
the current leader, stops that node first, then restarts each remaining node once. It invokes a
digest-pinned Pumba 1.2.1 container against the exact labeled Compose container name with a
60-second stop grace, longer than Nervix's default 50-second shutdown deadline. Docker must expose
the local `/var/run/docker.sock` to the Pumba container. The same stopped container is started
through Docker to preserve its image and named volume.

Each stop must produce Docker exit code zero and completed shutdown-phase logs. The independent
observer must see the listener outage and restoration; the source broker must advance while the
node is stopped. In a three-node run, the survivors must agree on a caught-up leader and the sink
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
A partition run defaults to 1,000 input records paced two seconds apart and a 50-minute bound.

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
inspects the single owned netem qdisc and peer filter, and tests the affected link with ICMP and a
prebuilt Alpine netcat transfer. The transfer counts all bytes at the receiver and times their
arrival. It records a healthy ICMP and transfer baseline before any fault.
Loss, delay, jitter and rate must also show their expected measured effect; an installed qdisc alone
cannot pass. SIGTERM heals each injector, after which the qdisc and ICMP delay must return to the
healthy state. The exit trap heals all run-owned faults even with `--keep` or an interrupted run.

The workload uses the separate prebuilt `kcat` producer at a declared fixed interval (750 ms by
default), with a 1,000-record fixture. `--load-interval-ms`, `--baseline-seconds`,
`--degrade-seconds`, and `--drain-seconds` set the load and measurement windows. The runner rejects
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

The host needs Bash, Docker with Compose, GNU `timeout`, OpenSSL, and jq. Kafka administration,
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
runs first stop their injectors and remove any Pumba-owned qdisc or INPUT rule left on a node. A
controller killed before its trap runs leaves its event subscriber to exit on its own 15 minutes
after the run's timeout. To remove the run's resources in that case, use:

```bash
just chaos cleanup --run-id <run-id>
```

`--keep` retains resources for interactive diagnosis and prints the same cleanup command.

Run the external verifier, pinned tool image and Compose contract checks directly with:

```bash
just chaos self-test
```
