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
inspection showed it paused and then running without a process restart. Docker pause and unpause
events supply the actual interval; a short pause outside 0.8–2 seconds or a long pause outside
15–100 seconds fails the experiment. Independent broker load, listener observation, peer-side
public status and control canaries continue around the fault. Short pauses may end before the
listener observer samples the outage; the runner records that limit and still requires the
observed leader and owners to stay stable and every listener to recover. Recovery time is measured
from the Docker unpause event, separately from the pause interval. Final Kafka ledgers require
every accepted record with the correct content and branch, reporting identical replay duplicates
separately. The run retains per-case Docker, public, broker, observer and Pumba evidence under
`pauses/` plus `results/pause-progress.json`.

The follower and execution-owner variants require three nodes. `--outage-seconds N` holds the
selected node down for at least 5–120 seconds (default 8). The controller reads the leader and
ingestor/emitter owners through the packaged CLI, selects the corresponding exact Compose node,
and checks its image, labels, restart policy and persistent volume through Docker inspection. It
rechecks the public role immediately before invoking digest-pinned Pumba with `kill --signal
SIGKILL`. A dry run must resolve only that container. The real kill must yield a Docker signal-9
event, a die event with exit code 137, and a stopped container throughout the declared outage.
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
wrong-branch records. Unexpected node exits and failures to converge also fail the command.
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

The controller resolves the supplied reference to its immutable local image ID before Compose
starts. If the reference is not local, it performs one bounded pull and then resolves the result.
The Compose file has no build directives. Every Nervix node and every disposable administration
container uses the resolved ID.

The host needs Bash, Docker with Compose, GNU `timeout`, OpenSSL, and jq. Kafka administration,
traffic, listener probes, metrics probes, and Nervix administration run in prebuilt containers.
The baseline provisions its Kafka topics explicitly, installs the checked-in NSPL graph with the
image's packaged `nervix-cli`, and reconstructs the accepted-input ledger from Kafka itself. It then
checks the ingestor consumer-group boundary and compares the output ledger by event ID and exact
content. Duplicate, missing, unexpected, and corrupt records are reported independently.

Each run writes bounded evidence under `target/chaos/<run-id>/`: the immutable image identity,
rendered Compose configuration, phase ledger, public cluster and placement output, listener and
metrics probes, broker offsets, accepted and observed traffic, verifier reports, logs, Docker events,
and container inspection. The default baseline fixture has 24 records and accepts at most 1,000
with `--records`.
Rolling runs additionally retain a Pumba command and version, each target's before/stop/start
inspection, stop and observer logs, measured stop and recovery times, per-restart public status and
metrics, and per-restart offset results. A failed identity, stop, exit, deadline, listener, or
recovery check leaves the manifest and available evidence under that run directory.

All owned containers, networks, and volumes carry `io.nervix.chaos.run=<run-id>`. This label also
gives Pumba scenarios an exact target selector; Nervix nodes additionally carry
`io.nervix.chaos.target=true`. Normal exit, failure, timeout, and catchable signals preserve
diagnostics and remove the labeled resources. Pause cleanup first unpauses every run-owned paused
container, including when Pumba fails or the controller receives a supported signal. If a
controller is killed before its trap runs, use:

```bash
just chaos cleanup --run-id <run-id>
```

`--keep` retains resources for interactive diagnosis and prints the same cleanup command.

Run the external verifier and Compose contract checks directly with:

```bash
just chaos self-test
```
