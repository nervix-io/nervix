# Deterministic Interconnect Simulation

The interconnect has a seeded network simulation built on [Turmoil](https://docs.rs/turmoil/0.7.2).
Each Nervix transport runs on a simulated host with its own Tokio runtime, virtual clock, TCP stack,
and DNS name. The hosts talk over simulated links that the test can delay, partition, hold, and
repair. The code above the socket is the production code: TLS 1.3 mutual authentication, HTTP/2
pools, typed rkyv envelopes, Arrow IPC payloads, relay admission and reconciliation, connection
lifecycle, and quota accounting all run unchanged. The same seed and inputs make the same decisions
in the same simulated order, so a network failure that once exposed a defect can be run again
exactly.

This chapter owns that simulation: where its code lives, how it builds, what a simulated host
controls, how a run is supervised, the fault model, how runs are compared and replayed, the
scenarios and what each one proves, the commands, the CI budget, the qualification evidence, and the
limits of every claim. Three rules hold throughout.

- The simulation tests interconnect contracts. It never claims that a whole Nervix node, a consensus
  group, gossip, the domain clock, a store, or an external system was simulated.
- A replay compares decisions, not bytes. A trace records identities, admission decisions, outcomes,
  and the simulated time at which they happened. It never records key material, certificate bytes,
  ciphertext, or payload values.
- A simulated host crash is not a process crash. It tears a runtime down with ordinary destructors,
  so durability and SIGKILL behavior keep their real-process evidence.

The harness is test code outside the layer order: it may name any layer, and product code never
names it. Product contracts stay with the chapters that own them: node-to-node transport with
[Cluster Interconnect](./interconnect.md), in-process ordering under Shuttle with [Data-Plane
Concurrency](./data-plane-concurrency.md), node stops and crash recovery with [Shutdown And
Recovery](./shutdown.md), domain time with [Domain Clock](./domain-clock.md), and the Cucumber
cluster harness with [Integration Test Lifecycle](./integration-test-lifecycle.md).

## What Each Suite Establishes

Four suites exercise the interconnect. Each establishes something the others cannot, and none of
them substitutes for another.

| Suite | Command | Scheduler and time | What it establishes | What it cannot establish |
| --- | --- | --- | --- | --- |
| Ordinary unit tests | `just test-interconnect`, `just test-execution` | Real Tokio runtime, paused or real time | Local logic of one owner: framing, codecs, quotas, identities | Behavior across a lossy network or between real processes |
| Shuttle checks | `just test-shuttle` | Shuttle's scheduler over wrapped primitives; no elapsed time | That in-process protocols hold their invariant under every explored interleaving | Sockets, elapsed deadlines, or anything across hosts. See [Deterministic Concurrency Verification](./data-plane-concurrency.md#deterministic-concurrency-verification) |
| Turmoil simulation | `just test-turmoil` | One Turmoil scheduler owns every host; each host has a paused Tokio clock that advances with simulated time | That the production transport keeps its deadlines, reconciliation, fencing, and resource bounds when links are delayed, partitioned, held, repaired, or a host restarts, and that the same seed replays the same decisions | Instruction-level races inside one job, kernel TCP behavior, durability, and whole-node behavior |
| Cucumber scenarios | `just test-scenarios --input <feature>` | Real nodes, real sockets, real time | Public behavior through NSPL, HTTP, the CLI, and cluster state, including real-process SIGKILL recovery | Exhaustive fault timing; failures are observed, not scheduled |

## Code Ownership

| Path | Kind | Owns |
| --- | --- | --- |
| `crates/interconnect/src/socket.rs` | Product seam | Selects the TCP listener, the outbound stream, and the `PeerResolver`: Tokio sockets and the node's Hickory resolver in production, `turmoil::net` sockets and DNS in the Turmoil build |
| `crates/interconnect/src/authentication.rs` | Product seam | `TransportClock`, the one UTC clock each TLS bundle judges certificates by. `TransportClock::system` in production, `TransportClock::from_provider` for a controlled environment |
| `crates/interconnect/src/entropy.rs` | Product seam | `TransportEntropy`, the source of the process epoch and relay grant identifiers. `TransportEntropy::operating_system` in production, `TransportEntropy::from_source` for a controlled environment |
| `crates/execution/src/workers.rs` | Product seam | The strategy that runs an admitted CPU job: the blocking pool, or a scheduler task in the Turmoil build |
| `crates/interconnect/src/lib.rs`, `crates/execution/src/lib.rs` | Product seam | The compile error when both scheduler modes are selected |
| `crates/interconnect/tests/simulation/runner.rs` | Harness | Run configuration, the scheduler thread and its real-time bound, host supervision, first-panic capture, the simulated UTC clock, seeded entropy, and the semantic trace. The library's own simulation tests include it as `simulation_runner` |
| `crates/interconnect/tests/simulation/scenario.rs` | Harness | Scenario identity, committed seeds, the seed sweep, fresh-process attempts, failure records, replay, and failure injection |
| `crates/interconnect/tests/simulation.rs` | Harness | The `simulation` test target: runner checks, the DNS and CPU scenarios, and the fresh-process record and replay checks |
| `crates/interconnect/tests/simulation/transport.rs` | Harness | The certificate authority fixture, typed Arrow requests, host synchronization, and the exchange and link-fault scenarios |
| `crates/interconnect/tests/simulation/relay.rs` | Harness | Relay reply loss, cancellation, and receiver-restart scenarios |
| `crates/interconnect/tests/simulation/isolation.rs` | Harness | The three-host stalled-peer scenario and the capacity bounds it checks |
| `crates/interconnect/src/authentication/simulation_tests.rs` | Harness | Certificate validity, expiry drain, handshake deadlines, and trace replay under the simulated clock |
| `crates/interconnect/src/wire.rs`, module `simulation_checks` | Harness | Seeded rkyv round trips through the execution owner |
| `crates/execution/src/workers.rs`, module `simulation_checks` | Harness | Queue slots, admission order, and charge ownership of the Turmoil CPU strategy |
| `justfile` and the `turmoil` job in `.github/workflows/check.yaml` | Tooling | The recipes, budgets, validation, and CI artifact |

Every product seam has exactly one production behavior, and the production value is the default a
node uses. A seam exists so that a controlled environment can supply a value; it is not a switch
between two production behaviors. The harness never reaches into product state: it constructs a
transport through its public API, supplies a clock and an entropy source, and observes the result
through requests, snapshots, and outcomes.

## Build Modes

The simulation is its own build. Its scheduler replaces the real network, so it can never share a
compilation with the production or Shuttle builds.

| Build | Selected by | Sockets and DNS | CPU jobs | Synchronization primitives | Tokio configuration |
| --- | --- | --- | --- | --- | --- |
| Normal | Default features | Tokio's operating-system sockets and the node's Hickory resolver | Tokio's blocking pool | Tokio, `parking_lot`, `dashmap`, `tokio-util` | Stable |
| Shuttle | The `shuttle` feature of each owning package | Not driven by the checks | `spawn_blocking` of Shuttle's modeled Tokio | Shuttle's modeled `tokio`, `parking_lot`, `dashmap`, and `tokio-util` | Stable |
| Turmoil | The `turmoil` feature of `nervix-interconnect`, which enables `nervix-execution/turmoil` | `turmoil::net` | A task on the simulated host's scheduler; storage jobs stay on the blocking pool | Real | `--cfg tokio_unstable` |
| Shuttle and Turmoil | Both features in one package | Fails to compile with `Shuttle and Turmoil scheduler modes cannot be enabled together` | | | |

Only the Turmoil recipes pass `--cfg tokio_unstable`. With it, Turmoil seeds each host runtime's
scheduling from the simulation seed and configures an unhandled task panic to shut that host's
runtime down, which fails the run. Without it, scheduling would not follow the seed and a panicking
task would only print, so the runner refuses to start. The workspace declares the cfg in its
`check-cfg` list so ordinary builds accept the attribute without enabling it. Turmoil 0.7.2, locked
in `Cargo.lock`, is used without its unstable filesystem feature.

Validation keeps the modes apart:

- `just validate-turmoil-dependencies` fails when the normal workspace dependency graph, with or
  without default features, contains a Turmoil package.
- `just validate-shuttle-dependencies` does the same for Shuttle.
- `just validate-simulation-feature-conflict` builds `nervix-execution` and `nervix-interconnect`
  with both features and requires the compile error above in each.
- `just lint` runs Clippy over every target of both packages with the `turmoil` feature, beside the
  production and Shuttle lints.
- `just test` runs the workspace with all features while excluding every package that offers a
  scheduler mode, runs those packages with default features, and then runs `just test-turmoil`.

## The Simulated Host

A simulated host is a named Turmoil host running one production transport. Everything the transport
owns is constructed inside the host: its credential bundle, executor, peer resolver, transport, and
peer targets.
Live sockets and transports are never shared between hosts. Hosts share only bounded readiness and
completion signals with each other and with the observing client.

### Sockets And DNS

The Turmoil build aliases `TcpListener` and `TcpStream` to `turmoil::net` and builds every transport
with `PeerResolver::simulated`, which answers from the simulated host's DNS table through
`turmoil::net::lookup_host`. The listener, every outbound dial, and peer resolution therefore use
simulated TCP and DNS. A peer endpoint such as `server:7443` resolves to the address Turmoil
assigned to the host named `server`, in IPv4 or IPv6 by configuration, and a name the table does not
hold fails as a name that does not exist. The scenarios register peers at their advertised
endpoints, so, as in production, every connection attempt resolves the peer's name again inside its
setup deadline and dials the answer. Nothing in the simulated path opens an operating-system socket,
constructs the production Hickory resolver, reads a resolver configuration or hosts file, or sends a
DNS packet, so a fault scenario cannot escape to the real network and no resolver state is shared
between hosts. The layers above the socket, `tokio-rustls`, `h2`, the envelope codec, and Arrow IPC,
are the production code. These scenarios exercise the transport over simulated names; Hickory's DNS
protocol, cache, and failure handling are checked by the resolver crate's own tests against local
DNS authorities outside the simulation, as described in [Peer Name
Resolution](./interconnect.md#peer-name-resolution).

### Bounded CPU Execution

The transport encodes and decodes through `nervix-execution`. In the Turmoil build an admitted CPU
job runs as a Tokio task on the host's scheduler instead of on the blocking pool, so the job body is
one scheduling step of the simulation. Queue slots, per-class worker reservations, memory charges,
and cooperative cancellation follow the same policy as production. A queued caller that leaves
releases its slot and charge; a running job keeps its worker and charge until it exits, even when
its caller leaves.

Turmoil can vary the order of tasks around a job, but not instructions inside it. Races inside a job
need Shuttle or real threads. Storage jobs still run on a real blocking thread and are outside the
simulated path. Only bounded executor probes and interconnect codec jobs are approved for the
simulation; storage jobs, external drivers, and unbounded CPU work are not.

### Time

A simulated host has three distinct notions of time, and the simulation controls each one
differently.

| Time | Source on a simulated host | Read by |
| --- | --- | --- |
| Scheduler time | Tokio's paused clock, advanced by Turmoil one tick at a time | Every transport deadline: connection setup, request and progress timeouts, liveness, reconnect backoff, relay grant lifetimes, the drain deadline derived from certificate expiry |
| Certificate UTC | The bundle's `TransportClock`, given a provider that reads the configured epoch plus simulated elapsed time, with an optional per-host skew | Rustls verification on both sides of a handshake, the transport's checks of its local and peer certificates, and the mapping of certificate expiry onto a monotonic drain deadline |
| Domain time | Not simulated | Domain-clock authority and execution time, owned by [Domain Clock](./domain-clock.md) |

Outside a simulated host the simulated UTC clock has no reading, and a certificate check fails
instead of falling back to the host wall clock. The certificate fixtures' epoch,
2027-01-15T00:00:00Z, lies after the wall clock of any machine the suite runs on today, so a check
that consulted the host clock by mistake would find every fixture certificate not yet valid. The
transport clock is physical infrastructure time only: it never defines domain time and never stamps
connector arrivals.

### Identities And Entropy

Node names are fixed per host. Each transport's process epoch and relay grant identifiers come from
a seeded source, `SimulatedEntropy`, whose stream is named after the host. One seed therefore
allocates the same identities on every host in every process, while two hosts sharing the seed draw
different values. Fixtures choose their own delivery identities and acknowledgement identifiers,
such as the channel incarnation and ACK identifier of each relay case, so a trace can name them.

Certificates are generated for every run with fixed validity windows: 2027-01-01 to 2028-01-01 for
the transport fixtures, and windows placed relative to the epoch for the certificate-clock checks.
Validity decisions therefore replay while key bytes differ between runs. Rustls and AWS-LC keep
their real cryptographic randomness; it changes ciphertext and never a decision the trace records.

The transport's concurrent maps use per-process hash seeds. Every walk over one of them that causes
effects runs in semantic key order, as [Cluster Interconnect](./interconnect.md#simulation-boundary)
lists, so map order cannot reach a decision.

## The Runner

`SimulationConfig::run` builds one Turmoil simulation from one configuration, lets a setup closure
add hosts and clients, and steps it on a dedicated scheduler thread until every client has finished
or a bound expires.

### Configuration

Every run is built from exactly these inputs, and a failure record stores all of them.

| Input | Meaning |
| --- | --- |
| `seed` | Turmoil's random seed: link latency draws, the random host order within each step, and each host runtime's scheduling seed. Fixtures derive their entropy streams from it |
| `epoch` | The UTC instant simulated time starts at. It must not precede the Unix epoch |
| `topology` | IPv4 or IPv6 host addresses |
| `network.min_message_latency`, `network.max_message_latency` | The range each simulated message's delay is drawn from with Turmoil's exponential curve. Every scenario uses 0 to 100 ms |
| `network.fail_rate`, `network.repair_rate` | Turmoil's chance per step that a link starts or stops dropping messages. Every scenario uses 0 and 1, so links fail only through the scenario's written fault plan |
| `network.tcp_capacity` | Segments one simulated connection buffers before its sender waits. Every scenario uses 64 |
| `bounds.simulated_duration` | Simulated time after which the run fails |
| `bounds.tick` | Simulated time one step advances. Every scenario uses 1 ms |
| `bounds.max_steps` | Steps after which the run fails |
| `bounds.wall_duration` | Real time for the run and its cleanup together |

The runner validates the configuration before it starts a thread. A zero duration, an epoch before
the Unix epoch, a build without `tokio_unstable`, or a real-time bound too large for the machine's
clock each fail with their own error, naming the scenario and seed.

### Host Lifecycle And Supervision

A host is registered with its software, a closure that Turmoil calls when the host starts and again
each time the host is bounced. Each call builds the host's transport afresh. A client is a future
that runs once; the run completes when every client has returned. Scenarios therefore add an
`observer` client that waits until each host has reported that it finished, so hosts, which never
complete a run themselves, still bound it.

- `HostSupervisor::run` spawns a host's body as a task on the host's runtime and returns the body's
  error or panic as the host's result. A failed host fails the next step.
- A panic in any other task on a host shuts that host's runtime down, which fails the run. The
  runner reports the first panic raised on the scheduler thread, with its message and location,
  rather than Tokio's generic shutdown message. A panic that something caught, such as a supervisor
  or Tokio while dropping a task, is reported the same way.
- A host shuts down every transport it bound before its body returns, and checks the transport's
  connection counts after shutdown.
- `SimulationConfig::run_with_control`, used through `ScenarioRun::simulate_with_control`, calls a
  control closure after every step that did not end the run. A fixture uses it to crash, release,
  repair, and bounce hosts at a protocol milestone a host published, never at a guessed step count.
- When the steps complete, the simulation and every host runtime are dropped in a cleanup phase of
  their own, inside the same real-time deadline. A panic while dropping fails a run whose steps all
  succeeded.

### How A Run Ends

| Ending | How the run fails |
| --- | --- |
| A host or client returns an error | With that error |
| A panic on the scheduler thread | With the first panic's message and source location. This covers a panic in a task on a simulated host, which shuts that host's runtime down, and a panic Tokio catches while it drops a task, which would otherwise only be printed |
| The simulated duration or the step limit runs out | With the bound and the simulated time reached |
| The real-time bound expires | With the phase the scheduler was in, the steps it completed, and the simulated time they reached, which locates a step that blocked the scheduler thread |
| Cleanup panics after the run completed | As a cleanup failure, even though every step succeeded |

The real-time deadline is fixed before the scheduler thread starts and is measured outside it, so it
expires even when a host blocks that thread and simulated time cannot advance. The blocked thread is
then abandoned and ends with the process that runs the attempt. A cleanup failure that follows a
failed run is written to standard error beside the run's own failure. The real thread and wall clock
decide only when a test fails, never how a simulated request proceeds.

## Fault Model

A fault plan is written with Turmoil's link and host controls. Each control is an abstraction of a
network or host event, and a scenario's claim is limited to the abstraction it used.

| Control | Effect in Turmoil 0.7.2 | Used by |
| --- | --- | --- |
| `partition(a, b)` | Drops every message between two hosts in both directions | Partition before connect; partitioned authenticated exchange |
| `partition_oneway(from, to)` | Drops messages in one direction only | One-way partition before connect; relay reply loss; receiver restart after body receipt and after admission |
| `repair(a, b)`, `repair_oneway(from, to)` | Restores delivery. Dropped messages are not replayed | Every partition above |
| `hold(a, b)` | Queues messages between two hosts instead of delivering them | Held authenticated exchange; receiver restart with a delayed reply; stalled-peer teardown |
| `release(a, b)` | Delivers every held message at once | The same scenarios |
| `crash(host)` | Drops the host's runtime and every task on it, running ordinary destructors | Receiver restart |
| `bounce(host)` | Restarts a host by calling its software again | Receiver restart |

The model has consequences that every scenario respects:

- Simulated TCP does not retransmit. A message dropped by a partition is lost for good, so after a
  repair an established HTTP/2 session can be unusable. The partitioned-exchange scenario therefore
  shuts its listener down, rebinds it, and retires the client pool before reconnecting. No scenario
  infers kernel retransmission timing or packet-loss recovery.
- Holding and one-way partitions cannot be combined on one link, so scenarios use them separately.
- Turmoil 0.7.2 has no disconnect control. TCP closure is exercised by shutting a listener down or
  crashing a host.
- A request whose deadline expires while its messages are held stays expired when they are released.
- Turmoil's own socket count can keep failed connection attempts after a partition. Scenarios
  measure session cleanup with the transport's connection counts instead.
- Random link failures are disabled. A scenario's faults are its written plan, applied at named
  protocol points.

## Semantic Traces And Replay Rules

A run records a semantic trace: an ordered list of events, each with the simulated time it happened
at, the host that recorded it, and what was decided. Fixtures record events at decisions: a
connection became ready, a setup failed, a deadline expired, a relay was admitted or fenced, an
epoch changed, a resource was released. The same scenario, seed, and inputs must record the same
trace.

- An event names identities, admission decisions, and outcomes. It never contains key material,
  certificate bytes, ciphertext, or payload values, which differ between runs without changing a
  decision or do not belong in a record. Payload assertions compare values without printing them.
- An event never contains a value that describes the process rather than the simulation, such as a
  process identifier, a real time, a memory address, or an iteration over a hashed collection.
  `process_dependent_trace` is a deliberately divergent scenario that records its process
  identifier, and a regression test requires it to be recorded as diverged.
- Every committed seed runs twice, each time in a fresh process, and the two traces and outcomes
  must be identical. The first differing event, or the first event only one run has, is a divergence
  and fails the seed. A divergence that can affect behavior is a defect to fix, never a seed to
  drop.
- A replay compares its outcome and its trace with the record, event by event.

Two nondeterminism sources remain outside the trace by design. Rustls and AWS-LC randomness changes
ciphertext and key bytes but no decision. Tokio numbers tasks across the whole process, and a
runtime tears its tasks down in an order derived from those numbers, so when a simulated host
crashes, the order in which its connections close depends on every task the process created before.
Each attempt and each replay therefore runs in a fresh process, where that count starts from the
same state. Tokio's per-runtime scheduling seed is not a residual source: Turmoil derives it from
the simulation seed because the build sets `tokio_unstable`.

## Scenarios, Seeds, And Attempts

A scenario is one case of a test: a name unique within the test, a written fault plan, and its
committed regression seeds. `Scenario::check` takes a function that builds a seed's configuration
and a function that exercises it, and runs them in the mode the environment selects.

| Variable | Mode |
| --- | --- |
| None | Run every committed seed of every case twice, each in a fresh process |
| `NERVIX_TURMOIL_SWEEP=<first>..<end>` | Replace every case's committed seeds with that range, end exclusive. Everything else is unchanged |
| `NERVIX_TURMOIL_INJECT_FAILURE=<duration>` | Add a client named `injected-failure` that fails at that simulated time, such as `5s`. The injection is a recorded input |
| `NERVIX_TURMOIL_REPLAY=<record>` | Run only the recorded case, once, with exactly the recorded inputs. It refuses to run beside a sweep or an injection |
| `NERVIX_TURMOIL_FAILURES=<directory>` | Where records are written. Unset, they go under Cargo's `CARGO_TARGET_TMPDIR`; the recipes set `target/turmoil-failures` |

The driver sets a fifth variable, `NERVIX_TURMOIL_ATTEMPT`, on the process it starts for one
attempt, and clears the other four there.

### Seed Selection

Committed seeds are small and deliberate: one or a few per case, distinct across cases, and listed
beside the fault plan they exercise. A case's committed set grows only by a seed that exposed a
defect, committed with its fix, so the regression set records what the simulation has found. The
sweep explores beyond them: `just test-turmoil-sweep` runs sixty-four consecutive seeds from 1000
through every case by default. A seed that fails in a sweep leaves an ordinary record, and once the
defect it found is fixed, it joins the committed seeds of its case. Seed 1036 of the receiver
restart with a delayed relay response is the first such seed.

### Fresh-Process Attempts

For each seed, the driver writes a record marked running, then starts the test binary again with the
exact test name, `--exact --include-ignored --test-threads=1 --nocapture`, and the attempt's
request. That process runs the case once and writes back its outcome and trace. The driver waits for
the run's real-time bound plus 30 seconds, then kills the process. A process that ends without
writing its result yields an unreported outcome holding its ending and the last 40 lines of its
output. A process that passes without running the requested case is a driver error, because the test
and case names no longer match. After both attempts pass and agree, the record is removed.

## Failure Records

A failed, panicked, diverged, or unreported seed leaves a JSON record at
`<failures>/<test>/<case>-seed-<seed>.json`, where the test and case are reduced to lowercase
letters and digits joined by dashes. The test then fails with a summary of the trace and the command
that replays the record.

| Field | Contents |
| --- | --- |
| `scenario` | The Cargo `package`, the test `target`, the libtest `test` name, the `case` within it, and its `fault_plan` |
| `build` | `source`, the checked-out commit and whether tracked files were modified; `lockfile_sha256` of `Cargo.lock`; `toolchain`, the output of `rustc -vV`; and whether `tokio_unstable` and `debug_assertions` were compiled in |
| `inputs` | The complete configuration, from seed to real-time bound, and any injected failure |
| `run` | `first`, or `repeat` when the second attempt failed after the first passed |
| `outcome` | Its `kind` and detail, as below |
| `events` | The semantic trace of that run, as far as it got |

| Outcome kind | Meaning |
| --- | --- |
| `running` | The seed had started when its process ended from outside, such as at the suite's real-time budget. The record still holds its inputs |
| `failed` | The runner returned an error: a failed host or client, an exhausted bound, or a scheduler panic |
| `panicked` | The scenario panicked on the test thread around the simulation, such as in a check after the run |
| `diverged` | Both runs passed with different traces; the record names the first differing event of each run |
| `unreported` | The attempt's process aborted, or outlived its bound and was killed, without reporting |

A record holds identities, inputs, outcomes, and semantic events only. It never holds certificates,
key material, ciphertext, or payload values, so it is safe to attach to an issue or a CI artifact.

## Commands

Every recipe builds with the repository's kache compiler wrapper and passes `--cfg tokio_unstable`
itself.

| Command | What it runs | Bound and exit |
| --- | --- | --- |
| `just test-turmoil [budget_seconds]` | The `nervix-execution` library under the `turmoil` feature, the interconnect library's `wire::simulation_checks` and `authentication::simulation_tests`, and every case of the `simulation` target over its committed seeds | Builds first, then runs inside a real-time budget of 480 seconds by default; exits `124` naming the in-progress records when it expires |
| `just test-turmoil-simulation [args]` | The `simulation` target alone, with libtest arguments, such as a test name and `--exact` | The per-run bounds only |
| `just test-turmoil-replay <record>` | Exactly the recorded package, target, test, and case, once, with the recorded inputs, in a fresh process | Exits nonzero when the replay fails, including when it reproduces the record, and when no case in the recorded test matches |
| `just test-turmoil-replay-check` | Injects a failure at five simulated seconds into the partition-before-connect case, requires exactly one record, and requires its replay to report `turmoil replay: reproduced the recorded outcome and trace` | Fails when any of those does not hold |
| `just test-turmoil-sweep [first] [count] [budget_seconds]` | Every case over `count` seeds from `first`, 64 from 1000 by default, each seed twice in fresh processes | A real-time budget of 1,500 seconds by default; exits `124` when it expires |
| `just coverage-turmoil <output>` | The same tests as `just test-turmoil` under `cargo llvm-cov`, writing an LCOV report | None beyond the per-run bounds |

The replay prints a verdict for every run:

| Line | Meaning |
| --- | --- |
| `build differs: …` | The recorded source, lockfile, toolchain, or build flags differ from this build. Any of them can legitimately change a run |
| `fault plan differs: …` | The case's written plan changed since the record was made |
| `trace: all <n> recorded events replayed identically` | Every recorded event recurred in order |
| `trace diverges at event <i>: …` | The first event that differs, from each side |
| `turmoil replay: reproduced the recorded outcome and trace` | The same outcome and trace; the failure reproduces |
| `turmoil replay: the recorded failure did not reproduce` | The replay passed, which is how a fix is confirmed |
| `turmoil replay: the replay failed differently from the record` | The replay failed with another outcome or trace |

The record and replay path is checked itself.
`injected_failure_is_recorded_and_replayed_in_a_fresh_process` injects a failure into a scenario in
a fresh process, requires exactly one record, replays it in a second fresh process, and requires the
recorded outcome and trace. `diverged_and_panicked_runs_leave_records` starts two deliberately
failing scenarios, one whose trace names the process that ran it and one whose check after the
simulation fails, and requires a diverged and a panicked record. `just test-turmoil-replay-check`
does the same through the documented commands. The suite still contains no failing test: the
injection exists only in the environment of the processes those checks start, and the failing
scenarios are ignored unless a check starts them.

## An Example Investigation

The failure below was produced on purpose with the recorded injection, which exercises the same path
a real failure takes. The output is quoted from that run, with paths shortened to the repository
root.

```bash
NERVIX_TURMOIL_INJECT_FAILURE=5s just test-turmoil-simulation \
    transport::network_disruption_respects_deadlines_and_repairs_authenticated_service --exact
```

1. **Read the failure.** The test fails with the case, seed, run ordinal, outcome, trace, and the
   replay command:

   ```text
   simulation transport::network_disruption_respects_deadlines_and_repairs_authenticated_service / "partition before connect" seed 61 (first run) failed: simulation partition before connect seed 61: host or client failed: injected harness failure at 5s simulated time
     0.001s client PartitionBeforeConnect installed; peer registered
   failure record: target/turmoil-failures/transport-network-disruption-respects-deadlines-and-repairs-authenticated-service/partition-before-connect-seed-61.json
   replay it in a fresh process: just test-turmoil-replay target/turmoil-failures/transport-network-disruption-respects-deadlines-and-repairs-authenticated-service/partition-before-connect-seed-61.json
   ```

   In CI the same record is in the job's `turmoil-failures` artifact. Its `run` field says whether
   the first attempt failed or only the repeat did. A `repeat` failure or a `diverged` outcome
   points at nondeterminism before it points at the product.

2. **Compare the build.** The record's `build` names the commit, lockfile hash, and toolchain. Check
   out that commit when the current branch has moved; the replay reports every remaining difference.

3. **Replay it.**

   ```bash
   just test-turmoil-replay target/turmoil-failures/transport-network-disruption-respects-deadlines-and-repairs-authenticated-service/partition-before-connect-seed-61.json
   ```

   ```text
   recorded outcome (first run): failed: simulation partition before connect seed 61: host or client failed: injected harness failure at 5s simulated time
   replayed outcome: failed: simulation partition before connect seed 61: host or client failed: injected harness failure at 5s simulated time
   trace: all 1 recorded events replayed identically
     0.001s client PartitionBeforeConnect installed; peer registered
   turmoil replay: reproduced the recorded outcome and trace
   ```

   The command exits with a failure status because the failure reproduced. The trace ends at the
   last decision before the failure: the client registered its peer 1 ms into the run, and the
   injected client failed at 5 seconds, before the twelve-second partition ended.

4. **Narrow it.** Read the case's fault plan and the fixture's events around the last recorded one.
   To see what happened between two events, add a temporary `tracing` subscriber that stamps lines
   with `turmoil::sim_elapsed()` and prints the lengths of byte arrays rather than their contents,
   then rerun the replay. Compare two runs of a divergence line by line from the first differing
   event.

5. **Fix it at its owner.** A product defect gets its fix and regression at the owning boundary,
   with a Cucumber scenario when the behavior is public. A harness defect gets its fix in the runner
   or driver. Replay the record again until it prints
   `turmoil replay: the recorded failure did not reproduce`, commit the seed to its case's committed
   seeds, and run `just test-turmoil`.

Seed 1036 is the recorded history of this procedure. The first sweep found that its two runs of the
receiver restart with a delayed relay response diverged after the simulated crash while they shared
one process. Tracing both runs with simulated timestamps showed that the crashed receiver's
connections closed in a different order, and that the order followed Tokio's process-wide task
numbering. The driver moved every attempt and replay into a fresh process, and seed 1036 became a
committed seed of that case.

## CI And Budgets

CI runs the simulation as a job of its own, `turmoil`, beside the default tests and the
`extra-tests` job that runs the Shuttle checks. Its build enables the `turmoil` feature and
`tokio_unstable`, so it shares no compilation with them, and a failed case's record is its own
artifact. The job runs `just test-turmoil`, then `just test-turmoil-replay-check`, and on failure
uploads `target/turmoil-failures` as the `turmoil-failures` artifact. The coverage job does not run
the simulation; `just coverage-turmoil` measures its lines locally.

Every bound nests inside the next, so the innermost expired bound names the stuck run, and the job
still has time to upload what it found. This follows the convention of [The Suite
Watchdog](./integration-test-lifecycle.md#the-suite-watchdog).

| Bound | Value | Expiry |
| --- | --- | --- |
| Simulated duration per run | 1, 30, 60, 90, or 120 seconds by case | The run fails with the bound and the simulated time reached |
| Scheduler steps per run | 200, 50,000, or 120,000 by case | The run fails with the bound and the simulated time reached |
| Real time per run | 3, 90, or 120 seconds by case | The run fails naming its phase, steps, and simulated time |
| Real time per attempt process | The run's real-time bound plus 30 seconds | The driver kills the process and records an unreported outcome |
| `just test-turmoil` tests | 480 seconds after the build | Exit `124`; in-progress records name the unfinished runs |
| `just test-turmoil-sweep` | 1,500 seconds | Exit `124` |
| CI `turmoil` job | 30 minutes | The emergency guard around the build, the tests, and the upload |

The sweep's 25 minutes is three times the 8m11s its default 64 seeds took after the build, one
attempt at a time, on the 32-thread workstation where it was introduced.

Measured wall times for the qualified revision:

| Run | Wall time |
| --- | --- |
| `just test-turmoil` locally, including compilation | 1m31s |
| `just test-turmoil-sweep 1000 64` locally | 11m02s, with a largest process of 131,060 KiB |
| CI `turmoil` job, including build and replay check | 3m21s and 3m18s on the last two qualifying changes |
| CI Shuttle stage of `extra-tests` | 9m03s of a job that took 27m33s inside its 45 minutes |
| CI `tests` job | 53m03s inside its 60 minutes |

## Scenario Matrix

Every case below is a `Scenario` in the `simulation` target. An ordinary run executes its committed
seeds twice each, in fresh processes; the sweep replaces the seeds without weakening any assertion.

| Case | Test | Committed seeds | Simulated / steps / real | Main assertion |
| --- | --- | --- | --- | --- |
| peer resolution | `peer_resolution_uses_simulated_dns` | 41 | 1 s / 200 / 3 s | A peer endpoint resolves to its simulated address through Turmoil DNS |
| bounded CPU worker | `bounded_cpu_job_runs_on_the_simulated_scheduler` | 1–12 | 1 s / 200 / 3 s | Every CPU and memory class runs its job on the scheduler thread, and every queue and reservation returns to zero |
| typed Arrow exchange | `transport::production_transport_exchanges_typed_arrow_batch_over_simulated_tcp` | 49 | 30 s / 50,000 / 90 s | After a listener rebind, production TLS, HTTP/2, typed envelopes, and Arrow IPC carry a typed request and a relay payload |
| multiple authenticated peers | `transport::multiple_peers_and_invalid_authentication_use_production_transport_contract` | 57 | 30 s / 50,000 / 90 s | One client resolves and exchanges with two peers, and a dial under a DNS identity the certificate does not name is rejected |
| partition before connect | `transport::network_disruption_respects_deadlines_and_repairs_authenticated_service` | 61 | 60 s / 50,000 / 90 s | Setup fails and liveness times out through twelve seconds of partition; repair restores authenticated service |
| one-way partition before connect | The same | 62 | 60 s / 50,000 / 90 s | The same, with only server-to-client messages dropped |
| held authenticated exchange | The same | 63 | 60 s / 50,000 / 90 s | A one-second liveness probe and a two-second request expire while held; release restores service |
| partitioned authenticated exchange | The same | 64 | 60 s / 50,000 / 90 s | Both deadlines expire; after repair, a rebound listener and a retired pool reconnect |
| relay response lost after admission | `transport::relay::relay_reconciliation_and_cancellation_survive_lost_replies` | 71 | 60 s / 50,000 / 90 s | A retry to the same receiver process returns the admitted outcome and ACK without a second enqueue, and a late cancellation returns admitted |
| relay cancellation before grant | The same | 72 | 60 s / 50,000 / 90 s | A cancellation that reaches the receiver first fences the delivery |
| relay cancellation while reply is lost | The same | 73 | 60 s / 50,000 / 90 s | Cancellation wins the admission fence while the body reply is lost |
| relay cancellation during reconnect | The same | 74 | 60 s / 50,000 / 90 s | Cancellation wins the admission fence while the sender reconnects |
| receiver restart after relay body receipt | `transport::relay::relay_restart_fences_unresolved_delivery_and_accepts_fresh_work` | 81 | 90 s / 50,000 / 90 s | A new process epoch leaves the unresolved delivery indeterminate and admits fresh work |
| receiver restart after runtime admission | The same | 83 | 90 s / 50,000 / 90 s | The same after runtime admission |
| receiver restart with delayed relay response | The same | 85, 1036 | 90 s / 50,000 / 90 s | A released pre-crash reply confirms only historical receipt; admission stays indeterminate against the new epoch |
| stalled peer isolation | `transport::isolation::stalled_peer_cannot_consume_unrelated_capacity_or_leak_reservations` | 91–93 | 120 s / 120,000 / 120 s | A stalled peer holds only its own connection's slots, unrelated work progresses, and every reservation is released |

The simulation target also holds 26 runner and driver checks, such as the supervision, bound, clock,
entropy, and trace checks and the fresh-process record and replay checks, and two ignored scenarios
that only those checks start. The library-level checks use fixed seeds outside the driver and are
rerun by name: the execution strategy's queue, admission-order, and charge checks; the seeded rkyv
round trips of seeds 1–12, alternating IPv4 and IPv6; and twelve certificate-clock checks, one of
which compares the traces of two fresh processes.

### Authenticated Exchange

The exchange cases prove that the simulated path is the production path. A server binds, shuts down,
and rebinds its listener in the same host, so listener cleanup is observable. The client then sends
a typed request carrying an Arrow batch and the same batch as a relay payload, and each side decodes
it with the production codec and checks the values without printing them. The three-host case adds a
second peer and a rejected dial whose DNS name the certificate does not carry, so authentication
failure is proven over the same connection path that succeeds.

The certificate-clock checks, beside the transport, prove that time-dependent authentication follows
the simulated clock on both peers: not-yet-valid and expired boundaries of the local and the peer
certificate, rejection by the client's and by the server's Rustls verifier under skew, the drain of
an accepted session at the earlier expiry on both sides, and the setup deadline of a stalled
handshake.

### Link Faults

The two setup partitions stay in place for twelve seconds of simulated time, so bounded reconnect
behavior is observable: setup failures are counted, liveness times out, and no connection appears.
The two established-link faults first complete an authenticated exchange, then expire a one-second
liveness probe and a two-second typed request, and require at least three seconds of simulated time
to have passed. Transport readiness and authenticated liveness are checked before each disruption;
after each repair, the client reconnects, passes liveness, and completes another exchange. Snapshots
bound open connections, pending requests, reconnect failures, and simulated sockets throughout, and
the transport's connection counts are checked after shutdown.

### Relay Reconciliation And Cancellation

The relay cases drop the receiver-to-sender reply once an Arrow batch has entered the receiver's
queue, then reconnect to the same receiver process. The retained attempt reconciles to its admitted
outcome and semantic ACK, and the application sees one enqueue. Cancellation competes with admission
through the attempt's one fence: before the grant, while the reply is lost, and while reconnecting,
cancellation wins; after admission, it returns the admitted outcome. The checks hold within the
receiver's retention contract in [Ordering, Retry, And
Reconciliation](./interconnect.md#ordering-retry-and-reconciliation). They say nothing about a
receiver restart, which the next group covers.

### Receiver Restart

The restart cases lose the reply at one of two milestones, after the Arrow body entered the
receiver's queue and after runtime admission, or hold an admitted attempt's reply. At the milestone
the receiver host publishes, the control closure crashes the receiver, repairs the one-way partition
or releases the held reply, and bounces the host. The bounced host binds a new transport under the
same node name with a distinct process epoch from its seeded entropy. The sender's retained delivery
resolves as indeterminate against that epoch and never enters the restarted receiver's queue; a
released pre-crash reply can confirm only that the old process received the body. A new delivery
identity then crosses the rebound listener and is admitted. The fixture checks the Arrow batch and
both process identities at the protocol boundary, and that the receiver crashed and restarted
exactly once.

### Stalled-Peer Isolation

A hub and two peers run on three hosts. The stalled peer accepts shared management operations and
never answers, leaves resource streams unread, and sends a relay batch the hub holds without
admitting. The healthy peer keeps exchanging shared management work, liveness, typed Arrow commands,
whole resource streams, and admitted relay batches with the hub throughout. Every observation of a
host checks its pool connections per peer, leased streams per connection, admissions per direction
and subquota, worker queues, and memory budgets against their configured bounds.

Key transitions assert exact values. The stalled peer holds all 32 shared management streams of its
connection while liveness, cancellation, and discovery still use the reserved streams, and one more
shared operation waits until its own deadline. A burst fills the one bulk worker and its eight-job
queue, the two submissions beyond them are refused without a charge, and control work is admitted in
the same pass. Unread streams stop at one HTTP/2 window each and release their admission and memory
at the five-second progress deadline. The held relay reserves its exact body, largest decoded batch,
and scratch until cancellation.

The fixture then holds the stalled link. The hub's own liveness deadline ends its probe to the
stalled peer while the healthy peer keeps exchanging every traffic class. Removing the stalled peer
from membership ends the hub's operations to it and closes the hub's pools to it at once, without
that peer's cooperation. Tearing the stalled host down while the link is still held closes the
connections the hub opened to it at once, together with the handlers still waiting to answer, and
releasing the link closes the hub's inbound connections from the torn-down peer. The [Traffic And
Resource Isolation](./interconnect.md#traffic-and-resource-isolation) contract is what these values
check.

## Simulated Restart And Real Process Crash

`Sim::crash` cancels the crashed host's Tokio tasks and drops its runtime. Destructors run, and the
host's connections close in the order its tasks are torn down, which Tokio's task numbering sets.
`Sim::bounce` calls the host's software again, which rebinds the listener and allocates a new
process epoch. The simulated hosts have no filesystem and no background thread of their own.

A simulated restart therefore establishes the transport's volatile contract: a new process epoch
fences in-memory delivery state, an unresolved delivery becomes indeterminate, and the transport
never replays it into the new process. It establishes nothing about fsync, power loss, SIGKILL,
partial writes, consensus or WASM checkpoint durability, or full-node recovery.

Those claims keep their real-process evidence.
`tests/features/cluster/process_crash_recovery.feature` starts a server process and kills it with
SIGKILL before checking recovery, and the [Shutdown And Recovery](./shutdown.md), [Consensus Storage
And Replication](./consensus-storage-and-replication.md), and [WASM State And
Recovery](./wasm-state.md) chapters own the durable contracts it checks. A Turmoil host bounce is
never cited as durability evidence.

## Adding A Bounded Scenario

A new case follows the same contract as the existing ones.

1. **Name the claim.** Write the interconnect contract the case checks and the fault that challenges
   it. A publicly observable outcome also needs its Cucumber scenario; the simulation adds fault
   timing, not public coverage.
2. **Place it with its owner.** Add it to the fixture module whose contract it checks, or to a new
   module declared from `transport.rs` with its own ownership header.
3. **Declare the scenario.** Give it a name unique within its test, a fault plan that says which
   controls it applies and when, and committed seeds that no other case uses.
4. **Bound it.** Start from the fixture's configuration and set the smallest simulated duration,
   step limit, and real-time bound that complete the plan with room to spare. Keep the network
   parameters lossless, so the written plan is the only fault.
5. **Build each host inside its host.** Bind a transport per host with `SimulatedEntropy` named
   after the host and a `SimulatedUtc` clock. Share only readiness and completion signals between
   hosts, run each host body under `HostSupervisor::run`, and have the `observer` client wait for
   every host to finish.
6. **Stay inside the simulated boundary.** Use Tokio time for every wait, and call
   `tokio::task::consume_budget().await` at the top of every loop. Never sleep a real thread, read
   the wall clock, open an operating-system socket, touch the filesystem, or submit storage or
   unbounded CPU work.
7. **Inject faults at protocol points.** Apply link controls from inside a host when the host
   reaches a decision, or apply host controls from the control closure when a host publishes a
   milestone.
8. **Record decisions.** Record an event at every decision the case asserts, with identities and
   outcomes only, never payloads, key material, process identifiers, or map-ordered listings.
9. **Prove release.** Shut every transport down before its host exits, assert its connection counts
   afterwards, and check every pool, queue, and reservation the plan touches against its bound.
10. **Run it.** Iterate with `just test-turmoil-simulation <test> --exact`, run
    `just test-turmoil-sweep` to look for divergence across seeds, then `just test-turmoil` and
    `just validate`.
11. **Keep this chapter current** in the same change: the scenario matrix, its bounds, and any new
    limit.

## Findings And Retained Regressions

The simulation changed product code only to remove nondeterminism sources that reached a decision.
Its other findings are recorded here and in their owning chapters as current behavior. The
qualification run found no further product defect and no behavior-affecting trace difference.

| Finding | Resolution | Retained regression |
| --- | --- | --- |
| Each certificate check read the process wall clock on its own: both Rustls verifiers, the transport's local and peer checks, and the expiry drain deadline | Each TLS bundle carries one `TransportClock`, which every check reads | The certificate-clock checks in `authentication::simulation_tests` |
| The process epoch and relay grant identifiers came straight from operating-system randomness | `TransportEntropy` supplies them, from the operating system in production and from a seeded source in the simulation | The fresh-process trace comparison in the certificate-clock checks, and every committed seed |
| Walks over concurrent maps that cause effects followed per-process hash order | Those walks run in semantic key order | Every committed seed |
| Transport shutdown closes the connections peers opened, with the handlers serving them, at once rather than draining them | The behavior is stated in [Cluster Interconnect](./interconnect.md) and [Shutdown And Recovery](./shutdown.md) | The stalled-peer teardown |
| A CPU queue refusal reaches a typed-request caller as `RequestError::Encode` when the first encode is refused, but as `RequestError::Transport` when an admitted request is refused at a later stage | The stalled-peer case asserts both shapes and counts every failure as exactly one queue refusal | The stalled-peer case |
| Two runs of one seed in one process diverged after a simulated crash, because teardown order follows Tokio's process-wide task numbering | Every attempt and every replay runs in a fresh process | Seed 1036 of the receiver restart with a delayed relay response |
| A panic in a host task surfaced only as Tokio's generic runtime-shutdown message | The runner reports the first panic of the scheduler thread, and supervises cleanup as its own phase | `simulation_host_task_panic_reports_its_own_message`, `simulation_cleanup_panic_fails_a_completed_run` |

## Guarantees And Limits

The simulation guarantees, for the audited path:

- The production transport runs over Turmoil TCP and DNS only, and no scenario reaches the host
  network.
- Transport deadlines follow the simulated clock, certificate validity follows the supplied UTC
  clock, and neither falls back to the host clock.
- One scenario, seed, and set of inputs makes the same recorded decisions, at the same simulated
  times, in every fresh process of the same build on the same host configuration.
- Every failure leaves a record that replays the same inputs in a fresh process and reports whether
  the failure reproduced.
- Every run, attempt, suite, and sweep ends inside a real-time bound, including one whose host
  blocks the scheduler thread.

The audit behind those guarantees covered the socket alias and simulated resolver, the transport
identities and certificate clocks, request and relay deadlines, the map walks with side effects, the
bounded CPU executor, and the runner. Its limits:

- The runner's real thread and wall clock decide only when a test fails, never how a request
  proceeds.
- Execution storage jobs use a real blocking thread and are outside every case.
- Default worker counts follow the host's CPU count, so a replay is established for the tested host
  configuration, not independently of host capacity.
- Rustls and AWS-LC keep their randomness, so traces compare authenticated decisions, never
  ciphertext.
- The TCP model neither retransmits nor models kernel timing, and Turmoil 0.7.2 has no disconnect
  control.
- A simulated host crash is not evidence of durability, as described above.

### Prerequisites For Whole-Cluster Simulation

Running whole Nervix nodes under Turmoil needs more than substituting TCP. Each of these is an open
prerequisite, not part of this simulation's claim:

- **Raft and gossip entropy.** OpenRaft election timing and Chitchat gossip draw their own
  randomness. Both would need a seeded owner without changing production entropy.
- **Storage and crash semantics.** Fjall runs background threads and writes real files. A simulated
  store would need defined filesystem, fsync, and crash behavior, kept apart from the real-process
  durability evidence.
- **Host attribution.** Work a node spawns, including blocking tasks, timers, DNS queries, and
  sockets, must run on the host that owns it; code that escapes to another runtime or thread escapes
  the simulation.
- **Domain time.** Domain-clock authority, generations, and deadlines would need controlled time
  that preserves the [Domain Clock](./domain-clock.md) contracts.
- **Connectors.** Every connector's I/O, acknowledgement, and commit boundary would need to be
  classified as a simulated fixture or a real-process test.
- **Host-independent capacities.** Execution worker counts would need fixed values, so admission
  traces stop depending on the host's CPU count.

Combining Shuttle's scheduler with Turmoil's network is a separate question. Shuttle owns the
interleaving of in-process tasks and Turmoil owns a runtime per host; the two scheduler modes are
mutually exclusive by build. Driving Shuttle over a simulated network would need a network substrate
integrated with Shuttle's own scheduler, such as `turmoil-net`, and is not part of either suite.

## Qualification Evidence

The simulation was qualified on 25 September 2026 at revision `7032bca2`, which also contains the
final Shuttle qualification. The results below are from that run, except the CI row, which is from
the change that recorded it.

| Run | Result |
| --- | --- |
| `just test-turmoil` | Passed in 1m31s including compilation: 14 execution tests, 12 interconnect library checks, and 34 simulation tests, with the ignored fresh-process probes started by their own checks |
| `just test-turmoil-sweep 1000 64` | Passed in 11m02s, inside its 25-minute budget: all 16 cases, each of 64 seeds twice in fresh processes, 2,048 attempts with identical outcomes and traces. The largest process used 131,060 KiB |
| `just test-turmoil-replay-check` | The injected failure left one record for the partition-before-connect case, seed 61, and a fresh process reproduced its outcome and trace |
| `just test-shuttle` | All 70 checks passed in ordinary mode and under the uncontrolled-nondeterminism detector, 140 invocations |
| `just test-interconnect`, `just test-execution` | 44 and 11 tests passed in the normal build |
| `just validate`, `just ratchet` | Passed, including the dependency and scheduler-conflict checks, with unchanged debt counts |
| Real authentication and transport, one and three nodes | `internal_tls`, `interconnect_health`, `interconnect_lifetime`, and `interconnect_observability`: 19 scenarios and 159 steps passed |
| Real-process crash recovery | `process_crash_recovery`: 2 scenarios and 57 steps passed with SIGKILL |
| CI | The `turmoil` job passed in 3m18s, the Shuttle stage in 9m03s, and the `tests` job in 53m03s |

No product defect and no behavior-affecting trace difference was found in the qualification. Seed
1036 remains a committed regression, and no failing seed was dropped.

The commands in this chapter were run again on `d2cec5f2`, the revision it documents, on a 32-thread
workstation. `just test-turmoil` passed in 1m01s with a warm build cache,
`just test-turmoil-replay-check` passed in 2 seconds, and `just test-turmoil-sweep` passed all 16
cases over its 64 seeds in 7m05s, leaving no failure record, with a largest process of 130,764 KiB.
The example investigation above was recorded and replayed on the same revision.
