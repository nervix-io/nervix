# Integration Test Lifecycle

The Cucumber scenario suite runs Nervix the way an operator does. It starts clusters, drives them
through the session API, brokers, and HTTP endpoints, and asserts on what they do. Many scenarios
run at once in one test process, each with nodes of its own, so the harness around them is a
concurrent system with failure modes of its own: a node that never becomes ready, a status request
that never gets a reply, a node that never stops, a step that never returns.

This chapter defines the harness's lifecycle contract: how a node, a scenario, and the whole run
start, how the harness observes them, how it stops them, and which deadline bounds each of those
steps. Three rules hold throughout.

- Every lifecycle operation has one owner and one absolute deadline, fixed when its phase starts. A
  retry, a poll, or a cleanup inside the phase receives only what is left of it.
- Every ending has a typed outcome. A node task is running or ended in one of four ways, a readiness
  probe reports what it observed, and a scenario publishes the phase it is in. No harness path
  reports an unknown reason.
- The run ends itself before the CI job limit and leaves the job time to upload the diagnostics it
  produced.

The harness is test code outside the layer order: it may name any layer, and no product code names
it. It observes product behavior and never redefines it. Product deadlines stay with the chapters
that own them: node shutdown with [Shutdown And Recovery](./shutdown.md), node-to-node communication
with [Cluster Interconnect](./interconnect.md), and client requests, redirects, reconnects, and
command identity with [Sessions](./sessions.md), the [Rust Client Library](./client-library.md), and
[Command Completion](./command-completion.md).

## What Runs Where

The suite is the `scenarios` test target, `tests/scenarios.rs`, running the features under
`tests/features`. Its lifecycle owners live in `tests/common`.

| Element | Where it runs | Owner |
| --- | --- | --- |
| Scenario steps and hooks | One runner task: Cucumber polls every running scenario, and the suite watchdog around them, from the task the binary's main thread blocks on | `tests/scenarios.rs` |
| In-process nodes | One Tokio task per node on the binary's multi-threaded runtime, which has one worker thread per CPU | The cluster fixture, `tests/common/cluster.rs` |
| Server processes | Child processes executing the `nervix-server` binary | The server-process fixture, `tests/common/server_process.rs` |
| CLI sessions | Child processes executing `nervix-cli`; a scenario reader retains at most 256 output lines | The scenario world, `tests/scenarios.rs` |
| Test dependencies | Containers started on first use and shared by every scenario of the run | `nervix-test-environment`, through `tests/common/dependencies.rs` |
| HTTP receivers | Tasks on the binary's runtime, one listener and one task per connection, owned by the scenario that started them | The HTTP receiver fixture, `tests/common/http_receiver.rs` |
| Client probes | A child process per probe of another language, or one blocking task for the in-process probe of the shared Rust binding, owned by the scenario that started it | The client probe fixture, `tests/common/client_conformance.rs` |

`tests-deps` builds the CLI and NSPL formatter in the normal target directory. The full and focused
client coverage recipes place those executables beside their instrumented server binary, where the
scenario runner resolves child processes. The child processes exercise the public interfaces; the
focused recipe measures their changed lines with instrumented binary unit tests.

The number of scenarios that run at once is the number of CPUs times the concurrency factor, set by
`NERVIX_TEST_CONCURRENCY_FACTOR` or `--concurrency-factor` and `1` by default. Cucumber's
`--concurrency` sets an absolute number instead. The CI `tests` job sets the factor to `2`, which is
32 concurrent scenarios on its 16-CPU runner. Three limits apply beneath that number: a scenario
tagged `@exclusive` runs alone, at most one scenario of the coordinated WASM state-reset feature
runs at a time, and at most two web console scenarios run at a time. Cucumber retries a failed
scenario twice; `--retry 0` turns retries off for a focused run.

## Product Deadlines And Harness Deadlines

Two kinds of deadline meet in every scenario.

A **product deadline** is configuration a node enforces on itself: its shutdown and drain timeouts,
its command retry validity and transaction idle timeout, its node-unavailability and election
timeouts. Test clusters start with test values for some of them. A scenario about one of them
configures it, drives the node to it, and asserts the outcome the owning chapter defines. No harness
deadline shortens a product deadline while the scenario can still observe it.

A **harness deadline** bounds how long the harness waits: for a status reply, for a node to become
ready, for a cluster to stop, for the whole run. Its expiry changes nothing inside a node. It ends a
wait with a typed failure, takes apart a task the harness owns, or ends the run. Aborting a node
task is containment rather than a product outcome, and the harness records it as forced cleanup, not
as something the node did.

The boundary between them is kept in four places.

- **Ordinary commands.** The NSPL commands a scenario runs go through the production Rust client,
  with its execution identity, redirects, and reconnects. No harness deadline shortens them. Only
  the status path described below is harness-owned, and it is a separate test-only boundary: it
  opens its own session and never redirects, reconnects, or retries by itself, so it adds no second
  client policy.
- **Cleanup.** Scenario cleanup runs after the scenario's assertions, so it spends the harness
  cleanup budget rather than waiting for a product shutdown deadline. A scenario whose subject is
  shutdown, drain, or deadline expiry stops its nodes itself, inside its body, under the product
  deadlines it configured.
- **How nodes stop.** The harness stops an in-process node through its shutdown coordinator, with
  the same stop request a first `SIGTERM` makes to a server process, and the node runs its shutdown
  phases under its configured deadline. Registering termination signals, the forced-exit supervisor,
  and exit statuses belong to the process boundary, which only the server-process fixture crosses.
  See [Shutdown And Recovery](./shutdown.md#stop-requests).
- **Test defaults.** An in-process node's shutdown timeout defaults to four minutes rather than the
  product's `50s`, which leaves the bounded shutdown phases scenarios configure by default room to
  finish, so only a scenario about the deadline reaches it. A server process runs with the product
  defaults unless its scenario passes an option such as `--shutdown-timeout`.

## Deadline Ownership

Every lifecycle operation of the harness is bounded by exactly one of these deadlines. Each budget
is a constant in the module that owns it, under `tests/common` unless named otherwise; where a
second module runs the operation, it is named after the owner.

| Operation | Owner | Deadline | When it passes |
| --- | --- | --- | --- |
| One status request: connect, open a session, send, receive | `status_request.rs` | 10 seconds, or what is left of the enclosing phase when that is less | The request fails naming the operation that was still pending |
| A status wait: leadership, voters, a consistent leader, interconnect status, applied index, a status fragment | `status_request.rs`, run by `cluster.rs` | 40 seconds from the start of the wait | The step fails with the elapsed time, the last status text, and the last typed failure |
| Status snapshots of every node, for teardown or for a failure message | `status_request.rs`, run by `cluster.rs` | 10 seconds for the whole cluster | Each node keeps its status, its typed failure, or the operation its request was in |
| Readiness of one startup attempt | `node_startup.rs`, run by `node_liveness.rs` | 36 seconds, inside the node's startup budget | The attempt fails naming the node, attempt, elapsed time, task state, and last readiness outcome |
| Cleanup after a failed startup attempt | `node_startup.rs` | 5 seconds, inside what the startup budget has left | The node task is aborted and joined |
| One node's startup, every attempt included | `node_startup.rs` | 84 seconds | The startup fails with the typed record of every attempt |
| The node startups of one cluster construction | `node_startup.rs`, run by `cluster.rs` | 84 seconds per node, shared by the whole construction | The node that ran out ends the construction |
| A node a scenario stops itself | `cluster.rs` | The longest of five minutes, the configured shutdown timeout, and the configured drain phases | The task is aborted and joined, and the step fails |
| Scenario cleanup of a whole cluster | `cluster_teardown.rs` | 60 seconds for every node together | Still-running tasks are aborted and joined and recorded as forced |
| Stopping a scenario's HTTP receivers | `http_receiver.rs`, run by `tests/scenarios.rs` | 6 seconds for every receiver together: 5 for its connections, 1 for its accept loop | Still-running connections, then the accept loop, are aborted and joined and recorded as forced |
| An HTTP receiver wait: captured requests or a recorded fault | `http_receiver.rs`, run by `tests/scenarios.rs` | 60 seconds from the start of the wait | The step fails with the captured count, the fault count, and the latest fault |
| A server process's readiness, exit, or log line | `server_process.rs` | 120, 120, and 60 seconds | The step fails, quoting the last 80 lines of the process log |
| A one-shot CLI command or a subscription output assertion | `tests/scenarios.rs` | 60 seconds for a command, 30 seconds for an expected subscription line | The step fails with the process result or retained output lines |
| One draw from the port pool | `port_pool.rs` | 65,536 consecutive draws that land on reserved ports | The draw fails with the pool exhausted |
| The whole scenario run | `suite_watchdog.rs` | 37 minutes, injectable | Every active scenario is reported, live nodes get a 60-second cleanup window, and the process exits `124` |
| Stopping the test dependencies after the run | `suite_watchdog.rs` | 2 minutes | The containers are left to the runner |
| Dropping the runtime after the run | `suite_watchdog.rs`, run by `tests/scenarios.rs` | 60 seconds | Blocking tasks still running are abandoned |

Everything else a scenario's body does, including its NSPL commands, broker and metric waits, and
assertions, keeps whatever bound its step declares. Some of those steps check their bound only
between requests, so a request that never returns outlasts it; the suite budget bounds them in every
case. It is also the only bound on the time a scenario spends queued for its concurrency permits and
on closing the browser during cleanup, neither of which has a budget of its own.

## Absolute Phase Deadlines

A phase deadline is one instant on the monotonic clock: the moment the phase started plus its
budget. Operations inside the phase receive that deadline rather than a fresh timeout, so the phase
ends when its budget says it does, however many requests, retries, or polls it made.

- **Nesting.** An operation inside a phase that may take at most a budget of its own gets whichever
  comes first: that budget from now, or the phase's deadline. A status request inside a status wait,
  a startup attempt inside a node startup, and a node startup inside a cluster construction are all
  nested this way.
- **Bounding.** Running an operation within a deadline ends it when the deadline passes. An
  operation that begins after the deadline has passed does not run at all, so a stream of
  immediately ready work cannot keep a phase alive.
- **Polling.** A poll repeats an attempt until one output is accepted or the deadline passes. Every
  attempt receives the same deadline, the pause between attempts never sleeps past it, and on expiry
  the poll returns the last output it rejected and the last failure it saw.
- **Owned work.** A wait that takes ownership of something it must clean up, such as the join handle
  of a node task, receives its deadline as a value and handles the expiry itself by aborting and
  then joining. A timeout wrapped around such a wait would cancel it after it had taken the handle,
  drop the handle, and leave the task running with nothing able to stop it.

The deadline keeps its start and its budget rather than their sum, as the product's shutdown
deadline does, so a budget of any length cannot overflow the clock.

## Deriving A Budget

A harness budget is derived from named policy inputs rather than written as an independent literal.
Each input is a measured duration, the slowest healthy value observed while the whole suite ran at
the CI concurrency factor, or a headroom factor that multiplies one. Every derived budget is checked
against the budgets around it by assertions evaluated at compile time, so a change that breaks an
ordering fails to build rather than producing a harness that outwaits itself.

| Budget | Derivation | Measured input |
| --- | --- | --- |
| Status request, 10 seconds | The 40-second status wait divided by the four requests it must outlast when none of them replies | The slowest healthy reply, 3.7 seconds, times a headroom of 2 fits in one request |
| Status snapshots, 10 seconds | One request timeout, because every node is asked at once | None of its own |
| Startup attempt, 36 seconds | A policy input | The slowest healthy node startup, 24.2 seconds, fits in one attempt; the same 3,465 startups had a 2.0-second median and a 4.3-second 99th percentile |
| Node startup, 84 seconds | Two full attempts, each 36 seconds of readiness, a 5-second cleanup slice, and a 1-second pause | Stays under a 90-second ceiling |
| Cluster cleanup, 60 seconds | The slowest healthy cluster stop, rounded up to 15 seconds, times a headroom of 4 | 12.2 seconds at the slowest over 163 cleanups, with a 0.15-second median and 1.6 seconds at the 90th percentile |
| Suite, 37 minutes | The 60-minute job limit, less 18 minutes of work before the suite and a 5-minute reserve after it | Outlasts the slowest healthy suite, 21m08s over five jobs and taken as 22 minutes, by 15 minutes of slack |

The assertions keep these orderings, among others:

- A status request is materially shorter than the waits and startup attempts that repeat it, and one
  attempt outlasts three readiness requests that never reply.
- Status snapshots are shorter than the waits whose failures they explain.
- A startup attempt outlasts the slowest healthy startup, a cleanup slice is short beside the
  attempt it ends, and the node startup budget stays under its ceiling.
- The cluster cleanup budget is shorter than the watchdog of a node a scenario stops itself, and the
  test shutdown timeout lies between the default shutdown phases and that watchdog.
- The suite watchdog's cleanup window fits inside the reserve, and so do the dependency stop and the
  runtime shutdown together.

The suite derivation holds with no margin: 22 minutes of slowest healthy suite plus 15 minutes of
slack is exactly the 37-minute budget. Raising either measured input, and both the suite and the
work before it grow with the workspace, fails that assertion at compile time until the job limit or
the slack changes. Measure the inputs again whenever the suite, its concurrency, or the runner
changes.

## Status Requests And Status Waits

The harness reads cluster state with `SHOW CLUSTER STATUS;`. A status request opens a new connection
to the node's session endpoint, in plaintext or over TLS trusting the test certificate authority
when the node serves its session API over HTTPS. It opens a session authenticated as the default
test user, sends the command with a request identity, no domain, and a fresh execution reference,
and reads the session's frames until the reply naming that request identity arrives, keeping the
session's request stream open so the node never sees the session end while it answers.

The request has one deadline: 10 seconds, or what is left of the phase that sent it. Its four
operations run in order, each within whatever that deadline has left.

| Operation | What it waits for |
| --- | --- |
| Endpoint connection | The transport connection, including the TLS handshake when the node serves HTTPS |
| Session establishment | The node accepting the authenticated session |
| Command send | Handing the command to the open session |
| Response receive | The reply naming the command's request identity, past any unsolicited server events that arrive first |

A node that never completes an operation holds the request in it until the deadline: a listener that
never accepts leaves a TLS handshake waiting, a node can accept the connection and never open the
session, and a node can take the command and never answer it, or answer only with unrelated events.

A request ends in one typed failure: the deadline passing, naming the operation still pending and
the budget; an endpoint or TLS authority that could not be configured; a command frame that could
not be encoded; a connection, session, or response-stream error carrying the transport's status; a
session that ended before the result; a frame from the node that does not decode, or a reply to the
command that is not a command result; or, when the caller needs the status text, an unsuccessful
command result with its disposition, message, and diagnostics.
Client diagnostic scenarios inspect the failed disposition and source span from that result, so
the assertion uses the delivered diagnostic rather than parsing the displayed message.

A **status wait** polls one node every 200 milliseconds until its parsed status satisfies a
condition, within 40 seconds from the start of the wait. The waits the harness performs are a
current leader, a specific leader or a leader other than a given node, a Raft state, the expected
voter set, an interconnect status to a peer, an applied index at least the leader's, and a fragment
of the status text. Two waits read every node at once: a leader the running nodes agree on, and a
consistent leader, which requires every node to name the same leader, that leader to report itself
`Leader`, and three consecutive polls to agree. Every request of a wait receives the wait's
deadline, so a wait whose last request never replies still ends at its original deadline rather than
one request timeout later. A wait that expires fails its step with the elapsed time, the last status
text it rejected, and the last typed failure it saw. The meaning of the `connected` interconnect
status these waits read belongs to [Cluster
Interconnect](./interconnect.md#application-health-and-availability).

A scenario step can also read status inside a window of its own, passing that window as the phase
deadline. Where the window bounds how long something is watched rather than how long one read may
take, every read keeps a full request timeout instead.

## Node Tasks And Readiness

An in-process node is the server application running as one Tokio task. The harness holds that task
through a single owner, which is in one of three states:

- **Not started.** No task has been spawned.
- **Running.** The task is live, and the owner holds its join handle.
- **Terminal.** The task ended, and the owner holds how: a **clean application exit**, an
  **application error** carrying the node's own report, a **panic**, or a **cancellation**.

Running and ended are read from the owner's state and never inferred from whether an error message
exists. Inspecting a task that has finished joins it and keeps its terminal outcome, so a diagnostic
never takes the handle a later stop needs. The task is consumed exactly once: a wait receives its
deadline and reports that the task was never started, had already ended, ended within the deadline,
or was aborted and joined when the deadline passed. Dropping a node's handle aborts a task that is
still running, which is the one path that cannot also join it.

A **readiness probe** is one status request, and it reports one of three outcomes:

- **Ready.** The node returned a successful command result, or one saying it is not the leader.
  Either way it answered an authenticated command.
- **Request failed.** The status request ended with its typed failure, including the operation a
  passed deadline interrupted.
- **Non-ready response.** The node answered with an unsuccessful result, kept with its kind,
  message, and diagnostics.

The readiness wait probes every 200 milliseconds within the attempt's deadline and remembers the
most recent outcome. The owner inspects the task before each probe and after it. The wait succeeds
when a probe is ready and the task still runs. It fails with **node task terminated before
readiness** when the task has ended, and with **startup deadline expired** when the deadline passed
while the task was running. Either failure names everything needed to tell resource pressure from a
deterministic failure:

```text
node '<node>' startup attempt <n> failed after <elapsed>: <failure>; task state: <state>; last readiness outcome: <outcome>
```

The task state is `not started`, `running`, or `terminal` with the terminal outcome, and the last
readiness outcome is the most recent probe or `no readiness probe completed`.

## Node Startup

Starting one node has one advertised worst case, 84 seconds, whatever happens inside it. The budget
starts before the first launch, and every readiness probe, every cleanup after a failed attempt, and
every pause between attempts receives only what is left of it.

```text
           one startup budget: 84 s, fixed before the first launch
 |<------------------------------------------------------------------->|
 | readiness | cleanup | pause | readiness | cleanup | pause | launch 3  |
 |  <= 36 s  |  <= 5 s |  1 s  |  <= 36 s  |  <= 5 s |  1 s  | remainder |
 |<-------- attempt 1 -------->|<-------- attempt 2 -------->|
```

The budget pays for two attempts at their full length. A third launch is allowed because a launch
that fails at once, such as one whose listen address another process took first, leaves most of the
budget unspent; that launch receives whatever the earlier ones left.

After an attempt fails, the harness classifies the failure, cleans up, and records the attempt.

| Failure | Classification |
| --- | --- |
| The harness could not launch the node: an address it could not parse, the test certificate authority, or the node's database directory | Terminal: the harness's own configuration, which the next launch repeats |
| Readiness did not arrive before the attempt's deadline | Transient: the resource pressure the budget exists for |
| The node's task ended with an application error binding its gRPC, HTTP, HTTPS, observability, or web console listen address, or starting the interconnect listener | Transient: another process took the port first, and fresh ports resolve it |
| The task ended with any other application error, a clean exit, a panic, or a cancellation | Terminal: the node decided it from its configuration, its stored state, or its own code |

Cleanup requests the node's stop and waits for its task within a 5-second slice of what the budget
has left. A node that never became ready has no drain to finish, so the product's shutdown deadline
plays no part here; when the slice passes, the task is aborted and joined. Cleanup also clears the
consensus fault registration the node held. After a transient failure the node moves to seven
freshly allocated ports, so a launch that lost a port race does not repeat it, and the harness
pauses one second before launching again.

The startup ends in one of four ways, each reported with the node, its budget, the time spent, and
the record of every attempt:

- the last attempt failed for a terminal reason;
- every launch the attempt limit allows was spent;
- the budget ran out before another launch could start;
- fresh ports for the next launch could not be allocated.

Each attempt's record names its number out of three, its classification, how much of the budget had
been spent once it was cleaned up, its typed failure, and its cleanup: nothing was launched, the
task stopped with its terminal outcome, or the task was aborted at the cleanup deadline. A database
lock that outlives an aborted attempt surfaces as the next attempt's application error, which is
terminal, so the startup ends with both attempts in its record.

The harness prints every transition to standard error as it happens, so a run heading for exhaustion
is legible while it is still going:

```text
node '<node>' startup: ready after <elapsed> on attempt <n>/3; <remaining> of its 84s budget left
node '<node>' startup attempt <n>/3 [<transient or terminal>] failed <elapsed> into the budget: <failure>; cleanup: <cleanup>; <remaining> of its 84s budget left
```

The budget bounds what the harness waits for, not what the operating system schedules. A runner that
stops running the harness's own tasks for longer than an attempt delays the check that ends it, and
the measured startups above include such a runner.

### Cluster Construction

A cluster is built one node at a time, bootstrap node first. The construction has one deadline, 84
seconds times the node count, and each node's startup budget is nested inside it, so no node can
outlive the construction. Once a node is ready, the harness asks the other nodes for the current
leader and that leader's applied index within one 40-second lookup, and then waits up to 40 seconds
for the new node to apply as far.

Once every node has started, construction waits for a current leader on the bootstrap node. A
cluster of more than one node must also converge, and each check is its own 40-second status wait: a
current leader on every node, the full voter set, a consistent leader on all nodes, and `connected`
interconnect status from every node to every peer, which is six waits in a three-node cluster. A
construction therefore waits up to 84 seconds per node for startups and then up to 40 seconds for
each status wait that follows.

The start step logs `cluster start requested:` with the node count, domain, and test id. When
construction fails, the harness stops the nodes it had started the way a scenario-driven stop does,
waiting for each in turn, returns their ports, logs `cluster start failed:` with the error, and
fails the step.

The other ways a scenario starts nodes reuse the same budgets. Restarting a whole cluster stops its
nodes and builds it again with a fresh construction deadline. Adding a node, or starting one a
scenario stopped, spends one 84-second node startup and the catch-up that follows it; adding a node
also repeats the convergence checks. A node started while its consensus connectivity is blocked
spends the node startup without the catch-up. A scenario whose subject is a node that cannot apply
what the leader committed starts it with one attempt's 36-second readiness and no retry, because a
retry would restart a node that had already consumed the failure the scenario armed.

## Scenario Phases

A scenario publishes the phase it is entering, so a reader sees the work in flight rather than the
last work that finished.

| Phase | Published when |
| --- | --- |
| `queued` | The suite has taken the scenario up, and it waits for the permits that let it run beside the scenarios already running |
| `started` | Its steps begin |
| `body complete` | Its steps have ended and their result is known |
| `teardown started` | Cleanup begins by releasing the faults the scenario injected |
| `teardown diagnostics` | Bounded status snapshots and the scenario's context are recorded |
| `stopping` | Its fixtures and cluster nodes are stopped |
| `finished` | The scenario and its cleanup have both ended |

```text
queued -> started -> body complete -> teardown started -> teardown diagnostics -> stopping -> finished
```

`finished` is published only once cleanup has completed. A scenario holding any other phase has not
finished, and a scenario whose log ends at a phase marker is still inside that phase.

Browser assertions that wait for an acknowledged subscription tab to disappear poll for up to 10
seconds. This bounds the server reply and browser update together; on expiry, the step reports the
tab text that remains visible.

The **active-scenario registry** holds every scenario that has started and not yet ended. A scenario
registers in its before hook, before it acquires its permits, so a scenario that never gets them is
visible while it waits. Its entry holds:

- its identity: the feature, the scenario name, and the line where it begins, which tells apart the
  example rows of an outline that share one name;
- its attempt, counted from one, which is exact because Cucumber takes up a retry only after the run
  before it has ended;
- its phase, when it entered that phase, and when it started.

The entry is keyed by a registration the scenario's world owns rather than by anything the feature
file supplies, and it leaves the registry when that world is dropped, whether the scenario passed,
failed, or panicked. The suite's report prints scenarios in order and can keep a finished scenario's
world until earlier scenarios have reported, so a `finished` entry can still be present when the
registry is read.

Entering a phase also appends a marker to the scenario log:

```text
scenario <phase>: feature="<feature>" scenario="<scenario>" line=<line> age=<age> <detail>
```

`body complete` carries `result=` with the body's result, which is passed, skipped, before hook
failed, or step failed with its error. `finished` carries `body=` with the same result and
`teardown=` with the cleanup record. Cleanup reports itself separately: a forced cleanup and a node
that panicked while stopping are recorded beside the scenario's result and never turn a passing body
into a failure.

## Scenario Teardown

The after hook runs every scenario's cleanup through the phases above.

```text
body complete
     |
teardown started      release paused health responses, domain-clock progress pauses, and
     |                command pauses; stop durable catch-up writers and clear consensus
     |                commit delays
teardown diagnostics  every node's status at once within 10 s; the scenario's context
     |
stopping              abort CLI output readers and kill their child processes;
     |                drop HTTP load, held uploads, server processes, and observers;
     |                stop HTTP receivers within 6 s; close the browser and the session;
     |                stop the cluster within 60 s;
     |                release proxies, silent peers, permits, and fixture ports
finished
```

### Teardown Diagnostics

The harness asks every node for its status at once, under one 10-second budget for the whole
cluster, so a node that never replies delays cleanup by at most that long and cannot hide another
node's status. Each node's outcome goes to the scenario log on its own line: its status text, the
operation its request was still in when the budget passed, or its typed failure.

```text
scenario teardown <identity>: node=<node> status=<status text>
scenario teardown <identity>: node=<node> status_timeout=<operation> pending after 10s
scenario teardown <identity>: node=<node> status_error=<failure>
```

A `scenario context:` line follows with the scenario's domain and test id, its last command error
and output, its last server error, and its last subscription and broker payloads. Diagnostics are
best effort: whatever they observe, cleanup continues into `stopping`. Steps that explain their own
failures can collect the same snapshots.

### Cluster Teardown

A cluster's nodes stop within one deadline of 60 seconds for all of them together.

1. Every node is asked to stop before any node is awaited.
2. Every node's task is awaited concurrently under the one deadline. Each wait owns its task, and a
   node still running when the deadline passes has its task aborted and then joined by that same
   wait.
3. Once every task has ended, whatever ended it, each node gives back what it holds: its seven port
   leases and its consensus fault registration.

A cluster of three wedged nodes therefore costs one budget rather than three, and no path detaches a
node task. The cleanup record reads `stopped <n> node(s) in <elapsed> of a 60s budget, <n> forced`,
and dropping the cluster afterwards removes the temporary storage its nodes wrote. Two more records
appear only when they apply:

```text
scenario cleanup forced: node "<node>" was still running at the cleanup deadline and was aborted and joined, leaving <outcome>
scenario live during forced cleanup: <identity> attempt=<n> phase=<phase> phase_age=<age> age=<age>
scenario teardown failed: <node record ending in a panic>
```

A forced cleanup is usually cleanup that ran beside work heavy enough to starve it, so the harness
names every scenario live at that moment. A node whose task panicked while stopping has no other
witness than its record. In the qualification runs that characterized them, forced cleanups were
nodes of WASM scenarios whose setup had already failed with `not-a-leader`, and those scenarios
passed when retried.

### What Cleanup Releases

Harness state goes back only after the tasks that used it have ended, so the next scenario never
finds a port, a fault, or a proxy taken.

- CLI output readers are aborted and their `kill_on_drop` child processes are dropped before node
  teardown. Background HTTP load, held uploads, and server processes are also dropped first.
  Dropping a server process kills it and returns its ports.
- Broker and syslog observers, HTTP receivers, the browser, and the session are closed before the
  cluster stops.
- The TCP proxies and silent interconnect peers a scenario placed in front of its nodes are released
  once those nodes have ended.
- The scenario's concurrency permits are released, and the ZeroMQ and syslog ports it drew for its
  own fixtures return to the pool last, once the nodes and observers that bound them are gone.

### Scenario-Driven Stops

A step that stops a node, stops every node, or restarts the cluster is part of the scenario's
subject, so it runs under the product's deadlines with a harness watchdog around them rather than
under the cleanup budget. The harness requests the stop and waits for the node's task for the
longest of five minutes, the node's configured shutdown timeout, and its configured drain phases:
the application drain when graceful drain is enabled plus the runtime's branch stop timeout for the
configured domain drain. A scenario that configures a longer product deadline raises the watchdog
with it, so the harness never cuts a product deadline short.
Node-specific stop steps resolve a saved scenario placeholder before selecting the node, including
the graceful stop used to exercise ownership handoff during drain.

A stop that ends cleanly is followed by a check that the node released its node and consensus
database locks. The step fails when the node's task panicked, when the watchdog passed and the task
had to be aborted and joined, or when a lock outlived the stop. Stopping every node requests every
stop first and then waits for each node in turn. A scenario can also begin stopping a node without
waiting, to observe how the cluster reacts while the node is on its way down; its cleanup waits for
it later.

## Ports

Every port the harness binds is drawn from one pool shared by all scenarios in the process. A draw
binds port zero on the loopback interface, reads the port the operating system chose, closes it, and
reserves it in the pool. The operating system would let a second scenario bind a closed port, so the
reservation is the only thing that keeps two scenarios apart. It cannot see sibling worktrees
running the same suite, which is why a node startup retries a lost bind on fresh ports.

| Holder | Ports | Returned |
| --- | --- | --- |
| In-process node | 7: gRPC, gRPC over HTTPS, HTTP, HTTPS, observability, web console, and interconnect | After its task has ended: at cluster cleanup, when a scenario stops every node, and when a failed startup attempt moves to fresh ports |
| Scenario fixtures | 4: ZeroMQ ingest and emit, syslog ingest and emit | At the end of cleanup |
| HTTP receiver | 1 per receiver, drawn with the scenario fixtures | At the end of cleanup, with the scenario fixtures |
| Server process | 6 | When the process is dropped |
| A node moved to a new interconnect address | 1 new interconnect port | The port it gave up stays reserved for the rest of the run |

A port returns only once nothing can still dial it. Every scenario names its nodes `node-1`,
`node-2`, and `node-3`, so a node of an unrelated scenario that binds a port a peer still dials
passes the peer-identity check and is caught only when its introduction fails to verify, which takes
long enough to fail a scenario. Keeping every port reserved is not an option either: the ports a
whole run draws exceed the range the operating system draws from, because Linux hands a port-zero
bind an odd port from the lower half of its ephemeral range, some seven thousand ports.

A draw is synchronous and runs on the runner task. A draw that looped until the operating system
offered a port the pool did not hold would stall every scenario and the suite watchdog with it, so a
draw that lands on reserved ports 65,536 times in a row fails with the pool exhausted, naming how
many ports the process holds, and gives back the ports it had already reserved. A pool with one free
port among seven thousand still yields it within that many draws almost every time, and a pool with
none ends the draw in about a second.

## Server Processes

Scenarios about signals, exit statuses, process startup, and open-file limits run `nervix-server` as
a real child process, because an in-process node cannot show whether the process boundary delivers a
signal to its shutdown coordinator. The fixture gives each process its own ports, database
directory, and interconnect credentials, forms a single-node cluster, and captures its standard
output and error in one log. It removes every `NERVIX_*` variable and `RUST_LOG` from the child's
environment, so the runner's configuration cannot silently reconfigure the server.

Readiness uses the same probe outcomes as an in-process node, probing every 100 milliseconds within
120 seconds, and fails at once with the exit status when the process exits first. Waiting for an
exit is bounded by 120 seconds, and waiting for a log line or an HTTP admission by 60 seconds. Those
failures quote the last 80 lines of the process log. Dropping the fixture kills the process, so
cleanup never waits for a graceful stop and a failed scenario never leaves a server running. What
the process does between a signal and its exit is product behavior; see [Shutdown And
Recovery](./shutdown.md#exit-status).

## Test Dependencies

Brokers, databases, and the other external systems that scenarios use run as containers, started the
first time a scenario needs one and shared by every scenario of the run. The run removes them when
it ends; `NERVIX_TESTCONTAINERS_MODE=reusable`, which `just test-scenarios-reuse` sets, keeps them
for the next run instead. Scenarios still provision the topics, queues, tables, and other entities
they use explicitly.

Harness Redis connections use an explicit ten-second budget for connection setup and each command
response to tolerate scheduling delay under parallel load. The driver's one-second connection and 500ms
response defaults are too short under concurrent scenario load: the three-node JAQ transformation
scenario reached a running ingestor but its publishing client timed out. The budget belongs to the
test client used for individual publishes, bursts, and subscriber-count observations. A request
failure still fails the step; a publish with an ambiguous result is never retried. The existing
bounded subscriber wait only republishes when Redis confirms that the publish reached zero
subscribers. Focused harness regressions delay setup and publish replies by two seconds and verify
that a silent broker still reaches the connection deadline.

After the run, the suite stops its dependencies within 2 minutes. A stop that does not finish is
abandoned and its containers are left to the runner, because waiting without a bound is how a run
that already has its result loses it to the job's own timeout. Dropping the runtime then waits at
most 60 seconds for blocking tasks a scenario left parked in a driver.

## HTTP Receivers

A scenario about a node sending HTTP requests to an external endpoint starts an in-process HTTP/1.1
receiver in its place, because only a receiver the harness controls can capture exactly what arrived
and choose exactly how to answer: a status sequence, a delayed, held, or lost response, a stalled
body, or malformed framing. A receiver can serve TLS with a certificate for chosen names and can
require the client certificate it issued, whose files a node mounts as a resource.

Everything a receiver holds is bounded, and exceeding a bound is recorded as a fault, not captured.

| Bound | Limit |
| --- | --- |
| One request head | 256 KiB and 512 header fields, room for a request at every HTTP emitter limit at once |
| One request body | 16 MiB |
| Captured requests | 4,096 per receiver |
| Kept faults | 256 per receiver; later faults are counted but not kept |

Every await a receiver connection makes also waits for the receiver's stop, so a held response or
a stalled body ends as soon as cleanup begins. The receivers of a scenario stop together in the
`stopping` phase, before the cluster, under one 6-second budget: 5 seconds for every connection to
end on its own, after which the rest are aborted and joined, and 1 second to join the accept loop,
after which it is aborted and joined too. The derivation asserts that this budget is shorter than
the cluster's cleanup budget. Each stop is recorded in the scenario log:

```text
HTTP receiver cleanup: <name>: stopped <n> connection(s) in <elapsed> of a 6s budget, <n> forced, <n> panicked; captured <n> request(s), recorded <n> fault(s)
scenario cleanup forced: HTTP receiver <name>: <the same record>
```

The second line appears only when a connection or the accept loop had to be aborted, or panicked.
A receiver's port is drawn with the scenario's fixture ports and goes back with them at the end of
cleanup, once the nodes that dialed it have ended.

## Client Probes

The cross-language conformance scenarios in `client_conformance.feature` run a small client of each
runtime against a scenario's cluster, or against the checked-in conformance corpus. The in-process
probe drives the shared Rust binding's C ABI from a blocking task of the binary's runtime, because
every call of the binding blocks its caller; every other probe is a child process that the fixture
starts with its target in `NERVIX_PROBE_*` variables and its standard input closed. A probe prints
one report line per observation on standard output, and the fixture keeps every line it read.

A probe's waits are bounded twice. The step that starts it waits at most 180 seconds for the line
that says its subscription is open, and the step that reads its report waits the duration the step
names for the probe to end, 180 seconds against a cluster and 60 against the corpus. Each probe also
ends itself: it gives up on its rows after 120 seconds, or on its whole run after 170. A failure
quotes every report line read so far, the exit status, and the probe's standard error. Dropping the
fixture kills a child process, so a failed scenario never leaves a probe running; the in-process
probe ends when its session fails against the stopped cluster, or at its own deadline.

Every example of a runtime other than the in-process probe is tagged `@client_conformance_toolchain`
and one `@client_probe_<runtime>` tag, and the suite excludes the first tag unless a run selects its
own tags, because those examples need toolchains the suite's job does not install. `just
test-client-conformance` builds every probe artifact and runs them; its first argument is the tag
expression that selects runtimes. The `client-conformance` CI job runs it on its own runner with a
60-minute limit and a 15-minute suite budget, which leaves the builds before the scenarios up to 40
minutes of the limit and keeps the same 5-minute reserve.

## The Suite Watchdog

The scenario run has one budget, 37 minutes from the moment it starts, which `--suite-budget` or
`NERVIX_TEST_SUITE_BUDGET` replaces with a duration such as `4m`. The budget is a clock rather than
a count of failures. Cucumber's fail-fast stops scheduling scenarios and leaves those already
running where they are, so it cannot end a run whose step, diagnostic, or node stop never returns;
the clock ends such a run at the same instant as one whose work returned at once. Until the budget
expires, the suite keeps running its intended failure and retry coverage.

The watchdog reads two registries: the active-scenario registry, and the live-cluster registry. A
cluster registers when a scenario builds it. Each node registers when its task is spawned, through a
registration the task itself owns, so the node leaves the registry when its task ends, whether it
returned, failed, panicked, or was aborted, and the watchdog can tell a cluster that stopped from
one still running without owning either. The registry holds each node's stop, its shutdown
coordinator, rather than the node, so the watchdog never reaches into a scenario that is still using
it.

When the budget expires:

1. The watchdog reads both registries before changing anything, so the diagnostic is the state the
   run expired in, and prints it to standard error at once. If the job's own limit kills the process
   during what follows, it has still said what it was doing. The scenario log receives the same
   diagnostic, with the cleanup outcome, once the cleanup window has ended.
2. It asks every live node to stop, then waits one 60-second window for all of them together,
   checking every 50 milliseconds.
3. It drops the run. That ends every scenario future still held, which aborts the node tasks the
   scenarios own and kills the server processes they started.
4. The suite stops its dependencies within 2 minutes and drops the runtime within 60 seconds.
5. The process prints the dependency teardown and the cleanup outcome, flushes its output, and exits
   with status `124`.

```text
suite timeout: the <budget> suite budget expired with <n> scenario(s) active
  suite timeout active: <identity> attempt=<n> phase=<phase> phase_age=<age> age=<age> nodes=[<nodes>]
  suite timeout unclaimed cluster: <identity> nodes=[<nodes>]; its scenario had already left the registry
suite timeout cleanup: asked <n> node(s) to stop and waited <elapsed> of a 60s window; every node ended itself
```

When a node is still running at the end of the window, the cleanup line names its cluster instead of
reporting that every node ended itself, and the scenario log adds
`suite timeout cleanup forced: the run was dropped with nodes still running`. A live cluster whose
scenario has already left the registry is a leak, and the unclaimed line is its only record.

The watchdog does not run after hooks. Faults a scenario injected stay in place while its nodes are
asked to stop, so a node held by one can still be running at the end of the window, and it is
aborted when the run is dropped. The induced timeout during qualification had one such node, held by
a consensus commit delay that only its scenario's cleanup releases.

### The CI Reserve

The suite runs inside `just test-coverage` in the CI `tests` job, after the builds and the test
binaries that precede it, the focused harness regressions among them. The job's `timeout-minutes` is
60, and the budget is derived from it so that the job ends on its own.

| Part of the job | Budget | Basis |
| --- | --- | --- |
| Work before the scenario binary starts | 18 minutes | Measured at 6m28s, 7m50s, 9m25s, 12m21s, and 15m26s over five jobs, and rising with the workspace |
| The scenario run | 37 minutes | What the limit leaves |
| After the budget expires | 5-minute reserve | At most 60 seconds of cleanup window, 2 minutes of dependency stop, and 60 seconds of runtime shutdown, four minutes in all, and then the log upload, measured at 2 to 3 seconds with 8 seconds of steps after it |

A healthy suite finishes inside its budget and exits `0` or reports its failures. A wedged one exits
`124` with its diagnostic and leaves the upload its reserve. Either way, a step that runs whatever
the job's result uploads the whole `tests/logs` directory as the `test-logs` artifact.

The job's limit remains the emergency guard outside the budget rather than the mechanism that ends a
wedged run. A job the limit cancels is killed wherever its scenarios are: the logs it uploads end
mid-scenario, with no summary and no record of what each scenario was doing. The limit and the
harness's copy of it change together.

## How Failure Reaches CI Output

The job log carries the scenario binary's standard output and standard error, and the `test-logs`
artifact carries `tests/logs`.

| Output | Where | What it holds |
| --- | --- | --- |
| Cucumber report | Standard output | Every scenario and step result in order, the world of a failed step, the summary, and every failed scenario repeated at the end |
| Harness transitions | Standard error | Node startup transitions and outcomes, and a suite timeout's diagnostic and cleanup |
| Scenario log | `tests/logs/cucumber.log` | The run's parallelism and suite budget, cluster start requests and failures, the NSPL commands scenarios run, every phase marker, teardown diagnostics and context, forced cleanups, and the suite timeout diagnostic |
| Node traces | `tests/logs/scenarios.log` | The trace output of every in-process node of the run |

Server processes write their logs into their own temporary directories, and a failure quotes the
tail it needs.

| Failure | How it surfaces |
| --- | --- |
| A step assertion | The step fails with its message; Cucumber retries the scenario and repeats the failure at the end of the report |
| A status wait that expired | The step fails with the elapsed time, the last status text, and the last typed failure |
| A node that never became ready | The step fails with every attempt's record, and a failed construction logs `cluster start failed:` |
| A teardown diagnostic that failed or timed out | The node's `status_error=` or `status_timeout=` line, and cleanup continues |
| A cleanup that aborted nodes, or a node that panicked while stopping | `scenario cleanup forced:` or `scenario teardown failed:` lines beside an unchanged scenario result |
| A scenario that never ends | The suite timeout diagnostic names it with its attempt, phase, phase age, and nodes |
| An exhausted port pool | The step that needed a port fails with the pool's report |

### Exit Status

| Ending | Status |
| --- | --- |
| Every scenario passed and the dependencies stopped cleanly | `0` |
| The run finished with failed steps, parsing errors, or hook errors, reported as a panic naming each count | `101` |
| The run finished but its dependencies failed to stop or were abandoned | `101` |
| The suite budget expired | `124` |

`124` is the status `timeout(1)` reports. It differs from `0`, from `1`, and from the `101` a panic
ends with, so a wedged suite is told apart from a passing or a failing one by its exit status alone.

## Adding A Harness Operation

A new operation that starts, observes, or stops something on the harness's behalf follows the same
contract.

1. **Find its phase.** An operation inside a phase, such as a startup attempt, a status wait, or a
   cleanup, takes that phase's deadline as a parameter and nests its own budget in it. Only a new
   phase starts a deadline of its own, and never inside a loop that retries within a phase: a retry
   that restarts its budget turns a bounded wait into an unbounded one.
2. **Derive its budget.** Name its policy inputs, usually the slowest healthy duration measured with
   the whole suite at the CI concurrency factor and the headroom that multiplies it. Derive the
   budget from them with constant arithmetic, and assert its order against the budgets around it:
   shorter than the phase that repeats it, and inside the cleanup budget or the reserve when it runs
   during cleanup. Record the measurement beside the input.
3. **Bound it by what it owns.** An operation that owns nothing runs within the deadline, or is
   polled until it, keeping its last output and last failure. An operation that takes ownership of
   something it must clean up receives the deadline as a value and handles the expiry itself: abort,
   then join. A loop yields on every iteration and pauses only until the deadline.
4. **Keep its failure typed.** Return a report of a semantic error enum the module owns, name the
   operation a passed deadline interrupted, and read state from typed variants. Never reduce an
   outcome to a boolean, infer a state from whether a message exists, or report an unknown reason.
   Failures carry no credentials and no payload values.
5. **Never block the runner task.** Every scenario and the suite watchdog share it. A synchronous
   loop needs a bound of its own, as the port draw has, and blocking work belongs on a blocking or
   spawned task.
6. **Make cancellation release it.** Whatever a scenario starts belongs to the scenario's world or
   its cluster, so dropping the run releases it: spawned work behind handles that abort on drop,
   child processes killed on drop, node tasks through the single task owner. A new kind of node
   publishes its stop to the live-cluster registry through a registration its task owns, and its
   cleanup belongs to the `stopping` phase. Ports, faults, and proxies go back only after the tasks
   that used them have ended.
7. **Report it.** Transitions a reader needs while a run is still going go to standard error or to
   the scenario log with the scenario's identity. A failure fails its step with the typed error's
   message, and diagnostics gathered on a failure path stay within the diagnostic budget.
8. **Test it.** Add a focused regression to `tests/harness_liveness.rs` that drives a stand-in
   endpoint or task, on a paused clock where the budget is long, so the bound is proven without
   production-scale waits, and run `just test-harness-liveness`. Run the affected features with
   `just test-scenarios --input <feature>` at the CI concurrency factor,
   `NERVIX_TEST_CONCURRENCY_FACTOR=2`.
9. **Keep this chapter current** in the same change.

## Guarantees And Limits

The harness guarantees:

- Every status request, status wait, readiness wait, node startup, teardown diagnostic, cluster
  cleanup, and post-run teardown has one absolute deadline, and no retry or poll restarts it.
- No diagnostic, timeout, or cleanup path detaches a node task. The only path that cannot join one
  is dropping the node's handle, which aborts it.
- A cluster's cleanup costs one budget whatever its size, and releases harness state only after
  every task has ended.
- `finished` is published only after cleanup has completed.
- The run ends within its budget, the cleanup window, and the bounded teardown, with a status that
  tells a timeout from a failure.

Its limits:

- Budgets bound what the harness waits for, not what the operating system schedules. A starved
  runner delays the checks that end a wait.
- The watchdog is polled on the runner task, so it cannot end a synchronous stall of that task. The
  job's limit is the guard that remains.
- Provisioning a Kafka topic's partitions and waiting for a consumer group's members query the
  broker synchronously on the runner task, for up to 5 seconds per query, and every scenario of the
  run waits while one of those queries runs.
- The watchdog runs no after hooks, so a node held by a fault its scenario injected can outlast the
  cleanup window and is aborted with the run.
- The time a scenario spends queued for permits and closing the browser have no budget of their own,
  and some ordinary step waits check their bound only between requests. The suite budget bounds all
  of them.
- When a cluster construction fails, the nodes it started are stopped one after another under the
  scenario-driven stop watchdog rather than under the cleanup budget.
- A construction's convergence checks are separate 40-second waits after its startup deadline.
- The suite budget's derivation holds with no margin.

## Qualification Evidence

`just test-harness-liveness` runs the 56 focused regressions that hold this contract in about four
seconds. They drive stand-in session services on real loopback sockets and stand-in node tasks, most
of them on a paused clock, and CI runs them before the scenario suite.

| Contract | Regressions |
| --- | --- |
| A status request ends at its deadline in the operation that stalled, and every probe outcome keeps its cause | `status_request_ends_at_its_deadline_while_the_connection_is_pending`, `status_request_ends_at_its_deadline_while_session_establishment_is_pending`, `status_request_ends_at_its_deadline_while_the_response_is_pending`, `status_request_ends_at_its_deadline_while_unrelated_responses_remain_ready`, `readiness_and_status_outcomes_retain_their_typed_cause` |
| A wait ends at its original deadline | `status_wait_ends_at_its_original_deadline_when_the_final_request_never_replies`, `nested_deadline_never_outlives_its_phase`, `startup_readiness_failure_reports_the_timed_out_status_operation` |
| Node task outcomes are distinct, and the task is consumed once | `running_task_reports_the_last_failed_probe_at_the_deadline`, `clean_application_exit_before_readiness_is_terminal`, `application_error_before_readiness_is_retained`, `panic_and_cancellation_have_distinct_terminal_outcomes`, `stop_and_drop_paths_use_the_task_inspected_for_diagnostics`, `forced_cleanup_aborts_and_joins_the_owned_task_once` |
| One startup budget with classified retries | `repeated_readiness_failure_spends_one_budget_across_every_attempt`, `cleanup_that_never_completes_is_aborted_inside_the_same_budget`, `exhaustion_reports_every_attempt_with_its_typed_cause`, `ports_that_cannot_be_reallocated_end_the_startup`, `a_bound_address_is_retried_until_a_launch_becomes_ready`, `a_last_attempt_still_becomes_ready_with_what_the_budget_left`, `an_application_error_ends_the_startup_without_another_launch`, `a_panicking_node_ends_the_startup_without_another_launch`, `a_launch_failure_ends_the_startup_before_anything_is_cleaned_up`, `sequential_cluster_construction_stays_inside_its_derived_budget` |
| Diagnostics are concurrent and never keep cleanup from starting | `status_snapshots_keep_a_healthy_node_while_another_node_stalls`, `failed_and_stalled_diagnostics_end_by_their_deadline_so_cleanup_starts`, `a_stalled_diagnostic_still_reaches_every_node_stop_in_a_cluster_of_one_and_of_three` |
| One cleanup budget per cluster, and truthful phases | `stuck_nodes_spend_one_cleanup_budget_in_a_cluster_of_one_and_of_three`, `a_single_node_cleanup_keeps_how_its_task_ended`, `a_panicking_node_is_the_only_cleanup_failure_a_three_node_cluster_reports`, `the_finished_phase_is_published_only_once_cleanup_has_completed`, `an_active_scenario_publishes_its_phase_and_the_age_of_that_phase` |
| The port pool is bounded and gives ports back | `a_draw_that_keeps_landing_on_reserved_ports_ends_at_the_draw_limit`, `an_exhausted_draw_gives_back_the_ports_it_had_reserved`, `a_draw_the_operating_system_refuses_is_reported_as_its_own_failure`, `ports_drawn_from_the_operating_system_are_distinct_and_reserved`, `a_released_port_can_be_drawn_again` |
| An HTTP receiver answers as scripted, records what it cannot capture, and stops within its budget | `the_receiver_captures_requests_and_answers_its_script_in_order`, `a_lost_response_is_captured_and_the_connection_closes_without_an_answer`, `chunked_bodies_interim_responses_and_raw_bytes_are_served_as_scripted`, `held_responses_and_stalled_bodies_end_within_the_stop_budget`, `requests_beyond_the_receiver_bounds_are_faults_not_captures`, `a_tls_receiver_accepts_the_client_certificate_it_issued_and_refuses_others`, `a_tls_receiver_is_refused_by_a_client_that_dials_a_name_its_certificate_lacks`, `every_documented_script_form_parses_and_unknown_forms_are_refused` |
| The suite watchdog names what was running and ends the run | `a_run_that_finishes_inside_its_budget_keeps_what_it_produced`, `a_stalled_scenario_body_is_named_with_its_attempt_phase_and_nodes`, `a_stalled_teardown_diagnostic_is_named_by_the_phase_it_is_in`, `a_node_that_never_stops_is_named_at_the_end_of_the_cleanup_window`, `a_cluster_that_outlives_its_scenario_is_named_as_unclaimed`, `a_retried_scenario_publishes_which_attempt_is_running`, `the_suite_budget_is_injectable_and_defaults_to_the_suite_policy`, `a_timed_out_suite_is_reported_apart_from_a_passing_and_a_failing_one`, `a_failing_suite_ends_the_process_by_unwinding`, `a_dependency_stop_that_never_returns_is_abandoned_at_its_budget`, `a_dependency_stop_that_finishes_keeps_what_it_reported` |

The high-parallelism qualification was recorded on 23 September 2026 for the change that landed as
`cec5f764`, at the CI concurrency factor of two scenarios per CPU with Cucumber's two retries unless
a run says otherwise. The local runs used a 24-CPU workstation shared with other builds, whose load
average stayed between 7 and 40, so 48 scenarios ran at once; CI used its 16-CPU runner, so 32 did.

| Run | Result |
| --- | --- |
| `just test-harness-liveness`, 20 repetitions | Every repetition passed, 48 regressions in 4.0 seconds each |
| `WASM processor restores guest state after cluster restart`, all three example rows, three runs with `--retry 0` | Every run passed. Every node became ready on its first attempt within 2.1 seconds, and the slowest cleanup took 8.7 of its 60 seconds with none forced |
| `tests/features/cluster/*.feature` | 206 scenarios passed in 500 seconds with 16 retries. 553 node startups: median 2.04, 99th percentile 22.0, and slowest 41.3 seconds; two became ready on their second attempt, after a 36-second readiness deadline whose last probe was a typed `Unauthenticated` reply. 200 cleanups, the slowest 45.1 seconds, none forced |
| `tests/features/runtime/wasm_*.feature` | 95 scenarios passed in 583 seconds with 18 retries. 288 startups, the slowest 17.4 seconds. Two cleanups were forced at the 60-second budget, each recorded with the node it aborted and the scenarios live beside it, and both scenarios passed when retried |
| The whole suite with `--suite-budget 4m` | Exit `124` after 313 seconds: the 240-second budget, the 60-second cleanup window, and 13 seconds of dependency and runtime teardown. The diagnostic named all 49 registered scenarios with their attempt, phase, phase age, and nodes, and the cleanup named the one node still running at the end of its window |
| The whole suite with a deliberately failing scenario added | Exit `101` reporting `3 step(s) failed` after 1,676 seconds. The failing scenario failed all three attempts while the rest of the suite ran beside it, and each attempt reached `finished` within 0.32 seconds of its body ending. 3,926 startups, 99th percentile 4.14 and slowest 21.4 seconds; eight transient bind failures were relaunched on fresh ports and became ready. Six cleanups were forced and recorded |
| The whole suite | Exit `101` after 1,490 seconds, with 1,749 of 1,752 scenarios passing. The three that did not are throughput and deadline assertions that failed only under the shared machine's load and pass in isolation and in CI. 3,937 startups, the slowest 20.7 seconds; four cleanups were forced and recorded |
| CI `tests` job with `NERVIX_TEST_SUITE_BUDGET=4m` | The job ended itself in 16m30s. The suite started 11m54s into the job, and its budget expired with 32 scenarios active, 29 in their body and 3 stopping, each named. All 53 live nodes stopped within 9.96 seconds of the cleanup window, the process exited `124`, and the `test-logs` artifact was uploaded 2 seconds later |
| CI `tests` job of the merged revision | Passed in 34m15s. The suite started 12m01s into the job and ran 1,754 scenarios and 19,384 steps in 19m26s with 2 retries. 3,832 node startups, all on their first attempt: median 1.77, 99th percentile 4.91, and slowest 6.37 seconds. No cleanup was forced |

After every run, no `nervix-server` process, scenario binary, or dependency container remained. The
[harness liveness qualification
ledger](https://github.com/nervix-io/nervix/blob/main/tests/harness-liveness-qualification-ledger.md)
records each run in full, with its artifacts, the executable evidence for every item of the
qualification matrix, and the comparison with the run the job limit cancelled.
