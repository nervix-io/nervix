# External Cluster Chaos

*Plexus, the Nervix graph definition language.*

The external chaos suite qualifies a packaged Nervix image by running it as a real cluster of
containers on one Docker host. It disturbs that cluster with real process, pause and network faults,
and it judges the outcome only from what the cluster shows through its public interfaces. The entry
point, `just chaos`, starts the runner in `scripts/chaos`. The runner is a set of Bash and jq
controllers that drive Docker Compose, the digest-pinned [Pumba](https://github.com/alexei-led/pumba)
fault injector, a Kafka broker and the image's own packaged `nervix-cli`. A run compiles nothing. It
needs no Rust toolchain, Cargo, local Nervix binary, repository test binary or Cucumber harness.

This chapter owns that suite:

- the entry point and its commands, the images a run qualifies and the host it needs
- the deployment, what a run observes and how it reads Kafka
- how each fault is injected and proven, every scenario and the verdicts it reaches
- seeded experiments and their replay
- failure categories and exit statuses, budgets, artifacts, cleanup and signal handling
- diagnostic images, the smoke and soak suites and their CI
- the limits of every claim

Four rules hold throughout.

- **Public observation only.** The runner reads the cluster through the packaged CLI over the
  session protocol, each node's own status route and `/readyz`, the observability metrics, the
  `info`-level operator log lines a node writes, the Kafka topics and consumer groups, and Docker's
  inspection and event stream. It never reads a node's storage, attaches a debugger, links product
  code or adds a hook to the image.
- **A fault counts only once it is proven.** A verdict may depend on a fault only after the runner
  has verified that the fault took effect on the exact container or link the plan named. The runner
  also heals every fault, and verifies the healing, before it captures diagnostics.
- **Controller bounds never move product deadlines.** Every wait, probe and phase has a bound of its
  own inside the run's whole-run bound. These bounds are sized for a loaded worker and never change a
  product timeout. A run that reaches one ends as a controller or observation failure, never as a
  pass.
- **One immutable image.** A run resolves the image it is given to an immutable local image ID once.
  A suite resolves its image once for all its runs. Every manifest records the Nervix image and
  every tool image by identity.

The suite is test tooling outside the layer order: it names no product code, and no product code
names it. The contracts its scenarios verify stay with the chapters that own them:

- graceful stop, drain and the former-owner fence: [Shutdown And Recovery](./shutdown.md)
- Raft durability, log retention and snapshot catch-up:
  [Consensus Storage And Replication](./consensus-storage-and-replication.md)
- relay delivery, acknowledgement forwarding and application health:
  [Cluster Interconnect](./interconnect.md)
- domain time: [Domain Clock](./domain-clock.md)
- guest checkpoints: [WASM State And Recovery](./wasm-state.md)
- scheduling, the voter observation grace and planned handoffs:
  [Control Plane](./control-plane.md#planned-ownership-handoffs-and-failover)
- backup cuts, archives and restore installation:
  [Backup And Restore Architecture](./backup-and-restore-architecture.md), and the statements and
  their reports: [Backup And Restore](./backup-and-restore.md)
- ingestor and emitter acknowledgement:
  [Connector Crates And The Connector Contract](./connector-contract.md)
- the deadlock detector:
  [Data-Plane Concurrency](./data-plane-concurrency.md#diagnostic-deadlock-detection)

## What The Suite Establishes

| Method | Runs | Establishes | Cannot establish |
| --- | --- | --- | --- |
| Cucumber scenarios ([Integration Test Lifecycle](./integration-test-lifecycle.md)) | Real nodes in a scenario binary or as local processes, with real sockets and time | Public behavior through Plexus, HTTP, the CLI and cluster state, including SIGKILL recovery of a local process | Behavior of the packaged image, kernel network faults, process pauses |
| Turmoil ([Deterministic Interconnect Simulation](./interconnect-simulation.md)) | The production transport on simulated hosts | Interconnect deadlines, reconciliation and fencing under scheduled link faults, with seeded replay | Whole nodes, consensus, storage, a real kernel |
| Shuttle and Loom ([Data-Plane Concurrency](./data-plane-concurrency.md)) | Production owners under a model checker | In-process interleavings and memory-ordering claims | Anything across processes or hosts |
| Deloxide lanes | Diagnostic nodes in the scenario harness | Active and potential deadlocks among tracked locks in the exercised workloads | The packaged image under real faults |
| External chaos on the release image | The packaged image as a Compose cluster under Pumba faults | That the image a release ships keeps delivery, ownership, consensus, state and domain time through real process, pause and network faults on a real kernel | Exhaustive fault timing, host power loss, storage corruption, a network of several hosts |
| External chaos on the diagnostic image | The same scenarios on the Deloxide image of the same revision | That no process the faults start, stop, pause or kill reports an active deadlock, an unreviewed potential cycle or lost evidence | An untracked path, an interleaving the run did not take |

No method substitutes for another. A passing suite discharges no Shuttle, Loom, Turmoil, Cucumber or
Bolero obligation, and none of those discharges a suite.

## Code Ownership

| Path | Owns |
| --- | --- |
| `justfile`: `chaos`, `test-chaos-*`, `coverage-chaos-*`, `verify-chaos-clock`, `chaos-wasm-fixture`, `build-chaos-local-image`, `build-chaos-diagnostic-image` | The entry point, the focused self-tests and their shell coverage, the clock verifier over retained transcripts, the WASM fixture generator, and two local image overlays |
| `scripts/chaos/chaos.sh` | The command surface: `list`, `run`, `replay`, `suite`, `cleanup`, `self-test`, `help` |
| `scripts/chaos/run-baseline.sh` | The controller of every scenario. It owns the arguments and their bounds, preflight, image and tool identity, TLS, the Compose deployment, graph installation, traffic, the final ledger, findings, diagnostics and the exit trap |
| `scripts/chaos/run-*.sh` | Scenario entry points that set each scenario's default fixture size and whole-run bound |
| `scripts/chaos/*-scenario.sh` | Each scenario's faults, observations and verdicts |
| `node-faults.sh`, `network-faults.sh`, `link-degradation.sh`, `link-ping-helper.sh`, `link-rate-helper.sh` | The fault primitives: process faults through Pumba and Docker, and one netem effect on a container's egress toward chosen addresses; partition plans, rules, effect matrices, preflight and healing; link degradation and its ICMP and transfer probes |
| `mixed-plan.sh`, `mixed-plan.jq` | Seeded plan generation, plan validation and default coverage |
| `verify-*.sh` | The external verifiers: ledgers, load pacing, Docker events, and crash, pause, partition, degraded-link, recovery, restart, drain, state, clock, mixed, endpoint, lifetimes and diagnostic evidence |
| `continuous-load.sh`, `observe-nodes.sh`, `observe-clock.sh` | The paced producer, the listener observer and the domain-clock observer, each running inside a container |
| `endpoint-client.py` | The [endpoints](#endpoints) scenario's HTTP and WebSocket client, which runs inside the pinned Python image and writes its ledger of attempts to its log |
| `docker-event-recording.sh` | The run's live Docker event recording and its marker windows |
| `output-boundary.sh`, `consumer-group.sh`, `startup-status.sh`, `image-package-check.sh`, `image-identity.sh`, `tool-images.sh`, `select-subnet.sh`, `require-jq.sh` | Kafka end-offset observations and the output boundary, the source consumer group's members and the nodes that run them, startup status publication, the image-package check, image identity rules, the tool image pins, network selection and the jq requirement |
| `verifier-self-check.sh` | The verifier self-check's bound, the identity of the chaos scripts and the judgement of the record a suite hands its runs |
| `cleanup.sh` | Removal of one run's labeled resources, with optional evidence capture |
| `suite.sh`, `suites.json`, `suite-report.jq` | The suites: catalog, shard budgets, runs, verdict records, summaries and suite cleanup |
| `compose.yaml`, `compose.diagnostic.yaml` | The deployment and its diagnostic overlay |
| `compose.fresh-cluster.yaml`, `compose.fresh-cluster.diagnostic.yaml` | The second cluster that a [fresh-cluster restore](#restore-into-a-fresh-cluster) starts, and its diagnostic overlay |
| `fixtures/` | The Plexus graphs, record generators, the WASM guest and an explicit mixed plan |
| `tests/` | The runner's self-tests |
| `.github/workflows/chaos.yaml`; the `chaos-*` and `build-diagnostic` jobs of `.github/workflows/docker-build.yaml` | CI |

## Images

### The Nervix Image Under Test

A run qualifies exactly one Nervix image, which `--image` names and which must already exist. The
name can be a published reference such as `ghcr.io/nervix-io/nervix@sha256:<digest>`, a local tag,
or a local image ID. Obtain one before the run:

```bash
# An image CI published, by digest
docker pull ghcr.io/nervix-io/nervix@sha256:<digest>
# The release image of this checkout, tagged nervix:debian
just docker-build-debian
# A debug server and CLI of this checkout layered over an available image
just build-chaos-local-image nervix:backup-local nervix:chaos-current
# The Deloxide diagnostic image of this checkout, or an overlay of one on an available image
just docker-build-diagnostic deloxide-order 23 nervix:diagnostic
just build-chaos-diagnostic-image nervix:diagnostic-local ghcr.io/nervix-io/nervix:debian-latest deloxide-order
```

The build recipes are preparation, not part of a run. Before Compose starts, the controller
resolves the reference to its immutable local image ID. A reference that is not local gets one pull,
bounded to 180 seconds. A reference that still cannot be resolved fails the run in preflight with a
setup error.

Both local overlays replace the base image's revision label with the checkout supplying their
binaries. A tracked modification adds `-dirty` to that revision, so a suite requiring the clean
commit refuses the image. A failed Git identity lookup stops the build before Docker runs. The
overlay identity controls exercise both build recipes with inert tools; they run through
`just test-chaos-local-image` and the full self-test.

The Compose file has no build directive and no fallback tag. Every node and every disposable
container of the image runs the resolved ID: the CLI containers, the clock observers and the
deadlock report. The manifest records:

- the requested reference
- the resolved image ID
- the image's repository digests
- the revision its `org.opencontainers.image.revision` label declares

Before any node starts, the image-package check runs a run-labeled container of the image without
a network. It requires an executable `/usr/local/bin/nervix-server` and
`/usr/local/bin/nervix-cli`, and a diagnostic image must also package `nervix-deadlock-report`. Its
predicate has a 30-second bound and its inspection a separate 20-second bound. A Docker client
timeout is a failed observation, even when the helper's inspected exit is zero. It is never read as
missing binaries. `diagnostics/image-package/` keeps the command, both streams, the outcomes and the
inspected state.

After startup, the controller confirms that every node container runs the resolved image. Once the
event recording is closed, `results/container-images.json` attributes every run-owned container,
markers included, to the Nervix image or to a tool image the manifest records. A container created
from any other image fails a run that otherwise passed, as a controller failure.

### Tool Images

Every other container comes from a tool image that `tool-images.sh` pins by a registry-qualified
digest:

| Tool | Release | Pinned by | Runs |
| --- | --- | --- | --- |
| Apache Kafka | 3.9.1 | Multi-architecture index digest | The broker, and topic and consumer-group administration |
| kcat | 1.7.1 | Its single linux/amd64 manifest, the only one published | The continuous loads, the baseline producer and the topic readers |
| Alpine | 3.22.6 | Multi-architecture index digest | Listener, readiness and metrics probes, the listener observer, link probes and Docker event markers |
| Python | 3.13.18 on Alpine 3.24 | Multi-architecture index digest | The [endpoints](#endpoints) scenario's HTTP and WebSocket client, which uses only the standard library |
| PostgreSQL | 17.11 on Alpine 3.24 | Multi-architecture index digest | The [lifetimes](#lifetimes) scenario's database for its pooled table emitters, with its data on a tmpfs |
| Pumba | 1.2.1 | Multi-architecture index digest | Every process and network fault |
| Pumba nettools | The helper published with Pumba 1.2.1 | Multi-architecture index digest | `tc` and `iptables` inside a node's network namespace |

Preflight makes each tool image the scenario uses local. It does so before it resolves the Nervix
image and before it creates any container, pulling a missing image once within a bound, and it
records each reference and image ID under `tool_images` in `manifest.json`. Every result file and
`results/finding.json` carry the same map.

A pin that cannot be pulled fails the run in preflight with exit status 2. The setup error names
the image on the console and in the manifest's `setup_error`, and Docker's output stays in
`diagnostics/tool-image-pull-<tool>.txt`. Moving a tool to another release means replacing its
digest in `tool-images.sh` and requalifying the scenarios that start it. The suite is qualified on
amd64 workers.

### Fixtures And The WASM Guest

| Fixture | Use |
| --- | --- |
| `fixtures/baseline.plexus` | The graph every scenario installs. It is described in the list below the table |
| `fixtures/generate-baseline.jq` | The records: a run-scoped `event_id`, a `sequence`, the branches `alpha`, `beta` and `gamma` in turn, and content derived from the sequence |
| `fixtures/stateful.plexus`, `fixtures/generate-stateful.jq` | The stateful scenario's graph and its two interleaved branches |
| `fixtures/paced.plexus`, `fixtures/generate-paced.jq` | The paced domain's graph and its two interleaved branches |
| `fixtures/wasm/processors/branch-counter.wat` | The stateful scenario's prebuilt WASM guest, in WebAssembly text |
| `fixtures/restore-state.plexus` | The restore scenarios' padded guest: a Kafka ingestor with domain-owned offsets and a detached, branched WASM processor |
| `fixtures/wasm/padded-state-wat.sh` | Writes the padded guest in WebAssembly text for a save size: each branch saves exactly that many pseudo-random bytes, restores only a save of that size, and emits nothing |
| `fixtures/mixed-burst-loss-leader-peer.json` | An explicit one-step mixed-instability plan the soak suite runs |

The baseline graph has three parts:

- the Kafka ingestor `chaos_ingestor`, which reads `chaos_input` with
  `OFFSET BY CONSUMER GROUP 'chaos_baseline'`, `MODE ACK SEQUENTIAL` and `ACK TIMEOUT 30s`
- the relay `chaos_records`, branched by `chaos_branch` with `TTL 5m MAX INSTANCES 16 EVICT LRU`
- the attached Kafka emitter `chaos_emitter`, which writes `chaos_output` with
  `MODE ACK PARALLEL MAX 16`

The packaged server loads the guest's WebAssembly text directly, so a run needs no guest toolchain.
The run uploads exactly that file with the packaged CLI's `UPLOAD RESOURCE` and records its SHA-256
and size under `fixtures.wasm`. `just chaos-wasm-fixture` regenerates the module from the current
guest ABI. The `nervix-wasm` package's `chaos_branch_counter` test, part of `just test`, fails while
the checked-in module differs from what the generator writes. The same test drives the module
through the host to prove its per-branch counts and its restore from saved state.

### Building Nothing

A run compiles nothing:

- the controller is shell and jq
- every Nervix process is the packaged image
- every tool is a pinned image
- the graphs are Plexus text
- the guest is WebAssembly text

CI holds this to the letter. The job that runs a shard removes every Rust toolchain directory from
the `PATH` the suite runs with. The job fails if `cargo`, `rustc` or `rustup` is still reachable.

## Host Requirements

A run needs a Linux Docker host with the following.

- **Run tools.** Bash; Docker Engine with the Compose plugin; OpenSSL; a `timeout` that takes
  `--foreground` and `--kill-after`; `awk`, `sed`, `grep`, `sort`, `wc` and `date`. A run refuses
  to start without them, as a setup error.
- **jq 1.8 or later.** The verifiers use jq 1.8's grammar, which jq 1.7 refuses to compile.
  `just chaos run`, `just chaos replay`, a suite run and `just chaos self-test` stop at once with a
  setup error that names an older jq. Listing, planning, reporting and cleaning up a suite work with
  jq 1.7.
- **Suite tools.** A suite also needs `setsid` from util-linux and a `tee` that takes `-p`, as GNU
  coreutils' does. Suite cleanup reads process state from `/proc` and `ps`.
- **The local Docker socket.** `/var/run/docker.sock` must exist. Every scenario except
  `baseline`, `backup`, `backup-cycles`, `restore-fresh-cluster` and `restore-volume` mounts it into
  Pumba's container, after checking Pumba's version and its access with a dry run.
- **Network fault support.** The partition, degraded-link, former-owner, backup-fault and
  restore-fault scenarios, the `owner-partition` and `voter-partition` faults and mixed instability
  need a kernel that offers the `prio`, `sfq` and `netem` queueing disciplines and `iptables`
  inside container network namespaces. Each run proves this on the worker before it starts; see
  [Network Preflight](#network-preflight).
  - GitHub-hosted `ubuntu-24.04` workers qualify once the job has loaded `sch_prio`, `sch_sfq` and
    `sch_netem`.
  - The repository's CI shards run on Boat `boat-large` workers, whose job loads the same modules
    before it pulls the image and stops the shard there when the kernel cannot provide one.

A host may run several runs and suites at once. Each run owns its Compose project, network, volumes
and labels, and runs share nothing but the Docker daemon and the host's capacity. Concurrent runs
slow each other down, which the budgets allow for; see [Budgets And Timing](#budgets-and-timing).

## Deployment

Each run deploys its own Compose project, `nervix-chaos-<run>`, from `compose.yaml`, with the cluster
ID `chaos-<run>`.

### Network And Addresses

`select-subnet.sh` chooses a `10.213.N.0/24` network that overlaps no existing Docker network or
host route. Nodes take the fixed addresses `.11`, `.12` and `.13`, so a peer-side partition rule
keeps matching a node across its restart. Every other container draws its address from the upper
half of the range. The manifest records the subnet, the dynamic range and the node addresses.

### Services

| Service | Image | Role |
| --- | --- | --- |
| `broker` | Kafka | One KRaft broker and controller, with automatic topic creation disabled. The run creates every topic explicitly, with one partition and replication factor 1 |
| `nervix-1` | Nervix | The bootstrap node, the only one given the default user's password |
| `nervix-2`, `nervix-3` | Nervix | The other voters of a three-node run, joining through `nervix-1` |
| `admin` | Nervix | Disposable containers that run the packaged `nervix-cli` against a node's session route |
| `broker-admin` | Kafka | Disposable topic and consumer-group administration |
| `kcat` | kcat | Disposable producers and topic readers |
| `probe` | Alpine | Disposable listener, readiness and metrics probes |
| `load`, `state-load`, `paced-load`, `life-load` | kcat | The continuous loads |
| `endpoint-load`, `endpoint-websocket` | Python | The [endpoints](#endpoints) scenario's HTTP and WebSocket clients |
| `postgres` | PostgreSQL | The [lifetimes](#lifetimes) scenario's database, with its data on a tmpfs |
| `observer` | Alpine | The independent listener observer |
| `clock-observer` | Nervix | One domain-clock observer per node |

### Node Settings

Every node listens for:

- sessions on 47391
- the web console on 47420
- observability on 9090
- the interconnect on 47395, with the run's TLS material

Each node also has these container settings:

- `init: true`
- `restart: "no"`: no automatic restart, so every restart is the controller's own explicit
  `docker start`
- `stop_grace_period: 60s`
- the `local` log driver, with two files of 2 MiB
- one named volume at `/var/lib/nervix`
- `RUST_LOG=nervix=info`

The scenarios change only ordinary settings, and they read the deployed values back from Docker
inspection where a verdict depends on them:

| Setting | Default deployment | Changed for |
| --- | --- | --- |
| Raft heartbeat, and the election window derived from it | 250 ms, so 1.5 to 3 seconds | 1.5 seconds, so 9 to 18 seconds, for `pause-resume`, `--fault owner-pause` and `--fault voter-pause` |
| Node unavailability timeout | 10 seconds | 15 seconds for the same pause deployments |
| Raft snapshot entry threshold, covered log entries retained | 10,000; 1,000 | 64; 16 for `stale-follower` |
| State snapshot interval, replica count | 30 seconds; 0 | 1 second, and on three nodes one replica, for `stateful` and `lifetimes`; one replica for `restore-fault` and `restore-volume` |
| Command retry validity | 15 minutes | 3 minutes for `restore-fault --fault reclamation-owner-crash` |

### Interconnect TLS

Each run generates its own certificate authority, RSA 2048 and valid for two days. A subject
attribute holds at most 64 characters while a run ID may have 96, so the authority's subject names
a digest of the run ID. Each node certificate names the node's host name and its
`nervix://cluster/<cluster>/node/<node>` identity. The private keys exist only in the run directory.
The run removes them when it ends, and suite cleanup removes those of any run whose exit trap never
ran.

### Labels And Ownership

Every container, network and volume a run owns carries `io.nervix.chaos.run=<run-id>`.
`io.nervix.chaos.role` names each container's role, such as `node`, `broker`, `admin`, `load`,
`observer`, `fault` or `deadlock-report`, and node containers also carry
`io.nervix.chaos.target=true`. The run label serves three purposes:

- Pumba's exact target selector
- the authority of cleanup
- the filter of the live event subscriber

## Commands

| Command | Does |
| --- | --- |
| `just chaos list` | Lists the scenarios with their topologies, cases, profiles and faults |
| `just chaos run SCENARIO --image IMAGE [OPTIONS]` | Runs one scenario. `--help` after the scenario prints its options |
| `just chaos replay RUN_DIRECTORY [--artifacts DIR] [--run-id ID] [--timeout SECONDS] [--verifier-self-check RECORD] [--keep]` | Replays a recorded mixed-instability run |
| `just chaos suite list` | Lists the suites, their shards, and each entry's command and budget |
| `just chaos suite smoke\|soak --image IMAGE [--image-kind KIND] [--image-revision REVISION] [--shard N] [--entry ID]... [--artifacts DIR] [--suite-id ID]` | Runs a suite, one shard of it, or named entries |
| `just chaos suite shards SUITE` | Prints the suite's CI matrix as JSON: each shard's entries with its execution, preparation, step and job budgets |
| `just chaos suite cleanup [--wait SECONDS] SUITE_DIRECTORY` | Ends what a suite left behind |
| `just chaos suite report [--expect-shards N] SUITE_JSON...` | Renders one summary of several shards' records, and fails unless every one passed |
| `just chaos cleanup --run-id RUN_ID [--evidence DIR]` | Removes one run's labeled resources, first capturing them into `DIR` |
| `just chaos self-test` | Runs every verifier, pinned-image and Compose contract check, then the real network-fault refusal controls |
| `just chaos help` | Prints the command summary |

`just chaos run` takes these options. A scenario refuses, as a setup error, any option it does not
take.

| Option | Applies to | Range and default |
| --- | --- | --- |
| `--image IMAGE` | Every scenario | Required |
| `--nodes 1\|3` | Every scenario. Only `baseline`, `backup`, `backup-cycles`, `rolling-restart`, `leader-crash`, `cluster-restart`, `stateful`, `endpoints`, `lifetimes` and `domain-time` accept `--nodes 1` | Default 3 |
| `--records N` | Every scenario | 1 to 1,000; mixed instability 1 to 20,000. Defaults are in the [catalog](#scenario-catalog) |
| `--timeout SECONDS` | Every scenario | 120 to 3,600; mixed instability from its duration plus 600 through 21,600. Defaults are in the catalog |
| `--artifacts DIR` | Every scenario | Default `target/chaos` |
| `--run-id ID` | Every scenario | 1 to 96 letters, digits, dots, underscores or hyphens, starting with a letter or digit. Generated when omitted. A run ID whose directory already exists is refused |
| `--verifier-self-check RECORD` | Every scenario | The record of the verifier self-check a suite executed, which the run adopts instead of executing `tests/self-test.sh`. A record that did not pass on this worker, during this boot, for exactly the run's chaos scripts is refused as a setup error before the run creates anything. A suite passes it to every run; a standalone run omits it |
| `--keep` | Every scenario | Keeps the run's resources after diagnostics and prints their cleanup command |
| `--outage-seconds N` | The crash scenarios, `cluster-restart`, `stateful`, `endpoints`, `lifetimes`, `domain-time`, `backup-fault` and the crash faults of `restore-fault` | 5 to 120. Default 8; in `stateful`, 45 for `owner-pause` and `owner-partition`; in `endpoints` and `lifetimes`, 45 for `owner-partition`; 30 for `voter-pause` and `voter-partition`. The `owner-partition` fault of `backup-fault` holds a fixed 35-second isolation. In `restore-fault --fault reclamation-owner-crash` the killed owner stays down at least until the other target reclaims its staging |
| `--case CASE`, `--partition-seconds N` | `partition-recovery` | `all`, `follower`, `asymmetric`, `leader` or `quorum-loss`, default `all`; 20 to 600, default 45 |
| `--isolation-seconds N` | `former-owner-restart` | 20 to 600, default 45 |
| `--fault FAULT` | `stateful`, `endpoints`, `lifetimes`, `domain-time`, `backup-fault`, `restore-fault` | See those scenarios. One node supports `none` and `cluster-restart`. Default `none`; `backup-fault` requires `owner-crash`, `leader-crash`, `owner-partition` or `rolling-restart`, and `restore-fault` requires `leader-crash`, `leader-partition`, `owner-crash` or `reclamation-owner-crash` |
| `--profile NAME` | `degraded-links` | `all`, `delay`, `jitter`, `random-loss`, `burst-loss`, `rate-limit` or `combined`, default `all` |
| `--load-interval-ms N` | `degraded-links` | 100 to 5,000, default 750 |
| `--baseline-seconds N`, `--degrade-seconds N`, `--drain-seconds N` | `degraded-links` | 10 to 120, default 15; 10 to 120, default 20; 10 to 180, default 90 |
| `--max-backlog N`, `--min-throughput-pct N` | `degraded-links` | 1 to 1,000, default 200; 1 to 100, default 50 |
| `--max-recovery-backlog N`, `--max-memory-bytes N`, `--max-pending N` | `degraded-links`, `mixed-instability` | 0 to `--max-backlog`, default 20; at least 1 MiB, default 1 GiB; at least 1, default 128 |
| `--seed N`, `--duration D`, `--policy POLICY`, `--coverage LIST`, `--plan FILE` | `mixed-instability` | See [Mixed Instability](#mixed-instability) |

The focused checks run parts of the runner on their own:

| Recipe | Checks |
| --- | --- |
| `just test-chaos-network-preflight [CASE]` | The real network-fault preflight and its refusal controls; a case name runs one control with the live installation, effect and healing check |
| `just test-chaos-rate-probe` | The production link transfer and ICMP probes, with delayed Docker launches and starts, stalled, partial, lost and refused transfers, failed log reads and cleanup, and invalid clocks |
| `just test-chaos-degraded-evidence` | The degraded-link controller, parser and effect limits at 40 and 400 packets |
| `just test-chaos-hold-quorum` | The mixed-instability hold check for a degraded link that leaves every voter in a quorum |
| `just test-chaos-pause-delivery` | A failover-length pause's emitter check on the retained leader and relay-owner pause timelines: the source consumer group's handoff from a paused ingestor owner, read before the relocated consumer joins and after the handoff deadline; a handoff that never comes or comes after the unpause; a stalled emitter; the window that opens at placement when the consumer stayed in place; and emitter progress sampled after the unpause |
| `just test-chaos-output-boundary [SELECTION]` | The suite preparation and baseline execution budgets and the Kafka end-offset observations, without Docker |
| `just test-chaos-verifier-self-check` | The identity of the chaos scripts, every reason a verifier self-check record does not apply, and a run's refusal of such a record before it creates anything, without Docker |
| `just test-chaos-image-package` | The image-package check and its refused or invalid observations |
| `just test-chaos-startup-status` | Bounded startup status publication |
| `just test-chaos-backup-fixture` | Derivation of the backup graph from the baseline fixture |
| `just test-chaos-backup-fault` | The option rules of `backup-fault`, `restore-fresh-cluster` and `backup-cycles`. It also checks the [fault backup verdict](#backup-faults) against synthetic reports: a clean refusal; a refusal that left the destination or a `.nervix-backup-` staging file; a failure without a JSON report or without a backup failure reason; an uncertain failure that keeps its execution reference; a success without an archive; leader-crash archives whose cut was engaged before and after the crash; and an archive whose cut released while the acknowledgement gate held |
| `just test-chaos-restore` | The option rules of `restore-fault` and `restore-volume`; the padded guest generator's bounds and layout; the node sampler's records, the staging verdicts read from them, including a pair of samples across a restart; and the [restore verdict](#restore-faults) against synthetic reports: a completed restore, one without every step applied, an incomplete one with and without its one failed step, a refusal before and after its target exists, an uncertain failure and an end without a JSON report |
| `just test-chaos-backup-state` | The [deduplicator and window verdict](#backup-cycles) over an exact set of outputs and one with replayed copies. It also checks the outputs that a restore which lost deduplicator keys, lost or doubled a window's retained rows, changed a record or dropped an output would produce, and malformed and missing inputs |
| `just test-chaos-stateful-verifiers` | The state, clock and window verdicts |
| `just test-chaos-lifetimes-verifiers` | The option rules of `lifetimes`, and its [verdicts](#lifetimes) against synthetic ledgers: records enriched with their branch's profile, with an ignored lower or equal timestamp's version, or before a replacement applied; a lost parked record; repeats with and without a fault, before and past the milestone; generated rows of a state never held or going back; malformed and lost windows; a rotation that evicted nothing; an expiring branch that never expired or kept its first profile; a lost late record; and buffered records that were not held or waited for their flush interval |
| `just test-chaos-endpoint-verifiers` | The option rules of `endpoints`, and its [route and WebSocket verdicts](#endpoints) against synthetic ledgers: accepted records that reach both domains, or a withdrawn domain's route; a lost, repeated, altered or unsent record; a record never accepted or abandoned; a client that stopped early; a bound route that answered 404; a retained connection that was refused with 1013, closed otherwise or not probed; a delivered probe; and a lost, repeated or altered frame |
| `just verify-chaos-clock ARGS` | The clock verifier over retained observer transcripts |
| `just coverage-chaos-*` | The same controls under kcov, keeping their shell coverage under `target/chaos-*-coverage/` |
| `just coverage-chaos-baseline IMAGE [NODES] [TIMEOUT] [RUN_ID]` | The complete baseline controller under kcov against a prebuilt image; use a fresh run ID for each collection |

These checks test the external runner. They make no claim about product recovery or model-checker
coverage.

## Anatomy Of A Run

### Phases

A run records each phase it enters in `phases.ndjson` and its final phase in `manifest.json`.

| Phase | Work |
| --- | --- |
| `action plan and fixtures` | Mixed instability only: it generates or copies the plan and validates it, then sizes and generates the fixture, before any container exists |
| `preflight` | Arguments and host tools; the tool images; the Nervix image; the live Docker event recording; the image-package check; a diagnostic image's evidence directories; Pumba's version and Docker access; the [network preflight](#network-preflight) |
| `verifier self-check` | `tests/self-test.sh`, bounded to 600 seconds, which proves the external verifiers on this worker before they judge the run. A run of a suite instead adopts the record of the check its suite executed once before its first run: it copies the record into `results/verifier-self-check.json` and the check's output into `results/verifier-self-test.txt`, and its manifest names the record under `verifier_self_check` |
| `TLS generation` | The run's certificate authority and node certificates |
| `Compose validation` | `docker compose config`, rendered into `compose.rendered.yaml`. Mixed instability also records the node settings and load interval Compose renders |
| `broker startup and provisioning` | The broker, within 90 seconds, and every topic the scenario uses |
| `Nervix startup` | `nervix-1`, then the other nodes. Each must answer `/readyz` and every listener within 120 seconds, and `SHOW CLUSTER STATUS` must name every node within 120 seconds |
| `startup status publication` | The validated public status, published by byte copy and atomic rename within 30 seconds |
| `image identity verification` | Every node container runs the resolved image |
| `Plexus graph installation` | The domain `chaos_baseline`, the graph through the packaged CLI, and `START` |
| `public placement evidence` | On three nodes, `RELOCATE` places the ingestor on `node-1`, the relay on `node-2` and the emitter on `node-3`, so traffic crosses nodes. Each owner must read back through `DESCRIBE` within 60 seconds |
| The scenario's own phases | Traffic startup, then each fault and its recovery |
| `load pacing` | The pacing verdict of every continuous load |
| `offset and output boundaries` | Consumer offsets at the source boundary, then the output boundary |
| `external ledger verification` | The exact ledger |
| `public diagnostics` | Final status, descriptions, consumer groups and metrics |

The exit trap then runs:

1. It heals every fault and captures diagnostics.
2. It closes the event recording.
3. It attributes every container to a recorded image.
4. On a diagnostic image, it qualifies the deadlock evidence.
5. It writes the finding of a failed run.
6. It removes the run's resources and private keys.
7. It records the final status.

### What A Run Observes

A run reads only public surfaces:

- **The packaged CLI.** Disposable `admin` containers run `nervix-cli` against one node's session
  route, for example `SHOW CLUSTER STATUS`, `SHOW PLACEMENTS`, the `DESCRIBE` commands, `RELOCATE`,
  `CREATE RESOURCE` and the `backup` command.
- **Each node's own status route.** A status counts only when it names the node that answered.
- **`/readyz` and every listener.** A node can report no leader for a moment during an election,
  and loss on a leader's link can start one. So a check that a fault left a node reachable requires
  its listeners, not its readiness.
- **The observability metrics.** Examples are relay delivery totals,
  `nervix_consensus_log_last_index`, `nervix_consensus_log_purged_index`,
  `nervix_consensus_log_snapshot_index`, `nervix_interconnect_requests_total` and interconnect
  pending work.
- **`info`-level operator log lines.** Examples are the shutdown phases, the drain-support outcome,
  `raft transition: state=Shutdown` and runtime admission after linearizable consensus catch-up.
- **The Kafka topics and consumer groups.** These are read by the broker's own CLI inside the
  running broker, and by kcat.
- **Docker inspection and the [live event recording](#the-live-docker-event-recording).**

### Traffic And Ledgers

The baseline produces its whole fixture with one kcat call and starts no continuous load. Every
other scenario takes its input from the `load` service, which runs `continuous-load.sh` in the
pinned kcat image. The load works like this:

- It produces one fixture record per interval to `chaos_input` until the runner writes its stop
  file or the fixture ends.
- Every record leaves through its own `kcat -P` call. kcat 1.7.1 reads standard input in 1,024-byte
  blocks, so one kcat fed a paced stream would deliver about nine records at once and then nothing
  for nine intervals. Once a call has returned, it has delivered its record, so no record waits
  inside the producer when the load stops or holds.
- A call that kcat reports as failed ends the load with a nonzero status and a message that names
  the record, and the runner's next check of the load fails the run.

Records fall due one interval apart on the kernel's uptime clock, which the load reads from
`/proc/uptime` to a hundredth of a second:

- A call that returns within half an interval of its due time keeps the schedule.
- A later call restarts the schedule from its return, so the load never catches up with a burst.

Two records are therefore at least half an interval apart, less the 10 ms the clock resolves.

The stateful and domain-time scenarios add a second load beside the first:

- `state-load`, at one record a second
- `paced-load`, at one record every 250 ms

Each runs the same script with its own fixture, topic and stop file. `manifest.json` lists every
load under `loads`, with its service, topic and interval.

After the load stops, the runner reconstructs the accepted-input ledger from the source topic
itself. A producer's own report is never the ledger: an ambiguous producer outcome is recorded, and
the broker decides what was accepted. The runner then checks two things:

- that the ingestor's consumer group has committed through the source boundary, within 120 seconds.
  The backup and restore scenarios have no consumer group to check: their ingestor keeps
  domain-owned offsets, which their archives record
- that the output topic holds at least the accepted input

`verify-ledger.sh` compares the outputs with the accepted inputs by event ID, exact content and
branch. It reports duplicate, missing, unexpected and corrupt records independently in
`results/ledger.json`. The baseline allows no duplicate. A fault scenario accepts identical replay
duplicates, which acknowledgement replay after a fault permits, and counts them separately; it
still fails on any missing, unexpected, corrupt or wrong-branch record.

When a fault scenario's output stays short of the accepted input past its bound, the runner still
saves the available output and runs the exact verifier, so the ledger names the missing IDs. The
run then exits with status 124. A recovery scenario gives the final ingestor-boundary query, a
disposable Kafka administration command, 60 seconds of its own. That allows for JVM startup on a
loaded worker. A failed query is a controller failure and cannot qualify delivery.

### Kafka Offsets And The Output Boundary

Every end-offset read runs the pinned Kafka CLI inside the already running broker. It creates no
helper container within the observation's deadline. Each read keeps its own
`traffic/offsets/<topic>.<attempt>/` directory with:

- raw stdout and stderr
- `result.json`, which records the exact command, the requested and effective bounds, the start
  and elapsed time, the exit code, the parsed offset and the observation's outcome

A missing, malformed or ambiguous partition-zero offset is an observation refusal, even when the
command exited successfully.

The output boundary must stabilize within a 30-second window, which the whole-run budget also
caps. The window requires three consecutive successful, equal reads at or above the accepted-input
minimum, and a failed or below-minimum read resets the sequence. `traffic/output-boundary.ndjson`
keeps the polls, and `results/output-boundary.json` records the result. When the boundary is not
established, the result names one of these reasons:

- `overall-deadline`, which makes the run a controller failure
- `observations-failed`, `below-minimum` or `unstable-output`, which make it an observation failure

The result also records the overall time that remained and the count of valid observations. An
observation failure does not by itself establish a missing record. A failed final query stays
`observations-failed` even when an earlier query returned a valid offset. Broker administration runs
without a terminal or stdin attachment, so it cannot borrow an input stream that the controller's
caller holds open.

### Load Pacing

After a load stops, the runner reads the create time the broker stored for each of its records into
`traffic/<service>-timestamps.txt`. A producer stamps that time when it hands the record to its
client, so the gaps between consecutive records are the gaps between the load's kcat calls.
`verify-load-pacing.sh` writes them to `results/<service>-pacing.json`:

- the minimum, median, 95th percentile, maximum and mean gap
- the number of gaps longer than one and a half intervals
- the longest gap, with its offset

Two records closer together than a quarter of the interval fail the run as a controller failure,
after its ledger and results are written. Every timing such a run reports was measured under a load
other than the declared one. A longer gap never fails a run: a call that waited on the broker makes
one record late, a held load is late by its hold, and both are reported.

## Fault Injection And Its Proof

### Common Rules

**Faults.** Every fault comes from a digest-pinned Pumba container, or from Docker itself for an
explicit start.

**Targets.**

- A fault names its target by the exact labeled Compose container, which the controller resolves
  from a public role read immediately before the fault.
- A Pumba dry run must select exactly that container, or exactly the planned set for a
  whole-cluster fault.
- Immediately before each crash or pause, after the control canary and the dry run, the runner
  waits up to 30 seconds for a settled public role read: the nodes agree on leader, Raft term and
  last log index, and the selected role still belongs to the same node.
- A timeout, or a role that moved, is an injection failure before any fault.

**Lifecycle events.** From each fault through its recovery, the run's live event recording must
hold exactly the planned node lifecycle events. An unexpected exit, kill, start or pause of any
node fails the run.

**Control canaries.**

- `CREATE RESOURCE` canaries run before, during and after faults, and `DESCRIBE RESOURCE` confirms
  their effect.
- A canary is classified as acknowledged, refused or uncertain. An attempt stopped by its external
  bound counts as uncertain.
- After recovery, every node must agree whether each canary took effect. An acknowledged effect
  must exist, and a refused one must not.

**Healing and diagnostics.** A run heals every fault it owns before it captures diagnostics, on
every kind of exit, including `--keep` and an interrupted run.

### Process Faults

- **Graceful stop.** Pumba's `stop --time 60` gives a 60-second grace, longer than the product's
  default 50-second shutdown deadline. The proof has three parts:
  - The stop must produce Docker exit code 0 and the completed shutdown-phase log lines.
  - When every other node is a settled, uncordoned voter, the stopped node has a live
    replacement. Its log must then report `shutdown drain-support phase finished
    outcome=Completed`: it handed its scheduled work to the others through the leader before it
    exited, the leader included. A drain that was refused, failed, abandoned or never answered is
    a product finding, because the work would then fail over as after a crash.
  - A one-node drain has no replacement and completes in place.
- **Crash.** Pumba's `kill --signal SIGKILL`. The recording must hold a signal-9 kill event and a
  die event with exit code 137, and the container must stay stopped for the whole declared
  outage.
- **Pause.** Pumba's `pause`, which freezes the container through Docker. The proof has three
  parts:
  - Docker inspection must show the container paused, then running, without a process restart.
  - The target's pause and unpause events in the recording supply the actual interval. A short
    pause outside 0.8 to 2 seconds, or a long pause outside 15 to 100 seconds, fails the
    experiment.
  - Recovery time is measured from the Docker unpause event, separately from the pause.
- **Restart.** Compose disables automatic restarts. The controller starts the same container ID
  explicitly and verifies its original image, named volume and address. It checks the final process
  start time against that explicit start, so a second crash cannot pass for recovery.

### Network Faults

A partition writes a plan before anything is installed. The plan names the directed node links to
block and the Pumba rules that block exactly those links, and the runner checks that the rules
imply exactly the planned links. The plan takes one of three forms:

- **Isolation.** The rules sit on the other nodes: netem drops their packets to the isolated
  address, and iptables drops packets arriving from it. The isolation therefore still holds while
  the isolated container restarts.
- **One-way loss.** One netem rule on the sending node.
- **Quorum loss.** Each node gets one netem configuration that covers both of its peers.

A partition plan targets only node-to-node links. The broker, load, observer, CLI and probe
containers keep their routes. Two scenarios install rules that are not a partition plan:

- [Backup Faults](#backup-faults) installs two netem rules, each on its own interface: an
  acknowledgement gate on the broker's egress to one node, and a rate limit on the leader's egress
  to the CLI's container.
- [Restore Faults](#restore-faults) delays the leader's egress toward both followers with one netem
  rule. Its `leader-partition` fault then installs an ordinary isolation of the leader beside that
  delay, and an INPUT rule on the leader that drops what the CLI's container sends it. That is the
  one installation that keeps another fault on an interface, so the run checks the leader's
  interface itself: exactly the delay and the client rule.

Installation follows a fixed sequence:

1. Every node interface must carry only its default qdisc and an empty INPUT chain, so faults are
   never stacked.
2. One Pumba container per node and rule kind, with its nettools helper, is dry-run against the
   exact node container and then started detached.
3. The runner reads each namespace's qdisc, u32 filters and INPUT rules until they match the plan
   exactly.

The runner then proves the effect with ICMP echo requests. For every directed node link, for the
verifier's route to each node, and for each node's route to the broker, it counts the requests the
receiving kernel accepted from one sender at a time. An intended link must accept none and every
other link must accept them. A Pumba exit without effective isolation therefore fails.

Healing also follows a fixed sequence:

1. The runner sends SIGTERM to every injector and requires exit code 0.
2. It requires every namespace back in its default state.
3. It measures the whole matrix open again.

### Link Degradation

A degradation profile applies one netem configuration to one directed link, the egress of the
sending node towards the fixed address of the receiver:

| Profile | Rule | Effect the run must measure |
| --- | --- | --- |
| `delay` | 180 ms | Average ICMP latency at least 100 ms above the healthy baseline, with at least 87.5% replies |
| `jitter` | 180 ms with 100 ms of normally distributed variation | Average latency at least 80 ms above the baseline and a spread of at least 70 ms, with at least 87.5% replies |
| `random-loss` | 30% independent loss | At least 7.5% loss and 12.5% replies, over 400 packets at 10 ms intervals |
| `burst-loss` | Pumba's four-state model, `p13=20`, `p31=15` | At least 7.5% loss, 12.5% replies, and at least two consecutive lost packets, over 400 packets at 10 ms intervals |
| `rate-limit` | 256 kbit/s | At least 75% replies, and a transfer at least twice as slow as healthy |
| `combined` | One `netem combine`: 180 ms delay, 60 ms jitter, 20% loss, 256 kbit/s | Average latency at least 80 ms above the baseline, at least 5% loss and 12.5% replies over 400 packets at 10 ms intervals, and a transfer at least twice as slow as healthy |

Before any fault, a run records a healthy ICMP and transfer baseline. Under a fault, it requires
the broker and every node's listeners to keep answering, but not their readiness. It also checks
two things against the plan: Pumba's exact selected container, and the single owned netem qdisc
and its peer filter. An installed qdisc alone cannot pass; the effect must be measured.

**ICMP samples.**

- The loss profiles, random, burst and combined loss, send 400 packets at 10 ms intervals. Every
  other profile, the healthy baseline and each healing check send 40 at 100 ms. Both send for about
  four seconds, and a sample that misses a reply waits one second more for it.
- The limits scale with the number sent. A large sample cannot turn four replies in forty into
  passing evidence, and a sample that is missing packets fails.
- Each ICMP sample runs in its own detached helper. Docker's start of the helper has a 60-second
  launch bound, and the sample then has its own 30-second bound, which a held start does not
  consume. Docker has been observed on GitHub-hosted workers holding a container's start for about
  30 seconds, which is why the two bounds are separate.
- The helper reports when its sample began and ended on the kernel uptime clock the controller also
  reads.
- `*.probe.json` keeps the launch, wait, ping, log and removal outcomes, with the launch and sample
  times. A failed observation names the stage that took the time and keeps the helper's raw
  streams and Docker's state and events for it.

**Sample size.** A loss floor is a claim about a random sample, so even a working fault misses it
with some probability. The larger sample keeps that probability negligible without lowering a
floor:

| Profile | Floor | Missed by a working fault, 40 packets | 400 packets |
| --- | --- | --- | --- |
| `combined` | 5% lost | 1.5 × 10⁻³, and 1.3 × 10⁻⁴ that it loses none | 1.1 × 10⁻¹⁸ |
| `random-loss` | 7.5% lost | 1.0 × 10⁻⁴ | 3.3 × 10⁻²⁹ |
| `burst-loss` | 7.5% lost | 9.6 × 10⁻⁴ | 4.8 × 10⁻²⁴ |
| `burst-loss` | 12.5% replies | 2.7 × 10⁻² | 1.3 × 10⁻¹⁰ |

Random and combined loss are independent per packet. Burst loss is Pumba's chain, which starts a
burst with probability 20% and ends one with 15%; its figures are for the less favorable of its two
states at the start of a sample. No other limit gives a working fault a comparable chance to fail:
a profile without loss need keep only 75% or 87.5% of its replies, and each latency limit sits far
below its rule's delay.

**The transfer.**

- The transfer sends 262,144 bytes through the pinned BusyBox netcat, with EOF half-close rather
  than an idle timeout. Sender and receiver share one 90-second deadline, which covers connection
  establishment, retransmissions, byte delivery and receiver completion.
- The receiver's launch, the Pumba dry run and the injector have the same 60-second launch bound as
  an ICMP helper.
- The sender records its kernel uptime just before connecting, and the receiver after counting the
  complete payload. Both must report the same boot identity and boot-time offset.
- `/proc/uptime` resolves 10 ms, so a duration's lower bound is its reported value less two clock
  ticks. The effect requires that lower bound under the fault to be at least twice the baseline's
  upper bound. This keeps the twofold contract under clock uncertainty, including a transfer inside
  one tick.
- A failed transfer is an observation failure that names both nodes and the receiver's address and
  port. Timing out the Docker client also removes its sender container.

**Healing.** SIGTERM heals each injector. The qdisc and ICMP latency must then return to their
healthy state.

These controller bounds never relax a product recovery deadline. Pumba parameters, hold times and
product deadlines do not change with the sample size. `just test-chaos-degraded-evidence` and
`just test-chaos-rate-probe` pin the counts, intervals and launch and sample bounds, and exercise
both sides of the inclusive thresholds.

The rate-probe self-test keeps its sender and receiver namespace owners alive until its cleanup
removes them. Their lifetime follows the whole control sequence, so a slow worker cannot expire a
namespace before a later control reaches it. Probe, sample and controller deadlines retain their
own bounds.

### Network Preflight

Before a network run creates its first node, the preflight proves the faults on this worker, within
120 seconds:

- A Pumba netem fault and an iptables fault must each block a throwaway canary container's gateway
  traffic.
- Each fault must heal on SIGTERM.

The pinned Pumba, nettools and probe images are already local from preflight, and Pumba's own helper
runs keep automatic pulling disabled. The rules inspection has a 30-second bound that includes the
Docker client and the helper's launch.

Netem installation is ready only after the inspection sees Pumba's `504d:` prio root, its
`5050:` leaf with 100% loss, and the destination classifier directing traffic to `504d:3`.
Pumba installs those parts separately. The existing 30-observation readiness loop waits for all
three before the blocked-traffic probe; a partial ruleset alone does not establish installation.

`diagnostics/network-preflight/` keeps the evidence:

- `result.json`, with the verdict, the failing stage, the original exit status, the elapsed time,
  helper identities and an independent cleanup outcome
- `operations.ndjson` and one numbered `operations/*.json` per command, written before the command
  starts, so an interrupted command stays identifiable beside its partial streams
- launch cidfiles, which keep a helper identifiable even when the Docker client fails after
  creating it

Cleanup removes only the run's canary, injector and the Pumba sidecars joined to run-owned
namespaces. It saves injector inspection and raw Pumba logs before removing a helper. A sidecar that
disappeared after enumeration is confirmed absent and recorded; a failed inspection of a
still-present sidecar fails cleanup.

The setup error reports the actual refusal. The runner does not infer that the worker lacks netem
or iptables from a Docker launch, inspection or timeout failure. Blocked traffic qualifies only
through BusyBox ping's missing-replies status. A Docker execution refusal or a command timeout is
no effect observation and fails with its own status. A helper that exited successfully does not
show that the Docker client finished within its bound.

`just test-chaos-network-preflight` qualifies the preflight itself with the pinned images. It works
in this order:

1. It prepares the pinned Pumba, nettools and probe images before it starts any fault.
   `image-preparation/<tool>/` records the reference, the inspection, any bounded pull and its
   confirmation, with native statuses and both streams. A preparation refusal ends the
   qualification before the first canary starts.
2. It verifies full-strength netem and iptables installation, blocked gateway traffic, SIGTERM
   healing and exact helper cleanup.
3. It runs its negative controls, each of which must fail with its own evidence:
   - an abrupt rules-inspection refusal
   - a real 30-second Docker-client timeout after the inspection helper exited
   - a failed injector-state inspection
   - a failed log retrieval and a failed cleanup
   - a Docker launch that created a canary before refusing
   - an injector that cannot start its nettools image
   - a sidecar that disappeared, as against one whose inspection failed while still present
   - a refused Docker execution of the blocked-traffic probe
   - a partial netem installation before its classifier, followed by the real injector's
     installation, blocked traffic and healing before the deliberate iptables inspection refusal

A control name, such as `probe-refusal`, runs that control with the live installation, effect and
healing check. `just chaos self-test` runs every control after the verifier checks, and CI runs them
once before each shard's timed suite, outside every scenario's budget. The recipe refuses an
existing evidence directory, so a stale launch cidfile can never become cleanup authority.
`just coverage-chaos-network-preflight IMAGE` adds the public runner's setup-error projection
against a built image, which ends before any node starts.

### The Live Docker Event Recording

Docker's buffer of recent events holds the daemon's most recent 256 events and is shared by every
container on the host. A replay through `docker events --since` returns only what remains of it, so
no verdict reads one.

Instead, before the run creates its first container, the controller starts a `docker events`
subscriber filtered to the run's label and places a labeled marker container. The recording starts
when the marker's creation appears in it. That proves the subscriber is live and that the daemon
stamps events on the controller's clock.

Each Docker-event check works on a window:

- The check closes its window with another marker and reads the window from the recording, for
  example a crash's kill and the node events through recovery, a pause interval, or a partition
  case's node events.
- Markers recorded in the stream must bracket the window, and `*.recording.json` keeps the bounds
  beside it.
- A recording that is missing, starts after a window opens, or ends before it closes fails the run
  as a controller failure. That includes a recording whose subscriber exited.

When diagnostics are captured, after every heal, a final marker closes the recording. The recording
must then cover the whole run within 64 MiB, and `diagnostics/docker-events.recording.json` keeps
its bounds. Because it holds the creation of every run-owned container, it is also the evidence of
[image attribution](#the-nervix-image-under-test). The subscriber of a controller killed before its
trap ran exits on its own, 15 minutes after the run's bound.

## Scenario Catalog

| Scenario | Nodes | Fault | Continuous load | Default records, bound |
| --- | --- | --- | --- | --- |
| `baseline` | 1 or 3 | None | None; one producer call | 24, 900 s |
| `backup` | 1 or 3 | A quiesced backup under load, verified offline and restored through the cluster as a stopped domain | 500 ms | 1,000, 1,800 s |
| `backup-cycles` | 1 or 3 | Eight quiesced backups under load, each verified offline, restored as a stopped domain and compared, with exact deduplicator and window verdicts | 500 ms | 1,000, 2,400 s |
| `backup-fault` | 3 | One fault against a quiesced backup: a SIGKILL of the state owner or the leader, or an isolation of the state owner, while an acknowledgement gate holds the cut; or a rolling restart while the archive streams | 1 s | 1,000, 2,400 s |
| `restore-fresh-cluster` | 3 | None; a quiesced backup restored with `RESUME` into a second three-node cluster | 1 s | 1,000, 2,400 s |
| `restore-fault` | 3 | One fault against a restore that a delay on the leader's links holds in staging: a SIGKILL of the leader, an isolation of the leader from its peers and the client, a SIGKILL of a state owner, or a SIGKILL of a state owner while a failed restore awaits reclamation | 1 s | 1,000, 2,400 s |
| `restore-volume` | 3 | None; three concurrent restores of an archive whose guest saves exceed the bulk budget, with every node's staging, storage and memory sampled | 1 s | 1,000, 2,400 s |
| `rolling-restart` | 1 or 3 | A graceful stop and restart of every node, leader first | 500 ms | 1,000, 1,200 s |
| `leader-crash` | 1 or 3 | SIGKILL of the leader | 500 ms | 1,000, 1,200 s |
| `follower-crash`, `ingestor-owner-crash`, `emitter-owner-crash` | 3 | SIGKILL of the role | 500 ms | 1,000, 1,200 s |
| `pause-resume` | 3 | Short and failover-length pauses of the leader and an execution owner | 1 s | 1,000, 1,800 s |
| `partition-recovery` | 3 | Follower, one-way, leader and quorum-loss partitions | 2 s | 1,000, 3,000 s |
| `degraded-links` | 3 | Six degradation profiles on one directed link | 750 ms, `--load-interval-ms` | 1,000, 2,400 s |
| `stale-follower` | 3 | An offline follower past log compaction | 1 s | 1,000, 1,800 s |
| `former-owner-restart` | 3 | A former owner restarted behind peer-side isolation | 1 s | 1,000, 1,800 s |
| `cluster-restart` | 1 or 3 | SIGKILL of every node | 1 s | 1,000, 1,800 s |
| `stateful` | 1 or 3; one node only with no fault or a cluster restart | None, or a crash, pause or partition of the state owner, or a cluster restart | 1 s, and `state-load` at 1 s | 1,000, 2,700 s |
| `endpoints` | 1 or 3; one node only with no fault or a cluster restart | None, or a crash or partition of the endpoint owner, or a cluster restart | 1 s, and HTTP endpoint intake at 250 ms | 1,000, 1,800 s |
| `lifetimes` | 1 or 3; one node only with no fault or a cluster restart | None, or a crash or partition of the state owner, or a cluster restart | 1 s, and `life-load` at 500 ms | 1,000, 2,700 s |
| `domain-time` | 1 or 3; one node only with no fault or a cluster restart | None, or each voter in turn crashed, paused, isolated or stopped, or a cluster restart | 1 s, and `paced-load` at 250 ms | 1,000, 2,700 s |
| `mixed-instability` | 3 | A seeded or explicit plan of the families above, alone and combined | 1 s | Twice the planned timeline plus 15 minutes of records, at most 20,000; the duration plus the larger of the duration and 20 minutes, at most 6 hours |

A fixture runs out when the load has produced its last record, so its size bounds how long faults
can be observed:

- In the crash scenarios, a smaller `--records` can exhaust the load before the fault is verified,
  and the run then fails as a setup limit.
- Degraded links refuse, up front, a fixture too short for the healthy baseline and each selected
  profile's fault window. The check does not budget each profile's recovery, so a fixture that
  passes it can still run out during a slow recovery, which also fails as a setup limit.
- Mixed instability sizes its fixture from its plan.

### Baseline

```bash
just chaos run baseline --image nervix:debian
just chaos run baseline --image nervix:debian --nodes 1
```

The baseline proves the deployment and the external verifiers before any fault is introduced. On
three nodes it places the ingestor, relay and emitter on three different nodes, so every record
crosses two node boundaries; on one node all three run on `node-1`. It produces its fixture,
reconstructs the accepted input from Kafka and requires an exact ledger with no duplicate. It
requires the consumer group to be committed through the source boundary. It also requires the
public metrics to count every record along the path:

- the ingestor's node, `node-1`, sent each record to the relay
- on three nodes, the relay's node received each one for the relay
- the emitter's node received each one for the emitter

`results/remote-path.json` records the owners and the metric evidence, and `results/baseline.json`
the verdict.

Before it deploys traffic, a standalone baseline executes its verifier self-check, which takes
minutes on a loaded worker. In a suite that check executes once, before the first run, within the
shard's [preparation budget](#shard-budgets), so a baseline entry's budget funds only its
execution: 180 seconds for the one-node baseline and 240 seconds for the three-node baseline, the
suite budgets of 3 and 4 minutes. Everything the baseline did after its verifier self-check,
teardown included, took 30 to 37 seconds on GitHub-hosted workers in the nightly soak of 22
October. These allowances change no product deadline, no output-stability window and no ledger
assertion.

### Backup Under Load

```bash
just chaos run backup --image nervix:debian
just chaos run backup --image nervix:debian --nodes 1
```

This scenario installs the baseline graph with the Kafka ingestor's
`OFFSET BY CONSUMER GROUP 'chaos_baseline'` replaced by `OFFSET BY DOMAIN`. The run therefore uses
domain-owned Kafka offsets, and the archive must hold those offsets as well as the branch lifecycle
state. A fixture without exactly one such clause stops the run with a setup error before the graph
is installed. Every verifier self-check derives the backup graph the same way, so a fixture change
that breaks this run fails every scenario's self-check.

The run proceeds in order:

1. It observes healthy ingestor and emitter progress.
2. While the load continues, the packaged CLI creates a quiesced archive of the domain. The report
   must show a completed quiesced cut.
3. The packaged CLI verifies the archive offline with `DESCRIBE BACKUP`. It must name the domain and
   its quiesced cut, and hold branch lifecycle state and the acknowledged Kafka domain offsets.
4. Through the cluster, the packaged CLI restores the archived domain as the new, stopped domain
   `chaos_restored`, backs that domain up and describes its archive offline. The run compares the
   two archives' public checkpoint identities, revisions, branch counts and exact Kafka positions.
   The run never starts the restored domain, while the source traffic continues.
5. After the cut, the runner requires renewed ingestor and emitter progress, stops the producer and
   checks the exact ledger, counting replay duplicates separately.

`results/backup-progress.json` records the cut's engagement and release times and its freeze
duration. `backup/` keeps both archives, both backup reports, the restore report, both archive
descriptions and the compared state inventories. This workload exercises one-node and three-node publication. The large guest-save and
post-`START` branch-isolation workloads run in the Cucumber and diagnostic suites.

### Backup Cycles

```bash
just chaos run backup-cycles --image nervix:debian
just chaos run backup-cycles --image nervix:debian --nodes 1
```

This scenario alternates eight quiesced backups and restores with continuous acknowledged Kafka
traffic. Beside the [backup graph](#backup-under-load), it installs `fixtures/backup-state.plexus`: an
attached deduplicator and a detached window processor on the graph's branched relay, each with its
own output topic.

- **The deduplicator** keys each branch's records by their sequence modulo 30. Exactly the records
  below sequence 30 are therefore unique, and every later record repeats a key its branch has seen.
- **The window** closes over twelve consecutive records of a branch.

Each cycle proceeds in order:

1. It waits for new acknowledged output.
2. The packaged CLI takes a quiesced backup and verifies the archive offline.
3. It restores the archive as the stopped domain `chaos_cycle_N` and backs that domain up again.
   For every lifecycle, offset, deduplicator and window checkpoint, it compares the public
   checkpoint identity, revision, branch count and exact Kafka position with the cycle's own cut.
4. It requires the archive's acknowledged `chaos_input` offset to be at or past the previous
   cycle's, so a cut never moves the domain's committed position back.

After the final source boundary, `verify-backup-state.sh` judges the deduplicator and window outputs
against the accepted-input ledger:

- every unique record and every closed window must appear
- every copy must equal its expected value
- nothing else may appear

A cut that lost or doubled a key or a retained row therefore fails the run. The verdict goes to
`results/backup-state.json`. The run then checks renewed traffic and the exact accepted-input ledger,
counting identical replays separately.

`backup-cycles/cycles.ndjson` records each cycle's freeze, backup command time, restore-and-compare
time and archive size. `results/backup-cycles-progress.json` records the freeze's minimum, median
and maximum.

### Backup Faults

```bash
just chaos run backup-fault --image nervix:debian --fault owner-crash
just chaos run backup-fault --image nervix:debian --fault leader-crash
just chaos run backup-fault --image nervix:debian --fault owner-partition
just chaos run backup-fault --image nervix:debian --fault rolling-restart
```

This scenario injects one fault into a quiesced backup under acknowledged Kafka traffic. It places
the backup graph across the three nodes:

- the Raft leader coordinates the backup
- one follower owns the ingestor and its relay, the domain's Kafka offsets and the branch lifecycle
  state
- the other follower runs the emitter

For the first three faults, an acknowledgement gate holds the cut open until the fault lands:

- **The gate.** Pumba netem on the broker drops every packet the broker sends to the emitter's
  node. The broker appends the batch the emitter publishes, but the emitter receives no
  acknowledgement. The ingestor's sequential acknowledgement then holds every later record behind
  it, and the cut's drain waits for that admitted publish.
- **Why it sits on the broker.** A node's interface carries only the fault itself, so a partition
  plan never overlaps the gate.
- **Injecting the fault.** The run first waits until the output stays unchanged across two
  observations while the source runs at least two records ahead. It then starts the backup
  through the emitter's node with a 30-second quiesce timeout. Once `DESCRIBE INGESTOR` reports
  `status: quiesced` and `quiesce state: domain pause`, it injects the fault, so the fault always
  lands inside a held cut.
- **Lifting the gate.** While the fault holds, the run records the source and output boundaries
  and lifts the gate, leaving the fault as the only obstacle. A cut the gate holds past its quiesce
  timeout fails. A publish held past the graph's 30-second `ACK TIMEOUT` is retried, and the ledger
  admits the retry as a replay.

The faults:

- **`owner-crash`** SIGKILLs the state owner and starts the same container again after the
  outage.
- **`leader-crash`** SIGKILLs the leader, which is the backup's coordinator, and starts it again.
- **`owner-partition`** isolates the state owner from both peers for 35 seconds, longer than the
  quiesce timeout, with rules on the peers, and then heals it.
- **`rolling-restart`** sets up a long download, then restarts every node while it streams:
  - It uploads an incompressible 16 MiB resource, so the archive carries it.
  - Before the CLI starts, it rate-limits the leader's packets to the CLI's container to
    512 kbit/s. Each 256 KiB download frame then takes four seconds, and the stream lasts
    minutes.
  - While the archive streams, it stops each follower gracefully in turn, and requires the
    download to keep growing while each is down.
  - It then stops the leader, which holds the archive.
  - The rate limit lived in the network namespace that the stopped leader container released.
    The run therefore kills its injector instead of asking it to remove rules from a namespace
    that no longer exists, and requires the restarted leader's interface to be back in its default
    state.

The fault backup must end in an outcome the backup contract allows:

- **Refusal.** The CLI reports a `backup failed:` reason, and neither the destination nor the
  client's `.nervix-backup-` staging file is left behind.
- **Completed archive.** It must verify offline, restore and compare like the
  [plain backup run](#backup-under-load).
- **After a leader crash.** The cut must have been engaged after the crash. A leader-tenure change
  refuses the archive of the cut it interrupts, and the new leader runs the backup again under the
  same execution reference.
- **Cut released too early.** A completed archive whose cut released before the gate began to lift
  breaks the drain contract, because the gate still withheld an admitted publish.
- **Interrupted download.** The failure must carry the backup's execution reference, and recovering
  by that reference must report that the restarted holder no longer retains the archive.

Whatever the outcome, the cluster must settle, the source and the emitter must move past the records
the fault found, and a fresh backup must verify offline, restore and compare. A product violation is
a recovery finding: the run still reaches the final exact ledger, then fails with every finding.

`results/backup-fault-progress.json` records:

- the placement, the outcome, the execution reference and any failure code
- the timings of the backup's start, the observed cut, the fault, the gate's lifting and the
  backup's end
- an isolation's installation, verification and heal, and the cluster's recovery
- the fresh backup's freeze, the time its command took to cut, assemble and download the archive,
  and the archive's size

For `rolling-restart` the archive carries the 16 MiB resource, so the download time measures the
archive's throughput to the client. `backup-fault/` keeps the fault's own evidence.

### Restore Into A Fresh Cluster

```bash
just chaos run restore-fresh-cluster --image nervix:debian
```

After a quiesced backup under acknowledged Kafka traffic, the run stops the original domain and
waits until its output has not moved for five seconds. It then starts a second Compose project,
`compose.fresh-cluster.yaml`, on the run's network:

- three nodes, `fresh-1` to `fresh-3`, served by `nervix-fresh-1` to `nervix-fresh-3`
- its own cluster ID and volumes
- certificates issued by the run's CA

Through the packaged CLI, the run restores the archive into that cluster with `RESUME`. The restored
ingestor must run on a fresh node, and the fresh cluster must keep consuming the same topics. Two
verdicts follow:

- **Kafka continuity.** The first record the fresh cluster emits must be the source record at the
  archived offset, and the records it emits must be exactly the source records from that offset
  onward. The run's final ledger covers both clusters' output, counting identical replays
  separately.
- **Deduplicator and window continuity.** The domain carries the deduplicator and window of
  `fixtures/backup-state.plexus`. The restored keys must keep suppressing the keys seen before the
  backup. Each window the cut left partly filled must close in the fresh cluster over rows from
  both sides of the backup. The `verify-backup-state.sh` verdict of
  [Backup Cycles](#backup-cycles) judges both clusters' deduplicator and window outputs together.

`results/restore-fresh-cluster-progress.json` records:

- the archived offset and the original output boundary
- the fresh cluster's leader
- the startup, restore and restore-to-running times

`restore-fresh/` keeps the restore's own evidence. On a [diagnostic image](#diagnostic-images), the
fresh nodes record and qualify their deadlock evidence like the original nodes. The exit trap
captures the fresh project's Compose logs beside the original project's.

### Restore Faults

```bash
just chaos run restore-fault --image nervix:debian --fault leader-crash
just chaos run restore-fault --image nervix:debian --fault leader-partition
just chaos run restore-fault --image nervix:debian --fault owner-crash
just chaos run restore-fault --image nervix:debian --fault reclamation-owner-crash
```

This scenario injects one fault into a restore while the restore stages runtime state, and judges
the outcome against the restore contract of
[Backup And Restore Architecture](./backup-and-restore-architecture.md#failure-semantics-at-every-boundary).
The domain
carries the graph of [Backup Cycles](#backup-cycles) and `fixtures/restore-state.plexus`:

- a Kafka ingestor with domain-owned offsets, which reads `chaos_padded_input`
- the detached WASM processor `chaos_padded`, branched like the baseline relay, whose guest
  `fixtures/wasm/padded-state-wat.sh` writes for the run. The guest saves exactly 2 MiB of
  pseudo-random bytes for every branch and emits nothing

The deployment keeps one state replica, so every guest save stages on an owner and a replica.

The run prepares the restore in order:

1. It starts the load, publishes one record for each of the three branches to
   `chaos_padded_input`, and waits until `DESCRIBE WASM PROCESSOR` reports a committed checkpoint
   of every branch that its replica confirmed.
2. It takes a quiesced backup under the load and verifies it offline, as in
   [Backup Under Load](#backup-under-load). The archive must also hold a 2 MiB guest save of every
   branch.
3. It starts the node sampler of [Restore Staging Volume](#restore-staging-volume) and reads the
   settled leader and followers.
4. It holds staging open with a delay. Pumba netem on the leader's egress toward both followers
   delays every packet by 100 ms, and the runner reads the leader's qdisc until it shows exactly
   that delay. The coordinator sends a checkpoint to a remote owner in ordered 64 KiB chunks, each
   awaited, so a guest save takes more than ten seconds to stage on a follower.
5. It starts the restore through the packaged CLI in the background, as the stopped domain
   `chaos_restored`. It injects the fault only once a follower's `nervix_restore_staging_bytes`
   reports staged bytes. A restore that ends first fails the run as an injection failure.

The faults:

- **`leader-crash`** restores through a follower and SIGKILLs the leader, the restore's coordinator,
  while it stages. The hold lived in the killed container's network namespace, so its injector is
  killed. The leader starts again after the outage. The CLI sends the archive again to the next
  leader under the same execution reference, and the next leader resumes the restore from its first
  step not recorded.
- **`owner-crash`** restores through the leader and SIGKILLs a follower that holds the restore's
  unpublished staging, then starts it again. The hold is lifted while the follower is down. The
  coordinator does not stage again on a node it lost, so a loss that interrupts a staging request
  fails the restore; when the follower's staging was already complete, the restore can publish once
  the follower is back. The verdicts below accept either end.
- **`reclamation-owner-crash`** reaches reclamation without a hook:
  - It cordons the leader, so both targets of every checkpoint are followers, and the deployment
    gives execution references a three-minute retry validity.
  - Once the restore stages on both followers, the run kills the CLI's container and then
    SIGKILLs the leader. No client sends the archive to the next leader, so the restore stays
    applying, and its staging retained, until the retry validity of its execution reference ends.
    The next leader then ends it as failed.
  - Before then, the run SIGKILLs the follower that did not become leader. It starts that follower
    again only once the new leader has reclaimed its own staging through a completed sweep. The
    restarted follower therefore learns of the failure as it catches up, and reclaims its staging
    after its restart.
- **`leader-partition`** stops the source domain and waits for its output to settle, cordons the
  leader and restores with `RESUME` through a follower. While the restore stages, the run isolates
  the leader:
  - The peers carry the rules of an ordinary [isolation](#network-faults).
  - An INPUT rule on the leader drops what the CLI's container sends it. Without that cut, the CLI
    would keep waiting on the isolated leader until the heal.
  - The leader keeps the hold beside them, which a plain partition would refuse as an overlapping
    fault, so the run checks this installation itself and then proves the effect matrix.

  The isolation holds until the CLI ends, at most 360 seconds. The CLI's reply wait ends after two
  minutes; it reconnects, is redirected to the next leader and sends the archive again under the
  same execution reference. The run then heals every rule, the hold included, and verifies each
  interface's default state and the open matrix.

The restore must end in an outcome the restore contract allows:

- **Completed.** The CLI exits 0 with a report that names the target and every step applied. After
  a leader crash or an isolation, the report is the resumed restore's. That proves the CLI sent the
  archive again under the same execution reference, since a new reference would be refused because
  the target already exists.
- **Failed.** The CLI reports `RESTORE_INCOMPLETE` with the one step that failed. `START` of the
  target must then be refused because its restore state installation is incomplete, and a restore of
  the same archive with the same options under a new name, `chaos_restored_2`, must complete.
- **Lost.** In `reclamation-owner-crash` no client waits for the outcome. `START` of the target must
  be refused in the same way, and a restore under a new name must complete.

Any other end is a recovery finding: a report without every step applied, a failure without exactly
one failed step, a refusal of a restore that created its target, or an end without a JSON report.
The run then judges what the fault left:

- **The restored domain equals the archive.** A completed, stopped restored domain is backed up
  again and described offline. Its checkpoint identities, revisions, branch counts and Kafka
  positions must equal the archive's, as in [Backup Cycles](#backup-cycles), and every guest save
  must have the archived length and BLAKE3 digest.
- **A resumed domain keeps running.** After `leader-partition` the restored domain runs, so its
  traffic is the verdict instead of a second archive. After the heal it must emit, consume past
  where the source domain stopped, and close two more windows of every branch. Its first record must
  be the source record at the archived offset, and it must emit exactly the source records from that
  offset onward. The stale coordinator's delayed staging or publication therefore neither replaced
  the generation the next leader completed nor cleared its runtime handles. The image exposes no
  publication generation and no count of refused requests, so the traffic is the only public
  evidence of that refusal.
- **Staging returns to zero.** On every node, two consecutive samples taken after the restores ended
  must report zero staging bytes and keys, with the count of completed sweeps rising between them.
  A restart restarts that count, so a pair across a restart does not qualify.
- **The source domain is unaffected.** After a crash, the cluster must settle and the ingestor and emitter
  must move past the boundaries recorded at the fault. Every fault ends with the exact ledger.
- **Deduplicator and window outputs.** After `leader-partition`, which crashes no node, the
  deduplicator and window verdict of [Backup Cycles](#backup-cycles) judges the outputs of both
  domains together. The crash faults do not judge them. When a crash loses the source domain's
  window owner, the replica's snapshot of the detached window can predate rows that relay fan-out
  already acknowledged upstream, and those rows are not delivered again; see
  [Shutdown And Recovery](./shutdown.md#when-the-former-owner-is-gone).

A fault during publication itself is not reachable without a hook: publication is one short request
to each node once staging completes. The Cucumber scenarios that pause a restore before state
publication carry that boundary: `A stale restore coordinator cannot republish after a new leader
resumes the archived lifecycle`, `A delayed coordinator cannot replace resumed deduplicator keys and
windows` and `A delayed coordinator cannot replace a resumed materialized generation larger than the
bulk budget`.

`results/restore-fault-progress.json` records:

- the placement, the fault's target and the hold
- each restore's outcome, execution reference and command time
- the restored domain and its comparison with the archive
- the times of the restore's start, the observed staging, the fault, the heal, the other target's
  reclamation and the settled staging

`restore-fault/` keeps the restore reports, the CLI's standard error, the staging observed at the
fault, the samples and the fault's own evidence.

The run's own bounds: 180 seconds for a follower to report staging, 900 seconds for each restore's
CLI to end, 300 seconds for staging to return to zero after the restores, and in
`reclamation-owner-crash` 420 seconds from the restore's start for the new leader to reclaim its
staging. None of them changes a product deadline.

### Restore Staging Volume

```bash
just chaos run restore-volume --image nervix:debian
```

This measurement entry runs three restores of one archive at once and samples every node while they
stage and publish. Its graph is the one of [Restore Faults](#restore-faults), with guest saves of
40 MiB for every branch, above the node's 32 MiB bulk budget, and one state replica. After the
quiesced backup, the archive holds three such saves. The run then:

1. Starts the node sampler. One probe container reads every node's `/metrics` every two seconds and
   records, stamped with its own clock:
   - `nervix_restore_staging_bytes`, `_keys` and `_limit_bytes`
   - `nervix_restore_staging_sweeps_total` and `nervix_restore_staging_reclaimed_bytes_total`
   - `nervix_restore_keyspaces_disk_bytes`
   - the `filesystem_storage` class of `nervix_execution_jobs_running` and
     `nervix_execution_jobs_pending`
   - the `bulk` and `restore_metadata` classes of `nervix_execution_memory_reserved_bytes`
   - `nervix_jemalloc_resident_bytes`

   A node that does not answer within the interval is recorded as unavailable for that sample.
2. Waits for two answered samples of every node, then starts three restores at once through the
   three nodes in turn, as the stopped domains `chaos_volume_1` to `chaos_volume_3`.
3. Requires every restore to complete, records each command's time, and waits until staging returns
   to zero on every node through completed sweeps, as in [Restore Faults](#restore-faults).
4. Takes two more samples of every node, stops the sampler and compares every restored domain with
   the archive.

Restored domains cannot be dropped, so the run measures the return of staging after the restores
complete, rather than after their targets are removed. `results/restore-volume-progress.json`
records the image's reference, ID and digest; the archive's size, guest save size, branches and
replicas; each restore's outcome, execution reference and command time; the time staging took to
return to zero; and for each node the peak and last value of every series before, during and after
the restores. `restore-volume/samples/samples.ndjson` keeps every sample. The values are
measurements of the worker the run used, not product limits.

### Rolling Restart

```bash
just chaos run rolling-restart --image nervix:debian
just chaos run rolling-restart --image nervix:debian --nodes 1
```

The load writes one record every 500 ms, an independent observer probes every listener, and Kafka
stays up throughout. The run stops the observed leader first and then each remaining node once,
each as a [graceful stop](#process-faults), and starts the same container again.

The observer probes each node's session and interconnect ports, `/livez`, `/readyz`, `/metrics`
and the web console every two seconds, and it reports a node unavailable when any probe fails.
While a node is stopped:

- within 20 seconds, the observer must report the node unavailable and the source broker must
  advance
- on three nodes, the survivors must agree on a caught-up leader within 90 seconds, and the emitter
  must then progress within 60 seconds

After the explicit start, the node's listeners must answer within 120 seconds and the observer must
report it ready within 30 seconds. Every node's listeners must then answer, within 30 seconds each.
Before the next stop, the run requires two things:

- within 150 seconds, agreed public leader and membership, caught-up applied Raft indexes, and
  settled ingestor and emitter state
- within 90 seconds after that, output progress

At the end the run stops the producer, waits for the consumer group and output boundaries, and runs
the exact ledger. A failed identity, stop, exit, deadline, listener or recovery check leaves the
manifest and the available evidence. `restarts/` keeps, for each target:

- its Pumba command and the Pumba image ID, beside the run's `pumba-version.txt`
- its before, stop and start inspections
- its stop and observer logs
- its measured stop and recovery times
- per-restart status, metrics and offsets

### Crash Scenarios

```bash
just chaos run leader-crash --image nervix:debian
just chaos run leader-crash --image nervix:debian --nodes 1
just chaos run follower-crash --image nervix:debian
just chaos run ingestor-owner-crash --image nervix:debian
just chaos run emitter-owner-crash --image nervix:debian
```

The controller reads the leader and the ingestor and emitter owners through the packaged CLI,
selects the corresponding Compose node, and checks its image, labels, restart policy and volume. It
then [kills](#process-faults) that node and holds it down for at least `--outage-seconds`. The
follower and owner variants need three nodes.

While the target is down, a three-node run requires three things, each waited for after the one
before:

- a caught-up survivor leader, within 90 seconds
- replacement owners for every graph node, within 120 seconds
- emitter progress, within 60 seconds

A one-node run requires listener, cluster and delivery recovery after the restart. In both
topologies, recovery proceeds in order after the explicit restart:

1. The target's listeners must answer within 120 seconds, and the observer must report it ready
   within 30 seconds.
2. Every node's listeners must answer, within 30 seconds each.
3. The cluster must settle within 150 seconds. The settlement time is measured from the restart.
4. Output must progress within 90 seconds.

Acknowledged control canaries from before and after the fault must stay visible, and the canary
after the fault must be acknowledged. A canary attempted during the outage records whether it was
acknowledged or uncertain, and its final effect. An acknowledged one whose effect is absent after
recovery fails the run.

`results/crash-progress.json` measures election, placement, outage, listener and settlement times;
election and placement are `null` where the fault does not require them. The exact ledger accepts
replay duplicates.

### Pause And Resume

```bash
just chaos run pause-resume --image nervix:debian
```

The three-node pause deployment uses a 1.5-second heartbeat, from which consensus derives a 9 to 18
second election window, and a 15-second unavailability timeout. The run verifies these in each
target's Docker inspection. It then runs four cases:

1. a one-second pause of the leader
2. a one-second pause of an execution owner
3. a 75-second pause of the owner
4. a 75-second pause of the leader

The short pause sits below the election and health thresholds and the long pause exceeds them.
Gossip uses an adaptive detector, so the short case checks that the observed leader and placement
stayed put rather than assuming that the settings prevent failover. A long case requires, while the
target is still paused:

- the survivors to agree on a caught-up leader other than the paused node, within 35 seconds
- every graph node to be owned on a survivor, within 35 seconds
- when the paused node ran the ingestor, the source consumer group to have members only on the
  relocated ingestor's node, one of which holds the partition, within 60 seconds of the pause
  request
- emitter progress, within 20 seconds of that handoff, or of the relocation when the paused node did not
  run the ingestor
- a control canary through a peer to finish before the unpause

A paused process neither leaves its consumer group nor answers for it, so the paused member keeps
its partition until its session expires. The relocated consumer joins the group at once, but Kafka's
group coordinator hands it the partition only after that expiry, `session.timeout.ms` after the
member's last heartbeat ([Kafka ingestors](./ingestors.md#kafka)). The baseline graph keeps
librdkafka's 45-second default. The 60-second bound adds 15 seconds for the rebalance and one read of
the group through the broker's CLI. Placement usually completes 22 to 26 seconds into a leader pause,
so an emitter window opened at placement would close before the partition can move.

After the long leader pause, the recovered leader must also differ from the paused one.

Broker load, listener observation, peer-side status and control canaries continue throughout. A
short pause can end before the listener observer samples it. The run records that limit and still
requires the leader and owners to stay put and every listener to recover. `pauses/` and
`results/pause-progress.json` keep per-case Docker, public, broker, observer and Pumba evidence.
Each round records its election, placement, group handoff and delivery times, and its
`group-handoff/` directory keeps the group read that showed the handoff.

### Partition And Recovery

```bash
just chaos run partition-recovery --image nervix:debian
just chaos run partition-recovery --image nervix:debian --case leader
just chaos run partition-recovery --image nervix:debian --case quorum-loss --partition-seconds 90
```

`--case all`, the default, runs four cases in one cluster:

- `follower` isolates the observed follower that owns execution
- `asymmetric` drops only the packets the leader sends to that follower
- `leader` isolates the observed leader
- `quorum-loss` leaves no two nodes able to exchange a packet

`--partition-seconds` sets the minimum verified isolation window. The deployment keeps the standard
heartbeat and unavailability settings and records them.

**During each fault.**

- Right after a [verified isolation](#network-faults), the run records the highest applied log
  index any node reports. A node cut off from every quorum must never apply past it for the whole
  fault.
- In quorum loss, no node may advance its Raft term.
- An isolated former leader may keep reporting itself leader. That label is recorded, and the run
  requires the connected majority to do three things: elect a caught-up leader within 60 seconds,
  acknowledge a canary within 30 seconds, and apply past the isolation boundary.
- In the asymmetric case, a majority must agree on a leader that reports itself leader, and a
  canary through the node outside the one-way link must be acknowledged.
- A canary sent to the isolated former leader, or to any node during quorum loss, is bounded to 20
  seconds and must not be acknowledged.
- A canary through an isolated follower may be redirected to the majority leader, so its outcome is
  recorded as a cluster observation.

**Ownership.**

- In the follower and leader cases, the majority must move the isolated node's execution within 90
  seconds. With the follower isolated, Kafka output must keep flowing.
- An isolated node keeps running work it was already admitted to, and Kafka can leave the source
  partition with the isolated former owner's consumer. The leader case therefore does not require
  majority delivery; it records the consumer-group members during the isolation. Members are
  attributed to nodes by their fixed addresses.
- At the end of every held fault except quorum loss, work owned by a node whose links stayed healthy
  must still be where it was.
- The follower case also SIGKILLs and restarts the isolated follower inside the partition. It
  requires the same container, volume and address, and measures the unchanged isolation and the
  node's status route again.

**After healing.**

- Within 150 seconds, every node must agree on a caught-up leader with every peer connected and no
  warnings.
- Within 90 seconds, Kafka membership must converge on the scheduled ingestor owner, and within
  another 90 seconds output must advance.
- A canary through the rejoined node must be acknowledged.
- The node events of the case must be none at all, or exactly the planned kill and restart.

`partitions/` keeps each case's plan, rules, link matrices, injector commands and logs, status
samples, canaries, consumer groups, node events and timings. `results/partition-progress.json`
summarizes them.

### Degraded Links

```bash
just chaos run degraded-links --image nervix:debian
just chaos run degraded-links --image nervix:debian --profile combined
```

The run degrades the directed relay-to-emitter link, from `nervix-2` to `nervix-3`. `--profile all`
runs the six [profiles](#link-degradation) one after another. The load runs at
`--load-interval-ms`. The runner refuses, up front, a fixture too short for the healthy baseline and
each selected profile's fault window. Before the first fault, it records the independently measured
healthy output rate and a healthy link. A rate below 0.5 records a second, or a healthy ICMP sample
missing any reply, fails the run as a setup error.

Repeated samples keep:

- all three nodes' public metrics
- Docker stats
- the broker's source and output boundaries and the estimated backlog
- interconnect pending work, relay attempts and resolved admissions
- TCP retransmissions in the sender's namespace
- reset and connection-failure retry indicators
- delivery latency sums and counts

`--max-backlog`, `--max-recovery-backlog`, `--max-memory-bytes`, `--max-pending` and
`--min-throughput-pct` are committed numeric inputs. A resource violation records the metric, limit,
profile, stage and time in `degraded/findings.ndjson`, and the run fails after the final
reconciliation.

Every healed profile must recover before its `--drain-seconds` deadline, in four ways:

- progress in each of three consecutive intervals
- reach the declared percentage of the healthy rate over their full window
- keep the backlog at or below the recovery limit
- return to a settled, connected cluster: an agreed caught-up leader, every peer connected and no
  warnings

A missed deadline fails at once. It leaves a profile-local `recovery-deadline.json` with the
baseline, the required and observed windows, the latest backlog, the sample timeline and the action
timeline, and its finding distinguishes a short observation window from stalled or slow output.
Source offsets and the exact ledger must reconcile after the load stops.

The run keeps:

- `degraded/worker.json`
- the image and load inputs in `manifest.json`
- `degraded/baseline.json`
- each profile's rule, effect and healing evidence
- the raw samples
- the action timelines in `phases.ndjson` and `degraded/actions.ndjson`
- `results/degraded-progress.json` and `results/degraded-links.json`

`--profile` repeats one effect on the same worker and image.

### Stale Follower

```bash
just chaos run stale-follower --image nervix:debian
```

This deployment sets two ordinary retention options, a snapshot entry threshold of 64 and 16
covered log entries retained, and checks them in every node's Docker inspection. The run proceeds in
order:

1. It selects the observed follower that owns execution, acknowledges a canary, and gracefully
   stops that follower. The drain must complete, because both survivors are live replacements. A
   drain that does not complete is a product finding, which fails the run after the catch-up checks.
2. The follower's own consensus metrics before the stop, and its `raft transition: state=Shutdown`
   line, record the last log index it held. The survivor leader's `nervix_consensus_log_last_index`
   bounds that index from above once the survivors agree on a leader.
3. While the follower is offline, the survivors must agree on a caught-up leader within 90 seconds,
   take over its execution within 120 seconds, and keep delivering.
4. Through the survivor leader, the packaged CLI makes five structural changes, and each must be
   acknowledged:
   - it creates the stopped domain `chaos_stale`
   - it creates the schemas `chaos_stale_kept` and `chaos_stale_dropped` in that domain
   - it drops `chaos_stale_dropped`
   - it relocates the relay onto the survivor that does not own it
5. Rounds of eight `CREATE RESOURCE` changes follow, one CLI session per change, until every
   survivor's `nervix_consensus_log_purged_index` exceeds the upper bound. The follower can then no
   longer catch up from the log. Twelve rounds without that fail the run, and
   `stale/compaction.ndjson` keeps every round's positions.
6. The runner starts the same container. Within 120 seconds, the follower's
   `nervix_consensus_log_snapshot_index` must cover the survivors' purged index, and its
   `raft.last_applied` must reach the index the leader had applied at the restart. The survivors'
   `nervix_interconnect_requests_total{operation="snapshot",outcome="answered"}` must rise, which
   shows the snapshot transfer itself.
7. Every acknowledged change must then be visible through the restarted node's own route:
   - the domain line in `SHOW CLUSTER STATUS`
   - the kept schema's `SHOW CREATE` text, and the dropped schema's absence
   - the relay's new owner, in `DESCRIBE RELAY` and in the node's schedule
   - every created resource
   - all three voters

   A change whose outcome was uncertain must read the same through the restarted node as through the
   leader.
8. Finally, the cluster must settle with every peer connected within 150 seconds, output must
   advance, and a canary through the restarted node must be acknowledged. The node events must be
   exactly the follower's signal-15 kill, its exit 0 and the explicit start. The survivors must keep
   their process incarnations and every volume its identity, and the exact ledger must pass.

`stale/applied.json` holds each change, its outcome and its effect as read through each node.
`results/stale-follower-progress.json` summarizes the policy, bounds, positions and timings.

### Former Owner Restart

```bash
just chaos run former-owner-restart --image nervix:debian
just chaos run former-owner-restart --image nervix:debian --isolation-seconds 90
```

The run proceeds in order:

1. It relocates the ingestor, relay and emitter onto the observed follower. That node must hold the
   source partition, which the consumer group shows by its fixed address, and must emit output.
2. After a settled read confirms the leader and all three owners, the run kills that former owner.
3. While the former owner is down, the survivors must take its work over. An acknowledged
   `RELOCATE` then moves each unit to the survivor that failover did not choose. The consumer group
   must run only on the new scheduled ingestor owner, while the former owner's stored schedule still
   names it the owner of everything.
4. Peer-side rules on both survivors drop the former owner's address in each direction. They are
   verified before the run starts the stopped container again from its image and volume, at the same
   address. The rules and the ICMP matrix then prove that the former owner reaches neither peer,
   while the verifier and broker routes stay open.
5. For at least `--isolation-seconds`, the run samples the former owner about every five seconds,
   with no gap over 20 seconds. Every sample must show:
   - its session, interconnect, console and observability listeners and its own status route
     answering
   - no consumer-group member at its address
   - no `nervix_messages_total` traffic
   - an applied index that never advances
   - no `runtime execution admitted after linearizable consensus catch-up` line in its log

   Its readiness and the schedule its own status reports are recorded. The survivors must keep
   delivering throughout the window.
6. Healing must restore default rules and an open matrix. Within 90 seconds, the former owner must
   then log runtime admission, no earlier than the start of the heal and after logging that it
   waited for linearizable consensus catch-up.
7. Within 150 seconds, every node must agree on a caught-up leader with every peer connected, and
   the former owner must report the changed owners through `DESCRIBE` and its own schedule. The
   consumer group must run only on the scheduled ingestor owner, output must advance, and a canary
   through the former owner must be acknowledged. The node events must be exactly its kill, exit
   and start.

`former-owner/` keeps the samples, admission timing, rules and matrices, and
`results/former-owner-restart-progress.json` summarizes them.

### Cluster Restart

```bash
just chaos run cluster-restart --image nervix:debian
just chaos run cluster-restart --image nervix:debian --nodes 1
```

The run proceeds in order:

1. After the cluster settles, the run acknowledges a canary. It reads each node's committed
   configuration through that node's own route: its domains, its voting membership, the
   `SHOW CREATE` text of every model of the graph, and the canary. All nodes must agree.
2. One Pumba kill names every node container. The recording must hold one signal-9 kill and one exit
   137 for each. The broker keeps accepting the independent producer's records while no node runs.
3. After at least `--outage-seconds`, one `docker start` starts every original container, and each
   must keep its image and named volume. The volumes' names, creation times and mount points are
   compared. A sampler started before the restart reads every node's own `SHOW CLUSTER STATUS`
   several times a second for 40 seconds.
4. Listeners must return within 120 seconds, the cluster must settle within 150 seconds, and output
   must resume within 90 seconds.

Ownership is judged against the [voter observation
grace](./control-plane.md#planned-ownership-handoffs-and-failover): a leader waits ten seconds from
its own process start for gossip to observe every voter before it fails over the work of a voter it
has not heard from. When every node that led after the restart observed every voter live within ten
seconds of its own Docker start event, each owner must keep its work. Otherwise failover is
permitted, and the verdict says which case applied.

Each node must report the same committed configuration as before the crash. The consumer group must
run only on the scheduled ingestor owner, and a canary after the restart must be acknowledged and
visible through every node. Each node's events must be exactly its kill, exit and start. The exact
ledger shows that every accepted record was delivered from the replayable source.
`results/cluster-restart-progress.json` records:

- the Raft, liveness and retention settings read from Docker inspection
- the ten-second grace and the external bounds
- the start times and the ownership verdict

The stale-follower, former-owner and cluster-restart scenarios share one findings channel. A product
violation that leaves the run meaningful goes to `results/recovery-findings.ndjson`, and the run
continues through recovery and the final ledger before it exits nonzero. These are process crash and
restart experiments on one host. The host, its page cache and its disks survive, so they establish
nothing about host power loss or storage corruption.

### Stateful

```bash
just chaos run stateful --image nervix:debian
just chaos run stateful --image nervix:debian --nodes 1
just chaos run stateful --image nervix:debian --fault owner-crash
just chaos run stateful --image nervix:debian --fault owner-pause
just chaos run stateful --image nervix:debian --fault owner-partition
just chaos run stateful --image nervix:debian --fault cluster-restart
just chaos run stateful --image nervix:debian --nodes 1 --fault cluster-restart
```

**The graph.** `fixtures/stateful.plexus` runs in the baseline domain beside the baseline path, which
keeps its own traffic and exact ledger. A separate kcat container produces records to
`chaos_state_input`, one a second and one kcat call per record. Two branches, `alpha` and `beta`,
interleave strictly by sequence, and every record carries its branch index. An `ACK PARALLEL`
ingestor with its own consumer group feeds four processors, each with its own attached Kafka emitter
and output topic:

- **The deduplicator `chaos_dedup`.** Every fourth record of a branch repeats the key of a record 3,
  23 or 79 positions back. Both branches name keys after the same indexes, so equal keys of
  different branches must never deduplicate each other.
- **The detached window processor `chaos_window`.** It aggregates 12 rows of a branch. It reports
  the smallest and largest index, the sum of the sequences, and the sum of 4 raised to each index
  modulo 24. The base-4 digits of that last sum tell which rows the window held, and how often.
- **The junction `chaos_enrich`.** It reads the branch's row of the materialized relay
  `chaos_profiles`, which a second ingestor fills from `chaos_profile_input`. Its `DEFAULT` names no
  branch.
- **The WASM processor `chaos_counter`.** Each branch's guest counts the rows it has processed,
  saves that count as its state, and emits every row with all of its input fields and the count.

The deployment uses a one-second state snapshot interval and, on three nodes, one state replica, so
the loss of an owner promotes a replica of each processor's state instead of resetting it. A pause
fault also deploys the pause scenario's heartbeat and unavailability settings. On three nodes, one
acknowledged `RELOCATE` moves the stateful entities onto the observed follower that the faults
target, and both stateful ingestors onto another node. A fault on the state owner therefore leaves
the Kafka consumers in place.

**The milestone.**

1. The run loads profile version 1 for both branches and requires
   `SHOW RELAY chaos_profiles MATERIALIZED STATE` to show it before any record is produced. It
   publishes version 2 after 40 records.
2. At 80 records it asks the load to hold, and the load holds before its 101st record. At that
   point each branch has two rows in an open window.
3. The milestone requires all of these:
   - the stateful consumer group committed through all 100 records, and the profile group through
     its end
   - every stateful output unchanged
   - `DESCRIBE WASM PROCESSOR chaos_counter FORMAT JSON` reporting both branch checkpoints committed
     at their latest revision and confirmed by every assigned replica
   - the materialized relay showing version 2 for both branches
4. The run then holds the drained state for five configured snapshot intervals, read from Docker
   inspection, and requires the outputs to stay unchanged throughout.

`stateful/milestone.json` keeps that evidence. The image exposes no publication milestone for
deduplicator, window or materialized relay state. Their durability verdicts therefore rest on this
drained and held boundary rather than on an observed publication, and the result says so.

**The fault.** The load resumes for four more records and holds again, and version 3 is published.
The runner reads the fault's boundaries while the load holds, which takes several broker round trips.
It releases the load as the fault begins, so the window rows open at the milestone are still open
when the fault strikes. `--fault` selects the fault:

- `none` holds no fault and allows no deviation at all
- `owner-crash` kills the state owner, requires the survivors to settle and the stateful work to
  leave it, holds the outage for `--outage-seconds` (default 8), and starts the same container
- `owner-pause` pauses the state owner for `--outage-seconds` (default 45, from 16 to 99), and
  requires the failover to happen during the pause
- `owner-partition` isolates the state owner with verified peer-side rules for at least
  `--outage-seconds` (default 45)
- `cluster-restart` kills every node and starts each from its own volume

Recovery requires a settled, connected cluster, an owner for every stateful entity and advancing
stateful output. The run continues until each branch has seen two things: the duplicates that refer
79 positions back to keys durable at the milestone, and at least 48 records after recovery. It then
stops the load and waits for the stateful group to commit through the final boundary.

**The verdicts.** Records are classified by sequence: before the milestone; in the volatile
interval from the milestone to the end of the fault's recovery; and after recovery.
`verify-state-evidence.sh` judges each processor against the accepted-input ledger, with three
expectations:

- **Replays.** An identical replay of a record after a fault is a replay duplicate, counted
  separately. Before a fault, or in a run without one, a replay is a violation.
- **Durable state.** State the milestone made durable must survive:
  - a later duplicate of a key first seen before the milestone must be dropped
  - the two rows each branch held open at the milestone must be aggregated exactly once, in a window
    that closes after the fault
  - a record after the milestone must carry at least profile version 2
  - every guest count must reach at least its branch index, because the checkpoint the guest
    restores reflects every acknowledged input
- **The volatile interval.** It permits what periodic checkpoints and replayed acknowledgements
  allow: a duplicate of a key first seen there may pass; a window row from it may be lost or
  aggregated twice; its profile version may fall back to version 2; and a guest may count the same
  replayed record repeatedly. A restored checkpoint may already reflect those inputs, including
  callbacks whose output was not observed. As the [WASM replay contract](wasm-state.md#replay-duplicates-and-exactly-once)
  permits repeated delivery, neither the number of distinct volatile inputs nor the observed output
  duplicates bounds the guest's count excess. The verdict records observed duplicates and
  post-recovery excess separately; it does not infer an unobserved callback count.

Every verdict also requires:

- the records' own content and branch
- the first occurrence of every key of each branch
- every record in the enriched and counted outputs
- windows of 12 rows of one branch, with the sums they report
- guest counts that equal the branch index before the fault and advance by one per record after
  recovery
- never the profile default or another branch's profile
- before a fault, every window made of 12 consecutive rows

A durability verdict that its fault did not exercise fails as missing evidence. An example is a run
that ended before a duplicate of a milestone key arrived after recovery. `stateful/verdict-*.json`
keeps each processor's verdict. A failing verdict is a product finding, and the run continues to the
baseline ledger before it exits nonzero. `results/stateful-progress.json` records the deployment,
boundaries, milestone, timings and every verdict.

### Endpoints

```bash
just chaos run endpoints --image nervix:debian
just chaos run endpoints --image nervix:debian --nodes 1
just chaos run endpoints --image nervix:debian --fault owner-crash
just chaos run endpoints --image nervix:debian --fault owner-partition
just chaos run endpoints --image nervix:debian --fault cluster-restart
just chaos run endpoints --image nervix:debian --nodes 1 --fault cluster-restart
```

**The graphs.** The baseline path keeps its own traffic and exact ledger in its domain. Two more
domains, `chaos_edge_primary` and `chaos_edge_peer`, each install `fixtures/endpoints.plexus`. Each
declares the VHOST `chaos-edge.example` and the HTTP endpoint `/events` on it, so the two domains
share one host and path. In each domain, an `HTTP ENDPOINT` ingestor in `MODE ACK MAX 64 ACK TIMEOUT
10s` feeds a relay and an attached Kafka emitter, each with its own output topic. The primary also
installs `fixtures/endpoints-websocket.plexus`: a `WEBSOCKETS ENDPOINT` ingestor in `MODE NO ACK` on
`/stream`, with its own relay, emitter and topic. Endpoint ingestors serve on every node. On three
nodes, one acknowledged `RELOCATE` in each domain moves the relays and emitters onto the observed
follower that the faults target.

**The client.** `endpoint-client.py` runs in the pinned Python image and sends its records through
every node's HTTP listener in turn, one record due every 250 ms. It finishes every attempt of a
record before it sends the next one. An attempt without HTTP 200 is attempted again through the
next node, since the sender owns retry. Every attempt is one line of its ledger, with its node, its
status, its retry delay and its outcome object, or the error that ended it. The runner holds and
releases the client at record numbers, and a record's sequence is its number, so each phase
boundary is a record.

**The phases.**

1. **Shared route.** Records 1 to 40 reach both domains.
2. **Withdrawn route.** The run stops the peer domain, and records 41 to 80 reach the primary
   alone. The route stays published for the primary while the peer's binding is withdrawn.
3. **Shared again.** The run starts the peer domain again, and records 81 to 120 reach both.
4. **WebSocket.** While the HTTP client holds, a second client opens a WebSocket through a node
   other than the endpoint owner and sends 20 frames. The run then stops and starts the primary
   domain, which replaces the source. The retained connection then sends one more frame, and
   the endpoint must close it with code 1013. The client then opens a new connection and sends
   20 more frames.
5. **The fault.** The HTTP client sends records 121 to 160, then the rest of its 400 records while
   the fault strikes. `--fault` selects the fault:
   - `none` holds no fault
   - `owner-crash` kills the endpoint owner, requires the endpoint work to leave it while it is
     down, holds the outage for `--outage-seconds` (default 8), and starts the same container
   - `owner-partition` isolates the endpoint owner with verified peer-side rules for at least
     `--outage-seconds` (default 45), and requires the endpoint work to leave it
   - `cluster-restart` kills every node and starts each from its own volume

   During the fault, a request through the faulted node, or one whose graph acknowledgement
   cannot complete, ends without HTTP 200. The client attempts it again through the next node
   until a node accepts it.

**The verdicts.** `verify-endpoint-evidence.sh` judges each domain's HTTP output against the
client's ledger, and the WebSocket output against the WebSocket client's ledger. `START` and `STOP`
complete only once every node's listener has its route or has withdrawn it, so a record
belongs to exactly one phase.

- **Delivery.** Every record answered with HTTP 200 while a domain's route was bound reaches that
  domain's output, with its exact content.
- **Withdrawal.** No record of the peer's withdrawal reaches the peer.
- **Duplicates.** A record may reach a domain more than once, or without a 200, only when one of
  its attempts ended without an answer, or with an answer that does not refuse admission. A 404,
  a 400 or a `not_admitted` outcome refuses admission.
- **Completion.** Every record is eventually answered with HTTP 200, none is abandoned at the
  client's four-minute deadline, the client reaches the end of its input, and no bound route
  answers 404.
- **WebSocket.**
  - Both connections are accepted.
  - Every frame of the retained and the new connection reaches its output exactly once, with its
    exact content.
  - The frame sent after the replacement is refused with code 1013 and never delivered.

`endpoints/verdict-*.json` keeps each verdict. A failing verdict is a product finding, and the run
continues to the baseline ledger before it exits nonzero. `results/endpoints-progress.json` records
the placement, phases, fault, outputs and every verdict.

### Lifetimes

```bash
just chaos run lifetimes --image nervix:debian
just chaos run lifetimes --image nervix:debian --nodes 1
just chaos run lifetimes --image nervix:debian --fault owner-crash
just chaos run lifetimes --image nervix:debian --fault owner-partition
just chaos run lifetimes --image nervix:debian --fault cluster-restart
just chaos run lifetimes --image nervix:debian --nodes 1 --fault cluster-restart
```

**The graph.** `fixtures/lifetimes.plexus` runs in the baseline domain beside the baseline path,
which keeps its own traffic and exact ledger. A separate kcat container produces records to
`chaos_life_input`, one every 500 ms. Four branches, red, blue, gold and green, interleave strictly,
and every record carries its index within its branch. Three branch declarations govern lifetimes:
one with a ten-minute TTL and a ceiling of 16 instances, which a branched window that computes
sketches requires and four branches never reach, one that expires after 20 seconds without
activity, and one bounded to two instances with `EVICT LRU`. The life ingestor's relay feeds four
paths, each with its own attached Kafka emitter and output topic:

- **The junction `chaos_life_enrich`.** It reads the branch's row of the materialized relay
  `chaos_life_profiles` with `REQUIRED WAIT`. A profile ingestor fills that relay from
  `chaos_life_profile_input` with `TIMESTAMP AT updated_at`, so the run chooses each row's
  timestamp, and `LAST BY TIMESTAMP` keeps a branch's row only for a strictly newer one.
- **The generator `chaos_life_generator`.** It emits each branch's materialized row every second.
- **The detached window `chaos_life_window`.** It aggregates each branch's records in tumbling
  six-second windows: their count, bounds and sum, an `APPROX_COUNT_DISTINCT` of the index modulo 3,
  and an `APPROX_QUANTILE` median. A window that computes sketches takes duration bounds.
- **The reingestor `chaos_life_rotate`.** It moves every record onto the LRU-bounded branches, which
  four branches keep evicting.

Four pipelines have their own sources, which the run produces to directly:

- **The expiring path.** A junction with `REQUIRED WAIT` reads the expiring relay's green row.
- **The late path.** Its emitter writes to `chaos_life_late_output`, which the run creates only
  after the emitter has reported its absence.
- **The buffered path.** Its route flushes every 300 seconds.
- **The pooled path.** Two attached Postgres table emitters write every record to their own table
  in the `postgres` container, with `ON CONFLICT ('event_id') DO NOTHING`. Both borrow from the
  client `chaos_life_pg`, whose pool holds one connection on each node. The run creates both tables
  before the graph starts.

On three nodes, one acknowledged `RELOCATE` moves every processor, relay and emitter of the
materialized, generated, window, rotating, expiring and pooled paths onto the observed follower that
the faults target, and a second moves their ingestors onto another node. The run reads every
entity's owner from `DESCRIBE RELOCATION ... ONTO NODE ... IGNORE PREFERENCES`, a dry run that moves
nothing, because `DESCRIBE` has no form for a generator. The deployment uses a one-second state
snapshot interval and, on three nodes, one state replica.

**The phases.** The run holds the life load after a record count, waits until the source has
acknowledged every record before it, and only then moves the hold, so each record's expected
profile is exact.

1. **Loaded profiles.** Red, gold and green publish version 1 at second 10 of the run's base minute,
   and `SHOW RELAY chaos_life_profiles MATERIALIZED STATE` must show them and no blue row.
2. **Parked records.** Blue has no profile, so blue's first six records park on `REQUIRED WAIT`.
   The load holds after 24 records. The source's committed offset stays below blue's first record.
   Blue then publishes version 1, and the source acknowledges through record 24.
3. **Timestamps.** Red publishes four more versions, and the run requires the relay's state after
   each one:

   | Red version | Second of the base minute | Red holds | Records produced after it |
   | ---: | ---: | ---: | --- |
   | 2 | 20 | 2 | 25 to 40 |
   | 3 | 15, lower | 2 | 41 to 56 |
   | 4 | 20, equal | 2 | 57 to 72 |
   | 5 | 30 | 5 | 73 onward |

   The run waits until the profile source has acknowledged each publication before it requires the
   state.
4. **A missing topic.** Five records reach the late path. Its emitter must report `status: ERROR`
   in `DESCRIBE EMITTER` while its topic is missing. The run then creates the topic, and the source
   must acknowledge every record and the emitter return to `status: OK`.
5. **A force flush.** Six records reach the buffered path. Once the nodes' metrics count them
   received, the output must still be empty. `ALTER KAFKA TOPIC INGESTOR chaos_life_flush_ingestor
   SET GENERAL ERROR IGNORE` then holds the ingestor and force-flushes its buffer, and the records
   must arrive within 60 seconds. A second alteration finds the pipeline idle.
6. **A full pool.** Four pooled records reach both tables. The run then locks both tables in one
   transaction and produces eight more. One emitter's insert must wait for its table lock while it
   holds the pool's one connection, and the other emitter must report `status: WAITING` with
   `waiting for a connection from client 'chaos_life_pg'` in `DESCRIBE EMITTER`. Meanwhile
   `pg_stat_activity` must show at most one connection of the client from each node. The locks
   hold through the fault.
7. **Expiry.** The expiring relay's green row publishes version 1, and eight green records carry it.
   The row must then disappear from `SHOW RELAY chaos_life_expiring_profiles MATERIALIZED STATE`
   within 150 seconds. Every entity that holds the branch releases it at its own scan, so the run
   then waits up to 150 more seconds for `nervix_branch_evictions_total`, summed over the nodes, to
   count a TTL eviction of that branch.

**The fault.** The load runs on while the fault strikes. `--fault` selects it:

- `none` holds no fault
- `owner-crash` kills the state owner, requires the work to leave it, holds the outage for
  `--outage-seconds` (default 8), and starts the same container
- `owner-partition` isolates the state owner with verified peer-side rules for at least
  `--outage-seconds` (default 45)
- `cluster-restart` kills every node and starts each from its own volume

After the fault, the materialized relay must hold red version 5 and version 1 of every other branch.
A pooled emitter must report its wait for the full pool again, now from the work's current owner,
so the fault ended the waits it interrupted without losing the records they held.

**Recreation.** The expired green row must still be absent, so neither restored nor replicated
state brought it back. Eight more green records then park until green publishes version 2, and they
must carry it. The run then ends the table locks, and both pooled emitters must write all twelve
records to their tables and return to `status: OK`. The run continues for 80 more life records,
then drains every path.

**The verdicts.** `verify-lifetimes-evidence.sh` judges each path against the accepted-input ledger
of its source. Records past the milestone of record 88 may be replayed identically after a fault,
because a fault can interrupt an acknowledgement after its record was written. Every other
deviation is a violation.

| Path | Requires |
| --- | --- |
| `enrich` | Every record exactly once, with its own fields and its branch's profile. Blue's parked records carry version 1. Red carries version 1 through record 24, version 2 through record 72 and version 5 after it, never version 3 or 4 |
| `generator` | Only profiles their branch held, versions that never go back, and at least three rows of every branch |
| `window` | A contiguous run of one branch's records, with exact count, bounds and sum. The distinct count is between 1 and the smaller of 3 and the count, and the median lies within the window's bounds. The closed windows aggregate every record exactly once, without a gap. After a fault, a window that holds a record between the milestone and the recovery is exempt, with every record it holds |
| `rotation` | Every record exactly once through the LRU-bounded relay, which evicted at least one branch |
| `expiring` | Every record exactly once, version 1 before the expiry and version 2 after the recreation, and a TTL eviction of the branch |
| `late` | Every late record exactly once after the emitter recovered |
| `flush` | Every buffered record exactly once, held until the alteration and force-flushed long before its flush interval |
| `pool` | Every pooled record exactly once in each table. An emitter reported its wait for the full pool, and after a fault reported it again. No node held more than one connection of the client while the emitter waited |

`lifetimes/verdict-*.json` keeps each verdict. A failing verdict is a product finding, and the run
continues to the baseline ledger before it exits nonzero. `results/lifetimes-progress.json` records
the placement, boundaries, fault, outputs and every verdict.

### Domain Time

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

**The setup.**

- The runner creates the paced domain `chaos_paced` with `PERIOD 1s SKEW 1s`, installs
  `fixtures/paced.plexus`, and starts it with `START AT NOW TIME RATE 4.0`, so logical time runs four
  times faster than physical time.
- A separate kcat container produces two interleaved branches to `chaos_paced_input` every 250 ms.
  The ingestor stamps each record with `TIMESTAMP NOW` and `received_at = now()`.
- A detached window of `WIDTH 8s DURATION` closes on that logical clock and records when it opened
  and closed.
- One independent observer per node, a container of the image under test, follows the domain's
  clock through that node's session route with the packaged CLI's `domain-clock`. It stamps every
  line with the host's clock and attaches again when its follow ends.

**The faults.** `--fault` selects the fault, and a voter fault runs one round per voter in node
order:

- `voter-crash` kills the voter and starts it again after `--outage-seconds` (default 8)
- `voter-pause` pauses it for `--outage-seconds` (default 30, from 16 to 99), under the pause
  deployment
- `voter-partition` isolates it with peer-side rules for at least `--outage-seconds` (default 30)
- `voter-stop` stops it gracefully and requires a completed drain, so the voter hands the paced graph
  to another node through the leader before it exits. It starts the voter again after
  `--outage-seconds` (default 8). A drain that does not complete is a product finding
- `cluster-restart` kills every node and starts each from its own volume
- `none` observes the healthy domain

Before each round, one acknowledged `RELOCATE` moves the paced graph onto another node, or onto the
voter itself for a graceful stop, and the cluster must be settled and connected. While a fault holds
away from the graph, its windows must keep closing. After each round, the cluster must settle, every
observer must tick again, and windows must close again.

**The clock verdict.** `verify-clock-evidence.sh` judges what the observers recorded, with these
requirements:

- **Generation and mapping.** Every attach and every state report after a reconnection must carry
  the first report's generation and mapping. No `START` follows the first, so the generation never
  changes.
- **Ticks.** Every tick must lie on the mapping's period grid, with its boundary at
  `logical origin + (id - 1) * period`, in that generation. The boundary must have been reached by
  the mapping at the authority's UTC observation. The serving-node logical reading must not
  precede the boundary and must lie between the mapping's projections at authority UTC and at the
  independent observer's receipt timestamp, with a ten-logical-millisecond measurement tolerance.
  The containers share the Docker host's UTC. Accepted progress and the serving reading are
  sampled separately: a pause between those samples can pair an earlier boundary with a current
  reading, and a delayed frame can carry an earlier reading. Every such tick is checked against
  this observation interval; its age does not supply a freshness bound. The interval cannot
  establish the exact snapshot time or distinguish projection errors that stay inside it. Tick
  IDs must increase within each attachment; the stall and handover checks separately establish
  progress and recovery.
- **Stalls.** A tick stall longer than two physical seconds, on an observer whose node stayed up,
  must fall inside a fault and end within 60 physical seconds. That is the authority bound the
  result records beside the deployed `NERVIX_NODE_UNAVAILABILITY_TIMEOUT`.
- **Handover.** From the start of each fault until recovery, a sampler reads every node's own
  `SHOW CLUSTER STATUS` several times a second. Once a surviving node no longer counts the faulted
  voter available, because its interconnect entry reads `unavailable` or has left the list, another
  voter must take the clock over within ten physical seconds. A node becomes unavailable only after
  its health probes have failed continuously for the deployed timeout, and a gossip warning alone
  does not end its availability, so a short fault can end before any replacement. The bound
  measures only the part of each continuous stall after that observation while the fault holds.
  Returning voters can change the eligible authority set and wait for runtime readiness, so gaps
  after healing keep the 60-second bound.
- **Authority rotation.** No public command, metric or `info` log names the clock authority, so a
  rotation covers every voter. The round whose fault stalls the ticks on every surviving observer is
  the one that removed the authority. A rotation without such a round fails as missing evidence.

**The window verdict.**

- Every paced window must close no earlier than eight logical seconds after it opened, which also
  shows that a graceful stop never emitted a partial window.
- Away from a relocation or an outage of its own node, a window must close no more than eight
  logical seconds late.
- A window must hold rows of one branch.
- Windows must keep closing on their logical deadlines while an authority stall withholds ticks. A
  stall caused by a graceful stop or a cluster restart is exempt, because that round also takes the
  graph's own node down.

Failover and shutdown bounds therefore stay physical, while windows follow logical time.
`domain-time/` keeps each round's fault evidence, the observers' transcripts, the window output with
Kafka timestamps, and both verdicts. `results/domain-time-progress.json` summarizes them, and each
round's availability samples sit in their own directory.

### Mixed Instability

```bash
just chaos run mixed-instability --image nervix:debian --seed 42 --duration 30m
just chaos run mixed-instability --image nervix:debian --seed 7 --duration 40m --policy temporary-quorum-loss
just chaos run mixed-instability --image nervix:debian --duration 10m --coverage kill:leader,degrade:follower
just chaos run mixed-instability --image nervix:debian --plan my-plan.json
```

A mixed-instability run executes a finite action plan on three nodes under continuous Kafka traffic.
The plan is a sequence of steps. Each step holds one fault or a combination of faults from these
families:

- `stop`: a graceful restart, as in the rolling restart
- `kill`: a crash
- `pause`: a pause of one or two seconds, or of 20 to 40 seconds
- `partition`: an isolation, one-way loss, or quorum loss
- `degrade`: one degradation profile on a directed link

A step can also combine faults:

- it can degrade the link between the two other nodes around a node's outage
- it can restart a node inside its own isolation
- under the temporary-quorum-loss policy, it can take a second voter out inside the first one's
  outage, partition every link, or crash every node

**Plans.**

- `--seed N`, from 0 to 2,147,483,646, selects the plan. With the same seed, `--duration` (60
  seconds to 4 hours, default 30 minutes), `--policy` and `--coverage`, `mixed-plan.sh` selects the
  same plan on every host, because it draws from its own arithmetic generator rather than the
  shell's. A seed omitted from the command is drawn and recorded.
- The generator first plans a step for every required coverage item that no earlier step covers.
  It then adds steps while the planned timeline still fits the duration, shuffles them, and lays
  them out with quiet gaps of 10 to 30 seconds.
- Each step records its earliest start and an estimate of how long it takes. A step starts no
  earlier than its offset and no earlier than the end of the step before it, so a slow recovery
  delays later steps instead of overlapping them.
- `--plan FILE` runs an explicit plan, which carries its own seed, duration, policy and coverage.
  Its estimates, from which the run sizes its fixture, are held to the generator's.
- The run writes the plan to `mixed/plan.json` and validates it before it creates a container, and
  the plan stays unchanged while it executes.

**Roles.** A plan never names a concrete node. Each step names a role: `leader`, `follower`,
`ingestor-owner`, `relay-owner`, `emitter-owner` or `cluster`. Its actions name logical references
that the step resolves from public observations when it runs:

- `target` is the node holding the role, choosing the follower that the step's pick selects in
  node order
- `peer` is the leader when the target is not, and otherwise the lowest-numbered follower
- `other` is the remaining node
- `cluster` is every node

Before the first fault, a step requires two settled observations in a row that resolve these
references identically. An observation reads every node's own `SHOW CLUSTER STATUS` and the owners
of the ingestor, relay and emitter through every node. It is settled when every node is connected
and free of warnings and all of them agree on leader, term, log position and owners.

**Quorum policy.** A kill, stop, pause or isolation takes its node out of a quorum, quorum loss takes
every node, and a degraded link takes none. One-way loss takes out whichever of its two nodes is not
the leader, because the leader keeps leading and loses either its way to the receiver or the
sender's acknowledgements. Validation judges each step under every reference that can lead it. Two
policies apply:

- `preserve-quorum`, the default, never lets two of the three voters be out at once
- `temporary-quorum-loss` allows that for at most 120 seconds in a step

`mixed-plan.sh validate` refuses a plan in any of these cases:

- its faults break its policy
- its network faults overlap, since each interface carries at most one owned netem configuration
- a node fault targets a node that carries another fault's rules
- a network fault is installed or healed while a node is out of the link checks
- a hold leaves its bounds
- an action carries a field its family does not take, or an ID other than lowercase letters and
  digits
- a step is estimated shorter than its longest hold plus what the generator plans beyond that kind
  of hold
- its required coverage no action plans; `quorum-loss` counts as planned only when two voters are
  out at once

Such a run fails with exit status 2 in the phase `action plan and fixtures`, before preflight and
before any container exists, and `mixed/plan-validation.json` lists every reason. While the run
executes, each fault is judged again immediately before it is injected, against the current state:
the nodes Docker reports stopped or paused, the link faults the step installed, and the fault
itself. A fault that would break the policy is refused as an injection failure before it alters a
container.

**Holds.**

- Before each step, every node container must be running and not paused, no run-owned injector may
  exist, every node interface must carry its default state, and the public view must settle, all
  within 150 seconds.
- Each fault is injected and verified as in the scenario it comes from. A node restarted inside an
  isolation must come back still isolated.
- While the faults hold, a quorum that can still communicate must agree on a caught-up leader within
  90 seconds. It must also acknowledge a canary through that leader within 30 seconds, or 60 under a
  degraded link.
- Random or burst loss on the one link left between the remaining quorum can stall commits until the
  loss heals. Such a step records these checks without requiring them.
- When every voter is still in a quorum, either end of a randomly or burst-lossy link may fall
  behind the other two voters, because the loss can stall the append stream to it. The other two
  must then agree on the caught-up leader, and the hold's `settled_without` names the end left out.
  A voter off the lossy link is never left out.
- With no quorum able to communicate, a canary through every running node is bounded to 20 seconds
  and none may be acknowledged. A node cut off from every quorum may neither apply past the boundary
  recorded when the partition was verified nor, in quorum loss, advance its term.
- A pause bounds these checks by its own end.
- Each hold is a minimum: a fault heals once its hold has passed since it was verified and its
  checks have finished.

**Recovery.**

- After the last heal of a step, every node must settle within 150 seconds and output must advance
  within 90 seconds.
- Within 120 seconds, the source may run at most `--max-recovery-backlog` records ahead of the emitter
  (default 20).
- A canary through every faulted node must be acknowledged.
- The public metrics may report at most `--max-pending` pending interconnect operations (default
  128).
- Each node's Docker lifecycle events from the step's start must be exactly those of its planned
  faults.
- At the end:
  - the events of the whole fault phase, which include the quiet gaps, must again be exactly the
    planned ones
  - every canary of the run must be present through every node when it was acknowledged, and absent
    when it was refused
  - the exact ledger must pass, with replay duplicates counted separately

**Sampling.**

- A background sampler records the ingestor and emitter end offsets and every node's Docker state, memory
  and CPU every 10 seconds for the whole fault phase, in `mixed/samples.ndjson`.
- A node whose memory exceeds `--max-memory-bytes` (default 1 GiB) is a product finding.
- Two samples more than 120 seconds apart fail the run, because the continuous observation had a
  gap.
- `results/mixed-resources.json` summarizes each node's first, last and peak memory, the peak
  backlog, and the longest ingestor and emitter stalls.
- `results/mixed-instability-progress.json` records every step's nodes, timings and recovery, with
  the distribution of settle, delivery and drain times across the run.

**The action trace.** `mixed/actions.ndjson` records, for each step:

- when it started and ended, and how late it started against its plan
- the nodes it selected, with the roles each held

For each action within the step, it records:

- what the action intended, and the node, container and roles it met
- the conditions its policy check judged: every node's Docker state, the faults already holding,
  and the voters left out of a quorum
- when it was started, verified and healed

The trace also holds every hold check and every moment two voters were out of a quorum.
`verify-mixed-evidence.sh trace` requires the trace to be complete and ordered, and the plan's
coverage to be met.

**Coverage.** `--coverage` lists `FAMILY:ROLE` items, where the family is `kill`, `stop`, `pause`,
`partition` or `degrade` and the role is one of the step roles, plus `quorum-loss`. The default is
every family against the leader and against a follower, plus `quorum-loss` under
temporary-quorum-loss. An item is covered by a verified action whose node held the role when its
step selected it, so one action can cover several items. `quorum-loss` is covered when a step left
two voters out of a quorum. A plan whose required coverage does not fit the duration is refused as a
setup error that names the duration it needs.

An incomplete trace, missing coverage or a gap in the samples fails the run as a controller failure
in `results/mixed-instability.json`. Product findings fail it as product failures after the final
ledger. A run that fails inside its fault phase still writes `results/mixed-trace.json`, whose
verdict is then incomplete and names the records it never reached, and `results/mixed-resources.json`
for the samples it took.

**Sizing.**

- The fixture holds twice the planned timeline plus 15 minutes of records, produced one a second,
  unless `--records` asks for more.
- The run is bounded by its duration plus the larger of that duration and 20 minutes, at most six
  hours, unless `--timeout` sets a bound from the duration plus 10 minutes through six hours.
- The deployment uses the standard heartbeat and unavailability settings, and the manifest records
  them as Compose renders them.

### Replay

```bash
just chaos replay target/chaos/<run-id>
```

`just chaos replay` reconstructs a mixed-instability experiment from what its run directory recorded.
It runs in a new Compose project, with a new run ID, network, TLS material and volumes, and resolves
every logical reference against the new cluster's own observations. The replay reuses these
recorded inputs:

- **The Nervix image.** The replay starts the recorded image by its image ID when the host holds
  it. Otherwise it uses a recorded repository digest, pulling it within a bound when it is not
  local.
- **The tool images.** It starts them by their recorded digest references rather than the pins
  checked in now. Each must have its recorded image ID or carry the repository digest its reference
  pins.
- **The plan, fixture and graph.** It copies the recorded plan, input fixture and Plexus graph, and
  each must still match the digest the manifest records.
- **The deployment and limits.** It deploys the recorded node settings and load interval, refuses to
  start when Compose renders anything else, and applies the recorded limits.

An image ID is the image store's own name for an image: the config digest in Docker's classic store,
and the manifest or index digest in the containerd store. A run recorded on a worker whose store is
of the other kind, such as a CI worker, is found through the repository digest, which names the same
content in every store. The replay's manifest records which of the two matched under
`replay_image_match`.

The replay refuses in two ways:

- A directory that is not a mixed-instability run, lacks any of these records, or holds a plan or
  fixture that no longer matches, fails before the replay creates anything.
- An unavailable image fails it in preflight with exit status 2, before it records a Docker event.

`--artifacts`, `--run-id`, `--timeout`, `--verifier-self-check` and `--keep` are the only options a
replay accepts, because none of them changes the recorded experiment, and its manifest names the run
it replays under `replay_of`.

Once a run's manifest records its deployment, a failure prints the replay command and records it as
the reproducer in `results/finding.json`. A run that fails earlier records the run command instead.
That command carries the plan when the run has one, and otherwise the seed, duration, policy and
coverage that select it, together with any `--records`, `--timeout` and limits it was given. A
replay that fails early records the replay of its source.

A replay reproduces the experiment's inputs and its sequence of actions, not its timing:

- **Identical:** the images, the deployment, the workload records and their pacing, the plan, the
  steps, the roles, the logical references, the parameters and the policy.
- **Not identical:** Linux scheduling, container start times, Raft elections, failure detection,
  Kafka rebalances, and the outcomes of random and burst packet loss.

So several things can differ between a run and its replay:

- which node holds a role when a step selects it
- how many records the producer has sent by then
- how long each recovery takes
- how a step's late start shifts the steps after it

Each run's trace records what it actually met.

## Verdicts, Findings And Failure Categories

A run passes when it exits 0 and its manifest records `status: passed`. A failed run records its
category, which separates product defects from problems of the experiment itself:

| Category | Meaning |
| --- | --- |
| `setup` | The run refused before it could start. Causes include an invalid option, a missing host tool, jq older than 1.8, an unresolvable image, a tool image that cannot be pulled, a Docker socket Pumba cannot use, a failed network preflight, an invalid plan, or a fixture too short for its plan. Exit status 2, and no product verdict |
| `controller` | The runner itself failed. Causes include an exhausted bound of its own, an incomplete event recording, a container from an unrecorded image, an incomplete action trace or a sampling gap, a load that broke its declared pacing, and a reporting failure |
| `observation` | An observation the verdict needs could not be made, such as an output boundary that never stabilized or a link probe that failed. It establishes no product failure |
| `injection` | A fault did not take effect as planned, could not be verified, or would have broken its policy. Examples are a dry run that selected another container, rules that do not match their plan, an ineffective or unhealable network fault, a pause outside its interval, and a role that moved before the fault |
| `product` | Nervix violated a contract the scenario verifies |
| `diagnostic` | On a diagnostic image: a deadlock finding, missing, partial, invalid or unbounded evidence, or evidence of another selection; see [Diagnostic Images](#diagnostic-images) |
| `unclassified` | A failure outside every classified check. An example is a bound reached during graph installation or placement, after startup and before the scenario's own checks begin. It establishes no product failure |

A failed run keeps `results/finding.json` when it is one of these scenarios:

- a crash, pause, partition or degraded-link run
- a recovery, backup-fault or fresh-cluster restore run
- a stateful, domain-time or mixed run

Every diagnostic failure keeps one too. It holds:

- the category and the phase
- the exit status
- the image ID and the image reference, pinned by digest when one is available
- the tool images
- a reproducer command
- the evidence paths that explain the failure

A failed baseline, backup, backup-cycles or rolling-restart run writes no finding yet, so a suite
also lists it as unclassified. [Cluster Chaos 33](https://app.clickup.com/t/86bc9dkk2) owns classifying them.

Two kinds of violation are handled differently:

- **Findings.** A violation that leaves the experiment meaningful is recorded as a finding, and the
  run continues through healing, recovery and the final ledger, so that it keeps every violation it
  can observe. It then exits nonzero with the findings listed. Examples are a quorum that does not
  commit, an unexpected acknowledgement, an applied entry past an isolation boundary, a failover
  away from a healthy node, a missing effect, and a resource limit exceeded. Findings are written
  to one file per family of scenarios:
  - partitions: `results/partition-findings.ndjson`
  - the recovery, backup-fault, fresh-cluster restore, stateful, domain-time and mixed scenarios:
    `results/recovery-findings.ndjson`
  - degraded links: `degraded/findings.ndjson`
- **Stopping failures.** A failure that makes the later steps meaningless stops the run at once,
  after it records every node's own status. Examples are an ineffective or unremovable fault, no
  majority leader, and no convergence after healing.

| Exit status | Meaning |
| --- | --- |
| 0 | Passed |
| 1 | Failed, with the category above |
| 2 | Setup error before any product verdict |
| 124 | A bounded command or wait, or the whole-run bound, ran out before the verdict. Also used when emitter output stayed short of the accepted input past its bound, after the exact ledger was written |
| 129, 130, 143 | Interrupted by HUP, INT or TERM. The exit trap still heals, captures and cleans up |

## Budgets And Timing

Every run has a whole-run bound, `--timeout`, and every bounded command and wait is clipped to what
remains of it. A suite passes each entry's budget as that entry's `--timeout`, so a run that
overruns fails by its own bound, with its own diagnostics. An entry's budget funds only its
execution: the suite executes the verifier self-check once, before its first run, within the
preparation its shard's step budget funds, and every run of the suite adopts that check's record.

After the bound, the teardown has a reserve of 15 minutes. Every teardown step is bounded on its own
and together they fit the reserve, which the live event subscriber also outlives the bound by. The
manifest records `timeout_seconds`, `teardown_reserve_seconds` and the measured `teardown_seconds`.

The controller's own bounds:

| Operation | Bound |
| --- | --- |
| Nervix image pull, when not local | 180 s, once |
| Image-package predicate, its inspection | 30 s, 20 s |
| Pumba version and Docker access dry run | 30 s each |
| Network preflight; its rules inspection | 120 s; 30 s |
| Verifier self-check, once for a suite or a standalone run | 600 s |
| Broker readiness | 90 s |
| Each node's readiness and listeners; every node in `SHOW CLUSTER STATUS` | 120 s; 120 s |
| Startup status publication | 30 s |
| Each owner after placement | 60 s |
| Settled public role before a crash or pause | 30 s |
| Consumer offsets at the source boundary | 120 s |
| Output containing the accepted input | 120 s; `--drain-seconds` for degraded links |
| Output stability window | 30 s |
| Final ingestor-boundary query of a recovery scenario | 60 s |
| Docker launch of an ICMP helper, transfer receiver, Pumba dry run or injector | 60 s |
| ICMP sample; link transfer; helper log read and removal | 30 s; 90 s; 20 s each |
| Deadlock evidence qualification | 300 s in all, at most 60 s per file |
| Exit-trap network healing; exit-trap cleanup | 180 s; 90 s |

The product bounds a scenario requires are listed with each scenario: survivor election, owner
relocation, delivery during an outage, returning listeners, settlement, catch-up and clock
replacement. None of the controller's bounds widens them.

Runtime timing is not deterministic. Linux scheduling, Docker's container starts, Raft elections,
gossip's failure detection, Kafka rebalances and the outcomes of random packet loss all vary from
run to run, and a loaded worker stretches all of them at once. The suite applies three rules:

- A wait for something to happen is bounded generously, because the wait ends as soon as the
  condition holds.
- An assertion that something did not happen measures a delay from a recorded event rather than
  gating on a window.
- A suite budget is about two and a half times the duration first measured on the CI worker, rounded
  up and at least three minutes. A 30-minute mixed run gets 60 minutes, its own default bound.

A run that ends on the runner's own budget is never a product verdict. [Cluster Chaos
53](https://app.clickup.com/t/86bcdkckc) owns sizing those budgets for loaded workers.
Measurements on a diagnostic image are not product performance evidence.

When every run still executed its own verifier self-check, the check took 139 to 164 seconds of
each run on GitHub-hosted workers in the nightly soak of 22 October. That is about two thirds of
the four-minute `stale-follower` entry, which then reached its bound in its last phase on both
images, although its recovery had already passed. A suite therefore proves the verifiers once
and keeps that preparation out of every entry's budget.

Suite budgets are sized for one suite on a dedicated CI worker. A standalone `just chaos run` still
executes its own check within its whole-run bound. On a shared or loaded host the check and the
run's own work both stretch, so a standalone run given a short `--timeout` can reach its bound and
fail with exit status 124. Such a failure is a limit of the host, not a verdict. There are three
remedies:

- run one suite per host
- run the entry through its suite, or alone with `just chaos run` under its larger default bound
- pass a larger `--timeout` to the run

## Artifacts

Each run writes bounded evidence under `ARTIFACTS/RUN_ID/`, by default `target/chaos/<run-id>/`.

| Path | Holds |
| --- | --- |
| `manifest.json` | The run's identity, status, final phase, exit status, image and tool identities, revision, diagnostic selection, network and addresses, loads, scenario settings, artifact limits, timeout and teardown, and any setup error or signal |
| `phases.ndjson` | Every phase entered, with its time |
| `compose.rendered.yaml`, `docker-compose-version.txt`, `pumba-version.txt` | The deployment as Compose rendered it, and the tool versions |
| `fixtures/` | The generated records and every graph and guest the run installed |
| `public/` | Every packaged-CLI command and its output: domain and graph creation, placements, descriptions, cluster status and metrics |
| `traffic/` | Accepted input, observed output, every Kafka end-offset read, the output-boundary polls, consumer groups, load timestamps and producer results |
| `results/` | The verdicts: the scenario's result and progress, the ledger, the output boundary, load pacing, container images, a diagnostic run's evidence verdict, `finding.json`. Also the verifier self-check's output, `verifier-self-test.txt`, and in a run of a suite the record it adopted, `verifier-self-check.json` |
| `diagnostics/` | The live event recording and its bounds, container inspection, Compose logs (a fresh-cluster restore adds `fresh-compose-ps.json` and `fresh-compose.log` for its second project), the image-package and startup-status records, preflight and healing records, cleanup output |
| `backup/`, `backup-cycles/`, `backup-fault/`, `restore-fresh/`, `restore-fault/`, `restore-volume/`, `restarts/`, `crash/`, `pauses/`, `partitions/`, `degraded/`, `stale/`, `former-owner/`, `restart/`, `stateful/`, `domain-time/`, `mixed/` | Each scenario's own evidence, in catalog order: `restarts/` belongs to rolling restarts and `restart/` to cluster restarts |
| `deadlock/` | A diagnostic run's per-node evidence files, their inspections and qualifications, in `node-N/` for the run's nodes and `fresh-N/` for a fresh-cluster restore's |
| `tls/` | The run's certificates. The private keys are removed when the run ends |

The manifest's `artifact_limits` record the bounds that keep a run directory small:

- 1,000 fixture records; 20,000 for mixed instability
- 2 MiB of Compose log per container file, two files each
- 64 MiB of Docker events
- 1 MiB of metrics per node
- 2 MiB of restart log and 1 MiB of observer log per restart

A suite writes one directory per run inside its own directory; see [Suites](#suites).

## Cleanup, Signals And Interruption

On a normal exit, a failure, a timeout or a catchable signal, the exit trap first ends every fault
the run still holds:

- **Pauses.** It unpauses every paused run-owned container, including when Pumba failed.
- **Network faults.** It stops the injectors and removes any Pumba-owned qdisc or INPUT rule left on
  a node, within 180 seconds.
- **Stopped nodes.** It starts every node container a restart or recovery case still held stopped.
- **Mixed runs.** A mixed run first stops its sampler and every pause it still waits for, then
  unpauses every node, heals every network fault and starts every stopped node.

The trap then captures diagnostics, removes the labeled resources within 90 seconds, and removes the
private keys. `--keep` retains the resources for interactive diagnosis and prints the cleanup
command:

```bash
just chaos cleanup --run-id <run-id>
just chaos cleanup --run-id <run-id> --evidence target/chaos/<run-id>-leftovers
```

A controller killed before its trap ran leaves its resources behind, and its event subscriber exits
on its own 15 minutes after the run's bound. `just chaos cleanup` removes those resources, including
Pumba sidecars left joined to run-owned containers. With `--evidence DIR`, it works in this order:

1. It captures what the run left behind: the listing and inspection of every container, network and
   volume carrying the run's label, and the last 2 MiB of each container's log.
2. It removes them.
3. It lists the label again to confirm that nothing survived.

A container that was exiting while its removal ran can still hold its network. Cleanup therefore
removes whatever the listing still finds, in at most three rounds two seconds apart, and fails only
when the last listing still finds something. `cleanup.json` in `DIR` records what was removed, the
rounds taken and what remained. A run that left nothing gets no evidence directory.

## Diagnostic Images

Every scenario and suite also runs against a Deloxide diagnostic image. That image is a test
artifact built from the same revision as a release image and never published as one. Its server
tracks thread-blocking locks with the detector of [Data-Plane
Concurrency](./data-plane-concurrency.md#diagnostic-deadlock-detection). Build one, or overlay a
locally built diagnostic server on an available image, and run it like any other image:

```bash
just docker-build-diagnostic deloxide-order 23 nervix:diagnostic
just build-chaos-diagnostic-image nervix:diagnostic-local ghcr.io/nervix-io/nervix:debian-latest deloxide-order
just chaos run leader-crash --image nervix:diagnostic-local
just chaos suite smoke --image nervix:diagnostic-local --image-kind deloxide-order
```

A diagnostic image declares its selection, `deloxide`, `deloxide-order` or `deloxide-stress`, in its
`io.nervix.diagnostic.selection` label. Both kinds of image declare their revision in
`org.opencontainers.image.revision`. After resolving the image, the run reads the label and records
the selection and revision in `manifest.json`. It requires the image to package
`nervix-deadlock-report`, and it adds `compose.diagnostic.yaml` to the deployment. Every node then
records its evidence in `deadlock/node-N/` of the run directory, mounted at
`/var/lib/nervix-deadlock` and named by `NERVIX_DEADLOCK_EVIDENCE`, so the evidence outlives the
node's container and volume. A fresh-cluster restore adds `compose.fresh-cluster.diagnostic.yaml`
to its second project in the same way. Its nodes record their evidence in `deadlock/fresh-N/`, and
the verdict below counts them beside the run's own nodes. Nothing else of the deployment, the
workload, the faults or the verdicts changes. Fault injection stays external, and no scenario adds a
deadlock hook.

Each process a diagnostic node starts writes one evidence file, `deadlock-PID-TIME.rkyv`, when its
diagnostic run starts, and replaces it after each finding. The run then proceeds in order:

1. Once the event recording is closed, the controller freezes every node that still runs, so no
   process records a finding after its evidence is read.
2. It qualifies each file with the image's own `nervix-deadlock-report`, in a container without a
   network whose only mount is the node's evidence directory, read-only. Each file gets its
   `.inspect.txt` and `.qualify.txt`, and `deadlock/qualification.ndjson` lists every file with its
   size, the tool's exit status and the selection it records.
3. The nodes run again before cleanup.
4. `verify-diagnostic-evidence.sh` writes `results/diagnostic-evidence.json`.

The verdict passes only when all of these hold:

- each node holds exactly one complete evidence file for each start of its container in the
  recording, and no partially written one
- every file is valid evidence of the declared selection and qualifies: it holds no active cycle, no
  unreviewed potential cycle and no loss
- no node container ended with status 3, the end of a process whose detector reported an active
  deadlock, or 4, the end of a failed diagnostic execution
- a node keeps at most 64 files and the run at most 64 MiB of evidence, and the report tool judges
  every file within the 300-second qualification budget, inside the teardown reserve

The result counts the process starts, files and bytes, and the active, potential, unreviewed and
lost findings. A run that otherwise passed fails in phase `diagnostic evidence`, with one of two
categories:

- `diagnostic` for a finding; for missing, partial, invalid or unbounded evidence; or for another
  selection
- `controller` when the recording is incomplete or the tool could not run

An active deadlock or a failed diagnostic execution also classifies an otherwise failed run as
`diagnostic`, because the node it ended explains what the run observed afterwards. A potential cycle
is never approved here: a correction removes the order from the workload, as in the diagnostic lane.

The evidence observes each process up to its end or the freeze:

- A kill, a pause and a graceful stop each end a process without a flush barrier, so a finding the
  detector had not yet delivered is not in the evidence.
- The detector observes only Nervix's tracked thread-blocking locks. Tokio's locks and channels,
  atomic protocols and network waits keep their own Shuttle, Loom, Turmoil and chaos evidence.
- A qualifying run proves no untracked path free of deadlocks.

## Suites

A suite is a named selection of runs from `suites.json`. CI and a developer start it with the same
command, against an already built image.

```bash
just chaos suite list
just chaos suite smoke --image ghcr.io/nervix-io/nervix@sha256:<digest>
just chaos suite soak --image ghcr.io/nervix-io/nervix@sha256:<digest> --shard 6
just chaos suite soak --image nervix:debian --entry partition-leader --entry stale-follower
just chaos suite smoke --image nervix:diagnostic --image-kind deloxide-order --image-revision "$(git rev-parse HEAD)"
```

### Catalog

Each entry is one `just chaos run` command with its arguments, a budget in minutes and a shard,
which is the unit one CI job runs. An entry may also carry a note. The suite's `ci_reserve_minutes`
is 21.

The `smoke` suite runs in three shards:

| Shard | Entry | Command | Budget (min) |
| ---: | --- | --- | ---: |
| 1 | `baseline-3` | `baseline --nodes 3` | 4 |
| 1 | `leader-crash-3` | `leader-crash --nodes 3` | 7 |
| 1 | `partition-follower` | `partition-recovery --case follower` | 9 |
| 2 | `endpoints-1` | `endpoints --nodes 1` | 10 |
| 2 | `endpoints-3-owner-crash` | `endpoints --nodes 3 --fault owner-crash` | 12 |
| 3 | `lifetimes-3-owner-crash` | `lifetimes --nodes 3 --fault owner-crash` | 25 |

The `soak` suite runs every delivered scenario, topology, case, profile and fault, in fourteen shards:

| Shard | Entries, with budgets in minutes |
| ---: | --- |
| 1 | `mixed-seed-42` (seed 42, `preserve-quorum`, 30 minutes) 60; `baseline-1` 3; `baseline-3` 4; `cluster-restart-1` 6; `cluster-restart-3` 7 |
| 2 | `mixed-seed-45` (seed 45, `temporary-quorum-loss`) 60; `leader-crash-1` 6; `leader-crash-3` 7; `follower-crash` 5 |
| 3 | `mixed-seed-136` (seed 136, `temporary-quorum-loss`) 60; `ingestor-owner-crash` 6; `emitter-owner-crash` 5; `stale-follower` 4; `former-owner-restart` 10 |
| 4 | `mixed-rotating-preserve` 60; `rolling-restart-1` 5; `rolling-restart-3` 7; `backup-1` 4; `backup-3` 4 |
| 5 | `mixed-rotating-temporary` 60; `mixed-burst-loss-leader-peer` 15; `pause-resume` 14 |
| 6 | `partition-follower` 9; `partition-asymmetric` 7; `partition-leader` 7; `partition-quorum-loss` 8; `degraded-delay`, `degraded-jitter`, `degraded-random-loss`, `degraded-burst-loss`, `degraded-rate-limit`, `degraded-combined` 10 each |
| 7 | `domain-time-1` 7; `domain-time-1-cluster-restart` 8; `domain-time-3` 7; `domain-time-3-cluster-restart` 10; `domain-time-3-voter-crash` 12; `domain-time-3-voter-pause` 20; `domain-time-3-voter-stop` 15 |
| 8 | `stateful-1` 20; `stateful-1-cluster-restart` 22; `stateful-3` 20; `stateful-3-owner-crash` 20 |
| 9 | `stateful-3-owner-pause` 20; `stateful-3-owner-partition` 25; `stateful-3-cluster-restart` 22; `domain-time-3-voter-partition` 25 |
| 10 | `backup-cycles-1` 10; `backup-cycles-3` 12; `restore-into-fresh-cluster` 12; `backup-under-owner-crash` 14; `restore-under-leader-crash` 12; `restore-under-owner-crash` 11; `restore-staging-volume` 11 |
| 11 | `backup-under-leader-crash` 14; `backup-under-partition` 15; `rolling-restart-during-backup-stream` 20; `restore-under-leader-partition` 20; `restore-reclamation-under-owner-crash` 19 |
| 12 | `endpoints-1` 10; `endpoints-1-cluster-restart` 12; `endpoints-3` 10; `endpoints-3-owner-crash`, `endpoints-3-owner-partition`, `endpoints-3-cluster-restart` 12 each |
| 13 | `lifetimes-1` 20; `lifetimes-1-cluster-restart` 22; `lifetimes-3` 22 |
| 14 | `lifetimes-3-owner-crash`, `lifetimes-3-owner-partition`, `lifetimes-3-cluster-restart` 25 each |

A few soak entries need explanation:

- **The rotating entries.** The two rotating mixed entries draw a new seed for every run, one per
  policy, and record it.
- **Seed 136.** It crashes a voter inside the crash of the bootstrap node. That exercises the
  recovered-peer contact path when the bootstrap name no longer resolves.
- **`mixed-burst-loss-leader-peer`.** It runs the one-step plan in
  `fixtures/mixed-burst-loss-leader-peer.json`: burst loss from the leader to one follower, which
  moved leadership to that follower and left the old leader behind the quorum in seed 993081495.

### Running A Suite

- **Order.** The entries of a selection run one after another, every one of them even after another
  fails. A suite never retries a failed run.
- **Selection.** `--shard` selects one shard and `--entry` selects named entries. Together they
  select both.
- **Before the first run.** The suite checks its own verdict logic with `tests/suite-self-test.sh`,
  which `just chaos self-test` also runs. After it has resolved the image, it executes the full
  verifier self-check, `tests/self-test.sh`, once and within its 600-second bound. The check leads
  a session of its own, as a run does. It passes only when it exits 0 and the chaos scripts were
  the same when it ended as when it began. The suite then writes `verifier-self-check.json`: the
  check's outcome and timing, the digest of its output, the identity of the chaos scripts it
  checked and the boot identity of the worker. Every run receives that record with
  `--verifier-self-check` and adopts it instead of executing its own check. A run refuses a record
  that did not pass on its worker, during its boot, for exactly its scripts, as a setup error
  before it creates anything.
- **A failed verifier self-check.** A check that fails, or whose scripts change while it executes,
  starts no run and leaves no record a run could adopt. The suite fails as a controller failure,
  with exit status 1, and records every entry as not started.
- **Image resolution.** The suite resolves the image once. A digest reference stays as given, and
  any other reference runs as the local image ID it resolves to, so every run of the suite uses one
  immutable image. It records the revision and diagnostic selection that the image's labels
  declare.
- **Kind and revision.** `--image-kind ordinary`, or a diagnostic selection, refuses an image whose
  labels declare another kind. `--image-revision REVISION` refuses an image whose labels declare
  another revision. Both refuse as a setup error before any run, so the suites of a release image and
  a diagnostic image show that both came from one revision.
- **Diagnostic entries.** On a diagnostic image, an entry passes only when its run recorded its nodes
  as diagnostic nodes of the image's selection and a passing evidence verdict.
- **Suite ID.** `--suite-id` takes 1 to 48 letters, digits, dots, underscores or hyphens. A suite
  whose directory already exists is refused.

A suite writes `ARTIFACTS/SUITE_ID/`, by default `target/chaos/<suite>-<time>-<pid>/`:

- **Run directories.** One per entry, named `<suite-id>-<entry>`, exactly as `just chaos run` writes
  it.
- **`logs/<entry>.log`.** Each run's console.
- **`suite.json`.** The machine-readable record. It names the suite and shard. It records the image:
  the requested reference, the resolved ID, the repository digests, the revision and the diagnostic
  selection. It records the worker: its kernel, Docker and Compose versions, CPUs and memory. For
  every entry it records:
  - the command, run ID, verdict, exit status, failure category, final phase, duration, timeout and
    teardown
  - the seed and policy
  - the reproducer
  - the delivery ledger: expected, observed, duplicate, missing, unexpected and incorrect records
  - the headline recovery timings
  - a mixed run's peak memory, backlog and output stall
  - a diagnostic run's evidence verdict and counts
  - the result files
- **`verifier-self-test.txt`, `verifier-self-check.json` and `verifier-self-test/`.** The verifier
  self-check's whole output, the record every run adopts, and the probe records a failed rate-probe
  self-test keeps. `suite.json` records the check under `verifier_self_check`: its status, exit
  status, start, end, duration, bound, process, output and record.
- **`summary.md`.** The same verdicts rendered for a reader, with the verifier self-check's outcome.
  The suite also prints it at the end.
- **`suite.log`.** The suite's whole console, which it keeps writing after its caller stops reading.

An entry passes only when its command exits 0 and its manifest records a passing run of the suite's
image. The suite fails an entry as a controller failure in three cases: the run left no manifest,
the manifest's exit status contradicts the command's, or the run resolved another image. A failed
entry takes the category of its `results/finding.json`. It is `setup` when the run refused before
creating its directory or recorded a setup error, and it is listed as unclassified otherwise. An
interrupted entry carries no category, because the category a run held when a signal ended it names
no cause. Its reproducer is the run's own when it recorded one, and otherwise the entry's command.

| Suite exit status | Meaning |
| --- | --- |
| 0 | Every entry passed |
| 1 | An entry failed or did not run |
| 2 | Setup error before any run, such as an unknown entry or an image that is neither local nor pullable |
| 128 plus the signal number | A signal ended the suite |

### Signals And Suite Cleanup

Each run executes in the background and leads a session of its own. The suite owns an interrupt,
termination or hangup, which reaches the run only through the suite, even when a terminal signals
the suite's whole process group. Each atomic record publication runs in a subprocess that ignores
INT, TERM and HUP through temporary-file creation, JSON rendering and rename. The controller handles
the pending signal after that publication finishes, preserving the child's identity before it
passes one TERM to the run. The verifier controls hold both JSON rendering and the completed rename
while sending a group hangup, then verify the interrupted record, one TERM and complete log output.
The run's
own exit trap then heals, captures and cleans up as on any exit, and records the run as interrupted.
The suite starts no later entry. The verifier self-check executes the same way: a signal while it
executes reaches it as the suite's one TERM, the suite records the check as interrupted, and no
entry starts. Its console reaches its caller through a tee that ignores a terminal's interrupts and
hangups and keeps writing `suite.log` when the caller stops reading, so the suite outlives its
console and still records every verdict.

`just chaos suite cleanup [--wait SECONDS] SUITE_DIRECTORY` ends what a suite left behind. The suite
records the process of its controller, of its verifier self-check and of every run it starts, and
cleanup works in this order:

1. A controller that still executes receives one TERM, which it passes to its run or verifier
   self-check as on any signal. A run or verifier self-check whose controller is gone receives one
   TERM itself.
2. Each has `--wait` seconds, 300 by default, to finish its exit work.
3. Cleanup then kills the controller if it still executes, and every process left in each run's
   session and in the session of a verifier self-check that still executed.
4. For every run the suite started, cleanup runs `just chaos cleanup --evidence` into
   `cleanup/<run-id>/`, and removes the private keys of any run whose exit trap never ran.
5. It keeps the verdict of a run that finished before its controller ended. It records entries that
   never reported an exit status as interrupted, and entries never started as not-started.
6. It records how the controller ended, which runs it signalled or killed, and under
   `cleanup.verifier_self_check` whether it signalled a verifier self-check that still executed and
   how many of its processes it killed. A check that still executed is recorded as interrupted.
   Cleanup then renders the summary again.

### Shard Budgets

`just chaos suite shards SUITE` prints the budgets CI applies to each shard:

- **Execution budget.** The sum of the shard's entry budgets.
- **Preparation budget.** 10 minutes, the verifier self-check's 600-second bound in whole minutes.
  The suite executes that check once, before its first run, so no entry's budget funds it.
- **Step budget.** The execution budget plus the preparation budget plus one 15-minute run teardown
  reserve, within which a run that reached its own bound finishes its teardown.
- **Job budget.** The step budget plus the suite's 21-minute CI reserve. The reserve funds the
  separate network-control qualification, with its 600-second cap and 10-second termination grace,
  and the steps after the suite: cleanup, which has its own 8-minute bound holding its five-minute
  wait, plus the summary and the uploads.

The smoke shards therefore run 20, 22 and 25 minutes of entries in steps of 45, 47 and 50 minutes
and jobs of 66, 68 and 71 minutes. The soak's fourteen shards hold 1,133 minutes of entries.

`just chaos suite report [--expect-shards N] SUITE_JSON...` renders one summary of several shards.
It fails unless each shard passed and `N` of them are present.

## CI

### When Suites Run

The `Docker Build` workflow builds the images a suite needs and calls the `Chaos` workflow,
`.github/workflows/chaos.yaml`, twice for each suite it runs:

- with the release image its build job published,
  `ghcr.io/nervix-io/nervix:<build-id>-debian-amd64`, as kind `ordinary`
- with the diagnostic image its `build-diagnostic` job published from the same checkout,
  `ghcr.io/nervix-io/nervix:<build-id>-debian-amd64-deloxide-order`, as kind `deloxide-order`

Both calls pass the revision the build checked out. The diagnostic image is pushed under that tag
only and never enters a published manifest, and its build runs only when a chaos suite needs it.

Both image build jobs record the shared short CPU, memory-access and filesystem calibrations.
The builder also probes its mounted Cargo target filesystem before compilation, retaining raw
results in the plain BuildKit log. These are host/build diagnostics, not chaos execution evidence
or a sustained SSD benchmark; [Integration Test Lifecycle](integration-test-lifecycle.md) owns
their bounds and interpretation. They do not alter image identity, suite budgets or verdicts.
The triggers are:

| Trigger | Suite |
| --- | --- |
| A pull request from a branch of this repository, labeled `chaos` | Smoke, against both images built from it |
| A pull request from a branch of this repository, labeled `chaos-soak` | Soak, against both images |
| The nightly build of `main`, at 02:00 UTC | Soak, against both images. That build publishes neither a manifest nor the book |
| A manual run of the `Chaos` workflow | The named suite against a named immutable image `REPOSITORY@sha256:DIGEST`, of a named kind and optionally a revision. It builds nothing |

A pull request labeled `no-docker` builds no image and so runs no suite. A label that builds no
image runs its Docker Build in a concurrency group of its own, so a pull request that adds `chaos`
beside other labels in one request keeps the build that `chaos` started. The other labels select
other methods, which a chaos run never substitutes for:

- `fuzz` runs only the Bolero sanitizer job, and `bolero-random` only the Bolero randomized job;
  see [Property Testing And Fuzzing](./property-testing-and-fuzzing.md)
- `book` builds and publishes only the book; see
  [Developing Nervix](./developing-nervix.md#building-the-documentation)
- `deloxide`, `shuttle`, `loom`, `turmoil` and `client-conformance` run their own lanes

### The Chaos Workflow

The `plan` job reads the shards and their budgets from `just chaos suite shards`. It resolves the
image to its digest, so every shard runs the same immutable image. A manual run must name a digest.

Each shard then runs on its own Boat `boat-large` Docker worker: an Ubuntu 24.04 virtual machine
with 8 CPUs and the 16 GB of the GitHub-hosted workers the suite was qualified on. The job works
in this order:

1. It loads the `sch_prio`, `sch_sfq` and `sch_netem` modules, and fails when the kernel cannot
   provide one.
2. It installs `just`, and jq 1.8.1 from its release, checked against the release's SHA-256,
   because the worker's own jq is Ubuntu's 1.7.
3. It removes every Rust toolchain directory from the `PATH` the suite runs with, and fails if
   `cargo`, `rustc` or `rustup` is still reachable.
4. It waits up to two minutes for the Docker daemon, which a Boat worker can start a few seconds
   after the job, then pulls the image and logs out of the registry.
5. It runs the real network refusal controls once, with `just test-chaos-network-preflight`, under
   their 600-second cap in an 11-minute step, outside every scenario's budget. Their raw evidence is
   kept beside the suite's.
6. It runs the suite:

   ```bash
   just chaos suite <suite> --shard <n> --image <digest> --image-kind <kind> \
       --image-revision <revision> --artifacts <dir> --suite-id ci-<run>-<attempt>-<suite>-<n>
   ```

   For a diagnostic image, the suite ID adds `-diagnostic` before the shard. The suite executes
   the verifier self-check once, inside the step's preparation budget, before its first run. The
   suite step ends at the shard's step budget, and the job ends at its job budget.
7. It always runs `just chaos suite cleanup` next. Cleanup waits up to five minutes for a
   controller that is still finishing its run's exit work, kills whatever still executes, and, as a
   second witness after the runs' own traps, captures and removes what any run left behind.
8. It publishes the shard summary and uploads two artifacts, kept for 14 days:
   - `chaos-verdict-<suite>-<n>-<kind>-<run>-<attempt>`, holding `suite.json` and `summary.md`
   - `chaos-<suite>-<n>-<kind>-<run>-<attempt>`, holding the whole suite directory, with every run
     directory and a diagnostic run's evidence files

   Because the run attempt is part of each name, a rerun never replaces the artifacts of the attempt
   it repeats. Because the kind sits between the shard and the run, one kind's verdicts are never
   read as another's.

The `verdict` job of each kind renders `just chaos suite report` over the latest attempt of every
shard of that kind. It fails when a shard did not pass or recorded no verdict, and it lists each
attempt that was rerun.

### Cancellation

When a suite step is cancelled or reaches its budget, the runner signals in sequence:

1. SIGINT to the step's process, `just`, which does not pass it on
2. 7.5 seconds later, SIGTERM, which `just` passes to the suite its recipe `exec`s
3. 2.5 seconds after that, SIGKILL to `just` alone

Shortly afterwards the runner stops reading the step's output. The suite and its run outlive the
step: the run heals, captures and tears down on the TERM and records its interruption, and the suite
writes its verdict into `suite.log` after its console has closed. The cleanup step that always
follows finds a controller still finishing and waits for it. A rerun attempt lists the cancelled
attempt in its verdict.

### Labels And Release Evidence

Label a pull request `chaos` when it changes either of these:

- the chaos runner
- the membership, failover, recovery or delivery behavior its scenarios verify

Use `chaos-soak` to run the whole matrix against the pull request's own images, and select both
labels when qualifying the complete matrix. A change to the diagnostic image, its evidence
collection, or a path its scenarios exercise under tracked locks keeps both kinds of image green. A
nightly run of another commit does not qualify a candidate revision.

`tests/release-evidence.toml` lists the smoke and soak suites against both images as release
evidence, and `just release-evidence <pull-request>` holds a pull request's head revision to it; see
[Developing Nervix](./developing-nervix.md#release-evidence).

## Investigating A Failure

To investigate a CI failure:

1. Download the shard's evidence artifact and read its `summary.md`. Each failed entry names its
   category, phase, reproducer, finding and run directory.
2. Open the run directory's `results/finding.json`, `phases.ndjson` and the console in
   `logs/<entry>.log`.
3. Read the evidence the finding lists.

A reproducer pins the image by digest, so it runs on any qualifying host:

```bash
# Reproduce a single scenario with the recorded options
just chaos run partition-recovery --image ghcr.io/nervix-io/nervix@sha256:<digest> --case follower
# Replay a mixed-instability run from its downloaded directory, with its recorded images
just chaos replay <downloaded-directory>/<suite-id>-<entry>
# Keep a run's containers for interactive inspection, then remove them
just chaos run leader-crash --image ghcr.io/nervix-io/nervix@sha256:<digest> --keep
just chaos cleanup --run-id <run-id> --evidence target/chaos/<run-id>-leftovers
```

An observation, injection, controller or setup failure is a defect of the experiment or its worker,
not a product verdict. It belongs to this suite's owners, and the product claims of that run stay
unestablished until it is repaired. Keep a failed attempt's evidence beside any later passing one:
a passing rerun never converts a failure into a pass.

## Guarantees And Limits

For an image that passes a scenario, the suite guarantees these things about the faults that run
injected and verified, on that worker:

- **Delivery.** Every record the source accepted was delivered with its exact content and branch,
  and no duplicate appeared beyond the identical replays its fault permits.
- **Ownership and consensus.** They recovered within the scenario's bounds, as observed through
  public interfaces.
- **Control-plane effects.** Every acknowledged effect exists, every refused one does not, and
  every node agrees on both.
- **Process faults.** Each one was exactly the planned process fault, on the planned container,
  proven by Docker's own events.
- **Network faults.** Each one blocked or degraded exactly the planned links, and nothing else, for
  at least its planned hold, and healed completely.

Its limits:

- **One host.** A whole-cluster crash keeps the host's kernel, page cache and disks. The suite
  establishes nothing about host power loss, torn writes or storage corruption, or about networks
  of several hosts or zones.
- **Timing.** Fault timing comes from real scheduling, not from a model, so a passing run proves the
  timings it met, not every timing. A replay reproduces the plan and inputs, not the schedule.
- **Network faults.** Partitions and link degradations touch only node-to-node links. The broker,
  the CLI path and the tools keep their routes, and the single Kafka broker is never crashed, paused
  or partitioned. [Backup Faults](#backup-faults) adds two rules off those links: an
  acknowledgement gate on the broker's egress, and a rate limit on the leader's egress to the CLI.
  Each only holds a cut or a download open; no verdict judges the broker or the CLI under them.
  [Restore Faults](#restore-faults) cuts the CLI off the isolated leader only so the CLI reaches the
  next leader before the heal.
- **Restore faults.** The faults land while a restore stages, which a delay makes last. A fault
  during state publication needs a hook, so the Cucumber scenarios that pause a restore before
  publication carry that boundary. The release image exposes no publication generation and no count
  of refused stale requests, so the refusal of a stale coordinator is judged only by the resumed
  domain's traffic.
- **Pause.** A pause is Docker's freeze of a container's processes; it is not a signal the process
  observes.
- **State publication.** A stateful run's durability verdicts for the deduplicator, window and
  materialized relay rest on a drained and held boundary, because the image exposes no publication
  milestone for them.
- **The clock authority.** It is unobservable, so the domain-time verdict identifies it by the round
  whose fault stalled every surviving observer.
- **Diagnostic images.** A diagnostic run proves nothing about untracked waits or an interleaving it
  did not take.
- **Workers and timing.** The suite is qualified on the workers recorded below. Timings measured on
  a loaded worker or a diagnostic image are not product performance evidence.

These limits are tracked as open work:

| Limit | Owner |
| --- | --- |
| A failed baseline, backup, backup-cycles or rolling-restart run keeps no finding | [Cluster Chaos 33](https://app.clickup.com/t/86bc9dkk2) |
| Degraded-link verdicts depend on the worker's load | [Cluster Chaos 37](https://app.clickup.com/t/86bc9vuff) |
| A graceful stop hands the paced graph over but not the clock authority | [Cluster Chaos 51](https://app.clickup.com/t/86bcdh3cz) |
| The runner's own budgets and fixtures for a loaded worker | [Cluster Chaos 53](https://app.clickup.com/t/86bcdkckc) |
| A leader spins more than one CPU while a voter is unreachable | [Cluster Chaos 54](https://app.clickup.com/t/86bcdkcm8) |
| Every load record opens its own broker connection | [Cluster Chaos 55](https://app.clickup.com/t/86bcdkcpu) |

## Qualification Evidence

Each delivering task recorded its runs, worker and images on its own ClickUp task. The full
qualification record, with every run, timing table and image, is on [Cluster Chaos
11](https://app.clickup.com/t/86bc85w03). This section summarizes what those runs established.

**Workers.** The suite was qualified on two kinds of worker:

- GitHub-hosted `ubuntu-24.04` CI workers: Linux 6.17, Docker 28.0.4 with Compose 2.38.2, 4 CPUs
  and 15.6 GiB
- Ubuntu 26.04.1 developer workers: Linux 7.0, Docker 29.8 and 29.9 with Compose 5.5 and 5.6,
  16 to 32 CPUs and 62 to 67 GB, often shared with unrelated builds at load averages up to 85

**Images.** Qualifications used official AMD64 images that CI built from recorded revisions of
`main` and that the runs pulled by digest, together with diagnostic images of the same revisions.
Where a fix had to be qualified before its official image existed, the run layered a server built
from the fixed source tree onto the same official image, and then requalified on the official image.

**Deliveries.** These tasks built the suite:

| Task | Delivered |
| --- | --- |
| [Cluster Chaos 01](https://app.clickup.com/t/86bc85vvz) | The external entry point, the Compose deployment, image and tool identity, and the baseline with its exact ledger |
| [Cluster Chaos 02](https://app.clickup.com/t/86bc85vw2) | Graceful rolling restarts under continuous traffic |
| [Cluster Chaos 03](https://app.clickup.com/t/86bc85vw6) | Leader, follower and execution-owner crashes |
| [Cluster Chaos 04](https://app.clickup.com/t/86bc85vwb) | Short and failover-length pauses |
| [Cluster Chaos 05](https://app.clickup.com/t/86bc85vyc) | Verified partitions, healing and quorum recovery |
| [Cluster Chaos 06](https://app.clickup.com/t/86bc85vyh) | Measured link degradation and bounded recovery |
| [Cluster Chaos 07](https://app.clickup.com/t/86bc85vyn) | Stale-follower snapshot catch-up, former-owner admission and whole-cluster restart |
| [Cluster Chaos 08](https://app.clickup.com/t/86bc85vyq) | Branch-local state and domain time through faults |
| [Cluster Chaos 09](https://app.clickup.com/t/86bc85vzz) | Seeded mixed-instability experiments and their replay |
| [Cluster Chaos 10](https://app.clickup.com/t/86bc85w01) | The smoke and soak suites in CI, against both images |
| [Backup 07](https://app.clickup.com/t/86bc8dbxv) | Backup cycles with exact deduplicator and window verdicts; quiesced backups under an owner crash, a leader crash, an owner partition and a rolling restart of the download; a restore into a fresh three-node cluster; and soak shards 10 and 11, which run them |
| [Backup: Qualify restores through a leader crash, a partitioned coordinator, an owner restart and concurrent staging volume](https://app.clickup.com/t/86bckvq84) | Restores held in staging through a leader crash, an isolation of the coordinating leader from its peers and the client, a state owner's crash, and a state owner's crash while a failed restore awaits reclamation; the measured staging volume of three concurrent restores; and their entries in soak shards 10 and 11 |
| [Typed Ratchet 10](https://app.clickup.com/t/86bc9etg7) | The [endpoints](#endpoints) and [lifetimes](#lifetimes) scenarios, which run the workloads that Typed Ratchet 04, 09, 11 and 12 handed to external qualification, and their smoke shards 2 and 3 and soak shards 12 to 14 |

**Product defects.** The suite found these product defects. Each was fixed through its own task
and requalified with the scenario that found it:

| Task | Defect and fix |
| --- | --- |
| [Cluster Chaos 12](https://app.clickup.com/t/86bc87uqe) | After a crash or a graceful restart, the former bootstrap leader restored its listeners but kept no live peers and its old Raft term, so the cluster never converged. A restarted node now seeds gossip from its recovered Raft membership |
| [Cluster Chaos 13](https://app.clickup.com/t/86bc8az2d) | A Kafka seek that failed during the rewind after a relay-owner crash could skip an accepted record. The source now retries the seek before its next poll |
| [Cluster Chaos 14](https://app.clickup.com/t/86bc8dqzu) | Attributed the historical relay-owner crash that accepted 226 records and emitted 225 to a defect separate from the rewind of task 13. A receiver had admitted a routed batch and acknowledged its own share after forced recovery removed its emitter, so the source committed past a record no emitter wrote. A routed batch that carries record acknowledgements now fails them when no attached consumer of its relay runs on the receiving node, and the source redelivers the record |
| [Cluster Chaos 15](https://app.clickup.com/t/86bc8fh8k) | After a partition healed, an isolated node could stay alone in gossip, or keep a failed outbound interconnect session, so the cluster never converged. Retained gossip routes are restored, and blackholed sessions are retired by HTTP/2 pings |
| [Cluster Chaos 16](https://app.clickup.com/t/86bc8fhah) | While one node was partitioned, the connected peers dropped each other from gossip, and the leader moved work off healthy nodes |
| [Cluster Chaos 20](https://app.clickup.com/t/86bc8py55) | Under one-way loss from the leader to one follower, control commands through the connected quorum went unacknowledged, because completion barriers waited for the unreachable follower. The barriers now use the leader's term-fenced application-health view |
| [Cluster Chaos 21](https://app.clickup.com/t/86bc9479z) | A relay owner could acknowledge a batch that it fanned out while a schedule swap moved an attached consumer, though no consumer processed the batch |
| [Cluster Chaos 22](https://app.clickup.com/t/86bc947an) | A resolution addressed to an earlier incarnation of a node could resolve an unrelated pending acknowledgement or admission of its new process. The IDs are now bound to the registering incarnation |
| [Cluster Chaos 23](https://app.clickup.com/t/86bc947b1) | In consumer-group mode, a rewind could stall on a partition that a rebalance had moved away, which left the ingestor stalled after the rejection that triggered the rewind |
| [Cluster Chaos 24](https://app.clickup.com/t/86bc947bp) | The leader re-added a restarted voter at its new interconnect address on every one-second pass, instead of recording the address once |
| [Cluster Chaos 32](https://app.clickup.com/t/86bc95vz6) | A forwarded record acknowledgement lost during burst loss stalled an `ACK SEQUENTIAL` source. A node now fails a forwarded acknowledgement its receiver stops reporting, as [Cluster Interconnect](./interconnect.md#record-acknowledgements-the-receiver-stops-reporting) describes |
| [Cluster Chaos 39](https://app.clickup.com/t/86bcd4hzv) | A restarted voter lost its work when gossip heard of it before observing it live |
| [Cluster Chaos 40](https://app.clickup.com/t/86bcd8q49) | A stopping follower's drain needed a user password. It now uses the node's own certificate |
| [Cluster Chaos 43](https://app.clickup.com/t/86bcdg30p) and [58](https://app.clickup.com/t/86bcdknqt) | A restarted window owner discarded its open windows. The restore was then requalified on an official image |
| [Cluster Chaos 50](https://app.clickup.com/t/86bcdg8uu) | A forced recovery prepared before the destination installed its schedule recreated a promoted replica's state |
| [Cluster Chaos 52](https://app.clickup.com/t/86bcdkcf1) and [60](https://app.clickup.com/t/86bcdwr9y) | A stopping node's drain was abandoned, through its one-second cordon release and through a Kafka ingestor held quiesced until the drain timeout |
| [Cluster Chaos 56](https://app.clickup.com/t/86bcdk4tn) | A restarting voter required a bootstrap host that no longer resolved, instead of using its recovered Raft members |
| [Cluster Chaos 67](https://app.clickup.com/t/86bcghajy) | The destination of a forced failover held its graph installation, and every later revision, for the deliveries in flight to a paused former owner, up to the 60-second entity gate deadline |
| [Backup: Qualify restores through a leader crash, a partitioned coordinator, an owner restart and concurrent staging volume](https://app.clickup.com/t/86bckvq84) | When forced recovery moved the replica of a Kafka ingestor's domain offsets while the ingestor kept running, the schedule rebound the offsets but left the ingestor committing through the originator of the superseded binding. Every commit was refused, and the source replayed from its last commit for as long as it ran. A reassignment now hands the kept ingestor the originator its offsets' new binding issued |

**Harness corrections.** These tasks corrected the suite itself:

| Task | Correction |
| --- | --- |
| [Cluster Chaos 28](https://app.clickup.com/t/86bc94a1j) | A settled public role read immediately before every crash and pause |
| [Cluster Chaos 29](https://app.clickup.com/t/86bc94a2q) | The live Docker event recording that every event verdict reads |
| [Cluster Chaos 30](https://app.clickup.com/t/86bc94a3d) | Kafka, kcat and probe images pinned by digest, and every tool identity recorded |
| [Cluster Chaos 44](https://app.clickup.com/t/86bcdg36a) | The load produced one record per interval, instead of in kcat's read-block bursts, with a pacing verdict |
| [Cluster Chaos 61](https://app.clickup.com/t/86bcdxk5q) | The rate probe no longer abandons a transfer that loss stalls for over a second |
| [Keep a caught-up leader during directed burst loss](https://app.clickup.com/t/86bcjn14h) | A hold no longer requires the lagging end of a randomly or burst-lossy link to join a connected quorum's leader agreement. `mixed-burst-loss-leader-peer` replays the step that found it |
| [Restore backup soak setup](https://app.clickup.com/t/86bcjn14f) | The backup graph derived again after the Kafka ingestor statement gained its connector and entity kind |
| [A combined degradation's sample that lost nothing](https://app.clickup.com/t/86bcm0fq2) | Random and combined loss judge their floors over 400 packets, as burst loss already did. Forty packets of a working 20% loss missed the combined floor about once in 700 samples |
| [Finish stale-follower inside its controller budget](https://app.clickup.com/t/86bcpkw1q) | Every run executed the full verifier self-check, 139 to 164 seconds on CI workers, inside its own entry budget, so the four-minute `stale-follower` entry reached its bound in its last phase on both images. A suite now executes the check once, before its first run, within its shard's preparation budget, and every run adopts the record. The baseline entries no longer fund the check, so their budgets are their 3- and 4-minute execution allowances |

The suite also corrected defects of its own that surfaced in its qualification runs:

- a certificate subject too long for suite run IDs
- a verifier self-check bound too short for a loaded worker
- verdicts that blamed the clock for a graph's own outage
- Docker launch time charged to a probe's sample

**Negative controls.** These showed that the verdicts fail when they should:

- an impossible 1 MiB memory limit failed both a degraded-link run and a mixed run with one finding
  per node, while their ledgers matched
- the replay of the mixed run failed the same way, with the same findings
- plans that break their policy or overlap their network faults were refused in under a second,
  before any container existed
- replays of tampered or unavailable run directories were refused before they created anything

**Cancellation.** It was qualified against the CI runner's own signal sequence, locally and in CI:

- the interrupted run healed, captured and tore down in seconds and recorded its interruption
- the suite wrote its verdict after its console closed
- the cleanup that followed found nothing left
- a rerun attempt listed the cancelled attempt in its verdict

**The backup entries.** Backup 07 ran the seven entries of soak shards 10 and 11, each on its own
GitHub-hosted worker. They ran against the release and diagnostic images of tested merge
`65b68fe8`, in runs [38933653759](https://github.com/nervix-io/nervix/actions/runs/38933653759) and
[38933658072](https://github.com/nervix-io/nervix/actions/runs/38933658072). All 14 runs passed.
On the diagnostic image, every process start left qualifying evidence with no active, potential or
lost finding. The task records each entry's ledger, outcome and timings.

**The restore entries.** [Backup: Qualify restores through a leader crash, a partitioned
coordinator, an owner restart and concurrent staging
volume](https://app.clickup.com/t/86bckvq84) ran the five restore entries, one per GitHub-hosted
worker, against the release and diagnostic images of tested merge `4a37b26e`, in runs
[39388139905](https://github.com/nervix-io/nervix/actions/runs/39388139905) and
[39387247054](https://github.com/nervix-io/nervix/actions/runs/39387247054). All 10 runs passed with
no missing record, in 4.3 to 7.9 minutes each, and the soak budgets are about two and a half times
those durations. The owner crash ended the restore it interrupted as incomplete on one image and
completed it on the other, so both ends the contract allows were judged. Before that campaign, the
owner crash found the defect in which a Kafka ingestor kept through a replica move stopped
committing its domain offsets. The task records each entry's ledger, outcome and timings, and the
staging volume's samples.

**The current suites.** The current suites run nightly against both images of `main`:

- In Docker Build run [38861733263](https://github.com/nervix-io/nervix/actions/runs/38861733263),
  of revision `8bc67c9a`, every shard of the soak suite passed on both the release and the
  diagnostic image.
- In run [39009603191](https://github.com/nervix-io/nervix/actions/runs/39009603191), of revision
  `ac67de6d`, 17 of 18 shards passed. The 18th failed in seed 42 because a combined degradation's
  40-packet ICMP sample lost no packet under an installed 20% loss.
  [Cluster Chaos: Find why a combined link degradation's 40-packet ICMP sample lost nothing under an
  installed 20% netem loss](https://app.clickup.com/t/86bcm0fq2) found the fault working. The 103
  combined samples of the 13 nightly soaks from 7 to 19 October, on both images, lost 18.7% of
  4,120 packets with the variance of independent loss, and their 134 random-loss samples lost
  29.1% of 5,360. The seeded netem generator of the failed qdisc, replayed from the seed the kernel
  reported, draws 20.0% losses. Forty packets of a working 20% loss miss the 5% floor about once in
  700 samples, so the loss profiles now sample 400 packets. Chaos run
  [39330398655](https://github.com/nervix-io/nervix/actions/runs/39330398655) qualified the larger
  samples on GitHub-hosted workers, with Linux 6.17 and 7.0, against the release image of
  `e69c0801`. Twelve combined runs lost 20.2% of 4,800 packets, at least 64 of 400 against the
  floor of 20, and four random-loss runs lost 30.3% of 1,600, at least 99 against 30. All 16 runs
  passed, and each sample took 5.0 seconds, against 4.9 for 40 lossy packets.
