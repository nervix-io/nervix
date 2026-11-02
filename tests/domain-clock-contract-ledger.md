# Domain clock contract and regression map

This map records clock ownership, named public scenarios and reusable fixtures for the
[domain clocks epic](https://app.clickup.com/t/86bbwcry1).
Historical audit findings, red baselines and validation results belong to their producing tasks:
[01](https://app.clickup.com/t/86bbwcrya), [02](https://app.clickup.com/t/86bbwcryd), [03](https://app.clickup.com/t/86bbwcryf), [04](https://app.clickup.com/t/86bbwcryg), [05](https://app.clickup.com/t/86bbwcryk), [06](https://app.clickup.com/t/86bbwcrz9), [07](https://app.clickup.com/t/86bbwcrza), [08](https://app.clickup.com/t/86bbwcrzd), [09](https://app.clickup.com/t/86bbwcrzf), [10](https://app.clickup.com/t/86bbwcrzh), [11](https://app.clickup.com/t/86bbwcrzy), [12](https://app.clickup.com/t/86bbwct04), [13](https://app.clickup.com/t/86bbwct06), [14](https://app.clickup.com/t/86bbwct0e).

The Cucumber scenarios exercise Plexus and runtime behavior through public endpoints and session
subscriptions. The clock-contract scenarios and arithmetic regressions run in the ordinary suite;
this map identifies their scope without asserting a result for a particular revision.

## Clock classification

| Clock class | Owns |
| --- | --- |
| Domain logical time | `now()`; `TIMESTAMP NOW` evaluated at delivery; generated event metadata; message-error occurrence; explicit `EACH`, `EVERY`, `COLLECT FOR`, `FLUSH EACH`, and `COMMIT EACH` durations; branch and materialized-state TTL; processor retention; window completion and emission; guest-requested WASM timeouts |
| Preserved source time | Explicit external event timestamps and broker metadata. Window membership uses the preserved event timestamp; emitted window high watermarks use domain logical time. |
| Physical monotonic time | Connect, TLS, RPC, and transport deadlines; retries and backoff; ACK and liveness; health and Raft timers; grants and byte-transfer limits; cancellation; shutdown and drain; snapshot maintenance; executor safety; the `FLUSH IMMEDIATE` 100 microsecond system batching minimum; the 5 millisecond source-idle collection bound |
| Actual UTC | External and administrative observation timestamps and HTTP-date `Retry-After` interpretation. A wait derived from an actual-UTC value is converted to a monotonic deadline. |

An explicitly unpaced domain supplies real UTC through the domain-time capability. A missing,
stopped, or uninstalled paced clock is a typed lifecycle outcome and cannot select a wall-clock
fallback. OTEL user `VALUES` use domain logical time, while `observed_time_unix_nano` uses physical
observation time. An omitted Sentry timestamp uses domain logical time; an explicit timestamp is
preserved.

A committed paced-clock mapping consists of a logical origin, physical UTC anchor, validated rate,
and lifecycle generation. Local reads cannot decrease within a generation. Progress reports never
replace or reanchor the mapping. Only an explicit `START` establishes a new mapping generation.
Node join, restart, leadership transfer, and scheduled-owner movement preserve the committed
mapping. An automatic `ALTER` pause withholds intake while logical time continues to progress.

Every live node installs the committed clock before executing the domain. One serialized and
generation-fenced authority emits bounded, coalescible progress. A stale start, stop, or progress
message has no effect on a later generation, and a transport message cannot create a domain.

Admission centers are `origin + n * PERIOD` for nonnegative `n`. Only centers through the mapping's
reached frontier are eligible, and Nervix retains the newest 256 eligible centers. The origin is
eligible as soon as the domain starts. `SKEW` is an inclusive logical tolerance around each eligible
center. Receipt delay does not change eligibility, and tolerance cannot make an unreached future
center eligible.

Recurring work retains its existing first-occurrence behavior and then stays anchored to its
logical schedule. Missed occurrences coalesce into one execution at the latest due instant, then
continue at the first future boundary; they do not burst through every missed instant. The due
logical time and fresh execution time are distinct inputs. Work that wakes after waiting revalidates
its lifecycle generation.

Per-node nondecrease and a consistent committed mapping are guarantees. Simultaneous equality on
different hosts and a distributed total event order are not guarantees. UTC synchronization is an
operator precondition; host-clock error scales with a paced rate, and `SKEW` is not a clock-sync
budget. Physical deadlines remain independent of wall-clock adjustments.

Every timestamp constructed by Nervix fits the signed Unix-nanosecond representation used by
persistence, wire protocols, and Arrow. Rates are finite and positive, and periods are positive and
supported. These values are validated at their owning boundary. Reachable overflow yields a typed
diagnostic; it cannot reset an anchor or survive behind an invariant claim.

## Coverage assignments

| Contract edge | Scenario and owner | Topology |
| --- | --- | --- |
| Origin, reached frontier, and inclusive `SKEW` | `A paced domain admits an event at its historical logical origin`, extended by `Admission retains exactly 256 reached logical tick windows with inclusive SKEW` in task 05 | One- and three-node random schedules |
| Retained 256 centers and future-center exclusion | `Admission retains exactly 256 reached logical tick windows with inclusive SKEW`, task 05 | One- and three-node random schedules |
| Delayed progress and local nondecrease | `Delayed clock progress cannot move observed logical time backwards`, tasks 02–04 | One- and three-node random schedules |
| Mapping generations and stale lifecycle messages | `One fenced authority emits coalesced progress for each clock generation`, task 04 | One scenario with the production sticky scheduler: two nodes, and a third added during the run |
| Missed cadence coalescing and fresh due time | `Slow external polling coalesces missed cadence with fresh due timestamps`, task 09 | One- and three-node random schedules |
| Logical collection/flush and physical minima | `Paced branch collection and flush follow logical time while Immediate and source idle remain physical`, task 07 | Two interleaved branches on one and three nodes, preserving fields |
| Logical emitter cadence and physical retry/ACK | `Emitter cadence follows logical time while retry and ACK deadlines remain physical`, task 08 | One- and three-node random schedules with controlled emitter failure |
| Fresh branch activity and logical retention | `Fresh interleaved records correlate after all initial branch state retires`, task 10; `Paced branch expiration follows domain logical time`; the production-owner unit `accepted_processor_input_records_branch_activity_at_its_own_domain_time` | Two interleaved branches on one and three nodes preserve fields and retire initial state before fresh correlations; the expiration outline checks logical retention, and the unit checks that accepted input refreshes domain-clock activity |
| One snapshot across expression contexts | `Every expression context observes its domain execution time`, task 06 | One- and three-node random schedules through ingestion, a junction, and a subscription, extended by focused generator, inferencer, SQS, database, Iceberg, Sentry, and OTEL scenarios |
| Failure, guest, generated metadata, and telemetry clock classes | `Generated errors WASM timeouts and telemetry timestamps use their assigned clock classes`, task 06 | One- and three-node random schedules with historical domain time, error routing, WASM lifecycle callbacks, tokenless output, and external telemetry fixtures |
| Client observation of the committed mapping, generations, and admission window | `A session attaches to a domain clock once and follows every generation` and `A client paces simulated events by the domain clock it is attached to`, [Clock Attach 01](https://app.clickup.com/t/86bc8azgz) | One- and three-node random schedules; the three-node attachment is served by a follower |
| Attached tick progress and pacing | `A session attaches to a domain clock once and follows every generation`, `A client paces simulated events by the domain clock it is attached to`, and `Nonleader clock-owner loss preserves the committed mapping`, [Clock Attach 02](https://app.clickup.com/t/86bc8azjm) | One- and three-node sessions check increasing tick ids, boundaries and state ordering; direct owner-loss keeps the original mapping and resumes higher ids, while the runtime observer unit `a_paced_generation_without_an_authority_is_observed_uninstalled` checks the unassigned-to-reassigned installation sequence |
| Authority takeover independent of another node's activation | `An installed survivor takes over the clock while another survivor installs the authority revision` and `A prepared clock observer receives replacement ticks before another node activates the revision`, [Cluster Chaos 65](https://app.clickup.com/t/86bcg7bby) | Three nodes with the production sticky scheduler, health failure and held runtime preparation; local and remote attached sessions receive replacement ticks within ten seconds and keep the mapping through recovery |
| Authority takeover from a lost consensus leader, ahead of a held graph installation | `A survivor takes the clock over from an authority that led consensus while the survivor's runtime installation waits` and `A survivor receives replacement clock ticks while its own runtime preparation waits`, with the leader retry unit test `leader_retries_an_authority_reassignment_its_leadership_refused`, [Cluster Chaos 66](https://app.clickup.com/t/86bcgad9k) | Three nodes with the production sticky scheduler. In the first, the lost authority also led consensus, the new leader's first authority proposal is refused, and the replacement authority's runtime installation is held; in the second, a receiver's runtime preparation is held. In both, a remote attached session receives replacement ticks within ten seconds and keeps the mapping through recovery |
| Attach reply and frame order, and refusal inside a transaction | `A domain clock attachment reply precedes its frames, a detach reply follows them, and a transaction refuses both`, [Clock Attach 01](https://app.clickup.com/t/86bc8azgz); the production observer and delivery order under `shuttle_delivery_sends_state_before_ticks_without_regressing_progress`, [Clock Attach 02](https://app.clickup.com/t/86bc8azjm) | One- and three-node sessions over the raw protocol; the Shuttle check explores accepted ticks, same-generation unassignment and reassignment, and a generation change |
| Attachment end when the domain leaves the serving node | The session unit `a_session_follows_a_domain_clock_until_it_detaches_or_the_domain_leaves_the_node`, [Clock Attach 01](https://app.clickup.com/t/86bc8azgz) | Unit only: no statement removes a domain from the committed state |
| An attach that reaches a node before its first installation of the committed domains | `The CLI follows a domain clock across a cluster restart` and `The CLI refuses to follow the clock of a missing domain`, the Shuttle check `shuttle_an_attach_waiting_for_the_first_installation_observes_its_domains`, and the units `an_attach_answers_once_its_node_has_installed_the_committed_domains`, `an_attach_waiting_for_the_committed_domains_ends_with_its_session` and `committed_domains_are_installed_once_the_first_committed_state_is_applied`, [Clock Attach: Keep domain clock attachments across node restarts](https://app.clickup.com/t/86bc90n6k) | One and three nodes, the restart outline across a whole-cluster restart |
| The CLI's clock output | `CLI attaches its session to the active domain's clock`, `The CLI follows a domain clock through its generations` and `The CLI restores a domain clock after transport loss`, [Clock Attach 03](https://app.clickup.com/t/86bc8azkb) | One and three nodes; the transport-loss scenario on one |
| The console's clock display | `The selected domain clock follows its mapping, ticks, and lifecycle` and `A reconnected console restores one attachment to its selected domain`, [Clock Attach 04](https://app.clickup.com/t/86bc8azmc) | One and three nodes through the browser; the reconnect scenario on three nodes across a leader change |
| Rust client restoration of followed clocks | `A producer and consumer resume their unchanged endpoint after a session restart`, and the client units `an_attached_clock_is_attached_again_on_a_new_session_and_reports_its_clock`, `a_refused_clock_restoration_is_repeated_on_the_same_session`, `a_clock_restoration_answered_already_attached_follows_the_new_session`, `a_clock_restoration_that_reaches_no_server_is_tried_again_by_the_next_read`, `an_attachment_moved_before_its_old_session_ends_keeps_the_tick_it_reported`, `a_detach_sent_while_a_refused_restoration_waits_releases_the_clock_for_good`, `a_detach_the_old_session_answers_during_a_reconnect_stays_released_on_the_new_session`, `a_detach_refused_during_a_reconnect_leaves_the_clock_restored_on_the_new_session`, `a_clock_attached_while_a_reconnect_opens_its_exchange_is_attached_again_on_it` and `a_restoration_left_unanswered_past_the_request_timeout_wakes_the_waiting_reader`; the Shuttle check `shuttle_a_detach_racing_a_restoration_leaves_no_attachment_of_its_clock` over the production clock owner | One and three nodes; the units over a scripted session, whose exchanges the test answers through the client's own reader to stage a redirect move, a detach racing a restoration and an unanswered restoration |
| The shared binding's attached clock and events | `A <runtime> client reads the running domain clock it attached to before its ticks and keeps its generations apart`, [Clock Attach 05](https://app.clickup.com/t/86bc8azn1) and [05A](https://app.clickup.com/t/86bc95p44); the [conformance ledger](client-conformance-ledger.md) records each runtime | One and three nodes for the C ABI in process, C, C++, Python, Java and Ruby |
| Client producers and consumers paced by an attached clock | `paced_simulation.feature`: `The <driver> driver paces readings by the attached clock at time rate <time_rate>`, `… keeps pacing through a node that executes none of its endpoints while the clock authority moves away`, `… readings stamped before the admission window are rejected under TIMESTAMP AT and admitted under TIMESTAMP NOW …`, and `… stops with its domain and continues in the next START generation without mixing generations …`, [Client I/O 05](https://app.clickup.com/t/86bc8bj8d) | The Rust and the shared-binding driver, at time rates 0.5 to 4.0, on one node and on three; the authority-move case pins the endpoints and relays off the authority with the production sticky scheduler |

## Reusable fixtures

| Need | Fixture |
| --- | --- |
| Historical paced time | The historical-origin public scenario starts at `2000-01-01T00:00:00Z`. |
| Slow paced time | `Paced branch expiration follows domain logical time` uses rate `0.01`, and task 08's held-flush, immediate, and acknowledgement-keepalive emitter scenarios reuse it. Task 10's fresh-activity scenario pairs rates `2.0` and `0.5` with logical TTLs `6s` and `1500ms` so both rates reach the same physical retention. |
| Fast paced time | `Prometheus ingestor follows paced domain logical time and cadence` and `Domain commands isolate session context and drive a replicated clock` use rate `4.0`; task 08's emitter and Iceberg cadence scenarios use rate `20.0`. |
| Time-dependent expressions | The delayed-progress scenario emits `now()` into a schema-backed `DATETIME` field before and after controlled progress delivery. |
| Delayed progress | `domain clock progress for domain ... is paused before delivery`, its bounded reached assertion, and `... resumes` form a test-only barrier around the next delivery. Teardown releases an armed barrier. |
| Recorded external source | The explicitly started HTTP mock exposes `/clock-source/{name}`. Each request records its actual UTC nanoseconds, monotonic nanoseconds, query parameters, and count. `delay_ms` controls response latency. `clock source recorder ... is reset` and its bounded request-count assertion use `/clock-source-observations/{name}`. |
| Emitter retry and ACK | Existing emitter fault/stall controls and broker observers drive the scenarios `Kafka ACK ingestor waits while an attached emitter is stalled`, `Kafka ACK SEQUENTIAL replays on attached emitter failure`, and task 08's named cadence scenario together with `A stalled emitter keeps its upstream acknowledgement alive on the physical clock`. ACK state remains in memory. |
| Join, restart, leader, and owner loss | Existing cluster start/stop/restart, leadership transfer, placement inspection, and entity-gate controls are reused. Clock-owner scenarios select the production sticky scheduler before startup, following `Losing the former owner during a held drain leaves relocation to failover`. |

The fixture endpoints do not provision product-owned external objects. A scenario starts the mock
dependency explicitly and scopes recorder names with `{{test_id}}`.

## Physical infrastructure deadline audit

A `Duration` is configuration, not a clock coordinate; it becomes physical only when the owner starts a monotonic timer. A domain clock is
introduced only for an explicit Plexus cadence or semantic lifetime. The resulting ownership is:

| Semantic owner | Physical operations retained by that owner | Bound and evidence |
| --- | --- | --- |
| Connector HTTP client | DNS, socket connection, TLS, request, response headers, and response-body transfer | The client's request timeout spans the request future and is separate from an ingestor's domain-bound `EVERY`. `HTTP request timeout stays physical while logical polling cadence scales` observes one timed-out request at rate `0.0001`, at least three at rate `100`, and at least 200 milliseconds between starts under a 250 millisecond timeout. |
| Polling ingestor lifecycle | Quiesce and shutdown cancellation of an in-flight HTTP or Prometheus operation | The HTTP request, HTTP response body, and complete Prometheus query futures race their task's shutdown and quiesce signals. `Cancelling an external poll stays physical across extreme clock generations` bounds `STOP` by 1500 milliseconds while a 30-second request is in flight, before and after replacing the historical clock generation. |
| Broker and connector runtime | Reconnect backoff, broker RPC/confirmation timeout, ACK timeout, and ACK-alive refresh | `RuntimeReconnectBackoff` and emitter confirmation waits use monotonic timers. `EmitterRetrySchedule` holds physical retry and ACK-alive deadlines separately from its still-armed logical flush or commit cadence. `Delayed acknowledgements stay alive on physical time at extreme domain rates` holds a Kafka delivery for 1200 milliseconds without replay at both extreme rates and both topologies. |
| Interconnect connection | TCP and TLS setup, certificate lifetime, reconnect, stream progress, request completion, idle retirement, and drain | Setup and progress waits use monotonic time. Certificate validity is read in actual UTC once and its remaining lifetime becomes a monotonic expiration deadline. A request constructs one deadline before acquiring its local queue lease and passes only the remaining duration to transfer, so queueing consumes the operation's bound. `stream_slot_queueing_consumes_the_request_deadline`, `membership_removal_cancels_an_active_request`, and `shutdown_cancels_an_active_request` are the direct unit controls. |
| Cluster liveness | Gossip probing, health RPCs, node-unavailability detection, TLS reload, and peer retirement | These are process and node liveness operations driven by monotonic intervals and cancellation tokens. `Silent peers obey the same shutdown bound without and after a domain clock` applies the same 12-second node-shutdown bound with no installed clock and after a `0.0001` to `100` generation change. |
| Raft | Election, heartbeat, membership mutation, replication RPC, snapshot transfer, and retained-log admission | OpenRaft receives physical heartbeat and election durations. RPC options become interconnect request deadlines; heartbeat traffic has a dedicated management connection, while snapshot chunks carry the remaining monotonic transfer deadline. Retention admission is bounded by one physical timeout around its queueing loop. |
| Source grouping and route buffers | Five-millisecond source-idle collection and the 100-microsecond `FLUSH IMMEDIATE` coalescing minimum | Source idle records a Tokio instant at accepted input. `FLUSH IMMEDIATE` constructs an opaque physical deadline; `COLLECT FOR` and `FLUSH EACH` construct logical deadlines. The task 07 branch-buffering scenarios and the `each_and_immediate_keep_distinct_deadline_coordinates` unit control cover the split. |
| Snapshot and maintenance owners | Runtime-state snapshot ticks, Raft snapshot and log-retention scans, branch maintenance scans, metrics maintenance, and connector maintenance | Each owner uses a Tokio interval or monotonic deadline and applies missed-tick policy locally. None binds a domain clock. Snapshot data and ACK state keep their existing persistence classifications. |
| Execution and memory admission | Worker admission, bounded queue capacity, cancellation, blocking-job completion, and shutdown join or abort grace | The executor owns its queue bounds and cancellation handles. Dropping queued work releases its reservation; running work observes cancellation while retaining its charge until exit. Runtime lifecycle owners impose physical grace periods on task joins and drains. |
| Materialized dependency owner | `REQUIRED WAIT` retention and explicit cancellation | There is no total-lifetime timer. Missing state retains the admitted Arrow batch and its ACK guard, wakes on state change or a physical availability poll, and restarts dependency resolution from the first declaration. Shutdown and terminal drain explicitly no-ACK the retained work. The public scenario retains two interleaved branches beyond 1200 milliseconds, preserves their keys and fields when state arrives, and then bounds cancellation of a third branch. |

### Permitted actual-UTC observations

Actual UTC has these semantic owners. None of these observations waits directly on wall time.

| Semantic owner | Permitted observation |
| --- | --- |
| Domain-clock mapping | An unpaced read, the physical anchor of a new paced mapping, and paced projection observe actual UTC through the runtime physical-time boundary. Per-node reads are clamped so they do not move backward within one generation. Logical waits convert their remaining duration into a process-local monotonic deadline. |
| External telemetry connectors | OTEL observation timestamps and omitted external observation fields use actual UTC. Sentry and OTEL interpret an HTTP-date `Retry-After` against actual UTC, convert the result to a duration, and hand it to the emitter's monotonic retry schedule. User-authored event timestamps and domain `VALUES` keep their declared logical or preserved-source classes. |
| Interconnect security | Certificate `not before` and `not after` values are compared with actual UTC. A valid certificate's remaining lifetime is converted immediately to a monotonic connection deadline. |
| Edge decoding | RFC 3164 syslog, whose wire form omits a year, supplies the current UTC year while decoding the external source timestamp. |
| Control-plane records | Transaction, resource, credential-verification, administrative event, and observation metadata use actual UTC because they describe real control-plane events rather than domain execution. |
| Cluster incarnation | A process start uses Unix-epoch nanoseconds as the restart generation that distinguishes gossip incarnations. It is identity metadata and never schedules a wait. |
| Metrics | Monotonic instants age live wall series. Actual UTC stamps persisted wall snapshots so restart downtime can be observed and supplies external scrape and latency-observation timestamps. Domain-series windows and event watermarks retain domain timestamps. |

Expression engines receive a bound execution snapshot from their caller. Their public execution
interfaces do not expose a context-free clock path and the engines do not observe actual UTC.
