# Domain clock contract and regression ledger

This ledger is the acceptance record for [Domain clocks 01](https://app.clickup.com/t/86bbwcrya)
under the [domain clocks epic](https://app.clickup.com/t/86bbwcry1). The source audit was made at
`fdd79a3cd7ed0d93be53f3710cf9ad67a87af574` on 8 September 2026. The four isolated failures from
that audit are represented here in current-source tests without repairing their product paths.

The Cucumber scenarios exercise NSPL and runtime behavior through public endpoints and session
subscriptions. Expected failures carry `@clock_contract_expected_failure`; the default Cucumber
run excludes that tag, while an explicit `--tags` selection runs the recorded red baseline. Direct
arithmetic regressions use ignored desired-behavior tests for the same reason. A later owning task
removes the quarantine when its repair makes the assertion pass.

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

## Finding ownership and named public scenarios

| Finding | Audited defect | Owning delivery task | Named public scenario |
| --- | --- | --- | --- |
| F1 | Joined and restarted nodes miss the anchor/rate; nonleader owner loss is not reconciled. | [03](https://app.clickup.com/t/86bbwcryf), [04](https://app.clickup.com/t/86bbwcryg) | `A joining or restarted node installs the current clock generation before execution`; `Nonleader clock-owner loss preserves the committed mapping` |
| F2 | Producers compete; authority and generation fences are absent; progress can implicitly create a domain. | [04](https://app.clickup.com/t/86bbwcryg) | `One fenced authority emits coalesced progress for each clock generation` |
| F3 | Delayed emission or replay reanchors logical time backwards. | [02](https://app.clickup.com/t/86bbwcryd), [03](https://app.clickup.com/t/86bbwcryf), [04](https://app.clickup.com/t/86bbwcryg) | `Delayed clock progress cannot move observed logical time backwards` |
| F4 | Admission and `TIMESTAMP NOW` mix physical and logical coordinates. | [05](https://app.clickup.com/t/86bbwcryk) | `A paced domain admits an event at its historical logical origin`; `Admission retains exactly 256 reached logical tick windows with inclusive SKEW` |
| F5 | Collection and flush policy is duplicated; emitter cadence is physical; Immediate is simulated; retry and cadence share state. | [03](https://app.clickup.com/t/86bbwcryf), [07](https://app.clickup.com/t/86bbwcrza), [08](https://app.clickup.com/t/86bbwcrzd) | `Paced branch collection and flush follow logical time while Immediate and source idle remain physical`; `Emitter cadence follows logical time while retry and ACK deadlines remain physical` |
| F6 | VM contexts, `VALUES`, inference, and subscription expressions can read wall time. | [06](https://app.clickup.com/t/86bbwcrz9) | `Every expression context observes its domain execution time` |
| F7 | Errors, tokenless WASM, and omitted generated timestamps use wall time. | [06](https://app.clickup.com/t/86bbwcrz9) | `Generated errors WASM timeouts and telemetry timestamps use their assigned clock classes` |
| F8 | HTTP polling ignores pace; Prometheus polling reuses stale or duplicate due times. | [09](https://app.clickup.com/t/86bbwcrzf) | `Slow external polling coalesces missed cadence with fresh due timestamps` |
| F9 | Branch activity is sampled before awaiting input. | [10](https://app.clickup.com/t/86bbwcrzh) | `Activity sampled after an awaited record keeps each logical branch alive` |
| F10 | Timestamp range and clock arithmetic invariants are unenforced. | [02](https://app.clickup.com/t/86bbwcryd) | `Out-of-range paced starts and projections return typed timestamp diagnostics` |

The cross-cutting physical deadline matrix belongs to [11](https://app.clickup.com/t/86bbwcrzy),
bounded progress transport belongs to [12](https://app.clickup.com/t/86bbwct04), architecture and
public documentation belong to [13](https://app.clickup.com/t/86bbwct06), and complete qualification
belongs to [14](https://app.clickup.com/t/86bbwct0e).

## Coverage assignments

| Contract edge | Scenario and owner | Topology |
| --- | --- | --- |
| Origin, reached frontier, and inclusive `SKEW` | `A paced domain admits an event at its historical logical origin`, extended by `Admission retains exactly 256 reached logical tick windows with inclusive SKEW` in task 05 | One- and three-node random schedules |
| Retained 256 centers and future-center exclusion | `Admission retains exactly 256 reached logical tick windows with inclusive SKEW`, task 05 | One- and three-node random schedules |
| Delayed progress and local nondecrease | `Delayed clock progress cannot move observed logical time backwards`, tasks 02–04 | One- and three-node random schedules |
| Mapping generations and stale lifecycle messages | `One fenced authority emits coalesced progress for each clock generation`, task 04 | Three nodes with the production sticky scheduler for owner loss; generic generation cases also run on one node |
| Missed cadence coalescing and fresh due time | `Slow external polling coalesces missed cadence with fresh due timestamps`, task 09 | One- and three-node random schedules |
| Logical collection/flush and physical minima | `Paced branch collection and flush follow logical time while Immediate and source idle remain physical`, task 07 | Two interleaved branches on one and three nodes, preserving fields |
| Logical emitter cadence and physical retry/ACK | `Emitter cadence follows logical time while retry and ACK deadlines remain physical`, task 08 | One- and three-node random schedules with controlled sink failure |
| Fresh branch activity and logical retention | `Activity sampled after an awaited record keeps each logical branch alive`, task 10 | Two interleaved branches on one and three nodes, preserving fields |

## Reusable fixtures

| Need | Fixture |
| --- | --- |
| Historical paced time | The historical-origin public scenario starts at `2000-01-01T00:00:00Z`. |
| Slow paced time | `Paced branch expiration follows domain logical time` uses rate `0.01`; task 10 extends that graph for fresh activity and retention. |
| Fast paced time | `Prometheus ingestor follows paced domain logical time and cadence` and `Domain commands isolate session context and drive a replicated clock` use rate `4.0`. |
| Time-dependent expressions | The delayed-progress scenario emits `now()` into a schema-backed `DATETIME` field before and after controlled progress delivery. |
| Delayed progress | `domain clock progress for domain ... is paused before delivery`, its bounded reached assertion, and `... resumes` form a test-only barrier around the next delivery. Teardown releases an armed barrier. |
| Recorded external source | The explicitly started HTTP mock exposes `/clock-source/{name}`. Each request records its actual UTC nanoseconds, monotonic nanoseconds, query parameters, and count. `delay_ms` controls response latency. `clock source recorder ... is reset` and its bounded request-count assertion use `/clock-source-observations/{name}`. |
| Sink retry and ACK | Existing emitter fault/stall controls and broker observers drive the scenarios `Kafka ACK ingestor waits while an attached emitter is stalled`, `Kafka ACK SEQUENTIAL replays on attached emitter failure`, and task 08's named cadence scenario. ACK state remains in memory. |
| Join, restart, leader, and owner loss | Existing cluster start/stop/restart, leadership transfer, placement inspection, and entity-gate controls are reused. Clock-owner scenarios select the production sticky scheduler before startup, following `Losing the former owner during a held drain leaves relocation to failover`. |

The fixture endpoints do not provision product-owned external objects. A scenario starts the mock
dependency explicitly and scopes recorder names with `{{test_id}}`.

## Baseline probes and controls

The desired-behavior unit probes established by task 01 are:

```text
runtime::domain_clock::tests::delayed_progress_delivery_does_not_move_logical_time_backwards
runtime::domain_clock::tests::paced_domains_admit_the_logical_origin
runtime::domain_clock::tests::scheduled_timestamp_addition_stays_in_the_serializable_range
runtime::domain_clock::tests::logical_time_projection_reports_range_overflow
```

Task 02 made the delayed-progress, scheduled-timestamp, and projection probes part of the ordinary
suite. The historical-origin probe remains ignored for task 05. The passing arithmetic control is
`runtime::domain_clock::tests::logical_rate_conversion_scales_physical_waits`.

The focused public scenarios are selected independently:

```console
just test-scenarios --input tests/features/runtime/domain_clock_contract.feature --tags @delayed_clock_progress
just test-scenarios --input tests/features/runtime/domain_clock_contract.feature --tags @logical_origin_admission
just test-scenarios --input tests/features/runtime/domain_clock_contract.feature --tags @domain_bound_clock
just test-scenarios --input tests/features/runtime/domain_clock_contract.feature --tags @domain_clock_authority
```

The delayed-progress scenario is part of the ordinary suite after task 02. The logical-origin
scenario remains quarantined for task 05. The untagged
`Out-of-range paced starts and projections return typed timestamp diagnostics` scenario records
task 02's F10 public coverage.

Physical controls are
`tests::connection_lifetime::send_queue_admission_is_deadline_bound`,
`tests::connection_lifetime::silent_outbound_tls_handshake_reaches_the_setup_deadline`, and the
public scenario `Peer churn and silent handshakes leave the node responsive`. Source-time controls
are `Unpaced HTTP ingestors accept explicit timestamp fields without a domain clock` and `Unpaced
Kafka ingestors accept explicit timestamp fields without a domain clock`.

The evidence recorded for this task consists only of the focused commands listed in the task's
validation record. It does not assert that the complete Cucumber suite or final domain-clock
qualification matrix has run; task 14 owns that claim.

## Task 01 validation record

Recorded on 8 September 2026 before the task 02 implementation:

| Probe | Result | Evidence |
| --- | --- | --- |
| Delayed progress unit regression | Expected red | Logical time moved from `1970-01-01T00:00:01Z` to `1970-01-01T00:00:00Z` when the delayed tick arrived. |
| Historical-origin admission unit regression | Expected red | Admission rejected the logical origin as outside every retained tick window. |
| Scheduled timestamp arithmetic unit regression | Expected red | Adding one nanosecond to `i64::MAX` produced a `Timestamp` that JSON could not serialize as signed Unix nanoseconds. |
| Logical-time projection arithmetic unit regression | Expected red | Projecting one nanosecond from an `i64::MAX` origin produced the same unserializable timestamp state. |
| Logical-rate conversion unit control | Pass | A one-second logical wait at rate `4.0` converted to a 250 millisecond physical wait. |
| `Delayed clock progress cannot move observed logical time backwards` | Expected red in both the one- and three-node examples | The public `now()` observation after controlled delivery was about one second earlier than the observation made while delivery was paused. |
| `A paced domain admits an event at its historical logical origin` | Expected red in both the one- and three-node examples | The event at the exact `START AT` origin produced no relay payload. |
| `External source fixture records request timing and count` | Pass in both the one- and three-node examples | Each explicitly started mock recorded one request, its query and timing data, and returned the expected relay payload. |
| HTTP explicit source-timestamp control | Pass in all three existing topology examples | An unpaced HTTP ingestor preserved its explicit event timestamp without a domain clock. |
| Kafka explicit source-timestamp control | Pass in all three existing topology examples | An unpaced Kafka ingestor preserved its explicit event timestamp without a domain clock. |
| Interconnect send-queue deadline control | Pass | Queue admission remained bounded by its monotonic deadline. |
| Silent outbound TLS deadline control | Pass | A silent peer reached the physical setup deadline. |
| `Peer churn and silent handshakes leave the node responsive` supplemental control | Red on both attempts | The third node remained unavailable after peer churn. This wider lifecycle failure is recorded separately from the passing focused physical-deadline controls and from the four clock regressions above. |
| Cucumber test target compilation with `testing` enabled | Pass | The scenario runner and all new step definitions compiled. |
| Default-feature workspace check | Pass | The workspace compiled without the test-only clock barrier enabled. |
| Default run of `domain_clock_contract.feature` | Pass | The two recorder-fixture examples passed; expected-failure examples were excluded by their quarantine tag. |
| `just validate` | Pass | Repository formatting, linting, checks, skill validation, and documentation tests completed successfully. |
| `just ratchet` | Pass | Every architecture-debt count remained at or below its checked-in baseline. |

No complete Cucumber-suite or final qualification result is claimed by this record.

## Task 02 validation record

Recorded on 8 September 2026 against the task 02 worktree:

| Probe | Result |
| --- | --- |
| Public out-of-range START reproducer before product changes | Expected red in all four one- and three-node endpoint examples; the invalid timestamp reached Raft serialization. |
| `Out-of-range paced starts and projections return typed timestamp diagnostics` | Pass in all four one- and three-node endpoint examples; 36 steps passed. |
| `Delayed clock progress cannot move observed logical time backwards` | Pass in both one- and three-node examples; 32 steps passed. |
| `nervix-models` unit suite | Pass; 98 tests covered timestamp endpoints, validated rates and periods, rounding, direct advancement, and overflow. |
| Runtime domain-clock unit suite | Pass; six task 02 tests passed. The historical-origin probe remains ignored for task 05. |
| `just validate` | Pass, including all-feature workspace Clippy and all 139 executable NSPL documentation blocks. |
| `just ratchet` | Pass with the new clock model included; the checked-in string-error debt fell from 520 to 516. |

No complete Cucumber-suite or final qualification result is claimed by this record.

## Task 03 validation record

Recorded on 9 September 2026 against the task 03 worktree:

| Probe | Result |
| --- | --- |
| Public join/restart reproducer before product changes | Expected red; execution on the joined node had no installed paced-clock mapping and produced no relay payload. |
| `A joining or restarted node installs the current clock generation before execution` | Pass; one scenario and all 12 steps passed through node join, full-cluster restart, node-local HTTP ingestion, and a historical `now()` observation. |
| Runtime domain-clock unit suite | Pass; 16 tests covered typed missing, stopped, uninstalled, and stale-generation outcomes; nondecreasing reads; pause preservation; cancellation; generation revalidation; deadline domain identity; due/fresh snapshots; and shared handle allocation. The historical-origin probe remains ignored for task 05. |
| Runtime domain-execution unit suite | Pass; four tests covered clock installation before execution binding, paused-state preservation, stop cleanup, and rejection of an uninstalled paced clock. |
| Physical monotonic deadline unit | Pass; the opaque physical capability waited on Tokio's monotonic timer. |
| WASM explicit invocation-time unit | Pass; one explicit context supplied guest time and timeout-request time without leaking into the next invocation. |
| Deadline capability compile-fail suite | Pass; both UI cases rejected crossing a logical deadline into the physical waiter or a physical deadline into the logical waiter. |
| `just validate` | Pass, including all-feature workspace Clippy with warnings denied, skill publication validation, and all 139 executable NSPL documentation blocks. |
| `just ratchet` | Pass; every architecture-debt count remained at or below its checked-in baseline, and the checked-in string-error debt fell from 514 to 512. |

No complete Cucumber-suite or final qualification result is claimed by this record.

## Task 04 validation record

Recorded on 9 September 2026 against the task 04 worktree:

| Probe | Result |
| --- | --- |
| Public live-owner-transfer reproducer before product changes | Expected red; after the third voter joined, the newly selected node never produced progress and the node-specific delivery barrier timed out. |
| `One fenced authority emits coalesced progress for each clock generation` and `Nonleader clock-owner loss preserves the committed mapping` | Pass; two scenarios and all 46 steps passed through live membership expansion, a held progress report across `STOP` and a later `START`, affected-node verification of the replacement mapping, leader transfer, follower and owner restarts with new incarnations and addresses, nonleader owner loss, and transfer to the surviving authority. |
| `A joining or restarted node installs the current clock generation before execution` | Pass; one scenario and all 12 steps verified join and full-cluster restart installation before node-local HTTP execution. |
| `Delayed clock progress cannot move observed logical time backwards` | Pass in both the one- and three-node examples; all 32 steps passed with progress held across physical time and then released. |
| Authority and progress unit coverage | Pass; pure selection covered membership and incarnation changes, consensus covered revision fencing and identical direct/transactional lifecycle commits, and runtime covered exact generation/revision/incarnation/peer fences, stopped and restarted generations, missing domains, bounded tick history, and execution refusal without installed authority. |
| `just validate` with `RUSTC_WRAPPER=kache` | Pass, including all-feature workspace Clippy with warnings denied, skill publication validation, and all 139 executable NSPL documentation blocks. |
| `just ratchet` | Pass; every architecture-debt count is at or below its checked-in baseline. Clamped-arithmetic debt fell from 13 to 11 and string-error debt fell from 512 to 510. |

No complete Cucumber-suite or final qualification result is claimed by this record.
