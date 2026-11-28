# State Inspection

*Plexus, the Nervix graph definition language.*

A running graph holds state: the latest row of every branch of a materialized relay, the keys a
deduplicator remembers, the rows a window retains, the rows a reorderer holds back, the records a
correlator waits to match, the tensors an inferencer carries and the checkpoints of a WASM guest.
State inspection lets an operator and a program read that state while the graph runs. One family
of Plexus statements, `SHOW <kind> <name> [MATERIALIZED] STATE`, reads it, and every read returns
one typed value, the `StateInspection` envelope, beside its text or JSON rendering.

The central rule is that a read is answered by the owner of the state, from what the owner has
already published or holds at a batch boundary, within bounds the owner enforces before anything
crosses the network, and that reading never changes the state it reads. A read names the cut it
saw, counts the whole selection it was asked for, returns at most one bounded page of it, and
fences its continuation to that cut, so a reader never combines two cuts by accident.

This chapter describes the architecture end to end and owns the contract every state read shares.
Each surface keeps its own detail: [Runtime Nodes](./processors.md) and
[Relay](./relay.md#inspect-materialized-state) own the statements and the rows of each kind,
[Client Session Protocol](./client-session-protocol.md#state-inspection-envelope) owns the wire
tables, [Cluster Interconnect](./interconnect.md#materialized-inspection-page-operation) owns the
page operations between nodes, [Data-Plane
Concurrency](./data-plane-concurrency.md#materialized-inspection-capture) owns how an owner captures
state beside the work that changes it, [Node Settings](./node-settings.md#state-inspection-capabilities)
owns the bounds, [WASM Processor Guests](./wasm-processor-guests.md#state-description) owns the
guest's description export and [WASM State And Recovery](./wasm-state.md#observability) the
guest-state facts.

## Ownership By Layer

| Layer | Owner | What it owns |
| --- | --- | --- |
| Vocabulary | State inspection models | Complete counts, the read origin and its `live` or `stored` stage, the typed refusal reasons, the section identities of every processor kind, a resolved selector that keeps only a branch fingerprint, continuation positions, the partition census catalog and the three inspection capabilities' setting definitions. |
| Language | Plexus | The `SHOW ... STATE` heads and the clauses they share: the `FOR` selector, `LIMIT` with its `AFTER` continuation and `FORMAT`, lowered into `ShowRelayMaterializedState`, `ShowProcessorState` and `ShowWasmProcessorState`. |
| Engines and infrastructure | The client wire | The `StateInspection` FlatBuffers tables, the Row sections written straight from Arrow columns by the shared Row encoder, and the text and JSON projections of the same value. |
| Engines and infrastructure | The interconnect | The three bounded page operations a coordinator asks a unit's owner with: materialized, processor and WASM state pages. |
| Engines and infrastructure | The continuation codec | The canonical encoding of a read's cursor and its identity checks, per inspected kind. |
| Engines and infrastructure | The WASM host and the guest protocol | Rendering a guest's description of its committed checkpoint on a scratch instance, and the `StateDescription` payload the guest returns. |
| Data plane | The owners of the state | Answering a bounded read: the materialized relay state, the deduplicator, window and inferencer generations a branch publishes, the reorderer and correlator branch tasks, and the WASM processor's committed guest-state facts. |
| Control plane | The inspection coordinator | Planning a read against one committed schedule, routing each execution unit to its owner, the census during a partition transition, fencing continuations, assembling the page and the `DESCRIBE` counts of stateful processors. The licensing policy answers `DESCRIBE LICENSE` the same way. |
| Edges | The session service and the clients | Encoding the whole reply under the session's limits, the Rust client and the host bindings that keep the typed envelope, the CLI's standalone JSON mode, and the web console, which shows the text projection. |

## The Statement Family

```text
SHOW RELAY <relay> MATERIALIZED STATE <clauses>
SHOW DEDUPLICATOR <deduplicator> STATE <clauses>
SHOW REORDERER <reorderer> STATE <clauses>
SHOW CORRELATOR <correlator> STATE <clauses>
SHOW WINDOW PROCESSOR <window_processor> STATE <clauses>
SHOW INFERENCER <inferencer> STATE <clauses>
SHOW WASM PROCESSOR <wasm_processor> STATE <clauses>

<clauses> :=
  [ FOR ALL BRANCHES | FOR UNBRANCHED | FOR BRANCH KEY { <field> = <literal>, ... } ]
  [ LIMIT <n> [ AFTER '<continuation>' ] ]
  [ FORMAT TEXT | FORMAT JSON ]
```

The head names the inspected entity by its Model kind, as every addressing statement does; a relay
adds `MATERIALIZED` because materialized state is the one state a relay has. The clauses come in
that order, each at most once, and every head shares one grammar for them, so completion offers the
composed phrases `ALL BRANCHES` and `BRANCH KEY` after `FOR` for every kind. A statement reads the
session's active domain and fails without one. Any node serves it: the node a session is attached
to coordinates the read and asks the owners, so a state read is never forwarded to the leader.
`DESCRIBE LICENSE` returns the same envelope for the cluster's licensing state, with no domain and
no clauses.

| Statement | State it reads | Sections |
| --- | --- | --- |
| `SHOW RELAY ... MATERIALIZED STATE` | The materialized row of every branch group | `records`: the relay's fields, then the row's low and high ingestion watermarks |
| `SHOW DEDUPLICATOR ... STATE` | The keys each branch remembers | `keys` |
| `SHOW REORDERER ... STATE` | The rows each route of a branch holds back | `pending` |
| `SHOW CORRELATOR ... STATE` | The records each side of a branch waits to match | `left`, `right` |
| `SHOW WINDOW PROCESSOR ... STATE` | The inputs each branch's window retains | `rows`, with the metadata sections `header` and `accumulators` |
| `SHOW INFERENCER ... STATE` | The `STATE` tensors each branch carries | `header`, then `tensor_1` onwards in `STATE` declaration order |
| `SHOW WASM PROCESSOR ... STATE` | The committed guest-state facts and the guest's description of each checkpoint | None: typed WASM facts instead |
| `DESCRIBE LICENSE` | The published licensing policy | `summary`, `capabilities`, `transitions` |

[Runtime Nodes](./processors.md#inspect-remembered-keys) and
[Relay](./relay.md#inspect-materialized-state) define each section's fields, order and meaning.

### Selecting branches

An omitted selector reads every branch, and the unbranched execution when the entity has one.
`FOR UNBRANCHED` is valid only for unbranched state, and `FOR ALL BRANCHES` and `FOR BRANCH KEY`
only for branched state. `FOR BRANCH KEY` names one complete key: it supplies every field of the
parametrizer's key schema exactly once, each with a literal of the field's exact declared type, a
string, a boolean, a signed 64-bit integer, a finite 64-bit float or a `DURATION` such as `30s`,
with no conversion between them. Any other selector is refused as `invalid_selection`. The selector
is resolved once, while the read is planned, to the fingerprint of the key's canonical text; from
then on the read, its continuation and every refusal carry only that fingerprint, never the key's
values. Selection is the same vocabulary `RESET WASM PROCESSOR ... STATE` uses for its scope.

### Limits and formats

`LIMIT n` bounds the data rows of one reply and must be positive. Without `LIMIT`, a read returns at
most the serving node's `inspection-max-rows`, or the session's collection bound when that is
smaller; a larger `n` is refused as `invalid_limit`. A WASM read counts entries instead of rows. The
default format is text; `FORMAT JSON` renders the same envelope as one JSON document. The format
chooses only the command's message: the typed envelope travels with either, and a program reads the
envelope rather than parsing the message.

## How A Read Is Answered

A read passes through the same stages for every kind.

1. **Parse.** The statement becomes its Model, with the selector, page request and format.
2. **Plan.** The coordinator takes one committed runtime state, the domain's schedule and its
   control revision, and checks the read against it: the entity exists with the inspected kind,
   it has the state the statement reads, its schema and branching are resolved, the selector fits
   its branching, the limit fits the bounds, and a continuation decodes and names this very read.
3. **Route.** The entity's execution units are read in unit order. Each unit's owner is the node
   the schedule runs it on, or the coordinator itself for a unit that runs on every node; a unit no
   live node runs has no owner to ask. The coordinator reads a unit it owns itself in place, and
   asks another owner through a page operation that names the domain, the entity, the unit, the
   committed control revision, the resolved selector, the position, the limits and the owner's
   exact process, and for a page of rows the schema fingerprint.
4. **Capture.** The owner checks that it is that process, that its committed state is still at
   that revision and schema and that it runs the unit, and reads the state from its source for that
   kind. Selection, counting and the limits apply on the owner, so only the selected page crosses
   the network.
5. **Assemble.** The coordinator adds each unit's complete counts to the page's, keeps at most the
   requested rows and the section bound, and issues a continuation when selected state remains.
6. **Reply.** The session service encodes the whole reply under the session's actual limits before
   it queues a byte. A state page that does not fit is replaced by a typed `reply_too_large`
   refusal in the requested format.
7. **Project.** The message is the text or JSON projection of the envelope; clients keep the
   envelope itself.

The whole read, every unit and page included, runs under the serving node's
`inspection-read-timeout`, counted from when the read began.

### Sources by kind

| Kind | What the owner reads | Rows the counts and `LIMIT` count | Bytes the counts report | What a continuation is fenced to |
| --- | --- | --- | --- | --- |
| Materialized relay | Shared Arrow views of the unit's materialized rows, captured under the relay's assignment barrier; a stopped domain's stored state | One row per branch group | The rows' logical payload bytes | The committed control revision, the schema, every captured source lifetime and row watermark (the cut fingerprint), and the last branch and offset |
| Deduplicator | One immutable published generation per selected branch, with expired keys excluded at one domain-clock reading | Remembered keys | The key values and their `seen_at` timestamps | Each branch's publication and visible expiry prefix |
| Window processor | One immutable published generation per selected branch | Retained inputs only | The `MAX STATE SIZE` charge of the retained state | Each branch's publication and lifetime |
| Inferencer | The tensor generation each selected branch published | Tensor rows only | The declared tensors' bytes of each selected branch | The publication cut, the owner incarnations, the committed revision and the selector |
| Reorderer | A bounded copy the branch task makes between two of its batches | Held rows, once per route that holds them | The logical Arrow bytes each route charges against `MAX BATCH SIZE` | Every read branch's lifetime and the events it handled |
| Correlator | A bounded copy the branch task makes between two of its batches | Pending records of both sides | The batches the pending records keep alive, each counted once | As for a reorderer |
| WASM processor | The unit's committed guest-state lifetimes, latest reset, recoveries and checkpoint samples, and a scratch render of each returned checkpoint | None: `LIMIT` counts branch entries, `branches` counts the selected branches and `rows` is zero | Zero | The committed control revision the read was planned at |
| License | The published licensing policy revision and its committed usage | Rows of its three sections | The sections' bytes | No continuation: one complete page |

Published generations are immutable, so a read of a deduplicator, window, inferencer or
materialized relay never waits for the branch task. The rows a reorderer or correlator holds are
owned by their branch task alone, so the read asks the task through its existing request channel,
and the task answers between two batches with an immutable copy. The WASM read never touches the
instance that processes input.

### Partition transitions

While a partitioned entity's partition count changes, a branch can be held by its old partition and
claimed by its new one. A materialized relay, deduplicator, window or inferencer read first takes a
census: it asks each captured owner for a count-only answer with its complete catalog of selected
branch fingerprints, and chooses for each branch the one partition that serves it, the current
partition when that partition has installed the branch and its holder otherwise. It then asks each
owner for pages that exclude the copies chosen elsewhere. Each page names its catalog again, and a
catalog that changed since the census refuses the read as `not_ready`, as do two holders with no
installed claimant, so a read during a transition never returns a missing or duplicated branch.
Reads outside a transition carry no catalog. [Data-Plane
Concurrency](./data-plane-concurrency.md#processor-state-inspection-requests) owns the census and
its exclusion rules.

### Stopped domains

A stopped domain keeps its placement but runs no unit. A materialized relay read of a stopped domain
reads the stored state without starting the domain, and its origin reports the `stored` stage. The
rows a reorderer or correlator holds live only in their running branch tasks, so a read of a
stopped one is refused as `owner_unavailable`, and its `DESCRIBE` reports every count of every unit
as unavailable for the same reason.

## The Envelope

`StateInspection` names the inspected entity and carries exactly one result, a page or a typed
refusal.

- **Entity.** A domain node, its domain and node reference, or the cluster license, which has
  neither a domain nor a Model name.
- **Page.** The complete counts of the whole selection, the number of units, the returned units in
  order and an optional continuation. A WASM page also names the processor's pinned module.
- **Unit.** The execution unit, its owner and its state replicas, the read origin, the unit's
  complete counts and its returned sections. A WASM unit carries its typed guest-state facts.
- **Origin.** The answering node's identity, which includes its process incarnation, a 32-byte
  fingerprint of the exact cut the owner captured (every branch-local source and captured row
  watermark), the generation and revision high-watermarks of that cut, and its stage: `live` for
  running state, `stored` for state read from storage.
- **Counts.** `branches` counts concrete branched keys, so unbranched state counts none even when
  it has a row; `rows` counts the rows `LIMIT` counts; `bytes` is the owner's byte charge for the
  selected state. Counts always describe the complete selection, never only the returned page.
- **Section.** A name, the shared `RowSchema`, the Row frames that carry its rows, the count of
  selected rows the page omitted and, for a materialized section, the positions of its watermark
  columns. A column the inspection appends after a payload, such as a watermark or `seen_at`, takes
  its base name, or the first `<name>_<n>` no payload field holds, so it never collides with a
  user's field.

Metadata sections, a window's `header` and `accumulators` and an inferencer's `header`, describe a
branch rather than its rows: they repeat on every page, consume the reply's bytes and the section
bound, and neither consume `LIMIT` nor advance a continuation. A window branch with no retained
input still has its metadata sections.

The text projection prints the entity, the counts, then each unit's owner, replicas and origin
followed by its sections' rows in the shared subscription Row presentation, and the continuation
last. The JSON projection serializes the same value with every cell typed: integers at full width,
floats by their bits, datetimes and durations in nanoseconds, bytes in base64, nested lists, null and
redacted cells. Neither projection is a second report: both are rendered from the verified
envelope, on the server, for the command's message.

## Continuations

A page that leaves selected state unread carries a continuation, and
`LIMIT n AFTER '<continuation>'` reads the next page. A page can end before `LIMIT` rows, when the
reply's bytes or the owner's memory share end it, and still carry a continuation.

The continuation is an opaque token. Today it is the cursor's canonical JSON in unpadded URL-safe
base64, within the session's string bound, but a client passes it back verbatim and never reads or
builds one. A materialized relay or processor cursor names the domain, the inspected node, its
schema fingerprint, the committed control revision, the cut fingerprint of every captured source,
the resolved selector and the position, either after a whole unit or a unit, branch and row offset
within it. A WASM cursor names the domain, the processor, the committed control revision, the
selector, the unit and the last entry returned.

A resume is a new read under the same fences. It must use the same statement head and selector,
and it is refused as `stale_continuation` when the token is not canonical, belongs to another kind,
entity or selector, or names a revision, schema, unit, source lifetime, publication or position the
owner no longer has. A refused resume starts again from a first page; its rows are never combined
with the abandoned sequence. Nervix keeps no cursor or page cache between requests and promises no
snapshot that spans requests: each page is captured afresh and accepted only when its fences still
hold.

## Refusals

A refused read is a failed command whose envelope carries the reason, rendered in the requested
format. The reason is one of a fixed set; the internal cause behind it is logged at `debug` with its
complete chain where the refusal is constructed, and no refusal names a branch field value.

| Reason | When the read is refused | What a reader does |
| --- | --- | --- |
| `unknown_entity` | The domain or the entity of that kind is not in the committed schedule | Check the name and the active domain |
| `not_materialized` | The relay has no materialized state, or the inferencer declares no `STATE` | Read a stateful entity |
| `invalid_selection` | The selector does not fit the entity's branching, or a branch key is incomplete or mistyped | Write a selector its key schema admits |
| `invalid_limit` | `LIMIT` exceeds the serving node's row bound or the session's collection bound | Ask for fewer rows |
| `stale_continuation` | The continuation is malformed or any of its fences changed, or a change committed between planning the read and the owner's answer | Start a new read |
| `owner_unavailable` | A unit has no running owner, as a stopped domain's reorderer or correlator has none, its owner or branch is stopping, or the owner's page could not be opened before the read's deadline or before liveness ended the owner | Read again once the unit runs |
| `not_ready` | The owner cannot answer yet: a unit still restores its branches, a partition census raced a branch's installation or removal, the entity's schema or branching is not resolved, or the owner admits no more work | Read again |
| `deadline` | The whole read passed `inspection-read-timeout`, or a receiver found no memory for a page before it | Read again, or with a smaller `LIMIT` |
| `reply_too_large` | A page, or a single row, could never fit the owner's or receiver's memory class or the snapshot section bound, or the complete reply exceeds the session's reply bound | Ask for fewer rows; a single oversized row cannot be read |

## Bounds And Memory

Three node capabilities bound a read: `inspection-max-rows`, the rows of one reply;
`inspection-max-sections`, its sections; and `inspection-read-timeout`, the elapsed time of the
whole read. [Node Settings](./node-settings.md#state-inspection-capabilities) owns their defaults and
validation. Each serving node enforces its own bounds, and the coordinator also holds the reply to
the session's limits: its collection bound for rows and sections, its string bound for the
continuation and its transfer bound for the whole reply. A processor page also keeps every body
within the owner's snapshot section bound, a WASM page within one Commands payload, and a guest
render within the processor's guest limits and the host's guest buffer.

A read takes the memory it needs from the bounded executor's classes, and a full class holds it
back; a full class never refuses it. The owner captures and encodes a page within allowances of the
state-read, restore metadata and bulk classes, taken in that order while it holds nothing of them.
The first row of a page is required, and the read waits for room for it; every later row joins only
while the share has room for it now, so a page ends where the share's room ends, with complete
counts and a continuation. A receiving coordinator admits a page's memory one step ahead of what
spends it: the descriptor's length first, then everything the bodies take, before it reads a body.
Processor pages charge for the selected column buffers, the views and indices that gather them,
and the stored run records that name their branches. Inferencer pages size their column builders
for the accepted tensors and headers. Owner pages add the descriptor and Arrow body charges when
they send the page to another node. The reader reports what its current bulk memory can hold with
the bodies' decoding charge. The owner stops before an optional row that would exceed that room.
The reader can wait for memory to receive the first row.
Only a size the whole class could never hold is refused, at once, as `reply_too_large`; a receiver
that finds no room before the read's deadline refuses it as `deadline`. Each step's charge comes from
the charge rule of the owner that spends it. [Cluster
Interconnect](./interconnect.md#processor-state-page-operation) and [Data-Plane
Concurrency](./data-plane-concurrency.md#materialized-inspection-capture) own those steps.

## Deadlines And Liveness

The read's deadline is the deadline of its caller, the coordinator that holds the statement and
waits for the answer: each page operation carries the read's timeout as the caller's deadline of its
exchange, the whole read ends at its own deadline whichever page it waits for, and no exchange has a
timer of its own. Whether an owner or its connection is gone is decided once, by the interconnect's
liveness and by membership, beneath every exchange; a page whose owner liveness ends is refused as
`owner_unavailable`. A busy or slow owner costs latency within the read's deadline, never a failure
of its own. [Liveness And Deadlines](./interconnect.md#liveness-and-deadlines) owns the rule.

## Sensitivity

An inspection shows state no more openly than a subscription shows the rows it delivers.

- A field declared sensitive, and a sensitive field of a branch key, is a redacted cell in every
  read, local or remote; text renders it as `"<masked>"`.
- A value derived from a sensitive field is redacted like it: a deduplicator key expression, a
  reorderer order key or a window argument that reads one. An inferencer's whole `values` cell is
  redacted when any input or branch-key field is sensitive, while port names, shapes and lifecycle
  facts stay visible.
- The columns an inspection adds, watermarks, sequences, arrival and deadline times and the metadata
  sections' facts, are not sensitive by construction.
- A selector's key values stay in the statement: the read keeps only their fingerprint.
- Guest state is always sensitive. Nervix cannot tell which part of a guest's state derives from
  sensitive data, and a guest can hold anything, private keys it derived itself included, so a
  WASM read reports each described section's name, fields, types and row count and never a value.
  Every guest section field is marked sensitive on the wire, and a decoder refuses one that is not.

There is no clause that reveals a redacted value: no inspection statement accepts a leakage
exception, and a separate permission to read guest-state values will come later.

## The Guest's Description

The bytes a guest saves are opaque to Nervix, so a guest describes its own state through the
required `nervix_describe_state` export, which writes a `StateDescription`: named sections, each a
schema and one Arrow IPC batch. For each branch entry a WASM read returns, the branch's owner
creates a scratch instance from the pinned module, initializes it with the branch's `BranchInit`,
restores the branch's committed checkpoint, asks for the description and discards the instance.
The render runs on the node's extension workers, under the processor's `MAX FUEL`, `MAX MEMORY` and
`MAX TABLE ELEMENTS`, the host's guest buffer limit and the read's deadline. It never runs on the
live instance, so it cannot change, delay or fail the branch, and the description lags the live
instance by at most the callback in progress.

A render that fails is reported in its branch's entry, with the `state description` failure stage,
or the stage of the operation that failed before it, and its diagnostic. It never latches the live
instance, fails a checkpoint, applies `ON GLOBAL ERROR` or reaches a session as a runtime error.
[WASM Processor Guests](./wasm-processor-guests.md#state-description) owns the export, the payload
and its validation; [Rust WASM Guest SDK](./wasm-guest-sdk.md#describing-state) owns
`Processor::describe_state` and its empty default.

## Summaries In DESCRIBE

A summary never needs a full read. `DESCRIBE DEDUPLICATOR`, `DESCRIBE REORDERER`,
`DESCRIBE CORRELATOR` and `DESCRIBE WINDOW PROCESSOR` report each execution unit's `branches:`, row
or key count and `state bytes:` through the same owner routing with a page that asks for no row,
and a unit whose owner cannot answer reports the typed refusal in place of each count, as in
`rows: unavailable (owner_unavailable)`. `DESCRIBE INFERENCER` reports each unit's published
branches, bytes and persistence progress, and `DESCRIBE WASM PROCESSOR` the default generation, the
latest reset and the counts of checkpoint stages and recoveries. These summaries read no row,
tensor, key or guest value.

## Concurrency

No recurring path gains a lock, a shared map, a wakeup or a publication step for inspection. An
owner capture is a cold job on the bounded Bulk executor with cancellation checks between its steps:
it clones immutable published generations, or, for a reorderer or correlator, receives the copy its
branch task makes at a batch boundary through the task's existing cold-path request channel. A
branch task answers a read between two batches, so a read never sees a partial batch, and a task
that is stopping refuses it instead. A read never refreshes a key's expiry, steps or emits a window,
changes what a reorderer releases or in which order, holds up a checkpoint, an acknowledgement or a
publication, or touches a live guest. [Data-Plane
Concurrency](./data-plane-concurrency.md#processor-state-inspection-requests) owns these protocols
and their evidence.

## Guarantees And Limits

A read guarantees:

- counts of the complete selected state of every unit it read, whatever the page returns;
- one captured cut per unit, named by its origin's fingerprint, from which every returned row of
  that unit comes;
- a continuation that resumes only under the fences of the cut it was issued for;
- no partial batch of a reorderer or correlator, and no duplicated or missing branch during a
  partition transition;
- sensitive values redacted on every path, and guest-state values never carried;
- a reply within the serving node's and the session's bounds, or a typed refusal;
- no change to the state, its publication cadence or the work that reads and writes it.

A read does not provide:

- one atomic cut across units: each unit's origin names its own cut, captured when its owner read
  it;
- a snapshot across requests, a cursor cache or a page history: a changed cut refuses a resume;
- a WASM description fresher than the committed checkpoint, or any guest-state value;
- inferencer header progress as a fence: persistence and replica progress may advance while the
  tensors stay fixed;
- metrics of its own: the command's typed outcome, its `debug` diagnostics and the `DESCRIBE`
  summaries are its observability;
- state no statement reads yet: the lifecycle of a parametrizer's branches (which branches exist,
  their incarnations and activity order), the keys, claims and parked input of a unit while its
  partition count changes, of which `DESCRIBE PARAMETRIZER` shows only counts, and the messages a
  `REQUIRED WAIT` dependency parks;
- the state of ingestors and emitters, such as Kafka domain offsets, and lookup hash maps, which
  are outside state inspection by decision, nor an archived guest save, which `DESCRIBE BACKUP`
  owns.

## Clients

Every client keeps `CommandOutcome.state` apart from the message, preserves the page-or-refusal
distinction, counts, units, owners, replicas, origins, section schemas, watermark positions, frames
and continuation, and never parses text to recover them.
[Client Implementation Manual](./client-implementation-manual.md#reading-typed-materialized-state)
states what an implementation must do, the [Rust Client Library](./client-library.md#typed-state-reads)
and the host guides show the typed objects, and [Client Host
Bindings](./client-host-bindings.md#typed-inspection) owns how a binding retains frames and their
bytes. The CLI's standalone `--command` mode prints exactly one JSON document for a single
`SHOW ... STATE` statement with `FORMAT JSON`
([Command Line Client](./client-tools-cli.md#session-and-transaction-statements)). The web console
shows the text projection.

## Evidence

| Method | What it establishes |
| --- | --- |
| Cucumber | One- and three-node public scenarios, most of them with interleaved keys of two branches: `tests/features/runtime/materialized_state_inspection.feature`, `processor_state_inspection.feature`, `published_processor_state_inspection.feature`, `inferencer_state_inspection.feature`, `wasm_state_inspection.feature`, `partition_state_inspection.feature` and `native_binding_inspection.feature` |
| Shuttle | The branch task's answer to a read, whole batches in arrival order or a refusal after the end, over the production owner (`shuttle_a_branch_inspection_answers_whole_batches_in_arrival_order_or_is_refused_after_the_end`) |
| Turmoil | Materialized pages recovering after a partition and lost stream chunks (`materialized_inspection_pages_recover_after_partition_and_lost_stream_chunks`) |
| Bolero | Lossless round trips of the envelope, the published processor sections, both cursor kinds, the three page messages, the guest's state description on both sides, the host views and the Python enums (`client-state-inspection`, `client-published-processor-sections`, `state-inspection-cursor`, `wasm-state-inspection-cursor`, `interconnect-state-inspection`, `interconnect-processor-state-inspection`, `interconnect-wasm-state-inspection`, `wasm-state-descriptions`, `wasm-host-state-descriptions`, `client-host-inspection-views`, `client-python-inspection-enums`) |
| Chaos | Every `SHOW ... STATE` family read through the packaged CLI while the three-node `owner-crash` fault kills the state owner and it recovers ([State Inspection During Failover](./cluster-chaos.md#state-inspection-during-failover)) |
| Benchmark | `just bench-state-inspection`: the cost a concurrent reader adds to a materialized relay owner and a window branch, at the default row bound and near the session transfer bound |
| Client conformance | The Python, Ruby and JVM suites' cluster-tier inspection tests and the C suite's contract read the typed envelope through each binding |

## Adding A Readable State

A new kind of runtime state, a new section, count or bound of an existing kind joins this contract
in the change that adds it:

- a `SHOW <kind> <name> STATE` head through the shared clauses, or a new section of an existing
  head, with parse, negative and completion tests;
- a source on the owner that reads published or batch-boundary state without a recurring lock, map
  or wait, applies selection and bounds before transport, takes its memory within the shared
  allowances and is fenced by a continuation that names everything a resume depends on;
- the section's name and fields in the vocabulary, its sensitivity derived from what it reads, and
  its counts' meaning stated in its owning chapter;
- the page operation's descriptor, the wire tables when the kind has typed facts, and their Bolero
  properties;
- `DESCRIBE` counts for the entity that holds it;
- the Rust client, every host binding and guide, and the CLI's standalone JSON mode;
- red-first one- and three-node scenarios with two branches, a Shuttle check when a task answers the
  read, and chaos coverage when the state is a branch-local runtime-state kind;
- this chapter, the owning manual section and the Plexus skill's routing.
