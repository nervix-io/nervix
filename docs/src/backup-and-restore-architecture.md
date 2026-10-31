# Backup And Restore Architecture

*Plexus, the Nervix graph definition language.*

A backup copies a whole cluster, or one domain, into one archive file on the machine of the client
that ran it. A restore recreates what an archive holds in a fresh cluster, in the cluster it came
from, or under a new domain name. This chapter defines how both work end to end: which layer owns
each part, the archive contract and its compatibility rules, how a domain is cut and its state
captured, how sections travel and become an archive, how a restore receives, plans and records its
progress, how restored state is staged and published as one complete generation on every node, how
a restored domain resumes, and what every failure leaves behind.

Three rules shape the design. The unit of consistency is the domain: a cluster backup is the users
plus every domain, each captured at its own cut, because Nervix has no semantics that span domains.
The archive is the one external contract the alpha stability rule permits: it is decoupled from
every internal storage representation, and its readers validate everything before they trust it.
And a restore never makes partial state visible: every node stages the complete state set of a
restored domain and selects it atomically, and a replicated gate keeps the domain from running until
every node has done so.

This chapter describes the architecture. [Backup And Restore](./backup-and-restore.md) owns the
statements, their options, the command-line and console surfaces, the archive's section and record
tables, the operator limits and the reported failures. [WASM State And Recovery](./wasm-state.md),
[Consensus Storage And Replication](./consensus-storage-and-replication.md), [Cluster
Interconnect](./interconnect.md), [Data-Plane Concurrency](./data-plane-concurrency.md) and
[Shutdown And Recovery](./shutdown.md) keep the contracts of the mechanisms backup and restore use.

Canonical emitter documents use Plexus connector/entity heads and complete typed destination bodies.
Restore reads those documents through the current composed grammar into semantic Models. Each
entity owns its publishing policy and exact external locator; Iceberg retains its storage-client
reference for planning. The registry's binary Model record header identifies this current stored
shape, and restoration uses the same validated Model-to-plan boundary as ordinary configuration.

## Ownership By Layer

| Layer | Owner | What it owns |
| --- | --- | --- |
| Vocabulary | Backup and restore models | The cut kinds and the quiesce counters a quiesced cut records, the archive summary a finished backup reports, a restore archive's declared identity (its size and BLAKE3 digest), the restore installation authority, and the restore report with its steps. |
| Language | Plexus | `BACKUP`, `RESTORE` and `DESCRIBE BACKUP`, lowered into Models. |
| Engines and infrastructure | The archive crate, `nervix-backup` | The archive format: the manifest, section paths, record kinds and their versions, the validated records the sections decode into, the manifest-first tar layout, the streamed record writers and the streaming reader that verifies an archive. It names the vocabulary, rkyv, tar and BLAKE3, and nothing of consensus, the registry, the runtime or where an archive is staged or sent. |
| Engines and infrastructure | Node staging | Quota-owned staged files for archives, captured sections and received uploads, and the node's staging quota. |
| Engines and infrastructure | The runtime state store | Listing a domain's checkpoints in one database snapshot, streaming restored checkpoints into an invisible generation namespace, durably selecting a complete generation through one pointer, and bounded reclamation under the restore staging quota. |
| Engines and infrastructure | Consensus | The restore execution record and its recorded steps, the commands that apply a step together with its record, the installation authority it admits, and the replicated installation gate. |
| Engines and infrastructure | The interconnect | The typed drain-status, capture, inventory, section-fetch and state-installation operations, with their pools, subquotas, deadlines and coordination identity. |
| Decisions | The restore planner | A pure plan from the archive description, the statement, the existing users and domains, and the steps already recorded: the user policy, the name checks, the resource versions a restore imports and the versions every model pins, the stopped domain state and the `RESUME` activation. |
| Decisions | The transaction planner | Each restored domain's model run, planned against the domain as the restore will create it. |
| Data plane | Owner capture | For the placements the node is the scheduled primary of: forcing branch publications and lifecycle checkpoints, the capture fence, sealing source offsets and lifecycle durably, one database snapshot, and producing each section into node staging. |
| Data plane | Branch restore | Installing every branch a restored lifecycle names, rebuilding windows from their retained rows, and binding restored state as a branch starts. |
| Control plane | The backup coordinator on the leader | Executing an admitted `BACKUP`: each domain's cut and the nodes it must reach, owner capture, section transfer, assembly under the staging quota, retention under the execution reference, and the report. |
| Control plane | The restore coordinator on the leader | Staging and verifying an archive, preparing and planning it, admitting the restore, applying its steps in order, and installing state. |
| Control plane | Restore checkpoint maintenance on every node | The sweep that reclaims abandoned restore attempts and unreferenced checkpoint chunks, and the staging measurements it publishes. |
| Edges | Archive assembly and restore preparation | Rendering committed Models as canonical Plexus text and parsing it back, and parsing archived text into Models. Both name the language parser, which is an edge dependency. |
| Edges | The session service | The `DownloadBackup` and `RestoreBackup` calls over gRPC and the console's WebSockets, and their typed replies. |
| Edges | Clients | `nervix-cli backup`, `restore` and `DESCRIBE BACKUP`, the Rust client's command wait, download and resend, and the web console's Backups dialog. |

A backup reads the committed control plane and the state stores of the owners that hold a domain's
state, and writes nothing back except its command record and the domain lifecycle changes of its
cut. A restore writes the control plane through consensus commands and writes state only into an
invisible namespace until publication selects it. Neither changes how the data plane runs: owners
capture under the same publication fences and assignment barriers that order ordinary checkpoints,
and a restored domain starts through the same execution plans as any other.

Membership, cordons, admission fences, mutation leases, transactions and their reports, command
executions and the retry fence, clock authority assignments and computed placements are never
backed up or restored. A restore recomputes placements for the cluster it runs in, and a restored
domain's clock authority is chosen there.

## The Archive Contract

An archive is one tar stream whose sections are of four kinds: canonical Plexus text for each
domain's models, the original bytes of a resource version's upload, a WASM guest's own saved bytes,
and records the archive crate owns. Records are rkyv; materialized relay rows, deduplicator keys and
window rows travel as Arrow IPC under the user's own schemas beside an rkyv descriptor. There is no
JSON in an archive. [Archive Format](./backup-and-restore.md#archive-format) lists every section and
record.

### Records Mirror The Contract

Archive records embed vocabulary types, such as names, timestamps, typed scalar values, branch keys
and schema fingerprints, and never a consensus, registry or runtime structure. The conversions
inside the archive crate and its callers are the only place an internal shape reaches the format, so
a change to an internal type changes those conversions and leaves the archive alone. Models travel
as canonical text because the canonical renderer and parser are the existing round-trip-tested
encoding of every model kind, independent of how the registry stores Models; an archive-owned Model
tree would duplicate the whole vocabulary, and the stored representation would couple every archive
to it. A backup renders each domain's models, parses the rendered text back, and refuses the archive
unless it yields exactly the committed models.

Every record section opens with a twelve-byte header: the magic `NVXBKREC`, then the record kind
and its format version as little-endian 16-bit integers. A reader checks the magic, then the kind,
then the version for exact equality, copies the payload into an aligned buffer and validates it with
bytecheck before it reads a value, and then validates each vocabulary value in the record's
conversion. A changed shape therefore fails at its header instead of validating as garbage.

### Layout And Verification

The writer lays an archive out completely before it writes a byte: every section's length and
BLAKE3 digest is known, so the total size is too. The manifest is the first entry and lists every
other section in the order the stream holds them, with its path, content kind, length and digest.
Every section entry is a regular file with owner-only permissions, owner and group zero, and a zero
modification time; a path longer than a tar header's name field is carried by a GNU long-name entry
before it.

A reader verifies an archive as a stream, without seeking:

1. The first header must be the regular `manifest.rkyv` entry within the 64 MiB record limit;
   anything else is refused before the manifest is read.
2. The manifest must hold archive format major version `2`, valid domain names, no duplicate path,
   and a known record kind for every record section.
3. Every following entry must be the next section the manifest names, with that exact length,
   within its content kind's bound, and with that digest once its bytes are drained. A missing,
   extra, misplaced or non-regular entry is refused.
4. A record that owns other sections comes before them: a resource version record before its upload,
   a WASM descriptor before its guest save, and a materialized, deduplicator or window descriptor
   before its groups. Within a descriptor's groups, and between the two sections of one group, the
   order is free. Each record must sit at the path its own contents name.
5. Once the stream ends, every domain must hold its domain record and models, every descriptor every
   group and every group both of its sections, and a cluster archive its users record.

A section a reader does not decode is still read whole for its length and digest. `DESCRIBE BACKUP`
runs this verification before it prints anything, and a restore runs it before it plans anything.

### Compatibility

The archive supports exactly one shape of each record: its current kind and version. There is no
migration, dual read or older-version path. Configuration records refuse the archive when their
header differs: the manifest, users, domain and resource version records are required to restore
anything. The seven state record kinds are optional: a state record whose header names a different
state kind than the manifest declares, or a version the reader does not support, is skipped with
its owned sections, and the restore reports a warning naming it so that entity starts without that
checkpoint. Any other defect in a state record, and an unknown kind tag in the manifest itself,
refuses the archive. Arrow sections must be one canonical IPC stream of the expected schema; the
restore scans each one before Arrow's reader sees it, as described under [Restore Intake And
Preparation](#restore-intake-and-preparation).

An archive holds password hashes, client secrets, TLS private keys, payload rows and branch keys.
Archive files are written owner-only, and no diagnostic, log or report echoes their contents.

## Cut Kinds

A backup captures each domain at one cut, in sequence, and records which kind it took:

| Cut | When | What it guarantees |
| --- | --- | --- |
| Quiesced | A running or paused domain, by default | Every input acknowledged before the cut is reflected in the archive and no admitted work remained at the cut. Inputs that were not acknowledged are redelivered by acknowledged sources after a restore, or lost for sources without acknowledgements, exactly as after a restart. The cut records when it engaged and released and what intake buffered, dropped and rejected meanwhile. |
| Stopped | A stopped domain | The domain's stored checkpoints, read from one database snapshot on each owner. The domain needs no pause. |
| Live | `WITHOUT PAUSE`, whatever the domain's status | The latest published checkpoints, captured without pausing a running domain: what a `SIGKILL` restart would recover. State and configuration need not be from one quiesced moment. |
| Configuration only | `WITHOUT STATE` | Models, the domain record and resources, with no state sections. |

A quiesced cut and a stopped cut hold the domain's replicated mutation lease, owned by the backup's
execution reference, so no transaction, alteration or other command changes the domain while it is
captured. A live cut and a configuration-only capture take no lease.

A stopped cut reads what the domain's stop left on disk. That stop completes only once every
deduplicator and window branch it ended has written what it published last, including a branch
ended at its stop grace whose snapshot task was still writing, and every materialized relay has
written what it held once the tasks that change it ended, however long that write outlasts their
stop grace, as [Draining Admitted Work](./shutdown.md#draining-admitted-work) describes. A stopped
cut that begins as soon as `STOP` returns therefore holds the same keyspaces, windows and
materialized generations a quiesced cut taken just before the stop held.

## The Quiesced Cut

A quiesced cut freezes a domain for one drain and one publication round, and the transfer happens
after the freeze ends. The leader's backup coordinator runs it in this order:

1. **Budget.** The cut's deadline is the statement's `TIMEOUT`, or the 60-second domain drain
   timeout when none is given, counted from before the lease wait.
2. **Tenure and lease.** The coordinator records its leader tenure, then acquires the domain's
   mutation lease for the execution, retrying every 50 milliseconds until the deadline; a
   committing transaction is waited for, not failed. The same execution rejoins a lease it already
   holds. It then takes the domain's leader-local alteration guard, which keeps the leader's own
   reconciliation from resuming the paused domain, and failover and automatic placement from
   moving its work, while the cut is held.
3. **Reach.** From the domain's committed schedule, which the lease now holds still, the
   coordinator takes every node the schedule assigns the domain's work to, and the scheduled
   primary of every placement whose state the archive captures. When the cluster does not report
   one of them live, the coordinator refuses the cut before anything pauses, naming the domain, an
   entity of that node and the node. It could neither observe that node's admitted work nor
   capture its state: a crashed node lost what it held, and a partitioned one may still admit
   work. It refuses as well, naming the domain and the entity, when the schedule places an entity
   whose state the archive captures on no node, as when every node that could host it is cordoned
   or down: that entity's last state lies on a node the cut cannot name.
4. **Pause.** A replicated pause stops intake as each ingestor's `ON QUIESCE` declares, and every
   live node applies the paused state. Applying the pause spends the cut's budget, as every step
   before it does, and the cut's deadline ends it: the coordinator waits for an installation slot,
   and for every live node to prepare and then ready the paused revision, only until the deadline.
   When the deadline ends one of those waits, the coordinator resumes the domain at once and the
   cut is refused in its drain phase, naming the wait the deadline ended. A local installation or
   activation the coordinator had begun finishes first, so no node is left holding part of a
   revision, and every node's own reconciliation goes on applying the newest committed revision,
   which is now the resume: a node that already applied the paused revision applies the resume
   after it, and one that had not yet installed it installs the resume instead. The replicated
   pause write itself is not cut short. A write abandoned after it was proposed may still commit
   and leave the domain paused with no cut left to resume it, so the coordinator waits for the
   write's outcome; a pause that commits after the deadline is resumed without the coordinator
   waiting for the cluster to apply it. An alteration's pause keeps waiting for as long as the
   cluster's own revision budget allows.
5. **Drain.** Every 50 milliseconds, the coordinator polls every live node and every node the
   schedule assigns the domain's work to for the admitted work it still holds: admitting ingestors
   and generators, outstanding source acknowledgement roots, relays that admitted a batch while the
   node read its view, relay and node buffers, and emitter buffers and publishes. That is the view
   the shutdown drain uses. A poll installs no runtime state: the pause had every live node apply
   the paused state before the first poll, and a node that had not yet stopped its intake would
   report its ingestors still admitting. Each poll therefore costs one request to its node, never a
   runtime installation that waits for every live node to prepare and ready its revision, so a
   loaded cluster's rounds do not consume the cut's budget. A scheduled node that stops answering
   keeps the round open until the deadline. A polled node that still holds admitted work starts a
   force-flush generation to release it, unless one is already running; a node that holds none
   starts none. Two kinds of held work are exempt, because their acknowledgements wait for input
   the paused domain does not admit and the cut captures them as state: messages parked by
   `REQUIRED WAIT`, and the rows a window retains until it steps past them, whose acknowledgement
   shares park the same way. Each force-flush generation re-evaluates the parked messages against
   the state present, and a message whose dependency is still absent stays parked through that
   read, so the re-evaluation adds no admitted work and cannot restart the round, however slowly
   the state is read. Once every node reports no admitted work, the coordinator asks every node to
   start a confirming force-flush generation and waits until every node reports it finished with
   no admitted work. New work restarts the round. A domain that holds no admitted work therefore
   re-evaluates its parked messages for the confirming generation alone, so a slow dependency read
   holds the cut for that read, after any periodic re-evaluation already under way when the
   confirmation arrives. A generation started for a node with nothing to release would be
   superseded by the confirmation after its participant had taken it, and the participant would
   read the dependency for both. The drain polls until the cut's own deadline. It never measures a
   budget of its own before the pause and restarts it once every node has applied the pause, so a
   pause the cluster applies slowly leaves the drain less time instead of lengthening the freeze.
6. **Capture.** Under the remaining budget, the coordinator checks its tenure, reads the domain's
   configuration at the applied revision, which becomes the revision and Raft log entry the archive
   records, and asks every scheduled state owner to capture at that revision, as described in
   [Capture On The Owners](#capture-on-the-owners). It refuses the cut when one of them is no
   longer reported live, and checks its tenure again afterwards.
7. **Resume.** The coordinator always resumes the domain, outside the deadline, and records the
   release time and the quiesce counters.
8. **Release.** It always releases the lease, and checks its tenure a last time.

A pause the cluster has not applied by the deadline fails the backup with the wait the deadline
ended and resumes the domain. A drain that does not finish by the deadline fails the backup with the
counts each node still held and resumes the domain; an unavailable emitter keeps its publish and
acknowledgement counts outstanding until then. A tenure change at any check refuses the cut, still attempts the resume and the release,
and leaves the command applying. The next leader holds no alteration guard for the domain, so its
reconciliation resumes the domain if it is still paused, and it re-executes every applying backup
under its execution reference: it rejoins the command's lease, pauses again, cuts, resumes,
releases, and retains its own archive for the client that retries the reference.

A stopped cut takes the lease and asks the scheduled state owners to capture without guard, pause
or drain, and refuses, as a quiesced cut does at its capture, when the cluster does not report one
of them live or the schedule places an entity whose state it archives on no node. Only then are the captured sections transferred, so neither the archive's size nor a
client's download rate lengthens a freeze.

### The Capture Fence

Branch lifecycle publications, Kafka offset commits and WASM checkpoints register on a per-domain
generation before they change what they have published. When a quiesced capture starts on an owner,
it closes that generation and waits for every publisher registered before the close to leave; a
publisher that arrives afterwards waits for the next generation. Every publication that registered
before the cut therefore completes into the database view the cut opens, and none that registered
after it can change that view. A live capture does not close the generation. [Data-Plane
Concurrency](./data-plane-concurrency.md) defines the fence's orderings; its Loom models are listed
under [Qualification Evidence](#qualification-evidence).

## Capture On The Owners

The coordinator sends one capture request, naming the domain, the cut revision and whether the cut
is quiesced, to every node the committed schedule names as the primary of a placement whose state
the archive captures: a WASM guest save, Kafka domain offsets, the branch lifecycle of an ingestor,
a reingestor or a processor, a materialized relay, a deduplicator keyspace or a window. A live cut
holds no lease, so the schedule can move a placement while it runs; it sends the request to every
other live node as well, and each owner stages what its own applied schedule assigns it. A cut
whose scheduled state owner the cluster does not report live fails, naming the domain, the entity
and the node, and so does a cut whose schedule places such an entity on no node, naming the domain
and the entity; neither assembles an archive without that state. A remote request's deadline is
what remains of the cut's budget, because staging a domain's state takes as long as that state is
large. A live cut's owners receive the default 60-second budget. The coordinator's own share of a
stopped or live capture runs without a deadline.

An owner accepts a capture only from the current leader. It waits up to five seconds to apply the
cut revision, brings its runtime plan current, checks the sender is still the leader, and reads the
domain's status. An owner that cannot reach the revision refuses the capture, which fails the
backup.

For a quiesced cut, the owner first asks each ingestor, reingestor and processor it executes to
checkpoint its current branch lifecycle, including branches created since the periodic snapshot.
Each deduplicator and window branch publishes on that request: a deduplicator its keyspace, a
window its retained rows when they changed. A WASM branch publishes nothing and its guest is not
asked to save: the checkpoint that completed its last callback already holds its guest state on the
owner and on every replica it named, so the cut archives that save as the state store holds it.
Saving the guest again would hold the frozen domain while the new save reached those replicas.
Capture then runs as one storage job:

1. A quiesced capture closes the domain's capture fence.
2. Every Kafka offset state and branch lifecycle checkpoint assigned to the node is sealed into the
   state store and synchronized to disk. A lifecycle that has not published its first checkpoint
   contributes none.
3. One database snapshot lists the domain's checkpoints by placement and stored size without reading
   their payloads; WASM guest saves are already durable in it. Everything the capture reads later
   comes from this snapshot, however late it is read.
4. A branch lifecycle is read only when a WASM, deduplicator or window branch of its entity needs a
   typed key, one lifecycle at a time, and only the needed keys are kept. A branch its lifecycle no
   longer holds was evicted and contributes nothing, even when its checkpoint remains for local
   reappearance.
5. A running or paused domain contributes the deduplicator keyspaces and windows its branch tasks
   published, each superseding its stored checkpoint, and the fresh materialized relay generations
   it captures under each relay's assignment barrier. A stopped domain contributes stored
   checkpoints only, materialized relays included.
6. Each selected state becomes archive sections in node staging, one checkpoint at a time.

| State | How it becomes sections |
| --- | --- |
| WASM guest save | The save stays in the cut's snapshot until its scheduled generation has been checked, so a superseded save is never opened. The storage job records a segmented header's bounded size, and staging admission uses that size without a database read on an async worker. Saves are opened one at a time: the inline checkpoint conversion is charged to `restore_metadata`, and the raw bytes stream into a quota-owned staged section under a fixed 192 KiB bulk working charge, so neither another save nor an encoded guest section is held whole during the write. One read supplies the bytes and the descriptor's revision, and the staged writer seals the section's actual length and digest. |
| Kafka offsets and branch lifecycle | The checkpoint is read once into an aligned allocation charged to `restore_metadata`, its entries convert one at a time, and the record streams through a 64 KiB bulk buffer into a staged file that seals its exact length and digest. Serializer scratch of one resolver per entry is charged beside it. Each section reserves the 64 MiB record limit of node staging while it is written. |
| Materialized relay | Shared Arrow row views, typed branch keys, per-row watermarks, revision, ownership fence and branch generation, taken under the relay's assignment barrier with at most 8 MiB of row metadata charged first. Capture reads the entries current at the cut, including updates since the periodic snapshot, and never waits for that interval; a live capture takes the same complete generation under the barrier. An empty relay contributes an explicit empty generation. A stopped relay is read from its stored checkpoint view, one bounded group at a time, retaining the original Arrow bytes. |
| Deduplicator keyspace and window | Every key with its first-seen time, or every retained row with its aggregate arguments, admission sequence, watermarks and branch incarnation and every linear histogram's pending delayed removals, written as an rkyv descriptor beside bounded Arrow groups. A branch with no live publication on its owner, an inactive window with a sealed publication included, contributes its stored checkpoint. |

A stored keyspace or window selection retains the cut's database view until its scheduled owner and
schema are checked. The owner opens one selected checkpoint at a time, streams it to quota-owned disk
in 64 KiB blocks, and admits an immutable mapping of it after unlinking its temporary name; the
checkpoint's revision comes from the same read as its bytes. Keyspace validation compares borrowed
native keys for uniqueness and records their sizes, and only the next archive group is converted into
owned key parts. A stored window validates its header, sequences, identities and delayed removals,
indexes its native column sections, decodes one native group at a time to find the canonical archive
boundaries, releases those columns, and then reads only the sections each input or argument group
overlaps. The descriptor and every group's order, bytes, length and digest follow the same archive
contract as a live publication's.

Capture fills each Arrow group to at most half of its bound, so a group's encoding never exceeds it;
a single larger row or key takes a group of its own. Cancellation is checked between stored chunks,
identities, keys and entries, and an interrupted conversion drops its private files and returns their
quota. A section that cannot be staged because the node's staging quota is full is refused rather
than waited for. A staged section stays registered under the capture's coordination identity until
the coordinator fetches it or 600 seconds pass.

Only the node the committed schedule names as a unit's primary captures its placements. An owner
runs the capture steps above for the placements it is the primary of and for no others, so the
archive holds each placement once. A replica's copy is never sealed, listed, read or charged, so a replica's
admission refusal cannot fail a cut. Owners agree on the primary because the cut's mutation lease
keeps the domain's schedule from changing, automatic placement included, while the cut is held; a
replica owns a placement only once failover promotes it, after a cut has released the lease. An
entity that executes on every cluster node, such as an HTTP or WebSocket endpoint ingestor or a
Syslog listener, still has a scheduled primary: the first node by name that the schedule assigns
it to, and only that node archives its branch lifecycle. After an owner has staged its sections,
the coordinator asks it for its inventory: the path, length, digest and kind of every section it
staged for this capture.

## Transfer, Assembly And Retention

Backup uses these interconnect operations, all bound to a coordination identity that names the
coordinator's node, its process epoch and a sequence, which the transport ties to the authenticated
sending process:

| Operation | Direction and form | Pool and subquota | Deadline | What the receiver checks |
| --- | --- | --- | --- | --- |
| Drain status | The coordinator to every live node and every node the schedule assigns the domain's work to, unary, polled every 50 ms; the coordinator answers itself in-process | Management, Liveness | 2 seconds per request, within the cut's budget | The sender is the current leader. Without installing runtime state, which the pause already applied, the node starts a force-flush generation while it holds admitted work and none is running, starts a confirming force-flush generation, or only observes, and reports its admitted-work counters. |
| Capture | The coordinator to every scheduled state owner, and for a live cut every other live node too, concurrently; the coordinator answers itself in-process | Management, Shared | What remains of the cut's budget | The sender is the current leader, the node has applied the cut revision within five seconds, its runtime plan is current and the sender is still the leader. |
| Capture inventory | The coordinator to each node, unary | Management, Shared | 5 seconds | The sender is the current leader. The reply lists that capture's sections in path order within one management message of at most 64 KiB. |
| Section fetch | The coordinator to the node that staged a section; a streamed response with its exact length | Bulk, Shared | 30 seconds to open the stream, then 5 seconds without progress | The sender is the current leader and the capture's coordinator. The stage is removed as it is served, so a section is fetched once, in 1 MiB reads. |

The coordinator fetches a domain's sections after it resumed the domain, before it cuts the next
one. A fetch takes the bulk pool's shared stream and admission rather than the Snapshot subquota
that materialized readers, replica checkpoint transfers and consensus snapshot transfers share. The
cut publishes new checkpoint revisions, and a replica catching up a state above the bulk budget
holds the one Snapshot stream toward its owner for as long as that transfer takes, which on a
loaded node outlasts the opening deadline. A refusal for capacity alone is retried every 50
milliseconds within one 30-second opening deadline; any other failure ends the backup. Each fetched
section is staged on the coordinator, the stream is released before the copy is sealed, and its
length and digest are checked against the inventory. Sections the coordinator staged itself are
taken without a copy.

Once every domain is captured, the coordinator assembles the archive. Each domain's models are
rendered as canonical text and parsed back, and must equal the committed models. Each included
resource version's upload is read from the coordinator's resource store, once to check its size
against the catalog and once to copy it, verified again. The manifest names the producer and
language releases, the cluster, the capture time, the scope, whether resource bytes are included,
and each domain's revision, Raft log entry and cut; the layout then fixes every section's place.
The final archive is staged as one artifact under the node staging quota, which waits for room,
while the fetched copies stay staged until assembly ends, so the coordinator briefly holds the
captured sections and the archive together. One staged artifact may hold 64 GiB, and a node stages
at most 128 GiB of artifacts at once, archives and snapshot transfers alike.

A finished backup records its outcome in its command execution: the archive's size, digest, capture
time, retention deadline, whether resources are included, its user count and each domain's
revision, cut, section count and section bytes, and the process incarnation of the node that
assembled the archive and retains it. The archive itself, the staged sections and the manifest are
never written to consensus.

The coordinator retains the archive in memory, under the execution reference, until the first
download that queues its last frame or until the reference's retry validity ends: 15 minutes by
default, counted from the time the reference was issued. A restart loses every archive a node
retained. A download is served only to the user who ran the backup, and only by the node the
recorded outcome names: leadership can move after a backup completes, and the client can recover
the outcome from any node, so the leader is not necessarily the node that holds the archive.

- A node that the recorded outcome names streams the archive while it retains it, and refuses the
  download once a download collected it.
- Any other node that applied the outcome sends the client to the named node, with that node's
  endpoints, while the named process incarnation is live, and refuses the download once it
  restarted or left the cluster.
- A node that has not applied the outcome redirects the download to the leader, which answers from
  it. An outcome reaches a client only once every live node applied it, so this happens only for a
  reference no backup recorded or one still applying.
- An archive a node retains under a reference whose recorded outcome names another node belongs to
  a run whose outcome was not recorded. It is released when a download asks for it, and never
  streamed. A run whose outcome the cluster refuses to record releases its archive at once.

The download streams chunks of at most 256 KiB with four frames queued
ahead of the client, and each frame must reach the client within its request timeout; the
download as a whole has no deadline, because an archive can take far longer to transfer than a
command takes to run. [Sessions](./sessions.md#backup-downloads) and the [Client Session
Protocol](./client-session-protocol.md) define the call, and [Backup And
Restore](./backup-and-restore.md#downloading-the-archive) what each client does with the bytes.

## Restore Intake And Preparation

A restore streams its archive to the leader through the `RestoreBackup` call. The first frame names
the statement, which must be exactly one `RESTORE`, and the archive's declared size and BLAKE3
digest; every later frame is a chunk. A follower answers with the leader before it reads a chunk,
for a dry run too. The leader reserves the archive's whole declared size in node staging before it
writes a byte, refusing rather than waiting when one archive would exceed 64 GiB or the node cannot
hold it now, charges each chunk to bulk memory, and refuses a stream that carries more or fewer
bytes, or other bytes, than it declared. Staging runs inside the request, so a stream the client
abandons drops its writer and returns its quota, and nothing is admitted. Once the whole archive
arrived, the restore runs in a task of its own, and a client that disconnects no longer stops it.

Preparation then verifies and describes the archive under admitted memory, on worker threads:

1. **Manifest preflight.** A 512-byte read, charged to bulk memory, validates the first tar header
   as the regular `manifest.rkyv` entry within the record limit. The manifest then decodes under a
   `restore_metadata` charge of sixteen times its length plus 2 MiB.
2. **Measuring pass.** One verifying read of the whole archive sizes the retained preparation
   reservation: sixteen times every record's bytes, the manifest included, sixty-four times every
   model text's bytes, and 2 MiB of fixed overhead. Resource uploads, guest saves and Arrow groups
   stay on disk and are charged separately when they convert. The reservation is taken from the
   `restore_metadata` class, 2 GiB per node by default, without waiting: a restore the class cannot
   hold now is refused before it changes anything.
3. **Description.** A second read decodes every record into the archive description under that
   reservation, with each domain's model text, at most 64 MiB. The reservation then grows by the
   fixed share of every key the archive's deduplicator descriptors count: its entry in the
   resident keyspace and its resolver in the streamed checkpoint's serializer scratch.
4. **Parsing.** Each domain's text is parsed as admitted bulk CPU work. Every statement must create
   one model, without `IF NOT EXISTS`; a failure names the domain, the statement's number and its
   line.

The description, the parsed Models and the reservation share one lifetime, and every later
conversion job retains that owner through cancellation. Later reads of a verified archive open
sections at their recorded offsets without digesting them again.

Unless the restore is `WITHOUT STATE`, preparation then converts state as installation will: every
archived branch key is checked against the branching its restored entity declares, and every
materialized relay, deduplicator keyspace and window is converted under the shapes the restored
models give it, and the converted files are dropped. A dry run runs the same conversion, so a dry
run that succeeds plans an archive whose state installs. [Archived Branch
Keys](./backup-and-restore.md#archived-branch-keys) defines the key check.

Every Arrow section is scanned before Arrow's reader sees it, whatever digests the archive carries
for its own bytes: it must be one stream of a schema message, one record batch and the end marker,
each message opening with the continuation marker and every declared length within the section, with
only field types Nervix carries, no dictionary encoding and no compression, and a record batch whose
field nodes and buffers match its schema. A section whose framing does not hold is refused as
misframed and any other mismatch as undecodable, before anything is allocated from a length the
section only declares.

### Converting Archived State

Archived state reaches each node's native checkpoint encoding through bounded conversions.
Materialized relays, deduplicator keyspaces and windows convert in planning, a dry run included,
and again in installation; branch lifecycle and Kafka offsets convert when installation stages
them:

- **Materialized relays.** Planning verifies each group's Arrow schema, row count and unique typed
  branch identities, checking every record identity's branch as it reads it. Conversion stages bounded
  native header, identity and column pieces and concatenates them into one quota-owned file through
  a 64 KiB buffer, which the streamed publisher installs like a guest save, including a container
  larger than the 32 MiB bulk budget; the pieces and the container briefly occupy twice its disk
  space. Typed branch uniqueness has an independent 8 MiB commands reservation and native row views an
  independent 8 MiB state-read reservation. Decoded columns are charged cumulatively to `state_read`
  through installation, and loading refuses further groups when that class is full. Row projection
  and Arrow encoding share one admission and one bounded bulk CPU job, so projected columns never
  wait for a second grant while retaining the first, and typed native identities convert only after
  their allocation is charged.
- **Deduplicator keyspaces.** Every key group decodes under the exact Arrow schema the restored
  `DEDUPLICATE ON` expressions give it, one bounded group at a time on the bulk CPU workers, charged to
  bulk memory only while it converts. The conversion holds the keyspace as the runtime holds a
  restored one, every key normalized in an expiry map, which is how a key the archive holds twice is
  found and refused; the keys' parts and values are charged to `restore_metadata` as each group is
  admitted, beside the fixed per-key share the description admitted. The native checkpoint then
  streams from the resident keyspace into a quota-owned file through a 64 KiB buffered writer under the
  fixed 2 MiB bulk grant, one entry at a time, in exactly the encoding the runtime writes for the same
  keys.
- **Windows.** Each archived group's input rows, then its argument columns, decode under the window's
  input and argument schemas and are sealed into quota-owned pieces as soon as each is admitted, so a
  conversion holds one archived section and its decoded batch at a time and never the whole window.
  Conversion checks row counts and each delayed histogram bucket against the restored histogram's
  bucket count. When a group's row identities fit one 1 MiB identity record, its archived input and
  argument sections become the checkpoint's Arrow sections unchanged; otherwise the group splits into
  groups that fit, whose columns are projected again. The window header, which counts the rows and the
  nested containers' bytes, is written last and placed first, the delayed removals follow in bounded
  typed sections, and the pieces are concatenated into the checkpoint file through one 64 KiB buffer,
  briefly occupying twice its disk space.
- **Branch lifecycle and Kafka offsets.** Each converts entry by entry directly into a quota-owned
  file through a 64 KiB buffered writer, with fixed-capacity serializer scratch holding one resolver
  per entry and the largest entry's nested resolvers, so neither the converted entries nor the encoded
  output is collected. Completed native files retain their actual length, digest and rounded disk
  quota until their last reader exits.
- **Guest saves** need no conversion and stream from the staged archive.

The preparation reservation covers overlapping owned archive values, aligned decode buffers, model
parsing and planning copies, branch text and typed-key conversion, and native serializer resolvers. A
window descriptor's record already carries each retained row's watermarks, so its record charge
covers the per-row identities a window conversion builds one group at a time. Resolver scratch
therefore grows with entry count, while strings and encoded checkpoint output stream. Archive readers
and received uploads retain their staged file and its disk quota through the storage job that reads
them, including its cancellation.

A conversion the node refuses only for room does not fail the restore. A bulk or `restore_metadata`
class without room, a full worker queue and a full staging quota judged nothing of the archive, so
the conversion releases what it holds and converts the refused unit again every 50 milliseconds for
up to 30 seconds after the node first refused it. For a window the unit is one section, read and
decoded again into a checkpoint exactly as it was before the refused attempt, whose staged pieces
were dropped; a window refused while its sealed pieces are finished into one checkpoint is converted
again from its first group; and a keyspace is converted again from its first group, because its
admitted keys are what held its charge. A refusal that outlasts that wait ends the conversion and is
reported as the node having had no room for it, since what the restore itself holds can be what
fills the budget. A request larger than a whole budget, and content that does not fit the restored
shapes, end it at once. Materialized conversion has no such wait: a refusal ends it.

## Planning

The restore planner is a synchronous pure function. Its inputs are the statement, the archive
description with its parsed Models, the existing users and domains, and the steps the restore
already recorded; its output is the plan, or a typed refusal. It runs as admitted bulk CPU work.

- **Scope.** `RESTORE CLUSTER` takes a cluster archive and every domain in it; `RESTORE DOMAIN` one
  domain of a cluster or domain archive, under its archived name or `AS` a new one.
- **Users.** Each archived user under the `ON EXISTING USER` policy. Under `FAIL` every existing name
  refuses the restore before anything changes; a step already recorded is not planned again.
- **Names.** A target domain name that exists refuses the restore, unless the restore already
  recorded creating it.
- **Resource versions.** Only completed versions are imported, under their archived numbers; a
  version the source assigned to a failed or unfinished upload stays a gap. Each imported version's
  archived upload must match its root checksum, which the plan compares with the upload section's
  verified digest, and must lie within its resource's declared sequence.
- **Pinning.** Every binding of every model must name an imported completed version by number.
- **Lifecycle.** The target is created stopped, with its archived pace, default placement, start
  generation and latest start point, and no clock mapping. With `RESUME`, the plan also carries the
  activation: running, with the archived clock mapping. A paced archive without a committed mapping
  cannot resume and is refused.
- **Model run.** Each domain's models are planned with the transaction planner against the domain as
  the restore will create it: stopped, without models, and with exactly the imported versions as
  completed uploads. A dry run reports that run's [transaction impact
  report](./transaction-quiescence.md) for each domain and every step as planned.

## Admission And Durable Progress

A restore is a persistent command under its execution reference, like every command that changes
the cluster; see [Command Completion](./command-completion.md). Its request identity is the BLAKE3
digest of the statement and the archive's declared size and digest. The leader admits it only after
it verified, prepared, planned and converted everything, and the admission binds a domain mutation
lease to the execution for every target domain, so no other command changes those domains while the
restore applies. Concurrent attempts with one reference on one node serialize on that reference.

Consensus keeps the restore's progress in its execution record: the statement, the archive identity,
the steps already applied, and the user outcome. A step is applied through a consensus command that
names the restore and the step, and consensus refuses a step whose prerequisite is not recorded and
a step recorded with a different effect. A recorded step is never applied twice:

| Step | How it is applied and recorded |
| --- | --- |
| Import users | One command imports every user under the policy and records the step. |
| Create domain | One command creates the stopped domain with its declared resources and their next version numbers, closes its installation gate, and records the step. Every live node then applies the stopped domain. |
| Import resource versions | Each version is installed from its archive section on the leader and published under its archived number through one command per version, keyed by the restore so that a repeat finds it imported, and the restore waits until every live node installed it, exactly as an upload completes. One more command records the step. |
| Apply models | The models are applied as one batch under the restore's lease, with full graph validation and the leader's content checks, unbounded by a transaction's statement and source-byte limits; an attempt that finds exactly these models already applied skips the batch, and any other models fail the step. State installation follows, and one command records the step together with the installed generation, which opens the installation gate, and, with `RESUME`, the activation. |

A step that fails ends the restore. The steps recorded before it stay applied, and a domain the
restore created stays stopped with what it committed; when the domain was created but did not become
usable on every live node, the create step itself stays applied and is reported as failed.

Client configuration inspection within the accepted apply-models step waits for command-memory
capacity and a filesystem worker-queue place. Temporary admission pressure does not fail a step
after its earlier effects have committed. The node retains the restore's archive and domain lease
through this wait; it holds no inspection copy before its memory charge is admitted. The service
task's shutdown cancellation drops the wait, and a worker already running retains its charge until
it exits. Oversized charges, a closed executor and actual configuration failures still fail the step.

Only the leader the archive was streamed to holds it. When leadership moves while a restore applies,
the restore stays applying. A leader that holds the restore's archive resumes it at its first step
not recorded; otherwise a client that sends the archive again under the same execution reference
gives the new leader what it needs, and the Rust client does so when it is redirected or its
connection fails. A repeat with the same reference while the restore applies on that leader is
answered at once, and one after it finished receives its recorded outcome and report. If no client
sends the archive before the reference's retry validity ends, the leader ends the restore as failed
after the steps it recorded. The staged archive is released when the outcome is recorded, or when
its retry validity ends; a dry run releases it when it answers.

## State Installation

State installation runs inside the apply-models step, after the models published the domain's
stopped schedule, so state is staged under the placements the restored schedule computes for this
cluster.
Decoded materialized containers keep their metadata and column charges in the independent
`state_read` pool until installation consumes them or their last source reader ends. Its capacity
equals the relay budget and counts separately in startup sizing, leaving relay work the capacity
it needs to evaluate, encode and dispatch while those source columns remain admitted.

Running generators keep remote source views only in their task-local read cache. They describe
the selected owner's complete sealed identity before each reuse and replace a changed view even
when its revision equals or precedes the one they observed before restart or restore. Cached
rows carry their admission charges in the separate bounded `state_read_cache` pool, whose
capacity equals the relay budget and is included in the startup total. Optional retention never
waits for space: a full pool returns the complete current read uncached. Cached references are
released before replacement admission and on
quiescence or failure. They neither persist nor select a restore generation; complete publication,
handle clearing and the installation gate remain the authority for resumption.

Snapshot metadata and Arrow decoders request their scratch memory without queueing while their
encoded input occupies the bulk pool. A refusal releases that input and preserves the typed
admission cause; a generator tries a complete source read again on its next occurrence. It does
not treat the refusal as absent or corrupt state. Small framing reads end before the next read,
so a stored snapshot does not retain a preceding frame's storage working charge across it.

### The Installation Authority

The coordinator first admits an installation generation through consensus. The admission is refused
unless the leader tenure is current, the domain is stopped, the restore holds the domain's lease and
its prerequisites are recorded, and no other execution owns the domain's installation. The admitted
authority names the leader, its term, the restore execution, the lease's revision and the
generation, which is the log position that admitted it, and the gate records it as installing. A
retry or a resumed restore always admits a new, higher generation.

Every storage mutation of the installation, local or on another node, runs under the consensus
applied-state read guard, which it holds while it checks that the tenure is still the authority's,
that the gate still records exactly this authority as installing, that the execution is still an
applying restore, that its lease and lease revision still match and that the domain is still
stopped, and then performs the mutation. Advancing the applied state, and with it releasing the gate
or admitting a newer generation, needs that lock's write guard, so no stale coordinator, however
delayed its request, can stage into, publish or clear a domain whose installation has moved on. The
state store separately refuses a publication with another authority whose generation is not newer
than the one it selected.

### Staging

The coordinator checks every archived branch key again against the schedule it installs under, then
stages each compatible checkpoint on every node the schedule assigns it to, owner and replicas, branch
lifecycles first. It skips, and the report later warns about, a section whose entity the schedule
does not hold or holds under another schema fingerprint, and a window whose restored processor is not
the archived window model or whose branch incarnation is not the one the restored lifecycle holds. A
WASM save is staged under the guest-state generation the restored schedule names for its branch.
`WITHOUT SOURCE OFFSETS` stages no Kafka offsets, and `WITHOUT STATE` stages nothing.

The coordinator stages its own copies straight from the archive file at their offsets. It sends each
remote copy through the state-installation operation, one checkpoint at a time:

| Action | What the receiving node does |
| --- | --- |
| Begin | Names the placement, branch fingerprint, revision, length and digest. The node stages that many bytes under its staging quota. |
| Chunk | Carries the next 1 to 64 KiB at exactly the next offset, within the declared length, under the same authority. Chunks are sent one at a time, each awaited. |
| Finish | The node checks the complete length and digest, then stages the checkpoint into the domain's restored namespace as one 2 MiB storage job. |
| Publish | Names the complete inventory the node must hold. The node publishes it as one 2 MiB storage job, its authority validated again. |

Each request uses the bulk pool's Snapshot subquota with a 30-second deadline, carries the authority,
and is accepted only from the authority's leader once the node applied the authority's generation,
which it waits up to five seconds for. A staged upload a coordinator abandons expires after 600
seconds. Guest saves stream from the archive file on the coordinator and from the sealed upload on a
remote node; neither path assembles a save in a second full-size buffer or reserves a file-read
chunk inside a reserved full checkpoint. Native lifecycle and Kafka offsets are converted entry by
entry into quota-owned files whose ordered 64 KiB windows feed both local installation and remote
transfer, so no checkpoint is held whole in memory on the way.

### Publication And The Gate

Publication runs on every live node, the coordinator first, each with the inventory of checkpoints
it was sent, so a node without assigned checkpoints publishes an empty set. A node validates that it
holds exactly that inventory under the authority, synchronizes the staged data, durably selects the
generation, and only then clears the domain's in-memory state handles; [Generation
Storage](#generation-storage) describes the sequence.

The installation gate is a replicated record per restored domain, from the step that creates it.
While it is closed, consensus refuses `START`, direct or transactional, and every replicated
domain or schedule change that would make the domain anything but stopped, so the domain cannot run
on any node until every node published its complete set. It survives failure, lease
release and node restart. Recording the apply-models step together with the installed authority
removes it; with
`RESUME`, the same command makes the domain running with its archived clock mapping, leaving its
start generation and latest start point exactly as archived. A failure before that leaves the gate
closed: every node keeps its complete previously selected set, and a failure during distributed
publication can leave nodes at different complete generations, but the domain cannot run. A domain
whose gate a failed restore left closed is restored again under another name, since a restore never
creates a domain whose name exists.

## Generation Storage

Each node's state store keeps a domain's checkpoints in namespaces. The initial namespace holds the
checkpoints a domain writes in ordinary operation; each restore attempt writes into a restored
namespace named by its installation generation, invisible to every reader until selected. One
publication pointer per domain names the selected restored generation; a domain without one reads
its initial namespace. A checkpoint is either inline or segmented: a segmented checkpoint's header
records its revision, length and BLAKE3 digest, and its bytes lie in chunks of exactly 64 KiB, the
last one shorter, keyed by placement, revision and offset. A restored checkpoint also has a receipt
binding it to the authority that staged it. A placement's encoding, with its domain and namespace, is
at most 60 KiB, which keeps every key within the database's key limit. A segmented header consists
only of its fixed archived root, so a reader checks a header's stored size before it copies or
decodes it, and a larger valid value is an inline checkpoint. A namespace's generation is separate
from a WASM branch's guest-state lifetime and from a checkpoint's revision.

Ordinary checkpoints use the same segmented form when they are large: a materialized relay, and a
window whose retained rows exceed one 8 MiB snapshot section, seal in bounded pieces to quota-owned
disk, cross-node generator reads stream the sealed file, and periodic persistence replaces a
segmented checkpoint only after its chunks are durable. A native checkpoint reader keeps one
database snapshot and at most one 64 KiB stored chunk across all its section reads.

Staging a checkpoint first removes any earlier attempt at the same placement in that generation,
complete or not, admits the checkpoint's whole footprint against the restore staging quota, writes
its chunks one bounded batch at a time with cancellation checked before each, verifies the digest,
and commits its header, index and receipt together. Staging writes are not synchronized; publication
is the durability point:

1. Every receipt, header and chunk the namespace holds is checked, from one database snapshot,
   against the expected inventory: the exact authority, every checkpoint's length and digest, and the
   counts and bytes the coordinator sent.
2. The staged data is synchronized to disk.
3. The pointer, holding the authority and the inventory, is committed and synchronized.
4. Obsolete namespaces of the domain, and receipts of this and earlier generations, are removed in
   bounded deletion batches.
5. The domain's in-memory state handles are cleared, and the job reports publication.

An exact retry under the same authority finds its pointer and repeats only durability and cleanup;
it cannot select different counts or another authority under the same generation. Cancellation or
failure at any point before the record of the step leaves the gate closed.

Every read selects the pointer, headers and chunks from one database snapshot, so a reader that
opened a view before a publication keeps its complete prior set while obsolete keys are deleted.
Checkpoint writers capture the namespace they will write into before they are admitted and compare it
again under the installation barrier, one node-wide blocking lock that ordinary and segmented
checkpoint writes, replica installation, handoff activation, forced-recovery replacement, purge,
restore staging and publication, and maintenance all take; a write queued for a namespace that
publication replaced becomes inert instead of writing into the restored set, and reports success.
Publication, a segmented checkpoint and a sweep that deleted anything hold the barrier across their
synchronizations. The store's storage format marker must be current: a different marker, or a
missing one beside nonempty checkpoint storage, fails with an instruction to recreate the node's
state directory.

Every installation, publication and maintenance job reserves 2 MiB of bulk memory for itself,
independent of how many checkpoints it handles and how large they are, and the generation
publisher has no admission limit on a set's aggregate payload. The reservation bounds the job's own
buffers, not the database's reads, caches, memtables, snapshot retention and compaction, nor a
branch's materialization of its state at `START`, which keep their own owners; a database size
query, for instance, can still read and cache a value maintenance only measures.

## Restore Staging Maintenance

Every node runs restore checkpoint maintenance, independently of leadership: once at startup, then
again one second after each sweep ends, whether it succeeded, failed or was deferred. A sweep takes
its 2 MiB only from room the bulk class has free when it begins, and otherwise is deferred: it
reclaims and measures nothing, publishes no series, and the next sweep tries again. It never waits
in the class's queue. A place there takes every byte the class releases until it is satisfied, so
it would hold every other bulk charge on the node behind it, including the next charge of an
operation that holds part of the room it waits for. Additional admission for a materialized
snapshot's request envelope, staging operations, stored section reads and captured-group
conversion therefore refuses without queueing while its input or reader remains charged.
Refusal releases the operation's retained memory and preserves its typed admission cause;
incomplete capture publishes no inventory, and a later backup can capture afresh once room
returns. Consensus Bulk storage independently admits working sets through store-local turns,
so a second storage working set leaves the rest of the class available for progress. Under sustained
bulk contention, reclamation therefore waits for room instead of delaying the node's bulk work. A
bulk class smaller than the working set refuses every sweep at once instead, counting the refusal
and logging it as a warning, because no free room could ever admit one.

An admitted sweep borrows one applied consensus revision under the applied-state read guard, and
takes the installation barrier. It retains the selected publication, the exact generation of an
installation that belongs to a restore still applying, even when leadership or its lease has
changed, every generation ahead of the node's applied log, and, on a node without an applied log,
every generation. It reclaims every other restored namespace: terminal, expired, missing and
superseded attempts, including chunks written before their receipt existed. In the selected
namespace it removes receipts, and checks one header per placement and revision to remove chunk sets
the selected header does not reference, because the header is absent, inline or names another
revision; ordinary checkpoints, replica installation, handoff, forced recovery and purge therefore
release unreachable restored chunks without another restore. Its cursors seek past the initial and
selected headers, indices and receipts and past retained generations, and in the selected chunk
namespace one header check lets a cursor skip a whole referenced set. Deletion runs in bounded
batches and synchronizes what it removed, and a cancelled or interrupted sweep resumes from the keys
that remain. Maintenance never releases a gate or changes a selected publication, and never removes
a domain's initial namespace, which only a publication's cleanup removes.

The restore staging quota, `--restore-staging-max-bytes`, by default the smaller of 128 GiB and the
size of the volume that holds the node database, bounds the key and value bytes of every
unpublished restored namespace on the node, across all domains, including partial chunks, headers,
indices and receipts; published data is outside it. A node measures that usage by reading every
unpublished entry once, one chunk at a time, and keeps the measurement while it stays exact.
Admission forgets it before the first chunk is written, and a checkpoint that then stages
completely restores it with exactly the footprint it admitted; a
staging attempt that fails after admission, one that replaces an earlier attempt, and a
publication leave it forgotten, so the next admission or sweep measures again, as the first one
after a start does. A sweep that deletes anything forgets it before deleting and measures again once
it finished. Admission and an idle sweep therefore read no staged entry, however much another
restore left unpublished on the node, and a restore staged beside a generation that is still
retained costs what it stages itself. The measurement is taken, kept and changed only under the
installation barrier.
Lowering the limit refuses further staging until usage fits and reclaims nothing by itself. [Backup
And Restore](./backup-and-restore.md#restore-checkpoint-storage-quota-and-metrics) lists the
metrics a completed sweep publishes.

## Lifecycle Resumption

A restored domain runs only after its gate opens. With `RESUME`, it runs at its archived start
generation and latest start point, so activation behaves like a whole-cluster restart: every kind of
restored state is retained. A paced domain's archived wall and logical origin and time rate are
installed verbatim, the mapping projects the time the domain was down, and the leader chooses a
fresh clock authority for this cluster; the archived logical frontier is inspection metadata, not a
new clock origin. An unpaced domain needs no mapping.

Without `RESUME`, the domain stays stopped. A later `START` advances its start generation, which
clears materialized relay state, because a materialized placement's identity mixes in the start
generation, and continues the restored WASM state, source positions, branch lifecycle, deduplicator
keys and windows, whose identities do not. To begin a new clock at the archived logical frontier, an
operator leaves the domain stopped and uses `START AT` with a time rate explicitly.

As the domain starts:

- A Kafka source resumes from its restored next offset for every partition of its current
  assignment, clamped to the partition's live low and high watermarks; a partition without a
  restored offset starts as the source declares.
- A materialized relay binds the rows restored under the exact schema fingerprint and archived start
  generation before a branch or generator starts, with their typed keys, watermarks, revision and
  branch generation; the restore sets the ownership fence to zero and the new assignment establishes
  its own. A relay starts each restored branch's idle lifetime at its own activation, because branch
  activity is not archived.
- A deduplicator branch holds every archived key, normalized as the restored `DEDUPLICATE ON`
  expressions key it, with the time it was first seen in the source cluster, so a key expires its
  `MAX TIME` after that first sighting.
- A window branch task re-admits the archived rows in admission order, which rebuilds every aggregate
  structure, sketches included, and then restores each linear histogram's pending delayed removals.
  A branch whose incarnation differs from its restored lifecycle's starts its window empty.
- A WASM branch restores the guest save its generation names, and the guest validates the bytes.

Archived Kafka positions follow acknowledged records, and a window acknowledges a row only when it
steps past it, so a domain that reads Kafka into a window stands its positions before the rows its
windows retained at the cut, and the resumed source reads those rows again; a restart has the same
effect.

## State Placement Identities

Restored state is addressed by what the restored domain computes, never by where the source cluster
kept it. A placement names the target domain, the state kind, the entity's kind and name, the
scheduled unit and, for branch-local state, the branch key; the source domain's name and the nodes
that owned the state are not carried. Every placement except Kafka domain offsets and
branch-aggregated metrics includes the
entity's schema fingerprint, the BLAKE3 digest of the schema Models it depends on, so identical
Models restored elsewhere address the same state, and a restore installs a section only under the
fingerprint the archive recorded; the archive records a Kafka ingestor's schema fingerprint beside
its offsets, and the restore checks it before installing them. A materialized placement's
fingerprint also mixes in the domain's start generation, and a WASM placement names the guest-state
generation its schedule assigns. The archive descriptors and section paths retain the unit, so
two units of one entity stage and install separate checkpoints. Restore accepts a state section
only when its unit exists in the restored schedule and sends it to that unit's assigned nodes.

Kafka offset checkpoints and their archive records also retain the exact optional generation of an
already applied explicit start. Restore installs it beside the next offsets, so resuming an archived
`START AT` generation does not rewind acknowledged records. An absent marker stays absent; deriving
it from the domain's start count would incorrectly claim that an incomplete source initialization
had finished. A later explicit start uses its new generation and resets the positions normally.

Branch keys are typed. Every archived key must belong to the branching its restored entity declares,
exactly, with no value converted to fit, so no installed checkpoint names a branch the entity's
routing could not reach. A section path names a branch by the BLAKE3 fingerprint of its key, or
`unbranched`; installation checks that a key's fingerprint matches the one its section names.

## Failure Semantics At Every Boundary

Section conversion, executor admission, checkpoint reads and staging keep their typed causes in
local reports. Capture and restore handlers project those reports into the fixed interconnect
failure only when returning their reply; its reason renders the context chain. A staging refusal
that the restore stream projects into its fixed intake outcome is also reported at its recovery
decision, so a failed write or seal retains an operator-visible cause.
A retained archive's read failure is reported before its download sends the fixed read refusal.

| Boundary | Failure | What remains | Recovery |
| --- | --- | --- | --- |
| Backup admission | The statement is batched, queued in a transaction, or sent to a follower | Nothing admitted | Send it alone, to the leader the redirect names. |
| Cut budget | A lease, guard, counters read, application of the pause, drain, configuration capture or owner capture does not finish within the cut's budget | The refusal names its typed active phase, that phase's age, total elapsed cut time and completed phase durations. A pause is undone and the lease released; an application of the pause also retains the wait the deadline ended, and a failed drain what each node still held | Inspect the phase and owner logs, then send the backup again with an appropriate `TIMEOUT` or after the stalled operation recovers. |
| Cut reach | A node the schedule assigns the domain's work to is not reported live when a quiesced cut starts, a scheduled state owner is not reported live when any cut captures, or the schedule places an entity whose state the archive captures on no node | Nothing paused when a quiesced cut refused before its pause, otherwise the pause is undone; the lease is released and no archive is assembled | Send the backup again once the node is back, or once failover has moved its placements or a node can host the entity again. |
| Leader change during a cut | The tenure check fails | The domain is resumed, by the coordinator or by the next leader's reconciliation, and the command stays applying | The next leader re-executes the backup under the same reference; the client retries the reference to receive its archive. |
| Outcome recording | Leadership moves, or the leader cannot confirm that it still leads, before the backup's outcome is recorded | The command stays applying, and the client receives `OutcomeUnknown` | The leader that resumes the command, or the client's retry of the reference, runs the backup again with new cuts; the client receives that run's outcome and archive. |
| Owner capture | An owner cannot apply the revision, convert a checkpoint, or stage a section | No archive is assembled. The coordinator's copies and the failing owner's partial files are dropped; other owners' staged sections expire after 600 seconds | Send the backup again; it takes a new cut. |
| Section transfer or assembly | A fetch fails, or the archive cannot be staged or encoded | No archive is retained | Send the backup again. |
| Retained archive | The node restarts, the retry validity ends, or a download collects it | The recorded outcome remains; its download is refused | Take a new backup. |
| Archive download from another node | Leadership moved after the outcome was recorded, or another run lost the race to record it, and the client asks a node that does not hold the archive | The archive stays retained on the node the outcome names. A losing run's archive is never streamed; it is released when a download asks for it, or at once when the cluster refuses its outcome | The download is sent on to the named node while its process is live; once that process restarted or left the cluster, the download is refused: take a new backup. |
| Restore intake | The stream is abandoned, oversized, truncated or has another digest, or staging cannot hold it | Nothing changed; staging quota returned | Send the archive again from its first byte. |
| Restore preparation and planning | Verification, preparation memory, parsing, the plan, a branch key or a conversion is refused | Nothing changed | Correct the archive, the target or the options; a refusal for room alone is retried for 30 seconds first. |
| A step | A consensus refusal, a version that does not install, a model batch the leader refuses, or state that does not convert or install | The recorded steps stay applied; a created domain stays stopped with its gate closed if installation did not complete | Restore the archive again under another name. |
| Distributed staging or publication | A node fails or a request is refused | Each node keeps its complete previously selected set, possibly at different generations; the gate stays closed | A resumed restore admits a new generation and installs it again. |
| Leader change while a restore applies | The archive's holder is no longer the leader | The restore stays applying; a delayed request of the earlier coordinator cannot mutate storage | Send the archive again under the same reference before retry validity ends; otherwise the restore ends as failed after its recorded steps. |
| Unpublished attempt | A terminal, expired or superseded attempt left staged data | Its namespace stays unpublished and the gate stays closed | Maintenance reclaims it once the node applied its generation. |

No failure message carries archive contents, password hashes, resource bytes or branch-key values;
[Errors And Diagnostics](./errors-and-diagnostics.md) owns the typed errors of each boundary and how
they are reported.

## Guarantees And Limits

Backup and restore guarantee that:

- an archive is verified completely, section by section, before `DESCRIBE BACKUP` prints it or a
  restore plans it, and a restore changes nothing before it verified, prepared and planned the whole
  archive;
- a quiesced cut reflects every input acknowledged before it, with no admitted work at the cut, and
  freezes the domain only for one drain and one publication round;
- a quiesced or stopped cut archives the state of every placement its committed schedule assigns,
  each from its scheduled primary, or fails naming the domain, the entity and the node it could not
  reach, and refuses while its schedule places an entity whose state it archives on no node; it
  never assembles an archive that lacks a placement's state;
- no step of a restore is applied twice, whatever retries, disconnects and leader changes occur;
- a restored domain never runs with partial state: every assigned owner and replica publishes its
  complete set, and the gate refuses `START` until every node has;
- no delayed coordinator can stage into, publish or clear a domain after its installation moved on;
- restored state is bound to the exact schemas and branch shapes of the restored models, and
  everything else is skipped with a warning or refused.

They hold within these limits:

- **Reachable owners.** A cut whose scheduled state owner is down fails until that node is back or
  failover has moved its placements, which happens only once the cut has released its lease; a
  replica's copy is never archived in the owner's place. A quiesced cut likewise refuses while a
  node the schedule assigns the domain's work to is down, and every cut while the schedule places
  an entity whose state it archives on no node, which lasts until a node can host the entity
  again. Liveness is the cluster's availability
  view, gossip bridged by application health: a node it still reports live is asked, and fails the
  backup if it does not answer, and a stopped node leaves it once its health probes have failed for
  the node-unavailability timeout, 10 seconds by default.
- **Live cuts and moving placements.** A live cut takes no lease, so failover or relocation can move
  a placement while its owners capture. Each owner stages what its own applied schedule assigns it,
  so a placement that moves during the cut can be staged by neither node, or by both, and a section
  staged twice fails the archive's layout. A quiesced or stopped cut holds the lease, so its
  schedule cannot move.
- **Memory.** One record section is at most 64 MiB, one archive 64 GiB, and a node stages at most
  128 GiB of artifacts. Restore preparation is bounded by the `restore_metadata` class, 2 GiB per node
  by default; capture conversion charges to it as well. Individual materialized relays, keyspaces,
  windows, guest saves and native metadata records may exceed the 32 MiB bulk budget; the
  [Limits](./backup-and-restore.md#limits) table states each bound.
- **The applied-state guard.** A staging or publication job and a maintenance sweep hold the consensus
  applied-state read guard for their whole duration, so the node's next applied revision is
  published only after the job finishes. A large checkpoint's staging, or a sweep that measures much
  unpublished data, therefore delays that node's application of consensus state while it runs.
- **Owner-local cut shares.** In a stopped or live capture, the coordinator's own share has no
  deadline, while remote owners share the cut's budget.
- **Archive collection.** A download collects the archive once it has queued the last frame, before
  the client confirms it, and a node restart loses every archive it retained.
- **Kafka-fed windows.** A window fed by Kafka counts the rows it retained at the cut again after a
  restore or a restart, because the archived positions stand before them.
- **Restored state on the move.** Replica catch-up and a planned ownership handoff carry each
  checkpoint as a description followed by a bulk-pool fetch, admitted against `restore_metadata` on
  both sides up to the 256 MiB checkpoint transfer bound, so a restored keyspace, window or guest
  save larger than one replication message still reaches its replicas and new owners. A handoff
  destination holds an entity's prepared checkpoints until activation, and the handoff's metadata
  boundary admits a materialized relay's resident checkpoint entry against the bulk budget;
  [Cluster Interconnect](./interconnect.md) defines both transfers.
- **Dry runs.** A dry run reports no skipped-state warnings, because they are derived from the schedule
  an applied restore publishes.
- **Faults inside publication.** The external chaos entries fault a restore while it stages, which a
  delay on the coordinating leader's links holds open in the release image. A fault inside
  publication itself needs a testing hook, so the Cucumber scenarios that pause a restore before
  state publication carry that boundary, and the refusal of a delayed coordinator's request is
  judged in a real process only by the traffic of the domain it would have replaced: the image
  exposes no publication generation and no count of refused requests.

## Observability

A backup reports its archive's size and digest, capture time and, per domain, the revision, the cut
kind, when a quiesced cut engaged and released, what intake buffered, dropped and rejected meanwhile,
and its section count and bytes. A restore reports each step's outcome, applied, planned, failed or not
attempted, each domain's status and start generation, a dry run's impact reports, and the warnings
for every skipped state section. `DESCRIBE BACKUP` inventories an archive without contacting a node.
[Backup And Restore](./backup-and-restore.md) shows each report.

Backup command diagnostics and restore step reasons render the owning context and its complete
cause chain. A coordinator's direct checkpoint calls retain the same runtime-state and staging
causes as an installation through the interconnect; interrupted guest and native-metadata captures
retain their cause in the backup failure. These diagnostics name operations and entities without
including archive contents.

Each domain cut logs its administrative phase entry and completion at `info`, with the domain,
execution reference, phase duration and total elapsed time. The timeout refusal retains the active
phase and ordered completed durations, including lease admission, alteration guard, opening
counters and drain. The drain phase includes the pause's application, and a refusal that ended
that application names, beneath the phase, the wait it ended and the revision it waited for. The controlled boundary after drain is `capture`; configuration capture, owner
capture and closing counters have distinct phases. Resume and lease release remain cleanup outside
the cut's deadline and do not receive a new capture budget.

Owner capture logs identify the node and coordination identity and distinguish revision wait,
cluster-state application, lifecycle checkpoints, storage capture, staging and inventory. The
capture operation returns a terminal reply, so a coordinator waiting for an owner reports owner
capture rather than claiming to know that node's current internal phase. Retained owner logs give
that finer attribution. Phase diagnostics carry no archive contents or branch-key values and do not
change archive records or node-to-node messages.

The leader logs at `info` when it assembled an archive, when a download collected one and when a
restore completed, with the execution reference and the domains; skipped state is logged at `warn`
on the leader. Maintenance failures are logged at `warn` on the node. There are no backup-specific
metrics: transfers appear in the interconnect's bulk byte and subquota series, and conversions in
the `bulk` and `restore_metadata` memory classes of `nervix_execution_memory_*`. Every completed
maintenance sweep publishes the `nervix_restore_staging_*` series and the shared checkpoint
keyspaces' disk allocation; until the first sweep completes they read zero.

## Qualification Evidence

| Guarantee | Evidence |
| --- | --- |
| Archive fidelity | Registered Bolero properties over complete archives, records and descriptors, streamed records against rkyv's encoding, Arrow groups, native lifecycle and Kafka conversion, the capture section mapping, and the restore installation and staging records, each with a malformed-input target; [Property Testing And Fuzzing](./property-testing-and-fuzzing.md) lists them. |
| Cut and capture | `backup.feature` and `backup_wait.feature` on one and three nodes: configuration-only, stopped, live and quiesced cuts, parked `REQUIRED WAIT` work, including work whose every re-evaluation reads its state slowly, a drain whose every node holds its runtime installation, an unavailable emitter, alone and after a pause application held for most of the cut's budget, whose drain is refused at the cut's own deadline, a committing transaction's lease, also after a lapsed leader lease refused that commit's first publication, a leader change during a cut and between cuts, and a leader change while the leader holds a backup's outcome before recording it, after which the backup runs again under its reference, holds its delayed cuts again and returns the second domain's expiry again; `backup_owners.feature` on three nodes, where a quiesced and a stopped cut, with no replica and with one, refuse once their deduplicator's scheduled owner has stopped and the leader no longer reports it live, a quiesced cut refuses before it pauses once the deduplicator's owner, or a node that only runs the domain's work, has stopped, and a quiesced and a live cut refuse while no node can host the deduplicator; `backup_stored_branch_capture.feature`, `backup_metadata.feature` and `backup_guest_saves.feature` for stored and large state within the bulk budget. |
| Capture fence | The Loom models `server.backup.cut-includes-publication` and `server.backup.cut-generation`, each qualified by weakenings that must fail, and the Shuttle checks of the fence, of forced deduplicator and window publication at the cut, and of materialized capture against eviction. |
| Downloads and retention | `backup.feature` and `backup_delivery.feature`, and the Shuttle checks that a download stays bounded, a lost client releases its hold once, and concurrent downloads collect an archive once. |
| Restore progress | `restore.feature` on one and three nodes: verification refusals, a failed step, an interrupted upload, joined retries and a leader change while applying; the Shuttle checks that a resumed restore never repeats a recorded effect, an abandoned upload releases its quota once, and concurrent retries join one execution. |
| Complete-set publication and the gate | `backup.feature`, `backup_generation.feature`, `backup_materialized.feature`, `backup_branch_state.feature` and `backup_branch_state_large.feature`: interrupted installation through a checkpoint staging failure, durable-publication failure, each including one whose coordinator loses leadership before recording it and whose recovery under the same execution reference fails again, restart and delayed coordinators; the consensus Shuttle checks that publication and handle clearing precede resume and fence delayed publishers. |
| Staging quota and maintenance | `backup_staging.feature`, the state store's storage regressions, the kept-measurement property, and the consensus Shuttle check that reclamation holds its revision through queued installation. |
| Interconnect operations | The Turmoil scenarios for a fenced section fetch across a partition, window sections above the bulk budget, a bounded inventory under a held link, and a restart during transfer, with their committed seeds; [Deterministic Interconnect Simulation](./interconnect-simulation.md) lists them. |
| Snapshot receive admission | `snapshot_receive_admission.feature` on one and three nodes uses archived identities of two interleaved branches to fetch actual native checkpoints over authenticated interconnect streams. Outer and nested receive overlap must return 503 while the probe holds all but 4096 bytes of the node's Bulk capacity, independent of earlier reservation releases; it then releases all input and transfers the complete original checkpoint after capacity returns. |
| Tracked locks | The restore storage owner tests and the `@restore_installation` and `@backup_wait` scenarios in the Deloxide selections, with their `stress.` twins in the stress selections; [Data-Plane Concurrency](./data-plane-concurrency.md) records what the detector does and does not see. |
| Real processes | The external chaos soak entries for backup under load, backup cycles, backup under owner crash, leader crash and partition, a rolling restart during a backup stream, and restore into a fresh cluster; and the [restore faults](./cluster-chaos.md#restore-faults) held in staging through a SIGKILL of the coordinating leader, an isolation of that leader from its peers and the client, a SIGKILL of a state owner, and a SIGKILL of a state owner while a restore whose coordinator and client were lost awaits its failure. Each restore completes under its execution reference or fails with `START` refused and a restore under another name completes; a completed restore equals its archive, a domain resumed after its coordinator's isolation consumes exactly from its archived offset, and staging returns to zero on every node through completed sweeps. The [staging volume](./cluster-chaos.md#restore-staging-volume) entry measures three concurrent restores of guest saves above the bulk budget. [External Cluster Chaos](./cluster-chaos.md) defines them. |

The bounded native recovery outline in `backup_wait.feature` holds the first cut for either two
or twelve seconds with a ten-second client wait, on one and three nodes. An exhausted wait can
precede the second cut without cancelling the admitted backup. A fresh client recovers its original
execution and downloads an archive retaining both quiesced cuts and the first cut's measured hold.

The harness releases every test lease, drain and capture gate before it shuts nodes down, including
after a failed body. `just test-backup-cut-cleanup` supervises deliberately failed public backup
cases at the lease and capture boundaries on one and three nodes: the bodies must retain their
failures and every node must stop within the existing cleanup budget. Retained public backup tasks finish before their peers
stop, sharing that same deadline with node shutdown. [Integration Test Lifecycle](./integration-test-lifecycle.md#what-cleanup-releases)
owns that independent supervision verdict and its retained source and logs. It changes no backup
completion or cancellation guarantee.

### Representation Checks

The registered [archive properties](./property-testing-and-fuzzing.md) compare complete current
manifests, users, domain lifecycle and clocks, resource catalogs, WASM descriptors and guest saves,
Kafka offsets, ordered branch lifecycle entries, materialized generation descriptors and every
identity group's typed branch keys and watermarks. Valid synthetic Arrow groups include nullable,
sensitive, nested, byte and floating-point columns. They exercise the production record validators,
the manifest-first tar writer, the streaming reader and the restore-content reader, checking every
section's membership, order, length, digest and bytes, and re-exporting verified extracted values
produces the same archive bytes. Canonical Plexus documents reparse to their complete ordered Models
across several domains. Synthetic payloads cover empty data, tar block boundaries and long section
paths, and entry permissions stay owner-only.

Separate malformed-input targets check current record headers and fields, inconsistent metadata,
missing or reordered sections and truncated streams before verified contents reach installation.
Materialized cases also reject inconsistent row and group counts, missing identity or column
sections, invalid keys and watermarks, and damaged Arrow bytes. Deduplicator and window properties
compare complete descriptors, typed branch keys, row sequences and watermarks, delayed histogram
removals and every Arrow group's bytes. The restore conversion property streams every rebuilt
keyspace and checks it against the runtime's own encoding of the same keys byte for byte, and
rebuilds windows both from archived sections kept whole and from groups split by a narrow identity
record; its malformed cases reject reversed watermarks, impossible sequences and group counts, a zero
incarnation, missing key, input or argument groups, misplaced group numbers and descriptors that name
more groups than the archive holds. The owning runtime column property separately checks
exact-schema encoding, restore conversion and complete native generation equality. The registered
keyspace and window properties also open admitted native checkpoint views and compare every complete
emitted Arrow group byte for byte with the resident path; the window oracle decodes the same native
checkpoint, since persistence can normalize unused Boolean buffer bits of a live carrier, and
restoring the bounded reader's output compares every significant row value and validity with the
live publication, together with revisions, counts, group boundaries, sequences, watermarks and
delayed removals.

Kafka offsets and branch lifecycle records are written by one streamed writer, whose output a
registered property compares byte for byte with rkyv's encoding of the complete wire shape, and two
more properties stream records from stored native lifecycles and from Kafka offset checkpoints in any
partition order, with partitions recorded twice, comparing every archived value. These
representation checks do not establish capture-fence or installation-ordering correctness, which
keep their production-owner concurrency and recovery evidence, and schema agreement with the
restored schedule remains the restore planner's contract, exercised by the public scenarios.

### Public Scenarios

- The one-node and three-node WASM and Kafka restore scenario compares every archived field and raw
  resource and guest byte before the restored domain starts, then proves later `START` behavior and
  two-branch isolation. It asserts domain renaming, the stopped lifecycle and the new guest lifetime
  explicitly; capture metadata belongs to each new backup.
- The materialized one-node and three-node restore scenario re-exports the stopped domain before
  `START`, comparing every descriptor field, identity and Arrow byte, including generations larger
  than the bulk budget. Its acknowledged materialized inputs keep an explicit event timestamp across
  retries, and an identical successful replay must preserve the exact revision count and every
  archived field, even when a failed earlier attempt reached only part of the relay fan-out. The
  restore resets the source ownership fence to zero in the stored checkpoint, while revisions and
  branch generations stay exact.
- The native metadata backup scenario restores lifecycle and Kafka metadata above the bulk budget
  stopped, interrupts its first re-export partway through a section, which publishes no archive, and
  re-exports it again with every value identical while each node stays within its bulk budget. It
  then restores that archive with `RESUME` and backs up the running domain, whose every lifecycle
  branch and partition offset is the same. The native metadata restore scenario also restarts the
  restored owner and replicas, preserves their placements and exact stored values, and checks later
  Kafka input and isolated output for both tenants. `just test-native-metadata-restore-repeat`
  qualifies its three-node example ten consecutive times with concurrency four and no retries; each
  attempt keeps its source revision, source patch, scenario output and node logs under
  `target/native-metadata-restore`, and only ten passing examples produce a completion record.
- The guest-save scenario uses two 40 MiB branch saves on one and three nodes. It measures every
  owner's bulk reservations during a public backup, interrupts a guest section after two chunks and
  verifies that no archive appears, then retries, restores with `--resume`, compares the guest bytes
  exactly and checks each branch's next output. A mismatch names each differing save with its
  lengths, digests, first differing offset and the bytes there, never the whole saves. A resumed
  restore replays the inputs whose acknowledgements its archive does not hold, so the exact
  comparison relies on every input of the source cut being acknowledged; a checkpoint of a save this
  large can outlast the source's `ACK TIMEOUT` under load, which the branch's keepalive of the inputs
  that checkpoint holds keeps from becoming a replay. See [WASM State And
  Recovery](./wasm-state.md#stages).
- The deduplicator and window scenarios on one and three nodes re-export a stopped restore with
  identical descriptors and Arrow groups, stop an installation before its first deduplicator or
  window checkpoint, which leaves `START` gated, and release a delayed coordinator's publication after
  a resumed restore, which is refused without changing the active keyspaces and windows. The running
  restore scenario re-exports its deduplicator state after the endpoint accepts a metric probe and
  before admitting new payments, comparing exact descriptors and Arrow groups, each key's original
  `seen_at` included, with the source archive; the restored window's retained rows are checked by
  their later output for both branches. A focused keyspace test checks that a restored duplicate
  stays suppressed until its original three-minute deadline and is accepted at that deadline; the
  public scenario observes eventual delivery after that boundary, because a loaded runner may delay
  the first post or the read beyond the deadline.
- The stopped cut outline on one and three nodes makes the last write of every window branch
  outlast its stop grace, takes a stopped backup as soon as `STOP` returns, and requires the same
  deduplicator and window sections a quiesced backup took before the stop. A difference names each
  section only one archive holds and the first differing byte of a shared one, never the bytes.
- The stored-branch capture outline exercises a stopped domain and a stopped restore with a keyspace
  and a window above 32 MiB beside 24 small branches, checks exact re-export, and reports sampled
  per-node bulk and metadata reservations separately from harness allocator statistics. Mappings are
  included in their admission class; jemalloc's resident figure excludes mappings outside jemalloc
  and is not the process's complete resident set.

The handles that read, originate and install Kafka offset and materialized relay state carry
rustdoc examples beside the current API, run by `just test-capability-docs`. The compiling examples
name every handle and show that each may be cloned except a materialized relay's originator, which
owns its branch records exclusively and moves; the paired `compile_fail` example rejects cloning that
originator. The other `compile_fail` examples call operations that are private to the runtime crate,
so they fail whichever handle calls them and do not establish which handle may call which operation.
That is fixed by the type each operation is defined on, and the runtime authority that admits each
operation under the capability its binding grants is checked by the assignment authority's Shuttle
checks and Loom models. Quiesced capture, restore, offset, generation and ownership behavior is
established by the ordinary, public and model evidence above, not by those examples.

The reusable Bolero workflow runs the ordinary randomized properties only for a pull request
labeled `bolero-random`, and the sanitizer campaigns, the preferred Bolero evidence, only for one
labeled `fuzz`. Check excludes that workflow only for a verified prose-only pull request carrying
neither label. A skipped run is neither execution nor coverage evidence. Coverage uses the
LLVM tools the toolchain declares, and patch coverage is advisory. The measured freeze durations,
archive throughput and restore times, with their commands and image identities, are recorded on
[Backup 07](https://app.clickup.com/t/86bc8dbxv); the freeze grows with a domain's branches and
state, from a tenth of a second for a few branches to about three seconds for a thousand branches on
one node and seven on three. The restore fault campaign and the staging volume measurements, with
their runs, commands and image identities, are recorded on [Backup: Qualify restores through a
leader crash, a partitioned coordinator, an owner restart and concurrent staging
volume](https://app.clickup.com/t/86bckvq84): three concurrent restores of 40 MiB guest saves staged
at most about 0.3 GB on a state owner, finished in under 20 seconds each on a GitHub-hosted worker,
and left no unpublished staging five seconds after they ended.
