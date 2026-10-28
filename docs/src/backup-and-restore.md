# Backup And Restore

*Plexus, the Nervix graph definition language.*

A backup writes a whole cluster, or one domain, into one archive file on the
machine of the client that ran it. The archive holds each domain's committed models as Plexus, each
domain's lifecycle and clock, its resource catalog with the original bytes of every resource
version, and, for a cluster backup, every user. The archive is a public format: a tar stream whose
sections standard tools can list and extract, and whose records every reader validates before it
trusts them.

A normal backup pauses each running domain in turn, waits for its intake and acknowledged work to
drain, then captures its committed configuration, WASM guest checkpoints, Kafka source offsets,
branch lifecycle, fresh materialized relay generations, deduplicator keyspaces and the rows every
window retains. It resumes that domain before transferring the captured sections into the
archive. Stopped domains need no pause. `WITHOUT PAUSE` captures the latest published state while
the domain runs and has crash-consistent rather than quiesced semantics; `WITHOUT STATE` captures
configuration only. Queued relay batches and acknowledgements remain volatile.

A restore recreates what an archive holds: every domain and user of a cluster archive in a fresh
cluster, whatever its nodes are named, or one domain beside the domains a cluster already has,
under its archived name or a new one.

This chapter owns the statements, their options, the command-line and console surfaces, the
archive's sections and records, the limits and the reported failures. [Backup And Restore
Architecture](./backup-and-restore-architecture.md) describes how they work: the cut protocol and
owner capture, transfer and retention, restore preparation, durable step progress, state
installation and generation publication, staging maintenance, and the evidence behind each
guarantee.

## Backing Up

```plexus
BACKUP CLUSTER TO '/var/backups/nervix/cluster.nvxb';
BACKUP DOMAIN payments TO './payments.nvxb';
BACKUP DOMAIN TO './current-domain.nvxb' WITHOUT RESOURCES;
BACKUP DOMAIN payments TO './payments-live.nvxb' WITHOUT PAUSE;
BACKUP CLUSTER TO './cluster-config.nvxb' WITHOUT STATE;
```

- `BACKUP CLUSTER` covers every domain and every user.
- `BACKUP DOMAIN <name>` covers one domain. Without a name it covers the session's selected domain.
  A domain backup holds no users.
- `WITHOUT RESOURCES` records every resource version with its checksums and sizes, and leaves out
  the version's bytes.
- `WITHOUT STATE` omits runtime state and takes no domain mutation lease.
- `WITHOUT PAUSE` includes published runtime checkpoints without pausing a running domain. Its
  state and configuration need not be from one quiesced cut.
- `TIMEOUT <duration>` bounds each running domain's cut, from the wait for its mutation lease
  through its owners' staging; the resume that follows is not bounded. It is valid with the normal
  quiesced capture, and without it the cut has 60 seconds. Domains are captured in sequence and each
  receives its own budget. Each remote owner stages its share of a domain's state within that same
  budget, however long its state takes to stage; a stopped domain's remote owners stage within the
  budget too, and a `WITHOUT PAUSE` capture's remote owners within the default one, while the
  leader's own share of those two captures has no deadline. The command's total wait includes all
  cuts, admission and archive assembly.
- The path names a file on the client's machine. A leading `~/` refers to the client user's home
  directory. In the web console it names the file the browser saves the archive as.

`BACKUP` runs in `nervix-cli`, in the Rust and C clients, and in the web console, which saves the
archive as a browser download; see [From The Web Console](#from-the-web-console). It must be sent
alone: not in a batch with other statements and not while a transaction is open.

Each domain records the applied configuration revision and Raft log entry of its own capture, and
every owner applies that revision before it reads state. For a quiesced running domain, the leader
holds a replicated domain mutation lease, pauses the domain and drains its admitted work across the
live nodes and every node the schedule assigns the domain's work to, asks the scheduled state owners
to publish and stage their checkpoints, and reads the configuration while the cut is held. Messages parked by `REQUIRED WAIT` and the rows a window retains do not hold
the drain: the cut captures them as state. The leader resumes the domain and releases its lease
before it copies the staged state into the final archive, and other domains continue
independently. A stopped domain needs no pause: its owners read their stored checkpoints. A
`WITHOUT PAUSE` capture takes no lease and reads the latest published checkpoints while the domain
runs, and reports a live cut even for a stopped domain.

Every state a cut archives comes from the node the domain's schedule names as that placement's
primary, never from a replica's copy. A cut whose scheduled state owner the cluster does not report
live fails, and so does a cut whose schedule places such an entity on no node, as when every node
that could host it is cordoned or down, and a quiesced cut, before it pauses, while a node that
runs the domain's work is not reported live, so the archive of a quiesced or stopped cut never
lacks a placement's state.
A `WITHOUT PAUSE` capture holds no lease, and a placement that failover or relocation moves while it
runs can be left out. Send the backup again once the node is back, or once failover has moved its
placements; with replicas, failover promotes one as soon as the failed cut has released its lease.

The cut captures every kind of runtime state the archive holds: WASM guest saves, Kafka source
offsets, branch lifecycle, fresh materialized relay generations, deduplicator keyspaces and the rows
every window retains. Each owner stages its share one checkpoint at a time, so state larger than the
node's bulk memory budget is archived within it. [The Quiesced
Cut](./backup-and-restore-architecture.md#the-quiesced-cut) and [Capture On The
Owners](./backup-and-restore-architecture.md#capture-on-the-owners) describe each step.

An unavailable emitter keeps its publish and ACK counts outstanding until `TIMEOUT` expires; the
failed backup reports those counts and resumes the domain. A leader change during a cut refuses
that cut. The next leader resumes the domain and runs the backup again under the same execution
reference, so a client that retries the reference receives the archive of that new cut.

The server renders each domain's models as Plexus, parses the rendered text back, and refuses the
backup unless it yields exactly the committed models. A resource version's bytes are read from the
leader's resource store and checked against the size the catalog records.

A completed backup reports the archive it wrote:

```text
backed up the cluster: 2 domains, 3 users, 48213504 bytes, digest 9f86d081…; archive written to '/var/backups/nervix/cluster.nvxb'
```

The client writes the archive into a private file beside the destination, with permissions that
only the owner may read and write (`0600`), and replaces the destination only once every byte
arrived and matched the archive's size and BLAKE3 digest. A destination never holds a partial or
foreign archive.

## From The Command Line

`nervix-cli backup` runs one backup and exits:

```sh
nervix-cli backup cluster --output cluster.nvxb
nervix-cli backup domain payments --output payments.nvxb --without-resources
nervix-cli backup domain payments --output payments.nvxb --timeout 30s
nervix-cli backup domain payments --output live.nvxb --without-pause
nervix-cli backup cluster --output config.nvxb --without-state
nervix-cli backup domain --output - > current-domain.nvxb
nervix-cli backup cluster --output cluster.nvxb --format json
```

| Argument | Meaning |
| --- | --- |
| `cluster` or `domain [NAME]` | What the archive covers. `domain` without a name covers `--domain`. |
| `--output PATH` | Where the archive is written. `-` writes it to standard output. |
| `--without-resources` | Leaves resource version bytes out of the archive. |
| `--without-state` | Captures configuration only. |
| `--without-pause` | Captures published runtime state without quiescing a running domain. |
| `--timeout DURATION` | Bounds the normal quiesced capture of each running domain. |
| `--backup-wait-timeout DURATION` | Bounds the overall command wait across all cuts, redirects and reconnects. Defaults to `10m`; also applies to Plexus through `--command` and the REPL. |
| `--execution-reference REFERENCE` | Recovers the same backup under its durable reference, with the original selected domain, scope, resources and capture options. The output destination may change. |
| `--format text` or `--format json` | How the report is printed. Text is the default. |

When the archive goes to standard output, the report goes to standard error, so standard output
carries only the archive. The command exits with a nonzero status whenever the archive was not
delivered. With `--format json` the report is one JSON document:

```json
{
  "execution_reference": "0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44",
  "output": "cluster.nvxb",
  "total_bytes": 48213504,
  "blake3": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
  "captured_at": "2026-09-27 12:00:00 UTC",
  "resources": "included",
  "users": 3,
  "domains": [
    { "domain": "payments", "revision": 812, "cut": { "kind": "quiesced", "engaged_at": "2026-09-29 10:00:00 UTC", "released_at": "2026-09-29 10:00:01 UTC", "buffered_records": 0, "buffered_bytes": 0, "dropped_records": 0, "rejected_records": 0 }, "sections": 7, "section_bytes": 48190112 }
  ]
}
```

A failure prints `{"error": {"code": "...", "message": "..."}}` instead. The codes are
`INVALID_ARGUMENTS`, `CONNECTION_FAILED`, `BACKUP_FAILED` for a failure between client and
server, `BACKUP_REFUSED` for a backup the server refused, `WRITE_FAILED` for an archive that did
not reach standard output, and `CLEANUP_FAILED` for an archive that reached standard output while
its staging directory could not be removed.

### Delivering To Standard Output

With `--output -`, the CLI first makes sure standard output can receive the archive. It then
downloads the archive into a staging directory of its own under the system's temporary directory
(`TMPDIR` on Unix), as a file only its owner may read and write (`0600`), and verifies it there
before it copies a byte to standard output. The complete download releases the server's copy of
the archive, so from then on the staged archive is the only one. The copy goes through a
descriptor of the CLI's own for standard output, without a buffer, so every write reaches the
operating system and every failed write is reported.

- A standard output that would discard the archive fails with `WRITE_FAILED` before anything is
  staged or sent: the null device, which accepts every byte and keeps none, and a standard output
  that was closed when the CLI started, where the CLI finds the null device instead. The CLI cannot
  tell the two apart and refuses both. No backup was taken, so the report names no execution
  reference; give the CLI a pipe, a file or a terminal and run the backup again.
- A staging directory that cannot be created fails with `WRITE_FAILED` before the backup is sent.
  No backup was admitted, so the report names no execution reference.
- A delivery that fails after the download fails with `WRITE_FAILED`: a reader that closed the
  pipe, a full disk behind a redirection, or a staged archive that could not be read. A closed pipe
  does not end the CLI through `SIGPIPE`; its failed write is reported like any other. Standard
  output may hold part of the archive, and the report goes to standard error as always. The CLI
  keeps the verified archive in its staging directory, and the report names it together with the
  backup's execution reference: JSON adds `error.execution_reference` and `error.archive`, and
  text ends with `recover backup REFERENCE from its verified archive at 'PATH'; ...`.
- When every byte reached standard output but the staging directory could not be removed, the
  command fails with `CLEANUP_FAILED` and names the execution reference and, in JSON, the directory
  as `error.staging`. The archive on standard output is complete; remove the directory, which may
  still hold a copy of it.

Recover an undelivered archive by delivering the kept file, for example by moving it to a working
destination. It is the archive the backup assembled, with the summary and cuts its execution
reference records, so no new capture is needed. Running the backup again with
`--execution-reference` returns that recorded outcome, but its download is refused as
`NotRetained`, because the first complete download collected the archive. Nothing removes a kept
archive: delete it once it is delivered, and treat it as the secret it is.

```sh
nervix-cli backup cluster --output - --format json | ssh backup-host 'cat > cluster.nvxb'
# {"error":{"code":"WRITE_FAILED","message":"the backup archive could not be written to standard output: Broken pipe (os error 32)","execution_reference":"0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44","archive":"/tmp/.tmpQ2x9Lk/backup.nvxb"}}
mv /tmp/.tmpQ2x9Lk/backup.nvxb cluster.nvxb
```

### Waiting And Recovering

The native client uses one bounded overall command wait for `BACKUP`, ten minutes by default.
`--backup-wait-timeout` sets it independently of each domain's `--timeout`, without counting or
predicting domains. Native wait options accept durations from one millisecond through 24 hours.
Redirects and reconnects share the same deadline and execution reference. Ordinary request and
retry deadlines remain separate; an archive download begins after the command completes and
bounds each frame's wait with the ordinary request timeout.

When the command wait expires, admission may already have happened. The CLI exits unsuccessfully
and reports the durable reference. A JSON `BACKUP_FAILED` report includes
`error.execution_reference` for an uncertain command or failed archive download. Recover promptly
with the same selected `--domain`, scope, resource inclusion and capture options:

```sh
nervix-cli --domain payments backup cluster --output cluster.nvxb --timeout 30s --backup-wait-timeout 4m --format json
nervix-cli --domain payments backup cluster --output recovered.nvxb --timeout 30s --backup-wait-timeout 10m --execution-reference 0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44
```

Use the reference returned by the first command. Recovery waits for the applying command or
returns its recorded outcome, then downloads the retained archive. `--output -` may also be used
for recovery. A `WRITE_FAILED` report that names a kept archive follows a complete download, which
already collected the server's copy, so it is recovered from that file instead; see
[Delivering To Standard Output](#delivering-to-standard-output). Changing capture inputs
conflicts with the bound request. A fresh reference starts a separate backup. Expiration of the
client wait ends only that waiter; it does not cancel the admitted command or extend the server's
retry validity or archive retention. A longer wait can therefore recover a terminal outcome whose
archive is already unavailable, especially when the server has a shorter retry validity. Such a
download is reported as a failure with its reference.

## Downloading The Archive

The leader assembles the archive in its staging area and retains it under the backup's execution
reference. The client then downloads it through the session service's `DownloadBackup` streaming
call, which the web console reaches through a console WebSocket of its own;
[Sessions](sessions.md#backup-downloads) describes the call.

- The archive is retained until a download has queued all of it, or until the retry validity of
  the backup's execution reference ends: 15 minutes by default, counted from the time the reference
  was issued; see [Command Completion](command-completion.md#lifecycle-and-ownership).
- A download is not bounded by the request timeout, because an archive may take far longer to
  transfer than a command takes to run. Each frame must arrive within the request timeout instead.
- A download that fails in transport starts again from the first byte. Running the backup again
  with the same execution reference returns its recorded outcome, and the client downloads the
  archive again while it is retained.
- The first download that queues the archive's last frame releases it, before its client confirms
  that the frame arrived. A later download of the same backup is refused, and a new backup is
  needed for another copy.
- Only the user who ran the backup may download its archive.
- The archive stays on the node that assembled it, and the backup's recorded outcome names that
  node. A download sent to another node is sent on to it, so a backup run again under its execution
  reference after leadership moved still downloads the archive, from whichever node the client
  reaches.
- A node that restarts loses the archives it retained, and their downloads are refused. If
  leadership moves while a backup runs, the new leader runs the backup again under the same
  execution reference and retains the archive it assembles. The run that records its outcome first
  is the backup's outcome, and its archive is the one downloaded; an archive another run assembled
  is released and never offered.

## Inspecting An Archive

```plexus
DESCRIBE BACKUP './cluster.nvxb';
DESCRIBE BACKUP './cluster.nvxb' FORMAT JSON;
```

`DESCRIBE BACKUP` runs in `nervix-cli` against a file on its own machine and contacts no server. It
reads the whole archive and verifies every section's length and digest before it prints anything.
The description names the archive format, the Nervix and Plexus releases that wrote it, the cluster,
the capture time, the scope, whether resource bytes are included, and the users of a cluster
archive. For each domain it names the revision and Raft log entry the domain was read at, its
status, pace and start count, its latest start point, the clock mapping and logical frontier when
the archive holds them, the size and digest of its models, its cut kind with a quiesced cut's
engagement and release times and quiesce counters, and each resource version with the
`root_checksum` and `manifest_checksum` that `DESCRIBE RESOURCE` prints for the same version.
It inventories WASM guest saves by processor and branch fingerprint, Kafka offsets by ingestor with
their partition count, branch lifecycle records by processor and branch count, and materialized
relays by schema fingerprint, revision, fence, branch generation, row count and bounded Arrow
groups. `FORMAT JSON` also lists every Kafka position by topic and partition. Deduplicator keyspaces
are listed by branch fingerprint with their key count, groups, revision and schema fingerprint.
Window processor state is listed by branch fingerprint with its retained rows, groups, branch
incarnation, next admission sequence, pending delayed histogram removals, revision, schema
fingerprint and window model digest. The inspection does not print branch-key field values or
deduplicator keys.

```text
backup: ./cluster.nvxb
format: 1
producer_version: 0.1.0
language_version: 0.1.0
cluster: nervix-3f1c
captured_at: 2026-09-27 12:00:00 UTC
scope: cluster
resources: included
users: default,operator
domains:
- domain=payments revision=812 raft_term=4 raft_index=812 status=RUNNING pace=unpaced start_version=2
  start_point: Resume
  models: bytes=4213 blake3=5d41402abc4b2a76b9719d911017c592…
  cut: quiesced
  cut_times: engaged_at=2026-09-27 12:00:00 UTC released_at=2026-09-27 12:00:01 UTC buffered_records=0 buffered_bytes=0 dropped_records=0 rejected_records=0
  resource_versions:
  - resource=proto version=1 state=completed root_checksum=… manifest_checksum=… file_count=3 total_bytes=18204 archive_bytes=24576 created_by_node=node-1 created_at=2026-09-20 08:15:00 UTC archive=included blake3=…
  runtime_state:
  - deduplicator=unique_payment_filter branch=4f2a… keys=1204 groups=1 revision=37 schema_fingerprint=…
  - window_processor=latency_window branch=4f2a… rows=1 groups=1 incarnation=1 next_sequence=6 delayed_removals=4 revision=12 schema_fingerprint=… model=…
```

An archive that fails verification is refused whole, naming the check that failed and, where the
check concerns one section, that section. The JSON form prints `{"error": {"code":
"INVALID_ARCHIVE", "message": "..."}}` instead.

## Restoring

```plexus
RESTORE CLUSTER FROM '/var/backups/nervix/cluster.nvxb' ON EXISTING USER SKIP;
RESTORE DOMAIN payments FROM './payments.nvxb';
RESTORE DOMAIN payments FROM './payments.nvxb' RESUME;
RESTORE DOMAIN payments AS payments_copy FROM '/var/backups/nervix/cluster.nvxb';
RESTORE DOMAIN payments AS payments_copy FROM './payments.nvxb' DRY RUN;
RESTORE DOMAIN payments FROM './payments.nvxb' WITHOUT SOURCE OFFSETS;
```

- `RESTORE CLUSTER` recreates every domain and imports every user of a cluster archive. It refuses
  a domain archive.
- `RESTORE DOMAIN <name>` recreates the archived domain `<name>` of a cluster or domain archive,
  and imports no users. `AS <new_name>` restores it under another name, which is how a domain is
  copied beside its original in the same cluster.
- `ON EXISTING USER` decides what a cluster restore does with an archived user the cluster already
  has. `FAIL`, the default, refuses the restore before it changes anything. `SKIP` keeps the
  existing user and its password. `REPLACE` gives the existing user the archived password hash.
  Every other archived user is created with its archived password hash, so its original password
  authenticates.
- `DRY RUN` receives and verifies the archive and plans the whole restore, and changes nothing.
- `RESUME` makes each restored domain running after its complete state set is published on every
  assigned owner and replica. Its start generation and latest start point stay exactly as archived.
  Omit it to leave the restored domain stopped. It precedes `ON EXISTING USER` and `DRY RUN`.
- `WITHOUT STATE` restores the configuration and purges state for the target domain.
- `WITHOUT SOURCE OFFSETS` restores WASM, branch lifecycle, materialized, deduplicator and window
  state but starts sources without the archived Kafka positions.
- A state section whose entity is absent or whose schema fingerprint differs from the published
  schedule is skipped. The restore succeeds and reports a warning naming the skipped state in its
  command diagnostics and CLI output; that entity starts without the skipped checkpoint. A dry run
  reports no such warnings, because they are derived from the schedule an applied restore
  publishes.
- Every other archived branch key must be a key of the branching its restored entity declares:
  each branch lifecycle entry, the branch each WASM, deduplicator and window descriptor names, and
  the branch of each materialized record identity. See
  [Archived Branch Keys](#archived-branch-keys).
- A window whose restored window processor is not the archived window model, or whose branch
  incarnation is not the one the restored branch lifecycle holds for that branch, is skipped with a
  warning naming the reason, and that branch's window starts empty.
- A state record whose header names another state kind than the manifest declares for its
  section, or a record version the reader does not support, is also skipped with a warning,
  together with the sections it owns. The archive reader still rejects malformed records of a
  supported kind and an unknown record kind in the manifest, and always validates section lengths
  and digests.
- The path names a file on the client's machine. A leading `~/` refers to the client user's home
  directory. In the web console it names the archive file the operator chose.

A fresh cluster already has the user its configuration creates when the cluster starts, and a
cluster archive holds that user too, so restoring a cluster archive into a fresh cluster takes
`ON EXISTING USER SKIP` to keep the fresh cluster's password, or `ON EXISTING USER REPLACE` to take
the archived one.

`RESTORE` runs in `nervix-cli`, in the Rust and C clients, and in the web console, which streams an
archive file the operator chooses; see [From The Web Console](#from-the-web-console). It must be
sent alone: not in a batch with other statements and not while a transaction is open.

### What A Restore Recreates

- **Users.** Each archived user, with its password hash exactly as the archive holds it.
- **Domains.** Each restored domain is initially created stopped, with its archived pace and placement
  default, start generation and latest start point. Complete publication releases its installation
  gate. With `RESUME`, that same consensus effect changes it to running, without a new `START`.
  `LIST DOMAINS`, `DESCRIBE DOMAIN`, restore reports and dry-run reports expose the status and start
  generation; `DESCRIBE DOMAIN` also exposes the start point and installed mapping.
- **Resources.** Each resource the domain declared, and each completed version under its archived
  number, with its checksums, file count, sizes, creation time and creating node as
  `DESCRIBE RESOURCE` showed them in the source cluster. A number the source assigned to an upload
  that failed or was still installing when the backup read it stays a gap, and the next upload of
  the resource receives the number the source would have assigned next. See
  [Resource Versions](resource-versions.md#restored-versions).
- **Models.** Every model of the domain, bound to exactly the resource versions it was bound to in
  the source cluster. `SHOW CREATE` prints each restored model as it printed it in the source.

A restore stages the archive's compatible branch lifecycle, WASM guest checkpoints, Kafka
positions, materialized relay checkpoints, deduplicator keyspaces and windows on every newly
assigned owner and replica after its models are scheduled. Each node validates the complete staged
set and durably selects it at once, so its readers see either the complete previous set or the
complete restored one; nodes without assigned checkpoints publish an empty set. A replicated
installation gate refuses `START` until every target node has published its complete
set; it survives failure, lease release and node restart. [State
Installation](./backup-and-restore-architecture.md#state-installation) describes the staging,
publication and gate. A source uses its restored next offset when it starts, clamped to the
partitions its current source assignment contains and clamped to their live low and high
watermarks. Materialized state binds to the exact restored schema fingerprint and archived start
generation before a branch or generator starts. Its rows, typed keys, watermarks, revision and
branch generation are preserved. A new assignment establishes its own ownership fence.
Materialized row watermarks are preserved event times, including explicit `TIMESTAMP AT` values,
but branch activity is not archived with those rows: a resumed or restarted relay starts its
restored branches' retention at its activation-time domain-clock snapshot, so a preserved past or
future event timestamp cannot expire a restored branch early or extend its idle lifetime.

A deduplicator branch restores every archived key under the restored schema fingerprint, with the
time the key was first seen in the source cluster. Each key is normalized as the restored
`DEDUPLICATE ON` expressions key the values they produce, so a duplicate of an archived message is
suppressed once the restored domain runs, and the key expires `MAX TIME` after its archived first
sighting rather than after the restore. A window branch task re-admits the archived rows in their
admission order when it starts, which rebuilds every aggregate structure, including sketches, and
then restores each linear histogram's pending delayed removals. A window that was partly filled at
the cut therefore closes over rows from both sides of the backup. The window must still be the
archived window model, and its branch must be the lifetime the restored branch lifecycle holds:
otherwise the restore reports a warning and that branch's window starts empty. Branch keys,
admission sequences, watermarks and aggregate argument values are preserved exactly.

Archived Kafka positions follow acknowledged records, and a window acknowledges a row only when it
steps past it. The positions of a domain that reads Kafka records into a window therefore stand
before the rows its windows retain at the cut, and a source that resumes from them reads those rows
again, which the restored window then admits beside the rows it restored. A restart has the same
effect; see [Shutdown And Recovery](shutdown.md).

For a paced `RESUME`, the archived wall/logical origin and time rate are installed verbatim. The
mapping projects elapsed downtime; the leader chooses a fresh clock authority for this cluster.
The archived frontier is inspection metadata, rather than a new clock origin. `TIMESTAMP AT`
admission follows the projected reached windows. A paced archive without its committed mapping
cannot resume and is refused before admission. An unpaced resume needs no clock mapping.

A normal `START` after a default stopped restore, or after `STOP`, advances the start generation
and clears materialized state. It preserves compatible WASM state, source positions, branch
lifecycle, deduplicator keys and windows. To establish a new clock at the saved logical frontier
instead, leave the domain stopped and use `START AT '<frontier>' TIME RATE <rate>` explicitly.

### Archived Branch Keys

A restore installs an archived branch key only when it is a key of the branching its restored
entity declares in the schedule the restore publishes. A processor runs every branch under its one
declaration, and a relay's materialized records carry its declared branch. An ingestor or a
reingestor keeps one branch lifecycle for all of its routes, and each route writes the branch of
the relay it targets, so a key it archived may belong to the branching of any of those relays,
unbranched included. A key belongs to a branching when it is absent exactly where the branching is
unbranched, and otherwise holds exactly the fields of the branch's key schema, each with a value of
exactly the field's declared type: the same integer width, a float that is finite, a datetime that
reads as RFC 3339 text, and an `ARRAY` of its declared length. A key lists its fields in field-name
order, the order the archive reader requires. No value is converted to fit, and a key that does not
belong is never staged, so no installed checkpoint names a branch the restored entity's branch
routing could not reach.

Planning checks every branch lifecycle entry and the branch of every WASM, deduplicator and window
descriptor whose entity the planned schedule holds under the archived schema fingerprint. A section
skipped because its entity is absent or holds another schema fingerprint is not checked, while a
window skipped only for its window model or branch incarnation still is. The conversion of a materialized relay checks the branch of each
record identity as it reads it. A dry run runs the same checks, and a restore `WITHOUT STATE`
installs no archived key and checks none. A key that does not belong refuses the whole restore
before it changes anything. The refusal names the archive section, the lifecycle entry, descriptor
or record identity, numbered from 1, the entity, the branches it runs in and, for a key of another
shape, the field that is missing, undeclared or of another type. It never includes a key value,
which a branch schema may declare `SENSITIVE`:

```text
restore refused: the branch key of lifecycle entry 1 in archive section 'domains/payments/state/branch_lifecycle/deduplicator/unique_payment_filter/branches.rkyv' does not belong to the branching of deduplicator 'unique_payment_filter' in domain 'payments': it is not a key of branch 'by_tenant': field 'tenant' does not hold a value of its declared type: expected STRING, found I64
```

Installation checks the same keys against the schedule it installs under before it stages any
state, so a restore resumed by a new leader or a retry stages none that does not belong either.

### Publishing The State Generation

A restore stages every checkpoint of a restored domain's state, on every node that will hold it, in
storage no reader sees, then publishes the complete set on each node at once: a reader sees either
the complete previous set or the complete restored one, never a mix. Restored state of any size
installs within each node's fixed working memory. Guest saves stream from the staged archive, and
materialized relays, deduplicator keyspaces, windows, branch lifecycle and Kafka offsets convert one
bounded group or entry at a time into quota-owned files, so a single relay, keyspace or window may
exceed the 32 MiB bulk budget. A conversion can briefly occupy twice its checkpoint's disk space.
[State Installation](./backup-and-restore-architecture.md#state-installation), [Converting Archived
State](./backup-and-restore-architecture.md#converting-archived-state) and [Generation
Storage](./backup-and-restore-architecture.md#generation-storage) describe each step.

Every Arrow section of an archive is scanned before Arrow's reader reads it, whatever digests the
archive carries for its own bytes. A section whose stream is not framed within its bytes is refused
as misframed, and one that declares a field type Nervix does not carry, a dictionary encoding or
compression, or a record batch at odds with its own schema, as undecodable, when planning reads it,
a dry run included. Nothing is allocated from a length the section only declares.

A conversion the node refuses only for room does not fail the restore. A memory class with no room
left, a worker queue that is full and a staging quota that cannot hold another piece judged nothing
of the archive, so the deduplicator or window conversion is tried again every 50 ms for up to 30
seconds after the node first refused it. A refusal that outlasts that wait ends the conversion and
is reported as the node having had no room for it, since what the restore itself holds can be what
fills the budget. A request larger than a whole budget, and archived content that does not fit the
restored shapes, end it at once. A materialized relay's conversion is not tried again: a refusal
ends it.

### Restore Checkpoint Storage Quota And Metrics

`nervix-server --restore-staging-max-bytes <bytes>` and
`NERVIX_RESTORE_STAGING_MAX_BYTES` set the node-local unpublished checkpoint limit. Without them the
node derives it at startup as the smaller of `128GiB` and the size of the volume that holds its
database, and a given limit larger than that volume refuses the start, as
[The Startup Preflight](./node-settings.md#the-startup-preflight) describes. This is separate from
the quota for archive and transfer files. Admission counts the
current key and value bytes of every unpublished restored namespace across all domains, including
partial chunks, headers, indices and receipts, reading each unpublished entry one chunk at a time
without assembling a payload. The complete incoming checkpoint footprint must fit before the first
chunk is written; the installation barrier prevents concurrent jobs from oversubscribing the
allowance. Staging a checkpoint again first removes the earlier attempt at the same placement in
that generation, complete or not, and reuses its allowance. A node keeps its measurement for as long
as it stays exact, so admission and an idle sweep read no staged entry; [Restore Staging
Maintenance](./backup-and-restore-architecture.md#restore-staging-maintenance) describes when it
measures again.
Published checkpoint data is outside this quota; leftover receipts remain staging until removed.
Lowering the allowance on restart preserves retained installations that already exceed it and
refuses additional staging until usage fits or those attempts end.
A quota refusal reports the limit, used bytes and requested bytes and leaves the target stopped
with its installation gate closed. After terminal failure, maintenance reclaims the attempt. An
uncertain applying restore is recovered by sending it again under its execution reference, as the
Rust client's `restore_with_reference` does; `nervix-cli restore` starts every restore under a
fresh reference. A terminal failed restore requires a fresh target domain for another restore.

A sweep runs only when the node's bulk memory has room for its 2 MiB working set; under bulk memory
pressure it is deferred to the next second rather than waiting for that room, so reclamation never
holds up the node's other bulk work. The public `/metrics` endpoint reports these node-local series
after completed sweeps, and reads zero until the first sweep completes:

| Series | Meaning |
| --- | --- |
| `nervix_restore_staging_bytes` | Current unpublished checkpoint key and value bytes |
| `nervix_restore_staging_keys` | Current unpublished checkpoint key count |
| `nervix_restore_staging_limit_bytes` | Unpublished checkpoint allowance, as configured or derived from the database volume at startup |
| `nervix_restore_staging_reclaimed_bytes_total` | Unpublished and unreferenced active checkpoint key/value bytes removed by completed sweeps since node start |
| `nervix_restore_staging_sweeps_total` | Completed sweeps since node start |
| `nervix_restore_keyspaces_disk_bytes` | SST allocation of shared checkpoint and publication keyspaces |

Logical usage excludes tombstones, database overhead and the bytes older snapshots retain. The
SST allocation includes initial and published checkpoints, retained snapshot data and space
awaiting compaction. Fjall reports its current LSM levels; files awaiting final unlink need not
appear. It excludes the shared journal and other keyspaces. This quota bounds live
unpublished data and does not impose a hard filesystem limit. Physical space falls as snapshots
close and database compaction reclaims files. A cancelled or failed sweep updates no series and
does not count its partial deletion; the next completed sweep reconstructs current usage, because
a sweep forgets the kept measurement before it removes anything it counts.

### Order Of Steps

A restore first receives the archive and plans everything it will do. Nothing changes before both
have finished:

1. **Receive.** The client streams the archive to the leader, which stages it in its staging area
   and checks that it is exactly the size and BLAKE3 digest the client declared.
2. **Verify.** The leader reads the whole archive and verifies its manifest, every section's length
   and digest, and every record. It parses the `models.plexus` of every domain the archive holds,
   which must hold one `CREATE` of a model per statement.
3. **Plan.** The leader resolves every archived user under the user policy, checks that no
   restored domain name exists, checks each imported resource version's bytes against the
   version's root checksum, pins every model to the resource versions the restore imports, and
   plans each domain's models with the transaction planner against the domain as the restore will
   create it. Unless the restore is `WITHOUT STATE`, it also checks every archived branch key and
   converts every materialized relay, deduplicator keyspace and window under the restored models.
   A dry run ends here and reports the plan.

The restore then applies its steps in order, and records each step in the replicated state as it
completes:

1. **Import users**, for a cluster restore.
2. For each restored domain, in archive order:
   1. **Create the domain**, stopped, with its declared resources.
   2. **Import its resource versions**: each version's bytes are installed from the archive on the
      leader and checked against the version's checksums, the version is published under its
      archived number, and the restore waits until every live node has installed it, exactly as an
      upload completes.
   3. **Apply its models** as one batch, with full graph validation and the leader's content
      checks: TLS material loads, lookup data loads, WASM modules compile, inference models load,
      and UDFs prepare. The batch is not bounded by the statement and source-byte limits of a
      transaction. Before recording this step, admit an installation generation bound to the
      leader tenure, restore execution and mutation lease. Stage compatible branch lifecycle,
      source offsets, WASM saves, materialized generations, deduplicator keyspaces and windows on
      the newly scheduled owners and replicas, then publish the complete set on every target node.
      Recording completion releases the replicated start gate.
      With `RESUME`, that same effect installs the archived clock mapping and makes the domain
      running; a delayed coordinator cannot republish after activation.
      `WITHOUT STATE` publishes an empty set, and `WITHOUT SOURCE OFFSETS` omits source positions.

A completed restore reports what it recreated, and each step:

```text
restored the cluster from '/var/backups/nervix/cluster.nvxb': 2 domains, 5 resource versions, 14 models, users 1 created, 1 skipped, 0 replaced; restored domains are stopped
- applied: import users
- applied: create domain 'payments'
- applied: import resource versions of domain 'payments'
- applied: apply models of domain 'payments'
- applied: create domain 'ledger'
- applied: import resource versions of domain 'ledger'
- applied: apply models of domain 'ledger'
```

A dry run reports every step as `planned`, and for each domain the report of its model run: the
[transaction impact report](transaction-quiescence.md) the transaction planner produces for the
batch of the domain's models.
Each domain also reports its planned or applied `status` and archived `start_version`.

A step that fails ends the restore. The steps before it stay applied, and the outcome names the
step and the reason, followed by the report of every step. Users, a created domain and the release
of the installation gate are recorded together with their effects; resource versions and the model
batch are applied first and are found applied when the restore repeats their step. A created
domain that does not become usable on every live node fails its create step after recording it, so
that step stays applied and is reported failed:

```text
restore failed at step 'apply models of domain 'payments'': ...; the steps before it stay applied
```

A domain the failed restore created stays stopped, with the resource versions and models it
committed. Its durable installation gate keeps `START` blocked if state installation did not
complete. Staging failures leave the complete previously published set visible on each node; a
failure during distributed publication may leave nodes at different complete generations, but
the domain cannot run. A restore never creates a domain whose name exists, so restore the archived
domain again under another name with `AS`. A cluster restore that failed before it reached some
domains leaves them for `RESTORE DOMAIN`.

### Retries, Disconnects And Leader Changes

A restore is a persistent command under its execution reference, like any command that changes the
cluster; see [Command Completion](command-completion.md). The leader admits it once the archive is
verified and planned, and from then on it holds the mutation lease of every domain it restores, so
no other command changes those domains while it applies.

- A restore sent to a follower is redirected to the leader, and the client sends the archive
  there.
- Once the leader has received the whole archive, the restore goes on when the client disconnects.
- Sending the restore again with the same execution reference joins it: while it still applies on
  the leader, the answer says so at once, and once it finished, the answer is its recorded outcome
  and report. The Rust client sends it again until it has the outcome.
- A restore stream the client abandons before the archive arrived whole changes nothing, and
  releases the staging space it reserved. Sending the restore again with the same execution
  reference sends the archive again from its first byte.
- Only the leader the archive was streamed to holds it. If leadership moves, or the leader
  restarts, while a restore applies, the restore stays applying, and the next leader resumes it
  from its first step not recorded once the client sends the archive again with the same execution
  reference. A leader that still holds the archive, such as one that regains leadership, resumes it
  without a resend. The Rust client sends it again when it is redirected or its connection fails.
  No step is applied twice: each is recorded together with its effect, or finds its effect already
  applied when it repeats. Resuming an unfinished state
  installation admits a new generation. Local and remote staging and publication revalidate
  authority at the storage mutation boundary; a delayed request from the preceding coordinator
  cannot replace the completed generation or clear its runtime handles.
- If no client sends the archive to the new leader before the retry validity of the execution
  reference ends, 15 minutes by default, the new leader ends the restore as failed. The steps it
  recorded stay applied, and the outcome says how many.

The staged archive is released when its restore's outcome is recorded, or when the retry validity
of the execution reference ends; a dry run releases it when it answers.

### Restoring From The Command Line

`nervix-cli restore` runs one restore and exits:

```sh
nervix-cli restore cluster --input cluster.nvxb --on-existing-user skip
nervix-cli restore domain payments --input payments.nvxb
nervix-cli restore domain payments --input payments.nvxb --resume
nervix-cli restore domain payments --as payments_copy --input cluster.nvxb --dry-run
nervix-cli restore domain payments --input payments.nvxb --without-source-offsets
nervix-cli restore domain payments --input payments.nvxb --without-state
nervix-cli restore cluster --input cluster.nvxb --on-existing-user replace --format json
```

| Argument | Meaning |
| --- | --- |
| `cluster` or `domain NAME` | What the restore recreates: every domain and user of a cluster archive, or the archived domain `NAME`. |
| `--input PATH` | The archive file to restore from. |
| `--as NEW_NAME` | Restores the domain under `NEW_NAME`. Only for `domain`. |
| `--on-existing-user fail`, `skip` or `replace` | The user policy of a cluster restore. `fail` when omitted. |
| `--dry-run` | Verifies the archive and plans the restore, changing nothing. |
| `--resume` | Makes the complete restored domain running at its archived generation and clock mapping. |
| `--without-state` | Installs configuration without runtime checkpoints. |
| `--without-source-offsets` | Installs WASM, branch and materialized state, and leaves Kafka source positions unset. |
| `--format text` or `--format json` | How the report is printed. Text is the default. |

While the archive streams, the command shows how much of it was sent on standard error, when
standard error is a terminal. It exits with a nonzero status whenever the restore did not complete.
With `--format json` the report is one JSON document:

```json
{
  "execution_reference": "0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44",
  "input": "cluster.nvxb",
  "mode": "apply",
  "message": "restored the cluster from 'cluster.nvxb': ...",
  "total_bytes": 48213504,
  "blake3": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
  "captured_at": "2026-09-27 12:00:00 UTC",
  "users": { "created": 1, "skipped": 1, "replaced": 0 },
  "domains": [
    {
      "source": "payments",
      "domain": "payments",
      "resource_versions": 3,
      "models": 9,
      "status": "STOPPED",
      "start_version": 2,
      "planned_models": null
    }
  ],
  "steps": [
    { "step": "import users", "outcome": "applied" },
    { "step": "create domain 'payments'", "outcome": "applied" },
    { "step": "import resource versions of domain 'payments'", "outcome": "applied" },
    { "step": "apply models of domain 'payments'", "outcome": "applied" }
  ],
  "warnings": []
}
```

`mode` is `dry_run` for a dry run, whose steps are `planned` and whose domains carry their model
run's transaction impact report in `planned_models`. A step's outcome is `applied`, `planned`,
`failed` or `not_attempted`. `users` is `null` for a domain restore.
`warnings` lists state sections skipped because their record kind or version is unsupported, or
their entity or schema no longer matches the restored schedule. Text output prints the same
warnings after the steps.

A failure prints `{"error": {"code": "...", "message": "..."}}` instead. The codes are
`INVALID_ARGUMENTS`, `CONNECTION_FAILED`, `RESTORE_FAILED` for a failure between client and server,
`RESTORE_REFUSED` for a restore refused before it changed anything, and `RESTORE_INCOMPLETE` for a
restore that failed at a step. A `RESTORE_INCOMPLETE` error carries the restore's report as
`report`, in the shape above.

## From The Web Console

The web console's **Backups** dialog takes a backup as a browser download and restores an archive
the operator uploads, with the same statements, options, reports, and failures as `nervix-cli`.
It runs on the console's own session and follows its leader redirect; a `BACKUP` or `RESTORE` typed
in the console's REPL runs through the same dialog. [Web Console](client-tools-web-console.md#backing-up-and-restoring)
describes the dialog itself.

- **Backup.** The console sends `BACKUP` as a command under an execution reference, then downloads
  the archive with the `DownloadBackup` call over a console WebSocket, starting at the node that
  reported the backup and following it to the console of the node that retains the archive when
  that is another node. It hands the archive to the browser as a download only once its size and BLAKE3
  digest match the backup's summary, under the last component of the file the statement names.
  A download that fails in transport starts again from the first byte after a one-second pause,
  for up to eight attempts and redirects in all, while the archive is retained. A download the
  server refuses ends with its reason, and the backup itself completed.
- **A reload resumes a backup.** The browser tab records the backup's execution reference, query
  text, and domain until its archive is downloaded. A reloaded page sends the backup again under
  that reference, which returns the recorded outcome instead of running the backup twice, and
  downloads the archive again while it is retained, unless the download the reload interrupted had
  already collected it by queuing its last bytes. Closing the tab or signing in as another user
  forgets the record.
- **Restore.** The console reads the chosen file once to measure its size and BLAKE3 digest, and
  streams it with the `RestoreBackup` call over a console WebSocket to the leader. The dialog runs
  the `DRY RUN` form first: its report, and each domain's planned model run drawn as an impact
  report, must plan the current options and file before the restore itself can run. The restore's
  report lists each step as applied, failed, or not attempted, and the result lists the warnings
  for the state the restore skipped.
- **Retries.** Every attempt of a restore sends the same execution reference, statement, and
  archive. A redirect streams it again to the leader, and a lost connection or an unknown outcome
  streams it again from its first byte, for about ten minutes; after that the console reports the
  outcome as unknown and names the reference.
- **What the browser shows.** The dialog shows the summary a backup reports and the report a
  restore returns, and never what an archive holds beyond them. `DESCRIBE BACKUP` runs only in
  `nervix-cli`.
- **Sensitivity.** The browser saves an archive with its downloads directory's permissions, not the
  owner-only file `nervix-cli` writes. A downloaded archive holds password hashes, client secrets,
  and TLS private keys; keep it where only its owner can read it.

## Archive Format

An archive is a tar stream. Every section is a regular file entry with owner-only permissions and a
zero modification time, and a path longer than a tar header holds is carried by a GNU long-name
entry before it. The manifest is always the first entry and lists every other section in the order
the stream holds them, so a reader validates everything that follows against it as the stream
arrives.

| Section | Content | Present |
| --- | --- | --- |
| `manifest.rkyv` | The manifest record | Always |
| `users.rkyv` | The users record | Cluster archives |
| `domains/<domain>/domain.rkyv` | The domain record | Every domain |
| `domains/<domain>/models.plexus` | The domain's models as Plexus | Every domain |
| `domains/<domain>/resources/<resource>/<version>/version.rkyv` | One resource version record | Every resource version |
| `domains/<domain>/resources/<resource>/<version>/archive.tar` | The version's original upload | Published versions, unless `WITHOUT RESOURCES` |
| `domains/<domain>/state/wasm_processor/<processor>/<branch>/descriptor.rkyv` | Typed WASM checkpoint identity, schema fingerprint, generation and revision | Saved WASM branches unless `WITHOUT STATE` |
| `domains/<domain>/state/wasm_processor/<processor>/<branch>/guest.bin` | Raw guest save bytes | With each WASM descriptor |
| `domains/<domain>/state/kafka_offset/<ingestor>/offsets.rkyv` | Ingestor schema fingerprint and next offsets by topic and partition | Published Kafka domain offsets unless `WITHOUT STATE` |
| `domains/<domain>/state/branch_lifecycle/<kind>/<processor>/branches.rkyv` | Typed branch keys, incarnations and LRU order | Published branch lifecycle unless `WITHOUT STATE` |
| `domains/<domain>/state/materialized_relay/<relay>/descriptor.rkyv` | Domain/relay identity, raw schema fingerprint, revision, fence, branch generation, row and group counts | Every captured materialized relay unless `WITHOUT STATE` |
| `domains/<domain>/state/materialized_relay/<relay>/groups/<group>/identities.rkyv` | Typed branch keys and low/high watermarks for the group's rows | Every nonempty materialized group |
| `domains/<domain>/state/materialized_relay/<relay>/groups/<group>/columns.arrow` | Exact-schema Arrow IPC columns in identity order | With each identity group |
| `domains/<domain>/state/deduplicator/<deduplicator>/<branch>/descriptor.rkyv` | Deduplicator identity, schema fingerprint, typed branch key, revision, key count and group count | Every captured deduplicator branch unless `WITHOUT STATE` |
| `domains/<domain>/state/deduplicator/<deduplicator>/<branch>/groups/<group>/keys.arrow` | Arrow IPC with one typed column per `DEDUPLICATE ON` expression and the `seen_at` timestamp of each key | Every nonempty deduplicator group |
| `domains/<domain>/state/window_processor/<processor>/<branch>/descriptor.rkyv` | Window identity, schema fingerprint, window model digest, typed branch key, revision, branch incarnation, row sequences and watermarks, group count and delayed histogram removals | Every captured window branch with a branch lifetime unless `WITHOUT STATE` |
| `domains/<domain>/state/window_processor/<processor>/<branch>/groups/<group>/input.arrow` | The retained input rows as Arrow IPC of the window's input relay schema | Every nonempty window group |
| `domains/<domain>/state/window_processor/<processor>/<branch>/groups/<group>/arguments.arrow` | Each retained row's aggregate argument values, one nullable column per argument in demand order, row-aligned with the input rows | With each window input group |

Materialized group coordinates are zero-based decimal numbers padded to ten digits. A descriptor
with zero rows has zero groups. A record comes before the sections it owns: a resource version
before its upload, a WASM descriptor before its guest save, and a descriptor before its groups.
Within a descriptor, groups may appear in any order, and so may the two sections of one group. The
archive reader verifies both sections of every group, and rejects missing, duplicate or
inconsistent current groups.
Deduplicator and window groups use the same coordinates. `<branch>` is the branch key's
fingerprint in hexadecimal, or `unbranched`. A deduplicator or window descriptor names at most as
many groups as it has keys or rows, and none when it has none; every named group must be present,
and a window group must hold both its input rows and its argument columns.

Every `.arrow` section is one canonical Arrow IPC stream: its schema message, one record batch
and the end-of-stream marker, which ends the section. Every message opens with the continuation
marker, and every length the stream declares, a message's metadata, its body and each column
buffer, lies within the section. Its schema declares only the field types Nervix carries, without
a dictionary encoding or compression, and its record batch declares the field nodes and buffers
that schema's fields take. A restore refuses any other section before it decodes or allocates
anything from it; `DESCRIBE BACKUP` verifies an Arrow section's length and digest without decoding
it.

A resource or state entity named `.` or `..` appears in a section path as `%2E` or `%2E%2E`.

A record section begins with a twelve-byte header: the magic `NVXBKREC`, then the record's kind and
its format version, each a little-endian 16-bit integer. The rest is an
[rkyv](https://rkyv.org) payload that a reader validates with bytecheck before it reads any value.

| Kind | Tag | Format version | Holds |
| --- | --- | --- | --- |
| Manifest | 1 | 2 | Archive format major version, producer and Plexus releases, cluster id, capture time, scope, whether resource bytes are included, each domain's revision, Raft log entry and cut kind, with a quiesced cut's engagement and release times and quiesce counters, and each section's path, content kind, length and BLAKE3 digest |
| Users | 2 | 1 | Every user's name and password hash as a PHC string |
| Domain | 3 | 1 | Pace, default placement policy, lifecycle status, start count, the point the latest start began from, the committed clock mapping, the logical instant the clock had reached, and each declared resource with its next version number |
| Resource version | 4 | 1 | The version's domain, resource and number, its upload state, and for a published version its checksums, file count, sizes, creation time and creating node |
| WASM guest state descriptor | 5 | 1 | The saved branch key, schema fingerprint, guest state generation and checkpoint revision |
| Kafka domain offsets | 6 | 2 | The ingestor schema fingerprint, optional applied explicit-start generation, and ordered topic and partition positions, expressed as the next offset to consume |
| Branch lifecycle | 7 | 1 | The owner kind, schema fingerprint, branch keys, last ingestion times and incarnations in LRU order |
| Materialized relay descriptor | 8 | 1 | Domain and relay names, schema fingerprint, revision, ownership fence, branch generation, row count and group count |
| Materialized record identities | 9 | 1 | Ordered row identities: an unbranched identity or typed branch fields, and low/high ingestion watermarks |
| Deduplicator keyspace descriptor | 10 | 1 | Domain and deduplicator names, schema fingerprint, branch fingerprint and typed branch fields, revision, key count and group count |
| Window state descriptor | 11 | 1 | Domain and window processor names, schema fingerprint, window model digest, branch fingerprint and typed branch fields, revision, branch incarnation, first and next admission sequence, each retained row's low/high ingestion watermarks, group count, and each aggregate's structure: rebuilt from the retained rows, or a linear histogram with its delayed removals as expiry time and bucket |

The archive format major version is `1`. A reader refuses invalid record magic, an unsupported
manifest or required configuration record, an unknown record kind in the manifest, a first entry
other than the manifest, and any section whose place, length or digest differs from what the
manifest declares. A state record whose header names another state kind than the manifest declares,
or an unsupported state record version, produces a restore warning and is skipped with the sections
it owns. [The Archive Contract](./backup-and-restore-architecture.md#the-archive-contract) defines
these rules.

`models.plexus` holds one `CREATE` statement per model, separated by blank lines, in an order that
creates every model after each model its configuration names. Resource bindings name explicit
versions.

An archive holds password hashes, and its models and resources can hold credentials and other
secrets. Store it as a secret.

The [Qualification Evidence](./backup-and-restore-architecture.md#qualification-evidence) of the
architecture chapter lists the properties that check archive fidelity and the public scenarios
that compare restored and re-exported state.

## Limits

| Limit | Value |
| --- | --- |
| One record section | 64 MiB |
| One archive | 64 GiB, the largest artifact a node stages |
| Archives and snapshot transfers a node stages at once | 128 GiB |
| Download chunk | 256 KiB |
| Frames a download queues ahead of its client | 4 |
| Retention | Until a download has queued all of it, or the retry validity of the execution reference ends, counted from when the reference was issued |
| Restore chunk | 256 KiB from the Rust client and the console; the leader accepts any nonempty chunk within one frame |
| Restore frames the Rust client queues ahead of the transport | 8 |
| Restore archive retention | Until the restore's outcome is recorded, or the retry validity of the execution reference ends |
| State section staging on an owner | Charged to the same node staging quota until fetched, or for at most 600 seconds; a Kafka offset or branch lifecycle section reserves the 64 MiB record limit while it is written |
| One owner's capture inventory | One management reply of at most 64 KiB per owner and domain |
| WASM guest save capture | One selected save at a time: `restore_metadata` reserves three times its stored checkpoint size for the inline Fjall value and decoded reader; three fixed 64 KiB bulk buffers cover segmented reads, transfer and staging. A save up to the 64 MiB guest limit can exceed the 32 MiB default bulk budget. |
| Kafka offset or branch lifecycle section capture | One checkpoint at a time per owner: `restore_metadata` holds twice the stored checkpoint plus 64 KiB for the whole section, beside serializer scratch of one resolver per entry; a 64 KiB bulk buffer streams the record |
| One Kafka offset or branch lifecycle section | The 64 MiB record limit |
| Native conversion I/O, checkpoint installation, complete generation publication and each maintenance sweep | Fixed 2 MiB bulk working-memory reservation per job; 64 KiB buffers and checkpoint chunks |
| Retained archive metadata and preparation per node | 2 GiB default independent admission class; 16 times record bytes plus 64 times Plexus bytes plus 2 MiB |
| Native conversion serializer scratch | Fixed-capacity entry resolvers plus the largest entry's nested resolvers, covered by the retained preparation charge |
| Materialized identity group / Arrow group | 1 MiB / 8 MiB |
| Materialized capture metadata / typed uniqueness metadata / native row views | 8 MiB each, independently admitted |
| Deduplicator key group / window input group / window argument group | 8 MiB each; capture fills a group to at most half that bound, and a single larger row takes a group of its own |
| Selected stored branch checkpoint capture | One checkpoint per owner at a time. Before opening, `restore_metadata` admits three times its stored length to cover an inline value's copies. Four 64 KiB bulk blocks plus 8 KiB cover staging. The complete immutable mapping and its retained indexes remain admitted; selected key parts and decoded columns have separate temporary metadata charges. |
| Stored window structure conversion | `restore_metadata` admits a conservative eight times the native length plus 256 KiB for typed removals, descriptor projection and header scratch, independently of the mapping and bounded column buffers. A checkpoint that cannot fit these reservations is refused with its entity named. |
| One archived deduplicator keyspace or window | Not bounded by the bulk budget. A keyspace's key parts and values are charged to `restore_metadata` while it converts, beside the fixed per-key share admitted with the description; a window conversion holds one archived section at a time |
| Window checkpoint identity record | 1 MiB of row identities; a larger archived group splits into groups that fit |
| Restore conversion admission wait | 30 seconds after the node first refuses one unit for room: a window section, a window while its checkpoint is finished, or a keyspace. The unit is converted again every 50 ms |
| Restored window rows a branch reopens | No row ceiling of their own; their row views are charged to the independent state-read memory class while the branch reopens the window |
| Physical checkpoint placement encoding | 60 KiB, including domain and installation namespace |
| Unpublished restore checkpoint keys and values per node | The smaller of 128 GiB and the size of the volume that holds the node database by default; configurable with `--restore-staging-max-bytes`, up to that size |
| Freeze of one running domain | From its pause until its owners have staged their state, at most the backup's `TIMEOUT`, or the 60-second domain drain timeout when none is given; archive assembly and the download follow the resume |

The freeze covers only the drain of admitted work, the confirming force flush and the owners'
staging. The confirming force flush re-reads the materialized state each message parked by
`REQUIRED WAIT` waits for, so a slow state read lengthens the freeze by that read, and by the rest
of a periodic re-read already under way; the drain starts no other flush on a node that holds no
admitted work. The leader resumes the domain before it assembles the archive, so neither the
archive's size nor the client's download rate lengthens the freeze, and a domain whose emitter stops
acknowledging stays frozen only until the timeout fails the backup. A state owner that crashes or
loses contact with its peers during the freeze, while the cluster still reports it live, fails the
backup with `backup failed:`, and the domain resumes either way, a node that was unreachable
resuming once it rejoins. A state owner the cluster reports down when the cut captures fails the
backup as well, naming the node, rather than leaving its state out of the archive; see [Guarantees
And Limits](./backup-and-restore-architecture.md#guarantees-and-limits). A leader change refuses
the cut it interrupted, and the new leader cuts again under the same execution reference. Stopping or
restarting the node that holds an archive ends its download, and the archive is lost with it.

A freeze grows with the branches the domain holds and the state they carry. The freezes measured
for [Backup 07](https://app.clickup.com/t/86bc8dbxv), with the commands and image identities that
produced them, range from a tenth of a second for a few branches to about three seconds for a
thousand branches on one node and seven on three, and stay under two seconds with 72 MiB of WASM
guest state, all far inside the drain timeout.

A backup larger than the staging quota fails. Owners stage their sections, and the leader fetches
them, only while node staging has room, and either refuses when it is full; the final archive waits
for room while the leader's staging area is full, until retained archives are downloaded or expire
and snapshot transfers finish. The leader holds the fetched sections and the assembled archive
together until assembly ends.

A restore stages its archive under the same limits, and is refused rather than kept waiting when
the archive is larger than one archive may be, or the leader's staging area cannot hold it now; send
it again once retained archives are released and snapshot transfers finish. A restore's model batch
is not bounded by the statement and source-byte limits of a transaction. Each node must admit the
bounded installation or publication job within its bulk working-memory budget (32 MiB by default).
A state set, and an individual materialized relay, deduplicator keyspace or window, may exceed that
budget. An individual job that cannot be admitted fails with the start gate still closed.

A restored deduplicator keyspace, window or guest save larger than one 2 MiB replication message is
installed on every assigned owner and replica, and its replicas follow the revisions the owner
publishes after `RESUME` through a bulk checkpoint transfer, up to the 256 MiB transfer bound. The
owner persists them: a window whose retained rows exceed one 8 MiB snapshot section is sealed in
pieces on quota-owned disk and published as a segmented checkpoint, so it survives a restart at
any size the node's snapshot staging quota holds rather than only within the bulk budget.

## Failures

A failed backup reports `backup failed:` and the reason, and its message never includes archive
contents. The reasons are:

- the named domain does not exist, or no domain is selected for `BACKUP DOMAIN`
- a domain's committed models do not form a valid graph, do not render as Plexus, or do not parse
  back to themselves
- a domain's clock mapping cannot be read at the capture time
- a resource version's bytes are not installed on the leader, or do not match their catalog entry
- the archive could not be staged on this node, because it is larger than one archive may be or
  staging failed, or a record does not encode
- a materialized checkpoint's next read, request envelope, staging operation or group conversion
  cannot fit beside the Bulk memory it retains; the additional allocation refuses immediately
  and releases its retained memory. Release the contention and start a new backup; extending
  `TIMEOUT` does not make the overlapping allocations fit
- a captured branch lifecycle or Kafka offset checkpoint does not decode, its record exceeds the
  64 MiB record limit, or its lifecycle keys or serializer scratch cannot be admitted to
  `restore_metadata` now, naming the entity; the checkpoint's own conversion charge waits for room
- a selected WASM guest save cannot be read or its staged write is interrupted; the failure names
  the processor without guest bytes, and its `restore_metadata` conversion charge waits for room
- a node that runs the domain's work is not reported live when a quiesced cut starts, naming the
  domain, an entity the node runs and the node; the domain was not paused
- a placement's scheduled state owner is not reported live when the cut captures, naming the
  domain, the entity and the node
- the schedule places an entity whose state the cut archives on no node, naming the domain and the
  entity; uncordon a node or bring one back so the entity can be placed
- an owner has not applied the cut revision within five seconds, or cannot capture or stage its
  share, naming the domain
- a quiesced domain cannot take its lease or capture its owners within its timeout, reported as
  `timed out waiting to capture domain '<domain>'` with the active phase, its age, total elapsed
  time and completed phase durations; a drain timeout also retains what each node still held.
  Inspect those phases before changing `TIMEOUT`; owner-local capture stages are retained in the
  administrative logs described in [Backup And Restore Architecture](backup-and-restore-architecture.md#observability)
- the backup coordinator lost its leader tenure while capturing a domain

A capture that fails or is interrupted assembles no archive. The leader drops the sections it
fetched and the failing owner drops its partial files; sections other owners staged for the capture
expire 600 seconds after they were staged, holding their staging quota until then. The backup can be
sent again, and takes a new cut. The same refusal applies to a selected stored keyspace or window
whose immutable view, native validation, typed metadata or bounded projection cannot be admitted.
Cancellation is checked between stored chunks, identities and keys; refused or interrupted staging
drops its private files and returns their quota. No partial branch checkpoint becomes an archive
section.

A restore stream the leader refuses reports `restore refused (<failure>):` and the reason, and
changed nothing. The failures are `InvalidStream` and `InvalidStatement` for a stream that does not
open with one `RESTORE` statement or continue with chunks, `SizeMismatch` and `DigestMismatch` for
chunks that add up to another size or have another digest, `QuotaExceeded` for an archive larger
than one archive may be or than the leader's staging area can hold now, and `StagingFailed` for an
archive the leader could not stage.

A restore the leader refuses after it received the archive reports `restore refused:` and the
reason, and changed nothing. The reasons are:

- the archive could not be read on the leader, or is not a valid backup archive: the check that
  failed is named
- a restored domain's `models.plexus` does not parse, naming the domain and the line, or holds a
  statement that creates no model, naming the domain, the statement's number and its line
- `RESTORE CLUSTER` was given a domain archive, or `RESTORE DOMAIN` names a domain the archive does
  not hold
- a restored domain name exists
- an archived user exists and the user policy is `FAIL`
- the archive was taken `WITHOUT RESOURCES` and holds completed resource versions, holds no bytes of
  a completed version, holds a version's bytes that do not match its root checksum, records a
  completed version without its checksums, or names a version outside its resource's declared
  sequence or a resource its catalog does not declare
- a model does not name its resource versions by number, or binds a resource version the restore
  does not import as completed
- `RESUME` was given for a paced domain the archive holds no clock mapping for
- a domain's models do not form a valid configuration, as the transaction planner finds, or do not
  give its deduplicator and window state an archive shape
- an archived branch key the restore would install is not a key of the branching its restored
  entity declares, naming the section, the lifecycle entry, descriptor or record identity, the
  entity, the branch and the field; see [Archived Branch Keys](#archived-branch-keys), or the
  restored entity declares no branching its archived branch keys could belong to
- an archived materialized relay, deduplicator keyspace or window does not convert under the shape
  its restored model gives it, or its conversion cannot be admitted, naming the entity; a dry run
  runs the same conversion. A deduplicator or window conversion the node refuses only for room is
  converted again first, and is refused only once that refusal has lasted its 30-second wait
- the archive's metadata, including the fixed share of every archived deduplicator key, exceeds the
  node's available restore preparation budget

A restore that failed at a step reports `restore failed at step '<step>':`, the reason, and that the
steps before it stay applied, together with the report of every step. The reasons are a consensus
refusal of the step, a created domain whose stopped state did not become usable on every live node,
a resource version that could not be installed or completed on every live node, a domain that holds
models the restore did not apply, a model batch the leader refused, such as lookup data that does
not load at its path or TLS material that does not load, a node that did not apply the installation
revision within five seconds, and state that does not convert or install. Installation converts
each keyspace and window as planning did, with the same wait for room, so a momentarily busy node
does not fail the step; a refusal for room that outlasts the wait does. An admitted restore that
cannot reach its first step not recorded, because the leader no longer holds its archive or the
restore no longer plans against the cluster, reports `restore failed:`, the reason, and that the
steps it recorded stay applied. A restore whose archive no client sent to a new leader before the
retry validity of its execution reference ended reports `restore stopped after the steps it
recorded`. No message includes password hashes or resource bytes.
