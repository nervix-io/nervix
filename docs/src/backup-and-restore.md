# Backup And Restore

A backup writes a whole cluster, or one domain, into one archive file on the
machine of the client that ran it. The archive holds each domain's committed models as NSPL, each
domain's lifecycle and clock, its resource catalog with the original bytes of every resource
version, and, for a cluster backup, every user. The archive is a public format: a tar stream whose
sections standard tools can list and extract, and whose records every reader validates before it
trusts them.

A normal backup pauses each running domain in turn, waits for its intake and acknowledged work to
drain, then captures its committed configuration, WASM guest checkpoints, Kafka source offsets,
and branch lifecycle. It resumes that domain before transferring the captured sections into the
archive. Stopped domains need no pause. `WITHOUT PAUSE` captures the latest published state while
the domain runs and has crash-consistent rather than quiesced semantics; `WITHOUT STATE` captures
configuration only. Relay contents and materialized state are not included.

A restore recreates what an archive holds: every domain and user of a cluster archive in a fresh
cluster, whatever its nodes are named, or one domain beside the domains a cluster already has,
under its archived name or a new one.

## Backing Up

```nspl
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
- `TIMEOUT <duration>` bounds each running domain's quiesce wait. It is valid with the normal
  quiesced capture.
- The path names a file on the client's machine. A leading `~/` refers to the client user's home
  directory.

`BACKUP` runs in `nervix-cli` and in the Rust and C clients. It must be sent alone: not in a batch
with other statements and not while a transaction is open. The web console refuses it, because a
browser session has no file to write.

Each domain records the applied configuration revision and Raft log entry of its own capture.
Before reading state, each owner waits up to five seconds to apply the selected log revision,
brings its runtime plan current, and checks the sending leader again. An owner that cannot reach
the revision refuses capture. This also applies to stopped and live captures.
For a quiesced running domain, the leader holds a replicated domain mutation lease, pauses and
drains the domain across live nodes, asks state owners to publish and stage their checkpoints,
and reads the configuration while the cut is held. The leader resumes the domain and releases its
lease before copying staged state into the final archive. Other domains continue independently.
Each owner asks its active ingestor, reingestor, and processor supervisors to checkpoint their
current branch lifecycle, including branches created since the periodic snapshot. Capture retains
the immutable checkpoint published by each entity's replication handle; a handle awaiting its first
checkpoint contributes no lifecycle state. It seals those checkpoints and the current Kafka offsets
durably before opening one database snapshot that also
contains the already durable WASM guest saves. A stopped domain reads its stored checkpoints
without active task requests.
The drain uses the shutdown admitted-work view: active intake and generators, active source ACK
roots, relay and node buffers, and emitter buffers or publishes. Parked `REQUIRED WAIT` messages
are exempt. Once every node reports no admitted work, the leader requests a confirming force-flush
generation on every node and waits for its obligations to finish before asking owners to capture.
An unavailable sink keeps its publish and ACK counts outstanding until `TIMEOUT` expires; the
failed backup reports those counts and resumes the domain.
A leader-tenure change during a cut refuses the archive. The paused-domain recovery path releases
the old coordinator's lease and resumes the domain under the new leader. A client that retries the
same execution reference may complete a new cut under that leader.

The server renders each domain's models as NSPL, parses the rendered text back, and refuses the
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
server, `BACKUP_REFUSED` for a backup the server refused, and `WRITE_FAILED` for an archive that
could not be written to standard output.

## Downloading The Archive

The leader assembles the archive in its staging area and retains it under the backup's execution
reference. The client then downloads it through the session service's `DownloadBackup` streaming
call; [Sessions](sessions.md#backup-downloads) describes the call.

- The archive is retained until a download receives all of it, or until the retry validity of the
  backup's execution reference ends, 15 minutes by default; see
  [Command Completion](command-completion.md#lifecycle-and-ownership).
- A download is not bounded by the request timeout, because an archive may take far longer to
  transfer than a command takes to run. Each frame must arrive within the request timeout instead.
- A download that fails in transport starts again from the first byte. Running the backup again
  with the same execution reference returns its recorded outcome, and the client downloads the
  archive again while it is retained.
- The first download that receives the whole archive releases it. A later download of the same
  backup is refused, and a new backup is needed for another copy.
- Only the user who ran the backup may download its archive.
- The archive stays on the node that assembled it. A download sent to another node that does not
  hold it is redirected to the leader.
- A node that restarts loses the archives it retained, and their downloads are refused. If
  leadership moves while a backup runs, the new leader runs the backup again under the same
  execution reference and retains the archive it assembles.

## Inspecting An Archive

```nspl
DESCRIBE BACKUP './cluster.nvxb';
DESCRIBE BACKUP './cluster.nvxb' FORMAT JSON;
```

`DESCRIBE BACKUP` runs in `nervix-cli` against a file on its own machine and contacts no server. It
reads the whole archive and verifies every section's length and digest before it prints anything.
The description names the archive format, the Nervix and NSPL releases that wrote it, the cluster,
the capture time, the scope, whether resource bytes are included, and the users of a cluster
archive. For each domain it names the revision and Raft log entry the domain was read at, its
status, cut kind, pace and start count, the size and digest of its models, and each resource version with the
`root_checksum` and `manifest_checksum` that `DESCRIBE RESOURCE` prints for the same version.
It also inventories WASM guest saves by processor and branch fingerprint, Kafka positions by
topic and partition, and branch lifecycle records by processor and branch count. The inspection
does not print branch-key field values.

```text
backup: ./cluster.nvxb
format: 2
producer_version: 0.1.0
language_version: 0.1.0
cluster: nervix-3f1c
captured_at: 2026-09-27 12:00:00 UTC
scope: cluster
resources: included
users: default,operator
domains:
- domain=payments revision=812 raft_term=4 raft_index=812 status=RUNNING pace=unpaced start_version=2
  cut: quiesced
  models: bytes=4213 blake3=5d41402abc4b2a76b9719d911017c592…
  resource_versions:
  - resource=proto version=1 state=completed root_checksum=… manifest_checksum=… file_count=3 total_bytes=18204 archive_bytes=24576 created_by_node=node-1 created_at=2026-09-20 08:15:00 UTC archive=included blake3=…
```

An archive that fails verification is refused whole, with the section and the check that failed.

## Restoring

```nspl
RESTORE CLUSTER FROM '/var/backups/nervix/cluster.nvxb' ON EXISTING USER SKIP;
RESTORE DOMAIN payments FROM './payments.nvxb';
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
- `WITHOUT STATE` restores the configuration and purges state for the target domain.
- `WITHOUT SOURCE OFFSETS` restores WASM and branch lifecycle but starts sources without the
  archived Kafka positions.
- A state section whose entity is absent or whose schema fingerprint differs from the published
  schedule is skipped. The restore succeeds and reports a warning naming the skipped state in its
  command diagnostics and CLI output; that entity starts without the skipped checkpoint.
- A verified state record with an unknown kind tag or unsupported record version is also skipped
  with a warning. The archive reader still rejects malformed records of a supported kind, and
  always validates section lengths and digests.
- The path names a file on the client's machine. A leading `~/` refers to the client user's home
  directory.

A fresh cluster already has the user its configuration creates when the cluster starts, and a
cluster archive holds that user too, so restoring a cluster archive into a fresh cluster takes
`ON EXISTING USER SKIP` to keep the fresh cluster's password, or `ON EXISTING USER REPLACE` to take
the archived one.

`RESTORE` runs in `nervix-cli` and in the Rust and C clients. It must be sent alone: not in a batch
with other statements and not while a transaction is open. The web console refuses it, because a
browser session has no file to read.

### What A Restore Recreates

- **Users.** Each archived user, with its password hash exactly as the archive holds it.
- **Domains.** Each restored domain is created stopped, with its archived pace and placement
  default, its start count, and the point its latest start began from. `LIST DOMAINS` shows it
  with its archived pace and the status `STOPPED`. `START` becomes available after the complete
  state installation succeeds. A restore never starts a domain.
- **Resources.** Each resource the domain declared, and each completed version under its archived
  number, with its checksums, file count, sizes, creation time and creating node as
  `DESCRIBE RESOURCE` showed them in the source cluster. A number the source assigned to an upload
  that failed or was still installing when the backup read it stays a gap, and the next upload of
  the resource receives the number the source would have assigned next. See
  [Resource Versions](resource-versions.md#restored-versions).
- **Models.** Every model of the domain, bound to exactly the resource versions it was bound to in
  the source cluster. `SHOW CREATE` prints each restored model as it printed it in the source.

A restore stages the archive's compatible branch lifecycle, WASM guest checkpoints, and Kafka
source positions on every newly assigned owner and replica after its models are scheduled. Each
node validates the complete staged inventory, synchronizes its generation namespace, then
atomically selects it through one durably synchronized active-generation pointer. Nodes without assigned checkpoints publish an empty
set. In-memory state handles are cleared only after that publication. A replicated installation
gate prevents `START` until every target node has published the complete set; it survives failure,
lease release and node restart. A source uses its restored next offset when it starts, clamped to
the partitions its current source assignment contains. The domain remains stopped. Relays and
materialized state start empty.

### Publishing The State Generation

A node writes restored checkpoint bytes into an installation-specific namespace in chunks of at
most 64 KiB. A completed checkpoint has a revision, length and BLAKE3 digest and an authority-bound
receipt; incomplete chunks have no completed receipt. Guest saves stream directly from the staged
archive on the coordinator and from the sealed upload file on a remote node. Neither path assembles
a guest save in a second full-size buffer or reserves a file-read chunk inside an already reserved
full checkpoint.

Publication checks each receipt, checkpoint header and ordered chunk against the complete expected
inventory. It synchronizes those writes before committing the small pointer containing the exact
installation authority and inventory, then synchronizes that pointer before clearing runtime
handles or acknowledging publication. Cancellation or failure on either side of pointer publication
leaves the replicated start gate closed. An exact retry repeats pointer durability and cleanup;
it cannot select different counts or a different authority under the same generation.

Every read selects the pointer, checkpoint headers and chunks from one database snapshot. Readers
that opened a view before publication keep its complete prior set while obsolete keys are deleted.
Checkpoint jobs retain the namespace they selected before execution and compare it again under
the installation barrier; a queued job for another namespace cannot write into the restored set.
Normal checkpoint writes and replica installation use the selected namespace. Generation identity
is separate from a WASM branch's guest-state lifetime and its checkpoint revision.

Successful publication removes obsolete namespace keys and completed or abandoned receipts through
bounded deletion batches. A snapshot can retain the data it still reads after that deletion.
Within the selected namespace, chunks whose checkpoint was replaced by a normal inline write or
purged remain on disk until a later generation removes that namespace. Reclaiming these unreferenced
chunks belongs to the staging and checkpoint storage maintenance work.
Maintenance of failed unpublished installations remains a separate staging-lifecycle concern; it
never releases the start gate. Runtime storage requires its current format marker. A missing or
invalid marker in nonempty checkpoint storage fails with an instruction to recreate the node state
directory.

Local archive readers and received upload readers retain their staged artifact and disk quota through the storage job, including cancellation.

The streamed state-install and publication jobs reserve 2 MiB per node, independent of total guest
bytes and checkpoint count. Physical checkpoint placement encodings are limited to 60 KiB, leaving room for revision
and chunk coordinates within the database key limit. Archive verification, model planning, typed
lifecycle and offset conversion, database caches, and loading a guest when the domain starts retain
their own allocation bounds. Encoded lifecycle and offset staging reserves twice the encoded
payload size plus 2 MiB; it can be refused if that individual conversion does not fit the bulk
budget. The generation publisher has no aggregate-payload admission limit.

### Order Of Steps

A restore first receives the archive and plans everything it will do. Nothing changes before both
have finished:

1. **Receive.** The client streams the archive to the leader, which stages it in its staging area
   and checks that it is exactly the size and BLAKE3 digest the client declared.
2. **Verify.** The leader reads the whole archive and verifies its manifest, every section's length
   and digest, every record, and each resource version's bytes against the version's root
   checksum. It parses the `models.nspl` of every domain the archive holds, which must hold one
   `CREATE` of a model per statement.
3. **Plan.** The leader resolves every archived user under the user policy, checks that no
   restored domain name exists, pins every model to the resource versions the restore imports, and
   plans each domain's models with the transaction planner against the domain as the restore will
   create it. A dry run ends here and reports the plan.

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
      source offsets and WASM saves on the newly scheduled owners and replicas, then publish the
      complete set on every target node. Recording completion releases the replicated start gate.
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

A step that fails ends the restore. The steps before it stay applied, and the outcome names the
step and the reason, followed by the report of every step:

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
  reference. The Rust client does so when it is redirected or its connection fails. A step is
  recorded together with its effect, so no step is applied twice. Resuming an unfinished state
  installation admits a new generation. Local and remote staging and publication revalidate
  authority at the storage mutation boundary; a delayed request from the preceding coordinator
  cannot replace the completed generation or clear its runtime handles.
- If no client sends the archive to the new leader before the retry validity of the execution
  reference ends, 15 minutes by default, the new leader ends the restore as failed. The steps it
  recorded stay applied, and the outcome says how many.

The staged archive is released when its restore finishes, or when the retry validity of the
execution reference ends.

### Restoring From The Command Line

`nervix-cli restore` runs one restore and exits:

```sh
nervix-cli restore cluster --input cluster.nvxb --on-existing-user skip
nervix-cli restore domain payments --input payments.nvxb
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
| `--without-state` | Installs configuration without runtime checkpoints. |
| `--without-source-offsets` | Installs WASM and branch state, and leaves Kafka source positions unset. |
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

## Archive Format

An archive is a tar stream. Every entry is a regular file with owner-only permissions and a zero
modification time, and names longer than a tar header holds use GNU long-name entries. The manifest
is always the first entry, so a reader validates everything that follows against it as the stream
arrives.

| Section | Content | Present |
| --- | --- | --- |
| `manifest.rkyv` | The manifest record | Always |
| `users.rkyv` | The users record | Cluster archives |
| `domains/<domain>/domain.rkyv` | The domain record | Every domain |
| `domains/<domain>/models.nspl` | The domain's models as NSPL | Every domain |
| `domains/<domain>/resources/<resource>/<version>/version.rkyv` | One resource version record | Every resource version |
| `domains/<domain>/resources/<resource>/<version>/archive.tar` | The version's original upload | Published versions, unless `WITHOUT RESOURCES` |
| `domains/<domain>/state/wasm_processor/<processor>/<branch>/descriptor.rkyv` | Typed WASM checkpoint identity, schema fingerprint, generation and revision | Saved WASM branches unless `WITHOUT STATE` |
| `domains/<domain>/state/wasm_processor/<processor>/<branch>/guest.bin` | Raw guest save bytes | With each WASM descriptor |
| `domains/<domain>/state/kafka_offset/<ingestor>/offsets.rkyv` | Ingestor schema fingerprint and next offsets by topic and partition | Published Kafka domain offsets unless `WITHOUT STATE` |
| `domains/<domain>/state/branch_lifecycle/<kind>/<processor>/branches.rkyv` | Typed branch keys, incarnations and LRU order | Published branch lifecycle unless `WITHOUT STATE` |

A resource named `.` or `..` appears in a section path as `%2E` or `%2E%2E`.

A record section begins with a twelve-byte header: the magic `NVXBKREC`, then the record's kind and
its format version, each a little-endian 16-bit integer. The rest is an
[rkyv](https://rkyv.org) payload that a reader validates with bytecheck before it reads any value.

| Kind | Tag | Format version | Holds |
| --- | --- | --- | --- |
| Manifest | 1 | 2 | Archive format major version, producer and NSPL releases, cluster id, capture time, scope, whether resource bytes are included, each domain's revision, Raft log entry and cut kind, and each section's path, content kind, length and BLAKE3 digest |
| Users | 2 | 1 | Every user's name and password hash as a PHC string |
| Domain | 3 | 1 | Pace, default placement policy, lifecycle status, start count, the point the latest start began from, the committed clock mapping, the logical instant the clock had reached, and each declared resource with its next version number |
| Resource version | 4 | 1 | The version's domain, resource and number, its upload state, and for a published version its checksums, file count, sizes, creation time and creating node |
| WASM guest state descriptor | 5 | 1 | The saved branch key, schema fingerprint, guest state generation and checkpoint revision |
| Kafka domain offsets | 6 | 1 | The ingestor schema fingerprint and ordered topic and partition positions, expressed as the next offset to consume |
| Branch lifecycle | 7 | 1 | The owner kind, schema fingerprint, branch keys, last ingestion times and incarnations in LRU order |

The archive format major version is `2`. A reader refuses invalid record magic, an unsupported
manifest or required configuration record, a first entry other than the manifest, and any section
whose place, length or digest differs from what the manifest declares. Unknown optional state
record kinds and unsupported state record versions produce restore warnings and are skipped.

`models.nspl` holds one `CREATE` statement per model, separated by blank lines, in an order that
creates every model after each model its configuration names. Resource bindings name explicit
versions.

An archive holds password hashes, and its models and resources can hold credentials and other
secrets. Store it as a secret.

### Archive Fidelity Checks

The registered [archive properties](./property-testing-and-fuzzing.md) compare complete current
manifests, users, domain lifecycle and clocks, resource catalogs, WASM descriptors and guest saves,
Kafka offsets and ordered branch lifecycle entries. They exercise the production record validators,
manifest-first tar writer, streaming reader and restore-content reader, checking every section's
membership, order, length, digest and bytes. Re-exporting verified extracted values produces the
same archive bytes. Canonical NSPL documents also reparse to their complete ordered Models across
multiple domains. Synthetic payloads cover empty data, tar block boundaries and long section paths;
entry permissions remain owner-only.

Separate malformed-input targets check current record headers and fields, inconsistent metadata,
missing or reordered sections and truncated streams before verified contents reach installation.
Schema agreement with the restored schedule remains the restore planner's contract, exercised by
public restore scenarios. The one-node and three-node WASM/Kafka restore scenario compares every
archived field and raw resource/guest byte before the restored domain starts, then proves subsequent
START behavior and two-branch isolation. It asserts domain renaming, the stopped lifecycle and the
new guest lifetime explicitly; capture metadata belongs to each new backup. These representation
checks do not establish capture-fence or installation-ordering correctness, which retain their
production-owner concurrency and recovery evidence.

## Limits

| Limit | Value |
| --- | --- |
| One record section | 64 MiB |
| One archive | 64 GiB, the largest artifact a node stages |
| Archives and snapshot transfers a node stages at once | 128 GiB |
| Download chunk | 256 KiB |
| Frames a download queues ahead of its client | 4 |
| Retention | Until downloaded, or the retry validity of the execution reference ends |
| Restore chunk | 256 KiB |
| Restore frames the Rust client queues ahead of the transport | 8 |
| Restore archive retention | Until the restore finishes, or the retry validity of the execution reference ends |
| State section staging on an owner | Charged to the same node staging quota until fetched or expired |
| Streamed guest installation and complete generation publication per node | Fixed 2 MiB bulk working-memory reservation; 64 KiB checkpoint chunks |
| Encoded lifecycle and offset staging per node | Twice the encoded record size plus 2 MiB |
| Physical checkpoint placement encoding | 60 KiB, including domain and installation namespace |

A backup larger than the staging quota fails. A backup that fits waits while the leader's staging
area is full, until retained archives are downloaded or expire and snapshot transfers finish.

A restore stages its archive under the same limits, and is refused rather than kept waiting when
the archive is larger than one archive may be, or the leader's staging area cannot hold it now; send
it again once retained archives are released and snapshot transfers finish. A restore's model batch
is not bounded by the statement and source-byte limits of a transaction. Each node must admit the
bounded installation or publication job within its bulk working-memory budget (32 MiB by default).
A state set may exceed that budget. An individual job that cannot be admitted fails with the start
gate still closed.

## Failures

A failed backup reports `backup failed:` and the reason, and its message never includes archive
contents. The reasons are:

- the named domain does not exist, or no domain is selected for `BACKUP DOMAIN`
- a domain's committed models do not form a valid graph, do not render as NSPL, or do not parse
  back to themselves
- a domain's clock mapping cannot be read at the capture time
- a resource version's bytes are not installed on the leader, or do not match their catalog entry
- the archive is larger than one archive may be, or a record does not encode
- a quiesced domain cannot pause, drain, capture its owners or resume within its timeout

A refused restore reports `restore refused:` and the reason, and changed nothing. The reasons are:

- the stream did not carry the archive its start declared: its chunks add up to another size, or
  its bytes have another digest
- the leader's staging area cannot hold the archive now, or the archive is larger than one archive
  may be
- the archive does not verify: the section and the check that failed are named
- a restored domain's `models.nspl` does not parse, naming the domain and the line, or holds a
  statement that creates no model, naming the domain, the statement's number and its line
- `RESTORE CLUSTER` was given a domain archive, or `RESTORE DOMAIN` names a domain the archive does
  not hold
- a restored domain name exists
- an archived user exists and the user policy is `FAIL`
- the archive was taken `WITHOUT RESOURCES` and holds completed resource versions, or holds a
  version's bytes that do not match its root checksum
- a model binds a resource version the restore does not import as completed
- a domain's models do not form a valid configuration, as the transaction planner finds

A restore that failed at a step reports `restore failed at step '<step>':`, the reason, and that the
steps before it stay applied, together with the report of every step. The reasons are a consensus
refusal of the step, a resource version that could not be installed or completed on every live
node, and a model batch the leader refused, such as lookup data that does not load at its path or
TLS material that does not load. A restore whose archive no client sent to a new leader before the
retry validity of its execution reference ended reports `restore stopped after the steps it
recorded`. No message includes password hashes or resource bytes.
