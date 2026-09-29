# Backup And Restore

A backup writes the configuration of a whole cluster, or of one domain, into one archive file on the
machine of the client that ran it. The archive holds each domain's committed models as NSPL, each
domain's lifecycle and clock, its resource catalog with the original bytes of every resource
version, and, for a cluster backup, every user. The archive is a public format: a tar stream whose
sections standard tools can list and extract, and whose records every reader validates before it
trusts them.

A backup copies configuration only. It records no relay contents, no materialized state, no WASM
guest state and no connector positions, and it takes no domain lease: every domain keeps running
while it is read.

## Backing Up

```nspl
BACKUP CLUSTER TO '/var/backups/nervix/cluster.nvxb';
BACKUP DOMAIN payments TO './payments.nvxb';
BACKUP DOMAIN TO './current-domain.nvxb' WITHOUT RESOURCES;
```

- `BACKUP CLUSTER` covers every domain and every user.
- `BACKUP DOMAIN <name>` covers one domain. Without a name it covers the session's selected domain.
  A domain backup holds no users.
- `WITHOUT RESOURCES` records every resource version with its checksums and sizes, and leaves out
  the version's bytes.
- The path names a file on the client's machine. A leading `~/` refers to the client user's home
  directory.

`BACKUP` runs in `nervix-cli` and in the Rust and C clients. It must be sent alone: not in a batch
with other statements and not while a transaction is open. The web console refuses it, because a
browser session has no file to write.

The server reads everything a backup holds from one applied revision of the replicated
configuration. A model change committed while the backup runs is either in every part of the
archive or in none of it, and the archive records that revision and the Raft log entry it was
applied from for each domain.

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
nervix-cli backup domain --output - > current-domain.nvxb
nervix-cli backup cluster --output cluster.nvxb --format json
```

| Argument | Meaning |
| --- | --- |
| `cluster` or `domain [NAME]` | What the archive covers. `domain` without a name covers `--domain`. |
| `--output PATH` | Where the archive is written. `-` writes it to standard output. |
| `--without-resources` | Leaves resource version bytes out of the archive. |
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
    { "domain": "payments", "revision": 812, "sections": 7, "section_bytes": 48190112 }
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
status, pace and start count, the size and digest of its models, and each resource version with the
`root_checksum` and `manifest_checksum` that `DESCRIBE RESOURCE` prints for the same version.

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
  models: bytes=4213 blake3=5d41402abc4b2a76b9719d911017c592…
  resource_versions:
  - resource=proto version=1 state=completed root_checksum=… manifest_checksum=… file_count=3 total_bytes=18204 archive_bytes=24576 created_by_node=node-1 created_at=2026-09-20 08:15:00 UTC archive=included blake3=…
```

An archive that fails verification is refused whole, with the section and the check that failed.

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

A resource named `.` or `..` appears in a section path as `%2E` or `%2E%2E`.

A record section begins with a twelve-byte header: the magic `NVXBKREC`, then the record's kind and
its format version, each a little-endian 16-bit integer. The rest is an
[rkyv](https://rkyv.org) payload that a reader validates with bytecheck before it reads any value.

| Kind | Tag | Format version | Holds |
| --- | --- | --- | --- |
| Manifest | 1 | 1 | Archive format major version, producer and NSPL releases, cluster id, capture time, scope, whether resource bytes are included, each domain's revision and Raft log entry, and each section's path, content kind, length and BLAKE3 digest |
| Users | 2 | 1 | Every user's name and password hash as a PHC string |
| Domain | 3 | 1 | Pace, default placement policy, lifecycle status, start count, the point the latest start began from, the committed clock mapping, the logical instant the clock had reached, and each declared resource with its next version number |
| Resource version | 4 | 1 | The version's domain, resource and number, its upload state, and for a published version its checksums, file count, sizes, creation time and creating node |

The archive format major version is `1`. A reader refuses a record whose magic, kind or format
version it does not know, a manifest of another major version, a first entry other than the
manifest, and any section whose place, length or digest differs from what the manifest declares.

`models.nspl` holds one `CREATE` statement per model, separated by blank lines, in an order that
creates every model after each model its configuration names. Resource bindings name explicit
versions.

An archive holds password hashes, and its models and resources can hold credentials and other
secrets. Store it as a secret.

## Limits

| Limit | Value |
| --- | --- |
| One record section | 64 MiB |
| One archive | 64 GiB, the largest artifact a node stages |
| Archives and snapshot transfers a node stages at once | 128 GiB |
| Download chunk | 256 KiB |
| Frames a download queues ahead of its client | 4 |
| Retention | Until downloaded, or the retry validity of the execution reference ends |

A backup larger than one archive may be fails. A backup that fits waits while the leader's staging
area is full, until retained archives are downloaded or expire and snapshot transfers finish.

## Failures

A failed backup reports `backup failed:` and the reason, and its message never includes archive
contents. The reasons are:

- the named domain does not exist, or no domain is selected for `BACKUP DOMAIN`
- a domain's committed models do not form a valid graph, do not render as NSPL, or do not parse
  back to themselves
- a domain's clock mapping cannot be read at the capture time
- a resource version's bytes are not installed on the leader, or do not match their catalog entry
- the archive is larger than one archive may be, or a record does not encode
