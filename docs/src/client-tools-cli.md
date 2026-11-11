# Command Line Client

*Plexus, the Nervix graph definition language.*

`nervix-cli` is the interactive Plexus client. It is installed alongside `nervix-server`
([Cargo Install From GitHub](installation-cargo.md)) and is also present in the container image
([Docker](installation-docker.md)).

## Connecting

With no arguments the client connects to `http://127.0.0.1:47391` as user `default` and prompts for
the password:

```bash
nervix-cli
```

```text
nervix-cli connected to http://127.0.0.1:47391
Type 'exit' to quit. Trailing ';' is optional.
[events] notifications are printed above the prompt

nervix[default]>
```

Point it at another cluster with `--server`, which takes a full URL. An `https://` URL enables TLS:

```bash
nervix-cli --server https://nervix.example.com:47390 --tls required --tls-ca-cert ./ca.pem
```

The server presents the certificate configured by its `--grpc-tls-cert` and `--grpc-tls-key`
options in `--grpc-mode https`. Provision the certificate for the hostname in `--server`, and give
the CLI its issuing CA. [Native session TLS](installation-cargo.md#native-session-tls) shows the
server options and their environment variables.

## Options

| Option | Environment variable | Default | Meaning |
| --- | --- | --- | --- |
| `--server <URL>` | | `http://127.0.0.1:47391` | session gRPC endpoint; the scheme selects TLS |
| `--tls <preferred\|required>` | | `preferred` | `required` refuses to connect over a non-`https` URL |
| `--tls-ca-cert <PATH>` | | | PEM certificate authority used to verify the server |
| `--dns-resolver-config <PATH>` | `NERVIX_DNS_RESOLVER_CONFIG` | `/etc/resolv.conf` | resolver configuration for native hostname connections |
| `--dns-hosts-file <PATH>` | `NERVIX_DNS_HOSTS_FILE` | `/etc/hosts` | hosts file consulted before DNS |
| `--dns-name-server <ADDRESS:PORT>` | `NERVIX_DNS_NAME_SERVERS` | from resolver configuration | replacement name server; repeatable or comma-separated |
| `--domain <NAME>` | | `default` | domain the session starts in |
| `--username <NAME>` | `NERVIX_USERNAME` | `default` | registry user |
| `--password <PASSWORD>` | `NERVIX_PASSWORD` | | prompted interactively when unset |
| `--command <Plexus>` | | | run statements once and exit |
| `--suggest <Plexus>` | | | print completion candidates as JSON and exit; cannot be combined with `--command` |
| `--cursor <OFFSET>` | | end of input | UTF-8 byte offset in `--suggest` input |
| `--execution-reference <REFERENCE>` | | | with `--command` or a node administration subcommand, send the command under the execution reference the CLI printed for an earlier uncertain run of it, which recovers that run's outcome |
| `--upload-identity <IDENTITY>` | | | with a `--command` that is one `UPLOAD RESOURCE`, send it under the upload identity the CLI printed for an earlier uncertain run of it, which recovers that run's outcome |
| `--backup-wait-timeout <DURATION>` | `NERVIX_CLI_BACKUP_WAIT_TIMEOUT` | `10m` | overall wait of one `BACKUP` command across its cuts, redirects and reconnects, from one millisecond through 24 hours; applies to `backup`, `--command` and the REPL |

There is no general CLI configuration file and no `--version` flag. `--server`, `--domain`, and the
TLS options have no environment-variable equivalents. The DNS options select the resolver used by
the native session; the system resolver and hosts files are the defaults, and
[Name Resolution](name-resolution.md) describes how that resolver answers. The password can come
from `NERVIX_PASSWORD` so it need not appear in shell history.

The table above is the short version. [nervix-cli Reference](nervix-cli-reference.md) is printed by
the binary itself while this book is built, so it is the authoritative list of every option and
subcommand for this release.

## Connection Capabilities

The native client's local capabilities are available in the REPL, one-shot commands and
subcommands. Every option below has the environment form `NERVIX_CLI_` followed by its uppercase
name with underscores. The typed native client definitions supply both help and runtime defaults.

| Option | Default | Meaning |
| --- | --- | --- |
| `--connect-timeout` | 10 s | deadline of each connection attempt |
| `--request-timeout` | 120 s | deadline of each request |
| `--retry-timeout` | 120 s | total request recovery deadline across redirects and retries |
| `--backup-wait-timeout` | 10 min | total wait for a backup, including recovery |
| `--retry-backoff` | 100 ms | initial reconnect/recovery delay |
| `--retry-max-backoff` | 1 s | ceiling for doubled retry delays |
| `--subscription-record-capacity` | 32 | events retained per subscription |
| `--subscription-retained-frames` | 1 | frame-limit units of bytes retained per subscription, including each event's header |
| `--subscriptions-retained-in-full` | 4 | multiplier for the aggregate subscription record and byte bounds |
| `--upload-frame-capacity` | 8 | queued outbound resource-upload frames |
| `--restore-frame-capacity` | 8 | queued outbound restore frames |

Durations accept one millisecond through 24 hours. The backoff base cannot exceed its ceiling.
Capacities must be positive, fit the native channel bound and produce addressable aggregate byte
and record limits. Validation happens before connection or credential prompting. Increasing a
capacity permits more retained memory; it does not change wire-frame limits or server policy.
Overflow still ends only the affected subscription with `ConsumerOverflow`, preserving other events.

For a slow connection, for example:

```bash
nervix-cli --connect-timeout 30s --request-timeout 3m --retry-timeout 5m --retry-max-backoff 2s
```

## Modes

The client runs in one of these modes. A subcommand takes precedence over the other modes:

1. **A subcommand**, such as `subscribe`, `domain-clock`, `backup`, `restore`, or `drain-node`.
2. **`--suggest`**, which prints completion candidates as JSON and exits; see [Completion](#completion).
3. **`--command`**, which submits Plexus, prints the result, and exits. A single supported
   inspection with `FORMAT JSON` reserves stdout for its typed JSON report; see
   [Session And Transaction Statements](#session-and-transaction-statements).
4. **The interactive REPL**, when none of the above is given.

There is no file-execution mode. To run a saved script, pass it through `--command`; to format
one, use [`nervix-plexus-format`](client-tools-plexus-format.md):

```bash
nervix-cli --domain quickstart --command "$(cat pipeline.plexus)"
```

## Exit Status

The CLI ends with one of these statuses. It decides the status from the typed outcome it received,
never from the text it printed:

| Status | Meaning |
| --- | --- |
| `0` | Everything completed: every statement of `--command`, the subcommand, or the REPL the operator left with `exit`, `quit`, `Ctrl-D` or `Ctrl-C` at the prompt. |
| `1` | It failed: the server reported that a statement failed, a subscription or an upload was refused, a statement was refused before it was sent, or the CLI could not connect, authenticate, or obtain an answer to a request that changed nothing. |
| `2` | The arguments were invalid. |
| `3` | An outcome did not arrive: a command or an upload may have taken effect and its outcome is not known yet, or a completed `BACKUP` did not download its archive. The CLI printed the execution reference or upload identity that recovers it; see [Uncertain Outcomes](#uncertain-outcomes). |
| `130` | Ctrl-C cancelled the command before the server admitted it, so it changed nothing; see [Interrupting A Statement](#interrupting-a-statement). |

A command of several statements takes the status of its worst statement: an unknown outcome over a
failure, and a failure over a completion. The REPL reports each statement's outcome as it ends and
keeps running whatever the outcome, so its own status only says how the session ended.

## Interrupting A Statement

Ctrl-C while a statement runs interrupts that statement, in the REPL, in `--command`, and in the
node administration subcommands. For a command the server runs, the CLI asks the server to cancel
it and reports what the server decided:

- When the server had not admitted the command yet, it never will, and nothing happened:

  ```text
  cancelled: the command was not admitted, so it changed nothing
  ```

  `--command` then exits with status `130`.
- When the server had already admitted it, the command keeps running and only the wait for it ends.
  Its outcome is not known yet, as [Uncertain Outcomes](#uncertain-outcomes) describes, and
  `--command` exits with status `3`.
- When the command finished before the cancellation took hold, the CLI prints its outcome as usual.

A second Ctrl-C stops waiting for the server's answer, which leaves the outcome not known. `UPLOAD
RESOURCE` and `RESTORE` stream their archive on a call of their own, which a cancellation cannot
reach, so Ctrl-C stops sending it; the server may already have received the whole archive, so the
outcome is not known. A statement the client serves itself, such as `LIST DOMAINS` or `CREATE
SUBSCRIPTION`, stops waiting at once and is reported as interrupted with status `3`, because it may
still take effect, as a subscription that is opening does. Ctrl-C at the prompt, with no statement
running, leaves the REPL.

## Uncertain Outcomes

A command's outcome is not known yet when the server reports that it admitted the command without
an outcome, when the client's retry deadline passes before the answer arrives, when the session that
carried it was lost and no new one could recover the answer, or when Ctrl-C ended the wait for an
admitted command. An admitted command keeps running, so its effect may still happen. The CLI prints
the cause and the execution reference that recovers it:

```text
error: command outcome for execution '01a1f3c2-7e4b-7a10-9c3d-5b2e8f6a1d00' is uncertain: the server cancelled the command request (AfterAdmission)
outcome: not known yet; the command may have been admitted. Run it again with --execution-reference 01a1f3c2-7e4b-7a10-9c3d-5b2e8f6a1d00 to recover its outcome.
```

Sending a command again under the same execution reference never applies it twice: the server
returns the outcome it recorded, or the outcome of the admitted command once it finishes. With
`--command`, pass the reference back:

```bash
nervix-cli --domain quickstart --command "CREATE RESOURCE orders_model;" \
  --execution-reference 01a1f3c2-7e4b-7a10-9c3d-5b2e8f6a1d00
```

In the REPL, running the same statement again in the same domain repeats it under its reference:

```text
nervix[quickstart]> CREATE RESOURCE orders_model;
^C
error: command outcome for execution '01a1f3c2-7e4b-7a10-9c3d-5b2e8f6a1d00' is uncertain: the server cancelled the command request (AfterAdmission)
outcome: not known yet; the command may have been admitted. Run the same statement again to recover its outcome under execution reference 01a1f3c2-7e4b-7a10-9c3d-5b2e8f6a1d00.
nervix[quickstart]> CREATE RESOURCE orders_model;
repeating the uncertain statement under execution reference 01a1f3c2-7e4b-7a10-9c3d-5b2e8f6a1d00
created resource 'orders_model'
```

The REPL keeps only the last statement whose outcome was not known, and repeats it when the next
statement with the same text runs in the same domain; once that statement's outcome is known, the
next run of it is a new command. A reference identifies one command: sending it with other text is
refused as an execution reference conflict, and a reference past the server's retry validity is
refused as expired, after which its outcome can no longer be recovered. An upload is recovered by
its upload identity instead, as [Session And Transaction Statements](#session-and-transaction-statements)
describes.

## The Interactive REPL

Inspect a configured connector client in the current domain with `DESCRIBE CLIENT warehouse;`.
It reports effective configuration, marks omitted declared defaults with `(default)`, and redacts
confidential values. `SHOW CREATE CLIENT warehouse;` renders the authored declaration.

### Prompt And Multi-Line Input

The prompt shows the active domain, and continuation lines are marked with dots:

```text
nervix[quickstart]> CREATE SCHEMA order_record (
....[quickstart]>   order_id STRING,
....[quickstart]>   amount I64
....[quickstart]> );
```

Input accumulates until the buffered statements parse or the line ends with `;`, so a trailing
semicolon is optional on a complete statement and a partial one simply keeps buffering. While a
transaction is open the prompt says so:

```text
nervix[quickstart tx]>
```

After `COMMIT` is accepted, an in-progress commit is shown as
`nervix[quickstart committing]>`. The prompt is driven by replicated transaction status rather
than a local boolean.

### Completion

`Tab` completes. Suggestions are computed by the server for the exact cursor position, so they
cover grammar keywords and the identifiers that actually exist: models in the active domain,
resource names and versions, session subscription names, and domain names. Inside a quoted path
that names a file or directory on this machine, such as `UPLOAD RESOURCE ... VERSION '<path>'`,
`BACKUP ... TO '<path>'`, `DESCRIBE BACKUP '<path>'`, and `RESTORE ... FROM '<path>'`, the client
completes local filesystem paths instead, expanding `~` to your home directory.

The replacement covers the current word even when the cursor is inside it, and keeps the text
after that word. The CLI reads successive bounded pages when the server has more matches. A
missing or stale domain or transaction context does not produce candidates from a different
configuration.

Multi-word creation heads are offered as one item, such as `PACED DOMAIN`, `WIRE JSON SCHEMA`,
`SIGNALING PROTOCOL`, `WINDOW PROCESSOR`, and `WASM PROCESSOR`. This also applies after
`CREATE IF NOT EXISTS`. Once part of a head is written, completion continues from that position;
after its name, suggestions follow that kind's body.

Connector completion follows the selected entity's declaration: delivery modes, options,
envelope clauses, property keys and client configuration keys stay specific to that entity.
Expression positions offer ordinary functions, ingestor expressions also offer the header reads
their entity supports, and emitter `INVOKE` offers header writes only for an entity that supports
them. This includes ingestor `FILTER WHERE`, emitter input `WHERE`, HTTP `METHOD` and `PATH`,
and constructions inside `ALTER`. After `metadata.`, an ingestor offers its declared metadata
fields. Suggestions come from the grammar at the cursor, so a duration or mode position does
not offer expression functions.

Codec and relay names follow what the statement can accept, by the same rules the web console's
forms apply to their choices. After `DECODE USING` only codecs that decode are offered, and after
`ENCODE USING` only codecs that encode. A relay is offered where it agrees with the relays the
statement has already named: after `CREATE JUNCTION copy FROM readings,` only relays with the
schema and branch of `readings` are suggested, and a junction's `TO` and `ON MESSAGE ERROR SEND TO`
offer only relays on that branch. The first relay of a `FROM` list offers every relay, since the
clauses that would narrow it come after it.

Scripts can request the same candidates with `--suggest 'DROP SCHEMA ord' --cursor 15`.
The cursor is a UTF-8 byte offset; omitting it selects the end of the input. The CLI prints JSON
containing a status and each candidate's display value, kind, and exact text edit.

While a transaction is open, completion describes the configuration that transaction is building:
models and resources its queued statements create are suggested before `COMMIT`, and a model whose
`DROP` is queued stops being suggested until a later statement recreates it. Only the session bound
to the transaction sees them; every other session is offered committed configuration alone. A
queued resource has no versions to suggest, because `UPLOAD RESOURCE` is not transaction content.

### History

Submitted lines are stored in `.nervix_client_history`, relative to the directory the client was
started in, capped at 200 entries. `ArrowUp` and `ArrowDown` walk it.

### Diagnostics

Server-reported errors are rendered as annotated reports against the text you submitted, with the
offending span highlighted in place rather than described by offset.

### Asynchronous Output

Subscription deliveries, server notifications, and state changes and ticks of an attached clock arrive
independently of the prompt and are printed above it:

```text
[events] subscription [watch] from [orders]: {"order_id":"o-1001","amount":1500}
[events] runtime ERROR: emitter 'redis_orders' publish failed
[events] leadership INFO: raft transition: state=Follower leader=node-2 term=3 last_log_index=41 last_applied=41
[events] membership INFO: node 'node-3' left
[events] membership INFO: node 'node-3' joined (incarnation 1793845627114630656)
[events] domain clock [simulation]: generation 2, stopped
[events] domain clock [simulation] tick: generation 3, id 12, boundary 2030-01-01T00:00:01.100Z, authority UTC 2026-09-27T00:00:00Z, node logical 2030-01-01T00:00:01.120Z
```

A server notice prints as `[events] <kind> <level>: <message>`, labelled by the kind the server
sent with it: `membership` when the connected node sees a node join, leave or restart as a new
incarnation, `leadership` for that node's Raft transitions and the membership operations it
proposes, `cluster parameters` when it installs a committed parameter revision, `runtime` for a
runtime failure of any node, `control plane` for a control-plane failure of the connected node,
and `licensing` for a licensing lifecycle observation or policy warning.
The level is `INFO`, `WARN` or `ERROR`.

The carried kind selects this label; the message is display text. A configuration mutation or a
subscription-interest update alone produces no membership line. Command completion is reported
through that command's outcome, independently of the notice stream; see
[What A Session Is Told](./client-session-protocol.md#what-a-session-is-told).

A domain clock line follows each newer state or accepted tick after the attach reply. A state or
tick that changes again before it is delivered or read is replaced by the newest one, so a slow
client can skip states and tick ids. The REPL queues at most 128 event lines, each
cut at 8 KiB with `… [line truncated]`; lines that arrive while it is full are dropped, and
`[events] notice: <n> event lines were omitted because terminal output could not keep up` reports
how many. When the server ends an attachment, or the session holding it is interrupted and the clock
is attached again on the next session, the line reads `[events] domain clock [<domain>] notice: ...`
with the reason.

While the prompt waits for input, events appear above it without a keypress. The text being
edited, the continuation of a multiline statement, completion and history search remain available.
While a statement runs, events stay in the bounded queue and print when the prompt resumes;
event readers never wait for the terminal. An overflow is reported when output next drains,
including when no later event arrives.

Ordinary `--command` runs do not print asynchronous subscription, clock or server-notice lines.
The single transaction-inspection JSON mode sends unrelated events to stderr, keeping stdout for
the report. Use `subscribe` or `domain-clock` for a stream that remains open after its initial
request.

Output keeps printing across reconnects. When the session is lost, the next statement you run opens
a new one; the client also opens one at once while a subscription or a followed clock waits to be
restored. Server notices then resume with the new session, rows arrive from every subscription
restored on it or opened after it, and a followed clock prints its state again once it is attached
there. When the client cannot open a session within its 120-second retry deadline, a line names
what is waiting and the whole chain of why, and the client keeps trying:

```text
[events] notice: subscription events could not resume yet: failed to connect to server: transport error: tcp connect error: tcp connect error: Connection refused (os error 111); the client keeps trying
```

After a reconnect the CLI opens every subscription and attaches every clock again. When the new
session refuses one for a reason a later attempt can overcome, a notice line reports why, the
server's message and when the next attempt follows:

```text
[events] subscription [watch] notice: opening the subscription again failed because the serving node could not open it now: failed to subscribe to relay 'orders': it was redefined while the subscription was being created; subscribe again; the next attempt follows in 2s
[events] domain clock [simulation] notice: attaching the clock again failed because the session holds a transaction: session-scoped and client-local statements cannot be queued in a transaction; the next attempt follows in 2s
```

A subscription whose relay was removed or redefined while the CLI had no session ends instead of
being tried again, with the same last line as when the server ends it, and a followed clock whose
domain no longer exists ends with its own end line.

`DELETE SUBSCRIPTION` of a subscription no open session holds, because its session ended or the new
session refused to open it again, completes at once and frees the name.

When the server ends a subscription because its relay was redefined or removed, the subscription's
last line says why:

```text
[events] subscription [watch] notice: the subscription ended because its relay no longer exists: session subscription 'watch' ended because relay 'orders' in domain 'quickstart' no longer exists
```

A redefined relay reads `because its relay was redefined` instead.

The CLI never opens an ended subscription again, including after a reconnect. `CREATE SUBSCRIPTION`
under the same name opens it again under the relay's current definition, and `DELETE SUBSCRIPTION`
of it completes at once and frees the name.

If server notices arrive faster than the CLI reads them, the client drops the ones it held and
`[events] notice: server notices were dropped because they arrived faster than they were read`
marks the gap; the notices after it keep printing.

### Leaving

`exit`, `quit`, `Ctrl-D`, or `Ctrl-C` at the prompt. Ctrl-C while a statement runs interrupts that
statement instead, as [Interrupting A Statement](#interrupting-a-statement) describes. A statement
that fails, is refused or leaves its outcome not known never ends the REPL: it prints the outcome,
or the whole chain of why the request failed, and draws the next prompt.

## Session And Transaction Statements

Some statements affect the client session or replicated transaction state rather than directly
applying one model change:

| Statement | Effect |
| --- | --- |
| `USE <domain>` | switch the session's active domain |
| `LIST DOMAINS` | list domains with pace and status |
| `BEGIN` / `COMMIT` / `REVERT` | open, apply, or discard a replicated transaction on the leader |
| `DESCRIBE TRANSACTION ['<id>'] [OPERATION <n>] [FORMAT TEXT \| JSON]` | read the attached transaction, or one named by id, without changing it |
| `DESCRIBE INFERENCER <name>` | inspect the active domain's model binding, tensor declarations and state summary from its execution owners |
| `UPLOAD RESOURCE <name> VERSION '<dir>'` | stream a local directory as a new version of that resource in the active domain |
| `BACKUP CLUSTER TO '<file>'` / `BACKUP DOMAIN [<name>] TO '<file>'` | back up configuration, users and resources into a local archive file |
| `DESCRIBE BACKUP '<file>' [FORMAT TEXT \| JSON]` | read and verify a local archive without contacting a server |
| `RESTORE CLUSTER FROM '<file>'` / `RESTORE DOMAIN <name> [AS <new_name>] FROM '<file>'` | recreate users, domains, resource versions and models from a local archive file |
| `CREATE SUBSCRIPTION` / `DELETE SUBSCRIPTION` | start and stop a read-only relay subscription |
| `ATTACH DOMAIN CLOCK` / `DETACH DOMAIN CLOCK` | start and stop following the active domain's clock |

`USE`, `LIST DOMAINS`, `UPLOAD RESOURCE`, `BACKUP`, `DESCRIBE BACKUP`, `RESTORE`, and the domain
clock statements must be submitted on their own, and never inside a transaction. `ATTACH DOMAIN CLOCK` prints the clock the serving node
has installed, then each change above the prompt as described in
[Asynchronous Output](#asynchronous-output):

```text
nervix[simulation]> ATTACH DOMAIN CLOCK;
attached to the clock of domain 'simulation': generation 1, paced: period 1s, skew 100ms, logical origin 2030-01-01T00:00:00Z, UTC anchor 2026-09-27T09:30:00.125Z, time rate 2
```

A second attach is refused because the session already follows that clock, and a detach without an
attachment is refused because it does not. `USE` does not detach a clock, so the REPL can follow the
clocks of several domains at once, and the CLI attaches the clocks it follows again after a
reconnect. See [Domain Clock Attachment](sessions.md#domain-clock-attachment). Read-only statements,
subscriptions, `CREATE DOMAIN`, `CREATE USER`, and node administration are also rejected while
queueing transaction content. `DESCRIBE TRANSACTION` is the exception: while a transaction is open
it reads that transaction's impact report without queueing anything or consuming an operation
number, so the next queued statement keeps the number it would have had. It must be submitted on its
own. With an id it reads another transaction of the same user and leaves the prompt, the active
domain, and the attached transaction as they were:

```bash
nervix-cli --domain production --command \
  "DESCRIBE TRANSACTION '01a0ca64-7062-7302-ac1e-43138ccc2067' OPERATION 2 FORMAT JSON;"
```

For a standalone `--command` inspection with `FORMAT JSON`, stdout contains exactly one JSON
document serialized from the typed report, without terminal decoration or unrelated events.
A state-read refusal prints its typed envelope with `rejection` and exits nonzero.
Other failures print one JSON object with `error.code` and `error.message` on stdout and exit
nonzero; a configuration, connection or request failure's message is the whole chain of causes,
such as `invalid server URL: relative URL without a base`.
Diagnostic details and unrelated events go to stderr. `FORMAT TEXT` uses normal readable terminal
output. Transaction inspection is complete; materialized relay, stateful processor and WASM
processor state inspections are bounded and carry an opaque continuation. See
[Inspect Materialized State](./relay.md#inspect-materialized-state) for selector clauses, typed
counts, row limits and stale-token handling,
[Inspect Held Rows](./processors.md#inspect-held-rows) for reorderer and correlator rows, and
[WASM State And Recovery](./wasm-state.md#observability) for WASM checkpoint facts. Standalone JSON
mode accepts `DESCRIBE TRANSACTION`, `SHOW RELAY ... MATERIALIZED STATE`,
`SHOW REORDERER ... STATE`, `SHOW CORRELATOR ... STATE`, `SHOW DEDUPLICATOR ... STATE`,
`SHOW WINDOW PROCESSOR ... STATE` and `SHOW WASM PROCESSOR ... STATE` with `FORMAT JSON`.
After a stale-preview `COMMIT` refusal, run `DESCRIBE TRANSACTION` for the
attached transaction before retrying the commit so the client fences it to the newly reviewed
planning basis.
See [Transaction Quiescence And Impact Inspection](./transaction-quiescence.md) for the report's
planned and actual scopes, historical topology, and diagnostic meanings.

`BEGIN` requires an existing active domain and binds the transaction to it; attaching a transaction
switches the active domain to the transaction's domain. An upload targets the active domain,
renders live progress, and finishes after every current live node incarnation has verified and
installed the assigned version:

```text
upload resource 'order_model' finished: 4.2 MiB sent, installed on every live node
uploaded resource version 1
```

See [Resources](resources.md#lifecycle) for what a resource version contains.

The CLI gives each upload one upload identity, which every call it opens for that upload carries.
When an upload's outcome is not known, because its call failed, passed its deadline or was
interrupted after the server may have received the whole archive, the CLI prints the identity that
recovers it:

```text
upload outcome: not known yet; the server may have installed it. Run it again with --upload-identity 01a1f3c4-0b6e-7c21-8d4a-2f9e6c3b7a10 to recover its outcome.
```

Sending the same directory again under the same identity installs nothing twice: it joins the
installation that is still running, or reports the version the earlier call installed. In the REPL,
running the same statement again in the same domain repeats it under its identity; with
`--command`, pass the identity back with `--upload-identity`. The directory must still hold the
same files, because the identity is bound to the archive its first complete call sent. `UPLOAD
RESOURCE` is refused while a transaction is open, because the version it would install would not
belong to the transaction, and `REVERT` could not take it back:

```text
upload resource 'order_model' failed after sending 0 B
error: client-local commands are not allowed while a transaction is active
```

Use `--command` for an atomic resource rebinding just like any other server statement:

```bash
nervix-cli --domain production --command \
  "REBIND RESOURCE order_model TO VERSION LATEST FOR INFERENCER score_orders;"
```

The command prints the resolved target, changed and selected usage counts, quiesce level, and a
sorted line for every selected model. In a transaction, the queued response is a provisional plan;
`COMMIT` resolves `LATEST` again before applying the atomic model step.

## Backing Up

`BACKUP` in the REPL or through `--command` runs the backup and downloads its archive into the
named file on this machine. The `backup` subcommand does the same with arguments instead of Plexus,
and exits with a nonzero status whenever the archive was not delivered:

```bash
nervix-cli backup cluster --output cluster.nvxb
nervix-cli --domain payments backup domain --output - --without-resources > payments.nvxb
nervix-cli backup domain payments --output payments.nvxb --format json
```

`--output -` writes the archive to standard output and the report to standard error. A standard
output that would discard the archive, the null device or one that was closed when the CLI started,
fails with `WRITE_FAILED` before the backup is taken. The archive is downloaded and verified in a
staging directory first; when standard output then fails, for example because its reader closed
the pipe, the CLI keeps the verified archive there and reports `WRITE_FAILED` with the backup's
execution reference and the kept file, which is the only copy left.
[Backup And Restore](backup-and-restore.md#delivering-to-standard-output) describes how to recover
it. `--format json` prints the report, or the failure, as one JSON document. `DESCRIBE
BACKUP` reads a local archive without connecting to a server, whether it is typed in the REPL or
passed to `--command`:

```bash
nervix-cli --command "DESCRIBE BACKUP 'cluster.nvxb' FORMAT JSON;"
```

The description lists every runtime state section of each domain under `runtime_state`. A
deduplicator line names its keyspace's branch fingerprint, key count, groups, revision and schema
fingerprint; a window processor line names its branch fingerprint, retained rows, groups, branch
incarnation, next admission sequence, pending delayed histogram removals, revision, schema
fingerprint and window model digest. JSON reports the same values with `kind` `deduplicator` or
`window_processor`, together with the length and digest of every section. Neither form prints
branch-key values or deduplicator keys.

See [Backup And Restore](backup-and-restore.md) for what an archive holds and how long the server
retains it.

## Restoring

`RESTORE` in the REPL or through `--command` streams the archive the statement names from this
machine to the leader, and prints the restore's report when it ends; `--command` exits with the
status of that outcome. A restore whose outcome is not known prints its execution reference, and
sending it again under that reference, by running the statement again in the REPL or with
`--execution-reference`, resumes it rather than repeating it. The `restore` subcommand does
the same with arguments instead of Plexus, shows how much of the archive was sent while it streams,
and exits with a nonzero status whenever the restore did not complete:

```bash
nervix-cli restore cluster --input cluster.nvxb --on-existing-user skip
nervix-cli restore domain payments --as payments_copy --input cluster.nvxb
nervix-cli restore domain payments --input payments.nvxb --dry-run --format json
nervix-cli restore domain payments --input payments.nvxb --resume --format json
```

`--resume` starts the complete restored domain at its archived generation and mapping, preserving
materialized rows, deduplicator keys and windows. The default leaves it stopped; a subsequent
normal `START` advances its generation and clears materialized state, while deduplicator keys and
windows continue. A window whose model or branch incarnation changed starts empty, and the report
lists it among its warnings. Text and JSON reports include each domain's status and
start generation, including the planned lifecycle of a dry run.

`--dry-run` verifies the archive and plans the restore without changing anything, and `--format
json` prints the report, or the failure, as one JSON document. The Rust client sends the archive
again under the same execution reference after a redirect or a lost connection, so a restore
interrupted by a leader change is resumed rather than repeated. See
[Backup And Restore](backup-and-restore.md#restoring) for what a restore recreates and the order of
its steps.

## Streaming A Relay

The `subscribe` subcommand opens a read-only session subscription and prints events until
interrupted:

```bash
nervix-cli --domain quickstart subscribe watch orders
```

```bash
nervix-cli --domain quickstart subscribe sampled orders \
  --dropping \
  --batch-sample-rate 0.1 \
  --where 'input.amount >= 1000'
```

| Flag | Meaning |
| --- | --- |
| `--dropping` | drop deliveries when the session transport queue is full |
| `--blocking` | block instead of dropping; this is the default, so the flag is only ever explicit |
| `--batch-sample-rate <0.0-1.0>` | sampling of each row the predicate selected |
| `--where <expression>` | Plexus predicate over delivered records, read exactly as the `WHERE` clause of a typed `CREATE SUBSCRIPTION` and validated locally before the subscription opens |

`--dropping` and `--blocking` are mutually exclusive. Subscription semantics, sampling, and
backpressure are covered in [Sessions](sessions.md). A subscription the server refuses to open,
such as one whose relay does not exist, prints the refusal and its diagnostics and exits with
status `1`; Ctrl-C ends a subscription that opened with status `0`.

The subcommand prints server notices beside the rows, and keeps printing both across reconnects:
the client opens the subscription again on its next session, reports the gap as an interruption
notice, and resumes the notices of the new session, as described in [Asynchronous
Output](#asynchronous-output). A subscription the server ended is not opened again: the subcommand
prints its end and exits with status `0`. This includes a relay removed or redefined while the
client reconnects. Opening a new generation under the same name requires a new invocation.

## Following A Domain Clock

The `domain-clock` subcommand attaches to the selected domain's clock and prints its state and
progress on stdout until interrupted:

```bash
nervix-cli --domain simulation domain-clock
```

The first line is the attach reply. Every later clock state, tick, interruption, or attachment end
uses the same one-line format as the REPL's [Asynchronous Output](#asynchronous-output):

```text
attached to the clock of domain 'simulation': generation 1, paced: period 1s, skew 100ms, logical origin 2030-01-01T00:00:00Z, UTC anchor 2026-09-27T09:30:00.125Z, time rate 2
[events] domain clock [simulation] tick: generation 1, id 1, boundary 2030-01-01T00:00:00Z, authority UTC 2026-09-27T09:30:00.125Z, node logical 2030-01-01T00:00:00Z
[events] domain clock [simulation]: generation 1, stopped
[events] domain clock [simulation]: generation 2, paced: period 1s, skew 100ms, logical origin 2026-09-27T09:31:00Z, UTC anchor 2026-09-27T09:31:00Z, time rate 1
```

The domain and generation identify each state. A paced state includes its period, skew, logical
origin, UTC anchor, and rate. A tick includes its id, logical boundary, the authority's UTC
observation, and the serving node's logical reading.

Stdout carries these lines and nothing else, and their format is stable, so scripts and external
observers can parse them. Every line has one of these forms:

| Line | Printed |
| --- | --- |
| `attached to the clock of domain '<domain>': <clock>` | first, as the attach reply |
| `[events] domain clock [<domain>]: <clock>` | when the serving node installs another clock, and when a new session attaches the clock again |
| `[events] domain clock [<domain>] tick: generation <n>, id <n>, boundary <time>, authority UTC <time>, node logical <time>` | when the serving node accepts a newer tick of a paced clock |
| `[events] domain clock [<domain>] notice: the session was interrupted; the clock is attached again on the next session` | when the session holding the attachment ends |
| `[events] domain clock [<domain>] notice: attaching the clock again failed because <cause>: <message>; the next attempt follows in <wait>` | when a new session refuses to attach the clock again; `<cause>` is `the session holds a transaction`, `the session rejected the request` or `the client could not complete the attempt` |
| `[events] domain clock [<domain>] notice: the attachment ended because <reason>` | when the server ends the attachment, as the last line |

`<clock>` is `generation <n>, ` followed by `stopped`, `uninstalled`, `unpaced`, or
`paced: period <duration>, skew <duration>, logical origin <time>, UTC anchor <time>, time rate <rate>`.
Within a line:

- In the attach reply, state, and tick lines, fields are separated by `, ` and appear in the order
  shown. Each field but the state is its name, one space, and its value, and a name can contain
  spaces, as `logical origin`, `UTC anchor`, `time rate`, `authority UTC` and `node logical` do.
  The state is `stopped`, `uninstalled`, `unpaced`, or `paced:` followed by one space and the
  period field.
- `<n>` is an unsigned decimal integer. The generation counts the domain's `START`s, and tick ids
  number the ticks of a generation from one: a tick's boundary is the logical origin plus the id
  minus one periods.
- `<time>` is an RFC 3339 instant in UTC with a `Z` offset and no fractional digits, or three, six,
  or nine of them, as the instant needs.
- `<duration>` has unit suffixes, such as `500ms`, `1s`, or `1s 500ms`, and can contain spaces, so
  read a field by its name rather than by splitting at spaces.
- `<rate>` is a decimal number, such as `2` or `0.25`.
- In a notice line, `<message>` is display text that can itself contain `, ` and `; `, and `<wait>`
  is a whole number of seconds such as `1s` or `30s`.

Within one session, the tick ids of a generation increase and can skip, because the authority
coalesces missed periods and a slow reader receives the newest tick rather than a backlog. When the
session is lost, for example because its node restarts, the interruption line reports the gap, and
the client opens a new session within its retry deadline and attaches the clock again. The output
then shows the clock the new session reports, and its ticks continue from the newest one that
session's node holds. Ticks accepted in between are not replayed, and the first tick after the
interruption can repeat the last id printed before it, or precede it when another node serves the
new session. A node that is still starting answers the attach once it has installed the cluster's
committed domains, so a restart never ends the attachment. When the new session refuses the attach
for another reason, a notice line reports it and when the client tries again on that session.

The server ends an attachment when the domain no longer exists on the serving node, and a new
session that no longer has the domain ends it the same way. `<reason>` then reads
`the domain no longer exists on the serving node`. The end line is the last clock line and the
command exits with status `0`. A restoration that cannot open a session within the client's retry
deadline prints `[events] notice: domain clock events could not resume yet: ...; the client keeps
trying`, then the next read tries again. The observer can therefore follow the same clock after
an outage longer than one retry deadline.

Ctrl-C exits with status `0`. When the last clock event reports an attached clock, the CLI detaches
it first. When an interruption or failed restoration is outstanding, it closes the observer
without opening a replacement session just to detach. A failed initial connection, a refused
attach, including a missing domain, a refused detach, or a session with no known server to reopen
prints the typed reason on stderr and exits with status `1`.

## Cluster Node Administration

Each of these submits one statement and exits:

| Command | Statement | Purpose |
| --- | --- | --- |
| `nervix-cli cordon-node <id>` | `CORDON NODE` | stop scheduling new work onto a node |
| `nervix-cli uncordon-node <id>` | `UNCORDON NODE` | allow scheduling again |
| `nervix-cli drain-node <id>` | `DRAIN NODE` | move scheduled graph nodes away and keep the node cordoned |
| `nervix-cli remove-node <id>` | `DROP NODE` | remove the node from cluster membership |

Drain and replication behaviour is described in
[Replication And Drain Behavior](metrics-and-observability.md#replication-and-drain-behavior).

Each subcommand exits with the status of its statement's outcome, as [Exit Status](#exit-status)
describes, and Ctrl-C interrupts it as it interrupts `--command`. One whose outcome is not known
prints its execution reference; run the same subcommand again with that reference, given before the
subcommand, to recover it:

```bash
nervix-cli --execution-reference 01a1f3c2-7e4b-7a10-9c3d-5b2e8f6a1d00 drain-node node-2
```

`drain-node` cordons the target before moving work. It visits domains and their hard placement
groups or independent runtime nodes in canonical order, drains one unit at a time, and leaves the
node cordoned. A successful result starts with the aggregate and effective quiesce level, followed
by one line per move:

```text
drained node 'node-2' (moved 2 of 2 scheduled graph node(s))
quiesce level: ENTITY_PAUSE
- kind=ingestor name=orders from=node-2 to=node-3 replicas=node-2 promoted_replica=yes
- kind=emitter name=warehouse from=node-2 to=node-1 replicas=none promoted_replica=no
```

Each move line identifies the runtime-node kind and name, former and destination owners, resulting
replicas, and whether the destination was promoted from a replica. A node with no scheduled graph
work reports `moved 0 of 0` and `quiesce level: DYNAMIC`.

If one unit cannot drain or activate, the command returns an error containing the same header plus
a `failed:` line for that unit. Independent units continue moving, so `moved` can be smaller than
`of`. The node stays cordoned, successful moves remain committed, and running `drain-node` again
retries the units it still owns.
The subcommand exits with status `1` for that failed outcome, even when some units moved; a
completed drain exits `0`, and an outcome that is not known yet exits `3` with its execution
reference.

## Shell Completions

```bash
nervix-cli completions bash > /etc/bash_completion.d/nervix-cli
```

`bash`, `elvish`, `fish`, `powershell`, and `zsh` are supported. This subcommand does not contact
the server, so it works before a cluster exists.

## Leader Redirects

Persistent statements are applied by the leader. The CLI gives each one a stable execution
reference and retains it until the terminal result. If the session lands on a follower, the client
follows the redirect and reconnects on its own, repeating the statement under the same reference,
and waits out an election the same way; ordinary commands are bounded by the client's 120-second
retry deadline. `BACKUP` uses the global `--backup-wait-timeout` instead, ten minutes by default,
for the entire command across all domain cuts and reconnections. This applies to Plexus in
`--command` and the REPL as well as the `backup` subcommand. Its per-domain `TIMEOUT` does not
determine that total wait. A statement the leader never answered in its wait is reported with its execution
reference as not known yet, and [Uncertain Outcomes](#uncertain-outcomes) describes how to recover
it. A redirect that reaches the terminal as a statement's own result is
printed as:

```text
topology: not-a-leader, retry on leader 'node-2' at http://10.0.0.12:47391/
```

After an uncertain backup, `backup --execution-reference REFERENCE` recovers the admitted command
and downloads its archive with the original selected domain, scope, resources and capture
options. The output file or stdout destination may change. The JSON failure report exposes
`error.execution_reference`; [Backup And Restore](backup-and-restore.md#waiting-and-recovering)
describes recovery and retention limits.

Transaction controls follow the same redirect beginning with `BEGIN`. The CLI retains the returned
transaction id and attaches it on the new connection before resuming any queued statement or
commit. Each append keeps its execution reference and expected queue position. A pending commit
waits for the exact retained terminal result recorded under its own reference. An open
transaction therefore survives an unclean connection loss or leader failover; a clean CLI exit
reverts it. See [Replicated Plexus Transactions](control-plane.md#replicated-plexus-transactions) and
[Client Session Protocol](client-session-protocol.md).
