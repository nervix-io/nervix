# Command Line Client

`nervix-cli` is the interactive NSPL client. It is installed alongside `nervix-server`
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
| `--command <NSPL>` | | | run statements once and exit |

There is no general CLI configuration file and no `--version` flag. `--server`, `--domain`, and the
TLS options have no environment-variable equivalents. The DNS options select the resolver used by
the native session; the system resolver and hosts files are the defaults, and
[Name Resolution](name-resolution.md) describes how that resolver answers. The password can come
from `NERVIX_PASSWORD` so it need not appear in shell history.

The table above is the short version. [nervix-cli Reference](nervix-cli-reference.md) is printed by
the binary itself while this book is built, so it is the authoritative list of every option and
subcommand for this release.

## Modes

The client runs in exactly one of three modes, in this order of precedence:

1. **A subcommand**, such as `subscribe`, `domain-clock`, `backup`, `restore`, or `drain-node`.
2. **`--command`**, which submits NSPL, prints the result, and exits.
3. **The interactive REPL**, when neither of the above is given.

There is no file-execution mode. To run a saved script, pass it through `--command`; to format
one, use [`nervix-nspl-format`](client-tools-nspl-format.md):

```bash
nervix-cli --domain quickstart --command "$(cat pipeline.nspl)"
```

## The Interactive REPL

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
[events] server ERROR: emitter 'redis_orders' publish failed
[events] topology INFO: raft transition: node-2 became leader
[events] domain clock [simulation]: generation 2, stopped
[events] domain clock [simulation] tick: generation 3, id 12, boundary 2030-01-01T00:00:01.100Z, authority UTC 2026-09-27T00:00:00Z, node logical 2030-01-01T00:00:01.120Z
```

A domain clock line follows every state change or accepted tick after the attach reply. A slow
client may skip tick ids because its pending tick is replaced by the newest one. When the server ends an
attachment, or the session holding it is interrupted and the clock is attached again on the next
session, the line reads `[events] domain clock [<domain>] notice: ...` with the reason.

The REPL prints these lines when it draws its next prompt: after the statement that is running
finishes, or when you press Enter on an empty line.

Output keeps printing across reconnects. When the session is lost, the next statement you run opens
a new one; the client also opens one at once while a subscription or a followed clock waits to be
restored. Server notices then resume with the new session, rows arrive from every subscription
restored on it or opened after it, and a followed clock prints its state again once it is attached
there. When the client cannot open a session within its 120-second retry deadline, a line names
what is waiting and why, and the client keeps trying:

```text
[events] notice: subscription events could not resume yet: failed to connect to server; the client keeps trying
```

After a reconnect the CLI opens every subscription and attaches every clock again. When the new
session refuses one, a notice line reports the server's message and when the next attempt follows:

```text
[events] subscription [watch] notice: opening the subscription again failed: stream 'orders' does not exist in domain 'quickstart'; the next attempt follows in 2s
[events] domain clock [simulation] notice: attaching the clock again failed: session-scoped and client-local statements cannot be queued in a transaction; the next attempt follows in 1s
```

`DELETE SUBSCRIPTION` of a subscription no open session holds, because its session ended or the new
session refused to open it again, completes at once and frees the name.

When the server ends a subscription because its relay was redefined or removed, the subscription's
last line reads:

```text
[events] subscription [watch] notice: the subscription ended: session subscription 'watch' ended because relay 'orders' in domain 'quickstart' no longer exists
```

The CLI never opens an ended subscription again, including after a reconnect. `CREATE SUBSCRIPTION`
under the same name opens it again under the relay's current definition, and `DELETE SUBSCRIPTION`
of it completes at once and frees the name.

If server notices arrive faster than the CLI reads them, the client drops the ones it held and
`[events] notice: server notices were dropped because they arrived faster than they were read`
marks the gap; the notices after it keep printing.

### Leaving

`exit`, `quit`, `Ctrl-D`, or `Ctrl-C`.

## Session And Transaction Statements

Some statements affect the client session or replicated transaction state rather than directly
applying one model change:

| Statement | Effect |
| --- | --- |
| `USE <domain>` | switch the session's active domain |
| `LIST DOMAINS` | list domains with pace and status |
| `BEGIN` / `COMMIT` / `REVERT` | open, apply, or discard a replicated transaction on the leader |
| `DESCRIBE TRANSACTION ['<id>'] [OPERATION <n>] [FORMAT TEXT \| JSON]` | read the attached transaction, or one named by id, without changing it |
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
attachment is refused because it does not. The CLI attaches the clocks it follows again after a
reconnect. See [Domain Clock Attachment](sessions.md#domain-clock-attachment). Read-only statements, subscriptions, `CREATE DOMAIN`, `CREATE USER`, and node
administration are also rejected while queueing transaction content. `DESCRIBE TRANSACTION` is the
exception: while a transaction is open it reads that transaction's impact report without queueing
anything or consuming an operation number, so the next queued statement keeps the number it would
have had. It must be submitted on its own. With an id it reads another transaction of the same user
and leaves the prompt, the active domain, and the attached transaction as they were:

```bash
nervix-cli --domain production --command \
  "DESCRIBE TRANSACTION '01a0ca64-7062-7302-ac1e-43138ccc2067' OPERATION 2 FORMAT JSON;"
```

For a standalone `--command` inspection with `FORMAT JSON`, stdout contains exactly one JSON
document serialized from the typed report, without terminal decoration or unrelated events.
Failures print one JSON object with `error.code` and `error.message` on stdout and exit nonzero;
diagnostic details and unrelated events go to stderr. `FORMAT TEXT` uses the normal readable
terminal output. Inspection output is complete rather than paginated or truncated, so redirect a
large text or JSON report to a file or downstream process when terminal output is impractical.
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
upload resource 'order_model' finished: 4.2 MiB sent
uploaded resource version 1
```

See [Resources](resources.md#lifecycle) for what a resource version contains.

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
named file on this machine. The `backup` subcommand does the same with arguments instead of NSPL,
and exits with a nonzero status whenever the archive was not delivered:

```bash
nervix-cli backup cluster --output cluster.nvxb
nervix-cli --domain payments backup domain --output - --without-resources > payments.nvxb
nervix-cli backup domain payments --output payments.nvxb --format json
```

`--output -` writes the archive to standard output and the report to standard error. `--format
json` prints the report, or the failure, as one JSON document. `DESCRIBE BACKUP` reads a local
archive without connecting to a server, whether it is typed in the REPL or passed to `--command`:

```bash
nervix-cli --command "DESCRIBE BACKUP 'cluster.nvxb' FORMAT JSON;"
```

See [Backup And Restore](backup-and-restore.md) for what an archive holds and how long the server
retains it.

## Restoring

`RESTORE` in the REPL or through `--command` streams the archive the statement names from this
machine to the leader, and prints the restore's report when it ends. The `restore` subcommand does
the same with arguments instead of NSPL, shows how much of the archive was sent while it streams,
and exits with a nonzero status whenever the restore did not complete:

```bash
nervix-cli restore cluster --input cluster.nvxb --on-existing-user skip
nervix-cli restore domain payments --as payments_copy --input cluster.nvxb
nervix-cli restore domain payments --input payments.nvxb --dry-run --format json
nervix-cli restore domain payments --input payments.nvxb --resume --format json
```

`--resume` starts the complete restored domain at its archived generation and mapping, preserving
materialized rows. The default leaves it stopped; a subsequent normal `START` advances its
generation and clears materialized state. Text and JSON reports include each domain's status and
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
| `--where <expression>` | NSPL predicate over delivered records, read exactly as the `WHERE` clause of a typed `CREATE SUBSCRIPTION` and validated locally before the subscription opens |

`--dropping` and `--blocking` are mutually exclusive. Subscription semantics, sampling, and
backpressure are covered in [Sessions](sessions.md).

The subcommand prints server notices beside the rows, and keeps printing both across reconnects:
the client opens the subscription again on its next session, reports the gap as an interruption
notice, and resumes the notices of the new session, as described in [Asynchronous
Output](#asynchronous-output). A subscription the server ended is not opened again: the subcommand
prints its end and keeps printing server notices until interrupted.

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
| `[events] domain clock [<domain>] notice: attaching the clock again failed: <message>; the next attempt follows in <wait>` | when a new session refuses to attach the clock again |
| `[events] domain clock [<domain>] notice: the attachment ended because <reason>` | when the server ends the attachment, as the last line |

`<clock>` is `generation <n>, ` followed by `stopped`, `uninstalled`, `unpaced`, or
`paced: period <duration>, skew <duration>, logical origin <time>, UTC anchor <time>, time rate <rate>`.
Within a line:

- In the attach reply, state, and tick lines, fields are separated by `, ` and appear in the order
  shown. Each field is its name, one space, and its value.
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

The server ends an attachment when the domain no longer exists on the serving node; the end line is
the last clock line and the command exits with status `0`, as it does after Ctrl-C detaches the
clock. A refused attach, including a missing domain, prints the typed reason on stderr and exits
nonzero, as does a lost session the client cannot replace within its retry deadline.

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
for the entire command across all domain cuts and reconnections. This applies to NSPL in
`--command` and the REPL as well as the `backup` subcommand. Its per-domain `TIMEOUT` does not
determine that total wait. A statement the leader never answered in its wait is reported with its execution
reference as not known yet. A redirect that reaches the terminal as a statement's own result is
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
reverts it. See [Replicated NSPL Transactions](control-plane.md#replicated-nspl-transactions) and
[Client Session Protocol](client-session-protocol.md).
