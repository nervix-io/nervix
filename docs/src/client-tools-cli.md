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
| `--domain <NAME>` | | `default` | domain the session starts in |
| `--username <NAME>` | `NERVIX_USERNAME` | `default` | registry user |
| `--password <PASSWORD>` | `NERVIX_PASSWORD` | | prompted interactively when unset |
| `--command <NSPL>` | | | run statements once and exit |

There is no configuration file and no `--version` flag. `--server`, `--domain`, and the TLS options
have no environment-variable equivalents; only the credentials do, so a password never has to
appear in shell history.

The table above is the short version. [nervix-cli Reference](nervix-cli-reference.md) is printed by
the binary itself while this book is built, so it is the authoritative list of every option and
subcommand for this release.

## Modes

The client runs in exactly one of three modes, in this order of precedence:

1. **A subcommand**, such as `subscribe`, `domain-clock`, or `drain-node`.
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
resource names and versions, session subscription names, and domain names. Inside
`UPLOAD RESOURCE ... VERSION '<path>'` the client completes local filesystem paths instead,
expanding `~` to your home directory.

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
[events] domain clock [simulation] tick: generation 3, id 12, boundary 2030-01-01T00:00:01.100000000Z, authority UTC 2026-09-27T00:00:00Z, node logical 2030-01-01T00:00:01.120000000Z
```

A domain clock line follows every state change or accepted tick after the attach reply. A slow
client may skip tick ids because its pending tick is replaced by the newest one. When the server ends an
attachment, or the session holding it is interrupted and the clock is attached again on the next
session, the line reads `[events] domain clock [<domain>] notice: ...` with the reason.

After a reconnect the CLI opens every subscription and attaches every clock again. When the new
session refuses one, a notice line reports the server's message and when the next attempt follows:

```text
[events] subscription [watch] notice: opening the subscription again failed: stream 'orders' does not exist in domain 'quickstart'; the next attempt follows in 2s
[events] domain clock [simulation] notice: attaching the clock again failed: session-scoped and client-local statements cannot be queued in a transaction; the next attempt follows in 1s
```

`DELETE SUBSCRIPTION` of a subscription no open session holds, because its session ended or the new
session refused to open it again, completes at once and frees the name.

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
| `CREATE SUBSCRIPTION` / `DELETE SUBSCRIPTION` | start and stop a read-only relay subscription |
| `ATTACH DOMAIN CLOCK` / `DETACH DOMAIN CLOCK` | start and stop following the active domain's clock |

`USE`, `LIST DOMAINS`, `UPLOAD RESOURCE`, and the domain clock statements must be submitted on
their own, and never inside a transaction. `ATTACH DOMAIN CLOCK` prints the clock the serving node
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
| `--where <expression>` | NSPL predicate over delivered records, validated locally before the subscription opens |

`--dropping` and `--blocking` are mutually exclusive. Subscription semantics, sampling, and
backpressure are covered in [Sessions](sessions.md).

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
observation, and the serving node's logical reading. Tick ids increase within a generation but may
skip when the client or server coalesces progress. After a redirect or transport loss, an
interruption line reports the gap and the client's restored attachment prints the fresh state. When
the new session refuses to attach the clock again, a notice line reports the refusal and when the
next attempt follows.
The server may end an attachment when the domain disappears; the end line is the last clock line
and the command exits. Ctrl-C detaches the clock and exits successfully. A refused attach,
including a missing domain, prints the typed reason on stderr and exits nonzero.

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
and waits out an election the same way; all of it is bounded by the client's 120-second retry
deadline. A statement the leader never answered in that time is reported with its execution
reference as not known yet. A redirect that reaches the terminal as a statement's own result is
printed as:

```text
topology: not-a-leader, retry on leader 'node-2' at http://10.0.0.12:47391/
```

Transaction controls follow the same redirect beginning with `BEGIN`. The CLI retains the returned
transaction id and attaches it on the new connection before resuming any queued statement or
commit. Each append keeps its execution reference and expected queue position. A pending commit
waits for the exact retained terminal result recorded under its own reference. An open
transaction therefore survives an unclean connection loss or leader failover; a clean CLI exit
reverts it. See [Replicated NSPL Transactions](control-plane.md#replicated-nspl-transactions) and
[Client Session Protocol](client-session-protocol.md).
