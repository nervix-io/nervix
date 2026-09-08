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

1. **A subcommand**, such as `subscribe` or `drain-node`.
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

Subscription deliveries and server notifications arrive independently of the prompt and are printed
above it:

```text
[events] subscription [watch] from [orders]: {"order_id":"o-1001","amount":1500}
[events] server ERROR: emitter 'redis_orders' publish failed
[events] topology INFO: raft transition: node-2 became leader
```

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
| `UPLOAD RESOURCE <name> VERSION '<dir>'` | stream a local directory as a new version of that resource in the active domain |
| `CREATE SUBSCRIPTION` / `DELETE SUBSCRIPTION` | start and stop a read-only relay subscription |

`USE`, `LIST DOMAINS`, and `UPLOAD RESOURCE` must be submitted on their own, and never inside a
transaction. Read-only statements, subscriptions, `CREATE DOMAIN`, `CREATE USER`, and node
administration are also rejected while queueing transaction content. `BEGIN` requires an existing
active domain and binds the transaction to it; attaching a transaction switches the active domain
to the transaction's domain. An upload targets the active domain, renders live progress, and
finishes once the cluster has replicated the version:

```text
upload resource 'order_model' finished: 4.2 MiB sent, replication complete
```

See [Resources](resources.md#lifecycle) for what a resource version contains.

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
| `--batch-sample-rate <0.0-1.0>` | per-arrival sampling of delivered batches |
| `--where <expression>` | NSPL predicate over delivered records, validated locally before connecting |

`--dropping` and `--blocking` are mutually exclusive. Subscription semantics, sampling, and
backpressure are covered in [Sessions](sessions.md).

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

Model statements are applied by the leader. If the session lands on a follower the client reports
the redirect and reconnects on its own, following up to four hops:

```text
topology: not-a-leader, retry on leader 'node-2' at http://10.0.0.12:47391
```

Transaction controls follow the same redirect beginning with `BEGIN`. The CLI retains the returned
transaction id and attaches it on the new connection before retrying any queued statement or
commit. If the attached progress shows that the cluster already accepted that operation, the CLI
uses the replicated state or retained commit result instead of submitting it twice. An open
transaction therefore survives an unclean connection loss or leader failover; a clean CLI exit
reverts it. See
[Replicated NSPL Transactions](control-plane.md#replicated-nspl-transactions).
