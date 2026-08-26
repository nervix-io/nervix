# Client Tools

Nervix ships two interactive clients, and both speak the same language to the same place: they open
a session against the cluster's session API and submit NSPL to the leader. Almost every statement in
this book can be typed into either one, and neither is required — a Nervix cluster is administered
entirely through NSPL, whatever submits it.

## Choosing A Client

| Client | Where it runs | Use it for |
| --- | --- | --- |
| [Command Line Client](client-tools-cli.md) | your terminal, installed separately | interactive work, scripting, shell completions, streaming a relay to stdout, node administration |
| [Web Console](client-tools-web-console.md) | your browser, served by every node | reading a running graph, exploring an unfamiliar domain, guided subscriptions, uploading resources |
| [Rust Client Library](client-library.md) | your own program | embedding Nervix control and subscriptions in a Rust application |

One further tool is not a client at all: [`nervix-nspl-format`](client-tools-nspl-format.md) formats
saved `.nspl` files and checks that they are formatted. It never opens a session.

The console is the better tool for understanding a graph you did not write, because it draws one.
The command line client is the better tool for repeating yourself, because it can be scripted.

Only two things differ. Uploading a resource version names a local directory in the command line
client and uses a file picker in the console; the console additionally offers guided actions —
clicking a graph item or an entity — that simply type NSPL for you.

## What Every Client Shares

- **The same statement surface.** Domains, schemas, codecs, relays, clients, runtime nodes, and
  emitters are all declared in NSPL ([NSPL Overview](nspl-overview.md)).
- **Session and transaction statements.** `USE`, `LIST DOMAINS`, `UPLOAD RESOURCE`,
  `CREATE SUBSCRIPTION`, and `DELETE SUBSCRIPTION` are session/client operations. `BEGIN`,
  `COMMIT`, and `REVERT` operate on a leader-owned, replicated transaction.
- **Server-driven completion.** Suggestions come from the cluster, not from a local copy of the
  grammar, so they include the identifiers that actually exist in the active domain.
- **HTTP Basic authentication** against a registry user.
- **Automatic leader redirects.** A session opened against a follower is redirected to the current
  leader; you do not need to know which node leads.
- **Automatic transaction attach.** Both interactive clients retain the transaction id and attach
  it before replaying a command after reconnect or leader failover, and do not repeat an operation
  whose replicated progress already advanced.

Sessions, subscriptions, sampling, and backpressure are described in [Sessions](sessions.md).

To get a cluster running first, see [Installation](installation.md) and
[Running Nervix](quickstart-running.md).
