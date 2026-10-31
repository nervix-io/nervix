# Database Client Connection Pools

Database clients declare how many connections Nervix may open for them. The pool-capable client
types are `POSTGRES`, `MYSQL`, `MONGODB`, and `REDIS`. Every client of those types states both a
minimum and a maximum connection count as part of its definition.

HTTP clients are outside this contract, including ClickHouse, HTTP polling, Prometheus, Sentry,
OTEL, SQS, Iceberg REST, and the object-store transports. Broker clients, WebSockets, Syslog, and
ZeroMQ keep their transport-specific connection policies. The pool clauses are available only for
the four types listed above. [ClickHouse connections](#clickhouse-connections) describes how a
ClickHouse client, which has no bounded pool, reuses its connections.

## Declaring the bounds

The clause is required, with its two bounds in this order, immediately after the client name and
before an optional resource mount:

```plexus,ignore
CREATE [IF NOT EXISTS] POSTGRES|MYSQL|MONGODB|REDIS CLIENT <name>
  POOL SIZE MIN <minimum> MAX <maximum>
  [MOUNT <resource> VERSION <n>|LATEST]
  CONFIG { <connector configuration> };
```

```plexus
CREATE POSTGRES CLIENT postgres_main
  POOL SIZE MIN 2 MAX 8
  CONFIG {
    'addr' = 'postgresql://USER:PASSWORD@HOST:5432/DATABASE?sslmode=verify-full'
  };

CREATE REDIS CLIENT redis_main
  POOL SIZE MIN 1 MAX 4
  MOUNT dev_tls VERSION 1
  CONFIG {
    'addr' = 'rediss://USER:PASSWORD@HOST:6379/0',
    'tls_ca_file' = '{{ dev_tls }}/ca.pem'
  };
```

`MIN` is the number of established, usable connections the pool maintains while it has local
users. It counts idle and checked-out connections together; it does not reserve that many idle
connections in addition to active ones. `MAX` is the ceiling on connections Nervix manages for the
client, counting connections being established or retired as well as those in use.

A minimum of zero allows the pool to become empty while it is idle. Equal positive bounds request a
fixed maintained size. A maximum of 1 is an ordinary bound, not a special case.

## One pool per client, per domain, per node

A pool belongs to one named client in one domain on one physical Nervix node. Every local emitter
of that client borrows from the same pool, so adding emitters does not add capacity and the bounds
describe the node's connections rather than each emitter's. Two separately named clients have
separate budgets even when their addresses match, and clients in different domains do too.

A pool is materialized only when the node has a local user that needs pooled operations.
Registering a client, or replicating its definition to a node with no local user, opens no
connections. Removing one user preserves the pool for the others; removing the last one closes it
and releases its resource mounts.

Placement therefore decides how many pools exist. A client with `POOL SIZE MIN 2 MAX 8` and local
users on three nodes maintains six connections cluster-wide and may reach 24. The bounds are not a
cluster-wide database quota, and capacity planning must also account for separately named clients
and other applications using the same database.

## Borrowing and waiting

A borrower holds a connection only while it performs an external operation or its necessary
metadata work. For Postgres the unit is one bounded insert: the metadata lookup of a write runs on
its first insert's connection, and an insert the database refuses for one of its rows writes those
rows one at a time on the same connection before returning it. MySQL likewise keeps one connection
for a bounded insert and the isolation of any rows it refuses. For Redis the unit is one publish
and its response. For MongoDB the driver checks a connection out for each command it sends, the
collection lookup before a write and each insert or bulk-write command, and returns it when the
command answers. A flush containing several inserts releases capacity between them so other
emitters can make progress. An emitter waiting out a flush interval or a retry backoff holds no
connection at all.

Because a Postgres or MySQL insert keeps its connection until it has an answer for every row it
carries, isolating a rejected row never waits for the pool a second time, whether other emitters
of the client queue for the same connections or the pool would have to open a new one. The host
handles the per-record outcomes when the write completes, including any later inserts in it.

When capacity is exhausted the operation waits, and that wait applies backpressure through the
emitter's existing bounded input and flush buffers. A full pool is a waiting state rather than a
failure: `DESCRIBE EMITTER <name>;` reports

```plain
status: WAITING
detail: waiting for a connection from client 'postgres_main' for 3s
```

A MongoDB write waits for a connection inside its driver, which reports no waiting state, so
`DESCRIBE EMITTER` never shows that wait as `WAITING`.

An acquisition timeout, an inability to connect, or the loss of a connection is an infrastructure
failure instead, and follows the emitter's existing retry policy and general-error reporting.

Postgres has a 30-second acquisition deadline covering pool wait, connection establishment,
authentication and validation, independent of how long an accepted query then runs. A MySQL client
that sets `timeout_ms` bounds each checkout, including connection establishment and authentication,
by that value, and bounds initial validation and each insert separately. Its minimum keeper uses
the same checkout bound. An exhausted pool reports `checking out a MySQL connection did not finish
within timeout_ms` with the configured duration, then the emitter retries on its policy. Omitting
`timeout_ms` leaves these MySQL waits unbounded. A Redis publish checks its connection out within the client's
`connect_timeout_ms`, 30 seconds by default, the same deadline that bounds opening each new command
connection: a checkout that runs out of it reports `checking out a Redis command connection did not
finish within connect_timeout_ms` with the bound. A MongoDB write has only its driver's deadlines:
server selection ends after `serverSelectionTimeoutMS`, 30 seconds by default, and opening a
connection's TCP and TLS session after `connectTimeoutMS`, 10 seconds by default, both set in
`addr`. The wait for a pooled connection, the connection handshake and each command's answer have
no deadline. The driver implements neither `waitQueueTimeoutMS` nor `socketTimeoutMS`, so setting
them in `addr` bounds none of these waits.

## What each bound counts

| Client type | Scope of `POOL SIZE MIN M MAX N` on one Nervix node |
| --- | --- |
| `POSTGRES` | Maintain M usable connections and allow at most N across all local users of the named client. |
| `MYSQL` | Maintain M usable connections and allow at most N across all local users of the named client. |
| `REDIS` | Maintain M usable command connections and allow at most N across all local publishers. Each Redis Pub/Sub ingestor owns one dedicated subscription connection that is never drawn from, returned to, or charged against the command pool. |
| `MONGODB` | Maintain M and allow at most N application connections independently for each ready MongoDB server, shared by all local users. Monitoring connections are driver-owned and counted separately. |

A Redis client used only by ingestors still declares both bounds but materializes no command pool
until a local publisher needs one. Consequently a client with `POOL SIZE MIN 1 MAX 4`, one local
publisher and two local Pub/Sub ingestors maintains one command connection, whether its publisher
is idle or publishing, and may use up to six Redis connections in total: four command connections
and two dedicated subscription connections.

## How each client type keeps its minimum

Every pool-capable client type keeps `MIN` by the same rule: the connections its local users have
checked out count toward it together with the idle ones, a pool below it opens the ones it is
missing, and a pool at or above it opens another only for a borrower that finds no connection
idle, up to `MAX`. The drivers count their own minimums differently, so Nervix supplies what a
driver leaves out:

| Client type | The driver's own minimum | What Nervix adds |
| --- | --- | --- |
| `POSTGRES` | Counts every connection the driver holds, idle or checked out. The driver opens it with the pool and replaces a connection it closes at a lifetime or after a failure when the pool would otherwise fall below it. | Nothing. |
| `MYSQL` | A retention floor only: the driver keeps up to that many idle connections once they are returned, and never opens one to reach it. | Once a second a maintenance task checks connections out of the pool one at a time, holding each, until the pool's established connections, the ones emitters hold included, reach `MIN`, and then returns them. The pool hands over its idle connections first and opens one for each checkout it has none idle for, so the task opens exactly the missing ones. |
| `MONGODB` | Counts every application connection to a server, available, being established or checked out. The driver populates each ready server's pool up to it in the background. | Nothing. |
| `REDIS` | Counts idle connections only, so every checkout would make the driver open another beside the connection in use, up to `MAX`. Nervix does not use it. | Opening the pool establishes `MIN` command connections. Whenever the pool closes one, Nervix counts the connections still established, the ones publishers hold included, and has the pool open the missing ones. |

For Redis, opening the pool establishes each of its `MIN` connections within the client's
`connect_timeout_ms`. A connection that cannot be opened fails the open with its cause, and the
emitter opens the pool again on its retry policy. When the pool later closes a connection, at a
lifetime or after a failed `PING`, Nervix brings it back to `MIN` at once. If Redis cannot be
reached then, each attempt waits up to `connect_timeout_ms` and the next follows as soon as it ends,
while a publisher that finds no connection idle still opens one for itself.

For MySQL, opening the pool validates one connection, and the maintenance task brings the pool to
`MIN` a second later. A checkout that fails ends its pass, which returns the connections it held,
and the next pass tries again a second later, while an emitter that finds no connection idle still
opens one for itself.

## Connection lifetimes

A `REDIS` client declares how long its command pool keeps a connection, with two Nervix-owned
`CONFIG` keys that `CREATE CLIENT` validates:

| Key | Default | What it limits |
| --- | --- | --- |
| `pool_idle_timeout_ms` | `600000`, 10 minutes | How long a command connection may sit idle in the pool |
| `pool_max_lifetime_ms` | `1800000`, 30 minutes | How old a command connection may become |

Each takes a positive whole number of schedulable milliseconds. A connection that has reached its
maximum lifetime is closed when the publish that borrowed it returns it, or at the pool's next check
while it is idle. A connection idle for `pool_idle_timeout_ms` is closed at the next check,
including one the pool keeps for `MIN`; the pool then opens connections again to keep `MIN`, so a
quiet client renews its minimum instead of holding the same connections indefinitely. The pool
checks its idle connections against both lifetimes every 30 seconds, its driver's interval, so an
idle connection closes up to 30 seconds after it reaches either lifetime.

Set `pool_idle_timeout_ms` below the idle limit of a proxy or managed endpoint in front of Redis,
and `pool_max_lifetime_ms` below any forced reconnection interval, so the pool retires a connection
before the endpoint drops it. A replacement connection resolves `addr` again. Neither key affects a
Redis Pub/Sub ingestor's dedicated subscription connection, which its client's `idle_timeout_ms`
governs.

```plexus
CREATE REDIS CLIENT redis_edge
  POOL SIZE MIN 1 MAX 4
  CONFIG {
    'addr' = 'redis://HOST:6379/0',
    'pool_idle_timeout_ms' = '240000',
    'pool_max_lifetime_ms' = '900000'
  };
```

`DESCRIBE CLIENT redis_edge;` reports both keys:

```plain
client: redis_edge
connector: REDIS
config:
addr: redis://HOST:6379/0
pool_idle_timeout_ms: 240000
pool_max_lifetime_ms: 900000
```

A client that omits a key reports its default, marked `(default)`, such as
`pool_max_lifetime_ms: 1800000 (default)`. `SHOW CREATE CLIENT` reports only the keys the client
was declared with.

## Accepted values

Both values are unquoted decimal integers written directly in the statement.

| Bound | Accepted range |
| --- | --- |
| `MIN` | 0 through 4,294,967,295 |
| `MAX` | 1 through 4,294,967,295 |

The minimum must not exceed the maximum. The clause appears exactly once and carries each bound
once, and neither count takes a unit, an expression, a template, an unlimited value, or an implicit
default. A statement that omits the clause or a bound, writes the bounds in the other order,
repeats one, places the clause after `MOUNT`, or gives a count outside its range is rejected where
it is read, before any client exists. The diagnostic points at the rejected text and never prints a
credential or a complete connection address.

## Bounds and connector configuration

The generated [Connector Capabilities](connector-capabilities.md#clients) reference lists every
client kind's declared pooling and CONFIG owner class. A driver's pass-through keys and the
Nervix-owned keys keep their respective entity contracts.

The bounds are structured client configuration, independent of the connector's string-valued
`CONFIG`. Endpoint, authentication, database, TLS, and supported session options continue to belong
to `CONFIG`. Connection-pool sizing is declared only through the `POOL SIZE` clause. Native
connector options for connection establishment, idle lifetime, and operation timeouts stay in
`CONFIG` and do not restate either bound.

Both counts are strongly persisted with the rest of the client definition, and
`SHOW CREATE CLIENT <name>;` reports them in the declared order:

```plain
CREATE POSTGRES CLIENT postgres_main
  POOL SIZE MIN 2 MAX 8
  CONFIG {
    'addr' = 'postgresql://USER:PASSWORD@HOST:5432/DATABASE?sslmode=verify-full'
  };
```

## Changing a bound

There is no `ALTER CLIENT`. Change either bound the same way as any other part of a client
definition: `DROP CLIENT` and `CREATE <CONNECTOR> CLIENT` in one transaction. A client that runtime entities
reference is replaced together with them, under the existing transactional quiesce contract for
those entities.

## ClickHouse connections

A ClickHouse client takes no `POOL SIZE` clause, and no declared bound limits how many connections
it opens. None of this chapter's pool rules apply to it: there is no pool per client, per domain,
per node, and no minimum is maintained. Each running ClickHouse emitter builds its own HTTP client
from the client's configuration, so separate emitters of one client never share a connection, and
adding emitters adds connections.

An emitter sends one insert at a time, and an insert uses one connection until ClickHouse answers
it, so an emitter uses one connection at a time, also while it writes the rows of a refused insert
again one by one. A connection an insert finishes with stays open for the emitter's next insert
until it has been idle for the client's `pool_idle_timeout_ms`, 2000 milliseconds unless the client
sets it, and is closed after that. The client keeps idle connections without a count limit, and the
next insert that finds none opens a new connection, resolving the host again. `tcp_keepalive_ms`,
60000 milliseconds unless the client sets it, starts TCP keep-alive probes on a connection that has
been idle that long. `timeout_ms` bounds each insert, its connection included; none of these keys
bounds how many connections an emitter opens over time. [Emitters](./emitters.md#clickhouse)
describes the client's keys.
