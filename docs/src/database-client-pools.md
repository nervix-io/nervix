# Database Client Connection Pools

Database clients declare how many connections Nervix may open for them. The pool-capable client
types are `POSTGRES`, `MYSQL`, `MONGODB`, and `REDIS`. Every client of those types states both a
minimum and a maximum connection count as part of its definition.

HTTP clients are outside this contract, including ClickHouse, HTTP polling, Prometheus, Sentry,
OTEL, SQS, Iceberg REST, and the object-store transports. Broker clients, WebSockets, Syslog, and
ZeroMQ keep their transport-specific connection policies. The pool clauses are available only for
the four types listed above.

## Declaring the bounds

The clause is required, with its two bounds in this order, immediately after the client type and
before an optional resource mount:

```nspl,ignore
CREATE [IF NOT EXISTS] CLIENT <name>
  TYPE POSTGRES|MYSQL|MONGODB|REDIS
  POOL SIZE MIN <minimum> MAX <maximum>
  [MOUNT <resource>]
  CONFIG { <connector configuration> };
```

```nspl
CREATE CLIENT postgres_main
  TYPE POSTGRES
  POOL SIZE MIN 2 MAX 8
  CONFIG {
    'addr' = 'postgresql://USER:PASSWORD@HOST:5432/DATABASE?sslmode=verify-full'
  };

CREATE CLIENT redis_main
  TYPE REDIS
  POOL SIZE MIN 1 MAX 4
  MOUNT dev_tls
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
metadata work. For Postgres and MySQL the unit is a bounded insert or its metadata lookup, and for
Redis it is one publish and its response; a flush containing several inserts releases capacity
between them so other emitters can make progress. An emitter waiting out a flush interval or a
retry backoff holds no connection at all.

When capacity is exhausted the operation waits, and that wait applies backpressure through the
emitter's existing bounded input and flush buffers. A full pool is a waiting state rather than a
failure: `DESCRIBE EMITTER <name>;` reports

```plain
status: WAITING
detail: waiting for a connection from client 'postgres_main' for 3s
```

An acquisition timeout, an inability to connect, or the loss of a connection is an infrastructure
failure instead, and follows the emitter's existing retry policy and general-error reporting.

Postgres has a 30-second acquisition deadline covering pool wait, connection establishment,
authentication and validation, independent of how long an accepted query then runs. MySQL and Redis
have no separate pool-wait deadline. MongoDB retains its native server-selection and operation
timeouts.

## What each bound counts

| Client type | Scope of `POOL SIZE MIN M MAX N` on one Nervix node |
| --- | --- |
| `POSTGRES` | Maintain M usable connections and allow at most N across all local users of the named client. |
| `MYSQL` | Maintain M usable connections and allow at most N across all local users of the named client. |
| `REDIS` | Maintain M usable command connections and allow at most N across all local publishers. Each Redis Pub/Sub ingestor owns one dedicated subscription connection that is never drawn from, returned to, or charged against the command pool. |
| `MONGODB` | Maintain M and allow at most N application connections independently for each ready MongoDB server, shared by all local users. Monitoring connections are driver-owned and counted separately. |

A Redis client used only by ingestors still declares both bounds but materializes no command pool
until a local publisher needs one. Consequently a client with `POOL SIZE MIN 1 MAX 4`, one local
publisher and two local Pub/Sub ingestors maintains one command connection and may use up to six
Redis connections in total: four command connections and two dedicated subscription connections.

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

The bounds are structured client configuration, independent of the connector's string-valued
`CONFIG`. Endpoint, authentication, database, TLS, and supported session options continue to belong
to `CONFIG`. Connection-pool sizing is declared only through the `POOL SIZE` clause. Native
connector options for connection establishment, idle lifetime, and operation timeouts stay in
`CONFIG` and do not restate either bound.

Both counts are strongly persisted with the rest of the client definition, and
`SHOW CREATE CLIENT <name>;` reports them in the declared order:

```plain
CREATE CLIENT postgres_main
  TYPE POSTGRES
  POOL SIZE MIN 2 MAX 8
  CONFIG {
    'addr' = 'postgresql://USER:PASSWORD@HOST:5432/DATABASE?sslmode=verify-full'
  };
```

## Changing a bound

There is no `ALTER CLIENT`. Change either bound the same way as any other part of a client
definition: `DROP CLIENT` and `CREATE CLIENT` in one transaction. A client that runtime entities
reference is replaced together with them, under the existing transactional quiesce contract for
those entities.
