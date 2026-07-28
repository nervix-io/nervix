# Your First Pipeline: Kafka To Redis

The first graph is deliberately trivial: JSON order records arrive on a Kafka topic, Nervix
decodes them, and an emitter publishes them to a Redis Pub/Sub channel. Along the way you meet
every building block that later steps reuse: domains, schemas, codecs, relays, clients, ingestors,
and emitters.

## Provision The Topic

Nervix never creates external entities, so create the Kafka topic first:

```bash
docker exec broker /opt/kafka/bin/kafka-topics.sh \
  --create --topic orders --bootstrap-server localhost:9092
```

## Create A Domain

Every entity in Nervix lives in an explicit domain — there is no implicit default. Open
`nervix-cli` and create one, then switch the session to it:

```nspl
CREATE UNPACED DOMAIN quickstart;

USE quickstart;
```

An unpaced domain admits records as they arrive, with no domain clock pacing — the right choice
for a first pipeline. See [Domains And Time](domains-and-time.md#unpaced-domains). `USE` is a
client-local statement that scopes the rest of your session to the domain.

## Open A Transaction

Submitting more than one model statement at a time requires a transaction, and it is also how a
graph is assembled atomically — nothing takes effect until `COMMIT;`. See
[NSPL Overview](nspl-overview.md) for the statement surface.

```nspl
BEGIN;
```

The graph declarations in the rest of this page run inside this transaction.

## Describe The Data

Nervix separates what a record looks like inside the graph from how it is serialized on the wire.
The internal schema is exact-typed; the wire schema describes the JSON payload in the wire
format's own type names; the codec bridges the two. See
[Schemas And Codecs](schemas-and-codecs.md#internal-schemas).

```nspl
CREATE SCHEMA order_record (
  order_id STRING,
  customer STRING,
  status STRING,
  amount I64,
  quantity I64
);

CREATE STRICT WIRE JSON SCHEMA order_wire (
  order_id string,
  customer string,
  status string,
  amount integer,
  quantity integer
);

CREATE CODEC order_codec
  FROM WIRE JSON SCHEMA order_wire
  TO SCHEMA order_record;
```

`STRICT` rejects payload fields that are not declared; `LOOSE` would drop them instead
([Wire Schemas](schemas-and-codecs.md#wire-schemas)). There are no implicit casts anywhere in
Nervix, so the codec is the only place where wire values become typed internal values. A
`DATETIME` field would additionally need an explicit encoding such as
`ENCODE created_at AS RFC3339` ([Codecs](schemas-and-codecs.md#codecs)).

## Create A Relay

Relays are the named streams that connect runtime nodes. Each relay carries records of exactly one
schema. See [Streams And State](relay.md).

```nspl
CREATE RELAY orders SCHEMA order_record UNBRANCHED;
```

`UNBRANCHED` means one shared stream. [Branched Processing](./quickstart-branched-processing.md)
introduces the alternative.

## Declare The External Connections

Connection endpoints never live on ingestors or emitters; they are named `CLIENT` objects that
nodes reference. Kafka client config is passed through to the Kafka driver, so
`bootstrap.servers` uses its familiar key.

```nspl
CREATE CLIENT kafka_local
  TYPE KAFKA
  CONFIG {
    'bootstrap.servers' = '127.0.0.1:9092'
  };

CREATE CLIENT redis_local
  TYPE REDIS
  CONFIG {
    'addr' = 'redis://127.0.0.1:6379/'
  };
```

## Ingest From Kafka

An ingestor brings external data into the graph. It decodes each payload with the codec and routes
the result to a relay:

```nspl
CREATE INGESTOR kafka_orders
  FROM KAFKA kafka_local
  TOPIC orders
  OFFSET BY CONSUMER GROUP quickstart
  MODE ACK SEQUENTIAL
  DECODE USING order_codec
  TO orders
    INHERIT ALL
    UNBRANCHED
    FLUSH EACH 100ms MAX BATCH SIZE 1MiB
    ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Reading top to bottom:

- `OFFSET BY CONSUMER GROUP` tracks progress in Kafka; `MODE ACK SEQUENTIAL` acknowledges records
  in order. The full Kafka source grammar is in
  [Supported Ingestor Types](ingestors.md#supported-ingestor-types).
- `TO orders` starts a route. Routes begin empty: `INHERIT ALL` copies every decoded field into
  the outgoing record, and later steps show `SET` constructing new values. See
  [The Working Message](working-message.md).
- Every route needs an explicit flush policy — `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or
  `FLUSH IMMEDIATE`. There are no hidden defaults.
- `ON MESSAGE ERROR LOG` is the route's per-record error policy;
  `ON GENERAL ERROR LOG` is the node-wide policy
  ([Runtime Node Error Policies](nspl-overview.md#runtime-node-error-policies)).

## Emit To Redis

An emitter drains a relay out of the graph. The Redis integration is Pub/Sub
([Supported Emitter Sinks](emitters.md#supported-emitter-sinks)), and the same codec that decoded
the input encodes it back to JSON:

```nspl
CREATE EMITTER redis_orders
  FROM orders
  ENCODE USING order_codec
  TO REDIS PUBSUB redis_local CHANNEL orders_out
  INHERIT ALL
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

## Commit And Start

`COMMIT;` applies the structural changes: the whole graph is validated and installed atomically.
Nothing processes data yet — that is the job of `START;`, the domain lifecycle command that begins
actual work ([Start And Stop](domains-and-time.md#start-and-stop)):

```nspl
COMMIT;

START;
```

## The Complete Script

```nspl
CREATE UNPACED DOMAIN quickstart;

USE quickstart;

BEGIN;

CREATE SCHEMA order_record (
  order_id STRING,
  customer STRING,
  status STRING,
  amount I64,
  quantity I64
);

CREATE STRICT WIRE JSON SCHEMA order_wire (
  order_id string,
  customer string,
  status string,
  amount integer,
  quantity integer
);

CREATE CODEC order_codec
  FROM WIRE JSON SCHEMA order_wire
  TO SCHEMA order_record;

CREATE RELAY orders SCHEMA order_record UNBRANCHED;

CREATE CLIENT kafka_local
  TYPE KAFKA
  CONFIG {
    'bootstrap.servers' = '127.0.0.1:9092'
  };

CREATE CLIENT redis_local
  TYPE REDIS
  CONFIG {
    'addr' = 'redis://127.0.0.1:6379/'
  };

CREATE INGESTOR kafka_orders
  FROM KAFKA kafka_local
  TOPIC orders
  OFFSET BY CONSUMER GROUP quickstart
  MODE ACK SEQUENTIAL
  DECODE USING order_codec
  TO orders
    INHERIT ALL
    UNBRANCHED
    FLUSH EACH 100ms MAX BATCH SIZE 1MiB
    ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;

CREATE EMITTER redis_orders
  FROM orders
  ENCODE USING order_codec
  TO REDIS PUBSUB redis_local CHANNEL orders_out
  INHERIT ALL
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;

COMMIT;

START;
```

## Send An Order

Subscribe to the output channel in one terminal:

```bash
docker exec redis redis-cli SUBSCRIBE orders_out
```

Produce an order in another:

```bash
docker exec -i broker /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders
```

```json
{"order_id":"o-1001","customer":"acme","status":"new","amount":1500,"quantity":3}
```

The Redis subscriber prints the record back as JSON. You can also watch any relay from inside
Nervix with a read-only session subscription ([Sessions](sessions.md)):

```bash
nervix-cli --domain quickstart subscribe watch orders
```

The pipeline works end to end. Next, add logic to it in
[Conditional Routing](./quickstart-conditional-routing.md).
