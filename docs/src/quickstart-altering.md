# Altering A Running Graph

Nothing built so far required tearing anything down — every step added nodes. `ALTER` statements
change what already exists: junction logic, relay properties, schemas, ingestor sources, emitter
sinks, stateful processor behavior, reingestor wiring, and generator cadence or routes. Nervix
classifies each validated change and pauses as little as possible — the statement result reports
the executed level
([ALTER Lock And Quiesce Classification](control-plane.md#alter-lock-and-quiesce-classification)):

- `DYNAMIC` — hot-applied; nothing pauses (relay capacity, junction filters and route logic,
  deduplicator or reorderer `MAX TIME`, emitter flush policy)
- `ENTITY_PAUSE` — only the affected node or relay is gated and drained (topology changes,
  deduplication keys, reorderer ordering, ingestor, reingestor, generator, and emitter
  reconfiguration, relay materialized state — the `ALTER RELAY` in
  [Generators](./quickstart-generators.md) ran at this level)
- `DOMAIN_PAUSE` — domain ingestion stops, in-flight work drains, the new graph is installed
  atomically, and flow resumes (schema and branching changes)

Before altering anything, `SHOW CREATE <kind> <name>;` prints the current canonical definition.

## Change Routing Logic — Dynamic

Lower the high-value threshold from `1000` to `500`. Route changes go through
[`ALTER JUNCTION`](processors.md#altering-junctions); `REPLACE ROUTE` requires the **complete**
route body, not a diff:

```nspl
ALTER JUNCTION route_orders
  REPLACE ROUTE TO high_value_orders
    INHERIT ALL
    SET tier = CASE
          WHEN input.amount >= 10000 THEN "vip"
          ELSE "high"
        END
    WHERE output.amount >= 500
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG;
```

The result reports `quiesce level: DYNAMIC` — the domain never paused, buffered work stayed in
place, and the very next order is evaluated against the new threshold. Produce a `700`-amount
order and watch it reach `high_value_out`, which the old threshold would have dropped.

Relay capacity is equally uneventful ([Altering Relays](relay.md#altering-relays), capacity
semantics under [Capacity](relay.md#capacity)):

```nspl
ALTER RELAY orders SET CAPACITY 8;
```

## Evolve A Schema — Domain Pause

Payments gain a `method`. A codec requires its wire schema to exactly match the internal schema,
so both must change in **one transaction** — the registry validates the complete candidate graph
before anything applies ([Altering Schemas](schemas-and-codecs.md#altering-schemas)):

```nspl
BEGIN;

ALTER WIRE JSON SCHEMA payment_wire
  ADD FIELD method string OPTIONAL;

ALTER SCHEMA payment_record
  ADD FIELD method STRING OPTIONAL;

COMMIT;
```

This reports `quiesce level: DOMAIN_PAUSE`: ingestion stops, buffered output is force-flushed (no
in-flight records are lost), the graph is swapped, and flow resumes — there is no user-facing
`PAUSE` statement, and a drain timeout rolls the whole batch back
([Automatic Model-Alteration Quiescing](domains-and-time.md#automatic-model-alteration-quiescing)).

Add the field as `OPTIONAL`: old payloads without it keep decoding, and the field finalizes as a
typed null. Verify by POSTing a payment with `"method":"card"` to `/payments` and watching the
`payments` relay.

Two rules worth knowing before you lean on this:

- One alteration at a time per domain — a concurrent mutation is rejected with
  `already has a model alteration in progress`, not queued.
- `DROP` plus `CREATE` of the same entity in one batch is compared as **one modification** — you
  cannot sneak a schema change past domain quiescing by recreating the relay.

Ingestors and emitters have their own operation sets —
[Altering Ingestors](ingestors.md#altering-ingestors) (every ingestor change is entity-pause) and
[Altering Emitters](emitters.md#altering-emitters) (flush changes are dynamic; sink, client, and
codec changes are entity-pause; source-list membership changes are domain-pause). Processor
operations are covered under
[Altering Deduplicators](processors.md#altering-deduplicators),
[Altering Reorderers](processors.md#altering-reorderers),
[Altering Reingestors](processors.md#altering-reingestors), and
[Altering Generators](processors.md#altering-generators).

Next: run the graph on a logical clock in [Paced Domains](./quickstart-paced-domains.md).
