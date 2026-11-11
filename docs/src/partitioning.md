# Inherited Partitions

A direct branch divides keys by their field fingerprint. An inherited branch follows the external
partition in which its producer received the record. The branch's fields still identify the
business key; the external coordinate decides which execution unit owns it. See
[Relay Partitions](relay.md#relay-partitions) for buffers, ordering, replicas and direct partitioning.

## Automatic And Grouped Inheritance

```plexus
CREATE SCHEMA tenant_key (tenant STRING);
CREATE BRANCH by_tenant SCHEMA tenant_key TTL 5m PARTITIONS INHERITED;
```

`PARTITIONS INHERITED` follows the observed partition count. A topic with twelve partitions gives
the branch twelve partitions, with external partition `p` assigned to branch partition `p`.
Until the external topology is observed, the branch awaits topology and has no execution units.
It never assumes a partition count of one. An observed unpartitioned Pulsar topic has one external
coordinate, zero.

```plexus
CREATE SCHEMA account_key (account STRING);
CREATE BRANCH by_account SCHEMA account_key TTL 5m PARTITIONS 4 INHERITED;
```

`PARTITIONS n INHERITED` declares a fixed number of groups, from 1 through 65535. External
partition `p` belongs to group `p mod n`. With twelve external partitions and four groups, group
zero reads partitions 0, 4 and 8; group one reads 1, 5 and 9. Every producer of the branch uses the
same committed grouping.

More groups than external partitions are allowed. With twelve external partitions and sixteen
groups, groups 12 through 15 are idle. `DESCRIBE BRANCH` reports the resolved count, external count
and idle groups. An automatic count above 65535 cannot be installed; a declared grouping can fold
a larger enumerable set into the supported number of branch partitions.
The largest native partition coordinate must also fit the connectors' signed 32-bit metadata.

## Producers And Key Identity

Kafka topic and Pulsar topic ingestors can construct inherited keys. Their immutable transport
positions supply the external partition. Branch assignments construct the key fields and cannot
choose the partition through an expression.

Other constructing ingestors and constructing reingestor routes are refused. Routes that preserve
an inherited key may continue to carry it through the graph. A key's canonical field text and
fingerprint remain field-based; changing its external partition does not create a different
business identity.

Co-producing topics must have equal observed partition counts. They must also publish each key
consistently to its partition to preserve that key's source order. Nervix checks the observed
counts; it does not reconstruct an undeclared publisher's hash function. A count mismatch is
reported through the ingestors' general error policies, suspends their intake and retains the last
committed layout and state until coherent observations are available.

## Reader Placement

With Kafka `OFFSET BY DOMAIN INSTANCES PER PARTITION`, instance `p` reads the external partitions
of branch group `p`. Its native offset originator belongs to that instance. The reader, branch
partition and an emitter using `INSTANCES PER PARTITION` can therefore execute together without an
interconnect relay hop. Grouping changes retain each external partition's acknowledged next offset.

With Pulsar `TYPE EXCLUSIVE` or `TYPE FAILOVER`, `INSTANCES PER PARTITION` requires inherited
partitions. Each instance consumes its group's explicit partition topics. Empty groups remain
idle and become active when a later committed topology assigns them a topic. `metadata.partition`
exposes the native partition index; an unpartitioned topic's native value is `-1`, which inheritance
maps to external coordinate zero.

Kafka consumer groups and Pulsar `TYPE SHARED` with `INSTANCES n` keep their broker-managed reader
assignment. Records still inherit their native partition and travel to its branch owner when the
reader runs elsewhere. A count of reader instances does not change the inherited grouping.
These readers admit only the committed external set. Kafka leaves an unadmitted partition's offset
unstored and pauses and rewinds that partition; Pulsar subscribes to the committed child topics.
Kafka confirms the exact unread starting offset in its consumer group before retiring that
reader, so a subsequent `START AT LATEST` join also resumes at the held record.
The next topology publication replaces the readers to admit newly observed partitions.

## Topology Changes And State

The leader observes every branch's producing topics and publishes the coherent external set,
grouping, reader layout and partition epoch together. Broker observations belong to the exact
configuration and leader tenure that requested them. A delayed result cannot replace a newer
configuration or a successor leader's decision. Publication changes the affected entities while
the rest of the domain continues running.

Before readers change, Nervix drains admitted work, including pending key claims, and transfers
committed offsets into the new reader layout. Intake resumes after the affected owners install the
new schedule. Kafka observation follows the leader's configured `kafka-partition-watch-interval`.
Pulsar observation and topology observer reconciliation use `partition-topology-poll-interval`.

Growing a topic from twelve to sixteen partitions advances the branch epoch. An automatic branch
then has sixteen partitions. A four-group branch keeps four groups, and the new external partitions
join their modulo groups. A source replacement also advances the epoch when its count stays the
same, because the new publisher may place a key differently.

When a key reappears, its new owner locates its retained state and claims it before processing the
waiting input. Inherited claims use the admitted origin and epoch, since a field fingerprint cannot
identify an unknown publisher's partition. Materialized reads likewise locate a retained row before
applying a default or treating it as absent. Checkpoints and backups retain the external origin so
restoring under a different group count uses that origin.

`ALTER BRANCH ... SET PARTITIONS INHERITED` and `ALTER BRANCH ... SET PARTITIONS n INHERITED`
select automatic or grouped inheritance. As with a direct recount, an operator must let an ongoing
key transition settle before changing the declaration again. External topology observations can
advance while idle keys remain retained; their holding units remain scheduled until those keys are
claimed or expire.

Backups capture the observed topology, current and retained partition epochs, holding units and
installed intake counts together with state. Restore installs that captured coordinate before
discovery, including when a broker is temporarily unavailable. This keeps retained keys and native
Kafka offsets addressable until a fresh observation is committed.
