# Entity Labels

Labels describe named entities in Nervix. They are a map of string keys to string values, managed
through Plexus and committed with the entity. Each owner keeps its own labels. Labels have no
effect on permissions, authentication, placement, or metric dimensions.

Domains, logical resources, cluster users and every domain Model support labels. Models include
schemas, all wire-schema kinds, clients, ingestors, emitters, processors, relays, configured
branches, virtual hosts, endpoints, codecs, signaling protocols, generators, lookups, UDFs and
placement definitions. Models and resource versions do not inherit labels from their domain or
resource. Runtime branches, partitions, execution instances, resource versions, sessions and
subscriptions have no label maps. Physical node tags follow the
[node settings](node-settings.md) contract.

## Keys, Values And Limits

A key is nonempty lowercase ASCII. Its first character is a letter or digit; subsequent
characters may also be `.`, `_`, `/` or `-`. Thus `owner`, `team/path` and `region-1` are valid.
Punctuation is part of the exact key, with no hierarchy or path interpretation.

A value is a case-sensitive UTF-8 string and may be empty. Nervix preserves it exactly, without
trimming, case conversion or normalization. An empty value differs from a missing key.
Labels are visible descriptive metadata, so they are unsuitable for secrets.

The committed [cluster parameters](node-settings.md#typed-definitions-and-defaults) bound each complete set:

| Parameter | Default | Meaning |
| --- | --- | --- |
| `ENTITY_LABELS_MAX_COUNT` | 64 | Entries per entity |
| `ENTITY_LABELS_MAX_KEY_BYTES` | 128 B | UTF-8 bytes per key |
| `ENTITY_LABELS_MAX_VALUE_BYTES` | 1024 B | UTF-8 bytes per value |
| `ENTITY_LABELS_MAX_BYTES` | 16384 B | Combined key and value bytes per entity |

The complete resulting set must fit all four limits. Duplicate keys in one authored operation
are errors, even when their values agree. A limit reduction that would invalidate committed
labels is refused atomically. Existing command, protocol and archive bounds also apply.

## Creation And Editing

The optional trailing `LABELS` clause creates the initial set:

```plexus
CREATE SCHEMA readings ( value F32 )
  LABELS { 'owner' = 'Operations', 'team/path' = 'Platform / East' };
CREATE UNPACED DOMAIN orders LABELS { 'owner' = 'Operations' };
CREATE RESOURCE orders_schema LABELS { 'status' = 'draft' };
CREATE USER orders_reader WITH PASSWORD 'example-password' LABELS { 'purpose' = 'Audit' };
```

Omission creates an empty set. `CREATE IF NOT EXISTS` leaves an existing entity and its labels
unchanged.

`SET LABELS` merges assignments, replacing the values of named keys and preserving other keys.
`DROP LABELS` removes named keys; an absent key is already removed. Both commands require the
target entity to exist and validate the complete result before any effect.

```plexus
SET LABELS ON SCHEMA readings { 'owner' = 'Platform', 'empty' = '' };
DROP LABELS ON SCHEMA readings { 'team/path', 'already-absent' };
```

Inspect the result in a separate request:

```plexus
SHOW LABELS ON SCHEMA readings FORMAT JSON;
SHOW LABELS ON DOMAIN orders;
SET LABELS ON RESOURCE orders_schema { 'status' = 'approved' };
DROP LABELS ON USER orders_reader { 'purpose' };
```

Targets use the entity's established addressing phrase. For example, a connector is addressed
as `INGESTOR intake` or `EMITTER archive`; a wire schema retains its kind as
`WIRE JSON SCHEMA payload`, `WIRE CBOR SCHEMA payload` or `WIRE AVRO SCHEMA payload`.
Models and resources resolve in the selected domain. `DOMAIN <name>` identifies a domain
directly, and `USER <name>` identifies a cluster account. Named domain and user targets work
without a selected domain.

## Transactions And Inspection

Model and resource label edits can run alone or in the selected domain's `BEGIN`, `COMMIT`
and `REVERT` lifecycle. A transaction applies resource edits in statement order, including edits
to a resource created earlier in that transaction. The attached session reads its staged labels.
Other sessions observe committed labels until the commit applies the edit. Conflicting edits
follow the same candidate and commit checks as configuration changes. Reversion discards queued
changes. Domain and user label edits are administrative commands and are refused inside a
transaction. Their reads show committed labels even in a session with an attached transaction.

`SHOW LABELS` returns one bounded typed result containing the resolved owner identity, complete
label map and a committed revision or an explicitly staged transaction view. `FORMAT TEXT` is
the default; `FORMAT JSON` is a projection of that same result. Clients receive the typed result
and do not reconstruct it from display text. Keys render in lexicographic order.
Send the inspection as a single-statement request. Inside a transaction it reads beside the
queue without appending an operation or changing the transaction's position.

The result also carries the committed policy revision and the four limits captured with the read.
A client validates the received complete set against that policy and its transport's outer bounds.

`DESCRIBE DOMAIN` and `DESCRIBE RESOURCE <name>` include labels in the text and typed result.
Individual resource version descriptions omit labels. Model `DESCRIBE` and canonical
`SHOW CREATE` expose the same metadata where those commands exist. Canonical creation text
preserves every key and exact value.

## Execution And Durability

Labels are control-plane metadata. A label-only commit leaves execution and state fingerprints,
tasks, branch state, allowances, resource bindings, connections, buffered work and acknowledgement
outcomes intact. It requires no pause, force flush, rebuild, reconnection or relocation. It keeps
every unit on the node the current schedule placed it on, even while that node is down or
cordoned, so it needs no ownership handoff and commits while a unit's owner is unavailable.
Placement changes only through configuration changes, relocation, drain and failover. A mixed
transaction still applies the normal safety contract for its actual configuration changes.
Client endpoint contracts and resolved branch-key schemas use configuration only. Later
configuration edits retain attachments and state whenever their actual execution contracts allow it,
even after metadata was edited.

Labels persist with the entity through registry snapshots, consensus replay, restart and archive
restore. Resource uploads and rebinds leave the logical resource's map intact. Editing a user's
labels leaves the account's credential intact. A domain archive includes its domain, Model and
resource catalog labels. A cluster archive also includes user labels. `WITHOUT STATE` and
`WITHOUT RESOURCES` retain the labels in their original archive scope. The latter omits resource
version bytes, so normal restore requirements for completed versions still apply.

Where a drop command exists, dropping an entity removes its metadata and recreation starts with
the explicitly supplied labels. A supported rename, including `RESTORE DOMAIN ... AS`, retains
labels. Domain, logical resource and user creation and inspection do not add drop or rename
commands for those owners.

[Transaction Quiescence](transaction-quiescence.md) owns commit effects and pause classification.
[Execution Plans](execution-plans.md) owns the separation between configuration and running work.
[Backup And Restore](backup-and-restore.md) owns archive selection and lifecycle.
