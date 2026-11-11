# Entity Labels

Labels describe persisted domain entities in Nervix. They are a map of string keys to string
values, managed through Plexus and committed with the entity. They do not grant permissions,
select placement, inherit from another entity, or become metric dimensions.

Every domain Model supports labels, including schemas, all wire-schema kinds, clients,
ingestors, emitters, processors, relays, configured branches, virtual hosts, endpoints, codecs,
signaling protocols, generators, lookups, UDFs and placement definitions. Entities that bind
resources retain those bindings when labels change.
Domains, logical resources and users are outside this surface. Runtime branches, partitions,
execution instances, resource versions, sessions and subscriptions are not separately labelable.
Physical node tags retain their separate [node settings](node-settings.md) contract.

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
```

Targets use the entity's established addressing phrase. For example, a connector is addressed
as `INGESTOR intake` or `EMITTER archive`; a wire schema retains its kind as
`WIRE JSON SCHEMA payload`, `WIRE CBOR SCHEMA payload` or `WIRE AVRO SCHEMA payload`.
The selected domain remains part of the target's typed identity.

## Transactions And Inspection

Label edits participate in the domain's `BEGIN`, `COMMIT` and `REVERT` lifecycle. An inspection
inside a transaction reads its coherent staged candidate, including earlier label edits. Other
sessions observe committed labels. Conflicting edits are subject to the same candidate and
commit fences as configuration changes. Reversion publishes nothing.

`SHOW LABELS` returns one bounded typed result containing the domain entity identity, complete
label map and a committed revision or an explicitly staged transaction view. `FORMAT TEXT` is
the default; `FORMAT JSON` is a projection of that same result. Clients receive the typed result
and do not reconstruct it from display text. Keys render in lexicographic order.
Send the inspection as a single-statement request. Inside a transaction it reads beside the
queue without appending an operation or changing the transaction's position.

The result also carries the committed policy revision and the four limits captured with the read.
A client validates the received complete set against that policy and its transport's outer bounds.

`DESCRIBE` and canonical `SHOW CREATE` expose the same metadata where those commands exist.
Canonical creation text preserves every key and exact value and can recreate the complete set.

## Execution And Durability

Labels are control-plane metadata. A label-only commit leaves execution and state fingerprints,
tasks, branch state, allowances, resource bindings, connections, buffered work and acknowledgement
outcomes intact. It requires no pause, force flush, rebuild, reconnection or relocation. A mixed
transaction still applies the normal safety contract for its actual configuration changes.
Client endpoint contracts and resolved branch-key schemas use configuration only. Later
configuration edits retain attachments and state whenever their actual execution contracts allow it,
even after metadata was edited.

Labels persist with the entity through registry snapshots, consensus replay, restart and archive
restore. `WITHOUT STATE` omits runtime state and retains labels. Dropping an entity removes its
metadata; recreation starts with only its explicitly supplied labels. A supported rename retains
labels and introduces no new rename operation.

[Transaction Quiescence](transaction-quiescence.md) owns commit effects and pause classification.
[Execution Plans](execution-plans.md) owns the separation between configuration and running work.
[Backup And Restore](backup-and-restore.md) owns archive selection and lifecycle.
