#  Lookups

Lookups are resource-backed reference data models that can be queried directly or used inside
structured NSPL expressions.

## Choosing An Enrichment Mechanism

Choose by ownership and update path:

| Need | Mechanism | Cost shape |
| --- | --- | --- |
| Reference data changes when an operator uploads a resource version | `HASH MAP` with `LOOKUP_HASH_MAP` | Static in-memory lookup data for the selected resource version |
| Live state is keyed by the current branch | `USING MATERIALIZED STATE` with `relay_state.<relay>.<field>` | Branch-local state persisted and snapshot-replicated with the runtime node |
| Live data is keyed differently from the current branch | Re-key through a `REINGESTOR` into the branch that owns the data, or join outside Nervix in an external store | Reingestion adds a graph stage and creates branch instances under the new key; an external join leaves storage and query cost outside Nervix |

Materialized dependencies require the exact compatible branch. See
[Materialized relay state](processors.md#materialized-relay-state) for the linked `REQUIRED WAIT`,
`REQUIRED SKIP`, and `DEFAULT` semantics. Do not use a materialized relay as a cross-branch index.

This boundary is structural. Hash maps amortize a versioned reference dataset. Materialized state
amortizes the latest record inside the branch that owns it. Neither mechanism turns runtime state
into an arbitrary-key query service.

The current lookup model is a hash map:

```nspl,ignore
CREATE [IF NOT EXISTS] HASH MAP <name>
  KEY <field>
  FROM RESOURCE <resource> VERSION <n>|LATEST
  PATH '<file>'
  DECODE USING <codec>;
```

The hash map loads records from a versioned `RESOURCE` file in the same domain as the hash map. The
file is decoded through the declared codec, and the `KEY <field>` value becomes the lookup key.
`VERSION` is mandatory. A number binds that exact completed version. `VERSION LATEST` resolves the
highest completed version when the statement is applied and stores the resulting number, so later
uploads and restarts do not move the hash map to another version. `SHOW CREATE HASH MAP` renders the
stored number. Creation succeeds only after the selected file has decoded and the index is usable
on every current live node, including while the domain is stopped. The next command may run
`LOOKUP` immediately; malformed input fails creation.

A resource file is read line by line, and each non-blank line is one payload for the codec. With a
schemaful codec, such as a JSON wire schema over JSON Lines, each line is one entry:

```nspl
CREATE RESOURCE zip_codes;
UPLOAD RESOURCE zip_codes VERSION './lookups/zip_codes';

CREATE SCHEMA zip_code_entry (
  zip STRING,
  city STRING,
  region STRING OPTIONAL
);

CREATE WIRE JSON SCHEMA zip_code_entry_wire MODE STRICT (
  zip string,
  city string,
  region string OPTIONAL
);

CREATE CODEC zip_code_entry_codec
  FROM WIRE JSON SCHEMA zip_code_entry_wire
  TO SCHEMA zip_code_entry;

CREATE HASH MAP zip_codes_by_zip
  KEY zip
  FROM RESOURCE zip_codes VERSION 1
  PATH 'lookup.jsonl'
  DECODE USING zip_code_entry_codec;
```

A JAQ-backed codec [unfolds](schemas-and-codecs.md#unfolding-payloads) each line, so one line may
contribute zero, one, or several entries, in the order its program yields them. When two entries
carry the same key, whether from two lines or from one, the later entry replaces the earlier one.
A line that fails to decode fails the hash map's creation with a diagnostic naming the line.

Direct lookup commands are session/control commands:

```nspl,ignore
DESCRIBE HASH MAP <name>;
LOOKUP <name> KEY '<key>';
```

`DESCRIBE HASH MAP` reports the loaded resource version, path, codec, owner/replica placement, key
field, and entry count, which counts distinct keys. `LOOKUP` returns the matching decoded record
when the key exists. These are read-only control-plane commands: they do not execute the graph or
require a running domain clock, so a loaded hash map remains directly queryable while its domain is
stopped.

Expressions can call `LOOKUP_HASH_MAP`:

```nspl,ignore
LOOKUP_HASH_MAP("<hash_map>", <key_expr>, "<field>")
```

The function evaluates `<key_expr>`, looks up that key in the named hash map, and returns the requested field from the matched lookup record. A missing key or missing optional field returns null. Referencing a field that is not present in the hash map schema is a statement-validation error.

Example enrichment:

```nspl
CREATE BRANCH by_zip
  SCHEMA zip_branch TTL 5m;

CREATE DEDUPLICATOR enrich_zip
  FROM inbound
  DEDUPLICATE ON input.zip
  MAX TIME 10m
  BRANCHED BY by_zip
  TO enriched
    INHERIT ALL
    SET city = LOOKUP_HASH_MAP("zip_codes_by_zip", input.zip, "city"),
        region = LOOKUP_HASH_MAP("zip_codes_by_zip", input.zip, "region")
    WHERE NOT is_null(output.city)
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG;
```

Lookup models are domain-owned. A hash map name must be unique within the active domain, and `CREATE IF NOT EXISTS HASH MAP ...` follows the same idempotent-create behavior as other model create statements.
