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

```nspl
CREATE [IF NOT EXISTS] HASH MAP <name>
  KEY <field>
  FROM RESOURCE <resource>
  [VERSION <n>]
  PATH '<file>'
  DECODE USING <codec>;
```

The hash map loads records from a versioned `RESOURCE` file. The file is decoded through the declared codec, and the `KEY <field>` value becomes the lookup key. If `VERSION <n>` is omitted, the hash map resolves the latest uploaded resource version when the model is created.

Resource files are typically newline-delimited encoded records, such as JSON Lines when the codec uses a JSON wire schema:

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
  FROM RESOURCE zip_codes
  PATH 'lookup.jsonl'
  DECODE USING zip_code_entry_codec;
```

Direct lookup commands are session/control commands:

```nspl
DESCRIBE HASH MAP <name>;
LOOKUP <name> KEY '<key>';
```

`DESCRIBE HASH MAP` reports the loaded resource version, path, codec, owner/replica placement, key field, and entry count. `LOOKUP` returns the matching decoded record when the key exists.

Expressions can call `LOOKUP_HASH_MAP`:

```nspl
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
