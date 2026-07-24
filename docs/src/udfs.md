# User-Defined Functions

NSPL user-defined functions (UDFs) are domain-owned functions written in Roto and invoked anywhere
an ordinary expression function can be used. They execute in process over Arrow columns: one UDF
invocation receives a complete batch, and column methods perform vectorized work without
serializing rows.

UDF creation is trusted-code administration, like uploading a WASM resource. Roto code is native
JIT-compiled code, not a sandbox for third-party programs.

Roto execution is synchronous. Nervix therefore schedules every UDF-bearing expression on its
blocking worker pool, including small Arrow batches and UDFs used while constructing branch keys,
so native UDF work does not occupy an asynchronous runtime worker.

## Declaration

```nspl
CREATE [IF NOT EXISTS] UDF <name>
  WITH ROTO_0_11
  ARGS (<argument> <type> [OPTIONAL], ...)
  RETURNS <type> [OPTIONAL]
  [VOLATILE]
  CODE $roto$
    fn <name>(...) -> ... {
        ...
    }
  $roto$;
```

A declaration has between one and eight arguments. Argument and return types use the schema type
grammar: all scalar types, `VEC<T>`, and `ARRAY<T, n>` are accepted. Code is limited to 64 KiB.
The declared signature must exactly match the named Roto entry function; implicit casts are never
inserted.

Dollar-quoted strings (`$$...$$` and `$tag$...$tag$`) are verbatim and may span lines. They are
valid wherever NSPL accepts a string literal.

For example:

```nspl
CREATE UDF risk_band
  WITH ROTO_0_11
  ARGS (score F64)
  RETURNS STRING
  CODE $roto$
    fn risk_band(score: F64Column) -> StringColumn {
        when_s(score.gt_s(0.9), "critical")
            .when_s(score.gt_s(0.7), "high")
            .otherwise_s("normal")
    }
  $roto$;
```

The Roto column names for scalar declarations are `U8Column`, `I8Column`, `U16Column`,
`I16Column`, `U32Column`, `I32Column`, `U64Column`, `I64Column`, `F32Column`, `F64Column`,
`BoolColumn`, `StringColumn`, and `DatetimeColumn`. A one-level sequence uses
`VecU8Column` through `VecDatetimeColumn`; deeper nesting uses `AnyColumn`.

## Calling UDFs

Calls use the dedicated, case-insensitive `udf::` qualifier and compose with fields, literals,
builtins, and other call sites:

```nspl
CREATE JUNCTION scoring FROM raw_events
  TO scored_events
    SET band = udf::risk_band(message.score)
    WHERE udf::risk_band(message.score) != 'normal'
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

The NSPL compiler validates exact arity and types when the consuming model is created. Use an
explicit `CAST` when conversion is intended. An unqualified call never resolves to a UDF. The
separate namespace allows a UDF name to equal a builtin name without changing either call when a
later Nervix version adds builtins. Results retain the maximum sensitivity of their arguments.

Roto `test` blocks execute during `CREATE UDF`. If any test rejects, creation fails with
`Roto test block failed` and the UDF is not persisted.

## Nulls, errors, and volatility

`OPTIONAL` controls boundary nullability; it does not change the Roto column type.

- A null row in a non-optional argument strictly produces a null result row without exposing that
  row to the UDF.
- An optional argument exposes its validity bitmap so the body can use null-aware operations.
- A non-optional result may contain nulls only for strict-propagation rows or rows carrying a
  side error. Any other null is a batch error.
- Checked column operations and `reject_where_s` add per-row errors to the same channel used by
  builtins, so the owning route's `ON MESSAGE ERROR` policy applies.
- A Roto trap, invalid result type or row count, or watchdog expiry is a whole-batch error.

The watchdog detects an overrun after native code returns; Roto provides no in-process
preemption. This is why UDF creation is operator-trusted and non-terminating or third-party code
must stay on the isolated WASM processor path.

UDFs are deterministic unless declared `VOLATILE`. Only a volatile UDF can call `now()`,
`rand_f64()`, or `uuid_v4()`. `now()` uses the execution-local domain clock. Deterministic calls
may be reused within one compiled expression; volatile calls are evaluated at every occurrence.
User code is never executed by constant folding.

## Roto column operations

Column methods operate elementwise and preserve the input row count. A base method takes another
column of the identical type; an `_s` method broadcasts a Roto scalar.

The `ROTO_0_11` catalog currently includes:

- integer and floating columns: `add`, `sub`, `mul`, `div`, their `_s` forms, `eq`, `lt`, `gt`,
  their `_s` forms, and batch `min`/`max`;
- boolean columns: `and`, `or`, `not`, plus string-column `select`;
- string columns: `trim`, `contains_s`, `regexp_replace`, and the slow-path `get`;
- `I64Column.cast_f64()` and `DatetimeColumn.lt_s(Timestamp)`;
- `VecStringColumn.contains_s`;
- string `coalesce`, `when_s(...).when_s(...).otherwise_s(...)`, and `reject_where_s`;
- `ColumnBuilder.bool`, `push`, `push_null`, and `finish` for explicit per-row fallback code;
- volatile `now`, `rand_f64`, and `uuid_v4`.

Arithmetic overflow, division by zero, and explicit rejection produce per-row errors. Invalid
scalar configuration such as a malformed regular expression fails the batch.

Use column operations whenever possible. `get` and a builder are the explicit slow path for logic
that has no vectorized kernel.

## Introspection and lifecycle

```nspl
SHOW UDFS;
DESCRIBE UDF risk_band;
SHOW CREATE UDF risk_band;
DROP UDF risk_band;
```

`DESCRIBE UDF` reports the language tag, signature, volatility, content hash, and referencing
nodes. `SHOW CREATE` preserves the Roto source bytes and chooses a safe dollar-quote delimiter.
Dropping a UDF is rejected while an active model references it.

Roto source is persisted; native JIT output is rebuilt in memory during domain activation. A
server that does not support a stored language tag rejects activation rather than reinterpreting
the source.
