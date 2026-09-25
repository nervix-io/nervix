# Expression Functions

This chapter is the reference for NSPL expressions: the operators, conversions, builtin functions,
and window aggregates they can use, with each one's exact types, null handling, errors, and limits.
Expressions appear on ingestors, processors, routes, and emitters, most often in route
construction:

```nspl,ignore
[INHERIT ...]
[SET <field> = <expr>, ...]
[WHERE <expr>]
[INVOKE write_header(<name-expr>, <value-expr>), ...]
```

`SET` assignments and `INVOKE` calls execute left to right. A transforming route begins empty and
may initialize fields with `INHERIT` and `SET`; a set-only route begins empty and supports only
`SET`. Route `WHERE` runs after output finalization. `INVOKE` and `write_header` are emitter-only;
side-effect functions are invalid inside ordinary expressions. See
[The Working Message](working-message.md) for the field scopes a route reads and how they resolve,
and [Where Expressions Run](#where-expressions-run) for every other place an expression appears.

General rules:

- function names are case-insensitive, and an alias such as `ceiling` or `substring` behaves as,
  and is reported under the name of, the function it stands for
- there is no implicit cast insertion; `expr AS TYPE` and `TRY_CAST(expr AS TYPE)` convert
  explicitly, as [Conversions](#conversions) describes, and `JSON_VALUE` reads a value of a declared
  type from JSON text, as [JSON Documents](#json-documents) describes
- argument and result types are validated when the statement is applied, except for the few
  expressions [Where Expressions Run](#where-expressions-run) lists as checked later
- sensitive values retain their sensitivity through expression evaluation; an output field that is
  not sensitive can hold one only through an explicit `leak_sensitive(...)`, as
  [Function Properties](#function-properties) describes
- `now()` returns the execution-local domain time as `DATETIME`, and `uuid_v7()` uses that same
  time when building the UUID

Domain-owned [Roto UDFs](./udfs.md) use the same call syntax and exact typing rules. Builtins take
the unqualified call surface. UDFs use the explicit `udf::` namespace.

The [Catalog](#catalog) at the end of this chapter lists every operator, builtin, and aggregate
with its signature, result, and the section that describes it.

## Choosing An Extension Tier

Use the smallest extension tier that fits the operation:

| Tier | Trust required | Statefulness | Unit of work | Execution | Failure containment |
| --- | --- | --- | --- | --- | --- |
| Builtins | None beyond Nervix itself | Stateless | One expression call | Async-safe runtime execution | Per-row error channel |
| Roto UDFs | Operator-trusted native code | Stateless | Vectorized column function with 1–8 arguments | Blocking worker pool | Per-row errors, whole-batch errors, and a post-return watchdog |
| WASM processors | Isolation suitable for third-party code | Branch-local guest state | Batch-and-route processor with guest-owned emission | One guest instance per branch | Message errors, global errors, and guest traps |

Tenant-supplied or potentially non-terminating logic belongs on the WASM processor path. Roto UDF
creation is operator-trusted administration. See [User-Defined Functions](udfs.md) for the native
execution contract and [Module Sharing And Branch Memory](wasm-processor-guests.md#module-sharing-and-branch-memory)
for the WASM isolation and memory boundary.

## Where Expressions Run

Every operator and builtin on this page is available wherever an expression is, except for the
functions that depend on their context. This table lists those, by the context an expression sits
in:

| Context | Window aggregates | `read_header`, `read_headers` | `LOOKUP_HASH_MAP` | `now()`, `uuid_v4()`, `uuid_v7()` |
| --- | --- | --- | --- | --- |
| Ingestor `FILTER WHERE` and route `INHERIT`, `SET`, and `WHERE` | No | On a header-capable source | Yes | Yes |
| `BRANCHED BY ... SET` on ingestors and reingestors | No | No | Yes | Yes |
| Processor `FROM ... WHERE` and `FILTER WHERE`, and the routes of junctions, deduplicators, reorderers, reingestors, inferencers, and WASM processors | No | No | Yes | Yes |
| Window route `SET` | Yes, and only here | No | No | Yes |
| Window route `WHERE` | No | No | Yes | Yes |
| Generator routes, correlator routes, and `CORRELATE WHERE` | No | No | No | Yes |
| `DEDUPLICATE ON`, reorderer `BY`, and inferencer `INPUTS` | No | No | No | Yes |
| Emitter `FROM ... WHERE`, route `SET`, `WHERE`, and `INVOKE`, HTTP `METHOD` and `PATH`, and SQS `FIFO GROUP` | No | No | Yes | Yes |
| Emitter `VALUES` and OpenTelemetry `ATTRIBUTES` | No | No | No | Yes |
| `ON MESSAGE ERROR SEND TO ... SET` | No | For a failure on a header-capable ingestor | No | Yes |
| Materialized-state `DEFAULT` | No | No | No | No |
| Session subscription `WHERE` | No | No | No | Yes |

- `write_header` is a statement of an emitter route's `INVOKE` clause, never part of an
  expression; see [Header Functions](#header-functions).
- A `udf::` call is available in every context. A materialized-state `DEFAULT` accepts only a UDF
  that is not `VOLATILE`, and reads no field at all: it is built from constants.
- Outside a window route, `count`, `sum`, `first`, `last`, `min`, and `max` are the
  [array and vector functions](#array-and-vector-functions), and every other aggregate name is an
  unknown function, such as `unknown function 'avg' with arity 1`.
- Set-only routes, which are the routes of windows, generators, correlators, inferencers, and WASM
  processors, accept no `INHERIT` and no `INVOKE`.

Reorderer `BY` expressions and emitter `VALUES` and `ATTRIBUTES` mappings are compiled when their
domain's execution is built rather than when the statement is applied, and a subscription's
`WHERE` when the subscription is created, so a type error in one of them is reported then.

## Evaluation Model

Builtins are evaluated over Arrow columns: one call computes its result for every message in a
batch together. A message's result never depends on the other messages in the batch, so batching
does not change any value. The one limit a batch's messages share is the bytes one `STRING` or
`BYTES` column holds; see [Result Size](#result-size). How a function traverses its column, whether
through an Arrow compute kernel, one pass over the column's value buffer, or a loop over its rows,
is internal to the function and does not change its results.

Several builtins run on libraries that choose SIMD instructions at run time from the ones the
node's CPU offers: JSON extraction finds a document's structure that way, base64 and hexadecimal
encoding and decoding process their octets that way, `sha256` uses the SHA instructions of x86-64
CPUs that have them, and substring searches such as `contains_any` and regular expressions scan
text that way. `xxh3_64` uses the SIMD instructions of the CPU target the binary is built for. The
checked numeric kernels and the other passes over value buffers are written as loops the compiler
can turn into vector instructions for that target, and Nervix makes no claim about which of them it
does. None of these choices changes a result. The one exception is the transcendental functions,
whose last places come from the platform's C math library; see [Numeric Functions](#numeric-functions).

The compiler applies three optimizations that preserve results in the same way:

- A deterministic call that cannot fail and whose arguments are all literals may be computed once
  instead of for every batch. It produces exactly the value that evaluating it for each message
  would, so `upper('grüßen')` and `upper(input.text)` agree when `input.text` holds `grüßen`.
- Identical deterministic expressions that cannot fail may be computed once and shared within one
  assignment or clause. Calls that return a new value for every message, such as `uuid_v4()`, and
  calls that can report a per-message error are evaluated at each occurrence, so each occurrence
  reports its own error.
- A literal, and any expression whose arguments are all literals or `now()`, is carried through a
  batch as one value rather than as a column of copies. A function reads it as one value where it
  can and expands it to a column only where a message-by-message operation or an output field
  needs one, so the result is the same either way. When such an expression fails, such as
  `1 / 0`, every message that evaluates it reports the error.

Every operand of an operator or a function is evaluated for every message the expression
evaluates, so `AND`, `OR`, `coalesce`, and the other operators never skip an operand. Only a
[conditional expression](#conditional-expressions) evaluates an operand for some messages and not
others.

## Function Properties

Every builtin follows these rules unless its own description says otherwise:

| Property | Contract |
| --- | --- |
| Types | Arguments are never converted implicitly. A function that accepts several types, such as `abs` over every numeric type, takes each of them as it is. |
| Nulls | A null argument produces a null result. `coalesce`, `nullif`, `concat`, `concat_ws`, `is_null`, `greatest`, and `least` define their own null handling, and a function can also yield null for present arguments where its description says so, such as `regexp_substr` without a match, `nth` past the end of a list, or `url_host` of a URL without a host. |
| Optional results | A result is optional when any argument is. `is_null`, `IS [NOT] DISTINCT FROM`, `now`, `uuid_v4`, `uuid_v7`, and `read_headers` are never optional; `coalesce`, `greatest`, and `least` are optional only when every argument is; and `TRY_CAST`, `JSON_VALUE`, `TRY_JSON_VALUE`, `nullif`, `LOOKUP_HASH_MAP`, `read_header`, `url_host`, `url_port`, `url_query`, `url_fragment`, `url_query_value`, and the list functions `first`, `last`, `nth`, `sum`, `min`, `max`, and `mean` are always optional. An optional result initializes a required field only through an expression that is never null, such as `coalesce(<result>, <default>)`; assigning it directly is rejected when the statement is applied with `SET field '<field>' may be null but the output field is required`. |
| Sensitivity | A result is sensitive when any argument is sensitive, including results such as `length(...)`, `is_null(...)`, and `count(...)` that do not contain the argument's value. A conditional is sensitive when its operand, any condition, or any result is. Literals and calls without arguments are not sensitive. Only `leak_sensitive(...)` removes sensitivity. |
| Volatility | Every builtin is deterministic except `now()`, which returns one value for an execution, and `uuid_v4()` and `uuid_v7()`, which return a new value for every message. |
| Errors | A function that can fail reports a per-message error and yields null for that message. Inside a conditional, only the selected arm can report one. [Errors](#errors) describes how an error reaches the message's error policy. |

Sensitivity is checked where a value is stored, never where it is only tested. Assigning a
sensitive value to an output field that is not sensitive is rejected with `SET field '<field>'
would store sensitive data in a non-sensitive output field; use leak_sensitive(...) to explicitly
remove sensitivity`, while a `WHERE` clause, a deduplication or ordering key, or a subscription
filter may read sensitive fields freely. `INHERIT <field> LEAK SENSITIVE`, written in an explicit
field list, copies a field as `leak_sensitive(input.<field>)` does. Emitters apply their own rules to
the values they send; see [Emitters](emitters.md).

## Errors

A function that cannot compute its result for a message reports an error of one of four kinds:

| Kind | Meaning | Examples |
| --- | --- | --- |
| `division_by_zero` | An integer division or remainder by zero | `integer division by zero`, `integer remainder by zero` |
| `overflow` | A result its type or its column cannot hold | `integer addition overflowed`, `date_add result is outside the DATETIME range`, `repeat result exceeds the text one STRING column holds` |
| `cast_failed` | A value that does not read as the requested type | `cannot cast value to Int64`, `base64_decode input is not valid encoded bytes`, `JSON_VALUE document is not valid JSON` |
| `invalid_argument` | An argument outside the function's domain, or a result that is not finite | `floating-point operation produced a non-finite result`, `invalid regular expression: ...`, `vector lengths differ: left has 3, right has 2` |

The sections below name the kind and message of every failure. A failed message yields null for
the failed operation, and the message then takes its context's error handling:

- A failure in route construction (`INHERIT`, `SET`, route `WHERE`, and `INVOKE`), in an ingestor's
  `FILTER WHERE`, in a generator route, and in an emitter's `FROM ... WHERE`, route, or `VALUES`
  hands the message to the route's `ON MESSAGE ERROR` policy; an ingestor `FILTER WHERE` failure
  goes to the policy of every route. `SET` runs before route `WHERE`, so a `SET` failure is
  reported even for a message that `WHERE` would drop.
- A failure in a processor's `FROM ... WHERE` or `FILTER WHERE`, or in the argument of a window
  aggregate, is logged, and the message is not acknowledged.
- A failure in a session subscription's `WHERE` skips the record, and the session is told how many
  records the filter skipped.
- Deduplication keys, reorderer `BY` expressions, `CORRELATE WHERE` conditions, inferencer `INPUTS`
  mappings, and `BRANCHED BY ... SET` have no message-error route. Keep expressions that can fail
  out of them, or guard them with a conditional, `TRY_CAST`, or `TRY_JSON_VALUE`.

In an `ON MESSAGE ERROR SEND TO ... SET`, a function failure has the `error` scope below. Every
failure an expression reports has the code `evaluation`; the kind appears in the message, which
names the node, the program, the kind, and the reason, such as `junction 'enrich' FILTER-MAP side
error division_by_zero: integer division by zero at 12..18`. The range at the end locates the
failed operation within the route's program. When several operations fail for one message, the
record reports the first.

| Field | Type | Holds |
| --- | --- | --- |
| `error.reference` | `STRING` | A stable UUIDv7 that identifies this failure |
| `error.code` | `STRING` | `evaluation` for every expression failure |
| `error.message` | `STRING` | The message above. It never contains a value of the message, except as the [regular expressions](#regular-expressions) section describes |
| `error.operation` | `STRING` | The clause that failed, such as `set`, `inherit`, `route_where`, `filter_where`, `source_where`, `values`, or `invoke` |
| `error.operation_index` | optional `U32` | The zero-based position of the failed assignment, counting `INHERIT` fields first, or null for a `WHERE` |
| `error.fields` | `VEC<STRING>` | The assigned field and every field the failed expression reads |
| `error.occurred_at` | `DATETIME` | The execution's domain time |

Some failures belong to a batch rather than to a message: a UDF trap, an invalid UDF result, or a
UDF that exceeds its watchdog, a failed key expression of `LOOKUP_HASH_MAP`, and a limit of a
column that a whole batch exceeds, such as the one [`format_datetime`](#writing-values) checks. Such
a failure fails every message of the batch: a processor reports an internal error and leaves the
batch unacknowledged, and an emitter applies its `ON GENERAL ERROR` policy.

A statement whose expression does not type-check is rejected when it is applied, with a message
such as `function 'trim' requires Utf8 input, found Int64`. These messages name types by their Arrow
names:

| NSPL type | Name in messages |
| --- | --- |
| `U8` through `I64` | `UInt8`, `Int8`, `UInt16`, `Int16`, `UInt32`, `Int32`, `UInt64`, `Int64` |
| `F32`, `F64` | `Float32`, `Float64` |
| `BOOL` | `Boolean` |
| `STRING` | `Utf8` |
| `BYTES` | `Binary` |
| `DATETIME` | `Datetime`, or `Timestamp(Nanosecond, Some("+00:00"))` |
| `ARRAY`, `VEC` | `FixedSizeList(...)`, `List(...)`, or `Generic` |

## Literals

| Literal | Type | Form |
| --- | --- | --- |
| Integer | `I64` | Decimal digits, such as `42`. A value above the largest `I64` is rejected. There is no negative literal: `-5` applies unary minus to `5` |
| Float | `F64` | Decimal digits with a fraction, such as `2.5`. An exponent, a leading `.`, and a trailing `.` are not float literals; write `'1e5' AS F64` |
| String | `STRING` | `'text'`, `"text"`, or dollar-quoted `$$text$$` and `$tag$text$tag$`, where the tag is letters, digits, and underscores. A quoted string cannot hold its own quote or a line break; a dollar-quoted string can hold anything but its closing delimiter, line breaks included. No escape sequence is interpreted, so `'a\nb'` is four characters |
| Boolean | `BOOL` | `TRUE`, `FALSE`, in any letter case |
| Null | the type its destination supplies | `NULL`, only as the whole value of an assignment to an optional field or as a result of a conditional whose other results give it a type |

There is no `DATETIME`, `BYTES`, NaN, or infinity literal. Convert a string instead:
`'2024-02-29T12:00:00Z' AS DATETIME`, `hex_decode('00ff')`, `'NaN' AS F64`, or `'inf' AS F64`. A
`NULL` anywhere else is rejected with `NULL requires a declared optional assignment target`; a typed
null comes from a conditional without `ELSE` or from `TRY_CAST`.

Integer literals are `I64` and float literals `F64`, and operands are never converted, so a
literal combined with a value of another width is written with a cast:
`input.priority > 5 AS I32`, `bitwise_and(input.flags, 255 AS U16)`, or `input.ratio * 0.5 AS F32`.
Arguments that take a count of any integer type, such as the digits of `round` and the counts of
the string and shift functions, accept an `I64` literal as it is.

### Operator Precedence

Operators bind in this order, tightest first:

| Level | Operators | Grouping |
| --- | --- | --- |
| 1 | Literals, fields, calls, `[...]`, `IF ... END`, `CASE ... END`, `TRY_CAST(...)`, `JSON_VALUE(...)`, `TRY_JSON_VALUE(...)`, `JSON_EXISTS(...)`, and parentheses | |
| 2 | `<expr> AS <type>` | Repeats from the left: `x AS I64 AS STRING` |
| 3 | Unary `-` and `NOT` | Repeat, the innermost applying first: `- -x` is `-(-x)` |
| 4 | `*`, `/`, `%` | From the left |
| 5 | `+`, `-` | From the left |
| 6 | `=`, `!=`, `<`, `<=`, `>`, `>=`, `IS [NOT] DISTINCT FROM`, `[NOT] IN (...)`, `[NOT] BETWEEN ... AND ...` | From the left |
| 7 | `AND` | From the left |
| 8 | `OR` | From the left |

Three consequences are easy to miss:

- `NOT` binds tighter than comparisons, so `NOT input.count > 5` negates `input.count` and is
  rejected with `operator Not is not valid for Int64`. Write `NOT (input.count > 5)` or
  `input.count <= 5`.
- `AS` binds tighter than unary minus, so `-128 AS I8` casts `128`, which fails. Write
  `(-128) AS I8`.
- An `AS` applies only to the operand it follows, so `input.low + input.high AS STRING` converts
  `input.high` alone, and `input.low + 1 AS I32` adds `1 AS I32`.

Comparisons group from the left like the arithmetic operators, so `a = b = c` compares `a = b` with
`c`, and `a < b < c` is rejected because `a < b` is a `BOOL`. A trailing comma is accepted in the
arguments of a call, the elements of an `IN` set, and the elements of `[...]`.

### Reserved Words

These words are reserved in expressions, including after a field scope such as `input.<field>`:
`WHERE`, `SET`, `INHERIT`, `ALL`, `EXCEPT`, `LEAK`, `SENSITIVE`, `INVOKE`, `AS`, `TRY_CAST`,
`JSON_VALUE`, `TRY_JSON_VALUE`, `JSON_EXISTS`, `AND`, `OR`, `NOT`, `TRUE`, `FALSE`, `NULL`, `IF`,
`CASE`, `WHEN`, `THEN`, `ELSE`, `END`, `IN`, `BETWEEN`, `IS`, `DISTINCT`, `FROM`, and `UDF`. A schema
may declare one of these field names, but an NSPL expression cannot reference it.

## Logical Operators

`AND`, `OR`, and `NOT` take `BOOL` operands and return `BOOL`. They follow three-valued logic, in
which null means an unknown truth value:

| `a` | `b` | `a AND b` | `a OR b` |
| --- | --- | --- | --- |
| `TRUE` | null | null | `TRUE` |
| `FALSE` | null | `FALSE` | null |
| null | null | null | null |

`NOT` of null is null. A `WHERE` selects only the messages whose condition is `TRUE`, so a null
condition drops the message. The result is optional when either operand is, even where the other
operand decides it.

Both operands are evaluated for every message, whatever the first one holds. A failure in the
second operand therefore fails the message even where the first operand already decides the
result: `input.divisor != 0 AND input.total / input.divisor > 10` reports `division_by_zero` for a
message whose divisor is `0`. Guard such an operand with a conditional, which evaluates it only for
the messages that select it:

```nspl,ignore
WHERE CASE WHEN input.divisor = 0 THEN FALSE ELSE input.total / input.divisor > 10 END
```

An operand of another type is rejected: `operator And is not valid for Int64`, or `binary operator
And requires matching operand types, found Int64 and Boolean` for operands of two types.

## Conditional Expressions

NSPL provides three self-delimited conditional forms:

```nspl,ignore
IF <bool-condition> THEN <result> ELSE <result> END

CASE
  WHEN <bool-condition> THEN <result>
  [WHEN <bool-condition> THEN <result> ...]
  [ELSE <result>]
END

CASE <operand>
  WHEN <match-value> THEN <result>
  [WHEN <match-value> THEN <result> ...]
  [ELSE <result>]
END
```

Every form uses first-match-wins ordering. A null condition is not a match, and a null operand never
equals a simple-`CASE` match value. Conditions are `BOOL`, and a simple-`CASE` operand and its match
values have one exact type; the operand is computed once, before any arm. All non-null results must
have the same exact type, which may be any scalar type but not `ARRAY` or `VEC`; implicit casts are
not inserted. An omitted `ELSE` is a typed null and therefore requires an optional destination. An
`IF` always includes `ELSE`. A statement that breaks these rules is rejected with a message such as
`CASE WHEN condition must evaluate to Boolean, found Int64`, `CASE results must have one exact type,
found Int64 and Utf8`, or `CASE result type List(...) is not supported by conditional selection`,
which `IF` reports too.

Conditional values are computed in the columnar batch engine, and every arm is evaluated only for
the messages that select it: a condition is evaluated for the messages no earlier arm answered, and
a result for the messages its condition selected. A function in an arm therefore never reports an
error for a message that selects another arm, so an error in an unselected arm never reaches the
message's [error handling](#errors), and a `CASE` guard shields a function from the messages it
cannot handle.
Context-injected operations such as window aggregates, header reads, and UDFs are invoked for the
selected messages only, and not at all in a batch where no message selects their arm; a whole-batch
failure of such an invocation still fails the batch it was invoked for.

The words `IF`, `CASE`, `WHEN`, `THEN`, `ELSE`, and `END` are [reserved](#reserved-words).

## Comparison And Equality

`=`, `!=`, `<`, `<=`, `>`, and `>=` require both operands to have the same exact type. `=` and `!=`
compare any scalar type, `BYTES` included, which compares octet by octet. The ordering comparisons
`<`, `<=`, `>`, and `>=` accept numeric, `STRING`, and `DATETIME` operands. A null operand makes the
comparison null. No comparison is defined for `ARRAY` and `VEC` values: compare their elements with
the [array and vector functions](#array-and-vector-functions) instead.

- `STRING` values compare by Unicode code point, which is also the order of their UTF-8 bytes.
  Comparison is case-sensitive and never depends on a locale.
- Floating-point comparisons follow IEEE 754. NaN is unequal to every value, including another
  NaN, so `=` is false, `!=` is true, and every ordering comparison with NaN is false. `0.0` and
  `-0.0` are equal.
- `nullif(a, b)`, a simple `CASE <operand> WHEN <value>`, `IS [NOT] DISTINCT FROM`, and `IN` decide
  equality exactly as `=` does.

`a IS NOT DISTINCT FROM b` is equality under which two nulls are equal: it is true when both
operands are null or both are present and `a = b`, and false otherwise. `a IS DISTINCT FROM b` is
its negation, a null-safe `!=`. Neither is ever null, so a required `BOOL` field can hold either,
and `input.region IS DISTINCT FROM input.home_region` is true where exactly one region is null,
where `input.region != input.home_region` is null. Both operands must have the same exact scalar
type. Present floats compare as `=` does, so NaN is distinct from every value, another NaN included,
and `0.0` is not distinct from `-0.0`.

`IN`, `BETWEEN`, and `IS [NOT] DISTINCT FROM` bind like the comparison operators and group from the
left with them, so `a = b IN (TRUE)` tests whether `a = b` is in the set. Unary `NOT` binds more
tightly than any comparison, so `NOT x IN (1, 2)` negates `x` before testing it; write
`x NOT IN (1, 2)` or `NOT (x IN (1, 2))` instead. See [Operator Precedence](#operator-precedence).
There is no `IS NULL` operator: test for null with `is_null(x)` and `NOT is_null(x)`.

## Membership And Ranges

`<value> IN (<element>, ...)` tests whether a value equals an element of a written set, and
`<value> NOT IN (<element>, ...)` is its negation. `<value> BETWEEN <low> AND <high>` tests whether a
value lies in an inclusive range, and `<value> NOT BETWEEN <low> AND <high>` is its negation.

```nspl,ignore
input.status IN ('open', 'held')
input.priority NOT IN (1 AS I32, 2 AS I32)
input.weight BETWEEN 1.0 AND 50.0
input.observed_at NOT BETWEEN input.window_start AND input.window_end
```

### Sets

A set lists its elements in parentheses, separated by commas. Every element is a constant: a
literal, optionally negated with `-` or `NOT` and cast with `AS` or `TRY_CAST`, such as
`-1 AS I32` or `'2026-01-01T00:00:00Z' AS DATETIME`. Each element is evaluated once, when the
statement is applied, exactly as the same expression evaluates for a message, so an element that
cannot be evaluated, such as `300 AS U8`, rejects the statement, and so does a `TRY_CAST` that
cannot convert its literal, whose typed null is no valid element. So does an element that reads a
field or calls a function; to test a value against other fields, compare it with `=` and combine
the comparisons with `OR`.

The operand may have any numeric type, `BOOL`, `STRING`, or `DATETIME`, and every element must have
exactly the operand's type. Integer literals are `I64` and float literals `F64`, so a set tested
against an `I32` or `F32` value casts each element: `input.priority IN (1 AS I32, 2 AS I32)`.

A value is an element of a set when it equals one of its elements under `=`:

- `NULL` is not a valid element, because no value equals it; test for null with `is_null(...)` or
  `IS NOT DISTINCT FROM`.
- An empty set is valid. No value is an element of it, a null one included, so `x IN ()` is false
  and `x NOT IN ()` true for every message.
- Otherwise a null operand makes both `IN` and `NOT IN` null, so a `WHERE` selects a message with a
  null operand for neither.
- Writing an element twice changes nothing.
- NaN equals no value, so a NaN element matches nothing and a NaN operand is an element of no set,
  while `0.0` and `-0.0` are the same element.

A set is prepared once for its program rather than for every batch or message. A small set of
numbers, `BOOL`, or `DATETIME` values is tested by comparing the value with each of its elements,
while a larger set, and every set of `STRING` values, is looked up by key, so a set of thousands of
elements still costs each message one lookup.

### Ranges

`x BETWEEN low AND high` is `x >= low AND x <= high` with `x` computed once, so an operand that fails
reports its error once. The operand and both bounds must have one exact type: any numeric type,
`STRING`, or `DATETIME`, the types `<` orders. Both bounds are inclusive, and a range whose low bound
is above its high bound holds no value; the bounds are never swapped.

Nulls follow `AND`: a null operand makes the result null, and a null bound makes it null unless the
comparison with the other bound is false, which makes it false. NaN lies in no range, so `BETWEEN`
is false for a NaN operand or bound and `NOT BETWEEN` is true. `NOT BETWEEN` negates `BETWEEN`; it is
not `x < low OR x > high`, which is false for a NaN operand.

The `AND` that closes a low bound belongs to the range, so `x BETWEEN 1 AND 5 AND y` is
`(x BETWEEN 1 AND 5) AND y`. A bound that is itself a comparison needs parentheses.

## Arithmetic

`+`, `-`, `*`, `/`, and `%` require both operands to have the same exact numeric type, and the result
has that type. Unary `-` applies to signed integer and floating-point operands; an unsigned operand
is rejected with `operator Neg is not valid for UInt8`. A null operand makes the result null.
Integer literals are `I64`, so `input.count + 1` over a `U32` field is rejected with `binary
operator Add requires matching operand types, found UInt32 and Int64`; write `input.count + 1 AS
U32`. `+` does not join text, which [`concat`](#string-functions) does, and `DATETIME` values have no
arithmetic operators: move and measure them with [`date_add` and `date_diff`](#datetime-functions),
or convert them to `I64` nanoseconds explicitly.

Arithmetic is checked. An operation whose result its type cannot hold reports a per-message error
and yields null for that message, instead of wrapping, saturating, or producing NaN or an infinity.
Only that message fails: every other message in the batch is computed as usual, and a message's
result never depends on the other messages in its batch.

- Integer `+`, `-`, and `*` report an `overflow` error when the exact result does not fit the operand
  type, so `-` over `U8`, `U16`, `U32`, or `U64` operands fails when the right operand is larger.
- Integer `/` truncates toward zero. A zero divisor reports a `division_by_zero` error, and dividing
  the minimum value of a signed type by `-1` reports an `overflow` error.
- Integer `%` returns the remainder with the sign of the dividend. A zero divisor reports a
  `division_by_zero` error. Every other remainder exists, so the remainder of the minimum value of a
  signed type by `-1` is `0`.
- Unary `-` on the minimum value of a signed integer type reports an `overflow` error.
- `F32` and `F64` arithmetic is IEEE 754 arithmetic at the operand's own width, and `%` returns the
  remainder with the sign of the dividend. A result that is NaN or an infinity reports an
  `invalid_argument` error, which includes every division by zero and every operation on NaN. The
  rule applies to the result, so an infinite operand fails only where the result is not finite:
  `1.0 / x` is `0.0` for an infinite `x`. Unary `-` on a float never fails: it changes the sign of
  every value, including zero, NaN, and the infinities.

The error's message names the failure: `integer addition overflowed`, `integer subtraction
overflowed`, `integer multiplication overflowed`, `integer division overflowed`, or `integer
negation overflowed` for an `overflow`; `integer division by zero` or `integer remainder by zero`
for a `division_by_zero`; and `floating-point operation produced a non-finite result` for an
`invalid_argument`.

## Conversions

A value never changes its type implicitly. Two explicit forms convert it to another scalar type.
They accept the same types and convert every value the same way, and differ only in what a value
that does not convert does:

| Form | Result | A value that does not convert |
| --- | --- | --- |
| `<expr> AS <type>` | `<type>`, optional exactly when `<expr>` is | Reports a per-message `cast_failed` error, such as `cannot cast value to Int64`, and yields null for that message, which takes the [error handling](#errors) of its context |
| `TRY_CAST(<expr> AS <type>)` | Optional `<type>` | Yields a typed null, and the message continues without an error |

`<type>` is a scalar type written with the same spellings in both forms, in any letter case: `U8`
or `UINT8`, `I8` or `INT8`, `U16` or `UINT16`, `I16` or `INT16`, `U32` or `UINT32`, `I32` or
`INT32`, `U64` or `UINT64`, `I64` or `INT64`, `F32` or `FLOAT32`, `F64` or `FLOAT64`, `BOOL` or
`BOOLEAN`, `STRING` or `UTF8`, `BYTES`, and `DATETIME`. Any other name is rejected with
`unsupported type '<name>'`. Conversions are defined between scalar values only: an `ARRAY` or
`VEC` type cannot be written as a target, and no conversion is defined for an `ARRAY` or `VEC`
operand. A `BYTES` value converts only to `BYTES`, which returns it unchanged, and no other type
converts to `BYTES`; a statement that asks for either is rejected with `BYTES conversions require
bytes_from_utf8, bytes_to_utf8, base64 or hex functions`, which name the functions that convert
explicitly. A null operand is not a failure: it converts to a typed null in both forms, so
`input.amount AS I64` over a message without an `amount` yields null and reports nothing. Test the
operand to tell a missing value from one that did not convert:

```nspl,ignore
SET amount = TRY_CAST(input.amount AS I64),
    amount_state = CASE
      WHEN is_null(input.amount) THEN 'missing'
      WHEN is_null(TRY_CAST(input.amount AS I64)) THEN 'malformed'
      ELSE 'converted'
    END
```

The result of `TRY_CAST` is optional even for a conversion that cannot fail, so it initializes a
required field only through an expression that is never null, such as
`coalesce(TRY_CAST(input.amount AS I64), 0)`, and a statement that assigns it to a required field
directly is rejected when it is applied. It keeps the sensitivity of its operand, as every
conversion does.

`TRY_CAST` suppresses only the failure of the conversion it performs. Every other failure of its
operand still fails the message with its own error:

- `TRY_CAST(100 / input.divisor AS STRING)` reports `division_by_zero` for a zero divisor.
- `TRY_CAST(input.raw AS I64 AS STRING)` converts `input.raw AS I64`, which reports `cast_failed`
  for text that is not an integer.
- A UDF called in the operand reports its own per-message errors, and a failure of the whole batch
  still fails the batch.

A conversion of a `TRY_CAST` result is an ordinary one: `TRY_CAST(input.raw AS I64) AS U8` fails a
message whose number does not fit `U8`, and yields null without an error for one whose text is not
an integer. Inside a conditional, a `TRY_CAST` converts only the messages that select its arm, as
every operation does.

The operand of `TRY_CAST` is the whole expression before its final `AS`, so
`TRY_CAST(input.low + input.high AS STRING)` converts the sum, while in
`input.low + input.high AS STRING` the `AS` applies to `input.high` alone.

A conversion between scalar types other than `BYTES` succeeds for every value, except for the
values the table below lists. Converting a value to its own type returns it unchanged.

- Every value other than `BYTES` converts to `STRING`. An integer is written in decimal, and a
  `BOOL` as `true` or `false`. A float is written in decimal without an exponent, with the fewest
  digits that read back as the same value: `1.0` is written `1`, `1e21` as `1` and 21 zeros, NaN as
  `NaN`, the infinities as `inf` and `-inf`, and negative zero as `-0`. A `DATETIME` is written in
  RFC 3339 with a `+00:00` offset and a fraction of three, six, or nine digits when its fraction is
  not zero, such as `2024-02-29T12:00:00+00:00` and `2024-02-29T12:00:00.500+00:00`.
- A number converts to `BOOL` as `false` for zero and `true` for every other value, including NaN,
  and a `BOOL` converts to a number as `0` or `1`.
- A number converts to `F32` or `F64` as the nearest value of that type, so an `F64` beyond the
  `F32` range becomes an infinity.
- A `DATETIME` converts to `I64` as its count of nanoseconds since `1970-01-01T00:00:00Z`, and to
  `F32` or `F64` as the nearest value to that count. An integer converts to the `DATETIME` that many
  nanoseconds after that instant, and a float to the `DATETIME` its count of nanoseconds names, cut
  toward zero, so `1.5 AS DATETIME` is one nanosecond after the epoch.

| From | To | Fails for |
| --- | --- | --- |
| An integer type | Another integer type | A value outside the target type's range |
| `U64` | `DATETIME` | A value above the largest `I64` |
| `F32`, `F64` | An integer type | NaN, an infinity, and a value whose integer part, which it is rounded to toward zero, is outside the target type's range |
| `F32`, `F64` | `DATETIME` | NaN, an infinity, and a value outside the `DATETIME` range when read as nanoseconds since `1970-01-01T00:00:00Z` |
| `STRING` | An integer type | Text other than an optional `+` or `-` followed by decimal digits, with no surrounding whitespace, and a number outside the target type's range |
| `STRING` | `F32`, `F64` | Text other than a decimal number with an optional sign, fraction, and exponent, such as `1e3`, `.5`, or `+1.5`, or `NaN`, `inf`, or `infinity` in any letter case and with an optional sign. A number beyond the type's range reads as an infinity |
| `STRING` | `BOOL` | Text other than `t`, `tr`, `tru`, `true`, `y`, `ye`, `yes`, `on`, or `1`, which read as `true`, and `f`, `fa`, `fal`, `fals`, `false`, `n`, `no`, `of`, `off`, or `0`, which read as `false`, in any letter case and with any surrounding whitespace |
| `STRING` | `DATETIME` | Text that is not an RFC 3339 date and time with a UTC offset, such as `2024-02-29T12:00:00Z` or `2024-02-29 13:00:00+01:00`, where a space may take the place of the `T`, and an instant outside the `DATETIME` range. Read other forms with [`parse_datetime`](#reading-text) |
| `DATETIME` | An integer type other than `I64` | A count of nanoseconds since `1970-01-01T00:00:00Z` outside the target type's range |
| `BOOL` | `DATETIME` | Every value |
| `DATETIME` | `BOOL` | Every value |

## JSON Documents

A `STRING` value holding a JSON document is read into typed values explicitly. Three forms read it,
each naming a path to one value in the document:

| Form | Result | A document or value that cannot be read |
| --- | --- | --- |
| `JSON_VALUE(<document>, '<path>' AS <type>)` | Optional `<type>` | Reports a per-message error that names the defect and yields null for that message, which takes the [error handling](#errors) of its context |
| `TRY_JSON_VALUE(<document>, '<path>' AS <type>)` | Optional `<type>` | Yields a typed null, and the message continues without an error |
| `JSON_EXISTS(<document>, '<path>')` | `BOOL`, optional exactly when `<document>` is | Reports a per-message error, as `JSON_VALUE` does |

`<document>` is any `STRING` expression; any other type rejects the statement when it is applied,
with an error such as `JSON_VALUE document must be STRING, found Int64`. A null document yields null
in all three forms and reports nothing. `<path>` is a string literal in single or double quotes,
parsed and checked when the statement is parsed, so a malformed path rejects the statement; see
[Paths](#paths). `JSON_VALUE`, `TRY_JSON_VALUE`, and `JSON_EXISTS` are [reserved](#reserved-words).

```nspl,ignore
SET customer_id = JSON_VALUE(input.payload, '$.customer.id' AS I64),
    tags = JSON_VALUE(input.payload, '$.tags' AS VEC<STRING>),
    first_quantity = TRY_JSON_VALUE(input.payload, '$.items[0].qty' AS U32),
    has_note = JSON_EXISTS(input.payload, '$.note')
```

A value read from a document keeps the document's sensitivity, as every function result does.

### Paths

A path starts with `$`, which names the whole document, followed by any number of steps, at most 64:

| Step | Leads to |
| --- | --- |
| `.name` | The member of an object named `name`, where `name` is ASCII letters, digits, and underscores not starting with a digit |
| `["name"]` | The member of an object with any name, written as a JSON string in double quotes with its escapes, such as `["unit price"]` or `["café"]` |
| `[n]` | The element of an array at the zero-based index `n`, from `0` to `4294967295`, written without leading zeros |

A path holds no whitespace, and has no wildcards, slices, filters, recursive descent, negative
indexes, or single-quoted names. Member names are compared exactly, letter case included, with no
Unicode normalization. Where an object repeats a member name, `JSON_VALUE` reads the last of them,
and `JSON_EXISTS` finds any of them. A path finds no value when an object lacks the member, an array
is shorter than the index, a member step meets an array, an index step meets an object, or a step
leads into a value that is neither: `$.a.b` finds nothing where `a` is a number, a string, JSON
null, or an array, and `$[0]` finds nothing in an object.

A malformed path rejects the statement with `invalid JSON path '<path>':` followed by the defect,
whose byte positions count from zero:

- `a path starts with '$'`
- `expected '.' or '[' at byte <n>`
- `expected a member name of letters, digits and underscores not starting with a digit at byte <n>;
  write any other name as ["name"]`
- `expected an array index or a double-quoted member name at byte <n>`
- `array index at byte <n> is not an integer from 0 to 4294967295 without leading zeros`
- `member name at byte <n> is not a valid JSON string`
- `expected ']' at byte <n>`
- `a path takes at most 64 steps`

A message error writes its path in one form, with `.name` where a name allows it and `["name"]`
otherwise, so `$["plain"]` appears as `$.plain`.

### Declared Types

`<type>` is a scalar type written with the same spellings as in [Conversions](#conversions), or a
`VEC<...>` or `ARRAY<..., n>` of declared types written as a schema field declares them, nested to
any depth, such as `VEC<VEC<I32>>` or `ARRAY<F32, 2, 3>`, where `ARRAY<F32, 2, 3>` is two arrays of
three and every fixed length is from 1 to 2,147,483,647. JSON has no date or byte type, so
`DATETIME` and `BYTES`, including as elements, reject the statement with an error such as
`JSON_VALUE cannot read DATETIME, which no JSON value is; read the text as STRING and convert it
explicitly`; read the text as a `STRING` and convert it with [`parse_datetime`](#reading-text) or
[`base64_decode`](#bytes-encodings-and-hashes). A value is never converted between kinds: a number is
not read as text, and text is not read as a number.

| Declared type | Reads | Fails for |
| --- | --- | --- |
| `BOOL` | `true` and `false` | Every other value |
| `STRING` | A JSON string, with its escapes decoded | Every other value, numbers included |
| An integer type | A JSON number with no fractional part, so `2`, `2.0`, and `2e0` read as `2` | A number with a fractional part, a number outside the type's range, and every value that is not a number |
| `F64` | A JSON number, as the nearest `F64` | Every value that is not a number |
| `F32` | A JSON number, as the nearest `F32` | A number beyond the `F32` range, and every value that is not a number |
| `VEC<type>` | A JSON array of any length, each element read as `type` | Every value that is not an array, and an array holding an element that `type` does not read |
| `ARRAY<type, n>` | A JSON array of exactly `n` elements, each read as `type` | Every value that is not an array of `n` elements, and an array holding an element that `type` does not read |

`VEC` and `ARRAY` elements are never null, so an array holding JSON null fails as any other element
of the wrong kind does. A number written as digits with an optional `-`, and no fraction or
exponent, is read exactly across the whole 64-bit range. Every other number is read as the nearest
`F64` first, and an integer type accepts it when that `F64` is an integer in its range:
`9007199254740993.0` reads as `9007199254740992`, and `1.0000000000000001` reads as `1`. An integer
written as digits beyond the 64-bit range reads as the nearest `F64` too, so it is out of range for
every integer type, except that the negative integers down to `-9223372036854776832` read as the
`F64` `-2^63`, which is the smallest `I64`. A number whose magnitude is beyond the `F64` range, such
as `1e400`, makes the document unreadable. `-0` reads as `0` for every type, and `-0.0` reads as `0`
for an integer type and as `-0.0` for `F64`.

### Missing Values, JSON Null, And Failures

What each form yields depends on what the path finds:

| The document | `JSON_VALUE` | `TRY_JSON_VALUE` | `JSON_EXISTS` |
| --- | --- | --- | --- |
| is not valid JSON | Error `cast_failed`: `JSON_VALUE document is not valid JSON` | Null | Error `cast_failed`: `JSON_EXISTS document is not valid JSON` |
| has no value at the path | Null | Null | `FALSE` |
| holds JSON null at the path | Null | Null | `TRUE` |
| holds a value of another kind | Error `cast_failed`, such as `JSON_VALUE found a JSON string at $.count where I64 is declared` | Null | `TRUE` |
| holds a number the type cannot hold | Error `cast_failed`, such as `JSON_VALUE number at $.level does not fit U8` | Null | `TRUE` |
| holds an array of the wrong length for an `ARRAY` | Error `cast_failed`, such as `JSON_VALUE array at $.point has 3 elements where 2 are declared` | Null | `TRUE` |

A number with a fraction where an integer is declared is a value of another kind, reported as
`JSON_VALUE found a fractional JSON number at $.count where I64 is declared`. A defect inside a
collection names the collection with `in` and the innermost declared element type, such as
`JSON_VALUE found JSON null in $.tags where I64 elements are declared` or `JSON_VALUE array in $.p
has 1 elements where 2 are declared`. An error names the path and the declared type but never a
value from the document, so it can be reported for a sensitive document.

`JSON_VALUE` and `TRY_JSON_VALUE` yield null both where the path finds nothing and where it finds
JSON null; `JSON_EXISTS` tells the two apart. Test the forms together to classify a document without
failing the message:

```nspl,ignore
SET amount = TRY_JSON_VALUE(input.payload, '$.amount' AS F64),
    amount_state = CASE
      WHEN is_null(input.payload) THEN 'no document'
      WHEN NOT is_null(TRY_JSON_VALUE(input.payload, '$.amount' AS F64)) THEN 'read'
      WHEN NOT is_null(TRY_JSON_VALUE(input.payload, '$.amount' AS STRING)) THEN 'text'
      ELSE 'missing, null, or unreadable'
    END
```

To tell a malformed document from a missing value, route the message through `JSON_EXISTS` or
`JSON_VALUE` and read the error in `ON MESSAGE ERROR`, where the error names the defect.

As with `TRY_CAST`, the result of `TRY_JSON_VALUE` and `JSON_VALUE` is optional, so it initializes a
required field only through an expression that is never null, such as
`coalesce(TRY_JSON_VALUE(input.payload, '$.qty' AS U32), 0 AS U32)`, and a statement that assigns
it to a required field directly is rejected when it is applied. `TRY_JSON_VALUE` suppresses only the
failures of reading its document: a failure of its document expression still fails the message.

### Document Limits

A document is read only when it holds at most 16,777,216 bytes and nests objects and arrays at most
128 deep. A larger or deeper document fails `JSON_VALUE` and `JSON_EXISTS` with an
`invalid_argument` error, such as `JSON_VALUE document exceeds 16777216 bytes` or `JSON_VALUE
document nests deeper than 128 levels`, and yields null from `TRY_JSON_VALUE`. A result whose text
or elements do not fit one column, as with [`repeat`](#result-size), reports an `overflow` error such
as `JSON_VALUE result exceeds what one VEC<STRING> column holds`, from `TRY_JSON_VALUE` too, as
`TRY_JSON_VALUE result exceeds ...`, since it is a limit of the batch rather than a defect of the
document. `JSON_EXISTS` builds no text and never reports it.

Reading a document is bounded by these limits. Like every builtin, an extraction runs to completion
for the batch it is evaluating, so stopping a node or a processor takes effect between batches.

### Reading A Document Once

Every extraction a route makes from the same document field reads one parse of each document,
whether the extractions are in one `SET` assignment or several, in `WHERE`, or a mix of
`JSON_VALUE`, `TRY_JSON_VALUE`, and `JSON_EXISTS`, so reading ten fields costs one parse rather than
ten. A document computed by an expression, such as `coalesce(input.payload, '{}')`, shares one
parse among the extractions of one assignment, and is parsed again for each assignment that reads
it. A document is parsed with SIMD instructions that find its structure, chosen at run time from
the ones the node's CPU offers, and each path is then followed and its value written straight into
a typed column of the declared type; no document is held as an untyped value. A document field that
an earlier `SET` assignment rewrites is a new column, whose documents are parsed again.

Inside a conditional arm, an extraction parses only the documents of the messages that select the
arm, and an extraction in the arm does not share its parse with one outside it. A document written
as a literal is parsed once for each batch and shared by every message of it, so a malformed
literal document fails every message that evaluates it.

## Null Handling

| Function | Returns | Notes |
| --- | --- | --- |
| `coalesce(a, b, ...)` | same type as inputs | Returns the first non-null argument, or a typed null when every argument is null. Takes one or more arguments of one exact type, which may be any type, `ARRAY`, `VEC`, and `BYTES` included |
| `is_null(x)` | `BOOL` | True when the input is null. Takes one argument of any type. Never null |
| `nullif(a, b)` | same type as inputs | Returns a typed null when `a = b`, and `a` otherwise, including when `b` is null. Both arguments must have the same type |

`coalesce` is optional only when every argument is, so a required last argument makes its result
required: `coalesce(input.nickname, input.name)` initializes a required field when `name` is
required. It evaluates every argument for every message, including the arguments after the first
present one, so a failing later argument fails the message; guard such an argument with a
conditional. There is no `IS NULL` operator and no `is_not_null` function: write `is_null(x)` and
`NOT is_null(x)`. A bare `NULL` is not an argument of any of these functions; see
[Literals](#literals).

## Extrema

| Function | Returns | Notes |
| --- | --- | --- |
| `greatest(a, ...)` | same type as inputs | The largest present argument, or a typed null when every argument is null |
| `least(a, ...)` | same type as inputs | The smallest present argument, or a typed null when every argument is null |
| `clamp(value, low, high)` | same type as inputs | `low` where `value < low`, `high` where `value > high`, and `value` otherwise |

`greatest` and `least` take one or more arguments of one exact scalar type: any numeric type,
`BOOL`, `STRING`, `DATETIME`, or `BYTES`. They skip null arguments, so their result is required when
any argument is. They order values the way the window `MIN` and `MAX` aggregates do: `BOOL` orders
`false` before `true`, `STRING` orders by Unicode code point, `BYTES` orders octet by octet, and
floating-point values order NaN above every other value and treat both zeros as equal. Among equal
values the earliest argument is returned, so `greatest(-0.0, 0.0)` is `-0.0` and
`greatest(0.0, -0.0)` is `0.0`. Every argument is evaluated, whichever one is returned.

`clamp` takes three arguments of one exact type: any numeric type, `STRING`, or `DATETIME`. It
compares exactly as `<` and `>` do, so it returns a NaN value unchanged and keeps `-0.0` inside a
range that starts at `0.0`. A null argument produces a null result. A message whose low bound is
above its high bound, or whose floating-point bound is NaN, reports an `invalid_argument` error,
`clamp lower bound is above its upper bound` or `clamp bound is NaN`, and yields null, so give a
route whose bounds can cross an `ON MESSAGE ERROR` policy. A message with a null argument is never
failed.

## Context And Identity

| Function | Returns | Notes |
| --- | --- | --- |
| `leak_sensitive(value)` | same type as input | Removes sensitivity from a value of any type, including every element of an `ARRAY` or `VEC` |
| `now()` | `DATETIME` | The execution-local domain time. Never null |
| `uuid_v4()` | `STRING` | A random UUID, new for every message |
| `uuid_v7()` | `STRING` | A time-ordered UUID built from the execution-local domain time, new for every message. Reports an `overflow` error when that time is before the Unix epoch |

`leak_sensitive` takes exactly one argument and returns it unchanged, optional exactly when the
argument is; it only changes what the statement may do with the value. `now()`, `uuid_v4()`, and
`uuid_v7()` take no arguments and are never sensitive.

`now()` is one snapshot of the domain clock for each unit of work, shared by every expression of
that unit: an ingested group of messages, a batch a processor accepts, and the timer, flush, or
wait release that resumes work. It is logical time in a paced domain, which can be before 1970, and
actual UTC in an unpaced one. See [Domain Clock](domain-clock.md) for how snapshots are taken.

Both UUID functions return the canonical 36-character lowercase form, such as
`0192f2b5-6c1d-7e3a-9f4b-2a7c1e5d8b90`, and every occurrence of a call in an expression returns its
own value. A version 7 UUID encodes its time as milliseconds since `1970-01-01T00:00:00Z` and fills
the remaining 74 bits at random, so UUIDs of different milliseconds order by time, while UUIDs of
one millisecond, such as those of one batch, have no order among themselves. In a paced domain
whose logical time is earlier than the epoch, such as one started with
`START AT '1969-07-20T20:17:00Z'`, `uuid_v7()` reports `uuid_v7 execution time is before the Unix
epoch` for every message that evaluates it until domain time reaches the epoch. It never encodes
another instant instead.

## Header Functions

| Function | Returns | Notes |
| --- | --- | --- |
| `read_header(name)` | optional `STRING` | Ingestor-only. Returns the first value, or `NULL` when absent |
| `read_headers(name)` | `VEC<STRING>` | Ingestor-only. Returns all values in order, or an empty vector when absent. Never null |
| `write_header(name, value)` | nothing | Emitter-only side effect. Valid only as a top-level call in the final `INVOKE` block |

Header names may be dynamic `STRING` expressions, and a name matches exactly, letter case included,
the names the connector recorded. A null name reads as absent. Header reads are available on
Endpoint (HTTP and WebSocket), HTTP client, Kafka, NATS, Pulsar, RabbitMQ, and SQS ingestors, in
their `FILTER WHERE` and route construction, and in an error route's construction for a failure on
such an ingestor. Header writes are available on HTTP, Kafka, NATS, Pulsar, RabbitMQ, and SQS
emitters. Anywhere else a statement is rejected when it is validated, with a message such as
`function 'read_header' is only available to ingestors whose connector supports headers`, `MQTT
ingestors do not support read_header or read_headers`, or `MQTT emitters do not support
write_header`.

`write_header` arguments must be statically non-null `STRING` expressions, and a sensitive argument
requires `leak_sensitive(...)`. They are evaluated after payload construction and route filtering.
Calls are staged in source order in a route-local envelope; if any call fails, no payload or partial
header envelope is published. See [Emitters](emitters.md) for each sink's header rules.

## Lookups And UDF Calls

| Call | Returns | Notes |
| --- | --- | --- |
| `LOOKUP_HASH_MAP('<hash_map>', <key>, '<field>')` | the field's type, always optional | The named field of the record whose key equals `<key>` in a [hash map](lookups.md), or null when the key is missing or null or the field is null |
| `udf::<name>(<arg>, ...)` | the UDF's declared result type | A domain-owned [Roto UDF](udfs.md) with 1–8 exactly typed arguments |

The hash-map name and the field are string literals, checked when the statement is applied; a key
expression that fails for any message fails its whole batch. Identical lookups of one program are
answered once. [Where Expressions Run](#where-expressions-run) lists the contexts that accept a
lookup, and [Lookups](lookups.md) describes hash maps and their resource versions.

A UDF call is optional when the UDF declares an `OPTIONAL` result or when a required parameter
receives an optional argument, is sensitive when any argument is, and is deterministic unless the
UDF is declared `VOLATILE`. It never runs for a message whose required argument is null or whose
earlier operation already failed, and yields null for it. See
[Nulls, errors, and volatility](udfs.md#nulls-errors-and-volatility) for its per-message and
whole-batch errors.

## String Functions

String functions count and select characters, meaning Unicode scalar values, rather than bytes or
grapheme clusters. A combining mark counts as a separate character. Positions count from 1. Every
text argument is exactly `STRING`: a `BYTES` value is rejected, so no string function measures,
joins, or searches octets. Convert between the two with the
[bytes functions](#bytes-encodings-and-hashes).

| Function | Returns | Notes |
| --- | --- | --- |
| `lower(text)` | `STRING` | Lowercases with Unicode full case mapping |
| `upper(text)` | `STRING` | Uppercases with Unicode full case mapping |
| `trim(text)` | `STRING` | Removes leading and trailing Unicode whitespace |
| `btrim(text)` | `STRING` | Same as `trim` |
| `ltrim(text)` | `STRING` | Removes leading Unicode whitespace |
| `rtrim(text)` | `STRING` | Removes trailing Unicode whitespace |
| `length(text)` | `I64` | Character count |
| `char_length(text)` | `I64` | Same as `length` |
| `bit_length(text)` | `I64` | Eight times the UTF-8 byte length |
| `octet_length(text)` | `I64` | UTF-8 byte length, read from the Arrow string offsets |
| `ascii(text)` | `I64` | Unicode code point of the first character, or `0` for an empty string |
| `initcap(text)` | `STRING` | Uppercases the first character of each run of letters and digits and lowercases the rest |
| `left(text, count)` | `STRING` | The first `count` characters. A negative `count` removes that many characters from the end |
| `right(text, count)` | `STRING` | The last `count` characters. A negative `count` removes that many characters from the start |
| `substr(text, start)` | `STRING` | Characters from position `start` to the end |
| `substr(text, start, length)` | `STRING` | At most `length` characters from position `start` |
| `substring(text, start)` | `STRING` | Alias for `substr` |
| `substring(text, start, length)` | `STRING` | Alias for `substr` |
| `concat(a, b, ...)` | `STRING` | Joins one or more `STRING` arguments in order. A null argument contributes nothing, so no message's result is null. See the note on its type below |
| `concat_ws(separator, a, ...)` | `STRING` | Joins the non-null values after `separator`, keeping empty strings, with `separator` between them. A null separator makes the result null |
| `repeat(text, count)` | `STRING` | The text repeated `count` times, or an empty string when `count` is at most `0` |
| `replace(text, from, to)` | `STRING` | Replaces every occurrence of `from`, matched as plain text from left to right without overlaps. An empty `from` inserts `to` before every character and at the end |
| `reverse(text)` | `STRING` | Reverses the characters |
| `lpad(text, length, fill)` | `STRING` | Pads on the left with repetitions of `fill` to `length` characters |
| `rpad(text, length, fill)` | `STRING` | Pads on the right with repetitions of `fill` to `length` characters |
| `split_part(text, delimiter, index)` | `STRING` | The part at position `index` |
| `split(text, delimiter)` | `VEC<STRING>` | All parts in order, including empty parts at the ends or between adjacent delimiters |
| `join(parts, separator)` | `STRING` | Joins an `ARRAY<STRING>` or `VEC<STRING>`; null elements contribute nothing |
| `normalize_nfc(text)` | `STRING` | Unicode canonical composition (NFC); does not change case or apply compatibility folding |
| `strpos(text, needle)` | `I64` | Position of the first occurrence of `needle`, `1` for an empty `needle`, or `0` when it does not occur |
| `translate(text, from_chars, to_chars)` | `STRING` | Replaces each character found in `from_chars` with the character at the same position in `to_chars`, and removes a character that has no counterpart. A character that `from_chars` repeats takes its first position, and characters of `to_chars` past the length of `from_chars` are ignored |
| `to_hex(value)` | `STRING` | Lowercase hexadecimal digits without a prefix. Integer input only; a negative value is written as its two's complement at the input's width, so an integer literal, which is `I64`, is written at 64 bits |
| `md5(text)` | `STRING` | The 32 lowercase hexadecimal digits of the MD5 digest of the text's UTF-8 bytes |

`lower`, `upper`, and `initcap` use Unicode case mappings, which never depend on the node's locale.
A mapping can change a value's length: `upper('Grüßen')` is `GRÜSSEN`. A literal, a field, and a
computed value holding the same text always convert to the same result. `initcap` starts a run at
every letter or digit that follows a character that is neither, and maps each character on its
own: a run that starts with `ß` starts with `SS`, no character takes a titlecase form, and a run
may start with a digit, so `initcap('3rd')` is `3rd`.

`count`, `start`, `length`, and `index` may be any integer type and are never narrowed to another:
an unsigned count above the `I64` range reaches past the end of any text, exactly as the largest
`I64` count does. A floating-point count is rejected.

`substr` treats a `start` at or before `1` as the first character and counts `length` from there. A
`start` past the end, or a negative `length`, returns an empty string.

`lpad` and `rpad` shorten text longer than `length` to its first `length` characters, return an
empty string when `length` is at most `0`, and return shorter text unchanged when `fill` is empty.

`split_part` returns an empty string when `index` is at most `0` or past the last part; it never
counts from the end. With an empty `delimiter`, the whole text is part `1`.

`split` uses the same empty-delimiter rule: it returns one part containing the whole text. An
empty input also produces one empty part. `join` on an empty list produces an empty string;
`concat_ws` with no non-null values after the separator does the same. These functions preserve
the written order.

`concat` never yields null for a message, but a call that has an optional argument has an optional
result, so it initializes a required field only through an expression that is never null, such as
`concat(coalesce(input.first, ''), ' ', coalesce(input.last, ''))`. `concat_ws` follows the same
rule.

### Result Size

The values one call produces for a batch share one `STRING` column, which holds at most
2,147,483,647 bytes of text. A function whose result can be longer than its arguments computes how
long each result is before it builds it, and a result that does not fit in what its column has left
reports an `overflow` error and yields null instead of being built:

| Function | Error |
| --- | --- |
| `repeat`, `lpad`, `rpad`, `concat_ws`, `join`, `normalize_nfc` | `<function> result exceeds the text one STRING column holds`, such as `repeat result exceeds the text one STRING column holds` |
| `split` | `split result exceeds 65,536 parts` for a result of more than 65,536 parts, and `split result exceeds the text one STRING column holds` |
| `base64_encode`, `hex_encode` | `<function> result exceeds the bytes one column holds` |

Inside a conditional arm the column holds only the results of the messages that select the arm, so a
message that selects another arm uses none of its text. A call whose arguments are all literals
computes one value that every message in the batch holds, so that value must fit once for each of
them: `repeat('ab', 600000000)` fits a batch of one message but reports the error on every message
of a batch of two. [JSON extraction](#document-limits), [URL](#urls) and [IP address](#ip-addresses-and-networks)
functions, and [`format_datetime`](#writing-values), bound their results in the same column limit.

`concat`, `replace`, `regexp_replace`, `translate`, `lower`, `upper`, and `initcap` build their
column without this check, so no message reports an `overflow` error for them. Keep the text they
produce for one batch within the column limit: for example, a `replace` with an empty `from`
multiplies the length of its text.

## String Predicates

Plain substring matching is exact and case-sensitive, and compares UTF-8 bytes, so it never
normalizes text.

| Function | Returns | Notes |
| --- | --- | --- |
| `contains(text, needle)` | `BOOL` | True when `needle` occurs in `text` |
| `starts_with(text, prefix)` | `BOOL` | True when `text` begins with `prefix` |
| `ends_with(text, suffix)` | `BOOL` | True when `text` ends with `suffix` |
| `contains_any(text, patterns)` | `BOOL` | True when any non-null string in an `ARRAY<STRING>` or `VEC<STRING>` occurs in `text`; an empty set is false and an empty pattern matches every non-null text |
| `like(text, pattern)` | `BOOL` | SQL LIKE: `%` matches zero or more Unicode characters and `_` matches one, newlines included; a backslash makes the character after it literal |
| `ilike(text, pattern)` | `BOOL` | The same wildcard rules with Unicode loose case-insensitive matching |

`like` and `ilike` match the entire text. A trailing backslash is a literal backslash.
`ilike` follows Arrow's Unicode loose matching: it ignores case without expanding characters,
so `ß` does not equal `SS`. For full Unicode case mapping, apply `lower` or `upper` explicitly;
neither matching function normalizes text. Each LIKE pattern is limited to 4,096 bytes of UTF-8
text: a longer pattern reports an `invalid_argument` error, `LIKE pattern exceeds 4 KiB`, on every
message that evaluates it, whatever its text holds.

`contains_any` compares exact case-sensitive text, including combining marks. A set written in the
call as `array(...)`, `vec(...)`, or `[...]` whose elements are all non-null `STRING` constants is
prepared once with the program. Every other set is read for each message, and the distinct sets of
one batch are prepared once each, keeping at most 64 of them for the batch and replacing the one
prepared first when more are read. A set has at most 128 non-null patterns and 65,536 bytes of
combined pattern text; a larger set reports an `invalid_argument` error, `contains_any pattern set
exceeds 128 patterns or 64 KiB`, on each message with a non-null text and set that uses it, even
when the set is a constant.

## Regular Expressions

Regular-expression functions take `STRING` arguments and use Rust regex syntax. A pattern that does
not compile reports a per-message `invalid_argument` error whose message is `invalid regular
expression:` followed by the parser's description of the defect.

A pattern written as a literal, or an expression that compiles to a constant, such as a `CASE` with
constant conditions or `lower`, `upper`, `trim`, or `coalesce` over literals, is compiled once when
the node is activated and reused by every batch. It is still not a configuration error: a constant
pattern that does not compile reports its per-message error only for the messages that evaluate it,
so an invalid pattern in a conditional arm no message selects reports nothing. Any other pattern,
including one built with `concat`, is compiled when a message first uses it and kept in a cache of
the 64 most recently compiled patterns per call, which evicts the pattern compiled longest ago and
keeps the outcome of a pattern that did not compile. A compiled pattern is limited to 10 MiB; a
pattern that compiles past that limit reports a per-message error like any other invalid pattern. A
message whose text, replacement, or group is null yields null without compiling its pattern, so an
invalid pattern reports nothing for it.

| Function | Returns | Notes |
| --- | --- | --- |
| `regexp_like(text, pattern)` | `BOOL` | True when the pattern matches anywhere in the text |
| `regexp_replace(text, pattern, replacement)` | `STRING` | Replaces every match. `$1` and `${name}` in `replacement` insert a capture group, and `$$` inserts `$`. A group that did not participate inserts nothing |
| `regexp_substr(text, pattern)` | optional `STRING` | The first match, or null when the pattern does not match |
| `regexp_extract(text, pattern, group)` | optional `STRING` | Numbered capture from the first leftmost match; group `0` is the complete match, and an absent group or match produces null |

`regexp_extract` accepts any integer type for `group`. A negative index produces null. Regex syntax
uses Unicode classes by default, while POSIX classes such as `[[:alpha:]]` are ASCII only. Matching
is leftmost-first and supports no backreferences or look-around; the engine keeps linear-time
search guarantees rather than enabling backtracking features. In a replacement, a group name runs
as far as letters, digits, and underscores continue, so `$1a` names a group `1a`; write `${1}a` to
follow group 1 with `a`.

`regexp_substr` and `regexp_extract` are null for a message the pattern does not match even when
every argument is required, so assign them to an `OPTIONAL` field or give them a value with
`coalesce`.

The description of an invalid pattern quotes the pattern. A pattern read from a field is part of the
message, so the error message of such a pattern carries its text: validate patterns before they
reach a route whose error records leave the node, or write patterns as literals.

## Bytes, Encodings And Hashes

`BYTES` values hold arbitrary octets, including zero and non UTF-8 bytes. These functions accept
exact types, propagate nulls, and preserve sensitivity. An encoded or hashed sensitive input stays
sensitive; emitting it still requires explicit leakage. A cast never converts between `BYTES` and
`STRING`: `input.raw AS BYTES` is rejected with `BYTES conversions require bytes_from_utf8,
bytes_to_utf8, base64 or hex functions`.

| Function | Returns | Notes |
| --- | --- | --- |
| `bytes_from_utf8(text)` | `BYTES` | The exact UTF-8 bytes of a `STRING`, with no terminator or normalization |
| `bytes_to_utf8(bytes)` | `STRING` | Decodes UTF-8; invalid sequences fail that message |
| `base64_encode(bytes)` | `STRING` | RFC 4648 standard alphabet with required `=` padding |
| `base64_decode(text)` | `BYTES` | Accepts canonical padded standard base64; the URL-safe alphabet, missing or extra padding, nonzero trailing bits, and whitespace fail that message |
| `hex_encode(bytes)` | `STRING` | Two lowercase hexadecimal digits per byte, without a prefix |
| `hex_decode(text)` | `BYTES` | Accepts hexadecimal pairs in either letter case, mixed in one value; odd length or any other character fails that message |
| `sha256(bytes)` | `BYTES` | The 32 raw SHA-256 digest bytes |
| `xxh3_64(bytes)` | `U64` | Stable XXH3 64-bit hash with seed zero |

An empty input encodes and decodes to an empty output. `md5` reads `STRING` text and returns
hexadecimal text, while `sha256` reads `BYTES` and returns raw bytes: hash text with
`sha256(bytes_from_utf8(input.text))` and write the digest with `hex_encode`.

`xxh3_64` hashes the input octets in their given order and returns the algorithm's unsigned 64-bit
number. The value is independent of host byte order and stays the same across nodes and restarts.
It is a noncryptographic hash, so use `sha256` where collision resistance matters.

A decoding failure reports a `cast_failed` error: `bytes_to_utf8 input is not valid UTF-8`,
`base64_decode input is not valid encoded bytes`, or `hex_decode input is not valid encoded bytes`.
Like every message error, it names the failed function without exposing its input.

## IP Addresses And Networks

An IP address is a `BYTES` value in network byte order: four octets for an IPv4 address and sixteen
for an IPv6 address, so its length is its family. `ip_from_string` and `is_ip_address` read an
address from text, `ip_in_network` reads its network from text, and every other address function
reads the octets as one 32-bit or 128-bit number. Parse an address once into a `BYTES` field and
mask and test that field, so each further test costs no second parse: `ip_from_string` can fail, so
two calls of it are two parses even in one expression. A function over an address that is not a
constant reports its own errors and is evaluated at each occurrence.

| Function | Returns | Notes |
| --- | --- | --- |
| `ip_from_string(text)` | `BYTES` | The address `text` writes: 4 octets for IPv4 and 16 for IPv6 |
| `ip_to_string(address)` | `STRING` | The address in its canonical text form |
| `ip_family(address)` | `I64` | `4` for an IPv4 address and `6` for an IPv6 address |
| `ip_trunc(address, prefix_length)` | `BYTES` | The address with every bit after its first `prefix_length` bits cleared: the first address of the network of that length that holds it. `prefix_length` may be any integer type |
| `ip_in_network(address, network)` | `BOOL` | True when `address` lies in `network`, a `STRING` in CIDR notation such as `'10.0.0.0/8'` |
| `ip_unmap(address)` | `BYTES` | The IPv4 address an IPv4-mapped IPv6 address carries, and every other address unchanged |
| `is_ip_address(text)` | `BOOL` | True when `ip_from_string` reads an address from `text`. Never fails |

`ip_from_string` reads an IPv4 address as four decimal numbers from `0` to `255` separated by dots,
each without leading zeros, so `010.0.0.1` is rejected rather than read as octal. It reads an IPv6
address in the text forms of RFC 4291: eight groups of one to four hexadecimal digits in either
letter case, `::` in place of one run of zero groups, and optionally the last two groups written as
an IPv4 address, as in `::ffff:192.0.2.1`. The text is the address alone: surrounding whitespace,
brackets, a port, a prefix length, and an IPv6 zone index such as `%eth0` are rejected. No function
resolves a name, so `ip_from_string('localhost')` fails like any other text that is not an address.

`ip_to_string` writes an IPv4 address in dotted decimal and an IPv6 address in the canonical form of
RFC 5952: lowercase hexadecimal groups without leading zeros, with the longest run of two or more
zero groups, or the first of two equally long runs, written as `::`. An IPv4-mapped address is
written with its IPv4 address in dotted decimal.
`ip_to_string(ip_from_string('2001:DB8:0:0:0:0:0:1'))` is `2001:db8::1`, and
`ip_to_string(ip_from_string('::FFFF:C000:201'))` is `::ffff:192.0.2.1`. A `BYTES` address renders
as base64 in subscriptions and JSON payloads, so write it with `ip_to_string` to show it as text.

A network is written as an address, `/`, and a prefix length in decimal without leading zeros: `0`
to `32` for an IPv4 network and `0` to `128` for an IPv6 network. The address may set no bit after
the prefix, so `'10.0.0.1/8'` is rejected rather than read as `'10.0.0.0/8'`. `'0.0.0.0/0'` holds
every IPv4 address and `'::/0'` every IPv6 address. A network written as a literal, or an expression
that compiles to a constant as a [constant pattern](#regular-expressions) does, is read once when the
statement is applied, and one that does not read rejects the statement with a message that names its
defect, such as `function 'ip_in_network' network '10.0.0.1/8' has host bits set past its prefix
length`. Any other network, including one built with `concat`, is read for each message, a message
whose network is the text last read reuses that reading, and a network that does not read fails
its message.

Each address has exactly one family, and no function converts between families unless it says so:

- `ip_in_network` is false for an address of the other family than its network, so no IPv6 address
  lies in an IPv4 network.
- `ip_from_string('::ffff:10.0.0.1')` is a 16-octet IPv6 address, the IPv4-mapped form of
  `10.0.0.1` that dual-stack sockets report for IPv4 clients. It does not lie in `'10.0.0.0/8'`, and
  `ip_unmap` of it does. Test `ip_in_network(ip_unmap(address), '10.0.0.0/8')` to treat such
  clients as IPv4.
- `ip_unmap` converts only IPv4-mapped addresses, the `::ffff:0:0/96` range. An IPv4-compatible
  address such as `::a09:807`, and every other IPv6 or IPv4 address, is returned unchanged.
- A prefix length counts bits at the address's own width, so `ip_trunc(address, 24)` keeps the first
  24 of the 32 bits of an IPv4 address and the first 24 of the 128 bits of an IPv6 address. Where
  both families occur, choose the length by family:
  `ip_trunc(address, IF ip_family(address) = 4 THEN 24 ELSE 48 END)`.

A null argument produces a null result. Otherwise a function fails only the messages it cannot
answer:

| Function | Fails when | Error |
| --- | --- | --- |
| `ip_from_string` | `text` is not an address in the forms above | `cast_failed`: `ip_from_string input is not an IPv4 or IPv6 address` |
| `ip_to_string`, `ip_family`, `ip_trunc`, `ip_in_network`, `ip_unmap` | `address` is neither 4 nor 16 octets long | `invalid_argument`, such as `ip_family input is not a 4 or 16 byte IP address` |
| `ip_trunc` | `prefix_length` is negative or longer than the address | `invalid_argument`, such as `ip_trunc prefix length must be 0 to 32 for an IPv4 address` |
| `ip_in_network` | A network that is not a constant does not read | `invalid_argument`: `ip_in_network network` followed by the defect: `is not written as address/prefix`, `address is not an IPv4 or IPv6 address`, `prefix length is not a decimal number without leading zeros`, `prefix length must be 0 to 32 for an IPv4 network`, or `has host bits set past its prefix length` |
| `ip_from_string`, `ip_trunc`, `ip_to_string` | The result does not fit in what its column has left, as in [Result Size](#result-size) | `overflow`, such as `ip_from_string result exceeds the bytes one column holds` or `ip_to_string result exceeds the text one STRING column holds` |

`ip_trunc` checks its `prefix_length` first, so a null prefix length yields null whatever the
address holds. An address function over a literal, such as `ip_from_string('192.0.2.300')`, is
computed once for each batch, so a literal that does not read fails every message that evaluates
it rather than the statement. A statement that passes an argument of another type is rejected, such
as `function 'ip_in_network' requires BYTES input, found Utf8`.

To route malformed text explicitly instead of failing the message, test it first:
`CASE WHEN is_ip_address(input.client) THEN ip_from_string(input.client) END` reads only the
messages that hold an address and is null for the others. Address functions keep the sensitivity
of their arguments, so whether a sensitive address lies in a network is sensitive.

```nspl,ignore
SET client = ip_from_string(input.client_ip),
    client_subnet = ip_to_string(ip_trunc(output.client, IF ip_family(output.client) = 4 THEN 24 ELSE 48 END)),
    internal = ip_in_network(ip_unmap(output.client), '10.0.0.0/8')
WHERE NOT output.internal
```

## URLs

URL functions read their `STRING` argument as an absolute URL under the WHATWG URL Standard, the
rules web browsers apply, through the Rust `url` crate. They compute only from the text: none
fetches a URL, resolves a host, or consults the public suffix list.

| Function | Returns | Notes |
| --- | --- | --- |
| `url_scheme(url)` | `STRING` | The scheme, lowercased, without its `:` |
| `url_host(url)` | optional `STRING` | The host, or null when the URL has none |
| `url_port(url)` | optional `I64` | The port, the scheme's default port when the URL names none, or null when it has neither |
| `url_path(url)` | `STRING` | The path, percent-encoded |
| `url_query(url)` | optional `STRING` | The query without its `?`, percent-encoded, or null when the URL has no `?` |
| `url_fragment(url)` | optional `STRING` | The fragment without its `#`, percent-encoded, or null when the URL has no `#` |
| `url_query_value(url, name)` | optional `STRING` | The decoded value of the first query parameter named `name`, or null when there is none |
| `url_query_values(url, name)` | `VEC<STRING>` | The decoded values of every query parameter named `name`, in query order, or an empty vector when there is none |
| `url_decode(text)` | `STRING` | `text` with every percent escape decoded |
| `is_url(text)` | `BOOL` | True when the other URL functions read `text` as a URL. Never fails |

A URL is read and normalized as the URL Standard specifies:

- Leading and trailing spaces and control characters are removed, and so are tabs and newlines
  anywhere in the text.
- The scheme is lowercased. For the schemes `http`, `https`, `ws`, `wss`, `ftp`, and `file`, the
  host is lowercased and an internationalized domain name is written in its ASCII form, so the host
  of `http://Bücher.example/` is `xn--bcher-kva.example`. An IPv4 host is written in dotted
  decimal, including the forms browsers accept, so the host of `http://0x7f.1/` is `127.0.0.1`.
- An IPv6 host is written as the URL Standard writes it, lowercase, compressed, and never in dotted
  decimal, and `url_host` returns it without its brackets: the host of `http://[::FFFF:192.0.2.1]/`
  is `::ffff:c000:201`, which `ip_from_string` reads.
- `.` and `..` path segments are resolved, and a space or any other character a path cannot hold is
  percent-encoded: `url_path('https://example.com/a/./b/../c d')` is `/a/c%20d`. The path of a URL
  of those six schemes starts with `/`.
- A port equal to the scheme's default is removed, so `url_port` answers `80` for
  `http://example.com:80/` as for `http://example.com/`. The defaults are `80` for `http` and `ws`,
  `443` for `https` and `wss`, and `21` for `ftp`. A URL of any other scheme that names no port has
  a null port.
- Other schemes have no default port and may have no host: the path of `mailto:ops@example.com` is
  `ops@example.com`, and its host is null. Their hosts are kept as written, letter case included,
  without the ASCII form of an internationalized name.

A component the URL lacks is null: the host of a `mailto:` or `urn:` URL and of a `file:` URL with
an empty host, the query of a URL without `?`, and the fragment of a URL without `#`. A URL that ends
in `?` or `#` has an empty query or fragment rather than a null one.

The query is read as `application/x-www-form-urlencoded` parameters. It is split at every `&`, an
empty parameter is skipped, and each parameter is split at its first `=` into a name and a value; a
parameter without `=` has an empty value. Names and values are percent-decoded with `+` read as a
space, so the query `q=hello+world%21&tag=x&tag=y%26z` has the value `hello world!` for `q` and the
values `x` and `y&z` for `tag`. A parameter matches when its decoded name equals `name` exactly,
letter case included. A parameter whose name does not decode matches no name. `url_query_value`
reads only the first matching parameter, so only its value must decode, while `url_query_values`
fails the message when any matching value does not decode.

`url_decode` replaces each `%` followed by two hexadecimal digits of either case with the octet they
name, and keeps every other character as it is, `+` included, so `url_decode(url_path(input.url))`
is the readable path. Rather than guess, it fails a message whose text has a `%` not followed by two
hexadecimal digits, or whose decoded octets are not UTF-8.

A null argument produces a null result. Otherwise a function fails only the messages it cannot
answer:

| Function | Fails when | Error |
| --- | --- | --- |
| Every URL function except `url_decode` and `is_url` | `url` is not an absolute URL | `cast_failed`, naming the function and the URL Standard's reason, such as `url_host input is not an absolute URL: relative URL without a base`; other reasons include `empty host`, `invalid port number`, `invalid IPv4 address`, `invalid IPv6 address`, `invalid domain character`, and `invalid international domain name`, which a space in the host of an `http` URL reports |
| `url_query_value`, `url_query_values` | The value of a matching parameter is not percent-encoded UTF-8 | `cast_failed`, such as `url_query_value input is not valid percent-encoded UTF-8` |
| `url_decode` | `text` is not percent-encoded UTF-8 | `cast_failed`: `url_decode input is not valid percent-encoded UTF-8` |
| Every URL function except `url_port` and `is_url` | The result does not fit in what its column has left | `overflow`, such as `url_path result exceeds the text one STRING column holds` or `url_query_values result exceeds what one VEC<STRING> column holds` |

A relative reference, such as the request target `/search?q=x` of an HTTP request line, is not an
absolute URL. Prefix it with a base explicitly:
`url_query_value(concat('http://localhost', input.target), 'q')`. To route text that may not be a
URL, test it first: `CASE WHEN is_url(input.referrer) THEN url_host(input.referrer) END` is null for
the messages whose referrer is not a URL.

Each call reads its own URL, so `url_host(input.url)` and `url_path(input.url)` each parse it, while
a URL that every message shares, such as a literal, is read once per batch, and `url_query_value`
and `url_query_values` reuse one reading while consecutive messages hold the same URL. `is_url`
over a constant is answered when the statement is applied. Percent-encoding and ASCII domain names
can make a component longer than the text it came from, and like the results of `repeat`, a
component that does not fit in what its `STRING` column has left reports an `overflow` error. URL
functions keep the sensitivity of their arguments, so the host of a sensitive URL is sensitive.

```nspl,ignore
SET scheme = url_scheme(input.referrer),
    referrer_host = url_host(input.referrer),
    campaign = url_query_value(input.referrer, 'utm_campaign'),
    tags = url_query_values(input.referrer, 'tag'),
    landing_path = url_decode(url_path(input.referrer))
```

## Numeric Functions

Numeric functions accept every integer and floating-point type unless a description below narrows
it. A function that returns `F64` reads an integer argument as the nearest `F64`, which is exact for
every type up to 32 bits and rounds `I64` and `U64` values beyond 2^53, and reads an `F32` argument
exactly, so its result follows the `F32`'s stored value. `atan2`, `log(base, x)`, and `pow` take two
arguments that may have different numeric types, and always return `F64`: `pow(-8, 3)` is the `F64`
`-512.0`. A floating-point result that is not finite, such as `sqrt(-1.0)`, `ln(0.0)`, or
`exp(1000.0)`, reports a per-message `invalid_argument` error and yields null instead of producing
NaN or an infinity. The error names the function by its canonical name, such as `pow produced a
non-finite result` for a call written `power(...)`.

| Function | Returns | Notes |
| --- | --- | --- |
| `abs(x)` | same numeric type as input | The absolute value. An unsigned value is returned unchanged, and `abs(-0.0)` is `0.0`. The minimum value of a signed integer type has none and reports an overflow |
| `acos(x)` | `F64` | Arc cosine in radians |
| `asin(x)` | `F64` | Arc sine in radians |
| `atan(x)` | `F64` | Arc tangent in radians |
| `atan2(y, x)` | `F64` | The angle in radians, from `-π` to `π`, of the point `(x, y)`. The signs of both arguments select the quadrant, so `atan2(0.0, -1.0)` is `π`. `y` comes first, and the two arguments may have different numeric types |
| `ceil(x)` | same numeric type as input | Rounds up. Integer input is returned unchanged |
| `ceiling(x)` | same numeric type as input | Alias for `ceil` |
| `cos(x)` | `F64` | Cosine of an angle in radians |
| `degrees(x)` | `F64` | Converts an angle from radians to degrees |
| `exp(x)` | `F64` | `e` raised to `x` |
| `floor(x)` | same numeric type as input | Rounds down. Integer input is returned unchanged |
| `ln(x)` | `F64` | Natural logarithm |
| `log(x)` | `F64` | Base-10 logarithm |
| `log(base, x)` | `F64` | Logarithm with explicit base |
| `log2(x)` | `F64` | Base-2 logarithm |
| `pow(x, y)` | `F64` | `x` raised to `y` |
| `power(x, y)` | `F64` | Alias for `pow` |
| `radians(x)` | `F64` | Converts an angle from degrees to radians |
| `round(x)` | same numeric type as input | Rounds to the nearest integer, with halves rounded away from zero. Integer input is returned unchanged |
| `round(x, digits)` | same numeric type as `x` | Rounds to `digits` decimal places, or for a negative `digits` to a multiple of `10^-digits`. See [Precision Rounding](#precision-rounding) |
| `sign(x)` | same numeric type as input | `-1` for a negative value, `1` for a positive one, and `0` for zero, so an unsigned value has the sign `0` or `1`. A float zero keeps its sign, an infinity has the sign `-1.0` or `1.0`, and NaN has no sign and reports an error |
| `sin(x)` | `F64` | Sine of an angle in radians |
| `sqrt(x)` | `F64` | Square root |
| `tan(x)` | `F64` | Tangent of an angle in radians |
| `trunc(x)` | same numeric type as input | Rounds toward zero. Integer input is returned unchanged |

`abs`, `ceil`, `floor`, `round`, `sign`, `sqrt`, and `trunc` are exact: each returns the correctly
rounded result, which is the same on every node. Rounding keeps the sign of a zero result, so
`ceil(-0.4)`, `round(-0.4)`, and `trunc(-0.5)` are `-0.0`, and `sqrt(-0.0)` is `-0.0` without an
error. `radians` and `degrees` multiply by the `F64` nearest to `π/180` and to `180/π`, so their
result is the same on every node and within two units in the last place of the exact conversion.

`acos`, `asin`, `atan`, `atan2`, `cos`, `exp`, `ln`, `log(x)`, `log2`, `pow`, `sin`, and `tan`
evaluate in IEEE 754 double precision through the C math library of the platform a node runs on,
which is glibc for the published Linux builds. Their precision is that library's: Nervix states no
bound of its own, and a node with another C library, another version of it, or a CPU for which the
library selects another implementation can differ in the last places. `log(base, x)` computes
`ln(x) / ln(base)`, so it carries the rounding of two logarithms and a division, which exceeds two
units in the last place for some arguments.

A function reports an error exactly where its result is not a finite value of its type. For finite
arguments, that is:

| Function | Arguments that report an error |
| --- | --- |
| `abs`, `ceil`, `floor`, `round(x)`, `sign`, `trunc` | None. `abs` over the minimum value of a signed integer type reports an `overflow` error |
| `round(x, digits)` | A negative `digits` whose rounded multiple exceeds the range of `x`'s type. A float reports an `invalid_argument` error and an integer an `overflow` error |
| `sqrt(x)` | `x < 0` |
| `ln(x)`, `log(x)`, `log2(x)` | `x <= 0` |
| `log(base, x)` | `x <= 0`, `base < 0`, and `base = 1`. A `base` of `0.0` or `-0.0` gives `0.0` or `-0.0`, since `ln(0)` is an infinity |
| `acos(x)`, `asin(x)` | `x < -1` and `x > 1` |
| `exp(x)` | `x` above about `709.78`, where the result overflows. A result too small to represent is `0.0` |
| `degrees(x)` | `x` beyond about `±3.14e306`, where the result overflows |
| `pow(x, y)` | A result that overflows, `x < 0` with a `y` that is not an integer, and `x = 0` with `y < 0` |
| `atan(x)`, `atan2(y, x)`, `cos(x)`, `radians(x)`, `sin(x)`, `tan(x)` | None |

A NaN or infinite argument reports an error unless a finite result is defined for it, as IEEE 754
defines, for example, for `atan` of an infinity, `atan2` of any arguments that are not NaN, `exp` of
negative infinity, `log(base, x)` with an infinite `base` and a finite positive `x`, and these
`pow` cases: `pow(1, y)` is `1` for every `y`, NaN included; `pow(x, 0)` and `pow(x, -0.0)` are
`1`; `pow(-1, ±inf)` is `1`; `pow(x, -inf)` is `0` where `|x| > 1` and `pow(x, inf)` is `0` where
`|x| < 1`; and `pow(±inf, y)` is a zero for a negative `y`. `sign` of an infinity is also defined.
`abs`, `ceil`, `floor`, `round`, and `trunc` report an `invalid_argument` error for a NaN or
infinite argument, such as `floating-point absolute value produced a non-finite result` or
`round produced a non-finite result`.

## Precision Rounding

`round(x, digits)` takes any integer type for `digits`, independently of the numeric type of `x`,
and returns `x`'s type. A positive `digits` counts decimal places after the point, `0` rounds to the
nearest integer as `round(x)` does, and a negative `digits` rounds to a multiple of a power of ten:
`round(1250, -2)` is `1300`. Halves round away from zero at every position, so `round(-0.125, 2)` is
`-0.13`. Every count is accepted. When `10^-digits` is finer than the smallest step between values
of `x`'s type near `x`, the result is `x` itself, and when `x` is less than half of `10^-digits` in
magnitude, the result is zero.

A float is rounded exactly. The result is the value of `x`'s type nearest to the stored value of `x`
rounded to `digits` decimal places, so it is the same on every node. The stored value of a decimal
literal is the nearest binary float, which can lie on either side of a decimal tie:

- `round(0.125, 2)` is `0.13`, because `0.125` is stored exactly and is a tie.
- `round(2.675, 2)` is `2.67`, because the `F64` nearest to `2.675` is slightly below it.
- An `F32` rounds its own stored value: the `F32` nearest to `0.15` is slightly above `0.15`, so
  `round(x, 1)` of that `F32` is `0.2`.

A float result that is zero keeps the sign of `x`, as `round(-0.004, 2)` is `-0.0`. Rounding to a
multiple of a power of ten can exceed the largest finite value, as `round(1.7976931348623157e308,
-308)` does, and reports an `invalid_argument` error.

An integer has no decimal places, so a `digits` of `0` or more returns it unchanged. A negative
`digits` rounds it to a multiple of `10^-digits`, and a multiple that does not fit its type reports
an `overflow` error: `round(x, -1)` over the `I32` value `2147483647` would be `2147483650`.

## Floating-Point Classification

Classification functions take an `F32` or `F64` argument. An integer argument is rejected when the
statement is applied, since no integer is NaN or infinite. A null argument produces a null result,
and no value makes them fail.

| Function | Returns | Notes |
| --- | --- | --- |
| `is_nan(x)` | `BOOL` | True for NaN, whatever its sign or payload |
| `is_finite(x)` | `BOOL` | True for every value that is neither NaN nor an infinity, including both zeros |
| `is_infinite(x)` | `BOOL` | True for positive and negative infinity |

A decoded float or a cast from `STRING` can hold NaN or an infinity, so a route can test for them
before calling a function that would report an error: `IF is_finite(input.reading) THEN
sqrt(abs(input.reading)) ELSE 0.0 END`.

## Bitwise Functions

Bitwise functions take integer arguments and read each value as its two's complement bits at the
width of its type, so `bitwise_not` over the `U8` value `0` is `255` and over the `I8` value `0` is
`-1`. A null argument produces a null result.

| Function | Returns | Notes |
| --- | --- | --- |
| `bitwise_and(a, b)` | same integer type as inputs | Both arguments must have the same integer type |
| `bitwise_or(a, b)` | same integer type as inputs | Both arguments must have the same integer type |
| `bitwise_xor(a, b)` | same integer type as inputs | Both arguments must have the same integer type |
| `bitwise_not(a)` | same integer type as input | Inverts every bit |
| `bit_count(a)` | `I64` | The number of set bits. A negative value counts its sign bits, so `bit_count` of the `I64` value `-1` is `64` |
| `shift_left(value, count)` | same integer type as `value` | `value` multiplied by `2^count` |
| `shift_right(value, count)` | same integer type as `value` | `value` divided by `2^count`, rounded toward negative infinity |

`bitwise_and`, `bitwise_or`, `bitwise_xor`, `bitwise_not`, and `bit_count` never fail.

A shift takes `count` from any integer type, independently of the type of `value`, and a negative
`count` reports an `invalid_argument` error. Shifts are checked like arithmetic:

- `shift_left` reports an `overflow` error when the product does not fit the type of `value`,
  including a signed value whose sign would change: `shift_left` of the `I8` value `64` by `1` fails,
  and of the `I8` value `-1` by `7` is `-128`. A `count` at or beyond the type's width moves every
  bit out, so it returns `0` for a zero `value` and fails for any other.
- `shift_right` never overflows. A signed value keeps its sign, so `shift_right(-5, 1)` is `-3`, and a
  `count` at or beyond the type's width returns `-1` for a negative `value` and `0` for any other.

## Datetime Functions

A `DATETIME` is a UTC instant with nanosecond precision. Its range is the signed Unix-nanosecond
range, `1677-09-21T00:12:43.145224192Z` through `2262-04-11T23:47:16.854775807Z`. Datetime functions
compute exactly from the values they are given and never read a clock, so a call gives the same
result for the same arguments whenever and on whichever node it runs. The current time enters an
expression as a value only through `now()`, which returns the execution-local domain time:
`date_trunc('day', now(), 'Europe/Berlin')` follows a paced domain's logical clock at any
`TIME RATE`, while the work a call does, and how long it takes, never depends on the domain's pace.

| Function | Returns | Notes |
| --- | --- | --- |
| `date_part(part, value[, zone])` | `I64` | One part of `value`'s local date and time in `zone`, named by `part` from the table of date parts below |
| `date_trunc(unit, value[, zone])` | `DATETIME` | The first instant of the local `unit` that holds `value` in `zone`. See [Calendar Arithmetic](#calendar-arithmetic) |
| `date_bin(unit, width, value, origin)` | `DATETIME` | The start of the bin that holds `value`, for bins `width` units wide that start at `origin` and at every whole number of widths before and after it |
| `date_add(unit, amount, value[, zone])` | `DATETIME` | `value` moved by `amount` units in `zone`, backward when `amount` is negative. `amount` may be any integer type |
| `date_diff(unit, start, end[, zone])` | `I64` | The whole units from `start` to `end` in `zone`, rounded toward zero. Negative when `end` is before `start` |
| `to_unix(unit, value)` | `I64` | The whole units from `1970-01-01T00:00:00Z` to `value`, rounded down |
| `from_unix(unit, count)` | `DATETIME` | The instant `count` units after `1970-01-01T00:00:00Z`, or before it when `count` is negative. `count` may be any integer type |
| `format_datetime(format, value[, zone])` | `STRING` | `value`'s local date and time in `zone`, written in `format`. See [Datetime Formats](#datetime-formats) |
| `parse_datetime(format, text[, zone[, disambiguation]])` | `DATETIME` | The instant `text` names when read in `format`. See [Datetime Formats](#datetime-formats) |

`unit`, `part`, `width`, `zone`, `format`, and `disambiguation` are literals written in the call, not
expressions, and they are checked when the statement is applied: a zone is resolved and a format is
compiled once then, never for each message. `unit`, `part`, `zone`, and `disambiguation` are
`STRING` literals whose names are case-insensitive, and `format` is a `STRING` literal. `width` is a
positive integer literal, and `width` units must not exceed 9,223,372,036,854,775,807 nanoseconds,
about 292 years. A name must match exactly apart from letter case, so plurals and abbreviations such
as `days` or `ms` are unknown. A call with an unknown name, a non-literal argument, an invalid
format, or a width that is not positive is rejected, and its message names what the call accepts,
such as `function 'date_bin' does not accept time unit 'month'; expected one of nanosecond,
microsecond, millisecond, second, minute, hour, day, week` or `function 'date_part' requires its
date part to be a STRING literal`. A call that names no `zone` reads in UTC; see
[Time Zones](#time-zones) for the zones a call can name.

The units `nanosecond` through `week` have a fixed length. A `DATETIME` has no leap seconds, so
every UTC day is exactly 86,400 seconds long. `month`, `quarter`, and `year` are calendar units,
whose length depends on the month they count from. `date_trunc`, `date_add`, and `date_diff` accept
every unit; `date_bin`, `to_unix`, and `from_unix` accept only units of fixed length.

| Unit | Length |
| --- | --- |
| `nanosecond` | 1 nanosecond |
| `microsecond` | 1,000 nanoseconds |
| `millisecond` | 1,000 microseconds |
| `second` | 1,000 milliseconds |
| `minute` | 60 seconds |
| `hour` | 60 minutes |
| `day` | 24 hours, or one local calendar day in an IANA time zone |
| `week` | 7 days, or seven local calendar days in an IANA time zone |
| `month` | One calendar month |
| `quarter` | Three calendar months, starting in January, April, July, and October |
| `year` | Twelve calendar months, starting in January |

A date part reads `value`'s local date and time in `zone`:

| Part | Range | Meaning |
| --- | --- | --- |
| `year` | 1677–2262 | The proleptic Gregorian year |
| `quarter` | 1–4 | The quarter of the year |
| `month` | 1–12 | The month |
| `day` | 1–31 | The day of the month |
| `hour` | 0–23 | The hour of the day |
| `minute` | 0–59 | The minute of the hour |
| `second` | 0–59 | The whole seconds of the minute |
| `millisecond` | 0–999 | The whole milliseconds past the second |
| `microsecond` | 0–999,999 | The whole microseconds past the second |
| `nanosecond` | 0–999,999,999 | The nanoseconds past the second |
| `day_of_week` | 0–6 | The day of the week, from Sunday as `0` |
| `day_of_year` | 1–366 | The day of the year |
| `iso_year` | 1677–2262 | The ISO 8601 week-numbering year that `iso_week` belongs to |
| `iso_week` | 1–53 | The ISO 8601 week, whose week 1 holds the year's first Thursday |
| `iso_day_of_week` | 1–7 | The ISO 8601 day of the week, from Monday as `1` |

Results are exact to the nanosecond, and each function rounds in one fixed direction:

- `date_trunc`, `date_bin`, and `to_unix` round toward negative infinity, including before the Unix
  epoch and before an origin, so a value belongs to the unit or bin that starts at or before it.
  `date_trunc('day', ...)` of `1969-12-31T23:59:59.999999999Z` is `1969-12-31T00:00:00Z`, and
  `to_unix('millisecond', ...)` of the same value is `-1`.
- In UTC, a day, and every shorter unit, starts a whole number of units after the Unix epoch. A week
  starts on Monday in `date_trunc`, while `to_unix('week', ...)` counts whole weeks from the epoch
  itself, which was a Thursday.
- `date_bin` accepts an `origin` before or after `value`. A value exactly on a bin boundary starts
  its own bin.
- `date_diff` rounds toward zero. For units that count elapsed time, exchanging `start` and `end`
  only changes the sign of the result, and `date_diff('second', start, end)` is `0` when the two
  values are less than a second apart in either direction.

A null argument produces a null result. `date_part`, `to_unix`, and `format_datetime` never fail a
message; see [Writing Values](#writing-values) for the one batch-wide limit of `format_datetime`.
Another function reports a per-message error and yields null exactly where it cannot produce a
result:

| Function | Fails when |
| --- | --- |
| `date_trunc`, `date_bin` | The unit or bin that holds `value` starts before the `DATETIME` range, with an `overflow` error |
| `date_add` | The moved instant is outside the `DATETIME` range, with an `overflow` error. Only the result is checked, so an `amount` whose units alone are longer than the range fails only when the moved instant is outside it |
| `from_unix` | The instant is outside the `DATETIME` range, with an `overflow` error |
| `date_diff` | The whole units do not fit `I64`, which only a count of nanoseconds between values more than about 292 years apart can reach, with an `overflow` error |
| `parse_datetime` | `text` does not name an instant in `format`, with a `cast_failed` error; its local time does not exist or is ambiguous in `zone`, with an `invalid_argument` error; or the instant is outside the `DATETIME` range, with an `overflow` error. See [Reading Text](#reading-text) |

The error's message names the function, such as `date_add result is outside the DATETIME range`,
`date_diff result does not fit I64`, or
`parse_datetime local time is ambiguous in America/New_York`.

```nspl,ignore
SET hour = date_part('hour', input.occurred_at),
    local_hour = date_part('hour', input.occurred_at, 'America/New_York'),
    quarter_hour = date_bin('minute', 15, input.occurred_at, from_unix('second', 0)),
    local_day = date_trunc('day', input.occurred_at, 'Europe/Berlin'),
    renewal = date_add('month', 1, input.subscribed_at),
    deadline = date_add('millisecond', input.timeout_ms, input.occurred_at),
    age_seconds = date_diff('second', input.occurred_at, now()),
    received_at = from_unix('millisecond', input.epoch_ms),
    label = format_datetime('%a %d %b %Y %H:%M %Z', input.occurred_at, 'Asia/Tokyo'),
    logged_at = parse_datetime('%d/%b/%Y:%H:%M:%S %z', input.log_time)
```

## Calendar Arithmetic

`date_trunc`, `date_add`, and `date_diff` work on the local calendar of their `zone`. In UTC and at a
fixed offset every day is 24 hours long. Under the rules of an IANA time zone a local day can be
shorter or longer, because daylight saving time and other transitions skip or repeat local times. An
`hour` or any shorter unit is always elapsed time: `date_add('hour', 1, ...)` is one hour later in
every zone.

`date_trunc` returns the first instant of the local unit that holds `value`: the earliest instant
from which the zone's clock showed times inside that unit without interruption until `value`. A day
starts at local midnight, a week on Monday, a month on its first day, a quarter on the first day of
January, April, July, or October, and a year on January 1.

- `date_trunc('day', ...)` of `2024-03-10T11:00:00Z` in `America/New_York` is
  `2024-03-10T05:00:00Z`, midnight in New York, even though that day is only 23 hours long.
- A unit whose local start a transition skips starts when the skipped span ends. `America/Sao_Paulo`
  skipped midnight on 2018-11-04, so that day starts at `2018-11-04T03:00:00Z`, local `01:00`.
- A local time a transition repeats stays in one unit. New York showed `01:00` through `02:00` twice
  on 2024-11-03, and `date_trunc('hour', ...)` of both `2024-11-03T05:30:00Z` and
  `2024-11-03T06:30:00Z` is `2024-11-03T05:00:00Z`, where the repeated hour began.
- In an IANA zone every unit follows the zone's clock, including local mean time before standard
  time: New York ran 4:56:02 behind UTC until 1883-11-18T17:00:00Z, when its clocks went back from
  12:03:58 local mean time to 12:00 Eastern Standard Time, so `date_trunc('day', ...)` of
  `1883-11-18T17:01:00Z` there is `1883-11-18T04:56:02Z`.

`date_add` moves `value` in local time:

- An `hour` and every shorter unit move by elapsed time.
- A `day` or a `week` moves by 24 hours or 7 days in UTC and at a fixed offset. In an IANA zone it
  moves the local date and keeps the local time of day, so `date_add('day', 1, ...)` of
  `2024-03-09T12:00:00Z` in `America/New_York` is `2024-03-10T11:00:00Z`, 23 hours later.
- A `month`, `quarter`, or `year` moves the local month and keeps the day of the month and the local
  time of day, unless the new month is shorter, in which case the result is on its last day:
  `date_add('month', 1, ...)` of `2024-01-31T23:30:00Z` is `2024-02-29T23:30:00Z`, and
  `date_add('year', 1, ...)` of `2024-02-29T12:00:00Z` is `2025-02-28T12:00:00Z`. Each call moves
  once from `value`, so adding one month twice to January 31, 2024 lands on March 29, while adding
  two months at once lands on March 31.
- When the moved local time falls in a span the zone skips, the result moves forward by the length
  of that span, and when the zone shows the moved local time twice, the result is the earlier
  instant. Adding a `day` to `2024-03-09T07:30:00Z` in New York lands on the skipped `02:30`, so the
  result is `2024-03-10T07:30:00Z`, local `03:30`.
- An `amount` of `0` returns `value` itself.

`date_diff` counts elapsed time for an `hour` and every shorter unit, and for a `day` or a `week` in
UTC or at a fixed offset. It counts whole calendar units for a `day` or a `week` in an IANA zone and
for a `month`, `quarter`, or `year` in every zone. A calendar count measures from `start`:

1. The count runs to `end`'s local date, moved toward `start` by as few days as it takes for
   `start`'s local time of day on that date to not lie past `end`. `date_diff('day', ...)` from
   `2024-03-09T12:00:00Z` to `2024-03-10T11:00:00Z` in `America/New_York` is `1`, because the start's
   local time of day, `07:00`, on 2024-03-10 in New York is `11:00` in UTC, and it is `0` in UTC,
   where the two instants are 23 hours apart.
2. Days count the dates between. Weeks are whole sevens of those days.
3. Months count from `start`'s local date, and a month is complete once the date has reached
   `start`'s day of the month. `date_diff('month', ...)` from `2024-01-31T00:00:00Z` to
   `2024-02-29T00:00:00Z` is `0`, even though `date_add('month', 1, ...)` of the start is the end.
   Quarters and years are whole threes and twelves of those months.

Because a calendar count measures from `start`, exchanging `start` and `end` can change its
magnitude as well as its sign. Two instants on the same local date are `0` days apart.

## Time Zones

A `zone` is one of:

- `'UTC'`.
- An IANA time zone name, such as `'Europe/Berlin'`, `'America/New_York'`, or `'Asia/Kolkata'`,
  matched without regard to letter case.
- A fixed UTC offset written `'+HH:MM'` or `'-HH:MM'`, from `'-23:59'` to `'+23:59'`. `'-00:00'`
  is the same zone as `'+00:00'`, which reads as UTC does but is written `+00:00` by `%Z` and `%Q`.

Anything else, including an abbreviation such as `'CEST'`, `'PST'`, or `'EDT'` that is not also an
IANA name, the host's local zone, or an offset written another way, is rejected when the statement
is applied, with `function '<name>' does not accept time zone '<zone>'; expected an IANA time zone
name, UTC, or a UTC offset such as '+05:30'`. Some names that look like abbreviations are IANA
names, and are accepted with the rules the database gives them: `'EST'`, `'MST'`, `'HST'`, `'CET'`,
`'EET'`, `'WET'`, `'MET'`, `'GMT'`, `'UCT'`, `'Universal'`, and `'Zulu'`, as well as the
`'Etc/GMT+5'` family, whose sign is the reverse of the offset's. A zone only decides how an instant
reads as a local date and time: every `DATETIME` a function returns is a UTC instant.

Time zone rules come from the IANA Time Zone Database bundled into Nervix, release `2026c` in this
version. Nervix never reads the time zone database, the time zone, or the locale of the host it runs
on, so every node of a cluster computes the same local times. A Nervix upgrade can bundle a newer
release, and a zone whose rules the release changed then reads differently in every computation
after the upgrade, including computations over instants in the past.

An IANA zone keeps the name the database gives it: `%Q` writes `America/New_York` for
`'america/new_york'`, and `US/Eastern` for the alias `'US/Eastern'`. A fixed offset is written in
its `+HH:MM` form by both `%Q` and `%Z`, and UTC as `UTC`.

## Datetime Formats

`format_datetime` writes a local date and time in a format, and `parse_datetime` reads one. A format
is a `STRING` literal in which `%` starts a directive and every other character, including
whitespace, is literal text written as it is and read only where it appears exactly. Names are
English and never depend on a locale.

| Directive | Writes and reads | Longest |
| --- | --- | --- |
| `%Y` | The year, in four digits | 4 |
| `%m` | The month, `01`–`12` | 2 |
| `%b`, `%B` | The month's name, abbreviated as `Jan` or in full as `January` | 3, 9 |
| `%d` | The day of the month, `01`–`31` | 2 |
| `%e` | The day of the month padded with a space, ` 1`–`31` | 2 |
| `%j` | The day of the year, `001`–`366` | 3 |
| `%a`, `%A` | The day of the week's name, abbreviated as `Mon` or in full as `Monday` | 3, 9 |
| `%u` | The ISO 8601 day of the week, from Monday as `1` to Sunday as `7` | 1 |
| `%w` | The day of the week, from Sunday as `0` to Saturday as `6` | 1 |
| `%G` | The ISO 8601 week-numbering year, in four digits | 4 |
| `%V` | The ISO 8601 week, `01`–`53` | 2 |
| `%H` | The hour of the 24-hour clock, `00`–`23` | 2 |
| `%I` | The hour of the 12-hour clock, `01`–`12` | 2 |
| `%p` | `AM` before noon and `PM` from noon | 2 |
| `%M` | The minute, `00`–`59` | 2 |
| `%S` | The second, `00`–`59` | 2 |
| `%f` | The fraction of the second in nine digits | 9 |
| `%1f`–`%9f` | The fraction of the second in exactly that many digits, truncated | 1–9 |
| `%.f` | Nothing for a whole second, and otherwise `.` and the fraction without trailing zeros | 10 |
| `%z` | The UTC offset as `+hhmm`, followed by `ss` when the offset has seconds | 7 |
| `%:z` | The UTC offset as `+hh:mm`, followed by `:ss` when the offset has seconds | 9 |
| `%::z` | The UTC offset as `+hh:mm:ss` | 9 |
| `%s` | The whole seconds since `1970-01-01T00:00:00Z`, rounded down | 20 |
| `%Z` | The abbreviation the zone shows, such as `EST`; written only | The zone's longest |
| `%Q` | The zone's name, such as `Europe/Berlin`; written only | The name's length |
| `%F` | `%Y-%m-%d` | 10 |
| `%T` | `%H:%M:%S` | 8 |
| `%R` | `%H:%M` | 5 |
| `%%`, `%n`, `%t` | A `%`, a newline, and a tab | 1 |

The `-` flag writes a number without padding: `%-m`, `%-d`, `%-j`, `%-V`, `%-H`, `%-I`, `%-M`, and
`%-S`. Any other directive, including a two-digit year, a locale's date or time such as `%c`, a
fraction written `%.3f` rather than `%3f`, and a flag on a directive that does not take it, such as
`%-e`, is rejected when the statement is applied, and the message names the directive and the byte
of the format it starts at, such as `unknown directive '%q' at byte 3`. A format that ends inside a
directive, such as a trailing `%` or `%-`, is rejected with `incomplete directive at byte <n>`. Offsets from local mean time
can have seconds, as New York's `-04:56:02` before 1883 does. `%s` counts whole seconds rounded
down, so `%s%.f` writes `1969-12-31T23:59:59.5Z` as `-1.5`: one second before the epoch plus half a
second.

Every value a format describes must fit in 256 bytes, counting every directive at its longest in
the call's zone. A format that could describe a longer value is rejected when the statement is
applied, so each value of a formatted column holds at most 256 bytes.

### Writing Values

`format_datetime` writes `value`'s local date and time in `zone`, and writes a null `value` as null.
It never fails a message. `format_datetime('%FT%T%:z', ...)` of `2024-11-03T06:30:00Z` in
`America/New_York` is `2024-11-03T01:30:00-05:00`, and an empty format writes an empty string.

A call sizes its column for every message it formats at the format's longest value before it writes
any, so a batch whose messages could together exceed the 2,147,483,647 bytes one `STRING` column
holds fails as a whole with `format_datetime values for <n> messages of up to <m> bytes each could
exceed the text one STRING column holds`. At the 256-byte maximum that takes more than 8,388,607
formatted messages in one batch.

### Reading Text

`parse_datetime` reads `text` from its first byte to its last and never guesses or normalizes what
it reads:

- Literal text must appear exactly, and a directive reads exactly its field: `%m` reads two digits,
  and `%e` reads a space and a digit, or two digits. A `-` flag reads as many digits as the field's
  width allows, with or without a leading zero. `%.f` reads nothing, or `.` and one to nine digits;
  a `.` without a digit after it does not match. `%s` reads an optional `-`, never a `+`, and one to
  nineteen digits. `%Y` and `%G` read any four digits, and a year outside the `DATETIME` range fails
  as that range does.
- Month and weekday names, and `AM` and `PM`, are read without regard to letter case. `%z`, `%:z`,
  and `%::z` read the forms they write, and also read an uppercase `Z` as UTC. Each part of an
  offset is at most `23:59:59`.
- Every field must lie in its range: a 13th month, hour `24`, and second `60` are rejected, since a
  `DATETIME` has no leap seconds. The date must exist, so `2023-02-29` is rejected rather than read
  as March 1, and a day of the week read with a calendar or ordinal date must be that date's day.

The first field that fails is reported, checking the time of day before the date: a text such as
`2023-02-29 24:00:00` reports its hour. A day of the year `366` in a common year and an ISO week
`53` in a year of 52 weeks are dates that do not exist.

A readable format reads each field at most once and names exactly one instant. An empty format
writes an empty string but reads no instant:

- A date, as `%Y` with a month and a day of the month, `%Y` with `%j`, `%G` with `%V` and a day of
  the week, or `%s` with nothing but a fraction of a second. `%Z` and `%Q` cannot be read.
- Optionally a time of day, as `%H`, or `%I` with `%p`. `%M` requires an hour, `%S` requires `%M`,
  and a fraction of a second requires `%S`. A time of day the format does not read is midnight.
- Optionally a UTC offset. A format that reads `%z`, `%:z`, or `%::z` places its local date and time
  at that offset, and one that reads `%s` names its instant directly: both take no `zone`. Every
  other format reads a local date and time and requires a `zone` to place it.

`disambiguation` decides the instant of a local date and time that `zone` skips or repeats:

| Disambiguation | Skipped local time | Repeated local time |
| --- | --- | --- |
| `reject` | Fails with an `invalid_argument` error | Fails with an `invalid_argument` error |
| `earlier` | The earlier of the two instants it could mean, read at the offset after the transition | The earlier instant |
| `later` | The later instant, read at the offset before the transition | The later instant |
| `compatible` | The later instant, as `date_add` moves local times | The earlier instant, as `date_add` moves local times |

A call without `disambiguation` rejects. In `America/New_York`, `2024-03-10 02:30:00` does not exist
and `2024-11-03 01:30:00` happens twice: `earlier` reads them as `2024-03-10T06:30:00Z` and
`2024-11-03T05:30:00Z`, and `later` as `2024-03-10T07:30:00Z` and `2024-11-03T06:30:00Z`.

A null `text` produces a null result. Every other failure fails only its message:

| Error | Message |
| --- | --- |
| `cast_failed` | `parse_datetime input does not match its format at byte 4: expected '-'`, naming the literal text or directive that did not match |
| `cast_failed` | `parse_datetime input continues past its format at byte 19` |
| `cast_failed` | `parse_datetime hour is out of range`, naming the field |
| `cast_failed` | `parse_datetime date does not exist` |
| `cast_failed` | `parse_datetime day of the week does not match the date` |
| `invalid_argument` | `parse_datetime local time does not exist in America/New_York` |
| `invalid_argument` | `parse_datetime local time is ambiguous in America/New_York` |
| `overflow` | `parse_datetime result is outside the DATETIME range` |

A message never quotes the text it could not read. It names byte positions, the format's own
literal text and directives, field names, and the zone.

## Array And Vector Functions

`[a, b, ...]` and `array(a, b, ...)` construct an `ARRAY` whose fixed width is the number of
arguments. `vec(a, b, ...)` constructs a `VEC`; `vec()` constructs an empty vector when it is the
whole value assigned to a declared `VEC` field, which supplies its element type. Constructor
elements must have exactly the same declared type, which may be any type, `BYTES`, `ARRAY`, and
`VEC` included, so a literal element beside a narrower field is cast: `[input.low, 0 AS I32]`. If
any element is null, the constructed container is null; schema elements themselves remain required.
A fixed `ARRAY` has at least one element, so `[]` and `array()` are rejected.

The functions below take `ARRAY` or `VEC` values described in
[Schemas And Codecs](schemas-and-codecs.md#internal-schemas), and a function of two lists accepts an
`ARRAY` and a `VEC`, or fixed arrays of different widths, together. The elements of a
multidimensional `ARRAY` are its outermost items. A null container produces a null result,
including for binary functions when either container is null.

| Function | Elements | Returns | Notes |
| --- | --- | --- | --- |
| `count(list)` | any | `I64` | Number of elements |
| `sum(list)` | numeric | optional element type | Sum of the elements. An empty list returns null |
| `first(list)` | numeric, `BOOL`, `STRING`, `DATETIME` | optional element type | The first element, or null for an empty list |
| `last(list)` | numeric, `BOOL`, `STRING`, `DATETIME` | optional element type | The last element, or null for an empty list |
| `nth(list, index)` | numeric, `BOOL`, `STRING`, `DATETIME` | optional element type | The element at `index`, counting from `0`, or null when `index` is negative or past the end. `index` may be any integer type |
| `contains(list, element)` | any scalar | `BOOL` | Whether `element`, of exactly the element type, occurs in the list. Empty lists return `false`; a null `element` returns null |
| `overlap(left, right)` | any scalar | `BOOL` | Whether two lists of the same exact element type share an element. An empty list returns `false` |
| `slice(list, start, length)` | any | `VEC<element>` | Selects up to `length` elements from the zero-based `start`; negative bounds act as zero and bounds past the end are clipped. Null bounds return null |
| `concat(list, ...)` | any | `ARRAY` or `VEC` | Concatenates lists with one exact element type. All fixed arrays produce a fixed array whose width is their sum; any vector input produces a vector |
| `min(list)`, `max(list)` | numeric, `BOOL`, `STRING`, `DATETIME` | optional element type | Least or greatest element. Empty lists return null |
| `mean(list)` | numeric | optional `F64` | Arithmetic mean. An empty list returns null; integer inputs round to `F64` for the calculation |
| `dot(left, right)` | numeric | element type | Dot product of two lists of one element type. Empty lists return zero. Integer multiplication and accumulation are checked at the element type |
| `distance(left, right)` | numeric | `F64` | Euclidean distance of two lists of one element type. Empty lists return zero; integer inputs round to `F64` for the calculation |

`nth` counts from `0`, unlike string positions: `nth(input.values, 0)` is the same element as
`first(input.values)`. A function whose elements the table narrows rejects any other element type
when the statement is validated, such as a list whose elements are themselves `ARRAY` or `VEC`
values, with `function 'first' requires ARRAY or VEC elements of a scalar type`. `first`, `last`,
`nth`, `min`, and `max` do not take `BYTES` elements.

`min` and `max` order floating-point elements by the IEEE 754 total order, in which `-0.0` is below
`0.0`, a NaN with the sign bit set is below every other value, and any other NaN is above every
other value. This differs from [`greatest` and `least`](#extrema) and from the window `MIN` and
`MAX` aggregates, which treat both zeros as equal and order every NaN above every other value.
Among equal elements, the first is returned.

`sum` follows the arithmetic operators: an integer sum that overflows its type reports `integer sum
overflowed` with the kind `overflow`, and a floating-point sum that is not finite reports
`floating-point sum produced a non-finite result` with the kind `invalid_argument`. `dot` checks its
products and their sum at the element type, so a dot product of two `U8` lists fails once it
reaches 256, with `integer dot product overflowed`. `dot` and `distance` require equal lengths in
every message; a mismatch reports `invalid_argument`, `vector lengths differ: left has 3, right has
2`, for that message. A non-finite `mean`, `dot`, or `distance` reports `invalid_argument`, such as
`mean produced a non-finite result`; `mean` sums its elements in `F64` before it divides, so a sum
beyond the `F64` range fails even where the mean itself would be finite. `contains` and `overlap`
use the scalar equality contract: NaN equals no element, and positive and negative zero are equal.
These functions compare exact element types; they never cast or stringify list elements.

In a [window route](#window-aggregates), `count`, `sum`, `first`, `last`, `min`, and `max` are always
window aggregates, like every other aggregate name. They take a per-row expression over `input` and
aggregate it across the rows the window retained, so `COUNT(input.values)` aggregates retained rows
rather than counting the elements of one list. A window route therefore cannot use these six list
functions, even inside an aggregate's argument, where `SUM(count(input.values))` is rejected as a
nested aggregate. Everywhere else these names are the list functions above.

## Window Aggregates

A [window processor](processors.md#window-processor) route computes its output from aggregates over
the rows its window retains when it emits. Aggregates appear directly in the route's `SET`, and
every route uses at least one. An aggregate reads per-row arguments from `input`, and `input` is
available only inside aggregate arguments, which read nothing else. Aggregate calls may take part in
larger scalar expressions, such as arithmetic, a conditional, a datetime function, or a `[...]` over
aggregates, combined with constants, but aggregates cannot be nested. Route `WHERE` reads the
finalized `output`. A statement that breaks these rules is rejected when it is applied, with a
message such as `input.latency is available only inside a window aggregate argument`, `aggregate
functions must not be nested inside aggregate arguments`, `CORR expects 2 argument(s), found 1`, or
`window output 'summary' must contain at least one aggregate function`.

Aggregate names are case-insensitive, and argument and result types are exact and checked when the
processor is applied. No aggregate argument may be `BYTES` or contain `BYTES`, which is rejected
with `... cannot retain BYTES in aggregate demand <n> argument <m>`.

| Function | Arguments | Returns | Typed null when |
| --- | --- | --- | --- |
| `COUNT(value)` | any | `I64` | never; counts every retained row |
| `COUNT_IF(condition)` | `BOOL` | `I64` | never; counts rows whose condition is true |
| `BOOL_AND(condition)` | `BOOL` | `BOOL` | no row has a condition |
| `BOOL_OR(condition)` | `BOOL` | `BOOL` | no row has a condition |
| `SUM(value)` | numeric | the argument's type | no row has a value |
| `AVG(value)` | numeric | `F64` | no row has a value |
| `MIN(value)`, `MAX(value)` | numeric, `BOOL`, `STRING`, or `DATETIME` | the argument's type | no row has a value |
| `FIRST(value)`, `LAST(value)` | any | the argument's type | no row has a value |
| `ARG_MIN(value, key)`, `ARG_MAX(value, key)` | any value; a numeric, `BOOL`, `STRING`, or `DATETIME` key | the value's type | no row has both a value and a key |
| `VAR_POP(value)`, `STDDEV_POP(value)` | numeric | `F64` | no row has a value |
| `VAR_SAMP(value)`, `STDDEV_SAMP(value)` | numeric | `F64` | fewer than two rows have a value |
| `COVAR_POP(first, second)` | numeric, numeric | `F64` | no row has both values |
| `COVAR_SAMP(first, second)` | numeric, numeric | `F64` | fewer than two rows have both values |
| `CORR(first, second)` | numeric, numeric | `F64` | fewer than two rows have both values, or either variable is constant across them |
| `PERCENTILE_LINEAR_HISTOGRAM(value, percentile, buckets, min, max, delay)` | numeric value, then constants | `F64` | the histogram counts no value |
| `APPROX_COUNT_DISTINCT(value, precision)` | numeric, `BOOL`, `STRING`, or `DATETIME`; constant precision 4–16 | `I64` | never; zero when no row contributes |
| `APPROX_QUANTILE(value, percentile, capacity)` | numeric; constant percentile 0–100 and capacity 32–4096 | `F64` | no row has a value |
| `APPROX_TOP_K(value, k, capacity)` | numeric, `BOOL`, `STRING`, or `DATETIME`; constants `1 <= k <= capacity <= 4096` | `VEC<value type>` | never; empty when no row contributes |

The [approximate sketches](#approximate-sketches) describe the last three.

**Nulls.** A row contributes to an aggregate only when every argument that aggregate reads is
present, so a null argument contributes nothing. `COUNT` is the exception: it counts every retained
row whatever its argument holds, so `SUM(input.amount) / COUNT(input.amount)` is not the mean of an
optional field; use `AVG`. A window emits only while it retains at least one row, so an aggregate
whose arguments are all required always has a value, except the sample statistics and `CORR`, which
can be undefined in any window. `COUNT`, `COUNT_IF`, `APPROX_COUNT_DISTINCT`, and `APPROX_TOP_K` are
never optional; `VAR_SAMP`, `STDDEV_SAMP`, `COVAR_SAMP`, and `CORR` are always optional; every other
aggregate is optional exactly when an argument it reads is. Assign an aggregate that can be null to
an `OPTIONAL` field or give it a value with `COALESCE`; assigning it to a required field is rejected
when the processor is applied.

**Order and ties.** `MIN`, `MAX`, `ARG_MIN`, and `ARG_MAX` return the earliest admitted row among
rows with equal keys. `FIRST` and `LAST` order rows by their ingestion low watermark, then by
admission. `BOOL` orders `false` before `true`, `STRING` orders by bytes, and floating-point keys
order NaN above every other value and treat both zeros as equal.

**Population and sample.** `VAR_POP`, `STDDEV_POP`, and `COVAR_POP` divide by the number of
contributing rows `n`; `VAR_SAMP`, `STDDEV_SAMP`, and `COVAR_SAMP` divide by `n - 1`. Standard
deviations are the square roots of the matching variances. `CORR` is the Pearson correlation, kept
within `[-1, 1]`.

**Numerical behavior.** `COUNT`, `COUNT_IF`, `BOOL_AND`, `BOOL_OR`, and `SUM` over integers are
exact: an integer `SUM` is accumulated without overflow and checked against its argument's type
only when the window emits. `SUM` over floating-point values carries the rounding error of every
addition beside the running total, and an `F32` `SUM` accumulates in `F64` and rounds to `F32` when
it emits. `AVG`, the variances, standard deviations, covariances, and `CORR` convert each argument
to the nearest `F64` and keep centered moments, so a variance is never the difference of two large
sums of squares. Stepping a window never subtracts a floating-point value from a statistic: the
statistic of the rows that remain is rebuilt from the rows themselves, so a value that left the
window, however large, leaves no rounding behind.

**Errors.** A row is refused at admission, and never changes the window, when an argument
expression fails for it, when a floating-point argument of `SUM`, `AVG`, a variance, standard
deviation, covariance, `CORR`, `PERCENTILE_LINEAR_HISTOGRAM`, or a sketch is NaN or infinite, or
when it would exceed the window's [`MAX STATE SIZE`](processors.md#window-processor) or pane limit.
A refused row is logged and not acknowledged, whatever the route's `ON MESSAGE ERROR` policy says;
a non-finite argument is reported by the first function, in alphabetical order, of the structure
that reads it, such as `AVG requires finite floating-point arguments`. At emission, a `SUM` that
does not fit its type, such as `SUM of the window does not fit Float32`, or a statistic that
overflows `F64` fails the window: every retained row's acknowledgement fails, the window is
cleared, and routes the window emitted before the failing one keep their output.

**Sensitivity.** An aggregate is sensitive when an argument reads a sensitive field, including
`COUNT`, `COUNT_IF`, `APPROX_COUNT_DISTINCT`, and `APPROX_TOP_K`, whose results do not contain the
field's values. Wrap the aggregate, or its argument, in `leak_sensitive(...)` to assign it to a
field that is not sensitive.

**Shared structures.** Aggregates of one route that can be answered from one structure over the same
arguments share it: `AVG`, the variances, and the standard deviations of one argument share a
`moments` structure; the covariances and `CORR` of one argument pair, in the same order, share
`co_moments`; `COUNT_IF`, `BOOL_AND`, and `BOOL_OR` share a `truth_counter`; `ARG_MIN` and `ARG_MAX`
share `arg_extremes`; `MIN` and `MAX` share `extremes`; `FIRST` and `LAST` share a `sequence`;
`APPROX_QUANTILE` calls with one argument and capacity share a `quantile_sketch`; and
`PERCENTILE_LINEAR_HISTOGRAM` calls with one argument, bucket count, bounds, and delay share a
`linear_histogram`. `DESCRIBE WINDOW PROCESSOR` lists every structure with the functions it serves
and the arguments it reads.

### Linear Histogram Percentiles

`PERCENTILE_LINEAR_HISTOGRAM(value, percentile, buckets, min, max, delay)` counts `value` in
`buckets` buckets of equal width from `min` to `max`, and returns the midpoint of the bucket that
holds the value of rank `round(percentile / 100 * (n - 1))` among the `n` counted values, clamped to
`[min, max]`. It does not interpolate within a bucket, so its precision is the bucket width,
`(max - min) / buckets`. A value at or below `min` counts in the first bucket and one at or above
`max` in the last, rather than being dropped.

`percentile` is an integer or float literal from `0` to `100`, `buckets` a positive integer literal,
and `min` and `max` numeric literals with `min` below `max`. A negative number is written with
unary minus, which is not a literal, so `min` and `max` are zero or above. `delay` is a duration
literal such as `'2s'`: a row that stepping removes from the window stays counted until the
watermark or the domain clock passes its removal time plus `delay`, and `'0s'` removes it at once.
An invalid delay is rejected with `invalid PERCENTILE_LINEAR_HISTOGRAM delay duration`. Each branch
holds eight bytes per bucket for each histogram, which `MAX STATE SIZE` does not count.

### Approximate Sketches

`APPROX_COUNT_DISTINCT`, `APPROX_QUANTILE`, and `APPROX_TOP_K` answer from bounded sketches instead
of the retained values. A route that uses one needs its window processor to declare `MAX STATE SIZE`,
duration `WIDTH` and `STEP`, and, when it is branched, a branch with `MAX INSTANCES ... EVICT LRU`,
as [Window processor](processors.md#window-processor) describes. The sketches ignore nulls and
refuse non-finite floating-point values as described under **Errors** above.

| Sketch | Algorithm | Accuracy | Reserved per pane |
| --- | --- | --- | --- |
| `APPROX_COUNT_DISTINCT(value, precision)` | HyperLogLog with `2^precision` registers over a stable, type-tagged BLAKE3 hash of each value | Relative standard error of about `1.04 / sqrt(2^precision)`: 26% at precision 4, 3.25% at 10, and 0.41% at 16 | `2^precision + 128` bytes |
| `APPROX_QUANTILE(value, percentile, capacity)` | t-digest with at most `capacity` centroids, smaller near the tails | Exact, interpolating linearly at rank `percentile / 100 * (n - 1)`, while at most `capacity` values contribute; beyond that, an interpolated estimate whose accuracy depends on the distribution and capacity, with no distribution-independent bound | `32 * capacity + 128` bytes |
| `APPROX_TOP_K(value, k, capacity)` | Misra-Gries frequency candidates, at most `capacity` keys | Every value that occurs more than `n / (capacity + 1)` times among the `n` contributing rows remains a candidate; values near the frequency cutoff may differ from an exact top-k | `160 * capacity + 128` bytes |

`APPROX_TOP_K` returns up to `k` values ordered by estimated frequency, then by the bytes of their
type-tagged key to break ties, so ties among numbers do not follow numeric order. Each value it
returns is the one the earliest retained row with that key holds. Its counts are internal.
`APPROX_COUNT_DISTINCT` hashes `-0.0` and `0.0` as one value and a `STRING` by its UTF-8 bytes.

**Memory.** A sketch window keeps one sketch for each pane, a span of time whose length is the
greatest common divisor of `WIDTH` and `STEP`, aligned to the Unix epoch, and merges the panes still
in the window when it emits. A window with duration `WIDTH` `w` and pane length `p` spans at most
`w / p + 2` panes. For each route, the processor is rejected when it is applied unless
`1024 + (the route's reservations summed) * (w / p + 3)` bytes fit its `MAX STATE SIZE`, with
`window processor '<name>' ... requires <bytes> bytes for <panes> sketch panes, above MAX STATE SIZE
<limit> bytes`. At run time each branch charges the reservation of every sketch in every active pane,
and of the merged sketch it builds at emission, beside the retained rows and their argument columns,
and a row that would exceed `MAX STATE SIZE` is refused. The configured worst case across branches
is therefore bounded by `MAX STATE SIZE * MAX INSTANCES`; an unbranched window has one state.

**Expiry.** Panes are merged only from rows still in the active window and rebuilt after stepping,
so a row that left the window never contributes to a later result. Mergeability does not make a
sketch retractable: nothing is subtracted from one. When a branch's ownership moves or a node
recovers, the sketches are rebuilt from the retained input columns that the branch's snapshots
carry.

**Determinism.** A sketch is deterministic for the same ordered inputs, pane layout, and
configuration, so every node computes the same estimate from the same rows.

**Measured accuracy.** In the VM Functions benchmark report, `APPROX_COUNT_DISTINCT` over full
windows of distinct keys measured a mean absolute relative error of 1.74% for 500 keys and 1.90% for
100 keys at precision 10, whose standard error is 3.25%, and 0.26% for 500 keys at precision 16,
whose standard error is 0.41%, over 31 windows each. The quantile and top-k sketches have no
measured accuracy beyond the exactness above.

## Limits

Every limit an expression can reach is fixed by Nervix rather than configured, except the window
state size a window processor declares:

| Limit | Bound | Reached by |
| --- | --- | --- |
| Text or octets one `STRING` or `BYTES` column holds | 2,147,483,647 bytes for the results of one call in one batch | The functions of [Result Size](#result-size), which fail each message that does not fit with `overflow`, and [`format_datetime`](#writing-values), which fails the batch |
| Parts of one `split` result | 65,536 | `split`, with `overflow` |
| LIKE pattern | 4,096 bytes | `like` and `ilike`, with `invalid_argument` |
| `contains_any` set | 128 non-null patterns and 65,536 bytes of pattern text; 64 distinct sets prepared per batch | `contains_any`, with `invalid_argument` |
| Compiled regular expression | 10 MiB; 64 compiled patterns cached per call | Regular-expression functions, with `invalid_argument` |
| JSON document | 16,777,216 bytes and 128 levels of nesting | `JSON_VALUE` and `JSON_EXISTS`, with `invalid_argument`; `TRY_JSON_VALUE` yields null |
| JSON path | 64 steps; array index up to 4,294,967,295 | Rejected when the statement is parsed |
| `DATETIME` | `1677-09-21T00:12:43.145224192Z` through `2262-04-11T23:47:16.854775807Z`, in nanoseconds | Datetime functions and conversions, with `overflow` |
| Datetime format | 256 bytes per value, counting every directive at its longest | Rejected when the statement is applied |
| `date_bin` width | 9,223,372,036,854,775,807 nanoseconds, about 292 years | Rejected when the statement is applied |
| Window sketches | Precision 4–16; capacity 32–4096; `k` at most the capacity | Rejected when the processor is applied |
| Window sketch state | `MAX STATE SIZE` per branch, and at most `MAX INSTANCES` branches | Rows refused at admission; see [Approximate Sketches](#approximate-sketches) |

The one bound on how long a builtin runs is the size of its input: every builtin finishes the batch
it is evaluating, bounded by the limits above, so stopping a node or a processor takes effect
between batches. A UDF has its own watchdog; see [User-Defined Functions](udfs.md).

## Measured Performance

Execution is columnar, so a batch pays a fixed cost once and each message pays for its own work.
Two measured facts, from the [VM batch-size sweep](nspl-overview.md) and from the VM function
workloads below, shape what an expression costs:

- Per-message cost falls steeply up to about a thousand messages per batch and then flattens.
  Batches above 1,024 messages execute on the blocking worker pool, whose hand-off costs more than
  a small batch does to execute.
- An expression costs what its functions do for the messages that evaluate them. A conditional arm
  runs only for the messages that select it, so its cost follows the share of messages it selects.

The table shows the median time to execute a complete compiled program over one batch, measured
with the VM function workloads on one 32-thread x86-64 development machine in September 2026, with
other builds running on the same machine. The absolute rates are specific to that machine and are
not guarantees; the ratios between rows are what carries over.

| Workload, 1,024 messages unless noted | Median per batch | Messages per second |
| --- | --- | --- |
| Checked integer arithmetic, no failures | 8.09 µs | 127 million |
| Checked integer arithmetic, sparse failures | 15.48 µs | 66 million |
| Checked integer arithmetic, dense failures | 51.07 µs | 20 million |
| Arithmetic, 1 message | 3.44 µs | 0.29 million |
| Arithmetic, 8 messages | 3.38 µs | 2.4 million |
| Arithmetic, 1,025 messages, on the blocking pool | 25.17 µs | 41 million |
| Arithmetic over a sliced batch | 10.39 µs | 99 million |
| List function over ragged `VEC` values | 384.74 µs | 2.7 million |
| `contains_any` with a per-message set, 32-byte ASCII text | 117.76 µs | 8.7 million |
| `contains_any` with a per-message set, 1,024-byte UTF-8 text | 143.61 µs | 7.1 million |
| `repeat` producing 1, 8, and 64 copies | 19.59, 25.76, and 40.18 µs | 52, 40, and 25 million |
| Four `JSON_VALUE` extractions from one document field | 548.58 µs | 1.9 million |
| Four `JSON_VALUE` extractions from four document fields | 1,861.2 µs | 0.55 million |
| Regular expression in a conditional arm that selects no, 1%, and 50% of messages | 2.45, 5.25, and 30.05 µs | 417, 195, and 34 million |

What the measurements show:

- A message that fails costs several times what a successful one does: dense failures made checked
  arithmetic six times slower. Guard expected failures with a conditional, `TRY_CAST`, or
  `TRY_JSON_VALUE` rather than relying on the error path.
- Extractions from one document share its parse, so four fields read from one document cost less
  than a third of four fields read from four documents.
- Variable-length work such as ragged lists, text search, and JSON runs at millions of messages per
  second rather than the hundred million of fixed-width arithmetic.

The workloads, commands, allocation observations, and hardware are recorded in the
[VM functions measurement report](https://github.com/nervix-io/nervix/blob/main/benches/reports/vm-functions-18.md).

## Catalog

Every operator, builtin, and aggregate, with the section that describes its types, nulls, and
errors. `T` is the argument's type, and a list is an `ARRAY` or a `VEC`.

| Name | Returns | Section |
| --- | --- | --- |
| `+`, `-`, `*`, `/`, `%`, unary `-` | numeric `T` | [Arithmetic](#arithmetic) |
| `AND`, `OR`, `NOT` | `BOOL` | [Logical Operators](#logical-operators) |
| `=`, `!=`, `<`, `<=`, `>`, `>=` | `BOOL` | [Comparison And Equality](#comparison-and-equality) |
| `IS [NOT] DISTINCT FROM` | `BOOL`, never null | [Comparison And Equality](#comparison-and-equality) |
| `[NOT] IN (...)` | `BOOL` | [Sets](#sets) |
| `[NOT] BETWEEN ... AND ...` | `BOOL` | [Ranges](#ranges) |
| `IF ... END`, `CASE ... END` | the result type | [Conditional Expressions](#conditional-expressions) |
| `<expr> AS <type>`, `TRY_CAST(<expr> AS <type>)` | `<type>` | [Conversions](#conversions) |
| `JSON_VALUE`, `TRY_JSON_VALUE` | optional declared type | [JSON Documents](#json-documents) |
| `JSON_EXISTS` | `BOOL` | [JSON Documents](#json-documents) |
| `coalesce`, `nullif` | `T` | [Null Handling](#null-handling) |
| `is_null` | `BOOL`, never null | [Null Handling](#null-handling) |
| `greatest`, `least`, `clamp` | `T` | [Extrema](#extrema) |
| `leak_sensitive` | `T` | [Context And Identity](#context-and-identity) |
| `now` | `DATETIME` | [Context And Identity](#context-and-identity) |
| `uuid_v4`, `uuid_v7` | `STRING` | [Context And Identity](#context-and-identity) |
| `read_header` | optional `STRING` | [Header Functions](#header-functions) |
| `read_headers` | `VEC<STRING>` | [Header Functions](#header-functions) |
| `write_header` | nothing; `INVOKE` only | [Header Functions](#header-functions) |
| `LOOKUP_HASH_MAP` | optional field type | [Lookups And UDF Calls](#lookups-and-udf-calls) |
| `udf::<name>` | the UDF's result type | [Lookups And UDF Calls](#lookups-and-udf-calls) |
| `lower`, `upper`, `initcap`, `trim`, `btrim`, `ltrim`, `rtrim`, `reverse`, `normalize_nfc` | `STRING` | [String Functions](#string-functions) |
| `left`, `right`, `substr`, `substring`, `repeat`, `lpad`, `rpad`, `replace`, `translate`, `split_part` | `STRING` | [String Functions](#string-functions) |
| `concat` over text, `concat_ws`, `join` | `STRING` | [String Functions](#string-functions) |
| `split` | `VEC<STRING>` | [String Functions](#string-functions) |
| `length`, `char_length`, `bit_length`, `octet_length`, `ascii`, `strpos` | `I64` | [String Functions](#string-functions) |
| `to_hex`, `md5` | `STRING` | [String Functions](#string-functions) |
| `contains` over text, `starts_with`, `ends_with`, `contains_any`, `like`, `ilike` | `BOOL` | [String Predicates](#string-predicates) |
| `regexp_like` | `BOOL` | [Regular Expressions](#regular-expressions) |
| `regexp_replace` | `STRING` | [Regular Expressions](#regular-expressions) |
| `regexp_substr`, `regexp_extract` | optional `STRING` | [Regular Expressions](#regular-expressions) |
| `bytes_from_utf8`, `base64_decode`, `hex_decode`, `sha256` | `BYTES` | [Bytes, Encodings And Hashes](#bytes-encodings-and-hashes) |
| `bytes_to_utf8`, `base64_encode`, `hex_encode` | `STRING` | [Bytes, Encodings And Hashes](#bytes-encodings-and-hashes) |
| `xxh3_64` | `U64` | [Bytes, Encodings And Hashes](#bytes-encodings-and-hashes) |
| `ip_from_string`, `ip_trunc`, `ip_unmap` | `BYTES` | [IP Addresses And Networks](#ip-addresses-and-networks) |
| `ip_to_string` | `STRING` | [IP Addresses And Networks](#ip-addresses-and-networks) |
| `ip_family` | `I64` | [IP Addresses And Networks](#ip-addresses-and-networks) |
| `ip_in_network`, `is_ip_address` | `BOOL` | [IP Addresses And Networks](#ip-addresses-and-networks) |
| `url_scheme`, `url_path`, `url_decode` | `STRING` | [URLs](#urls) |
| `url_host`, `url_query`, `url_fragment`, `url_query_value` | optional `STRING` | [URLs](#urls) |
| `url_port` | optional `I64` | [URLs](#urls) |
| `url_query_values` | `VEC<STRING>` | [URLs](#urls) |
| `is_url` | `BOOL` | [URLs](#urls) |
| `abs`, `ceil`, `ceiling`, `floor`, `round`, `trunc`, `sign` | numeric `T` | [Numeric Functions](#numeric-functions) |
| `acos`, `asin`, `atan`, `atan2`, `cos`, `sin`, `tan`, `degrees`, `radians`, `exp`, `ln`, `log`, `log2`, `pow`, `power`, `sqrt` | `F64` | [Numeric Functions](#numeric-functions) |
| `round(x, digits)` | numeric `T` | [Precision Rounding](#precision-rounding) |
| `is_nan`, `is_finite`, `is_infinite` | `BOOL` | [Floating-Point Classification](#floating-point-classification) |
| `bitwise_and`, `bitwise_or`, `bitwise_xor`, `bitwise_not`, `shift_left`, `shift_right` | integer `T` | [Bitwise Functions](#bitwise-functions) |
| `bit_count` | `I64` | [Bitwise Functions](#bitwise-functions) |
| `date_part`, `date_diff`, `to_unix` | `I64` | [Datetime Functions](#datetime-functions) |
| `date_trunc`, `date_bin`, `date_add`, `from_unix`, `parse_datetime` | `DATETIME` | [Datetime Functions](#datetime-functions) |
| `format_datetime` | `STRING` | [Datetime Formats](#datetime-formats) |
| `[...]`, `array` | `ARRAY` | [Array And Vector Functions](#array-and-vector-functions) |
| `vec`, `slice` | `VEC` | [Array And Vector Functions](#array-and-vector-functions) |
| `concat` over lists | `ARRAY` or `VEC` | [Array And Vector Functions](#array-and-vector-functions) |
| `count` over a list | `I64` | [Array And Vector Functions](#array-and-vector-functions) |
| `sum`, `first`, `last`, `nth`, `min`, `max` over a list | optional element type | [Array And Vector Functions](#array-and-vector-functions) |
| `contains` over a list, `overlap` | `BOOL` | [Array And Vector Functions](#array-and-vector-functions) |
| `mean` | optional `F64` | [Array And Vector Functions](#array-and-vector-functions) |
| `dot` | element type | [Array And Vector Functions](#array-and-vector-functions) |
| `distance` | `F64` | [Array And Vector Functions](#array-and-vector-functions) |
| `COUNT`, `COUNT_IF`, `APPROX_COUNT_DISTINCT` | `I64` | [Window Aggregates](#window-aggregates) |
| `BOOL_AND`, `BOOL_OR` | `BOOL` | [Window Aggregates](#window-aggregates) |
| `SUM`, `MIN`, `MAX`, `FIRST`, `LAST` | the argument's type | [Window Aggregates](#window-aggregates) |
| `ARG_MIN`, `ARG_MAX` | the value's type | [Window Aggregates](#window-aggregates) |
| `AVG`, `VAR_POP`, `VAR_SAMP`, `STDDEV_POP`, `STDDEV_SAMP`, `COVAR_POP`, `COVAR_SAMP`, `CORR` | `F64` | [Window Aggregates](#window-aggregates) |
| `PERCENTILE_LINEAR_HISTOGRAM` | `F64` | [Linear Histogram Percentiles](#linear-histogram-percentiles) |
| `APPROX_QUANTILE` | `F64` | [Approximate Sketches](#approximate-sketches) |
| `APPROX_TOP_K` | `VEC` of the value's type | [Approximate Sketches](#approximate-sketches) |

## Example

This graph runs without any external system: a node's own HTTP endpoint receives access records,
a junction describes each one with the address, URL, JSON, datetime, conditional, identity, and hash
functions, and a window processor summarizes latency with exact and approximate aggregates. Create
the domain and select it:

```nspl
CREATE UNPACED DOMAIN functions_example;

USE functions_example;
```

Then declare the graph:

```nspl
BEGIN;

CREATE SCHEMA access_event (
  client STRING,
  url STRING,
  payload STRING,
  logged STRING,
  latency_ms I64
);

CREATE WIRE JSON SCHEMA access_wire MODE STRICT (
  client string,
  url string,
  payload string,
  logged string,
  latency_ms integer
);

CREATE CODEC access_codec
  FROM WIRE JSON SCHEMA access_wire
  TO SCHEMA access_event;

CREATE SCHEMA access_summary (
  client_address BYTES,
  client_subnet STRING,
  internal BOOL,
  host STRING OPTIONAL,
  campaign STRING OPTIONAL,
  customer_id I64 OPTIONAL,
  tags VEC<STRING> OPTIONAL,
  logged_at DATETIME,
  local_hour I64,
  latency_band STRING,
  event_id STRING,
  url_digest STRING
);

CREATE SCHEMA latency_window (
  requests I64,
  mean_latency F64,
  p90_latency F64,
  distinct_clients I64
);

CREATE RELAY access_events SCHEMA access_event UNBRANCHED;

CREATE RELAY access_summaries SCHEMA access_summary UNBRANCHED;

CREATE RELAY latency_windows SCHEMA latency_window UNBRANCHED;

CREATE VHOST functions_edge access.example.com;

CREATE ENDPOINT access_ingress
  ON functions_edge
  PATH '/access'
  TYPE HTTP;

CREATE INGESTOR access_intake
  FROM ENDPOINT access_ingress MODE NO_ACK SEQUENTIAL
  ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING access_codec
  TO access_events
    INHERIT ALL
    UNBRANCHED
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;

CREATE JUNCTION describe_access
  FROM access_events
  UNBRANCHED
  TO access_summaries
    SET client_address = ip_from_string(input.client),
        client_subnet = ip_to_string(ip_trunc(output.client_address, IF ip_family(output.client_address) = 4 THEN 24 ELSE 48 END)),
        internal = ip_in_network(ip_unmap(output.client_address), '10.0.0.0/8'),
        host = url_host(input.url),
        campaign = url_query_value(input.url, 'utm_campaign'),
        customer_id = TRY_JSON_VALUE(input.payload, '$.customer.id' AS I64),
        tags = JSON_VALUE(input.payload, '$.tags' AS VEC<STRING>),
        logged_at = parse_datetime('%d/%b/%Y:%H:%M:%S %z', input.logged),
        local_hour = date_part('hour', output.logged_at, 'Europe/Berlin'),
        latency_band = CASE
          WHEN input.latency_ms < 100 THEN 'fast'
          WHEN input.latency_ms < 1000 THEN 'slow'
          ELSE 'stalled'
        END,
        event_id = uuid_v7(),
        url_digest = hex_encode(sha256(bytes_from_utf8(lower(trim(input.url)))))
    WHERE NOT output.internal OR output.latency_band != 'fast'
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG;

CREATE WINDOW PROCESSOR access_latency
  FROM access_events
  WIDTH 10s DURATION
  STEP 10s DURATION
  MAX STATE SIZE 1MiB
  UNBRANCHED
  TO latency_windows
    SET requests = COUNT(input.client),
        mean_latency = AVG(input.latency_ms),
        p90_latency = APPROX_QUANTILE(input.latency_ms, 90, 64),
        distinct_clients = APPROX_COUNT_DISTINCT(input.client, 12)
    ON MESSAGE ERROR LOG;

COMMIT;
```

`describe_access` parses the client address once into the `BYTES` field `client_address` and
reads it back through `output.client_address`, so `client_subnet` and `internal` cost no second
parse. `TRY_JSON_VALUE` yields a typed null for a customer id that is not an integer, and every
extraction shares one parse of each payload. `local_hour` reads the local hour in Berlin from the
`logged_at` value an earlier assignment computed. The route drops fast internal requests.

Start the domain, subscribe to both outputs, each in its own terminal, and post three records:

```nspl
START;
```

```bash
nervix-cli --domain functions_example subscribe access_watch access_summaries
nervix-cli --domain functions_example subscribe latency_watch latency_windows

curl -X POST http://127.0.0.1:8080/access -H 'Host: access.example.com' \
  -H 'Content-Type: application/json' \
  -d '{"client":"203.0.113.9","url":"https://Shop.Example.com/landing?utm_campaign=spring&tag=a","payload":"{\"customer\":{\"id\":42},\"tags\":[\"new\",\"vip\"]}","logged":"25/Sep/2026:14:03:07 +0000","latency_ms":85}'
curl -X POST http://127.0.0.1:8080/access -H 'Host: access.example.com' \
  -H 'Content-Type: application/json' \
  -d '{"client":"10.1.2.3","url":"https://shop.example.com/cart","payload":"{\"customer\":{\"id\":\"c-7\"},\"tags\":[]}","logged":"25/Sep/2026:14:03:08 +0000","latency_ms":1450}'
curl -X POST http://127.0.0.1:8080/access -H 'Host: access.example.com' \
  -H 'Content-Type: application/json' \
  -d '{"client":"10.1.2.4","url":"https://shop.example.com/","payload":"{\"tags\":[\"x\"]}","logged":"25/Sep/2026:14:03:09 +0000","latency_ms":40}'
```

`access_summaries` receives the first two records, and the third, a fast internal request, is
filtered out. The subscription renders `BYTES` as base64 and omits a null optional field, and
`event_id` differs on every run:

```json
{"campaign":"spring","client_address":"ywBxCQ==","client_subnet":"203.0.113.0","customer_id":42,"event_id":"01a0d977-422d-7530-a7df-1f7cc335ffd8","host":"shop.example.com","internal":false,"latency_band":"fast","local_hour":16,"logged_at":"2026-09-25T14:03:07+00:00","tags":["new","vip"],"url_digest":"e3573155b66ec8947e80bc6a6cf1d8dc62108261d9d8c0b64906259bd7f47302"}
{"client_address":"CgECAw==","client_subnet":"10.1.2.0","event_id":"01a0d977-423f-7d17-820b-d2be58c6d798","host":"shop.example.com","internal":true,"latency_band":"stalled","local_hour":16,"logged_at":"2026-09-25T14:03:08+00:00","tags":[],"url_digest":"aebca4f73e8e9c15a3679803b396f37c61a468bf9e24a60a95fe1d6d1078131c"}
```

When the ten-second window closes, `latency_windows` receives one summary of all three requests.
Three latencies are within the quantile sketch's capacity of 64, so `p90_latency` is exact: rank
`0.9 * 2` interpolates between `85` and `1450`.

```json
{"distinct_clients":3,"mean_latency":525.0,"p90_latency":1177.0,"requests":3}
```
