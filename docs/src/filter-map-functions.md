# Expression Functions

These functions are available in structured NSPL expressions on ingestors, processors, routes,
and emitters:

```nspl,ignore
[INHERIT ...]
[SET <field> = <expr>, ...]
[WHERE <expr>]
[INVOKE write_header(<name-expr>, <value-expr>), ...]
```

`SET` assignments and `INVOKE` calls execute left to right. A transforming route begins empty and
may initialize fields with `INHERIT` and `SET`; a set-only route begins empty and supports only
`SET`. Route `WHERE` runs after output finalization. `INVOKE` and `write_header` are emitter-only;
side-effect functions are invalid inside ordinary expressions.

See [The Working Message](working-message.md) for transforming-route field scopes and resolution.

`ON MESSAGE ERROR SEND TO ... SET` uses this same scalar expression engine with its error-specific
scopes. Aggregates are unavailable there. Header reads remain available when the failed operation
belongs to a header-capable ingestor and the original source envelope was captured.

General rules:

- function names are case-insensitive
- there is no implicit cast insertion; `expr AS TYPE` and `TRY_CAST(expr AS TYPE)` convert
  explicitly, as [Conversions](#conversions) describes
- argument and result types are validated when the statement is applied
- sensitive values retain their sensitivity through expression evaluation; internal relay and node outputs may assign them to a non-sensitive field only with an explicit `leak_sensitive(...)`
- every sensitive value crossing an emitter boundary requires `leak_sensitive(...)` or explicit
  `INHERIT <field> LEAK SENSITIVE`
- `NOW()` returns the execution-local domain time as `DATETIME`
- `UUID_V7()` uses that same execution-local domain time when building the UUID

Domain-owned [Roto UDFs](./udfs.md) use the same call syntax and exact typing rules. Builtins take
the unqualified call surface. UDFs use the explicit `udf::` namespace.

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

## Evaluation Model

Builtins are evaluated over Arrow columns: one call computes its result for every message in a
batch together. A message's result never depends on the other messages in the batch, so batching
does not change any value. The one limit a batch's messages share is the bytes one `STRING` or
`BYTES` column holds, which `repeat`, `lpad`, `rpad`, and every function whose result can be longer
than its arguments check before they build a result; see [String Functions](#string-functions). How
a function traverses its column, whether through an Arrow compute kernel, one pass over the column's
value buffer, or a loop over its rows, is internal to the function and does not change its results.

A pass over a value buffer is written so the compiler can turn it into the vector instructions of
the CPU a Nervix binary is built for. Builtins contain no hand-written SIMD code, and the vector
instructions a node's CPU offers never change a builtin's result.

The compiler applies three optimizations that preserve results in the same way:

- A deterministic call that cannot fail and whose arguments are all literals may be computed once
  instead of for every batch. It produces exactly the value that evaluating it for each message
  would, so `upper('grüßen')` and `upper(input.text)` agree when `input.text` holds `grüßen`.
- Identical deterministic expressions that cannot fail may be computed once per batch and shared.
  Calls that return a new value for every message, such as `uuid_v4()`, and calls that can report
  a per-message error are evaluated at each occurrence, so each occurrence reports its own error.
- A literal, and any expression whose arguments are all literals or `now()`, is carried through a
  batch as one value rather than as a column of copies. A function reads it as one value where it
  can and expands it to a column only where a message-by-message operation or an output field
  needs one, so the result is the same either way. When such an expression fails, such as
  `1 / 0`, every message that evaluates it reports the error.

## Function Properties

Every builtin follows these rules unless its own description says otherwise:

| Property | Contract |
| --- | --- |
| Types | Arguments are never converted implicitly. A function that accepts several types, such as `abs` over every numeric type, takes each of them as it is. |
| Nulls | A null argument produces a null result. `coalesce`, `nullif`, `concat`, `is_null`, `greatest`, and `least` define their own null handling, and `url_host`, `url_port`, `url_query`, `url_fragment`, and `url_query_value` are also null for a URL that lacks what they read. |
| Sensitivity | A result is sensitive when any argument is sensitive, including results such as `length(...)`, `is_null(...)`, and `count(...)` that do not contain the argument's value. Only `leak_sensitive(...)` removes sensitivity. |
| Volatility | Every builtin is deterministic except `now()`, which returns one value for an execution, and `uuid_v4()` and `uuid_v7()`, which return a new value for every message. |
| Errors | A function that can fail reports a per-message error and yields null for that message. The error activates `ON MESSAGE ERROR`. Inside a conditional, only the selected arm can report one. |

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
equals a simple-`CASE` match value. All non-null results must have the same exact type; implicit
casts are not inserted. An omitted `ELSE` is a typed null and therefore requires an optional
destination. An `IF` always includes `ELSE`.

Conditional values are computed in the columnar batch engine, and every arm is evaluated only for
the messages that select it: a condition is evaluated for the messages no earlier arm answered, and
a result for the messages its condition selected. A function in an arm therefore never reports an
error for a message that selects another arm, so an error in an unselected arm never activates
`ON MESSAGE ERROR`, and a `CASE` guard shields a function from the messages it cannot handle.
Context-injected operations such as window aggregates, header reads, and UDFs are invoked for the
selected messages only, and not at all in a batch where no message selects their arm; a whole-batch
failure of such an invocation still fails the batch it was invoked for.

The words `IF`, `CASE`, `WHEN`, `THEN`, `ELSE`, and `END` are reserved in expressions, including
after a field scope such as `input.<field>`. A schema may declare one of these field names, but an
NSPL expression cannot reference it.

## Comparison And Equality

`=`, `!=`, `<`, `<=`, `>`, and `>=` require both operands to have the same exact type. The ordering
comparisons `<`, `<=`, `>`, and `>=` accept numeric, `STRING`, and `DATETIME` operands. A null
operand makes the comparison null.

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
where `input.region != input.home_region` is null. Both operands must have the same exact type,
which may be any numeric type, `BOOL`, `STRING`, or `DATETIME`. Present floats compare as `=` does,
so NaN is distinct from every value, another NaN included, and `0.0` is not distinct from `-0.0`.

`IN`, `BETWEEN`, and `IS [NOT] DISTINCT FROM` bind like the comparison operators and group from the
left with them, so `a = b IN (TRUE)` tests whether `a = b` is in the set. Unary `NOT` binds more
tightly than any comparison, so `NOT x IN (1, 2)` negates `x` before testing it; write
`x NOT IN (1, 2)` or `NOT (x IN (1, 2))` instead. The words `IN`, `BETWEEN`, `IS`, `DISTINCT`, and
`FROM` are reserved in expressions, including after a field scope such as `input.<field>`, like the
[conditional keywords](#conditional-expressions).

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

## Extrema

| Function | Returns | Notes |
| --- | --- | --- |
| `greatest(a, ...)` | same type as inputs | The largest present argument, or a typed null when every argument is null |
| `least(a, ...)` | same type as inputs | The smallest present argument, or a typed null when every argument is null |
| `clamp(value, low, high)` | same type as inputs | `low` where `value < low`, `high` where `value > high`, and `value` otherwise |

`greatest` and `least` take one or more arguments of one exact type: any numeric type, `BOOL`,
`STRING`, or `DATETIME`. They skip null arguments, so their result is required when any argument
is. They order values the way the window `MIN` and `MAX` aggregates do: `BOOL` orders `false` before
`true`, `STRING` orders by Unicode code point, and floating-point values order NaN above every other
value and treat both zeros as equal. Among equal values the earliest argument is returned, so
`greatest(-0.0, 0.0)` is `-0.0` and `greatest(0.0, -0.0)` is `0.0`.

`clamp` takes three arguments of one exact type: any numeric type, `STRING`, or `DATETIME`. It
compares exactly as `<` and `>` do, so it returns a NaN value unchanged and keeps `-0.0` inside a
range that starts at `0.0`. A null argument produces a null result. A message whose low bound is
above its high bound, or whose floating-point bound is NaN, reports an `invalid_argument` error
naming the invalid bounds and yields null, so give a route whose bounds can cross an
`ON MESSAGE ERROR` policy. A message with a null argument is never failed.

## Arithmetic

`+`, `-`, `*`, `/`, and `%` require both operands to have the same exact numeric type, and the result
has that type. Unary `-` applies to signed integer and floating-point operands. A null operand makes
the result null.

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

The error's message names the failure, such as `integer addition overflowed`, `integer remainder by
zero`, `integer left shift by a negative count`, or `floating-point operation produced a non-finite
result`.

## Conversions

A value never changes its type implicitly. Two explicit forms convert it to another scalar type.
They accept the same types and convert every value the same way, and differ only in what a value
that does not convert does:

| Form | Result | A value that does not convert |
| --- | --- | --- |
| `<expr> AS <type>` | `<type>`, optional exactly when `<expr>` is | Reports a per-message `cast_failed` error, such as `cannot cast value to Int64`, and yields null for that message, which activates `ON MESSAGE ERROR` |
| `TRY_CAST(<expr> AS <type>)` | Optional `<type>` | Yields a typed null, and the message continues without an error |

`<type>` is a scalar type written with the same spellings in both forms, such as `I64` or `INT64`,
`F64` or `FLOAT64`, `BOOL`, `STRING`, and `DATETIME`; see [NSPL Overview](nspl-overview.md) for
every spelling. A null operand is not a failure: it converts to a typed null in both forms, so
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
`input.low + input.high AS STRING` the `AS` applies to `input.high` alone. `TRY_CAST` is reserved
in expressions in the same way as the [conditional keywords](#conditional-expressions).

A conversion between scalar types succeeds for every value, except for the values the table
below lists:

- Every value converts to `STRING`. A float is written in decimal without an exponent, and a
  `DATETIME` in RFC 3339 with a `+00:00` offset, such as `2024-02-29T12:00:00+00:00`.
- A number converts to `BOOL` as `false` for zero and `true` for every other value, including NaN,
  and a `BOOL` converts to a number as `0` or `1`.
- A number converts to `F32` or `F64` as the nearest value of that type, so an `F64` beyond the
  `F32` range becomes an infinity.
- A `DATETIME` converts to `I64` as its count of nanoseconds since `1970-01-01T00:00:00Z`, and to
  `F32` or `F64` as the nearest value to that count. An integer converts to the `DATETIME` that many
  nanoseconds after that instant.

| From | To | Fails for |
| --- | --- | --- |
| An integer type | Another integer type | A value outside the target type's range |
| `U64` | `DATETIME` | A value above the largest `I64` |
| `F32`, `F64` | An integer type | NaN, an infinity, and a value whose integer part, which it is rounded to toward zero, is outside the target type's range |
| `F32`, `F64` | `DATETIME` | NaN, an infinity, and a value outside the `DATETIME` range when read as nanoseconds since `1970-01-01T00:00:00Z` |
| `STRING` | An integer type | Text other than an optional `+` or `-` followed by decimal digits, and a number outside the target type's range |
| `STRING` | `F32`, `F64` | Text other than a decimal number with an optional sign, fraction, and exponent, or `NaN`, `inf`, or `infinity` in any letter case. A number beyond the type's range reads as an infinity |
| `STRING` | `BOOL` | Text other than `t`, `tr`, `tru`, `true`, `y`, `ye`, `yes`, `on`, or `1`, which read as `true`, and `f`, `fa`, `fal`, `fals`, `false`, `n`, `no`, `of`, `off`, or `0`, which read as `false`, in any letter case and with any surrounding whitespace |
| `STRING` | `DATETIME` | Text that is not an RFC 3339 date and time with a UTC offset, such as `2024-02-29T12:00:00Z`, and an instant outside the `DATETIME` range |
| `DATETIME` | An integer type other than `I64` | A count of nanoseconds since `1970-01-01T00:00:00Z` outside the target type's range |
| `BOOL` | `DATETIME` | Every value |
| `DATETIME` | `BOOL` | Every value |

## Header Functions

| Function | Returns | Notes |
| --- | --- | --- |
| `read_header(name)` | optional `STRING` | Ingestor-only. Returns the first value, or `NULL` when absent |
| `read_headers(name)` | `VEC<STRING>` | Ingestor-only. Returns all values in order, or an empty vector when absent |
| `write_header(name, value)` | nothing | Emitter-only side effect. Valid only as a top-level call in the final `INVOKE` block |

Header names may be dynamic `STRING` expressions. Header reads are available only on Endpoint (HTTP and WebSocket), HTTP client, Kafka, NATS, Pulsar, RabbitMQ, and SQS ingestors. Header writes are available only on Kafka, NATS, Pulsar, RabbitMQ, and SQS emitters. Unsupported connectors are rejected when the statement is validated.

`write_header` arguments must be statically non-null `STRING` expressions. They are evaluated after
payload construction and route filtering. Calls are staged in source order in a route-local
envelope; if any call fails, no payload or partial header envelope is published.

## Context And Identity

| Function | Returns | Notes |
| --- | --- | --- |
| `leak_sensitive(value)` | same type as input | Explicitly removes the sensitivity flag from a value |
| `now()` | `DATETIME` | Current execution-local domain timestamp |
| `uuid_v4()` | `STRING` | Random UUID string, new for every message |
| `uuid_v7()` | `STRING` | Time-ordered UUID string based on the execution-local domain clock, new for every message. Reports an `overflow` error when that time is before the Unix epoch |

A version 7 UUID encodes its time as milliseconds since `1970-01-01T00:00:00Z`. In a paced domain
whose logical time is earlier, such as one started with `START AT '1969-07-20T20:17:00Z'`,
`uuid_v7()` reports `uuid_v7 execution time is before the Unix epoch` for every message that
evaluates it until domain time reaches the epoch. It never encodes another instant instead.

## Null Handling

| Function | Returns | Notes |
| --- | --- | --- |
| `coalesce(a, b, ...)` | same type as inputs | Returns the first non-null argument, or a typed null when every argument is null. All arguments must have the same type |
| `is_null(x)` | `BOOL` | True when the input is null. Never null |
| `nullif(a, b)` | same type as inputs | Returns a typed null when `a = b`, and `a` otherwise, including when `b` is null. Both arguments must have the same type |

## String Functions

String functions count and select characters, meaning Unicode scalar values, rather than bytes.
Positions count from 1.

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
| `ascii(text)` | `I64` | Unicode code point of the first character, or `0` for an empty string |
| `initcap(text)` | `STRING` | Uppercases the first character of each run of letters and digits and lowercases the rest |
| `left(text, count)` | `STRING` | The first `count` characters. A negative `count` removes that many characters from the end |
| `right(text, count)` | `STRING` | The last `count` characters. A negative `count` removes that many characters from the start |
| `substr(text, start)` | `STRING` | Characters from position `start` to the end |
| `substr(text, start, length)` | `STRING` | At most `length` characters from position `start` |
| `substring(text, start)` | `STRING` | Alias for `substr` |
| `substring(text, start, length)` | `STRING` | Alias for `substr` |
| `concat(a, b, ...)` | `STRING` | Joins the arguments in order. A null argument contributes nothing, so the result is never null. All arguments must be `STRING` |
| `repeat(text, count)` | `STRING` | The text repeated `count` times, or an empty string when `count` is at most `0` |
| `replace(text, from, to)` | `STRING` | Replaces every occurrence of `from`, matched as plain text |
| `reverse(text)` | `STRING` | Reverses the characters |
| `lpad(text, length, fill)` | `STRING` | Pads on the left with repetitions of `fill` to `length` characters |
| `rpad(text, length, fill)` | `STRING` | Pads on the right with repetitions of `fill` to `length` characters |
| `split_part(text, delimiter, index)` | `STRING` | The part at position `index` |
| `strpos(text, needle)` | `I64` | Position of the first occurrence of `needle`, or `0` when it does not occur |
| `translate(text, from_chars, to_chars)` | `STRING` | Replaces each character found in `from_chars` with the character at the same position in `to_chars`, and removes a character that has no counterpart |
| `to_hex(value)` | `STRING` | Lowercase hexadecimal digits without a prefix. Integral input only; a negative value is written as its two's complement at the input's width |
| `md5(text)` | `STRING` | Lowercase hexadecimal digest of the UTF-8 bytes |

`lower`, `upper`, and `initcap` use Unicode case mappings, which never depend on the node's locale.
A mapping can change a value's length: `upper('Grüßen')` is `GRÜSSEN`. A literal, a field, and a
computed value holding the same text always convert to the same result.

`count`, `start`, `length`, and `index` may be any integer type and are never narrowed to another:
an unsigned count above the `I64` range reaches past the end of any text, exactly as the largest
`I64` count does.

`substr` treats a `start` at or before `1` as the first character and counts `length` from there. A
`start` past the end, or a negative `length`, returns an empty string.

`lpad` and `rpad` shorten text longer than `length` to its first `length` characters, return an
empty string when `length` is at most `0`, and return shorter text unchanged when `fill` is empty.

`split_part` returns an empty string when `index` is at most `0` or past the last part. With an
empty `delimiter`, the whole text is part `1`.

`repeat`, `lpad`, and `rpad` compute the length of a result before they build it. The values one
call produces for a batch share one `STRING` column, which holds at most 2,147,483,647 bytes of
text, so a result that does not fit in what its column has left reports an `overflow` error, such
as `repeat result exceeds the text one STRING column holds`, and yields null instead of being
built. Inside a conditional arm the column holds only the results of the messages that select the
arm, so a message that selects another arm uses none of its text. A call whose arguments are all
literals computes one value that every message in the batch holds, so that value must fit once for
each of them: `repeat('ab', 600000000)` fits a batch of one message but reports the error on every
message of a batch of two.

## Bytes, Encodings And Hashes

`BYTES` values hold arbitrary octets, including zero and non UTF-8 bytes. These functions accept
exact types, propagate nulls, and preserve sensitivity. An encoded or hashed sensitive input stays
sensitive; emitting it still requires explicit leakage.

| Function | Returns | Notes |
| --- | --- | --- |
| `bytes_from_utf8(text)` | `BYTES` | The exact UTF-8 bytes of a `STRING`, with no terminator or normalization |
| `bytes_to_utf8(bytes)` | `STRING` | Decodes UTF-8; invalid sequences fail that message |
| `base64_encode(bytes)` | `STRING` | RFC 4648 standard alphabet with required `=` padding |
| `base64_decode(text)` | `BYTES` | Accepts canonical padded standard base64; invalid alphabet, padding, or whitespace fails that message |
| `hex_encode(bytes)` | `STRING` | Two lowercase hexadecimal digits per byte, without a prefix |
| `hex_decode(text)` | `BYTES` | Accepts upper or lowercase hexadecimal pairs; odd length or any other character fails that message |
| `sha256(bytes)` | `BYTES` | The 32 raw SHA-256 digest bytes |
| `xxh3_64(bytes)` | `U64` | Stable XXH3 64-bit hash with seed zero |

`xxh3_64` hashes the input octets in their given order and returns the algorithm's unsigned 64-bit
number. The value is independent of host byte order and stays the same across nodes and restarts;
if exported as bytes, its canonical byte order is big endian. It is a noncryptographic hash, so use
`sha256` where collision resistance matters. Encoding uses SIMD-backed native libraries when the
node's CPU supports them.

Decoding failures enter the route's `ON MESSAGE ERROR` policy with `error.code = evaluation`.
`error.operation` names the route operation, such as `set`; the diagnostic names the failed
function without exposing its input.

## IP Addresses And Networks

An IP address is a `BYTES` value in network byte order: four octets for an IPv4 address and sixteen
for an IPv6 address, so its length is its family. `ip_from_string` reads an address from text, and
every other address function reads the octets as one 32-bit or 128-bit number and never reads text.
Parse an address once into a `BYTES` field and mask and test that field, so each further test costs
no second parse: `ip_from_string` can fail, so two calls of it are two parses even in one
expression.

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
every IPv4 address and `'::/0'` every IPv6 address. A network written as a literal, or computed from
literals alone, is read once when the statement is applied, and one that does not read rejects the
statement with a message that names its defect, such as
`function 'ip_in_network' network '10.0.0.1/8' has host bits set past its prefix length`. A network
read from a field is read for each message, a message that repeats the previous message's network
reuses that reading, and a network that does not read fails its message.

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
| `ip_in_network` | A network read from a field does not read | `invalid_argument`: `ip_in_network network` followed by the defect: `is not written as address/prefix`, `address is not an IPv4 or IPv6 address`, `prefix length is not a decimal number without leading zeros`, `prefix length must be 0 to 32 for an IPv4 network`, or `has host bits set past its prefix length` |

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
| `url_fragment(url)` | optional `STRING` | The fragment without its `#`, or null when the URL has no `#` |
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
  `ops@example.com`, and its host is null.

A component the URL lacks is null: the host of a `mailto:` or `urn:` URL and of a `file:` URL with
an empty host, the query of a URL without `?`, and the fragment of a URL without `#`. A URL that ends
in `?` or `#` has an empty query or fragment rather than a null one.

The query is read as `application/x-www-form-urlencoded` parameters. It is split at every `&`, an
empty parameter is skipped, and each parameter is split at its first `=` into a name and a value; a
parameter without `=` has an empty value. Names and values are percent-decoded with `+` read as a
space, so the query `q=hello+world%21&tag=x&tag=y%26z` has the value `hello world!` for `q` and the
values `x` and `y&z` for `tag`. A parameter matches when its decoded name equals `name` exactly,
letter case included. A parameter whose name does not decode matches no name, and a matching value
that does not decode fails the message.

`url_decode` replaces each `%` followed by two hexadecimal digits of either case with the octet they
name, and keeps every other character as it is, `+` included, so `url_decode(url_path(input.url))`
is the readable path. Rather than guess, it fails a message whose text has a `%` not followed by two
hexadecimal digits, or whose decoded octets are not UTF-8.

A null argument produces a null result. Otherwise a function fails only the messages it cannot
answer:

| Function | Fails when | Error |
| --- | --- | --- |
| Every URL function except `url_decode` and `is_url` | `url` is not an absolute URL | `cast_failed`, naming the function and the URL Standard's reason, such as `url_host input is not an absolute URL: relative URL without a base`; other reasons include `empty host`, `invalid port number`, and `invalid international domain name` |
| `url_query_value`, `url_query_values` | The value of a matching parameter is not percent-encoded UTF-8 | `cast_failed`, such as `url_query_value input is not valid percent-encoded UTF-8` |
| `url_decode` | `text` is not percent-encoded UTF-8 | `cast_failed`: `url_decode input is not valid percent-encoded UTF-8` |

A relative reference, such as the request target `/search?q=x` of an HTTP request line, is not an
absolute URL. Prefix it with a base explicitly:
`url_query_value(concat('http://localhost', input.target), 'q')`. To route text that may not be a
URL, test it first: `CASE WHEN is_url(input.referrer) THEN url_host(input.referrer) END` is null for
the messages whose referrer is not a URL.

Each call reads its own URL, so `url_host(input.url)` and `url_path(input.url)` each parse it, while
a URL that every message shares, such as a literal, is read once per batch. Percent-encoding and
ASCII domain names can make a component longer than the text it came from, and like the results of
`repeat`, a component that does not fit in what its `STRING` column has left reports an `overflow`
error. URL functions keep the sensitivity of their arguments, so the host of a sensitive URL is
sensitive.

```nspl,ignore
SET scheme = url_scheme(input.referrer),
    referrer_host = url_host(input.referrer),
    campaign = url_query_value(input.referrer, 'utm_campaign'),
    tags = url_query_values(input.referrer, 'tag'),
    landing_path = url_decode(url_path(input.referrer))
```

## String Predicates

Matching is exact and case-sensitive.

| Function | Returns | Notes |
| --- | --- | --- |
| `contains(text, needle)` | `BOOL` | True when `needle` occurs in `text` |
| `starts_with(text, prefix)` | `BOOL` | True when `text` begins with `prefix` |
| `ends_with(text, suffix)` | `BOOL` | True when `text` ends with `suffix` |

## Regular Expressions

Regular-expression functions take `STRING` arguments and use Rust regex syntax. A pattern that does
not compile reports a per-message `invalid_argument` error.

A pattern written as a literal, or computed from literals alone, is compiled once when the node is
activated and reused by every batch. It is still not a configuration error: a literal pattern that
does not compile reports its per-message error only for the messages that evaluate it, so an
invalid pattern in a conditional arm no message selects reports nothing. A pattern read from a
field is compiled when a message first uses it and kept in a cache of the 64 most recently
compiled patterns per call, which evicts the pattern compiled longest ago. A compiled pattern is
limited to 10 MiB; a pattern that compiles past that limit reports a per-message error like any
other invalid pattern.

| Function | Returns | Notes |
| --- | --- | --- |
| `regexp_like(text, pattern)` | `BOOL` | True when the pattern matches anywhere in the text |
| `regexp_replace(text, pattern, replacement)` | `STRING` | Replaces every match. `$1` and `${name}` in `replacement` insert a capture group, and `$$` inserts `$` |
| `regexp_substr(text, pattern)` | `STRING` | The first match, or null when the pattern does not match |

## Numeric Functions

Numeric functions accept every integer and floating-point type unless a description below narrows
it. A function that returns `F64` reads an integer argument as the nearest `F64`, which is exact for
every type up to 32 bits and rounds `I64` and `U64` values beyond 2^53. A floating-point result that
is not finite, such as `sqrt(-1.0)`, `ln(0.0)`, or `exp(1000.0)`, reports a per-message
`invalid_argument` error and yields null instead of producing NaN or an infinity.

| Function | Returns | Notes |
| --- | --- | --- |
| `abs(x)` | same numeric type as input | The absolute value. The minimum value of a signed integer type has none and reports an overflow |
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
| `sign(x)` | same numeric type as input | `-1` for a negative value, `1` for a positive one, and `0` for zero. A float zero keeps its sign, an infinity has the sign `-1.0` or `1.0`, and NaN has no sign and reports an error |
| `sin(x)` | `F64` | Sine of an angle in radians |
| `sqrt(x)` | `F64` | Square root |
| `tan(x)` | `F64` | Tangent of an angle in radians |
| `trunc(x)` | same numeric type as input | Rounds toward zero. Integer input is returned unchanged |

`abs`, `ceil`, `floor`, `round`, `sign`, `sqrt`, and `trunc` are exact: each returns the correctly
rounded result, which is the same on every node. `radians` and `degrees` multiply by the `F64`
nearest to `π/180` and to `180/π`, so their result is the same on every node and within two units
in the last place of the exact conversion. `acos`, `asin`, `atan`, `atan2`, `cos`, `exp`, `ln`,
`log`, `log2`, `pow`, `sin`, and `tan` evaluate in IEEE 754 double precision and return a result
within two units in the last place of the exact value. Nodes on different platforms may differ in
that last place.

A function reports an error exactly where its result does not exist in its type. For finite
arguments, that is:

| Function | Arguments that report an error |
| --- | --- |
| `abs`, `ceil`, `floor`, `round(x)`, `sign`, `trunc` | None. `abs` over the minimum value of a signed integer type reports an `overflow` error instead |
| `round(x, digits)` | A negative `digits` whose rounded multiple exceeds the range of `x`'s type. A float reports an `invalid_argument` error and an integer an `overflow` error |
| `sqrt(x)` | `x < 0` |
| `ln(x)`, `log(x)`, `log2(x)` | `x <= 0` |
| `log(base, x)` | `x <= 0`, `base < 0`, and `base = 1`; the result is `ln(x) / ln(base)` |
| `acos(x)`, `asin(x)` | `x < -1` and `x > 1` |
| `exp(x)` | `x` above about `709.78`, where the result overflows. A result too small to represent is `0.0` |
| `degrees(x)` | `x` beyond about `±3.14e306`, where the result overflows |
| `pow(x, y)` | A result that overflows, `x < 0` with a `y` that is not an integer, and `x = 0` with `y < 0` |
| `atan(x)`, `atan2(y, x)`, `cos(x)`, `radians(x)`, `sin(x)`, `tan(x)` | None |

A NaN or infinite argument reports an error unless a finite result is defined for it, as IEEE 754
defines for `atan` of an infinity, `atan2` of any arguments that are not NaN, `exp` of negative
infinity, and `pow(x, 0)`, and as `sign` defines for an infinity.

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
about 292 years. A call with an unknown name, a non-literal argument, an invalid format, or a width
that is not positive is rejected, and its message names what the call accepts. A call that names no
`zone` reads in UTC; see [Time Zones](#time-zones) for the zones a call can name.

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

A null argument produces a null result. `date_part`, `to_unix`, and `format_datetime` never fail.
Another function reports a per-message error and yields null exactly where it cannot produce a
result:

| Function | Fails when |
| --- | --- |
| `date_trunc`, `date_bin` | The unit or bin that holds `value` starts before the `DATETIME` range, with an `overflow` error |
| `date_add` | The moved instant is outside the `DATETIME` range, even when `amount` units alone would be longer than the range, with an `overflow` error |
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
  time: New York ran 4:56:02 behind UTC until noon on 1883-11-18, so `date_trunc('day', ...)` of
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
  once from `value`, so adding one month twice to January 31 lands on March 29, while adding two
  months at once lands on March 31.
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
   `2024-03-09T12:00:00Z` to `2024-03-10T11:00:00Z` in `America/New_York` is `1`, because noon on
   2024-03-10 in New York is `11:00` in UTC, and it is `0` in UTC, where the two instants are 23
   hours apart.
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
- A fixed UTC offset written `'+HH:MM'` or `'-HH:MM'`, from `'-23:59'` to `'+23:59'`.

Anything else, including an abbreviation such as `'CEST'` that is not also an IANA name, the
host's local zone, or an offset written another way, is rejected when the statement is applied. A zone only
decides how an instant reads as a local date and time: every `DATETIME` a function returns is a UTC
instant.

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
`%-S`. Any other directive, including a two-digit year, a locale's date or time such as `%c`, and a
flag on a directive that does not take it, is rejected when the statement is applied, and the
message names the directive and the byte of the format it starts at. Offsets from local mean time
can have seconds, as New York's `-04:56:02` before 1883 does. `%s` counts whole seconds rounded
down, so `%s%.f` writes `1969-12-31T23:59:59.5Z` as `-1.5`: one second before the epoch plus half a
second.

Every value a format describes must fit in 256 bytes, counting every directive at its longest in
the call's zone. A format that could describe a longer value is rejected when the statement is
applied, so each value of a formatted column holds at most 256 bytes.

### Writing Values

`format_datetime` writes `value`'s local date and time in `zone`, and writes a null `value` as null.
It never fails. `format_datetime('%FT%T%:z', ...)` of `2024-11-03T06:30:00Z` in `America/New_York`
is `2024-11-03T01:30:00-05:00`.

### Reading Text

`parse_datetime` reads `text` from its first byte to its last and never guesses or normalizes what
it reads:

- Literal text must appear exactly, and a directive reads exactly its field: `%m` reads two digits,
  and `%e` reads a space and a digit, or two digits. A `-` flag reads one digit up to the field's
  width, with or without a leading zero. `%.f` reads nothing, or `.` and one to nine digits. `%s`
  reads an optional `-` and one to nineteen digits.
- Month and weekday names, and `AM` and `PM`, are read without regard to letter case. `%z`, `%:z`,
  and `%::z` read the forms they write, and also read `Z` as UTC.
- Every field must lie in its range: a 13th month, hour `24`, and second `60` are rejected, since a
  `DATETIME` has no leap seconds. The date must exist, so `2023-02-29` is rejected rather than read
  as March 1, and a day of the week read with a calendar or ordinal date must be that date's day.

A readable format reads each field at most once and names exactly one instant:

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

A message never quotes the text it could not read, only byte positions, directives, and field
names.

## Array And Vector Functions

These functions take one `ARRAY` or `VEC` value, described in
[Schemas And Codecs](schemas-and-codecs.md#internal-schemas). The elements of a multidimensional
`ARRAY` are its outermost items. A null list produces a null result.

| Function | Returns | Notes |
| --- | --- | --- |
| `count(list)` | `I64` | Number of elements, counting null elements |
| `sum(list)` | element type | Sum of the non-null elements. Numeric elements only. An empty list, or one whose elements are all null, returns null |
| `first(list)` | element type | The first element, or null for an empty list |
| `last(list)` | element type | The last element, or null for an empty list |
| `nth(list, index)` | element type | The element at `index`, counting from `0`, or null when `index` is negative or past the end. `index` may be any integer type |

`nth` counts from `0`, unlike string positions: `nth(input.values, 0)` is the same element as
`first(input.values)`. `first`, `last`, and `nth` require scalar or `DATETIME` elements; a list
whose elements are themselves `ARRAY` or `VEC` values is rejected when the statement is validated.

`sum` follows the arithmetic operators. An integer sum that overflows its type reports an overflow,
and a floating-point sum that is not finite reports a per-message error.

In a [window processor](processors.md#window-processor) route, `count`, `sum`, `first`, and `last`
are always window aggregates, like every other
[window aggregate function](processors.md#window-aggregate-functions). They take a per-row
expression over `input` and aggregate it across the rows the window retained, so
`COUNT(input.values)` aggregates retained rows rather than counting the elements of one list.
Everywhere else these names are the list functions above.

## Example

```nspl,ignore
INHERIT ALL EXCEPT raw
SET normalized = lower(trim(input.raw)),
    observed_at = now(),
    event_id = uuid_v7(),
    prefix = left(trim(input.raw), 5),
    digest = md5(trim(input.raw)),
    magnitude = abs(input.amount),
    rooted = sqrt(input.score),
    price = round(input.price, 2),
    heading = degrees(atan2(input.north, input.east)),
    alert_flags = bitwise_and(input.flags, 255 AS U16),
    peak = greatest(input.first_reading, input.second_reading),
    bounded = clamp(input.score, 0, 100),
    moved = input.region IS DISTINCT FROM input.home_region,
    retries = coalesce(TRY_CAST(input.retries_text AS I32), 0)
WHERE output.active
  AND regexp_like(lower(trim(input.raw)), 'warn|error')
  AND input.status IN ('open', 'held')
  AND input.amount BETWEEN 1 AND 1000
```
