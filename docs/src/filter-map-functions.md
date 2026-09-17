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
- there is no implicit cast insertion
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
does not change any value. How a function traverses its column, whether through an Arrow compute
kernel, one pass over the column's value buffer, or a loop over its rows, is internal to the
function and does not change its results.

A pass over a value buffer is written so the compiler can turn it into the vector instructions of
the CPU a Nervix binary is built for. Builtins contain no hand-written SIMD code, and the vector
instructions a node's CPU offers never change a builtin's result.

The compiler applies two optimizations that preserve results in the same way:

- A deterministic call that cannot fail and whose arguments are all literals may be computed once
  instead of for every batch. It produces exactly the value that evaluating it for each message
  would, so `upper('grüßen')` and `upper(input.text)` agree when `input.text` holds `grüßen`.
- Identical deterministic expressions that cannot fail may be computed once per batch and shared.
  Calls that return a new value for every message, such as `uuid_v4()`, and calls that can report
  a per-message error are evaluated at each occurrence, so each occurrence reports its own error.

## Function Properties

Every builtin follows these rules unless its own description says otherwise:

| Property | Contract |
| --- | --- |
| Types | Arguments are never converted implicitly. A function that accepts several types, such as `abs` over every numeric type, takes each of them as it is. |
| Nulls | A null argument produces a null result. `coalesce`, `nullif`, `concat`, and `is_null` define their own null handling. |
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

Conditional values are computed in the columnar batch engine. Per-message evaluation errors are
observed only for the selected result arm, so an error in an unselected arm does not activate
`ON MESSAGE ERROR`. Context-injected operations such as window aggregates and header reads retain
their existing batch-level failure behavior.

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
- `nullif(a, b)` and a simple `CASE <operand> WHEN <value>` decide equality exactly as `=` does.

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
| `uuid_v7()` | `STRING` | Time-ordered UUID string based on the execution-local domain clock, new for every message |

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

`substr` treats a `start` at or before `1` as the first character and counts `length` from there. A
`start` past the end, or a negative `length`, returns an empty string.

`lpad` and `rpad` shorten text longer than `length` to its first `length` characters, return an
empty string when `length` is at most `0`, and return shorter text unchanged when `fill` is empty.

`split_part` returns an empty string when `index` is at most `0` or past the last part. With an
empty `delimiter`, the whole text is part `1`.

## String Predicates

Matching is exact and case-sensitive.

| Function | Returns | Notes |
| --- | --- | --- |
| `contains(text, needle)` | `BOOL` | True when `needle` occurs in `text` |
| `starts_with(text, prefix)` | `BOOL` | True when `text` begins with `prefix` |
| `ends_with(text, suffix)` | `BOOL` | True when `text` ends with `suffix` |

## Regular Expressions

Regular-expression functions take `STRING` arguments and use Rust regex syntax. A pattern that does
not compile reports a per-message error.

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
result for the same arguments whenever it runs. The current time enters an expression as a value only
through `now()`, which returns the execution-local domain time: `date_trunc('day', now())` follows a
paced domain's logical clock at any `TIME RATE`.

| Function | Returns | Notes |
| --- | --- | --- |
| `date_part(part, value)` | `I64` | One part of `value` in UTC, named by `part` from the table of date parts below |
| `date_trunc(unit, value)` | `DATETIME` | The start of the `unit` that holds `value`. A day starts at midnight UTC and a week on Monday |
| `date_bin(unit, width, value, origin)` | `DATETIME` | The start of the bin that holds `value`, for bins `width` units wide that start at `origin` and at every whole number of widths before and after it |
| `date_add(unit, amount, value)` | `DATETIME` | `value` moved by `amount` units, backward when `amount` is negative. `amount` may be any integer type |
| `date_diff(unit, start, end)` | `I64` | The whole units from `start` to `end`, rounded toward zero. Negative when `end` is before `start` |
| `to_unix(unit, value)` | `I64` | The whole units from `1970-01-01T00:00:00Z` to `value`, rounded down |
| `from_unix(unit, count)` | `DATETIME` | The instant `count` units after `1970-01-01T00:00:00Z`, or before it when `count` is negative. `count` may be any integer type |

`unit`, `part`, and `width` are literals written in the call, not expressions, and they are checked
when the statement is applied. `unit` and `part` are `STRING` literals whose names are
case-insensitive. `width` is a positive integer literal, and `width` units must not exceed
9,223,372,036,854,775,807 nanoseconds, about 292 years. A call with an unknown name, a non-literal
argument, or a width that is not positive is rejected, and its message names what the call accepts.

Every unit has a fixed length. A `DATETIME` has no leap seconds, so every UTC day is exactly 86,400
seconds long. A calendar month or year has no fixed length and is not a unit.

| Unit | Length |
| --- | --- |
| `nanosecond` | 1 nanosecond |
| `microsecond` | 1,000 nanoseconds |
| `millisecond` | 1,000 microseconds |
| `second` | 1,000 milliseconds |
| `minute` | 60 seconds |
| `hour` | 60 minutes |
| `day` | 24 hours |
| `week` | 7 days |

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
- A day, and every shorter unit, starts a whole number of units after the Unix epoch. A week starts
  on Monday in `date_trunc`, while `to_unix('week', ...)` counts whole weeks from the epoch itself,
  which was a Thursday.
- `date_bin` accepts an `origin` before or after `value`. A value exactly on a bin boundary starts
  its own bin.
- `date_diff` rounds toward zero, so exchanging `start` and `end` only changes the sign of its result.
  `date_diff('second', start, end)` is `0` when the two values are less than a second apart in either
  direction.

A null argument produces a null result. `date_part` and `to_unix` never fail. Another function
reports a per-message `overflow` error and yields null exactly where its result cannot be
represented:

| Function | Fails when |
| --- | --- |
| `date_trunc`, `date_bin` | The unit or bin that holds `value` starts before the `DATETIME` range |
| `date_add` | The moved instant is outside the `DATETIME` range, even when `amount` units alone would be longer than the range |
| `from_unix` | The instant is outside the `DATETIME` range |
| `date_diff` | The whole units do not fit `I64`, which only a count of nanoseconds between values more than about 292 years apart can reach |

The error's message names the function, such as `date_add result is outside the DATETIME range` or
`date_diff result does not fit I64`.

```nspl,ignore
SET hour = date_part('hour', input.occurred_at),
    quarter_hour = date_bin('minute', 15, input.occurred_at, from_unix('second', 0)),
    deadline = date_add('millisecond', input.timeout_ms, input.occurred_at),
    age_seconds = date_diff('second', input.occurred_at, now()),
    received_at = from_unix('millisecond', input.epoch_ms)
```

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
    alert_flags = bitwise_and(input.flags, 255 AS U16)
WHERE output.active AND regexp_like(lower(trim(input.raw)), 'warn|error')
```
