# Expression Functions

These functions are available in structured NSPL expressions on ingestors, processors, routes,
and emitters:

```nspl
[INHERIT ...]
[SET <field> = <expr>, ...]
[WHERE <expr>]
[INVOKE write_header(<name-expr>, <value-expr>), ...]
```

`SET` assignments and `INVOKE` calls execute left to right. A transforming route begins empty and
may initialize fields with `INHERIT` and `SET`; a set-only route begins empty and supports only
`SET`. Route `WHERE` runs after output finalization. `INVOKE` and `write_header` are emitter-only;
side-effect functions are invalid inside ordinary expressions.

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
| `uuid_v4()` | `STRING` | Random UUID string |
| `uuid_v7()` | `STRING` | Time-ordered UUID string based on the execution-local domain clock |

## Null Handling

| Function | Returns | Notes |
| --- | --- | --- |
| `coalesce(a, b, ...)` | same type as inputs | All arguments must have the same type |
| `is_null(x)` | `BOOL` | True when the input is null |
| `nullif(a, b)` | same type as inputs | Returns null when the inputs are equal |

## String Functions

| Function | Returns | Notes |
| --- | --- | --- |
| `lower(text)` | `STRING` | Lowercases the input |
| `upper(text)` | `STRING` | Uppercases the input |
| `trim(text)` | `STRING` | Trims both ends |
| `btrim(text)` | `STRING` | Trims both ends |
| `ltrim(text)` | `STRING` | Trims the left side |
| `rtrim(text)` | `STRING` | Trims the right side |
| `length(text)` | `I64` | Character count |
| `char_length(text)` | `I64` | Character count |
| `bit_length(text)` | `I64` | Bit length of the UTF-8 string |
| `ascii(text)` | `I64` | ASCII code of the first character |
| `initcap(text)` | `STRING` | Title-cases words |
| `left(text, count)` | `STRING` | Left substring |
| `right(text, count)` | `STRING` | Right substring |
| `substr(text, start)` | `STRING` | 1-based substring |
| `substr(text, start, length)` | `STRING` | 1-based substring with explicit length |
| `substring(text, start)` | `STRING` | Alias for `substr` |
| `substring(text, start, length)` | `STRING` | Alias for `substr` |
| `concat(a, b, ...)` | `STRING` | All arguments must be `STRING` |
| `repeat(text, count)` | `STRING` | Repeats the string |
| `replace(text, from, to)` | `STRING` | Plain string replacement |
| `reverse(text)` | `STRING` | Reverses characters |
| `lpad(text, length, fill)` | `STRING` | Left pad with the fill text |
| `rpad(text, length, fill)` | `STRING` | Right pad with the fill text |
| `split_part(text, delimiter, index)` | `STRING` | 1-based part index |
| `strpos(text, needle)` | `I64` | 1-based position |
| `translate(text, from_chars, to_chars)` | `STRING` | Character-by-character translation |
| `to_hex(value)` | `STRING` | Integral input only |
| `md5(text)` | `STRING` | Lowercase hexadecimal digest |

## String Predicates

| Function | Returns | Notes |
| --- | --- | --- |
| `contains(text, needle)` | `BOOL` | Substring test |
| `starts_with(text, prefix)` | `BOOL` | Prefix test |
| `ends_with(text, suffix)` | `BOOL` | Suffix test |

## Regular Expressions

Regular-expression functions take `STRING` arguments and use Rust regex syntax.

| Function | Returns | Notes |
| --- | --- | --- |
| `regexp_like(text, pattern)` | `BOOL` | True when the pattern matches |
| `regexp_replace(text, pattern, replacement)` | `STRING` | Replaces all matches |
| `regexp_substr(text, pattern)` | `STRING` | Returns the first matching substring |

## Numeric Functions

| Function | Returns | Notes |
| --- | --- | --- |
| `abs(x)` | same numeric type as input | Numeric input only |
| `acos(x)` | `F64` | Numeric input only |
| `asin(x)` | `F64` | Numeric input only |
| `atan(x)` | `F64` | Numeric input only |
| `ceil(x)` | same numeric type as input | Numeric input only |
| `ceiling(x)` | same numeric type as input | Alias for `ceil` |
| `cos(x)` | `F64` | Numeric input only |
| `exp(x)` | `F64` | Numeric input only |
| `floor(x)` | same numeric type as input | Numeric input only |
| `ln(x)` | `F64` | Numeric input only |
| `log(x)` | `F64` | Base-10 logarithm |
| `log(base, x)` | `F64` | Logarithm with explicit base |
| `pow(x, y)` | `F64` | Numeric inputs only |
| `power(x, y)` | `F64` | Alias for `pow` |
| `round(x)` | same numeric type as input | Numeric input only |
| `sqrt(x)` | `F64` | Numeric input only |
| `tan(x)` | `F64` | Numeric input only |

## Example

```nspl
INHERIT ALL EXCEPT raw
SET normalized = lower(trim(input.raw)),
    observed_at = now(),
    event_id = uuid_v7(),
    prefix = left(trim(input.raw), 5),
    digest = md5(trim(input.raw)),
    magnitude = abs(input.amount),
    rooted = sqrt(input.score)
WHERE output.active AND regexp_like(lower(trim(input.raw)), 'warn|error')
```
