# The Working Message

A transforming route operates on one working message. It begins as the route's input view. The
route constructs a new output shape: `INHERIT` carries fields into that shape, ordered `SET`
assignments rewrite it step by step, and finalization fixes it as the route output.
`message.<field>` always reads the working message as it currently stands.

Three scopes pin reads to points on that timeline:

- `input.<field>` pins the original record. It never changes.
- `message.<field>` reads the live current value.
- `output.<field>` reads only a value that this route has already explicitly initialized.

The precise resolution rule is:

During transforming construction, `message.field` and a bare RHS field read the working output and
fall back to the exact-compatible `input.field` only while the output field is uninitialized.
After finalization, route `WHERE` sees the finalized output and performs no fallback.

Set-only output-construction routes have no `message` or `input`.

## One Record, Step By Step

Assume the input record contains `x = 2` and `untouched = 9`. Both fields are required `I64`.
This route constructs an output with `x`, `copied_untouched`, and other required fields:

```nspl,ignore
INHERIT x
SET x = message.x + 3,
    copied_untouched = message.untouched,
    x = message.x * 2
```

The working message changes in written order:

| Point | Initialized output | Read | Result |
| --- | --- | --- | --- |
| Before `INHERIT` | nothing | `message.x` | `2` from the exact-compatible input view |
| After `INHERIT x` | `x = 2` | `message.x` | `2` from the initialized output |
| After the first `SET` | `x = 5` | `message.x` | `5` |
| While setting `copied_untouched` | `x = 5` | `message.untouched` | `9` from input; `untouched` was never inherited or set |
| While repeating the `x` target | `x = 5` | `message.x` | `5`, so the final `x` is `10` |
| After finalization | `x = 10`, `copied_untouched = 9` | route `WHERE` | only the finalized output; no input fallback |

This is not an implicit identity transformation. Enrichment is a basic operation of Nervix, and
the working message is the native consequence of native inheritance: a route can carry input
values forward while progressively replacing selected values. Use `input` and `output` when every
read should be pinned to the original record or to explicit output initialization.

## Scope Boundaries And Edge Cases

Exact compatibility means the same Nervix type and the same nullability. If the input and output
both declare a field name but their types or nullability differ, an uninitialized output field does
not make the incompatible input field available through `message` or a bare read. Use an explicit,
valid conversion from `input.<field>` where the language supports one.

Set-only output-construction routes begin with an empty output, reject `INHERIT`, and expose neither
`message` nor `input`. They must initialize every required output field. Omitted optional fields
finalize as typed nulls. Direct-emitter `VALUES` is a separate source-row mapping surface:
`input`, `message`, and bare fields read the source row, while `output` is unavailable.

Correlators have no default input scope. They expose `left` and `right`, reject bare RHS field
reads, and do not expose `input` or `message`.

Generated inferencer and WASM routes are set-only. Bare reads resolve against immutable generated
state until a same-named output field has been initialized. Generated state is independently
visible to every route and never initializes route output automatically.

Error routes expose the original eligible `input`, or `left` and `right` for correlators; the exact
captured `relay_state` snapshot; structured `error`; and the all-optional `partial_output` view.
Their ordered assignments may read values initialized by earlier error-record assignments. They do
not turn a partial output into a transforming working message.

See [Runtime Nodes](processors.md#filters-and-construction) for route families and
[Data Plane](data-plane.md#working-message-execution) for the implementation boundary.
