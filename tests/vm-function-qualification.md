# VM function public qualification matrix

This ledger maps the function contracts in [Expression Functions](../docs/src/filter-map-functions.md)
to public Cucumber scenarios. A family scenario proves its values and failures through an NSPL
graph and session subscription. The context and client scenarios below exercise the common
compiler, execution, and transport boundaries. Scenario outlines run on one and three nodes unless
the cited feature explicitly tests a topology-specific event.

## Function families and corrected behavior

| Contract | Public scenarios | Qualification |
| --- | --- | --- |
| Conditional selection, comparison, equality, and active rows | [Conditional expressions](features/runtime/conditional_expressions.feature), [expression semantics](features/runtime/expression_function_semantics.feature) | Selected arms alone evaluate failures, including casts, patterns, headers, UDFs and float functions; `NULLIF` agrees with equality for NaN and signed zero. |
| Membership, ranges, null-safe equality, extrema | [Membership and extrema](features/runtime/membership_ranges_extrema.feature) | Ingestor and source filters, routes, read-only subscriptions, lookup keys, inferencer mappings, nulls, invalid types, sensitivity. |
| Checked arithmetic | [Checked numeric execution](features/runtime/checked_numeric_execution.feature) | Batch-local overflow, division and non-finite errors; reingestor and emitter error routes. |
| Explicit and tolerant conversions | [Tolerant conversions](features/runtime/tolerant_conversions.feature), [strict schema typing](features/runtime/schema_type_strictness.feature) | `AS` errors, `TRY_CAST` typed nulls, conditional selection, output field type rejection, materialized defaults and inferencer input. |
| JSON extraction | [JSON extraction](features/runtime/json_extraction.feature) | Declared scalar and collection types, strict errors, tolerant nulls, active-row selection and sensitivity. |
| Hash map lookup | [Lookup hash map](features/runtime/lookup_hash_map.feature) | Filters before lookup, missing-key validation and independent resolution for output routes. |
| Header reads and context identity | [Conditional expressions](features/runtime/conditional_expressions.feature), [HTTP ingestor logic](features/runtime/http_ingestor_logic.feature), [datetime functions](features/runtime/datetime_functions.feature) | Header-capable ingestion scopes, selected-arm header reads, UUID calls and execution-local domain time. |
| Null handling | [Expression semantics](features/runtime/expression_function_semantics.feature), [membership and extrema](features/runtime/membership_ranges_extrema.feature), [client conformance](features/runtime/client_conformance.feature) | `NULLIF` equality, null-safe comparison, and `coalesce`/`nullif` results in typed client rows. |
| Unicode string functions, predicates and aliases | [Expression semantics](features/runtime/expression_function_semantics.feature), [string search](features/runtime/string_search.feature), [client conformance](features/runtime/client_conformance.feature) | Literal, column and repeated calls agree; case conversion, search, `LIKE`, normalization, alias behavior and exact Unicode transport. |
| Regular expressions | [Expression semantics](features/runtime/expression_function_semantics.feature), [conditional expressions](features/runtime/conditional_expressions.feature) | Prepared and dynamic patterns, selected-arm evaluation, per-message pattern errors. |
| Bytes, encodings, hashes | [Bytes functions](features/runtime/bytes_functions.feature), [client conformance](features/runtime/client_conformance.feature) | Binary round trips, invalid text errors, sensitivity, and function-produced bytes through independent clients. |
| IP, CIDR and URL functions | [Network functions](features/runtime/network_functions.feature) | Typed address parsing, route selection, URL components, invalid-row isolation, type rejection and sensitivity. |
| Numeric math, classification, rounding and bits | [Numeric classification](features/runtime/numeric_classification_math_bits.feature), [checked numeric execution](features/runtime/checked_numeric_execution.feature) | Exact signatures, finite-result checks, shifts and rounding overflow, per-message errors. |
| Datetime and domain time | [Datetime functions](features/runtime/datetime_functions.feature), [client conformance](features/runtime/client_conformance.feature) | Event timestamps, execution-local `now()`, bounds, type rejection and nanosecond transport extremes. |
| Calendar, time zone and formatting | [Calendar datetime](features/runtime/calendar_datetime_functions.feature) | Local-calendar arithmetic, ambiguous local time handling, format validation and domain execution time. |
| Fixed arrays and vectors | [Array and vector functions](features/runtime/array_vector_functions.feature), [expression semantics](features/runtime/expression_function_semantics.feature) | Fixed widths, ragged and empty vectors, nested element rejection, item count bounds and overflow. |
| Window exact statistics and sketches | [Window statistics](features/runtime/window_statistics.feature), [window sketches](features/runtime/window_sketches.feature) | Branch-local sliding/tumbling results, typed nulls, contract rejection, eviction and recovery. |

## Execution contexts and boundaries

| Requirement | Public scenarios or checks | Evidence boundary |
| --- | --- | --- |
| Ingestor and reingestor | [Membership and extrema](features/runtime/membership_ranges_extrema.feature), [checked numeric execution](features/runtime/checked_numeric_execution.feature), [HTTP ingestor logic](features/runtime/http_ingestor_logic.feature) | Source/route predicates and construction; reingestor message error route. |
| Ordinary and error routes | [Expression semantics](features/runtime/expression_function_semantics.feature), [bytes functions](features/runtime/bytes_functions.feature), [network functions](features/runtime/network_functions.feature) | Route-local construction, failed-row isolation and structured error output. |
| Windows and concrete branches | [Window statistics](features/runtime/window_statistics.feature), [window sketches](features/runtime/window_sketches.feature) | Interleaved branch state, nulls, eviction, restart and ownership change. |
| Generator and materialized state | [Generator](features/runtime/generator.feature), [junction](features/runtime/junction.feature), [tolerant conversions](features/runtime/tolerant_conversions.feature) | Set-only construction, captured state, branch-local snapshots, defaults and error scopes. |
| Emitters | [Checked numeric execution](features/runtime/checked_numeric_execution.feature), [Kafka emission](features/runtime/kafka_emission.feature) | Function errors route before the external boundary; emission keeps explicit header and leakage contracts. |
| Read-only subscriptions and capability limits | [Membership and extrema](features/runtime/membership_ranges_extrema.feature), [subscription options](features/runtime/session_subscription_options.feature), [subscription capability compile-fail cases](ui/subscription_predicate_capabilities) | Filtered views only; the type boundary forbids a construction program or arbitrary predicate. |
| Roto and WASM extensions | [UDF](features/runtime/udf.feature), [WASM processor](features/runtime/wasm_processor.feature) | Roto calls compose with builtins over columns; guest inputs, errors and branch state stay at the WASM boundary. |
| Plan activation, schema mutation and prepared patterns | [VM function qualification](features/runtime/vm_function_qualification.feature), [alter running domain](features/runtime/alter_running_domain.feature) | A committed schema/codec/function change installs a new signature, and replacing a route installs its new prepared regex before ingestion resumes. Lookup rebinding is tracked by its own linked task. |
| Final Client Wire | [Client conformance](features/runtime/client_conformance.feature), [client conformance ledger](client-conformance-ledger.md), [wire corpus](../crates/client-wire/conformance) | Native gRPC, binary WebSocket, C ABI bindings and independent Go/TypeScript clients verify widths, nullability, sensitivity, bytes, nanosecond timestamps, arrays and 64-bit extremes. |

The VM function scenarios assert values at the session boundary. The checked-in wire corpus also
tests shapes that cannot be sent by the scalar binding API, including nested arrays. The live client
scenario passes function-produced string, bytes, integer, datetime and sensitive values through the
same subscription protocol as the other runtime scenarios.

## Verification

The new `A schema mutation installs a newly compiled function plan before ingestion resumes` and
`Replacing a route installs its newly prepared regular expression` outlines passed on both one-
and three-node clusters (4 scenarios, 38 steps). The updated `Case conversion agrees across
literal, column and repeated expressions` outline also passed on both topologies.

The full `just test` run passed 1,986 scenarios and 21,628 steps, including the client conformance
suite. The standalone client conformance run passed 19 scenarios; client wire tests, subscription
capability compile-fail cases, the NSPL completion walk, `just ratchet`, and `just validate` passed.
The focused `just test-coverage-feature` run passed the new qualification, expression semantics,
and client conformance features and produced `lcov.info`. This patch changes Cucumber specifications
and Markdown only; it has no modified Rust source lines for Codecov patch coverage to measure.
