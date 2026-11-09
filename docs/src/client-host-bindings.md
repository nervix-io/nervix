# Client Host Bindings

The native Python, Ruby and JVM libraries project the shared `nervix-client-host` runtime through
pyo3, magnus and JNI. The C and C++ binding projects that runtime through `nervix-client-ffi`.
The host guides own application examples; the [Client Implementation Manual](client-implementation-manual.md)
owns protocol obligations and the [Client Session Protocol](client-session-protocol.md) owns the
wire contract. Host bindings keep host-specific value and lifetime handling at this edge.

## Typed Inspection

`Outcome::state_inspection()` supplies the existing owned `Inspection`. Its state is the
verified `StateInspection`, and its cached JSON is that envelope's lossless serialization.
Bindings retain this owner for page/refusal, entity, complete counts, unit identity, ordered
replicas, captured origin, section and opaque continuation access. They add no second inspection
representation and derive no typed value from display text.

Python and Ruby metadata objects share one inspection owner. JVM unit and section handles lease
the same inspection with immutable checked indices, so closing a parent does not invalidate a
child. `InspectionFrame` retains exactly one verified Row frame and its schema. Its construction
checks schema conformance, clones the shared backing bytes and does not retain the whole page.
Schema handles returned by a frame may outlive that frame.

The Python buffer exporter, Ruby read-only `IO::Buffer` exporter and JVM direct-buffer lease
already used for event bytes also retain inspection frames. Derived slices preserve that owner
after all parent objects close or are collected. These are the session's exact Row/Cell bytes,
with integer widths, float bits, timestamps, nested values, branch identity, null and redaction
preserved. They are distinct from the Arrow IPC exposed by batches.

Refusals retain their typed reason and have no page counts, units or continuation. Empty pages
remain successful pages. Unit classification is `Single`, `Partition` or `Instance`; its optional
index preserves the current unit identity. JVM unsigned 64-bit metadata stays in Java `long`
bits. A continuation remains opaque to every binding; server selection and fencing rules own it.

The native owner tests compare complete JSON, exact frame bytes and schema metadata while testing
independent close/GC lifetimes. The host's registered bounded inspection property checks the
complete envelope and shared-frame projection. The one-/three-node public scenarios in
`native_binding_inspection.feature` cover empty pages, refusal, sensitive branched pages,
continuation and views retained after session close. Normal pytest, minitest and JUnit suites
also exercise the public classes; their exact member and test inventory is checked by
`just validate-client-suites`.
