# Client Libraries

*Plexus, the Nervix graph definition language.*

A client library lets your own program do what the [Client Tools](client-tools.md) do: open a
session against the cluster's session API, submit Plexus, read relay rows, follow a domain clock,
and publish to and consume from client endpoints. Choose the library of the language your program is
written in.

## Choosing A Library

| Library | Runtime | Install | Use it for |
| --- | --- | --- | --- |
| [Rust Client Library](client-library.md) | Rust | the `nervix-client-core` crate of the Nervix repository | a Rust program, and the reference every other library runs on |
| [JVM Client Library](client-library-jvm.md) | Java 21 and newer | the self-loading jar `just build-client-jvm` builds | Java and other JVM applications, with typed column buffers and canonical Arrow IPC |
| [Python Client Library](client-library-python.md) | CPython 3.12 and newer | the `nervix_client` wheel `just build-client-python` builds | a Python program, with Arrow data exchanged with pyarrow and polars |
| [Ruby Client Library](client-library-ruby.md) | CRuby 3.3 and newer | the native `nervix-client` gem `just build-client-ruby` builds | Ruby applications, with collector-owned handles and Arrow IPC buffers |
| [C Client Library](client-library-c.md) | C11 | the `nervix-client-ffi` shared library and `nervix_client.h` | a C program with explicit ownership and typed Arrow columns |
| [C++ Client Library](client-library-c.md) | C++17 | the same C ABI, with application-owned custom deleters | a C++ program using RAII and canonical Arrow IPC |

## What Every Library Shares

- **One client.** Every library runs the Rust client, `nervix-client-core`, in your process: the
  Python package, JVM library and Ruby gem on pyo3, JNI and magnus over the host session runtime every binding
  shares, and C and C++ through its C ABI. Leader redirects, reconnection, request correlation,
  execution identity, and the restoration of subscriptions, domain clocks, producers and consumers
  behave the same in every language, because they are the same code.
- **One recovery.** A call that ends before its outcome is known names the command's execution
  reference, and executing the same prepared execution again recovers that outcome instead of running
  the command twice.
- **One protocol.** Every library speaks the [Client Session Protocol](client-session-protocol.md),
  and the [Client Implementation Manual](client-implementation-manual.md) states the rules a host must
  follow as normative B- and N-rules; each library chapter links the rules where they apply.
- **Tested examples.** Every code block of a host language's chapter, such as the Python one, is a
  test of that host's own suite, which runs against a real cluster, so a documented example that
  stops working fails a test.
