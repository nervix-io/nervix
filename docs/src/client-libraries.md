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

The JVM, Python, Ruby and C and C++ libraries are built from the Nervix repository today; none is
published to a package registry.

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
  follow as normative host rules: B- for the C ABI, N- for Python, R- for Ruby and J- for the JVM.
  Each library chapter links the rules where they apply.
- **One shape.** The JVM, Python, Ruby and C and C++ chapters have the same sections in the same
  order, from Install and Connect through Errors, threads and lifetimes to What The Library Does
  Not Expose and A Complete Application, so a developer who knows one finds each answer at the same
  place in the next.
- **Tested examples.** Every code block of those chapters is a test of that library's own suite,
  written in its language and test framework. Some run without a cluster and the rest against
  one- and three-node clusters, so a documented example that stops working fails a test.

## How The Libraries Differ

Each library follows its own language's conventions for threads, ownership, Arrow data and types:

| Library | Threads | Lifetimes | Arrow | Typing |
| --- | --- | --- | --- | --- |
| Rust | Async: the application awaits each call on its own Tokio runtime | Rust ownership | `RecordBatch` exchange behind the optional `arrow` feature | Rust types |
| JVM | Blocking calls hold no Java monitor while they wait; several threads share a session, and another thread can trigger a call's `Cancel` | `AutoCloseable` handles with try-with-resources and a Cleaner fallback; a read-only direct buffer keeps its owner alive | Canonical Arrow IPC bytes and direct buffers, and bulk Java arrays; Arrow Java is optional | Java 21 classes, records and enums; unsigned 64-bit values keep their bits in a `long` |
| Python | Blocking calls release the GIL; any thread may call, and another thread can trigger a call's `Cancel` | Python owns every object; `with` blocks release sessions, endpoints and deliveries, and a `memoryview` keeps its owner alive | The Arrow PyCapsule interface exchanges batches and rows with pyarrow and polars | The wheel ships a typed stub and a `py.typed` marker for mypy and pyright |
| Ruby | Blocking calls release the GVL; a Ruby thread interrupt ends only its own call | The collector releases native objects; block forms and `ensure` close them, and a read-only `IO::Buffer` keeps its owner alive | Canonical Arrow IPC as binary strings or `IO::Buffer`, and typed or bulk column reads; no Arrow gem | Dynamic, without type signatures |
| C and C++ | Blocking calls from any thread except one driving a Tokio runtime; a field list or builder is used by one thread at a time | The caller releases every owned handle once with its matching function; C++ holds them in `std::unique_ptr` with custom deleters | Bulk typed column buffers and canonical Arrow IPC; no Arrow C Data interface | The declarations of `nervix_client.h`; strings are a pointer and a length |

Only the Rust library exposes the typed transaction status, transaction attach and inspection, the
typed domain list, structured choice lookups, server notices, the leadership view, upload progress
and upload identities of your choosing. Each other chapter's What The Library Does Not Expose
section says what that library leaves out.
