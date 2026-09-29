//! The execution-sensitive primitives Nervix code obtains through one boundary.
//!
//! A build runs in one execution mode. Ordinary execution is the default; `shuttle`, `loom` and
//! `turmoil` are the opt-in modes, and a dependency graph enables at most one of them. Every crate
//! that owns a mode feature forwards it to this crate, and Cargo unifies this crate's features
//! across the whole graph, so a vocabulary type, an engine and the server compiled into one test
//! binary all receive the same backend. Selection depends only on those features, never on
//! `cfg(test)`: the library a test links is production code compiled for the selected mode.
//!
//! | Family | Path | Capability |
//! | --- | --- | --- |
//! | Atomics, orderings and fences | [`sync::atomic`] | Portable |
//! | Async synchronization: locks, notification, semaphores, channels, cancellation | [`sync`] | `native` |
//! | Thread-blocking synchronization: locks, condition variables, barriers, one-time initialization, channels | `sync::blocking` | `native` |
//! | Tasks: spawning, joining, yielding, aborting, tracking, cooperative budgeting | `task` | `native` |
//! | The async runtime, and the attributes and macro that build and drive it | `runtime`, `test`, `main`, `select!` | `native` |
//! | Streams over channels | `stream` | `native` |
//! | Atomic reference publication | `publication` | `native` |
//! | Concurrent maps and queues | `collections` | `native` |
//! | Operating-system threads and thread-local storage | `thread`, `thread_local!` | `native` |
//! | Real primitives outside every model | [`unmodeled`] | As the family |
//!
//! Loom models isolated synchronous owners, so a Loom build provides atomics, threads and
//! thread-local storage, and no other native family: an async operation is unavailable there
//! rather than silently real.
//!
//! Ordinary execution pays nothing for the boundary: every path is a direct re-export of the
//! library item, with no wrapper, allocation, dispatch or scheduling point. A modeled primitive
//! exists only inside a run of its model, and using one outside that run is a test configuration
//! failure that the backend reports; there is no fallback to a real primitive. A real primitive
//! that must stay outside every model is reached through [`unmodeled`] and nowhere else.
//!
//! The portable surface, [`sync::atomic`] and the unmodeled atomics, builds for every target,
//! including the browser. Everything else is the `native` capability, and requesting a capability or
//! a mode the target cannot provide is a compile error rather than a different implementation.
//!
//! Layer: primitives.
//!
//! - **Owns.** Selecting the backend of every governed primitive for the build's execution mode,
//!   rejecting incompatible modes and target capabilities, and the one named path to a real
//!   primitive that stays outside every model.
//! - **Depends on.** The standard library, the synchronization and runtime libraries it selects
//!   from, and the Shuttle or Loom runtime while that mode is selected.
//! - **Must not know.** Anything in Nervix, and any scenario, exploration bound or assertion of a
//!   check. It selects a backend; the harness that runs a model owns how the model is explored.

// A Loom build has atomics, threads and thread-local storage, and no async family: naming one fails
// to compile rather than running a real primitive. `just test-primitives` runs these in that build.
#![cfg_attr(
    all(feature = "loom", feature = "native"),
    doc = r#"
In a Loom build the atomics, threads and thread-local storage are available:

```
use nervix_primitives::{sync::atomic::AtomicBool, thread::spawn};
```

and no other native family is, so a Loom build that names one fails to compile:

```compile_fail,E0432
use nervix_primitives::sync::Notify;
```

```compile_fail,E0432
use nervix_primitives::sync::blocking::Mutex;
```

```compile_fail,E0432
use nervix_primitives::task::spawn;
```

```compile_fail,E0432
use nervix_primitives::runtime::Runtime;
```

```compile_fail,E0432
use nervix_primitives::select;
```

```compile_fail,E0432
use nervix_primitives::collections::DashMap;
```

```compile_fail,E0432
use nervix_primitives::publication::ArcSwap;
```
"#
)]

#[cfg(all(feature = "loom", feature = "shuttle"))]
compile_error!(
    "nervix-primitives: the `loom` and `shuttle` execution modes cannot be enabled together. \
     Cargo unifies this crate's features across the dependency graph, so a graph selects one \
     mode; run each modeled suite in its own build invocation."
);
#[cfg(all(feature = "loom", feature = "turmoil"))]
compile_error!(
    "nervix-primitives: the `loom` and `turmoil` execution modes cannot be enabled together. \
     Cargo unifies this crate's features across the dependency graph, so a graph selects one \
     mode; run each modeled suite in its own build invocation."
);
#[cfg(all(feature = "shuttle", feature = "turmoil"))]
compile_error!(
    "nervix-primitives: the `shuttle` and `turmoil` execution modes cannot be enabled together. \
     Cargo unifies this crate's features across the dependency graph, so a graph selects one \
     mode; run each modeled suite in its own build invocation."
);
#[cfg(all(target_family = "wasm", feature = "native"))]
compile_error!(
    "nervix-primitives: the `native` capability provides operating-system threads and the async \
     runtime, which a wasm target does not have."
);
#[cfg(all(
    target_family = "wasm",
    any(feature = "loom", feature = "shuttle", feature = "turmoil")
))]
compile_error!(
    "nervix-primitives: the `loom`, `shuttle` and `turmoil` execution modes run on native targets \
     only."
);

// The runtime attributes name the boundary by its crate name, including in this crate's own tests.
#[cfg(feature = "native")]
extern crate self as nervix_primitives;

#[cfg(all(feature = "native", not(feature = "loom")))]
pub mod collections;
#[cfg(all(feature = "native", not(feature = "loom")))]
pub mod publication;
#[cfg(all(feature = "native", not(feature = "loom")))]
pub mod runtime;
#[cfg(all(feature = "native", feature = "shuttle", not(feature = "loom")))]
mod scheduling;
#[cfg(all(feature = "native", not(feature = "loom")))]
pub mod stream;
pub mod sync;
#[cfg(all(feature = "native", not(feature = "loom")))]
pub mod task;
#[cfg(feature = "native")]
pub mod thread;
pub mod unmodeled;

#[cfg(all(feature = "native", not(any(feature = "loom", feature = "shuttle"))))]
pub use std::thread_local;

#[cfg(all(feature = "native", feature = "loom"))]
pub use loom::thread_local;
/// Run an async `main` on the runtime of the build's execution mode.
#[cfg(all(feature = "native", not(feature = "loom")))]
pub use nervix_primitives_macros::main;
/// Run an async test on the runtime of the build's execution mode.
///
/// The arguments are Tokio's test attribute's, except `crate`: the execution mode decides the
/// runtime, so naming one fails to compile.
///
/// ```compile_fail
/// #[nervix_primitives::test(crate = "tokio")]
/// async fn picks_its_own_runtime() {}
/// ```
#[cfg(all(feature = "native", not(feature = "loom")))]
pub use nervix_primitives_macros::test;
#[cfg(all(feature = "native", feature = "shuttle", not(feature = "loom")))]
pub use shuttle::thread_local;
#[cfg(all(feature = "native", feature = "shuttle", not(feature = "loom")))]
pub use shuttle_tokio::select;
#[cfg(all(feature = "native", not(any(feature = "loom", feature = "shuttle"))))]
pub use tokio::select;

/// What the runtime attributes expand to: Tokio's attributes, and the items their expansion names
/// through its crate path, which are the selected runtime's. Not a path for any other code.
///
/// The attributes are Tokio's in every mode because they build their runtime only through the
/// crate path they are given. Shuttle's own test attribute instead finds its runtime by reading the
/// calling package's manifest for a dependency of a fixed name, so it is not used.
#[cfg(all(feature = "native", not(feature = "loom")))]
#[doc(hidden)]
pub mod __private {
    #[cfg(feature = "shuttle")]
    pub use shuttle_tokio::{pin, runtime};
    pub use tokio::{main, test};
    #[cfg(not(feature = "shuttle"))]
    pub use tokio::{pin, runtime};
}

#[cfg(test)]
mod tests;
