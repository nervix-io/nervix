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
//! | Shared ownership: Nervix-owned references, and the standard strong and weak references an external API or a weak reference requires | [`sync::Arc`], [`sync::StdArc`], [`sync::StdWeak`] | Portable |
//! | Async synchronization: locks, notification, waker registration, semaphores, channels, cancellation | [`sync`] | `native` |
//! | Thread-blocking synchronization: locks, condition variables, barriers, one-time initialization, channels | `sync::blocking` | `native` |
//! | Tasks: spawning, joining, yielding, aborting, tracking, cooperative budgeting, and the mechanism that runs an admitted CPU job | `task` | `native` |
//! | Timers and the monotonic clock: sleeps, deadlines, timeouts, intervals and instants | `time` | `native` |
//! | Sockets: TCP listeners and streams, UDP and local sockets | `net` | `native` |
//! | The async runtime, and the attributes and macro that build and drive it | `runtime`, `test`, `main`, `select!` | `native` |
//! | Streams over channels | `stream` | `native` |
//! | Atomic reference publication | `publication` | `native` |
//! | Concurrent maps and queues | `collections` | `native` |
//! | Operating-system threads and thread-local storage | `thread`, `thread_local!` | `native` |
//! | Real primitives outside every model | [`unmodeled`] | As the family |
//!
//! Loom models isolated synchronous owners: atomics, the threads a model spawns, joins, parks and
//! yields, and thread-local storage. It models no async family, lock, collection, publication,
//! timer or socket, so a Loom build takes the ordinary libraries for those, outside every model,
//! and the boundary check rejects them in Loom model code: a model never reaches a real primitive
//! silently.
//!
//! Turmoil replaces the network and the clock, not synchronization. A Turmoil build takes the
//! ordinary primitives, running on the simulated host whose task uses them; its sockets and its
//! name lookup are Turmoil's, its timers follow the simulated host's clock, and an admitted CPU job
//! runs as one task of that host's scheduler.
//!
//! Ordinary execution pays nothing for the boundary: every path is a direct re-export of the
//! library item, with no wrapper, allocation, dispatch or scheduling point. A modeled primitive
//! exists only inside a run of its model, and using one outside that run is a test configuration
//! failure that the backend reports; there is no fallback to a real primitive. A real primitive
//! that must stay outside every model is reached through [`unmodeled`] and nowhere else. A build
//! that selects a mode is therefore a test artifact, and every binary Nervix ships declares itself
//! with [`product_binary!`], which fails to compile in such a build.
//!
//! Shared ownership is real in every mode: no model checker counts references, so a reference count
//! establishes no ordering a check claims.
//!
//! The portable surface, [`sync::atomic`], shared ownership, and the unmodeled atomics, one-time
//! initialization and `futures` families, builds for every target, including the browser.
//! Everything else is the `native` capability, and requesting a capability or a mode the target
//! cannot provide is a compile error rather than a different implementation.
//!
//! Layer: primitives.
//!
//! - **Owns.** Selecting the backend of every governed primitive for the build's execution mode,
//!   rejecting incompatible modes and target capabilities, and the one named path to a real
//!   primitive that stays outside every model.
//! - **Depends on.** The standard library, the synchronization, runtime and network libraries it
//!   selects from, and the Shuttle or Loom runtime or Turmoil's network while that mode is
//!   selected.
//! - **Must not know.** Anything in Nervix, and any scenario, exploration bound or assertion of a
//!   check. It selects a backend; the harness that runs a model owns how the model is explored.
//!   It supplies mechanisms and decides no policy: which resolver answers a name, whether work is
//!   admitted, and which clock a deadline is measured on stay with the resolver, the bounded
//!   executor and the clock owners.

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

/// Declare the calling crate a product binary, which builds only for ordinary execution.
///
/// A build that selects an execution mode is a test artifact: its modeled primitives exist only
/// inside a run of their model, so a server or client built that way could not run as a product and
/// must never be published. Every binary Nervix ships invokes this once in its crate root with its
/// name, and a build that selects a mode fails to compile there, naming the binary and the mode.
#[cfg(not(any(feature = "loom", feature = "shuttle", feature = "turmoil")))]
#[macro_export]
macro_rules! product_binary {
    ($binary:literal) => {};
}

#[cfg(feature = "loom")]
#[macro_export]
macro_rules! product_binary {
    ($binary:literal) => {
        ::core::compile_error!(::core::concat!(
            $binary,
            " is a product binary and builds only for ordinary execution; a build that selects \
             the `loom` execution mode is a test artifact"
        ));
    };
}

#[cfg(all(feature = "shuttle", not(feature = "loom")))]
#[macro_export]
macro_rules! product_binary {
    ($binary:literal) => {
        ::core::compile_error!(::core::concat!(
            $binary,
            " is a product binary and builds only for ordinary execution; a build that selects \
             the `shuttle` execution mode is a test artifact"
        ));
    };
}

#[cfg(all(feature = "turmoil", not(any(feature = "loom", feature = "shuttle"))))]
#[macro_export]
macro_rules! product_binary {
    ($binary:literal) => {
        ::core::compile_error!(::core::concat!(
            $binary,
            " is a product binary and builds only for ordinary execution; a build that selects \
             the `turmoil` execution mode is a test artifact"
        ));
    };
}

// The runtime attributes name the boundary by its crate name, including in this crate's own tests.
#[cfg(feature = "native")]
extern crate self as nervix_primitives;

#[cfg(feature = "native")]
pub mod collections;
#[cfg(feature = "native")]
pub mod net;
#[cfg(feature = "native")]
pub mod publication;
#[cfg(feature = "native")]
pub mod runtime;
#[cfg(all(feature = "native", feature = "shuttle", not(feature = "loom")))]
mod scheduling;
#[cfg(feature = "native")]
pub mod stream;
pub mod sync;
#[cfg(feature = "native")]
pub mod task;
#[cfg(feature = "native")]
pub mod thread;
#[cfg(feature = "native")]
pub mod time;
pub mod unmodeled;

#[cfg(all(feature = "native", not(any(feature = "loom", feature = "shuttle"))))]
pub use std::thread_local;

/// Run an async `main` on the runtime of the build's execution mode.
#[cfg(feature = "native")]
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
#[cfg(feature = "native")]
pub use nervix_primitives_macros::test;
#[cfg(all(feature = "native", feature = "shuttle", not(feature = "loom")))]
pub use shuttle::thread_local;
#[cfg(all(feature = "native", feature = "shuttle", not(feature = "loom")))]
pub use shuttle_tokio::select;
#[cfg(all(feature = "native", not(feature = "shuttle")))]
pub use tokio::select;

/// What the boundary's attributes and macros expand to. Not a path for any other code.
///
/// The runtime attributes are Tokio's in every mode, with the items their expansion names through
/// its crate path, which are the selected runtime's: Tokio's attributes build their runtime only
/// through the crate path they are given. Shuttle's own test attribute instead finds its runtime by
/// reading the calling package's manifest for a dependency of a fixed name, so it is not used. A
/// Loom build's thread-local macro forwards to Loom's.
/// Declare thread-local storage for the build's execution mode: Loom's in a Loom build, where each of
/// a model's threads sees its own value.
///
/// Loom's own macro takes no `const` initializer, so this one accepts the standard library's form
/// and initializes each thread's value from the same expression.
#[cfg(all(feature = "native", feature = "loom"))]
#[macro_export]
macro_rules! thread_local {
    () => {};
    ($(#[$attr:meta])* $vis:vis static $name:ident: $t:ty = const { $init:expr }; $($rest:tt)*) => (
        $crate::__private::loom_thread_local!($(#[$attr])* $vis static $name: $t = $init);
        $crate::thread_local!($($rest)*);
    );
    ($(#[$attr:meta])* $vis:vis static $name:ident: $t:ty = const { $init:expr }) => (
        $crate::__private::loom_thread_local!($(#[$attr])* $vis static $name: $t = $init);
    );
    ($(#[$attr:meta])* $vis:vis static $name:ident: $t:ty = $init:expr; $($rest:tt)*) => (
        $crate::__private::loom_thread_local!($(#[$attr])* $vis static $name: $t = $init);
        $crate::thread_local!($($rest)*);
    );
    ($(#[$attr:meta])* $vis:vis static $name:ident: $t:ty = $init:expr) => (
        $crate::__private::loom_thread_local!($(#[$attr])* $vis static $name: $t = $init);
    );
}

#[cfg(feature = "native")]
#[doc(hidden)]
pub mod __private {
    #[cfg(feature = "loom")]
    pub use loom::thread_local as loom_thread_local;
    #[cfg(feature = "shuttle")]
    pub use shuttle_tokio::{pin, runtime};
    pub use tokio::{main, test};
    #[cfg(not(feature = "shuttle"))]
    pub use tokio::{pin, runtime};
}

#[cfg(test)]
mod tests;
