//! The execution-sensitive primitives Nervix code obtains through one boundary.
//!
//! A build runs in one execution mode. Ordinary execution is the default; `shuttle`, `loom` and
//! `turmoil` are the opt-in modes, and a dependency graph enables at most one of them. Every crate
//! that owns a mode feature forwards it to this crate, and Cargo unifies this crate's features
//! across the whole graph, so a vocabulary type, an engine and the server compiled into one test
//! binary all receive the same backend. Selection depends only on those features, never on
//! `cfg(test)`: the library a test links is production code compiled for the selected mode.
//!
//! | Mode | Atomics and fences | Threads |
//! | --- | --- | --- |
//! | Ordinary | The standard library's, re-exported unchanged | The operating system's |
//! | Shuttle | Shuttle's: each operation is a scheduling point, every ordering behaves as `SeqCst` | Shuttle's modeled threads |
//! | Loom | Loom's: each operation is explored under the C11 orderings Loom models | Loom's modeled threads |
//! | Turmoil | The standard library's, on the simulated host that runs the caller | The operating system's |
//!
//! Ordinary execution pays nothing for the boundary: every path is a direct re-export of the
//! standard library item, with no wrapper, allocation, dispatch or scheduling point. A modeled
//! primitive exists only inside a run of its model, and using one outside that run is a test
//! configuration failure that the backend reports; there is no fallback to a real primitive. A
//! real primitive that must stay outside every model is reached through [`unmodeled`] and nowhere
//! else.
//!
//! The portable surface, [`sync::atomic`] and [`unmodeled`], builds for every target, including
//! the browser. Operating-system threads are the `native` capability, and requesting a capability
//! or a mode the target cannot provide is a compile error rather than a different implementation.
//!
//! Layer: primitives.
//!
//! - **Owns.** Selecting the backend of every governed primitive for the build's execution mode,
//!   rejecting incompatible modes and target capabilities, and the one named path to a real
//!   primitive that stays outside every model.
//! - **Depends on.** The standard library, and the Shuttle or Loom runtime while that mode is
//!   selected.
//! - **Must not know.** Anything in Nervix, and any scenario, exploration bound or assertion of a
//!   check. It selects a backend; the harness that runs a model owns how the model is explored.

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
    "nervix-primitives: the `native` capability provides operating-system threads, which a wasm \
     target does not have."
);
#[cfg(all(
    target_family = "wasm",
    any(feature = "loom", feature = "shuttle", feature = "turmoil")
))]
compile_error!(
    "nervix-primitives: the `loom`, `shuttle` and `turmoil` execution modes run on native targets \
     only."
);

pub mod sync;
#[cfg(feature = "native")]
pub mod thread;
pub mod unmodeled;

#[cfg(test)]
mod tests;
