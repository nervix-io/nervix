//! Node lifecycle setting definitions, owned by the shared vocabulary.
//!
//! Layer: control plane.
//! - **Owns.** The control plane's import of the node lifecycle configuration.
//! - **Depends on.** Vocabulary setting definitions.
//! - **Must not know.** Command-line parsing or shutdown task execution.

pub use nervix_models::{DrainTimeout, ShutdownSettings, ShutdownSettingsError, ShutdownTimeout};
