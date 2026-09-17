//! The contract between the Nervix host and the connector crates it drives.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The source and sink contracts, the host handles a connector may call, and the value
//!   types that cross the boundary.
//! - **Depends on.** The vocabulary, Arrow, `error-stack` and Tokio.
//! - **Must not know.** Relays, branches, schedules, Models, the registry or the runtime. A
//!   connector receives a typed plan and host handles, and reaches nothing past them.
//!
//! Every external integration belongs in its own crate under `crates/connectors/`, which implements
//! the source contract, the sink contract, or both. A connector crate is an engine: the host drives
//! it, and it decides nothing about the graph. It names this crate, the vocabulary, Arrow,
//! `error-stack`, Tokio and its own driver, and never the server or another connector crate. The
//! host keeps task lifecycle, branch routing, acknowledgement tracking, quiesce, retry and flush
//! cadence, buffering, metrics and events.
//!
//! The server is the composition root and the only crate that names every connector. It implements
//! the host handles and converts Models into the typed plan each connector receives, so no
//! connector reads a Model. Capabilities the registry validates live in the vocabulary, where
//! validation reads them without naming a connector crate.
