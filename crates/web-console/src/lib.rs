//! The console's testable core. The browser entry point lives in `main.rs`; everything that can
//! be reasoned about without a DOM lives here.
//!
//! Layer: edges.
//!
//! - **Owns.** What the console draws and how it is placed: a graph snapshot becomes items, edges
//!   and branch groups, and geometry is computed here as a pure function of topology.
//! - **Depends on.** The proto wire types, the dataflow-graph description, the language layer for
//!   editor completion, and the vocabulary.
//! - **Must not know.** The server's internals. Every value it shows arrived over the session or
//!   cluster API.

pub mod graph;
