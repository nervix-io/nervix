//! What must hold before a domain's Models are allowed to become a graph.
//!
//! Layer: decisions.
//!
//! - **Owns.** Every rule a Model must satisfy: its schemas and their sensitivity, the wire forms
//!   it decodes and encodes, the branch it reads and writes, what each processor may produce, the
//!   external contract of each connector, the materialized state it depends on, and where its
//!   errors go.
//! - **Depends on.** The vocabulary, the VM's type system, and the graph being built.
//! - **Must not know.** How a validated Model is stored, placed or executed.

pub(in crate::registry) mod branching;
pub(in crate::registry) mod connector;
pub(in crate::registry) mod expression;
pub(in crate::registry) mod materialized_state;
pub(in crate::registry) mod message_error;
pub(in crate::registry) mod processor;
pub(in crate::registry) mod schema;
pub(in crate::registry) mod vm;
pub(in crate::registry) mod wire;
