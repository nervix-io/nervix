//! The Nervix server: everything a node does once its inputs are Models.
//!
//! Layer: decisions, data plane, control plane and edges, in one crate. Each module below opens
//! with the contract for the layer it belongs to, and the crate split follows those names.
//!
//! - **Owns.** Validating and scheduling Models, executing the resulting graph on this node,
//!   coordinating the cluster, and serving the session, HTTP, cluster and metrics surfaces.
//! - **Depends on.** The vocabulary, the engines, and the language layer at its session edge alone.
//! - **Must not know.** NSPL syntax outside the session adapter, and the harnesses that drive it.
//!   Nothing sits above this crate, so its only boundaries are the ones its modules declare.
//!
//! The crate breaks its own contract by spanning four layers in one compilation unit. That is what
//! the split resolves, and until it happens the module headers are where the boundary is written
//! down.

#![recursion_limit = "256"]

#[cfg(all(feature = "testing", not(debug_assertions)))]
compile_error!(
    "the `testing` feature lowers Argon2 password hashing parameters and must not be compiled \
     with release-like profiles"
);

pub mod application;
pub mod cluster;
pub mod jaq_program;
pub mod memory_pressure;
pub mod metrics;
pub(crate) mod registry;
pub mod resource;
pub mod runtime;
pub mod runtime_ack;
pub mod runtime_schema;

pub use nervix_proto as proto;
#[cfg(feature = "testing")]
pub use registry::SchedulerMode;
