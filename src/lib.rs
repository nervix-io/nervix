//! The Nervix server: everything a node does once its inputs are Models.
//!
//! Layer: decisions, data plane, control plane and edges, in one crate. Each module below opens
//! with the contract for the layer it belongs to, and the crate split follows those names.
//!
//! - **Owns.** Validating and scheduling Models, executing the resulting graph on this node,
//!   coordinating the cluster, and serving the session, HTTP, cluster and metrics surfaces. It is
//!   the composition root, and the only crate that names every connector.
//! - **Depends on.** The vocabulary, the engines, and the language layer at its session edge alone.
//! - **Must not know.** NSPL syntax outside the session adapter, and the harnesses that drive it.
//!   Nothing sits above this crate, so its only boundaries are the ones its modules declare.
//!
//! Connectors follow one rule. A connector crate is an engine driven by this crate's data plane,
//! which hands it a typed plan the decision layer converted from Models, so no connector reads a
//! Model. The capabilities the registry validates live in the vocabulary, because the registry
//! names no connector crate.
//!
//! The crate breaks its own contract twice. It spans four layers in one compilation unit, and its
//! data plane still holds every connector although a connector is an engine. The split resolves
//! both: the layers separate along the module headers, and each external integration moves into
//! its own crate under `crates/connectors/` behind the `nervix-connector` contract, leaving this
//! crate to compose them. Until then the module headers are where the boundary is written down.

#![recursion_limit = "256"]

#[cfg(feature = "shuttle")]
extern crate shuttle_dashmap as dashmap;
#[cfg(feature = "shuttle")]
extern crate shuttle_parking_lot as parking_lot;
#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;
#[cfg(feature = "shuttle")]
extern crate shuttle_tokio_stream as tokio_stream;
#[cfg(feature = "shuttle")]
extern crate shuttle_tokio_util as tokio_util;
#[cfg(feature = "shuttle")]
extern crate tokio as tokio_real;

#[cfg(all(feature = "testing", not(debug_assertions)))]
compile_error!(
    "the `testing` feature lowers Argon2 password hashing parameters and must not be compiled \
     with release-like profiles"
);

pub mod application;
pub mod cluster;
mod domain_clock_authority;
#[cfg(feature = "testing")]
mod fault_injection;
pub mod jaq_program;
pub mod memory_pressure;
pub mod metrics;
pub(crate) mod registry;
pub mod resource;
pub(crate) mod resource_interconnect;
pub mod runtime;
pub mod runtime_ack;
pub mod runtime_schema;
#[cfg(all(test, feature = "shuttle"))]
mod shuttle_test;
#[doc(hidden)]
pub mod subscription_row;
pub(crate) mod task_shutdown;

#[cfg(feature = "testing")]
pub use fault_injection::FaultInjection;
pub use nervix_proto as proto;
#[cfg(feature = "testing")]
pub use registry::SchedulerMode;

/// The value stored in product structs at the injection boundary.
///
/// Test builds resolve this alias to the real shared injector. Normal builds resolve it to a
/// zero-sized marker and do not compile the injector module.
#[cfg(feature = "testing")]
#[doc(hidden)]
pub type ConfiguredFaultInjection = fault_injection::FaultInjection;

#[cfg(not(feature = "testing"))]
#[derive(Clone, Debug, Default)]
#[doc(hidden)]
pub struct ConfiguredFaultInjection {
    _marker: (),
}
