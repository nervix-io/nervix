//! The Nervix server: everything a node does once its inputs are Models.
//!
//! Layer: decisions, data plane, control plane and edges, in one crate. Each module below opens
//! with the contract for the layer it belongs to, and the crate split follows those names.
//!
//! - **Owns.** Validating and scheduling Models, executing the resulting graph on this node,
//!   coordinating the cluster, and serving the session, HTTP, cluster and metrics surfaces. It is
//!   the composition root, and the only crate that names every connector.
//! - **Depends on.** The vocabulary, the engines, and the language layer at its session edge alone.
//! - **Must not know.** Plexus syntax outside the session adapter, and the harnesses that drive it.
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

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "server composition defaults to coordination; modules declare their inner or \
                  edge boundary"
    )
)]
#![recursion_limit = "256"]

#[cfg(all(feature = "testing", not(debug_assertions)))]
compile_error!(
    "the `testing` feature enables test fault injection and must not be compiled with \
     release-like profiles"
);

/// The process allocator for the whole crate: jemalloc, wrapped in tests by `alloc_count`'s counter
/// so the allocation-counting tests of every module measure the same allocator.
#[cfg(not(test))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// The test build's counting wrapper around the same allocator.
#[cfg(test)]
#[global_allocator]
static GLOBAL: alloc_count::AllocCounter<tikv_jemallocator::Jemalloc> =
    alloc_count::AllocCounter(tikv_jemallocator::Jemalloc);

pub mod application;
#[cfg(test)]
mod archive_allocation_tests;
pub mod capacity;
#[cfg(test)]
mod captured_logs;
mod client_row;
pub mod cluster;
mod domain_clock_authority;
mod emitter_execution_plan;
mod emitter_start_plan;
#[cfg(feature = "testing")]
mod fault_injection;
mod inferencer_state_snapshot;
pub mod licensing;
pub mod memory_pressure;
pub mod metrics;
pub(crate) mod registry;
pub mod resource;
pub(crate) mod resource_interconnect;
pub mod runtime;
pub mod runtime_ack;
pub mod runtime_schema;
#[cfg(all(test, feature = "shuttle"))]
mod shuttle_selection_tests;
mod state_inspection_cursor;
#[doc(hidden)]
pub mod subscription_row;
pub(crate) mod task_shutdown;

#[cfg(feature = "testing")]
pub use fault_injection::{
    BackupCutBoundary, DomainResumeBoundary, FaultInjection, ForeignExecutionObservation,
    ModelPublicationRefusalKind, RelayCapabilityRefusal, RestoreStagingKind,
    StandaloneTransactionBoundary, StartPlacementBoundary, WasmCheckpointWindow,
    WasmStatePageOverflow, WasmStateResetRequestError,
};
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

#[cfg(not(feature = "testing"))]
impl ConfiguredFaultInjection {
    pub(crate) fn licensing_clock_changes(
        &self,
    ) -> Option<nervix_primitives::sync::watch::Receiver<std::time::Duration>> {
        None
    }

    pub(crate) const fn licensing_clock_offset(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }

    pub(crate) const fn gossip_send_delay(
        &self,
        _destination: &nervix_models::ClusterNodeName,
    ) -> Option<std::time::Duration> {
        None
    }

    /// A node built without the test harness always drains its admitted work when it shuts down.
    pub(crate) const fn runs_shutdown_drain(&self) -> bool {
        true
    }

    pub(crate) const fn overflows_wasm_state_page_reply(
        &self,
        _domain: &nervix_models::DomainName,
        _processor: &nervix_models::ModelName,
        _page: &nervix_models::WasmUnitStatePage,
    ) -> bool {
        false
    }
}

pub use application::licensing::ConfiguredLicensingTestSupport;
