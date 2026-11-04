//! Valid cluster policy values across their complete declared scalar ranges.
//!
//! Outside the layer order: test harness.
//! - **Owns.** Bounded current parameter generation from deterministic byte inputs.
//! - **Depends on.** Vocabulary setting definitions and the shared entropy cursor.
//! - **Must not know.** Parsing, consensus storage or runtime installation.

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{ClusterParameterSeedValues, ClusterParameterValues};

use super::{Arbitrary, Entropy};

impl Arbitrary<'_> {
    pub fn cluster_parameter_record(&mut self) -> nervix_models::ClusterParameterRecord {
        let revision = std::num::NonZeroU64::new(self.entropy.any_u64().max(1))
            .assured("generated revisions are positive");
        nervix_models::ClusterParameterRecord {
            revision,
            values: self.cluster_parameter_values(),
        }
    }

    pub fn cluster_parameter_values(&mut self) -> ClusterParameterValues {
        let entropy = &mut self.entropy;
        ClusterParameterValues::resolve(ClusterParameterSeedValues {
            node_unavailability_timeout: Some(parameter_duration(entropy)),
            domain_drain_timeout: Some(parameter_duration(entropy)),
            entity_gate_deadline: Some(parameter_duration(entropy)),
            transaction_idle_timeout: Some(parameter_duration(entropy)),
            transaction_tombstone_retention: Some(parameter_duration(entropy)),
            command_retry_validity: Some(parameter_duration(entropy)),
            state_snapshot_interval: Some(parameter_duration(entropy)),
            transaction_max_statements: Some(entropy.boundary_biased(0..=u64::from(u32::MAX))),
            transaction_max_source_bytes: Some(entropy.boundary_biased(0..=u64::MAX)),
            transaction_max_open: Some(entropy.boundary_biased(0..=u64::from(u32::MAX))),
            command_execution_capacity: Some(entropy.boundary_biased(1..=u64::from(u32::MAX))),
            replica_count: Some(entropy.boundary_biased(0..=u64::from(u32::MAX))),
            resource_max_archive_bytes: Some(entropy.boundary_biased(1..=u64::MAX)),
            resource_max_extracted_bytes: Some(entropy.boundary_biased(1..=u64::MAX)),
            resource_max_file_count: Some(entropy.boundary_biased(1..=u64::MAX)),
            max_batch_messages: Some(entropy.boundary_biased(1..=u64::from(u32::MAX))),
            client_producer_session_bytes: Some(entropy.boundary_biased(1..=u64::MAX)),
            client_consumer_session_bytes: Some(entropy.boundary_biased(1..=u64::MAX)),
            udf_code_bytes: Some(entropy.boundary_biased(1..=u64::MAX)),
            http_header_bytes: Some(entropy.boundary_biased(1..=u64::MAX)),
            http_header_fields: Some(
                entropy.boundary_biased(1..=nervix_models::HttpHeaderFields::MAXIMUM),
            ),
            client_batch_rows: Some(entropy.boundary_biased(1..=u64::from(u32::MAX))),
        })
        .assured("the generator constructs every parameter within its declared current bounds")
    }
}

fn parameter_duration(entropy: &mut Entropy<'_>) -> Duration {
    let seconds = entropy.boundary_biased(0..=u64::from(u32::MAX));
    let nanos = if seconds == u64::from(u32::MAX) {
        0
    } else {
        entropy.boundary_biased(0..=999_999_999)
    };
    let nanos = if seconds == 0 { nanos.max(1) } else { nanos };
    Duration::new(
        seconds,
        u32::try_from(nanos).assured("the nanosecond fraction is below one second"),
    )
}
