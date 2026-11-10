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
    pub fn password_credential(&mut self) -> nervix_models::PasswordCredential {
        let iterations = self
            .entropy
            .boundary_biased(1..=nervix_models::PasswordIterations::MAXIMUM);
        let salt = std::array::from_fn(|_| self.entropy.byte());
        let digest = std::array::from_fn(|_| self.entropy.byte());
        nervix_models::PasswordCredential::from_parts(iterations, salt, digest)
            .assured("generated password costs are within the declared bound")
    }

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
        let suite_names: &[&str] = entropy.pick([
            &["fips"][..],
            &["default"][..],
            &["TLS13_CHACHA20_POLY1305_SHA256", "TLS13_AES_128_GCM_SHA256"][..],
            &["TLS13_AES_128_GCM_SHA256", "fips"][..],
        ]);
        let group_names: &[&str] = entropy.pick([
            &["fips"][..],
            &["default"][..],
            &["X25519", "fips"][..],
            &["X25519MLKEM768", "secp256r1"][..],
        ]);
        let tls_cipher_suites =
            nervix_models::TlsCipherSuites::parse_names(suite_names.iter().copied())
                .assured("the declared selection includes a TLS 1.3 suite");
        let tls_key_exchange_groups =
            nervix_models::TlsKeyExchangeGroups::parse_names(group_names.iter().copied())
                .assured("the declared selection includes a TLS 1.2-capable group");
        ClusterParameterValues::resolve(ClusterParameterSeedValues {
            password_iterations: Some(
                entropy.boundary_biased(1..=nervix_models::PasswordIterations::MAXIMUM),
            ),
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
            entity_labels_max_count: Some(entropy.boundary_biased(0..=u64::MAX)),
            entity_labels_max_key_bytes: Some(entropy.boundary_biased(0..=u64::MAX)),
            entity_labels_max_value_bytes: Some(entropy.boundary_biased(0..=u64::MAX)),
            entity_labels_max_bytes: Some(entropy.boundary_biased(0..=u64::MAX)),
            http_header_bytes: Some(entropy.boundary_biased(1..=u64::MAX)),
            http_header_fields: Some(
                entropy.boundary_biased(1..=nervix_models::HttpHeaderFields::MAXIMUM),
            ),
            client_batch_rows: Some(entropy.boundary_biased(1..=u64::from(u32::MAX))),
            tls_cipher_suites: Some(tls_cipher_suites),
            tls_key_exchange_groups: Some(tls_key_exchange_groups),
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
