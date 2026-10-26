//! Bootstrap-only options for cluster policy without a local settings facade.
//!
//! Layer: edges.
//! - **Owns.** Reading optional first-bootstrap seeds and rendering them as node options.
//! - **Depends on.** Vocabulary parameter definitions and CLI scalar forms.
//! - **Must not know.** Consensus, membership or runtime installation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns bootstrap option conversion")
)]
use nervix_models::SettingDefinition as _;

use super::settings::CommandLine;
use crate::application::cli_values::{HumanBytes, HumanDuration};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "First-bootstrap cluster parameters")]
pub struct ClusterParameterArgs {
    #[arg(long = nervix_models::DomainDrainTimeout::NAME, env = "NERVIX_DOMAIN_DRAIN_TIMEOUT", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::DomainDrainTimeout, "First-bootstrap seed; change a running cluster with ALTER CLUSTER SET"))]
    pub domain_drain_timeout: Option<HumanDuration>,
    #[arg(long = nervix_models::EntityGateDeadline::NAME, env = "NERVIX_ENTITY_GATE_DEADLINE", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::EntityGateDeadline, "First-bootstrap seed; change a running cluster with ALTER CLUSTER SET"))]
    pub entity_gate_deadline: Option<HumanDuration>,
    #[arg(long = nervix_models::MaxBatchMessages::NAME, env = "NERVIX_MAX_BATCH_MESSAGES", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::MaxBatchMessages, "First-bootstrap seed; change a running cluster with ALTER CLUSTER SET"))]
    pub max_batch_messages: Option<u64>,
    #[arg(long = nervix_models::ClientProducerSessionBytes::NAME, env = "NERVIX_CLIENT_PRODUCER_SESSION_BYTES", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::ClientProducerSessionBytes, "First-bootstrap seed; change a running cluster with ALTER CLUSTER SET"))]
    pub client_producer_session_bytes: Option<HumanBytes>,
    #[arg(long = nervix_models::ClientConsumerSessionBytes::NAME, env = "NERVIX_CLIENT_CONSUMER_SESSION_BYTES", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::ClientConsumerSessionBytes, "First-bootstrap seed; change a running cluster with ALTER CLUSTER SET"))]
    pub client_consumer_session_bytes: Option<HumanBytes>,
    #[arg(long = nervix_models::UdfCodeBytes::NAME, env = "NERVIX_UDF_CODE_BYTES", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::UdfCodeBytes, "First-bootstrap seed; change a running cluster with ALTER CLUSTER SET"))]
    pub udf_code_bytes: Option<HumanBytes>,
    #[arg(long = nervix_models::HttpHeaderBytes::NAME, env = "NERVIX_HTTP_HEADER_BYTES", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::HttpHeaderBytes, "First-bootstrap seed; change a running cluster with ALTER CLUSTER SET"))]
    pub http_header_bytes: Option<HumanBytes>,
    #[arg(long = nervix_models::HttpHeaderFields::NAME, env = "NERVIX_HTTP_HEADER_FIELDS", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::HttpHeaderFields, "First-bootstrap seed; change a running cluster with ALTER CLUSTER SET"))]
    pub http_header_fields: Option<u64>,
    #[arg(long = nervix_models::ClientBatchRows::NAME, env = "NERVIX_CLIENT_BATCH_ROWS", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::ClientBatchRows, "First-bootstrap seed; change a running cluster with ALTER CLUSTER SET"))]
    pub client_batch_rows: Option<u64>,
}
impl ClusterParameterArgs {
    pub(super) fn from_seeds(seeds: &nervix_models::ClusterParameterSeedValues) -> Self {
        Self {
            domain_drain_timeout: seeds.domain_drain_timeout.map(HumanDuration),
            entity_gate_deadline: seeds.entity_gate_deadline.map(HumanDuration),
            max_batch_messages: seeds.max_batch_messages,
            client_producer_session_bytes: seeds
                .client_producer_session_bytes
                .map(|bytes| HumanBytes(ubyte::ByteUnit::Byte(bytes))),
            client_consumer_session_bytes: seeds
                .client_consumer_session_bytes
                .map(|bytes| HumanBytes(ubyte::ByteUnit::Byte(bytes))),
            udf_code_bytes: seeds
                .udf_code_bytes
                .map(|bytes| HumanBytes(ubyte::ByteUnit::Byte(bytes))),
            http_header_bytes: seeds
                .http_header_bytes
                .map(|bytes| HumanBytes(ubyte::ByteUnit::Byte(bytes))),
            client_batch_rows: seeds.client_batch_rows,
            http_header_fields: seeds.http_header_fields,
        }
    }
    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.optional(
            nervix_models::DomainDrainTimeout::NAME,
            self.domain_drain_timeout,
        );
        command_line.optional(
            nervix_models::EntityGateDeadline::NAME,
            self.entity_gate_deadline,
        );
        command_line.optional(
            nervix_models::MaxBatchMessages::NAME,
            self.max_batch_messages,
        );
        command_line.optional(
            nervix_models::ClientProducerSessionBytes::NAME,
            self.client_producer_session_bytes,
        );
        command_line.optional(
            nervix_models::ClientConsumerSessionBytes::NAME,
            self.client_consumer_session_bytes,
        );
        command_line.optional(nervix_models::UdfCodeBytes::NAME, self.udf_code_bytes);
        command_line.optional(nervix_models::HttpHeaderBytes::NAME, self.http_header_bytes);
        command_line.optional(nervix_models::ClientBatchRows::NAME, self.client_batch_rows);
        command_line.optional(
            nervix_models::HttpHeaderFields::NAME,
            self.http_header_fields,
        );
    }
}
