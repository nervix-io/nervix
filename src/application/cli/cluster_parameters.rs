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
    #[arg(long = nervix_models::PayloadUnfoldLimit::NAME, env = "NERVIX_PAYLOAD_UNFOLD_LIMIT", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::PayloadUnfoldLimit, "The most records a codec unfolds from one ingested payload"))]
    pub payload_unfold_limit: Option<u64>,
    #[arg(long = nervix_models::ForcedRecoveryBudget::NAME, env = "NERVIX_FORCED_RECOVERY_BUDGET", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::ForcedRecoveryBudget, "How long a forced ownership recovery gives its destination to prepare"))]
    pub forced_recovery_budget: Option<HumanDuration>,
    #[arg(long = nervix_models::OwnershipHandoffPollInterval::NAME, env = "NERVIX_OWNERSHIP_HANDOFF_POLL_INTERVAL", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::OwnershipHandoffPollInterval, "How often planned ownership handoffs are reconciled"))]
    pub ownership_handoff_poll_interval: Option<HumanDuration>,
    #[arg(long = nervix_models::EntityGatePollInterval::NAME, env = "NERVIX_ENTITY_GATE_POLL_INTERVAL", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::EntityGatePollInterval, "How often an entity gate observes the drain it waits for"))]
    pub entity_gate_poll_interval: Option<HumanDuration>,
    #[arg(long = nervix_models::DomainDrainPollInterval::NAME, env = "NERVIX_DOMAIN_DRAIN_POLL_INTERVAL", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::DomainDrainPollInterval, "How often a domain drain observes the work it waits for"))]
    pub domain_drain_poll_interval: Option<HumanDuration>,
    #[arg(long = nervix_models::RuntimeReadinessPropagationBound::NAME, env = "NERVIX_RUNTIME_READINESS_PROPAGATION_BOUND", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::RuntimeReadinessPropagationBound, "How long a live node has to report an applied revision ready"))]
    pub runtime_readiness_propagation_bound: Option<HumanDuration>,
    #[arg(long = nervix_models::TlsCipherSuites::NAME, env = "NERVIX_TLS_CIPHER_SUITES", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::TlsCipherSuites, "The cipher suites of outward TLS connections, in order, as comma-separated algorithm names or aliases"))]
    pub tls_cipher_suites: Option<nervix_crypto_policy::TlsCipherSuites>,
    #[arg(long = nervix_models::TlsKeyExchangeGroups::NAME, env = "NERVIX_TLS_KEY_EXCHANGE_GROUPS", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::TlsKeyExchangeGroups, "The key-exchange groups of outward TLS connections, in order, as comma-separated algorithm names or aliases"))]
    pub tls_key_exchange_groups: Option<nervix_crypto_policy::TlsKeyExchangeGroups>,
    #[arg(long = nervix_models::PasswordIterations::NAME, env = "NERVIX_PASSWORD_ITERATIONS", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::PasswordIterations, "The PBKDF2 iterations of newly created user credentials"))]
    pub password_iterations: Option<u64>,
    #[arg(long = nervix_models::LicensingRequestTimeout::NAME, env = "NERVIX_LICENSING_REQUEST_TIMEOUT", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::LicensingRequestTimeout, "One licensing request, including resolution, TLS and the response body"))]
    pub licensing_request_timeout: Option<HumanDuration>,
    #[arg(long = nervix_models::LicensingRetryBackoffMax::NAME, env = "NERVIX_LICENSING_RETRY_BACKOFF_MAX", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::LicensingRetryBackoffMax, "The ceiling of the exponential licensing retry"))]
    pub licensing_retry_backoff_max: Option<HumanDuration>,
    #[arg(long = nervix_models::LicensingStreamIdleTimeout::NAME, env = "NERVIX_LICENSING_STREAM_IDLE_TIMEOUT", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::LicensingStreamIdleTimeout, "How long the licensing notification stream may stay idle"))]
    pub licensing_stream_idle_timeout: Option<HumanDuration>,
    #[arg(long = nervix_models::DomainDrainTimeout::NAME, env = "NERVIX_DOMAIN_DRAIN_TIMEOUT", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::DomainDrainTimeout, "How long a domain drain waits for the domain's admitted work"))]
    pub domain_drain_timeout: Option<HumanDuration>,
    #[arg(long = nervix_models::EntityGateDeadline::NAME, env = "NERVIX_ENTITY_GATE_DEADLINE", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::EntityGateDeadline, "How long a coordinated entity gate may take to engage and drain"))]
    pub entity_gate_deadline: Option<HumanDuration>,
    #[arg(long = nervix_models::RemoteAckAliveInterval::NAME, env = "NERVIX_REMOTE_ACK_ALIVE_INTERVAL", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::RemoteAckAliveInterval, "How often a receiver reports on a remote delivery it retains, and held local work reports the acknowledgements it holds alive"))]
    pub remote_ack_alive_interval: Option<HumanDuration>,
    #[arg(long = nervix_models::RemoteAckSweepInterval::NAME, env = "NERVIX_REMOTE_ACK_SWEEP_INTERVAL", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::RemoteAckSweepInterval, "How often a sender applies membership and the admission bound to the remote deliveries and acknowledgements it waits for"))]
    pub remote_ack_sweep_interval: Option<HumanDuration>,
    #[arg(long = nervix_models::RemoteRelayTotalTimeout::NAME, env = "NERVIX_REMOTE_RELAY_TOTAL_TIMEOUT", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::RemoteRelayTotalTimeout, "The whole budget of one remote admission"))]
    pub remote_relay_total_timeout: Option<HumanDuration>,

    #[arg(long = nervix_models::MaxBatchMessages::NAME, env = "NERVIX_MAX_BATCH_MESSAGES", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::MaxBatchMessages, "The most records one declared emitter batch may contain"))]
    pub max_batch_messages: Option<u64>,
    #[arg(long = nervix_models::ClientProducerSessionBytes::NAME, env = "NERVIX_CLIENT_PRODUCER_SESSION_BYTES", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::ClientProducerSessionBytes, "The outstanding producer payload bytes one session may reserve"))]
    pub client_producer_session_bytes: Option<HumanBytes>,
    #[arg(long = nervix_models::ClientConsumerSessionBytes::NAME, env = "NERVIX_CLIENT_CONSUMER_SESSION_BYTES", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::ClientConsumerSessionBytes, "The outstanding consumer payload bytes one session may reserve"))]
    pub client_consumer_session_bytes: Option<HumanBytes>,
    #[arg(long = nervix_models::UdfCodeBytes::NAME, env = "NERVIX_UDF_CODE_BYTES", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::UdfCodeBytes, "The most source bytes one Roto function may declare"))]
    pub udf_code_bytes: Option<HumanBytes>,
    #[arg(long = nervix_models::UdfCompileBudget::NAME, env = "NERVIX_UDF_COMPILE_BUDGET", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::UdfCompileBudget, "How long compiling one Roto UDF and running its test blocks may take on the node that compiles it"))]
    pub udf_compile_budget: Option<HumanDuration>,
    #[arg(long = nervix_models::EntityLabelsMaxCount::NAME, env = "NERVIX_ENTITY_LABELS_MAX_COUNT", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::EntityLabelsMaxCount, "The most labels one entity may carry"))]
    pub entity_labels_max_count: Option<u64>,
    #[arg(long = nervix_models::EntityLabelsMaxKeyBytes::NAME, env = "NERVIX_ENTITY_LABELS_MAX_KEY_BYTES", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::EntityLabelsMaxKeyBytes, "The most UTF-8 bytes of one label key"))]
    pub entity_labels_max_key_bytes: Option<HumanBytes>,
    #[arg(long = nervix_models::EntityLabelsMaxValueBytes::NAME, env = "NERVIX_ENTITY_LABELS_MAX_VALUE_BYTES", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::EntityLabelsMaxValueBytes, "The most UTF-8 bytes of one label value"))]
    pub entity_labels_max_value_bytes: Option<HumanBytes>,
    #[arg(long = nervix_models::EntityLabelsMaxBytes::NAME, env = "NERVIX_ENTITY_LABELS_MAX_BYTES", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::EntityLabelsMaxBytes, "The most combined key and value bytes of one entity's labels"))]
    pub entity_labels_max_bytes: Option<HumanBytes>,
    #[arg(long = nervix_models::HttpHeaderBytes::NAME, env = "NERVIX_HTTP_HEADER_BYTES", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::HttpHeaderBytes, "The most application header bytes one HTTP request may carry"))]
    pub http_header_bytes: Option<HumanBytes>,
    #[arg(long = nervix_models::HttpHeaderFields::NAME, env = "NERVIX_HTTP_HEADER_FIELDS", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::HttpHeaderFields, "The most distinct application header fields one HTTP request may carry"))]
    pub http_header_fields: Option<u64>,
    #[arg(long = nervix_models::ClientBatchRows::NAME, env = "NERVIX_CLIENT_BATCH_ROWS", help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::ClientBatchRows, "The most rows one client payload batch may contain"))]
    pub client_batch_rows: Option<u64>,
}
impl ClusterParameterArgs {
    pub(super) fn from_seeds(seeds: &nervix_models::ClusterParameterSeedValues) -> Self {
        Self {
            payload_unfold_limit: seeds.payload_unfold_limit,
            remote_ack_alive_interval: seeds.remote_ack_alive_interval.map(HumanDuration),
            remote_ack_sweep_interval: seeds.remote_ack_sweep_interval.map(HumanDuration),
            remote_relay_total_timeout: seeds.remote_relay_total_timeout.map(HumanDuration),
            forced_recovery_budget: seeds.forced_recovery_budget.map(HumanDuration),
            ownership_handoff_poll_interval: seeds
                .ownership_handoff_poll_interval
                .map(HumanDuration),
            entity_gate_poll_interval: seeds.entity_gate_poll_interval.map(HumanDuration),
            domain_drain_poll_interval: seeds.domain_drain_poll_interval.map(HumanDuration),
            runtime_readiness_propagation_bound: seeds
                .runtime_readiness_propagation_bound
                .map(HumanDuration),
            tls_cipher_suites: seeds.tls_cipher_suites,
            tls_key_exchange_groups: seeds.tls_key_exchange_groups,
            password_iterations: seeds.password_iterations,
            licensing_request_timeout: seeds.licensing_request_timeout.map(HumanDuration),
            licensing_retry_backoff_max: seeds.licensing_retry_backoff_max.map(HumanDuration),
            licensing_stream_idle_timeout: seeds.licensing_stream_idle_timeout.map(HumanDuration),
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
            udf_compile_budget: seeds.udf_compile_budget.map(HumanDuration),
            entity_labels_max_count: seeds.entity_labels_max_count,
            entity_labels_max_key_bytes: seeds
                .entity_labels_max_key_bytes
                .map(|bytes| HumanBytes(ubyte::ByteUnit::Byte(bytes))),
            entity_labels_max_value_bytes: seeds
                .entity_labels_max_value_bytes
                .map(|bytes| HumanBytes(ubyte::ByteUnit::Byte(bytes))),
            entity_labels_max_bytes: seeds
                .entity_labels_max_bytes
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
            nervix_models::ForcedRecoveryBudget::NAME,
            self.forced_recovery_budget,
        );
        command_line.optional(
            nervix_models::OwnershipHandoffPollInterval::NAME,
            self.ownership_handoff_poll_interval,
        );
        command_line.optional(
            nervix_models::EntityGatePollInterval::NAME,
            self.entity_gate_poll_interval,
        );
        command_line.optional(
            nervix_models::DomainDrainPollInterval::NAME,
            self.domain_drain_poll_interval,
        );
        command_line.optional(
            nervix_models::RuntimeReadinessPropagationBound::NAME,
            self.runtime_readiness_propagation_bound,
        );
        command_line.optional(
            nervix_models::LicensingRequestTimeout::NAME,
            self.licensing_request_timeout,
        );
        command_line.optional(
            nervix_models::LicensingRetryBackoffMax::NAME,
            self.licensing_retry_backoff_max,
        );
        command_line.optional(
            nervix_models::LicensingStreamIdleTimeout::NAME,
            self.licensing_stream_idle_timeout,
        );
        command_line.optional(
            nervix_models::PayloadUnfoldLimit::NAME,
            self.payload_unfold_limit,
        );
        command_line.optional(
            nervix_models::RemoteAckAliveInterval::NAME,
            self.remote_ack_alive_interval,
        );
        command_line.optional(
            nervix_models::RemoteAckSweepInterval::NAME,
            self.remote_ack_sweep_interval,
        );
        command_line.optional(
            nervix_models::RemoteRelayTotalTimeout::NAME,
            self.remote_relay_total_timeout,
        );

        command_line.optional(nervix_models::TlsCipherSuites::NAME, self.tls_cipher_suites);
        command_line.optional(
            nervix_models::TlsKeyExchangeGroups::NAME,
            self.tls_key_exchange_groups,
        );
        command_line.optional(
            nervix_models::PasswordIterations::NAME,
            self.password_iterations,
        );
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
        command_line.optional(
            nervix_models::UdfCompileBudget::NAME,
            self.udf_compile_budget,
        );
        command_line.optional(
            nervix_models::EntityLabelsMaxCount::NAME,
            self.entity_labels_max_count,
        );
        command_line.optional(
            nervix_models::EntityLabelsMaxKeyBytes::NAME,
            self.entity_labels_max_key_bytes,
        );
        command_line.optional(
            nervix_models::EntityLabelsMaxValueBytes::NAME,
            self.entity_labels_max_value_bytes,
        );
        command_line.optional(
            nervix_models::EntityLabelsMaxBytes::NAME,
            self.entity_labels_max_bytes,
        );
        command_line.optional(nervix_models::HttpHeaderBytes::NAME, self.http_header_bytes);
        command_line.optional(nervix_models::ClientBatchRows::NAME, self.client_batch_rows);
        command_line.optional(
            nervix_models::HttpHeaderFields::NAME,
            self.http_header_fields,
        );
    }
}
