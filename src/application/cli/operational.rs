//! Optional operational capability arguments and their single startup conversion.
//!
//! Layer: edges.
//! - **Owns.** Parsing and rendering the node's control-plane, session and client-link options.
//! - **Depends on.** The vocabulary's capability definitions and CLI scalar forms.
//! - **Must not know.** Consensus, transports, graph execution or live settings mutation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns operational capability option conversion")
)]

use std::num::NonZeroU64;

use error_stack::Report;
use nervix_models::*;

use super::settings::CommandLine;
use crate::application::cli_values::{HumanBytes, HumanDuration};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Operational node capabilities")]
pub struct OperationalArgs {
    #[arg(long = MembershipReconcileInterval::NAME, env = "NERVIX_CONTROL_PLANE_MEMBERSHIP_INTERVAL", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().control_plane.membership_interval()).to_string(), MembershipReconcileInterval::DERIVATION))]
    pub control_plane_membership_interval: Option<HumanDuration>,
    #[arg(long = SchedulingReconcileInterval::NAME, env = "NERVIX_CONTROL_PLANE_SCHEDULING_INTERVAL", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().control_plane.scheduling_interval()).to_string(), SchedulingReconcileInterval::DERIVATION))]
    pub control_plane_scheduling_interval: Option<HumanDuration>,
    #[arg(long = ResourceReconcileInterval::NAME, env = "NERVIX_CONTROL_PLANE_RESOURCE_REPLICATION_INTERVAL", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().control_plane.resource_replication_interval()).to_string(), ResourceReconcileInterval::DERIVATION))]
    pub control_plane_resource_replication_interval: Option<HumanDuration>,
    #[arg(long = HealthProbeInterval::NAME, env = "NERVIX_CONTROL_PLANE_HEALTH_PROBE_INTERVAL", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().control_plane.health_probe_interval()).to_string(), HealthProbeInterval::DERIVATION))]
    pub control_plane_health_probe_interval: Option<HumanDuration>,
    #[arg(long = LeadershipSyncRetry::NAME, env = "NERVIX_CONTROL_PLANE_LEADERSHIP_SYNC_RETRY", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().control_plane.leadership_sync_retry()).to_string(), LeadershipSyncRetry::DERIVATION))]
    pub control_plane_leadership_sync_retry: Option<HumanDuration>,
    #[arg(long = TransactionSweepInterval::NAME, env = "NERVIX_CONTROL_PLANE_TRANSACTION_SWEEP_INTERVAL", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().control_plane.transaction_sweep_interval()).to_string(), TransactionSweepInterval::DERIVATION))]
    pub control_plane_transaction_sweep_interval: Option<HumanDuration>,
    #[arg(long = RestoreStagingSweepInterval::NAME, env = "NERVIX_CONTROL_PLANE_RESTORE_STAGING_SWEEP_INTERVAL", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().control_plane.restore_staging_sweep_interval()).to_string(), RestoreStagingSweepInterval::DERIVATION))]
    pub control_plane_restore_staging_sweep_interval: Option<HumanDuration>,
    #[arg(long = TransactionRecoveryConcurrency::NAME, env = "NERVIX_CONTROL_PLANE_TRANSACTION_RECOVERY_CONCURRENCY", help = capability_help(NodeOperationalCapabilities::default().control_plane.transaction_recovery_concurrency().to_string(), TransactionRecoveryConcurrency::DERIVATION))]
    pub control_plane_transaction_recovery_concurrency: Option<NonZeroU64>,
    #[arg(long = ResourceReplicationConcurrency::NAME, env = "NERVIX_CONTROL_PLANE_RESOURCE_REPLICATION_CONCURRENCY", help = capability_help(NodeOperationalCapabilities::default().control_plane.resource_replication_concurrency().to_string(), ResourceReplicationConcurrency::DERIVATION))]
    pub control_plane_resource_replication_concurrency: Option<NonZeroU64>,
    #[arg(long = EntityGateReleaseRetryInterval::NAME, env = "NERVIX_CONTROL_PLANE_ENTITY_GATE_RELEASE_RETRY_INTERVAL", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().control_plane.entity_gate_release_retry_interval()).to_string(), EntityGateReleaseRetryInterval::DERIVATION))]
    pub control_plane_entity_gate_release_retry_interval: Option<HumanDuration>,
    #[arg(long = EntityGateEngagementRetryInterval::NAME, env = "NERVIX_CONTROL_PLANE_ENTITY_GATE_ENGAGEMENT_RETRY_INTERVAL", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().control_plane.entity_gate_engagement_retry_interval()).to_string(), EntityGateEngagementRetryInterval::DERIVATION))]
    pub control_plane_entity_gate_engagement_retry_interval: Option<HumanDuration>,
    #[arg(long = TransactionApplicationRetryInterval::NAME, env = "NERVIX_CONTROL_PLANE_TRANSACTION_APPLICATION_RETRY_INTERVAL", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().control_plane.transaction_application_retry_interval()).to_string(), TransactionApplicationRetryInterval::DERIVATION))]
    pub control_plane_transaction_application_retry_interval: Option<HumanDuration>,
    #[arg(long = DomainClockProgressRetryInterval::NAME, env = "NERVIX_CONTROL_PLANE_DOMAIN_CLOCK_PROGRESS_RETRY_INTERVAL", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().control_plane.domain_clock_progress_retry_interval()).to_string(), DomainClockProgressRetryInterval::DERIVATION))]
    pub control_plane_domain_clock_progress_retry_interval: Option<HumanDuration>,
    #[arg(long = BackupRetryInterval::NAME, env = "NERVIX_CONTROL_PLANE_BACKUP_RETRY_INTERVAL", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().control_plane.backup_retry_interval()).to_string(), BackupRetryInterval::DERIVATION))]
    pub control_plane_backup_retry_interval: Option<HumanDuration>,
    #[arg(long = RestoreAdmissionWait::NAME, env = "NERVIX_CONTROL_PLANE_RESTORE_ADMISSION_WAIT", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().control_plane.restore_admission_wait()).to_string(), RestoreAdmissionWait::DERIVATION))]
    pub control_plane_restore_admission_wait: Option<HumanDuration>,
    #[arg(long = RestoreAdmissionRetryInterval::NAME, env = "NERVIX_CONTROL_PLANE_RESTORE_ADMISSION_RETRY_INTERVAL", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().control_plane.restore_admission_retry_interval()).to_string(), RestoreAdmissionRetryInterval::DERIVATION))]
    pub control_plane_restore_admission_retry_interval: Option<HumanDuration>,
    #[arg(long = ModelInspectionTimeout::NAME, env = "NERVIX_CONTROL_PLANE_MODEL_INSPECTION_TIMEOUT", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().control_plane.model_inspection_timeout()).to_string(), ModelInspectionTimeout::DERIVATION))]
    pub control_plane_model_inspection_timeout: Option<HumanDuration>,
    #[arg(long = LeaderKafkaPartitionWatchInterval::NAME, env = "NERVIX_CONTROL_PLANE_KAFKA_PARTITION_WATCH_INTERVAL", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().control_plane.kafka_partition_watch_interval()).to_string(), LeaderKafkaPartitionWatchInterval::DERIVATION))]
    pub control_plane_kafka_partition_watch_interval: Option<HumanDuration>,
    #[arg(long = InterconnectTlsReloadInterval::NAME, env = "NERVIX_INTERCONNECT_TLS_RELOAD_INTERVAL", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().control_plane.tls_reload_interval()).to_string(), InterconnectTlsReloadInterval::DERIVATION))]
    pub control_plane_tls_reload_interval: Option<HumanDuration>,
    #[arg(long = AuthenticationAttemptsPerSecond::NAME, env = "NERVIX_AUTHENTICATION_ATTEMPTS_PER_SECOND", help = capability_help(NodeOperationalCapabilities::default().control_plane.authentication_attempts_per_second().to_string(), AuthenticationAttemptsPerSecond::DERIVATION))]
    pub control_plane_authentication_attempts_per_second: Option<NonZeroU64>,
    #[arg(long = SessionFrameBytes::NAME, env = "NERVIX_SESSION_FRAME_BYTES", help = capability_help(HumanBytes(ubyte::ByteUnit::Byte(NodeOperationalCapabilities::default().session.bounds().frame_bytes().get())).to_string(), SessionFrameBytes::DERIVATION))]
    pub bounds_frame_bytes: Option<HumanBytes>,
    #[arg(long = SessionTransferBytes::NAME, env = "NERVIX_SESSION_TRANSFER_BYTES", help = capability_help(HumanBytes(ubyte::ByteUnit::Byte(NodeOperationalCapabilities::default().session.bounds().transfer_bytes().get())).to_string(), SessionTransferBytes::DERIVATION))]
    pub bounds_transfer_bytes: Option<HumanBytes>,
    #[arg(long = SessionNestingDepth::NAME, env = "NERVIX_SESSION_NESTING_DEPTH", help = capability_help(NodeOperationalCapabilities::default().session.bounds().nesting_depth().to_string(), SessionNestingDepth::DERIVATION))]
    pub bounds_nesting_depth: Option<NonZeroU64>,
    #[arg(long = SessionCollectionEntries::NAME, env = "NERVIX_SESSION_COLLECTION_ENTRIES", help = capability_help(NodeOperationalCapabilities::default().session.bounds().collection_entries().to_string(), SessionCollectionEntries::DERIVATION))]
    pub bounds_collection_entries: Option<NonZeroU64>,
    #[arg(long = SessionStringBytes::NAME, env = "NERVIX_SESSION_STRING_BYTES", help = capability_help(HumanBytes(ubyte::ByteUnit::Byte(NodeOperationalCapabilities::default().session.bounds().string_bytes().get())).to_string(), SessionStringBytes::DERIVATION))]
    pub bounds_string_bytes: Option<HumanBytes>,
    #[arg(long = SessionQueueMemoryBytes::NAME, env = "NERVIX_SESSION_QUEUE_MEMORY_BYTES", help = capability_help(HumanBytes(ubyte::ByteUnit::Byte(NodeOperationalCapabilities::default().session.queue_memory_bytes().get())).to_string(), SessionQueueMemoryBytes::DERIVATION))]
    pub session_queue_memory_bytes: Option<HumanBytes>,
    #[arg(long = SessionSubscriptionRowsPerFrame::NAME, env = "NERVIX_SESSION_SUBSCRIPTION_ROWS_PER_FRAME", help = capability_help(NodeOperationalCapabilities::default().session.subscription_rows_per_frame().to_string(), SessionSubscriptionRowsPerFrame::DERIVATION))]
    pub session_subscription_rows_per_frame: Option<NonZeroU64>,
    #[arg(long = SessionNoticeCapacity::NAME, env = "NERVIX_SESSION_NOTICE_CAPACITY", help = capability_help(NodeOperationalCapabilities::default().session.notice_capacity().to_string(), SessionNoticeCapacity::DERIVATION))]
    pub session_notice_capacity: Option<NonZeroU64>,
    #[arg(long = SessionLeadershipCheckInterval::NAME, env = "NERVIX_SESSION_LEADERSHIP_CHECK_INTERVAL", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().session.leadership_check_interval()).to_string(), SessionLeadershipCheckInterval::DERIVATION))]
    pub session_leadership_check_interval: Option<HumanDuration>,
    #[arg(long = SessionSnapshotInterval::NAME, env = "NERVIX_SESSION_SNAPSHOT_INTERVAL", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().session.snapshot_interval()).to_string(), SessionSnapshotInterval::DERIVATION))]
    pub session_snapshot_interval: Option<HumanDuration>,
    #[arg(long = ClientLinkHeartbeatInterval::NAME, env = "NERVIX_CLIENT_LINK_HEARTBEAT_INTERVAL", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().links.heartbeat_interval()).to_string(), ClientLinkHeartbeatInterval::DERIVATION))]
    pub links_heartbeat_interval: Option<HumanDuration>,
    #[arg(long = ClientProducerLinkOpenTimeout::NAME, env = "NERVIX_CLIENT_LINK_PRODUCER_OPEN_TIMEOUT", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().links.producer_open_timeout()).to_string(), ClientProducerLinkOpenTimeout::DERIVATION))]
    pub links_producer_open_timeout: Option<HumanDuration>,
    #[arg(long = ClientConsumerLinkOpenTimeout::NAME, env = "NERVIX_CLIENT_LINK_CONSUMER_OPEN_TIMEOUT", help = capability_help(HumanDuration(NodeOperationalCapabilities::default().links.consumer_open_timeout()).to_string(), ClientConsumerLinkOpenTimeout::DERIVATION))]
    pub links_consumer_open_timeout: Option<HumanDuration>,
    #[arg(long = ClientProducerLinkAnswerCapacity::NAME, env = "NERVIX_CLIENT_LINK_PRODUCER_ANSWER_CAPACITY", help = capability_help(NodeOperationalCapabilities::default().links.producer_answer_capacity().to_string(), ClientProducerLinkAnswerCapacity::DERIVATION))]
    pub links_producer_answer_capacity: Option<NonZeroU64>,
    #[arg(long = ClientConsumerLinkAnswerCapacity::NAME, env = "NERVIX_CLIENT_LINK_CONSUMER_ANSWER_CAPACITY", help = capability_help(NodeOperationalCapabilities::default().links.consumer_answer_capacity().to_string(), ClientConsumerLinkAnswerCapacity::DERIVATION))]
    pub links_consumer_answer_capacity: Option<NonZeroU64>,
    #[arg(long = ClientConsumerLinkWriterCapacity::NAME, env = "NERVIX_CLIENT_LINK_CONSUMER_WRITER_CAPACITY", help = capability_help(NodeOperationalCapabilities::default().links.consumer_writer_capacity().to_string(), ClientConsumerLinkWriterCapacity::DERIVATION))]
    pub links_consumer_writer_capacity: Option<NonZeroU64>,
}

fn capability_help(default: String, derivation: Option<&str>) -> String {
    match derivation {
        Some(rule) => {
            format!("Node capability; omitted values follow {rule} ({default} with default inputs)")
        }
        None => format!("Node capability; default: {default}"),
    }
}

fn positive_bytes<D: SettingDefinition>(
    value: Option<HumanBytes>,
) -> Result<Option<NonZeroU64>, Report<NodeOperationalError>> {
    match value {
        None => Ok(None),
        Some(value) => {
            let positive = NonZeroU64::new(value.0.as_u64()).ok_or_else(|| {
                Report::new(NodeOperationalError::NonPositive { setting: D::NAME })
            })?;
            Ok(Some(positive))
        }
    }
}

impl OperationalArgs {
    pub(super) fn settings(
        &self,
    ) -> Result<NodeOperationalCapabilities, Report<NodeOperationalError>> {
        Ok(NodeOperationalCapabilities {
            control_plane: ControlPlaneCapabilities::resolve(ControlPlaneCapabilityValues {
                membership_interval: self.control_plane_membership_interval.map(|value| value.0),
                scheduling_interval: self.control_plane_scheduling_interval.map(|value| value.0),
                resource_replication_interval: self
                    .control_plane_resource_replication_interval
                    .map(|value| value.0),
                health_probe_interval: self
                    .control_plane_health_probe_interval
                    .map(|value| value.0),
                leadership_sync_retry: self
                    .control_plane_leadership_sync_retry
                    .map(|value| value.0),
                transaction_sweep_interval: self
                    .control_plane_transaction_sweep_interval
                    .map(|value| value.0),
                restore_staging_sweep_interval: self
                    .control_plane_restore_staging_sweep_interval
                    .map(|value| value.0),
                transaction_recovery_concurrency: self
                    .control_plane_transaction_recovery_concurrency,
                resource_replication_concurrency: self
                    .control_plane_resource_replication_concurrency,
                entity_gate_release_retry_interval: self
                    .control_plane_entity_gate_release_retry_interval
                    .map(|value| value.0),
                entity_gate_engagement_retry_interval: self
                    .control_plane_entity_gate_engagement_retry_interval
                    .map(|value| value.0),
                transaction_application_retry_interval: self
                    .control_plane_transaction_application_retry_interval
                    .map(|value| value.0),
                domain_clock_progress_retry_interval: self
                    .control_plane_domain_clock_progress_retry_interval
                    .map(|value| value.0),
                backup_retry_interval: self
                    .control_plane_backup_retry_interval
                    .map(|value| value.0),
                restore_admission_wait: self
                    .control_plane_restore_admission_wait
                    .map(|value| value.0),
                restore_admission_retry_interval: self
                    .control_plane_restore_admission_retry_interval
                    .map(|value| value.0),
                model_inspection_timeout: self
                    .control_plane_model_inspection_timeout
                    .map(|value| value.0),
                kafka_partition_watch_interval: self
                    .control_plane_kafka_partition_watch_interval
                    .map(|value| value.0),
                tls_reload_interval: self.control_plane_tls_reload_interval.map(|value| value.0),
                authentication_attempts_per_second: self
                    .control_plane_authentication_attempts_per_second,
            })?,
            session: SessionCapabilities::resolve(SessionCapabilityValues {
                bounds: SessionBoundValues {
                    frame_bytes: positive_bytes::<SessionFrameBytes>(self.bounds_frame_bytes)?,
                    transfer_bytes: positive_bytes::<SessionTransferBytes>(
                        self.bounds_transfer_bytes,
                    )?,
                    nesting_depth: self.bounds_nesting_depth,
                    collection_entries: self.bounds_collection_entries,
                    string_bytes: positive_bytes::<SessionStringBytes>(self.bounds_string_bytes)?,
                },
                queue_memory_bytes: positive_bytes::<SessionQueueMemoryBytes>(
                    self.session_queue_memory_bytes,
                )?,
                subscription_rows_per_frame: self.session_subscription_rows_per_frame,
                notice_capacity: self.session_notice_capacity,
                leadership_check_interval: self
                    .session_leadership_check_interval
                    .map(|value| value.0),
                snapshot_interval: self.session_snapshot_interval.map(|value| value.0),
            })?,
            links: ClientLinkCapabilities::resolve(ClientLinkCapabilityValues {
                heartbeat_interval: self.links_heartbeat_interval.map(|value| value.0),
                producer_open_timeout: self.links_producer_open_timeout.map(|value| value.0),
                consumer_open_timeout: self.links_consumer_open_timeout.map(|value| value.0),
                producer_answer_capacity: self.links_producer_answer_capacity,
                consumer_answer_capacity: self.links_consumer_answer_capacity,
                consumer_writer_capacity: self.links_consumer_writer_capacity,
            })?,
        })
    }

    pub(super) fn from_settings(settings: &NodeOperationalCapabilities) -> Self {
        Self {
            control_plane_membership_interval: Some(HumanDuration(
                settings.control_plane.membership_interval(),
            )),
            control_plane_scheduling_interval: Some(HumanDuration(
                settings.control_plane.scheduling_interval(),
            )),
            control_plane_resource_replication_interval: Some(HumanDuration(
                settings.control_plane.resource_replication_interval(),
            )),
            control_plane_health_probe_interval: Some(HumanDuration(
                settings.control_plane.health_probe_interval(),
            )),
            control_plane_leadership_sync_retry: Some(HumanDuration(
                settings.control_plane.leadership_sync_retry(),
            )),
            control_plane_transaction_sweep_interval: Some(HumanDuration(
                settings.control_plane.transaction_sweep_interval(),
            )),
            control_plane_restore_staging_sweep_interval: Some(HumanDuration(
                settings.control_plane.restore_staging_sweep_interval(),
            )),
            control_plane_transaction_recovery_concurrency: Some(
                settings.control_plane.transaction_recovery_concurrency(),
            ),
            control_plane_resource_replication_concurrency: Some(
                settings.control_plane.resource_replication_concurrency(),
            ),
            control_plane_entity_gate_release_retry_interval: Some(HumanDuration(
                settings.control_plane.entity_gate_release_retry_interval(),
            )),
            control_plane_entity_gate_engagement_retry_interval: Some(HumanDuration(
                settings
                    .control_plane
                    .entity_gate_engagement_retry_interval(),
            )),
            control_plane_transaction_application_retry_interval: Some(HumanDuration(
                settings
                    .control_plane
                    .transaction_application_retry_interval(),
            )),
            control_plane_domain_clock_progress_retry_interval: Some(HumanDuration(
                settings
                    .control_plane
                    .domain_clock_progress_retry_interval(),
            )),
            control_plane_backup_retry_interval: Some(HumanDuration(
                settings.control_plane.backup_retry_interval(),
            )),
            control_plane_restore_admission_wait: Some(HumanDuration(
                settings.control_plane.restore_admission_wait(),
            )),
            control_plane_restore_admission_retry_interval: Some(HumanDuration(
                settings.control_plane.restore_admission_retry_interval(),
            )),
            control_plane_model_inspection_timeout: Some(HumanDuration(
                settings.control_plane.model_inspection_timeout(),
            )),
            control_plane_kafka_partition_watch_interval: Some(HumanDuration(
                settings.control_plane.kafka_partition_watch_interval(),
            )),
            control_plane_tls_reload_interval: Some(HumanDuration(
                settings.control_plane.tls_reload_interval(),
            )),
            control_plane_authentication_attempts_per_second: Some(
                settings.control_plane.authentication_attempts_per_second(),
            ),
            bounds_frame_bytes: Some(HumanBytes(ubyte::ByteUnit::Byte(
                settings.session.bounds().frame_bytes().get(),
            ))),
            bounds_transfer_bytes: Some(HumanBytes(ubyte::ByteUnit::Byte(
                settings.session.bounds().transfer_bytes().get(),
            ))),
            bounds_nesting_depth: Some(settings.session.bounds().nesting_depth()),
            bounds_collection_entries: Some(settings.session.bounds().collection_entries()),
            bounds_string_bytes: Some(HumanBytes(ubyte::ByteUnit::Byte(
                settings.session.bounds().string_bytes().get(),
            ))),
            session_queue_memory_bytes: Some(HumanBytes(ubyte::ByteUnit::Byte(
                settings.session.queue_memory_bytes().get(),
            ))),
            session_subscription_rows_per_frame: Some(
                settings.session.subscription_rows_per_frame(),
            ),
            session_notice_capacity: Some(settings.session.notice_capacity()),
            session_leadership_check_interval: Some(HumanDuration(
                settings.session.leadership_check_interval(),
            )),
            session_snapshot_interval: Some(HumanDuration(settings.session.snapshot_interval())),
            links_heartbeat_interval: Some(HumanDuration(settings.links.heartbeat_interval())),
            links_producer_open_timeout: Some(HumanDuration(
                settings.links.producer_open_timeout(),
            )),
            links_consumer_open_timeout: Some(HumanDuration(
                settings.links.consumer_open_timeout(),
            )),
            links_producer_answer_capacity: Some(settings.links.producer_answer_capacity()),
            links_consumer_answer_capacity: Some(settings.links.consumer_answer_capacity()),
            links_consumer_writer_capacity: Some(settings.links.consumer_writer_capacity()),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.optional(
            MembershipReconcileInterval::NAME,
            self.control_plane_membership_interval,
        );
        command_line.optional(
            SchedulingReconcileInterval::NAME,
            self.control_plane_scheduling_interval,
        );
        command_line.optional(
            ResourceReconcileInterval::NAME,
            self.control_plane_resource_replication_interval,
        );
        command_line.optional(
            HealthProbeInterval::NAME,
            self.control_plane_health_probe_interval,
        );
        command_line.optional(
            LeadershipSyncRetry::NAME,
            self.control_plane_leadership_sync_retry,
        );
        command_line.optional(
            TransactionSweepInterval::NAME,
            self.control_plane_transaction_sweep_interval,
        );
        command_line.optional(
            RestoreStagingSweepInterval::NAME,
            self.control_plane_restore_staging_sweep_interval,
        );
        command_line.optional(
            TransactionRecoveryConcurrency::NAME,
            self.control_plane_transaction_recovery_concurrency,
        );
        command_line.optional(
            ResourceReplicationConcurrency::NAME,
            self.control_plane_resource_replication_concurrency,
        );
        command_line.optional(
            EntityGateReleaseRetryInterval::NAME,
            self.control_plane_entity_gate_release_retry_interval,
        );
        command_line.optional(
            EntityGateEngagementRetryInterval::NAME,
            self.control_plane_entity_gate_engagement_retry_interval,
        );
        command_line.optional(
            TransactionApplicationRetryInterval::NAME,
            self.control_plane_transaction_application_retry_interval,
        );
        command_line.optional(
            DomainClockProgressRetryInterval::NAME,
            self.control_plane_domain_clock_progress_retry_interval,
        );
        command_line.optional(
            BackupRetryInterval::NAME,
            self.control_plane_backup_retry_interval,
        );
        command_line.optional(
            RestoreAdmissionWait::NAME,
            self.control_plane_restore_admission_wait,
        );
        command_line.optional(
            RestoreAdmissionRetryInterval::NAME,
            self.control_plane_restore_admission_retry_interval,
        );
        command_line.optional(
            ModelInspectionTimeout::NAME,
            self.control_plane_model_inspection_timeout,
        );
        command_line.optional(
            LeaderKafkaPartitionWatchInterval::NAME,
            self.control_plane_kafka_partition_watch_interval,
        );
        command_line.optional(
            InterconnectTlsReloadInterval::NAME,
            self.control_plane_tls_reload_interval,
        );
        command_line.optional(
            AuthenticationAttemptsPerSecond::NAME,
            self.control_plane_authentication_attempts_per_second,
        );
        command_line.optional(SessionFrameBytes::NAME, self.bounds_frame_bytes);
        command_line.optional(SessionTransferBytes::NAME, self.bounds_transfer_bytes);
        command_line.optional(SessionNestingDepth::NAME, self.bounds_nesting_depth);
        command_line.optional(
            SessionCollectionEntries::NAME,
            self.bounds_collection_entries,
        );
        command_line.optional(SessionStringBytes::NAME, self.bounds_string_bytes);
        command_line.optional(
            SessionQueueMemoryBytes::NAME,
            self.session_queue_memory_bytes,
        );
        command_line.optional(
            SessionSubscriptionRowsPerFrame::NAME,
            self.session_subscription_rows_per_frame,
        );
        command_line.optional(SessionNoticeCapacity::NAME, self.session_notice_capacity);
        command_line.optional(
            SessionLeadershipCheckInterval::NAME,
            self.session_leadership_check_interval,
        );
        command_line.optional(
            SessionSnapshotInterval::NAME,
            self.session_snapshot_interval,
        );
        command_line.optional(
            ClientLinkHeartbeatInterval::NAME,
            self.links_heartbeat_interval,
        );
        command_line.optional(
            ClientProducerLinkOpenTimeout::NAME,
            self.links_producer_open_timeout,
        );
        command_line.optional(
            ClientConsumerLinkOpenTimeout::NAME,
            self.links_consumer_open_timeout,
        );
        command_line.optional(
            ClientProducerLinkAnswerCapacity::NAME,
            self.links_producer_answer_capacity,
        );
        command_line.optional(
            ClientConsumerLinkAnswerCapacity::NAME,
            self.links_consumer_answer_capacity,
        );
        command_line.optional(
            ClientConsumerLinkWriterCapacity::NAME,
            self.links_consumer_writer_capacity,
        );
    }
}
