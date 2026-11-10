//! Bounded operational capability values with every dependent setting resolved.
//!
//! Outside the layer order: test harness.
//! - **Owns.** Complete current session, control-plane and client-link claim generation.
//! - **Depends on.** Vocabulary constructors and the deterministic entropy cursor.
//! - **Must not know.** Transports, running queues or server composition.

use std::{num::NonZeroU64, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::*;

use crate::Entropy;

impl Entropy<'_> {
    fn operational_duration(&mut self) -> Duration {
        Duration::from_nanos(self.boundary_biased(1..=1_000_000_000_000))
    }

    fn operational_count(&mut self) -> NonZeroU64 {
        NonZeroU64::new(self.boundary_biased(1..=64)).assured("the generated count is positive")
    }

    pub fn announced_link_heartbeat(&mut self) -> AnnouncedLinkHeartbeat {
        ClientLinkCapabilities::resolve(ClientLinkCapabilityValues {
            heartbeat_interval: Some(self.operational_duration()),
            ..Default::default()
        })
        .assured("the generated positive cadence is in the setting's domain")
        .announced_heartbeat()
    }

    pub fn session_bounds(&mut self) -> SessionBounds {
        let maximum =
            u64::try_from(SessionFrameBytes::MAXIMUM).assured("the frame ceiling fits u64");
        let minimum = u64::try_from(SessionFrameBytes::MINIMUM).assured("the frame floor fits u64");
        let frame = self.boundary_biased(minimum..=maximum);
        let transfer = self.boundary_biased(frame..=maximum);
        SessionBounds::resolve(SessionBoundValues {
            frame_bytes: NonZeroU64::new(frame),
            transfer_bytes: NonZeroU64::new(transfer),
            nesting_depth: NonZeroU64::new(self.boundary_biased(
                u64::try_from(SessionNestingDepth::MINIMUM).assured("the floor fits u64")
                    ..=u64::try_from(SessionNestingDepth::MAXIMUM).assured("the ceiling fits u64"),
            )),
            collection_entries: NonZeroU64::new(self.boundary_biased(1..=u64::from(u32::MAX))),
            string_bytes: NonZeroU64::new(self.boundary_biased(1..=transfer)),
        })
        .assured("the generated bounds hold their relationships and fit native and browser targets")
    }

    pub fn node_operational_capabilities(&mut self) -> NodeOperationalCapabilities {
        let bounds = self.session_bounds();
        let queue_memory = bounds
            .frame_bytes()
            .get()
            .checked_mul(2 * self.boundary_biased(1..=32))
            .assured("at most sixty-four frames of at most 256 MiB fit u64");
        let rows = self.boundary_biased(1..=bounds.collection_entries().get().min(4096));
        NodeOperationalCapabilities {
            control_plane: ControlPlaneCapabilities::resolve(ControlPlaneCapabilityValues {
                membership_interval: Some(self.operational_duration()),
                scheduling_interval: Some(self.operational_duration()),
                resource_replication_interval: Some(self.operational_duration()),
                health_probe_interval: Some(self.operational_duration()),
                leadership_sync_retry: Some(self.operational_duration()),
                transaction_sweep_interval: Some(self.operational_duration()),
                restore_staging_sweep_interval: Some(self.operational_duration()),
                transaction_recovery_concurrency: Some(self.operational_count()),
                resource_replication_concurrency: Some(self.operational_count()),
                entity_gate_release_retry_interval: Some(self.operational_duration()),
                entity_gate_engagement_retry_interval: Some(self.operational_duration()),
                transaction_application_retry_interval: Some(self.operational_duration()),
                domain_clock_progress_retry_interval: Some(self.operational_duration()),
                backup_retry_interval: Some(self.operational_duration()),
                restore_admission_wait: Some(self.operational_duration()),
                restore_admission_retry_interval: Some(self.operational_duration()),
                model_inspection_timeout: Some(self.operational_duration()),
                kafka_partition_watch_interval: Some(self.operational_duration()),
                tls_reload_interval: Some(self.operational_duration()),
                authentication_attempts_per_second: Some(self.operational_count()),
            })
            .assured("all generated local operational values are positive and bounded"),
            session: SessionCapabilities::resolve(SessionCapabilityValues {
                bounds: bounds.values(),
                queue_memory_bytes: NonZeroU64::new(queue_memory),
                subscription_rows_per_frame: NonZeroU64::new(rows),
                notice_capacity: Some(self.operational_count()),
                leadership_check_interval: Some(self.operational_duration()),
                snapshot_interval: Some(self.operational_duration()),
            })
            .assured(
                "the generated queue holds both lanes and the row count fits the collection bound",
            ),
            links: ClientLinkCapabilities::resolve(ClientLinkCapabilityValues {
                heartbeat_interval: Some(self.operational_duration()),
                producer_open_timeout: Some(self.operational_duration()),
                consumer_open_timeout: Some(self.operational_duration()),
                producer_answer_capacity: Some(self.operational_count()),
                consumer_answer_capacity: Some(self.operational_count()),
                consumer_writer_capacity: Some(self.operational_count()),
            })
            .assured("the generated link capacities and cadences are positive and bounded"),
        }
    }
}
