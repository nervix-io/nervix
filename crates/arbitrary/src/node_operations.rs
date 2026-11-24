//! Bounded operational capability values with every dependent setting resolved.
//!
//! Outside the layer order: test harness.
//! - **Owns.** Complete current session, control-plane, client-link, trace export and web console
//!   claim generation.
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

    /// A share of root traces on the closed unit interval, both ends included.
    fn sample_ratio(&mut self) -> SampleRatio {
        let thousandths = u32::try_from(self.boundary_biased(0..=1000))
            .assured("the generated thousandths are at most 1000");
        SampleRatio::new(f64::from(thousandths) / 1000.0)
    }

    pub fn trace_export_capabilities(&mut self) -> TraceExportCapabilities {
        let queue = self.boundary_biased(1..=65_536);
        let batch = self.boundary_biased(1..=queue);
        TraceExportCapabilities::resolve(TraceExportCapabilityValues {
            sample_ratio: Some(self.sample_ratio()),
            export_timeout: Some(self.operational_duration()),
            export_interval: Some(self.operational_duration()),
            queue_capacity: NonZeroU64::new(queue),
            export_batch_spans: NonZeroU64::new(batch),
        })
        .assured("the generated ratio, cadences and a batch within its queue are in each domain")
    }

    /// A web console wait: whole milliseconds from the browser timer's resolution through the
    /// longest wait, both included.
    fn web_console_wait(&mut self) -> Duration {
        let shortest = u64::try_from(BROWSER_TIMER_RESOLUTION.as_millis())
            .assured("a millisecond counted in milliseconds fits u64");
        let longest = u64::try_from(LONGEST_WEB_CONSOLE_WAIT.as_millis())
            .assured("a day counted in milliseconds fits u64");
        Duration::from_millis(self.boundary_biased(shortest..=longest))
    }

    /// The web console's waits, a longest backoff at least each first wait, and a restore queue
    /// the browser can count.
    pub fn web_console_capabilities(&mut self) -> WebConsoleCapabilities {
        let retry_backoff = self.web_console_wait();
        let restoration_backoff = self.web_console_wait();
        let floor = u64::try_from(retry_backoff.max(restoration_backoff).as_millis())
            .assured("a first wait of at most a day counted in milliseconds fits u64");
        let longest = u64::try_from(LONGEST_WEB_CONSOLE_WAIT.as_millis())
            .assured("a day counted in milliseconds fits u64");
        let retry_max_backoff = Duration::from_millis(self.boundary_biased(floor..=longest));
        let frames = self.boundary_biased(1..=u64::from(WebConsoleRestoreFrameCapacity::MAXIMUM));
        WebConsoleCapabilities::resolve(WebConsoleCapabilityValues {
            request_timeout: Some(self.web_console_wait()),
            connect_timeout: Some(self.web_console_wait()),
            retry_backoff: Some(retry_backoff),
            retry_max_backoff: Some(retry_max_backoff),
            restoration_backoff: Some(restoration_backoff),
            restore_frame_capacity: NonZeroU64::new(frames),
        })
        .assured(
            "the generated waits are whole milliseconds within their bounds, the longest backoff \
             is at least each first wait, and the browser can count the restore queue",
        )
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
                runtime_state_retry_interval: Some(self.operational_duration()),
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
            traces: self.trace_export_capabilities(),
            web_console: self.web_console_capabilities(),
        }
    }
}
