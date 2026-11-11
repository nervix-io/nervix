//! Node settings across the command line that renders and reads them.
//!
//! Layer: test harness.
//! - **Owns.** Generation of current, valid node settings for every settings group, with their
//!   relationships held, and the complete round-trip oracle of rendering them as options and
//!   reading them back, together with the duration and byte quantity forms help prints defaults in.
//! - **Depends on.** The settings groups, the options that render and read them, and the edge's
//!   duration and byte forms.
//! - **Must not know.** What any owner does with its values.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the node settings command-line boundary")
)]

use std::time::Duration;

use nervix_arbitrary::Entropy;
use nervix_consensus::{RaftRetentionBounds, RaftRetentionSettings, RaftTimings};
use ubyte::ByteUnit;

use super::*;
use crate::{
    application::{
        CommandExecutionPolicy, NodeSettings, SchedulingSettings, ShutdownSettings,
        TransactionBounds, TransactionLimits,
        cli_values::{HumanBytes, HumanDuration},
    },
    capacity::CapacitySettings,
    cluster::{MembershipSettings, MembershipValues},
    memory_pressure::MemoryPressureSettings,
    resource::ResourceSettings,
    runtime::RuntimeSettings,
};

const CASE_BYTES: usize = 2048;

/// The most whole seconds a generated duration spans, far beyond any timing a node runs with and
/// well inside what duration text can name.
const LONGEST_SECONDS: u64 = 1_000_000_000_000;

/// The most whole milliseconds a generated timing prerequisite spans above its floor.
const LONGEST_TIMING_MILLIS: u64 = 1_000_000_000;

/// Builds current node settings, each group valid on its own and every relationship held.
struct GeneratedSettings<'bytes> {
    entropy: Entropy<'bytes>,
}

impl<'bytes> GeneratedSettings<'bytes> {
    fn execution_workers(&mut self) -> nervix_execution::ExecutionOverrides {
        nervix_execution::ExecutionOverrides {
            control_cpu_workers: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(1..=8))
                        .verified("generated small counts fit usize"),
                )
            } else {
                None
            },
            control_cpu_pending_jobs: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(1..=8))
                        .verified("generated small counts fit usize"),
                )
            } else {
                None
            },
            credentials_cpu_workers: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(1..=8))
                        .verified("generated small counts fit usize"),
                )
            } else {
                None
            },
            credentials_cpu_pending_jobs: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(1..=8))
                        .verified("generated small counts fit usize"),
                )
            } else {
                None
            },
            data_cpu_workers: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(1..=8))
                        .verified("generated small counts fit usize"),
                )
            } else {
                None
            },
            data_cpu_pending_jobs: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(1..=8))
                        .verified("generated small counts fit usize"),
                )
            } else {
                None
            },
            extension_cpu_workers: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(1..=8))
                        .verified("generated small counts fit usize"),
                )
            } else {
                None
            },
            extension_cpu_pending_jobs: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(1..=8))
                        .verified("generated small counts fit usize"),
                )
            } else {
                None
            },
            bulk_cpu_workers: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(1..=8))
                        .verified("generated small counts fit usize"),
                )
            } else {
                None
            },
            bulk_cpu_pending_jobs: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(1..=8))
                        .verified("generated small counts fit usize"),
                )
            } else {
                None
            },
            consensus_storage_workers: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(1..=8))
                        .verified("generated small counts fit usize"),
                )
            } else {
                None
            },
            consensus_storage_pending_jobs: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(1..=8))
                        .verified("generated small counts fit usize"),
                )
            } else {
                None
            },
            filesystem_storage_workers: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(1..=8))
                        .verified("generated small counts fit usize"),
                )
            } else {
                None
            },
            filesystem_storage_pending_jobs: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(1..=8))
                        .verified("generated small counts fit usize"),
                )
            } else {
                None
            },
            connector_storage_workers: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(1..=8))
                        .verified("generated small counts fit usize"),
                )
            } else {
                None
            },
            connector_storage_pending_jobs: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(1..=8))
                        .verified("generated small counts fit usize"),
                )
            } else {
                None
            },
            ..Default::default()
        }
    }

    fn execution(&mut self) -> nervix_execution::ExecutionOverrides {
        let workers = self.execution_workers();
        nervix_execution::ExecutionOverrides {
            intake_memory_bytes: if self.entropy.flag() {
                Some(ByteUnit::Byte(
                    self.entropy.between(1..=u64::from(u32::MAX)),
                ))
            } else {
                None
            },
            management_memory_bytes: if self.entropy.flag() {
                Some(ByteUnit::Byte(
                    self.entropy.between(1..=u64::from(u32::MAX)),
                ))
            } else {
                None
            },
            commands_memory_bytes: if self.entropy.flag() {
                Some(ByteUnit::Byte(
                    self.entropy.between(1..=u64::from(u32::MAX)),
                ))
            } else {
                None
            },
            relay_memory_bytes: if self.entropy.flag() {
                Some(ByteUnit::Byte(
                    self.entropy.between(1..=u64::from(u32::MAX)),
                ))
            } else {
                None
            },
            state_read_memory_bytes: if self.entropy.flag() {
                Some(ByteUnit::Byte(
                    self.entropy.between(1..=u64::from(u32::MAX)),
                ))
            } else {
                None
            },
            state_read_cache_memory_bytes: if self.entropy.flag() {
                Some(ByteUnit::Byte(
                    self.entropy.between(1..=u64::from(u32::MAX)),
                ))
            } else {
                None
            },
            bulk_memory_bytes: if self.entropy.flag() {
                Some(ByteUnit::Byte(
                    self.entropy.between(1..=u64::from(u32::MAX)),
                ))
            } else {
                None
            },
            bulk_window_memory_bytes: if self.entropy.flag() {
                Some(ByteUnit::Byte(
                    self.entropy.between(1..=u64::from(u32::MAX)),
                ))
            } else {
                None
            },
            restore_metadata_memory_bytes: if self.entropy.flag() {
                Some(ByteUnit::Byte(
                    self.entropy.between(1..=u64::from(u32::MAX)),
                ))
            } else {
                None
            },
            credentials_memory_bytes: if self.entropy.flag() {
                Some(ByteUnit::Byte(
                    self.entropy.between(1..=u64::from(u32::MAX)),
                ))
            } else {
                None
            },
            relay_encoded_bytes: if self.entropy.flag() {
                Some(ByteUnit::Byte(
                    self.entropy.between(1..=u64::from(u32::MAX)),
                ))
            } else {
                None
            },
            relay_decoded_bytes: if self.entropy.flag() {
                Some(ByteUnit::Byte(
                    self.entropy.between(1..=u64::from(u32::MAX)),
                ))
            } else {
                None
            },
            relay_scratch_bytes: if self.entropy.flag() {
                Some(ByteUnit::Byte(
                    self.entropy.between(1..=u64::from(u32::MAX)),
                ))
            } else {
                None
            },
            bulk_chunk_bytes: if self.entropy.flag() {
                Some(ByteUnit::Byte(
                    self.entropy.between(1..=u64::from(u32::MAX)),
                ))
            } else {
                None
            },
            resident_replication_batches: if self.entropy.flag() {
                std::num::NonZeroU32::new(
                    u32::try_from(self.entropy.between(1..=16))
                        .verified("a generated resident count fits u32"),
                )
            } else {
                None
            },
            ..workers
        }
    }

    fn new(bytes: &'bytes [u8]) -> Self {
        Self {
            entropy: Entropy::new(bytes),
        }
    }

    fn duration(&mut self) -> Duration {
        let seconds = self.entropy.boundary_biased(0..=LONGEST_SECONDS);
        let nanos = self.entropy.between(0..=999_999_999);
        let nanos = u32::try_from(nanos).verified("the range above ends below a second");
        Duration::new(seconds, nanos)
    }

    /// A duration of at least one nanosecond.
    fn positive_duration(&mut self) -> Duration {
        let duration = self.duration();
        if duration.is_zero() {
            return Duration::from_nanos(1);
        }
        duration
    }

    /// A duration whose whole milliseconds are at least `floor_millis`, with any sub-millisecond
    /// remainder.
    fn timing_from(&mut self, floor_millis: u64) -> Duration {
        let extra = self.entropy.boundary_biased(0..=LONGEST_TIMING_MILLIS);
        let millis = floor_millis
            .checked_add(extra)
            .assured("the floor and the extra are each far below u64::MAX");
        let sub_millisecond = self.entropy.between(0..=999_999);
        let sub_millisecond =
            u32::try_from(sub_millisecond).verified("the range above ends below a millisecond");
        Duration::from_millis(millis)
            .checked_add(Duration::from_nanos(u64::from(sub_millisecond)))
            .assured("a bounded timing and less than a millisecond fit a duration")
    }

    fn bytes(&mut self) -> ByteUnit {
        let unit = self.entropy.pick([
            ByteUnit::B,
            ByteUnit::KiB,
            ByteUnit::MiB,
            ByteUnit::GiB,
            ByteUnit::TiB,
        ]);
        let count = self.entropy.boundary_biased(0..=u64::MAX / unit.as_u64());
        ByteUnit::Byte(
            count
                .checked_mul(unit.as_u64())
                .verified("the count is bounded by the unit it multiplies"),
        )
    }

    fn count(&mut self) -> u64 {
        self.entropy.any_u64()
    }

    fn raft(&mut self) -> RaftTimings {
        let heartbeat = self.timing_from(1);
        RaftTimings::resolve(Some(heartbeat)).assured(
            "the heartbeat is at least a millisecond and twelve of it fit u64 milliseconds",
        )
    }

    fn membership(&mut self) -> MembershipSettings {
        MembershipSettings::resolve(MembershipValues {
            node_unavailability_timeout: Some(self.cluster_duration()),
            gossip_interval: Some(self.timing_from(1)),
            clock_skew_tolerance: Some(self.timing_from(1)),
        })
        .assured("each membership timing is at least a millisecond and bounded")
    }

    fn raft_retention(&mut self) -> RaftRetentionSettings {
        RaftRetentionSettings::resolve(RaftRetentionBounds {
            snapshot_entry_threshold: Some(self.count().min(u64::MAX - 1)),
            snapshot_byte_threshold: Some(self.bytes()),
            covered_entries_retained: Some(self.count()),
            covered_bytes_retained: Some(self.bytes()),
            retained_log_cap: Some(self.bytes()),
        })
        .assured("the generated snapshot threshold leaves room for a larger replication lag")
    }

    fn capacity(&mut self) -> CapacitySettings {
        let memory_limit = if self.entropy.flag() {
            Some(ByteUnit::Byte(self.entropy.between(1..=u64::MAX)))
        } else {
            None
        };
        let cpu_count = if self.entropy.flag() {
            Some(self.entropy.between(1..=u64::MAX))
        } else {
            None
        };
        CapacitySettings::resolve(memory_limit, cpu_count)
            .assured("a memory limit, when given, is at least one byte")
    }

    fn memory_pressure(&mut self) -> MemoryPressureSettings {
        let watermarks = if self.entropy.flag() {
            let high = self.entropy.between(1..=u64::MAX);
            let below_high = high
                .checked_sub(1)
                .verified("the high watermark is at least one");
            let low = self.entropy.between(0..=below_high);
            Some((ByteUnit::Byte(high), ByteUnit::Byte(low)))
        } else {
            None
        };
        let check_interval = self.positive_duration();
        let resume_jitter = self.duration();
        MemoryPressureSettings::resolve(
            watermarks.map(|(high, _)| high),
            watermarks.map(|(_, low)| low),
            Some(check_interval),
            Some(resume_jitter),
        )
        .assured("the watermarks are paired, ordered and checked at a positive interval")
    }

    fn shutdown(&mut self) -> ShutdownSettings {
        let shutdown_timeout = self.duration();
        // A drain this long is far longer than any node takes, and never longer than the
        // shutdown.
        let longest_drain = u64::try_from(shutdown_timeout.as_nanos()).unwrap_or(u64::MAX);
        let drain_timeout = Duration::from_nanos(self.entropy.boundary_biased(0..=longest_drain));
        ShutdownSettings::resolve(Some(shutdown_timeout), Some(drain_timeout))
            .assured("the drain never exceeds the shutdown")
    }

    fn cluster_duration(&mut self) -> Duration {
        let seconds = self.entropy.boundary_biased(0..=u64::from(u32::MAX));
        let nanos = if seconds == u64::from(u32::MAX) {
            0
        } else {
            self.entropy.boundary_biased(0..=999_999_999)
        };
        let nanos = if seconds == 0 { nanos.max(1) } else { nanos };
        Duration::new(
            seconds,
            u32::try_from(nanos).assured("generated nanoseconds are below one second"),
        )
    }

    fn cluster_count(&mut self) -> usize {
        usize::try_from(self.entropy.boundary_biased(0..=u64::from(u32::MAX)))
            .assured("a native target represents the cluster count domain")
    }

    fn positive_bytes(&mut self) -> ubyte::ByteUnit {
        ubyte::ByteUnit::Byte(self.bytes().as_u64().max(1))
    }

    fn infrastructure_count(&mut self, maximum: u64) -> usize {
        usize::try_from(self.entropy.between(1..=maximum))
            .verified("infrastructure generator bounds fit supported target widths")
    }

    fn infrastructure_duration(&mut self) -> Duration {
        Duration::from_millis(self.entropy.boundary_biased(1..=86_400_000))
    }

    fn interconnect(&mut self) -> nervix_interconnect::TransportSettings {
        let max_peers = self.infrastructure_count(128);
        let queue = self.infrastructure_count(4096);
        let request_quota =
            if self.entropy.flag() {
                Some(self.infrastructure_count(
                    u64::try_from(queue).assured("the generated queue fits u64"),
                ))
            } else {
                None
            };
        let backoff = self.infrastructure_duration();
        let maximum = backoff.max(self.infrastructure_duration());
        nervix_interconnect::TransportSettings::resolve(
            nervix_interconnect::TransportSettingValues {
                max_peers: Some(max_peers),
                concurrent_handshakes: Some(self.infrastructure_count(128)),
                incoming_queue_capacity: Some(queue),
                request_quota,
                stream_receive_window: Some(ByteUnit::Byte(
                    self.entropy.between(1..=(1 << 31) - 1),
                )),
                connection_receive_window: Some(ByteUnit::Byte(
                    self.entropy.between(1..=(1 << 31) - 1),
                )),
                connection_setup_timeout: Some(self.infrastructure_duration()),
                request_timeout: Some(self.infrastructure_duration()),
                progress_timeout: Some(self.infrastructure_duration()),
                reconnect_backoff: Some(backoff),
                max_reconnect_backoff: Some(maximum),
                reconnect_jitter_permille: Some(
                    u16::try_from(self.entropy.up_to(1000)).assured("permille fits u16"),
                ),
            },
        )
        .assured("generated positive bounds fit primitive and HTTP/2 limits")
    }

    fn dns(&mut self, peers: usize) -> nervix_dns::DnsSettings {
        nervix_dns::DnsSettings::resolve(
            nervix_dns::DnsSettingValues {
                cache_entries: Some(self.entropy.between(1..=65536)),
                positive_max_ttl: Some(self.infrastructure_duration()),
                negative_max_ttl: Some(self.infrastructure_duration()),
                hook_lookup_budget: Some(self.infrastructure_duration()),
                address_family_policy: Some(self.entropy.pick([
                    nervix_dns::AddressFamilies::DualStack,
                    nervix_dns::AddressFamilies::Ipv4,
                    nervix_dns::AddressFamilies::Ipv6,
                ])),
                trust_negative_answers: Some(self.entropy.flag()),
            },
            peers,
        )
        .assured("generated resolver bounds are positive")
    }

    fn database<N: nervix_models::DatabaseNamespace>(
        &mut self,
    ) -> nervix_models::DatabaseSettings<N> {
        nervix_models::DatabaseSettings::resolve(nervix_models::DatabaseSettingValues {
            cache_bytes: Some(ByteUnit::Byte(self.entropy.between(1..=64 << 20))),
            journal_bytes: Some(ByteUnit::Byte(self.entropy.between((64 << 20)..=1 << 30))),
            workers: if self.entropy.flag() {
                Some(self.infrastructure_count(16))
            } else {
                None
            },
        })
        .assured("the generated cache, journal and worker inputs satisfy database bounds")
    }

    fn consensus_operations(&mut self) -> nervix_consensus::RaftOperationSettings {
        let backoff = self.infrastructure_duration();
        let maximum = backoff.max(self.infrastructure_duration());
        nervix_consensus::RaftOperationSettings::resolve(nervix_consensus::RaftOperationValues {
            event_capacity: Some(self.infrastructure_count(4096)),
            append_batch_entries: Some(self.entropy.between(1..=4096)),
            outstanding_append_batches: Some(self.infrastructure_count(4096)),
            outstanding_append_bytes: Some(ByteUnit::Byte(
                self.entropy.between((2 << 20)..=64 << 20),
            )),
            append_idle_bound: Some(self.infrastructure_duration()),
            snapshot_transfer_timeout: Some(self.infrastructure_duration()),
            membership_mutation_timeout: Some(self.infrastructure_duration()),
            retention_interval: Some(self.infrastructure_duration()),
            retention_admission_timeout: Some(self.infrastructure_duration()),
            purge_batch_entries: Some(self.entropy.between(1..=4096)),
            api_channel_capacity: Some(self.entropy.between(1..=4096)),
            notification_channel_capacity: Some(self.entropy.between(1..=4096)),
            state_machine_channel_capacity: Some(self.entropy.between(1..=4096)),
            api_batch_capacity: Some(self.entropy.between(1..=4096)),
            replication_backoff: Some(backoff),
            replication_backoff_maximum: Some(maximum),
            membership_step_down_delay: Some(self.infrastructure_duration()),
        })
        .assured("generated engine capacities fit bounded counts and milliseconds")
    }

    #[cfg(test)]
    fn listeners(&mut self) -> crate::application::listener_settings::ListenerSettings {
        use crate::application::listener_settings::{ListenerOverrides, ListenerSettings};
        ListenerSettings::resolve(ListenerOverrides {
            max_connections: Some(
                u32::try_from(self.entropy.between(1..=1024)).verified("bounded listener count"),
            ),
            header_bytes: Some(ByteUnit::Byte(self.entropy.between(8192..=67108864))),
            buffer_bytes: Some(ByteUnit::Byte(self.entropy.between(8192..=67108864))),
            header_timeout: Some(Duration::from_secs(self.entropy.between(1..=120))),
            tls_timeout: Some(Duration::from_secs(self.entropy.between(1..=120))),
            idle_timeout: Some(Duration::from_secs(self.entropy.between(1..=120))),
            accept_retry_delay: Some(Duration::from_secs(self.entropy.between(1..=120))),
            http_keepalive: Some(self.entropy.flag()),
            http_body_bytes: Some(ByteUnit::Byte(self.entropy.between(8192..=67108864))),
            http_body_copy_bytes: Some(ByteUnit::Byte(self.entropy.between(8192..=67108864))),
            http_body_timeout: Some(Duration::from_secs(self.entropy.between(1..=120))),
            http_refusal_drain_timeout: Some(Duration::from_secs(self.entropy.between(1..=120))),
            websocket_message_bytes: Some(ByteUnit::Byte(self.entropy.between(8192..=67108864))),
            websocket_frame_bytes: Some(ByteUnit::Byte(self.entropy.between(8192..=67108864))),
            grpc_header_bytes: Some(ByteUnit::Byte(self.entropy.between(8192..=67108864))),
            grpc_concurrency: Some(
                u32::try_from(self.entropy.between(1..=1024)).verified("bounded listener count"),
            ),
            grpc_request_timeout: Some(Duration::from_secs(self.entropy.between(1..=120))),
            grpc_max_streams: Some(
                u32::try_from(self.entropy.between(1..=1024)).verified("bounded listener count"),
            ),
            grpc_keepalive_interval: Some(Duration::from_secs(self.entropy.between(1..=120))),
            grpc_keepalive_timeout: Some(Duration::from_secs(self.entropy.between(1..=120))),
            tcp_keepalive_time: Some(Duration::from_secs(self.entropy.between(1..=120))),
            tcp_keepalive_interval: Some(Duration::from_secs(self.entropy.between(1..=120))),
            tcp_keepalive_retries: Some(
                u32::try_from(self.entropy.between(1..=127)).verified("bounded listener count"),
            ),
        })
        .assured("generated bounded listener inputs satisfy their scalar contracts")
    }

    fn node_settings(&mut self) -> NodeSettings {
        let execution = self.execution();
        let runtime_threads = crate::application::RuntimeThreadOverrides {
            worker_threads: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(1..=8)).verified("small worker count"),
                )
            } else {
                None
            },
            max_blocking_threads: if self.entropy.flag() {
                std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.between(16..=512))
                        .verified("bounded thread count"),
                )
            } else {
                None
            },
            thread_stack_size: if self.entropy.flag() {
                Some(ByteUnit::Mebibyte(self.entropy.between(1..=16)))
            } else {
                None
            },
        };
        let client_endpoints = crate::runtime::ClientEndpointOverrides {
            producer_node_bytes: if self.entropy.flag() {
                Some(self.positive_bytes())
            } else {
                None
            },
            consumer_node_bytes: if self.entropy.flag() {
                Some(self.positive_bytes())
            } else {
                None
            },
        };
        let tls_cipher_suites = self
            .entropy
            .pick(["fips", "default", "TLS13_CHACHA20_POLY1305_SHA256"])
            .parse()
            .assured("the generated TLS suites include TLS 1.3");
        let tls_key_exchange_groups = self
            .entropy
            .pick(["fips", "default", "X25519"])
            .parse()
            .assured("the generated groups support every retained TLS 1.2 suite");
        let raft = self.raft();
        let raft_retention = self.raft_retention();
        let membership = self.membership();
        let scheduling = SchedulingSettings::resolve(Some(self.cluster_count()))
            .assured("the scheduling settings declare no validation");
        let restore_staging_max_bytes = if self.entropy.flag() {
            Some(self.bytes())
        } else {
            None
        };
        let mut runtime =
            RuntimeSettings::resolve(Some(self.cluster_duration()), restore_staging_max_bytes)
                .assured("the runtime settings declare no validation");
        runtime.vm = nervix_vm::VmSettings::resolve(nervix_vm::VmSettingOverrides {
            inline_row_limit: Some(
                usize::try_from(self.entropy.boundary_biased(0..=4096))
                    .assured("bounded rows fit usize"),
            ),
            regex_search_cache_bytes: Some(self.entropy.boundary_biased(1..=8_388_608)),
            dynamic_pattern_cache_capacity: Some(
                usize::try_from(self.entropy.boundary_biased(1..=128))
                    .assured("bounded caches fit usize"),
            ),
            pattern_sets_per_batch: Some(
                usize::try_from(self.entropy.boundary_biased(1..=128))
                    .assured("bounded sets fit usize"),
            ),
        })
        .assured("generated positive cache sizes validate");
        runtime.wasm_engine = nervix_wasm::WasmEngineSettings::resolve(
            Some(self.entropy.boundary_biased(1..=8_388_608)),
            Some(self.entropy.boundary_biased(0..=8_589_934_592)),
        )
        .assured("bounded engine sizes fit this native host");
        runtime.connector_host = crate::runtime::ConnectorHostSettings::resolve(
            crate::runtime::ConnectorHostSettingOverrides {
                retry_multiplier: std::num::NonZeroU32::new(
                    u32::try_from(self.entropy.boundary_biased(1..=8))
                        .assured("bounded multiplier fits u32"),
                ),
                retry_jitter_percent: Some(
                    u8::try_from(self.entropy.boundary_biased(0..=100))
                        .assured("percentages fit u8"),
                ),
                kafka_assignment_retry: Some(self.cluster_duration()),
                kafka_partition_watch_interval: Some(self.cluster_duration()),
                syslog_queue_capacity: std::num::NonZeroUsize::new(
                    usize::try_from(self.entropy.boundary_biased(1..=128))
                        .assured("bounded queue fits usize"),
                ),
                syslog_read_chunk_bytes: Some(self.entropy.boundary_biased(1..=65_536)),
            },
        )
        .assured("generated connector host bounds validate");
        let transactions = TransactionLimits::resolve(TransactionBounds {
            idle_timeout: Some(self.cluster_duration()),
            tombstone_retention: Some(self.cluster_duration()),
            max_statements: Some(self.cluster_count()),
            max_source_bytes: Some(self.count()),
            max_open: Some(self.cluster_count()),
        })
        .assured("the transaction limits declare no validation");
        let commands = CommandExecutionPolicy::resolve(
            Some(self.cluster_duration()),
            Some(self.cluster_count().max(1)),
        )
        .assured("the command execution settings declare no validation");
        let memory_pressure = self.memory_pressure();
        let shutdown = self.shutdown();
        let resources = ResourceSettings::resolve(
            Some(self.positive_bytes()),
            Some(self.positive_bytes()),
            Some(self.count().max(1)),
        )
        .assured("the resource settings declare no validation");
        let capacity = self.capacity();
        let limits = nervix_execution::OperationLimits::default();
        let peer_receive = nervix_interconnect::PeerReceiveOverrides {
            relay_batch_bytes: Some(ByteUnit::Byte(
                self.positive_bytes()
                    .as_u64()
                    .min(limits.relay_encoded_bytes.as_u64()),
            )),
            relay_decoded_bytes: Some(ByteUnit::Byte(
                self.positive_bytes()
                    .as_u64()
                    .min(limits.relay_decoded_bytes.as_u64()),
            )),
            bulk_chunk_bytes: Some(ByteUnit::Byte(
                self.positive_bytes()
                    .as_u64()
                    .min(limits.bulk_chunk_bytes.as_u64()),
            )),
            bulk_window_bytes: Some(ByteUnit::Byte(self.positive_bytes().as_u64().min(16 << 20))),
        };
        let interconnect = self.interconnect();
        let dns = self.dns(interconnect.max_peers());
        let mut settings = NodeSettings {
            interconnect,
            dns,
            consensus_operations: self.consensus_operations(),
            node_database: self.database(),
            consensus_database: self.database(),
            listeners: self.listeners(),
            parameter_seeds: nervix_models::ClusterParameterSeedValues {
                tls_cipher_suites: Some(tls_cipher_suites),
                tls_key_exchange_groups: Some(tls_key_exchange_groups),
                ..Default::default()
            },
            raft,
            raft_retention,
            membership,
            scheduling,
            runtime,
            transactions,
            commands,
            memory_pressure,
            shutdown,
            resources,
            capacity,
            peer_receive,
            execution,
            runtime_threads,
            client_endpoints,
        };
        let args = super::SettingsArgs::from(&settings);
        settings.parameter_seeds = args.parameter_seeds();
        settings
    }
}

#[test]
fn bolero_node_settings_survive_their_command_line() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|bytes: &[u8]| {
            let mut generated = GeneratedSettings::new(bytes);
            let settings = generated.node_settings();
            let read = settings
                .with_overrides(&[])
                .assured("every generated setting renders as an option the command line reads");
            assert_eq!(
                read, settings,
                "every setting of every group survives its command line"
            );

            let duration = generated.duration();
            let read_duration = HumanDuration(duration)
                .to_string()
                .parse::<HumanDuration>()
                .assured("a rendered duration is duration text");
            assert_eq!(read_duration, HumanDuration(duration));

            let bytes = generated.bytes();
            let read_bytes = HumanBytes(bytes)
                .to_string()
                .parse::<HumanBytes>()
                .assured("a rendered byte quantity is byte text");
            assert_eq!(read_bytes, HumanBytes(bytes));
        });
}
