//! Turning parsed arguments into a configured node.
//!
//! Layer: edges.
//!
//! - **Owns.** Reading the argument set into the listeners, stores and cluster identity one
//!   `Application` holds, and releasing what it opened when a later step fails.
//! - **Depends on.** The argument definition and the stores it opens.
//! - **Must not know.** What the configured node goes on to run.

use std::{net::SocketAddr, path::PathBuf};

use error_stack::{Report, ResultExt};
use fjall::Database;
use nervix_consensus::{Consensus, RaftRetentionPolicy};
use nervix_interconnect::Transport;
use triomphe::Arc;

use super::{Application, Args, error, error::AppError};
use crate::{
    cluster,
    memory_pressure::MemoryPressureConfig,
    registry::Registry,
    resource::{ResourceStore, ResourceStoreLimits},
    runtime::Runtime,
};

pub(in crate::application) struct ApplicationStartup {
    pub(in crate::application) db: Database,
    pub(in crate::application) resource_store: Arc<ResourceStore>,
    pub(in crate::application) registry: Arc<Registry>,
    pub(in crate::application) runtime: Runtime,
    pub(in crate::application) consensus: Option<Consensus>,
    pub(in crate::application) interconnect: Option<Transport>,
}

impl ApplicationStartup {
    pub(in crate::application) async fn terminate(self) {
        self.runtime.shutdown().await;
        if let Some(consensus) = &self.consensus {
            consensus.shutdown().await;
        }
        if let Some(interconnect) = &self.interconnect {
            interconnect.shutdown().await;
        }
        if let Err(error) = tokio::task::spawn_blocking(move || drop(self)).await {
            error!(error = %error, "failed to join application startup cleanup task");
        }
    }
}

impl TryFrom<Args> for Application {
    type Error = Report<AppError>;

    fn try_from(args: Args) -> Result<Self, Self::Error> {
        let addr = args
            .addr
            .parse::<SocketAddr>()
            .change_context(AppError::ParseAddress)?;
        let grpc_https_listen_addr = args
            .grpc_https_listen_addr
            .as_deref()
            .map(|addr| {
                addr.parse::<SocketAddr>()
                    .change_context(AppError::ParseGrpcHttpsListenAddress)
            })
            .transpose()?;
        let grpc_advertise_addr = match args.grpc_advertise_addr.as_deref() {
            Some(addr) => addr.parse::<cluster::HostPort>().map_err(|error| {
                Report::new(AppError::ParseGrpcAdvertiseAddress).attach_printable(error)
            })?,
            None => addr.into(),
        };
        let grpc_https_advertise_addr = args
            .grpc_https_advertise_addr
            .as_deref()
            .map(|addr| {
                addr.parse::<cluster::HostPort>().map_err(|error| {
                    Report::new(AppError::ParseGrpcHttpsAdvertiseAddress).attach_printable(error)
                })
            })
            .transpose()?;
        let http_listen_addr = args
            .http_listen_addr
            .parse::<SocketAddr>()
            .change_context(AppError::ParseHttpListenAddress)?;
        let https_listen_addr = args
            .https_listen_addr
            .parse::<SocketAddr>()
            .change_context(AppError::ParseHttpsListenAddress)?;
        let observability_listen_addr = args
            .observability_listen_addr
            .parse::<SocketAddr>()
            .change_context(AppError::ParseObservabilityListenAddress)?;
        let web_console_listen_addr = args
            .web_console_listen_addr
            .parse::<SocketAddr>()
            .change_context(AppError::ParseWebConsoleListenAddress)?;
        let web_console_advertise_addr = args
            .web_console_advertise_addr
            .as_deref()
            .map(|addr| {
                addr.parse::<cluster::HostPort>().map_err(|error| {
                    Report::new(AppError::ParseWebConsoleListenAddress).attach_printable(error)
                })
            })
            .transpose()?;
        let web_console_https_listen_addr = args
            .web_console_https_listen_addr
            .as_deref()
            .map(|addr| {
                addr.parse::<SocketAddr>()
                    .change_context(AppError::ParseWebConsoleHttpsListenAddress)
            })
            .transpose()?;
        let interconnect_listen_addr = match args.interconnect_listen_addr.as_deref() {
            Some(addr) => addr
                .parse::<SocketAddr>()
                .change_context(AppError::ParseInterconnectListenAddress)?,
            None => cluster::derive_peer_addr(addr)
                .ok_or_else(|| Report::new(AppError::DeriveInterconnectAddress))?,
        };
        let interconnect_advertise_addr = match args.interconnect_advertise_addr.as_deref() {
            Some(addr) => addr.parse::<cluster::HostPort>().map_err(|error| {
                Report::new(AppError::ParseInterconnectAdvertiseAddress).attach_printable(error)
            })?,
            None => {
                let port = grpc_advertise_addr
                    .port()
                    .checked_add(1)
                    .ok_or_else(|| Report::new(AppError::DeriveInterconnectAddress))?;
                grpc_advertise_addr.with_port(port)
            }
        };
        let memory_pressure = match (args.memory_high_watermark, args.memory_low_watermark) {
            (Some(high_watermark), Some(low_watermark)) => {
                let config = MemoryPressureConfig::builder()
                    .high_watermark(high_watermark)
                    .low_watermark(low_watermark)
                    .check_interval(args.memory_pressure_check_interval)
                    .resume_jitter(args.memory_pressure_resume_jitter)
                    .build();
                config.validate().map_err(|error| {
                    Report::new(AppError::InvalidMemoryPressureConfig).attach_printable(error)
                })?;
                Some(config)
            }
            (Some(_), None) => {
                return Err(Report::new(AppError::MissingMemoryPressureLowWatermark));
            }
            (None, Some(_)) => {
                return Err(Report::new(AppError::MissingMemoryPressureHighWatermark));
            }
            (None, None) => None,
        };
        Ok(Self::builder()
            .addr(addr)
            .grpc_mode(args.grpc_mode)
            .grpc_https_listen_addr(grpc_https_listen_addr)
            .grpc_https_advertise_addr(grpc_https_advertise_addr)
            .http_listen_addr(http_listen_addr)
            .https_listen_addr(https_listen_addr)
            .observability_listen_addr(observability_listen_addr)
            .web_console_listen_addr(web_console_listen_addr)
            .web_console_advertise_addr(web_console_advertise_addr)
            .web_console_https_listen_addr(web_console_https_listen_addr)
            .web_console_tls_cert(args.web_console_tls_cert)
            .web_console_tls_key(args.web_console_tls_key)
            .cluster_id(args.cluster_id)
            .node_id(args.node_id)
            .grpc_advertise_addr(grpc_advertise_addr)
            .interconnect_listen_addr(interconnect_listen_addr)
            .interconnect_advertise_addr(interconnect_advertise_addr)
            .interconnect_tls_ca(args.interconnect_tls_ca)
            .interconnect_tls_cert(args.interconnect_tls_cert)
            .interconnect_tls_key(args.interconnect_tls_key)
            .allow_bootstrap(args.allow_bootstrap)
            .default_user(args.default_user)
            .init_default_user_password(args.init_default_user_password)
            .node_unavailability_timeout(args.node_unavailability_timeout)
            .raft_heartbeat_interval(args.raft_heartbeat_interval)
            .raft_election_timeout_min(args.raft_election_timeout_min)
            .raft_election_timeout_max(args.raft_election_timeout_max)
            .raft_retention(RaftRetentionPolicy {
                snapshot_entry_threshold: args.raft_snapshot_entry_threshold,
                snapshot_byte_threshold: args.raft_snapshot_byte_threshold.as_u64(),
                covered_entries_retained: args.raft_covered_log_entries_retained,
                covered_bytes_retained: args.raft_covered_log_bytes_retained.as_u64(),
                retained_log_cap_bytes: args.raft_retained_log_cap.as_u64(),
                ..RaftRetentionPolicy::default()
            })
            .transaction_idle_timeout(args.transaction_idle_timeout)
            .transaction_tombstone_retention(args.transaction_tombstone_retention)
            .transaction_max_statements(args.transaction_max_statements)
            .transaction_max_source_bytes(args.transaction_max_source_bytes)
            .transaction_max_open(args.transaction_max_open)
            .replica_count(args.replica_count)
            .state_snapshot_interval(args.state_snapshot_interval)
            .memory_pressure(memory_pressure)
            .drain_timeout(args.drain_timeout)
            .cluster_bootstrap_host(args.cluster_bootstrap_host)
            .db_path(PathBuf::from(args.db_path))
            .temp_dir(args.temp_dir)
            .resource_store_limits(ResourceStoreLimits {
                max_archive_bytes: args.resource_max_archive_bytes.as_u64(),
                max_extracted_bytes: args.resource_max_extracted_bytes.as_u64(),
                max_file_count: args.resource_max_file_count,
            })
            .build())
    }
}
