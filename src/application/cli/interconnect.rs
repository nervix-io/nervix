//! The node's declared interconnect options at the command-line edge.
//!
//! Layer: edges.
//! - **Owns.** Reading and rendering transport setting inputs without choosing their defaults.
//! - **Depends on.** Transport declarations and the shared human-readable edge quantities.
//! - **Must not know.** Peer connection, request admission or runtime ownership.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns interconnect argument conversion")
)]

use error_stack::Report;
use nervix_interconnect::{TransportSettingValues, TransportSettings, TransportSettingsError};
use nervix_models::SettingDefinition as _;

use super::settings::CommandLine;
use crate::application::cli_values::{HumanBytes, HumanDuration};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Interconnect")]
pub struct InterconnectArgs {
    #[arg(long = nervix_interconnect::ReconnectJitterPermille::NAME, env = "NERVIX_INTERCONNECT_RECONNECT_JITTER_PERMILLE", help = super::settings::capability_help::<nervix_interconnect::ReconnectJitterPermille>("Reconnect jitter permille", nervix_interconnect::ReconnectJitterPermille::default_value(&())))]
    pub reconnect_jitter_permille: Option<u16>,
    #[arg(long = nervix_interconnect::MaxPeers::NAME, env = "NERVIX_INTERCONNECT_MAX_PEERS", help = super::settings::capability_help::<nervix_interconnect::MaxPeers>("Max peers", nervix_interconnect::MaxPeers::default_value(&())))]
    pub max_peers: Option<usize>,
    #[arg(long = nervix_interconnect::ConcurrentHandshakes::NAME, env = "NERVIX_INTERCONNECT_CONCURRENT_HANDSHAKES", help = super::settings::capability_help::<nervix_interconnect::ConcurrentHandshakes>("Concurrent handshakes", nervix_interconnect::ConcurrentHandshakes::default_value(&())))]
    pub concurrent_handshakes: Option<usize>,
    #[arg(long = nervix_interconnect::IncomingQueueCapacity::NAME, env = "NERVIX_INTERCONNECT_INCOMING_QUEUE_CAPACITY", help = super::settings::capability_help::<nervix_interconnect::IncomingQueueCapacity>("Incoming queue capacity", nervix_interconnect::IncomingQueueCapacity::default_value(&())))]
    pub incoming_queue_capacity: Option<usize>,
    #[arg(long = nervix_interconnect::RequestQuota::NAME, env = "NERVIX_INTERCONNECT_REQUEST_QUOTA", help = super::settings::capability_help::<nervix_interconnect::RequestQuota>("Request quota", "derived"))]
    pub request_quota: Option<usize>,
    #[arg(long = nervix_interconnect::StreamReceiveWindow::NAME, env = "NERVIX_INTERCONNECT_STREAM_RECEIVE_WINDOW", help = super::settings::capability_help::<nervix_interconnect::StreamReceiveWindow>("Stream receive window", HumanBytes(nervix_interconnect::StreamReceiveWindow::default_value(&()))))]
    pub stream_receive_window: Option<HumanBytes>,
    #[arg(long = nervix_interconnect::ConnectionReceiveWindow::NAME, env = "NERVIX_INTERCONNECT_CONNECTION_RECEIVE_WINDOW", help = super::settings::capability_help::<nervix_interconnect::ConnectionReceiveWindow>("Connection receive window", HumanBytes(nervix_interconnect::ConnectionReceiveWindow::default_value(&()))))]
    pub connection_receive_window: Option<HumanBytes>,
    #[arg(long = nervix_interconnect::ConnectionSetupTimeout::NAME, env = "NERVIX_INTERCONNECT_CONNECTION_SETUP_TIMEOUT", help = super::settings::capability_help::<nervix_interconnect::ConnectionSetupTimeout>("Connection setup timeout", HumanDuration(nervix_interconnect::ConnectionSetupTimeout::default_value(&()))))]
    pub connection_setup_timeout: Option<HumanDuration>,
    #[arg(long = nervix_interconnect::RequestTimeout::NAME, env = "NERVIX_INTERCONNECT_REQUEST_TIMEOUT", help = super::settings::capability_help::<nervix_interconnect::RequestTimeout>("Request timeout", HumanDuration(nervix_interconnect::RequestTimeout::default_value(&()))))]
    pub request_timeout: Option<HumanDuration>,
    #[arg(long = nervix_interconnect::ProgressTimeout::NAME, env = "NERVIX_INTERCONNECT_PROGRESS_TIMEOUT", help = super::settings::capability_help::<nervix_interconnect::ProgressTimeout>("Progress timeout", HumanDuration(nervix_interconnect::ProgressTimeout::default_value(&()))))]
    pub progress_timeout: Option<HumanDuration>,
    #[arg(long = nervix_interconnect::ReconnectBackoff::NAME, env = "NERVIX_INTERCONNECT_RECONNECT_BACKOFF", help = super::settings::capability_help::<nervix_interconnect::ReconnectBackoff>("Reconnect backoff", HumanDuration(nervix_interconnect::ReconnectBackoff::default_value(&()))))]
    pub reconnect_backoff: Option<HumanDuration>,
    #[arg(long = nervix_interconnect::MaxReconnectBackoff::NAME, env = "NERVIX_INTERCONNECT_MAX_RECONNECT_BACKOFF", help = super::settings::capability_help::<nervix_interconnect::MaxReconnectBackoff>("Max reconnect backoff", HumanDuration(nervix_interconnect::MaxReconnectBackoff::default_value(&()))))]
    pub max_reconnect_backoff: Option<HumanDuration>,
}

impl InterconnectArgs {
    pub(super) fn settings(&self) -> Result<TransportSettings, Report<TransportSettingsError>> {
        TransportSettings::resolve(TransportSettingValues {
            reconnect_jitter_permille: self.reconnect_jitter_permille,
            max_peers: self.max_peers,
            concurrent_handshakes: self.concurrent_handshakes,
            incoming_queue_capacity: self.incoming_queue_capacity,
            request_quota: self.request_quota,
            stream_receive_window: self.stream_receive_window.map(|value| value.0),
            connection_receive_window: self.connection_receive_window.map(|value| value.0),
            connection_setup_timeout: self.connection_setup_timeout.map(|value| value.0),
            request_timeout: self.request_timeout.map(|value| value.0),
            progress_timeout: self.progress_timeout.map(|value| value.0),
            reconnect_backoff: self.reconnect_backoff.map(|value| value.0),
            max_reconnect_backoff: self.max_reconnect_backoff.map(|value| value.0),
        })
    }

    pub(super) fn from_settings(settings: &TransportSettings) -> Self {
        let values = settings.values();
        Self {
            reconnect_jitter_permille: values.reconnect_jitter_permille,
            max_peers: values.max_peers,
            concurrent_handshakes: values.concurrent_handshakes,
            incoming_queue_capacity: values.incoming_queue_capacity,
            request_quota: values.request_quota,
            stream_receive_window: values.stream_receive_window.map(HumanBytes),
            connection_receive_window: values.connection_receive_window.map(HumanBytes),
            connection_setup_timeout: values.connection_setup_timeout.map(HumanDuration),
            request_timeout: values.request_timeout.map(HumanDuration),
            progress_timeout: values.progress_timeout.map(HumanDuration),
            reconnect_backoff: values.reconnect_backoff.map(HumanDuration),
            max_reconnect_backoff: values.max_reconnect_backoff.map(HumanDuration),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.optional(
            nervix_interconnect::ReconnectJitterPermille::NAME,
            self.reconnect_jitter_permille,
        );
        command_line.optional(nervix_interconnect::MaxPeers::NAME, self.max_peers);
        command_line.optional(
            nervix_interconnect::ConcurrentHandshakes::NAME,
            self.concurrent_handshakes,
        );
        command_line.optional(
            nervix_interconnect::IncomingQueueCapacity::NAME,
            self.incoming_queue_capacity,
        );
        command_line.optional(nervix_interconnect::RequestQuota::NAME, self.request_quota);
        command_line.optional(
            nervix_interconnect::StreamReceiveWindow::NAME,
            self.stream_receive_window,
        );
        command_line.optional(
            nervix_interconnect::ConnectionReceiveWindow::NAME,
            self.connection_receive_window,
        );
        command_line.optional(
            nervix_interconnect::ConnectionSetupTimeout::NAME,
            self.connection_setup_timeout,
        );
        command_line.optional(
            nervix_interconnect::RequestTimeout::NAME,
            self.request_timeout,
        );
        command_line.optional(
            nervix_interconnect::ProgressTimeout::NAME,
            self.progress_timeout,
        );
        command_line.optional(
            nervix_interconnect::ReconnectBackoff::NAME,
            self.reconnect_backoff,
        );
        command_line.optional(
            nervix_interconnect::MaxReconnectBackoff::NAME,
            self.max_reconnect_backoff,
        );
    }
}
