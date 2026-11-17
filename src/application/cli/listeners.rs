//! Command-line inputs for the node's listener bounds.
//!
//! Layer: edges.
//! - **Owns.** Parsing and rendering listener options.
//! - **Depends on.** Listener setting declarations and CLI quantities.
//! - **Must not know.** Sockets, requests or admission.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns listener command-line options")
)]

use error_stack::Report;
use nervix_models::SettingDefinition;

use super::settings::CommandLine;
use crate::application::{
    cli_values::{HumanBytes, HumanDuration},
    listener_settings::*,
};

#[derive(clap::Args, Debug, Clone, Default, PartialEq)]
#[command(next_help_heading = "Listeners")]
pub struct ListenerArgs {
    #[arg(id = MaxConnections::NAME, long = MaxConnections::NAME, env = "NERVIX_LISTENER_MAX_CONNECTIONS", help = "Local listener max connections; omitted values use the declared default")]
    pub max_connections: Option<u32>,
    #[arg(id = HeaderBytes::NAME, long = HeaderBytes::NAME, env = "NERVIX_LISTENER_HEADER_BYTES", help = "Local listener header bytes; omitted values use the declared default")]
    pub header_bytes: Option<HumanBytes>,
    #[arg(id = BufferBytes::NAME, long = BufferBytes::NAME, env = "NERVIX_LISTENER_BUFFER_BYTES", help = "Local listener buffer bytes; omitted values use the declared default")]
    pub buffer_bytes: Option<HumanBytes>,
    #[arg(id = HeaderTimeout::NAME, long = HeaderTimeout::NAME, env = "NERVIX_LISTENER_HEADER_TIMEOUT", help = "Local listener header timeout; omitted values use the declared default")]
    pub header_timeout: Option<HumanDuration>,
    #[arg(id = TlsTimeout::NAME, long = TlsTimeout::NAME, env = "NERVIX_LISTENER_TLS_TIMEOUT", help = "Local listener tls timeout; omitted values use the declared default")]
    pub tls_timeout: Option<HumanDuration>,
    #[arg(id = IdleTimeout::NAME, long = IdleTimeout::NAME, env = "NERVIX_LISTENER_IDLE_TIMEOUT", help = "Local listener idle timeout; omitted values use the declared default")]
    pub idle_timeout: Option<HumanDuration>,
    #[arg(id = AcceptRetryDelay::NAME, long = AcceptRetryDelay::NAME, env = "NERVIX_LISTENER_ACCEPT_RETRY_DELAY", help = "Local listener accept retry delay; omitted values use the declared default")]
    pub accept_retry_delay: Option<HumanDuration>,
    #[arg(id = HttpKeepalive::NAME, long = HttpKeepalive::NAME, env = "NERVIX_LISTENER_HTTP_KEEPALIVE", help = "Local listener http keepalive; omitted values use the declared default")]
    pub http_keepalive: Option<bool>,
    #[arg(id = HttpBodyBytes::NAME, long = HttpBodyBytes::NAME, env = "NERVIX_HTTP_BODY_BYTES", help = "Local http body bytes; omitted values use the declared default")]
    pub http_body_bytes: Option<HumanBytes>,
    #[arg(id = HttpBodyTimeout::NAME, long = HttpBodyTimeout::NAME, env = "NERVIX_HTTP_BODY_TIMEOUT", help = "Local http body timeout; omitted values use the declared default")]
    pub http_body_timeout: Option<HumanDuration>,
    #[arg(id = HttpRefusalDrainTimeout::NAME, long = HttpRefusalDrainTimeout::NAME, env = "NERVIX_HTTP_REFUSAL_DRAIN_TIMEOUT", help = "Transport drain after an HTTP refusal; omitted values use the declared default")]
    pub http_refusal_drain_timeout: Option<HumanDuration>,
    #[arg(id = HttpBodyCopyBytes::NAME, long = HttpBodyCopyBytes::NAME, env = "NERVIX_HTTP_BODY_COPY_BYTES", help = "Maximum body bytes copied between cancellation checks; defaults to 64KiB")]
    pub http_body_copy_bytes: Option<HumanBytes>,
    #[arg(id = WebsocketMessageBytes::NAME, long = WebsocketMessageBytes::NAME, env = "NERVIX_WEBSOCKET_MESSAGE_BYTES", help = "Local websocket message bytes; omitted values use the declared default")]
    pub websocket_message_bytes: Option<HumanBytes>,
    #[arg(id = WebsocketFrameBytes::NAME, long = WebsocketFrameBytes::NAME, env = "NERVIX_WEBSOCKET_FRAME_BYTES", help = "Local websocket frame bytes; omitted values use the declared default")]
    pub websocket_frame_bytes: Option<HumanBytes>,
    #[arg(id = GrpcConcurrency::NAME, long = GrpcConcurrency::NAME, env = "NERVIX_GRPC_CONCURRENCY", help = "Local grpc concurrency; omitted values use the declared default")]
    pub grpc_concurrency: Option<u32>,
    #[arg(id = GrpcHeaderBytes::NAME, long = GrpcHeaderBytes::NAME, env = "NERVIX_GRPC_HEADER_BYTES", help = "HTTP/2 header list byte capacity; defaults to 16KiB")]
    pub grpc_header_bytes: Option<HumanBytes>,
    #[arg(id = GrpcRequestTimeout::NAME, long = GrpcRequestTimeout::NAME, env = "NERVIX_GRPC_REQUEST_TIMEOUT", help = "Local grpc request timeout; omitted values use the declared default")]
    pub grpc_request_timeout: Option<HumanDuration>,
    #[arg(id = GrpcMaxStreams::NAME, long = GrpcMaxStreams::NAME, env = "NERVIX_GRPC_MAX_STREAMS", help = "Local grpc max streams; omitted values use the declared default")]
    pub grpc_max_streams: Option<u32>,
    #[arg(id = GrpcKeepaliveInterval::NAME, long = GrpcKeepaliveInterval::NAME, env = "NERVIX_GRPC_KEEPALIVE_INTERVAL", help = "Local grpc keepalive interval; omitted values use the declared default")]
    pub grpc_keepalive_interval: Option<HumanDuration>,
    #[arg(id = GrpcKeepaliveTimeout::NAME, long = GrpcKeepaliveTimeout::NAME, env = "NERVIX_GRPC_KEEPALIVE_TIMEOUT", help = "Local grpc keepalive timeout; omitted values use the declared default")]
    pub grpc_keepalive_timeout: Option<HumanDuration>,
    #[arg(id = TcpKeepaliveTime::NAME, long = TcpKeepaliveTime::NAME, env = "NERVIX_LISTENER_TCP_KEEPALIVE_TIME", help = "Local listener tcp keepalive time; omitted values use the declared default")]
    pub tcp_keepalive_time: Option<HumanDuration>,
    #[arg(id = TcpKeepaliveInterval::NAME, long = TcpKeepaliveInterval::NAME, env = "NERVIX_LISTENER_TCP_KEEPALIVE_INTERVAL", help = "Local listener tcp keepalive interval; omitted values use the declared default")]
    pub tcp_keepalive_interval: Option<HumanDuration>,
    #[arg(id = TcpKeepaliveRetries::NAME, long = TcpKeepaliveRetries::NAME, env = "NERVIX_LISTENER_TCP_KEEPALIVE_RETRIES", help = "Local listener tcp keepalive retries; omitted values use the declared default")]
    pub tcp_keepalive_retries: Option<u32>,
}
impl ListenerArgs {
    pub(super) fn settings(&self) -> Result<ListenerSettings, Report<ListenerSettingsError>> {
        ListenerSettings::resolve(ListenerOverrides {
            max_connections: self.max_connections,
            header_bytes: self.header_bytes.map(|v| v.0),
            buffer_bytes: self.buffer_bytes.map(|v| v.0),
            header_timeout: self.header_timeout.map(|v| v.0),
            tls_timeout: self.tls_timeout.map(|v| v.0),
            idle_timeout: self.idle_timeout.map(|v| v.0),
            accept_retry_delay: self.accept_retry_delay.map(|v| v.0),
            http_keepalive: self.http_keepalive,
            http_body_bytes: self.http_body_bytes.map(|v| v.0),
            http_body_copy_bytes: self.http_body_copy_bytes.map(|v| v.0),
            http_body_timeout: self.http_body_timeout.map(|v| v.0),
            http_refusal_drain_timeout: self.http_refusal_drain_timeout.map(|v| v.0),
            websocket_message_bytes: self.websocket_message_bytes.map(|v| v.0),
            websocket_frame_bytes: self.websocket_frame_bytes.map(|v| v.0),
            grpc_header_bytes: self.grpc_header_bytes.map(|v| v.0),
            grpc_concurrency: self.grpc_concurrency,
            grpc_request_timeout: self.grpc_request_timeout.map(|v| v.0),
            grpc_max_streams: self.grpc_max_streams,
            grpc_keepalive_interval: self.grpc_keepalive_interval.map(|v| v.0),
            grpc_keepalive_timeout: self.grpc_keepalive_timeout.map(|v| v.0),
            tcp_keepalive_time: self.tcp_keepalive_time.map(|v| v.0),
            tcp_keepalive_interval: self.tcp_keepalive_interval.map(|v| v.0),
            tcp_keepalive_retries: self.tcp_keepalive_retries,
        })
    }
    pub(super) fn from_settings(settings: &ListenerSettings) -> Self {
        Self {
            max_connections: Some(settings.max_connections()),
            header_bytes: Some(HumanBytes(settings.header_bytes())),
            buffer_bytes: Some(HumanBytes(settings.buffer_bytes())),
            header_timeout: Some(HumanDuration(settings.header_timeout())),
            tls_timeout: Some(HumanDuration(settings.tls_timeout())),
            idle_timeout: Some(HumanDuration(settings.idle_timeout())),
            accept_retry_delay: Some(HumanDuration(settings.accept_retry_delay())),
            http_keepalive: Some(settings.http_keepalive()),
            http_body_bytes: Some(HumanBytes(settings.http_body_bytes())),
            http_body_copy_bytes: Some(HumanBytes(settings.http_body_copy_bytes())),
            http_body_timeout: Some(HumanDuration(settings.http_body_timeout())),
            http_refusal_drain_timeout: Some(HumanDuration(settings.http_refusal_drain_timeout())),
            websocket_message_bytes: Some(HumanBytes(settings.websocket_message_bytes())),
            websocket_frame_bytes: Some(HumanBytes(settings.websocket_frame_bytes())),
            grpc_header_bytes: Some(HumanBytes(settings.grpc_header_bytes())),
            grpc_concurrency: Some(settings.grpc_concurrency()),
            grpc_request_timeout: Some(HumanDuration(settings.grpc_request_timeout())),
            grpc_max_streams: Some(settings.grpc_max_streams()),
            grpc_keepalive_interval: Some(HumanDuration(settings.grpc_keepalive_interval())),
            grpc_keepalive_timeout: Some(HumanDuration(settings.grpc_keepalive_timeout())),
            tcp_keepalive_time: Some(HumanDuration(settings.tcp_keepalive_time())),
            tcp_keepalive_interval: Some(HumanDuration(settings.tcp_keepalive_interval())),
            tcp_keepalive_retries: Some(settings.tcp_keepalive_retries()),
        }
    }
    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.optional(MaxConnections::NAME, self.max_connections);
        command_line.optional(HeaderBytes::NAME, self.header_bytes);
        command_line.optional(BufferBytes::NAME, self.buffer_bytes);
        command_line.optional(HeaderTimeout::NAME, self.header_timeout);
        command_line.optional(TlsTimeout::NAME, self.tls_timeout);
        command_line.optional(IdleTimeout::NAME, self.idle_timeout);
        command_line.optional(AcceptRetryDelay::NAME, self.accept_retry_delay);
        command_line.optional(HttpKeepalive::NAME, self.http_keepalive);
        command_line.optional(HttpBodyBytes::NAME, self.http_body_bytes);
        command_line.optional(HttpBodyCopyBytes::NAME, self.http_body_copy_bytes);
        command_line.optional(HttpBodyTimeout::NAME, self.http_body_timeout);
        command_line.optional(
            HttpRefusalDrainTimeout::NAME,
            self.http_refusal_drain_timeout,
        );
        command_line.optional(WebsocketMessageBytes::NAME, self.websocket_message_bytes);
        command_line.optional(WebsocketFrameBytes::NAME, self.websocket_frame_bytes);
        command_line.optional(GrpcHeaderBytes::NAME, self.grpc_header_bytes);
        command_line.optional(GrpcConcurrency::NAME, self.grpc_concurrency);
        command_line.optional(GrpcRequestTimeout::NAME, self.grpc_request_timeout);
        command_line.optional(GrpcMaxStreams::NAME, self.grpc_max_streams);
        command_line.optional(GrpcKeepaliveInterval::NAME, self.grpc_keepalive_interval);
        command_line.optional(GrpcKeepaliveTimeout::NAME, self.grpc_keepalive_timeout);
        command_line.optional(TcpKeepaliveTime::NAME, self.tcp_keepalive_time);
        command_line.optional(TcpKeepaliveInterval::NAME, self.tcp_keepalive_interval);
        command_line.optional(TcpKeepaliveRetries::NAME, self.tcp_keepalive_retries);
    }
}
