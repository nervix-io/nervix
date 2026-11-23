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
    #[arg(id = MaxConnections::NAME, long = MaxConnections::NAME, env = "NERVIX_LISTENER_MAX_CONNECTIONS", help = super::settings::capability_help::<MaxConnections>("Connections each public TCP listener admits, including upgraded WebSockets and TLS handshakes", <MaxConnections as SettingDefinition>::default_value(&())))]
    pub max_connections: Option<u32>,
    #[arg(id = HeaderBytes::NAME, long = HeaderBytes::NAME, env = "NERVIX_LISTENER_HEADER_BYTES", help = super::settings::capability_help::<HeaderBytes>("Largest HTTP/1 request head and chunked trailers", HumanBytes(<HeaderBytes as SettingDefinition>::default_value(&()))))]
    pub header_bytes: Option<HumanBytes>,
    #[arg(id = MaxHeaders::NAME, long = MaxHeaders::NAME, env = "NERVIX_LISTENER_MAX_HEADERS", help = super::settings::derived_capability_help::<MaxHeaders>("Headers in one HTTP/1 request head, WebSocket upgrades included; at most the three-byte header lines listener-header-bytes holds"))]
    pub max_headers: Option<u32>,
    #[arg(id = BufferBytes::NAME, long = BufferBytes::NAME, env = "NERVIX_LISTENER_BUFFER_BYTES", help = super::settings::capability_help::<BufferBytes>("HTTP/1 read buffer, and the largest body copy made on the connection; at least 8KiB", HumanBytes(<BufferBytes as SettingDefinition>::default_value(&()))))]
    pub buffer_bytes: Option<HumanBytes>,
    #[arg(id = HeaderTimeout::NAME, long = HeaderTimeout::NAME, env = "NERVIX_LISTENER_HEADER_TIMEOUT", help = super::settings::capability_help::<HeaderTimeout>("Deadline of a complete HTTP/1 request head", HumanDuration(<HeaderTimeout as SettingDefinition>::default_value(&()))))]
    pub header_timeout: Option<HumanDuration>,
    #[arg(id = TlsTimeout::NAME, long = TlsTimeout::NAME, env = "NERVIX_LISTENER_TLS_TIMEOUT", help = super::settings::capability_help::<TlsTimeout>("Deadline of an HTTPS, console TLS or gRPC TLS handshake", HumanDuration(<TlsTimeout as SettingDefinition>::default_value(&()))))]
    pub tls_timeout: Option<HumanDuration>,
    #[arg(id = IdleTimeout::NAME, long = IdleTimeout::NAME, env = "NERVIX_LISTENER_IDLE_TIMEOUT", help = super::settings::capability_help::<IdleTimeout>("How long accepted public I/O may make no read or write progress", HumanDuration(<IdleTimeout as SettingDefinition>::default_value(&()))))]
    pub idle_timeout: Option<HumanDuration>,
    #[arg(id = AcceptRetryDelay::NAME, long = AcceptRetryDelay::NAME, env = "NERVIX_LISTENER_ACCEPT_RETRY_DELAY", help = super::settings::capability_help::<AcceptRetryDelay>("Pause after a node-local accept failure, on public and interconnect listeners", HumanDuration(<AcceptRetryDelay as SettingDefinition>::default_value(&()))))]
    pub accept_retry_delay: Option<HumanDuration>,
    #[arg(id = HttpKeepalive::NAME, long = HttpKeepalive::NAME, env = "NERVIX_LISTENER_HTTP_KEEPALIVE", help = super::settings::capability_help::<HttpKeepalive>("Whether HTTP/1 connections are reused", <HttpKeepalive as SettingDefinition>::default_value(&())))]
    pub http_keepalive: Option<bool>,
    #[arg(id = HttpBodyBytes::NAME, long = HttpBodyBytes::NAME, env = "NERVIX_HTTP_BODY_BYTES", help = super::settings::capability_help::<HttpBodyBytes>("Largest complete HTTP endpoint payload, including chunked input", HumanBytes(<HttpBodyBytes as SettingDefinition>::default_value(&()))))]
    pub http_body_bytes: Option<HumanBytes>,
    #[arg(id = HttpBodyTimeout::NAME, long = HttpBodyTimeout::NAME, env = "NERVIX_HTTP_BODY_TIMEOUT", help = super::settings::capability_help::<HttpBodyTimeout>("Deadline of a complete HTTP input, including moving its body to a larger buffer", HumanDuration(<HttpBodyTimeout as SettingDefinition>::default_value(&()))))]
    pub http_body_timeout: Option<HumanDuration>,
    #[arg(id = HttpRefusalDrainTimeout::NAME, long = HttpRefusalDrainTimeout::NAME, env = "NERVIX_HTTP_REFUSAL_DRAIN_TIMEOUT", help = super::settings::capability_help::<HttpRefusalDrainTimeout>("Transport drain after a completed HTTP input refusal", HumanDuration(<HttpRefusalDrainTimeout as SettingDefinition>::default_value(&()))))]
    pub http_refusal_drain_timeout: Option<HumanDuration>,
    #[arg(id = HttpBodyCopyBytes::NAME, long = HttpBodyCopyBytes::NAME, env = "NERVIX_HTTP_BODY_COPY_BYTES", help = super::settings::capability_help::<HttpBodyCopyBytes>("Body bytes a data worker moves between cancellation checks", HumanBytes(<HttpBodyCopyBytes as SettingDefinition>::default_value(&()))))]
    pub http_body_copy_bytes: Option<HumanBytes>,
    #[arg(id = WebsocketMessageBytes::NAME, long = WebsocketMessageBytes::NAME, env = "NERVIX_WEBSOCKET_MESSAGE_BYTES", help = super::settings::capability_help::<WebsocketMessageBytes>("Largest ingest endpoint WebSocket message", HumanBytes(<WebsocketMessageBytes as SettingDefinition>::default_value(&()))))]
    pub websocket_message_bytes: Option<HumanBytes>,
    #[arg(id = WebsocketFrameBytes::NAME, long = WebsocketFrameBytes::NAME, env = "NERVIX_WEBSOCKET_FRAME_BYTES", help = super::settings::capability_help::<WebsocketFrameBytes>("Largest ingest endpoint WebSocket frame", HumanBytes(<WebsocketFrameBytes as SettingDefinition>::default_value(&()))))]
    pub websocket_frame_bytes: Option<HumanBytes>,
    #[arg(id = WebsocketWriteBufferBytes::NAME, long = WebsocketWriteBufferBytes::NAME, env = "NERVIX_WEBSOCKET_WRITE_BUFFER_BYTES", help = super::settings::capability_help::<WebsocketWriteBufferBytes>("Outgoing bytes an endpoint or console WebSocket collects before writing", HumanBytes(<WebsocketWriteBufferBytes as SettingDefinition>::default_value(&()))))]
    pub websocket_write_buffer_bytes: Option<HumanBytes>,
    #[arg(id = GrpcConcurrency::NAME, long = GrpcConcurrency::NAME, env = "NERVIX_GRPC_CONCURRENCY", help = super::settings::capability_help::<GrpcConcurrency>("Concurrent request handlers per gRPC connection", <GrpcConcurrency as SettingDefinition>::default_value(&())))]
    pub grpc_concurrency: Option<u32>,
    #[arg(id = GrpcHeaderBytes::NAME, long = GrpcHeaderBytes::NAME, env = "NERVIX_GRPC_HEADER_BYTES", help = super::settings::capability_help::<GrpcHeaderBytes>("Largest HTTP/2 header list", HumanBytes(<GrpcHeaderBytes as SettingDefinition>::default_value(&()))))]
    pub grpc_header_bytes: Option<HumanBytes>,
    #[arg(id = GrpcRequestTimeout::NAME, long = GrpcRequestTimeout::NAME, env = "NERVIX_GRPC_REQUEST_TIMEOUT", help = super::settings::capability_help::<GrpcRequestTimeout>("gRPC handler response deadline; an established streaming response keeps its lifetime", HumanDuration(<GrpcRequestTimeout as SettingDefinition>::default_value(&()))))]
    pub grpc_request_timeout: Option<HumanDuration>,
    #[arg(id = GrpcMaxStreams::NAME, long = GrpcMaxStreams::NAME, env = "NERVIX_GRPC_MAX_STREAMS", help = super::settings::capability_help::<GrpcMaxStreams>("HTTP/2 streams per gRPC connection", <GrpcMaxStreams as SettingDefinition>::default_value(&())))]
    pub grpc_max_streams: Option<u32>,
    #[arg(id = GrpcStreamWindowBytes::NAME, long = GrpcStreamWindowBytes::NAME, env = "NERVIX_GRPC_STREAM_WINDOW_BYTES", help = super::settings::capability_help::<GrpcStreamWindowBytes>("HTTP/2 receive window of one gRPC stream", HumanBytes(<GrpcStreamWindowBytes as SettingDefinition>::default_value(&()))))]
    pub grpc_stream_window_bytes: Option<HumanBytes>,
    #[arg(id = GrpcConnectionWindowBytes::NAME, long = GrpcConnectionWindowBytes::NAME, env = "NERVIX_GRPC_CONNECTION_WINDOW_BYTES", help = super::settings::capability_help::<GrpcConnectionWindowBytes>("HTTP/2 receive window of one gRPC connection; at least 65535 bytes", HumanBytes(<GrpcConnectionWindowBytes as SettingDefinition>::default_value(&()))))]
    pub grpc_connection_window_bytes: Option<HumanBytes>,
    #[arg(id = GrpcSendBufferBytes::NAME, long = GrpcSendBufferBytes::NAME, env = "NERVIX_GRPC_SEND_BUFFER_BYTES", help = super::settings::capability_help::<GrpcSendBufferBytes>("Response bytes one gRPC stream queues before it waits", HumanBytes(<GrpcSendBufferBytes as SettingDefinition>::default_value(&()))))]
    pub grpc_send_buffer_bytes: Option<HumanBytes>,
    #[arg(id = GrpcMaxPendingResetStreams::NAME, long = GrpcMaxPendingResetStreams::NAME, env = "NERVIX_GRPC_MAX_PENDING_RESET_STREAMS", help = super::settings::capability_help::<GrpcMaxPendingResetStreams>("Client-reset gRPC streams that may wait unaccepted before the connection ends", <GrpcMaxPendingResetStreams as SettingDefinition>::default_value(&())))]
    pub grpc_max_pending_reset_streams: Option<u32>,
    #[arg(id = GrpcMaxErrorResetStreams::NAME, long = GrpcMaxErrorResetStreams::NAME, env = "NERVIX_GRPC_MAX_ERROR_RESET_STREAMS", help = super::settings::capability_help::<GrpcMaxErrorResetStreams>("gRPC streams reset for a client's protocol errors before the connection ends", <GrpcMaxErrorResetStreams as SettingDefinition>::default_value(&())))]
    pub grpc_max_error_reset_streams: Option<u32>,
    #[arg(id = GrpcKeepaliveInterval::NAME, long = GrpcKeepaliveInterval::NAME, env = "NERVIX_GRPC_KEEPALIVE_INTERVAL", help = super::settings::capability_help::<GrpcKeepaliveInterval>("HTTP/2 server ping interval", HumanDuration(<GrpcKeepaliveInterval as SettingDefinition>::default_value(&()))))]
    pub grpc_keepalive_interval: Option<HumanDuration>,
    #[arg(id = GrpcKeepaliveTimeout::NAME, long = GrpcKeepaliveTimeout::NAME, env = "NERVIX_GRPC_KEEPALIVE_TIMEOUT", help = super::settings::capability_help::<GrpcKeepaliveTimeout>("HTTP/2 ping acknowledgement budget", HumanDuration(<GrpcKeepaliveTimeout as SettingDefinition>::default_value(&()))))]
    pub grpc_keepalive_timeout: Option<HumanDuration>,
    #[arg(id = TcpKeepaliveTime::NAME, long = TcpKeepaliveTime::NAME, env = "NERVIX_LISTENER_TCP_KEEPALIVE_TIME", help = super::settings::capability_help::<TcpKeepaliveTime>("Kernel TCP keepalive idle time", HumanDuration(<TcpKeepaliveTime as SettingDefinition>::default_value(&()))))]
    pub tcp_keepalive_time: Option<HumanDuration>,
    #[arg(id = TcpKeepaliveInterval::NAME, long = TcpKeepaliveInterval::NAME, env = "NERVIX_LISTENER_TCP_KEEPALIVE_INTERVAL", help = super::settings::capability_help::<TcpKeepaliveInterval>("Kernel TCP keepalive probe interval", HumanDuration(<TcpKeepaliveInterval as SettingDefinition>::default_value(&()))))]
    pub tcp_keepalive_interval: Option<HumanDuration>,
    #[arg(id = TcpKeepaliveRetries::NAME, long = TcpKeepaliveRetries::NAME, env = "NERVIX_LISTENER_TCP_KEEPALIVE_RETRIES", help = super::settings::capability_help::<TcpKeepaliveRetries>("Kernel TCP keepalive unanswered probe count", <TcpKeepaliveRetries as SettingDefinition>::default_value(&())))]
    pub tcp_keepalive_retries: Option<u32>,
}
impl ListenerArgs {
    pub(super) fn settings(&self) -> Result<ListenerSettings, Report<ListenerSettingsError>> {
        ListenerSettings::resolve(ListenerOverrides {
            max_connections: self.max_connections,
            header_bytes: self.header_bytes.map(|v| v.0),
            max_headers: self.max_headers,
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
            websocket_write_buffer_bytes: self.websocket_write_buffer_bytes.map(|v| v.0),
            grpc_header_bytes: self.grpc_header_bytes.map(|v| v.0),
            grpc_concurrency: self.grpc_concurrency,
            grpc_request_timeout: self.grpc_request_timeout.map(|v| v.0),
            grpc_max_streams: self.grpc_max_streams,
            grpc_stream_window_bytes: self.grpc_stream_window_bytes.map(|v| v.0),
            grpc_connection_window_bytes: self.grpc_connection_window_bytes.map(|v| v.0),
            grpc_send_buffer_bytes: self.grpc_send_buffer_bytes.map(|v| v.0),
            grpc_max_pending_reset_streams: self.grpc_max_pending_reset_streams,
            grpc_max_error_reset_streams: self.grpc_max_error_reset_streams,
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
            max_headers: Some(settings.max_headers()),
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
            websocket_write_buffer_bytes: Some(HumanBytes(settings.websocket_write_buffer_bytes())),
            grpc_header_bytes: Some(HumanBytes(settings.grpc_header_bytes())),
            grpc_concurrency: Some(settings.grpc_concurrency()),
            grpc_request_timeout: Some(HumanDuration(settings.grpc_request_timeout())),
            grpc_max_streams: Some(settings.grpc_max_streams()),
            grpc_stream_window_bytes: Some(HumanBytes(settings.grpc_stream_window_bytes())),
            grpc_connection_window_bytes: Some(HumanBytes(settings.grpc_connection_window_bytes())),
            grpc_send_buffer_bytes: Some(HumanBytes(settings.grpc_send_buffer_bytes())),
            grpc_max_pending_reset_streams: Some(settings.grpc_max_pending_reset_streams()),
            grpc_max_error_reset_streams: Some(settings.grpc_max_error_reset_streams()),
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
        command_line.optional(MaxHeaders::NAME, self.max_headers);
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
        command_line.optional(
            WebsocketWriteBufferBytes::NAME,
            self.websocket_write_buffer_bytes,
        );
        command_line.optional(GrpcHeaderBytes::NAME, self.grpc_header_bytes);
        command_line.optional(GrpcConcurrency::NAME, self.grpc_concurrency);
        command_line.optional(GrpcRequestTimeout::NAME, self.grpc_request_timeout);
        command_line.optional(GrpcMaxStreams::NAME, self.grpc_max_streams);
        command_line.optional(GrpcStreamWindowBytes::NAME, self.grpc_stream_window_bytes);
        command_line.optional(
            GrpcConnectionWindowBytes::NAME,
            self.grpc_connection_window_bytes,
        );
        command_line.optional(GrpcSendBufferBytes::NAME, self.grpc_send_buffer_bytes);
        command_line.optional(
            GrpcMaxPendingResetStreams::NAME,
            self.grpc_max_pending_reset_streams,
        );
        command_line.optional(
            GrpcMaxErrorResetStreams::NAME,
            self.grpc_max_error_reset_streams,
        );
        command_line.optional(GrpcKeepaliveInterval::NAME, self.grpc_keepalive_interval);
        command_line.optional(GrpcKeepaliveTimeout::NAME, self.grpc_keepalive_timeout);
        command_line.optional(TcpKeepaliveTime::NAME, self.tcp_keepalive_time);
        command_line.optional(TcpKeepaliveInterval::NAME, self.tcp_keepalive_interval);
        command_line.optional(TcpKeepaliveRetries::NAME, self.tcp_keepalive_retries);
    }
}
