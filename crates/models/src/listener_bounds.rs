//! The public listener capacity a concrete node announces.
//!
//! Layer: vocabulary.
//! - **Owns.** The resolved connection, input, deadline and keepalive claims of one node.
//! - **Depends on.** Positive quantities, durations and serialization.
//! - **Must not know.** Defaults, sockets, admission, detection or running services.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns a node's resolved listener capacity claim")
)]

use std::{
    num::{NonZeroU32, NonZeroU64},
    time::Duration,
};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// Bounds shared by a node's public listeners; protocol-specific fields apply to that protocol.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ListenerBounds {
    pub max_connections: NonZeroU32,
    pub buffer_bytes: NonZeroU64,
    pub header_bytes: NonZeroU64,
    pub header_timeout: Duration,
    pub tls_timeout: Duration,
    pub idle_timeout: Duration,
    pub accept_retry_delay: Duration,
    pub http_keepalive: bool,
    pub http_body_bytes: NonZeroU64,
    pub http_body_copy_bytes: NonZeroU64,
    pub http_body_timeout: Duration,
    pub http_refusal_drain_timeout: Duration,
    pub websocket_message_bytes: NonZeroU64,
    pub websocket_frame_bytes: NonZeroU64,
    pub grpc_header_bytes: NonZeroU64,
    pub grpc_concurrency: NonZeroU32,
    pub grpc_request_timeout: Duration,
    pub grpc_max_streams: NonZeroU32,
    pub grpc_keepalive_interval: Duration,
    pub grpc_keepalive_timeout: Duration,
    pub tcp_keepalive_time: Duration,
    pub tcp_keepalive_interval: Duration,
    pub tcp_keepalive_retries: NonZeroU32,
}
