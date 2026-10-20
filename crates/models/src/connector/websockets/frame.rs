//! The data-frame kind a WebSockets message retains from its transport.
//!
//! Layer: vocabulary.
//! - **Owns.** The text and binary data-frame kinds exposed as `metadata.frame_type`.
//! - **Depends on.** Declarative string rendering.
//! - **Must not know.** WebSocket framing, payload bytes, connection state or signaling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::AsRefStr, strum::Display)]
#[strum(serialize_all = "lowercase")]
pub enum WebsocketsFrameType {
    Text,
    Binary,
}
