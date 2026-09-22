//! WebSocket source transport and signaling protocol execution.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** WebSocket client connections and the compiled signaling protocol shared by client
//!   sources and server endpoint sessions.
//! - **Depends on.** The connector contract, WebSocket transport, jaq engine, protobuf reflection,
//!   vocabulary values, `error-stack`, and Tokio.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, endpoint
//!   routing, or another connector implementation.

mod signaling;
mod source;

pub use signaling::{
    CompiledSignalingProtocol, SignalingDataSink, SignalingProtobufDescriptors,
    SignalingProtocolCompileError, WebsocketSignalingError, WebsocketSignalingSession,
};
pub use source::{
    WebsocketSource, WebsocketSourceMessage, WebsocketSourcePlan, WebsocketSourcePlanError,
};
