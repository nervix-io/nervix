//! Outbound WebSocket ingestor transport and shared signaling execution.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The outbound client's transport requirements, ingestor lifecycle, configuration and
//!   frames, and signaling compilation and execution shared with server endpoint sessions.
//! - **Depends on.** The connector contract, typed plans and host services, vocabulary values,
//!   execution primitives, WebSocket transport, jaq, protobuf reflection, and contextual errors.
//! - **Must not know.** Configuration Models, Plexus, runtime collectors, relays, branches,
//!   schedules, registry state, endpoint routing or listeners, or another connector implementation.
//!
//! Current boundary violation: the signaling compiler consumes configuration Model steps and
//! parses their timeout text. Converting those inputs into a typed signaling plan belongs to the
//! decision layer; this engine's ownership contract does not grant permission to consume Models.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies a validated connector plan")
)]

mod ingestor;
mod signaling;

pub use ingestor::{
    WebsocketsConnectionIngestor, WebsocketsConnectionIngestorMessage,
    WebsocketsConnectionIngestorPlan, WebsocketsConnectionIngestorPlanError,
};
pub use signaling::{
    CompiledSignalingProtocol, SignalingDataEmitter, SignalingFrameEncodeError,
    SignalingProtobufDescriptors, SignalingProtocolCompileError, WebsocketDataFrame,
    WebsocketSignalingError, WebsocketSignalingSession,
};
