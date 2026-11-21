//! The keepalive a server sends for a request it still serves.
//!
//! A client that bounds how long it waits for a request asks the server, in the request's
//! `ClientMessage`, to report at an interval that it still serves the request. Each report is a
//! `RequestProgress` reply. It completes nothing: the client restarts its wait at each one, so a
//! request the server keeps serving for longer than the client's own deadline is still answered,
//! while a server that stops reporting still lets that deadline end the wait.

use std::time::Duration;

use error_stack::Report;
use meticulous::ResultExt as _;

use crate::{
    RequestId, ServerFrame,
    codec::{EncodedUnion, Encoder, WireDecodeError, WireEncodeError},
    frame::EncodedFrame,
    limits::SessionLimits,
    server::finish_server_message,
    wire,
};

/// How often a request asks the server to report that it still serves it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProgressInterval(Duration);

impl ProgressInterval {
    /// The shortest interval a request may ask for, so that one request cannot make its session
    /// send more than ten reports a second.
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "the client session protocol fixes the shortest progress interval a request \
                      may ask for: session.fbs states ClientMessage.progress_interval_ms is at \
                      least 100"
        )
    )]
    pub const SHORTEST: Duration = Duration::from_millis(100);

    /// The interval, or `None` when it is shorter than [`Self::SHORTEST`] or holds more whole
    /// milliseconds than the protocol's field carries.
    pub fn new(interval: Duration) -> Option<Self> {
        if interval < Self::SHORTEST {
            return None;
        }
        let milliseconds = u64::try_from(interval.as_millis()).ok()?;
        Some(Self(Duration::from_millis(milliseconds)))
    }

    /// The interval a client derives from the longest it waits for a reply: four reports fit in
    /// that wait, so a single late report never ends it. An interval below the shortest the
    /// protocol allows is raised to it, and a wait longer than the protocol's field can express
    /// asks for the longest interval the field holds.
    pub fn within(wait: Duration) -> Self {
        let quarter = wait / 4;
        if quarter < Self::SHORTEST {
            return Self(Self::SHORTEST);
        }
        match u64::try_from(quarter.as_millis()) {
            Ok(milliseconds) => Self(Duration::from_millis(milliseconds)),
            Err(_) => Self(Duration::from_millis(u64::MAX)),
        }
    }

    pub fn get(self) -> Duration {
        self.0
    }

    pub(crate) fn wire(self) -> u64 {
        u64::try_from(self.0.as_millis())
            .assured("every constructor keeps the interval within whole u64 milliseconds")
    }

    pub(crate) fn decode(
        field: &'static str,
        milliseconds: Option<u64>,
    ) -> Result<Option<Self>, Report<WireDecodeError>> {
        let Some(milliseconds) = milliseconds else {
            return Ok(None);
        };
        match Self::new(Duration::from_millis(milliseconds)) {
            Some(interval) => Ok(Some(interval)),
            None => Err(Report::new(WireDecodeError::InvalidValue {
                field,
                kind: "progress interval of at least 100 milliseconds",
            })),
        }
    }
}

/// A report that the server still serves the request `request_id` names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestProgress {
    pub request_id: RequestId,
}

impl RequestProgress {
    /// Encodes the report as one server frame, which always fits a frame of any session.
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<ServerFrame>, Report<WireEncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let progress = wire::RequestProgress::create(encoder.fbb(), &wire::RequestProgressArgs {});
        let reply = wire::Reply::create(
            encoder.fbb(),
            &wire::ReplyArgs {
                request_id: self.request_id.wire(),
                body_type: wire::ReplyBody::RequestProgress,
                body: Some(progress.as_union_value()),
            },
        );
        finish_server_message(encoder, EncodedUnion::new(wire::ServerBody::Reply, reply))
    }
}
