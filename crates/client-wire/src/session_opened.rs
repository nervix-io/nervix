//! The checked opening of the current WebSocket session.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The bounded session opening, its limits and pacing, and their complete checked
//!   decode.
//! - **Depends on.** The frame codec, vocabulary bounds and setting definitions, and the fixed
//!   opening envelope.
//! - **Must not know.** Authentication, client recovery or server runtime state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the caller-driven session opening codec")
)]

use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_models::{
    CommandRetryValidity, SessionBoundValues, SessionBounds, SessionSnapshotInterval, Setting,
    WebConsoleCapabilities, WebConsoleCapabilityValues,
};

use crate::{
    EncodedFrame, ServerEvent, ServerFrame, ServerMessage, SessionLimits, VerifiedFrame,
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_size},
    server::finish_server_message,
    wire,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SessionOpeningError {
    #[error("the WebSocket opening is not a bounded server frame")]
    Frame,
    #[error("the WebSocket opening does not decode")]
    Message,
    #[error("the first WebSocket frame must announce its session limits and pacing")]
    Expected,
}

/// What the serving node announces first on every WebSocket: the limits the session's frames keep
/// and the pacing its client follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionOpened {
    pub limits: SessionLimits,
    pub pacing: SessionPacing,
}

/// How the serving node paces a WebSocket session: how often it sends the selected domain's
/// snapshot, how long the cluster recognizes a command's execution reference, and the waits and
/// restore queue of the web console it serves. A client adopts the pacing of the node each opening
/// reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionPacing {
    snapshot_interval: Setting<SessionSnapshotInterval>,
    command_retry_validity: Option<Setting<CommandRetryValidity>>,
    web_console: WebConsoleCapabilities,
}

impl SessionPacing {
    /// `command_retry_validity` is absent while the node has not applied the cluster's committed
    /// parameters.
    pub fn new(
        snapshot_interval: Setting<SessionSnapshotInterval>,
        command_retry_validity: Option<Setting<CommandRetryValidity>>,
        web_console: WebConsoleCapabilities,
    ) -> Self {
        Self {
            snapshot_interval,
            command_retry_validity,
            web_console,
        }
    }

    /// How often the node sends the selected domain's snapshot.
    pub fn snapshot_interval(&self) -> Duration {
        *self.snapshot_interval.value()
    }

    /// How long the cluster recognizes a command's execution reference from the time the
    /// reference was issued, once the node applied the cluster's committed parameters.
    pub fn command_retry_validity(&self) -> Option<Duration> {
        self.command_retry_validity
            .map(|validity| *validity.value())
    }

    /// The waits and restore queue of the web console the node serves.
    pub fn web_console(&self) -> WebConsoleCapabilities {
        self.web_console
    }
}

/// Every value at its definition's default, which is what a client paces itself by before its
/// first opening.
impl Default for SessionPacing {
    fn default() -> Self {
        Self {
            snapshot_interval: Setting::resolve(None, &())
                .assured("the snapshot interval's default is valid"),
            command_retry_validity: Some(
                Setting::resolve(None, &()).assured("the retry validity's default is valid"),
            ),
            web_console: WebConsoleCapabilities::default(),
        }
    }
}

/// The nanoseconds the opening carries for one of its durations.
fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).assured(
        "every announced duration is validated to at most u32::MAX seconds, which u64 nanoseconds \
         hold",
    )
}

/// A required duration of the opening, before its definition checks it.
fn required_nanos(
    decoder: Decoder<'_>,
    field: &'static str,
    value: Option<u64>,
) -> Result<Duration, Report<WireDecodeError>> {
    let nanos = decoder.non_zero(field, decoder.required(field, value)?)?;
    Ok(Duration::from_nanos(nanos.get()))
}

impl SessionOpened {
    /// Verifies the one fixed-envelope opening before an ordinary-frame codec is installed.
    pub fn verify(payload: bytes::Bytes) -> Result<Self, Report<SessionOpeningError>> {
        let frame = VerifiedFrame::<ServerFrame>::verify(payload, &SessionLimits::OPENING)
            .change_context(SessionOpeningError::Frame)?;
        match ServerMessage::decode(&frame).change_context(SessionOpeningError::Message)? {
            ServerMessage::Event(ServerEvent::SessionOpened(opened)) => Ok(opened),
            _ => Err(Report::new(SessionOpeningError::Expected)),
        }
    }

    pub fn encode(&self) -> Result<EncodedFrame<ServerFrame>, Report<WireEncodeError>> {
        let mut encoder = Encoder::new(
            SessionLimits::OPENING.frame_bytes(),
            &SessionLimits::OPENING,
        );
        let web_console = self.pacing.web_console();
        let command_retry_validity_nanos = self.pacing.command_retry_validity().map(nanos);
        let opening = wire::SessionOpened::create(
            encoder.fbb(),
            &wire::SessionOpenedArgs {
                frame_bytes: Some(wire_size(self.limits.frame_bytes())),
                transfer_bytes: Some(wire_size(self.limits.transfer_bytes())),
                nesting_depth: Some(wire_size(self.limits.nesting_depth())),
                collection_entries: Some(wire_size(self.limits.collection_entries())),
                string_bytes: Some(wire_size(self.limits.string_bytes())),
                snapshot_interval_nanos: Some(nanos(self.pacing.snapshot_interval())),
                command_retry_validity_nanos,
                web_console_request_timeout_nanos: Some(nanos(web_console.request_timeout())),
                web_console_connect_timeout_nanos: Some(nanos(web_console.connect_timeout())),
                web_console_retry_backoff_nanos: Some(nanos(web_console.retry_backoff())),
                web_console_retry_max_backoff_nanos: Some(nanos(web_console.retry_max_backoff())),
                web_console_restoration_backoff_nanos: Some(nanos(
                    web_console.restoration_backoff(),
                )),
                web_console_restore_frame_capacity: Some(
                    web_console.restore_frame_capacity().get(),
                ),
            },
        );
        finish_server_message(
            encoder,
            EncodedUnion::new(wire::ServerBody::SessionOpened, opening),
        )
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        value: wire::SessionOpened<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let limits = Self::decode_limits(decoder, value)?;
        let pacing = Self::decode_pacing(decoder, value)?;
        Ok(Self { limits, pacing })
    }

    fn decode_limits(
        decoder: Decoder<'_>,
        value: wire::SessionOpened<'_>,
    ) -> Result<SessionLimits, Report<WireDecodeError>> {
        let bounds = SessionBounds::resolve(SessionBoundValues {
            frame_bytes: Some(decoder.non_zero(
                "SessionOpened.frame_bytes",
                decoder.required("SessionOpened.frame_bytes", value.frame_bytes())?,
            )?),
            transfer_bytes: Some(decoder.non_zero(
                "SessionOpened.transfer_bytes",
                decoder.required("SessionOpened.transfer_bytes", value.transfer_bytes())?,
            )?),
            nesting_depth: Some(decoder.non_zero(
                "SessionOpened.nesting_depth",
                decoder.required("SessionOpened.nesting_depth", value.nesting_depth())?,
            )?),
            collection_entries: Some(decoder.non_zero(
                "SessionOpened.collection_entries",
                decoder.required(
                    "SessionOpened.collection_entries",
                    value.collection_entries(),
                )?,
            )?),
            string_bytes: Some(decoder.non_zero(
                "SessionOpened.string_bytes",
                decoder.required("SessionOpened.string_bytes", value.string_bytes())?,
            )?),
        })
        .change_context(WireDecodeError::InvalidValue {
            field: "SessionOpened.limits",
            kind: "consistent session bounds",
        })?;
        Ok(SessionLimits::from(&bounds))
    }

    fn decode_pacing(
        decoder: Decoder<'_>,
        value: wire::SessionOpened<'_>,
    ) -> Result<SessionPacing, Report<WireDecodeError>> {
        let snapshot_interval = required_nanos(
            decoder,
            "SessionOpened.snapshot_interval_nanos",
            value.snapshot_interval_nanos(),
        )?;
        let snapshot_interval =
            Setting::new(snapshot_interval, &()).change_context(WireDecodeError::InvalidValue {
                field: "SessionOpened.snapshot_interval_nanos",
                kind: "session snapshot interval",
            })?;
        let command_retry_validity = match value.command_retry_validity_nanos() {
            Some(nanos) => {
                let field = "SessionOpened.command_retry_validity_nanos";
                let validity = Duration::from_nanos(decoder.non_zero(field, nanos)?.get());
                let validity =
                    Setting::new(validity, &()).change_context(WireDecodeError::InvalidValue {
                        field,
                        kind: "command retry validity",
                    })?;
                Some(validity)
            }
            None => None,
        };
        let restore_frame_capacity = decoder.non_zero(
            "SessionOpened.web_console_restore_frame_capacity",
            decoder.required(
                "SessionOpened.web_console_restore_frame_capacity",
                value.web_console_restore_frame_capacity(),
            )?,
        )?;
        let web_console = WebConsoleCapabilities::resolve(WebConsoleCapabilityValues {
            request_timeout: Some(required_nanos(
                decoder,
                "SessionOpened.web_console_request_timeout_nanos",
                value.web_console_request_timeout_nanos(),
            )?),
            connect_timeout: Some(required_nanos(
                decoder,
                "SessionOpened.web_console_connect_timeout_nanos",
                value.web_console_connect_timeout_nanos(),
            )?),
            retry_backoff: Some(required_nanos(
                decoder,
                "SessionOpened.web_console_retry_backoff_nanos",
                value.web_console_retry_backoff_nanos(),
            )?),
            retry_max_backoff: Some(required_nanos(
                decoder,
                "SessionOpened.web_console_retry_max_backoff_nanos",
                value.web_console_retry_max_backoff_nanos(),
            )?),
            restoration_backoff: Some(required_nanos(
                decoder,
                "SessionOpened.web_console_restoration_backoff_nanos",
                value.web_console_restoration_backoff_nanos(),
            )?),
            restore_frame_capacity: Some(restore_frame_capacity),
        })
        .change_context(WireDecodeError::InvalidValue {
            field: "SessionOpened.web_console",
            kind: "consistent web console pacing",
        })?;
        Ok(SessionPacing::new(
            snapshot_interval,
            command_retry_validity,
            web_console,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The wire fields of a valid opening at the declared defaults, then changed by `change`.
    fn encoded(change: impl FnOnce(&mut wire::SessionOpenedArgs)) -> bytes::Bytes {
        let defaults = SessionPacing::default().web_console();
        let limits = SessionLimits::DEFAULT;
        let mut fields = wire::SessionOpenedArgs {
            frame_bytes: Some(wire_size(limits.frame_bytes())),
            transfer_bytes: Some(wire_size(limits.transfer_bytes())),
            nesting_depth: Some(wire_size(limits.nesting_depth())),
            collection_entries: Some(wire_size(limits.collection_entries())),
            string_bytes: Some(wire_size(limits.string_bytes())),
            snapshot_interval_nanos: Some(nanos(SessionPacing::default().snapshot_interval())),
            command_retry_validity_nanos: Some(nanos(Duration::from_secs(900))),
            web_console_request_timeout_nanos: Some(nanos(defaults.request_timeout())),
            web_console_connect_timeout_nanos: Some(nanos(defaults.connect_timeout())),
            web_console_retry_backoff_nanos: Some(nanos(defaults.retry_backoff())),
            web_console_retry_max_backoff_nanos: Some(nanos(defaults.retry_max_backoff())),
            web_console_restoration_backoff_nanos: Some(nanos(defaults.restoration_backoff())),
            web_console_restore_frame_capacity: Some(defaults.restore_frame_capacity().get()),
        };
        change(&mut fields);
        let mut encoder = Encoder::new(
            SessionLimits::OPENING.frame_bytes(),
            &SessionLimits::OPENING,
        );
        let opening = wire::SessionOpened::create(encoder.fbb(), &fields);
        finish_server_message(
            encoder,
            EncodedUnion::new(wire::ServerBody::SessionOpened, opening),
        )
        .assured("a test opening fits its fixed envelope")
        .into_bytes()
    }

    #[test]
    fn an_opening_keeps_its_limits_and_pacing_with_and_without_a_retry_validity() {
        let opened = SessionOpened::verify(encoded(|_| {})).assured("the defaults are valid");
        assert_eq!(opened.limits, SessionLimits::DEFAULT);
        assert_eq!(opened.pacing, SessionPacing::default());
        let opened = SessionOpened::verify(encoded(|fields| {
            fields.command_retry_validity_nanos = None;
        }))
        .assured("a node that has not applied the cluster's parameters announces no validity");
        assert_eq!(opened.pacing.command_retry_validity(), None);
        assert_eq!(
            opened.pacing.web_console(),
            SessionPacing::default().web_console()
        );
        let reencoded = SessionOpened::verify(
            opened
                .encode()
                .assured("a decoded opening encodes again")
                .into_bytes(),
        )
        .assured("an opening without a validity verifies again");
        assert_eq!(reencoded, opened);
    }

    #[test]
    fn an_opening_whose_pacing_breaks_its_definitions_is_refused() {
        /// One way an opening's pacing breaks its definitions, and the field change that makes it.
        struct Case {
            breaks: &'static str,
            change: fn(&mut wire::SessionOpenedArgs),
        }
        let cases = [
            Case {
                breaks: "no snapshot interval",
                change: |fields| {
                    fields.snapshot_interval_nanos = None;
                },
            },
            Case {
                breaks: "a zero snapshot interval",
                change: |fields| {
                    fields.snapshot_interval_nanos = Some(0);
                },
            },
            Case {
                breaks: "a zero retry validity",
                change: |fields| {
                    fields.command_retry_validity_nanos = Some(0);
                },
            },
            Case {
                breaks: "no request timeout",
                change: |fields| {
                    fields.web_console_request_timeout_nanos = None;
                },
            },
            Case {
                breaks: "a wait of part of a millisecond",
                change: |fields| {
                    fields.web_console_connect_timeout_nanos = Some(1_500_000);
                },
            },
            Case {
                breaks: "a wait beyond a day",
                change: |fields| {
                    fields.web_console_request_timeout_nanos = Some(86_400_001_000_000);
                },
            },
            Case {
                breaks: "a longest backoff below the first retry wait",
                change: |fields| {
                    fields.web_console_retry_max_backoff_nanos = Some(100_000_000);
                },
            },
            Case {
                breaks: "no restore queue",
                change: |fields| {
                    fields.web_console_restore_frame_capacity = Some(0);
                },
            },
            Case {
                breaks: "a restore queue the browser cannot count",
                change: |fields| {
                    fields.web_console_restore_frame_capacity = Some(16_384);
                },
            },
        ];
        for case in cases {
            let refused = SessionOpened::verify(encoded(case.change))
                .expect_err("an opening whose pacing breaks a definition is refused");
            assert_eq!(
                refused.current_context(),
                &SessionOpeningError::Message,
                "{} must fail the opening",
                case.breaks
            );
        }
    }
}
