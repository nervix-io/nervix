//! The checked opening of the current WebSocket session.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The bounded numeric session opening and its complete checked decode.
//! - **Depends on.** The frame codec, vocabulary bounds and fixed opening envelope.
//! - **Must not know.** Authentication, client recovery or server runtime state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the caller-driven session opening codec")
)]

use error_stack::{Report, ResultExt as _};
use nervix_models::{SessionBoundValues, SessionBounds};

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
    #[error("the first WebSocket frame must announce its session limits")]
    Expected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionOpened {
    pub limits: SessionLimits,
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
        let opening = wire::SessionOpened::create(
            encoder.fbb(),
            &wire::SessionOpenedArgs {
                frame_bytes: Some(wire_size(self.limits.frame_bytes())),
                transfer_bytes: Some(wire_size(self.limits.transfer_bytes())),
                nesting_depth: Some(wire_size(self.limits.nesting_depth())),
                collection_entries: Some(wire_size(self.limits.collection_entries())),
                string_bytes: Some(wire_size(self.limits.string_bytes())),
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
        Ok(Self {
            limits: SessionLimits::from(&bounds),
        })
    }
}
