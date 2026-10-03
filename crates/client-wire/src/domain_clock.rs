//! Domain clock attachment: observed state and tick progress, attach and detach replies, and the
//! frames an attached session receives.

use std::fmt;

use error_stack::Report;
use flatbuffers::WIPOffset;
use meticulous::OptionExt as _;
use nervix_models::{
    DomainClockObservation, DomainClockObservedState, DomainClockPeriod, DomainClockSkew,
    DomainClockState, DomainClockTickObservation, DomainName, DomainTimeRate, PacedDomainClock,
    Timestamp,
};

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_enum},
    frame::{EncodedFrame, ServerFrame},
    limits::SessionLimits,
    server::finish_server_message,
    wire,
};

/// Encodes a domain clock as the serving node has it installed.
pub(crate) fn encode_observation<'fbb>(
    encoder: &mut Encoder<'fbb>,
    observation: &DomainClockObservation,
) -> WIPOffset<wire::DomainClockObservation<'fbb>> {
    let state = match &observation.state {
        DomainClockObservedState::Stopped => EncodedUnion::new(
            wire::DomainClockObservedState::StoppedDomainClock,
            wire::StoppedDomainClock::create(encoder.fbb(), &wire::StoppedDomainClockArgs {}),
        ),
        DomainClockObservedState::Uninstalled => EncodedUnion::new(
            wire::DomainClockObservedState::UninstalledDomainClock,
            wire::UninstalledDomainClock::create(
                encoder.fbb(),
                &wire::UninstalledDomainClockArgs {},
            ),
        ),
        DomainClockObservedState::Unpaced => EncodedUnion::new(
            wire::DomainClockObservedState::UnpacedDomainClock,
            wire::UnpacedDomainClock::create(encoder.fbb(), &wire::UnpacedDomainClockArgs {}),
        ),
        DomainClockObservedState::Paced(paced) => EncodedUnion::new(
            wire::DomainClockObservedState::PacedDomainClock,
            wire::PacedDomainClock::create(
                encoder.fbb(),
                &wire::PacedDomainClockArgs {
                    period_nanos: paced.period.as_nanos(),
                    skew_nanos: paced.skew.as_nanos(),
                    logical_origin_unix_nanos: paced.mapping.logical_start().unix_nanos(),
                    utc_anchor_unix_nanos: paced.mapping.wall_started_at().unix_nanos(),
                    time_rate: Some(paced.mapping.time_rate().get()),
                },
            ),
        ),
    };
    wire::DomainClockObservation::create(
        encoder.fbb(),
        &wire::DomainClockObservationArgs {
            generation: observation.generation,
            state_type: state.discriminant,
            state: Some(state.value),
        },
    )
}

/// Reads a domain clock as the serving node has it installed.
pub(crate) fn decode_observation(
    decoder: Decoder<'_>,
    observation: wire::DomainClockObservation<'_>,
) -> Result<DomainClockObservation, Report<WireDecodeError>> {
    let state = match observation.state_type() {
        wire::DomainClockObservedState::StoppedDomainClock => DomainClockObservedState::Stopped,
        wire::DomainClockObservedState::UninstalledDomainClock => {
            DomainClockObservedState::Uninstalled
        }
        wire::DomainClockObservedState::UnpacedDomainClock => DomainClockObservedState::Unpaced,
        wire::DomainClockObservedState::PacedDomainClock => {
            let paced = observation
                .state_as_paced_domain_clock()
                .assured("an observation's discriminant names the member its accessor reads");
            DomainClockObservedState::Paced(decode_paced(decoder, paced)?)
        }
        undeclared => {
            return Err(decoder.unknown_union("DomainClockObservation.state", undeclared.0));
        }
    };
    Ok(DomainClockObservation {
        generation: observation.generation(),
        state,
    })
}

fn decode_paced(
    decoder: Decoder<'_>,
    paced: wire::PacedDomainClock<'_>,
) -> Result<PacedDomainClock, Report<WireDecodeError>> {
    let period = decoder.non_zero("PacedDomainClock.period_nanos", paced.period_nanos())?;
    let rate = decoder.required("PacedDomainClock.time_rate", paced.time_rate())?;
    let rate = DomainTimeRate::try_from(rate).map_err(|error| {
        Report::new(error).change_context(WireDecodeError::InvalidValue {
            field: "PacedDomainClock.time_rate",
            kind: "positive finite time rate",
        })
    })?;
    let mapping = DomainClockState::new(
        Timestamp::from_unix_nanos(paced.utc_anchor_unix_nanos()),
        Timestamp::from_unix_nanos(paced.logical_origin_unix_nanos()),
        rate,
    );
    Ok(PacedDomainClock {
        period: DomainClockPeriod::from_nanos(period),
        skew: DomainClockSkew::from_nanos(paced.skew_nanos()),
        mapping,
    })
}

/// What became of a request to attach the session to a domain's clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainClockAttachDisposition {
    /// The session follows the domain's clock, which the serving node has installed as `clock`.
    Attached {
        domain: DomainName,
        clock: DomainClockObservation,
    },
    /// The session already follows the domain's clock. Nothing changed.
    AlreadyAttached(DomainName),
    /// The serving node has no domain by that name.
    DomainNotFound(DomainName),
    /// The request was refused; the outcome's message says why.
    Failed,
}

/// The reply to a request to attach the session to a domain's clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainClockAttachOutcome {
    pub disposition: DomainClockAttachDisposition,
    pub message: String,
}

impl DomainClockAttachOutcome {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let disposition = match &self.disposition {
            DomainClockAttachDisposition::Attached { domain, clock } => {
                let domain = encoder.text("DomainClockAttached.domain", domain.as_str())?;
                let clock = encode_observation(encoder, clock);
                let attached = wire::DomainClockAttached::create(
                    encoder.fbb(),
                    &wire::DomainClockAttachedArgs {
                        domain: Some(domain),
                        clock: Some(clock),
                    },
                );
                EncodedUnion::new(
                    wire::DomainClockAttachDisposition::DomainClockAttached,
                    attached,
                )
            }
            DomainClockAttachDisposition::AlreadyAttached(domain) => {
                let domain = encoder.text("DomainClockAlreadyAttached.domain", domain.as_str())?;
                let already = wire::DomainClockAlreadyAttached::create(
                    encoder.fbb(),
                    &wire::DomainClockAlreadyAttachedArgs {
                        domain: Some(domain),
                    },
                );
                EncodedUnion::new(
                    wire::DomainClockAttachDisposition::DomainClockAlreadyAttached,
                    already,
                )
            }
            DomainClockAttachDisposition::DomainNotFound(domain) => {
                let domain = encoder.text("DomainNotFound.domain", domain.as_str())?;
                let not_found = wire::DomainNotFound::create(
                    encoder.fbb(),
                    &wire::DomainNotFoundArgs {
                        domain: Some(domain),
                    },
                );
                EncodedUnion::new(
                    wire::DomainClockAttachDisposition::DomainNotFound,
                    not_found,
                )
            }
            DomainClockAttachDisposition::Failed => EncodedUnion::new(
                wire::DomainClockAttachDisposition::RequestFailed,
                wire::RequestFailed::create(encoder.fbb(), &wire::RequestFailedArgs {}),
            ),
        };
        let message = encoder.text("DomainClockAttachOutcome.message", &self.message)?;
        let outcome = wire::DomainClockAttachOutcome::create(
            encoder.fbb(),
            &wire::DomainClockAttachOutcomeArgs {
                disposition_type: disposition.discriminant,
                disposition: Some(disposition.value),
                message: Some(message),
            },
        );
        Ok(EncodedUnion::new(
            wire::ReplyBody::DomainClockAttachOutcome,
            outcome,
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::DomainClockAttachOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let disposition = if let Some(attached) = outcome.disposition_as_domain_clock_attached() {
            DomainClockAttachDisposition::Attached {
                domain: decoder.name("DomainClockAttached.domain", attached.domain())?,
                clock: decode_observation(decoder, attached.clock())?,
            }
        } else if let Some(already) = outcome.disposition_as_domain_clock_already_attached() {
            DomainClockAttachDisposition::AlreadyAttached(
                decoder.name("DomainClockAlreadyAttached.domain", already.domain())?,
            )
        } else if let Some(not_found) = outcome.disposition_as_domain_not_found() {
            DomainClockAttachDisposition::DomainNotFound(
                decoder.name("DomainNotFound.domain", not_found.domain())?,
            )
        } else if let wire::DomainClockAttachDisposition::RequestFailed = outcome.disposition_type()
        {
            DomainClockAttachDisposition::Failed
        } else {
            return Err(decoder.unknown_union(
                "DomainClockAttachOutcome.disposition",
                outcome.disposition_type().0,
            ));
        };
        let message = decoder.text("DomainClockAttachOutcome.message", outcome.message())?;
        Ok(Self {
            disposition,
            message,
        })
    }
}

/// What became of a request to detach the session from a domain's clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainClockDetachDisposition {
    /// The session no longer follows the domain's clock. No frame about it follows the reply.
    Detached(DomainName),
    /// The session does not follow the domain's clock. Nothing changed.
    NotAttached(DomainName),
    /// The request was refused; the outcome's message says why.
    Failed,
}

/// The reply to a request to detach the session from a domain's clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainClockDetachOutcome {
    pub disposition: DomainClockDetachDisposition,
    pub message: String,
}

impl DomainClockDetachOutcome {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let disposition = match &self.disposition {
            DomainClockDetachDisposition::Detached(domain) => {
                let domain = encoder.text("DomainClockDetached.domain", domain.as_str())?;
                let detached = wire::DomainClockDetached::create(
                    encoder.fbb(),
                    &wire::DomainClockDetachedArgs {
                        domain: Some(domain),
                    },
                );
                EncodedUnion::new(
                    wire::DomainClockDetachDisposition::DomainClockDetached,
                    detached,
                )
            }
            DomainClockDetachDisposition::NotAttached(domain) => {
                let domain = encoder.text("DomainClockNotAttached.domain", domain.as_str())?;
                let not_attached = wire::DomainClockNotAttached::create(
                    encoder.fbb(),
                    &wire::DomainClockNotAttachedArgs {
                        domain: Some(domain),
                    },
                );
                EncodedUnion::new(
                    wire::DomainClockDetachDisposition::DomainClockNotAttached,
                    not_attached,
                )
            }
            DomainClockDetachDisposition::Failed => EncodedUnion::new(
                wire::DomainClockDetachDisposition::RequestFailed,
                wire::RequestFailed::create(encoder.fbb(), &wire::RequestFailedArgs {}),
            ),
        };
        let message = encoder.text("DomainClockDetachOutcome.message", &self.message)?;
        let outcome = wire::DomainClockDetachOutcome::create(
            encoder.fbb(),
            &wire::DomainClockDetachOutcomeArgs {
                disposition_type: disposition.discriminant,
                disposition: Some(disposition.value),
                message: Some(message),
            },
        );
        Ok(EncodedUnion::new(
            wire::ReplyBody::DomainClockDetachOutcome,
            outcome,
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::DomainClockDetachOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let disposition = if let Some(detached) = outcome.disposition_as_domain_clock_detached() {
            DomainClockDetachDisposition::Detached(
                decoder.name("DomainClockDetached.domain", detached.domain())?,
            )
        } else if let Some(not_attached) = outcome.disposition_as_domain_clock_not_attached() {
            DomainClockDetachDisposition::NotAttached(
                decoder.name("DomainClockNotAttached.domain", not_attached.domain())?,
            )
        } else if let wire::DomainClockDetachDisposition::RequestFailed = outcome.disposition_type()
        {
            DomainClockDetachDisposition::Failed
        } else {
            return Err(decoder.unknown_union(
                "DomainClockDetachOutcome.disposition",
                outcome.disposition_type().0,
            ));
        };
        let message = decoder.text("DomainClockDetachOutcome.message", outcome.message())?;
        Ok(Self {
            disposition,
            message,
        })
    }
}

/// The installation of an attached domain clock changed on the serving node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainClockObserved {
    pub domain: DomainName,
    pub clock: DomainClockObservation,
}

impl DomainClockObserved {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<ServerFrame>, Report<WireEncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let domain = encoder.text("DomainClockObserved.domain", self.domain.as_str())?;
        let clock = encode_observation(&mut encoder, &self.clock);
        let observed = wire::DomainClockObserved::create(
            encoder.fbb(),
            &wire::DomainClockObservedArgs {
                domain: Some(domain),
                clock: Some(clock),
            },
        );
        finish_server_message(
            encoder,
            EncodedUnion::new(wire::ServerBody::DomainClockObserved, observed),
        )
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        observed: wire::DomainClockObserved<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            domain: decoder.name("DomainClockObserved.domain", observed.domain())?,
            clock: decode_observation(decoder, observed.clock())?,
        })
    }
}

/// The newest accepted tick of an attached domain's paced clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainClockTicked {
    pub domain: DomainName,
    pub tick: DomainClockTickObservation,
}

impl DomainClockTicked {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<ServerFrame>, Report<WireEncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let domain = encoder.text("DomainClockTicked.domain", self.domain.as_str())?;
        let ticked = wire::DomainClockTicked::create(
            encoder.fbb(),
            &wire::DomainClockTickedArgs {
                domain: Some(domain),
                generation: self.tick.generation,
                tick_id: self.tick.tick_id,
                logical_boundary_unix_nanos: self.tick.logical_boundary.unix_nanos(),
                authority_utc_unix_nanos: self.tick.authority_utc.unix_nanos(),
                serving_logical_unix_nanos: self.tick.serving_logical.unix_nanos(),
            },
        );
        finish_server_message(
            encoder,
            EncodedUnion::new(wire::ServerBody::DomainClockTicked, ticked),
        )
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        ticked: wire::DomainClockTicked<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            domain: decoder.name("DomainClockTicked.domain", ticked.domain())?,
            tick: DomainClockTickObservation {
                generation: decoder
                    .non_zero("DomainClockTicked.generation", ticked.generation())?
                    .get(),
                tick_id: decoder
                    .non_zero("DomainClockTicked.tick_id", ticked.tick_id())?
                    .get(),
                logical_boundary: Timestamp::from_unix_nanos(ticked.logical_boundary_unix_nanos()),
                authority_utc: Timestamp::from_unix_nanos(ticked.authority_utc_unix_nanos()),
                serving_logical: Timestamp::from_unix_nanos(ticked.serving_logical_unix_nanos()),
            },
        })
    }
}

/// Why the server ended a session's attachment to a domain clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DomainClockAttachmentEndReason {
    /// The domain no longer exists on the serving node.
    DomainRemoved,
}

wire_enum!(
    ALL_DOMAIN_CLOCK_ATTACHMENT_END_REASONS: DomainClockAttachmentEndReason
        => wire::DomainClockAttachmentEndReason { DomainRemoved }
);

impl fmt::Display for DomainClockAttachmentEndReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DomainRemoved => {
                formatter.write_str("the domain no longer exists on the serving node")
            }
        }
    }
}

/// The server ended a session's attachment to a domain clock. It is the last frame about that
/// attachment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainClockAttachmentEnded {
    pub domain: DomainName,
    pub reason: DomainClockAttachmentEndReason,
}

impl DomainClockAttachmentEnded {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<ServerFrame>, Report<WireEncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let domain = encoder.text("DomainClockAttachmentEnded.domain", self.domain.as_str())?;
        let ended = wire::DomainClockAttachmentEnded::create(
            encoder.fbb(),
            &wire::DomainClockAttachmentEndedArgs {
                domain: Some(domain),
                reason: Some(self.reason.into()),
            },
        );
        finish_server_message(
            encoder,
            EncodedUnion::new(wire::ServerBody::DomainClockAttachmentEnded, ended),
        )
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        ended: wire::DomainClockAttachmentEnded<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            domain: decoder.name("DomainClockAttachmentEnded.domain", ended.domain())?,
            reason: decoder
                .required_enumeration("DomainClockAttachmentEnded.reason", ended.reason())?,
        })
    }
}
