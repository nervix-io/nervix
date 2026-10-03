//! Domain clock attachment: every attach and detach disposition and every clock frame round trips
//! with the clock in each installation state, and a malformed clock is refused.

use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use nervix_models::{DomainClockTickObservation, Timestamp};

use super::{
    fixtures::{
        decode_error, decode_event, finish_raw, limits, name, raw_server, round_trip_reply,
    },
    samples::domain_clock_observations,
};
use crate::{
    DomainClockAttachDisposition, DomainClockAttachOutcome, DomainClockAttachmentEndReason,
    DomainClockAttachmentEnded, DomainClockDetachDisposition, DomainClockDetachOutcome,
    DomainClockObserved, DomainClockTicked, Reply, ReplyBody, ServerEvent, ServerMessage,
    WireDecodeError, wire,
};

fn reply(body: ReplyBody) -> Reply {
    Reply {
        request_id: super::fixtures::request(9),
        body,
    }
}

#[test]
fn every_attach_disposition_round_trips_with_the_clock_in_every_state() {
    let mut dispositions = domain_clock_observations()
        .into_iter()
        .map(|clock| DomainClockAttachDisposition::Attached {
            domain: name("simulation"),
            clock,
        })
        .collect::<Vec<_>>();
    dispositions.push(DomainClockAttachDisposition::AlreadyAttached(name(
        "simulation",
    )));
    dispositions.push(DomainClockAttachDisposition::DomainNotFound(name(
        &"d".repeat(128),
    )));
    dispositions.push(DomainClockAttachDisposition::Failed);
    for disposition in dispositions {
        let original = reply(ReplyBody::DomainClockAttach(DomainClockAttachOutcome {
            disposition,
            message: "attached to the clock of domain 'simulation'".to_string(),
        }));
        assert_eq!(round_trip_reply(&original), original);
    }
}

#[test]
fn every_detach_disposition_round_trips() {
    for disposition in [
        DomainClockDetachDisposition::Detached(name("simulation")),
        DomainClockDetachDisposition::NotAttached(name("simulation")),
        DomainClockDetachDisposition::Failed,
    ] {
        let original = reply(ReplyBody::DomainClockDetach(DomainClockDetachOutcome {
            disposition,
            message: String::new(),
        }));
        assert_eq!(round_trip_reply(&original), original);
    }
}

#[test]
fn clock_frames_round_trip_with_the_clock_in_every_state() {
    for clock in domain_clock_observations() {
        let observed = DomainClockObserved {
            domain: name("simulation"),
            clock,
        };
        let frame = observed
            .encode(&limits())
            .unwrap_or_else(|error| panic!("a clock frame fits the default limits: {error}"));
        let ServerEvent::DomainClockObserved(decoded) = decode_event(frame) else {
            panic!("a clock frame decodes as a clock observation");
        };
        assert_eq!(decoded, observed);
    }
    let ended = DomainClockAttachmentEnded {
        domain: name("simulation"),
        reason: DomainClockAttachmentEndReason::DomainRemoved,
    };
    let frame = ended
        .encode(&limits())
        .unwrap_or_else(|error| panic!("an end frame fits the default limits: {error}"));
    let ServerEvent::DomainClockAttachmentEnded(decoded) = decode_event(frame) else {
        panic!("an end frame decodes as an attachment end");
    };
    assert_eq!(decoded, ended);
}

#[test]
fn tick_frame_round_trips_with_every_progress_field() {
    let ticked = DomainClockTicked {
        domain: name("simulation"),
        tick: DomainClockTickObservation {
            generation: 7,
            tick_id: 42,
            logical_boundary: Timestamp::from_unix_nanos(1_000),
            authority_utc: Timestamp::from_unix_nanos(2_000),
            serving_logical: Timestamp::from_unix_nanos(3_000),
        },
    };
    let frame = ticked
        .encode(&limits())
        .unwrap_or_else(|error| panic!("a tick frame fits the default limits: {error}"));
    let ServerEvent::DomainClockTicked(decoded) = decode_event(frame) else {
        panic!("a tick frame decodes as a tick");
    };
    assert_eq!(decoded, ticked);
}

/// A hand-built clock frame whose state `write_state` writes.
fn observed_frame(
    write_state: impl FnOnce(
        &mut FlatBufferBuilder<'static>,
    ) -> (wire::DomainClockObservedState, WIPOffset<UnionWIPOffset>),
) -> bytes::Bytes {
    let mut builder = FlatBufferBuilder::new();
    // Writes a NONE discriminant explicitly, which a defaulted slot would otherwise omit.
    builder.force_defaults(true);
    let domain = builder.create_string("simulation");
    let (state_type, state) = write_state(&mut builder);
    let clock = wire::DomainClockObservation::create(
        &mut builder,
        &wire::DomainClockObservationArgs {
            generation: 4,
            state_type,
            state: Some(state),
        },
    );
    let observed = wire::DomainClockObserved::create(
        &mut builder,
        &wire::DomainClockObservedArgs {
            domain: Some(domain),
            clock: Some(clock),
        },
    );
    let root = wire::ServerMessage::create(
        &mut builder,
        &wire::ServerMessageArgs {
            body_type: wire::ServerBody::DomainClockObserved,
            body: Some(observed.as_union_value()),
        },
    );
    finish_raw(builder, root, "NXSM")
}

fn paced_frame(period_nanos: u64, time_rate: Option<f64>) -> bytes::Bytes {
    observed_frame(|builder| {
        let paced = wire::PacedDomainClock::create(
            builder,
            &wire::PacedDomainClockArgs {
                period_nanos,
                skew_nanos: 0,
                logical_origin_unix_nanos: 0,
                utc_anchor_unix_nanos: 0,
                time_rate,
            },
        );
        (
            wire::DomainClockObservedState::PacedDomainClock,
            paced.as_union_value(),
        )
    })
}

#[test]
fn a_paced_clock_needs_a_positive_period_and_a_positive_finite_rate() {
    let frame = raw_server(paced_frame(1, Some(1.0)));
    assert!(ServerMessage::decode(&frame).is_ok());

    let frame = raw_server(paced_frame(0, Some(1.0)));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::ZeroValue {
            field: "PacedDomainClock.period_nanos",
        }
    );
    let frame = raw_server(paced_frame(1, None));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::MissingField {
            field: "PacedDomainClock.time_rate",
        }
    );
    for rate in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let frame = raw_server(paced_frame(1, Some(rate)));
        assert_eq!(
            decode_error(ServerMessage::decode(&frame)),
            WireDecodeError::InvalidValue {
                field: "PacedDomainClock.time_rate",
                kind: "positive finite time rate",
            },
            "rate {rate}"
        );
    }
}

#[test]
fn an_undeclared_clock_state_is_refused() {
    for discriminant in [0, 5, 255] {
        let frame = raw_server(observed_frame(|builder| {
            let stopped =
                wire::StoppedDomainClock::create(builder, &wire::StoppedDomainClockArgs {});
            (
                wire::DomainClockObservedState(discriminant),
                stopped.as_union_value(),
            )
        }));
        assert_eq!(
            decode_error(ServerMessage::decode(&frame)),
            WireDecodeError::UnknownUnionVariant {
                field: "DomainClockObservation.state",
                discriminant,
            }
        );
    }
}

#[test]
fn an_attachment_end_needs_a_declared_reason() {
    for (reason, expected) in [
        (
            None,
            WireDecodeError::MissingField {
                field: "DomainClockAttachmentEnded.reason",
            },
        ),
        (
            Some(wire::DomainClockAttachmentEndReason(1)),
            WireDecodeError::UnknownEnumValue {
                field: "DomainClockAttachmentEnded.reason",
                value: 1,
            },
        ),
    ] {
        let mut builder = FlatBufferBuilder::new();
        let domain = builder.create_string("simulation");
        let ended = wire::DomainClockAttachmentEnded::create(
            &mut builder,
            &wire::DomainClockAttachmentEndedArgs {
                domain: Some(domain),
                reason,
            },
        );
        let root = wire::ServerMessage::create(
            &mut builder,
            &wire::ServerMessageArgs {
                body_type: wire::ServerBody::DomainClockAttachmentEnded,
                body: Some(ended.as_union_value()),
            },
        );
        let frame = raw_server(finish_raw(builder, root, "NXSM"));
        assert_eq!(decode_error(ServerMessage::decode(&frame)), expected);
    }
}

#[test]
fn a_clock_outcome_with_an_undeclared_disposition_is_refused() {
    let mut builder = FlatBufferBuilder::new();
    let message = builder.create_string("");
    let failed = wire::RequestFailed::create(&mut builder, &wire::RequestFailedArgs {});
    let outcome = wire::DomainClockDetachOutcome::create(
        &mut builder,
        &wire::DomainClockDetachOutcomeArgs {
            disposition_type: wire::DomainClockDetachDisposition(4),
            disposition: Some(failed.as_union_value()),
            message: Some(message),
        },
    );
    let frame = raw_server(super::fixtures::finish_reply(
        builder,
        wire::ReplyBody::DomainClockDetachOutcome,
        outcome.as_union_value(),
    ));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::UnknownUnionVariant {
            field: "DomainClockDetachOutcome.disposition",
            discriminant: 4,
        }
    );
}
