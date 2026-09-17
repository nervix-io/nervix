//! Client requests: every variant round trips at the edges of its values, and malformed, unknown
//! or oversized requests are refused with a typed error.

use flatbuffers::{FlatBufferBuilder, WIPOffset};
use meticulous::ResultExt as _;

use super::{
    fixtures::{
        checked, decode_error, finish_raw, limits, name, raw_client, reference, request,
        round_trip_client, settings, size,
    },
    samples::client_messages,
};
use crate::{
    ClientMessage, ClientRequest, CommandRequest, DecodeError, EncodeError, SessionLimitSettings,
    SuggestRequest, WireValueError, wire,
};

#[test]
fn every_client_request_round_trips() {
    for message in client_messages() {
        assert_eq!(round_trip_client(&message), message);
    }
}

#[test]
fn every_request_variant_is_sampled() {
    let mut sampled = client_messages()
        .iter()
        .map(|message| match message.request {
            ClientRequest::Command(_) => 0,
            ClientRequest::Suggest(_) => 1,
            ClientRequest::ListDomains => 2,
            ClientRequest::SelectDomain(_) => 3,
            ClientRequest::AttachTransaction(_) => 4,
            ClientRequest::InspectTransaction(_) => 5,
            ClientRequest::Subscribe(_) => 6,
            ClientRequest::Unsubscribe(_) => 7,
            ClientRequest::Cancel(_) => 8,
        })
        .collect::<Vec<_>>();
    sampled.dedup();
    assert_eq!(sampled, (0..9).collect::<Vec<_>>());
    assert_eq!(
        wire::ClientRequest::ENUM_VALUES.len(),
        10,
        "the schema declares NONE and one member per request variant"
    );
}

#[test]
fn request_identity_spans_its_full_range() {
    for id in [1, u64::MAX] {
        let message = ClientMessage {
            request_id: request(id),
            request: ClientRequest::ListDomains,
        };
        assert_eq!(round_trip_client(&message), message);
    }
}

fn raw_message<'fbb>(
    builder: &mut FlatBufferBuilder<'fbb>,
    request_id: u64,
    request_type: wire::ClientRequest,
    request_value: WIPOffset<flatbuffers::UnionWIPOffset>,
) -> WIPOffset<wire::ClientMessage<'fbb>> {
    wire::ClientMessage::create(
        builder,
        &wire::ClientMessageArgs {
            request_id,
            request_type,
            request: Some(request_value),
        },
    )
}

fn list_domains_frame(request_id: u64, request_type: wire::ClientRequest) -> bytes::Bytes {
    let mut builder = FlatBufferBuilder::new();
    // Writes a NONE discriminant explicitly, which a defaulted slot would otherwise omit.
    builder.force_defaults(true);
    let list = wire::ListDomainsRequest::create(&mut builder, &wire::ListDomainsRequestArgs {});
    let root = raw_message(
        &mut builder,
        request_id,
        request_type,
        list.as_union_value(),
    );
    finish_raw(builder, root, "NXCM")
}

#[test]
fn a_zero_request_identity_is_refused() {
    let frame = raw_client(list_domains_frame(
        0,
        wire::ClientRequest::ListDomainsRequest,
    ));
    assert_eq!(frame.request_id(), None);
    assert_eq!(
        decode_error(ClientMessage::decode(&frame)),
        DecodeError::ZeroValue {
            field: "ClientMessage.request_id",
        }
    );
}

#[test]
fn an_undeclared_request_variant_is_refused_with_its_request_identity() {
    for discriminant in [
        wire::ClientRequest::NONE,
        wire::ClientRequest(10),
        wire::ClientRequest(255),
    ] {
        let frame = raw_client(list_domains_frame(42, discriminant));
        assert_eq!(frame.request_id(), Some(request(42)));
        assert_eq!(
            decode_error(ClientMessage::decode(&frame)),
            DecodeError::UnknownUnionVariant {
                field: "ClientMessage.request",
                discriminant: discriminant.0,
            }
        );
    }
}

fn command_frame(query: &str, domain: Option<&str>, execution_reference: &str) -> bytes::Bytes {
    let mut builder = FlatBufferBuilder::new();
    let query = builder.create_string(query);
    let domain = domain.map(|domain| builder.create_string(domain));
    let execution_reference = builder.create_string(execution_reference);
    let command = wire::CommandRequest::create(
        &mut builder,
        &wire::CommandRequestArgs {
            query: Some(query),
            domain,
            execution_reference: Some(execution_reference),
            expected_transaction_position: None,
            expected_preview: None,
        },
    );
    let root = raw_message(
        &mut builder,
        1,
        wire::ClientRequest::CommandRequest,
        command.as_union_value(),
    );
    finish_raw(builder, root, "NXCM")
}

#[test]
fn invalid_names_and_references_are_refused() {
    let frame = raw_client(command_frame(
        "SHOW DOMAINS;",
        Some("not a domain"),
        "valid",
    ));
    assert_eq!(
        decode_error(ClientMessage::decode(&frame)),
        DecodeError::InvalidValue {
            field: "CommandRequest.domain",
            kind: "name",
        }
    );
    let frame = raw_client(command_frame(
        "SHOW DOMAINS;",
        Some("tenant.child"),
        "valid",
    ));
    assert_eq!(
        decode_error(ClientMessage::decode(&frame)),
        DecodeError::InvalidValue {
            field: "CommandRequest.domain",
            kind: "name",
        }
    );
    for invalid in ["", "has space", &"r".repeat(129)] {
        let frame = raw_client(command_frame("SHOW DOMAINS;", None, invalid));
        assert_eq!(
            decode_error(ClientMessage::decode(&frame)),
            DecodeError::InvalidValue {
                field: "CommandRequest.execution_reference",
                kind: "execution reference",
            }
        );
    }
}

#[test]
fn an_absent_transaction_position_differs_from_position_zero() {
    let mut message = ClientMessage {
        request_id: request(1),
        request: ClientRequest::Command(CommandRequest {
            query: "CREATE SCHEMA s ( id U8 );".to_string(),
            domain: None,
            execution_reference: reference("position"),
            expected_transaction_position: None,
            expected_preview: None,
        }),
    };
    assert_eq!(round_trip_client(&message), message);
    if let ClientRequest::Command(command) = &mut message.request {
        command.expected_transaction_position = Some(nervix_models::TransactionPosition::new(0));
    }
    assert_eq!(round_trip_client(&message), message);
}

fn suggest_frame(input: &str, cursor: u32) -> bytes::Bytes {
    let mut builder = FlatBufferBuilder::new();
    let input = builder.create_string(input);
    let suggest = wire::SuggestRequest::create(
        &mut builder,
        &wire::SuggestRequestArgs {
            input: Some(input),
            cursor,
            domain: None,
        },
    );
    let root = raw_message(
        &mut builder,
        1,
        wire::ClientRequest::SuggestRequest,
        suggest.as_union_value(),
    );
    finish_raw(builder, root, "NXCM")
}

#[test]
fn a_suggestion_cursor_must_fall_on_a_character_boundary() {
    let input = "SELECT ü";
    let error = SuggestRequest::new(input.to_string(), 8, None).expect_err("byte 8 is inside 'ü'");
    assert_eq!(
        error.current_context(),
        &WireValueError::CursorOffCharBoundary {
            cursor: 8,
            length: 9,
        }
    );
    assert!(SuggestRequest::new(input.to_string(), 9, None).is_ok());
    assert!(SuggestRequest::new(input.to_string(), 10, None).is_err());

    for cursor in [0, 7, 9] {
        let frame = raw_client(suggest_frame(input, cursor));
        let decoded = ClientMessage::decode(&frame);
        if cursor == 9 {
            let decoded = decoded.assured("the end of the input is a character boundary");
            let ClientRequest::Suggest(suggest) = decoded.request else {
                panic!("a suggest frame decodes as a suggest request");
            };
            assert_eq!(suggest.cursor(), 9);
            assert_eq!(suggest.input(), input);
            assert_eq!(suggest.domain(), None);
        } else {
            assert!(decoded.is_ok(), "cursor {cursor} starts a character");
        }
    }
    for cursor in [8, 10, u32::MAX] {
        let frame = raw_client(suggest_frame(input, cursor));
        assert_eq!(
            decode_error(ClientMessage::decode(&frame)),
            DecodeError::InvalidValue {
                field: "SuggestRequest.cursor",
                kind: "character boundary of the input",
            }
        );
    }
}

fn subscribe_frame(subscription_type: Option<wire::SubscriptionType>) -> bytes::Bytes {
    let mut builder = FlatBufferBuilder::new();
    let domain = builder.create_string("tenant");
    let statement = builder.create_string("CREATE SUBSCRIPTION live FROM orders;");
    let subscribe = wire::SubscribeRequest::create(
        &mut builder,
        &wire::SubscribeRequestArgs {
            domain: Some(domain),
            statement: Some(statement),
            subscription_type,
        },
    );
    let root = raw_message(
        &mut builder,
        1,
        wire::ClientRequest::SubscribeRequest,
        subscribe.as_union_value(),
    );
    finish_raw(builder, root, "NXCM")
}

#[test]
fn a_subscription_type_must_be_selected_and_supported() {
    let frame = raw_client(subscribe_frame(Some(wire::SubscriptionType::Row)));
    assert!(ClientMessage::decode(&frame).is_ok());

    let frame = raw_client(subscribe_frame(None));
    assert_eq!(
        decode_error(ClientMessage::decode(&frame)),
        DecodeError::MissingField {
            field: "SubscribeRequest.subscription_type",
        }
    );
    for unsupported in [1, 2, 255] {
        let frame = raw_client(subscribe_frame(Some(wire::SubscriptionType(unsupported))));
        assert_eq!(
            decode_error(ClientMessage::decode(&frame)),
            DecodeError::UnknownEnumValue {
                field: "SubscribeRequest.subscription_type",
                value: unsupported,
            }
        );
    }
}

fn inspect_frame(target: wire::InspectionTarget, operation: Option<u64>) -> bytes::Bytes {
    let mut builder = FlatBufferBuilder::new();
    let attached =
        wire::AttachedTransaction::create(&mut builder, &wire::AttachedTransactionArgs {});
    let inspect = wire::InspectTransactionRequest::create(
        &mut builder,
        &wire::InspectTransactionRequestArgs {
            target_type: target,
            target: Some(attached.as_union_value()),
            operation,
        },
    );
    let root = raw_message(
        &mut builder,
        1,
        wire::ClientRequest::InspectTransactionRequest,
        inspect.as_union_value(),
    );
    finish_raw(builder, root, "NXCM")
}

#[test]
fn inspection_requests_refuse_operation_zero_and_unknown_targets() {
    let frame = raw_client(inspect_frame(
        wire::InspectionTarget::AttachedTransaction,
        Some(0),
    ));
    assert_eq!(
        decode_error(ClientMessage::decode(&frame)),
        DecodeError::ZeroValue {
            field: "InspectTransactionRequest.operation",
        }
    );
    let frame = raw_client(inspect_frame(wire::InspectionTarget(9), None));
    assert_eq!(
        decode_error(ClientMessage::decode(&frame)),
        DecodeError::UnknownUnionVariant {
            field: "InspectTransactionRequest.target",
            discriminant: 9,
        }
    );
}

fn cancel_frame(target: u64) -> bytes::Bytes {
    let mut builder = FlatBufferBuilder::new();
    let cancel = wire::CancelRequest::create(
        &mut builder,
        &wire::CancelRequestArgs {
            target_request_id: target,
        },
    );
    let root = raw_message(
        &mut builder,
        1,
        wire::ClientRequest::CancelRequest,
        cancel.as_union_value(),
    );
    finish_raw(builder, root, "NXCM")
}

#[test]
fn a_cancel_request_must_name_a_request() {
    let frame = raw_client(cancel_frame(0));
    assert_eq!(
        decode_error(ClientMessage::decode(&frame)),
        DecodeError::ZeroValue {
            field: "CancelRequest.target_request_id",
        }
    );
}

#[test]
fn string_limits_are_inclusive_on_both_sides() {
    let limited = checked(SessionLimitSettings {
        string_bytes: size(32),
        ..settings()
    });
    let message = |query: String| ClientMessage {
        request_id: request(1),
        request: ClientRequest::Command(CommandRequest {
            query,
            domain: None,
            execution_reference: reference("limits"),
            expected_transaction_position: None,
            expected_preview: None,
        }),
    };
    let exact = message("q".repeat(32))
        .encode(&limited)
        .assured("a 32-byte query fits a 32-byte string limit");
    let exact = exact
        .verify(&limited)
        .assured("an encoded frame verifies under its own limits");
    assert!(ClientMessage::decode(&exact).is_ok());

    let error = message("q".repeat(33))
        .encode(&limited)
        .expect_err("a 33-byte query exceeds a 32-byte string limit");
    assert_eq!(
        error.current_context(),
        &EncodeError::StringTooLong {
            field: "CommandRequest.query",
            actual: 33,
            limit: 32,
        }
    );

    let long = message("q".repeat(33))
        .encode(&limits())
        .assured("a 33-byte query fits the default limits");
    let long = long
        .verify(&limited)
        .assured("string limits are applied when decoding, not verifying");
    assert_eq!(
        decode_error(ClientMessage::decode(&long)),
        DecodeError::StringTooLong {
            field: "CommandRequest.query",
            actual: 33,
            limit: 32,
        }
    );
}

#[test]
fn names_are_limited_by_their_own_grammar() {
    let message = ClientMessage {
        request_id: request(1),
        request: ClientRequest::SelectDomain(crate::SelectDomainRequest {
            domain: name(&"d".repeat(128)),
        }),
    };
    assert_eq!(round_trip_client(&message), message);
}
