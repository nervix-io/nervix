//! Replies: every reply body and disposition round trips, and malformed replies are refused.

use flatbuffers::FlatBufferBuilder;
use meticulous::ResultExt as _;
use nervix_models::{
    DomainClockPeriod, DomainClockSkew, DomainPace, DomainStatus, TransactionInspection,
    TransactionInspectionRejection, TransactionLifecycle, TransactionPosition, TransactionStatus,
    TransactionStatusError, WasmCheckpointCounts, WasmCheckpointInspection, WasmCheckpointStage,
    WasmStateGeneration, WasmStateInspection,
};

use super::{
    fixtures::{
        decode_error, finish_raw, limits, name, operation, raw_server, request, round_trip_reply,
    },
    samples::{
        command_dispositions, command_outcome, diagnostics, impact_report, leader, row_schema,
        subscription, transaction, transaction_states,
    },
};
use crate::{
    AttachDisposition, AttachOutcome, CancelOutcome, CancelState, CancellationStage, DomainInfo,
    DomainList, DomainSelection, InspectionOutcome, LeaderRedirect, Reply, ReplyBody,
    RequestCancelled, RequestRejected, RequestRejection, ServerMessage, SourceSpan,
    SubscribeDisposition, SubscribeOutcome, SubscriptionOpened, SubscriptionType, SuggestOutcome,
    Suggestion, SuggestionKind, UnsubscribeDisposition, UnsubscribeOutcome, WireDecodeError,
    WireValueError, wire,
};

fn reply(body: ReplyBody) -> Reply {
    Reply {
        request_id: request(u64::MAX),
        body,
    }
}

fn assert_round_trips(body: ReplyBody) {
    let original = reply(body);
    assert_eq!(round_trip_reply(&original), original);
}

#[test]
fn every_command_disposition_round_trips() {
    for disposition in command_dispositions() {
        assert_round_trips(ReplyBody::Command(Box::new(command_outcome(disposition))));
    }
}

#[test]
fn a_command_outcome_without_statements_diagnostics_or_transaction_round_trips() {
    let mut outcome = command_outcome(crate::CommandDisposition::Completed {
        already_existed: false,
    });
    outcome.origin = crate::OutcomeOrigin::Executed;
    outcome.message = String::new();
    outcome.diagnostics.clear();
    outcome.statements.clear();
    outcome.transaction = None;
    assert_round_trips(ReplyBody::Command(Box::new(outcome)));
}

#[test]
fn a_command_outcome_carries_the_inspection_it_read_beside_its_own_binding() {
    for operation_number in [None, Some(operation(2))] {
        let mut outcome = command_outcome(crate::CommandDisposition::Completed {
            already_existed: false,
        });
        outcome.statements.clear();
        outcome.transaction = Some(transaction(TransactionLifecycle::Open));
        outcome.inspection = Some(Box::new(TransactionInspection {
            transaction: transaction(TransactionLifecycle::Committed),
            operation: operation_number,
            report: impact_report(),
        }));
        assert_round_trips(ReplyBody::Command(Box::new(outcome)));
    }
}

#[test]
fn a_wasm_description_carries_the_same_typed_checkpoint_facts_as_its_text() {
    let mut outcome = command_outcome(crate::CommandDisposition::Completed {
        already_existed: false,
    });
    outcome.wasm_state = Some(Box::new(WasmStateInspection {
        resource: "guest_bundle"
            .try_into()
            .assured("the fixture resource name is valid"),
        resource_version: 3,
        file: "processors/guest.wasm".to_string(),
        default_generation: WasmStateGeneration::FIRST,
        reset: None,
        reset_readiness: None,
        recoveries: Vec::new(),
        omitted_recoveries: 0,
        checkpoint_counts: WasmCheckpointCounts {
            total: 1,
            replica_confirmed: 1,
            ..WasmCheckpointCounts::default()
        },
        checkpoints: vec![WasmCheckpointInspection {
            branch: None,
            generation: WasmStateGeneration::FIRST,
            committed_revision: Some(std::num::NonZeroU64::MIN),
            latest_revision: Some(std::num::NonZeroU64::MIN),
            stage: WasmCheckpointStage::ReplicaConfirmed,
            required_replicas: Some(1),
            confirmed_replicas: Some(1),
        }],
        omitted_checkpoints: 0,
    }));
    assert_round_trips(ReplyBody::Command(Box::new(outcome)));
}

#[test]
fn every_transaction_state_round_trips() {
    for state in transaction_states() {
        let mut outcome = command_outcome(crate::CommandDisposition::Failed);
        outcome.transaction = Some(transaction(state));
        assert_round_trips(ReplyBody::Command(Box::new(outcome)));
    }
}

#[test]
fn transaction_counts_span_their_range_and_stay_consistent() {
    let domain = || name("tenant");
    let full = TransactionStatus::new(
        "id".to_string(),
        domain(),
        TransactionLifecycle::Open,
        TransactionPosition::new(usize::MAX),
        usize::MAX,
    )
    .assured("applied equal to accepted is consistent");
    assert_eq!(full.pending_operations(), 0);
    let empty = TransactionStatus::new(
        String::new(),
        domain(),
        TransactionLifecycle::Committing,
        TransactionPosition::new(0),
        0,
    )
    .assured("an empty transaction is consistent");
    assert_eq!(empty.accepted_operations().accepted_operations(), 0);
    let committing = transaction(TransactionLifecycle::Committing);
    assert_eq!(committing.pending_operations(), 1);
    assert_eq!(committing.applied_operations(), 2);
    assert_eq!(
        committing.transaction_id(),
        "0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44"
    );
    assert_eq!(committing.domain().as_str(), "tenant");
    assert!(committing.lifecycle().is_active());
    let finished = transaction(TransactionLifecycle::Committed);
    assert_eq!(finished.pending_operations(), 0);
    assert!(!finished.lifecycle().is_active());

    for status in [full, empty] {
        let mut outcome = command_outcome(crate::CommandDisposition::Failed);
        outcome.transaction = Some(status);
        assert_round_trips(ReplyBody::Command(Box::new(outcome)));
    }

    let error = TransactionStatus::new(
        "id".to_string(),
        domain(),
        TransactionLifecycle::Open,
        TransactionPosition::new(1),
        2,
    )
    .expect_err("more applied than accepted operations");
    assert_eq!(
        error.current_context(),
        &TransactionStatusError::AppliedOperationsExceedAccepted {
            applied: 2,
            accepted: 1,
        }
    );
}

fn status_frame(accepted: u64, applied: u64, failing_operation: Option<u64>) -> bytes::Bytes {
    let mut builder = FlatBufferBuilder::new();
    let id = builder.create_string("id");
    let domain = builder.create_string("tenant");
    let (state_type, state) = match failing_operation {
        Some(failing_operation) => {
            let error = builder.create_string("failed");
            let failed = wire::TransactionFailed::create(
                &mut builder,
                &wire::TransactionFailedArgs {
                    failing_operation,
                    error: Some(error),
                },
            );
            (
                wire::TransactionState::TransactionFailed,
                failed.as_union_value(),
            )
        }
        None => {
            let open = wire::TransactionOpen::create(&mut builder, &wire::TransactionOpenArgs {});
            (
                wire::TransactionState::TransactionOpen,
                open.as_union_value(),
            )
        }
    };
    let status = wire::TransactionStatus::create(
        &mut builder,
        &wire::TransactionStatusArgs {
            transaction_id: Some(id),
            domain: Some(domain),
            state_type,
            state: Some(state),
            accepted_operations: accepted,
            applied_operations: applied,
        },
    );
    let attached = wire::TransactionAttached::create(
        &mut builder,
        &wire::TransactionAttachedArgs {
            transaction: Some(status),
        },
    );
    let message = builder.create_string("");
    let diagnostics = builder.create_vector::<flatbuffers::WIPOffset<wire::Diagnostic>>(&[]);
    let outcome = wire::AttachOutcome::create(
        &mut builder,
        &wire::AttachOutcomeArgs {
            disposition_type: wire::AttachDisposition::TransactionAttached,
            disposition: Some(attached.as_union_value()),
            message: Some(message),
            diagnostics: Some(diagnostics),
        },
    );
    finish_reply(
        builder,
        wire::ReplyBody::AttachOutcome,
        outcome.as_union_value(),
    )
}

fn finish_reply(
    mut builder: FlatBufferBuilder<'_>,
    body_type: wire::ReplyBody,
    body: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>,
) -> bytes::Bytes {
    let reply = wire::Reply::create(
        &mut builder,
        &wire::ReplyArgs {
            request_id: 5,
            body_type,
            body: Some(body),
        },
    );
    let root = wire::ServerMessage::create(
        &mut builder,
        &wire::ServerMessageArgs {
            body_type: wire::ServerBody::Reply,
            body: Some(reply.as_union_value()),
        },
    );
    finish_raw(builder, root, "NXSM")
}

#[test]
fn inconsistent_transaction_status_is_refused() {
    let frame = raw_server(status_frame(1, 2, None));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::InvalidValue {
            field: "TransactionStatus.applied_operations",
            kind: "operation count",
        }
    );
    let frame = raw_server(status_frame(1, 0, Some(0)));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::ZeroValue {
            field: "TransactionFailed.failing_operation",
        }
    );
    let frame = raw_server(status_frame(2, 2, Some(u64::MAX)));
    assert!(ServerMessage::decode(&frame).is_ok());
}

#[test]
fn every_attach_disposition_round_trips() {
    let dispositions = [
        AttachDisposition::Attached(transaction(TransactionLifecycle::Open)),
        AttachDisposition::AlreadyFinished(transaction(TransactionLifecycle::Expired)),
        AttachDisposition::Failed,
        AttachDisposition::NotLeader(LeaderRedirect {
            leader: Some(leader()),
        }),
        AttachDisposition::NotLeader(LeaderRedirect { leader: None }),
    ];
    for disposition in dispositions {
        assert_round_trips(ReplyBody::Attach(AttachOutcome {
            disposition,
            message: "attached".to_string(),
            diagnostics: diagnostics(),
        }));
    }
}

#[test]
fn suggestions_round_trip() {
    assert_round_trips(ReplyBody::Suggest(SuggestOutcome {
        suggestions: vec![
            Suggestion {
                value: "SCHEMA".to_string(),
                kind: SuggestionKind::Text,
            },
            Suggestion {
                value: "~/models/".to_string(),
                kind: SuggestionKind::LocalDirectoryLookup,
            },
            Suggestion {
                value: String::new(),
                kind: SuggestionKind::Text,
            },
        ],
    }));
    assert_round_trips(ReplyBody::Suggest(SuggestOutcome {
        suggestions: Vec::new(),
    }));
}

#[test]
fn domain_lists_and_selections_round_trip() {
    let paced = DomainPace::Paced {
        period: DomainClockPeriod::try_from(std::time::Duration::from_nanos(1))
            .assured("one nanosecond is a period"),
        skew: DomainClockSkew::try_from(std::time::Duration::from_nanos(u64::MAX))
            .assured("any nanosecond count is a skew"),
    };
    let slowest = DomainPace::Paced {
        period: DomainClockPeriod::try_from(std::time::Duration::from_nanos(u64::MAX))
            .assured("any non-zero nanosecond count is a period"),
        skew: DomainClockSkew::ZERO,
    };
    assert_round_trips(ReplyBody::DomainList(DomainList {
        domains: vec![
            DomainInfo {
                domain: name("stopped"),
                status: DomainStatus::Stopped,
                pace: DomainPace::Unpaced,
            },
            DomainInfo {
                domain: name("running"),
                status: DomainStatus::Running,
                pace: paced,
            },
            DomainInfo {
                domain: name("paused"),
                status: DomainStatus::Paused,
                pace: slowest,
            },
        ],
    }));
    assert_round_trips(ReplyBody::DomainList(DomainList {
        domains: Vec::new(),
    }));
    assert_round_trips(ReplyBody::DomainSelection(DomainSelection::Selected(name(
        "tenant",
    ))));
    assert_round_trips(ReplyBody::DomainSelection(DomainSelection::NotFound(name(
        "missing",
    ))));
}

#[test]
fn a_paced_domain_needs_a_period() {
    let mut builder = FlatBufferBuilder::new();
    let domain = builder.create_string("tenant");
    let paced = wire::PacedDomain::create(
        &mut builder,
        &wire::PacedDomainArgs {
            period_nanos: 0,
            skew_nanos: 0,
        },
    );
    let info = wire::DomainInfo::create(
        &mut builder,
        &wire::DomainInfoArgs {
            domain: Some(domain),
            status: Some(wire::DomainStatus::Running),
            pace_type: wire::DomainPace::PacedDomain,
            pace: Some(paced.as_union_value()),
        },
    );
    let domains = builder.create_vector(&[info]);
    let list = wire::DomainList::create(
        &mut builder,
        &wire::DomainListArgs {
            domains: Some(domains),
        },
    );
    let frame = raw_server(finish_reply(
        builder,
        wire::ReplyBody::DomainList,
        list.as_union_value(),
    ));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::ZeroValue {
            field: "PacedDomain.period_nanos",
        }
    );
}

#[test]
fn every_inspection_outcome_round_trips() {
    for operation_number in [None, Some(operation(1)), Some(operation(usize::MAX))] {
        assert_round_trips(ReplyBody::Inspection(InspectionOutcome::Inspected(
            Box::new(TransactionInspection {
                transaction: transaction(TransactionLifecycle::Failed {
                    failing_operation: operation(1),
                    error: "failed".to_string(),
                }),
                operation: operation_number,
                report: impact_report(),
            }),
        )));
    }
    for rejection in [
        TransactionInspectionRejection::NoAttachedTransaction,
        TransactionInspectionRejection::TransactionNotFound,
        TransactionInspectionRejection::NotOwner,
        TransactionInspectionRejection::OperationNotFound,
    ] {
        assert_round_trips(ReplyBody::Inspection(InspectionOutcome::Rejected {
            rejection,
            message: "rejected".to_string(),
        }));
    }
    assert_round_trips(ReplyBody::Inspection(InspectionOutcome::NotLeader(
        LeaderRedirect {
            leader: Some(leader()),
        },
    )));
}

#[test]
fn subscription_lifecycle_replies_round_trip() {
    assert_round_trips(ReplyBody::Subscribe(SubscribeOutcome {
        disposition: SubscribeDisposition::Opened(Box::new(SubscriptionOpened {
            subscription: subscription(),
            domain: name("tenant"),
            relay: name("orders"),
            subscription_type: SubscriptionType::Row,
            schema: row_schema(),
        })),
        message: "created subscription 'live'".to_string(),
        diagnostics: Vec::new(),
    }));
    let mut unbranched = row_schema();
    unbranched.branch = None;
    unbranched.fields.clear();
    assert_round_trips(ReplyBody::Subscribe(SubscribeOutcome {
        disposition: SubscribeDisposition::Opened(Box::new(SubscriptionOpened {
            subscription: crate::SubscriptionHandle {
                name: name("live"),
                generation: super::fixtures::non_zero(1),
            },
            domain: name("tenant"),
            relay: name("orders"),
            subscription_type: SubscriptionType::Row,
            schema: unbranched,
        })),
        message: String::new(),
        diagnostics: Vec::new(),
    }));
    assert_round_trips(ReplyBody::Subscribe(SubscribeOutcome {
        disposition: SubscribeDisposition::Failed,
        message: "relay 'orders' does not exist".to_string(),
        diagnostics: diagnostics(),
    }));
    assert_round_trips(ReplyBody::Unsubscribe(UnsubscribeOutcome {
        disposition: UnsubscribeDisposition::Deleted(subscription()),
        message: "deleted".to_string(),
        diagnostics: Vec::new(),
    }));
    assert_round_trips(ReplyBody::Unsubscribe(UnsubscribeOutcome {
        disposition: UnsubscribeDisposition::Failed,
        message: "no such subscription".to_string(),
        diagnostics: diagnostics(),
    }));
}

#[test]
fn cancellation_and_rejection_replies_round_trip() {
    for state in [CancelState::Requested, CancelState::NotInFlight] {
        assert_round_trips(ReplyBody::Cancel(CancelOutcome {
            target: request(1),
            state,
        }));
    }
    for stage in [
        CancellationStage::BeforeAdmission,
        CancellationStage::AfterAdmission,
    ] {
        assert_round_trips(ReplyBody::Cancelled(RequestCancelled { stage }));
    }
    for rejection in [
        RequestRejection::InvalidRequest,
        RequestRejection::UnsupportedRequest,
        RequestRejection::UnsupportedValue,
        RequestRejection::DuplicateRequestId,
        RequestRejection::ReplyTooLarge,
        RequestRejection::TooManyRequestsInFlight,
        RequestRejection::ServerBusy,
    ] {
        assert_round_trips(ReplyBody::Rejected(RequestRejected {
            rejection,
            field: Some("SubscribeRequest.subscription_type".to_string()),
            message: "unsupported subscription type".to_string(),
        }));
    }
    assert_round_trips(ReplyBody::Rejected(RequestRejected {
        rejection: RequestRejection::InvalidRequest,
        field: None,
        message: String::new(),
    }));
}

#[test]
fn replies_refuse_missing_and_undeclared_enums() {
    let mut builder = FlatBufferBuilder::new();
    let cancel = wire::CancelOutcome::create(
        &mut builder,
        &wire::CancelOutcomeArgs {
            target_request_id: 1,
            state: None,
        },
    );
    let frame = raw_server(finish_reply(
        builder,
        wire::ReplyBody::CancelOutcome,
        cancel.as_union_value(),
    ));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::MissingField {
            field: "CancelOutcome.state",
        }
    );

    let mut builder = FlatBufferBuilder::new();
    let value = builder.create_string("SCHEMA");
    let suggestion = wire::Suggestion::create(
        &mut builder,
        &wire::SuggestionArgs {
            value: Some(value),
            kind: Some(wire::SuggestionKind(2)),
        },
    );
    let suggestions = builder.create_vector(&[suggestion]);
    let outcome = wire::SuggestOutcome::create(
        &mut builder,
        &wire::SuggestOutcomeArgs {
            suggestions: Some(suggestions),
        },
    );
    let frame = raw_server(finish_reply(
        builder,
        wire::ReplyBody::SuggestOutcome,
        outcome.as_union_value(),
    ));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::UnknownEnumValue {
            field: "Suggestion.kind",
            value: 2,
        }
    );
}

#[test]
fn replies_refuse_undeclared_bodies_and_missing_request_identity() {
    let mut builder = FlatBufferBuilder::new();
    let cancelled = wire::RequestCancelled::create(
        &mut builder,
        &wire::RequestCancelledArgs {
            stage: Some(wire::CancellationStage::AfterAdmission),
        },
    );
    let frame = raw_server(finish_reply(
        builder,
        wire::ReplyBody(200),
        cancelled.as_union_value(),
    ));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::UnknownUnionVariant {
            field: "Reply.body",
            discriminant: 200,
        }
    );

    let mut builder = FlatBufferBuilder::new();
    let cancelled = wire::RequestCancelled::create(
        &mut builder,
        &wire::RequestCancelledArgs {
            stage: Some(wire::CancellationStage::AfterAdmission),
        },
    );
    let reply = wire::Reply::create(
        &mut builder,
        &wire::ReplyArgs {
            request_id: 0,
            body_type: wire::ReplyBody::RequestCancelled,
            body: Some(cancelled.as_union_value()),
        },
    );
    let root = wire::ServerMessage::create(
        &mut builder,
        &wire::ServerMessageArgs {
            body_type: wire::ServerBody::Reply,
            body: Some(reply.as_union_value()),
        },
    );
    let frame = raw_server(finish_raw(builder, root, "NXSM"));
    assert_eq!(frame.request_id(), None);
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::ZeroValue {
            field: "Reply.request_id",
        }
    );
}

fn diagnostic_reply(span: Option<wire::SourceSpan>, grpc_uri: &str) -> bytes::Bytes {
    let mut builder = FlatBufferBuilder::new();
    let node = builder.create_string("node-2");
    let uri = builder.create_string(grpc_uri);
    let endpoints = wire::LeaderEndpoints::create(
        &mut builder,
        &wire::LeaderEndpointsArgs {
            node: Some(node),
            grpc_uri: Some(uri),
            web_console_uri: None,
        },
    );
    let redirect = wire::LeaderRedirect::create(
        &mut builder,
        &wire::LeaderRedirectArgs {
            leader: Some(endpoints),
        },
    );
    let message = builder.create_string("diagnostic");
    let diagnostic = wire::Diagnostic::create(
        &mut builder,
        &wire::DiagnosticArgs {
            message: Some(message),
            span: span.as_ref(),
        },
    );
    let diagnostics = builder.create_vector(&[diagnostic]);
    let empty = builder.create_string("");
    let outcome = wire::AttachOutcome::create(
        &mut builder,
        &wire::AttachOutcomeArgs {
            disposition_type: wire::AttachDisposition::LeaderRedirect,
            disposition: Some(redirect.as_union_value()),
            message: Some(empty),
            diagnostics: Some(diagnostics),
        },
    );
    finish_reply(
        builder,
        wire::ReplyBody::AttachOutcome,
        outcome.as_union_value(),
    )
}

#[test]
fn spans_must_be_ordered_and_endpoints_must_be_uris() {
    let frame = raw_server(diagnostic_reply(
        Some(wire::SourceSpan::new(4, 4)),
        "https://node-2:7443",
    ));
    assert!(ServerMessage::decode(&frame).is_ok());

    let frame = raw_server(diagnostic_reply(
        Some(wire::SourceSpan::new(5, 4)),
        "https://node-2:7443",
    ));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::InvalidValue {
            field: "Diagnostic.span",
            kind: "source span",
        }
    );
    let error = SourceSpan::new(5, 4).expect_err("a reversed span");
    assert_eq!(
        error.current_context(),
        &WireValueError::ReversedSourceSpan { start: 5, end: 4 }
    );

    let frame = raw_server(diagnostic_reply(None, "not a uri"));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::InvalidValue {
            field: "LeaderEndpoints.grpc_uri",
            kind: "URI",
        }
    );
}

#[test]
fn replies_default_to_single_frames_under_the_default_limits() {
    let delivery = reply(ReplyBody::Cancelled(RequestCancelled {
        stage: CancellationStage::BeforeAdmission,
    }))
    .encode(&limits())
    .assured("a small reply fits the default limits");
    assert!(matches!(delivery, crate::ReplyDelivery::Frame(_)));
}
