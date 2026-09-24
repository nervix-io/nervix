//! Unsolicited server messages: every event round trips, carries no request identity, and malformed
//! events are refused.

use flatbuffers::FlatBufferBuilder;
use meticulous::ResultExt as _;
use nervix_models::{DomainPace, DomainStatus, ModelKind, ModelName, NodeRef};

use super::{
    fixtures::{
        decode_error, decode_event, finish_raw, limits, name, non_zero, raw_server, verify_server,
    },
    samples::{leader, rows_frame, subscription},
};
use crate::{
    ClusterObserved, DomainEntity, DomainInfo, DomainSnapshotObserved, DomainsObserved,
    LeaderRedirect, Leadership, LeadershipObserved, NoticeLevel, RowsSkippedCause, ServerEvent,
    ServerMessage, ServerNotice, SessionEndReason, SessionEnding, SubscriptionDeliveryLost,
    SubscriptionEndReason, SubscriptionEnded, SubscriptionRowsSkipped, WireDecodeError, wire,
};

#[test]
fn notices_round_trip_at_every_level() {
    for level in [NoticeLevel::Info, NoticeLevel::Warning, NoticeLevel::Error] {
        let notice = ServerNotice {
            level,
            message: "connected to leader 'node-1'".to_string(),
        };
        let frame = notice.encode(&limits()).assured("a notice fits the limits");
        let ServerEvent::Notice(decoded) = decode_event(frame) else {
            panic!("a notice decodes as a notice");
        };
        assert_eq!(decoded, notice);
    }
}

#[test]
fn every_leadership_observation_round_trips() {
    for leadership in [
        Leadership::ServingNode(name("node-1")),
        Leadership::Remote(leader()),
        Leadership::Unknown,
    ] {
        let observed = LeadershipObserved { leadership };
        let frame = observed
            .encode(&limits())
            .assured("leadership fits the limits");
        let ServerEvent::Leadership(decoded) = decode_event(frame) else {
            panic!("leadership decodes as leadership");
        };
        assert_eq!(decoded, observed);
    }
}

#[test]
fn domain_and_cluster_observations_round_trip() {
    let observed = DomainsObserved {
        domains: vec![DomainInfo {
            domain: name("tenant"),
            status: DomainStatus::Running,
            pace: DomainPace::Unpaced,
        }],
    };
    let frame = observed.encode(&limits()).assured("domains fit the limits");
    let ServerEvent::Domains(decoded) = decode_event(frame) else {
        panic!("domains decode as domains");
    };
    assert_eq!(decoded, observed);

    for cluster in [
        ClusterObserved {
            running_domains: 0,
            graph_nodes: 0,
            relays: 0,
        },
        ClusterObserved {
            running_domains: u64::MAX,
            graph_nodes: u64::MAX,
            relays: u64::MAX,
        },
    ] {
        let frame = cluster
            .encode(&limits())
            .assured("a summary fits the limits");
        let ServerEvent::Cluster(decoded) = decode_event(frame) else {
            panic!("a summary decodes as a summary");
        };
        assert_eq!(decoded, cluster);
    }
}

#[test]
fn a_domain_snapshot_reads_its_graph_in_place() {
    let entities = vec![
        DomainEntity::Model(NodeRef::new(ModelKind::Relay, name::<ModelName>("orders"))),
        DomainEntity::Resource {
            name: name("model"),
            latest_version: Some(non_zero(u64::MAX)),
        },
        DomainEntity::Resource {
            name: name("catalog"),
            latest_version: None,
        },
    ];
    let graph_json = r#"{"domain":"tenant","nodes":[{"id":"RELAY:orders"}],"edges":[]}"#;
    let frame = DomainSnapshotObserved::encode(&name("tenant"), graph_json, &entities, &limits())
        .assured("a snapshot fits the limits");
    let ServerEvent::DomainSnapshot(snapshot) = decode_event(frame) else {
        panic!("a snapshot decodes as a snapshot");
    };
    assert_eq!(snapshot.domain().as_str(), "tenant");
    assert_eq!(snapshot.graph_json(), graph_json);
    assert_eq!(snapshot.entities(), entities.as_slice());
    let frame = snapshot.frame().bytes();
    let start = frame.as_ptr().addr();
    let graph = snapshot.graph_json().as_ptr().addr();
    assert!((start..start + frame.len()).contains(&graph));
}

#[test]
fn subscription_reports_round_trip() {
    let lost = SubscriptionDeliveryLost {
        subscription: subscription(),
        dropped_rows: non_zero(u64::MAX),
    };
    let ServerEvent::SubscriptionDeliveryLost(decoded) =
        decode_event(lost.encode(&limits()).assured("a report fits the limits"))
    else {
        panic!("a delivery report decodes as a delivery report");
    };
    assert_eq!(decoded, lost);

    for cause in [
        RowsSkippedCause::FilterFailed,
        RowsSkippedCause::DomainTimeUnavailable,
        RowsSkippedCause::EncodingFailed,
    ] {
        let skipped = SubscriptionRowsSkipped {
            subscription: subscription(),
            cause,
            skipped_rows: non_zero(1),
            message: "filter failed: division by zero".to_string(),
        };
        let ServerEvent::SubscriptionRowsSkipped(decoded) = decode_event(
            skipped
                .encode(&limits())
                .assured("a report fits the limits"),
        ) else {
            panic!("a skip report decodes as a skip report");
        };
        assert_eq!(decoded, skipped);
    }

    for (reason, message) in [
        (
            SubscriptionEndReason::RelayRemoved,
            "relay 'orders' no longer exists",
        ),
        (
            SubscriptionEndReason::RelayChanged,
            "relay 'orders' was redefined",
        ),
    ] {
        let ended = SubscriptionEnded {
            subscription: subscription(),
            reason,
            message: message.to_string(),
        };
        let ServerEvent::SubscriptionEnded(decoded) =
            decode_event(ended.encode(&limits()).assured("a report fits the limits"))
        else {
            panic!("an end report decodes as an end report");
        };
        assert_eq!(decoded, ended);
    }
}

#[test]
fn every_session_ending_round_trips() {
    for reason in [
        SessionEndReason::ServerShuttingDown,
        SessionEndReason::LeaderRedirect(LeaderRedirect {
            leader: Some(leader()),
        }),
        SessionEndReason::LeaderRedirect(LeaderRedirect { leader: None }),
        SessionEndReason::ProtocolViolated {
            message: "a text message is not a session frame".to_string(),
        },
    ] {
        let ending = SessionEnding { reason };
        let ServerEvent::SessionEnding(decoded) = decode_event(
            ending
                .encode(&limits())
                .assured("an ending fits the limits"),
        ) else {
            panic!("an ending decodes as an ending");
        };
        assert_eq!(decoded, ending);
    }
}

#[test]
fn unsolicited_messages_never_name_a_request() {
    let frames = [
        ServerNotice {
            level: NoticeLevel::Info,
            message: String::new(),
        }
        .encode(&limits()),
        LeadershipObserved {
            leadership: Leadership::Unknown,
        }
        .encode(&limits()),
        ClusterObserved {
            running_domains: 1,
            graph_nodes: 2,
            relays: 3,
        }
        .encode(&limits()),
        Ok(rows_frame(&limits())),
    ];
    for frame in frames {
        let frame = verify_server(frame.assured("the event fits the limits"));
        assert_eq!(frame.request_id(), None);
        assert!(matches!(
            ServerMessage::decode(&frame),
            Ok(ServerMessage::Event(_))
        ));
    }
}

fn notice_frame(body_type: wire::ServerBody, level: Option<wire::NoticeLevel>) -> bytes::Bytes {
    let mut builder = FlatBufferBuilder::new();
    builder.force_defaults(true);
    let message = builder.create_string("notice");
    let notice = wire::ServerNotice::create(
        &mut builder,
        &wire::ServerNoticeArgs {
            level,
            message: Some(message),
        },
    );
    let root = wire::ServerMessage::create(
        &mut builder,
        &wire::ServerMessageArgs {
            body_type,
            body: Some(notice.as_union_value()),
        },
    );
    finish_raw(builder, root, "NXSM")
}

#[test]
fn malformed_events_are_refused() {
    for discriminant in [
        wire::ServerBody::NONE,
        wire::ServerBody(12),
        wire::ServerBody(255),
    ] {
        let frame = raw_server(notice_frame(discriminant, Some(wire::NoticeLevel::Info)));
        assert_eq!(
            decode_error(ServerMessage::decode(&frame)),
            WireDecodeError::UnknownUnionVariant {
                field: "ServerMessage.body",
                discriminant: discriminant.0,
            }
        );
    }
    let frame = raw_server(notice_frame(wire::ServerBody::ServerNotice, None));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::MissingField {
            field: "ServerNotice.level",
        }
    );
    let frame = raw_server(notice_frame(
        wire::ServerBody::ServerNotice,
        Some(wire::NoticeLevel(3)),
    ));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::UnknownEnumValue {
            field: "ServerNotice.level",
            value: 3,
        }
    );
}

#[test]
fn a_domain_entity_must_be_declared() {
    let mut builder = FlatBufferBuilder::new();
    let domain = builder.create_string("tenant");
    let graph = builder.create_string("{}");
    let name = builder.create_string("model");
    let resource = wire::ResourceEntity::create(
        &mut builder,
        &wire::ResourceEntityArgs {
            name: Some(name),
            latest_version: Some(0),
        },
    );
    let entry = wire::DomainEntityEntry::create(
        &mut builder,
        &wire::DomainEntityEntryArgs {
            entity_type: wire::DomainEntity::ResourceEntity,
            entity: Some(resource.as_union_value()),
        },
    );
    let entities = builder.create_vector(&[entry]);
    let snapshot = wire::DomainSnapshotObserved::create(
        &mut builder,
        &wire::DomainSnapshotObservedArgs {
            domain: Some(domain),
            graph_json: Some(graph),
            entities: Some(entities),
        },
    );
    let root = wire::ServerMessage::create(
        &mut builder,
        &wire::ServerMessageArgs {
            body_type: wire::ServerBody::DomainSnapshotObserved,
            body: Some(snapshot.as_union_value()),
        },
    );
    let frame = raw_server(finish_raw(builder, root, "NXSM"));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::ZeroValue {
            field: "ResourceEntity.latest_version",
        }
    );
}
