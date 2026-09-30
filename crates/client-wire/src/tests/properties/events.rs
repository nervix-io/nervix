//! Unsolicited events keep every value, with borrowed graph bytes owned by their verified frame.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current frame cases and their complete representation assertions.
//! - **Depends on.** Production wire codecs and vocabulary test generators.
//! - **Must not know.** Runtime Arrow arrays, service dispatch or live external systems.

use std::collections::BTreeSet;

use meticulous::ResultExt as _;
use nervix_models::{DomainClockTickObservation, DomainPace, ModelKind, NodeRef};

use super::WireValues;
use crate::*;

pub(super) fn check(bytes: &[u8]) -> BTreeSet<wire::ServerBody> {
    let mut values = WireValues::new(bytes);
    let limits = SessionLimits::DEFAULT;
    let mut covered = BTreeSet::new();
    macro_rules! event {
        ($variant:ident, $original:expr) => {{
            let original = $original;
            let encoded = original
                .encode(&limits)
                .assured("a bounded typed event fits");
            let frame = encoded.verify(&limits).assured("encoded event verifies");
            covered.insert(frame.root().body_type());
            assert_eq!(frame.request_id(), None);
            let detached = frame.detached();
            assert_eq!(detached.bytes(), frame.bytes());
            drop(frame);
            let ServerMessage::Event(ServerEvent::$variant(decoded)) =
                ServerMessage::decode(&detached).assured("valid events never skip failures")
            else {
                panic!("the event's variant must be preserved");
            };
            drop(detached);
            assert_eq!(decoded, original);
        }};
    }
    for &level in crate::event::ALL_NOTICE_LEVELS {
        event!(
            Notice,
            ServerNotice {
                level,
                message: values.arbitrary.string()
            }
        );
    }
    for leadership in [
        Leadership::ServingNode(values.arbitrary.name()),
        Leadership::Remote(crate::tests::samples::leader()),
        Leadership::Unknown,
    ] {
        event!(Leadership, LeadershipObserved { leadership });
    }
    let mut domains = Vec::new();
    for status in crate::domain::ALL_DOMAIN_STATUSES {
        let pace = if values.arbitrary.entropy().flag() {
            DomainPace::Paced {
                period: values.arbitrary.clock_period(),
                skew: values.arbitrary.clock_skew(),
            }
        } else {
            DomainPace::Unpaced
        };
        domains.push(DomainInfo {
            status: status.clone(),
            domain: values.arbitrary.name(),
            pace,
        });
    }
    event!(Domains, DomainsObserved { domains });
    event!(
        Cluster,
        ClusterObserved {
            running_domains: values.arbitrary.entropy().any_u64(),
            graph_nodes: values.arbitrary.entropy().any_u64(),
            relays: values.arbitrary.entropy().any_u64()
        }
    );
    let domain = values.arbitrary.name();
    // The graph is deliberately opaque text at this boundary, not parsed or normalized JSON.
    let graph = values.arbitrary.string();
    let entities = vec![
        DomainEntity::Model(NodeRef::new(
            ModelKind::Relay,
            values.arbitrary.name::<nervix_models::ModelName>(),
        )),
        DomainEntity::Resource {
            name: values.arbitrary.name(),
            latest_version: None,
        },
        DomainEntity::Resource {
            name: values.arbitrary.name(),
            latest_version: Some(values.arbitrary.positive_u64()),
        },
    ];
    let encoded = DomainSnapshotObserved::encode(&domain, &graph, &entities, &limits)
        .assured("bounded graph fits");
    let frame = encoded.verify(&limits).assured("graph frame verifies");
    covered.insert(frame.root().body_type());
    let ServerMessage::Event(ServerEvent::DomainSnapshot(snapshot)) =
        ServerMessage::decode(&frame).assured("valid snapshot decodes")
    else {
        panic!("a graph snapshot retains its variant");
    };
    drop(frame);
    let retained = snapshot.clone();
    drop(snapshot);
    assert_eq!(retained.domain(), &domain);
    assert_eq!(retained.graph_json(), graph);
    assert_eq!(retained.entities(), entities);
    let owned_graph = retained.graph_json().to_owned();
    let owned_entities = retained.entities().to_vec();
    drop(retained);
    assert_eq!(owned_graph, graph);
    assert_eq!(owned_entities, entities);
    event!(
        SubscriptionDeliveryLost,
        SubscriptionDeliveryLost {
            subscription: values.subscription(),
            dropped_rows: values.arbitrary.positive_u64()
        }
    );
    for &cause in crate::subscription::ALL_ROWS_SKIPPED_CAUSES {
        event!(
            SubscriptionRowsSkipped,
            SubscriptionRowsSkipped {
                subscription: values.subscription(),
                cause,
                skipped_rows: values.arbitrary.positive_u64(),
                message: values.arbitrary.string()
            }
        );
    }
    for &reason in crate::subscription::ALL_SUBSCRIPTION_END_REASONS {
        event!(
            SubscriptionEnded,
            SubscriptionEnded {
                subscription: values.subscription(),
                reason,
                message: values.arbitrary.string()
            }
        );
    }
    for reason in [
        SessionEndReason::ServerShuttingDown,
        SessionEndReason::LeaderRedirect(values.redirect()),
        SessionEndReason::ProtocolViolated {
            message: values.arbitrary.string(),
        },
    ] {
        event!(SessionEnding, SessionEnding { reason });
    }
    for state in 0..4 {
        event!(
            DomainClockObserved,
            DomainClockObserved {
                domain: values.arbitrary.name(),
                clock: values.clock(state)
            }
        );
    }
    event!(
        DomainClockTicked,
        DomainClockTicked {
            domain: values.arbitrary.name(),
            tick: DomainClockTickObservation {
                generation: values.arbitrary.positive_u64().get(),
                tick_id: values.arbitrary.positive_u64().get(),
                logical_boundary: values.timestamp(),
                authority_utc: values.timestamp(),
                serving_logical: values.timestamp()
            }
        }
    );
    for &reason in crate::domain_clock::ALL_DOMAIN_CLOCK_ATTACHMENT_END_REASONS {
        event!(
            DomainClockAttachmentEnded,
            DomainClockAttachmentEnded {
                domain: values.arbitrary.name(),
                reason
            }
        );
    }
    for &admission in crate::producer::ALL_PRODUCER_ADMISSIONS {
        event!(
            ProducerAdmissionChanged,
            ProducerAdmissionChanged {
                producer: ProducerId::opened_by(values.request_id()),
                admission
            }
        );
    }
    for &reason in crate::producer::ALL_PRODUCER_END_REASONS {
        event!(
            ProducerEnded,
            ProducerEnded {
                producer: ProducerId::opened_by(values.request_id()),
                reason,
                message: values.arbitrary.string()
            }
        );
    }
    covered
}

#[test]
fn bolero_events_keep_complete_values_and_owned_views() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            check(bytes);
        });
}

#[test]
fn event_generator_covers_the_current_schema() {
    let covered = check(&[0xff; 2048]);
    let mut declared: BTreeSet<_> = wire::ServerBody::ENUM_VALUES.iter().copied().collect();
    declared.remove(&wire::ServerBody::NONE);
    declared.remove(&wire::ServerBody::Reply);
    declared.remove(&wire::ServerBody::SubscriptionRows);
    assert_eq!(covered, declared);
    for seed in [0, 1, 2, 3, 4, 5, 0x80] {
        check(&[seed; 2048]);
    }
}
