//! The producer registry routes each exchange's producer events to the producer they name, and
//! the retry backoff doubles up to its maximum.

use std::{num::NonZeroU64, time::Duration};

use nervix_client_wire::{ProducerAdmissionChanged, ProducerEnded, ProducerId, RequestId};
use nervix_models::{ClientProducerAdmission, ClientProducerEndReason};
use nervix_primitives::sync::Arc;

use super::{ProducerEnd, ProducerRegistry, next_backoff};

fn producer(id: u64) -> ProducerId {
    ProducerId::opened_by(RequestId::new(
        NonZeroU64::new(id).unwrap_or(NonZeroU64::MIN),
    ))
}

#[test]
fn events_reach_the_producer_of_their_own_exchange_only() {
    let registry = ProducerRegistry::default();
    let first = Arc::new(());
    let second = Arc::new(());
    registry.opened(&first, producer(5), ClientProducerAdmission::Open);
    registry.opened(&second, producer(5), ClientProducerAdmission::Suspended);
    let Some(first_signals) = registry.signals(&first, producer(5)) else {
        panic!("the first exchange's producer is registered");
    };
    let Some(second_signals) = registry.signals(&second, producer(5)) else {
        panic!("the second exchange's producer is registered under the same identity");
    };

    registry.admission(
        &first,
        ProducerAdmissionChanged {
            producer: producer(5),
            admission: ClientProducerAdmission::Suspended,
        },
    );
    assert_eq!(
        *first_signals.admission.borrow(),
        ClientProducerAdmission::Suspended
    );
    assert_eq!(
        *second_signals.admission.borrow(),
        ClientProducerAdmission::Suspended,
        "the second producer kept the state it opened with"
    );

    registry.ended(
        &second,
        ProducerEnded {
            producer: producer(5),
            reason: ClientProducerEndReason::Relocated,
            message: "moved".to_string(),
        },
    );
    assert_eq!(
        *second_signals.end.borrow(),
        Some(ProducerEnd::Ended {
            reason: ClientProducerEndReason::Relocated,
            message: "moved".to_string(),
        })
    );
    assert_eq!(*first_signals.end.borrow(), None);
    assert!(registry.signals(&second, producer(5)).is_none());

    registry.exchange_ended(&first);
    assert_eq!(*first_signals.end.borrow(), Some(ProducerEnd::SessionLost));
    assert!(registry.signals(&first, producer(5)).is_none());
}

#[test]
fn the_backoff_doubles_up_to_its_maximum() {
    let maximum = Duration::from_millis(500);
    let mut backoff = Duration::from_millis(100);
    let mut observed = Vec::new();
    for _ in 0..5 {
        backoff = next_backoff(backoff, maximum);
        observed.push(backoff);
    }
    assert_eq!(
        observed,
        vec![
            Duration::from_millis(200),
            Duration::from_millis(400),
            maximum,
            maximum,
            maximum,
        ]
    );
    assert_eq!(next_backoff(Duration::MAX, maximum), maximum);
}
