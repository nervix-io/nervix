//! State-machine tests for desired client subscriptions.

use std::num::NonZeroU64;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{SubscriptionDeliveryLost, SubscriptionEndReason};
use nervix_models::{RelayName, SubscriptionDeliveryBehavior};

use super::*;

fn name(value: &str) -> SubscriptionName {
    SubscriptionName::parse(value).assured("the test names satisfy the subscription grammar")
}

fn handle(value: &str, generation: u64) -> SubscriptionHandle {
    SubscriptionHandle {
        name: name(value),
        generation: NonZeroU64::new(generation).assured("test generations are positive"),
    }
}

fn contract(value: &str, domain: &str) -> SubscriptionContract {
    SubscriptionContract {
        domain: DomainName::parse(domain).assured("test domains satisfy the name grammar"),
        create: CreateSubscription {
            name: name(value),
            relay: RelayName::parse("events").assured("the test relay name is valid"),
            delivery_behavior: SubscriptionDeliveryBehavior::Dropping,
            batch_sample_rate: Some("25%".to_string()),
            where_clause: None,
        },
        subscription_type: SubscriptionType::Row,
    }
}

/// What the registry reports on its own, which the tests compare.
#[derive(Debug, PartialEq, Eq)]
enum Reported {
    Interrupted(SubscriptionInterruption),
    RestorationFailed(SubscriptionRestorationFailure),
    Ended(SubscriptionEnded),
}

fn next_reported(desired: &DesiredSubscriptions) -> Option<Reported> {
    match desired.take_event()? {
        SubscriptionEvent::Interrupted(interrupted) => Some(Reported::Interrupted(interrupted)),
        SubscriptionEvent::RestorationFailed(failure) => Some(Reported::RestorationFailed(failure)),
        SubscriptionEvent::Ended(ended) => Some(Reported::Ended(ended)),
        other => {
            panic!("the registry reports gaps, failed restorations and ends only, not {other:?}")
        }
    }
}

/// An event of `handle` that decides nothing, which reaches the caller exactly while the registry
/// delivers that generation.
fn rows_lost(handle: &SubscriptionHandle) -> SubscriptionEvent {
    SubscriptionEvent::DeliveryLost(SubscriptionDeliveryLost {
        subscription: handle.clone(),
        dropped_rows: NonZeroU64::MIN,
    })
}

/// The end the server sends when the relay of `handle` is redefined.
fn relay_changed(handle: &SubscriptionHandle) -> SubscriptionEnded {
    SubscriptionEnded {
        subscription: handle.clone(),
        reason: SubscriptionEndReason::RelayChanged,
        message: "relay 'events' was redefined".to_string(),
    }
}

fn interrupted(value: &str, generation: u64) -> Reported {
    Reported::Interrupted(SubscriptionInterruption {
        subscription: handle(value, generation),
    })
}

fn deletion(cancellation: Cancellation) -> DeleteAttempt {
    let Cancellation::Delete(attempt) = cancellation else {
        panic!("the deletion asks the server");
    };
    attempt
}

/// A registry holding `watch`, which the exchange of `generation` opened as generation 1.
fn opened(generation: &Arc<()>) -> DesiredSubscriptions {
    let desired = DesiredSubscriptions::new();
    let pending = desired
        .begin(contract("watch", "tenant"), generation.clone())
        .verified("the registry starts empty");
    desired.created(&pending, Some(&handle("watch", 1)));
    desired
}

#[test]
fn unacknowledged_creation_is_not_restored_after_loss() {
    let desired = DesiredSubscriptions::new();
    let first_generation = Arc::new(());
    let pending = desired
        .begin(contract("watch", "tenant"), first_generation.clone())
        .verified("the registry starts empty");
    desired.ended(&first_generation);

    assert!(desired.restore(Arc::new(())).is_empty());
    desired.created(&pending, None);
    assert_eq!(desired.lifecycle(&name("watch")), None);
    assert_eq!(next_reported(&desired), None);
}

#[test]
fn acknowledged_contract_restores_after_repeated_loss_and_keeps_its_options() {
    let desired = DesiredSubscriptions::new();
    let first_generation = Arc::new(());
    let pending = desired
        .begin(contract("watch", "tenant"), first_generation.clone())
        .verified("the registry starts empty");
    let first = handle("watch", 1);
    desired.created(&pending, Some(&first));
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Active(first.clone()))
    );

    desired.ended(&first_generation);
    assert_eq!(next_reported(&desired), Some(interrupted("watch", 1)));
    let second_generation = Arc::new(());
    let mut attempts = desired.restore(second_generation.clone());
    let retry = attempts
        .pop()
        .verified("the acknowledged subscription is restored");
    assert_eq!(retry.contract.domain, contract("watch", "tenant").domain);
    assert_eq!(
        retry.contract.create.delivery_behavior,
        SubscriptionDeliveryBehavior::Dropping
    );
    assert_eq!(
        retry.contract.create.batch_sample_rate.as_deref(),
        Some("25%")
    );
    assert_eq!(retry.contract.subscription_type, SubscriptionType::Row);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Restoring(first.clone()))
    );

    desired.ended(&second_generation);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Interrupted(first.clone()))
    );
    assert!(!desired.admit(&rows_lost(&first), &second_generation));
    desired.created(&retry, None);
    assert!(desired.retry(&name("watch"), &second_generation).is_none());
    let third_generation = Arc::new(());
    let mut attempts = desired.restore(third_generation.clone());
    let retry = attempts
        .pop()
        .verified("the interruption remains restorable");
    desired.created(&retry, None);
    let retry = desired
        .retry(&name("watch"), &third_generation)
        .verified("a failed restore on the live exchange can be retried");
    let reopened = handle("watch", 2);
    desired.created(&retry, Some(&reopened));
    assert!(desired.admit(&rows_lost(&reopened), &third_generation));
    assert!(!desired.admit(&rows_lost(&first), &third_generation));
}

#[test]
fn cancellation_fences_a_late_restore_and_name_reuse() {
    let first_generation = Arc::new(());
    let desired = opened(&first_generation);
    desired.ended(&first_generation);
    let second_generation = Arc::new(());
    let mut attempts = desired.restore(second_generation.clone());
    let retry = attempts
        .pop()
        .verified("the acknowledged subscription is restored");

    let deletion = deletion(desired.cancel(&name("watch"), second_generation.clone()));
    assert_eq!(next_reported(&desired), None);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Closing)
    );
    assert!(desired.deletion_waits(&deletion));
    assert!(matches!(
        desired.cancel(&name("watch"), second_generation.clone()),
        Cancellation::InFlight
    ));
    assert!(
        desired
            .begin(contract("watch", "other"), second_generation.clone())
            .is_none()
    );
    let late = handle("watch", 2);
    desired.created(&retry, Some(&late));
    assert!(!desired.deletion_waits(&deletion));
    assert!(
        matches!(desired.deletion_target(&deletion), DeletionTarget::Server),
        "the late success leaves a generation the server must delete"
    );
    assert!(!desired.admit(&rows_lost(&late), &second_generation));

    desired.deleted(&deletion, DeletionResolution::Deleted);
    let next = desired
        .begin(contract("watch", "other"), second_generation.clone())
        .verified("successful cleanup releases the name");
    desired.created(&retry, Some(&late));
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Creating)
    );
    desired.created(&next, Some(&handle("watch", 3)));
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Active(handle("watch", 3)))
    );
}

#[test]
fn failed_deletion_keeps_the_name_fenced_until_retry_succeeds() {
    let generation = Arc::new(());
    let desired = opened(&generation);
    let first = deletion(desired.cancel(&name("watch"), generation.clone()));
    desired.deleted(&first, DeletionResolution::Refused);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::DeletionFailed)
    );
    assert!(
        desired
            .begin(contract("watch", "tenant"), generation.clone())
            .is_none()
    );
    let second = deletion(desired.cancel(&name("watch"), generation.clone()));
    desired.deleted(&first, DeletionResolution::Deleted);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Closing)
    );
    desired.deleted(&second, DeletionResolution::Deleted);
    assert!(
        desired
            .begin(contract("watch", "tenant"), generation)
            .is_some()
    );
}

#[test]
fn deletion_of_an_untracked_name_fences_creation_and_ignores_late_results() {
    let desired = DesiredSubscriptions::new();
    let first_generation = Arc::new(());
    let first = deletion(desired.cancel(&name("watch"), first_generation.clone()));
    assert!(!first.tracked);
    assert!(
        desired
            .begin(contract("watch", "tenant"), first_generation.clone())
            .is_none()
    );
    desired.ended(&first_generation);
    let second_generation = Arc::new(());
    let next = desired
        .begin(contract("watch", "tenant"), second_generation.clone())
        .verified("the ended session releases its deletion fence");
    desired.deleted(&first, DeletionResolution::Deleted);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Creating)
    );
    desired.created(&next, Some(&handle("watch", 2)));
    assert!(desired.admit(&rows_lost(&handle("watch", 2)), &second_generation));
}

#[test]
fn a_refused_deletion_of_a_name_the_client_never_held_leaves_the_name_free() {
    let desired = DesiredSubscriptions::new();
    let generation = Arc::new(());
    let attempt = deletion(desired.cancel(&name("watch"), generation.clone()));
    assert!(matches!(
        desired.deletion_target(&attempt),
        DeletionTarget::Server
    ));
    desired.deleted(&attempt, DeletionResolution::Refused);
    assert_eq!(desired.lifecycle(&name("watch")), None);
    assert!(
        desired
            .begin(contract("watch", "tenant"), generation)
            .is_some()
    );
}

#[test]
fn consumer_overflow_is_terminal_for_that_delivery_generation() {
    let desired = DesiredSubscriptions::new();
    let generation = Arc::new(());
    let pending = desired
        .begin(contract("watch", "tenant"), generation.clone())
        .verified("the registry starts empty");
    let active = handle("watch", 1);
    desired.acknowledge(&active, &generation);
    desired.overflow(&active, &generation);
    desired.created(&pending, Some(&active));
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::DeliveryFailed(active.clone()))
    );
    desired.ended(&generation);
    assert!(desired.restore(Arc::new(())).is_empty());
    assert!(
        matches!(
            desired.cancel(&name("watch"), generation.clone()),
            Cancellation::Closed
        ),
        "the session that held the failed delivery ended, so nothing is left to delete"
    );
    assert_eq!(desired.lifecycle(&name("watch")), None);
    assert!(
        desired
            .begin(contract("watch", "tenant"), generation)
            .is_some()
    );
}

#[test]
fn a_subscription_whose_session_ended_is_deleted_without_the_server() {
    let first_generation = Arc::new(());
    let desired = opened(&first_generation);
    desired.ended(&first_generation);
    let second_generation = Arc::new(());
    assert!(matches!(
        desired.cancel(&name("watch"), second_generation.clone()),
        Cancellation::Closed
    ));
    assert_eq!(desired.lifecycle(&name("watch")), None);
    assert_eq!(
        next_reported(&desired),
        None,
        "a deleted subscription reports no gap"
    );
    assert!(desired.restore(second_generation.clone()).is_empty());
    assert!(
        desired
            .begin(contract("watch", "tenant"), second_generation)
            .is_some()
    );
}

#[test]
fn a_subscription_whose_restoration_was_refused_is_deleted_without_the_server() {
    let first_generation = Arc::new(());
    let desired = opened(&first_generation);
    desired.ended(&first_generation);
    let second_generation = Arc::new(());
    let retry = desired
        .restore(second_generation.clone())
        .pop()
        .verified("the acknowledged subscription is restored");
    desired.created(&retry, None);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Interrupted(handle("watch", 1)))
    );
    assert!(matches!(
        desired.cancel(&name("watch"), second_generation.clone()),
        Cancellation::Closed
    ));
    assert_eq!(desired.lifecycle(&name("watch")), None);
    assert!(desired.retry(&name("watch"), &second_generation).is_none());
}

#[test]
fn a_deletion_waiting_for_a_refused_creation_needs_no_server() {
    let desired = DesiredSubscriptions::new();
    let generation = Arc::new(());
    let pending = desired
        .begin(contract("watch", "tenant"), generation.clone())
        .verified("the registry starts empty");
    let attempt = deletion(desired.cancel(&name("watch"), generation.clone()));
    assert!(attempt.tracked);
    assert!(desired.deletion_waits(&attempt));
    desired.created(&pending, None);
    assert!(!desired.deletion_waits(&attempt));
    assert!(matches!(
        desired.deletion_target(&attempt),
        DeletionTarget::NotOpened
    ));
    assert!(
        desired
            .begin(contract("watch", "tenant"), generation)
            .is_some(),
        "the completed deletion releases the name"
    );
}

#[test]
fn a_deletion_whose_session_ended_closes_the_subscription() {
    let generation = Arc::new(());
    let desired = opened(&generation);
    let attempt = deletion(desired.cancel(&name("watch"), generation.clone()));
    desired.deleted(&attempt, DeletionResolution::SessionEnded);
    assert_eq!(desired.lifecycle(&name("watch")), None);

    let deleting = deletion(desired.cancel(&name("other"), generation.clone()));
    desired.ended(&generation);
    assert!(matches!(
        desired.deletion_target(&deleting),
        DeletionTarget::SessionEnded
    ));
    assert!(
        desired
            .begin(contract("watch", "tenant"), Arc::new(()))
            .is_some()
    );
}

#[test]
fn failed_restorations_follow_the_gap_and_keep_only_the_newest_unread_one() {
    let first_generation = Arc::new(());
    let desired = opened(&first_generation);
    desired.ended(&first_generation);
    let second_generation = Arc::new(());
    let first_retry = desired
        .restore(second_generation.clone())
        .pop()
        .verified("the acknowledged subscription is restored");
    desired.created(&first_retry, None);
    assert!(desired.restoration_failed(
        &first_retry,
        "relay 'events' is starting".to_string(),
        Duration::from_secs(1)
    ));
    let second_retry = desired
        .retry(&name("watch"), &second_generation)
        .verified("a refused restoration is retried on its exchange");
    assert!(
        !desired.restoration_failed(&first_retry, "stale".to_string(), Duration::from_secs(1)),
        "an attempt that was retried reports nothing more"
    );
    desired.created(&second_retry, None);
    assert!(desired.restoration_failed(
        &second_retry,
        "relay 'events' does not exist".to_string(),
        Duration::from_secs(2)
    ));

    assert_eq!(next_reported(&desired), Some(interrupted("watch", 1)));
    assert_eq!(
        next_reported(&desired),
        Some(Reported::RestorationFailed(
            SubscriptionRestorationFailure {
                subscription: handle("watch", 1),
                message: "relay 'events' does not exist".to_string(),
                retry_after: Duration::from_secs(2),
            }
        ))
    );
    assert_eq!(next_reported(&desired), None);

    desired.ended(&second_generation);
    assert!(
        !desired.restoration_failed(
            &second_retry,
            "too late".to_string(),
            Duration::from_secs(4)
        ),
        "a restoration whose exchange ended reports nothing more"
    );
    assert!(matches!(
        desired.cancel(&name("watch"), Arc::new(())),
        Cancellation::Closed
    ));
    assert_eq!(next_reported(&desired), None);
}

#[test]
fn repeated_exchange_losses_keep_one_pending_gap_per_subscription() {
    let first_generation = Arc::new(());
    let desired = opened(&first_generation);
    desired.ended(&first_generation);

    let second_generation = Arc::new(());
    let mut attempts = desired.restore(second_generation.clone());
    let second_attempt = attempts
        .pop()
        .verified("the first acknowledgement is restorable");
    let second = handle("watch", 2);
    desired.created(&second_attempt, Some(&second));
    desired.ended(&second_generation);

    assert_eq!(next_reported(&desired), Some(interrupted("watch", 2)));
    assert_eq!(next_reported(&desired), None);
}

#[test]
fn a_generation_the_server_ended_stays_ended_and_is_never_restored() {
    let first_generation = Arc::new(());
    let desired = opened(&first_generation);
    let first = handle("watch", 1);
    let end = relay_changed(&first);
    desired.end(&end, &first_generation);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Ended(first.clone()))
    );
    assert!(
        !desired.has_acknowledged_desired(),
        "an ended subscription waits for no restoration"
    );
    assert!(desired.admit(&SubscriptionEvent::Ended(end), &first_generation));

    desired.ended(&first_generation);
    assert_eq!(
        next_reported(&desired),
        None,
        "an ended subscription reports no gap, and its end was read"
    );
    let second_generation = Arc::new(());
    assert!(desired.restore(second_generation.clone()).is_empty());
    assert!(desired.retry(&name("watch"), &second_generation).is_none());
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Ended(first))
    );
}

#[test]
fn the_events_before_an_end_reach_the_caller_and_nothing_follows_the_end() {
    let generation = Arc::new(());
    let desired = opened(&generation);
    let first = handle("watch", 1);
    let end = relay_changed(&first);
    desired.end(&end, &generation);

    assert!(
        desired.admit(&rows_lost(&first), &generation),
        "an event queued before the end still reaches the caller"
    );
    assert!(!desired.admit(&rows_lost(&handle("watch", 2)), &generation));
    assert!(desired.admit(&SubscriptionEvent::Ended(end.clone()), &generation));
    assert!(
        !desired.admit(&rows_lost(&first), &generation),
        "nothing follows the end the caller read"
    );
    assert!(
        !desired.admit(&SubscriptionEvent::Ended(end), &generation),
        "the end reaches the caller once"
    );
}

#[test]
fn an_end_its_exchange_discarded_unread_is_reported_once() {
    let first_generation = Arc::new(());
    let desired = opened(&first_generation);
    let first = handle("watch", 1);
    let end = relay_changed(&first);
    desired.end(&end, &first_generation);
    desired.ended(&first_generation);

    assert!(
        !desired.admit(&SubscriptionEvent::Ended(end.clone()), &first_generation),
        "the queue of an ended exchange delivers nothing more"
    );
    assert_eq!(next_reported(&desired), Some(Reported::Ended(end)));
    assert_eq!(next_reported(&desired), None);
    desired.ended(&first_generation);
    desired.ended(&Arc::new(()));
    assert_eq!(next_reported(&desired), None);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Ended(first))
    );
}

#[test]
fn deleting_an_ended_subscription_needs_no_server_and_releases_its_name() {
    let generation = Arc::new(());
    let desired = opened(&generation);
    desired.end(&relay_changed(&handle("watch", 1)), &generation);

    assert!(matches!(
        desired.cancel(&name("watch"), generation.clone()),
        Cancellation::Ended
    ));
    assert_eq!(desired.lifecycle(&name("watch")), None);
    desired.ended(&generation);
    assert_eq!(
        next_reported(&desired),
        None,
        "a deleted subscription reports no end"
    );
    assert!(
        desired
            .begin(contract("watch", "tenant"), generation)
            .is_some()
    );
}

#[test]
fn subscribing_under_an_ended_name_opens_a_new_generation() {
    let generation = Arc::new(());
    let desired = opened(&generation);
    let first = handle("watch", 1);
    let end = relay_changed(&first);
    desired.end(&end, &generation);

    let reopening = desired
        .begin(contract("watch", "tenant"), generation.clone())
        .verified("the name of an ended generation is free to subscribe again");
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Creating)
    );
    assert!(
        !desired.admit(&SubscriptionEvent::Ended(end), &generation),
        "the replaced generation's unread end no longer reaches the caller"
    );
    let second = handle("watch", 2);
    desired.acknowledge(&second, &generation);
    desired.created(&reopening, Some(&second));
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Active(second.clone()))
    );
    assert!(desired.admit(&rows_lost(&second), &generation));
    assert!(!desired.admit(&rows_lost(&first), &generation));
    assert!(
        desired
            .begin(contract("watch", "tenant"), generation)
            .is_none(),
        "an active name stays refused"
    );
}

#[test]
fn subscribing_again_drops_the_reported_end_of_the_generation_it_replaces() {
    let first_generation = Arc::new(());
    let desired = opened(&first_generation);
    desired.end(&relay_changed(&handle("watch", 1)), &first_generation);
    desired.ended(&first_generation);

    let second_generation = Arc::new(());
    let reopening = desired
        .begin(contract("watch", "tenant"), second_generation)
        .verified("the name of an ended generation is free to subscribe again");
    assert_eq!(next_reported(&desired), None);
    desired.created(&reopening, None);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        None,
        "a refused subscription leaves nothing behind"
    );
}

#[test]
fn a_late_opening_reply_does_not_reopen_an_ended_generation() {
    let first_generation = Arc::new(());
    let desired = DesiredSubscriptions::new();
    let pending = desired
        .begin(contract("watch", "tenant"), first_generation.clone())
        .verified("the registry starts empty");
    let first = handle("watch", 1);
    desired.acknowledge(&first, &first_generation);
    desired.end(&relay_changed(&first), &first_generation);
    desired.created(&pending, Some(&first));
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Ended(first))
    );

    let second_generation = Arc::new(());
    let reopening = desired
        .begin(contract("watch", "tenant"), second_generation.clone())
        .verified("the name of an ended generation is free to subscribe again");
    desired.created(&reopening, Some(&handle("watch", 2)));
    desired.ended(&second_generation);
    let third_generation = Arc::new(());
    let restoring = desired
        .restore(third_generation.clone())
        .pop()
        .verified("the acknowledged subscription is restored");
    let third = handle("watch", 3);
    desired.acknowledge(&third, &third_generation);
    desired.end(&relay_changed(&third), &third_generation);
    desired.created(&restoring, Some(&third));
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Ended(third))
    );
}

#[test]
fn an_end_after_a_failed_delivery_leaves_the_overflow_as_the_last_event() {
    let generation = Arc::new(());
    let desired = opened(&generation);
    let first = handle("watch", 1);
    desired.overflow(&first, &generation);
    let end = relay_changed(&first);
    desired.end(&end, &generation);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Ended(first.clone()))
    );
    assert!(desired.admit(&SubscriptionEvent::ConsumerOverflow(first), &generation));
    assert!(!desired.admit(&SubscriptionEvent::Ended(end), &generation));

    desired.ended(&generation);
    assert_eq!(next_reported(&desired), None);
    assert!(matches!(
        desired.cancel(&name("watch"), Arc::new(())),
        Cancellation::Ended
    ));
}

#[test]
fn an_end_the_queue_could_not_retain_leaves_the_overflow_as_the_last_event() {
    let generation = Arc::new(());
    let desired = opened(&generation);
    let first = handle("watch", 1);
    desired.end(&relay_changed(&first), &generation);
    desired.overflow(&first, &generation);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Ended(first.clone()))
    );
    assert!(desired.admit(&SubscriptionEvent::ConsumerOverflow(first), &generation));

    desired.ended(&generation);
    assert_eq!(next_reported(&desired), None);
}

#[test]
fn an_end_leaves_a_deletion_in_flight_to_the_server() {
    let generation = Arc::new(());
    let desired = opened(&generation);
    let deleting = deletion(desired.cancel(&name("watch"), generation.clone()));
    desired.end(&relay_changed(&handle("watch", 1)), &generation);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Closing)
    );
    assert!(
        matches!(desired.deletion_target(&deleting), DeletionTarget::Server),
        "the server keeps the ended name until the deletion releases it"
    );
    desired.deleted(&deleting, DeletionResolution::Deleted);
    assert_eq!(desired.lifecycle(&name("watch")), None);
}

#[test]
fn an_end_of_another_generation_or_exchange_changes_nothing() {
    let first_generation = Arc::new(());
    let desired = opened(&first_generation);
    let first = handle("watch", 1);
    desired.end(&relay_changed(&handle("watch", 2)), &first_generation);
    desired.end(&relay_changed(&first), &Arc::new(()));
    desired.end(&relay_changed(&handle("other", 1)), &first_generation);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Active(first.clone()))
    );

    desired.ended(&first_generation);
    desired.end(&relay_changed(&first), &first_generation);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Interrupted(first))
    );
}
