//! State-machine tests for desired client subscriptions.

use std::num::NonZeroU64;

use meticulous::{OptionExt as _, ResultExt as _};
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
    assert_eq!(desired.take_interruption(), None);
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
    assert_eq!(
        desired.take_interruption(),
        Some(SubscriptionInterruption {
            subscription: first.clone()
        })
    );
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
    assert!(!desired.can_deliver(&first, &second_generation));
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
    assert!(desired.can_deliver(&reopened, &third_generation));
    assert!(!desired.can_deliver(&first, &third_generation));
}

#[test]
fn cancellation_fences_a_late_restore_and_name_reuse() {
    let desired = DesiredSubscriptions::new();
    let first_generation = Arc::new(());
    let initial = desired
        .begin(contract("watch", "tenant"), first_generation.clone())
        .verified("the registry starts empty");
    let first = handle("watch", 1);
    desired.created(&initial, Some(&first));
    desired.ended(&first_generation);
    let second_generation = Arc::new(());
    let mut attempts = desired.restore(second_generation.clone());
    let retry = attempts
        .pop()
        .verified("the acknowledged subscription is restored");

    let deletion = desired
        .cancel(&name("watch"), second_generation.clone())
        .verified("the subscription is not already being deleted");
    assert_eq!(desired.take_interruption(), None);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Closing)
    );
    assert!(desired.deletion_waits(&deletion));
    assert!(
        desired
            .cancel(&name("watch"), second_generation.clone())
            .is_none()
    );
    assert!(
        desired
            .begin(contract("watch", "other"), second_generation.clone())
            .is_none()
    );
    let late = handle("watch", 2);
    assert!(desired.created(&retry, Some(&late)));
    assert!(!desired.deletion_waits(&deletion));
    assert!(!desired.can_deliver(&late, &second_generation));

    desired.deleted(&deletion, true);
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
    let desired = DesiredSubscriptions::new();
    let generation = Arc::new(());
    let pending = desired
        .begin(contract("watch", "tenant"), generation.clone())
        .verified("the registry starts empty");
    desired.created(&pending, Some(&handle("watch", 1)));
    let first = desired
        .cancel(&name("watch"), generation.clone())
        .verified("deletion starts once");
    desired.deleted(&first, false);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::DeletionFailed)
    );
    assert!(
        desired
            .begin(contract("watch", "tenant"), generation.clone())
            .is_none()
    );
    let second = desired
        .cancel(&name("watch"), generation.clone())
        .verified("failed deletion can be retried");
    desired.deleted(&first, true);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Closing)
    );
    desired.deleted(&second, true);
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
    let first = desired
        .cancel(&name("watch"), first_generation.clone())
        .verified("no deletion is in flight");
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
    desired.deleted(&first, true);
    assert_eq!(
        desired.lifecycle(&name("watch")),
        Some(SubscriptionLifecycle::Creating)
    );
    desired.created(&next, Some(&handle("watch", 2)));
    assert!(desired.can_deliver(&handle("watch", 2), &second_generation));
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
    let deletion = desired
        .cancel(&name("watch"), generation.clone())
        .verified("deletion starts once");
    desired.deleted(&deletion, true);
    assert_eq!(desired.lifecycle(&name("watch")), None);
}

#[test]
fn repeated_exchange_losses_keep_one_pending_gap_per_subscription() {
    let desired = DesiredSubscriptions::new();
    let first_generation = Arc::new(());
    let first_attempt = desired
        .begin(contract("watch", "tenant"), first_generation.clone())
        .verified("the registry starts empty");
    let first = handle("watch", 1);
    desired.created(&first_attempt, Some(&first));
    desired.ended(&first_generation);

    let second_generation = Arc::new(());
    let mut attempts = desired.restore(second_generation.clone());
    let second_attempt = attempts
        .pop()
        .verified("the first acknowledgement is restorable");
    let second = handle("watch", 2);
    desired.created(&second_attempt, Some(&second));
    desired.ended(&second_generation);

    assert_eq!(
        desired.take_interruption(),
        Some(SubscriptionInterruption {
            subscription: second
        })
    );
    assert_eq!(desired.take_interruption(), None);
}
