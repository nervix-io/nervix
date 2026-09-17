//! Handler publication and membership waits racing each other, explored under Shuttle.
//!
//! Layer: test harness.
//!
//! - **Owns.** The publication invariants of the handler table and the lost-wakeup invariant of a
//!   membership wait.
//! - **Depends on.** The production request state and the interconnect Shuttle runner.
//! - **Must not know.** Sockets, peers, or what a registered handler answers.

use std::{collections::BTreeSet, marker::PhantomData, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::ClusterNodeName;
use rkyv::{Archive, Deserialize, Serialize};
use triomphe::Arc;

use super::{
    ErasedRequestHandler, HandlerRegistration, InterconnectRequest, RequestContext, RequestState,
    TypedRequestHandler,
};
use crate::{observation::TransportObservations, shuttle_test::check_random_and_pct};

#[derive(Debug, Archive, Serialize, Deserialize)]
struct Ping;

impl InterconnectRequest for Ping {
    type Response = ();

    const NAME: &'static str = "test_ping";
    const TIMEOUT: Duration = Duration::from_secs(1);
}

const DISTINCT_NAMES: [&str; 6] = [
    "test_first",
    "test_second",
    "test_third",
    "test_fourth",
    "test_fifth",
    "test_sixth",
];
const CONTESTED_NAME: &str = "test_contested";
const CONTESTED_REGISTRATIONS: usize = 2;

fn request_state() -> Arc<RequestState> {
    Arc::new(RequestState::new(
        1,
        Arc::new(TransportObservations::default()),
    ))
}

fn ping_handler() -> Arc<Box<dyn ErasedRequestHandler>> {
    let erased: Box<dyn ErasedRequestHandler> = Box::new(TypedRequestHandler::<Ping, _> {
        handler: Arc::new(|_context: RequestContext, _request: Ping| async {}),
        request: PhantomData,
    });
    Arc::new(erased)
}

/// Eight registrations publish at once: six under distinct names, and two contending for one name.
/// Every rebuilt table is published through a compare-and-swap that Shuttle may preempt around, so
/// a registration that loses the swap must rebuild from the table that won it.
fn racing_registrations_publish_every_handler_once() {
    let requests = request_state();
    let handler = ping_handler();

    let mut distinct = Vec::with_capacity(DISTINCT_NAMES.len());
    for name in DISTINCT_NAMES {
        let requests = Arc::clone(&requests);
        let registration = HandlerRegistration::Request(Arc::clone(&handler));
        distinct.push(shuttle::thread::spawn(move || {
            requests.publish_handler(name, registration)
        }));
    }
    let mut contested = Vec::with_capacity(CONTESTED_REGISTRATIONS);
    for _ in 0..CONTESTED_REGISTRATIONS {
        let requests = Arc::clone(&requests);
        let registration = HandlerRegistration::Request(Arc::clone(&handler));
        contested.push(shuttle::thread::spawn(move || {
            requests.publish_handler(CONTESTED_NAME, registration)
        }));
    }

    for registration in distinct {
        registration
            .join()
            .assured("a registration thread only publishes a handler table")
            .assured("every distinct name is registered by exactly one thread");
    }
    let mut contested_publications = 0_usize;
    for registration in contested {
        let published = registration
            .join()
            .assured("a registration thread only publishes a handler table");
        if published.is_ok() {
            contested_publications = contested_publications
                .checked_add(1)
                .assured("two threads contend for the contested name");
        }
    }
    assert_eq!(
        contested_publications, 1,
        "exactly one of the registrations racing for {CONTESTED_NAME} must publish it"
    );

    let table = requests.handlers.load_full();
    for name in DISTINCT_NAMES.into_iter().chain([CONTESTED_NAME]) {
        assert!(
            table.requests.contains_key(name),
            "the {name} registration was lost to a concurrent publication"
        );
    }
    let repeated = requests.publish_handler(
        DISTINCT_NAMES[0],
        HandlerRegistration::Request(Arc::clone(&handler)),
    );
    assert!(
        repeated.is_err(),
        "a published name cannot be registered again"
    );
}

#[test]
fn racing_registrations_lose_no_handler_and_publish_each_name_once() {
    check_random_and_pct(racing_registrations_publish_every_handler_once);
}

fn node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("the test node name follows the public node-name grammar")
}

/// A caller checks that its target is live and then waits for it to leave, while discovery
/// publishes a membership that keeps the target and then one without it. A publication that lands
/// between the caller's check and its wait must still end the wait: if it were lost, the caller
/// would wait forever and Shuttle would report the deadlock.
fn membership_change_before_the_wait_ends_it() {
    shuttle::future::block_on(async {
        let requests = request_state();
        let peer = node("node-a");
        let target = node("node-b");
        requests.replace_live_nodes(&BTreeSet::from([peer.clone(), target.clone()]));

        let caller = tokio::spawn({
            let requests = Arc::clone(&requests);
            let target = target.clone();
            async move {
                if requests.target_is_live(&target) {
                    requests.target_left(&target).await;
                }
                assert!(
                    !requests.target_is_live(&target),
                    "a membership wait ended while its target was still live"
                );
            }
        });
        let discovery = tokio::spawn({
            let requests = Arc::clone(&requests);
            async move {
                requests.replace_live_nodes(&BTreeSet::from([peer.clone(), target]));
                requests.replace_live_nodes(&BTreeSet::from([peer]));
            }
        });

        discovery
            .await
            .assured("publishing membership never panics");
        caller
            .await
            .assured("the caller panics only on a violated invariant, which fails the check");
    });
}

#[test]
fn a_membership_change_between_a_callers_check_and_its_wait_is_never_lost() {
    check_random_and_pct(membership_change_before_the_wait_ends_it);
}
