//! Publication and source-ending races over the production endpoint owner.
//!
//! Layer: test harness.
//! - **Owns.** Whole-table observations and admission through retained source lifetimes.
//! - **Depends on.** The production endpoint publisher and the model harness's Shuttle runner.
//! - **Must not know.** HTTP sockets or the publication primitive's internal memory ordering.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::ResultExt as _;
use nervix_model_harness::shuttle::check_interleavings;
use nervix_models::{IngestorName, ModelKind};
use nervix_primitives::thread;

use super::*;

const JOINS: &str = "Shuttle fails the execution when a model thread panics";

fn domain() -> DomainName {
    DomainName::parse("endpoint_model").assured("the check uses a valid domain name")
}

fn identity() -> DomainNodeRef {
    DomainNodeRef::node_in(
        domain(),
        ModelKind::Ingestor,
        IngestorName::parse("source").assured("the check uses a valid ingestor name"),
    )
}

fn key(path: &str) -> HttpRouteKey {
    HttpRouteKey {
        host: "edge.example.com".to_string(),
        path: path.to_string(),
    }
}

fn endpoint(endpoint_type: EndpointType) -> RoutedEndpoint {
    let kind = match endpoint_type {
        EndpointType::Http => nervix_models::EndpointKind::Http,
        EndpointType::Websockets => nervix_models::EndpointKind::Websockets {
            signaling_protocol: None,
        },
    };
    RoutedEndpoint { kind }
}

fn publication_keeps_every_route_of_a_domain_in_one_revision() {
    let routes = Arc::new(EndpointIntakeRoutes::<usize>::default());
    routes.replace_domain(
        &domain(),
        vec![
            (key("/a"), endpoint(EndpointType::Http)),
            (key("/b"), endpoint(EndpointType::Http)),
        ],
    );
    let writer_routes = routes.clone();
    let writer = thread::spawn(move || {
        writer_routes.replace_domain(
            &domain(),
            vec![
                (key("/a"), endpoint(EndpointType::Websockets)),
                (key("/b"), endpoint(EndpointType::Websockets)),
            ],
        );
    });
    let reader = thread::spawn(move || {
        for _ in 0..3 {
            let snapshot = routes.current.load();
            let a = snapshot
                .route("edge.example.com", "/a")
                .assured("every revision defines a");
            let b = snapshot
                .route("edge.example.com", "/b")
                .assured("every revision defines b");
            assert_eq!(
                a.endpoint_type(),
                b.endpoint_type(),
                "a reader observes the complete installed revision"
            );
            thread::yield_now();
        }
    });
    writer.join().assured(JOINS);
    reader.join().assured(JOINS);
}

#[test]
fn shuttle_endpoint_publication_keeps_every_route_of_a_domain_in_one_revision() {
    check_interleavings(publication_keeps_every_route_of_a_domain_in_one_revision);
}

fn unbind_fences_retained_requests_and_preserves_a_concurrent_replacement() {
    let routes = Arc::new(EndpointIntakeRoutes::default());
    let keys = [key("/events")];
    routes.replace_domain(
        &domain(),
        vec![(keys[0].clone(), endpoint(EndpointType::Websockets))],
    );
    let preceding = routes.bind(identity(), 1, &keys);
    let retained = routes
        .resolve("edge.example.com", "/events")
        .into_route()
        .assured("the initial route is installed");
    let ending_routes = routes.clone();
    let ending_keys = keys.clone();
    let ending = thread::spawn(move || ending_routes.unbind(&preceding, &ending_keys));
    let replacement_routes = routes.clone();
    let replacement_keys = keys.clone();
    let replacing =
        thread::spawn(move || replacement_routes.bind(identity(), 2, &replacement_keys));
    let observing = thread::spawn(move || {
        // A request admitted before unbind may finish with the intake it borrowed. Its route
        // keeps that allocation alive; later admission always reads the source lifetime again.
        let admitted = retained.bindings()[0].intake();
        thread::yield_now();
        if let Some(value) = admitted.as_deref() {
            assert_eq!(*value, 1);
        }
        retained
    });
    ending.join().assured(JOINS);
    let replacement = replacing.join().assured(JOINS);
    let retained = observing.join().assured(JOINS);
    assert!(
        retained.bindings()[0].intake().is_none(),
        "completed unbind ends intake through the preceding table"
    );
    assert_eq!(replacement.intake().as_deref(), Some(&2));
    let current = routes
        .resolve("edge.example.com", "/events")
        .into_route()
        .assured("the configured route remains installed");
    let live: Vec<_> = current
        .bindings()
        .iter()
        .filter_map(|binding| binding.intake().as_deref().copied())
        .collect();
    assert_eq!(
        live,
        vec![2],
        "unbind removes only the source lifetime it ended"
    );
}

#[test]
fn shuttle_endpoint_unbind_fences_retained_requests_and_preserves_a_concurrent_replacement() {
    check_interleavings(unbind_fences_retained_requests_and_preserves_a_concurrent_replacement);
}

fn domain_teardown_ends_intake_even_when_a_request_retains_the_table() {
    let routes = Arc::new(EndpointIntakeRoutes::default());
    let keys = [key("/events")];
    routes.replace_domain(
        &domain(),
        vec![(keys[0].clone(), endpoint(EndpointType::Http))],
    );
    routes.mark_installed();
    routes.bind(identity(), 1, &keys);
    let ending_routes = routes.clone();
    let ending = thread::spawn(move || ending_routes.withdraw_domain(&domain()));
    let retained = routes.resolve("edge.example.com", "/events");
    ending.join().assured(JOINS);
    if let EndpointResolution::Routed(retained) = retained {
        assert!(retained.bindings()[0].intake().is_none());
    }
    assert!(matches!(
        routes.resolve("edge.example.com", "/events"),
        EndpointResolution::Unrouted
    ));
}

#[test]
fn shuttle_endpoint_domain_teardown_ends_intake_even_when_a_request_retains_the_table() {
    check_interleavings(domain_teardown_ends_intake_even_when_a_request_retains_the_table);
}

fn a_stopping_domains_withdrawal_keeps_the_intake_a_retained_request_borrowed() {
    let routes = Arc::new(EndpointIntakeRoutes::default());
    let keys = [key("/events")];
    routes.replace_domain(
        &domain(),
        vec![(keys[0].clone(), endpoint(EndpointType::Http))],
    );
    routes.mark_installed();
    let source = routes.bind(identity(), 1, &keys);
    let withdrawing_routes = routes.clone();
    let withdrawing = thread::spawn(move || withdrawing_routes.withdraw_stopping_domain(&domain()));
    // A request resolves the route before or after the stop withdraws it. One that resolved it
    // before keeps the intake it borrowed, which a withdrawal, unlike a teardown, never ends.
    let retained = routes.resolve("edge.example.com", "/events");
    withdrawing.join().assured(JOINS);
    if let EndpointResolution::Routed(retained) = retained {
        assert_eq!(
            retained.bindings()[0].intake().as_deref(),
            Some(&1),
            "a withdrawal never ends the intake a request borrowed"
        );
    }
    assert!(
        matches!(
            routes.resolve("edge.example.com", "/events"),
            EndpointResolution::Unrouted
        ),
        "a request that arrives after the withdrawal finds no route"
    );
    assert_eq!(source.intake().as_deref(), Some(&1));
}

#[test]
fn shuttle_endpoint_a_stopping_domains_withdrawal_keeps_the_intake_a_retained_request_borrowed() {
    check_interleavings(a_stopping_domains_withdrawal_keeps_the_intake_a_retained_request_borrowed);
}

fn a_source_that_ends_while_its_stop_is_called_off_is_never_published_again() {
    let routes = Arc::new(EndpointIntakeRoutes::default());
    let keys = [key("/events")];
    routes.replace_domain(
        &domain(),
        vec![(keys[0].clone(), endpoint(EndpointType::Http))],
    );
    routes.mark_installed();
    let ending_source = routes.bind(identity(), 1, &keys);
    let staying_source = routes.bind(identity(), 2, &keys);
    routes.withdraw_stopping_domain(&domain());
    let ending_routes = routes.clone();
    let ending_keys = keys.clone();
    let ending = thread::spawn(move || ending_routes.unbind(&ending_source, &ending_keys));
    let restoring_routes = routes.clone();
    let restoring = thread::spawn(move || restoring_routes.restore_withdrawn_domain(&domain()));
    ending.join().assured(JOINS);
    restoring.join().assured(JOINS);
    let current = routes
        .resolve("edge.example.com", "/events")
        .into_route()
        .assured("the called-off stop published the route again");
    assert_eq!(
        current.bindings().len(),
        1,
        "a source that ended while the stop was called off is never published again"
    );
    assert_eq!(current.bindings()[0].intake().as_deref(), Some(&2));
    assert_eq!(staying_source.intake().as_deref(), Some(&2));
}

#[test]
fn shuttle_endpoint_a_source_that_ends_while_its_stop_is_called_off_is_never_published_again() {
    check_interleavings(a_source_that_ends_while_its_stop_is_called_off_is_never_published_again);
}
