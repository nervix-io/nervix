//! Endpoint publication and intake lifetime regressions.
//!
//! Layer: test harness.
//! - **Owns.** The current route table's visible-route and retained-intake contracts.
//! - **Depends on.** The production endpoint table and typed domain identities.
//! - **Must not know.** HTTP transport, decoding, or graph scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::collections::BTreeMap;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{IngestorName, ModelKind};
use nervix_primitives::sync::Arc;

use super::*;

fn domain(index: u8) -> DomainName {
    DomainName::parse(&format!("domain_{index}")).assured("the test uses valid domain names")
}

fn identity(domain: &DomainName) -> DomainNodeRef {
    DomainNodeRef::node_in(
        domain.clone(),
        ModelKind::Ingestor,
        IngestorName::parse("source").assured("the test uses a valid ingestor name"),
    )
}

fn key(host: u8, path: u8) -> HttpRouteKey {
    HttpRouteKey {
        host: format!("host{host}.example.com"),
        path: format!("/path{path}"),
    }
}

fn endpoint(endpoint_type: EndpointType) -> RoutedEndpoint {
    RoutedEndpoint {
        endpoint_type,
        signaling_protocol: None,
    }
}

/// Publishes `definitions` for `domain`, which no other domain of the test publishes.
fn publish(
    routes: &EndpointIntakeRoutes<usize>,
    domain: &DomainName,
    definitions: Vec<(HttpRouteKey, RoutedEndpoint)>,
) {
    routes
        .replace_domain(domain, definitions)
        .assured("the test publishes no host and path another domain owns");
}

fn intakes(route: &EndpointIntakeRoute<usize>) -> Vec<usize> {
    route
        .bindings()
        .iter()
        .filter_map(|binding| binding.intake().as_deref().copied())
        .collect()
}

#[test]
fn resolved_routes_borrow_one_intake_allocation_until_unbind() {
    // An intake need not implement Clone. The table only shares its lifetime handle.
    struct Intake;
    let routes = EndpointIntakeRoutes::default();
    let owner = domain(0);
    let keys = [key(0, 0), key(1, 0)];
    publish(
        &routes,
        &owner,
        keys.iter()
            .cloned()
            .map(|key| (key, endpoint(EndpointType::Http)))
            .collect(),
    );
    let binding = routes.bind(identity(&owner), Intake, &keys);
    let resolved = routes
        .resolve("host0.example.com", "/path0")
        .into_route()
        .assured("the route is installed");
    let lease = binding.intake();
    let intake = lease.as_deref().assured("the source is bound");
    for _ in 0..20 {
        let repeated = routes
            .resolve("host0.example.com:8080", "/path0")
            .into_route()
            .assured("the same route is installed");
        assert!(Arc::ptr_eq(&resolved, &repeated));
        assert!(std::ptr::eq(
            intake,
            repeated.bindings()[0]
                .intake()
                .as_deref()
                .assured("the source is bound")
        ));
    }
    routes.unbind(&binding, &keys);
    assert!(binding.intake().is_none());
    assert!(resolved.bindings()[0].intake().is_none());
    let current = routes
        .resolve("host1.example.com", "/path0")
        .into_route()
        .assured("the configured route remains installed");
    assert!(current.bindings().is_empty());
}

#[test]
fn replacing_one_domain_keeps_other_domains_and_ends_retained_intakes() {
    let routes = EndpointIntakeRoutes::default();
    let first = domain(0);
    let second = domain(1);
    let first_keys = [key(0, 0)];
    let second_keys = [key(1, 0)];
    publish(
        &routes,
        &first,
        vec![(first_keys[0].clone(), endpoint(EndpointType::Http))],
    );
    publish(
        &routes,
        &second,
        vec![(second_keys[0].clone(), endpoint(EndpointType::Websockets))],
    );
    routes.mark_installed();
    let first_binding = routes.bind(identity(&first), 1, &first_keys);
    let second_binding = routes.bind(identity(&second), 2, &second_keys);
    let retained = routes
        .resolve("host0.example.com", "/path0")
        .into_route()
        .assured("the first domain publishes the route");
    assert_eq!(retained.endpoint_type(), EndpointType::Http);
    assert_eq!(intakes(&retained), vec![1]);
    publish(
        &routes,
        &first,
        vec![(key(1, 1), endpoint(EndpointType::Http))],
    );
    assert!(first_binding.intake().is_none());
    assert!(intakes(&retained).is_empty());
    assert!(matches!(
        routes.resolve("host0.example.com", "/path0"),
        EndpointResolution::Unrouted
    ));
    let current = routes
        .resolve("host1.example.com", "/path0")
        .into_route()
        .assured("the second domain still publishes its route");
    assert_eq!(current.endpoint_type(), EndpointType::Websockets);
    assert_eq!(intakes(&current), vec![2]);
    routes.withdraw_domain(&second);
    assert!(second_binding.intake().is_none());
    assert!(matches!(
        routes.resolve("host1.example.com", "/path0"),
        EndpointResolution::Unrouted
    ));
    assert_eq!(
        routes
            .resolve("host1.example.com", "/path1")
            .into_route()
            .assured("the first domain's new route is installed")
            .endpoint_type(),
        EndpointType::Http
    );
}

#[test]
fn a_route_another_domain_publishes_refuses_the_whole_replacement() {
    let routes = EndpointIntakeRoutes::default();
    let owner = domain(0);
    let claimant = domain(1);
    let shared = [key(0, 0)];
    publish(
        &routes,
        &owner,
        vec![(shared[0].clone(), endpoint(EndpointType::Http))],
    );
    publish(
        &routes,
        &claimant,
        vec![(key(1, 0), endpoint(EndpointType::Http))],
    );
    routes.mark_installed();
    let owner_binding = routes.bind(identity(&owner), 1, &shared);
    let claimant_binding = routes.bind(identity(&claimant), 2, &[key(1, 0)]);

    let refused = routes.replace_domain(
        &claimant,
        vec![
            (key(1, 1), endpoint(EndpointType::Websockets)),
            (shared[0].clone(), endpoint(EndpointType::Websockets)),
        ],
    );
    assert_eq!(
        refused,
        Err(EndpointRouteConflict {
            host: "host0.example.com".to_owned(),
            path: "/path0".to_owned(),
            owner: owner.clone(),
        })
    );

    // The refused replacement changed nothing: the owner keeps its route and intake, the
    // claimant keeps the routes and intake it had, and its new route is not published.
    assert_eq!(owner_binding.intake().as_deref(), Some(&1));
    assert_eq!(claimant_binding.intake().as_deref(), Some(&2));
    let shared_route = routes
        .resolve("host0.example.com", "/path0")
        .into_route()
        .assured("the owner still publishes the route");
    assert_eq!(shared_route.endpoint_type(), EndpointType::Http);
    assert_eq!(intakes(&shared_route), vec![1]);
    let claimant_route = routes
        .resolve("host1.example.com", "/path0")
        .into_route()
        .assured("the claimant keeps the route it had");
    assert_eq!(intakes(&claimant_route), vec![2]);
    assert!(matches!(
        routes.resolve("host1.example.com", "/path1"),
        EndpointResolution::Unrouted
    ));
}

#[test]
fn unbinding_a_preceding_source_does_not_unbind_its_replacement() {
    let routes = EndpointIntakeRoutes::default();
    let owner = domain(0);
    let keys = [key(0, 0)];
    publish(
        &routes,
        &owner,
        vec![(keys[0].clone(), endpoint(EndpointType::Http))],
    );
    routes.mark_installed();
    let preceding = routes.bind(identity(&owner), 1, &keys);
    let retained = routes
        .resolve("host0.example.com", "/path0")
        .into_route()
        .assured("the preceding route is installed");
    let replacement = routes.bind(identity(&owner), 2, &keys);
    routes.unbind(&preceding, &keys);
    assert!(intakes(&retained).is_empty());
    assert_eq!(replacement.intake().as_deref(), Some(&2));
    assert_eq!(
        intakes(
            &routes
                .resolve("host0.example.com", "/path0")
                .into_route()
                .assured("the replacement route is installed")
        ),
        vec![2]
    );
    routes.clear();
    assert!(replacement.intake().is_none());
    // Clearing the runtime returns the table to awaiting its next installation.
    assert!(matches!(
        routes.resolve("host0.example.com", "/path0"),
        EndpointResolution::Uninstalled
    ));
}

#[test]
fn bind_after_domain_teardown_cannot_republish_a_route() {
    let routes = EndpointIntakeRoutes::default();
    let owner = domain(0);
    let keys = [key(0, 0)];
    publish(
        &routes,
        &owner,
        vec![(keys[0].clone(), endpoint(EndpointType::Http))],
    );
    routes.mark_installed();
    routes.withdraw_domain(&owner);
    let binding = routes.bind(identity(&owner), 1, &keys);
    assert!(matches!(
        routes.resolve("host0.example.com", "/path0"),
        EndpointResolution::Unrouted
    ));
    routes.unbind(&binding, &keys);
}

#[test]
fn a_missing_route_is_final_only_once_a_revision_installed_the_table() {
    let routes = EndpointIntakeRoutes::<usize>::default();
    let owner = domain(0);
    let published = key(0, 0);
    let missing = key(1, 1);
    assert!(matches!(
        routes.resolve(&missing.host, &missing.path),
        EndpointResolution::Uninstalled
    ));
    publish(
        &routes,
        &owner,
        vec![(published.clone(), endpoint(EndpointType::Http))],
    );
    // A route that is already published is served before the revision completes.
    assert!(matches!(
        routes.resolve(&published.host, &published.path),
        EndpointResolution::Routed(_)
    ));
    assert!(matches!(
        routes.resolve(&missing.host, &missing.path),
        EndpointResolution::Uninstalled
    ));
    routes.mark_installed();
    assert!(matches!(
        routes.resolve(&missing.host, &missing.path),
        EndpointResolution::Unrouted
    ));
    routes.withdraw_domain(&owner);
    assert!(matches!(
        routes.resolve(&published.host, &published.path),
        EndpointResolution::Unrouted
    ));
    routes.mark_installed();
    assert!(matches!(
        routes.resolve(&published.host, &published.path),
        EndpointResolution::Unrouted
    ));
    routes.clear();
    assert!(matches!(
        routes.resolve(&published.host, &published.path),
        EndpointResolution::Uninstalled
    ));
}

#[test]
fn a_retained_route_keeps_its_signaling_allocation_across_replacement() {
    use nervix_models::{SignalingProtocolName, SignalingProtocolOnConnect, SignalingWireFormat};

    let protocol = Arc::new(
        CompiledSignalingProtocol::compile_parts(
            &SignalingProtocolName::parse("handshake")
                .assured("the test uses a valid protocol name"),
            &SignalingWireFormat::Json,
            &SignalingProtocolOnConnect {
                accept_data: true,
                steps: Vec::new(),
                fail_matchers: Vec::new(),
                timeout: "5s".to_string(),
            },
            None,
        )
        .assured("an empty native signaling protocol compiles"),
    );
    let routes = EndpointIntakeRoutes::<usize>::default();
    let owner = domain(0);
    let key = key(0, 0);
    publish(
        &routes,
        &owner,
        vec![(
            key.clone(),
            RoutedEndpoint {
                endpoint_type: EndpointType::Websockets,
                signaling_protocol: Some(protocol.clone()),
            },
        )],
    );
    let retained = routes
        .resolve(&key.host, &key.path)
        .into_route()
        .assured("the owner publishes the route");
    assert!(Arc::ptr_eq(
        retained
            .signaling_protocol()
            .assured("the published endpoint has signaling"),
        &protocol
    ));
    publish(
        &routes,
        &owner,
        vec![(key.clone(), endpoint(EndpointType::Http))],
    );
    let current = routes
        .resolve(&key.host, &key.path)
        .into_route()
        .assured("the owner publishes its replacement route");
    assert_eq!(current.endpoint_type(), EndpointType::Http);
    assert!(current.signaling_protocol().is_none());
    assert!(Arc::ptr_eq(
        retained
            .signaling_protocol()
            .assured("retained signaling remains pinned"),
        &protocol
    ));
}

#[test]
fn bolero_endpoint_sequences_preserve_visible_routes_and_intake_lifetimes() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            let routes = EndpointIntakeRoutes::default();
            let mut configured: BTreeMap<DomainName, ahash::HashMap<HttpRouteKey, EndpointType>> =
                BTreeMap::new();
            // Whether a revision installed the table since it was created or last cleared, which
            // decides whether a route no domain publishes is final.
            let mut installed = false;
            struct Binding {
                owner: DomainName,
                key: HttpRouteKey,
                handle: EndpointBinding<usize>,
                live: bool,
            }
            struct Retained {
                route: Arc<EndpointIntakeRoute<usize>>,
                intakes: Vec<usize>,
            }
            let mut bindings: BTreeMap<usize, Binding> = BTreeMap::new();
            // At most four visible routes per step of the bounded 64-operation sequence.
            let mut retained: Vec<Retained> = Vec::new();
            for (step, byte) in bytes.iter().copied().enumerate() {
                let owner = domain((byte / 5) % 2);
                let selected_key = key((byte / 10) % 2, (byte / 20) % 2);
                match byte % 6 {
                    0 => {
                        let kind = if byte & 128 == 0 {
                            EndpointType::Http
                        } else {
                            EndpointType::Websockets
                        };
                        let mut published_by_another = None;
                        for (other, other_routes) in &configured {
                            if *other != owner && other_routes.contains_key(&selected_key) {
                                published_by_another = Some(other.clone());
                            }
                        }
                        let outcome = routes
                            .replace_domain(&owner, vec![(selected_key.clone(), endpoint(kind))]);
                        match published_by_another {
                            // A refused replacement leaves the table, and so the reference, as
                            // they were.
                            Some(other) => {
                                assert_eq!(
                                    outcome,
                                    Err(EndpointRouteConflict {
                                        host: selected_key.host.clone(),
                                        path: selected_key.path.clone(),
                                        owner: other,
                                    })
                                );
                            }
                            None => {
                                outcome.assured("no other domain publishes the route");
                                configured.insert(
                                    owner.clone(),
                                    ahash::HashMap::from_iter([(selected_key, kind)]),
                                );
                                for binding in bindings.values_mut() {
                                    if binding.owner == owner {
                                        binding.live = false;
                                    }
                                }
                            }
                        }
                    }
                    1 => {
                        if let Some(domain_routes) = configured.get(&owner)
                            && domain_routes.contains_key(&selected_key)
                        {
                            let handle = routes.bind(
                                identity(&owner),
                                step,
                                std::slice::from_ref(&selected_key),
                            );
                            bindings.insert(
                                step,
                                Binding {
                                    owner,
                                    key: selected_key,
                                    handle,
                                    live: true,
                                },
                            );
                        }
                    }
                    2 => {
                        // A generated source identity may be absent; absence leaves the table intact.
                        if let Some(binding) = bindings.get_mut(&(usize::from(byte) % 64)) {
                            routes.unbind(&binding.handle, std::slice::from_ref(&binding.key));
                            binding.live = false;
                        }
                    }
                    3 => {
                        routes.withdraw_domain(&owner);
                        configured.remove(&owner);
                        for binding in bindings.values_mut() {
                            if binding.owner == owner {
                                binding.live = false;
                            }
                        }
                    }
                    4 => {
                        routes.clear();
                        configured.clear();
                        installed = false;
                        for binding in bindings.values_mut() {
                            binding.live = false;
                        }
                    }
                    _ => {
                        routes.mark_installed();
                        installed = true;
                    }
                }
                for (id, binding) in &bindings {
                    let expected = if binding.live { Some(id) } else { None };
                    assert_eq!(binding.handle.intake().as_deref(), expected);
                }
                for host in 0..2 {
                    for path in 0..2 {
                        let key = key(host, path);
                        // At most one domain publishes a route, so its kind is that domain's.
                        let mut expected_kind = None;
                        for domain_routes in configured.values() {
                            if let Some(kind) = domain_routes.get(&key) {
                                assert!(expected_kind.is_none());
                                expected_kind = Some(*kind);
                            }
                        }
                        let actual = routes.resolve(&key.host, &key.path);
                        let Some(kind) = expected_kind else {
                            if installed {
                                assert!(matches!(actual, EndpointResolution::Unrouted));
                            } else {
                                assert!(matches!(actual, EndpointResolution::Uninstalled));
                            }
                            continue;
                        };
                        let actual = actual
                            .into_route()
                            .assured("the reference contains a configured route");
                        assert_eq!(actual.endpoint_type(), kind);
                        let mut expected = Vec::new();
                        for (id, binding) in &bindings {
                            if binding.live && binding.key == key {
                                expected.push(*id);
                            }
                        }
                        assert_eq!(intakes(&actual), expected);
                        retained.push(Retained {
                            route: actual,
                            intakes: expected,
                        });
                    }
                }
                for snapshot in &retained {
                    let mut expected = Vec::new();
                    for id in &snapshot.intakes {
                        if bindings
                            .get(id)
                            .assured("a retained route names an existing source")
                            .live
                        {
                            expected.push(*id);
                        }
                    }
                    assert_eq!(intakes(&snapshot.route), expected);
                }
            }
        });
}
