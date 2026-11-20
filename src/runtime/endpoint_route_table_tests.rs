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

use meticulous::ResultExt as _;
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
    let kind = match endpoint_type {
        EndpointType::Http => nervix_models::EndpointKind::Http,
        EndpointType::Websockets => nervix_models::EndpointKind::Websockets {
            signaling_protocol: None,
        },
    };
    RoutedEndpoint { kind }
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
    routes.replace_domain(
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
    let keys = [key(0, 0)];
    routes.replace_domain(
        &first,
        vec![(keys[0].clone(), endpoint(EndpointType::Http))],
    );
    routes.replace_domain(
        &second,
        vec![(keys[0].clone(), endpoint(EndpointType::Websockets))],
    );
    routes.mark_installed();
    let first_binding = routes.bind(identity(&first), 1, &keys);
    let second_binding = routes.bind(identity(&second), 2, &keys);
    let retained = routes
        .resolve("host0.example.com", "/path0")
        .into_route()
        .assured("both domains publish the route");
    assert_eq!(retained.endpoint_type(), EndpointType::Websockets);
    assert_eq!(intakes(&retained), vec![1, 2]);
    routes.replace_domain(&first, vec![(key(1, 1), endpoint(EndpointType::Http))]);
    assert!(first_binding.intake().is_none());
    assert_eq!(intakes(&retained), vec![2]);
    let current = routes
        .resolve("host0.example.com", "/path0")
        .into_route()
        .assured("the second domain still publishes the route");
    assert_eq!(intakes(&current), vec![2]);
    routes.withdraw_domain(&second);
    assert!(second_binding.intake().is_none());
    assert!(matches!(
        routes.resolve("host0.example.com", "/path0"),
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
fn unbinding_a_preceding_source_does_not_unbind_its_replacement() {
    let routes = EndpointIntakeRoutes::default();
    let owner = domain(0);
    let keys = [key(0, 0)];
    routes.replace_domain(
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
    routes.replace_domain(
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
    routes.replace_domain(
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
fn a_retained_route_keeps_its_selected_signaling_allocation() {
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
    let first = domain(0);
    let second = domain(1);
    let key = key(0, 0);
    routes.replace_domain(
        &first,
        vec![(
            key.clone(),
            RoutedEndpoint {
                kind: nervix_models::EndpointKind::Websockets {
                    signaling_protocol: Some(protocol.clone()),
                },
            },
        )],
    );
    routes.replace_domain(&second, vec![(key.clone(), endpoint(EndpointType::Http))]);
    let retained = routes
        .resolve(&key.host, &key.path)
        .into_route()
        .assured("both domains publish the route");
    assert!(Arc::ptr_eq(
        retained
            .signaling_protocol()
            .assured("the selected endpoint has signaling"),
        &protocol
    ));
    routes.withdraw_domain(&first);
    let current = routes
        .resolve(&key.host, &key.path)
        .into_route()
        .assured("the second domain still publishes its route");
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
fn a_stopping_domains_routes_are_withdrawn_while_its_bindings_keep_their_intake() {
    let routes = EndpointIntakeRoutes::default();
    let stopping = domain(0);
    let running = domain(1);
    let shared = key(0, 0);
    let own = key(1, 0);
    routes.replace_domain(
        &stopping,
        vec![
            (shared.clone(), endpoint(EndpointType::Http)),
            (own.clone(), endpoint(EndpointType::Http)),
        ],
    );
    routes.replace_domain(
        &running,
        vec![(shared.clone(), endpoint(EndpointType::Http))],
    );
    routes.mark_installed();
    let stopping_binding = routes.bind(identity(&stopping), 1, &[shared.clone(), own]);
    let running_binding = routes.bind(identity(&running), 2, std::slice::from_ref(&shared));
    let retained = routes
        .resolve("host0.example.com", "/path0")
        .into_route()
        .assured("the shared route is installed");

    routes.withdraw_stopping_domain(&stopping);

    // A request that arrives now finds only the running domain on the shared route, and no route
    // where only the stopping domain published one.
    let current = routes
        .resolve("host0.example.com", "/path0")
        .into_route()
        .assured("the running domain still publishes the shared route");
    assert_eq!(intakes(&current), vec![2]);
    assert!(matches!(
        routes.resolve("host1.example.com", "/path0"),
        EndpointResolution::Unrouted
    ));
    // The stopping domain's binding keeps its intake for a request that already resolved it.
    assert_eq!(stopping_binding.intake().as_deref(), Some(&1));
    assert_eq!(intakes(&retained), vec![1, 2]);
    assert_eq!(running_binding.intake().as_deref(), Some(&2));
}

#[test]
fn a_called_off_stop_publishes_its_routes_again_with_each_live_binding_in_its_place() {
    let routes = EndpointIntakeRoutes::default();
    let stopping = domain(0);
    let running = domain(1);
    let shared = key(0, 0);
    let keys = std::slice::from_ref(&shared);
    routes.replace_domain(
        &stopping,
        vec![(shared.clone(), endpoint(EndpointType::Http))],
    );
    routes.replace_domain(
        &running,
        vec![(shared.clone(), endpoint(EndpointType::Http))],
    );
    routes.mark_installed();
    let first = routes.bind(identity(&stopping), 1, keys);
    let between = routes.bind(identity(&running), 2, keys);
    let ending = routes.bind(identity(&stopping), 3, keys);
    routes.withdraw_stopping_domain(&stopping);
    routes.unbind(&ending, keys);
    // A source of the stopping domain bound while its routes are withdrawn waits with them.
    let later = routes.bind(identity(&stopping), 4, keys);
    let during = routes
        .resolve("host0.example.com", "/path0")
        .into_route()
        .assured("the running domain publishes the shared route");
    assert_eq!(intakes(&during), vec![2]);

    routes.restore_withdrawn_domain(&stopping);

    let restored = routes
        .resolve("host0.example.com", "/path0")
        .into_route()
        .assured("the called-off stop published the route again");
    assert_eq!(
        intakes(&restored),
        vec![1, 2, 4],
        "every live binding returns in the order the table created it"
    );
    assert_eq!(
        restored.bindings().len(),
        3,
        "a binding that ended while withdrawn is never published again"
    );
    assert_eq!(first.intake().as_deref(), Some(&1));
    assert_eq!(between.intake().as_deref(), Some(&2));
    assert_eq!(later.intake().as_deref(), Some(&4));
}

#[test]
fn the_teardown_of_a_stopped_domain_ends_the_bindings_its_stop_withdrew() {
    let routes = EndpointIntakeRoutes::default();
    let owner = domain(0);
    let keys = [key(0, 0)];
    routes.replace_domain(
        &owner,
        vec![(keys[0].clone(), endpoint(EndpointType::Http))],
    );
    routes.mark_installed();
    let binding = routes.bind(identity(&owner), 1, &keys);
    routes.withdraw_stopping_domain(&owner);

    routes.withdraw_domain(&owner);

    assert!(binding.intake().is_none());
    routes.restore_withdrawn_domain(&owner);
    assert!(matches!(
        routes.resolve("host0.example.com", "/path0"),
        EndpointResolution::Unrouted
    ));
}

#[test]
fn a_rebuild_while_stopping_publishes_routes_its_stop_withdraws_again() {
    let routes = EndpointIntakeRoutes::default();
    let owner = domain(0);
    let keys = [key(0, 0)];
    let definitions = vec![(keys[0].clone(), endpoint(EndpointType::Http))];
    routes.replace_domain(&owner, definitions.clone());
    routes.mark_installed();
    let binding = routes.bind(identity(&owner), 1, &keys);
    routes.withdraw_stopping_domain(&owner);

    // A rebuild tears the execution down, ending its bindings, and publishes the definitions anew.
    routes.replace_domain(&owner, definitions);
    assert!(binding.intake().is_none());
    let rebuilt = routes
        .resolve("host0.example.com", "/path0")
        .into_route()
        .assured("the rebuild published the route");
    assert!(rebuilt.bindings().is_empty());
    routes.withdraw_stopping_domain(&owner);
    routes.withdraw_stopping_domain(&owner);
    assert!(matches!(
        routes.resolve("host0.example.com", "/path0"),
        EndpointResolution::Unrouted
    ));

    routes.restore_withdrawn_domain(&owner);
    let restored = routes
        .resolve("host0.example.com", "/path0")
        .into_route()
        .assured("the called-off stop published the route again");
    assert!(restored.bindings().is_empty());
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
            // The routes each stopping domain published before its stop withdrew them. A domain is
            // configured or withdrawn, never both.
            let mut withdrawn: BTreeMap<DomainName, ahash::HashMap<HttpRouteKey, EndpointType>> =
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
                match byte % 8 {
                    0 => {
                        let kind = if byte & 128 == 0 {
                            EndpointType::Http
                        } else {
                            EndpointType::Websockets
                        };
                        routes.replace_domain(&owner, vec![(selected_key.clone(), endpoint(kind))]);
                        configured.insert(
                            owner.clone(),
                            ahash::HashMap::from_iter([(selected_key, kind)]),
                        );
                        withdrawn.remove(&owner);
                        for binding in bindings.values_mut() {
                            if binding.owner == owner {
                                binding.live = false;
                            }
                        }
                    }
                    1 => {
                        // A source binds a route its domain publishes, or one its domain's stop
                        // withdrew, where it waits for the stop to be called off.
                        let published = configured
                            .get(&owner)
                            .is_some_and(|domain_routes| domain_routes.contains_key(&selected_key));
                        let waiting = withdrawn
                            .get(&owner)
                            .is_some_and(|domain_routes| domain_routes.contains_key(&selected_key));
                        if published || waiting {
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
                        withdrawn.remove(&owner);
                        for binding in bindings.values_mut() {
                            if binding.owner == owner {
                                binding.live = false;
                            }
                        }
                    }
                    4 => {
                        routes.clear();
                        configured.clear();
                        withdrawn.clear();
                        installed = false;
                        for binding in bindings.values_mut() {
                            binding.live = false;
                        }
                    }
                    5 => {
                        routes.withdraw_stopping_domain(&owner);
                        if let Some(domain_routes) = configured.remove(&owner) {
                            withdrawn.insert(owner, domain_routes);
                        }
                    }
                    6 => {
                        routes.restore_withdrawn_domain(&owner);
                        if let Some(domain_routes) = withdrawn.remove(&owner) {
                            configured.insert(owner, domain_routes);
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
                        let mut expected_kind = None;
                        for domain_routes in configured.values() {
                            if let Some(kind) = domain_routes.get(&key)
                                && (expected_kind.is_none() || *kind == EndpointType::Websockets)
                            {
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
                        // A route lists every live source bound to it whose domain's stop has not
                        // withdrawn it, in the order the sources were bound.
                        let mut expected = Vec::new();
                        for (id, binding) in &bindings {
                            if binding.live
                                && binding.key == key
                                && !withdrawn.contains_key(&binding.owner)
                            {
                                expected.push(*id);
                            }
                        }
                        assert_eq!(intakes(&actual), expected);
                        assert_eq!(
                            actual.bindings().len(),
                            expected.len(),
                            "a route never publishes an ended source"
                        );
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
