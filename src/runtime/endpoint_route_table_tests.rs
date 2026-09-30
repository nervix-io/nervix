//! Endpoint publication and intake lifetime regressions.
//!
//! Layer: test harness.
//! - **Owns.** The current route table's visible-route and retained-intake contracts.
//! - **Depends on.** The production endpoint table and typed domain identities.
//! - **Must not know.** HTTP transport, decoding, or graph scheduling.

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
    RoutedEndpoint {
        endpoint_type,
        signaling_protocol: None,
    }
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
        .assured("the route is installed");
    let lease = binding.intake();
    let intake = lease.as_deref().assured("the source is bound");
    for _ in 0..20 {
        let repeated = routes
            .resolve("host0.example.com:8080", "/path0")
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
    let first_binding = routes.bind(identity(&first), 1, &keys);
    let second_binding = routes.bind(identity(&second), 2, &keys);
    let retained = routes
        .resolve("host0.example.com", "/path0")
        .assured("both domains publish the route");
    assert_eq!(retained.endpoint_type(), EndpointType::Websockets);
    assert_eq!(intakes(&retained), vec![1, 2]);
    routes.replace_domain(&first, vec![(key(1, 1), endpoint(EndpointType::Http))]);
    assert!(first_binding.intake().is_none());
    assert_eq!(intakes(&retained), vec![2]);
    let current = routes
        .resolve("host0.example.com", "/path0")
        .assured("the second domain still publishes the route");
    assert_eq!(intakes(&current), vec![2]);
    routes.withdraw_domain(&second);
    assert!(second_binding.intake().is_none());
    assert!(routes.resolve("host0.example.com", "/path0").is_none());
    assert_eq!(
        routes
            .resolve("host1.example.com", "/path1")
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
    let preceding = routes.bind(identity(&owner), 1, &keys);
    let retained = routes
        .resolve("host0.example.com", "/path0")
        .assured("the preceding route is installed");
    let replacement = routes.bind(identity(&owner), 2, &keys);
    routes.unbind(&preceding, &keys);
    assert!(intakes(&retained).is_empty());
    assert_eq!(replacement.intake().as_deref(), Some(&2));
    assert_eq!(
        intakes(
            &routes
                .resolve("host0.example.com", "/path0")
                .assured("the replacement route is installed")
        ),
        vec![2]
    );
    routes.clear();
    assert!(replacement.intake().is_none());
    assert!(routes.resolve("host0.example.com", "/path0").is_none());
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
    routes.withdraw_domain(&owner);
    let binding = routes.bind(identity(&owner), 1, &keys);
    assert!(routes.resolve("host0.example.com", "/path0").is_none());
    routes.unbind(&binding, &keys);
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
                endpoint_type: EndpointType::Websockets,
                signaling_protocol: Some(protocol.clone()),
            },
        )],
    );
    routes.replace_domain(&second, vec![(key.clone(), endpoint(EndpointType::Http))]);
    let retained = routes
        .resolve(&key.host, &key.path)
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
fn bolero_endpoint_sequences_preserve_visible_routes_and_intake_lifetimes() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            let routes = EndpointIntakeRoutes::default();
            let mut configured: BTreeMap<DomainName, ahash::HashMap<HttpRouteKey, EndpointType>> =
                BTreeMap::new();
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
                match byte % 5 {
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
                        for binding in bindings.values_mut() {
                            if binding.owner == owner {
                                binding.live = false;
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
                    _ => {
                        routes.clear();
                        configured.clear();
                        for binding in bindings.values_mut() {
                            binding.live = false;
                        }
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
                            assert!(actual.is_none());
                            continue;
                        };
                        let actual = actual.assured("the reference contains a configured route");
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
