//! Published server endpoint definitions and ingestor intake lifetimes.
//!
//! Layer: data plane.
//! - **Owns.** Whole-table endpoint publication and the exact lifetime of each bound intake.
//! - **Depends on.** Typed domain identities, prepared endpoint metadata, and publication primitives.
//! - **Must not know.** Decoding, request transport, graph planning, or ingestor task scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use std::{cmp::Ordering as Comparison, collections::BTreeMap};

use ahash::{HashMap, HashMapExt as _};
use meticulous::OptionExt as _;
use nervix_connector_websockets::CompiledSignalingProtocol;
use nervix_primitives::{
    publication::{ArcSwap, ArcSwapOption, Guard},
    sync::{
        Arc, CancellationToken, StdArc,
        atomic::{AtomicU64, Ordering},
    },
};
use sorted_vec::SortedVec;

use super::{
    DomainName, DomainNodeRef, EndpointType, HttpRouteKey, RoutedEndpoint,
    endpoint::normalize_http_host,
};

/// A source's identity and optional intake stay shared across every published route it binds.
pub(super) struct EndpointBinding<T> {
    inner: Arc<EndpointBindingLifetime<T>>,
}

struct EndpointBindingLifetime<T> {
    #[cfg_attr(
        nervix_lint,
        nervix::identity(node, reason = "endpoint intake belongs to this domain node")
    )]
    identity: DomainNodeRef,
    /// When the route table created this lifetime, relative to every other it created. A route
    /// keeps its bindings in this order, so a binding published again after its domain's stop was
    /// called off returns to its place, and the first failure of a request stays the one of the
    /// earliest binding.
    sequence: u64,
    intake: ArcSwapOption<T>,
    ending: CancellationToken,
}

impl<T> Clone for EndpointBinding<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Bindings compare by their creation sequence, which the one route table that creates them makes
/// unique, so equal bindings are the same lifetime.
impl<T> PartialEq for EndpointBinding<T> {
    fn eq(&self, other: &Self) -> bool {
        self.inner.sequence == other.inner.sequence
    }
}

impl<T> Eq for EndpointBinding<T> {}

impl<T> PartialOrd for EndpointBinding<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Comparison> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for EndpointBinding<T> {
    fn cmp(&self, other: &Self) -> Comparison {
        self.inner.sequence.cmp(&other.inner.sequence)
    }
}

impl<T> EndpointBinding<T> {
    pub(super) fn identity(&self) -> &DomainNodeRef {
        &self.inner.identity
    }

    /// A present lease may finish after source ending; later loads observe absence.
    pub(super) fn intake(&self) -> Guard<Option<StdArc<T>>> {
        self.inner.intake.load()
    }

    fn end(&self) {
        self.inner.ending.cancel();
        self.inner.intake.store(None);
    }

    /// Whether this source lifetime ended, after which no route publishes it again.
    fn has_ended(&self) -> bool {
        self.inner.intake.load().is_none()
    }

    /// Resolves when this exact source lifetime ends, including through a retained route.
    pub(super) async fn ended(&self) {
        self.inner.ending.cancelled().await;
    }
}

/// One immutable resolved route, retained by a request or an entire WebSocket connection.
pub(crate) struct EndpointIntakeRoute<T> {
    definitions: BTreeMap<DomainName, RoutedEndpoint>,
    selected: RoutedEndpoint,
    /// In the order the route table created them.
    bindings: SortedVec<EndpointBinding<T>>,
}

impl<T> Clone for EndpointIntakeRoute<T> {
    fn clone(&self) -> Self {
        Self {
            definitions: self.definitions.clone(),
            selected: self.selected.clone(),
            bindings: self.bindings.clone(),
        }
    }
}

impl<T> EndpointIntakeRoute<T> {
    pub(crate) fn endpoint_type(&self) -> EndpointType {
        self.selected.kind.endpoint_type()
    }

    pub(crate) fn signaling_protocol(&self) -> Option<&Arc<CompiledSignalingProtocol>> {
        self.selected.kind.signaling_protocol()
    }

    pub(super) fn bindings(&self) -> &[EndpointBinding<T>] {
        &self.bindings
    }

    fn select_definition(&mut self) {
        // WebSocket routing takes precedence when domains publish both transport kinds. Among
        // definitions of that kind, domain order makes signaling selection deterministic.
        let selected = self
            .definitions
            .values()
            .find(|definition| definition.kind.endpoint_type() == EndpointType::Websockets)
            .or_else(|| self.definitions.values().next())
            .assured("a published endpoint route has at least one configured definition");
        self.selected = selected.clone();
    }
}

/// What a request finds for its host and path in the node's current route publication.
pub(crate) enum EndpointResolution<R> {
    /// The published route, which the request retains for its whole lifetime.
    Routed(R),
    /// The node installed its routes, and no domain publishes one for this host and path.
    Unrouted,
    /// The node has installed no revision since it started, so an absent route may still be one
    /// the cluster binds: the request is refused as temporarily unavailable, never as missing.
    Uninstalled,
}

#[cfg(test)]
impl<R> EndpointResolution<R> {
    /// The published route, or `None` for either kind of absence.
    pub(crate) fn into_route(self) -> Option<R> {
        match self {
            Self::Routed(route) => Some(route),
            Self::Unrouted | Self::Uninstalled => None,
        }
    }
}

/// Whether the table holds every route of an installed revision, so that a missing route is final.
#[derive(Clone, Copy, Default)]
enum RouteTableInstallation {
    /// No revision was installed since the node started or since the runtime cleared the table.
    #[default]
    Awaiting,
    /// A revision installed every domain's routes; later revisions keep the table current.
    Installed,
}

struct EndpointRouteTable<T> {
    hosts: HashMap<String, HashMap<String, Arc<EndpointIntakeRoute<T>>>>,
    /// What each stopping domain published before its stop withdrew it, kept until the stop ends,
    /// which ends it, or is called off, which publishes it again.
    withdrawn: BTreeMap<DomainName, WithdrawnRoutes<T>>,
    installation: RouteTableInstallation,
}

impl<T> Default for EndpointRouteTable<T> {
    fn default() -> Self {
        Self {
            hosts: HashMap::new(),
            withdrawn: BTreeMap::new(),
            installation: RouteTableInstallation::default(),
        }
    }
}

impl<T> Clone for EndpointRouteTable<T> {
    fn clone(&self) -> Self {
        Self {
            hosts: self.hosts.clone(),
            withdrawn: self.withdrawn.clone(),
            installation: self.installation,
        }
    }
}

/// The routes a stopping domain published before its stop withdrew them: each route's definition,
/// and each binding of the domain's ingestors with the route it was bound to. A withdrawn binding
/// keeps its intake lifetime, so a request it admitted before the stop finishes through it.
struct WithdrawnRoutes<T> {
    definitions: HashMap<HttpRouteKey, RoutedEndpoint>,
    bindings: Vec<WithdrawnBinding<T>>,
}

impl<T> Default for WithdrawnRoutes<T> {
    fn default() -> Self {
        Self {
            definitions: HashMap::new(),
            bindings: Vec::new(),
        }
    }
}

impl<T> Clone for WithdrawnRoutes<T> {
    fn clone(&self) -> Self {
        Self {
            definitions: self.definitions.clone(),
            bindings: self.bindings.clone(),
        }
    }
}

/// A binding of a withdrawn route, with the route it returns to.
struct WithdrawnBinding<T> {
    key: HttpRouteKey,
    binding: EndpointBinding<T>,
}

impl<T> Clone for WithdrawnBinding<T> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            binding: self.binding.clone(),
        }
    }
}

impl<T> EndpointRouteTable<T> {
    fn route(&self, host: &str, path: &str) -> Option<&Arc<EndpointIntakeRoute<T>>> {
        self.hosts.get(host)?.get(path)
    }

    /// Whether any route publishes a definition of `domain`. The scan visits every route once, as
    /// the cold writers that derive a replacement table do.
    fn publishes(&self, domain: &DomainName) -> bool {
        for paths in self.hosts.values() {
            for route in paths.values() {
                if route.definitions.contains_key(domain) {
                    return true;
                }
            }
        }
        false
    }

    /// Publishes `definition` of `domain` on the route of `key`, creating the route where no domain
    /// publishes it yet.
    fn insert_definition(
        &mut self,
        domain: &DomainName,
        key: &HttpRouteKey,
        definition: &RoutedEndpoint,
    ) {
        let paths = self.hosts.entry(key.host.clone()).or_default();
        if let Some(route) = paths.get_mut(&key.path) {
            let route = Arc::make_mut(route);
            route.definitions.insert(domain.clone(), definition.clone());
            route.select_definition();
        } else {
            paths.insert(
                key.path.clone(),
                Arc::new(EndpointIntakeRoute {
                    definitions: BTreeMap::from([(domain.clone(), definition.clone())]),
                    selected: definition.clone(),
                    bindings: SortedVec::new(),
                }),
            );
        }
    }

    /// Moves what `domain` publishes out of the routes into its withdrawn routes. Each of its
    /// bindings keeps its intake lifetime.
    fn withdraw(&mut self, domain: &DomainName) {
        let mut withdrawn = self.withdrawn.remove(domain).unwrap_or_default();
        self.hosts.retain(|host, paths| {
            paths.retain(|path, route| {
                let Some(definition) = route.definitions.get(domain) else {
                    return true;
                };
                let key = HttpRouteKey {
                    host: host.clone(),
                    path: path.clone(),
                };
                withdrawn
                    .definitions
                    .insert(key.clone(), definition.clone());
                let route = Arc::make_mut(route);
                route.definitions.remove(domain);
                route.bindings.retain(|binding| {
                    if &binding.identity().domain != domain {
                        return true;
                    }
                    withdrawn.bindings.push(WithdrawnBinding {
                        key: key.clone(),
                        binding: binding.clone(),
                    });
                    false
                });
                if route.definitions.is_empty() {
                    return false;
                }
                route.select_definition();
                true
            });
            !paths.is_empty()
        });
        self.withdrawn.insert(domain.clone(), withdrawn);
    }

    /// Publishes again what `domain` published before its stop withdrew it, with each binding whose
    /// intake lifetime goes on, in its place among the route's bindings.
    fn restore(&mut self, domain: &DomainName) {
        let Some(withdrawn) = self.withdrawn.remove(domain) else {
            return;
        };
        for (key, definition) in &withdrawn.definitions {
            self.insert_definition(domain, key, definition);
        }
        for withdrawn_binding in withdrawn.bindings {
            if withdrawn_binding.binding.has_ended() {
                continue;
            }
            let Some(paths) = self.hosts.get_mut(&withdrawn_binding.key.host) else {
                continue;
            };
            let Some(route) = paths.get_mut(&withdrawn_binding.key.path) else {
                continue;
            };
            Arc::make_mut(route)
                .bindings
                .insert(withdrawn_binding.binding);
        }
    }

    fn remove_domain(&mut self, domain: &DomainName) {
        // A domain whose stop withdrew its routes ends them with every lifetime bound to them.
        if let Some(withdrawn) = self.withdrawn.remove(domain) {
            for withdrawn_binding in &withdrawn.bindings {
                withdrawn_binding.binding.end();
            }
        }
        self.hosts.retain(|_, paths| {
            paths.retain(|_, route| {
                if !route.definitions.contains_key(domain) {
                    return true;
                }
                let route = Arc::make_mut(route);
                route.definitions.remove(domain);
                route.bindings.retain(|binding| {
                    if &binding.identity().domain == domain {
                        binding.end();
                        return false;
                    }
                    true
                });
                if route.definitions.is_empty() {
                    return false;
                }
                route.select_definition();
                true
            });
            !paths.is_empty()
        });
    }
}

/// Cold lifecycle writers derive a whole replacement; readers perform borrowed map lookups.
pub(super) struct EndpointIntakeRoutes<T> {
    current: ArcSwap<EndpointRouteTable<T>>,
    /// The creation sequence of the next binding. It only names an order, so a relaxed increment
    /// that is unique suffices.
    next_binding: AtomicU64,
}

impl<T> Default for EndpointIntakeRoutes<T> {
    fn default() -> Self {
        Self {
            current: ArcSwap::from_pointee(EndpointRouteTable::default()),
            next_binding: AtomicU64::new(0),
        }
    }
}

impl<T> EndpointIntakeRoutes<T> {
    /// Resolves the host and path against one load of the publication, so the route and whether
    /// its absence is final come from the same table.
    pub(super) fn resolve(
        &self,
        host: &str,
        path: &str,
    ) -> EndpointResolution<Arc<EndpointIntakeRoute<T>>> {
        let current = self.current.load();
        if let Some(route) = current.route(&normalize_http_host(host), path) {
            return EndpointResolution::Routed(route.clone());
        }
        match current.installation {
            RouteTableInstallation::Installed => EndpointResolution::Unrouted,
            RouteTableInstallation::Awaiting => EndpointResolution::Uninstalled,
        }
    }

    /// Records that a revision installed every domain's routes, after which a missing route is
    /// one that no domain publishes.
    pub(super) fn mark_installed(&self) {
        if let RouteTableInstallation::Installed = self.current.load().installation {
            return;
        }
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.installation = RouteTableInstallation::Installed;
            next
        });
    }

    pub(super) fn replace_domain(
        &self,
        domain: &DomainName,
        definitions: Vec<(HttpRouteKey, RoutedEndpoint)>,
    ) {
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.remove_domain(domain);
            for (key, definition) in &definitions {
                next.insert_definition(domain, key, definition);
            }
            next
        });
    }

    pub(super) fn withdraw_domain(&self, domain: &DomainName) {
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.remove_domain(domain);
            next
        });
    }

    /// Withdraws every route a stopping `domain` publishes, so a request that arrives later finds
    /// none. Each binding of the domain's ingestors keeps its intake lifetime: a request admitted
    /// before the stop finishes through the intake it borrowed, and the stop's teardown or its
    /// calling off decides what follows.
    pub(super) fn withdraw_stopping_domain(&self, domain: &DomainName) {
        if !self.current.load().publishes(domain) {
            return;
        }
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.withdraw(domain);
            next
        });
    }

    /// Publishes again every route a called-off stop of `domain` withdrew, with the bindings whose
    /// intake lifetime goes on.
    pub(super) fn restore_withdrawn_domain(&self, domain: &DomainName) {
        if !self.current.load().withdrawn.contains_key(domain) {
            return;
        }
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            next.restore(domain);
            next
        });
    }

    pub(super) fn bind(
        &self,
        identity: DomainNodeRef,
        intake: T,
        keys: &[HttpRouteKey],
    ) -> EndpointBinding<T> {
        let binding = EndpointBinding {
            inner: Arc::new(EndpointBindingLifetime {
                identity,
                sequence: self.next_binding.fetch_add(1, Ordering::Relaxed),
                intake: ArcSwapOption::from(Some(StdArc::new(intake))),
                ending: CancellationToken::new(),
            }),
        };
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            // A binding of a domain whose stop withdrew its routes waits with them, and is published
            // with them if the stop is called off.
            if let Some(withdrawn) = next.withdrawn.get_mut(&binding.identity().domain) {
                for key in keys {
                    if withdrawn.definitions.contains_key(key) {
                        withdrawn.bindings.push(WithdrawnBinding {
                            key: key.clone(),
                            binding: binding.clone(),
                        });
                    }
                }
                return next;
            }
            for key in keys {
                let Some(paths) = next.hosts.get_mut(&key.host) else {
                    continue;
                };
                let Some(route) = paths.get_mut(&key.path) else {
                    continue;
                };
                if route.definitions.contains_key(&binding.identity().domain) {
                    Arc::make_mut(route).bindings.insert(binding.clone());
                }
            }
            next
        });
        binding
    }

    pub(super) fn unbind(&self, binding: &EndpointBinding<T>, keys: &[HttpRouteKey]) {
        // End admission first, including through route allocations retained by connections.
        binding.end();
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            for key in keys {
                let Some(paths) = next.hosts.get_mut(&key.host) else {
                    continue;
                };
                let Some(route) = paths.get_mut(&key.path) else {
                    continue;
                };
                Arc::make_mut(route)
                    .bindings
                    .retain(|candidate| !Arc::ptr_eq(&candidate.inner, &binding.inner));
            }
            // A binding its domain's stop withdrew is never published again once it ended.
            if let Some(withdrawn) = next.withdrawn.get_mut(&binding.identity().domain) {
                withdrawn
                    .bindings
                    .retain(|candidate| !Arc::ptr_eq(&candidate.binding.inner, &binding.inner));
            }
            next
        });
    }

    pub(super) fn clear(&self) {
        self.current.rcu(|current| {
            for paths in current.hosts.values() {
                for route in paths.values() {
                    for binding in route.bindings() {
                        binding.end();
                    }
                }
            }
            for withdrawn in current.withdrawn.values() {
                for withdrawn_binding in &withdrawn.bindings {
                    withdrawn_binding.binding.end();
                }
            }
            EndpointRouteTable::default()
        });
    }
}

#[cfg(test)]
#[path = "endpoint_route_table_tests.rs"]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
#[path = "endpoint_route_table_shuttle_tests.rs"]
mod shuttle_tests;
