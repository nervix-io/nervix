//! Published server endpoint definitions and source intake lifetimes.
//!
//! Layer: data plane.
//! - **Owns.** Whole-table endpoint publication and the exact lifetime of each bound intake.
//! - **Depends on.** Typed domain identities, prepared endpoint metadata, and publication primitives.
//! - **Must not know.** Decoding, request transport, graph planning, or source task scheduling.

use std::{collections::BTreeMap, sync::Arc as StdArc};

use ahash::{HashMap, HashMapExt as _};
use meticulous::OptionExt as _;
use nervix_connector_websockets::CompiledSignalingProtocol;
use nervix_primitives::publication::{ArcSwap, ArcSwapOption, Guard};
use triomphe::Arc;

use super::{
    DomainName, DomainNodeRef, EndpointType, HttpRouteKey, RoutedEndpoint,
    endpoint::normalize_http_host,
};

/// A source's identity and optional intake stay shared across every published route it binds.
pub(super) struct EndpointBinding<T> {
    inner: Arc<EndpointBindingLifetime<T>>,
}

struct EndpointBindingLifetime<T> {
    identity: DomainNodeRef,
    intake: ArcSwapOption<T>,
}

impl<T> Clone for EndpointBinding<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
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
        self.inner.intake.store(None);
    }
}

/// One immutable resolved route, retained by a request or an entire WebSocket connection.
pub(crate) struct EndpointIntakeRoute<T> {
    definitions: BTreeMap<DomainName, RoutedEndpoint>,
    selected: RoutedEndpoint,
    bindings: Vec<EndpointBinding<T>>,
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
        self.selected.endpoint_type
    }

    pub(crate) fn signaling_protocol(&self) -> Option<&Arc<CompiledSignalingProtocol>> {
        self.selected.signaling_protocol.as_ref()
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
            .find(|definition| definition.endpoint_type == EndpointType::Websockets)
            .or_else(|| self.definitions.values().next())
            .assured("a published endpoint route has at least one configured definition");
        self.selected = selected.clone();
    }
}

struct EndpointRouteTable<T> {
    hosts: HashMap<String, HashMap<String, Arc<EndpointIntakeRoute<T>>>>,
}

impl<T> Default for EndpointRouteTable<T> {
    fn default() -> Self {
        Self {
            hosts: HashMap::new(),
        }
    }
}

impl<T> Clone for EndpointRouteTable<T> {
    fn clone(&self) -> Self {
        Self {
            hosts: self.hosts.clone(),
        }
    }
}

impl<T> EndpointRouteTable<T> {
    fn route(&self, host: &str, path: &str) -> Option<&Arc<EndpointIntakeRoute<T>>> {
        self.hosts.get(host)?.get(path)
    }

    fn remove_domain(&mut self, domain: &DomainName) {
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
}

impl<T> Default for EndpointIntakeRoutes<T> {
    fn default() -> Self {
        Self {
            current: ArcSwap::from_pointee(EndpointRouteTable::default()),
        }
    }
}

impl<T> EndpointIntakeRoutes<T> {
    pub(super) fn resolve(&self, host: &str, path: &str) -> Option<Arc<EndpointIntakeRoute<T>>> {
        self.current
            .load()
            .route(&normalize_http_host(host), path)
            .cloned()
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
                let paths = next.hosts.entry(key.host.clone()).or_default();
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
                            bindings: Vec::new(),
                        }),
                    );
                }
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

    pub(super) fn bind(
        &self,
        identity: DomainNodeRef,
        intake: T,
        keys: &[HttpRouteKey],
    ) -> EndpointBinding<T> {
        let binding = EndpointBinding {
            inner: Arc::new(EndpointBindingLifetime {
                identity,
                intake: ArcSwapOption::from(Some(StdArc::new(intake))),
            }),
        };
        self.current.rcu(|current| {
            let mut next = current.as_ref().clone();
            for key in keys {
                let Some(paths) = next.hosts.get_mut(&key.host) else {
                    continue;
                };
                let Some(route) = paths.get_mut(&key.path) else {
                    continue;
                };
                if route.definitions.contains_key(&binding.identity().domain) {
                    Arc::make_mut(route).bindings.push(binding.clone());
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
