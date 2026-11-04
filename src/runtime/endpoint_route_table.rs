//! Published server endpoint definitions and source intake lifetimes.
//!
//! Layer: data plane.
//! - **Owns.** Whole-table endpoint publication and the exact lifetime of each bound intake.
//! - **Depends on.** Typed domain identities, prepared endpoint metadata, and publication primitives.
//! - **Must not know.** Decoding, request transport, graph planning, or source task scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use ahash::{HashMap, HashMapExt as _};
use nervix_connector_websockets::CompiledSignalingProtocol;
use nervix_primitives::{
    publication::{ArcSwap, ArcSwapOption, Guard},
    sync::{Arc, CancellationToken, StdArc},
};
use thiserror::Error;

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

    /// Resolves when this exact source lifetime ends, including through a retained route.
    pub(super) async fn ended(&self) {
        self.inner.ending.cancelled().await;
    }
}

/// One immutable resolved route, retained by a request or an entire WebSocket connection.
///
/// A host and path belongs to the one domain that publishes it: the registry gives every hostname
/// to one domain's vhost, so a route holds one definition and the bindings of its owner only.
pub(crate) struct EndpointIntakeRoute<T> {
    owner: DomainName,
    definition: RoutedEndpoint,
    bindings: Vec<EndpointBinding<T>>,
}

impl<T> Clone for EndpointIntakeRoute<T> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            definition: self.definition.clone(),
            bindings: self.bindings.clone(),
        }
    }
}

impl<T> EndpointIntakeRoute<T> {
    pub(crate) fn endpoint_type(&self) -> EndpointType {
        self.definition.endpoint_type
    }

    pub(crate) fn signaling_protocol(&self) -> Option<&Arc<CompiledSignalingProtocol>> {
        self.definition.signaling_protocol.as_ref()
    }

    pub(super) fn bindings(&self) -> &[EndpointBinding<T>] {
        &self.bindings
    }
}

/// A host and path a domain tried to publish while another domain publishes it.
///
/// The registry refuses a hostname two domains' vhosts claim before any revision carries it, so a
/// conflict here is a revision the registry did not judge, not an ordinary outcome. The publication
/// that found it changed nothing.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("host '{host}' path '{path}' is published by domain '{owner}'")]
pub(crate) struct EndpointRouteConflict {
    pub(crate) host: String,
    pub(crate) path: String,
    pub(crate) owner: DomainName,
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
    installation: RouteTableInstallation,
}

impl<T> Default for EndpointRouteTable<T> {
    fn default() -> Self {
        Self {
            hosts: HashMap::new(),
            installation: RouteTableInstallation::default(),
        }
    }
}

impl<T> Clone for EndpointRouteTable<T> {
    fn clone(&self) -> Self {
        Self {
            hosts: self.hosts.clone(),
            installation: self.installation,
        }
    }
}

impl<T> EndpointRouteTable<T> {
    fn route(&self, host: &str, path: &str) -> Option<&Arc<EndpointIntakeRoute<T>>> {
        self.hosts.get(host)?.get(path)
    }

    /// Removes every route `domain` owns and ends the intakes bound to them, including through
    /// route allocations a request or a WebSocket connection retains.
    fn remove_domain(&mut self, domain: &DomainName) {
        self.hosts.retain(|_, paths| {
            paths.retain(|_, route| {
                if route.owner != *domain {
                    return true;
                }
                for binding in &route.bindings {
                    binding.end();
                }
                false
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

    /// Replaces `domain`'s complete set of routes with `definitions`, ending the intakes bound to
    /// the routes it withdraws. A host and path another domain publishes refuses the whole
    /// replacement and leaves the table as it was, including `domain`'s current routes.
    pub(super) fn replace_domain(
        &self,
        domain: &DomainName,
        definitions: Vec<(HttpRouteKey, RoutedEndpoint)>,
    ) -> Result<(), EndpointRouteConflict> {
        let mut conflict = None;
        self.current.rcu(|current| {
            // The update may run again against a newer table, so each run judges afresh.
            conflict = None;
            let mut next = current.as_ref().clone();
            next.remove_domain(domain);
            for (key, definition) in &definitions {
                let paths = next.hosts.entry(key.host.clone()).or_default();
                if let Some(route) = paths.get(&key.path) {
                    conflict = Some(EndpointRouteConflict {
                        host: key.host.clone(),
                        path: key.path.clone(),
                        owner: route.owner.clone(),
                    });
                    return StdArc::clone(current);
                }
                paths.insert(
                    key.path.clone(),
                    Arc::new(EndpointIntakeRoute {
                        owner: domain.clone(),
                        definition: definition.clone(),
                        bindings: Vec::new(),
                    }),
                );
            }
            StdArc::new(next)
        });
        match conflict {
            Some(conflict) => Err(conflict),
            None => Ok(()),
        }
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
                ending: CancellationToken::new(),
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
                if route.owner == binding.identity().domain {
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
