//! One live pooled client per named client on this node, shared by every local user of that client.
//!
//! Layer: data plane.
//! Owns: the pooled-client service of the emitter host: the instance behind each named client on
//! this node, whatever connector opened it, the interest its local users hold in it, and the wait
//! a user publishes between asking for a connection and being handed one.
//! Depends on: the vocabulary that names a domain, a client and a connector, the declared pool
//! bounds, the resolved client configuration, and the connector contract's pooled client, through
//! which a connector crate says how its client opens.
//! Must not know: any connector crate or driver, which graph node asked for a connection, how a
//! batch is built, or how a schedule was decided.
//!
//! A pool belongs to one named client in one domain on one physical node. Every local emitter of
//! that client borrows from the same instance, which is what makes the declared bounds a property
//! of the transport rather than of whoever happened to open a connection first. The service is
//! keyed by connector kind and generic over the pooled client: the composition root names the pool
//! type of the emitter it opens, and nothing here names a transport.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "named client pools are registered, replaced and retired when their users attach"
    )
)]

use std::any::Any;

use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    ClientResourceMounts, PooledClient, PooledClientHost, PooledClientWaits, ResolvedClientConfig,
};
use nervix_models::{ClientPoolBounds, ConnectorKind};
use url::Url;

use super::*;

/// Why this node refused to open one client's pooled client before its connector saw it.
///
/// The connector names itself so a diagnostic says whose client was refused, and no variant
/// carries a credential or a complete connection address.
#[derive(Debug, thiserror::Error)]
pub(in crate::runtime) enum OpenClientError {
    /// A configuration entry or address parameter sizes the pool beside the client's `POOL SIZE`.
    #[error(
        "invalid {connector} client configuration: '{key}' sizes the connection pool, which is \
         declared by POOL SIZE MIN ... MAX ... on the client"
    )]
    SizesPool {
        connector: ConnectorKind,
        key: String,
    },
}

/// Reject connector configuration that tries to size the connection pool.
///
/// Capacity is declared once, in the client's `POOL SIZE` clause. A raw entry or address parameter
/// that also sizes the pool is rejected rather than ignored, so an operator never has two
/// disagreeing answers with the winner decided by which driver read which.
fn reject_pool_sizing(
    connector: ConnectorKind,
    config: &[nervix_models::ClientConfigEntry],
) -> Result<(), Report<OpenClientError>> {
    let rejected = |key: &str| {
        Report::new(OpenClientError::SizesPool {
            connector,
            key: key.to_string(),
        })
    };
    for entry in config {
        if nervix_models::ClientPooling::Pooled.reserves_config_key(&entry.key) {
            return Err(rejected(&entry.key));
        }
        // Only an address carries query parameters, and only its parameters can reach a driver's
        // own sizing; other values are opaque strings the driver never parses that way.
        if !entry.key.eq_ignore_ascii_case("addr") {
            continue;
        }
        let Ok(url) = Url::parse(&entry.value) else {
            continue;
        };
        for (key, _) in url.query_pairs() {
            if nervix_models::ClientPooling::Pooled.reserves_config_key(&key) {
                return Err(rejected(&key));
            }
        }
    }
    Ok(())
}

/// Why this node could not supply a shared client.
///
/// A client that could not open keeps its cause beneath it: this node's own refusal of its
/// configuration, or the typed failure of the connector that opens it.
#[derive(Debug, thiserror::Error)]
pub(in crate::runtime) enum SharedClientError {
    #[error("failed to open client '{client}'")]
    Open { client: String },
    #[error("client '{client}' is not a {expected} client")]
    WrongConnector {
        client: String,
        expected: ConnectorKind,
    },
}

/// One named client's instance on this node, together with everything it needs to stay usable.
pub(in crate::runtime) struct SharedClient {
    /// The pooled client its connector opened, behind the type only that connector's crate and
    /// the composition root name: the shared `Arc` of its [`PooledClient`].
    instance: Box<dyn Any + Send + Sync>,
    /// The rendered resource mount this instance reads its TLS material from.
    ///
    /// The instance outlives every individual user, so the mount is held here rather than by the
    /// user that happened to resolve it first, and is released when the instance closes.
    _mounts: Option<Arc<ClientResourceMounts>>,
}

impl SharedClient {
    /// The pooled client behind this client as connector `K`'s, or an error naming the mismatch.
    fn pooled<K: PooledClient>(
        &self,
        client: &ClientName,
    ) -> Result<Arc<K>, Report<SharedClientError>> {
        match self.instance.downcast_ref::<Arc<K>>() {
            Some(pooled) => Ok(pooled.clone()),
            None => Err(Report::new(SharedClientError::WrongConnector {
                client: client.as_str().to_string(),
                expected: K::CONNECTOR,
            })),
        }
    }
}

/// One slot of the registry: the instance and the number of local users holding it open.
pub(in crate::runtime) struct SharedClientSlot {
    /// Built once however many users race to be first, so a burst of emitters starting together
    /// opens one pool rather than one pool each.
    instance: StdArc<nervix_primitives::sync::OnceCell<StdArc<SharedClient>>>,
    users: usize,
}

/// One local user's interest in this node's shared instance of a client.
///
/// A lease is held for exactly as long as its emitter can use the client. The instance closes when
/// the last lease is dropped, so removing one user preserves the pool for the others and removing
/// the last one releases the connections and the resource mount together.
pub(in crate::runtime) struct SharedClientLease {
    runtime: Runtime,
    key: DomainNodeRef,
    /// Keeps the instance and its resource mount alive for this user, whatever becomes of the
    /// registry slot it was leased from.
    _client: StdArc<SharedClient>,
}

impl Drop for SharedClientLease {
    fn drop(&mut self) {
        self.runtime.release_shared_client(&self.key);
    }
}

/// One emitter's lease on its client's pooled client, as its connector's type.
pub(in crate::runtime) struct LeasedPooledClient<K> {
    /// The node's shared instance, which the emitter writes through.
    pub(in crate::runtime) pooled: Arc<K>,
    /// The emitter's interest in that instance, given back when it is dropped.
    pub(in crate::runtime) interest: SharedClientLease,
}

/// What a graph node is waiting for while it holds no connection yet.
///
/// `DESCRIBE` reads this, so an emitter blocked on a full pool reports the wait instead of looking
/// idle next to a connection it does not have.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct PoolWait {
    pub(in crate::runtime) client: ClientName,
    pub(in crate::runtime) since: Instant,
}

impl PoolWait {
    /// How this wait reads in `DESCRIBE`, naming the client and how long the node has waited.
    pub(in crate::runtime) fn describe(&self) -> String {
        let waited = Duration::from_secs(self.since.elapsed().as_secs());
        format!(
            "waiting for a connection from client '{}' for {}",
            self.client.as_str(),
            humantime::format_duration(waited)
        )
    }
}

/// Publication retained by one pooled emitter. Its connector serializes connection borrows.
pub(in crate::runtime) struct PoolWaitSlot {
    client: ClientName,
    active: ArcSwapOption<PoolWait>,
    interactions: EmitterInteractionMetrics,
}

impl PoolWaitSlot {
    fn new(client: ClientName, interactions: EmitterInteractionMetrics) -> Self {
        Self {
            client,
            active: ArcSwapOption::empty(),
            interactions,
        }
    }
}

/// One pooled emitter's wait publication, which its lease publishes its checkouts' waits through.
///
/// Registry interest follows the emitter lifetime, independently of individual connection borrows.
pub(in crate::runtime) struct PoolWaitRegistration {
    pub(in crate::runtime) slot: Arc<PoolWaitSlot>,
    runtime: Runtime,
    key: DomainNodeUnitRef,
}

impl PooledClientWaits for PoolWaitRegistration {
    /// Publishes the wait of a checkout the pool did not answer at once. A checkout that is ready
    /// never reaches this, so it performs no publication or registry operation.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "a pending checkout publishes its wait through the emitter's retained slot"
        )
    )]
    fn begin(&self, since: Instant) {
        self.slot.active.store(Some(StdArc::new(PoolWait {
            client: self.slot.client.clone(),
            since,
        })));
    }

    /// Records the wait and clears its publication: on success, failure or cancellation of the
    /// checkout that published it.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "an answered or abandoned checkout records and withdraws its wait through \
                      the emitter's retained slot"
        )
    )]
    fn end(&self, since: Instant) {
        self.slot.interactions.record_pool_wait(since.elapsed());
        self.slot.active.store(None);
    }
}

impl Drop for PoolWaitRegistration {
    fn drop(&mut self) {
        self.runtime
            .inner
            .pool_waits
            .remove_if(&self.key, |_, current| Arc::ptr_eq(current, &self.slot));
    }
}

impl Runtime {
    /// This node's shared instance of `client` as connector `K`'s pooled client, opening it on
    /// first local use with `bounds`.
    ///
    /// The instance is built from the first user's resolved configuration. Every pool-capable
    /// client renders the same configuration for all of its users, so the instance a later user
    /// joins is the one its own configuration describes.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "emitter installation retains one shared-client lease for its configuration"
        )
    )]
    pub(in crate::runtime) async fn lease_shared_client<K: PooledClient>(
        &self,
        domain: &DomainName,
        client: &EmitterClientSpec,
        bounds: ClientPoolBounds,
    ) -> Result<LeasedPooledClient<K>, Report<SharedClientError>> {
        let name = &client.name;
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Client, name.clone());
        let cell = {
            let mut slot = self
                .inner
                .shared_clients
                .entry(key.clone())
                .or_insert_with(|| SharedClientSlot {
                    instance: StdArc::new(nervix_primitives::sync::OnceCell::new()),
                    users: 0,
                });
            slot.users = slot
                .users
                .checked_add(1)
                .assured("a node holds far fewer clients open than usize can count");
            StdArc::clone(&slot.instance)
        };

        let opened = cell
            .get_or_try_init(|| self.open_shared_client::<K>(name, bounds, &client.config))
            .await;

        let shared = match opened {
            Ok(shared) => StdArc::clone(shared),
            Err(error) => {
                // The interest was taken before the open was attempted, so it has to be given back
                // here; a failed open leaves the slot empty and the next user retries it.
                self.release_shared_client(&key);
                return Err(error);
            }
        };
        // From here the interest is the lease's, so a client another connector opened gives it
        // back when the refused lease is dropped.
        let interest = SharedClientLease {
            runtime: self.clone(),
            key,
            _client: StdArc::clone(&shared),
        };
        let pooled = shared.pooled::<K>(name)?;
        Ok(LeasedPooledClient { pooled, interest })
    }

    /// Build the pooled client behind one pool-capable client, as its connector opens it.
    async fn open_shared_client<K: PooledClient>(
        &self,
        name: &ClientName,
        bounds: ClientPoolBounds,
        resolved: &ResolvedClientConfig,
    ) -> Result<StdArc<SharedClient>, Report<SharedClientError>> {
        let unopened = || SharedClientError::Open {
            client: name.as_str().to_string(),
        };
        let config = resolved.entries.as_slice();
        reject_pool_sizing(K::CONNECTOR, config).change_context_lazy(unopened)?;
        // A pool opens before any emitter of its client exists, so it takes the node's services
        // here rather than from an emitter's handle.
        let host = PooledClientHost::new(self.dns().cloned(), self.inner.executor.clone());
        // A connector states its own open failure, and the client names which one could not open.
        let pooled = K::open(config, bounds, &host)
            .await
            .change_context_lazy(unopened)?;
        Ok(StdArc::new(SharedClient {
            instance: Box::new(Arc::new(pooled)),
            _mounts: resolved.mounts.clone(),
        }))
    }

    /// Give back one user's interest, closing the instance once the last one leaves.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "emitter teardown releases its exact shared-client lease"
        )
    )]
    fn release_shared_client(&self, key: &DomainNodeRef) {
        let closed = {
            let Some(mut slot) = self.inner.shared_clients.get_mut(key) else {
                return;
            };
            slot.users = slot
                .users
                .checked_sub(1)
                .verified("a lease is created only after its slot took an interest");
            slot.users == 0
        };
        if closed {
            self.inner.shared_clients.remove(key);
        }
    }

    /// Register one pooled emitter's stable wait publication for observers.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "emitter installation retains one pool-wait publication slot"
        )
    )]
    pub(in crate::runtime) fn register_pool_wait(
        &self,
        waiter: DomainNodeUnitRef,
        client: ClientName,
        interactions: EmitterInteractionMetrics,
    ) -> PoolWaitRegistration {
        let slot = Arc::new(PoolWaitSlot::new(client, interactions));
        self.inner.pool_waits.insert(waiter.clone(), slot.clone());
        PoolWaitRegistration {
            slot,
            runtime: self.clone(),
            key: waiter,
        }
    }

    pub(in crate::runtime) fn pool_wait(&self, waiter: &DomainNodeUnitRef) -> Option<PoolWait> {
        let slot = self.inner.pool_waits.get(waiter)?;
        let wait = slot.active.load_full()?;
        Some((*wait).clone())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_connector::{HostServiceError, PooledClientLease};
    use nervix_connector_fake::{
        FakePooledClient, FakePooledClientError, OtherFakePooledClient, REFUSE_OPEN,
        RESOLVE_THROUGH_NODE,
    };
    use nervix_models::ClientConfigEntry;

    use super::*;

    fn assert_pool_outcomes(slot: &PoolWaitSlot, expected: u64) {
        let suffix = format!("total={expected}");
        assert!(slot.interactions.describe_lines().iter().any(|line| {
            line.starts_with("nervix_emitter_pool_waits_total{") && line.ends_with(&suffix)
        }));
    }

    fn pool_metrics(runtime: &Runtime) -> EmitterInteractionMetrics {
        runtime.inner.metrics.register_emitter_interactions(
            &domain("pool_wait"),
            &named::<EmitterName>("emitter"),
            Unit::SINGLE,
            None,
            nervix_models::EmitterEntityKind::Postgres(nervix_models::PostgresEmitterEntity::Table),
        )
    }

    fn bounds() -> ClientPoolBounds {
        ClientPoolBounds::new(0, nonzero_ext::nonzero!(1u32)).assured("zero is below one")
    }

    fn client(name: &str, entries: &[(&str, &str)]) -> EmitterClientSpec {
        EmitterClientSpec {
            name: named(name),
            config: ResolvedClientConfig {
                entries: entries
                    .iter()
                    .map(|(key, value)| ClientConfigEntry {
                        key: key.to_string(),
                        value: value.to_string(),
                    })
                    .collect(),
                mounts: None,
            },
        }
    }

    fn client_key(name: &str) -> DomainNodeRef {
        DomainNodeRef::node_in(
            domain("pool_wait"),
            ModelKind::Client,
            named::<ClientName>(name),
        )
    }

    /// A lease on a scripted pool whose waits the emitter `key` publishes through this node.
    fn waiting_lease(
        runtime: &Runtime,
        key: &DomainNodeUnitRef,
        pooled: Arc<FakePooledClient>,
    ) -> (PooledClientLease<FakePooledClient>, Arc<PoolWaitSlot>) {
        let registration =
            runtime.register_pool_wait(key.clone(), named("database"), pool_metrics(runtime));
        let slot = registration.slot.clone();
        (PooledClientLease::new(pooled, registration, ()), slot)
    }

    #[nervix_primitives::test]
    async fn retained_pool_wait_reports_only_pending_borrows_and_clears_on_cancel() {
        let runtime = Runtime::new();
        let key = DomainNodeUnitRef::single(DomainNodeRef::node_in(
            domain("pool_wait"),
            ModelKind::Emitter,
            named::<EmitterName>("emitter"),
        ));
        let opened = runtime
            .lease_shared_client::<FakePooledClient>(
                &domain("pool_wait"),
                &client("database", &[]),
                bounds(),
            )
            .await
            .assured("the scripted pool opens");
        let (lease, slot) = waiting_lease(&runtime, &key, opened.pooled.clone());
        for _ in 0..100 {
            assert_eq!(lease.checkout(std::future::ready(7)).await, 7);
        }
        assert!(slot.active.load().is_none());
        assert_pool_outcomes(&slot, 0);
        let (sender, receiver) = oneshot::channel::<u8>();
        let mut borrow = Box::pin(lease.checkout(receiver));
        std::future::poll_fn(|context| {
            assert!(borrow.as_mut().poll(context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let waiting = runtime
            .pool_wait(&key)
            .assured("the pending borrow is visible to DESCRIBE");
        assert_eq!(waiting.client, named::<ClientName>("database"));
        sender.send(9).assured("the borrow retains its receiver");
        assert_eq!(borrow.await.assured("the connection borrow succeeds"), 9);
        assert!(slot.active.load().is_none());
        assert_pool_outcomes(&slot, 1);
        let mut borrow = Box::pin(lease.checkout(std::future::pending::<()>()));
        std::future::poll_fn(|context| {
            assert!(borrow.as_mut().poll(context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert!(slot.active.load().is_some());
        drop(borrow);
        assert!(slot.active.load().is_none());
        assert_pool_outcomes(&slot, 2);
        let (sender, receiver) = oneshot::channel::<()>();
        let mut failed = Box::pin(lease.checkout(receiver));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(failed.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(sender);
        assert!(failed.await.is_err());
        assert_pool_outcomes(&slot, 3);
        assert!(
            lease
                .checkout(std::future::ready(Err::<(), ()>(())))
                .await
                .is_err()
        );
        assert_pool_outcomes(&slot, 3);
        drop(lease);
        assert!(runtime.pool_wait(&key).is_none());
        assert!(!runtime.inner.pool_waits.contains_key(&key));
    }

    #[test]
    fn pool_registration_teardown_preserves_a_replacement_emitter() {
        let runtime = Runtime::new();
        let key = DomainNodeUnitRef::single(DomainNodeRef::node_in(
            domain("pool_wait"),
            ModelKind::Emitter,
            named::<EmitterName>("emitter"),
        ));
        let first =
            runtime.register_pool_wait(key.clone(), named("database"), pool_metrics(&runtime));
        let replacement =
            runtime.register_pool_wait(key.clone(), named("database"), pool_metrics(&runtime));
        drop(first);
        let since = Instant::now();
        replacement.begin(since);
        assert!(runtime.pool_wait(&key).is_some());
        replacement.end(since);
        drop(replacement);
        assert!(!runtime.inner.pool_waits.contains_key(&key));
    }

    #[nervix_primitives::test]
    async fn local_emitters_of_one_client_share_one_instance_until_the_last_lets_it_go() {
        let runtime = Runtime::new();
        let spec = client("database", &[("addr", "db.internal")]);
        let first = runtime
            .lease_shared_client::<FakePooledClient>(&domain("pool_wait"), &spec, bounds())
            .await
            .assured("the first local emitter opens the pool");
        let second = runtime
            .lease_shared_client::<FakePooledClient>(&domain("pool_wait"), &spec, bounds())
            .await
            .assured("a later local emitter joins the pool");

        assert!(
            Arc::ptr_eq(&first.pooled, &second.pooled),
            "both emitters borrow from the one instance of their client"
        );
        assert_eq!(first.pooled.opening.config, spec.config.entries);
        assert_eq!(first.pooled.opening.bounds, bounds());
        let key = client_key("database");
        drop(first);
        assert!(
            runtime.inner.shared_clients.contains_key(&key),
            "removing one emitter preserves the pool for the other"
        );
        drop(second);
        assert!(
            !runtime.inner.shared_clients.contains_key(&key),
            "the last lease closes the instance"
        );
    }

    #[nervix_primitives::test]
    async fn a_client_another_connector_opened_is_refused_by_name_and_keeps_its_users() {
        let runtime = Runtime::new();
        let spec = client("database", &[]);
        let opened = runtime
            .lease_shared_client::<FakePooledClient>(&domain("pool_wait"), &spec, bounds())
            .await
            .assured("the scripted pool opens");

        let Err(refused) = runtime
            .lease_shared_client::<OtherFakePooledClient>(&domain("pool_wait"), &spec, bounds())
            .await
        else {
            panic!("a client is the pooled client of the one connector that opened it");
        };

        assert!(matches!(
            refused.current_context(),
            SharedClientError::WrongConnector { client, expected }
                if client == "database" && *expected == ConnectorKind::MySql
        ));
        // The refused lease gave its interest back, and the user that holds the client keeps it.
        let key = client_key("database");
        assert!(runtime.inner.shared_clients.contains_key(&key));
        drop(opened);
        assert!(!runtime.inner.shared_clients.contains_key(&key));
    }

    #[nervix_primitives::test]
    async fn a_pool_that_fails_to_open_keeps_its_typed_cause_beneath_its_client_and_is_retried() {
        let runtime = Runtime::new();
        let refusing = client("cache", &[(REFUSE_OPEN, "")]);
        let Err(error) = runtime
            .lease_shared_client::<FakePooledClient>(&domain("pool_wait"), &refusing, bounds())
            .await
        else {
            panic!("the configuration scripts a refusal");
        };

        assert!(matches!(
            error.current_context(),
            SharedClientError::Open { client } if client == "cache"
        ));
        assert_eq!(
            error
                .frames()
                .find_map(|frame| frame.downcast_ref::<FakePooledClientError>()),
            Some(&FakePooledClientError::Refused),
            "the connector's own failure stays beneath the client that could not open"
        );
        // The failed open gave its interest back and left nothing behind, so the next user opens
        // the client again from its own configuration.
        assert!(
            !runtime
                .inner
                .shared_clients
                .contains_key(&client_key("cache"))
        );
        runtime
            .lease_shared_client::<FakePooledClient>(
                &domain("pool_wait"),
                &client("cache", &[]),
                bounds(),
            )
            .await
            .assured("the next user retries the open");
    }

    #[nervix_primitives::test]
    async fn a_pool_that_dials_by_name_reports_a_missing_node_resolver_beneath_its_client() {
        let error = Runtime::default()
            .lease_shared_client::<FakePooledClient>(
                &domain("pool_wait"),
                &client("cache", &[(RESOLVE_THROUGH_NODE, "")]),
                bounds(),
            )
            .await
            .err()
            .assured("a pool that resolves through its node requires the node resolver");

        assert!(matches!(
            error.current_context(),
            SharedClientError::Open { client } if client == "cache"
        ));
        assert_eq!(
            error
                .frames()
                .find_map(|frame| frame.downcast_ref::<HostServiceError>()),
            Some(&HostServiceError::DnsNotInstalled)
        );
    }

    #[nervix_primitives::test]
    async fn configuration_that_sizes_the_pool_is_refused_before_its_connector_opens() {
        let runtime = Runtime::new();
        for (entries, sizing_key) in [
            (vec![("pool_max", "8")], "pool_max"),
            (
                vec![("addr", "db://db.internal/app?pool_max=8")],
                "pool_max",
            ),
        ] {
            let Err(error) = runtime
                .lease_shared_client::<FakePooledClient>(
                    &domain("pool_wait"),
                    &client("database", &entries),
                    bounds(),
                )
                .await
            else {
                panic!("capacity is declared once, by POOL SIZE on the client");
            };

            assert!(matches!(
                error.current_context(),
                SharedClientError::Open { client } if client == "database"
            ));
            let cause = error
                .frames()
                .find_map(|frame| frame.downcast_ref::<OpenClientError>())
                .assured("the node's own refusal stays beneath the client");
            assert!(matches!(
                cause,
                OpenClientError::SizesPool { connector, key }
                    if *connector == ConnectorKind::Postgres && key == sizing_key
            ));
            assert_eq!(
                cause.to_string(),
                format!(
                    "invalid POSTGRES client configuration: '{sizing_key}' sizes the connection \
                     pool, which is declared by POOL SIZE MIN ... MAX ... on the client"
                )
            );
            assert!(
                error.downcast_ref::<FakePooledClientError>().is_none(),
                "the connector never saw the refused configuration"
            );
        }
    }
}
