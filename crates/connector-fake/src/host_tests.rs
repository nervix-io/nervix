//! Every service of the host handles, reached by the fake connectors a host opened with them.
//!
//! Layer: test harness.
//! - **Owns.** The checks that each service behind an ingestor's and an emitter's host handle
//!   reaches the connector that was opened with the handle, that a service the host did not
//!   provide is refused with its typed reason, that a leased pooled client publishes its waits,
//!   and that the handles of a pooled client and of a partition inspector hand over their node's
//!   own services.
//! - **Depends on.** The fake connectors and hosts and the contract they implement.
//! - **Must not know.** Runtime internals or real transports.

use std::{
    future::{Future, pending, poll_fn, ready},
    pin::pin,
    task::Poll,
    time::Duration,
};

use async_trait::async_trait;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::*;
use nervix_dns::{DnsConfiguration, DnsResolver, DnsSettings, NameServers};
use nervix_execution::{Executor, MemoryClass};
use nervix_models::{
    ClientConfigEntry, ClientPoolBounds, ConnectorEvent, ConnectorKind, DeclaredEvents,
    MessageErrorClass,
};
use nervix_primitives::sync::{
    Arc, StdArc,
    atomic::{AtomicBool, Ordering},
    blocking::Mutex,
};

use crate::*;

const DECLARED: ConnectorEvent = ConnectorEvent::PublishedWithoutSubscribers;

/// A resolver over empty configuration files, which no check asks to resolve a name.
async fn resolver() -> (DnsResolver, tempfile::TempDir) {
    let files = tempfile::tempdir().assured("temporary resolver files can be created");
    let resolver_configuration = files.path().join("resolv.conf");
    let hosts_file = files.path().join("hosts");
    std::fs::write(&resolver_configuration, "options timeout:1 attempts:1\n")
        .assured("the resolver configuration can be written");
    std::fs::write(&hosts_file, "").assured("the hosts file can be written");
    let dns = DnsResolver::load(
        DnsConfiguration {
            resolver_configuration,
            hosts_file,
            name_servers: NameServers::Explicit(vec![
                "127.0.0.1:53"
                    .parse()
                    .assured("the loopback name server address is valid"),
            ]),
        },
        DnsSettings::default(),
    )
    .await
    .assured("the resolver loads its fixture configuration");
    (dns, files)
}

/// Whether `handed` is the node's own `executor`: a charge taken through one shows in the other's
/// budget.
fn is_the_nodes_executor(handed: &Executor, executor: &Executor) -> bool {
    let before = executor.memory(MemoryClass::Intake).reserved_bytes;
    let charge = handed
        .try_reserve(MemoryClass::Intake, 64)
        .assured("the fixture's intake class has room for sixty-four bytes");
    let charged = executor.memory(MemoryClass::Intake).reserved_bytes;
    drop(charge);
    charged == before + 64
}

/// Domain-owned offsets that answer one scripted generation and record what was committed.
struct ScriptedOffsets {
    generation: u64,
    committed: Arc<Mutex<Vec<DomainOffsetPosition>>>,
}

#[async_trait]
impl DomainOffsetServices for ScriptedOffsets {
    fn generation(&self) -> Option<u64> {
        Some(self.generation)
    }

    async fn initialization(
        &self,
        _partitions: &[i32],
    ) -> DomainOffsetResult<DomainOffsetInitialization> {
        Ok(DomainOffsetInitialization {
            generation: self.generation,
            start: DomainOffsetStart::Resume {
                positions: Vec::new(),
                missing_partition_timestamp: None,
            },
            assignment: None,
        })
    }

    async fn reset(
        &self,
        _generation: u64,
        _positions: Vec<DomainOffsetPosition>,
    ) -> DomainOffsetResult<()> {
        Ok(())
    }

    async fn commit(&self, position: DomainOffsetPosition) -> DomainOffsetResult<()> {
        self.committed.lock().push(position);
        Ok(())
    }
}

/// What one connector reads its compiled signaling protocol as.
#[derive(Debug, PartialEq, Eq)]
struct CompiledProtocol(&'static str);

async fn opened_ingestor(host: &FakeIngestorHost) -> FakeIngestor {
    let observed = Arc::new(Mutex::new(IngestorObservations::default()));
    let plan = FakeIngestorPlan::new(std::collections::BTreeMap::from([(
        0,
        Ok(FakeIngestor::empty(observed)),
    )]));
    FakeIngestor::open(&plan, &host.host(), 0)
        .await
        .assured("the configured instance opens")
}

fn refusal<T>(answer: Result<T, error_stack::Report<HostServiceError>>) -> HostServiceError {
    match answer {
        Ok(_) => panic!("the handle must refuse a service its host does not provide"),
        Err(error) => *error.current_context(),
    }
}

#[nervix_primitives::test]
async fn an_ingestor_keeps_the_handle_it_was_opened_with_and_reaches_every_service_behind_it() {
    let (dns, _files) = resolver().await;
    let tls_policy = OutwardTlsPolicy::default();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let executor = Executor::default();
    let fixture = FakeIngestorHost::declaring(DeclaredEvents::new(&[DECLARED]))
        .with_executor(executor.clone())
        .with_dns(dns)
        .with_tls_policy(tls_policy.clone())
        .with_domain_offsets(DomainOffsets::new(ScriptedOffsets {
            generation: 7,
            committed: committed.clone(),
        }))
        .with_signaling_protocol(Arc::new(CompiledProtocol("signals")));
    let connector = opened_ingestor(&fixture).await;
    let host = connector.host();

    host.record_transient_error("broker refused".to_string(), Duration::from_secs(3));
    assert_eq!(
        fixture.observations().transient_error.as_deref(),
        Some("broker refused")
    );
    assert_eq!(
        fixture.observations().transient_errors,
        [("broker refused".to_string(), Duration::from_secs(3))]
    );
    host.clear_transient_error();
    assert_eq!(fixture.observations().transient_error, None);
    assert_eq!(fixture.observations().transient_clears, 1);
    host.report_error("session replaced".to_string());
    assert_eq!(fixture.observations().reported_errors, ["session replaced"]);

    assert!(is_the_nodes_executor(&host.executor(), &executor));
    host.dns().assured("the host provides its node's resolver");
    assert!(
        StdArc::ptr_eq(&host.tls_policy().provider(), &tls_policy.provider()),
        "the handle hands over the node's own policy publication"
    );

    let instance_end = host.cancellation();
    assert!(!instance_end.is_cancelled());
    fixture.end_instance();
    assert!(instance_end.is_cancelled());

    let overflow = host.read_ahead_overflow();
    overflow.record_dropped_payload();
    overflow.clone().record_dropped_payload();
    assert_eq!(fixture.observations().dropped_payloads, 2);
    host.event_counter(DECLARED)
        .assured("the entity declares the event")
        .increment();
    assert_eq!(fixture.observations().events.get(&DECLARED), Some(&1));

    let offsets = host
        .domain_offsets()
        .assured("the host keeps this ingestor's domain offsets");
    assert_eq!(offsets.generation(), Some(7));
    let position = DomainOffsetPosition {
        topic: "orders".to_string(),
        partition: 2,
        offset: 11,
    };
    offsets
        .commit(position.clone())
        .await
        .assured("the scripted offsets accept the commit");
    assert_eq!(*committed.lock(), [position]);

    let protocol = host
        .signaling_protocol::<Arc<CompiledProtocol>>()
        .assured("the host compiled the value this connector reads")
        .assured("the ingestor names a signaling protocol");
    assert_eq!(*protocol, CompiledProtocol("signals"));
}

#[nervix_primitives::test]
async fn an_ingestor_handle_refuses_each_service_its_host_does_not_provide() {
    let fixture =
        FakeIngestorHost::default().with_signaling_protocol(Arc::new(CompiledProtocol("x")));
    let connector = opened_ingestor(&fixture).await;
    let host = connector.host();

    assert_eq!(refusal(host.dns()), HostServiceError::DnsNotInstalled);
    assert_eq!(
        refusal(host.domain_offsets()),
        HostServiceError::DomainOffsetsNotKept
    );
    assert_eq!(
        refusal(host.signaling_protocol::<Arc<String>>()),
        HostServiceError::SignalingProtocolOfOtherConnector
    );
    let undeclared = host
        .event_counter(DECLARED)
        .err()
        .assured("an entity that declares no event has no recorder to hand over");
    assert_eq!(undeclared.current_context().event, DECLARED);

    let without_protocol = opened_ingestor(&FakeIngestorHost::default()).await;
    assert!(
        without_protocol
            .host()
            .signaling_protocol::<Arc<CompiledProtocol>>()
            .assured("an ingestor that names no protocol is not refused one")
            .is_none()
    );
}

#[nervix_primitives::test]
async fn an_ingestor_publishes_its_failures_through_the_handle_it_is_driven_with() {
    let fixture = FakeIngestorHost::default();
    let handle = fixture.host();
    let mut connector = opened_ingestor(&fixture).await;
    connector.status_retry = Some(Duration::from_millis(1));
    connector.failed_reads_left = 1;
    connector
        .resume_results
        .push_back(Err(error_stack::Report::new(IngestorError::Resume {
            connector: "fake",
        })));

    assert!(connector.resume(&handle).await.is_err());
    assert_eq!(
        fixture.observations().transient_error.as_deref(),
        Some("failed to resume fake ingestor")
    );
    assert_eq!(
        connector
            .resume(&handle)
            .await
            .assured("the next scripted resume succeeds"),
        IngestorResume::Ready
    );
    assert_eq!(fixture.observations().transient_error, None);
    assert_eq!(fixture.observations().transient_clears, 1);

    let read = connector
        .next_batch(
            &handle,
            IngestorBatchRequest {
                max_messages: std::num::NonZeroUsize::MIN,
                batch_timeout: None,
            },
            &IngestorReadCancellation::new(),
        )
        .await;
    assert!(read.is_err());
    assert_eq!(
        fixture.observations().transient_error.as_deref(),
        Some("failed to read from fake ingestor")
    );
}

/// Acknowledgements that record how the node's general error policy settled them.
#[derive(Default)]
struct RecordedAcknowledgements {
    rejected: Arc<Mutex<Vec<String>>>,
}

impl EmitterRecordAcknowledgementServices for RecordedAcknowledgements {
    fn acknowledge(&self) {}

    fn keep_alive(&self) {}

    fn reject(&self, reason: String) {
        self.rejected.lock().push(reason);
    }

    fn is_empty(&self) -> bool {
        false
    }
}

#[nervix_primitives::test]
async fn an_emitter_keeps_the_handle_it_was_opened_with_and_reaches_every_service_behind_it() {
    let (dns, _files) = resolver().await;
    let tls_policy = OutwardTlsPolicy::default();
    let executor = Executor::default();
    let fixture = FakeEmitterHost::declaring(DeclaredEvents::new(&[DECLARED]))
        .with_executor(executor.clone())
        .with_dns(dns)
        .with_tls_policy(tls_policy.clone());
    let emitter = FakeEmitter::<Records>::open(
        FakeEmitter::<Records>::new([PublishAnswer::ConfirmAll]),
        fixture.host(),
    )
    .await
    .assured("the fixture opens");
    let host = emitter.host();

    host.event_counter(DECLARED)
        .assured("the entity declares the event")
        .increment();
    host.record_transient_error("broker refused".to_string(), Duration::from_secs(3));
    assert_eq!(
        fixture.observations().transient_error.as_deref(),
        Some("broker refused")
    );
    host.clear_transient_error();
    host.mark_ready();
    host.mark_unready();
    host.record_prepared_payload(EmitterPayloadMeasurement::sized(2, 64));
    host.record_payload_subdivisions(3);
    host.report_error("session replaced".to_string());
    host.handle_general_error(
        &EmitterRecordAcknowledgements::new(RecordedAcknowledgements::default()),
        MessageErrorClass::INTERNAL,
        "the destination refused the write".to_string(),
    );

    let observed = fixture.observations();
    assert_eq!(observed.events.get(&DECLARED), Some(&1));
    assert_eq!(observed.transient_error, None);
    assert_eq!(observed.readiness, [true, false]);
    assert_eq!(observed.payloads, [EmitterPayloadMeasurement::sized(2, 64)]);
    assert_eq!(observed.subdivisions, 3);
    assert_eq!(observed.reported_errors, ["session replaced"]);
    assert_eq!(
        observed.general_errors,
        [(
            MessageErrorClass::INTERNAL,
            "the destination refused the write".to_string()
        )]
    );

    assert_eq!(host.staging_directory(), std::env::temp_dir());
    assert!(is_the_nodes_executor(&host.executor(), &executor));
    host.dns().assured("the host provides its node's resolver");
    assert!(
        StdArc::ptr_eq(&host.tls_policy().provider(), &tls_policy.provider()),
        "the handle hands over the node's own policy publication"
    );
    let task_end = host.cancellation();
    assert!(!task_end.is_cancelled());
    fixture.end_task();
    assert!(task_end.is_cancelled());
}

/// What a host gives back when an emitter lets its lease go.
struct Interest(Arc<AtomicBool>);

impl Drop for Interest {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

async fn opened_pool(config: &[(&str, &str)]) -> FakePooledClient {
    let entries = entries(config);
    FakePooledClient::open(
        &entries,
        bounds(),
        &PooledClientHost::new(None, Executor::default()),
    )
    .await
    .assured("the scripted pool opens")
}

fn entries(config: &[(&str, &str)]) -> Vec<ClientConfigEntry> {
    config
        .iter()
        .map(|(key, value)| ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        })
        .collect()
}

fn bounds() -> ClientPoolBounds {
    ClientPoolBounds::new(0, std::num::NonZeroU32::new(2).assured("two is not zero"))
        .assured("zero is below two")
}

#[nervix_primitives::test]
async fn an_emitter_takes_the_pooled_client_its_host_leased_and_no_other_connectors() {
    let released = Arc::new(AtomicBool::new(false));
    let waits = FakePooledClientWaits::default();
    let lease = PooledClientLease::new(
        Arc::new(opened_pool(&[("addr", "db.internal")]).await),
        waits,
        Interest(released.clone()),
    );
    let fixture = FakeEmitterHost::default();
    let host = fixture.leasing(lease);

    let taken = host
        .pool::<FakePooledClient>()
        .assured("the host leased this connector's pool");
    assert_eq!(
        taken.client().opening.config,
        entries(&[("addr", "db.internal")])
    );
    assert_eq!(
        refusal(host.pool::<OtherFakePooledClient>()),
        HostServiceError::PooledClientOfOtherConnector {
            leased: ConnectorKind::Postgres,
            requested: ConnectorKind::MySql,
        }
    );
    assert_eq!(
        refusal(fixture.host().pool::<FakePooledClient>()),
        HostServiceError::PooledClientNotLeased
    );

    // The interest outlives the handle for as long as the emitter keeps the lease it took.
    drop(host);
    assert!(!released.load(Ordering::Relaxed));
    drop(taken);
    assert!(released.load(Ordering::Relaxed));
}

#[nervix_primitives::test]
async fn a_leased_checkout_publishes_its_wait_only_while_the_pool_has_not_answered() {
    let waits = FakePooledClientWaits::default();
    let lease = PooledClientLease::new(Arc::new(opened_pool(&[]).await), waits.clone(), ());

    for _ in 0..3 {
        assert_eq!(lease.checkout(ready(7)).await, 7);
    }
    assert_eq!(
        waits.observations(),
        PooledClientWaitObservations::default(),
        "a checkout the pool answers at once publishes nothing"
    );

    let (answer, answered) = nervix_primitives::sync::oneshot::channel::<u8>();
    {
        let mut checkout = pin!(lease.checkout(answered));
        poll_fn(|context| {
            assert!(checkout.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        let waiting = waits.observations();
        assert_eq!((waiting.begun, waiting.ended), (1, 0));
        assert!(waiting.waiting_since.is_some());
        answer.send(9).assured("the checkout keeps its receiver");
        assert_eq!(checkout.await.assured("the pool answers"), 9);
    }
    let answered = waits.observations();
    assert_eq!((answered.begun, answered.ended), (1, 1));
    assert_eq!(answered.waiting_since, None);

    {
        let mut abandoned = pin!(lease.checkout(pending::<()>()));
        poll_fn(|context| {
            assert!(abandoned.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(waits.observations().waiting_since.is_some());
    }
    let cancelled = waits.observations();
    assert_eq!(
        (cancelled.begun, cancelled.ended),
        (2, 2),
        "a checkout its emitter abandons ends its wait"
    );
    assert_eq!(cancelled.waiting_since, None);

    let held = lease.begin_wait();
    assert_eq!(waits.observations().begun, 3);
    drop(held);
    assert_eq!(waits.observations().ended, 3);
}

#[nervix_primitives::test]
async fn a_pool_opens_through_the_services_its_node_provides_and_names_one_it_lacks() {
    let executor = Executor::default();
    let without_resolver = PooledClientHost::new(None, executor.clone());
    assert!(is_the_nodes_executor(
        without_resolver.executor(),
        &executor
    ));

    let refused =
        FakePooledClient::open(&entries(&[(REFUSE_OPEN, "")]), bounds(), &without_resolver)
            .await
            .err()
            .assured("the configuration scripts a refusal");
    assert_eq!(*refused.current_context(), FakePooledClientError::Refused);

    let unresolved = OtherFakePooledClient::open(
        &entries(&[(RESOLVE_THROUGH_NODE, "")]),
        bounds(),
        &without_resolver,
    )
    .await
    .err()
    .assured("a pool that dials by name needs its node's resolver");
    assert_eq!(
        *unresolved.current_context(),
        FakePooledClientError::HostService
    );
    assert_eq!(
        unresolved
            .frames()
            .find_map(|frame| frame.downcast_ref::<HostServiceError>()),
        Some(&HostServiceError::DnsNotInstalled)
    );

    let (dns, _files) = resolver().await;
    let resolved = OtherFakePooledClient::open(
        &entries(&[(RESOLVE_THROUGH_NODE, "")]),
        bounds(),
        &PooledClientHost::new(Some(dns), Executor::default()),
    )
    .await
    .assured("the node provides its resolver");
    assert!(resolved.opening.resolved_through_node);
    assert_eq!(resolved.opening.bounds, bounds());
}

/// A partition inspector runs outside every ingestor instance, so it asks its own handle, which
/// hands over the node's own executor and the node's own outward TLS policy publication.
#[nervix_primitives::test]
async fn a_partition_inspector_reaches_its_nodes_executor_and_tls_policy_through_its_handle() {
    let executor = Executor::default();
    let tls_policy = OutwardTlsPolicy::default();

    let host = PartitionInspectorHost::new(executor.clone(), tls_policy.clone());

    assert!(is_the_nodes_executor(host.executor(), &executor));
    assert!(
        StdArc::ptr_eq(&host.tls_policy().provider(), &tls_policy.provider()),
        "the handle hands over the node's own policy publication"
    );
}
