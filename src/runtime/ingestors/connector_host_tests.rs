//! What a connector reaches through the handle the runtime's ingestor host hands it.
//!
//! Layer: test harness.
//! - **Owns.** The checks that each service behind the runtime's ingestor handle reaches this
//!   node's own owners, that a connector's status stands beside its loop's, and that the instance
//!   ends for its connector with whoever drives it.
//! - **Depends on.** The production ingestor host and connector handle, and the endpoint ingestor
//!   fixture that installs a domain with one running ingestor.
//! - **Must not know.** Broker drivers or the ingestor loops that call the host.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_connector::{
    DomainOffsetInitialization, DomainOffsetPosition, DomainOffsetResult, DomainOffsetServices,
    DomainOffsetStart, DomainOffsets, HostServiceError,
};
use nervix_execution::MemoryClass;

use super::{
    extension_tests::{UnfoldingDomain, WAITING_INTAKE},
    *,
};
use crate::runtime::{
    events::RuntimeErrorEvent,
    ingestors::connector_host::IngestorEntityServices,
    task_status::{TaskFailure, TaskFailures},
};

/// The failures the fixture's ingestor unit publishes now: its loop's, then its connector's.
fn published(fixture: &UnfoldingDomain) -> (Option<String>, Option<String>) {
    let status =
        fixture
            .runtime
            .ingestor_status(&DomainNodeUnitRef::single(DomainNodeRef::node_in(
                fixture.domain.clone(),
                ModelKind::Ingestor,
                fixture.ingestor.clone(),
            )));
    let error = |failure: &Arc<TaskFailure<RuntimeReconnectStatus>>| failure.error.clone();
    match status.connection.failures().as_deref() {
        None => (None, None),
        Some(TaskFailures::Task(task)) => (Some(error(task)), None),
        Some(TaskFailures::Connector(connector)) => (None, Some(error(connector))),
        Some(TaskFailures::Both { task, connector }) => (Some(error(task)), Some(error(connector))),
    }
}

/// A connector that reports its own connection publishes beside the loop that drives it: each
/// withdraws only what it published, so a read that succeeds never hides a connection its
/// connector still reports lost, and a connector that recovers never clears its loop's failure.
#[nervix_primitives::test]
async fn a_connector_publishes_its_status_beside_its_loops_and_each_withdraws_only_its_own() {
    let mut fixture = UnfoldingDomain::start().await;
    let (_shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;
    let handle = host.connector_host();

    handle.record_transient_error("session lost".to_string(), Duration::from_secs(3));
    host.record_transient_error("read failed".to_string(), Duration::from_secs(1));
    assert_eq!(
        published(&fixture),
        (
            Some("read failed".to_string()),
            Some("session lost".to_string())
        )
    );

    host.clear_transient_error();
    assert_eq!(
        published(&fixture),
        (None, Some("session lost".to_string())),
        "the loop's success withdrew only the loop's failure"
    );

    host.record_transient_error("read failed".to_string(), Duration::from_secs(1));
    handle.clear_transient_error();
    assert_eq!(
        published(&fixture),
        (Some("read failed".to_string()), None),
        "the connector's recovery withdrew only the connector's failure"
    );
    host.clear_transient_error();
    assert_eq!(published(&fixture), (None, None));

    drop(host);
    fixture.finish().await;
}

/// The handle reaches this node's own owners: the executor every job of the node is admitted by,
/// the published outward TLS policy, and the node's event bus under the ingestor's name. This
/// fixture's node runs without a resolver, which the handle refuses by name.
#[nervix_primitives::test]
async fn a_connector_reaches_its_nodes_own_services_through_its_handle() {
    let fixture = UnfoldingDomain::start().await;
    let connector_host = fixture.connector_host(IngestorEntityServices::default());
    let handle = connector_host.handle();
    let mut events = fixture.runtime.events().subscribe();

    let before = fixture
        .runtime
        .executor()
        .memory(MemoryClass::Intake)
        .reserved_bytes;
    let charge = handle
        .executor()
        .try_reserve(MemoryClass::Intake, 64)
        .expect("the fixture's intake class has room for sixty-four bytes");
    assert_eq!(
        fixture
            .runtime
            .executor()
            .memory(MemoryClass::Intake)
            .reserved_bytes,
        before + 64,
        "a charge taken through the handle is a charge on the node's executor"
    );
    drop(charge);

    assert!(
        StdArc::ptr_eq(
            &handle.tls_policy().provider(),
            &fixture.runtime.outward_tls().provider()
        ),
        "the handle hands over the node's own policy publication"
    );

    let unresolved = handle
        .dns()
        .err()
        .assured("the fixture's node runs without a resolver");
    assert_eq!(
        *unresolved.current_context(),
        HostServiceError::DnsNotInstalled
    );

    handle.report_error("session replaced".to_string());
    let RuntimeEvent::Error(reported) = events
        .recv()
        .await
        .expect("the handle publishes the connector's error event");
    assert_eq!(
        reported,
        RuntimeErrorEvent {
            message: "ingestor error for ingestor 'event_source' in domain 'default': session \
                      replaced"
                .to_string(),
            error: None,
        }
    );

    let undeclared = handle
        .event_counter(nervix_models::ConnectorEvent::PublishedWithoutSubscribers)
        .err()
        .assured("the fixture's entity declares no event");
    assert_eq!(
        undeclared.current_context().event,
        nervix_models::ConnectorEvent::PublishedWithoutSubscribers
    );

    drop(connector_host);
    fixture.finish().await;
}

/// The payloads a connection drops for lack of read-ahead room count in the series of the
/// connector's own unit, which the connector resolves through its handle when it opens.
#[nervix_primitives::test]
async fn a_connector_records_read_ahead_drops_in_its_units_series_through_its_handle() {
    let fixture = UnfoldingDomain::start().await;
    let connector_host = fixture.connector_host(IngestorEntityServices::default());

    let overflow = connector_host.handle().read_ahead_overflow();
    overflow.record_dropped_payload();
    overflow.clone().record_dropped_payload();

    let exposed = fixture.runtime.metrics().prometheus_text();
    let dropped = exposed
        .lines()
        .find(|line| {
            line.starts_with("nervix_ingestor_read_ahead_dropped_payloads_total{")
                && line.contains(r#"ingestor="event_source""#)
        })
        .assured("the unit's read-ahead series is registered once its connector asks for it");
    assert!(
        dropped.ends_with(" 2"),
        "both drops count in the unit's series: {dropped}"
    );

    drop(connector_host);
    fixture.finish().await;
}

/// Domain-owned offsets that answer one scripted generation.
struct ScriptedOffsets;

#[async_trait]
impl DomainOffsetServices for ScriptedOffsets {
    fn generation(&self) -> Option<u64> {
        Some(7)
    }

    async fn initialization(
        &self,
        _partitions: &[i32],
    ) -> DomainOffsetResult<DomainOffsetInitialization> {
        Ok(DomainOffsetInitialization {
            generation: 7,
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

    async fn commit(&self, _position: DomainOffsetPosition) -> DomainOffsetResult<()> {
        Ok(())
    }
}

/// What one connector reads its compiled signaling protocol as.
#[derive(Debug, PartialEq, Eq)]
struct CompiledProtocol(&'static str);

/// What a unit's composer prepared for its entity reaches the connector through the handle, and a
/// unit whose composer prepared nothing is refused by name instead.
#[nervix_primitives::test]
async fn a_connector_takes_what_its_composer_prepared_for_its_entity_through_its_handle() {
    let fixture = UnfoldingDomain::start().await;

    let prepared = fixture.connector_host(IngestorEntityServices {
        domain_offsets: Some(DomainOffsets::new(ScriptedOffsets)),
        signaling_protocol: Some(Box::new(Arc::new(CompiledProtocol("signals")))),
    });
    let offsets = prepared
        .handle()
        .domain_offsets()
        .assured("the composer prepared this unit's domain offsets");
    assert_eq!(offsets.generation(), Some(7));
    let protocol = prepared
        .handle()
        .signaling_protocol::<Arc<CompiledProtocol>>()
        .assured("the composer compiled the value this connector reads")
        .assured("the ingestor names a signaling protocol");
    assert_eq!(*protocol, CompiledProtocol("signals"));
    let other = prepared
        .handle()
        .signaling_protocol::<Arc<String>>()
        .err()
        .assured("the compiled protocol is another connector's value");
    assert_eq!(
        *other.current_context(),
        HostServiceError::SignalingProtocolOfOtherConnector
    );

    let bare = fixture.connector_host(IngestorEntityServices::default());
    let unkept = bare
        .handle()
        .domain_offsets()
        .err()
        .assured("this unit keeps no offsets in its domain");
    assert_eq!(
        *unkept.current_context(),
        HostServiceError::DomainOffsetsNotKept
    );
    assert!(
        bare.handle()
            .signaling_protocol::<Arc<CompiledProtocol>>()
            .assured("an ingestor that names no protocol is not refused one")
            .is_none()
    );

    drop((prepared, bare));
    fixture.finish().await;
}

/// The instance ends for its connector when whoever drives it lets it go: the loop's host once
/// the loop ends or its task is aborted, or the composer when a start is abandoned after the
/// connector opened.
#[nervix_primitives::test]
async fn an_instance_ends_for_its_connector_with_the_host_that_drives_it() {
    let mut fixture = UnfoldingDomain::start().await;

    let abandoned = fixture.connector_host(IngestorEntityServices::default());
    let abandoned_end = abandoned.handle().cancellation();
    assert!(!abandoned_end.is_cancelled());
    drop(abandoned);
    assert!(
        abandoned_end.is_cancelled(),
        "a start abandoned after its connector opened ends the instance"
    );

    let (_shutdown_tx, shutdown) = watch::channel(false);
    let quiesce = fixture.quiesce();
    let host = fixture.host(quiesce, shutdown, WAITING_INTAKE).await;
    let instance_end = host.connector_host().cancellation();
    assert!(
        !instance_end.is_cancelled(),
        "the instance lives while its loop's host does"
    );
    drop(host);
    assert!(
        instance_end.is_cancelled(),
        "the loop letting its host go ends the instance"
    );

    fixture.finish().await;
}
