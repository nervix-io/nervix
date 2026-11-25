//! Materialized inspection identity at the subscription and state-application boundaries.
//!
//! Layer: test harness.
//! - **Owns.** A deterministic delivery schedule with the materialized consumer held unpolled.
//! - **Depends on.** Production relay fan-out, materialized application and owner-side pages.
//! - **Must not know.** Sessions, cursor encoding or publication backend internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use ahash::HashMap;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    AckMode, ModelKind, RelayName, ResolvedBranching, SchemaFingerprint,
    StateInspectionBranchSelection, Timestamp, Unit,
};
use nervix_primitives::{
    sync::{StdArc, watch},
    time::timeout,
};

use super::{
    MaterializedPageBounds, MaterializedPageQuery, MaterializedPageReply, ResidentEncodingPlan,
    StatePageReadError,
};
use crate::{
    runtime::{
        BranchKey, BranchPresence, DomainRoutingSnapshot, PageReply, RelayBoundaryFanout,
        RelayBoundaryServices, RelayBranchPresences, RelayPartitioning, RelayRecordBatch,
        RelayRetention, RelayRuntimeFanIn, RelayStateTaskSpec, Runtime,
        RuntimeMaterializedRelaySpec, RuntimeState, RuntimeStatePlacement, VmSchemaSensitivity,
        domain, install_unpaced_test_domain, named, publish_state_identity, test_schema,
    },
    runtime_ack::AckSet,
    runtime_schema::{RuntimeValue, test_runtime_row},
};

#[nervix_primitives::test]
async fn subscription_delivery_can_precede_the_materialized_inspection_generation() {
    timeout(std::time::Duration::from_secs(30), async {
        let runtime = Runtime::default();
        let domain = domain("inspection_publication");
        let relay = named::<RelayName>("events");
        install_unpaced_test_domain(&runtime, &domain);
        publish_state_identity(&runtime, &domain, ModelKind::Relay, relay.clone());
        let schema = test_schema(&[("seq", nervix_models::ParseAsType::I64)]);
        let placement = RuntimeStatePlacement {
            domain: domain.clone(),
            state: RuntimeState::MaterializedRelay {
                schema: SchemaFingerprint::from_digest([7; 32]),
            },
            kind: ModelKind::Relay,
            identifier: relay.clone().into(),
            unit: Unit::SINGLE,
            branch_key: None,
        };
        let mut assignment = runtime.replicated_materialized_stream_state(
            placement,
            schema.arrow_schema(),
            None,
            Vec::new(),
            None,
        );
        let mut state = assignment
            .originator
            .take()
            .assured("local assignment originates");
        let record = |seq| {
            test_runtime_row([("seq".to_string(), RuntimeValue::I64(seq))])
                .with_ingested_at_watermarks(Timestamp::from_unix_nanos(seq))
        };
        runtime
            .apply_materialized_stream_records(&mut state, &None, [record(4)])
            .await
            .assured("the initial row is published");
        let services = RelayBoundaryServices::new(
            RelayBoundaryFanout::direct_with_capacity(std::num::NonZeroUsize::MIN),
            1,
            0,
            Vec::new(),
            None,
            RelayBranchPresences::single(nervix_primitives::sync::Arc::new(BranchPresence::new())),
            nervix_primitives::sync::Arc::new(crate::runtime_ack::AckRootTracker::default()),
        );
        let specs = [(
            relay.clone(),
            RuntimeMaterializedRelaySpec::new(
                schema.arrow_schema(),
                VmSchemaSensitivity::default(),
                ResolvedBranching::unbranched(),
            ),
        )]
        .into_iter()
        .collect();
        let routing = DomainRoutingSnapshot {
            materialized_stream_reads: runtime.materialized_relay_publications(
                &domain,
                &specs,
                &HashMap::default(),
            ),
            materialized_stream_specs: specs,
            ..DomainRoutingSnapshot::default()
        };
        runtime
            .inner
            .domain_routings
            .get(&domain)
            .assured("the domain has routing")
            .store(StdArc::new(routing));
        let bounds = || MaterializedPageBounds {
            rows: 1,
            bytes: 1024,
        };
        let before = runtime
            .capture_materialized_state_page(
                &domain,
                &relay,
                MaterializedPageQuery {
                    unit: Unit::SINGLE,
                    selection: StateInspectionBranchSelection::All,
                    after: None,
                    bounds: bounds(),
                    catalog_requested: false,
                    filter: None,
                    reply: crate::runtime::MaterializedPageReply::Local,
                },
            )
            .await
            .assured("the owner supplies the initial page")
            .page;
        let mut subscription = services.subscription_receiver();
        let materializer = services
            .fanout
            .runtime_consumer_receiver_for_mode(AckMode::Attached);
        let batch = RelayRecordBatch::single(schema, None, record(5), AckSet::empty())
            .assured("one valid row forms a relay batch");
        services
            .fanout_owner_batch(
                &domain,
                &relay,
                Unit::SINGLE,
                &batch,
                &runtime.inner.fault_injection,
                &watch::channel(false).1,
            )
            .await
            .assured("the owner fans out to both independent consumers");
        let delivered = subscription
            .recv()
            .await
            .assured("the subscription receives seq=5");
        assert_eq!(
            delivered.try_into_messages().assured("valid row")[0]
                .record
                .value_at(0)
                .assured("seq column"),
            Some(RuntimeValue::I64(5))
        );
        let after_subscription = runtime
            .capture_materialized_state_page(
                &domain,
                &relay,
                MaterializedPageQuery {
                    unit: Unit::SINGLE,
                    selection: StateInspectionBranchSelection::All,
                    after: None,
                    bounds: bounds(),
                    catalog_requested: false,
                    filter: None,
                    reply: crate::runtime::MaterializedPageReply::Local,
                },
            )
            .await
            .assured("the owner supplies a page while its input is queued")
            .page;
        assert_eq!(
            before.fingerprint, after_subscription.fingerprint,
            "subscription receipt does not publish the materialized consumer's queued row"
        );
        assert_eq!(
            before.generation.revision(),
            after_subscription.generation.revision()
        );
        assert_eq!(
            after_subscription.generation.records()[0]
                .row
                .value_at(0)
                .assured("seq column"),
            Some(RuntimeValue::I64(4))
        );
        let task = runtime.spawn_relay_state_task(
            &domain,
            RelayStateTaskSpec {
                relay: relay.clone(),
                unit: Unit::SINGLE,
                partitioning: RelayPartitioning::Whole,
                state,
                retention: RelayRetention::default(),
                receiver: RelayRuntimeFanIn::new(materializer),
                transition: None,
                parked_capacity: 1,
            },
        );
        task.stop(crate::runtime::TaskStopWait::Teardown {
            grace: std::time::Duration::from_secs(30),
        })
        .await
        .assured("the actual materialized task drains and publishes its queued update");
        let after_application = runtime
            .capture_materialized_state_page(
                &domain,
                &relay,
                MaterializedPageQuery {
                    unit: Unit::SINGLE,
                    selection: StateInspectionBranchSelection::All,
                    after: None,
                    bounds: bounds(),
                    catalog_requested: false,
                    filter: None,
                    reply: crate::runtime::MaterializedPageReply::Local,
                },
            )
            .await
            .assured("the owner supplies the updated page")
            .page;
        assert_ne!(before.fingerprint, after_application.fingerprint);
        assert!(after_application.generation.revision() > before.generation.revision());
        assert_eq!(
            before.generation.branch_generation(),
            after_application.generation.branch_generation()
        );
        assert_eq!(
            after_application.generation.records()[0]
                .row
                .value_at(0)
                .assured("seq column"),
            Some(RuntimeValue::I64(5))
        );
        assert_eq!(before.counts, after_application.counts);
    })
    .await
    .assured("both delivery boundaries complete within the owner-test budget");
}

/// A runtime whose one materialized relay holds a row in each of several branches, with the state
/// that owns them.
struct TenantRelay {
    runtime: Runtime,
    domain: nervix_models::DomainName,
    relay: RelayName,
    _assignment: crate::runtime::MaterializedRelayStateAssignment,
    _state: crate::runtime::MaterializedRelayStateOriginator,
}

/// A runtime whose one materialized relay `events` holds `tenants` branches, one row each of
/// `payload` bytes of text, routed for inspection on this node.
async fn tenant_relay(tenants: i64, payload: usize) -> TenantRelay {
    let runtime = Runtime::default();
    let domain = domain("inspection_reply");
    let relay = named::<RelayName>("events");
    install_unpaced_test_domain(&runtime, &domain);
    publish_state_identity(&runtime, &domain, ModelKind::Relay, relay.clone());
    let schema = test_schema(&[("payload", nervix_models::ParseAsType::String)]);
    let placement = RuntimeStatePlacement {
        domain: domain.clone(),
        state: RuntimeState::MaterializedRelay {
            schema: SchemaFingerprint::from_digest([7; 32]),
        },
        kind: ModelKind::Relay,
        identifier: relay.clone().into(),
        unit: Unit::SINGLE,
        branch_key: None,
    };
    let mut assignment = runtime.replicated_materialized_stream_state(
        placement,
        schema.arrow_schema(),
        None,
        Vec::new(),
        None,
    );
    let mut state = assignment
        .originator
        .take()
        .assured("a local assignment originates its state");
    for tenant in 0..tenants {
        let key = BranchKey::from_fields([(
            nervix_models::FieldName::parse("tenant").assured("a valid field name"),
            RuntimeValue::I64(tenant),
        )])
        .assured("one typed key field");
        let row = test_runtime_row([(
            "payload".to_string(),
            RuntimeValue::String("p".repeat(payload)),
        )])
        .with_ingested_at_watermarks(Timestamp::from_unix_nanos(tenant + 1));
        runtime
            .apply_materialized_stream_records(&mut state, &Some(key), [row])
            .await
            .assured("the owner applies every branch's row");
    }
    let specs = [(
        relay.clone(),
        RuntimeMaterializedRelaySpec::new(
            schema.arrow_schema(),
            VmSchemaSensitivity::default(),
            ResolvedBranching::unbranched(),
        ),
    )]
    .into_iter()
    .collect();
    let routing = DomainRoutingSnapshot {
        materialized_stream_reads: runtime.materialized_relay_publications(
            &domain,
            &specs,
            &HashMap::default(),
        ),
        materialized_stream_specs: specs,
        ..DomainRoutingSnapshot::default()
    };
    runtime
        .inner
        .domain_routings
        .get(&domain)
        .assured("the fixture installed the domain's routing")
        .store(StdArc::new(routing));
    TenantRelay {
        runtime,
        domain,
        relay,
        _assignment: assignment,
        _state: state,
    }
}

/// A first page of every row at the default bounds, reaching its reader as `reply`.
fn page_query(
    after: Option<(Option<nervix_models::BranchKeyFingerprint>, u64)>,
    reply: MaterializedPageReply,
) -> MaterializedPageQuery {
    MaterializedPageQuery {
        unit: Unit::SINGLE,
        selection: StateInspectionBranchSelection::All,
        after,
        bounds: MaterializedPageBounds {
            rows: nervix_models::StateInspectionSettings::default()
                .max_rows
                .value()
                .get(),
            bytes: 64 * 1024 * 1024,
        },
        catalog_requested: false,
        filter: None,
        reply,
    }
}

/// The reply of a page another node reads, under `runtime`'s bounds.
fn remote(runtime: &Runtime) -> MaterializedPageReply {
    MaterializedPageReply::Remote {
        descriptor: PageReply::descriptor_share(runtime.inspection_executor()),
    }
}

/// Holds every byte of `runtime`'s free bulk memory but `room`.
fn hold_all_but(runtime: &Runtime, room: u64) -> nervix_execution::Reservation {
    let executor = runtime.inspection_executor();
    let memory = executor.memory(nervix_execution::MemoryClass::Bulk);
    let free = memory.capacity_bytes - memory.reserved_bytes;
    executor
        .try_reserve(nervix_execution::MemoryClass::Bulk, free - room)
        .assured("the free pool admits the hold")
}

/// A capture takes its memory one step ahead: without room it waits instead of refusing, and it
/// answers once the room frees.
#[nervix_primitives::test]
async fn a_materialized_capture_waits_for_the_memory_its_census_names() {
    let TenantRelay {
        runtime,
        domain,
        relay,
        ..
    } = &tenant_relay(4, 1024).await;
    let held = hold_all_but(runtime, 0);
    let mut capturing = std::pin::pin!(runtime.capture_materialized_state_page(
        domain,
        relay,
        page_query(None, MaterializedPageReply::Local),
    ));
    let early = timeout(std::time::Duration::from_millis(200), capturing.as_mut()).await;
    assert!(
        early.is_err(),
        "a capture without room waits for it instead of answering"
    );
    drop(held);
    let captured = timeout(std::time::Duration::from_secs(30), capturing)
        .await
        .assured("the capture answers once its memory frees")
        .assured("the capture succeeds");
    assert_eq!(captured.page.generation.records().len(), 4);
    assert!(
        captured.page.reply.is_none(),
        "a page this node presents sets nothing aside"
    );
}

/// What setting aside the container of `rows` of the relay's rows, in page order, takes.
async fn container_need(
    runtime: &Runtime,
    domain: &nervix_models::DomainName,
    relay: &RelayName,
    rows: usize,
) -> u64 {
    let page = runtime
        .capture_materialized_state_page(
            domain,
            relay,
            page_query(None, MaterializedPageReply::Local),
        )
        .await
        .assured("an idle owner answers a page")
        .page;
    let schema = page.generation.records()[0].row.arrow_schema();
    let mut plan = ResidentEncodingPlan::new(runtime.inspection_executor(), &schema);
    for record in &page.generation.records()[..rows] {
        plan = plan
            .with(record)
            .assured("every fixture row fits a section");
    }
    plan.need().assured("a fixture plan's need fits u64")
}

/// A page another node reads sets aside what encoding each selected row into its container adds,
/// and ends, with its continuation and complete counts, where the bulk pool cannot hold the next
/// row's share. The next page goes on from there.
#[nervix_primitives::test]
async fn a_remote_materialized_page_ends_where_bulk_memory_cannot_hold_its_next_row() {
    let TenantRelay {
        runtime,
        domain,
        relay,
        ..
    } = &tenant_relay(4, 1024 * 1024).await;
    let two = container_need(runtime, domain, relay, 2).await;
    let three = container_need(runtime, domain, relay, 3).await;
    assert!(three > two, "a third row adds to the container's need");
    // Room for the capture's own admission, small beside a 1 MiB row, and two rows' container.
    let held = hold_all_but(runtime, two + (three - two) / 2);
    let captured = runtime
        .capture_materialized_state_page(domain, relay, page_query(None, remote(runtime)))
        .await
        .assured("the owner answers what fits");
    let page = captured.page;
    assert_eq!(
        page.generation.records().len(),
        2,
        "the page ends where the third row no longer fits"
    );
    assert!(page.more, "the page continues");
    assert_eq!(page.offset, 2);
    assert_eq!(page.counts.rows, 4, "the counts stay complete");
    let reply = page
        .reply
        .as_ref()
        .assured("a page another node reads sets its reply aside");
    assert_eq!(
        reply.container.bytes(),
        two,
        "the container's share is what its two rows take"
    );
    let after = Some((page.last_branch, page.offset));
    drop(page);
    drop(held);
    let rest = runtime
        .capture_materialized_state_page(domain, relay, page_query(after, remote(runtime)))
        .await
        .assured("the owner answers the rest")
        .page;
    assert_eq!(rest.generation.records().len(), 2);
    assert!(!rest.more, "the second page returns the rest");
}

/// A page another node reads that cannot set aside even its first row's share is not ready.
#[nervix_primitives::test]
async fn a_remote_materialized_page_without_room_for_its_first_row_is_not_ready() {
    let TenantRelay {
        runtime,
        domain,
        relay,
        ..
    } = &tenant_relay(2, 1024 * 1024).await;
    let one = container_need(runtime, domain, relay, 1).await;
    let held = hold_all_but(runtime, one / 2);
    let refused = runtime
        .capture_materialized_state_page(domain, relay, page_query(None, remote(runtime)))
        .await;
    let Err(error) = refused else {
        panic!("a page without room for its first row was answered");
    };
    assert!(
        matches!(error.current_context(), StatePageReadError::Admission),
        "{error:?}"
    );
    drop(held);
}

/// A page another node reads encodes its container, digest, descriptor and length only from what
/// it set aside: with every other byte of the bulk pool taken, its stream is ready at once.
#[nervix_primitives::test]
async fn a_remote_materialized_page_encodes_its_reply_from_what_it_set_aside() {
    let TenantRelay {
        runtime,
        domain,
        relay,
        ..
    } = &tenant_relay(3, 64 * 1024).await;
    let captured = runtime
        .capture_materialized_state_page(domain, relay, page_query(None, remote(runtime)))
        .await
        .assured("an idle owner answers a page");
    let page = captured.page;
    let descriptor = nervix_interconnect::MaterializedStatePageDescriptor {
        origin: nervix_models::StateInspectionOrigin {
            node: nervix_models::ClusterNodeIdentity::new(
                named("node-1"),
                nervix_models::ClusterNodeIncarnation::new(1),
            ),
            fingerprint: page.fingerprint,
            generation: page.generation.branch_generation(),
            revision: page.generation.revision(),
            stage: page.stage,
        },
        fence: page.generation.fence(),
        counts: page.counts,
        offset: page.offset,
        last_branch: page.last_branch,
        more: page.more,
    };
    let held = hold_all_but(runtime, 0);
    let streamed = timeout(
        std::time::Duration::from_secs(30),
        crate::runtime::state_inspection_exchange::stream_materialized_page(
            runtime.inspection_executor(),
            Ok((descriptor, page)),
            64 * 1024 * 1024,
        ),
    )
    .await
    .assured("the owner never waits for room its page holds");
    assert!(
        streamed.is_ok(),
        "the page's reply is encoded from what it set aside"
    );
    drop(streamed);
    drop(held);
}
