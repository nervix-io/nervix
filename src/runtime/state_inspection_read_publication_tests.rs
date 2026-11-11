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
use nervix_primitives::{sync::StdArc, time::timeout};

use super::MaterializedPageBounds;
use crate::{
    runtime::{
        BranchPresence, DomainRoutingSnapshot, RelayBoundaryFanout, RelayBoundaryServices,
        RelayBranchPresences, RelayPartitioning, RelayRecordBatch, RelayRetention,
        RelayRuntimeFanIn, RelayStateTaskSpec, Runtime, RuntimeMaterializedRelaySpec, RuntimeState,
        RuntimeStatePlacement, VmSchemaSensitivity, domain, install_unpaced_test_domain, named,
        publish_state_identity, test_schema,
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
                Unit::SINGLE,
                StateInspectionBranchSelection::All,
                None,
                bounds(),
                false,
                None,
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
                Unit::SINGLE,
                StateInspectionBranchSelection::All,
                None,
                bounds(),
                false,
                None,
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
        task.stop(std::time::Duration::from_secs(30))
            .await
            .assured("the actual materialized task drains and publishes its queued update");
        let after_application = runtime
            .capture_materialized_state_page(
                &domain,
                &relay,
                Unit::SINGLE,
                StateInspectionBranchSelection::All,
                None,
                bounds(),
                false,
                None,
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
