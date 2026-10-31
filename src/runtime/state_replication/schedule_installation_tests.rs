//! Layer: test harness.
//! Owns: recovery schedule selection, its installation wait and its deadline outcomes.
//! May depend on: the production runtime, current schedule Models and the Shuttle runner.
//! Must not know: checkpoint encoding, connector drivers or state recreation policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns focused tests of runtime installation")
)]

use nervix_models::{
    ClusterSchedule, CreateRelay, CreateSchema, DomainSchedule, DomainStatus, ParseAsType,
    RelayBranching, ResolvedBranching, ScheduledNode, SchemaField, SchemaFingerprint, SchemaName,
};
use nonzero_ext::nonzero;

use super::*;

struct InstallationFixture {
    domain: DomainName,
    node: NodeRef,
    schedule: ClusterSchedule,
    fingerprint: [u8; 32],
}

impl InstallationFixture {
    fn new() -> Self {
        let domain = domain("recovery_installation");
        let source = named::<ClusterNodeName>("node-1");
        let schema = named::<SchemaName>("event");
        let schema_node = ScheduledNode::new(
            nervix_models::Model::Schema(CreateSchema {
                name: schema.clone(),
                fields: vec![SchemaField {
                    name: named("sequence"),
                    ty: ParseAsType::U64,
                    optional: false,
                    sensitive: false,
                }],
            }),
            SchemaFingerprint::from_digest([3; 32]),
        );
        let relay = ScheduledNode::new(
            nervix_models::Model::Relay(CreateRelay {
                name: named("events"),
                schema,
                buffer: nonzero!(4usize),
                branching: RelayBranching::unbranched(),
                materialized_state: None,
            }),
            SchemaFingerprint::from_digest([3; 32]),
        )
        .with_resolved_branching(Some(ResolvedBranching::unbranched()))
        .placed_on(Some(source.clone()), vec![source]);
        let node = relay.identity();
        let schedule = DomainSchedule::new(domain.clone(), vec![schema_node, relay], Vec::new());
        let fingerprint = ExecutionRevision::ownership_fingerprint(&schedule)
            .assured("the complete current schedule has an ownership fingerprint");
        Self {
            domain,
            node,
            schedule: ClusterSchedule::from_iter([schedule]),
            fingerprint,
        }
    }

    async fn install(&self, runtime: &Runtime) {
        let mut state = unpaced_domain_state(self.domain.as_str());
        state.status = DomainStatus::Stopped;
        runtime
            .apply_cluster_state(
                &named::<ClusterNodeName>("node-2"),
                1,
                &BTreeMap::from([(self.domain.clone(), state)]),
                &BTreeMap::new(),
                &self.schedule,
            )
            .await
            .assured("the complete stopped relay schedule installs without external resources");
    }

    #[cfg(not(feature = "shuttle"))]
    fn request(&self) -> nervix_interconnect::PrepareForcedOwnershipRecoveryRequest {
        nervix_interconnect::PrepareForcedOwnershipRecoveryRequest {
            operation_id: "await-installation".to_owned(),
            source: named("node-1"),
            destination: named("node-2"),
            destination_incarnation: ClusterNodeIncarnation::new(42),
            domain: self.domain.clone(),
            unit: nervix_models::Unit::Single,
            entity: self.node.clone(),
            base_schedule_fingerprint: self.fingerprint,
            target_schedule_fingerprint: self.fingerprint,
        }
    }
}

async fn installation_racing_recovery_resolves_the_exact_node() {
    let fixture = InstallationFixture::new();
    let runtime = Runtime::new();
    let waiting_runtime = runtime.clone();
    let domain = fixture.domain.clone();
    let node = fixture.node.clone();
    let fingerprint = fixture.fingerprint;
    let waiter = nervix_primitives::task::spawn(async move {
        waiting_runtime
            .installed_forced_recovery_node(&domain, &node, fingerprint)
            .await
    });
    fixture.install(&runtime).await;
    let installed = waiter
        .await
        .assured("a spawned test failure propagates through its join")
        .assured("the committed matching schedule supplies the requested node");
    assert_eq!(installed.identity(), fixture.node);
    assert_eq!(
        installed.primary_node(),
        Some(&named::<ClusterNodeName>("node-1"))
    );
}

#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test]
async fn recovery_resolves_a_schedule_installed_concurrently() {
    installation_racing_recovery_resolves_the_exact_node().await;
}

#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test]
async fn recovery_installation_deadline_names_the_domain_and_node() {
    let fixture = InstallationFixture::new();
    let runtime = Runtime::new();
    let outcome = runtime
        .prepare_forced_ownership_recovery(fixture.request(), Instant::now())
        .await;
    let failure = match outcome {
        Ok(_) => panic!("an uninstalled schedule cannot prepare recovery"),
        Err(failure) => failure,
    };
    match failure.current_context() {
        OwnershipHandoffError::ScheduleInstallationDeadline { domain, node } => {
            assert_eq!(domain, &fixture.domain);
            assert_eq!(node, &fixture.node);
        }
        failure => panic!("the installation deadline must retain its identity: {failure}"),
    }
}

#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test]
async fn installed_recovery_schedule_retains_fingerprint_and_node_validation() {
    let fixture = InstallationFixture::new();
    let runtime = Runtime::new();
    fixture.install(&runtime).await;
    for (node, fingerprint) in [
        (fixture.node.clone(), [9; 32]),
        (
            NodeRef::new(ModelKind::Relay, named::<ModelName>("unassigned")),
            fixture.fingerprint,
        ),
    ] {
        nervix_primitives::task::consume_budget().await;
        let outcome = runtime
            .installed_forced_recovery_node(&fixture.domain, &node, fingerprint)
            .await;
        let failure = match outcome {
            Ok(_) => panic!("a recovery must match the installed schedule and its node"),
            Err(failure) => failure,
        };
        assert!(matches!(
            failure.current_context(),
            OwnershipHandoffError::Schedule(_)
        ));
    }
}

#[cfg(feature = "shuttle")]
#[test]
fn shuttle_recovery_never_misses_initial_schedule_installation() {
    nervix_model_harness::shuttle::check_interleavings(|| {
        shuttle::future::block_on(installation_racing_recovery_resolves_the_exact_node());
    });
}
