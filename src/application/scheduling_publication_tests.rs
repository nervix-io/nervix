//! Schedule-publication tests for captured planning inputs.
//!
//! Layer: test harness.
//!
//! - **Owns.** Regression coverage that publication uses the basis captured during preparation.
//! - **Depends on.** The scheduling control plane and the server test fixture.
//! - **Must not know.** Connector implementations or runtime execution details.

use meticulous::ResultExt as _;
use nervix_models::{DomainName, DomainSchedule, PlacementPolicy};
use nervix_recovery::Discarded as _;
use tokio::sync::mpsc;

use super::super::{
    subscription::SessionSubscriptions,
    test_fixtures::{TestService, build_test_service},
};
use crate::proto::CommandRequest;

#[tokio::test]
async fn schedule_publication_keeps_the_basis_it_prepared() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let domain = DomainName::parse("default").assured("the test domain name is valid");
    let inputs = service
        .inner
        .consensus
        .domain_planning_inputs(&domain)
        .await;
    service
        .inner
        .consensus
        .replace_domain_schedule(
            inputs,
            Some(DomainSchedule::new(domain.clone(), [], Vec::new())),
            None,
        )
        .await
        .assured("the single-node test consensus accepts the initial schedule");

    let prepared = service
        .prepare_domain_schedule(&domain, None, PlacementPolicy::Neutral)
        .await
        .assured("the current domain has a complete planning basis");
    assert!(prepared.inputs.schedule().is_some());
    assert!(prepared.schedule.is_none());

    let relocations = service
        .publish_domain_schedule(&domain, None, None)
        .await
        .assured("the unchanged captured basis permits schedule publication");
    assert_eq!(relocations, 0);
    assert!(
        service
            .inner
            .consensus
            .current_schedule()
            .await
            .domain(&domain)
            .is_none()
    );

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}

#[tokio::test]
async fn drain_reports_when_its_captured_eligibility_has_no_replacement() {
    let TestService {
        service,
        registry,
        path,
    } = build_test_service(true).await;
    let (tx, _rx) = mpsc::channel(16);
    let mut subscriptions = SessionSubscriptions::new();

    for command in [
        "CREATE SCHEMA event ( id I64 );",
        "CREATE RELAY events SCHEMA event UNBRANCHED;",
    ] {
        let result = service
            .process_command(
                CommandRequest {
                    query: command.to_string(),
                    domain: "default".to_string(),
                    execution_reference: uuid::Uuid::now_v7().to_string(),
                    expected_transaction_position: None,
                    expected_preview: None,
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(
            result.success,
            "test graph command must succeed: {command}: {}",
            result.message
        );
    }

    let local_node = service.inner.consensus.local_node_id().clone();
    let result = service.drain_node(local_node, None).await;
    assert!(!result.success);
    assert!(
        result
            .message
            .contains("no live schedulable raft voters remain")
    );

    subscriptions.stop_all(&service).await;
    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}
