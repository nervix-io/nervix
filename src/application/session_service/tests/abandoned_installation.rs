//! An installation whose caller goes away in the middle of it.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The regression that a stopped domain's teardown, begun by the application an
//!   interconnect handler runs, ends whole when the interconnect server drops that handler.
//! - **Depends on.** The single-node test service, its fault injection, the consensus commands
//!   that request and end a domain's stop, and the runtime's stop ledger and report.
//! - **Must not know.** How the interconnect server notices a reset stream, or how the stop's owner
//!   decides that the domain rests.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "abandoned installation regressions remain outside product execution"
    )
)]

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_interconnect::domain_stop::StopReportAnswer;
use nervix_models::{DomainName, DomainStatus, OperatorStopRequest, StopEnding, StopRequest};
use nervix_primitives::time::timeout;
use nervix_recovery::Discarded as _;

use crate::application::{
    domain_clock::current_timestamp,
    subscription::SessionSubscriptions,
    test_fixtures::{
        TestService, build_test_service_with_fault_injection, test_command_request,
        test_execution_reference,
    },
};

/// The leader's domain drain asks every node for a domain's drain status, and the node's handler
/// applies the committed runtime state before it answers. When the leader's request deadline
/// passes, the leader resets the stream and the interconnect server drops the handler wherever it
/// is. That application can be the one tearing down another domain whose stop has just ended. The
/// test drops it at the worst moment it can reach: its teardown is held once the stopped domain's
/// execution is out of the node and before that execution's tasks end. The test then releases the
/// hold and lets the node apply the committed state again, as its reconciliation does. The node
/// must still have ended the domain's tasks, written its final snapshot and completed its report of
/// the stop, or the stop's `STOP` never answers.
#[nervix_primitives::test]
async fn a_teardown_begun_by_a_dropped_interconnect_handler_still_completes_the_stop_report() {
    let fault_injection = crate::FaultInjection::default();
    let TestService {
        service,
        registry,
        path,
    } = build_test_service_with_fault_injection(true, fault_injection.clone()).await;
    let domain = DomainName::parse("default").assured("the fixture domain name is a valid literal");
    let mut subscriptions = SessionSubscriptions::new();
    // Two relays and a junction between them: an execution whose tasks the teardown waits for.
    let configured = service
        .test_command(
            test_command_request(
                "BEGIN; CREATE SCHEMA event ( id I64 ); CREATE RELAY events SCHEMA event \
                 UNBRANCHED; CREATE RELAY forwarded SCHEMA event UNBRANCHED; CREATE JUNCTION \
                 forward FROM events UNBRANCHED TO forwarded INHERIT ALL FLUSH IMMEDIATE ON \
                 MESSAGE ERROR LOG; COMMIT;",
                "default",
            ),
            &mut subscriptions,
        )
        .await;
    assert!(
        configured.succeeded(),
        "the fixture graph is created: {configured:?}"
    );
    subscriptions.stop_all().await;
    service.test_start_domain("default").await;

    // The stop is requested and its intake stop applied, as a `STOP` does before it waits. Nothing
    // is in motion, so the stop's owner would end it at once; the test commits that ending itself,
    // so the next application on this node is the one that tears the domain down.
    service
        .inner
        .consensus
        .request_domain_stop(
            domain.clone(),
            StopRequest::Operator {
                reference: test_execution_reference(),
                request: OperatorStopRequest {
                    requested_at: current_timestamp(),
                    limit: None,
                },
            },
            None,
        )
        .await
        .assured("the leader of the fixture's single-node consensus commits the request");
    service
        .apply_current_cluster_state()
        .await
        .assured("the fixture node applies the stopping domain");
    let stopping = service
        .inner
        .consensus
        .current_domain(&domain)
        .await
        .assured("the fixture domain exists");
    let began_at_revision = stopping
        .stopping()
        .verified("the request above began the domain's stop")
        .began_at_revision;
    let start_version = stopping.start_version;
    service
        .inner
        .consensus
        .end_domain_stop(domain.clone(), began_at_revision, StopEnding::Completed)
        .await
        .assured("the leader of the fixture's single-node consensus commits the ending");

    // The drain-status handler's application, driven until its teardown reaches the hold and then
    // dropped, as the interconnect server drops a handler whose stream reset.
    fault_injection.pause_domain_teardown(domain.clone());
    let mut handler = Box::pin(service.apply_current_cluster_state());
    let held = timeout(Duration::from_secs(60), async {
        nervix_primitives::select! {
            biased;
            () = fault_injection.wait_for_domain_teardown_pause(&domain) => true,
            _ = &mut handler => false,
        }
    })
    .await;
    assert_eq!(
        held,
        Ok(true),
        "the handler's application begins the stopped domain's teardown and is held in it"
    );
    assert!(
        service.inner.runtime.stop_teardown_holds_execution(&domain),
        "the held teardown has taken the domain's execution out of the node"
    );
    drop(handler);
    fault_injection.release_domain_teardown_pause(&domain);

    service
        .apply_current_cluster_state()
        .await
        .assured("the fixture node applies the stopped domain, as its reconciliation does");
    let ended = service
        .inner
        .consensus
        .current_domain(&domain)
        .await
        .assured("the fixture domain exists");
    assert_eq!(ended.status, DomainStatus::Stopped);
    assert_eq!(
        service.inner.runtime.stop_report(&domain, start_version),
        StopReportAnswer::Reported(Vec::new()),
        "the teardown the dropped handler began ran whole, so the node reports the stop, which \
         cut nothing"
    );

    drop(service);
    drop(registry);
    std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
}
