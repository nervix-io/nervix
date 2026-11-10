//! Causal boundary observations for command probes.
//!
//! Layer: harness tests, outside the product layer order.
//! - **Owns.** Node and stage causality, reply ordering, retained witnesses and transport failures.
//! - **Depends on.** The command probe, current wire outcomes and owned one-shot signals.
//! - **Must not know.** Product admission, reconciliation or transport implementations.

use ahash::HashMap;
use futures_util::FutureExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{CommandDisposition, CommandOutcome, OutcomeOrigin, UnknownOutcomeCause};
use nervix_models::{ClusterNodeName, CommandExecutionReference, DomainName};
use nervix_primitives::sync::oneshot;
use nervix_server::{DomainResumeBoundary, FaultInjection, StandaloneTransactionBoundary};

use super::command_probe::{
    CommandPause, CommandProbe, CommandProbes, CommandRefusalPhase, ProbeReply,
};

fn outcome() -> CommandOutcome {
    CommandOutcome {
        execution_reference: CommandExecutionReference::parse(
            "018bcfe5-6800-7000-8000-000000000001",
        )
        .assured("the fixture reference is UUIDv7"),
        origin: OutcomeOrigin::Executed,
        disposition: CommandDisposition::Completed {
            already_existed: false,
        },
        message: "command completed".to_string(),
        diagnostics: Vec::new(),
        statements: Vec::new(),
        transaction: None,
        transaction_admission: None,
        inspection: None,
        entity_labels: None,
        state: None,
        resource: None,
        backup: None,
        restore: None,
    }
}

fn gate() -> (CommandProbe, oneshot::Sender<()>) {
    gate_at(CommandPause::Finalization)
}

fn gate_at(point: CommandPause) -> (CommandProbe, oneshot::Sender<()>) {
    let (sender, receiver) = oneshot::channel();
    let witness = async move {
        receiver.await.assured("the test owns the gate sender");
        point
    }
    .boxed();
    let node = ClusterNodeName::parse("node-1").assured("the fixture node name is valid");
    let waits = HashMap::from_iter([(node, vec![witness])]);
    (CommandProbe::pauses(waits), sender)
}

#[nervix_primitives::test]
async fn a_reply_before_the_gate_is_not_an_after_boundary_reply() {
    let (mut probe, _gate) = gate();
    let expected = outcome();
    let reply = probe
        .observe(Some("node-1"), std::future::ready(Ok(expected.clone())))
        .await
        .assured("the fixture reply completes");
    let ProbeReply::BeforeBoundary(actual) = reply else {
        panic!("a reply before the gate cannot satisfy its observation");
    };
    assert_eq!(actual, expected);
    assert!(!probe.reached().await);
}

#[nervix_primitives::test]
async fn a_gate_observation_does_not_finish_the_wait_for_its_reply() {
    let (mut probe, gate) = gate();
    let (reply_sender, reply_receiver) = oneshot::channel();
    let expected = outcome();
    let supplied = expected.clone();
    let mut observing = Box::pin(probe.observe(Some("node-1"), async move {
        reply_receiver
            .await
            .assured("the test owns the reply sender");
        Ok(supplied)
    }));
    assert!(observing.as_mut().now_or_never().is_none());
    gate.send(()).assured("the witness still owns its receiver");
    assert!(observing.as_mut().now_or_never().is_none());
    reply_sender
        .send(())
        .assured("the request still waits for its reply");
    let reply = observing.await.assured("the fixture reply completes");
    let ProbeReply::AfterBoundary(actual) = reply else {
        panic!("the gate preceded this attempt's reply");
    };
    assert_eq!(actual, expected);
}

#[nervix_primitives::test]
async fn a_reached_gate_is_retained_for_later_waiters() {
    let (mut probe, gate) = gate();
    gate.send(()).assured("the witness owns its receiver");
    assert!(probe.reached().await);
    for _ in 0..2 {
        let expected = outcome();
        let reply = probe
            .observe(Some("node-1"), std::future::ready(Ok(expected.clone())))
            .await
            .assured("the later request completes");
        let ProbeReply::AfterBoundary(actual) = reply else {
            panic!("a consumed witness remains observed for later waiters");
        };
        assert_eq!(actual, expected);
    }
}

fn follower_refusal() -> CommandOutcome {
    CommandOutcome {
        disposition: CommandDisposition::NotLeader(nervix_client_wire::LeaderRedirect {
            leader: None,
        }),
        message: "not-a-leader".to_string(),
        ..outcome()
    }
}

#[nervix_primitives::test]
async fn a_reconcilers_pause_does_not_make_a_refused_request_an_admitted_attempt() {
    let (mut probe, gate) = gate();
    gate.send(())
        .assured("the reconciler reaches the retained pause");
    assert!(probe.reached().await);
    let expected = follower_refusal();
    let reply = probe
        .observe(Some("node-1"), std::future::ready(Ok(expected.clone())))
        .await
        .assured("the follower answers the recovery request");
    let ProbeReply::BeforeBoundary(actual) = reply else {
        panic!("a follower refused this attempt before admission, despite the reconciler's pause");
    };
    assert_eq!(actual, expected);
    assert!(probe.reached().await);
}

#[nervix_primitives::test]
async fn a_pause_during_a_refused_request_does_not_attribute_finalization_to_it() {
    let (mut probe, gate) = gate();
    let (reply_sender, reply_receiver) = oneshot::channel();
    let expected = follower_refusal();
    let supplied = expected.clone();
    let mut observing = Box::pin(probe.observe(Some("node-1"), async move {
        reply_receiver
            .await
            .assured("the fixture retains its reply sender");
        Ok(supplied)
    }));
    assert!(observing.as_mut().now_or_never().is_none());
    gate.send(())
        .assured("the reconciler reaches the pause while this request waits");
    assert!(observing.as_mut().now_or_never().is_none());
    reply_sender
        .send(())
        .assured("the follower request still waits for its reply");
    let reply = observing.await.assured("the follower answers");
    let ProbeReply::BeforeBoundary(actual) = reply else {
        panic!("a pause in another execution attempt cannot admit the follower's request");
    };
    assert_eq!(actual, expected);
}

#[nervix_primitives::test]
async fn a_failed_reply_after_the_pause_is_preserved_for_the_scenarios_assertion() {
    let (mut probe, gate) = gate();
    gate.send(()).assured("the fixture reaches the pause");
    let expected = CommandOutcome {
        disposition: CommandDisposition::Failed,
        message: "recording the command failed".to_string(),
        ..outcome()
    };
    let reply = probe
        .observe(Some("node-1"), std::future::ready(Ok(expected.clone())))
        .await
        .assured("the request reports its failure");
    let ProbeReply::AfterBoundary(actual) = reply else {
        panic!("a failed admitted attempt must remain visible to the scenario");
    };
    assert_eq!(actual, expected);
}

#[nervix_primitives::test]
async fn a_pause_only_observes_requests_sent_to_its_node() {
    for destination in [Some("node-2"), None] {
        let (mut probe, gate) = gate();
        gate.send(()).assured("the selected node reaches its pause");
        assert!(probe.reached().await);
        let expected = outcome();
        let reply = probe
            .observe(destination, std::future::ready(Ok(expected.clone())))
            .await
            .assured("the other request completes");
        let ProbeReply::BeforeBoundary(actual) = reply else {
            panic!("another or unknown destination cannot consume this node's pause");
        };
        assert_eq!(actual, expected);
    }
}

#[nervix_primitives::test]
async fn a_detached_transaction_reply_cannot_satisfy_an_admitted_pause() {
    let (mut probe, gate) = gate();
    gate.send(()).assured("the execution reaches its pause");
    let expected = CommandOutcome {
        disposition: CommandDisposition::TransactionDetached {
            transaction_id: "fixture-transaction".to_string(),
        },
        message: "the transaction is not attached".to_string(),
        ..outcome()
    };
    let reply = probe
        .observe(Some("node-1"), std::future::ready(Ok(expected.clone())))
        .await
        .assured("the unbound request answers");
    let ProbeReply::BeforeBoundary(actual) = reply else {
        panic!("a request refused before admission must remain a prerequisite");
    };
    assert_eq!(actual, expected);
}

#[nervix_primitives::test]
async fn an_admission_or_delivery_pause_can_expose_an_unadmitted_redirect() {
    for point in [
        CommandPause::Admission,
        CommandPause::ReferenceLookup,
        CommandPause::ResponseDelivery,
    ] {
        let (mut probe, gate) = gate_at(point);
        gate.send(())
            .assured("the request reaches its selected pause");
        let expected = follower_refusal();
        let reply = probe
            .observe(Some("node-1"), std::future::ready(Ok(expected.clone())))
            .await
            .assured("the request answers after its pause");
        let ProbeReply::AfterBoundary(actual) = reply else {
            panic!("this stage can legitimately precede an unadmitted redirect");
        };
        assert_eq!(actual, expected);
    }
}

#[nervix_primitives::test]
async fn every_reached_stage_on_a_node_remains_available_to_its_reply() {
    for points in [
        [CommandPause::Finalization, CommandPause::Admission],
        [CommandPause::Admission, CommandPause::Finalization],
    ] {
        let node = ClusterNodeName::parse("node-1").assured("the fixture node name is valid");
        let waits = points
            .into_iter()
            .map(|point| std::future::ready(point).boxed())
            .collect();
        let mut probe = CommandProbe::pauses(HashMap::from_iter([(node, waits)]));
        assert!(probe.reached().await);
        let expected = follower_refusal();
        let reply = probe
            .observe(Some("node-1"), std::future::ready(Ok(expected.clone())))
            .await
            .assured("the request completes after both retained stages");
        let ProbeReply::AfterBoundary(actual) = reply else {
            panic!("a reached admission pause remains available beside an admitted-work pause");
        };
        assert_eq!(actual, expected);
    }
}

#[nervix_primitives::test]
async fn a_later_stage_stays_observable_while_its_nodes_reply_is_pending() {
    let node = ClusterNodeName::parse("node-1").assured("the fixture node name is valid");
    let (started, began) = oneshot::channel();
    let (admission, admitted) = oneshot::channel();
    let admission_witness = async move {
        started
            .send(())
            .assured("the test retains its initialization receiver");
        admitted.await.assured("the test owns the admission sender");
        CommandPause::Admission
    }
    .boxed();
    let waits = vec![
        std::future::ready(CommandPause::Finalization).boxed(),
        admission_witness,
    ];
    let mut probe = CommandProbe::pauses(HashMap::from_iter([(node, waits)]));
    let (reply_sender, reply_receiver) = oneshot::channel();
    let expected = follower_refusal();
    let supplied = expected.clone();
    let mut observing = Box::pin(probe.observe(Some("node-1"), async move {
        reply_receiver
            .await
            .assured("the test owns the reply sender");
        Ok(supplied)
    }));
    assert!(observing.as_mut().now_or_never().is_none());
    began
        .now_or_never()
        .verified("all of the node's witnesses were initialized")
        .assured("the waiting stage sent its initialization observation");
    admission
        .send(())
        .assured("the later stage keeps its exact witness lifetime");
    assert!(observing.as_mut().now_or_never().is_none());
    reply_sender
        .send(())
        .assured("the request still waits for its complete reply");
    let reply = observing.await.assured("the request completes");
    let ProbeReply::AfterBoundary(actual) = reply else {
        panic!("the later admission witness preceded this raw redirect");
    };
    assert_eq!(actual, expected);
}

#[nervix_primitives::test]
async fn every_nodes_initial_witness_is_polled_even_when_another_is_already_reached() {
    let mut waits = HashMap::default();
    let mut beginnings = Vec::new();
    for name in ["node-1", "node-2"] {
        let node = ClusterNodeName::parse(name).assured("the fixture node name is valid");
        let (started, began) = oneshot::channel();
        let witness = async move {
            started
                .send(())
                .assured("the test retains its initialization receiver");
            CommandPause::Finalization
        }
        .boxed();
        waits.insert(node, vec![witness]);
        beginnings.push(began);
    }
    let mut probe = CommandProbe::pauses(waits);
    assert!(probe.reached().await);
    for began in beginnings {
        began
            .now_or_never()
            .verified("every node's witness was initialized")
            .assured("the initialized witness sent its observation");
    }
}

#[nervix_primitives::test]
async fn a_transport_failure_does_not_claim_the_gate_was_reached() {
    let (mut probe, _gate) = gate();
    let reply = probe
        .observe(
            Some("node-1"),
            std::future::ready(Err(std::io::Error::other("request transport ended"))),
        )
        .await;
    assert!(matches!(reply, Err(error) if error.to_string() == "request transport ended"));
    assert!(!probe.reached().await);
}

#[nervix_primitives::test]
async fn released_pauses_do_not_become_later_commands_boundaries() {
    let node = ClusterNodeName::parse("node-1").assured("the fixture node name is valid");
    let domain = DomainName::parse("probe_test").assured("the fixture domain name is valid");
    let faults = FaultInjection::default();
    let mut probes = CommandProbes::default();
    let reference = outcome().execution_reference;
    let points = [
        CommandPause::Admission,
        CommandPause::ReferenceLookup,
        CommandPause::DurableAdmission,
        CommandPause::Finalization,
        CommandPause::ResponseDelivery,
        CommandPause::DomainResume(domain.clone(), DomainResumeBoundary::Proposal),
        CommandPause::DomainResume(domain.clone(), DomainResumeBoundary::Failure),
        CommandPause::StandaloneTransaction(domain.clone(), StandaloneTransactionBoundary::Lookup),
        CommandPause::StandaloneTransaction(
            domain.clone(),
            StandaloneTransactionBoundary::Preflight,
        ),
        CommandPause::StandaloneTransaction(domain, StandaloneTransactionBoundary::CommitFailure),
    ];
    for point in points {
        match &point {
            CommandPause::Admission => faults.pause_command_admission_on(node.clone()),
            CommandPause::ReferenceLookup => {
                faults.pause_command_reference_lookup_on(node.clone());
            }
            CommandPause::DurableAdmission => {
                faults.pause_command_after_durable_admission_on(node.clone());
            }
            CommandPause::Finalization => faults.pause_command_finalization_on(node.clone()),
            CommandPause::ResponseDelivery => {
                faults.pause_command_response_delivery_on(node.clone());
            }
            CommandPause::DomainResume(domain, boundary) => {
                faults.pause_domain_resume(node.clone(), domain.clone(), *boundary);
            }
            CommandPause::StandaloneTransaction(domain, boundary) => {
                faults.pause_standalone_transaction(node.clone(), domain.clone(), *boundary);
            }
        }
        probes.arm_pause(node.clone(), point.clone());
        let mut probe = probes
            .for_command(&reference, faults.clone())
            .verified("the current arm supplies a witness");
        assert!(!probe.reached().await);
        probes.release_pause(&node, point);
        assert!(probes.for_command(&reference, faults.clone()).is_none());
    }
    faults.release_all_command_pauses();
}

#[nervix_primitives::test]
async fn a_matching_lease_message_without_fault_consumption_does_not_satisfy_a_probe() {
    let faults = FaultInjection::default();
    for (phase, message) in [
        (CommandRefusalPhase::Admission, "was being admitted"),
        (CommandRefusalPhase::Finalization, "was being finalized"),
    ] {
        let mut expected = outcome();
        expected.disposition =
            CommandDisposition::OutcomeUnknown(UnknownOutcomeCause::LeadershipLost);
        expected.message =
            format!("the leader could not confirm its lease in term 1 while the command {message}");
        let mut probes = CommandProbes::default();
        probes.arm_refusal(expected.execution_reference.clone(), phase);
        let mut probe = probes
            .for_command(&expected.execution_reference, faults.clone())
            .verified("the exact reference selects the armed refusal");
        let reply = probe
            .observe(Some("node-1"), std::future::ready(Ok(expected.clone())))
            .await
            .assured("the fixture reply completes");
        let ProbeReply::BeforeBoundary(actual) = reply else {
            panic!("a matching message cannot replace an exact-reference fault receipt");
        };
        assert_eq!(actual, expected);
        assert!(!probe.reached().await);
        let other = CommandExecutionReference::parse("018bcfe5-6800-7000-8000-000000000002")
            .assured("the other fixture reference is UUIDv7");
        assert!(probes.for_command(&other, faults.clone()).is_none());
    }
}
