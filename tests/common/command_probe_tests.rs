//! Causal boundary observations for command probes.
//!
//! Layer: harness tests, outside the product layer order.
//! - **Owns.** Reply-before-gate, gate-before-reply, retained-witness and transport-failure checks.
//! - **Depends on.** The command probe, current wire outcomes and owned one-shot signals.
//! - **Must not know.** Product admission, reconciliation or transport implementations.

use futures_util::FutureExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{CommandDisposition, CommandOutcome, OutcomeOrigin, UnknownOutcomeCause};
use nervix_models::{ClusterNodeName, CommandExecutionReference, DomainName};
use nervix_primitives::sync::oneshot;
use nervix_server::{DomainResumeBoundary, FaultInjection};

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
        wasm_state: None,
        resource: None,
        backup: None,
        restore: None,
    }
}

fn gate() -> (CommandProbe, oneshot::Sender<()>) {
    let (sender, receiver) = oneshot::channel();
    let witness = async move {
        receiver.await.assured("the test owns the gate sender");
    }
    .boxed();
    (CommandProbe::pause(witness), sender)
}

#[nervix_primitives::test]
async fn a_reply_before_the_gate_is_not_an_after_boundary_reply() {
    let (mut probe, _gate) = gate();
    let expected = outcome();
    let reply = probe
        .observe(std::future::ready(Ok(expected.clone())))
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
    let mut observing = Box::pin(probe.observe(async move {
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
            .observe(std::future::ready(Ok(expected.clone())))
            .await
            .assured("the later request completes");
        let ProbeReply::AfterBoundary(actual) = reply else {
            panic!("a consumed witness remains observed for later waiters");
        };
        assert_eq!(actual, expected);
    }
}

#[nervix_primitives::test]
async fn a_transport_failure_does_not_claim_the_gate_was_reached() {
    let (mut probe, _gate) = gate();
    let reply = probe
        .observe(std::future::ready(Err(std::io::Error::other(
            "request transport ended",
        ))))
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
        CommandPause::DomainResume(domain, DomainResumeBoundary::Failure),
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
            .observe(std::future::ready(Ok(expected.clone())))
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
