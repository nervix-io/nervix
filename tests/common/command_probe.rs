//! Boundaries an intentionally interrupted command must reach before its raw outcome is observed.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The command pauses and exact-reference lease refusal a scenario armed, and a
//!   retained witness that stops prerequisite recovery once that boundary has been reached.
//! - **Depends on.** The fault harness's existing one-shot barriers, consensus write probes,
//!   public command outcomes and the future awaiting a command's reply.
//! - **Must not know.** Assertion policy, expected command success or failure, or product recovery decisions.

use ahash::HashSet;
use futures_util::{FutureExt as _, future::BoxFuture};
use nervix_client_wire::{CommandDisposition, CommandOutcome, UnknownOutcomeCause};
use nervix_models::{ClusterNodeName, CommandExecutionReference, DomainName};
use nervix_server::{DomainResumeBoundary, FaultInjection};

#[derive(Clone, Copy, Debug)]
pub(crate) enum CommandRefusalPhase {
    Admission,
    Finalization,
}

impl From<CommandRefusalPhase> for nervix_consensus::CommandWrite {
    fn from(phase: CommandRefusalPhase) -> Self {
        match phase {
            CommandRefusalPhase::Admission => Self::Admission,
            CommandRefusalPhase::Finalization => Self::Finalization,
        }
    }
}

impl CommandRefusalPhase {
    fn matches_reply(self, outcome: &CommandOutcome) -> bool {
        let phase = match self {
            Self::Admission => "was being admitted",
            Self::Finalization => "was being finalized",
        };
        matches!(
            outcome.disposition,
            CommandDisposition::OutcomeUnknown(UnknownOutcomeCause::LeadershipLost)
        ) && outcome
            .message
            .starts_with("the leader could not confirm its lease in term ")
            && outcome.message.contains(phase)
    }
}

pub(crate) enum ProbeReply {
    BeforeBoundary(CommandOutcome),
    AfterBoundary(CommandOutcome),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CommandPause {
    Admission,
    ReferenceLookup,
    DurableAdmission,
    Finalization,
    ResponseDelivery,
    DomainResume(DomainName, DomainResumeBoundary),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ArmedPause {
    node: ClusterNodeName,
    point: CommandPause,
}

impl ArmedPause {
    async fn wait(self, faults: FaultInjection) {
        match self.point {
            CommandPause::Admission => faults.wait_for_command_admission_pause(&self.node).await,
            CommandPause::ReferenceLookup => {
                faults
                    .wait_for_command_reference_lookup_pause(&self.node)
                    .await
            }
            CommandPause::DurableAdmission => {
                faults
                    .wait_for_command_durable_admission_pause(&self.node)
                    .await
            }
            CommandPause::Finalization => {
                faults.wait_for_command_finalization_pause(&self.node).await
            }
            CommandPause::ResponseDelivery => {
                faults
                    .wait_for_command_response_delivery_pause(&self.node)
                    .await
            }
            CommandPause::DomainResume(domain, boundary) => {
                faults
                    .wait_for_domain_resume_pause(&self.node, &domain, boundary)
                    .await
            }
        }
    }
}

#[derive(Clone, Debug)]
struct ArmedRefusal {
    reference: CommandExecutionReference,
    phase: CommandRefusalPhase,
}

/// A scenario's current arms. Releasing a pause retires it before a later command is started.
#[derive(Default, Debug)]
pub(crate) struct CommandProbes {
    pauses: HashSet<ArmedPause>,
    refusal: Option<ArmedRefusal>,
}

impl CommandProbes {
    pub(crate) fn arm_pause(&mut self, node: ClusterNodeName, point: CommandPause) {
        self.pauses.insert(ArmedPause { node, point });
    }

    pub(crate) fn release_pause(&mut self, node: &ClusterNodeName, point: CommandPause) {
        self.pauses.remove(&ArmedPause {
            node: node.clone(),
            point,
        });
    }

    pub(crate) fn arm_refusal(
        &mut self,
        reference: CommandExecutionReference,
        phase: CommandRefusalPhase,
    ) {
        self.refusal = Some(ArmedRefusal { reference, phase });
    }

    pub(crate) fn for_command(
        &self,
        reference: &CommandExecutionReference,
        faults: FaultInjection,
    ) -> Option<CommandProbe> {
        // A refusal can be an earlier prerequisite of a pause (the cancellation probe does this).
        // In that case reaching the pause is the goal; the refusal remains a separate assertion.
        if !self.pauses.is_empty() {
            let waits = self
                .pauses
                .iter()
                .cloned()
                .map(|pause| pause.wait(faults.clone()).boxed())
                .collect::<Vec<_>>();
            let witness = async move {
                let ((), _, _) = futures_util::future::select_all(waits).await;
            }
            .boxed();
            return Some(CommandProbe::pause(witness));
        }
        let refusal = self
            .refusal
            .as_ref()
            .filter(|armed| armed.reference == *reference)?;
        Some(CommandProbe {
            boundary: ProbeBoundary::Refusal {
                faults,
                armed: refusal.clone(),
            },
            reached: false,
        })
    }
}

enum ProbeBoundary {
    Pause(BoxFuture<'static, ()>),
    Refusal {
        faults: FaultInjection,
        armed: ArmedRefusal,
    },
}

pub(crate) struct CommandProbe {
    boundary: ProbeBoundary,
    reached: bool,
}

impl CommandProbe {
    pub(crate) fn pause(witness: BoxFuture<'static, ()>) -> Self {
        Self {
            boundary: ProbeBoundary::Pause(witness),
            reached: false,
        }
    }

    /// A pause must be observed before this attempt's reply, not after an earlier reply while
    /// reconciliation runs. Once observed, a repeated waiter still sends its request and waits.
    pub(crate) async fn observe<F>(&mut self, request: F) -> std::io::Result<ProbeReply>
    where
        F: std::future::Future<Output = std::io::Result<CommandOutcome>>,
    {
        let mut request = Box::pin(request);
        match &mut self.boundary {
            ProbeBoundary::Pause(witness) => {
                if self.reached || witness.as_mut().now_or_never().is_some() {
                    self.reached = true;
                    return request.await.map(ProbeReply::AfterBoundary);
                }
                nervix_primitives::select! {
                    biased;
                    outcome = &mut request => outcome.map(ProbeReply::BeforeBoundary),
                    () = witness => {
                        self.reached = true;
                        request.await.map(ProbeReply::AfterBoundary)
                    },
                }
            }
            ProbeBoundary::Refusal { faults, armed } => {
                let outcome = request.await?;
                let delivered = faults
                    .command_write_refused_as_lapsed_lease(&armed.reference, armed.phase.into())
                    .await;
                self.reached |= delivered;
                if delivered && armed.phase.matches_reply(&outcome) {
                    Ok(ProbeReply::AfterBoundary(outcome))
                } else {
                    Ok(ProbeReply::BeforeBoundary(outcome))
                }
            }
        }
    }

    /// Poll before sending the first attempt, so pause futures retain the exact barrier lifetime.
    /// The witness remains reached even after a later step releases or rearms that pause point.
    pub(crate) async fn reached(&mut self) -> bool {
        if self.reached {
            return true;
        }
        self.reached = match &mut self.boundary {
            ProbeBoundary::Pause(witness) => witness.as_mut().now_or_never().is_some(),
            ProbeBoundary::Refusal { faults, armed } => {
                faults
                    .command_write_refused_as_lapsed_lease(&armed.reference, armed.phase.into())
                    .await
            }
        };
        self.reached
    }
}
