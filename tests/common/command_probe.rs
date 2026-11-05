//! Boundaries an intentionally interrupted command must reach before its raw outcome is observed.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The command pauses and exact-reference lease refusal a scenario armed, and
//!   retained node and stage witnesses that stop prerequisite recovery at an eligible reply.
//! - **Depends on.** The fault harness's existing one-shot barriers, consensus write probes,
//!   public command outcomes and the future awaiting a command's reply.
//! - **Must not know.** Assertion policy, expected command success or failure, or product recovery decisions.

use std::{
    future::{Future, poll_fn},
    task::{Context, Poll},
};

use ahash::{HashMap, HashSet};
use futures_util::{FutureExt as _, future::BoxFuture};
use meticulous::OptionExt as _;
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

impl CommandPause {
    fn observes(&self, outcome: &CommandOutcome) -> bool {
        let admitted = match self {
            Self::Admission | Self::ReferenceLookup | Self::ResponseDelivery => false,
            Self::DurableAdmission | Self::Finalization | Self::DomainResume(_, _) => true,
        };
        let unadmitted = matches!(
            outcome.disposition,
            CommandDisposition::NotLeader(_) | CommandDisposition::TransactionDetached { .. }
        );
        !admitted || !unadmitted
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ArmedPause {
    node: ClusterNodeName,
    point: CommandPause,
}

impl ArmedPause {
    async fn wait(self, faults: FaultInjection) -> CommandPause {
        match &self.point {
            CommandPause::Admission => faults.wait_for_command_admission_pause(&self.node).await,
            CommandPause::ReferenceLookup => {
                faults
                    .wait_for_command_reference_lookup_pause(&self.node)
                    .await;
            }
            CommandPause::DurableAdmission => {
                faults
                    .wait_for_command_durable_admission_pause(&self.node)
                    .await;
            }
            CommandPause::Finalization => {
                faults.wait_for_command_finalization_pause(&self.node).await;
            }
            CommandPause::ResponseDelivery => {
                faults
                    .wait_for_command_response_delivery_pause(&self.node)
                    .await;
            }
            CommandPause::DomainResume(domain, boundary) => {
                faults
                    .wait_for_domain_resume_pause(&self.node, domain, *boundary)
                    .await;
            }
        }
        self.point
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
            let mut waits = HashMap::<ClusterNodeName, Vec<_>>::default();
            for pause in self.pauses.iter().cloned() {
                waits
                    .entry(pause.node.clone())
                    .or_default()
                    .push(pause.wait(faults.clone()).boxed());
            }
            return Some(CommandProbe::pauses(waits));
        }
        let refusal = self
            .refusal
            .as_ref()
            .filter(|armed| armed.reference == *reference)?;
        Some(CommandProbe {
            boundary: ProbeBoundary::Refusal {
                faults,
                armed: refusal.clone(),
                reached: false,
            },
        })
    }
}

enum PauseWitness {
    Waiting(BoxFuture<'static, CommandPause>),
    Reached(CommandPause),
}

impl PauseWitness {
    fn poll_reached(&mut self, context: &mut Context<'_>) -> bool {
        if let Self::Waiting(witness) = self
            && let Poll::Ready(point) = witness.as_mut().poll(context)
        {
            *self = Self::Reached(point);
            return true;
        }
        false
    }

    fn point(&self) -> Option<&CommandPause> {
        match self {
            Self::Waiting(_) => None,
            Self::Reached(point) => Some(point),
        }
    }
}

struct NodePauses {
    witnesses: Vec<PauseWitness>,
}

impl NodePauses {
    fn poll_reached(&mut self, context: &mut Context<'_>) -> bool {
        let mut advanced = false;
        for witness in &mut self.witnesses {
            advanced |= witness.poll_reached(context);
        }
        advanced
    }

    fn reached(&mut self) -> bool {
        poll_fn(|context| {
            self.poll_reached(context);
            Poll::Ready(
                self.witnesses
                    .iter()
                    .any(|witness| witness.point().is_some()),
            )
        })
        .now_or_never()
        .verified("sampling the owned command witnesses is immediately ready")
    }

    fn waiting(&self) -> bool {
        self.witnesses
            .iter()
            .any(|witness| matches!(witness, PauseWitness::Waiting(_)))
    }

    fn reply(&self, node: &str, outcome: CommandOutcome) -> ProbeReply {
        let eligible = self
            .witnesses
            .iter()
            .filter_map(PauseWitness::point)
            .find(|point| point.observes(&outcome));
        let observed = eligible.is_some();
        if let Some(point) =
            eligible.or_else(|| self.witnesses.iter().find_map(PauseWitness::point))
        {
            tracing::info!(
                node,
                reference = %outcome.execution_reference,
                stage = ?point,
                disposition = ?outcome.disposition,
                observed,
                "command reply compared with its node's retained pause"
            );
        }
        if observed {
            ProbeReply::AfterBoundary(outcome)
        } else {
            ProbeReply::BeforeBoundary(outcome)
        }
    }

    async fn observe<F>(&mut self, node: &str, request: F) -> std::io::Result<ProbeReply>
    where
        F: Future<Output = std::io::Result<CommandOutcome>>,
    {
        self.reached();
        if !self.waiting() {
            return request.await.map(|outcome| self.reply(node, outcome));
        }
        let mut request = Box::pin(request);
        loop {
            let outcome = nervix_primitives::select! {
                biased;
                outcome = &mut request => Some(outcome),
                () = poll_fn(|context| {
                    if self.poll_reached(context) {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }) => None,
            };
            if let Some(outcome) = outcome {
                return outcome.map(|outcome| self.reply(node, outcome));
            }
            if !self.waiting() {
                return request.await.map(|outcome| self.reply(node, outcome));
            }
        }
    }
}

enum ProbeBoundary {
    Pauses(HashMap<ClusterNodeName, NodePauses>),
    Refusal {
        faults: FaultInjection,
        armed: ArmedRefusal,
        reached: bool,
    },
}

pub(crate) struct CommandProbe {
    boundary: ProbeBoundary,
}

impl CommandProbe {
    pub(crate) fn pauses(
        waits: HashMap<ClusterNodeName, Vec<BoxFuture<'static, CommandPause>>>,
    ) -> Self {
        let witnesses = waits
            .into_iter()
            .map(|(node, waits)| {
                (
                    node,
                    NodePauses {
                        witnesses: waits.into_iter().map(PauseWitness::Waiting).collect(),
                    },
                )
            })
            .collect();
        Self {
            boundary: ProbeBoundary::Pauses(witnesses),
        }
    }

    /// A pause must precede a reply eligible for its stage on its node. A reached witness is
    /// retained for later waiters, which still send their request and wait for its complete reply.
    pub(crate) async fn observe<F>(
        &mut self,
        node: Option<&str>,
        request: F,
    ) -> std::io::Result<ProbeReply>
    where
        F: Future<Output = std::io::Result<CommandOutcome>>,
    {
        match &mut self.boundary {
            ProbeBoundary::Pauses(witnesses) => {
                let Some(node) = node else {
                    return request.await.map(ProbeReply::BeforeBoundary);
                };
                let Some(witness) = witnesses.get_mut(node) else {
                    return request.await.map(ProbeReply::BeforeBoundary);
                };
                witness.observe(node, request).await
            }
            ProbeBoundary::Refusal {
                faults,
                armed,
                reached,
            } => {
                let outcome = request.await?;
                let delivered = faults
                    .command_write_refused_as_lapsed_lease(&armed.reference, armed.phase.into())
                    .await;
                *reached |= delivered;
                if delivered && armed.phase.matches_reply(&outcome) {
                    Ok(ProbeReply::AfterBoundary(outcome))
                } else {
                    Ok(ProbeReply::BeforeBoundary(outcome))
                }
            }
        }
    }

    /// Poll before sending the first attempt, so every armed stage retains its exact barrier lifetime.
    /// The witnesses remain reached after a later step releases or rearms their pause points.
    pub(crate) async fn reached(&mut self) -> bool {
        match &mut self.boundary {
            ProbeBoundary::Pauses(witnesses) => {
                let mut reached = false;
                for witness in witnesses.values_mut() {
                    reached |= witness.reached();
                }
                reached
            }
            ProbeBoundary::Refusal {
                faults,
                armed,
                reached,
            } => {
                if !*reached {
                    *reached = faults
                        .command_write_refused_as_lapsed_lease(&armed.reference, armed.phase.into())
                        .await;
                }
                *reached
            }
        }
    }
}
