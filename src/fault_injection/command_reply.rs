//! Controlled substitutions in replies to an already executed session command.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** A domain/query's scripted replies, three uncertain ones or one naming another
//!   execution, and the execution identities the script observed.
//! - **Depends on.** Public session outcomes, vocabulary identities and the shared test injector.
//! - **Must not know.** Browser state, client retries, consensus implementation or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "owns bounded testing-only command reply substitution"
    )
)]

use meticulous::ResultExt as _;
use nervix_client_wire::{
    CommandDisposition, CommandOutcome, CommandRequest, OutcomeOrigin, UnknownOutcomeCause,
};
use nervix_models::{CommandExecutionReference, DomainName};

use super::FaultInjection;

/// The message of the failure a foreign-execution script answers with, which belongs to no
/// command the scenario sent.
const FOREIGN_EXECUTION_MESSAGE: &str =
    "the testing peer answered with another execution's failure";

#[derive(Debug)]
pub(super) struct CommandReplyScript {
    query: String,
    substitution: CommandReplySubstitution,
    /// The references the command's successful replies answered, in order, bounded by the
    /// substitution.
    references: Vec<CommandExecutionReference>,
}

/// What a script puts in place of its command's successful replies.
#[derive(Debug)]
enum CommandReplySubstitution {
    /// The first three replies are uncertain, one per current cause; the fourth is the command's
    /// own.
    Uncertain,
    /// The first reply is a failure under another execution's reference; the second is the
    /// command's own.
    ForeignExecution { foreign: CommandExecutionReference },
}

/// What a foreign-execution script observed of its command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignExecutionObservation {
    /// The reference the substituted reply named instead of the command's.
    pub foreign: CommandExecutionReference,
    /// The references of the command's first two successful replies: the substituted one and the
    /// one that followed it.
    pub answered: Vec<CommandExecutionReference>,
}

impl FaultInjection {
    /// Replaces three successful replies for exactly this domain and query with the three
    /// current uncertainty causes. Execution and its stored outcome are left intact.
    pub fn make_command_replies_uncertain(&self, domain: DomainName, query: String) {
        self.inner.command_reply_scripts.insert(
            domain,
            CommandReplyScript {
                query,
                substitution: CommandReplySubstitution::Uncertain,
                references: Vec::new(),
            },
        );
    }

    /// The identities of the three substituted replies and the subsequent terminal reply.
    pub fn uncertain_command_references(
        &self,
        domain: &DomainName,
    ) -> Vec<CommandExecutionReference> {
        let Some(script) = self.inner.command_reply_scripts.get(domain) else {
            return Vec::new();
        };
        script.references.clone()
    }

    /// Answers the first successful reply for exactly this domain and query with a failure under
    /// another execution's reference, a fresh UUIDv7. Execution and its stored outcome are left
    /// intact, so the command's own outcome answers it when it is sent again.
    pub fn answer_command_with_foreign_execution(&self, domain: DomainName, query: String) {
        let foreign = CommandExecutionReference::parse(uuid::Uuid::now_v7().to_string())
            .assured("a hyphenated UUID is within the execution reference grammar");
        self.inner.command_reply_scripts.insert(
            domain,
            CommandReplyScript {
                query,
                substitution: CommandReplySubstitution::ForeignExecution { foreign },
                references: Vec::new(),
            },
        );
    }

    /// What the foreign-execution script armed for `domain` observed, or `None` while another
    /// script, or none, is armed there.
    pub fn foreign_execution_observation(
        &self,
        domain: &DomainName,
    ) -> Option<ForeignExecutionObservation> {
        let script = self.inner.command_reply_scripts.get(domain)?;
        let CommandReplySubstitution::ForeignExecution { foreign } = &script.substitution else {
            return None;
        };
        Some(ForeignExecutionObservation {
            foreign: foreign.clone(),
            answered: script.references.clone(),
        })
    }

    /// Applies the armed script only to a successful reply for its exact domain/query, recording
    /// only as many references as its substitution observes, so further commands cannot grow the
    /// retained observation.
    pub(crate) fn substitute_command_reply(
        &self,
        request: &CommandRequest,
        outcome: &mut CommandOutcome,
    ) {
        let Some(domain) = &request.domain else {
            return;
        };
        let Some(mut script) = self.inner.command_reply_scripts.get_mut(domain) else {
            return;
        };
        if script.query != request.query
            || !matches!(outcome.disposition, CommandDisposition::Completed { .. })
        {
            return;
        }
        script.substitute(request, outcome);
    }
}

impl CommandReplyScript {
    /// Substitutes the reply its command's successful attempts have reached.
    fn substitute(&mut self, request: &CommandRequest, outcome: &mut CommandOutcome) {
        match &self.substitution {
            CommandReplySubstitution::Uncertain => {
                let cause = match self.references.len() {
                    0 => UnknownOutcomeCause::LeadershipLost,
                    1 => UnknownOutcomeCause::StillApplying,
                    2 => UnknownOutcomeCause::NotYetAuthoritative,
                    3 => {
                        self.references.push(request.execution_reference.clone());
                        return;
                    }
                    _ => return,
                };
                self.references.push(request.execution_reference.clone());
                outcome.disposition = CommandDisposition::OutcomeUnknown(cause);
                outcome.message = "the testing peer has not made the command outcome \
                                   authoritative yet"
                    .to_string();
                outcome.statements.clear();
            }
            CommandReplySubstitution::ForeignExecution { foreign } => match self.references.len() {
                0 => {
                    let foreign = foreign.clone();
                    self.references.push(request.execution_reference.clone());
                    *outcome = Self::foreign_failure(foreign);
                }
                1 => self.references.push(request.execution_reference.clone()),
                _ => {}
            },
        }
    }

    /// The outcome of another execution that failed, as a server answers one: every field this
    /// command's own outcome carried is gone with it.
    fn foreign_failure(foreign: CommandExecutionReference) -> CommandOutcome {
        CommandOutcome {
            execution_reference: foreign,
            origin: OutcomeOrigin::Executed,
            disposition: CommandDisposition::Failed,
            message: FOREIGN_EXECUTION_MESSAGE.to_string(),
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
}
