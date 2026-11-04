//! Controlled uncertainty in replies to an already executed session command.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** A domain/query's three uncertain replies and its four observed execution identities.
//! - **Depends on.** Public session outcomes, vocabulary identities and the shared test injector.
//! - **Must not know.** Browser state, client retries, consensus implementation or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "owns bounded testing-only command reply substitution"
    )
)]

use nervix_client_wire::{CommandDisposition, CommandOutcome, CommandRequest, UnknownOutcomeCause};
use nervix_models::{CommandExecutionReference, DomainName};

use super::FaultInjection;

#[derive(Debug)]
pub(super) struct CommandReplyScript {
    query: String,
    references: Vec<CommandExecutionReference>,
}

impl FaultInjection {
    /// Replaces three successful replies for exactly this domain and query with the three
    /// current uncertainty causes. Execution and its stored outcome are left intact.
    pub fn make_command_replies_uncertain(&self, domain: DomainName, query: String) {
        self.inner.command_reply_scripts.insert(
            domain,
            CommandReplyScript {
                query,
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

    /// Applies the armed script only to a successful reply for its exact domain/query. Records
    /// at most four references, so further commands cannot grow the retained observation.
    pub(crate) fn substitute_uncertain_command_reply(
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
        let cause = match script.references.len() {
            0 => UnknownOutcomeCause::LeadershipLost,
            1 => UnknownOutcomeCause::StillApplying,
            2 => UnknownOutcomeCause::NotYetAuthoritative,
            3 => {
                script.references.push(request.execution_reference.clone());
                return;
            }
            _ => return,
        };
        script.references.push(request.execution_reference.clone());
        outcome.disposition = CommandDisposition::OutcomeUnknown(cause);
        outcome.message =
            "the testing peer has not made the command outcome authoritative yet".to_string();
        outcome.statements.clear();
    }
}
