//! Transaction tombstone retention tests.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Reclamation assertions against applying and terminal command executions.
//! - **Depends on.** The production consensus state machine and current transaction records.
//! - **Must not know.** Sessions, transport retries or runtime execution.

use std::{collections::BTreeSet, time::Duration};

use meticulous::ResultExt as _;
use nervix_models::{CommandExecutionReference, DomainName, Timestamp, UserName};

use crate::{
    CommandExecution, CommandExecutionAdmissionPolicy, CommandExecutionDisposition,
    CommandExecutionEffect, CommandExecutionResult, CommandExecutionTransactionRequest,
    CommandExecutionTransactionTarget, ConsensusCommand, ConsensusResponse, ReplicatedTransaction,
    StateMachineData, TransactionActivity, apply_consensus_command,
};

struct RetentionFixture {
    state: StateMachineData,
    reference: CommandExecutionReference,
    owner: UserName,
}

impl RetentionFixture {
    fn new(effect: CommandExecutionEffect) -> Self {
        let reference = CommandExecutionReference::parse("018bcfe5-6800-7000-8000-000000000001")
            .assured("the fixture reference is a UUIDv7");
        let owner = UserName::parse("app_user")
            .assured("the fixture owner is an identifier-shaped literal");
        let policy = CommandExecutionAdmissionPolicy::at(
            Timestamp::from_unix_nanos(1_700_000_010_000_000_000),
            Duration::from_secs(60),
            8,
        );
        let mut state = StateMachineData::default();
        let execution = CommandExecution::applying(
            reference.clone(),
            owner.clone(),
            None,
            [1; 32],
            Timestamp::from_unix_nanos(1),
            effect,
        );
        let result = apply_consensus_command(
            &mut state,
            &ConsensusCommand::AdmitCommandExecution {
                execution: Box::new(execution),
                mutation_domains: BTreeSet::new(),
                policy,
            },
        );
        assert_eq!(result.response, ConsensusResponse::Applied);
        Self {
            state,
            reference,
            owner,
        }
    }

    fn add_finished_transaction(&mut self, id: &str, command: Option<CommandExecutionReference>) {
        let domain = DomainName::parse("tenant")
            .assured("the fixture domain is an identifier-shaped literal");
        let activity = TransactionActivity::from_timeout(
            Timestamp::from_unix_nanos(1),
            Duration::from_secs(60),
        );
        let mut transaction = match command {
            Some(reference) => ReplicatedTransaction::open_for_command(
                id.to_string(),
                domain,
                self.owner.clone(),
                activity,
                reference,
            ),
            None => {
                ReplicatedTransaction::open(id.to_string(), domain, self.owner.clone(), activity)
            }
        };
        transaction
            .revert(&self.owner, activity, 1)
            .assured("the fixture transaction is open and belongs to this owner");
        self.state.transactions.insert(id.to_string(), transaction);
    }

    fn remove_due(&mut self) {
        apply_consensus_command(
            &mut self.state,
            &ConsensusCommand::RemoveFinishedTransactions {
                finished_before: Timestamp::from_unix_nanos(10),
            },
        );
    }

    fn finish_execution(&mut self) {
        let result = apply_consensus_command(
            &mut self.state,
            &ConsensusCommand::FinishCommandExecution {
                reference: self.reference.clone(),
                owner: self.owner.clone(),
                request_digest: [1; 32],
                at: Timestamp::from_unix_nanos(11),
                result: Box::new(CommandExecutionResult {
                    disposition: CommandExecutionDisposition::Completed {
                        already_existed: false,
                    },
                    message: "completed".to_string(),
                    diagnostics: Vec::new(),
                    statements: Vec::new(),
                    transaction: None,
                    transaction_admission: None,
                    backup: None,
                    restore: None,
                }),
            },
        );
        assert_eq!(result.response, ConsensusResponse::Applied);
    }
}

#[test]
fn applying_transaction_requests_retain_their_finished_target_until_finalization() {
    let activity =
        TransactionActivity::from_timeout(Timestamp::from_unix_nanos(1), Duration::from_secs(60));
    for target in [
        CommandExecutionTransactionTarget::New {
            id: "target".to_string(),
            activity,
        },
        CommandExecutionTransactionTarget::Existing {
            id: "target".to_string(),
            activity,
        },
    ] {
        let mut fixture = RetentionFixture::new(CommandExecutionEffect::TransactionRequest(
            Box::new(CommandExecutionTransactionRequest {
                target,
                operations: Vec::new(),
            }),
        ));
        fixture.add_finished_transaction("target", None);
        fixture.add_finished_transaction("unrelated", None);
        assert_eq!(
            fixture
                .state
                .finished_transaction_removals(Timestamp::from_unix_nanos(10)),
            ["unrelated"]
        );
        fixture.remove_due();
        assert_eq!(fixture.state.transactions.len(), 1);
        assert!(fixture.state.transactions.contains_key("target"));
        assert!(
            fixture
                .state
                .finished_transaction_removals(Timestamp::from_unix_nanos(10))
                .is_empty()
        );

        fixture.finish_execution();
        assert_eq!(
            fixture
                .state
                .finished_transaction_removals(Timestamp::from_unix_nanos(10)),
            ["target"]
        );
        fixture.remove_due();
        assert_eq!(fixture.state.transactions.len(), 0);
    }
}

#[test]
fn applying_commands_retain_their_internal_transaction_and_refresh_attempts() {
    let mut fixture = RetentionFixture::new(CommandExecutionEffect::Transaction {
        transaction_id: "internal-attempt".to_string(),
        source: "STOP".to_string(),
        statement: Box::new(nervix_models::Statement::StopDomain(
            nervix_models::StopDomain,
        )),
    });
    fixture.add_finished_transaction("internal-attempt", Some(fixture.reference.clone()));
    fixture.add_finished_transaction("planning-refresh", Some(fixture.reference.clone()));
    fixture.add_finished_transaction("unrelated", None);
    fixture.remove_due();
    assert_eq!(fixture.state.transactions.len(), 2);
    assert!(fixture.state.transactions.contains_key("internal-attempt"));
    assert!(fixture.state.transactions.contains_key("planning-refresh"));
    assert!(
        fixture
            .state
            .finished_transaction_removals(Timestamp::from_unix_nanos(10))
            .is_empty()
    );

    fixture.finish_execution();
    fixture.remove_due();
    assert_eq!(fixture.state.transactions.len(), 0);
}
