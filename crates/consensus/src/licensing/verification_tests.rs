//! Signed decision acceptance, monotone proposal ordering and recovery verification.
//!
//! Layer: test harness.
//! - **Owns.** Deterministic current-shape proofs through production licensing owners.
//! - **Depends on.** Real protocol signers, typed commands and production storage fixtures.
//! - **Must not know.** Service networking, clocks or runtime policy publication.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_licensing_protocol::{
    documents::{Decision, DecisionResponse},
    test_fixture::{self, Fixture},
};
use nervix_models::Timestamp;
use nervix_primitives::sync::Arc;

use super::*;
use crate::{
    ConsensusCommand, ConsensusConflict, ConsensusResponse, StateMachineData,
    apply_consensus_command, cluster_identity,
};

pub(crate) struct SignedFixture {
    pub(crate) signing: Fixture,
    pub(crate) identity: crate::ClusterIdentityRecord,
    pub(crate) environment: LicensingEnvironment,
    pub(crate) decision: Decision,
    documents: nervix_licensing_protocol::documents::TrustDocuments,
}
impl SignedFixture {
    pub(crate) fn new() -> Self {
        let signing = Fixture::new();
        let identity = cluster_identity::tests::candidate(0x11);
        let mut decision = signing.decision();
        decision.activation.cluster_uid = identity.public().cluster_uid;
        decision.activation.cluster_key_kid = identity.public().kid;
        let environment = LicensingEnvironment::testing(
            decision.service.clone(),
            signing.embedded(),
            test_fixture::limits().bytes,
        );
        let documents = signing.documents();
        Self {
            documents,
            signing,
            identity,
            environment,
            decision,
        }
    }
    pub(crate) fn received(
        &self,
        decision: &Decision,
        held: &[StoredTrustDocument],
    ) -> ReceivedLicenseDecision {
        ReceivedLicenseDecision::from_response(
            DecisionResponse {
                decision: self.signing.sign(decision),
                trust: self.documents.clone(),
                server_time: decision.issued_at,
            },
            Timestamp::try_from(decision.issued_at).assured("fixture timestamp fits"),
            decision.in_reply_to,
            held,
            &self.environment,
        )
        .assured("actual root signatures verify")
    }
    pub(crate) fn binding(&self, received: &ReceivedLicenseDecision) -> ActivationBinding {
        LicensingRecord::free(self.identity.public().created_at)
            .prepare_activation(
                &self.identity.public(),
                nervix_models::ActivationKind::Persistent,
                "TESTABCD".to_owned(),
                received,
                &self.environment,
            )
            .assured("fixture decision matches independent identity")
    }
    pub(super) fn state(&self) -> StateMachineData {
        let mut state = StateMachineData {
            licensing_environment: Arc::new(self.environment.clone()),
            ..StateMachineData::default()
        };
        cluster_identity::install(&mut state, &self.identity);
        state
    }
}

#[test]
fn activation_refresh_ordering_exact_replay_and_time_are_committed_together() {
    let fixture = SignedFixture::new();
    let mut state = fixture.state();
    let received = fixture.received(&fixture.decision, &[]);
    let binding = fixture.binding(&received);
    let initial = ConsensusCommand::ActivateLicense {
        binding: Box::new(binding),
        decision: Box::new(received.clone()),
    };
    assert_eq!(
        apply_consensus_command(&mut state, &initial).response,
        ConsensusResponse::Applied
    );
    assert!(
        state
            .cluster_identity
            .as_ref()
            .verified("identity retained")
            .public()
            .ever_activated
    );
    assert!(
        state
            .licensing_verification
            .as_ref()
            .verified("derived verification exists")
            .is_ok()
    );
    let saved = state.licensing.clone().verified("committed activation");
    let replay = ConsensusCommand::ApplyLicenseDecision {
        decision: Box::new(received),
        expected_revision: u64::MAX,
    };
    assert_eq!(
        apply_consensus_command(&mut state, &replay).response,
        ConsensusResponse::Applied
    );
    assert_eq!(
        state
            .licensing
            .as_ref()
            .verified("activation retained")
            .as_ref(),
        saved.as_ref()
    );
    let mut next = fixture.decision.clone();
    next.decision_revision += 1;
    next.decision_id = test_fixture::id(101);
    next.in_reply_to = test_fixture::id(11);
    let received = fixture.received(&next, &saved.trust);
    let stale = ConsensusCommand::ApplyLicenseDecision {
        decision: Box::new(received.clone()),
        expected_revision: 1,
    };
    assert_eq!(
        apply_consensus_command(&mut state, &stale).response,
        ConsensusResponse::Conflict(ConsensusConflict::LicenseDecision {
            reason: LicenseDecisionError::StaleDecision
        })
    );
    assert_eq!(
        state
            .licensing
            .as_ref()
            .verified("activation retained")
            .as_ref(),
        saved.as_ref()
    );
    let update = ConsensusCommand::ApplyLicenseDecision {
        decision: Box::new(received),
        expected_revision: fixture.decision.decision_revision,
    };
    assert_eq!(
        apply_consensus_command(&mut state, &update).response,
        ConsensusResponse::Applied
    );
    assert_eq!(
        apply_consensus_command(&mut state, &replay).response,
        ConsensusResponse::Conflict(ConsensusConflict::LicenseDecision {
            reason: LicenseDecisionError::StaleDecision
        })
    );
    let before = state
        .licensing
        .as_ref()
        .verified("activation retained")
        .licensing_time;
    let later = before
        .checked_add(std::time::Duration::from_secs(300))
        .assured("bounded time advance");
    assert_eq!(
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::AdvanceLicensingTime { observed_at: later }
        )
        .response,
        ConsensusResponse::Applied
    );
    assert_eq!(
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::AdvanceLicensingTime {
                observed_at: before
            }
        )
        .response,
        ConsensusResponse::Applied
    );
    assert_eq!(
        state
            .licensing
            .as_ref()
            .verified("activation retained")
            .licensing_time,
        later
    );
    assert_eq!(state.runtime_revision, 0);
}

#[test]
fn invalid_signed_binding_never_changes_committed_state_and_reload_is_semantic() {
    let fixture = SignedFixture::new();
    let mut state = fixture.state();
    let received = fixture.received(&fixture.decision, &[]);
    let binding = fixture.binding(&received);
    let mut wrong = fixture.decision.clone();
    wrong.activation.generation += 1;
    let rejected = ConsensusCommand::ActivateLicense {
        binding: Box::new(binding.clone()),
        decision: Box::new(fixture.received(&wrong, &[])),
    };
    let before = state.licensing.clone();
    assert!(matches!(
        apply_consensus_command(&mut state, &rejected).response,
        ConsensusResponse::Conflict(ConsensusConflict::LicenseDecision { .. })
    ));
    assert_eq!(state.licensing, before);
    let accepted = state
        .licensing
        .as_ref()
        .verified("free singleton")
        .accept(
            &fixture.identity.public(),
            &binding,
            &received,
            None,
            &fixture.environment,
        )
        .assured("valid current decision applies");
    let mut corrupt = accepted.clone();
    let LicensingMode::Activated(activation) = &mut corrupt.mode else {
        unreachable!()
    };
    activation.decision.bytes.push('!');
    assert!(corrupt.validate(&fixture.identity.public()).is_ok());
    assert!(
        corrupt
            .verify(&fixture.identity.public(), &fixture.environment)
            .verified("activated verification is present")
            .is_err()
    );
    assert!(
        accepted
            .verify(&fixture.identity.public(), &fixture.environment)
            .verified("verification present")
            .is_ok()
    );
}

#[test]
fn trust_installation_retains_a_revoked_committed_decision_and_refuses_new_signatures() {
    use nervix_licensing_protocol::{
        document::DocumentType,
        documents::{RevocationKind, WorkingKeyRevocation},
    };
    let fixture = SignedFixture::new();
    let mut state = fixture.state();
    let received = fixture.received(&fixture.decision, &[]);
    let binding = fixture.binding(&received);
    assert_eq!(
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::ActivateLicense {
                binding: Box::new(binding),
                decision: Box::new(received),
            }
        )
        .response,
        ConsensusResponse::Applied
    );
    let revocation = WorkingKeyRevocation {
        doc: RevocationKind::WorkingKeyRevocation,
        protocol: 1,
        serial: 7,
        root_epoch: 0,
        issued_at: test_fixture::now(),
        revoked_at: test_fixture::now(),
        kid: fixture.signing.working.key_id().clone(),
        reason: "fixture custody change".to_owned(),
    };
    let document = StoredTrustDocument {
        bytes: serde_json::to_vec(
            &nervix_licensing_protocol::documents::SignedTrustDocument::Signatures(
                fixture.signing.root_sign(&revocation),
            ),
        )
        .assured("signed trust serializes"),
        typ: DocumentType::Revocation,
        serial: revocation.serial,
        kid: Some(revocation.kid),
    };
    let install = ConsensusCommand::InstallTrustDocuments {
        documents: vec![document.clone()],
    };
    for _ in 0..2 {
        assert_eq!(
            apply_consensus_command(&mut state, &install).response,
            ConsensusResponse::Applied
        );
        assert!(
            state
                .licensing_verification
                .as_ref()
                .verified("derived verification")
                .as_ref()
                .assured("committed decision remains usable")
                .signing_key_revoked
        );
    }
    let saved = state.licensing.clone().verified("committed activation");
    let mut next = fixture.decision.clone();
    next.decision_revision += 1;
    next.decision_id = test_fixture::id(101);
    let update = ConsensusCommand::ApplyLicenseDecision {
        decision: Box::new(fixture.received(&next, &saved.trust)),
        expected_revision: fixture.decision.decision_revision,
    };
    assert!(matches!(
        apply_consensus_command(&mut state, &update).response,
        ConsensusResponse::Conflict(ConsensusConflict::LicenseDecision { .. })
    ));
    assert_eq!(state.licensing.as_ref(), Some(&saved));
    let mut altered = document;
    altered.bytes.push(b'!');
    assert!(matches!(
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::InstallTrustDocuments {
                documents: vec![altered]
            }
        )
        .response,
        ConsensusResponse::Conflict(ConsensusConflict::LicenseDecision { .. })
    ));
    assert_eq!(state.licensing.as_ref(), Some(&saved));
}
