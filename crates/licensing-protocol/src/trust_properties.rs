//! Bounded malformed-wire and threshold-chain checks with real signatures.

use meticulous::{OptionExt as _, ResultExt as _};

use crate::{
    document::*, documents::*, representation_properties::number, test_fixture::*, trust::*,
};

#[test]
fn bolero_jws_malformed() {
    let fixture = Fixture::new();
    let compact = fixture.sign(&fixture.decision());
    bolero::check!()
        .with_iterations(128)
        .with_max_len(256)
        .for_each(|input| {
            // Any framing/payload/header mutation must either refuse or verify the exact original
            // logical decision. Arbitrary bytes independently exercise bounded no-panic decoding.
            if let Ok(text) = std::str::from_utf8(input) {
                assert!(verify::<Decision>(text, fixture.working.public_key(), limits()).is_err());
            }
            let mut framing: jose_jws::Flattened = compact.parse().assured("maintained framing");
            let mut signature = framing.signature.signature.as_ref().to_vec();
            let index = usize::from(input.first().copied().unwrap_or(0)) % signature.len();
            signature[index] ^= 1;
            framing.signature.signature = signature.into();
            assert!(
                verify::<Decision>(&framing.to_string(), fixture.working.public_key(), limits())
                    .is_err()
            );
            let original: Decision = verify(&compact, fixture.working.public_key(), limits())
                .assured("original signature");
            assert_eq!(original, fixture.decision());
        });
}

#[test]
fn bolero_trust_chain() {
    let fixture = Fixture::new();
    bolero::check!()
        .with_iterations(128)
        .with_max_len(256)
        .for_each(|input| {
            let mut authorization = fixture.authorization();
            authorization.serial = number(input);
            let set = fixture.root_sign(&authorization);
            let documents = TrustDocuments {
                documents: vec![SignedTrustDocument::Signatures(set.clone())],
            };
            round_trip(&documents);
            let trust = TrustState::build(&fixture.embedded(), &documents, limits())
                .assured("distinct root threshold");
            let compact = fixture.sign(&fixture.decision());
            assert_eq!(
                trust
                    .verify_decision(&compact, DecisionVerification::Apply, limits())
                    .assured("authorized working key")
                    .decision(),
                &fixture.decision()
            );
            let mut insufficient = set;
            insufficient.signatures.pop();
            let documents = TrustDocuments {
                documents: vec![SignedTrustDocument::Signatures(insufficient.clone())],
            };
            assert!(TrustState::build(&fixture.embedded(), &documents, limits()).is_err());
            insufficient.signatures.push(
                insufficient
                    .signatures
                    .first()
                    .assured("one signature remains")
                    .clone(),
            );
            assert!(
                insufficient
                    .parse(DocumentType::WorkingKey, limits())
                    .is_err()
            );
            let mut revoked = trust.original_documents().clone();
            let revocation = WorkingKeyRevocation {
                doc: RevocationKind::WorkingKeyRevocation,
                protocol: 1,
                serial: number(input),
                issued_at: now(),
                root_epoch: 0,
                kid: fixture.working.key_id().clone(),
                revoked_at: now(),
                reason: "property".to_owned(),
            };
            revoked.documents.push(SignedTrustDocument::Signatures(
                fixture.root_sign(&revocation),
            ));
            let trust = TrustState::build(&fixture.embedded(), &revoked, limits())
                .assured("verified revocation");
            assert!(
                trust
                    .verify_decision(&compact, DecisionVerification::Apply, limits())
                    .is_err()
            );
            assert!(
                trust
                    .verify_decision(&compact, DecisionVerification::Reload, limits())
                    .assured("reload verifies and records revocation")
                    .signing_key_revoked()
            );
        });
}
