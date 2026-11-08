//! Exact fixed structures, original signing bytes and operation-specific request shapes.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use meticulous::ResultExt as _;

use crate::{document::*, documents::*, requests::*, test_fixture::*, trust::*};

#[test]
fn every_signed_document_and_response_round_trips_through_its_owner() {
    let fixture = Fixture::new();
    signed_round_trip(&fixture.working, &fixture.decision());
    signed_round_trip(&fixture.roots[0], &fixture.authorization());
    for request in fixture.requests() {
        signed_round_trip(&fixture.cluster, &request);
    }
    let ticket = StreamTicket {
        common: fixture.common(),
        activation_id: id(1),
        state: fixture.committed(),
    };
    signed_round_trip(&fixture.cluster, &ticket);
    let proof = KeyRotationProof {
        challenge: Secret::new("proof-challenge".to_owned()),
        cluster_uid: id(2),
        activation_id: id(1),
        new_kid: fixture.working.key_id().clone(),
    };
    signed_round_trip(&fixture.working, &proof);
    round_trip(&DecisionResponse {
        decision: fixture.sign(&fixture.decision()),
        trust: fixture.documents(),
        server_time: now(),
    });
    round_trip(&fixture.embedded());
    round_trip(&ChallengeRequest { cluster_uid: id(2) });
    round_trip(&ChallengeResponse {
        challenge: Secret::new("challenge".to_owned()),
        expires_at: at(60),
    });
}

#[test]
fn fixed_request_shapes_refuse_extras_and_partial_committed_state() {
    let fixture = Fixture::new();
    for request in fixture.requests() {
        let mut value = serde_json::to_value(&request).assured("current request");
        value["extra"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<Request>(value).is_err());
    }
    let request = Request::Refresh {
        common: fixture.common(),
        activation_id: id(1),
        state: RefreshState::Adoption,
        transitions: Vec::new(),
    };
    let mut value = serde_json::to_value(&request).assured("adoption request");
    value["applied_decision_revision"] = serde_json::json!(2);
    assert!(serde_json::from_value::<Request>(value).is_err());
    let secret = Secret::new("never-log-this-value".to_owned());
    assert!(!format!("{secret:?}").contains(secret.expose()));
}

#[test]
fn raw_header_profile_refuses_every_additional_member_even_null() {
    let fixture = Fixture::new();
    for name in [
        "jku",
        "jwk",
        "x5u",
        "x5c",
        "x5t",
        "x5t#S256",
        "crit",
        "b64",
        "unexpected",
    ] {
        let mut header = serde_json::json!({"alg":"ES256", "kid":fixture.working.key_id(), "typ":DocumentType::Decision});
        header[name] = serde_json::Value::Null;
        let header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).assured("header fixture"));
        let compact = format!("{header}.e30.AA");
        assert!(
            UnverifiedDocument::parse(&compact, DocumentType::Decision, limits()).is_err(),
            "{name}"
        );
    }
    let compact = fixture.sign(&fixture.decision());
    assert!(UnverifiedDocument::parse(&compact, DocumentType::Request, limits()).is_err());
    let mut short = limits();
    short.bytes = std::num::NonZeroUsize::MIN;
    assert!(UnverifiedDocument::parse(&compact, DocumentType::Decision, short).is_err());
    assert!(fixture.working.sign(&fixture.decision(), short).is_err());
}

#[test]
fn signature_is_checked_before_payload_json_and_original_input_is_not_reserialized() {
    let fixture = Fixture::new();
    let compact = fixture.sign(&fixture.decision());
    let mut framing: jose_jws::Flattened = compact.parse().assured("maintained decoder");
    framing.payload = Some(Vec::from(b"not JSON".as_slice()).into());
    let malformed = framing.to_string();
    let error = verify::<Decision>(&malformed, fixture.working.public_key(), limits())
        .err()
        .assured("tampered original bytes");
    assert!(matches!(
        error.current_context(),
        DocumentError::Verification
    ));
    assert!(error.contains::<nervix_crypto::jws::JwsError>());
    use meticulous::OptionExt as _;
    let source = error
        .downcast_ref::<nervix_crypto::jws::LibraryError>()
        .assured("native typed source retained");
    assert_eq!(
        source.kind(),
        &nervix_crypto::jws::LibraryErrorKind::InvalidSignature
    );
    // Reformatting an otherwise equal JSON payload also invalidates the received signature.
    let payload =
        serde_json::to_vec_pretty(&fixture.decision()).assured("same payload with whitespace");
    framing.payload = Some(payload.into());
    assert!(
        verify::<Decision>(&framing.to_string(), fixture.working.public_key(), limits()).is_err()
    );
}

#[test]
fn unprovisioned_and_inconsistent_trust_bundles_fail_closed() {
    let fixture = Fixture::new();
    assert!(PRODUCTION_TRUST_BUNDLE.is_none());
    let mut embedded = fixture.embedded();
    embedded.epochs.clear();
    assert!(TrustState::build(&embedded, &fixture.documents(), limits()).is_err());
    let mut embedded = fixture.embedded();
    embedded.epochs[0].roots[1] = embedded.epochs[0].roots[0].clone();
    assert!(TrustState::build(&embedded, &fixture.documents(), limits()).is_err());
    let mut authorization = fixture.authorization();
    authorization.kid = fixture.cluster.key_id().clone();
    let documents = TrustDocuments {
        documents: vec![SignedTrustDocument::Signatures(
            fixture.root_sign(&authorization),
        )],
    };
    assert!(TrustState::build(&fixture.embedded(), &documents, limits()).is_err());
}

#[test]
fn compact_single_root_trust_and_public_key_profiles_are_exact() {
    let fixture = Fixture::new();
    let mut embedded = fixture.embedded();
    embedded.epochs[0].threshold = std::num::NonZeroUsize::MIN;
    let compact = fixture.roots[0]
        .sign(&fixture.authorization(), limits())
        .assured("single-root authorization");
    let documents = TrustDocuments {
        documents: vec![SignedTrustDocument::Compact(compact)],
    };
    round_trip(&documents);
    assert!(TrustState::build(&embedded, &documents, limits()).is_ok());
    let key = fixture.working.public_key();
    round_trip(key);
    assert_eq!(key.coordinates().x.len(), 32);
    for field in ["d", "kid", "unexpected"] {
        let mut value = serde_json::to_value(key).assured("public JWK");
        value[field] = serde_json::json!("private-or-extra");
        assert!(serde_json::from_value::<crate::keys::PublicKey>(value).is_err());
    }
    for coordinate in ["", "AA", "AA=="] {
        let mut value = serde_json::to_value(key).assured("public JWK");
        value["x"] = serde_json::json!(coordinate);
        assert!(serde_json::from_value::<crate::keys::PublicKey>(value).is_err());
    }
}
