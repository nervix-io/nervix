//! Cross-build signature vectors: one compact JWS of every licensing document type signed by the
//! ordinary AWS-LC build and one signed by its FIPS build, which every build verifies through this
//! crate's own verification (licensing specification §17.6).
//!
//! AWS-LC's ECDSA signatures are randomized, so each build's vectors are produced once and
//! committed: `just signature-vectors ordinary` and `just signature-vectors fips` rewrite one
//! build's set through [`write_the_signature_vectors`]. Nervix CI verifies the FIPS build's vectors
//! on the ordinary module, and the licensing service's FIPS lane the ordinary build's vectors on the
//! FIPS module. The documents name only fixed keys and literal values, never the shared fixture
//! documents, conditions or clock, so the vectors change only when a signed document's shape does.

use std::{collections::BTreeMap, num::NonZeroUsize};

use chrono::{DateTime, TimeDelta, Utc};
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_crypto::{
    jws,
    provider::{ModuleMode, module_mode},
};
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::{
    conditions::*,
    document::*,
    documents::*,
    requests::*,
    test_fixture::{id, limits},
    trust::*,
};

/// The keys every build's vectors are signed with. Public test-only keys: never use them outside
/// these interoperability fixtures.
struct VectorKeys {
    /// The embedded epoch-0 root, which signs the succession to epoch 1.
    root: DocumentSigner,
    /// The epoch-1 root the succession installs, which signs the authorization and revocation.
    successor_root: DocumentSigner,
    /// The authorized working key, which signs the decision.
    working: DocumentSigner,
    /// The working key the revocation names.
    retired_working: DocumentSigner,
    /// The cluster key, which signs the request carrying it and the stream ticket.
    cluster: DocumentSigner,
}

impl VectorKeys {
    fn load() -> Self {
        Self {
            root: Self::signer(include_bytes!("test_data/signature_vectors/root.pk8")),
            successor_root: Self::signer(include_bytes!(
                "test_data/signature_vectors/successor_root.pk8"
            )),
            working: Self::signer(include_bytes!("test_data/signature_vectors/working.pk8")),
            retired_working: Self::signer(include_bytes!(
                "test_data/signature_vectors/retired_working.pk8"
            )),
            cluster: Self::signer(include_bytes!("test_data/signature_vectors/cluster.pk8")),
        }
    }

    fn signer(pkcs8: &[u8]) -> DocumentSigner {
        DocumentSigner::from_pkcs8(pkcs8).assured("the committed test keys are P-256 PKCS#8 keys")
    }

    /// The anchors the vectors' trust chain starts from: the root alone, with a threshold of one,
    /// so that a single compact JWS carries each trust document.
    fn embedded(&self) -> EmbeddedTrustBundle {
        EmbeddedTrustBundle {
            epochs: vec![RootSet {
                epoch: 0,
                roots: vec![self.root.public_key().clone()],
                threshold: NonZeroUsize::MIN,
            }],
            successions: Vec::new(),
        }
    }

    fn succession(&self) -> RootSuccession {
        RootSuccession {
            doc: SuccessionKind::RootSuccession,
            protocol: 1,
            serial: 1,
            issued_at: time(-7_200),
            from_epoch: 0,
            to_epoch: 1,
            roots: vec![self.successor_root.public_key().clone()],
            threshold: NonZeroUsize::MIN,
            effective_at: time(-7_200),
        }
    }

    fn authorization(&self) -> WorkingKeyAuthorization {
        WorkingKeyAuthorization {
            doc: AuthorizationKind::WorkingKeyAuthorization,
            protocol: 1,
            serial: 2,
            issued_at: time(-3_600),
            root_epoch: 1,
            kid: self.working.key_id().clone(),
            public_jwk: self.working.public_key().clone(),
            uses: vec![WorkingKeyUse::Decision],
            not_before: time(-3_600),
            not_after: time(7_776_000),
        }
    }

    fn revocation(&self) -> WorkingKeyRevocation {
        WorkingKeyRevocation {
            doc: RevocationKind::WorkingKeyRevocation,
            protocol: 1,
            serial: 3,
            issued_at: time(-1_800),
            root_epoch: 1,
            kid: self.retired_working.key_id().clone(),
            revoked_at: time(-1_800),
            reason: "superseded by a new working key".to_owned(),
        }
    }

    fn decision(&self) -> Decision {
        Decision {
            doc: DecisionKind::LicenseDecision,
            protocol: 1,
            decision_id: id(10),
            decision_revision: 3,
            issued_at: time(0),
            service: service(),
            activation: Activation {
                activation_id: id(1),
                cluster_uid: id(2),
                cluster_key_kid: self.cluster.key_id().clone(),
                generation: 1,
                kind: ActivationKind::Persistent,
                created_at: time(-600),
                ephemeral_expires_at: None,
            },
            license: License {
                license_id: id(3),
                organization_id: id(4),
                term_starts_at: time(-86_400),
                term_ends_at: time(31_536_000),
                plan: PlanVersion {
                    plan_code: "signature-vectors".to_owned(),
                    version: 2,
                },
                assignment_revision: 4,
            },
            status: DecisionStatus::Active,
            conditions: DecisionConditions {
                conditions_revision: 6,
                usage: UsageConditions {
                    capabilities: BTreeMap::from([(
                        "vectors.capability".to_owned(),
                        ConditionValue::Known(Capability::Enabled),
                    )]),
                    quotas: BTreeMap::from([(
                        "cluster.nodes".to_owned(),
                        ConditionValue::Known(Quota::Bounded { value: 5 }),
                    )]),
                },
                operating: OperatingConditions {
                    quotas: BTreeMap::from([(
                        "license.activations.persistent".to_owned(),
                        ConditionValue::Known(Quota::Unlimited),
                    )]),
                    durations: BTreeMap::from([(
                        "refresh.interval".to_owned(),
                        ConditionValue::Known(ConditionDuration::Bounded { seconds: 60 }),
                    )]),
                    policies: BTreeMap::from([(
                        "offline.on_lapse".to_owned(),
                        ConditionValue::Known(OfflineLapsePolicy::Freeze),
                    )]),
                    sets: BTreeMap::from([(
                        "activation.kinds".to_owned(),
                        ConditionValue::Known(vec![
                            ActivationKind::Persistent,
                            ActivationKind::Ephemeral,
                        ]),
                    )]),
                },
            },
            timing: DecisionTiming {
                refresh_after: time(60),
                overdue_at: time(180),
                valid_until: Some(time(3_600)),
                term_ends_at: time(31_536_000),
                ephemeral_expires_at: None,
            },
            transitions: vec![Transition {
                transition_id: id(20),
                from_conditions_revision: 5,
                to_conditions_revision: 6,
                started_at: time(0),
                deadline: time(900),
                trigger: TransitionTrigger::Decision {
                    decision_id: id(10),
                },
                reason: TransitionReason::PlanChanged,
                affected: vec![AffectedQuota {
                    condition: "cluster.nodes".to_owned(),
                    previous: Quota::Unlimited,
                    target: Quota::Bounded { value: 5 },
                    usage: 7,
                }],
            }],
            notices: vec![Notice {
                code: "clock_skew_observed".to_owned(),
                params: BTreeMap::from([("seconds".to_owned(), serde_json::json!(2))]),
            }],
            in_reply_to: id(40),
        }
    }

    fn common(&self, request_id: Uuid) -> RequestCommon {
        RequestCommon {
            protocol: 1,
            request_id,
            challenge: Secret::new("signature-vector-challenge".to_owned()),
            sent_at: time(-5),
            aud: service(),
            cluster_uid: id(2),
            display_name: "signature vectors λ".to_owned(),
            client: ClientDescription {
                product: ClientProduct::NervixServer,
                version: "0.1.0".to_owned(),
            },
            key_generation: 1,
            trust_held: TrustHeld {
                working_keys: vec![self.working.key_id().clone()],
                revocation_serials: vec![3],
                succession_serials: vec![1],
            },
        }
    }

    /// An activation creation, self-signed by the cluster key it carries.
    fn request(&self) -> Request {
        Request::Create {
            common: self.common(id(40)),
            license_key: Secret::new("NVXK-VECTORS-test-only".to_owned()),
            kind: ActivationKind::Persistent,
            cluster_key: self.cluster.public_key().clone(),
        }
    }

    fn stream_ticket(&self) -> StreamTicket {
        StreamTicket {
            common: self.common(id(41)),
            activation_id: id(1),
            state: CommittedState {
                applied_decision_revision: 3,
                applied_conditions_revision: 6,
                licensing_time: time(10),
            },
        }
    }

    /// Signs every document on the linked module.
    fn sign(&self) -> SignatureVectors {
        SignatureVectors {
            decision: self
                .working
                .sign(&self.decision(), limits())
                .assured("the bounded vector decision signs"),
            request: self
                .cluster
                .sign(&self.request(), limits())
                .assured("the bounded vector request signs"),
            stream_ticket: self
                .cluster
                .sign(&self.stream_ticket(), limits())
                .assured("the bounded vector stream ticket signs"),
            working_key: self
                .successor_root
                .sign(&self.authorization(), limits())
                .assured("the bounded vector authorization signs"),
            revocation: self
                .successor_root
                .sign(&self.revocation(), limits())
                .assured("the bounded vector revocation signs"),
            succession: self
                .root
                .sign(&self.succession(), limits())
                .assured("the bounded vector succession signs"),
        }
    }
}

fn service() -> Url {
    Url::parse("https://licensing.nervix.io").assured("a fixed service URL")
}

/// The vectors' documents are issued at one fixed instant; every other time is an offset from it.
fn time(offset_seconds: i64) -> DateTime<Utc> {
    let issued_at = DateTime::parse_from_rfc3339("2026-11-09T12:00:00Z")
        .assured("a fixed RFC 3339 instant")
        .with_timezone(&Utc);
    let offset =
        TimeDelta::try_seconds(offset_seconds).assured("vector offsets fit chrono seconds");
    issued_at
        .checked_add_signed(offset)
        .assured("vector offsets stay within a year of November 2026")
}

/// One build's vectors: a compact JWS of every licensing document type, under its `typ`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SignatureVectors {
    #[serde(rename = "nervix.licensing.decision")]
    decision: String,
    #[serde(rename = "nervix.licensing.request")]
    request: String,
    #[serde(rename = "nervix.licensing.stream-ticket")]
    stream_ticket: String,
    #[serde(rename = "nervix.licensing.trust.working-key")]
    working_key: String,
    #[serde(rename = "nervix.licensing.trust.revocation")]
    revocation: String,
    #[serde(rename = "nervix.licensing.trust.succession")]
    succession: String,
}

impl SignatureVectors {
    /// The vectors `module`'s build signed.
    fn committed(module: ModuleMode) -> Self {
        let json = match module {
            ModuleMode::Ordinary => include_str!("test_data/signature_vectors/ordinary.json"),
            ModuleMode::Fips => include_str!("test_data/signature_vectors/fips.json"),
        };
        serde_json::from_str(json).assured("the committed vectors are one complete set")
    }

    fn path(module: ModuleMode) -> &'static str {
        match module {
            ModuleMode::Ordinary => concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/test_data/signature_vectors/ordinary.json"
            ),
            ModuleMode::Fips => concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/test_data/signature_vectors/fips.json"
            ),
        }
    }

    fn compacts(&self) -> [&str; 6] {
        [
            &self.decision,
            &self.request,
            &self.stream_ticket,
            &self.working_key,
            &self.revocation,
            &self.succession,
        ]
    }

    /// The trust documents as a licensing service supplies them, each one compact JWS.
    fn trust_documents(
        succession: String,
        working_key: String,
        revocation: String,
    ) -> TrustDocuments {
        TrustDocuments {
            documents: vec![
                SignedTrustDocument::Compact(succession),
                SignedTrustDocument::Compact(working_key),
                SignedTrustDocument::Compact(revocation),
            ],
        }
    }

    fn trust(&self, keys: &VectorKeys) -> TrustState {
        let documents = Self::trust_documents(
            self.succession.clone(),
            self.working_key.clone(),
            self.revocation.clone(),
        );
        TrustState::build(&keys.embedded(), &documents, limits())
            .assured("the vectors' trust documents verify from the embedded root")
    }

    /// Verifies every vector the way a receiver does, and checks what each one decodes to: the
    /// trust documents through the whole chain from the embedded root, the decision through the
    /// working key that chain authorizes, the creation request under the cluster key it carries
    /// and the stream ticket under the cluster key of its activation.
    fn verify(&self, keys: &VectorKeys) {
        let trust = self.trust(keys);
        let mut contents = Vec::new();
        for document in trust.documents() {
            contents.push(document.content().clone());
        }
        assert_eq!(
            contents,
            vec![
                TrustDocumentContent::Succession(keys.succession()),
                TrustDocumentContent::Authorization(keys.authorization()),
                TrustDocumentContent::Revocation(keys.revocation()),
            ]
        );
        let decision = trust
            .verify_decision(&self.decision, DecisionVerification::Apply, limits())
            .assured("the vector decision verifies under its authorized working key");
        assert_eq!(decision.decision(), &keys.decision());
        assert!(!decision.signing_key_revoked());
        let request = Request::verify(&self.request, RequestSigner::CarriedClusterKey, limits())
            .assured("the vector request verifies under the cluster key it carries");
        assert_eq!(request, keys.request());
        let ticket: StreamTicket = verify(&self.stream_ticket, keys.cluster.public_key(), limits())
            .assured("the vector stream ticket verifies under the cluster key");
        assert_eq!(ticket, keys.stream_ticket());
    }

    /// Changes one signature byte of each vector in turn and checks that the same verification
    /// refuses it for its signature.
    fn refuse_tampered(&self, keys: &VectorKeys) {
        let embedded = keys.embedded();
        for documents in [
            Self::trust_documents(
                tampered(&self.succession),
                self.working_key.clone(),
                self.revocation.clone(),
            ),
            Self::trust_documents(
                self.succession.clone(),
                tampered(&self.working_key),
                self.revocation.clone(),
            ),
            Self::trust_documents(
                self.succession.clone(),
                self.working_key.clone(),
                tampered(&self.revocation),
            ),
        ] {
            let error = TrustState::build(&embedded, &documents, limits())
                .err()
                .assured("a trust document with a changed signature byte does not verify");
            assert!(matches!(
                error.current_context(),
                TrustError::Threshold {
                    verified: 0,
                    required: 1
                }
            ));
            assert_signature_refused(&error);
        }
        let error = self
            .trust(keys)
            .verify_decision(
                &tampered(&self.decision),
                DecisionVerification::Apply,
                limits(),
            )
            .err()
            .assured("a decision with a changed signature byte does not verify");
        assert!(matches!(error.current_context(), TrustError::Document));
        assert_signature_refused(&error);
        let error = Request::verify(
            &tampered(&self.request),
            RequestSigner::CarriedClusterKey,
            limits(),
        )
        .err()
        .assured("a request with a changed signature byte does not verify");
        assert_eq!(
            error.current_context(),
            &RequestVerificationError::Signature
        );
        assert_signature_refused(&error);
        let error = verify::<StreamTicket>(
            &tampered(&self.stream_ticket),
            keys.cluster.public_key(),
            limits(),
        )
        .err()
        .assured("a stream ticket with a changed signature byte does not verify");
        assert!(matches!(
            error.current_context(),
            DocumentError::Verification
        ));
        assert_signature_refused(&error);
    }
}

/// `compact` with the lowest bit of its last signature byte flipped, reframed by the maintained
/// compact codec, which keeps the protected header and payload segments as they were.
fn tampered(compact: &str) -> String {
    let mut framing: jose_jws::Flattened =
        compact.parse().assured("committed vectors are compact JWS");
    let last = framing
        .signature
        .signature
        .last_mut()
        .assured("an ES256 signature holds 64 bytes");
    *last ^= 1;
    framing.to_string()
}

/// A refused vector reports the document's signature failure with the maintained verifier's
/// typed refusal of the signature beneath it.
fn assert_signature_refused<C>(report: &Report<C>) {
    assert!(
        matches!(
            report.downcast_ref::<DocumentError>(),
            Some(DocumentError::Verification)
        ),
        "{report:?}"
    );
    let source = report
        .downcast_ref::<jws::LibraryError>()
        .assured("the verifier's refusal stays beneath the document context");
    assert_eq!(source.kind(), &jws::LibraryErrorKind::InvalidSignature);
}

#[test]
fn the_ordinary_builds_vectors_verify_on_this_build() {
    let keys = VectorKeys::load();
    let vectors = SignatureVectors::committed(ModuleMode::Ordinary);
    vectors.verify(&keys);
    vectors.refuse_tampered(&keys);
}

#[test]
fn the_fips_builds_vectors_verify_on_this_build() {
    let keys = VectorKeys::load();
    let vectors = SignatureVectors::committed(ModuleMode::Fips);
    vectors.verify(&keys);
    vectors.refuse_tampered(&keys);
}

#[test]
fn each_build_signed_its_own_vectors() {
    let ordinary = SignatureVectors::committed(ModuleMode::Ordinary);
    let fips = SignatureVectors::committed(ModuleMode::Fips);
    for (ordinary, fips) in ordinary.compacts().into_iter().zip(fips.compacts()) {
        assert_ne!(
            ordinary, fips,
            "randomized signatures differ when each build signs on its own"
        );
    }
}

/// Rewrites the linked module's vectors. `just signature-vectors <build>` runs it.
#[test]
#[ignore = "writes src/test_data/signature_vectors/<build>.json for the linked AWS-LC module"]
fn write_the_signature_vectors() {
    let keys = VectorKeys::load();
    let vectors = keys.sign();
    vectors.verify(&keys);
    let mut json = serde_json::to_string_pretty(&vectors).assured("the vectors serialize");
    json.push('\n');
    std::fs::write(SignatureVectors::path(module_mode()), json)
        .assured("the test data directory is writable in a checkout");
}
