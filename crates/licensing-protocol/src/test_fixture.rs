//! Current signed-document fixtures shared by rule and representation checks.

use std::{collections::BTreeMap, num::NonZeroUsize};

use chrono::{DateTime, TimeDelta, Utc};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_crypto::ecdsa::EcdsaP256KeyPair;
use serde::{Serialize, de::DeserializeOwned};
use url::Url;
use uuid::Uuid;

use crate::{conditions::*, document::*, documents::*, requests::*, trust::*};

pub fn limits() -> DocumentLimits {
    DocumentLimits {
        bytes: NonZeroUsize::new(65_536).assured("nonzero fixture bound"),
        signatures: NonZeroUsize::new(8).assured("nonzero fixture bound"),
        trust_documents: NonZeroUsize::new(64).assured("nonzero fixture bound"),
    }
}

pub fn now() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-11-06T12:00:00.123456789Z")
        .assured("fixed RFC 3339 fixture")
        .with_timezone(&Utc)
}

pub fn at(seconds: i64) -> DateTime<Utc> {
    let delta = TimeDelta::try_seconds(seconds).assured("fixture offsets fit chrono seconds");
    now()
        .checked_add_signed(delta)
        .assured("fixture offsets stay near November 2026")
}

pub fn id(value: u128) -> Uuid {
    uuid::Builder::from_u128(value)
        .with_variant(uuid::Variant::RFC4122)
        .with_version(uuid::Version::SortRand)
        .into_uuid()
}

pub fn signer() -> DocumentSigner {
    let private = EcdsaP256KeyPair::generate_pkcs8().assured("AWS-LC generates a fixture key");
    DocumentSigner::from_pkcs8(private.as_ref())
        .assured("the generated key has the P-256 PKCS#8 profile")
}

pub fn fixture_transition(trigger: TransitionTrigger) -> Transition {
    Transition {
        transition_id: id(90),
        from_conditions_revision: 4,
        to_conditions_revision: 5,
        started_at: now(),
        deadline: at(100),
        trigger,
        reason: TransitionReason::PlanChanged,
        affected: vec![AffectedQuota {
            condition: CLUSTER_NODES.to_owned(),
            previous: Quota::Unlimited,
            target: Quota::Bounded { value: 3 },
            usage: 7,
        }],
    }
}

pub fn empty_conditions() -> Conditions {
    Conditions {
        usage: UsageConditions {
            capabilities: BTreeMap::new(),
            quotas: BTreeMap::new(),
        },
        operating: OperatingConditions {
            quotas: BTreeMap::new(),
            durations: BTreeMap::new(),
            policies: BTreeMap::new(),
            sets: BTreeMap::new(),
        },
    }
}

pub fn plan() -> Conditions {
    let mut conditions = empty_conditions();
    conditions.usage = FREE_POLICY.usage();
    conditions.operating.quotas = BTreeMap::from([
        (
            PERSISTENT_ACTIVATIONS.to_owned(),
            ConditionValue::Known(Quota::Bounded { value: 3 }),
        ),
        (
            EPHEMERAL_ACTIVATIONS.to_owned(),
            ConditionValue::Known(Quota::Bounded { value: 2 }),
        ),
    ]);
    conditions.operating.durations = BTreeMap::from([
        (
            EPHEMERAL_LIFETIME.to_owned(),
            ConditionValue::Known(ConditionDuration::Bounded { seconds: 86_400 }),
        ),
        (
            REFRESH_INTERVAL.to_owned(),
            ConditionValue::Known(ConditionDuration::Bounded { seconds: 60 }),
        ),
        (
            REFRESH_OVERDUE.to_owned(),
            ConditionValue::Known(ConditionDuration::Bounded { seconds: 120 }),
        ),
        (
            OFFLINE_ALLOWANCE.to_owned(),
            ConditionValue::Known(ConditionDuration::Unbounded),
        ),
        (
            CAPABILITY_GRACE.to_owned(),
            ConditionValue::Known(ConditionDuration::Bounded { seconds: 300 }),
        ),
        (
            QUOTA_GRACE.to_owned(),
            ConditionValue::Known(ConditionDuration::Bounded { seconds: 900 }),
        ),
    ]);
    conditions.operating.policies.insert(
        OFFLINE_LAPSE.to_owned(),
        ConditionValue::Known(OfflineLapsePolicy::Freeze),
    );
    conditions.operating.sets.insert(
        ACTIVATION_KINDS.to_owned(),
        ConditionValue::Known(vec![ActivationKind::Persistent, ActivationKind::Ephemeral]),
    );
    conditions
}

pub fn resolution_facts() -> ResolutionFacts {
    ResolutionFacts {
        license_status: LicenseStatus::Active,
        term_starts_at: at(-3_600),
        term_ends_at: at(3_600),
        activation_status: ActivationStatus::Active,
        created_at: at(-300),
        lifetime: ActivationLifetime::Persistent,
        at: now(),
    }
}

pub struct Fixture {
    pub roots: [DocumentSigner; 3],
    pub working: DocumentSigner,
    pub cluster: DocumentSigner,
}

impl Fixture {
    pub fn new() -> Self {
        Self {
            roots: [signer(), signer(), signer()],
            working: signer(),
            cluster: signer(),
        }
    }

    pub fn embedded(&self) -> EmbeddedTrustBundle {
        EmbeddedTrustBundle {
            epochs: vec![RootSet {
                epoch: 0,
                roots: self
                    .roots
                    .iter()
                    .map(|signer| signer.public_key().clone())
                    .collect(),
                threshold: NonZeroUsize::new(2).assured("nonzero root threshold"),
            }],
            successions: Vec::new(),
        }
    }

    pub fn authorization(&self) -> WorkingKeyAuthorization {
        WorkingKeyAuthorization {
            doc: AuthorizationKind::WorkingKeyAuthorization,
            protocol: 1,
            serial: 1,
            issued_at: at(-120),
            root_epoch: 0,
            kid: self.working.key_id().clone(),
            public_jwk: self.working.public_key().clone(),
            uses: vec![WorkingKeyUse::Decision],
            not_before: at(-10_000),
            not_after: at(10_000),
        }
    }

    pub fn root_sign<T: Document>(&self, document: &T) -> SignatureSet {
        SignatureSet {
            signatures: vec![
                self.roots[0]
                    .sign(document, limits())
                    .assured("bounded current root document"),
                self.roots[1]
                    .sign(document, limits())
                    .assured("bounded current root document"),
            ],
        }
    }

    pub fn documents(&self) -> TrustDocuments {
        TrustDocuments {
            documents: vec![SignedTrustDocument::Signatures(
                self.root_sign(&self.authorization()),
            )],
        }
    }

    pub fn trust(&self) -> TrustState {
        TrustState::build(&self.embedded(), &self.documents(), limits())
            .assured("two distinct fixture roots authorize the working key")
    }

    pub fn decision(&self) -> Decision {
        let conditions = plan();
        Decision {
            doc: DecisionKind::LicenseDecision,
            protocol: 1,
            decision_id: id(100),
            decision_revision: 2,
            issued_at: now(),
            service: Url::parse("https://licensing.nervix.io").assured("fixed service URL"),
            activation: Activation {
                activation_id: id(1),
                cluster_uid: id(2),
                cluster_key_kid: self.cluster.key_id().clone(),
                generation: 1,
                kind: ActivationKind::Persistent,
                created_at: at(-300),
                ephemeral_expires_at: None,
            },
            license: License {
                license_id: id(3),
                organization_id: id(4),
                term_starts_at: at(-3_600),
                term_ends_at: at(3_600),
                plan: PlanVersion {
                    plan_code: "fixture".to_owned(),
                    version: 3,
                },
                assignment_revision: 7,
            },
            status: DecisionStatus::Active,
            conditions: DecisionConditions {
                conditions_revision: 5,
                usage: conditions.usage,
                operating: conditions.operating,
            },
            timing: DecisionTiming {
                refresh_after: at(60),
                overdue_at: at(180),
                valid_until: None,
                term_ends_at: at(3_600),
                ephemeral_expires_at: None,
            },
            transitions: Vec::new(),
            notices: vec![Notice {
                code: "clock_skew_observed".to_owned(),
                params: BTreeMap::from([("seconds".to_owned(), serde_json::json!(190))]),
            }],
            in_reply_to: id(10),
        }
    }

    pub fn sign(&self, decision: &Decision) -> String {
        self.working
            .sign(decision, limits())
            .assured("bounded current decision")
    }

    pub fn common(&self) -> RequestCommon {
        RequestCommon {
            protocol: 1,
            request_id: id(10),
            challenge: Secret::new("fixture-challenge".to_owned()),
            sent_at: now(),
            aud: Url::parse("https://licensing.nervix.io").assured("fixed service URL"),
            cluster_uid: id(2),
            display_name: "cluster λ".to_owned(),
            client: ClientDescription {
                product: ClientProduct::NervixServer,
                version: "0.1.0-dev".to_owned(),
            },
            key_generation: 1,
            trust_held: TrustHeld {
                working_keys: vec![self.working.key_id().clone()],
                revocation_serials: vec![2, 3],
                succession_serials: vec![4, 5],
            },
        }
    }

    pub fn committed(&self) -> CommittedState {
        CommittedState {
            applied_decision_revision: 2,
            applied_conditions_revision: 5,
            licensing_time: now(),
        }
    }

    pub fn requests(&self) -> Vec<Request> {
        vec![
            Request::Create {
                common: self.common(),
                license_key: Secret::new("NVXK-ABCDEFGH-fixture-secret".to_owned()),
                kind: ActivationKind::Persistent,
                cluster_key: self.cluster.public_key().clone(),
            },
            Request::Refresh {
                common: self.common(),
                activation_id: id(1),
                state: RefreshState::Adoption,
                transitions: Vec::new(),
            },
            Request::Refresh {
                common: self.common(),
                activation_id: id(1),
                state: RefreshState::Committed(self.committed()),
                transitions: Vec::new(),
            },
            Request::Release {
                common: self.common(),
                activation_id: id(1),
                state: self.committed(),
                grants_until: at(600),
                reason: "fixture release".to_owned(),
            },
            Request::RotateKey {
                common: self.common(),
                activation_id: id(1),
                state: self.committed(),
                new_key: self.working.public_key().clone(),
                proof: "fixture-proof".to_owned(),
            },
        ]
    }
}

pub fn round_trip<T>(value: &T)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let bytes = encode_json(value, limits().bytes).assured("bounded current fixture serializes");
    let read: T = serde_json::from_slice(&bytes).assured("current fixture deserializes");
    assert_eq!(&read, value);
    let encoded = encode_json(&read, limits().bytes).assured("read fixture serializes");
    // Complete canonical bytes additionally distinguish JSON floating-point representations,
    // including negative zero, where Value's numeric equality alone is insufficient.
    assert_eq!(encoded, bytes);
}

pub fn signed_round_trip<T>(signer: &DocumentSigner, value: &T)
where
    T: Document + PartialEq + std::fmt::Debug,
{
    let compact = signer
        .sign(value, limits())
        .assured("bounded current document signs");
    let read: T =
        verify(&compact, signer.public_key(), limits()).assured("current document verifies");
    assert_eq!(&read, value);
    assert_eq!(
        encode_json(&read, limits().bytes).assured("read document serializes"),
        encode_json(value, limits().bytes).assured("original document serializes")
    );
}
