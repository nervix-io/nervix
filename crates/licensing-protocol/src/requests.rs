//! Exact operation payloads, their signers and retry identity for signed cluster requests.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Current request shapes, which key a received request must verify under, secret
//!   ownership and challenge-independent request digests.
//! - **Depends on.** Typed protocol values, maintained serde JSON and the cryptography boundary.
//! - **Must not know.** HTTP, challenge storage, idempotency tables, Models or clocks.

use std::{fmt, num::NonZeroUsize};

use chrono::{DateTime, Utc};
use error_stack::{Report, ResultExt as _};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de::Error as _, ser::SerializeMap as _,
};
use thiserror::Error;
use url::Url;
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::{
    conditions::ActivationKind,
    document::{
        Document, DocumentError, DocumentLimits, DocumentType, UnverifiedDocument, encode_json,
    },
    documents::Transition,
    keys::{KeyId, PublicKey},
};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: String) -> Self {
        Self(value)
    }
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Secret").finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClientProduct {
    NervixServer,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientDescription {
    pub product: ClientProduct,
    pub version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustHeld {
    pub working_keys: Vec<KeyId>,
    pub revocation_serials: Vec<u64>,
    pub succession_serials: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestCommon {
    pub protocol: u8,
    pub request_id: Uuid,
    pub challenge: Secret,
    pub sent_at: DateTime<Utc>,
    pub aud: Url,
    pub cluster_uid: Uuid,
    pub display_name: String,
    pub client: ClientDescription,
    pub key_generation: u64,
    pub trust_held: TrustHeld,
}

impl RequestCommon {
    fn serialize_fields<M: serde::ser::SerializeMap>(&self, map: &mut M) -> Result<(), M::Error> {
        map.serialize_entry("protocol", &self.protocol)?;
        map.serialize_entry("request_id", &self.request_id)?;
        map.serialize_entry("challenge", &self.challenge)?;
        map.serialize_entry("sent_at", &self.sent_at)?;
        map.serialize_entry("aud", &self.aud)?;
        map.serialize_entry("cluster_uid", &self.cluster_uid)?;
        map.serialize_entry("display_name", &self.display_name)?;
        map.serialize_entry("client", &self.client)?;
        map.serialize_entry("key_generation", &self.key_generation)?;
        map.serialize_entry("trust_held", &self.trust_held)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedState {
    pub applied_decision_revision: u64,
    pub applied_conditions_revision: u64,
    pub licensing_time: DateTime<Utc>,
}

impl CommittedState {
    fn serialize_fields<M: serde::ser::SerializeMap>(&self, map: &mut M) -> Result<(), M::Error> {
        map.serialize_entry("applied_decision_revision", &self.applied_decision_revision)?;
        map.serialize_entry(
            "applied_conditions_revision",
            &self.applied_conditions_revision,
        )?;
        map.serialize_entry("licensing_time", &self.licensing_time)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefreshState {
    Adoption,
    Committed(CommittedState),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    Create {
        common: RequestCommon,
        license_key: Secret,
        kind: ActivationKind,
        cluster_key: PublicKey,
    },
    Refresh {
        common: RequestCommon,
        activation_id: Uuid,
        state: RefreshState,
        transitions: Vec<Transition>,
    },
    Release {
        common: RequestCommon,
        activation_id: Uuid,
        state: CommittedState,
        grants_until: DateTime<Utc>,
        reason: String,
    },
    RotateKey {
        common: RequestCommon,
        activation_id: Uuid,
        state: CommittedState,
        new_key: PublicKey,
        proof: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamTicket {
    pub common: RequestCommon,
    pub activation_id: Uuid,
    pub state: CommittedState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyRotationProof {
    pub challenge: Secret,
    pub cluster_uid: Uuid,
    pub activation_id: Uuid,
    pub new_kid: KeyId,
}

impl Document for Request {
    const TYPE: DocumentType = DocumentType::Request;
}
impl Document for StreamTicket {
    const TYPE: DocumentType = DocumentType::StreamTicket;
}
impl Document for KeyRotationProof {
    const TYPE: DocumentType = DocumentType::Request;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestDigest([u8; 32]);

impl RequestDigest {
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    fn compute<T: Serialize>(
        request: &T,
        limit: NonZeroUsize,
    ) -> Result<Self, Report<RequestError>> {
        let bytes = encode_json(request, limit).change_context(RequestError::Encoding)?;
        let mut value: serde_json::Value =
            serde_json::from_slice(&bytes).change_context(RequestError::Encoding)?;
        let Some(object) = value.as_object_mut() else {
            return Err(Report::new(RequestError::Encoding));
        };
        object.remove("challenge");
        object.remove("sent_at");
        // serde_json's preserve_order feature may be enabled by another workspace consumer.
        // The logical digest must have the same byte order in every service/product build.
        value.sort_all_objects();
        let canonical = encode_json(&value, limit).change_context(RequestError::Encoding)?;
        Ok(Self(nervix_crypto::digest::sha256(&canonical)))
    }
}

struct RequestParts<T> {
    common: RequestCommon,
    body: T,
}

// Every operation has a flat, deny-unknown-fields wire structure. Expanding the common fields
// here keeps serde's exact field checks without flattening optional operation-specific fields.
macro_rules! flat_request {
    ($wire:ident, $body:ident { $($field:ident: $ty:ty),* $(,)? }) => {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct $wire {
            protocol: u8, request_id: Uuid, challenge: Secret, sent_at: DateTime<Utc>, aud: Url,
            cluster_uid: Uuid, display_name: String, client: ClientDescription, key_generation: u64,
            trust_held: TrustHeld,
            $($field: $ty,)*
        }
        struct $body { $($field: $ty,)* }
        impl $wire {
            fn into_parts(self) -> RequestParts<$body> {
                let Self { protocol, request_id, challenge, sent_at, aud, cluster_uid, display_name,
                    client, key_generation, trust_held, $($field,)* } = self;
                RequestParts {
                    common: RequestCommon { protocol, request_id, challenge, sent_at, aud, cluster_uid,
                        display_name, client, key_generation, trust_held },
                    body: $body { $($field,)* },
                }
            }
        }
    };
}

macro_rules! request_operations {
    ($($variant:ident, $wire:ident, $body:ident => $name:literal { $($fields:tt)* });* $(;)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
        pub enum Operation { $(#[serde(rename = $name)] $variant,)* }
        #[derive(Deserialize)]
        #[serde(tag = "op", deny_unknown_fields)]
        enum RequestWire { $(#[serde(rename = $name)] $variant($wire),)* }
        $(flat_request!($wire, $body { $($fields)* });)*
    };
}

request_operations! {
    Create, CreateWire, CreateBody => "activation.create" { license_key: Secret, kind: ActivationKind, cluster_key: PublicKey };
    Refresh, RefreshWire, RefreshBody => "activation.refresh" { activation_id: Uuid, applied_decision_revision: Option<u64>, applied_conditions_revision: Option<u64>, licensing_time: Option<DateTime<Utc>>, transitions: Vec<Transition> };
    Release, ReleaseWire, ReleaseBody => "activation.release" { activation_id: Uuid, applied_decision_revision: u64, applied_conditions_revision: u64, licensing_time: DateTime<Utc>, grants_until: DateTime<Utc>, reason: String };
    RotateKey, RotateWire, RotateBody => "activation.rotate_key" { activation_id: Uuid, applied_decision_revision: u64, applied_conditions_revision: u64, licensing_time: DateTime<Utc>, new_key: PublicKey, proof: String };
    Subscribe, SubscribeWire, SubscribeBody => "events.subscribe" { activation_id: Uuid, applied_decision_revision: u64, applied_conditions_revision: u64, licensing_time: DateTime<Utc> };
}

#[derive(Debug, Error)]
pub enum RequestError {
    #[error("the request's committed revisions and licensing time must be present together")]
    State,
    #[error("the operation is not valid for this signed document type")]
    DocumentType,
    #[error("failed to compute request identity")]
    Encoding,
}

/// The key a received request must verify under (licensing specification §9.2).
#[derive(Clone, Copy, Debug)]
pub enum RequestSigner<'a> {
    /// `activation.create`, self-signed by the cluster key its payload carries: the signature
    /// proves possession of that key, and the license key in the payload authorizes the request.
    CarriedClusterKey,
    /// Every other operation, signed by the cluster key the activation is bound to.
    Bound(&'a PublicKey),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum RequestVerificationError {
    #[error("the signed request is malformed, exceeds its bound or breaks the JWS profile")]
    Envelope,
    #[error("the request is signed by a key other than the one it must verify under")]
    Signer,
    #[error("the request signature is invalid")]
    Signature,
    #[error("the signed payload is not a current request of the expected kind")]
    Payload,
}

/// The one member of a self-signed request read before its signature verifies.
#[derive(Deserialize)]
struct CarriedClusterKey {
    cluster_key: PublicKey,
}

impl Request {
    /// Verifies a received compact request under `signer` and returns its payload. Nothing of the
    /// payload is interpreted before the signature verifies, except the carried cluster key of a
    /// self-signed request, which is read only to be the key that must verify it: the protected
    /// header's `kid` must be that key's thumbprint and the signature must verify under it over
    /// the received bytes before the request as a whole is decoded.
    pub fn verify(
        compact: &str,
        signer: RequestSigner<'_>,
        limits: DocumentLimits,
    ) -> Result<Self, Report<RequestVerificationError>> {
        let document = UnverifiedDocument::parse(compact, DocumentType::Request, limits)
            .change_context(RequestVerificationError::Envelope)?;
        match signer {
            RequestSigner::Bound(key) => Self::verify_under(&document, key),
            RequestSigner::CarriedClusterKey => {
                let carried: CarriedClusterKey = serde_json::from_slice(document.payload())
                    .change_context(RequestVerificationError::Payload)?;
                let request = Self::verify_under(&document, &carried.cluster_key)?;
                // Only a creation carries the key that signed it; the verified creation's own
                // cluster key is the carried one, read from the same verified bytes.
                let Self::Create { .. } = &request else {
                    return Err(Report::new(RequestVerificationError::Payload));
                };
                Ok(request)
            }
        }
    }

    fn verify_under(
        document: &UnverifiedDocument,
        key: &PublicKey,
    ) -> Result<Self, Report<RequestVerificationError>> {
        match document.verify::<Self>(key) {
            Ok(request) => Ok(request),
            Err(report) => {
                let context = RequestVerificationError::of(report.current_context());
                Err(report.change_context(context))
            }
        }
    }
}

impl RequestVerificationError {
    /// What a failed verification of a parsed request means. Parsing already refused every
    /// framing, header and size failure, so they remain envelope failures here.
    const fn of(failure: &DocumentError) -> Self {
        match failure {
            DocumentError::KeyIdentity => Self::Signer,
            DocumentError::Verification => Self::Signature,
            DocumentError::Payload => Self::Payload,
            DocumentError::Size { .. }
            | DocumentError::Compact
            | DocumentError::Header
            | DocumentError::Encoding
            | DocumentError::SigningKey
            | DocumentError::SignatureCount { .. }
            | DocumentError::PayloadMismatch
            | DocumentError::RepeatedSigner => Self::Envelope,
        }
    }
}

impl Request {
    pub fn common(&self) -> &RequestCommon {
        match self {
            Self::Create { common, .. }
            | Self::Refresh { common, .. }
            | Self::Release { common, .. }
            | Self::RotateKey { common, .. } => common,
        }
    }

    pub fn common_mut(&mut self) -> &mut RequestCommon {
        match self {
            Self::Create { common, .. }
            | Self::Refresh { common, .. }
            | Self::Release { common, .. }
            | Self::RotateKey { common, .. } => common,
        }
    }

    pub const fn operation(&self) -> Operation {
        match self {
            Self::Create { .. } => Operation::Create,
            Self::Refresh { .. } => Operation::Refresh,
            Self::Release { .. } => Operation::Release,
            Self::RotateKey { .. } => Operation::RotateKey,
        }
    }

    /// Logical retry identity includes every current field except the fresh challenge and signing
    /// timestamp. Sorted JSON object keys make alternative wire member order immaterial.
    pub fn digest(&self, limit: NonZeroUsize) -> Result<RequestDigest, Report<RequestError>> {
        RequestDigest::compute(self, limit)
    }
}

impl Serialize for Request {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("op", &self.operation())?;
        self.common().serialize_fields(&mut map)?;
        match self {
            Self::Create {
                license_key,
                kind,
                cluster_key,
                ..
            } => {
                map.serialize_entry("license_key", license_key)?;
                map.serialize_entry("kind", kind)?;
                map.serialize_entry("cluster_key", cluster_key)?;
            }
            Self::Refresh {
                activation_id,
                state,
                transitions,
                ..
            } => {
                map.serialize_entry("activation_id", activation_id)?;
                if let RefreshState::Committed(state) = state {
                    state.serialize_fields(&mut map)?;
                }
                map.serialize_entry("transitions", transitions)?;
            }
            Self::Release {
                activation_id,
                state,
                grants_until,
                reason,
                ..
            } => {
                map.serialize_entry("activation_id", activation_id)?;
                state.serialize_fields(&mut map)?;
                map.serialize_entry("grants_until", grants_until)?;
                map.serialize_entry("reason", reason)?;
            }
            Self::RotateKey {
                activation_id,
                state,
                new_key,
                proof,
                ..
            } => {
                map.serialize_entry("activation_id", activation_id)?;
                state.serialize_fields(&mut map)?;
                map.serialize_entry("new_key", new_key)?;
                map.serialize_entry("proof", proof)?;
            }
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Request {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = RequestWire::deserialize(deserializer)?;
        match wire {
            RequestWire::Create(wire) => {
                let RequestParts { common, body } = wire.into_parts();
                Ok(Self::Create {
                    common,
                    license_key: body.license_key,
                    kind: body.kind,
                    cluster_key: body.cluster_key,
                })
            }
            RequestWire::Refresh(wire) => {
                let RequestParts { common, body } = wire.into_parts();
                let state = match (
                    body.applied_decision_revision,
                    body.applied_conditions_revision,
                    body.licensing_time,
                ) {
                    (None, None, None) => RefreshState::Adoption,
                    (
                        Some(applied_decision_revision),
                        Some(applied_conditions_revision),
                        Some(licensing_time),
                    ) => RefreshState::Committed(CommittedState {
                        applied_decision_revision,
                        applied_conditions_revision,
                        licensing_time,
                    }),
                    _ => return Err(D::Error::custom(RequestError::State)),
                };
                Ok(Self::Refresh {
                    common,
                    activation_id: body.activation_id,
                    state,
                    transitions: body.transitions,
                })
            }
            RequestWire::Release(wire) => {
                let RequestParts { common, body } = wire.into_parts();
                let state = CommittedState {
                    applied_decision_revision: body.applied_decision_revision,
                    applied_conditions_revision: body.applied_conditions_revision,
                    licensing_time: body.licensing_time,
                };
                Ok(Self::Release {
                    common,
                    activation_id: body.activation_id,
                    state,
                    grants_until: body.grants_until,
                    reason: body.reason,
                })
            }
            RequestWire::RotateKey(wire) => {
                let RequestParts { common, body } = wire.into_parts();
                let state = CommittedState {
                    applied_decision_revision: body.applied_decision_revision,
                    applied_conditions_revision: body.applied_conditions_revision,
                    licensing_time: body.licensing_time,
                };
                Ok(Self::RotateKey {
                    common,
                    activation_id: body.activation_id,
                    state,
                    new_key: body.new_key,
                    proof: body.proof,
                })
            }
            RequestWire::Subscribe(_) => Err(D::Error::custom(RequestError::DocumentType)),
        }
    }
}

impl Serialize for StreamTicket {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("op", &Operation::Subscribe)?;
        self.common.serialize_fields(&mut map)?;
        self.state.serialize_fields(&mut map)?;
        map.serialize_entry("activation_id", &self.activation_id)?;
        map.end()
    }
}

impl StreamTicket {
    pub fn digest(&self, limit: NonZeroUsize) -> Result<RequestDigest, Report<RequestError>> {
        RequestDigest::compute(self, limit)
    }
}

impl<'de> Deserialize<'de> for StreamTicket {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let RequestWire::Subscribe(wire) = RequestWire::deserialize(deserializer)? else {
            return Err(D::Error::custom(RequestError::DocumentType));
        };
        let RequestParts { common, body } = wire.into_parts();
        Ok(Self {
            common,
            activation_id: body.activation_id,
            state: CommittedState {
                applied_decision_revision: body.applied_decision_revision,
                applied_conditions_revision: body.applied_conditions_revision,
                licensing_time: body.licensing_time,
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChallengeRequest {
    pub cluster_uid: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChallengeResponse {
    pub challenge: Secret,
    pub expires_at: DateTime<Utc>,
}
