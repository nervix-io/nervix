//! Root thresholds, succession and stable working-key verification.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Verified trust derivation and the distinction between apply and reload revocation.
//! - **Depends on.** Exact signed documents, public key identities and caller-supplied bounds.
//! - **Must not know.** Configuration paths for anchors, clocks, persistence, graphs or HTTP.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use thiserror::Error;

use crate::{
    document::{Document, DocumentLimits, DocumentType, SignatureSet, UnverifiedDocument},
    documents::{
        Decision, EmbeddedTrustBundle, RootSet, RootSuccession, TrustDocuments,
        WorkingKeyAuthorization, WorkingKeyRevocation, WorkingKeyUse,
    },
    keys::{KeyId, PublicKey},
};

/// The launch ceremony owns provisioning. Absence grants no trust and cannot be replaced through
/// a runtime configuration path. Test and ceremony callers supply their own explicit engine input.
pub const PRODUCTION_TRUST_BUNDLE: Option<EmbeddedTrustBundle> = None;

#[derive(Debug, Error)]
pub enum TrustError {
    #[error("the trust input exceeds its document bound")]
    DocumentCount,
    #[error("the trust input contains no embedded root epoch")]
    NoAnchors,
    #[error("a root set has an invalid threshold or repeated public key")]
    RootSet,
    #[error("root epoch {epoch} is declared more than once with different roots")]
    EpochConflict { epoch: u64 },
    #[error("the trust document has an invalid signature profile or payload")]
    Document,
    #[error("the signature set has {verified} verified roots, below its threshold of {required}")]
    Threshold { verified: usize, required: usize },
    #[error("the document names a different root epoch from its verified signers")]
    RootEpoch,
    #[error("the root succession must advance exactly one epoch")]
    EpochGap,
    #[error("the trust document uses an unsupported protocol")]
    Protocol,
    #[error("the authorization's key identity does not match its public key")]
    KeyIdentity,
    #[error("the working-key authorization has an inverted validity window")]
    AuthorizationWindow,
    #[error("the decision's signing key has no verified authorization")]
    UnknownSigningKey,
    #[error("the signing key is not authorized at the decision's issue time")]
    NotAuthorized,
    #[error("the signing key is revoked for newly applied decisions")]
    SigningKeyRevoked,
    #[error("a trust document repeats a serial with different signed bytes")]
    SerialConflict,
}

struct PreparedRootSet {
    original: RootSet,
    keys: BTreeMap<KeyId, PublicKey>,
}

impl RootSet {
    fn prepare(&self, limits: DocumentLimits) -> Result<PreparedRootSet, Report<TrustError>> {
        if self.roots.len() > limits.signatures.get() || self.threshold.get() > self.roots.len() {
            return Err(Report::new(TrustError::RootSet));
        }
        let mut keys = BTreeMap::new();
        for key in &self.roots {
            let kid = key.key_id().change_context(TrustError::KeyIdentity)?;
            if keys.insert(kid, key.clone()).is_some() {
                return Err(Report::new(TrustError::RootSet));
            }
        }
        Ok(PreparedRootSet {
            original: self.clone(),
            keys,
        })
    }
}

trait RootDocument: Document {
    fn root_epoch(&self) -> u64;
    fn protocol(&self) -> u8;
}

impl RootDocument for RootSuccession {
    fn root_epoch(&self) -> u64 {
        self.from_epoch
    }
    fn protocol(&self) -> u8 {
        self.protocol
    }
}
impl RootDocument for WorkingKeyAuthorization {
    fn root_epoch(&self) -> u64 {
        self.root_epoch
    }
    fn protocol(&self) -> u8 {
        self.protocol
    }
}
impl RootDocument for WorkingKeyRevocation {
    fn root_epoch(&self) -> u64 {
        self.root_epoch
    }
    fn protocol(&self) -> u8 {
        self.protocol
    }
}

impl PreparedRootSet {
    fn verify<T: RootDocument>(
        &self,
        set: &SignatureSet,
        limits: DocumentLimits,
    ) -> Result<T, Report<TrustError>> {
        let members = set
            .parse(T::TYPE, limits)
            .change_context(TrustError::Document)?;
        let mut verified = 0usize;
        let mut payload = None;
        let mut failure = None;
        for member in members {
            let Some(key) = self.keys.get(member.key_id()) else {
                continue;
            };
            match member.verify::<T>(key) {
                Ok(document) => {
                    verified = verified
                        .checked_add(1)
                        .assured("verified roots are bounded by the signature count");
                    if payload.is_none() {
                        payload = Some(document);
                    }
                }
                Err(error) => failure = Some(error),
            }
        }
        if verified < self.original.threshold.get() {
            let context = TrustError::Threshold {
                verified,
                required: self.original.threshold.get(),
            };
            return match failure {
                Some(error) => Err(error.change_context(context)),
                None => Err(Report::new(context)),
            };
        }
        let document = payload.assured("a nonzero verified threshold supplied a payload");
        if document.protocol() != 1 {
            return Err(Report::new(TrustError::Protocol));
        }
        if document.root_epoch() != self.original.epoch {
            return Err(Report::new(TrustError::RootEpoch));
        }
        Ok(document)
    }
}

/// Derived only after every supplied trust document has verified. Its retained signed bytes are
/// available for storage; reloading derives the same state from those bytes and the build anchors.
pub struct TrustState {
    roots: BTreeMap<u64, PreparedRootSet>,
    authorizations: BTreeMap<KeyId, Vec<WorkingKeyAuthorization>>,
    revoked: BTreeSet<KeyId>,
    original: TrustDocuments,
}

impl TrustState {
    pub fn build(
        embedded: &EmbeddedTrustBundle,
        documents: &TrustDocuments,
        limits: DocumentLimits,
    ) -> Result<Self, Report<TrustError>> {
        let Some(embedded_count) = embedded
            .epochs
            .len()
            .checked_add(embedded.successions.len())
        else {
            return Err(Report::new(TrustError::DocumentCount));
        };
        let Some(count) = embedded_count.checked_add(documents.documents.len()) else {
            return Err(Report::new(TrustError::DocumentCount));
        };
        if count > limits.trust_documents.get() {
            return Err(Report::new(TrustError::DocumentCount));
        }
        if embedded.epochs.is_empty() {
            return Err(Report::new(TrustError::NoAnchors));
        }
        // Validate borrowed lengths before retaining any signed input or deriving root identities.
        // The total document count above also bounds these preflight walks.
        for set in &embedded.successions {
            set.check_bounds(limits)
                .change_context(TrustError::Document)?;
        }
        for document in &documents.documents {
            document
                .check_bounds(limits)
                .change_context(TrustError::Document)?;
        }
        let mut state = Self {
            roots: BTreeMap::new(),
            authorizations: BTreeMap::new(),
            revoked: BTreeSet::new(),
            original: documents.clone(),
        };
        for root in &embedded.epochs {
            state.install_roots(root, limits)?;
        }
        let mut successions = embedded.successions.clone();
        let mut authorizations = Vec::new();
        let mut revocations = Vec::new();
        for document in &documents.documents {
            let set = document
                .signature_set(limits)
                .change_context(TrustError::Document)?;
            match set
                .document_type(limits)
                .change_context(TrustError::Document)?
            {
                DocumentType::Succession => successions.push(set),
                DocumentType::WorkingKey => authorizations.push(set),
                DocumentType::Revocation => revocations.push(set),
                _ => return Err(Report::new(TrustError::Document)),
            }
        }
        state.install_successions(successions, limits)?;
        for set in authorizations {
            let authorization: WorkingKeyAuthorization = state.verify_roots(&set, limits)?;
            if authorization.kid
                != authorization
                    .public_jwk
                    .key_id()
                    .change_context(TrustError::KeyIdentity)?
            {
                return Err(Report::new(TrustError::KeyIdentity));
            }
            if authorization.not_before > authorization.not_after {
                return Err(Report::new(TrustError::AuthorizationWindow));
            }
            state
                .authorizations
                .entry(authorization.kid.clone())
                .or_default()
                .push(authorization);
        }
        for set in revocations {
            let revocation: WorkingKeyRevocation = state.verify_roots(&set, limits)?;
            state.revoked.insert(revocation.kid);
        }
        state.check_serials(limits)?;
        Ok(state)
    }

    fn install_roots(
        &mut self,
        root: &RootSet,
        limits: DocumentLimits,
    ) -> Result<(), Report<TrustError>> {
        if let Some(existing) = self.roots.get(&root.epoch) {
            if &existing.original != root {
                return Err(Report::new(TrustError::EpochConflict { epoch: root.epoch }));
            }
            return Ok(());
        }
        let prepared = root.prepare(limits)?;
        self.roots.insert(prepared.original.epoch, prepared);
        Ok(())
    }

    fn verify_roots<T: RootDocument>(
        &self,
        set: &SignatureSet,
        limits: DocumentLimits,
    ) -> Result<T, Report<TrustError>> {
        let mut last_failure = None;
        for roots in self.roots.values() {
            match roots.verify(set, limits) {
                Ok(document) => return Ok(document),
                Err(error) => last_failure = Some(error),
            }
        }
        Err(last_failure.assured("a trust state always has an embedded root epoch"))
    }

    fn install_successions(
        &mut self,
        mut pending: Vec<SignatureSet>,
        limits: DocumentLimits,
    ) -> Result<(), Report<TrustError>> {
        while !pending.is_empty() {
            let mut remaining = Vec::new();
            let mut failure = None;
            let before = pending.len();
            for set in pending {
                match self.verify_roots::<RootSuccession>(&set, limits) {
                    Ok(succession) => {
                        if succession.from_epoch.checked_add(1) != Some(succession.to_epoch) {
                            return Err(Report::new(TrustError::EpochGap));
                        }
                        self.install_roots(
                            &RootSet {
                                epoch: succession.to_epoch,
                                roots: succession.roots,
                                threshold: succession.threshold,
                            },
                            limits,
                        )?;
                    }
                    Err(error) => {
                        remaining.push(set);
                        failure = Some(error);
                    }
                }
            }
            if remaining.len() == before {
                return Err(failure.assured("every remaining succession failed verification"));
            }
            pending = remaining;
        }
        Ok(())
    }

    fn check_serials(&self, limits: DocumentLimits) -> Result<(), Report<TrustError>> {
        #[derive(PartialEq, Eq, PartialOrd, Ord)]
        struct SerialIdentity {
            document_type: &'static str,
            serial: u64,
        }
        let mut seen = BTreeMap::new();
        for signed in &self.original.documents {
            let set = signed
                .signature_set(limits)
                .change_context(TrustError::Document)?;
            let kind = set
                .document_type(limits)
                .change_context(TrustError::Document)?;
            let serial = match kind {
                DocumentType::WorkingKey => {
                    self.verify_roots::<WorkingKeyAuthorization>(&set, limits)?
                        .serial
                }
                DocumentType::Revocation => {
                    self.verify_roots::<WorkingKeyRevocation>(&set, limits)?
                        .serial
                }
                DocumentType::Succession => {
                    self.verify_roots::<RootSuccession>(&set, limits)?.serial
                }
                _ => return Err(Report::new(TrustError::Document)),
            };
            let identity = SerialIdentity {
                document_type: kind.as_str(),
                serial,
            };
            if let Some(previous) = seen.insert(identity, signed)
                && previous != signed
            {
                return Err(Report::new(TrustError::SerialConflict));
            }
        }
        Ok(())
    }

    pub fn original_documents(&self) -> &TrustDocuments {
        &self.original
    }

    pub fn verify_decision(
        &self,
        compact: &str,
        kind: DecisionVerification,
        limits: DocumentLimits,
    ) -> Result<VerifiedDecision, Report<TrustError>> {
        let parsed = UnverifiedDocument::parse(compact, DocumentType::Decision, limits)
            .change_context(TrustError::Document)?;
        let Some(authorizations) = self.authorizations.get(parsed.key_id()) else {
            return Err(Report::new(TrustError::UnknownSigningKey));
        };
        let first = authorizations
            .first()
            .assured("authorization map entries are installed with a document");
        let decision: Decision = parsed
            .verify(&first.public_jwk)
            .change_context(TrustError::Document)?;
        let mut authorized = false;
        for authorization in authorizations {
            if authorization.authorizes(decision.issued_at) {
                authorized = true;
                break;
            }
        }
        if !authorized {
            return Err(Report::new(TrustError::NotAuthorized));
        }
        let revoked = self.revoked.contains(parsed.key_id());
        if revoked && kind == DecisionVerification::Apply {
            return Err(Report::new(TrustError::SigningKeyRevoked));
        }
        Ok(VerifiedDecision {
            decision,
            compact: compact.to_owned(),
            signing_key_revoked: revoked,
        })
    }
}

impl WorkingKeyAuthorization {
    fn authorizes(&self, time: DateTime<Utc>) -> bool {
        self.not_before <= time
            && time <= self.not_after
            && self.uses.contains(&WorkingKeyUse::Decision)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecisionVerification {
    Apply,
    Reload,
}

pub struct VerifiedDecision {
    decision: Decision,
    compact: String,
    signing_key_revoked: bool,
}

impl VerifiedDecision {
    pub fn decision(&self) -> &Decision {
        &self.decision
    }
    pub fn compact(&self) -> &str {
        &self.compact
    }
    pub const fn signing_key_revoked(&self) -> bool {
        self.signing_key_revoked
    }
}
