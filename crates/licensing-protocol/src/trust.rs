//! Root thresholds, succession and stable working-key verification.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Verified trust derivation, the facts verified trust documents state, the
//!   documents a holder lacks, and the distinction between apply and reload revocation.
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
        Decision, EmbeddedTrustBundle, RootSet, RootSuccession, SignedTrustDocument,
        TrustBundleResponse, TrustDocuments, WorkingKeyAuthorization, WorkingKeyRevocation,
        WorkingKeyUse,
    },
    keys::{KeyId, PublicKey},
    requests::TrustHeld,
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

/// What a verified trust document states.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrustDocumentContent {
    Authorization(WorkingKeyAuthorization),
    Revocation(WorkingKeyRevocation),
    Succession(RootSuccession),
}

impl TrustDocumentContent {
    pub const fn document_type(&self) -> DocumentType {
        match self {
            Self::Authorization(_) => DocumentType::WorkingKey,
            Self::Revocation(_) => DocumentType::Revocation,
            Self::Succession(_) => DocumentType::Succession,
        }
    }

    pub const fn serial(&self) -> u64 {
        match self {
            Self::Authorization(authorization) => authorization.serial,
            Self::Revocation(revocation) => revocation.serial,
            Self::Succession(succession) => succession.serial,
        }
    }

    /// The root epoch whose roots signed the document; a succession is signed by the epoch it
    /// leaves.
    pub const fn root_epoch(&self) -> u64 {
        match self {
            Self::Authorization(authorization) => authorization.root_epoch,
            Self::Revocation(revocation) => revocation.root_epoch,
            Self::Succession(succession) => succession.from_epoch,
        }
    }
}

/// One trust document in its exact original signed form, with what its root signatures verified.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedTrustDocument {
    signed: SignedTrustDocument,
    content: TrustDocumentContent,
}

impl VerifiedTrustDocument {
    pub fn signed(&self) -> &SignedTrustDocument {
        &self.signed
    }

    pub fn content(&self) -> &TrustDocumentContent {
        &self.content
    }
}

/// Where a pending succession came from: the build's own anchors, or a supplied document at its
/// position in the supplied list.
#[derive(Clone, Copy, Debug)]
enum SuccessionOrigin {
    Embedded,
    Supplied { position: usize },
}

struct PendingSuccession {
    set: SignatureSet,
    origin: SuccessionOrigin,
}

/// A supplied authorization or revocation awaiting verification, at its position in the supplied
/// list.
struct PendingDocument {
    set: SignatureSet,
    position: usize,
}

/// What each supplied document verified to, by its position in the supplied list.
struct VerifiedPositions(Vec<Option<TrustDocumentContent>>);

impl VerifiedPositions {
    fn new(count: usize) -> Self {
        Self(vec![None; count])
    }

    fn record(&mut self, position: usize, content: TrustDocumentContent) {
        let slot = self.0.get_mut(position).assured(
            "positions come from enumerating the supplied documents this table was sized for",
        );
        *slot = Some(content);
    }

    /// The supplied documents with what each verified to, in supplied order.
    fn into_documents(self, supplied: &TrustDocuments) -> Vec<VerifiedTrustDocument> {
        let mut documents = Vec::with_capacity(supplied.documents.len());
        for (signed, content) in supplied.documents.iter().zip(self.0) {
            let content = content.verified(
                "every supplied position was classified and its document verified before the \
                 build reads them back, or the build already failed",
            );
            documents.push(VerifiedTrustDocument {
                signed: signed.clone(),
                content,
            });
        }
        documents
    }
}

/// Derived only after every supplied trust document has verified. Its retained signed bytes are
/// available for storage; reloading derives the same state from those bytes and the build anchors.
pub struct TrustState {
    roots: BTreeMap<u64, PreparedRootSet>,
    authorizations: BTreeMap<KeyId, Vec<WorkingKeyAuthorization>>,
    revoked: BTreeSet<KeyId>,
    original: TrustDocuments,
    /// Every supplied document, verified, in supplied order.
    documents: Vec<VerifiedTrustDocument>,
    /// The successions the build's own anchors carry, verified, in their embedded order.
    embedded_successions: Vec<VerifiedTrustDocument>,
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
            documents: Vec::new(),
            embedded_successions: Vec::new(),
        };
        for root in &embedded.epochs {
            state.install_roots(root, limits)?;
        }
        let mut successions = Vec::new();
        for set in &embedded.successions {
            successions.push(PendingSuccession {
                set: set.clone(),
                origin: SuccessionOrigin::Embedded,
            });
        }
        let mut authorizations = Vec::new();
        let mut revocations = Vec::new();
        for (position, document) in documents.documents.iter().enumerate() {
            let set = document
                .signature_set(limits)
                .change_context(TrustError::Document)?;
            match set
                .document_type(limits)
                .change_context(TrustError::Document)?
            {
                DocumentType::Succession => successions.push(PendingSuccession {
                    set,
                    origin: SuccessionOrigin::Supplied { position },
                }),
                DocumentType::WorkingKey => authorizations.push(PendingDocument { set, position }),
                DocumentType::Revocation => revocations.push(PendingDocument { set, position }),
                _ => return Err(Report::new(TrustError::Document)),
            }
        }
        // Every position is classified above, and each class below verifies every member or fails
        // the build.
        let mut verified = VerifiedPositions::new(documents.documents.len());
        state.install_successions(successions, limits, &mut verified)?;
        for pending in authorizations {
            let authorization: WorkingKeyAuthorization =
                state.verify_roots(&pending.set, limits)?;
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
                .push(authorization.clone());
            verified.record(
                pending.position,
                TrustDocumentContent::Authorization(authorization),
            );
        }
        for pending in revocations {
            let revocation: WorkingKeyRevocation = state.verify_roots(&pending.set, limits)?;
            state.revoked.insert(revocation.kid.clone());
            verified.record(
                pending.position,
                TrustDocumentContent::Revocation(revocation),
            );
        }
        state.documents = verified.into_documents(documents);
        state.check_serials()?;
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

    /// Installs every pending succession whose source epoch is trusted, repeating until all are
    /// installed. A supplied succession's verified payload is recorded at its supplied position;
    /// an embedded one joins the state's embedded successions.
    fn install_successions(
        &mut self,
        mut pending: Vec<PendingSuccession>,
        limits: DocumentLimits,
        verified: &mut VerifiedPositions,
    ) -> Result<(), Report<TrustError>> {
        while !pending.is_empty() {
            let mut remaining = Vec::new();
            let mut failure = None;
            let before = pending.len();
            for succession in pending {
                match self.verify_roots::<RootSuccession>(&succession.set, limits) {
                    Ok(content) => {
                        if content.from_epoch.checked_add(1) != Some(content.to_epoch) {
                            return Err(Report::new(TrustError::EpochGap));
                        }
                        self.install_roots(
                            &RootSet {
                                epoch: content.to_epoch,
                                roots: content.roots.clone(),
                                threshold: content.threshold,
                            },
                            limits,
                        )?;
                        let content = TrustDocumentContent::Succession(content);
                        match succession.origin {
                            SuccessionOrigin::Embedded => {
                                self.embedded_successions.push(VerifiedTrustDocument {
                                    signed: SignedTrustDocument::Signatures(succession.set),
                                    content,
                                });
                            }
                            SuccessionOrigin::Supplied { position } => {
                                verified.record(position, content);
                            }
                        }
                    }
                    Err(error) => {
                        remaining.push(succession);
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

    /// Refuses two supplied documents of one type and serial with different signed bytes, reading
    /// each document's serial from what it verified to.
    fn check_serials(&self) -> Result<(), Report<TrustError>> {
        #[derive(PartialEq, Eq, PartialOrd, Ord)]
        struct SerialIdentity {
            document_type: &'static str,
            serial: u64,
        }
        let mut seen = BTreeMap::new();
        for document in &self.documents {
            let identity = SerialIdentity {
                document_type: document.content.document_type().as_str(),
                serial: document.content.serial(),
            };
            if let Some(previous) = seen.insert(identity, &document.signed)
                && previous != &document.signed
            {
                return Err(Report::new(TrustError::SerialConflict));
            }
        }
        Ok(())
    }

    /// Every supplied trust document, verified, in supplied order.
    pub fn documents(&self) -> &[VerifiedTrustDocument] {
        &self.documents
    }

    /// Every trusted root set, embedded or established by a verified succession, by epoch.
    pub fn root_sets(&self) -> impl Iterator<Item = &RootSet> {
        self.roots.values().map(|prepared| &prepared.original)
    }

    /// A verified authorization of `kid` for decisions that covers `at`, when one exists.
    pub fn authorization(
        &self,
        kid: &KeyId,
        at: DateTime<Utc>,
    ) -> Option<&WorkingKeyAuthorization> {
        let authorizations = self.authorizations.get(kid)?;
        authorizations
            .iter()
            .find(|authorization| authorization.authorizes(at))
    }

    /// Whether a verified revocation names `kid`.
    pub fn is_revoked(&self, kid: &KeyId) -> bool {
        self.revoked.contains(kid)
    }

    /// The trust documents a holder of `held` lacks to verify a decision `signing_key` signed: the
    /// signing key's authorizations when the holder holds no authorization of that key, and every
    /// revocation and succession whose serial it does not hold, the build's own embedded
    /// successions first.
    pub fn missing(&self, held: &TrustHeld, signing_key: &KeyId) -> TrustDocuments {
        let held_keys: BTreeSet<&KeyId> = held.working_keys.iter().collect();
        let held_revocations: BTreeSet<u64> = held.revocation_serials.iter().copied().collect();
        let held_successions: BTreeSet<u64> = held.succession_serials.iter().copied().collect();
        let mut documents = Vec::new();
        for document in self.embedded_successions.iter().chain(&self.documents) {
            let lacking = match &document.content {
                TrustDocumentContent::Authorization(authorization) => {
                    &authorization.kid == signing_key && !held_keys.contains(&authorization.kid)
                }
                TrustDocumentContent::Revocation(revocation) => {
                    !held_revocations.contains(&revocation.serial)
                }
                TrustDocumentContent::Succession(succession) => {
                    !held_successions.contains(&succession.serial)
                }
            };
            if lacking {
                documents.push(document.signed.clone());
            }
        }
        TrustDocuments { documents }
    }

    /// The trust bundle this state publishes: its root sets by epoch, then every verified
    /// succession (the build's own embedded ones first), authorization and revocation in its
    /// exact signed form.
    pub fn publication(&self) -> TrustBundleResponse {
        let mut publication = TrustBundleResponse {
            epochs: self.root_sets().cloned().collect(),
            successions: Vec::new(),
            authorizations: Vec::new(),
            revocations: Vec::new(),
        };
        for document in self.embedded_successions.iter().chain(&self.documents) {
            let signed = document.signed.clone();
            match &document.content {
                TrustDocumentContent::Authorization(_) => publication.authorizations.push(signed),
                TrustDocumentContent::Revocation(_) => publication.revocations.push(signed),
                TrustDocumentContent::Succession(_) => publication.successions.push(signed),
            }
        }
        publication
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
