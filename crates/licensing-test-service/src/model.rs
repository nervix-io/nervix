//! The protocol peer's bounded, serial in-memory transaction owner.
//!
//! Layer: test harness.
//! - **Owns.** Challenges, activation capacity, idempotency, real signatures and scripted answers.
//! - **Depends on.** The shared licensing protocol and explicit fixture time.
//! - **Must not know.** Cluster policy, Raft or scenario step implementation.

use std::collections::{BTreeMap, VecDeque};

use axum::{body::Body, response::Response};
use base64::Engine as _;
use chrono::{DateTime, TimeDelta, Utc};
use error_stack::ResultExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_crypto::{ecdsa::EcdsaP256KeyPair, random};
use nervix_licensing_protocol::{
    conditions::*, document::*, documents::*, problems::*, requests::*,
};
use serde::Serialize;
use url::Url;
use uuid::Uuid;

use crate::{ServiceError, limits};
pub(crate) mod notifications;

#[cfg(test)]
mod tests;

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "each scenario peer retains at most 4096 script and request identities; this \
                  finite fixture envelope is absent from deployed services"
    )
)]
const MAX_ENTRIES: usize = 4096;
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the HTTPS fixture challenge-renewal contract uses five-minute tickets shared by \
                  its controllable clock and challenge tests"
    )
)]
const CHALLENGE_SECONDS: i64 = 300;

#[derive(Clone, Debug)]
pub enum Fault {
    Refuse(ProblemKind),
    Delay(std::time::Duration),
    LoseResponse,
    Adopt,
    Tamper(Tamper),
}

#[derive(Clone, Copy, Debug)]
pub enum Tamper {
    Signature,
    UnauthorizedKey,
    RevokedKey,
    Cluster,
    Key,
    Generation,
    Activation,
    Revision,
    Timing,
    Service,
    Request,
    PriorDecision,
}

struct HeldActivation {
    notification_revision: u64,
    binding: Activation,
    public_key: nervix_licensing_protocol::keys::PublicKey,
    decision: Decision,
    prior_decision: Option<Decision>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RequestIdentity {
    cluster: Uuid,
    request: Uuid,
}

struct SavedResponse {
    digest: RequestDigest,
    status: u16,
    body: Vec<u8>,
}
struct Challenge {
    cluster: Uuid,
    until: DateTime<Utc>,
}

pub struct Model {
    notifications: notifications::Notifications,
    pub(crate) bundle: EmbeddedTrustBundle,
    pub(crate) requests: Vec<Request>,
    pub(crate) request_count: u64,
    address: Url,
    now: DateTime<Utc>,
    working: DocumentSigner,
    roots: [DocumentSigner; 3],
    documents: TrustDocuments,
    plan: Conditions,
    conditions_revision: u64,
    challenges: BTreeMap<String, Challenge>,
    activations: BTreeMap<Uuid, HeldActivation>,
    responses: BTreeMap<RequestIdentity, SavedResponse>,
    faults: VecDeque<Fault>,
    refusal: Option<ProblemKind>,
    license_key: Secret,
}

pub(crate) fn id() -> Uuid {
    uuid::Builder::from_random_bytes(random::bytes()).into_uuid()
}

fn signer() -> error_stack::Result<DocumentSigner, ServiceError> {
    let key = EcdsaP256KeyPair::generate_pkcs8().change_context(ServiceError::Model)?;
    DocumentSigner::from_pkcs8(key.as_ref()).change_context(ServiceError::Model)
}

impl Model {
    pub(crate) fn new(address: Url, now: DateTime<Utc>) -> error_stack::Result<Self, ServiceError> {
        let roots = [signer()?, signer()?, signer()?];
        let working = signer()?;
        let bundle = EmbeddedTrustBundle {
            epochs: vec![RootSet {
                epoch: 0,
                roots: roots.iter().map(|root| root.public_key().clone()).collect(),
                threshold: std::num::NonZeroUsize::new(2).assured("two-root fixture threshold"),
            }],
            successions: Vec::new(),
        };
        let authorization = WorkingKeyAuthorization {
            doc: AuthorizationKind::WorkingKeyAuthorization,
            protocol: 1,
            serial: 1,
            issued_at: now,
            root_epoch: 0,
            kid: working.key_id().clone(),
            public_jwk: working.public_key().clone(),
            uses: vec![WorkingKeyUse::Decision],
            not_before: now
                .checked_sub_signed(TimeDelta::days(365))
                .assured("fixture clock is within chrono range"),
            not_after: now
                .checked_add_signed(TimeDelta::days(365))
                .assured("fixture clock is within chrono range"),
        };
        let mut signatures = Vec::new();
        for root in &roots[..2] {
            signatures.push(
                root.sign(&authorization, limits())
                    .change_context(ServiceError::Model)?,
            );
        }
        let documents = TrustDocuments {
            documents: vec![SignedTrustDocument::Signatures(SignatureSet { signatures })],
        };
        Ok(Self {
            notifications: notifications::Notifications::default(),
            bundle,
            requests: Vec::new(),
            request_count: 0,
            address,
            now,
            working,
            roots,
            documents,
            plan: default_plan(),
            conditions_revision: 1,
            challenges: BTreeMap::new(),
            activations: BTreeMap::new(),
            responses: BTreeMap::new(),
            faults: VecDeque::new(),
            refusal: None,
            license_key: Secret::new("NVXK-TESTABCD-fixture-secret".to_owned()),
        })
    }

    pub fn script(&mut self, fault: Fault) {
        assert!(self.faults.len() < MAX_ENTRIES);
        self.faults.push_back(fault);
    }
    pub fn refuse(&mut self, refusal: Option<ProblemKind>) {
        self.refusal = refusal;
    }
    pub fn advance(&mut self, duration: std::time::Duration) {
        let delta = TimeDelta::from_std(duration).assured("bounded fixture duration fits chrono");
        self.now = self
            .now
            .checked_add_signed(delta)
            .assured("scenario time remains within chrono range");
    }
    pub fn conditions(&self) -> Conditions {
        self.plan.clone()
    }
    pub fn set_time(&mut self, now: DateTime<Utc>) {
        self.now = self.now.max(now);
    }
    pub fn plan(&mut self, plan: Conditions) {
        Catalog::validate(&plan, true, false, false)
            .assured("scenario supplies a complete current plan");
        self.plan = plan;
        self.conditions_revision = self
            .conditions_revision
            .checked_add(1)
            .assured("a bounded scenario cannot exhaust revisions");
        self.notify_change();
    }
    pub fn activation_count(&self) -> usize {
        self.activations.len()
    }

    pub(crate) fn answer(&mut self, path: &str, bytes: &[u8]) -> (Response, Option<Fault>) {
        self.request_count = self
            .request_count
            .checked_add(1)
            .assured("bounded fixture cannot exhaust requests");
        if path == "/v1/challenges" {
            return (self.challenge(bytes), None);
        }
        let fault = self.faults.pop_front();
        let compact = match std::str::from_utf8(bytes) {
            Ok(value) => value,
            Err(_) => return (problem(ProblemKind::InvalidRequest, None), None),
        };
        let refresh = refresh_id(path);
        let signer = match refresh {
            None => RequestSigner::CarriedClusterKey,
            Some(id) => match self.activations.get(&id) {
                Some(activation) => RequestSigner::Bound(&activation.public_key),
                None => return (problem(ProblemKind::ActivationUnknown, None), None),
            },
        };
        let request = match Request::verify(compact, signer, limits()) {
            Ok(request) => request,
            Err(_) => return (problem(ProblemKind::SignatureInvalid, None), None),
        };
        if self.requests.len() >= MAX_ENTRIES {
            return (
                problem(
                    ProblemKind::Unavailable {
                        retry_after_seconds: 1,
                    },
                    None,
                ),
                None,
            );
        }
        self.requests.push(request.clone());
        let common = request.common();
        if let Some(Fault::Refuse(kind)) = &fault {
            return (problem(kind.clone(), None), None);
        }
        if let Some(kind) = &self.refusal {
            return (problem(kind.clone(), None), None);
        }
        let identity = RequestIdentity {
            cluster: common.cluster_uid,
            request: common.request_id,
        };
        let digest = request
            .digest(limits().bytes)
            .assured("the verified bounded request has a bounded identity");
        if let Some(saved) = self.responses.get(&identity) {
            if saved.digest == digest {
                return (
                    response(saved.status, saved.body.clone(), "application/json"),
                    fault,
                );
            }
            return (
                problem(ProblemKind::RequestConflict, Some(common.request_id)),
                None,
            );
        }
        if common.protocol != 1 || common.aud != self.address {
            return (
                problem(ProblemKind::InvalidRequest, Some(common.request_id)),
                None,
            );
        }
        let challenge = self.challenges.remove(common.challenge.expose());
        if !challenge.is_some_and(|challenge| {
            challenge.cluster == common.cluster_uid && challenge.until > self.now
        }) {
            return (
                problem(ProblemKind::ChallengeInvalid, Some(common.request_id)),
                None,
            );
        }
        let result = self.process(&request, refresh, fault.as_ref());
        let (status, body) = match result {
            Ok(answer) => (
                if refresh.is_some() { 200 } else { 201 },
                serde_json::to_vec(&answer).assured("typed fixture answer serializes"),
            ),
            Err(kind) => return (problem(kind, Some(identity.request)), None),
        };
        if matches!(fault, Some(Fault::Adopt)) && refresh.is_none() {
            // The fixture caps this inventory at MAX_ENTRIES, independently of the scenario.
            let activation_id = self
                .activations
                .values()
                .find(|activation| activation.binding.cluster_uid == common.cluster_uid)
                .verified("successful creation installed its binding")
                .binding
                .activation_id;
            return (
                problem(
                    ProblemKind::AlreadyActivated { activation_id },
                    Some(identity.request),
                ),
                None,
            );
        }
        assert!(
            self.responses.len() < MAX_ENTRIES,
            "scenario response inventory is bounded"
        );
        self.responses.insert(
            identity,
            SavedResponse {
                digest,
                status,
                body: body.clone(),
            },
        );
        (response(status, body, "application/json"), fault)
    }

    fn challenge(&mut self, bytes: &[u8]) -> Response {
        let request: ChallengeRequest = match serde_json::from_slice(bytes) {
            Ok(value) => value,
            Err(_) => return problem(ProblemKind::InvalidRequest, None),
        };
        self.challenges
            .retain(|_, challenge| challenge.until > self.now);
        if self.challenges.len() >= MAX_ENTRIES {
            return problem(
                ProblemKind::RateLimited {
                    retry_after_seconds: 1,
                },
                None,
            );
        }
        let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(random::bytes::<32>());
        let expires_at = self
            .now
            .checked_add_signed(TimeDelta::seconds(CHALLENGE_SECONDS))
            .assured("fixture challenge fits chrono range");
        self.challenges.insert(
            token.clone(),
            Challenge {
                cluster: request.cluster_uid,
                until: expires_at,
            },
        );
        json(
            200,
            &ChallengeResponse {
                challenge: Secret::new(token),
                expires_at,
            },
        )
    }

    fn process(
        &mut self,
        request: &Request,
        route: Option<Uuid>,
        fault: Option<&Fault>,
    ) -> Result<DecisionResponse, ProblemKind> {
        let common = request.common();
        let activation_id = match request {
            Request::Create {
                license_key,
                kind,
                cluster_key,
                ..
            } if route.is_none() => self.create(common, license_key, kind, cluster_key)?,
            Request::Refresh { activation_id, .. } if route == Some(*activation_id) => {
                self.refresh(common, activation_id, fault)?
            }
            _ => return Err(ProblemKind::InvalidRequest),
        };
        Ok(self.signed_response(activation_id, fault))
    }

    fn create(
        &mut self,
        common: &RequestCommon,
        license_key: &Secret,
        kind: &ActivationKind,
        cluster_key: &nervix_licensing_protocol::keys::PublicKey,
    ) -> Result<Uuid, ProblemKind> {
        if license_key != &self.license_key {
            return Err(ProblemKind::LicenseKeyInvalid);
        }
        // This scenario inventory has a fixed MAX_ENTRIES bound; it is never a product lookup.
        if let Some(activation) = self
            .activations
            .values()
            .find(|activation| activation.binding.cluster_uid == common.cluster_uid)
        {
            return Err(ProblemKind::AlreadyActivated {
                activation_id: activation.binding.activation_id,
            });
        }
        let allowed = self
            .plan
            .operating
            .sets
            .get(ACTIVATION_KINDS)
            .and_then(ConditionValue::known)
            .assured("validated plan has activation kinds");
        if !allowed.contains(kind) {
            return Err(ProblemKind::KindNotAllowed);
        }
        let quota_name = match kind {
            ActivationKind::Persistent => PERSISTENT_ACTIVATIONS,
            ActivationKind::Ephemeral => EPHEMERAL_ACTIVATIONS,
        };
        let quota = self
            .plan
            .operating
            .quotas
            .get(quota_name)
            .and_then(ConditionValue::known)
            .assured("validated plan has activation quota");
        let count = u64::try_from(
            self.activations
                .values()
                .filter(|held| held.binding.kind == *kind)
                .count(),
        )
        .assured("fixture activation bound fits u64");
        if !quota.admits(
            count
                .checked_add(1)
                .assured("fixture activation bound fits u64"),
        ) {
            return Err(ProblemKind::QuotaExhausted);
        }
        let activation_id = id();
        let binding = Activation {
            activation_id,
            cluster_uid: common.cluster_uid,
            cluster_key_kid: cluster_key
                .key_id()
                .assured("verified cluster key has a thumbprint"),
            generation: common.key_generation,
            kind: *kind,
            created_at: self.now,
            ephemeral_expires_at: match kind {
                ActivationKind::Persistent => None,
                ActivationKind::Ephemeral => Some(self.at(EPHEMERAL_LIFETIME)),
            },
        };
        let decision = self.decision(binding.clone(), common.request_id, 1);
        self.activations.insert(
            activation_id,
            HeldActivation {
                notification_revision: 0,
                binding,
                public_key: cluster_key.clone(),
                decision,
                prior_decision: None,
            },
        );
        Ok(activation_id)
    }

    fn refresh(
        &mut self,
        common: &RequestCommon,
        activation_id: &Uuid,
        fault: Option<&Fault>,
    ) -> Result<Uuid, ProblemKind> {
        let held = self
            .activations
            .get(activation_id)
            .ok_or(ProblemKind::ActivationUnknown)?;
        if held.binding.cluster_uid != common.cluster_uid {
            return Err(ProblemKind::BindingMismatch);
        }
        if held.binding.generation != common.key_generation {
            return Err(ProblemKind::GenerationMismatch);
        }
        let binding = held.binding.clone();
        let revision = held
            .decision
            .decision_revision
            .checked_add(1)
            .assured("fixture revisions are bounded");
        let decision = self.decision(binding, common.request_id, revision);
        if !matches!(fault, Some(Fault::Tamper(Tamper::PriorDecision))) {
            let held = self
                .activations
                .get_mut(activation_id)
                .verified("activation was read above");
            held.prior_decision = Some(std::mem::replace(&mut held.decision, decision));
        }
        Ok(*activation_id)
    }

    fn signed_response(&self, activation_id: Uuid, fault: Option<&Fault>) -> DecisionResponse {
        let held = self
            .activations
            .get(&activation_id)
            .verified("a creation or refresh holds its activation");
        let mut decision = held.decision.clone();
        if matches!(fault, Some(Fault::Tamper(Tamper::PriorDecision))) {
            decision = held
                .prior_decision
                .as_ref()
                .verified("prior-decision script follows a successful refresh")
                .clone();
        }
        let mut unauthorized = None;
        if let Some(Fault::Tamper(tamper)) = fault {
            match tamper {
                Tamper::Cluster => decision.activation.cluster_uid = id(),
                Tamper::Key => decision.activation.cluster_key_kid = self.working.key_id().clone(),
                Tamper::Generation => {
                    decision.activation.generation = decision
                        .activation
                        .generation
                        .checked_add(1)
                        .assured("bounded fixture generation")
                }
                Tamper::Activation => decision.activation.activation_id = id(),
                Tamper::Revision => decision.decision_revision = 0,
                Tamper::Timing => {
                    decision.timing.overdue_at = decision
                        .issued_at
                        .checked_sub_signed(TimeDelta::seconds(1))
                        .assured("fixture time is bounded")
                }
                Tamper::Service => {
                    decision.service =
                        Url::parse("https://other.invalid").assured("fixed other service URL")
                }
                Tamper::Request => decision.in_reply_to = id(),
                Tamper::UnauthorizedKey => {
                    unauthorized = Some(signer().assured("fixture signer generates"))
                }
                Tamper::Signature | Tamper::PriorDecision | Tamper::RevokedKey => {}
            }
        }
        let signing = unauthorized.as_ref().unwrap_or(&self.working);
        let mut compact = signing
            .sign(&decision, limits())
            .assured("bounded fixture decision signs");
        if matches!(fault, Some(Fault::Tamper(Tamper::Signature))) {
            let position = compact
                .rfind('.')
                .verified("the maintained encoder emitted compact JWS")
                .checked_add(1)
                .assured("a bounded document index fits usize");
            let replacement = if compact.as_bytes()[position] == b'A' {
                "B"
            } else {
                "A"
            };
            compact.replace_range(
                position..position.checked_add(1).assured("bounded signature index"),
                replacement,
            );
        }
        let mut trust = self.documents.clone();
        if matches!(fault, Some(Fault::Tamper(Tamper::RevokedKey))) {
            let revocation = WorkingKeyRevocation {
                doc: RevocationKind::WorkingKeyRevocation,
                protocol: 1,
                serial: 2,
                issued_at: self.now,
                root_epoch: 0,
                kid: self.working.key_id().clone(),
                revoked_at: self.now,
                reason: "scripted revocation".to_owned(),
            };
            let mut signatures = Vec::new();
            for root in &self.roots[..2] {
                signatures.push(
                    root.sign(&revocation, limits())
                        .assured("bounded fixture revocation signs"),
                );
            }
            trust
                .documents
                .push(SignedTrustDocument::Signatures(SignatureSet { signatures }));
        }
        DecisionResponse {
            decision: compact,
            trust,
            server_time: self.now,
        }
    }

    fn at(&self, condition: &str) -> DateTime<Utc> {
        let duration = self
            .plan
            .operating
            .duration(condition)
            .assured("validated fixture plan supplies duration");
        let ConditionDuration::Bounded { seconds } = duration else {
            panic!("fixture requires a bounded duration for {condition}");
        };
        self.now
            .checked_add_signed(TimeDelta::seconds(
                i64::try_from(seconds).assured("fixture duration fits i64"),
            ))
            .assured("fixture deadline fits chrono")
    }

    fn decision(&self, binding: Activation, request: Uuid, revision: u64) -> Decision {
        let term_starts_at = self
            .now
            .checked_sub_signed(TimeDelta::days(1))
            .assured("fixture term fits chrono");
        let term_ends_at = self
            .now
            .checked_add_signed(TimeDelta::days(30))
            .assured("fixture term fits chrono");
        let expiry = binding.ephemeral_expires_at;
        let lifetime = match expiry {
            Some(expires_at) => ActivationLifetime::Ephemeral { expires_at },
            None => ActivationLifetime::Persistent,
        };
        let resolved = self
            .plan
            .resolve(
                &empty_conditions(),
                ResolutionFacts {
                    organization_status: OrganizationStatus::Active,
                    license_status: LicenseStatus::Active,
                    term_starts_at,
                    term_ends_at,
                    activation_status: ActivationStatus::Active,
                    created_at: binding.created_at,
                    lifetime,
                    at: self.now,
                },
                limits().bytes,
            )
            .assured("fixture facts resolve through shared protocol");
        let refresh_after = self.at(REFRESH_INTERVAL);
        let overdue_duration = match self
            .plan
            .operating
            .duration(REFRESH_OVERDUE)
            .assured("validated overdue duration")
        {
            ConditionDuration::Bounded { seconds } => seconds,
            ConditionDuration::Unbounded => panic!("overdue duration is bounded"),
        };
        let overdue_at = refresh_after
            .checked_add_signed(TimeDelta::seconds(
                i64::try_from(overdue_duration).assured("fixture duration fits i64"),
            ))
            .assured("fixture deadline fits chrono");
        let valid_until = match self
            .plan
            .operating
            .duration(OFFLINE_ALLOWANCE)
            .assured("validated offline allowance")
        {
            ConditionDuration::Unbounded => None,
            ConditionDuration::Bounded { .. } => Some(self.at(OFFLINE_ALLOWANCE)),
        };
        Decision {
            doc: DecisionKind::LicenseDecision,
            protocol: 1,
            decision_id: id(),
            decision_revision: revision,
            issued_at: self.now,
            service: self.address.clone(),
            activation: binding,
            license: License {
                license_id: Uuid::from_u128(1),
                organization_id: Uuid::from_u128(2),
                term_starts_at,
                term_ends_at,
                plan: PlanVersion {
                    plan_code: "scenario".to_owned(),
                    version: 1,
                },
                assignment_revision: self.conditions_revision,
            },
            status: resolved.status(),
            conditions: DecisionConditions {
                conditions_revision: self.conditions_revision,
                usage: resolved.values().usage.clone(),
                operating: resolved.values().operating.clone(),
            },
            timing: DecisionTiming {
                refresh_after,
                overdue_at,
                valid_until,
                term_ends_at,
                ephemeral_expires_at: expiry,
            },
            transitions: Vec::new(),
            notices: Vec::new(),
            in_reply_to: request,
        }
    }
}

fn empty_conditions() -> Conditions {
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

fn refresh_id(path: &str) -> Option<Uuid> {
    let path = path.strip_prefix("/v1/activations/")?;
    let id = path.strip_suffix("/refresh")?;
    Uuid::parse_str(id).ok()
}

fn default_plan() -> Conditions {
    let mut plan = empty_conditions();
    plan.usage = FREE_POLICY.usage();
    for (name, value) in [(CLUSTER_NODES, 16), (CLUSTER_DOMAINS, 64)] {
        plan.usage.quotas.insert(
            name.to_owned(),
            ConditionValue::Known(Quota::Bounded { value }),
        );
    }
    for name in [PERSISTENT_ACTIVATIONS, EPHEMERAL_ACTIVATIONS] {
        plan.operating.quotas.insert(
            name.to_owned(),
            ConditionValue::Known(Quota::Bounded { value: 16 }),
        );
    }
    for (name, seconds) in [
        (EPHEMERAL_LIFETIME, 86_400),
        (REFRESH_INTERVAL, 600),
        (REFRESH_OVERDUE, 1_200),
        (CAPABILITY_GRACE, 300),
        (QUOTA_GRACE, 300),
    ] {
        plan.operating.durations.insert(
            name.to_owned(),
            ConditionValue::Known(ConditionDuration::Bounded { seconds }),
        );
    }
    plan.operating.durations.insert(
        OFFLINE_ALLOWANCE.to_owned(),
        ConditionValue::Known(ConditionDuration::Unbounded),
    );
    plan.operating.policies.insert(
        OFFLINE_LAPSE.to_owned(),
        ConditionValue::Known(OfflineLapsePolicy::Freeze),
    );
    plan.operating.sets.insert(
        ACTIVATION_KINDS.to_owned(),
        ConditionValue::Known(vec![ActivationKind::Persistent, ActivationKind::Ephemeral]),
    );
    plan
}

fn response(status: u16, body: Vec<u8>, content_type: &'static str) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .header("cache-control", "no-store")
        .body(Body::from(body))
        .assured("fixture statuses and headers are valid HTTP")
}
fn json(status: u16, value: &impl Serialize) -> Response {
    response(
        status,
        serde_json::to_vec(value).assured("typed fixture response serializes"),
        "application/json",
    )
}
fn problem(kind: ProblemKind, request_id: Option<Uuid>) -> Response {
    let status = kind.code().status();
    json_problem(
        status,
        &ProblemDetails {
            title: kind.code().title().to_owned(),
            detail: kind.code().as_str().to_owned(),
            kind,
            request_id,
        },
    )
}
fn json_problem(status: u16, details: &ProblemDetails) -> Response {
    response(
        status,
        serde_json::to_vec(details).assured("shared problem details serialize"),
        "application/problem+json",
    )
}
