//! Stateful rotation, release and transfer for the bounded signed protocol peer.
//!
//! Layer: test harness.
//! - **Owns.** Bound route verification, retired-key replay and terminal activation capacity.
//! - **Depends on.** The peer transaction owner and shared cryptographic protocol.
//! - **Must not know.** Cluster consensus, policy implementation or scenario assertions.

use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Route {
    Create,
    Refresh(Uuid),
    RotateKey(Uuid),
    Release(Uuid),
}

impl Route {
    pub(super) fn parse(path: &str) -> Option<Self> {
        if path == "/v1/activations" {
            return Some(Self::Create);
        }
        let (id, operation) = path.strip_prefix("/v1/activations/")?.split_once('/')?;
        let id = Uuid::parse_str(id).ok()?;
        match operation {
            "refresh" => Some(Self::Refresh(id)),
            "key" => Some(Self::RotateKey(id)),
            "release" => Some(Self::Release(id)),
            _ => None,
        }
    }
}

pub(super) struct RetiredBinding {
    key: nervix_licensing_protocol::keys::PublicKey,
    generation: u64,
    request_id: Uuid,
}

impl HeldActivation {
    pub(super) fn holds_capacity(&self, at: DateTime<Utc>) -> bool {
        self.decision.status == DecisionStatus::Active
            || self.capacity_until.is_some_and(|until| until > at)
    }
}

impl Model {
    pub fn reduce_domain_quota_with_grace(&mut self) {
        let mut plan = self.conditions();
        plan.usage.quotas.insert(
            CLUSTER_DOMAINS.to_owned(),
            ConditionValue::Known(Quota::Bounded { value: 1 }),
        );
        self.plan(plan);
        self.transition_usage = Some(nervix_licensing_protocol::impact::UsageSnapshot {
            cluster_nodes: 3,
            cluster_domains: 2,
        });
    }
    pub(super) fn verify_request(
        &self,
        compact: &str,
        route: Route,
    ) -> error_stack::Result<Request, PeerRefusal> {
        let id = match route {
            Route::Create => {
                return Request::verify(compact, RequestSigner::CarriedClusterKey, limits())
                    .change_context(PeerRefusal {
                        kind: ProblemKind::SignatureInvalid,
                    });
            }
            Route::Refresh(id) | Route::RotateKey(id) | Route::Release(id) => id,
        };
        let held = self
            .activations
            .get(&id)
            .ok_or_else(|| PeerRefusal::report(ProblemKind::ActivationUnknown))?;
        let request =
            match Request::verify(compact, RequestSigner::Bound(&held.public_key), limits()) {
                Ok(request) => request,
                Err(error) if matches!(route, Route::RotateKey(_)) => {
                    let retired = match &held.retired {
                        Some(retired) => retired,
                        None => {
                            return Err(error.change_context(PeerRefusal {
                                kind: ProblemKind::SignatureInvalid,
                            }));
                        }
                    };
                    let request =
                        Request::verify(compact, RequestSigner::Bound(&retired.key), limits())
                            .change_context(PeerRefusal {
                                kind: ProblemKind::SignatureInvalid,
                            })?;
                    let common = request.common();
                    let identity = RequestIdentity {
                        cluster: common.cluster_uid,
                        request: common.request_id,
                    };
                    if common.request_id != retired.request_id
                        || common.key_generation != retired.generation
                        || !self.responses.contains_key(&identity)
                    {
                        return Err(PeerRefusal::report(ProblemKind::BindingMismatch));
                    }
                    request
                }
                Err(error) => {
                    return Err(error.change_context(PeerRefusal {
                        kind: ProblemKind::SignatureInvalid,
                    }));
                }
            };
        let matches = match &request {
            Request::Refresh { activation_id, .. } => route == Route::Refresh(*activation_id),
            Request::RotateKey { activation_id, .. } => route == Route::RotateKey(*activation_id),
            Request::Release { activation_id, .. } => route == Route::Release(*activation_id),
            _ => false,
        };
        if !matches || request.common().cluster_uid != held.binding.cluster_uid {
            return Err(PeerRefusal::report(ProblemKind::BindingMismatch));
        }
        Ok(request)
    }

    pub(super) fn rotate_key(
        &mut self,
        common: &RequestCommon,
        activation_id: Uuid,
        state: &CommittedState,
        new_key: &nervix_licensing_protocol::keys::PublicKey,
        proof: &str,
    ) -> error_stack::Result<Uuid, PeerRefusal> {
        let held = self
            .activations
            .get_mut(&activation_id)
            .ok_or_else(|| PeerRefusal::report(ProblemKind::ActivationUnknown))?;
        if held.binding.generation != common.key_generation {
            return Err(PeerRefusal::report(ProblemKind::GenerationMismatch));
        }
        if held.decision.status != DecisionStatus::Active
            || state.applied_decision_revision != held.decision.decision_revision
        {
            return Err(PeerRefusal::report(ProblemKind::BindingMismatch));
        }
        let proof: KeyRotationProof =
            verify(proof, new_key, limits()).change_context(PeerRefusal {
                kind: ProblemKind::ProofInvalid,
            })?;
        let kid = new_key.key_id().change_context(PeerRefusal {
            kind: ProblemKind::ProofInvalid,
        })?;
        if proof.challenge != common.challenge
            || proof.cluster_uid != common.cluster_uid
            || proof.activation_id != activation_id
            || proof.new_kid != kid
        {
            return Err(PeerRefusal::report(ProblemKind::ProofInvalid));
        }
        held.retired = Some(RetiredBinding {
            key: std::mem::replace(&mut held.public_key, new_key.clone()),
            generation: held.binding.generation,
            request_id: common.request_id,
        });
        held.binding.generation = held
            .binding
            .generation
            .checked_add(1)
            .assured("bounded scenario generation");
        held.binding.cluster_key_kid = kid;
        let mut decision = held.decision.clone();
        decision.activation = held.binding.clone();
        decision.decision_id = id();
        decision.decision_revision = decision
            .decision_revision
            .checked_add(1)
            .assured("bounded scenario revision");
        decision.issued_at = self.now;
        decision.in_reply_to = common.request_id;
        held.prior_decision = Some(std::mem::replace(&mut held.decision, decision));
        Ok(activation_id)
    }

    pub(super) fn release(
        &mut self,
        common: &RequestCommon,
        activation_id: Uuid,
        grants_until: DateTime<Utc>,
    ) -> Result<Uuid, ProblemKind> {
        let issued_at = self.now;
        let held = self
            .activations
            .get_mut(&activation_id)
            .ok_or(ProblemKind::ActivationUnknown)?;
        if held.binding.generation != common.key_generation {
            return Err(ProblemKind::GenerationMismatch);
        }
        let refreshed =
            super::timing::RefreshTiming::new(issued_at, &held.decision.conditions.operating);
        let mut decision = held.decision.clone();
        decision.decision_id = id();
        decision.decision_revision = decision
            .decision_revision
            .checked_add(1)
            .assured("bounded scenario revision");
        decision.conditions.conditions_revision = decision
            .conditions
            .conditions_revision
            .checked_add(1)
            .assured("bounded scenario revision");
        decision.conditions.usage = FREE_POLICY.usage();
        decision.status = DecisionStatus::Released;
        decision.issued_at = issued_at;
        decision.timing.refresh_after = refreshed.refresh_after;
        decision.timing.overdue_at = refreshed.overdue_at;
        decision.timing.valid_until = refreshed.valid_until;
        decision
            .transitions
            .retain(|transition| transition.deadline > issued_at);
        decision.in_reply_to = common.request_id;
        held.capacity_until = (grants_until > self.now).then_some(grants_until);
        held.prior_decision = Some(std::mem::replace(&mut held.decision, decision));
        Ok(activation_id)
    }

    /// An administrator explicitly ends the former binding and releases its slot.
    pub fn transfer_activations(&mut self) {
        for held in self.activations.values_mut() {
            held.decision.status = DecisionStatus::Transferred;
            held.decision.conditions.conditions_revision = held
                .decision
                .conditions
                .conditions_revision
                .checked_add(1)
                .assured("bounded scenario revision");
            held.decision.conditions.usage = FREE_POLICY.usage();
            held.capacity_until = None;
        }
        self.notify_change();
    }

    pub fn active_activation_count(&self) -> usize {
        self.activations
            .values()
            .filter(|activation| activation.decision.status == DecisionStatus::Active)
            .count()
    }
}
