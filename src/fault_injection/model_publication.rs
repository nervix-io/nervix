//! A leader's refusal of a transaction model step's publication and of the pause release after it,
//! for public pause recovery scenarios.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The domain-scoped, one-shot refusal of the next model step publication and of the
//!   domain pause release its coordinator attempts next, how the leader refuses them, and the
//!   record of whether both were delivered.
//! - **Depends on.** Vocabulary identities.
//! - **Must not know.** Model planning, transaction progress, consensus or command recovery.

use nervix_models::DomainName;

use super::FaultInjection;

/// How a leader refuses the writes a model publication refusal covers, before appending them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumString, strum::AsRefStr)]
pub enum ModelPublicationRefusalKind {
    /// A leader whose lease lapsed refuses the writes while it still leads, naming no leader.
    #[strum(serialize = "a lapsed leader lease")]
    LapsedLease,
    /// A leader whose retained log reached its cap refuses the writes once reclamation has not
    /// kept up.
    #[strum(serialize = "a saturated consensus log")]
    SaturatedLog,
}

/// How far the refusal armed for a domain's next model step publication has come.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelPublicationRefusalStage {
    /// The next model step publication in the domain is refused.
    Armed,
    /// The publication was refused, and the next pause release in the domain is refused too.
    ReleaseArmed,
    /// Both refusals were delivered.
    Delivered,
}

/// The refusal armed for a domain's next model step publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ModelPublicationRefusal {
    kind: ModelPublicationRefusalKind,
    stage: ModelPublicationRefusalStage,
}

impl FaultInjection {
    /// Makes the leader refuse the next transaction model step publication in `domain`, and the
    /// domain pause release its coordinator attempts after it, the way `kind` names.
    pub fn refuse_next_model_publication(
        &self,
        domain: DomainName,
        kind: ModelPublicationRefusalKind,
    ) {
        self.inner.refused_model_publications.insert(
            domain,
            ModelPublicationRefusal {
                kind,
                stage: ModelPublicationRefusalStage::Armed,
            },
        );
    }

    /// Whether both refusals armed for `domain`'s next model step publication were delivered.
    pub fn model_publication_refusal_delivered(&self, domain: &DomainName) -> bool {
        let Some(refusal) = self.inner.refused_model_publications.get(domain) else {
            return false;
        };
        refusal.stage == ModelPublicationRefusalStage::Delivered
    }

    /// Delivers the refusal armed for `domain`'s next model step publication once, and arms the
    /// refusal of the pause release that follows it.
    pub(crate) fn take_model_publication_refusal(
        &self,
        domain: &DomainName,
    ) -> Option<ModelPublicationRefusalKind> {
        let mut refusal = self.inner.refused_model_publications.get_mut(domain)?;
        if refusal.stage != ModelPublicationRefusalStage::Armed {
            return None;
        }
        refusal.stage = ModelPublicationRefusalStage::ReleaseArmed;
        Some(refusal.kind)
    }

    /// Delivers the refusal of the pause release that follows a refused model step publication in
    /// `domain` once, and records that both refusals were delivered.
    pub(crate) fn take_model_pause_release_refusal(
        &self,
        domain: &DomainName,
    ) -> Option<ModelPublicationRefusalKind> {
        let mut refusal = self.inner.refused_model_publications.get_mut(domain)?;
        if refusal.stage != ModelPublicationRefusalStage::ReleaseArmed {
            return None;
        }
        refusal.stage = ModelPublicationRefusalStage::Delivered;
        Some(refusal.kind)
    }
}
