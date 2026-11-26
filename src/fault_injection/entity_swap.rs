//! Fault controls inside one node's entity swap, at the point where the swap replaces a named
//! entity: its previous runtime, if this node ran one, has stopped, a processor's having handed its
//! branches over, and its replacement, if this node runs one, has not started yet.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The swap faults a scenario arms for one node and domain: a pause that holds one swap
//!   at that point, and a failure that makes every swap fail there until the scenario clears it,
//!   which it refuses for an ingestor, whose step cannot fail once its change commits.
//! - **Depends on.** Vocabulary identities and the shared test pause.
//! - **Must not know.** Schedules, swap ordering, execution state or the runtime.

#![cfg_attr(
    nervix_lint,
    nervix::context(outside, reason = "owns test swap controls for one node and domain")
)]

use nervix_models::{ClusterNodeName, DomainName, ModelKind, NodeRef};
use nervix_primitives::sync::Arc;

use super::{FaultInjection, TestPause};

/// The node and domain whose entity swaps a fault applies to.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(super) struct EntitySwapFaultKey {
    node: ClusterNodeName,
    domain: DomainName,
}

impl EntitySwapFaultKey {
    fn new(node: &ClusterNodeName, domain: &DomainName) -> Self {
        Self {
            node: node.clone(),
            domain: domain.clone(),
        }
    }
}

/// A pause that holds the next swap while it replaces `entity`.
#[derive(Debug, Clone)]
pub(super) struct EntitySwapReplacementPause {
    entity: NodeRef,
    pause: Arc<TestPause>,
}

impl FaultInjection {
    /// Makes every entity swap of `domain` on `node` fail while it replaces `entity`, until
    /// [`Self::clear_entity_swap_failures_on`].
    ///
    /// An ingestor's step only stops its units, and nothing after a change commits may fail it,
    /// so a failure cannot be armed for an ingestor: a scenario that arms one tests a state a
    /// committed change never reaches.
    pub fn fail_entity_swaps_while_replacing_on(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
        entity: NodeRef,
    ) {
        assert_ne!(
            entity.kind,
            ModelKind::Ingestor,
            "an ingestor's step of a committed change cannot fail, so no swap failure is armed \
             for ingestor '{}'",
            entity.identifier.as_str()
        );
        self.inner
            .entity_swap_replacement_failures
            .insert(EntitySwapFaultKey::new(node, domain), entity);
    }

    /// Lets the entity swaps of `domain` on `node` run without the failure armed above.
    pub fn clear_entity_swap_failures_on(&self, node: &ClusterNodeName, domain: &DomainName) {
        self.inner
            .entity_swap_replacement_failures
            .remove(&EntitySwapFaultKey::new(node, domain));
    }

    /// Holds the next entity swap of `domain` on `node` while it replaces `entity`, until the
    /// scenario releases it.
    pub fn pause_entity_swap_while_replacing_on(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
        entity: NodeRef,
    ) {
        self.inner.entity_swap_replacement_pauses.insert(
            EntitySwapFaultKey::new(node, domain),
            EntitySwapReplacementPause {
                entity,
                pause: Arc::new(TestPause::default()),
            },
        );
    }

    /// Waits until the swap armed above holds at its replacement pause.
    pub async fn wait_for_entity_swap_replacement_pause(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
    ) {
        let pause = self.entity_swap_replacement_pause(node, domain);
        pause.wait_until_reached().await;
    }

    /// Releases the swap held at its replacement pause.
    pub fn release_entity_swap_replacement_pause(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
    ) {
        let pause = self.entity_swap_replacement_pause(node, domain);
        pause.release();
    }

    fn entity_swap_replacement_pause(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
    ) -> Arc<TestPause> {
        let Some(armed) = self
            .inner
            .entity_swap_replacement_pauses
            .get(&EntitySwapFaultKey::new(node, domain))
        else {
            panic!(
                "no entity swap replacement pause is armed for domain '{}' on node '{}'",
                domain.as_str(),
                node.as_str()
            );
        };
        armed.pause.clone()
    }

    /// Holds a swap of `domain` on `node` that is replacing `entity`, when a scenario armed that
    /// pause, until the scenario releases it. The pause holds one swap.
    pub(crate) async fn pause_entity_swap_while_replacing_if_armed(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
        entity: &NodeRef,
    ) {
        let key = EntitySwapFaultKey::new(node, domain);
        let pause = match self.inner.entity_swap_replacement_pauses.get(&key) {
            Some(armed) if armed.entity == *entity => armed.pause.clone(),
            Some(_) | None => return,
        };
        pause.reach();
        pause.wait_until_released().await;
        self.inner.entity_swap_replacement_pauses.remove(&key);
    }

    /// Whether a swap of `domain` on `node` that is replacing `entity` fails there.
    pub(crate) fn entity_swap_fails_while_replacing(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
        entity: &NodeRef,
    ) -> bool {
        let key = EntitySwapFaultKey::new(node, domain);
        match self.inner.entity_swap_replacement_failures.get(&key) {
            Some(armed) => *armed == *entity,
            None => false,
        }
    }
}
