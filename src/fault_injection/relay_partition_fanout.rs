//! The first buffered batch of one relay partition's owner held before its fan-out, while the
//! owners of the relay's other partitions fan out as usual, for the public relay partition
//! scenarios.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The relay partitions armed to hold their owner's next fan-out, the one batch each
//!   hold claims, and its reached and released observations.
//! - **Depends on.** Vocabulary identities and the shared pause barriers.
//! - **Must not know.** Relay owner tasks, dispatch permits, routes, buffers or consumers.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "owns test relay partition fan-out holds and their observations"
    )
)]

use nervix_models::{DomainName, RelayName, Unit};
use nervix_primitives::sync::{Arc, atomic::Ordering};

use super::{FaultInjection, pause::TestPause};

/// One partition of one relay of a domain, as a scenario names it. The domain is kept lowercase,
/// as every domain-keyed fault of the fault injection module keeps it.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(super) struct OwnerRelayPartitionFanoutPauseKey {
    domain: String,
    relay: String,
    partition: u32,
}

impl OwnerRelayPartitionFanoutPauseKey {
    fn new(domain: &str, relay: &str, partition: u32) -> Self {
        Self {
            domain: domain.to_ascii_lowercase(),
            relay: relay.to_string(),
            partition,
        }
    }
}

impl FaultInjection {
    /// Pauses the first batch the owner of `partition` of `relay` fans out, before it acquires a
    /// dispatch permit. The owners of the relay's other partitions keep fanning out.
    pub fn pause_owner_relay_partition_fanout(&self, domain: &str, relay: &str, partition: u32) {
        self.inner.owner_relay_partition_fanout_pauses.insert(
            OwnerRelayPartitionFanoutPauseKey::new(domain, relay, partition),
            Arc::new(TestPause::default()),
        );
    }

    pub async fn wait_for_owner_relay_partition_fanout_pause(
        &self,
        domain: &str,
        relay: &str,
        partition: u32,
    ) {
        let pause = self.owner_relay_partition_fanout_pause(
            &OwnerRelayPartitionFanoutPauseKey::new(domain, relay, partition),
        );
        pause.wait_until_reached().await;
    }

    pub fn release_owner_relay_partition_fanout_pause(
        &self,
        domain: &str,
        relay: &str,
        partition: u32,
    ) {
        let pause = self.owner_relay_partition_fanout_pause(
            &OwnerRelayPartitionFanoutPauseKey::new(domain, relay, partition),
        );
        pause.release();
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "one scenario-owned pause of one relay partition's owner fan-out is claimed \
                      once",
            key = "one test domain, relay and partition",
            bound = "one keyed read; its guard ends before the test-controlled wait"
        )
    )]
    pub(crate) async fn pause_owner_relay_partition_fanout_if_armed(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        unit: Unit,
    ) {
        let Unit::Partition(partition) = unit else {
            return;
        };
        let key =
            OwnerRelayPartitionFanoutPauseKey::new(domain.as_str(), relay.as_str(), partition);
        let Some(pause) = self
            .inner
            .owner_relay_partition_fanout_pauses
            .get(&key)
            .map(|pause| pause.value().clone())
        else {
            return;
        };
        if pause.claimed.swap(true, Ordering::AcqRel) {
            return;
        }
        pause.reach();
        pause.wait_until_released().await;
    }

    fn owner_relay_partition_fanout_pause(
        &self,
        key: &OwnerRelayPartitionFanoutPauseKey,
    ) -> Arc<TestPause> {
        let Some(pause) = self.inner.owner_relay_partition_fanout_pauses.get(key) else {
            panic!(
                "relay owner fan-out pause for partition {} of relay '{}' in domain '{}' is not \
                 armed",
                key.partition, key.relay, key.domain
            );
        };
        pause.value().clone()
    }
}
