//! Quota limits configured by a licensing scenario before its nodes start.
//!
//! Layer: test harness.
//! - **Owns.** One shared pair of bounded cluster quotas for a scenario fixture.
//! - **Depends on.** Shared quota names and the fault handle's publication primitive.
//! - **Must not know.** Admission, policy evaluation, consensus or service requests.

use nervix_licensing_vocabulary::{Quota, QuotaName};

use super::FaultInjection;

fn index(name: QuotaName) -> usize {
    match name {
        QuotaName::ClusterNodes => 0,
        QuotaName::ClusterDomains => 1,
    }
}

impl FaultInjection {
    pub fn set_licensing_quota(&self, name: QuotaName, limit: u64) {
        self.inner
            .licensing_quotas
            .send_modify(|quotas| quotas[index(name)] = Some(limit));
    }

    pub(crate) fn licensing_quota(&self, name: QuotaName, quota: Quota) -> Quota {
        match self.inner.licensing_quotas.borrow()[index(name)] {
            Some(value) => Quota::Bounded { value },
            None => quota,
        }
    }
}
