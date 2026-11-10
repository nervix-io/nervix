//! Domain creation's licensing admission boundary.
//!
//! Layer: control plane.
//! - **Owns.** Capturing the published quota and retaining typed refusals under domain context.
//! - **Depends on.** The policy handle, committed usage observations and domain vocabulary.
//! - **Must not know.** Quota arithmetic, storage encoding or service connectivity.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns domain licensing admission")
)]

use error_stack::Report;
use nervix_models::DomainName;
use thiserror::Error;

use super::session_service::SessionServiceImpl;
use crate::licensing::{Quota, QuotaName};

#[derive(Debug, Error)]
#[error("cannot admit creation of domain '{domain}'")]
pub(in crate::application) struct DomainLicensingAdmissionError {
    domain: DomainName,
}

impl SessionServiceImpl {
    pub(in crate::application) fn domain_quota(&self) -> Quota {
        self.inner.licensing.quota(QuotaName::ClusterDomains)
    }

    pub(in crate::application) fn admit_domain_creation(
        &self,
        domain: &DomainName,
        replacement: bool,
    ) -> Result<Quota, Report<DomainLicensingAdmissionError>> {
        let usage = self
            .inner
            .consensus
            .observer()
            .licensing_snapshot()
            .usage
            .cluster_domains;
        match self
            .inner
            .licensing
            .admit(QuotaName::ClusterDomains, usage, 1, replacement)
        {
            Ok(constraint) => Ok(constraint.quota),
            Err(report) => Err(report.change_context(DomainLicensingAdmissionError {
                domain: domain.clone(),
            })),
        }
    }
}
