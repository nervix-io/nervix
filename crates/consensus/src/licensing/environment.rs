//! Immutable verification inputs fixed before a consensus store opens.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The compiled deployment binding, trust anchors and admitted document work bounds.
//! - **Depends on.** The protocol's build bundle and the executor's declared byte capacity.
//! - **Must not know.** Requests, clocks, graph state or operational licensing policy.

use std::num::NonZeroUsize;

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_licensing_protocol::{
    document::DocumentLimits,
    documents::{EmbeddedTrustBundle, TrustDocuments},
    trust::TrustState,
};
use url::Url;

use super::activation::LicenseDecisionError;

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the licensing protocol binds production decisions to this compiled service \
                  deployment"
    )
)]
pub const LICENSING_SERVICE: &str = "https://licensing.nervix.io";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LicensingEnvironment {
    service: Url,
    bundle: Option<EmbeddedTrustBundle>,
    document_bytes: NonZeroUsize,
}

impl Default for LicensingEnvironment {
    fn default() -> Self {
        Self::production(
            nervix_execution::OperationLimits::default()
                .management_event_bytes
                .as_u64(),
        )
    }
}

impl LicensingEnvironment {
    /// Document work is charged to the same management-event capacity on every member.
    pub fn production(document_bytes: u64) -> Self {
        Self {
            service: Url::parse(LICENSING_SERVICE)
                .assured("compiled service is an absolute HTTPS URL"),
            bundle: nervix_licensing_protocol::trust::PRODUCTION_TRUST_BUNDLE.clone(),
            document_bytes: NonZeroUsize::new(
                usize::try_from(document_bytes)
                    .assured("the executor's validated management capacity fits this host"),
            )
            .assured("executor management capacity is nonzero"),
        }
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn testing(
        service: Url,
        bundle: EmbeddedTrustBundle,
        document_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            service,
            bundle: Some(bundle),
            document_bytes,
        }
    }

    pub fn service(&self) -> &Url {
        &self.service
    }
    pub fn is_provisioned(&self) -> bool {
        self.bundle.is_some()
    }

    pub fn limits(&self) -> DocumentLimits {
        // Each signature and each retained document must occupy at least one input byte. The
        // aggregate input byte budget therefore also bounds both work lists, with no second policy.
        DocumentLimits {
            bytes: self.document_bytes,
            signatures: self.document_bytes,
            trust_documents: self.document_bytes,
        }
    }

    pub(super) fn trust(
        &self,
        documents: &TrustDocuments,
    ) -> error_stack::Result<TrustState, LicenseDecisionError> {
        let bundle = self
            .bundle
            .as_ref()
            .ok_or_else(|| Report::new(LicenseDecisionError::Trust))?;
        TrustState::build(bundle, documents, self.limits())
            .change_context(LicenseDecisionError::Trust)
    }
}
