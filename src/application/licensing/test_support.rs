//! Construction-time test deployment inputs for licensing.
//!
//! Layer: test harness.
//! - **Owns.** A fixture address, embedded anchors and its additional TLS root path.
//! - **Depends on.** The shared bundle and immutable consensus environment.
//! - **Must not know.** Service implementation, policy, requests or graph execution.

#[cfg(feature = "testing")]
use meticulous::{OptionExt as _, ResultExt as _};

#[cfg(feature = "testing")]
#[derive(Clone, Debug, Default)]
pub struct LicensingTestSupport {
    deployment: Option<TestDeployment>,
}

#[cfg(feature = "testing")]
#[derive(Clone, Debug)]
struct TestDeployment {
    address: url::Url,
    bundle: nervix_licensing_protocol::documents::EmbeddedTrustBundle,
    ca_file: String,
}

#[cfg(feature = "testing")]
impl LicensingTestSupport {
    pub fn new(
        address: url::Url,
        bundle: nervix_licensing_protocol::documents::EmbeddedTrustBundle,
        ca_file: String,
    ) -> Self {
        Self {
            deployment: Some(TestDeployment {
                address,
                bundle,
                ca_file,
            }),
        }
    }
    pub(crate) fn environment(&self, bytes: u64) -> nervix_consensus::LicensingEnvironment {
        match &self.deployment {
            Some(deployment) => nervix_consensus::LicensingEnvironment::testing(
                deployment.address.clone(),
                deployment.bundle.clone(),
                std::num::NonZeroUsize::new(
                    usize::try_from(bytes).assured("validated executor bytes fit usize"),
                )
                .verified("management byte capacity is positive"),
            ),
            None => nervix_consensus::LicensingEnvironment::production(bytes),
        }
    }
    pub(crate) fn tls_entries(&self) -> Vec<nervix_models::ClientConfigEntry> {
        match &self.deployment {
            Some(deployment) => vec![nervix_models::ClientConfigEntry {
                key: "tls_ca_file".to_owned(),
                value: deployment.ca_file.clone(),
            }],
            None => Vec::new(),
        }
    }
}

#[cfg(not(feature = "testing"))]
#[derive(Clone, Debug, Default)]
pub struct ConfiguredLicensingTestSupport {
    _marker: (),
}

#[cfg(feature = "testing")]
pub type ConfiguredLicensingTestSupport = LicensingTestSupport;

#[cfg(not(feature = "testing"))]
impl ConfiguredLicensingTestSupport {
    pub(crate) fn environment(&self, bytes: u64) -> nervix_consensus::LicensingEnvironment {
        nervix_consensus::LicensingEnvironment::production(bytes)
    }
    pub(crate) fn tls_entries(&self) -> Vec<nervix_models::ClientConfigEntry> {
        Vec::new()
    }
}
