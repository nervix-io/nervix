//! TLS providers selected by their Nervix purpose.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Outward provider selection, rustls default installation and interconnect algorithms.
//! - **Depends on.** Rustls's AWS-LC provider, primitive shared ownership and process initialization.
//! - **Must not know.** Certificates, trust stores, network clients, Models or cluster settings.

use error_stack::Report;
use nervix_primitives::{sync::StdArc, unmodeled::sync::LazyLock};
use nervix_recovery::Discarded as _;
use rustls::{
    CipherSuite, NamedGroup,
    crypto::{CryptoProvider, aws_lc_rs},
};
use thiserror::Error;

mod policy;
pub use policy::algorithm_provider;

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "FIPS-approved TLS 1.3 AES-GCM suites; interconnect prerequisites cannot depend \
                  on cluster parameters"
    )
)]
const INTERCONNECT_SUITES: [CipherSuite; 2] = [
    CipherSuite::TLS13_AES_256_GCM_SHA384,
    CipherSuite::TLS13_AES_128_GCM_SHA256,
];

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "SP 800-56A P-256/P-384 and rustls 0.23.46's FIPS-classified post-quantum-first \
                  ML-KEM hybrid; interconnect prerequisites are fixed"
    )
)]
const INTERCONNECT_GROUPS: [NamedGroup; 3] = [
    NamedGroup::secp256r1,
    NamedGroup::secp384r1,
    NamedGroup::X25519MLKEM768,
];

/// The outward choice a TLS stack projects into its own provider selector.
///
/// Stacks that accept rustls directly use `rustls`; SDKs retain their own algorithm filtering
/// when mapping this choice into their native mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutwardTlsProvider {
    AwsLc,
}

impl OutwardTlsProvider {
    /// The full provider, preserving AWS-LC's algorithms and preference order.
    pub fn rustls(self) -> CryptoProvider {
        match self {
            Self::AwsLc => aws_lc_rs::default_provider(),
        }
    }
}

pub const fn outward_tls_provider() -> OutwardTlsProvider {
    OutwardTlsProvider::AwsLc
}

/// Install the outward process default, preserving a host's or concurrent caller's installation.
/// Rustls owns the installation protocol; the fast path constructs no provider or allocation.
pub fn install_default_provider() {
    if CryptoProvider::get_default().is_some() {
        return;
    }
    outward_tls_provider().rustls().install_default().discarded(
        "rustls retains the provider installed first by the host or a concurrent caller",
    );
}

static FIPS_CLIENT_PROVIDER: LazyLock<StdArc<CryptoProvider>> = LazyLock::new(|| {
    StdArc::new(algorithm_provider(
        nervix_crypto_policy::TlsCipherSuites::default(),
        nervix_crypto_policy::TlsKeyExchangeGroups::default(),
    ))
});

/// The explicit immutable default for native tools and bindings, shared across their channels.
/// An embedding host's process default does not replace this selection.
pub fn fips_provider() -> StdArc<CryptoProvider> {
    StdArc::clone(&FIPS_CLIENT_PROVIDER)
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum InterconnectAlgorithmError {
    #[error("the interconnect provider has no cipher suites")]
    NoCipherSuites,
    #[error("the interconnect provider has no key exchange groups")]
    NoKeyExchangeGroups,
    #[error("cipher suite {suite:?} is outside the interconnect algorithm policy")]
    CipherSuite { suite: CipherSuite },
    #[error("key exchange group {group:?} is outside the interconnect algorithm policy")]
    KeyExchangeGroup { group: NamedGroup },
}

/// Check algorithm membership without claiming that an ordinary AWS-LC build is FIPS-certified.
fn validate_interconnect_algorithms(
    provider: &CryptoProvider,
) -> Result<(), Report<InterconnectAlgorithmError>> {
    if provider.cipher_suites.is_empty() {
        return Err(Report::new(InterconnectAlgorithmError::NoCipherSuites));
    }
    if provider.kx_groups.is_empty() {
        return Err(Report::new(InterconnectAlgorithmError::NoKeyExchangeGroups));
    }
    for suite in &provider.cipher_suites {
        // The policy has exactly two suites, independent of graph or cluster size.
        if !INTERCONNECT_SUITES.contains(&suite.suite()) {
            return Err(Report::new(InterconnectAlgorithmError::CipherSuite {
                suite: suite.suite(),
            }));
        }
    }
    for group in &provider.kx_groups {
        // The policy has exactly three groups, independent of graph or cluster size.
        if !INTERCONNECT_GROUPS.contains(&group.name()) {
            return Err(Report::new(InterconnectAlgorithmError::KeyExchangeGroup {
                group: group.name(),
            }));
        }
    }
    Ok(())
}

static INTERCONNECT_PROVIDER: LazyLock<StdArc<CryptoProvider>> = LazyLock::new(|| {
    let mut provider = outward_tls_provider().rustls();
    provider
        .cipher_suites
        .retain(|suite| INTERCONNECT_SUITES.contains(&suite.suite()));
    provider
        .kx_groups
        .retain(|group| INTERCONNECT_GROUPS.contains(&group.name()));
    StdArc::new(provider)
});

/// The immutable checked provider shared by every interconnect client/server bundle.
pub fn interconnect_provider()
-> Result<&'static StdArc<CryptoProvider>, Report<InterconnectAlgorithmError>> {
    let provider = &*INTERCONNECT_PROVIDER;
    validate_interconnect_algorithms(provider)?;
    Ok(provider)
}

#[cfg(test)]
mod tests;
