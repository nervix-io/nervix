//! Compile the pure TLS catalog into AWS-LC implementations.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Algorithm implementation selection in caller preference order.
//! - **Depends on.** The pure crypto catalog and rustls AWS-LC.
//! - **Must not know.** Models, nodes, publication or connections.

use meticulous::OptionExt as _;
use nervix_crypto_policy::{TlsCipherSuites, TlsKeyExchangeGroups};
use rustls::crypto::{CryptoProvider, aws_lc_rs};

/// Compile in operator preference order. Catalog coverage is checked against the provider by
/// unit tests; a missing implementation is a build defect, never an operator input refusal.
pub fn algorithm_provider(suites: TlsCipherSuites, groups: TlsKeyExchangeGroups) -> CryptoProvider {
    CryptoProvider {
        cipher_suites: suites
            .iter()
            .map(|selected| {
                *aws_lc_rs::ALL_CIPHER_SUITES
                    .iter()
                    .find(|suite| u16::from(suite.suite()) == selected.iana())
                    .assured("every declared suite has an AWS-LC implementation")
            })
            .collect(),
        kx_groups: groups
            .iter()
            .map(|selected| {
                *aws_lc_rs::ALL_KX_GROUPS
                    .iter()
                    .find(|group| u16::from(group.name()) == selected.iana())
                    .assured("every declared group has an AWS-LC implementation")
            })
            .collect(),
        ..aws_lc_rs::default_provider()
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    #[test]
    fn every_catalog_member_compiles_in_requested_order() {
        let suites = TlsCipherSuites::resolve(["default"]).assured("the catalog alias exists");
        let groups = TlsKeyExchangeGroups::resolve(["default"]).assured("the catalog alias exists");
        let provider = algorithm_provider(suites, groups);
        assert_eq!(
            provider
                .cipher_suites
                .iter()
                .map(|s| u16::from(s.suite()))
                .collect::<Vec<_>>(),
            suites.iter().map(|s| s.iana()).collect::<Vec<_>>()
        );
        assert_eq!(
            provider
                .kx_groups
                .iter()
                .map(|g| u16::from(g.name()))
                .collect::<Vec<_>>(),
            groups.iter().map(|g| g.iana()).collect::<Vec<_>>()
        );
    }
}
