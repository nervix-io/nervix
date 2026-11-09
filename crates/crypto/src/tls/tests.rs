//! The provider choices, typed membership refusals and process-lifetime ownership.
//!
//! Layer: test harness.
//! - **Owns.** Assertions over the cryptography boundary's TLS policy.
//! - **Depends on.** The production providers and rustls's algorithm identities.
//! - **Must not know.** Network topology, credentials or graph execution.

use meticulous::{OptionExt as _, ResultExt as _};

use super::*;

#[test]
fn interconnect_keeps_exactly_the_approved_algorithms_in_provider_order() {
    let provider = interconnect_provider().assured("the fixed interconnect policy is valid");
    let outward = outward_tls_provider().rustls();
    let suites: Vec<_> = provider
        .cipher_suites
        .iter()
        .map(|suite| suite.suite())
        .collect();
    let groups: Vec<_> = provider
        .kx_groups
        .iter()
        .map(|group| group.name())
        .collect();
    let mut expected_suites = Vec::new();
    for suite in &outward.cipher_suites {
        if INTERCONNECT_SUITES.contains(&suite.suite()) {
            expected_suites.push(suite.suite());
        }
    }
    let mut expected_groups = Vec::new();
    for group in &outward.kx_groups {
        if INTERCONNECT_GROUPS.contains(&group.name()) {
            expected_groups.push(group.name());
        }
    }
    assert_eq!(suites, expected_suites);
    assert_eq!(groups, expected_groups);
    assert_eq!(suites.len(), INTERCONNECT_SUITES.len());
    assert_eq!(groups.len(), INTERCONNECT_GROUPS.len());
}

#[test]
fn outward_tls_preserves_the_complete_aws_lc_default() {
    let provider = outward_tls_provider().rustls();
    let default = aws_lc_rs::default_provider();
    assert_eq!(provider.cipher_suites, default.cipher_suites);
    assert_eq!(
        provider
            .kx_groups
            .iter()
            .map(|group| group.name())
            .collect::<Vec<_>>(),
        default
            .kx_groups
            .iter()
            .map(|group| group.name())
            .collect::<Vec<_>>(),
    );
}

#[test]
fn interconnect_reuses_its_process_provider() {
    let first = interconnect_provider().assured("the fixed policy is valid");
    let second = interconnect_provider().assured("the same fixed policy is valid");
    assert!(StdArc::ptr_eq(first, second));
}

#[test]
fn default_installation_keeps_the_provider_installed_first() {
    if CryptoProvider::get_default().is_none() {
        let mut host = outward_tls_provider().rustls();
        host.cipher_suites.truncate(1);
        host.install_default()
            .discarded("rustls retains whichever process provider installed first");
    }
    let first = StdArc::clone(CryptoProvider::get_default().assured("rustls installed a provider"));
    install_default_provider();
    install_default_provider();
    assert!(StdArc::ptr_eq(
        &first,
        CryptoProvider::get_default().assured("the installed process provider stays present"),
    ));
}

#[test]
fn membership_refusals_keep_the_exact_algorithm() {
    let mut suites = interconnect_provider()
        .assured("the fixed policy is valid")
        .as_ref()
        .clone();
    suites
        .cipher_suites
        .push(aws_lc_rs::cipher_suite::TLS13_CHACHA20_POLY1305_SHA256);
    let Err(error) = validate_interconnect_algorithms(&suites) else {
        panic!("ChaCha20 is outside this policy");
    };
    assert_eq!(
        error.current_context(),
        &InterconnectAlgorithmError::CipherSuite {
            suite: CipherSuite::TLS13_CHACHA20_POLY1305_SHA256,
        }
    );
    let mut groups = interconnect_provider()
        .assured("the fixed policy is valid")
        .as_ref()
        .clone();
    groups.kx_groups.push(aws_lc_rs::kx_group::X25519);
    let Err(error) = validate_interconnect_algorithms(&groups) else {
        panic!("standalone X25519 is outside this policy");
    };
    assert_eq!(
        error.current_context(),
        &InterconnectAlgorithmError::KeyExchangeGroup {
            group: NamedGroup::X25519,
        }
    );
}

#[test]
fn an_empty_algorithm_selection_is_a_typed_failure() {
    let mut provider = interconnect_provider()
        .assured("the fixed policy is valid")
        .as_ref()
        .clone();
    provider.cipher_suites.clear();
    let Err(error) = validate_interconnect_algorithms(&provider) else {
        panic!("a provider needs suites");
    };
    assert_eq!(
        error.current_context(),
        &InterconnectAlgorithmError::NoCipherSuites
    );
    let mut provider = interconnect_provider()
        .assured("the fixed policy is valid")
        .as_ref()
        .clone();
    provider.kx_groups.clear();
    let Err(error) = validate_interconnect_algorithms(&provider) else {
        panic!("a provider needs groups");
    };
    assert_eq!(
        error.current_context(),
        &InterconnectAlgorithmError::NoKeyExchangeGroups
    );
}

#[test]
fn native_clients_reuse_the_explicit_fips_provider() {
    let first = fips_provider();
    assert!(StdArc::ptr_eq(&first, &fips_provider()));
    assert!(first.cipher_suites.iter().all(|suite| !matches!(
        suite.suite(),
        CipherSuite::TLS13_CHACHA20_POLY1305_SHA256
            | CipherSuite::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256
            | CipherSuite::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256
    )));
    assert!(
        first
            .kx_groups
            .iter()
            .all(|group| group.name() != NamedGroup::X25519)
    );
}
