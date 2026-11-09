//! The production TLS publication and cache owner under competing configuration capture.
//!
//! Outside the layer order: test harness.
//! - **Owns.** Coherent pairs, immutable captured snapshots and eventual cache replacement.
//! - **Depends on.** The production policy/resolver, selected threads and the model runner.
//! - **Must not know.** Network handshakes, library-internal memory ordering or reference counts.

use meticulous::ResultExt as _;
use nervix_model_harness::shuttle::check_dfs;
use nervix_primitives::thread;
use rustls::client::ResolvesClientConfig as _;

use super::*;

#[test]
fn shuttle_capture_racing_publication_keeps_one_complete_pair_and_the_retained_snapshot() {
    check_dfs(
        || {
            let policy = OutwardTlsPolicy::default();
            let template = StdArc::new(
                ClientConfig::builder_with_provider(policy.provider())
                    .with_safe_default_protocol_versions()
                    .assured("the default policy supports TLS")
                    .with_root_certificates(rustls::RootCertStore::empty())
                    .with_no_client_auth(),
            );
            let resolver = Arc::new(ClientSelection {
                policy: policy.clone(),
                cache: Mutex::new(None),
            });
            let retained = resolver
                .resolve(&template)
                .assured("the initial policy compiles");
            let suites =
                TlsCipherSuites::resolve(["TLS13_CHACHA20_POLY1305_SHA256"]).assured("known");
            let groups = TlsKeyExchangeGroups::resolve(["X25519"]).assured("known");
            let initial_suites = retained.crypto_provider().cipher_suites.clone();
            let initial_groups = retained
                .crypto_provider()
                .kx_groups
                .iter()
                .map(|g| g.name())
                .collect::<Vec<_>>();
            let writer_policy = policy.clone();
            let writer = thread::spawn(move || writer_policy.install(suites, groups));
            let reader_resolver = resolver.clone();
            let reader_template = template.clone();
            let reader = thread::spawn(move || {
                reader_resolver
                    .resolve(&reader_template)
                    .assured("either pair compiles")
            });
            writer.join().assured("the bounded writer completes");
            let captured = reader.join().assured("the bounded reader completes");
            let captured_suites = &captured.crypto_provider().cipher_suites;
            let captured_groups = captured
                .crypto_provider()
                .kx_groups
                .iter()
                .map(|g| g.name())
                .collect::<Vec<_>>();
            let next_suites =
                vec![rustls::crypto::aws_lc_rs::cipher_suite::TLS13_CHACHA20_POLY1305_SHA256];
            let next_groups = vec![rustls::NamedGroup::X25519];
            assert!(
                (captured_suites == &initial_suites && captured_groups == initial_groups)
                    || (captured_suites == &next_suites && captured_groups == next_groups)
            );
            let after = resolver
                .resolve(&template)
                .assured("capture after completed publication uses the new pair");
            assert_eq!(after.crypto_provider().cipher_suites, next_suites);
            assert_eq!(
                after
                    .crypto_provider()
                    .kx_groups
                    .iter()
                    .map(|g| g.name())
                    .collect::<Vec<_>>(),
                next_groups
            );
            assert_eq!(retained.crypto_provider().cipher_suites, initial_suites);
            assert_eq!(
                retained
                    .crypto_provider()
                    .kx_groups
                    .iter()
                    .map(|g| g.name())
                    .collect::<Vec<_>>(),
                initial_groups
            );
        },
        None,
    );
}
