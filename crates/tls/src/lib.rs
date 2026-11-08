//! Immutable outward algorithm snapshots captured by new TLS connections.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Publishing node-owned algorithm pairs and caching connection configurations.
//! - **Depends on.** The crypto boundary, rustls and primitive publication/locks.
//! - **Must not know.** Models, consensus, listeners or connector lifecycles.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns TLS snapshot compilation and publication")
)]

use nervix_crypto::tls::algorithm_provider;
use nervix_crypto_policy::{TlsCipherSuites, TlsKeyExchangeGroups};
use nervix_primitives::{
    publication::ArcSwap,
    sync::{Arc, StdArc, blocking::Mutex},
};
use rustls::{ClientConfig, ServerConfig, crypto::CryptoProvider};

#[derive(Debug)]
struct Algorithms {
    suites: TlsCipherSuites,
    groups: TlsKeyExchangeGroups,
    provider: StdArc<CryptoProvider>,
}

/// A node-owned coherent pair. Publication does not close connections or reconstruct drivers.
#[derive(Clone)]
pub struct OutwardTlsPolicy(Arc<ArcSwap<Algorithms>>);

impl std::fmt::Debug for OutwardTlsPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OutwardTlsPolicy")
            .field("algorithms", &self.0.load())
            .finish()
    }
}

impl Default for OutwardTlsPolicy {
    fn default() -> Self {
        Self::new(TlsCipherSuites::default(), TlsKeyExchangeGroups::default())
    }
}

impl OutwardTlsPolicy {
    pub fn new(suites: TlsCipherSuites, groups: TlsKeyExchangeGroups) -> Self {
        Self(Arc::new(ArcSwap::from_pointee(Algorithms {
            suites,
            groups,
            provider: StdArc::new(algorithm_provider(suites, groups)),
        })))
    }

    pub fn install(&self, suites: TlsCipherSuites, groups: TlsKeyExchangeGroups) {
        let current = self.0.load();
        if current.suites == suites && current.groups == groups {
            return;
        }
        self.0.store(StdArc::new(Algorithms {
            suites,
            groups,
            provider: StdArc::new(algorithm_provider(suites, groups)),
        }));
    }

    pub fn provider(&self) -> StdArc<CryptoProvider> {
        self.0.load().provider.clone()
    }

    pub fn client_config(&self, mut config: ClientConfig) -> ClientConfig {
        config.handshake_config_resolver = Some(rustls::client::ClientConfigResolver::new(
            StdArc::new(ClientSelection {
                policy: self.clone(),
                cache: Mutex::new(None),
            }),
        ));
        config
    }

    pub fn server_config(&self, mut config: ServerConfig) -> ServerConfig {
        config.handshake_config_resolver = Some(rustls::server::ServerConfigResolver::new(
            StdArc::new(ServerSelection {
                policy: self.clone(),
                cache: Mutex::new(None),
            }),
        ));
        config
    }
}

macro_rules! selection {
    ($name:ident, $config:ident, $trait:path) => {
        #[derive(Debug)]
        struct $name {
            policy: OutwardTlsPolicy,
            // At most one immutable snapshot per template; connections retain what they borrow.
            cache: Mutex<Option<(StdArc<Algorithms>, StdArc<$config>)>>,
        }
        impl $trait for $name {
            fn resolve(
                &self,
                template: &StdArc<$config>,
            ) -> Result<StdArc<$config>, rustls::Error> {
                let algorithms = self.policy.0.load_full();
                // The original template already has this provider. Reuse it without a lock or
                // configuration clone. Never cache the template itself: it owns this resolver.
                if StdArc::ptr_eq(template.crypto_provider(), &algorithms.provider) {
                    return Ok(template.clone());
                }
                let mut cache = self.cache.lock();
                if let Some((applied, config)) = cache.as_ref()
                    && StdArc::ptr_eq(applied, &algorithms)
                {
                    return Ok(config.clone());
                }
                let config = StdArc::new(
                    template
                        .as_ref()
                        .clone()
                        .with_crypto_provider(algorithms.provider.clone())?,
                );
                *cache = Some((algorithms, config.clone()));
                Ok(config)
            }
            fn clone_for_config(&self) -> StdArc<dyn $trait> {
                StdArc::new(Self {
                    policy: self.policy.clone(),
                    cache: Mutex::new(None),
                })
            }
        }
    };
}

selection!(
    ClientSelection,
    ClientConfig,
    rustls::client::ResolvesClientConfig
);
selection!(
    ServerSelection,
    ServerConfig,
    rustls::server::ResolvesServerConfig
);

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use meticulous::ResultExt as _;
    use rustls::client::ResolvesClientConfig as _;

    use super::*;

    #[test]
    fn configuration_cache_is_bounded_and_keeps_template_fields_after_publication() {
        let policy = OutwardTlsPolicy::default();
        let mut config = ClientConfig::builder_with_provider(policy.provider())
            .with_safe_default_protocol_versions()
            .assured("the default algorithms support TLS")
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        config.alpn_protocols = vec![b"h2".to_vec()];
        let template = StdArc::new(config);
        let resolver = ClientSelection {
            policy: policy.clone(),
            cache: Mutex::new(None),
        };
        let first = resolver
            .resolve(&template)
            .assured("the complete policy compiles");
        assert!(StdArc::ptr_eq(&first, &template));
        assert!(resolver.cache.lock().is_none());
        assert!(StdArc::ptr_eq(
            &first,
            &resolver.resolve(&template).assured("cached")
        ));
        let suites = TlsCipherSuites::resolve(["TLS13_CHACHA20_POLY1305_SHA256"]).assured("known");
        let groups = TlsKeyExchangeGroups::resolve(["X25519"]).assured("known");
        policy.install(suites, groups);
        let next = resolver
            .resolve(&template)
            .assured("the next complete policy compiles");
        assert!(!StdArc::ptr_eq(&first, &next));
        assert_eq!(next.alpn_protocols, template.alpn_protocols);
        assert_eq!(next.crypto_provider().cipher_suites.len(), 1);
        assert!(first.crypto_provider().cipher_suites.len() > 1);
        policy.install(suites, groups);
        assert!(StdArc::ptr_eq(
            &next,
            &resolver.resolve(&template).assured("same policy")
        ));
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;
