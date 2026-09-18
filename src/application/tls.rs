//! The certificates every listener and the interconnect present, and when they are reloaded.
//!
//! Layer: edges.
//!
//! - **Owns.** Reading TLS material from disk, the per-vhost SNI resolver every node's HTTPS
//!   listener presents, and the reloads that pick up a rotated certificate without a restart.
//! - **Depends on.** The resource store for vhost material and the admitted runtime state for the
//!   vhosts.
//! - **Must not know.** What travels over a connection once it is established.

use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    sync::Arc as StdArc,
};

use blake3::Hasher;
use error_stack::{Report, ResultExt as _};
use nervix_interconnect::{
    HandlerRegistrationError, HttpsListenerInstallation, HttpsListenerInstallationRequest,
    HttpsListenerInstallationResponse, TlsConfigBundle, Transport,
};
#[cfg(not(feature = "testing"))]
use nervix_models::ClusterNodeName;
use nervix_models::{ClusterSchedule, DomainName, Model, ResourceId, ResourceName, VhostName};
use parking_lot::RwLock;
use rustls::{
    RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
    server::ResolvesServerCertUsingSni,
    sign::CertifiedKey,
};
use rustls_pki_types::pem::{Error as PemError, PemObject};
use thiserror::Error;
use tokio::{
    sync::Mutex as AsyncMutex,
    time::{Duration, interval},
};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Identity as TonicIdentity, ServerTlsConfig};
use tracing::{info, warn};
use triomphe::Arc;

use super::AppError;
use crate::{ConfiguredFaultInjection, cluster::ClusterHandle, resource::ResourceStore};

const INTERCONNECT_TLS_RELOAD_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(in crate::application) struct InterconnectTlsPaths {
    pub(in crate::application) ca: PathBuf,
    pub(in crate::application) certificate: PathBuf,
    pub(in crate::application) private_key: PathBuf,
}

pub(in crate::application) struct InterconnectTlsMaterial {
    ca: Vec<u8>,
    certificate: Vec<u8>,
    private_key: Vec<u8>,
}

impl InterconnectTlsPaths {
    pub(in crate::application) async fn read(&self) -> io::Result<InterconnectTlsMaterial> {
        let ca = tokio::fs::read(&self.ca).await?;
        let certificate = tokio::fs::read(&self.certificate).await?;
        let private_key = tokio::fs::read(&self.private_key).await?;
        Ok(InterconnectTlsMaterial {
            ca,
            certificate,
            private_key,
        })
    }
}

impl InterconnectTlsMaterial {
    pub(in crate::application) fn fingerprint(&self) -> blake3::Hash {
        let mut hasher = Hasher::new();
        hasher.update(b"interconnect-ca\0");
        hasher.update(&self.ca);
        hasher.update(b"interconnect-certificate\0");
        hasher.update(&self.certificate);
        hasher.update(b"interconnect-private-key\0");
        hasher.update(&self.private_key);
        hasher.finalize()
    }

    pub(in crate::application) fn tls_bundle(
        &self,
    ) -> Result<TlsConfigBundle, Report<nervix_interconnect::TlsConfigError>> {
        TlsConfigBundle::from_pem(&self.ca, &self.certificate, &self.private_key)
    }
}

pub(in crate::application) async fn reload_interconnect_tls(
    transport: Transport,
    paths: InterconnectTlsPaths,
    initial_fingerprint: blake3::Hash,
    shutdown: CancellationToken,
) {
    let mut applied_fingerprint = initial_fingerprint;
    let mut pending_fingerprint = None;
    let mut reported_failure = None;
    let mut ticker = interval(INTERCONNECT_TLS_RELOAD_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::task::consume_budget().await;
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = ticker.tick() => {}
        }

        let material = match paths.read().await {
            Ok(material) => material,
            Err(error) => {
                let failure = error.to_string();
                if reported_failure.as_ref() != Some(&failure) {
                    warn!(error = %error, "failed to read replacement interconnect TLS files");
                    reported_failure = Some(failure);
                }
                pending_fingerprint = None;
                continue;
            }
        };
        let fingerprint = material.fingerprint();
        if fingerprint == applied_fingerprint {
            pending_fingerprint = None;
            reported_failure = None;
            continue;
        }
        if pending_fingerprint != Some(fingerprint) {
            pending_fingerprint = Some(fingerprint);
            reported_failure = None;
            continue;
        }

        let replacement = match material.tls_bundle() {
            Ok(replacement) => replacement,
            Err(error) => {
                let failure = error.to_string();
                if reported_failure.as_ref() != Some(&failure) {
                    warn!(error = %error, "replacement interconnect TLS files are invalid");
                    reported_failure = Some(failure);
                }
                continue;
            }
        };
        if let Err(error) = transport.replace_tls(replacement).await {
            let failure = error.to_string();
            if reported_failure.as_ref() != Some(&failure) {
                warn!(error = %error, "failed to replace interconnect TLS credentials");
                reported_failure = Some(failure);
            }
            continue;
        }

        applied_fingerprint = fingerprint;
        pending_fingerprint = None;
        reported_failure = None;
        info!("reloaded interconnect TLS credentials");
    }
}

const VHOST_TLS_CERT_PATH: &str = "tls.crt";

const VHOST_TLS_KEY_PATH: &str = "tls.key";

const VHOST_TLS_CA_PATH: &str = "ca.crt";

pub(in crate::application) const INTERNAL_TLS_CA_FILE: &str = "ca.pem";

const INTERNAL_TLS_CERT_FILE: &str = "node.pem";

const INTERNAL_TLS_KEY_FILE: &str = "node-key.pem";

/// Why a file a model reads from a resource version cannot be opened.
#[derive(Debug, Error)]
pub(in crate::application) enum ResourceFileError {
    #[error("{label} file '{}' does not exist", path.display())]
    Missing {
        label: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    #[error("{label} path '{}' is not a file", path.display())]
    NotAFile { label: &'static str, path: PathBuf },
}

/// Why TLS material read from disk cannot be presented.
#[derive(Debug, Error)]
pub(in crate::application) enum TlsMaterialError {
    #[error("the {file} path cannot be resolved inside the resource")]
    ResolvePath { file: &'static str },
    #[error("a required TLS file is unavailable")]
    Unavailable,
    #[error("no certificates found in '{}'", path.display())]
    NoCertificates { path: PathBuf },
    #[error("no PEM items found in '{}'", path.display())]
    NoPemItems { path: PathBuf },
    #[error("failed to read PEM file '{}': {source}", path.display())]
    Pem { path: PathBuf, source: PemError },
    #[error("'{}' contains an unusable CA certificate: {source}", path.display())]
    CaCertificate {
        path: PathBuf,
        source: rustls::Error,
    },
    #[error("the certificate chain and private key do not form a usable key: {source}")]
    CertifiedKey { source: rustls::Error },
    #[error("the rustls crypto provider is not installed")]
    CryptoProvider,
}

impl TlsMaterialError {
    /// The PEM failure reading `path`, keeping a file without PEM items apart from a malformed one.
    fn pem(path: &Path, source: PemError) -> Report<Self> {
        match source {
            PemError::NoItemsFound => Report::new(Self::NoPemItems {
                path: path.to_path_buf(),
            }),
            source => Report::new(Self::Pem {
                path: path.to_path_buf(),
                source,
            }),
        }
    }
}

/// Why this node's HTTPS listener could not present the TLS VHOSTs of a runtime state.
#[derive(Debug, Error)]
pub(in crate::application) enum HttpsListenerError {
    #[error(
        "failed to load TLS resource '{resource}@{version}' for VHOST '{vhost}' in domain \
         '{domain}'"
    )]
    LoadTlsResource {
        domain: DomainName,
        vhost: VhostName,
        resource: ResourceName,
        version: u64,
    },
    #[error(
        "failed to present TLS hostname '{hostname}' for VHOST '{vhost}' in domain '{domain}': \
         {source}"
    )]
    Hostname {
        domain: DomainName,
        vhost: VhostName,
        hostname: String,
        source: rustls::Error,
    },
    #[error("the fault injector failed the HTTPS listener installation")]
    FaultInjected,
}

pub(in crate::application) fn internal_tls_path(file_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tls")
        .join("dev")
        .join(file_name)
}

pub(in crate::application) fn load_web_console_tls_server_config(
    cert_path: &Path,
    key_path: &Path,
) -> Result<StdArc<ServerConfig>, Report<AppError>> {
    nervix_interconnect::install_rustls_crypto_provider();
    let cert_chain =
        load_certificates_from_pem_file(cert_path).change_context(AppError::LoadWebConsoleTls)?;
    let private_key =
        load_private_key_from_pem_file(key_path).change_context(AppError::LoadWebConsoleTls)?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)
        .map_err(|error| {
            Report::new(AppError::LoadWebConsoleTls).attach_printable(error.to_string())
        })?;
    Ok(StdArc::new(config))
}

pub(in crate::application) async fn load_grpc_tls_server_config()
-> Result<ServerTlsConfig, Report<AppError>> {
    nervix_interconnect::install_rustls_crypto_provider();
    let cert_path = internal_tls_path(INTERNAL_TLS_CERT_FILE);
    let key_path = internal_tls_path(INTERNAL_TLS_KEY_FILE);
    let cert_pem = tokio::fs::read(cert_path)
        .await
        .map_err(|error| Report::new(AppError::LoadGrpcTls).attach_printable(error.to_string()))?;
    let key_pem = tokio::fs::read(key_path)
        .await
        .map_err(|error| Report::new(AppError::LoadGrpcTls).attach_printable(error.to_string()))?;
    Ok(ServerTlsConfig::new().identity(TonicIdentity::from_pem(cert_pem, key_pem)))
}

/// Loads the certificate a TLS VHOST presents from one version of its bundle, after proving the
/// bundle's CA certificate parses too.
pub(in crate::application) async fn load_vhost_tls_materials(
    resource_store: &ResourceStore,
    id: &ResourceId,
) -> Result<CertifiedKey, Report<TlsMaterialError>> {
    let cert_path = resource_store
        .resolve_content_path(id, VHOST_TLS_CERT_PATH)
        .change_context(TlsMaterialError::ResolvePath {
            file: VHOST_TLS_CERT_PATH,
        })?;
    let key_path = resource_store
        .resolve_content_path(id, VHOST_TLS_KEY_PATH)
        .change_context(TlsMaterialError::ResolvePath {
            file: VHOST_TLS_KEY_PATH,
        })?;
    let ca_path = resource_store
        .resolve_content_path(id, VHOST_TLS_CA_PATH)
        .change_context(TlsMaterialError::ResolvePath {
            file: VHOST_TLS_CA_PATH,
        })?;

    ensure_file_exists(&cert_path, "tls certificate")
        .await
        .change_context(TlsMaterialError::Unavailable)?;
    ensure_file_exists(&key_path, "tls private key")
        .await
        .change_context(TlsMaterialError::Unavailable)?;
    ensure_file_exists(&ca_path, "tls CA certificate")
        .await
        .change_context(TlsMaterialError::Unavailable)?;

    load_root_store_from_pem_file(&ca_path)?;
    let cert_chain = load_certificates_from_pem_file(&cert_path)?;
    let private_key = load_private_key_from_pem_file(&key_path)?;
    let Some(provider) = rustls::crypto::CryptoProvider::get_default() else {
        return Err(Report::new(TlsMaterialError::CryptoProvider));
    };
    CertifiedKey::from_der(cert_chain, private_key, provider)
        .map_err(|source| Report::new(TlsMaterialError::CertifiedKey { source }))
}

pub(in crate::application) async fn ensure_file_exists(
    path: &Path,
    label: &'static str,
) -> Result<(), Report<ResourceFileError>> {
    let metadata = tokio::fs::metadata(path).await.map_err(|source| {
        Report::new(ResourceFileError::Missing {
            label,
            path: path.to_path_buf(),
            source,
        })
    })?;
    if metadata.is_file() {
        Ok(())
    } else {
        Err(Report::new(ResourceFileError::NotAFile {
            label,
            path: path.to_path_buf(),
        }))
    }
}

fn load_root_store_from_pem_file(path: &Path) -> Result<RootCertStore, Report<TlsMaterialError>> {
    let certs = load_certificates_from_pem_file(path)?;
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert).map_err(|source| {
            Report::new(TlsMaterialError::CaCertificate {
                path: path.to_path_buf(),
                source,
            })
        })?;
    }
    Ok(roots)
}

fn load_certificates_from_pem_file(
    path: &Path,
) -> Result<Vec<CertificateDer<'static>>, Report<TlsMaterialError>> {
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(|source| TlsMaterialError::pem(path, source))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TlsMaterialError::pem(path, source))?;
    if certs.is_empty() {
        return Err(Report::new(TlsMaterialError::NoCertificates {
            path: path.to_path_buf(),
        }));
    }
    Ok(certs)
}

fn load_private_key_from_pem_file(
    path: &Path,
) -> Result<PrivateKeyDer<'static>, Report<TlsMaterialError>> {
    PrivateKeyDer::from_pem_file(path).map_err(|source| TlsMaterialError::pem(path, source))
}

/// Which VHOST of which domain a listener entry presents.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ListenerVhostKey {
    domain: DomainName,
    vhost: VhostName,
}

/// What the HTTPS listener presents for one TLS VHOST.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ListenerVhost {
    hostnames: Vec<String>,
    resource: ResourceId,
}

/// The TLS VHOSTs of every domain in one runtime state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ListenerVhosts {
    vhosts: BTreeMap<ListenerVhostKey, ListenerVhost>,
}

impl ListenerVhosts {
    /// Every TLS VHOST the schedule defines. A stopped domain keeps its schedule, so the listener
    /// presents its VHOSTs too.
    fn from_schedule(schedule: &ClusterSchedule) -> Self {
        let mut vhosts = BTreeMap::new();
        for (domain, domain_schedule) in &schedule.domains {
            for node in domain_schedule.nodes.values() {
                let Model::Vhost(vhost) = node.config.as_ref() else {
                    continue;
                };
                let Some(tls) = vhost.tls.as_ref() else {
                    continue;
                };
                let key = ListenerVhostKey {
                    domain: domain.clone(),
                    vhost: vhost.name.clone(),
                };
                let presented = ListenerVhost {
                    hostnames: vhost.hostnames.clone(),
                    resource: ResourceId::new(domain.clone(), tls.resource.clone(), tls.version),
                };
                vhosts.insert(key, presented);
            }
        }
        Self { vhosts }
    }

    /// The server configuration presenting these VHOSTs, absent when there is none to present.
    async fn server_config(
        &self,
        resource_store: &ResourceStore,
    ) -> Result<Option<StdArc<ServerConfig>>, Report<HttpsListenerError>> {
        if self.vhosts.is_empty() {
            return Ok(None);
        }
        nervix_interconnect::install_rustls_crypto_provider();
        let mut resolver = ResolvesServerCertUsingSni::new();
        for (key, presented) in &self.vhosts {
            tokio::task::consume_budget().await;
            let certified_key = load_vhost_tls_materials(resource_store, &presented.resource)
                .await
                .change_context_lazy(|| HttpsListenerError::LoadTlsResource {
                    domain: key.domain.clone(),
                    vhost: key.vhost.clone(),
                    resource: presented.resource.identifier.clone(),
                    version: presented.resource.version,
                })?;
            for hostname in &presented.hostnames {
                resolver
                    .add(hostname, certified_key.clone())
                    .map_err(|source| {
                        Report::new(HttpsListenerError::Hostname {
                            domain: key.domain.clone(),
                            vhost: key.vhost.clone(),
                            hostname: hostname.clone(),
                            source,
                        })
                    })?;
            }
        }
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(StdArc::new(resolver));
        Ok(Some(StdArc::new(config)))
    }
}

#[cfg(not(feature = "testing"))]
impl ConfiguredFaultInjection {
    /// A build without the test harness arms no listener failure.
    fn take_armed_https_listener_installation_failure(&self, _node_id: &ClusterNodeName) -> bool {
        false
    }
}

/// What the HTTPS listener presents and how its latest installation ended.
struct ListenerInstallation {
    /// The TLS VHOSTs the current server configuration presents.
    presented: ListenerVhosts,
    /// The latest installation attempt; `Pending` before this process attempted one.
    latest: HttpsListenerInstallation,
}

/// The certificates this node's HTTPS listener presents.
///
/// Every node installs them from the TLS VHOSTs of the runtime state it applies, so all listeners
/// present the certificates of the same revision. Established connections keep the session they
/// negotiated; only new handshakes see an installation. A failed installation leaves the
/// certificates already presented in place and is reported for the revision that asked for it.
#[derive(Clone)]
pub(in crate::application) struct HttpsListenerCertificates {
    inner: Arc<HttpsListenerCertificatesInner>,
}

struct HttpsListenerCertificatesInner {
    /// Also held by the application startup that opened it.
    resource_store: StdArc<ResourceStore>,
    fault_injection: ConfiguredFaultInjection,
    /// Also held by the application, which starts and shuts it down.
    cluster: Arc<ClusterHandle>,
    /// Read by the HTTPS listener for every connection it accepts.
    server_config: RwLock<Option<StdArc<ServerConfig>>>,
    /// Serializes installations with the VHOSTs they replace.
    installation: AsyncMutex<ListenerInstallation>,
}

impl HttpsListenerCertificates {
    pub(in crate::application) fn new(
        resource_store: &StdArc<ResourceStore>,
        fault_injection: &ConfiguredFaultInjection,
        cluster: &Arc<ClusterHandle>,
    ) -> Self {
        Self {
            inner: Arc::new(HttpsListenerCertificatesInner {
                resource_store: resource_store.clone(),
                fault_injection: fault_injection.clone(),
                cluster: cluster.clone(),
                server_config: RwLock::new(None),
                installation: AsyncMutex::new(ListenerInstallation {
                    presented: ListenerVhosts::default(),
                    latest: HttpsListenerInstallation::Pending,
                }),
            }),
        }
    }

    /// The configuration a newly accepted connection handshakes with, absent while no TLS VHOST is
    /// presented.
    pub(in crate::application) fn server_config(&self) -> Option<StdArc<ServerConfig>> {
        self.inner.server_config.read().clone()
    }

    /// Presents the TLS VHOSTs of the runtime state at `revision`. An unchanged set of VHOSTs keeps
    /// the configuration already presented. Each revision is attempted once: a node applies one
    /// revision from several paths, and the outcome its first attempt reported is the one every
    /// barrier for that revision observes, so a revision already attempted, or older than the
    /// latest attempt, is ignored. A failed installation is attempted again at the next revision.
    pub(in crate::application) async fn install(
        &self,
        revision: u64,
        schedule: &ClusterSchedule,
    ) -> Result<(), Report<HttpsListenerError>> {
        let desired = ListenerVhosts::from_schedule(schedule);
        let mut installation = self.inner.installation.lock().await;
        if let Some(latest) = installation.latest.revision()
            && latest >= revision
        {
            return Ok(());
        }
        if desired == installation.presented {
            installation.latest = HttpsListenerInstallation::Installed { revision };
            return Ok(());
        }

        let local_node = self.inner.cluster.local_node_identity().await;
        let injected = self
            .inner
            .fault_injection
            .take_armed_https_listener_installation_failure(local_node.node_id());
        let server_config = if injected {
            Err(Report::new(HttpsListenerError::FaultInjected))
        } else {
            desired.server_config(&self.inner.resource_store).await
        };
        match server_config {
            Ok(server_config) => {
                *self.inner.server_config.write() = server_config;
                installation.presented = desired;
                installation.latest = HttpsListenerInstallation::Installed { revision };
                info!(revision, "installed HTTPS listener TLS configuration");
                Ok(())
            }
            Err(error) => {
                installation.latest = HttpsListenerInstallation::Failed {
                    revision,
                    reason: format!("{error:#}"),
                };
                Err(error)
            }
        }
    }

    /// Where the listener stands against `revision`: the latest installation when it was made for
    /// that revision or a later one, otherwise `Pending`.
    pub(in crate::application) async fn installation(
        &self,
        revision: u64,
    ) -> HttpsListenerInstallation {
        let installation = self.inner.installation.lock().await;
        match installation.latest.revision() {
            Some(latest) if latest >= revision => installation.latest.clone(),
            Some(_) | None => HttpsListenerInstallation::Pending,
        }
    }

    /// Answers another node's HTTPS listener installation barrier for this process incarnation.
    pub(in crate::application) fn register_installation_handler(
        &self,
        interconnect: &Transport,
    ) -> Result<(), Report<HandlerRegistrationError>> {
        let certificates = self.clone();
        interconnect.register_handler::<HttpsListenerInstallationRequest, _, _>(
            move |_context, request| {
                let certificates = certificates.clone();
                async move {
                    let identity = certificates.inner.cluster.local_node_identity().await;
                    let installation = certificates.installation(request.revision).await;
                    HttpsListenerInstallationResponse {
                        identity,
                        installation,
                    }
                }
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::PathBuf, sync::Arc as StdArc};

    use nervix_interconnect::HttpsListenerInstallation;
    use nervix_models::{
        ClusterSchedule, CreateVhost, DomainName, DomainSchedule, Model, ResourceId, ScheduledNode,
        SchemaFingerprint, VhostTlsResource,
    };

    use super::{
        super::test_fixtures::{
            TestService, build_test_service, named, node_named, test_tls_files,
        },
        HttpsListenerError, ListenerVhostKey, ListenerVhosts, ResourceFileError, ResourceStore,
        TlsMaterialError, VHOST_TLS_CA_PATH, VHOST_TLS_CERT_PATH, VHOST_TLS_KEY_PATH,
        load_vhost_tls_materials,
    };

    fn vhost(name: &str, hostname: &str, tls_version: Option<u64>) -> Model {
        Model::Vhost(CreateVhost {
            name: named(name),
            hostnames: vec![hostname.to_string()],
            tls: tls_version.map(|version| VhostTlsResource {
                resource: named("tls_bundle"),
                version,
            }),
        })
    }

    fn cluster_schedule(domains: Vec<(&str, Vec<Model>)>) -> ClusterSchedule {
        let mut schedules = BTreeMap::new();
        for (domain, models) in domains {
            let domain = DomainName::parse(domain).expect("the fixture domain is valid");
            let nodes = models
                .into_iter()
                .map(|model| ScheduledNode::new(model, SchemaFingerprint::from_digest([1; 32])));
            schedules.insert(
                domain.clone(),
                DomainSchedule::new(domain, nodes, Vec::new()),
            );
        }
        ClusterSchedule { domains: schedules }
    }

    /// Writes a usable bundle, whose certificate names `localhost`, into `version` of the `orders`
    /// domain's `tls_bundle` resource and returns that version's content directory.
    fn write_test_bundle(resource_store: &ResourceStore, version: u64) -> PathBuf {
        let bundle = test_tls_files("test", &node_named("node-a"));
        let content = resource_store.content_root(&ResourceId::new(
            named("orders"),
            named("tls_bundle"),
            version,
        ));
        std::fs::create_dir_all(&content).expect("the fixture bundle directory is created");
        std::fs::copy(&bundle.ca, content.join(VHOST_TLS_CA_PATH))
            .expect("the fixture CA is copied");
        std::fs::copy(&bundle.certificate, content.join(VHOST_TLS_CERT_PATH))
            .expect("the fixture certificate is copied");
        std::fs::copy(&bundle.private_key, content.join(VHOST_TLS_KEY_PATH))
            .expect("the fixture private key is copied");
        content
    }

    #[test]
    fn the_listener_presents_the_tls_vhosts_of_every_domain() {
        let schedule = cluster_schedule(vec![
            (
                "orders",
                vec![
                    vhost("edge", "orders.example.com", Some(1)),
                    vhost("plain", "plain.example.com", None),
                ],
            ),
            (
                "billing",
                vec![vhost("edge", "billing.example.com", Some(3))],
            ),
        ]);

        let vhosts = ListenerVhosts::from_schedule(&schedule);

        let keys = vhosts.vhosts.keys().cloned().collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                ListenerVhostKey {
                    domain: named("billing"),
                    vhost: named("edge"),
                },
                ListenerVhostKey {
                    domain: named("orders"),
                    vhost: named("edge"),
                },
            ]
        );
        let billing = &vhosts.vhosts[&keys[0]];
        assert_eq!(billing.hostnames, vec!["billing.example.com".to_string()]);
        assert_eq!(
            billing.resource,
            ResourceId::new(named("billing"), named("tls_bundle"), 3)
        );
    }

    #[tokio::test]
    async fn a_failed_installation_is_reported_for_its_revision_and_keeps_the_presented_certificates()
     {
        let TestService { service, path, .. } = build_test_service(false).await;
        let certificates = service.inner.https_certificates.clone();
        write_test_bundle(&service.inner.resource_store, 1);
        let first = cluster_schedule(vec![("orders", vec![vhost("edge", "localhost", Some(1))])]);
        let second = cluster_schedule(vec![("orders", vec![vhost("edge", "localhost", Some(2))])]);

        assert_eq!(
            certificates.installation(5).await,
            HttpsListenerInstallation::Pending
        );
        certificates
            .install(5, &first)
            .await
            .expect("the first version's bundle loads");
        assert_eq!(
            certificates.installation(5).await,
            HttpsListenerInstallation::Installed { revision: 5 }
        );
        let presented = certificates
            .server_config()
            .expect("the TLS VHOST is presented");

        certificates
            .install(6, &first)
            .await
            .expect("an unchanged VHOST set installs without reloading");
        let unchanged = certificates
            .server_config()
            .expect("the TLS VHOST is still presented");
        assert!(StdArc::ptr_eq(&presented, &unchanged));

        let error = certificates
            .install(7, &second)
            .await
            .expect_err("the second version has no bundle on this node");
        assert!(
            format!("{error:#}").contains(
                "failed to load TLS resource 'tls_bundle@2' for VHOST 'edge' in domain 'orders'"
            ),
            "{error:#}"
        );
        let HttpsListenerInstallation::Failed { revision, reason } =
            certificates.installation(6).await
        else {
            panic!("the failed installation is the latest one at or after revision 6");
        };
        assert_eq!(revision, 7);
        assert!(reason.contains("tls_bundle@2"), "{reason}");
        let kept = certificates
            .server_config()
            .expect("the previous certificates stay presented");
        assert!(StdArc::ptr_eq(&presented, &kept));

        certificates
            .install(6, &first)
            .await
            .expect("an older runtime state is ignored");
        write_test_bundle(&service.inner.resource_store, 2);
        certificates
            .install(7, &second)
            .await
            .expect("an attempted revision is not attempted again");
        assert!(matches!(
            certificates.installation(7).await,
            HttpsListenerInstallation::Failed { revision: 7, .. }
        ));
        assert_eq!(
            certificates.installation(8).await,
            HttpsListenerInstallation::Pending
        );
        certificates
            .install(8, &second)
            .await
            .expect("the next revision attempts the installation again");
        assert_eq!(
            certificates.installation(8).await,
            HttpsListenerInstallation::Installed { revision: 8 }
        );
        let replaced = certificates
            .server_config()
            .expect("the second version is presented");
        assert!(!StdArc::ptr_eq(&presented, &replaced));

        drop(service);
        std::fs::remove_dir_all(&path).expect("the test database directory is removed");
    }

    #[tokio::test]
    async fn unusable_tls_material_is_classified_by_what_is_wrong_with_it() {
        let TestService { service, path, .. } = build_test_service(false).await;
        let resource_store = &service.inner.resource_store;
        let content = write_test_bundle(resource_store, 1);
        let bundle = ResourceId::new(named("orders"), named("tls_bundle"), 1);
        let usable_key = std::fs::read(content.join(VHOST_TLS_KEY_PATH))
            .expect("the fixture private key is read");
        let usable_ca =
            std::fs::read(content.join(VHOST_TLS_CA_PATH)).expect("the fixture CA is read");

        std::fs::write(content.join(VHOST_TLS_KEY_PATH), "not a private key")
            .expect("the key without PEM items is written");
        let error = load_vhost_tls_materials(resource_store, &bundle)
            .await
            .expect_err("a key file without PEM items is unusable");
        assert!(
            matches!(
                error.current_context(),
                TlsMaterialError::NoPemItems { path } if path.ends_with(VHOST_TLS_KEY_PATH)
            ),
            "{error:?}"
        );

        std::fs::write(
            content.join(VHOST_TLS_KEY_PATH),
            "-----BEGIN PRIVATE KEY-----\n!!!!\n-----END PRIVATE KEY-----\n",
        )
        .expect("the malformed key is written");
        let error = load_vhost_tls_materials(resource_store, &bundle)
            .await
            .expect_err("a PEM item that is not base64 is unusable");
        assert!(
            matches!(
                error.current_context(),
                TlsMaterialError::Pem { path, .. } if path.ends_with(VHOST_TLS_KEY_PATH)
            ),
            "{error:?}"
        );

        std::fs::write(content.join(VHOST_TLS_KEY_PATH), &usable_key)
            .expect("the usable key is restored");
        std::fs::write(
            content.join(VHOST_TLS_CA_PATH),
            "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        )
        .expect("the CA that is not a certificate is written");
        let error = load_vhost_tls_materials(resource_store, &bundle)
            .await
            .expect_err("a CA that is not a certificate is unusable");
        assert!(
            matches!(
                error.current_context(),
                TlsMaterialError::CaCertificate { path, .. } if path.ends_with(VHOST_TLS_CA_PATH)
            ),
            "{error:?}"
        );

        std::fs::write(content.join(VHOST_TLS_CA_PATH), &usable_ca)
            .expect("the usable CA is restored");
        std::fs::remove_file(content.join(VHOST_TLS_CERT_PATH))
            .expect("the certificate file is removed");
        std::fs::create_dir(content.join(VHOST_TLS_CERT_PATH))
            .expect("a directory takes the certificate's place");
        let error = load_vhost_tls_materials(resource_store, &bundle)
            .await
            .expect_err("a certificate path that is a directory is unusable");
        assert!(
            matches!(error.current_context(), TlsMaterialError::Unavailable),
            "{error:?}"
        );
        assert!(
            matches!(
                error.downcast_ref::<ResourceFileError>(),
                Some(ResourceFileError::NotAFile {
                    label: "tls certificate",
                    ..
                })
            ),
            "{error:?}"
        );

        drop(service);
        std::fs::remove_dir_all(&path).expect("the test database directory is removed");
    }

    #[tokio::test]
    async fn a_hostname_the_certificate_does_not_name_fails_the_installation() {
        let TestService { service, path, .. } = build_test_service(false).await;
        let certificates = service.inner.https_certificates.clone();
        write_test_bundle(&service.inner.resource_store, 1);
        let schedule = cluster_schedule(vec![(
            "orders",
            vec![vhost("edge", "orders.example.com", Some(1))],
        )]);

        let error = certificates
            .install(3, &schedule)
            .await
            .expect_err("the certificate names only localhost");

        assert!(
            matches!(
                error.current_context(),
                HttpsListenerError::Hostname { hostname, .. } if hostname == "orders.example.com"
            ),
            "{error:?}"
        );
        let HttpsListenerInstallation::Failed { revision, reason } =
            certificates.installation(3).await
        else {
            panic!("the installation of revision 3 failed");
        };
        assert_eq!(revision, 3);
        assert!(
            reason.contains(
                "failed to present TLS hostname 'orders.example.com' for VHOST 'edge' in domain \
                 'orders'"
            ),
            "{reason}"
        );
        assert!(certificates.server_config().is_none());

        drop(service);
        std::fs::remove_dir_all(&path).expect("the test database directory is removed");
    }

    #[tokio::test]
    async fn a_runtime_state_without_tls_vhosts_stops_presenting_certificates() {
        let TestService { service, path, .. } = build_test_service(false).await;
        let certificates = service.inner.https_certificates.clone();
        write_test_bundle(&service.inner.resource_store, 1);
        let with_tls =
            cluster_schedule(vec![("orders", vec![vhost("edge", "localhost", Some(1))])]);
        let without_tls =
            cluster_schedule(vec![("orders", vec![vhost("edge", "localhost", None)])]);

        certificates
            .install(1, &with_tls)
            .await
            .expect("the bundle loads");
        assert!(certificates.server_config().is_some());
        certificates
            .install(2, &without_tls)
            .await
            .expect("a VHOST without TLS needs no certificate");

        assert!(certificates.server_config().is_none());
        assert_eq!(
            certificates.installation(2).await,
            HttpsListenerInstallation::Installed { revision: 2 }
        );

        drop(service);
        std::fs::remove_dir_all(&path).expect("the test database directory is removed");
    }
}
