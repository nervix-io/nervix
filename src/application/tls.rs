//! The certificates every listener and the interconnect present, and when they are reloaded.
//!
//! Layer: edges.
//!
//! - **Owns.** Reading TLS material from disk, the per-vhost SNI resolver, and the reload that
//!   picks up a rotated certificate without a restart.
//! - **Depends on.** The resource store for vhost material and the registry for the vhosts.
//! - **Must not know.** What travels over a connection once it is established.

use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc as StdArc,
};

use blake3::Hasher;
use error_stack::Report;
use nervix_interconnect::{TlsConfigBundle, Transport};
use nervix_models::{CreateVhost, ModelKind, ResourceId};
use rustls::{
    RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
    server::ResolvesServerCertUsingSni,
    sign::CertifiedKey,
};
use rustls_pki_types::pem::{Error as PemError, PemObject};
use tokio::time::{Duration, interval};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Identity as TonicIdentity, ServerTlsConfig};
use tracing::{info, warn};

use super::{AppError, resource::resolve_resource_id, session_service::SessionServiceImpl};
use crate::resource::ResourceStore;

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

pub(in crate::application) struct VhostTlsMaterials {
    certified_key: CertifiedKey,
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
    let cert_chain = load_certificates_from_pem_file(cert_path)
        .map_err(|error| Report::new(AppError::LoadWebConsoleTls).attach_printable(error))?;
    let private_key = load_private_key_from_pem_file(key_path)
        .map_err(|error| Report::new(AppError::LoadWebConsoleTls).attach_printable(error))?;
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

pub(in crate::application) async fn load_vhost_tls_materials(
    resource_store: &ResourceStore,
    id: &ResourceId,
) -> Result<VhostTlsMaterials, String> {
    let cert_path = resource_store
        .resolve_content_path(id, VHOST_TLS_CERT_PATH)
        .map_err(|error| error.to_string())?;
    let key_path = resource_store
        .resolve_content_path(id, VHOST_TLS_KEY_PATH)
        .map_err(|error| error.to_string())?;
    let ca_path = resource_store
        .resolve_content_path(id, VHOST_TLS_CA_PATH)
        .map_err(|error| error.to_string())?;

    ensure_file_exists(&cert_path, "tls certificate").await?;
    ensure_file_exists(&key_path, "tls private key").await?;
    ensure_file_exists(&ca_path, "tls CA certificate").await?;

    let _roots = load_root_store_from_pem_file(&ca_path)?;
    let cert_chain = load_certificates_from_pem_file(&cert_path)?;
    let private_key = load_private_key_from_pem_file(&key_path)?;
    let provider = rustls::crypto::CryptoProvider::get_default()
        .ok_or_else(|| "rustls crypto provider is not installed".to_string())?;
    let certified_key = CertifiedKey::from_der(cert_chain, private_key, provider)
        .map_err(|error| error.to_string())?;

    Ok(VhostTlsMaterials { certified_key })
}

pub(in crate::application) async fn ensure_file_exists(
    path: &Path,
    label: &str,
) -> Result<(), String> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|_| format!("{label} file '{}' does not exist", path.display()))?;
    if metadata.is_file() {
        Ok(())
    } else {
        Err(format!("{label} path '{}' is not a file", path.display()))
    }
}

fn load_root_store_from_pem_file(path: &Path) -> Result<RootCertStore, String> {
    let certs = load_certificates_from_pem_file(path)?;
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert).map_err(|error| error.to_string())?;
    }
    Ok(roots)
}

fn load_certificates_from_pem_file(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(map_pem_error_to_string)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(map_pem_error_to_string)?;
    if certs.is_empty() {
        return Err(format!("no certificates found in '{}'", path.display()));
    }
    Ok(certs)
}

fn load_private_key_from_pem_file(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    PrivateKeyDer::from_pem_file(path).map_err(map_pem_error_to_string)
}

fn map_pem_error_to_string(error: PemError) -> String {
    match error {
        PemError::NoItemsFound => "no PEM items found".to_string(),
        other => other.to_string(),
    }
}

impl SessionServiceImpl {
    pub(in crate::application) async fn refresh_http_tls_server_config(
        &self,
    ) -> Result<(), String> {
        nervix_interconnect::install_rustls_crypto_provider();
        let resources = self.inner.consensus.current_resources().await;
        let domains = self.inner.consensus.current_domains().await;
        let mut resolver = ResolvesServerCertUsingSni::new();
        let mut configured_tls = false;

        for domain_id in domains.keys() {
            tokio::task::consume_budget().await;
            let Ok(vhost_ids) =
                self.inner
                    .registry
                    .list_identifiers(domain_id, ModelKind::Vhost, "")
            else {
                continue;
            };

            for vhost_id in vhost_ids {
                tokio::task::consume_budget().await;
                let Ok(Some(vhost)) = self.inner.registry.get::<CreateVhost>(domain_id, &vhost_id)
                else {
                    continue;
                };
                let Some(tls) = vhost.tls.as_ref() else {
                    continue;
                };

                let id =
                    match resolve_resource_id(&resources, domain_id, &tls.resource, tls.version) {
                        Ok(id) => id,
                        Err(error) => {
                            warn!(
                                domain = domain_id.as_str(),
                                vhost = vhost.name.as_str(),
                                resource = tls.resource.as_str(),
                                error,
                                "failed to resolve VHOST TLS resource version"
                            );
                            continue;
                        }
                    };
                let version = id.version;
                let certified_key =
                    match load_vhost_tls_materials(&self.inner.resource_store, &id).await {
                        Ok(materials) => materials.certified_key,
                        Err(error) => {
                            warn!(
                                domain = domain_id.as_str(),
                                vhost = vhost.name.as_str(),
                                resource = tls.resource.as_str(),
                                version,
                                error,
                                "failed to load VHOST TLS materials"
                            );
                            continue;
                        }
                    };

                let mut applied_hostname = false;
                for hostname in &vhost.hostnames {
                    if let Err(error) = resolver.add(hostname, certified_key.clone()) {
                        warn!(
                            domain = domain_id.as_str(),
                            vhost = vhost.name.as_str(),
                            hostname,
                            resource = tls.resource.as_str(),
                            version,
                            error = %error,
                            "failed to add VHOST TLS hostname to SNI resolver"
                        );
                        continue;
                    }
                    applied_hostname = true;
                }
                configured_tls |= applied_hostname;
            }
        }

        let mut guard = self.inner.http_tls_server_config.write();
        if configured_tls {
            let config = ServerConfig::builder()
                .with_no_client_auth()
                .with_cert_resolver(StdArc::new(resolver));
            *guard = Some(StdArc::new(config));
        } else {
            *guard = None;
        }
        Ok(())
    }
}
