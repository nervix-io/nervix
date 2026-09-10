//! Cluster and node identity carried by the mandatory interconnect certificate.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Loading cluster trust, TLS 1.3 configuration, and certificate SAN identity.
//! - **Depends on.** The shared cluster node name vocabulary and X.509/TLS primitives.
//! - **Must not know.** HTTP/2 pools, request routing, or runtime operations.

use std::{io, net::IpAddr, path::Path, sync::Arc as StdArc};

use error_stack::Report;
use nervix_models::ClusterNodeName;
use percent_encoding::percent_decode_str;
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
};
use rustls_pki_types::pem::{Error as PemError, PemObject};
use url::Url;
use x509_parser::{
    extensions::GeneralName,
    prelude::{FromDer as _, X509Certificate},
};

use super::TlsConfigError;

const INTERCONNECT_ALPN: &[u8] = b"h2";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CertificateIdentity {
    pub(crate) cluster_id: String,
    pub(crate) node_id: ClusterNodeName,
    pub(crate) not_before_unix_seconds: i64,
    pub(crate) not_after_unix_seconds: i64,
    dns_names: Vec<String>,
    ip_addresses: Vec<IpAddr>,
}

impl CertificateIdentity {
    pub(crate) fn from_certificate(
        certificate: &CertificateDer<'_>,
    ) -> Result<Self, Report<TlsConfigError>> {
        let (_, parsed) = X509Certificate::from_der(certificate.as_ref())
            .map_err(|error| TlsConfigError::InvalidCertificate(error.to_string()))?;
        let san = parsed
            .subject_alternative_name()
            .map_err(|error| TlsConfigError::InvalidCertificate(error.to_string()))?
            .ok_or(TlsConfigError::MissingSubjectAlternativeName)?;

        let mut identity = None;
        let mut dns_names = Vec::new();
        let mut ip_addresses = Vec::new();
        for name in &san.value.general_names {
            match name {
                GeneralName::URI(uri) => {
                    let parsed_identity = parse_identity_uri(uri)?;
                    if identity.replace(parsed_identity).is_some() {
                        return Err(Report::new(TlsConfigError::MultipleIdentityUris));
                    }
                }
                GeneralName::DNSName(name) => dns_names.push(name.to_ascii_lowercase()),
                GeneralName::IPAddress(bytes) => match *bytes {
                    [a, b, c, d] => {
                        ip_addresses.push(IpAddr::from([*a, *b, *c, *d]));
                    }
                    [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p] => {
                        ip_addresses.push(IpAddr::from([
                            *a, *b, *c, *d, *e, *f, *g, *h, *i, *j, *k, *l, *m, *n, *o, *p,
                        ]));
                    }
                    _ => return Err(Report::new(TlsConfigError::InvalidEndpointSan)),
                },
                _ => {}
            }
        }
        let (cluster_id, node_id) = identity.ok_or(TlsConfigError::MissingIdentityUri)?;
        Ok(Self {
            cluster_id,
            node_id,
            not_before_unix_seconds: parsed.validity().not_before.timestamp(),
            not_after_unix_seconds: parsed.validity().not_after.timestamp(),
            dns_names,
            ip_addresses,
        })
    }

    pub(crate) fn validate_local(
        &self,
        cluster_id: &str,
        node_id: &ClusterNodeName,
        advertised_host: &str,
    ) -> Result<(), Report<TlsConfigError>> {
        if self.cluster_id != cluster_id {
            return Err(Report::new(TlsConfigError::ClusterIdentityMismatch {
                expected: cluster_id.to_string(),
                actual: self.cluster_id.clone(),
            }));
        }
        if &self.node_id != node_id {
            return Err(Report::new(TlsConfigError::NodeIdentityMismatch {
                expected: node_id.clone(),
                actual: self.node_id.clone(),
            }));
        }
        if !self.matches_endpoint(advertised_host) {
            return Err(Report::new(TlsConfigError::EndpointIdentityMismatch {
                endpoint: advertised_host.to_string(),
            }));
        }
        Ok(())
    }

    pub(crate) fn matches_endpoint(&self, host: &str) -> bool {
        let host = host.trim_matches(['[', ']']);
        if let Ok(ip) = host.parse::<IpAddr>() {
            return self.ip_addresses.contains(&ip);
        }
        self.dns_names
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(host))
    }
}

fn parse_identity_uri(raw: &str) -> Result<(String, ClusterNodeName), Report<TlsConfigError>> {
    let uri = Url::parse(raw).map_err(|error| TlsConfigError::InvalidIdentityUri {
        uri: raw.to_string(),
        reason: error.to_string(),
    })?;
    if uri.scheme() != "nervix" || uri.host_str() != Some("cluster") {
        return Err(Report::new(TlsConfigError::InvalidIdentityUri {
            uri: raw.to_string(),
            reason: "expected nervix://cluster/<cluster-id>/node/<node-id>".to_string(),
        }));
    }
    if !uri.username().is_empty()
        || uri.password().is_some()
        || uri.port().is_some()
        || uri.query().is_some()
        || uri.fragment().is_some()
    {
        return Err(Report::new(TlsConfigError::InvalidIdentityUri {
            uri: raw.to_string(),
            reason: "identity URI cannot contain user-info, a port, query, or fragment".to_string(),
        }));
    }
    let segments = uri
        .path_segments()
        .ok_or_else(|| TlsConfigError::InvalidIdentityUri {
            uri: raw.to_string(),
            reason: "identity URI has no path segments".to_string(),
        })?
        .collect::<Vec<_>>();
    let [cluster_segment, "node", node_segment] = segments.as_slice() else {
        return Err(Report::new(TlsConfigError::InvalidIdentityUri {
            uri: raw.to_string(),
            reason: "expected exactly /<cluster-id>/node/<node-id>".to_string(),
        }));
    };
    let cluster_id = percent_decode_str(cluster_segment)
        .decode_utf8()
        .map_err(|error| TlsConfigError::InvalidIdentityUri {
            uri: raw.to_string(),
            reason: error.to_string(),
        })?
        .into_owned();
    if cluster_id.is_empty() {
        return Err(Report::new(TlsConfigError::InvalidIdentityUri {
            uri: raw.to_string(),
            reason: "cluster identity is empty".to_string(),
        }));
    }
    let node_raw = percent_decode_str(node_segment)
        .decode_utf8()
        .map_err(|error| TlsConfigError::InvalidIdentityUri {
            uri: raw.to_string(),
            reason: error.to_string(),
        })?;
    let node_id = ClusterNodeName::parse(node_raw.as_ref()).map_err(|error| {
        TlsConfigError::InvalidIdentityUri {
            uri: raw.to_string(),
            reason: error.to_string(),
        }
    })?;
    Ok((cluster_id, node_id))
}

#[derive(Clone)]
pub struct TlsConfigBundle {
    pub(crate) client_config: StdArc<ClientConfig>,
    pub(crate) server_config: StdArc<ServerConfig>,
    pub(crate) certificate: CertificateIdentity,
}

impl TlsConfigBundle {
    pub fn from_pem_files(
        ca_cert_path: impl AsRef<Path>,
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
    ) -> Result<Self, Report<TlsConfigError>> {
        super::install_rustls_crypto_provider();

        let ca_certs = load_certificates(ca_cert_path.as_ref())?;
        let cert_chain = load_certificates(cert_path.as_ref())?;
        let certificate =
            CertificateIdentity::from_certificate(cert_chain.first().ok_or_else(|| {
                TlsConfigError::MissingCertificate(cert_path.as_ref().display().to_string())
            })?)?;
        let private_key = load_private_key(key_path.as_ref())?;

        Self::from_parts(ca_certs, cert_chain, private_key, certificate)
    }

    /// Builds one immutable client/server TLS bundle from a consistent in-memory view of its PEM
    /// files. Callers that watch projected secrets can read all files before replacing a live
    /// bundle, without the parser reopening paths that may change between reads.
    pub fn from_pem(
        ca_cert_pem: &[u8],
        cert_pem: &[u8],
        key_pem: &[u8],
    ) -> Result<Self, Report<TlsConfigError>> {
        super::install_rustls_crypto_provider();

        let ca_certs = load_certificates_from_pem(ca_cert_pem, "CA certificate PEM")?;
        let cert_chain = load_certificates_from_pem(cert_pem, "node certificate PEM")?;
        let certificate =
            CertificateIdentity::from_certificate(cert_chain.first().ok_or_else(|| {
                TlsConfigError::MissingCertificate("node certificate PEM".to_string())
            })?)?;
        let private_key = load_private_key_from_pem(key_pem, "node private-key PEM")?;

        Self::from_parts(ca_certs, cert_chain, private_key, certificate)
    }

    fn from_parts(
        ca_certs: Vec<CertificateDer<'static>>,
        cert_chain: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
        certificate: CertificateIdentity,
    ) -> Result<Self, Report<TlsConfigError>> {
        let mut roots = RootCertStore::empty();
        for cert in ca_certs {
            roots.add(cert).map_err(TlsConfigError::from)?;
        }

        let mut client_config =
            ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_root_certificates(roots.clone())
                .with_client_auth_cert(cert_chain.clone(), private_key.clone_key())
                .map_err(TlsConfigError::from)?;
        client_config.alpn_protocols = vec![INTERCONNECT_ALPN.to_vec()];

        let verifier = WebPkiClientVerifier::builder(StdArc::new(roots))
            .build()
            .map_err(|error| TlsConfigError::Io(io::Error::other(error.to_string())))?;
        let mut server_config =
            ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_client_cert_verifier(verifier)
                .with_single_cert(cert_chain, private_key)
                .map_err(TlsConfigError::from)?;
        server_config.alpn_protocols = vec![INTERCONNECT_ALPN.to_vec()];

        Ok(Self {
            client_config: StdArc::new(client_config),
            server_config: StdArc::new(server_config),
            certificate,
        })
    }
}

fn load_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, Report<TlsConfigError>> {
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(map_pem_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(map_pem_error)?;
    if certs.is_empty() {
        return Err(Report::new(TlsConfigError::MissingCertificate(
            path.display().to_string(),
        )));
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, Report<TlsConfigError>> {
    match PrivateKeyDer::from_pem_file(path) {
        Ok(key) => Ok(key),
        Err(PemError::NoItemsFound) => Err(Report::new(TlsConfigError::MissingPrivateKey(
            path.display().to_string(),
        ))),
        Err(error) => Err(Report::new(map_pem_error(error))),
    }
}

fn load_certificates_from_pem(
    pem: &[u8],
    source: &'static str,
) -> Result<Vec<CertificateDer<'static>>, Report<TlsConfigError>> {
    let certificates = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(map_pem_error)?;
    if certificates.is_empty() {
        return Err(Report::new(TlsConfigError::MissingCertificate(
            source.to_string(),
        )));
    }
    Ok(certificates)
}

fn load_private_key_from_pem(
    pem: &[u8],
    source: &'static str,
) -> Result<PrivateKeyDer<'static>, Report<TlsConfigError>> {
    match PrivateKeyDer::from_pem_slice(pem) {
        Ok(key) => Ok(key),
        Err(PemError::NoItemsFound) => Err(Report::new(TlsConfigError::MissingPrivateKey(
            source.to_string(),
        ))),
        Err(error) => Err(Report::new(map_pem_error(error))),
    }
}

fn map_pem_error(error: PemError) -> TlsConfigError {
    match error {
        PemError::NoItemsFound => TlsConfigError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "no PEM items found",
        )),
        PemError::Io(error) => TlsConfigError::Io(error),
        other => TlsConfigError::Io(io::Error::new(io::ErrorKind::InvalidData, other)),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_identity_uri;

    #[test]
    fn identity_uri_rejects_components_outside_the_canonical_shape() {
        for uri in [
            "nervix://user@cluster/test/node/node-1",
            "nervix://cluster:443/test/node/node-1",
            "nervix://cluster/test/node/node-1?scope=other",
            "nervix://cluster/test/node/node-1#other",
        ] {
            assert!(
                parse_identity_uri(uri).is_err(),
                "noncanonical identity URI should be rejected: {uri}"
            );
        }
    }
}
