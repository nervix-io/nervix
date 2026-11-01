//! Metadata of a frame received by a Syslog listener.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** A frame's transport and peer address, the verified TLS client subject shared by
//!   one connection's frames, and their declared metadata visit.
//! - **Depends on.** The connector metadata contract, rustls, X.509 subject decoding and shared
//!   ownership from the primitive boundary.
//! - **Must not know.** Codecs, relays, branch routing, registry validation or Plexus.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::net::SocketAddr;

use error_stack::Report;
use nervix_connector::{IngestMessageMetadata, IngestMetadataValues};
use nervix_primitives::sync::Arc;
use rustls::{ServerConnection, pki_types::CertificateDer};
use strum::AsRefStr;
use x509_parser::prelude::{FromDer as _, X509Certificate};

use super::SyslogConnectionError;

/// Each transport carries only the identity it can establish. TLS frames share one immutable
/// connection identity, so the subject is decoded once and never copied per frame.
#[derive(Clone, AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub(super) enum SyslogPeer {
    Udp(SocketAddr),
    Tcp(SocketAddr),
    Tls(Arc<SyslogTlsPeer>),
}

pub(super) struct SyslogTlsPeer {
    address: SocketAddr,
    subject: Option<String>,
}

impl SyslogPeer {
    /// Called only after the accepted connection has completed its TLS handshake. Rustls
    /// exposes a peer certificate here only when the listener verified the client identity.
    pub(super) fn verified_tls(
        address: SocketAddr,
        connection: &ServerConnection,
    ) -> error_stack::Result<Self, SyslogConnectionError> {
        let mut subject = None;
        if let Some(certificates) = connection.peer_certificates()
            && let Some(certificate) = certificates.first()
        {
            subject = Some(SyslogTlsPeer::decode_subject(certificate)?);
        }
        Ok(Self::Tls(Arc::new(SyslogTlsPeer { address, subject })))
    }

    pub(super) fn address(&self) -> SocketAddr {
        match self {
            Self::Udp(address) | Self::Tcp(address) => *address,
            Self::Tls(peer) => peer.address,
        }
    }

    fn subject(&self) -> Option<&str> {
        match self {
            Self::Udp(_) | Self::Tcp(_) => None,
            Self::Tls(peer) => peer.subject.as_deref(),
        }
    }
}

impl SyslogTlsPeer {
    fn decode_subject(
        certificate: &CertificateDer<'_>,
    ) -> error_stack::Result<String, SyslogConnectionError> {
        let (_, parsed) = X509Certificate::from_der(certificate.as_ref()).map_err(|source| {
            Report::new(source).change_context(SyslogConnectionError::TlsPeerCertificate)
        })?;
        Ok(parsed.subject().to_string())
    }
}

impl IngestMessageMetadata for SyslogPeer {
    fn visit_metadata(&self, values: &mut dyn IngestMetadataValues) {
        values.rendered(&self.address());
        values.string(self.as_ref());
        if let Some(subject) = self.subject() {
            values.string(subject);
        } else {
            values.absent();
        }
    }
}

#[cfg(test)]
mod tests {
    //! Complete Syslog metadata retention and verified-certificate subject decoding.
    //!
    //! Layer: test harness.
    //! - **Owns.** Bounded current peer values, complete retained rows and X.509 subject cases.
    //! - **Depends on.** The production peer metadata owner and connector retention contract.
    //! - **Must not know.** Runtime routing, registry mutation or scheduling.

    #![cfg_attr(
        nervix_lint,
        nervix::layer(harness, reason = "tests the metadata engine boundary")
    )]

    use meticulous::ResultExt as _;
    use nervix_connector::{RetainedIngestMetadata, RetainedIngestMetadataValue as Value};
    use rcgen::{CertificateParams, DnType, KeyPair};

    use super::*;
    use crate::listener::SyslogListenerIngestorMessage;

    #[test]
    fn the_verified_leaf_subject_keeps_every_distinguished_name_attribute() {
        let mut params = CertificateParams::new(vec!["collector.example.com".to_string()])
            .assured("the test DNS name is valid");
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::OrganizationName, "Example");
        params
            .distinguished_name
            .push(DnType::CommonName, "collector.example.com");
        let key = KeyPair::generate().assured("the test key can be generated");
        let certificate = params
            .self_signed(&key)
            .assured("the test certificate is valid");
        let subject = SyslogTlsPeer::decode_subject(certificate.der())
            .assured("the generated certificate is valid DER");
        assert_eq!(subject, "O=Example, CN=collector.example.com");
    }

    #[test]
    fn a_subject_decode_failure_keeps_its_typed_x509_cause() {
        let certificate = CertificateDer::from(vec![0]);
        let Err(error) = SyslogTlsPeer::decode_subject(&certificate) else {
            panic!("a zero byte is not an X.509 certificate");
        };
        assert!(matches!(
            error.current_context(),
            SyslogConnectionError::TlsPeerCertificate
        ));
        assert!(error.contains::<x509_parser::nom::Err<x509_parser::error::X509Error>>());
    }

    #[test]
    fn tls_frames_share_one_connection_identity() {
        let identity = Arc::new(SyslogTlsPeer {
            address: SocketAddr::from(([192, 0, 2, 7], 6514)),
            subject: Some("CN=collector.example.com".to_string()),
        });
        let peer = SyslogPeer::Tls(identity.clone());
        let SyslogPeer::Tls(cloned) = peer.clone() else {
            panic!("cloning a TLS peer keeps its transport");
        };
        assert!(Arc::ptr_eq(&identity, &cloned));
    }

    #[test]
    fn bolero_listener_metadata_is_retained_exactly() {
        use nervix_connector::{IngestTransportPosition, IngestorMessage as _};

        bolero::check!()
            .with_iterations(256)
            .with_max_len(64)
            .for_each(|bytes: &[u8]| {
                let mut input = [0_u8; 64];
                let count = bytes.len().min(input.len());
                input[..count].copy_from_slice(&bytes[..count]);
                let port = u16::from_be_bytes([input[2], input[3]]);
                let address;
                if input[1] & 1 == 0 {
                    address = SocketAddr::from(([input[4], input[5], input[6], input[7]], port));
                } else {
                    let mut segments = [0_u16; 8];
                    for (segment, pair) in segments.iter_mut().zip(input[4..20].chunks_exact(2)) {
                        *segment = u16::from_be_bytes([pair[0], pair[1]]);
                    }
                    address = SocketAddr::from((segments, port));
                }
                let peer;
                let transport;
                let mut expected_subject = Value::Absent;
                match input[0] % 3 {
                    0 => {
                        peer = SyslogPeer::Udp(address);
                        transport = "udp";
                    }
                    1 => {
                        peer = SyslogPeer::Tcp(address);
                        transport = "tcp";
                    }
                    _ => {
                        let mut subject = None;
                        if input[20] & 1 != 0 {
                            let mut name = String::from("CN=");
                            for byte in &input[21..] {
                                let letter = b"abcdefghijklmnopqrstuvwxyz"[usize::from(byte % 26)];
                                name.push(char::from(letter));
                            }
                            expected_subject = Value::String(name.clone());
                            subject = Some(name);
                        }
                        peer = SyslogPeer::Tls(Arc::new(SyslogTlsPeer { address, subject }));
                        transport = "tls";
                    }
                }
                let message = SyslogListenerIngestorMessage::new(bytes.to_vec(), peer);
                let row = message.metadata();
                assert_eq!(
                    row.position,
                    Some(IngestTransportPosition::Syslog { peer: address })
                );
                let retained = RetainedIngestMetadata::capture(row.fields);
                drop(message);
                assert_eq!(
                    retained.values(),
                    [
                        Value::String(address.to_string()),
                        Value::String(transport.to_string()),
                        expected_subject,
                    ]
                );
                assert_eq!(RetainedIngestMetadata::capture(&retained), retained);
            });
    }
}
