//! Mutually authenticated TLS sessions and the clock their certificates are judged against.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The UTC clock that judges certificate validity, the transport's explicit validity
//!   and expiry checks, and establishing one TLS 1.3 session with a verified cluster peer.
//! - **Depends on.** Certificate identity, Rustls, and Tokio's monotonic clock.
//! - **Must not know.** HTTP/2 pools, request routing, or runtime operations.

use std::{net::SocketAddr, sync::Arc as StdArc, time::Duration};

use error_stack::Report;
use nervix_models::ClusterNodeName;
use rustls::{
    pki_types::{CertificateDer, ServerName},
    time_provider::{DefaultTimeProvider, TimeProvider},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::{Instant, timeout},
};
use tokio_rustls::{TlsAcceptor, TlsConnector, client, server};

use super::{TlsConfigBundle, TlsConfigError, TransportError};
use crate::identity::CertificateIdentity;

#[cfg(all(test, feature = "turmoil"))]
mod simulation_tests;

/// The UTC clock that one TLS bundle judges certificate validity against.
///
/// Rustls verification on both sides of a handshake and the transport's own validity and expiry
/// checks read this one clock, so they can never disagree about whether a certificate is current.
/// Monotonic deadlines, including the moment a connection drains because a certificate expired,
/// stay on Tokio's clock.
#[derive(Debug, Clone)]
pub struct TransportClock {
    provider: StdArc<dyn TimeProvider>,
}

impl TransportClock {
    /// The process wall clock, which every production node uses.
    pub fn system() -> Self {
        Self {
            provider: StdArc::new(DefaultTimeProvider),
        }
    }

    /// A clock supplied by the owner of a controlled environment, such as a simulation.
    pub fn from_provider(provider: StdArc<dyn TimeProvider>) -> Self {
        Self { provider }
    }

    pub(crate) fn provider(&self) -> StdArc<dyn TimeProvider> {
        StdArc::clone(&self.provider)
    }

    fn unix_seconds(&self) -> Result<i64, Report<TlsConfigError>> {
        let Some(now) = self.provider.current_time() else {
            return Err(Report::new(TlsConfigError::ClockUnavailable));
        };
        let seconds = i64::try_from(now.as_secs()).map_err(|_| TlsConfigError::ClockOutOfRange)?;
        Ok(seconds)
    }

    /// Accept `identity` only while this clock reads inside its validity window.
    pub(crate) fn ensure_current(
        &self,
        identity: &CertificateIdentity,
    ) -> Result<(), Report<TlsConfigError>> {
        let now = self.unix_seconds()?;
        if identity.not_before_unix_seconds > now {
            return Err(Report::new(TlsConfigError::NotYetValid));
        }
        if identity.not_after_unix_seconds <= now {
            return Err(Report::new(TlsConfigError::Expired));
        }
        Ok(())
    }

    /// The monotonic instant at which the earlier of a session's two certificates expires.
    fn expiration_deadline(
        &self,
        local: &CertificateIdentity,
        peer: &CertificateIdentity,
    ) -> Result<Instant, TransportError> {
        let expires_at = local
            .not_after_unix_seconds
            .min(peer.not_after_unix_seconds);
        let now = self
            .unix_seconds()
            .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;
        let remaining = expires_at
            .checked_sub(now)
            .ok_or_else(|| TransportError::InvalidHandshake(TlsConfigError::Expired.to_string()))?;
        if remaining <= 0 {
            return Err(TransportError::InvalidHandshake(
                TlsConfigError::Expired.to_string(),
            ));
        }
        let remaining = u64::try_from(remaining)
            .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;
        Instant::now()
            .checked_add(Duration::from_secs(remaining))
            .ok_or_else(|| {
                TransportError::InvalidHandshake(
                    "certificate expiration exceeds the monotonic clock range".to_string(),
                )
            })
    }
}

/// One TLS 1.3 session whose peer presented a current certificate for this cluster.
pub(crate) struct AuthenticatedSession<S> {
    pub(crate) stream: S,
    pub(crate) peer: CertificateIdentity,
    /// When the earlier of the two session certificates expires. The connection drains then.
    pub(crate) expires_at: Instant,
}

/// The TLS facts a completed handshake reports about its peer, before the transport accepts it.
struct NegotiatedPeer<'a> {
    alpn: Option<&'a [u8]>,
    certificates: Option<&'a [CertificateDer<'static>]>,
}

impl TlsConfigBundle {
    /// Open a client session over `io` and verify that the server is a current member of
    /// `cluster_id`, and `expected_node` when the caller knows which node it dialed.
    ///
    /// The caller bounds the whole setup, including the transport connection this runs over.
    pub(crate) async fn connect<IO>(
        &self,
        io: IO,
        server_name: &str,
        cluster_id: &str,
        expected_node: Option<&ClusterNodeName>,
    ) -> Result<AuthenticatedSession<client::TlsStream<IO>>, TransportError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        self.clock
            .ensure_current(&self.certificate)
            .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;
        let name = ServerName::try_from(server_name.to_string())
            .map_err(|_| TransportError::InvalidServerName(server_name.to_string()))?;
        let stream = TlsConnector::from(StdArc::clone(&self.client_config))
            .connect(name, io)
            .await?;
        let (_, connection) = stream.get_ref();
        let negotiated = NegotiatedPeer {
            alpn: connection.alpn_protocol(),
            certificates: connection.peer_certificates(),
        };
        let peer = self.authenticate_peer(&negotiated, cluster_id, expected_node)?;
        let expires_at = self.clock.expiration_deadline(&self.certificate, &peer)?;
        Ok(AuthenticatedSession {
            stream,
            peer,
            expires_at,
        })
    }

    /// Accept a server session over `io` within `setup_timeout`, and verify that the client is a
    /// current member of `cluster_id`.
    pub(crate) async fn accept<IO>(
        &self,
        io: IO,
        peer_addr: SocketAddr,
        setup_timeout: Duration,
        cluster_id: &str,
    ) -> Result<AuthenticatedSession<server::TlsStream<IO>>, Report<TransportError>>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        self.clock
            .ensure_current(&self.certificate)
            .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;
        let handshake = TlsAcceptor::from(StdArc::clone(&self.server_config)).accept(io);
        let Ok(accepted) = timeout(setup_timeout, handshake).await else {
            return Err(Report::new(TransportError::ConnectionSetupTimeout {
                peer: peer_addr,
                timeout: setup_timeout,
            }));
        };
        let stream = accepted.map_err(TransportError::from)?;
        let (_, connection) = stream.get_ref();
        let negotiated = NegotiatedPeer {
            alpn: connection.alpn_protocol(),
            certificates: connection.peer_certificates(),
        };
        let peer = self.authenticate_peer(&negotiated, cluster_id, None)?;
        let expires_at = self.clock.expiration_deadline(&self.certificate, &peer)?;
        Ok(AuthenticatedSession {
            stream,
            peer,
            expires_at,
        })
    }

    fn authenticate_peer(
        &self,
        negotiated: &NegotiatedPeer<'_>,
        cluster_id: &str,
        expected_node: Option<&ClusterNodeName>,
    ) -> Result<CertificateIdentity, TransportError> {
        if negotiated.alpn != Some(b"h2".as_slice()) {
            return Err(TransportError::InvalidHandshake(
                "TLS did not negotiate ALPN h2".to_string(),
            ));
        }
        let Some(certificates) = negotiated.certificates else {
            return Err(TransportError::InvalidHandshake(
                "peer did not present a certificate".to_string(),
            ));
        };
        let Some(certificate) = certificates.first() else {
            return Err(TransportError::InvalidHandshake(
                "peer did not present a certificate".to_string(),
            ));
        };
        let identity = CertificateIdentity::from_certificate(certificate)
            .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;
        self.clock
            .ensure_current(&identity)
            .map_err(|error| TransportError::InvalidHandshake(error.to_string()))?;
        if identity.cluster_id != cluster_id {
            return Err(TransportError::InvalidHandshake(format!(
                "peer certificate identifies cluster '{}', expected '{}'",
                identity.cluster_id, cluster_id
            )));
        }
        if let Some(expected_node) = expected_node
            && &identity.node_id != expected_node
        {
            return Err(TransportError::InvalidHandshake(format!(
                "peer certificate identifies node '{}', expected '{}'",
                identity.node_id, expected_node
            )));
        }
        Ok(identity)
    }
}
