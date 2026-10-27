//! Real TLS regressions for the Syslog sender's independence from runner polling.
//!
//! Layer: test harness.
//! - **Owns.** A TLS peer with the scenario's one-second handshake deadline, and a caller that
//!   stops polling its send until the peer has received the complete frame or timed out.
//! - **Depends on.** The actual scenario sender, Rustls and the binary's runtime workers.
//! - **Must not know.** Nervix graph execution or the scenario world's resource storage.

use std::{future::poll_fn, task::Poll, time::Duration};

use meticulous::ResultExt as _;
use nervix_primitives::{
    net::TcpListener,
    sync::{StdArc, oneshot},
    task::AbortOnDropHandle,
    time::timeout,
};
use rcgen::{CertificateParams, KeyPair};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use rustls_pki_types::{PrivateKeyDer, ServerName, pem::PemObject as _};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio_rustls::TlsAcceptor;

use crate::syslog_tls::SyslogTlsSender;

/// The product scenario's handshake bound, unchanged by the caller's scheduling delay.
const HANDSHAKE_BOUND: Duration = Duration::from_secs(1);
/// Bounds fixture failure reporting, rather than a product wait or scheduling assumption.
const FIXTURE_BOUND: Duration = Duration::from_secs(30);
const FRAME: &[u8] = b"12 test payload";

struct TlsPeer {
    sender: SyslogTlsSender,
    start_handshake: oneshot::Sender<()>,
    received: oneshot::Receiver<Result<Vec<u8>, String>>,
    task: AbortOnDropHandle<()>,
}

impl TlsPeer {
    async fn start() -> Self {
        nervix_interconnect::install_rustls_crypto_provider();
        let key = KeyPair::generate().assured("the test key algorithm is supported");
        let certificate = CertificateParams::new(vec!["127.0.0.1".to_string()])
            .assured("a loopback IP is a valid certificate name")
            .self_signed(&key)
            .assured("the generated test key signs its certificate");
        let mut roots = RootCertStore::empty();
        roots
            .add(certificate.der().clone())
            .assured("the generated certificate is a valid trust anchor");
        let config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let server_key = PrivateKeyDer::from_pem_slice(key.serialize_pem().as_bytes())
            .assured("rcgen serializes its key as valid PEM");
        let server = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate.der().clone()], server_key)
            .assured("the generated server certificate matches its key");
        let acceptor = TlsAcceptor::from(StdArc::new(server));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .assured("the fixture can bind a loopback listener");
        let addr = listener
            .local_addr()
            .assured("the bound listener has an address");
        let sender = SyslogTlsSender::new(
            addr.to_string(),
            ServerName::try_from("127.0.0.1").assured("the loopback IP is a valid server name"),
            config,
        );
        let (receipt, received) = oneshot::channel();
        let (start_handshake, handshake_started) = oneshot::channel();
        let task = AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
            let result = async {
                let (stream, _) = listener.accept().await.map_err(|error| error.to_string())?;
                handshake_started
                    .await
                    .assured("the caller releases the handshake after its first poll");
                let handshake = timeout(HANDSHAKE_BOUND, acceptor.accept(stream)).await;
                let mut stream = match handshake {
                    Ok(Ok(stream)) => stream,
                    Ok(Err(error)) => return Err(format!("TLS handshake failed: {error}")),
                    Err(_) => return Err("TLS handshake exceeded its one-second deadline".into()),
                };
                let mut bytes = vec![0; FRAME.len()];
                timeout(FIXTURE_BOUND, stream.read_exact(&mut bytes))
                    .await
                    .map_err(|error| error.to_string())?
                    .map_err(|error| error.to_string())?;
                Ok(bytes)
            }
            .await;
            receipt
                .send(result)
                .assured("the caller waits for the TLS peer's result");
        }));
        Self {
            sender,
            start_handshake,
            received,
            task,
        }
    }
}

/// Poll once to start the same client operation the step awaits, then withhold every subsequent
/// caller poll until the independent peer reports its outcome. Awaiting the peer here never polls
/// the sender again, representing a delayed runner without blocking any runtime worker.
async fn await_without_runner_polls<T>(
    send: impl Future<Output = T>,
    start_handshake: oneshot::Sender<()>,
    peer: oneshot::Receiver<Result<Vec<u8>, String>>,
) -> T {
    let mut send = std::pin::pin!(send);
    poll_fn(|context| {
        assert!(
            send.as_mut().poll(context).is_pending(),
            "the sender cannot finish before the peer starts its handshake"
        );
        Poll::Ready(())
    })
    .await;
    start_handshake
        .send(())
        .assured("the peer waits until the caller's first poll ends");
    let completion = timeout(FIXTURE_BOUND, peer)
        .await
        .assured("the TLS peer reports its outcome while the runner is unpolled");
    let received = completion.assured("the TLS peer sends its outcome before ending");
    assert_eq!(received, Ok(FRAME.to_vec()));
    send.await
}

#[nervix_primitives::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_syslog_tls_publication_finishes_while_the_runner_is_unpolled() {
    let peer = TlsPeer::start().await;
    await_without_runner_polls(
        peer.sender.publish(FRAME.to_vec()),
        peer.start_handshake,
        peer.received,
    )
    .await;
    peer.task.await.assured("the TLS peer completed its read");
}

#[nervix_primitives::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_syslog_tls_split_write_finishes_while_the_runner_is_unpolled() {
    let peer = TlsPeer::start().await;
    let mut stream = await_without_runner_polls(
        peer.sender.write(FRAME.to_vec()),
        peer.start_handshake,
        peer.received,
    )
    .await;
    stream
        .shutdown()
        .await
        .assured("the caller closes its retained TLS sender");
    peer.task.await.assured("the TLS peer completed its read");
}
