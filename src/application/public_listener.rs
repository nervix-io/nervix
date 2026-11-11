//! The connections a node's public listeners accept, which a failed accept never stops.
//!
//! Layer: edges.
//!
//! - **Owns.** Accept admission, connection leases, TCP options, capacity refusals and retry pacing.
//! - **Depends on.** The listening socket, resolved settings, Hyper and the accept-failure seam.
//! - **Must not know.** Routes, authentication, graph execution or shutdown ordering.
//!
//! A public listener that stops requests the node's stop, so a listener that ended on a failed
//! accept would end the node. None does: a listener stops only when the node closes admission. A
//! connection its client reset or abandoned before the listener took it fails only itself, and the
//! listener accepts the next at once. Any other failure, such as the process running out of file
//! descriptors, is a fault of the node rather than of one connection. The listener reports it and
//! accepts again after its declared retry pause: the first such
//! failure after the listener last took a connection is reported at `warn` and the ones repeating
//! it at `debug`, and the connection that ends the run is reported at `info` with how many accepts
//! failed in it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::{convert::Infallible, io, net::SocketAddr, pin::Pin};

use hyper::{Request, body::Incoming, service::service_fn};
use hyper_util::rt::TokioIo;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_interconnect::is_connection_accept_failure;
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, StdArc},
    time::{Sleep, sleep},
};
use nervix_recovery::Reported as _;
use tracing::{debug, info, warn};

use super::{listener_io::ListenerIo, listener_settings::ListenerSettings};
use crate::ConfiguredFaultInjection;
#[cfg(feature = "testing")]
use crate::fault_injection::ListenerAcceptFaults;

/// The public listeners of a node, as their records name them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(in crate::application) enum PublicListenerKind {
    /// The native gRPC session listener.
    Grpc,
    /// The HTTP listener of the node's `VHOST` endpoints.
    Http,
    /// The HTTPS listener of the node's `VHOST` endpoints.
    Https,
    /// The observability listener.
    Observability,
    /// The web console listener.
    Console,
    /// The web console's TLS listener.
    ConsoleTls,
}

/// How a public listener of an ordinary build accepts: as its socket does.
#[cfg(not(feature = "testing"))]
struct ListenerAcceptFaults {
    _marker: (),
}

#[cfg(not(feature = "testing"))]
impl ListenerAcceptFaults {
    async fn accept(&self, listener: &TcpListener) -> io::Result<(TcpStream, SocketAddr)> {
        listener.accept().await
    }
}

/// The accepts of a testing build's public listener, which fail where a scenario armed it.
#[cfg(feature = "testing")]
fn listener_accept_faults(injection: &ConfiguredFaultInjection) -> ListenerAcceptFaults {
    injection.listener_accept_faults()
}

/// The accepts of an ordinary build's public listener, which no scenario can fail.
#[cfg(not(feature = "testing"))]
fn listener_accept_faults(_injection: &ConfiguredFaultInjection) -> ListenerAcceptFaults {
    ListenerAcceptFaults { _marker: () }
}

/// One public listener, which accepts until the node closes admission whatever an accept meets.
pub(in crate::application) struct PublicListener {
    listener: TcpListener,
    kind: PublicListenerKind,
    /// The accepts that failed since the listener last took a connection.
    failed_accepts: u64,
    faults: ListenerAcceptFaults,
    settings: ListenerSettings,
    connections: StdArc<Semaphore>,
    retry: Option<Pin<Box<Sleep>>>,
    refusal: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl PublicListener {
    pub(in crate::application) fn new(
        listener: TcpListener,
        kind: PublicListenerKind,
        injection: &ConfiguredFaultInjection,
        settings: ListenerSettings,
    ) -> Self {
        Self {
            listener,
            kind,
            failed_accepts: 0,
            settings,
            connections: StdArc::new(Semaphore::new(
                usize::try_from(settings.max_connections())
                    .assured("MaxConnections validation checks native semaphore capacity"),
            )),
            retry: None,
            refusal: None,
            faults: listener_accept_faults(injection),
        }
    }

    pub(super) fn settings(&self) -> ListenerSettings {
        self.settings
    }

    /// The next connection a client opened to this listener, with the address it came from, once
    /// the listener takes one. A failed accept is reported and the listener accepts again; it
    /// never ends the wait.
    pub(in crate::application) async fn accept(&mut self) -> (ListenerIo<TcpStream>, SocketAddr) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let refusing = self.refusal.is_some();
            let accepted = nervix_primitives::select! {
                biased;
                _ = async {
                    if let Some(refusal) = self.refusal.as_mut() {
                        refusal.as_mut().await;
                    }
                }, if refusing => {
                    self.refusal = None;
                    continue;
                },
                accepted = async {
                    if let Some(retry) = self.retry.as_mut() {
                        retry.as_mut().await;
                        self.retry = None;
                    }
                    self.faults.accept(&self.listener).await
                } => accepted,
            };
            match accepted {
                Ok((stream, peer)) => {
                    self.took_connection();
                    stream
                        .set_nodelay(true)
                        .reported("disabling Nagle on an accepted connection");
                    let lease = match StdArc::clone(&self.connections).try_acquire_owned() {
                        Ok(lease) => lease,
                        Err(_) => {
                            debug!(
                                listener = <&'static str>::from(self.kind),
                                reason = %super::listener_capacity::InputRefusal::ConnectionCapacity,
                                "refusing a connection at its declared bound"
                            );
                            if matches!(
                                self.kind,
                                PublicListenerKind::Http
                                    | PublicListenerKind::Console
                                    | PublicListenerKind::Observability
                            ) {
                                self.refuse_http_connection(stream);
                            }
                            continue;
                        }
                    };
                    if let Err(error) = nervix_primitives::net::configure_tcp_keepalive(
                        &stream,
                        self.settings.tcp_keepalive_time(),
                        self.settings.tcp_keepalive_interval(),
                        self.settings.tcp_keepalive_retries(),
                    ) {
                        warn!(%error, "could not configure an accepted connection's keepalive");
                        continue;
                    }
                    return (
                        ListenerIo::new(stream, lease, self.settings.idle_timeout()),
                        peer,
                    );
                }
                Err(error) => self.failed(&error),
            }
        }
    }

    /// Keep one bounded refusal beside accepts, never a task or an accumulating refusal queue.
    /// A stalled excess peer cannot delay admitting a healthy connection when capacity returns.
    fn refuse_http_connection(&mut self, stream: TcpStream) {
        if self.refusal.is_some() {
            return;
        }
        let settings = self.settings;
        let refusal = async move {
            let mut builder = super::listener_io::http_builder(settings);
            builder.keep_alive(false);
            let mut connection = builder.serve_connection(
                TokioIo::new(stream),
                service_fn(|_: Request<Incoming>| async {
                    let refusal = super::listener_capacity::InputRefusal::ConnectionCapacity;
                    let response = super::http_endpoint::text_response(
                        refusal.status(),
                        format!("{refusal}\n"),
                    );
                    Ok::<_, Infallible>(response)
                }),
            );
            match nervix_primitives::time::timeout(settings.header_timeout(), &mut connection).await
            {
                Ok(Ok(())) => {
                    let parts = connection.into_parts();
                    let stream = parts.io.into_inner();
                    drop(parts.read_buf);
                    super::listener_io::RefusedTransport::new(stream, settings)
                        .finish()
                        .await;
                }
                Ok(Err(error)) => {
                    Err::<(), _>(error_stack::Report::new(error))
                        .reported("sending the listener's connection-capacity refusal");
                }
                Err(error) => {
                    debug!(%error, "an excess connection outlived its refusal deadline");
                }
            }
        };
        self.refusal = Some(Box::pin(refusal));
    }

    fn took_connection(&mut self) {
        if self.failed_accepts == 0 {
            return;
        }
        let listener: &'static str = self.kind.into();
        info!(
            listener,
            failed_accepts = self.failed_accepts,
            "a public listener accepts connections again"
        );
        self.failed_accepts = 0;
    }

    fn failed(&mut self, error: &io::Error) {
        let listener: &'static str = self.kind.into();
        if is_connection_accept_failure(error) {
            debug!(listener, %error, "a connection ended before its listener accepted it");
            return;
        }
        self.retry = Some(Box::pin(sleep(self.settings.accept_retry_delay())));
        self.failed_accepts = self.failed_accepts.checked_add(1).assured(
            "a listener retries an accept at most once per poll, so its failures in one run take \
             centuries to reach u64::MAX",
        );
        if self.failed_accepts == 1 {
            warn!(
                listener,
                %error,
                "a public listener failed to accept a connection; it keeps accepting"
            );
        } else {
            debug!(
                listener,
                %error,
                failed_accepts = self.failed_accepts,
                "a public listener failed to accept a connection again"
            );
        }
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use std::time::Duration;

    use nervix_primitives::{net::TcpStream, time::timeout};

    use super::*;
    use crate::{captured_logs::CapturedLogs, fault_injection::FaultInjection};

    const WAIT: Duration = Duration::from_secs(30);

    #[nervix_primitives::test]
    async fn a_failed_accept_is_reported_and_the_listener_takes_the_next_connection() {
        let injection = FaultInjection::default();
        let socket = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port binds");
        let address = socket
            .local_addr()
            .expect("a bound listener has an address");
        let mut listener = PublicListener::new(
            socket,
            PublicListenerKind::Console,
            &injection,
            ListenerSettings::default(),
        );
        injection.fail_next_listener_accept(address);
        let client = TcpStream::connect(address)
            .await
            .expect("the listener's backlog takes the connection");

        let logs = CapturedLogs::default();
        // `#[nervix_primitives::test]` runs the whole future on the calling thread, so the
        // thread-local default the guard installs stays in force across the await.
        let guard = tracing::subscriber::set_default(logs.subscriber());
        let (accepted, peer) = timeout(WAIT, listener.accept())
            .await
            .expect("the listener takes the connection after its failed accept");
        drop(guard);

        timeout(WAIT, injection.listener_accept_failure(address))
            .await
            .expect("the armed failure happened before the connection was taken");
        assert_eq!(
            peer,
            client
                .local_addr()
                .expect("a connected client has an address")
        );
        assert!(
            accepted
                .nodelay()
                .expect("an accepted socket reports its options"),
            "an accepted connection has Nagle's algorithm disabled"
        );
        assert_eq!(
            listener.failed_accepts, 0,
            "taking a connection ends the run"
        );
        let logs = logs.contents();
        assert!(
            logs.contains("a public listener failed to accept a connection; it keeps accepting")
                && logs.contains("listener=\"console\"")
                && logs.contains("injected public listener accept failure"),
            "the failed accept is reported with its listener and error:\n{logs}"
        );
        assert!(
            logs.contains("a public listener accepts connections again")
                && logs.contains("failed_accepts=1"),
            "the connection that ends the run is reported with how many accepts failed:\n{logs}"
        );
    }

    #[nervix_primitives::test]
    async fn a_run_of_failed_accepts_warns_once_and_a_connection_failure_never_warns() {
        let injection = FaultInjection::default();
        let socket = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port binds");
        let mut listener = PublicListener::new(
            socket,
            PublicListenerKind::Observability,
            &injection,
            ListenerSettings::default(),
        );

        let logs = CapturedLogs::of(|| {
            listener.failed(&io::Error::from(io::ErrorKind::ConnectionAborted));
            listener.failed(&io::Error::other("the process has no descriptor left"));
            listener.failed(&io::Error::other("the process has no descriptor left"));
            listener.failed(&io::Error::from(io::ErrorKind::ConnectionReset));
        });

        assert_eq!(
            listener.failed_accepts, 2,
            "only the failures that are not a connection's own count in the run"
        );
        assert_eq!(
            logs.matches("a public listener failed to accept a connection; it keeps accepting")
                .count(),
            1,
            "the run is reported once, at its first failure:\n{logs}"
        );
        assert!(
            !logs.contains("a connection ended before its listener accepted it"),
            "a connection's own failure is a debug record, below the default level:\n{logs}"
        );
    }

    #[test]
    fn only_an_accept_its_connection_failed_is_skipped_without_a_report() {
        for kind in [
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::Interrupted,
        ] {
            assert!(
                is_connection_accept_failure(&io::Error::from(kind)),
                "{kind:?}"
            );
        }
        for kind in [
            io::ErrorKind::Other,
            io::ErrorKind::OutOfMemory,
            io::ErrorKind::PermissionDenied,
        ] {
            assert!(
                !is_connection_accept_failure(&io::Error::from(kind)),
                "{kind:?}"
            );
        }
    }
}
