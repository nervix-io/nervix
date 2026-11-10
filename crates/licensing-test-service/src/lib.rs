//! A scenario-owned HTTPS implementation of the shared licensing contract.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** A bounded in-memory service, signed answers, fault scripts and request observations.
//! - **Depends on.** The shared protocol, AWS-LC, maintained HTTP/TLS stacks and selected primitives.
//! - **Must not know.** Server policy, consensus, Plexus or any internal authorization flag.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "scenario-owned signed HTTPS peer, absent from product graphs"
    )
)]

mod model;
#[cfg(test)]
mod tests;
mod tls;

use std::{num::NonZeroUsize, path::Path, time::Duration};

use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::Uri,
    routing::post,
};
use chrono::{DateTime, Utc};
use error_stack::ResultExt as _;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
    service::TowerToHyperService,
};
use meticulous::{OptionExt as _, ResultExt as _};
pub use model::{Fault, Model, Tamper};
use nervix_licensing_protocol::{document::DocumentLimits, documents::EmbeddedTrustBundle};
use nervix_primitives::{
    net::TcpListener,
    sync::{Arc, CancellationToken, Mutex, watch},
    task::{AbortOnDropHandle, JoinSet},
    time::timeout,
};
use nervix_recovery::Discarded as _;
use thiserror::Error;
use url::Url;

/// These fixture bounds admit the specification's largest current documents while keeping a
/// stalled scenario finite. They are harness policy, never product or commercial defaults.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the scenario peer and its malformed-response fixtures share a 64-KiB admission \
                  envelope; this peer is absent from product graphs"
    )
)]
pub const MAX_BODY_BYTES: usize = 65_536;
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the 64-connection scenario peer bounds its TLS task inventory for the one- and \
                  three-node public checks"
    )
)]
const MAX_CONNECTIONS: usize = 64;
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "a fixture connection lasts at most two minutes so a stalled peer stays inside \
                  the verification envelope"
    )
)]
const CONNECTION_BUDGET: Duration = Duration::from_secs(120);
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "fixture teardown joins its bounded connection inventory within the five-second \
                  peer-stop contract"
    )
)]
const STOP_BUDGET: Duration = Duration::from_secs(5);

pub fn limits() -> DocumentLimits {
    DocumentLimits {
        bytes: NonZeroUsize::new(MAX_BODY_BYTES).assured("fixture body bound is nonzero"),
        signatures: NonZeroUsize::new(8).assured("fixture signature bound is nonzero"),
        trust_documents: NonZeroUsize::new(64).assured("fixture trust bound is nonzero"),
    }
}

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("failed to prepare the licensing fixture's TLS identity")]
    Tls,
    #[error("failed to bind the licensing fixture")]
    Bind,
    #[error("failed to prepare the licensing fixture's signed model")]
    Model,
    #[error("the licensing fixture did not stop within its budget")]
    Stop,
}

type SharedModel = Arc<Mutex<Model>>;

#[derive(Clone)]
struct ServerState {
    model: SharedModel,
    online: watch::Receiver<bool>,
    stop: CancellationToken,
}

pub struct Service {
    model: SharedModel,
    identity: tls::Identity,
    address: Url,
    stop: CancellationToken,
    online: watch::Sender<bool>,
    task: Option<AbortOnDropHandle<()>>,
}

impl Service {
    pub async fn start(at: DateTime<Utc>) -> error_stack::Result<Self, ServiceError> {
        let identity = tls::Identity::generate()?;
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .change_context(ServiceError::Bind)?;
        let socket = listener.local_addr().change_context(ServiceError::Bind)?;
        let mut address = Url::parse("https://127.0.0.1").assured("fixed HTTPS base URL");
        address
            .set_port(Some(socket.port()))
            .assured("HTTPS URLs accept a port");
        let model = Arc::new(Mutex::new(Model::new(address.clone(), at)?));
        let (online, reachable) = watch::channel(true);
        let stop = CancellationToken::new();
        let router = Router::new()
            .route("/v1/challenges", post(request))
            .route("/v1/activations", post(request))
            .route("/v1/activations/{id}/refresh", post(request))
            .route("/v1/activations/{id}/key", post(request))
            .route("/v1/activations/{id}/release", post(request))
            .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
            .with_state(ServerState {
                model: model.clone(),
                online: reachable.clone(),
                stop: stop.clone(),
            });
        let stopped = stop.clone();
        let acceptor = identity.acceptor.clone();
        let task = nervix_primitives::task::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::select! {
                    _ = stopped.cancelled() => break,
                    result = connections.join_next(), if !connections.is_empty() => {
                        result.discarded("connection tasks have no externally required return value");
                    },
                    accepted = listener.accept(), if connections.len() < MAX_CONNECTIONS => {
                        let Ok((socket, _)) = accepted else { break; };
                        if !*reachable.borrow() {
                            continue;
                        }
                        let acceptor = acceptor.clone();
                        let router = router.clone();
                        let ended = stopped.clone();
                        connections.spawn(async move {
                            let connection = async {
                                let Ok(stream) = acceptor.accept(socket).await else { return; };
                                let mut builder = auto::Builder::new(TokioExecutor::new());
                                builder.http2().max_concurrent_streams(Some(16));
                                builder.serve_connection(TokioIo::new(stream), TowerToHyperService::new(router))
                                    .await.discarded("fault scripts may end a peer before its response completes");
                            };
                            nervix_primitives::select! {
                                _ = ended.cancelled() => {},
                                result = timeout(CONNECTION_BUDGET, connection) => {
                                    result.discarded("the fixture bounds every connection lifetime");
                                },
                            }
                        });
                    },
                }
            }
            connections.abort_all();
            while let Some(result) = connections.join_next().await {
                nervix_primitives::task::consume_budget().await;
                result.discarded("fixture shutdown joins every aborted connection");
            }
        });
        Ok(Self {
            model,
            identity,
            address,
            stop,
            online,
            task: Some(AbortOnDropHandle::new(task)),
        })
    }

    pub fn address(&self) -> &Url {
        &self.address
    }
    pub fn ca_file(&self) -> &Path {
        &self.identity.ca_file
    }
    /// Refuse transport setup while keeping the exact listener address for reconnection.
    pub fn set_online(&self, online: bool) {
        self.online.send_replace(online);
    }
    pub async fn trust_bundle(&self) -> EmbeddedTrustBundle {
        self.model.lock().await.bundle.clone()
    }
    pub async fn configure<T>(&self, update: impl FnOnce(&mut Model) -> T) -> T {
        update(&mut *self.model.lock().await)
    }
    pub async fn request_count(&self) -> u64 {
        self.model.lock().await.request_count
    }
    pub async fn requests(&self) -> Vec<nervix_licensing_protocol::requests::Request> {
        self.model.lock().await.requests.clone()
    }
    pub async fn stop(&mut self) -> error_stack::Result<(), ServiceError> {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            timeout(STOP_BUDGET, task)
                .await
                .change_context(ServiceError::Stop)?
                .change_context(ServiceError::Stop)?;
        }
        Ok(())
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

async fn request(
    State(state): State<ServerState>,
    uri: Uri,
    body: Bytes,
) -> axum::response::Response {
    if !*state.online.borrow() {
        return lost_response();
    }
    let (response, fault) = state.model.lock().await.answer(uri.path(), &body);
    match fault {
        Some(Fault::Delay(duration)) => {
            nervix_primitives::select! {
                _ = state.stop.cancelled() => return lost_response(),
                _ = nervix_primitives::time::sleep(duration) => {},
            }
        }
        Some(Fault::LoseResponse) => return lost_response(),
        _ => {}
    }
    response
}

fn lost_response() -> axum::response::Response {
    let error = std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "scripted lost licensing response",
    );
    axum::response::Response::new(axum::body::Body::from_stream(futures_util::stream::once(
        async { Err::<Bytes, _>(error) },
    )))
}
