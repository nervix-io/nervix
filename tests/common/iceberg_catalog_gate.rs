//! A loopback HTTP/1.1 proxy a scenario stands in front of the Iceberg REST catalog, so it decides
//! what becomes of each commit to one table while every other request reaches the catalog unchanged.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One loopback listener, the connections it serves, the requests it forwards to the
//!   catalog, and the commits to its table it holds until the scenario refuses or passes each one,
//!   with the instants each arrived and was decided.
//! - **Depends on.** Hyper's HTTP/1.1 server, Reqwest, and the boundary's sockets, tasks, watch
//!   channel and cancellation.
//! - **Must not know.** Nervix, or the Iceberg REST protocol beyond which request commits to a
//!   table and the answer a catalog refuses a commit with.
//!
//! A commit to a table is a `POST` to the table's own resource, `…/namespaces/<namespace>/tables/
//! <table>`. Loading the table is a `GET` of that resource and every other request names another
//! one, so a method and a path identify a commit. The gate answers a refused commit `409
//! Conflict`, the answer a catalog gives when the requirements a commit carries no longer hold, so
//! the client knows the table did not change. A passed commit reaches the catalog, which answers
//! it. The gate records when each held commit arrived and when the scenario refused it, so a
//! scenario measures the delay between a refusal and the commit that retries it instead of waiting
//! for silence.
//!
//! Everything the gate keeps is bounded by the scenario: it records only the commits it held, and a
//! commit is held only while the scenario holds commits and decides them one at a time.

use std::{convert::Infallible, future::Future, io, net::SocketAddr};

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::{
    Method, Request, Response, StatusCode,
    body::Incoming,
    header::{self, HeaderMap, HeaderName, HeaderValue},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{Arc, CancellationToken, watch},
    task::JoinHandle,
    time::Instant,
};
use nervix_recovery::Discarded as _;
use url::Url;

/// The body of a refused commit: the error a REST catalog answers a commit whose requirements no
/// longer hold with.
const REFUSAL_BODY: &str = r#"{"error":{"message":"the scenario's catalog gate refused this commit","type":"CommitFailedException","code":409}}"#;

/// What the gate does with a commit to its table when one arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitPolicy {
    /// Holds the commit until the scenario refuses or passes it.
    Hold,
    /// Passes the commit to the catalog.
    Pass,
}

/// What the scenario decided for one held commit, and when.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitDecision {
    Refused { at: Instant },
    Passed,
}

/// One commit the gate held: when it arrived and, once the scenario decided it, the decision.
#[derive(Debug, Clone, Copy)]
struct HeldCommit {
    arrived_at: Instant,
    decision: Option<CommitDecision>,
}

/// What the gate does with the next commit to its table, and every commit it held, in arrival
/// order.
#[derive(Debug, Clone)]
struct GateCommits {
    policy: CommitPolicy,
    held: Vec<HeldCommit>,
}

impl GateCommits {
    /// Where the first commit the scenario has not decided yet is among the held commits.
    fn undecided(&self) -> Option<usize> {
        self.held
            .iter()
            .position(|commit| commit.decision.is_none())
    }

    /// When the gate refused a commit last before `index`, if it refused any.
    fn refusal_before(&self, index: usize) -> Option<Instant> {
        let mut refused_at = None;
        for commit in self.held.iter().take(index) {
            if let Some(CommitDecision::Refused { at }) = commit.decision {
                refused_at = Some(at);
            }
        }
        refused_at
    }

    /// Decides the first undecided commit, and returns where it is, or nothing when the gate
    /// holds none.
    fn decide(&mut self, decision: CommitDecision) -> Option<usize> {
        let index = self.undecided()?;
        let commit = self.held.get_mut(index)?;
        commit.decision = Some(decision);
        Some(index)
    }
}

/// A commit the gate holds for the scenario to decide.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HeldCommitArrival {
    /// When the commit reached the gate.
    pub(crate) arrived_at: Instant,
    /// When the gate refused the commit it held before this one, if it refused any.
    pub(crate) previous_refusal: Option<Instant>,
}

/// A loopback proxy in front of the Iceberg REST catalog that holds every commit to one table until
/// the scenario refuses or passes it, and forwards every other request.
#[derive(Debug)]
pub(crate) struct IcebergCatalogGate {
    local_addr: SocketAddr,
    commits: watch::Sender<GateCommits>,
    /// Cancelled to close every connection the gate serves, when the gate stops.
    connections: CancellationToken,
    listener: JoinHandle<()>,
}

/// What every connection the gate serves answers its requests with.
struct GateService {
    catalog: Url,
    /// The end of the path of the gated table's resource, `/tables/<table>`.
    table_resource: String,
    client: reqwest::Client,
    commits: watch::Sender<GateCommits>,
}

impl IcebergCatalogGate {
    /// Listens on a loopback port and forwards every request it serves to `catalog`, holding each
    /// commit to `table` until the scenario decides it.
    pub(crate) async fn start(catalog: Url, table: &str) -> io::Result<Self> {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let local_addr = listener.local_addr()?;
        // The gate forwards what the catalog sends as the catalog sent it, compressed or not.
        let client = reqwest::Client::builder()
            .no_proxy()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .build()
            .map_err(io::Error::other)?;
        let (commits, _) = watch::channel(GateCommits {
            policy: CommitPolicy::Hold,
            held: Vec::new(),
        });
        let service = Arc::new(GateService {
            catalog,
            table_resource: format!("/tables/{table}"),
            client,
            commits: commits.clone(),
        });
        let connections = CancellationToken::new();
        let listener =
            nervix_primitives::task::spawn(Self::accept(listener, service, connections.clone()));
        Ok(Self {
            local_addr,
            commits,
            connections,
            listener,
        })
    }

    /// The catalog URI a client reaches the catalog through the gate with.
    pub(crate) fn uri(&self) -> String {
        format!("http://{}", self.local_addr)
    }

    /// Resolves with the first commit the gate holds that the scenario has not decided yet.
    pub(crate) fn held_commit(
        &self,
    ) -> impl Future<Output = io::Result<HeldCommitArrival>> + use<> {
        let mut commits = self.commits.subscribe();
        async move {
            let commits = commits
                .wait_for(|commits| commits.undecided().is_some())
                .await
                .map_err(io::Error::other)?;
            let Some(index) = commits.undecided() else {
                return Err(io::Error::other(
                    "the gate held a commit that was decided before it was observed",
                ));
            };
            let Some(commit) = commits.held.get(index) else {
                return Err(io::Error::other(
                    "an undecided commit is one the gate holds",
                ));
            };
            Ok(HeldCommitArrival {
                arrived_at: commit.arrived_at,
                previous_refusal: commits.refusal_before(index),
            })
        }
    }

    /// Answers the commit the gate holds with a catalog's refusal, and returns when it refused it,
    /// or nothing when the gate holds no commit.
    pub(crate) fn refuse_held_commit(&self) -> Option<Instant> {
        let refused_at = Instant::now();
        let mut refused = None;
        self.commits.send_if_modified(|commits| {
            refused = commits.decide(CommitDecision::Refused { at: refused_at });
            refused.is_some()
        });
        refused.map(|_| refused_at)
    }

    /// Passes the commit the gate holds to the catalog, and holds the commits after it. Whether the
    /// gate held a commit to pass.
    pub(crate) fn pass_held_commit(&self) -> bool {
        let mut passed = None;
        self.commits.send_if_modified(|commits| {
            passed = commits.decide(CommitDecision::Passed);
            passed.is_some()
        });
        passed.is_some()
    }

    /// Passes the commit the gate holds, if it holds one, and every commit after it.
    pub(crate) fn pass_commits(&self) {
        self.commits.send_modify(|commits| {
            commits.policy = CommitPolicy::Pass;
            for commit in &mut commits.held {
                if commit.decision.is_none() {
                    commit.decision = Some(CommitDecision::Passed);
                }
            }
        });
    }

    async fn accept(listener: TcpListener, service: Arc<GateService>, stopped: CancellationToken) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let accepted = nervix_primitives::select! {
                _ = stopped.cancelled() => return,
                accepted = listener.accept() => accepted,
            };
            let Ok((client, _)) = accepted else {
                return;
            };
            nervix_primitives::task::spawn(Self::serve(
                client,
                Arc::clone(&service),
                stopped.clone(),
            ));
        }
    }

    /// Serves one client connection until the client closes it or the gate stops.
    async fn serve(client: TcpStream, service: Arc<GateService>, stopped: CancellationToken) {
        let connection = http1::Builder::new().serve_connection(
            TokioIo::new(client),
            service_fn(move |request| {
                let service = Arc::clone(&service);
                async move { Ok::<_, Infallible>(service.answer(request).await) }
            }),
        );
        nervix_primitives::select! {
            _ = stopped.cancelled() => {}
            served = connection => {
                served.discarded("a connection the client ends early ends only its own requests");
            }
        }
    }
}

impl GateService {
    /// Answers one request: a commit to the gated table as the scenario decides it, and every other
    /// request as the catalog does.
    async fn answer(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        if self.commits_to_table(&request)
            && let Some(CommitDecision::Refused { .. }) = self.decision().await
        {
            return Self::refusal();
        }
        self.forward(request).await
    }

    /// Whether `request` commits to the gated table.
    fn commits_to_table(&self, request: &Request<Incoming>) -> bool {
        request.method() == Method::POST && request.uri().path().ends_with(&self.table_resource)
    }

    /// Holds a commit that arrives while the gate holds commits until the scenario decides it, and
    /// returns the decision. A commit that arrives while the gate passes commits has none.
    async fn decision(&self) -> Option<CommitDecision> {
        let mut held = None;
        self.commits.send_if_modified(|commits| {
            if commits.policy == CommitPolicy::Pass {
                return false;
            }
            held = Some(commits.held.len());
            commits.held.push(HeldCommit {
                arrived_at: Instant::now(),
                decision: None,
            });
            true
        });
        let index = held?;
        let mut commits = self.commits.subscribe();
        let decided = commits
            .wait_for(|commits| {
                commits
                    .held
                    .get(index)
                    .is_some_and(|commit| commit.decision.is_some())
            })
            .await;
        // The service holds a sender for as long as it answers, so the wait ends with a decision.
        let decided = decided.ok()?;
        decided.held.get(index)?.decision
    }

    /// The answer a catalog refuses a commit whose requirements no longer hold with.
    fn refusal() -> Response<Full<Bytes>> {
        let mut refusal = Response::new(Full::new(Bytes::from_static(REFUSAL_BODY.as_bytes())));
        *refusal.status_mut() = StatusCode::CONFLICT;
        refusal.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        refusal
    }

    /// Sends `request` to the catalog and answers with what the catalog answered.
    async fn forward(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let (parts, body) = request.into_parts();
        let body = match body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(error) => {
                return Self::bad_gateway(&format!("reading the request failed: {error}"));
            }
        };
        let mut url = self.catalog.clone();
        url.set_path(parts.uri.path());
        url.set_query(parts.uri.query());
        let mut headers = parts.headers;
        Self::remove_connection_headers(&mut headers);
        headers.remove(header::HOST);
        headers.remove(header::CONTENT_LENGTH);
        let sent = self
            .client
            .request(parts.method, url)
            .headers(headers)
            .body(body)
            .send()
            .await;
        let answer = match sent {
            Ok(answer) => answer,
            Err(error) => {
                return Self::bad_gateway(&format!("the catalog did not answer: {error}"));
            }
        };
        let status = answer.status();
        let mut headers = answer.headers().clone();
        let body = match answer.bytes().await {
            Ok(body) => body,
            Err(error) => {
                return Self::bad_gateway(&format!("reading the catalog's answer failed: {error}"));
            }
        };
        Self::remove_connection_headers(&mut headers);
        headers.remove(header::CONTENT_LENGTH);
        let mut forwarded = Response::new(Full::new(body));
        *forwarded.status_mut() = status;
        *forwarded.headers_mut() = headers;
        forwarded
    }

    /// Removes the headers that describe one hop's connection rather than the message, which each
    /// side of the gate sets for its own connection.
    fn remove_connection_headers(headers: &mut HeaderMap) {
        for name in [
            header::CONNECTION,
            header::PROXY_AUTHENTICATE,
            header::PROXY_AUTHORIZATION,
            header::TE,
            header::TRAILER,
            header::TRANSFER_ENCODING,
            header::UPGRADE,
            HeaderName::from_static("keep-alive"),
        ] {
            headers.remove(name);
        }
    }

    /// The answer to a request the gate could not carry to the catalog and back.
    fn bad_gateway(reason: &str) -> Response<Full<Bytes>> {
        let mut answer = Response::new(Full::new(Bytes::from(reason.to_string())));
        *answer.status_mut() = StatusCode::BAD_GATEWAY;
        answer
    }
}

impl Drop for IcebergCatalogGate {
    fn drop(&mut self) {
        self.listener.abort();
        self.connections.cancel();
    }
}
