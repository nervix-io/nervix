//! Bounded cluster-status requests the integration-test harness sends to a node.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The status-specific raw session command path, the four operations of one status
//!   request and the deadline that bounds each of them, their typed failures, requesting several
//!   nodes at once, and the harness status timing policy.
//! - **Depends on.** The generated session gRPC client, the command outcome vocabulary of the
//!   session client, and the phase deadline.
//! - **Must not know.** Ordinary scenario commands, scenario state, or the production session
//!   client's request, redirect and reconnect policy.

use std::{collections::BTreeMap, future::Future, io, net::SocketAddr, path::PathBuf};

use error_stack::Report;
use futures_util::future::join_all;
use nervix_client_core::{
    CommandOutcome, CommandOutcomeKind, Diagnostic,
    proto::{
        CommandRequest, SessionRequest, session_request, session_response::Event,
        session_service_client::SessionServiceClient,
    },
};
use thiserror::Error;
use tokio::{sync::mpsc, time::Duration};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request,
    metadata::{AsciiMetadataValue, errors::InvalidMetadataValue},
    transport::{Certificate, ClientTlsConfig, Endpoint},
};

use super::phase_deadline::{BeforeDeadline, PhaseDeadline};

/// How long every harness status wait may take: leadership, membership, interconnect and
/// applied-index convergence. A policy input, and the budget scenarios are written against.
pub(crate) const STATUS_WAIT_BUDGET: Duration = Duration::from_secs(40);
/// How many status requests a status wait outlasts when none of them ever replies, so a single
/// stalled connection cannot decide the wait. A policy input.
const STATUS_REQUESTS_PER_WAIT: u32 = 4;
/// One status request, from connecting to the node until its command result arrives.
pub(crate) const STATUS_REQUEST_TIMEOUT: Duration =
    match STATUS_WAIT_BUDGET.checked_div(STATUS_REQUESTS_PER_WAIT) {
        Some(timeout) => timeout,
        None => panic!("a status wait must outlast at least one status request"),
    };
/// Status diagnostics request every node at once, so their whole budget is one request timeout,
/// and a node that never replies delays whatever follows the diagnostics by at most that long.
pub(crate) const STATUS_DIAGNOSTIC_BUDGET: Duration = STATUS_REQUEST_TIMEOUT;
/// How many readiness requests a node startup attempt outlasts when none of them ever replies, so a
/// single stalled connection cannot decide the attempt. A policy input for every startup budget
/// that polls readiness with status requests.
pub(crate) const STATUS_REQUESTS_PER_STARTUP: u32 = 3;
/// The slowest reply a healthy node gave one status request while the whole scenario suite ran at
/// the CI concurrency factor of two scenarios per CPU. A policy input: measure it again when the
/// suite or its concurrency changes.
const SLOWEST_HEALTHY_STATUS_REPLY: Duration = Duration::from_millis(3_700);
/// How many times the slowest healthy reply a status request allows before the harness treats its
/// node as stalled, so a runner slower than the measuring one still gets its replies. A policy
/// input.
const STATUS_REPLY_HEADROOM: u32 = 2;
const _: () = assert!(
    STATUS_REQUESTS_PER_WAIT >= 2 && STATUS_REQUESTS_PER_STARTUP >= 2,
    "a status request must be materially shorter than the wait or startup that retries it"
);
const _: () = assert!(
    STATUS_DIAGNOSTIC_BUDGET.as_nanos() < STATUS_WAIT_BUDGET.as_nanos(),
    "status diagnostics must stay short beside the waits whose failures they explain"
);
const _: () = assert!(
    match SLOWEST_HEALTHY_STATUS_REPLY.checked_mul(STATUS_REPLY_HEADROOM) {
        Some(allowance) => allowance.as_nanos() <= STATUS_REQUEST_TIMEOUT.as_nanos(),
        None => false,
    },
    "a status request must leave the slowest healthy reply its headroom"
);

/// One step of a status request, in the order the request performs them.
#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::Display)]
pub(crate) enum StatusOperation {
    #[strum(serialize = "endpoint connection")]
    Connect,
    #[strum(serialize = "session establishment")]
    OpenSession,
    #[strum(serialize = "command send")]
    SendCommand,
    #[strum(serialize = "response receive")]
    ReceiveResponse,
}

impl StatusOperation {
    /// Runs this step of a status request until it finishes or the request deadline passes.
    pub(crate) async fn within<F>(
        self,
        deadline: PhaseDeadline,
        step: F,
    ) -> Result<F::Output, Report<StatusRequestError>>
    where
        F: Future,
    {
        let bounded = deadline.bound(step).await;
        match bounded {
            BeforeDeadline::Finished(output) => Ok(output),
            BeforeDeadline::Passed => Err(Report::new(StatusRequestError::DeadlinePassed {
                operation: self,
                budget: deadline.budget(),
            })),
        }
    }
}

/// Why a status request ended without the node's status.
#[derive(Debug, Error)]
pub(crate) enum StatusRequestError {
    #[error("{operation} was still pending when the {budget:?} status request deadline passed")]
    DeadlinePassed {
        operation: StatusOperation,
        budget: Duration,
    },
    #[error("the status endpoint could not be configured")]
    ConfigureEndpoint(#[source] tonic::transport::Error),
    #[error("the status TLS authority at {} could not be read", .authority.display())]
    ReadTlsAuthority {
        authority: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("the status request credentials are not valid request metadata")]
    Authorization(#[source] InvalidMetadataValue),
    #[error("the status endpoint connection failed")]
    Connect(#[source] tonic::transport::Error),
    #[error("opening the status session failed")]
    OpenSession(#[source] tonic::Status),
    #[error("the status session closed before its command was sent")]
    SendCommand,
    #[error("the status response stream failed")]
    ReceiveResponse(#[source] tonic::Status),
    #[error("the status session ended before the command result arrived")]
    SessionEnded,
    #[error(
        "the status command returned an unsuccessful {kind:?} result: {message}; diagnostics: \
         {diagnostics:?}"
    )]
    Unsuccessful {
        kind: CommandOutcomeKind,
        message: String,
        diagnostics: Vec<Diagnostic>,
    },
}

/// How a status request secures its connection to the node.
#[derive(Clone, Debug)]
pub(crate) enum StatusTransport {
    Plaintext,
    /// TLS that trusts the certificate authority stored at `authority`.
    Tls {
        authority: PathBuf,
    },
}

impl StatusTransport {
    fn endpoint(&self, address: SocketAddr) -> Result<Endpoint, Report<StatusRequestError>> {
        match self {
            Self::Plaintext => Endpoint::from_shared(format!("http://{address}"))
                .map_err(|error| Report::new(StatusRequestError::ConfigureEndpoint(error))),
            Self::Tls { authority } => {
                let endpoint = Endpoint::from_shared(format!("https://{address}"))
                    .map_err(|error| Report::new(StatusRequestError::ConfigureEndpoint(error)))?;
                let authority_pem = std::fs::read(authority).map_err(|source| {
                    Report::new(StatusRequestError::ReadTlsAuthority {
                        authority: authority.clone(),
                        source,
                    })
                })?;
                let tls =
                    ClientTlsConfig::new().ca_certificate(Certificate::from_pem(authority_pem));
                endpoint
                    .tls_config(tls)
                    .map_err(|error| Report::new(StatusRequestError::ConfigureEndpoint(error)))
            }
        }
    }
}

/// A node's session endpoint and the credentials a status request presents to it.
pub(crate) struct StatusEndpoint {
    address: SocketAddr,
    transport: StatusTransport,
    authorization: String,
}

impl StatusEndpoint {
    const STATUS_QUERY: &str = "SHOW CLUSTER STATUS;";

    pub(crate) fn new(
        address: SocketAddr,
        transport: StatusTransport,
        authorization: String,
    ) -> Self {
        Self {
            address,
            transport,
            authorization,
        }
    }

    /// Sends `SHOW CLUSTER STATUS;` on a new session and returns the node's command outcome.
    ///
    /// The request's deadline is the status request timeout, or the time left in `phase` when
    /// that is shorter. Connecting, opening the session, sending the command and receiving its
    /// result each receive only the time left before that one deadline.
    pub(crate) async fn request(
        &self,
        phase: PhaseDeadline,
    ) -> Result<CommandOutcome, Report<StatusRequestError>> {
        let deadline = phase.nested(STATUS_REQUEST_TIMEOUT);
        let endpoint = self.transport.endpoint(self.address)?;
        let authorization = AsciiMetadataValue::try_from(self.authorization.as_str())
            .map_err(|error| Report::new(StatusRequestError::Authorization(error)))?;

        let connected = StatusOperation::Connect
            .within(deadline, endpoint.connect())
            .await?;
        let channel = connected.map_err(|error| Report::new(StatusRequestError::Connect(error)))?;

        // Holding the command sender keeps the session's request stream open until the result
        // arrives, so the node never sees the session end while it answers.
        let (command_tx, command_rx) = mpsc::channel(1);
        let mut session = Request::new(ReceiverStream::new(command_rx));
        session
            .metadata_mut()
            .insert("authorization", authorization);
        let mut client = SessionServiceClient::new(channel);
        let opened = StatusOperation::OpenSession
            .within(deadline, client.session(session))
            .await?;
        let mut responses = opened
            .map_err(|status| Report::new(StatusRequestError::OpenSession(status)))?
            .into_inner();

        let sent = StatusOperation::SendCommand
            .within(deadline, command_tx.send(Self::status_command()))
            .await?;
        sent.map_err(|_| Report::new(StatusRequestError::SendCommand))?;

        loop {
            tokio::task::consume_budget().await;
            let received = StatusOperation::ReceiveResponse
                .within(deadline, responses.message())
                .await?;
            let response = received
                .map_err(|status| Report::new(StatusRequestError::ReceiveResponse(status)))?;
            let Some(response) = response else {
                return Err(Report::new(StatusRequestError::SessionEnded));
            };
            match response.event {
                Some(Event::Result(result)) => return Ok(CommandOutcome::from(*result)),
                // Server, subscription and session-state events may precede the result.
                Some(
                    Event::Subscription(_)
                    | Event::Server(_)
                    | Event::Suggest(_)
                    | Event::Snapshot(_)
                    | Event::Domains(_)
                    | Event::Cluster(_),
                )
                | None => {}
            }
        }
    }

    /// The status text the node reported. An unsuccessful command result is a failure too.
    pub(crate) async fn cluster_status(
        &self,
        phase: PhaseDeadline,
    ) -> Result<String, Report<StatusRequestError>> {
        let outcome = self.request(phase).await?;
        if outcome.success {
            Ok(outcome.message)
        } else {
            Err(Report::new(StatusRequestError::Unsuccessful {
                kind: outcome.kind,
                message: outcome.message,
                diagnostics: outcome.diagnostics,
            }))
        }
    }

    /// Requests every node's status text at once, so a node that never replies cannot delay
    /// another node's reply. Each node keeps its own outcome: its status text, or the typed
    /// failure that ended its request, including the operation a passed deadline interrupted.
    pub(crate) async fn cluster_statuses<K>(
        endpoints: &BTreeMap<K, Self>,
        phase: PhaseDeadline,
    ) -> BTreeMap<K, Result<String, Report<StatusRequestError>>>
    where
        K: Clone + Ord,
    {
        let requests = endpoints
            .values()
            .map(|endpoint| endpoint.cluster_status(phase));
        let statuses = join_all(requests).await;
        endpoints.keys().cloned().zip(statuses).collect()
    }

    fn status_command() -> SessionRequest {
        SessionRequest {
            request: Some(session_request::Request::Command(CommandRequest {
                query: Self::STATUS_QUERY.to_string(),
                // Cluster status belongs to no domain, so the request names none.
                domain: String::new(),
                execution_reference: uuid::Uuid::now_v7().to_string(),
                expected_transaction_position: None,
            })),
        }
    }
}
