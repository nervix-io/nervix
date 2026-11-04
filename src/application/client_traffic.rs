//! What a node records about the traffic its client listeners carry: the calls they authenticate
//! or refuse, the sessions they serve, the requests those sessions decode, how the requests end,
//! and the transfer parts of replies larger than one frame.
//!
//! Layer: edges.
//!
//! - **Owns.** The series of that traffic for each client listener, resolved once when the
//!   service starts so that no request looks a label up, and the operator's record of a call whose
//!   credentials did not authenticate it.
//! - **Depends on.** The node's metric families, the session transports, the wire vocabulary of
//!   requests, rejections and cancellation stages, and the verdicts authentication reaches.
//! - **Must not know.** What a request does once it is served, or what credentials a call
//!   presented: no series and no record carries a user name, a password or a payload value.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::net::SocketAddr;

use meticulous::OptionExt as _;
use nervix_client_wire::{CancellationStage, ClientRequestKind, ReplyBody, RequestRejection};
use nervix_models::UserName;
use prometheus::{IntCounter, IntGauge};
use strum::IntoEnumIterator as _;
use tracing::{debug, warn};

use super::{
    authentication::{AuthenticationRefusal, BasicAuthCredentials},
    session::SessionTransport,
    session_service::SessionServiceImpl,
};
use crate::metrics::ClientTrafficMetrics;

/// What became of one call's authentication, as the authentication series labels it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::IntoStaticStr, strum::EnumIter)]
#[strum(serialize_all = "snake_case")]
enum AuthenticationOutcome {
    Authenticated,
    Missing,
    Rejected,
    Unavailable,
}

impl AuthenticationOutcome {
    fn of(authenticated: &Result<UserName, AuthenticationRefusal>) -> Self {
        match authenticated {
            Ok(_) => Self::Authenticated,
            Err(AuthenticationRefusal::Missing) => Self::Missing,
            Err(AuthenticationRefusal::Rejected) => Self::Rejected,
            Err(AuthenticationRefusal::Unavailable) => Self::Unavailable,
        }
    }
}

/// How a reply ends the request it answers, as the traffic series count it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) enum ReplyEnding {
    /// The request was served, whatever its outcome says.
    Served,
    /// The request was not served.
    Rejected(RequestRejection),
    /// A cancellation ended the request.
    Cancelled(CancellationStage),
}

impl ReplyEnding {
    pub(in crate::application) fn of(body: &ReplyBody) -> Self {
        match body {
            ReplyBody::Rejected(rejected) => Self::Rejected(rejected.rejection),
            ReplyBody::Cancelled(cancelled) => Self::Cancelled(cancelled.stage),
            _ => Self::Served,
        }
    }
}

/// The client listener a call arrived on and the address its connection came from, when the
/// listener knows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::application) struct ClientCall {
    pub(in crate::application) transport: SessionTransport,
    pub(in crate::application) peer: Option<SocketAddr>,
}

/// The traffic series of every client listener of one node.
pub(in crate::application) struct ClientTraffic {
    grpc: ListenerTraffic,
    console: ListenerTraffic,
}

/// The traffic series of one client listener, each child resolved when the service starts.
struct ListenerTraffic {
    sessions: IntGauge,
    sessions_opened: IntCounter,
    requests: ahash::HashMap<ClientRequestKind, IntCounter>,
    rejections: ahash::HashMap<RequestRejection, IntCounter>,
    cancellations: ahash::HashMap<CancellationStage, IntCounter>,
    transfer_parts: IntCounter,
    authentications: ahash::HashMap<AuthenticationOutcome, IntCounter>,
}

impl ListenerTraffic {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "every child of a client listener's series is resolved once, when the \
                      session service starts"
        )
    )]
    fn new(metrics: &ClientTrafficMetrics, transport: SessionTransport) -> Self {
        let transport: &'static str = transport.into();
        let mut requests = ahash::HashMap::default();
        for kind in ClientRequestKind::iter() {
            requests.insert(kind, metrics.requests(transport, kind.into()));
        }
        let mut rejections = ahash::HashMap::default();
        for rejection in RequestRejection::iter() {
            rejections.insert(rejection, metrics.rejections(transport, rejection.into()));
        }
        let mut cancellations = ahash::HashMap::default();
        for stage in CancellationStage::iter() {
            cancellations.insert(stage, metrics.cancellations(transport, stage.into()));
        }
        let mut authentications = ahash::HashMap::default();
        for outcome in AuthenticationOutcome::iter() {
            authentications.insert(outcome, metrics.authentications(transport, outcome.into()));
        }
        Self {
            sessions: metrics.sessions(transport),
            sessions_opened: metrics.sessions_opened(transport),
            requests,
            rejections,
            cancellations,
            transfer_parts: metrics.transfer_parts(transport),
            authentications,
        }
    }
}

/// One session a client listener serves, which the listener's open sessions count until it is
/// dropped.
pub(in crate::application) struct ServedSession {
    sessions: IntGauge,
}

impl Drop for ServedSession {
    fn drop(&mut self) {
        self.sessions.dec();
    }
}

impl ClientTraffic {
    pub(in crate::application) fn new(metrics: &ClientTrafficMetrics) -> Self {
        Self {
            grpc: ListenerTraffic::new(metrics, SessionTransport::Grpc),
            console: ListenerTraffic::new(metrics, SessionTransport::Console),
        }
    }

    fn listener(&self, transport: SessionTransport) -> &ListenerTraffic {
        match transport {
            SessionTransport::Grpc => &self.grpc,
            SessionTransport::Console => &self.console,
        }
    }

    /// Counts a session `transport` began to serve, which stays among its open sessions until the
    /// returned guard is dropped.
    pub(in crate::application) fn serve_session(
        &self,
        transport: SessionTransport,
    ) -> ServedSession {
        let listener = self.listener(transport);
        listener.sessions_opened.inc();
        listener.sessions.inc();
        ServedSession {
            sessions: listener.sessions.clone(),
        }
    }

    /// Counts a request a session on `transport` decoded.
    pub(in crate::application) fn request(
        &self,
        transport: SessionTransport,
        kind: ClientRequestKind,
    ) {
        self.listener(transport)
            .requests
            .get(&kind)
            .assured("every request kind's child is resolved when the traffic is built")
            .inc();
    }

    /// Counts how a reply a session on `transport` queued ended its request: a rejection by its
    /// reason, a cancellation by the stage it found the request at, and nothing for a request the
    /// session served.
    pub(in crate::application) fn reply(&self, transport: SessionTransport, ending: ReplyEnding) {
        let listener = self.listener(transport);
        match ending {
            ReplyEnding::Served => {}
            ReplyEnding::Rejected(rejection) => listener
                .rejections
                .get(&rejection)
                .assured("every rejection's child is resolved when the traffic is built")
                .inc(),
            ReplyEnding::Cancelled(stage) => listener
                .cancellations
                .get(&stage)
                .assured("every cancellation stage's child is resolved when the traffic is built")
                .inc(),
        }
    }

    /// Counts one transfer part a session on `transport` queued.
    pub(in crate::application) fn transfer_part(&self, transport: SessionTransport) {
        self.listener(transport).transfer_parts.inc();
    }

    fn authentication(
        &self,
        call: ClientCall,
        authenticated: &Result<UserName, AuthenticationRefusal>,
    ) {
        let outcome = AuthenticationOutcome::of(authenticated);
        self.listener(call.transport)
            .authentications
            .get(&outcome)
            .assured("every authentication outcome's child is resolved when the traffic is built")
            .inc();
    }
}

impl SessionServiceImpl {
    /// Authenticates the credentials `call` presented, counts what became of the attempt, and
    /// records a call whose credentials did not authenticate it for the operator.
    ///
    /// The record names the listener and the address the call came from and nothing the call
    /// presented, so neither a password nor a user name an operator did not create reaches the
    /// log.
    pub(in crate::application) async fn authenticate_client(
        &self,
        call: ClientCall,
        credentials: Option<&BasicAuthCredentials>,
    ) -> Result<UserName, AuthenticationRefusal> {
        let authenticated = self.authenticate(credentials).await;
        self.inner.traffic.authentication(call, &authenticated);
        let transport: &'static str = call.transport.into();
        // A record leaves out a field whose value is `None`, so a call whose transport knows no
        // peer address is recorded without one.
        let peer = call.peer.map(tracing::field::display);
        match &authenticated {
            Err(AuthenticationRefusal::Rejected) => {
                warn!(transport, peer, "client authentication failed");
            }
            Err(AuthenticationRefusal::Missing) => {
                debug!(transport, peer, "a client call presented no credentials");
            }
            Err(AuthenticationRefusal::Unavailable) | Ok(_) => {}
        }
        authenticated
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_client_wire::{RequestCancelled, RequestRejected};

    use super::*;
    use crate::{
        application::test_fixtures::{TestService, build_test_service},
        captured_logs::CapturedLogs,
        metrics::RuntimeMetrics,
    };

    /// The value `metrics` export for exactly `series`, a family name with its labels in
    /// exposition order, or `None` when they export no such series.
    fn exported(metrics: &RuntimeMetrics, series: &str) -> Option<f64> {
        let text = metrics.prometheus_text();
        for line in text.lines() {
            let Some(value) = line.strip_prefix(series) else {
                continue;
            };
            let Some(value) = value.strip_prefix(' ') else {
                continue;
            };
            let value = value
                .parse()
                .expect("the Prometheus text format prints a sample's value as a number");
            return Some(value);
        }
        None
    }

    #[test]
    fn every_series_of_every_listener_is_exported_once_the_traffic_is_built() {
        let metrics = RuntimeMetrics::default();
        let _traffic = ClientTraffic::new(metrics.client_traffic());

        for transport in SessionTransport::iter() {
            let transport: &'static str = transport.into();
            let mut series = vec![
                format!("nervix_sessions{{transport=\"{transport}\"}}"),
                format!("nervix_sessions_opened_total{{transport=\"{transport}\"}}"),
                format!("nervix_session_transfer_parts_total{{transport=\"{transport}\"}}"),
            ];
            for kind in ClientRequestKind::iter() {
                let kind: &'static str = kind.into();
                series.push(format!(
                    "nervix_session_requests_total{{request=\"{kind}\",transport=\"{transport}\"}}"
                ));
            }
            for rejection in RequestRejection::iter() {
                let rejection: &'static str = rejection.into();
                series.push(format!(
                    "nervix_session_rejections_total{{rejection=\"{rejection}\",transport=\"\
                     {transport}\"}}"
                ));
            }
            for stage in CancellationStage::iter() {
                let stage: &'static str = stage.into();
                series.push(format!(
                    "nervix_session_cancellations_total{{stage=\"{stage}\",transport=\"\
                     {transport}\"}}"
                ));
            }
            for outcome in AuthenticationOutcome::iter() {
                let outcome: &'static str = outcome.into();
                series.push(format!(
                    "nervix_client_authentications_total{{outcome=\"{outcome}\",transport=\"\
                     {transport}\"}}"
                ));
            }
            for series in series {
                assert_eq!(exported(&metrics, &series), Some(0.0), "{series}");
            }
        }
    }

    #[test]
    fn a_served_session_is_open_until_its_guard_is_dropped() {
        let metrics = RuntimeMetrics::default();
        let traffic = ClientTraffic::new(metrics.client_traffic());

        let first = traffic.serve_session(SessionTransport::Console);
        let second = traffic.serve_session(SessionTransport::Console);
        assert_eq!(
            exported(&metrics, "nervix_sessions{transport=\"console\"}"),
            Some(2.0)
        );
        drop(first);
        assert_eq!(
            exported(&metrics, "nervix_sessions{transport=\"console\"}"),
            Some(1.0)
        );
        drop(second);
        assert_eq!(
            exported(&metrics, "nervix_sessions{transport=\"console\"}"),
            Some(0.0)
        );
        assert_eq!(
            exported(
                &metrics,
                "nervix_sessions_opened_total{transport=\"console\"}"
            ),
            Some(2.0)
        );
        assert_eq!(
            exported(&metrics, "nervix_sessions_opened_total{transport=\"grpc\"}"),
            Some(0.0)
        );
    }

    #[test]
    fn a_reply_counts_the_rejection_or_cancellation_that_ended_its_request() {
        let metrics = RuntimeMetrics::default();
        let traffic = ClientTraffic::new(metrics.client_traffic());
        let rejected = ReplyBody::Rejected(RequestRejected {
            rejection: RequestRejection::TooManyRequestsInFlight,
            field: None,
            message: "a test rejection".to_string(),
        });
        let cancelled = ReplyBody::Cancelled(RequestCancelled {
            stage: CancellationStage::AfterAdmission,
        });

        traffic.reply(SessionTransport::Grpc, ReplyEnding::of(&rejected));
        traffic.reply(SessionTransport::Grpc, ReplyEnding::of(&cancelled));
        traffic.reply(SessionTransport::Grpc, ReplyEnding::Served);

        for (series, value) in [
            (
                "nervix_session_rejections_total{rejection=\"too_many_requests_in_flight\",\
                 transport=\"grpc\"}",
                1.0,
            ),
            (
                "nervix_session_cancellations_total{stage=\"after_admission\",transport=\"grpc\"}",
                1.0,
            ),
            (
                "nervix_session_cancellations_total{stage=\"before_admission\",transport=\"grpc\"}",
                0.0,
            ),
            (
                "nervix_session_rejections_total{rejection=\"too_many_requests_in_flight\",\
                 transport=\"console\"}",
                0.0,
            ),
        ] {
            assert_eq!(exported(&metrics, series), Some(value), "{series}");
        }
    }

    #[nervix_primitives::test]
    async fn a_refused_call_is_recorded_with_its_listener_and_peer_and_nothing_it_presented() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(false).await;
        let peer: SocketAddr = "192.0.2.7:41000"
            .parse()
            .assured("the test peer is an address literal");
        let call = ClientCall {
            transport: SessionTransport::Console,
            peer: Some(peer),
        };
        let presented = BasicAuthCredentials {
            username: "presented_user".to_string(),
            password: "presented-secret".to_string(),
        };

        let logs = CapturedLogs::default();
        // `#[nervix_primitives::test]` runs the whole future on the calling thread, so the
        // thread-local default the guard installs stays in force across the awaits.
        let guard = tracing::subscriber::set_default(logs.subscriber());
        let refused = service.authenticate_client(call, Some(&presented)).await;
        let missing = service.authenticate_client(call, None).await;
        drop(guard);

        assert_eq!(refused, Err(AuthenticationRefusal::Rejected));
        assert_eq!(missing, Err(AuthenticationRefusal::Missing));
        let logs = logs.contents();
        assert_eq!(
            logs.matches("client authentication failed").count(),
            1,
            "the refused call is recorded once:\n{logs}"
        );
        assert!(
            logs.contains("transport=\"console\"") && logs.contains("peer=192.0.2.7:41000"),
            "the record names the listener and the peer:\n{logs}"
        );
        assert!(
            !logs.contains("presented_user") && !logs.contains("presented-secret"),
            "the record carries nothing the call presented:\n{logs}"
        );
        assert!(
            !logs.contains("a client call presented no credentials"),
            "a call without credentials is a debug record, below the default level:\n{logs}"
        );
        assert_eq!(
            exported(
                &service.inner.runtime.metrics(),
                "nervix_client_authentications_total{outcome=\"rejected\",transport=\"console\"}"
            ),
            Some(1.0)
        );
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn an_authentication_is_counted_by_its_outcome_on_its_listener() {
        let metrics = RuntimeMetrics::default();
        let traffic = ClientTraffic::new(metrics.client_traffic());
        let call = ClientCall {
            transport: SessionTransport::Console,
            peer: None,
        };
        let user = UserName::parse("default").expect("the test user name is an identifier");

        traffic.authentication(call, &Ok(user));
        traffic.authentication(call, &Err(AuthenticationRefusal::Rejected));
        traffic.authentication(call, &Err(AuthenticationRefusal::Rejected));
        traffic.authentication(call, &Err(AuthenticationRefusal::Missing));
        traffic.authentication(call, &Err(AuthenticationRefusal::Unavailable));

        for (outcome, value) in [
            ("authenticated", 1.0),
            ("rejected", 2.0),
            ("missing", 1.0),
            ("unavailable", 1.0),
        ] {
            let series = format!(
                "nervix_client_authentications_total{{outcome=\"{outcome}\",transport=\"console\"\
                 }}"
            );
            assert_eq!(exported(&metrics, &series), Some(value), "{series}");
        }
        assert_eq!(
            exported(
                &metrics,
                "nervix_client_authentications_total{outcome=\"rejected\",transport=\"grpc\"}"
            ),
            Some(0.0)
        );
    }
}
