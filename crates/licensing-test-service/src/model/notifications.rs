//! Authenticated, bounded notification sessions and controllable stream faults.
//!
//! Layer: test harness.
//! - **Owns.** Single-use tickets, per-activation revisions, SSE output and stream observations.
//! - **Depends on.** The shared protocol, serial fixture Model and maintained axum/headers APIs.
//! - **Must not know.** Nervix policy, consensus, leader identity or scenario implementation.

use std::{collections::BTreeSet, convert::Infallible, time::Duration};

use axum::{
    extract::{Path, State},
    http::HeaderMap,
    response::{IntoResponse as _, Sse, sse::Event},
};
use error_stack::{Report, ResultExt as _};
use headers::{Authorization, HeaderMapExt as _, authorization::Bearer};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_licensing_protocol::{
    document::verify,
    notifications::{DecisionNotification, Notification, NotificationState},
    problems::ProblemKind,
    requests::StreamTicket,
};
use nervix_primitives::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        watch,
    },
    time::sleep,
};
use uuid::Uuid;

use super::{MAX_ENTRIES, Model, problem};
use crate::{ServerState, SharedModel, limits};

// Healthy streams emit this fixture heartbeat; the idle-reconnect scenarios use a three-second
// deadline to distinguish keepalive traffic from the deliberately blocked mode.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the scenario peer emits a comment every second so healthy streams stay inside \
                  the three-second idle-reconnect verification envelope; absent from product \
                  graphs"
    )
)]
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StreamMode {
    #[default]
    Normal,
    Withhold,
    Blocked,
    Oversized,
    StaleSync,
}

#[derive(Clone, Debug)]
pub struct StreamObservation {
    pub request_id: Uuid,
    pub last_event: Option<u64>,
}

pub(super) struct Notifications {
    wake: watch::Sender<()>,
    epoch: u64,
    mode: StreamMode,
    duplicate: bool,
    sync_sent: u64,
    hints_sent: u64,
    consumed: BTreeSet<Uuid>,
    observations: Vec<StreamObservation>,
    // Each HTTP response's drop guard outlives the Model borrow that opened it.
    active: Arc<AtomicUsize>,
}
impl Default for Notifications {
    fn default() -> Self {
        Self {
            wake: watch::channel(()).0,
            epoch: 0,
            mode: StreamMode::Normal,
            duplicate: false,
            sync_sent: 0,
            hints_sent: 0,
            consumed: BTreeSet::new(),
            observations: Vec::new(),
            active: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Notifications {
    fn wake(&self) {
        self.wake.send_replace(());
    }
}

#[derive(Debug, thiserror::Error)]
#[error("licensing notification admission refused")]
struct AdmissionError(ProblemKind);

fn refusal(kind: ProblemKind) -> Report<AdmissionError> {
    Report::new(AdmissionError(kind))
}

impl Model {
    pub fn notification_mode(&mut self, mode: StreamMode) {
        self.notifications.mode = mode;
        self.notifications.wake();
    }
    pub fn duplicate_notifications(&mut self, duplicate: bool) {
        self.notifications.duplicate = duplicate;
    }
    pub fn disconnect_notifications(&mut self) {
        self.notifications.epoch = self
            .notifications
            .epoch
            .checked_add(1)
            .assured("bounded fixture disconnects fit u64");
        self.notifications.wake();
    }
    pub fn active_notification_streams(&self) -> usize {
        self.notifications.active.load(Ordering::Relaxed)
    }
    pub fn notification_observations(&self) -> Vec<StreamObservation> {
        self.notifications.observations.clone()
    }
    pub fn notification_stream_ready(&self) -> bool {
        self.active_notification_streams() == 1
            && (self.notifications.mode == StreamMode::Blocked || self.notifications.sync_sent > 0)
    }
    pub fn duplicate_hints_were_sent(&self) -> bool {
        !self.notifications.duplicate || self.notifications.hints_sent >= 2
    }
    pub(super) fn notify_change(&mut self) {
        for activation in self.activations.values_mut() {
            activation.notification_revision = activation
                .notification_revision
                .checked_add(1)
                .assured("bounded fixture notifications fit u64");
        }
        self.notifications.wake();
    }
    fn notification_state(&self, id: Uuid) -> Option<NotificationState> {
        let activation = self.activations.get(&id)?;
        Some(NotificationState {
            notification_revision: activation.notification_revision,
            decision_revision: activation.decision.decision_revision,
            conditions_revision: self.conditions_revision,
            trust_serial: 1,
        })
    }
    fn open_stream(
        &mut self,
        id: Uuid,
        headers: &HeaderMap,
    ) -> Result<Admission, Report<AdmissionError>> {
        self.request_count = self
            .request_count
            .checked_add(1)
            .assured("bounded fixture request count");
        let authorization = headers
            .typed_get::<Authorization<Bearer>>()
            .ok_or_else(|| refusal(ProblemKind::SignatureInvalid))?;
        let activation = self
            .activations
            .get(&id)
            .ok_or_else(|| refusal(ProblemKind::ActivationUnknown))?;
        let ticket: StreamTicket = verify(authorization.token(), &activation.public_key, limits())
            .change_context(AdmissionError(ProblemKind::SignatureInvalid))?;
        let common = &ticket.common;
        if ticket.activation_id != id || common.cluster_uid != activation.binding.cluster_uid {
            return Err(refusal(ProblemKind::BindingMismatch));
        }
        if common.key_generation != activation.binding.generation {
            return Err(refusal(ProblemKind::GenerationMismatch));
        }
        if common.protocol != 1 || common.aud != self.address {
            return Err(refusal(ProblemKind::InvalidRequest));
        }
        if self.notifications.consumed.contains(&common.request_id) {
            return Err(refusal(ProblemKind::RequestConflict));
        }
        let challenge = self.challenges.remove(common.challenge.expose());
        if !challenge.is_some_and(|challenge| {
            challenge.cluster == common.cluster_uid && challenge.until > self.now
        }) {
            return Err(refusal(ProblemKind::ChallengeInvalid));
        }
        if self.notifications.consumed.len() >= MAX_ENTRIES {
            return Err(refusal(ProblemKind::RateLimited {
                retry_after_seconds: 1,
            }));
        }
        let last_event = match headers.get("last-event-id") {
            Some(value) => Some(
                value
                    .to_str()
                    .change_context(AdmissionError(ProblemKind::InvalidRequest))?
                    .parse::<u64>()
                    .change_context(AdmissionError(ProblemKind::InvalidRequest))?,
            ),
            None => None,
        };
        self.notifications.consumed.insert(common.request_id);
        self.notifications.observations.push(StreamObservation {
            request_id: common.request_id,
            last_event,
        });
        self.notifications.active.fetch_add(1, Ordering::Relaxed);
        Ok(Admission {
            epoch: self.notifications.epoch,
            updates: self.notifications.wake.subscribe(),
            active: Active(self.notifications.active.clone()),
        })
    }
}

struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}
struct Admission {
    epoch: u64,
    updates: watch::Receiver<()>,
    active: Active,
}
struct Session {
    model: SharedModel,
    id: Uuid,
    admission: Admission,
    stop: nervix_primitives::sync::CancellationToken,
    last: Option<u64>,
    repeat: Option<Notification>,
}
impl Session {
    async fn next(&mut self) -> Option<Event> {
        // The guard belongs to the response body, including a client-side cancellation.
        let _active = &self.admission.active;
        loop {
            nervix_primitives::task::consume_budget().await;
            let mode;
            {
                let mut model = self.model.lock().await;
                if self.admission.epoch != model.notifications.epoch {
                    return None;
                }
                mode = model.notifications.mode;
                if mode == StreamMode::Oversized {
                    let size = crate::MAX_BODY_BYTES
                        .checked_mul(2)
                        .assured("twice the fixed fixture envelope fits usize");
                    return Some(
                        Event::default()
                            .event("decision")
                            .id("0")
                            .data("x".repeat(size)),
                    );
                }
                if mode != StreamMode::Blocked {
                    if let Some(repeat) = self.repeat.take() {
                        model.notifications.hints_sent = model
                            .notifications
                            .hints_sent
                            .checked_add(1)
                            .assured("bounded fixture hint count");
                        return Some(event(repeat));
                    }
                    let mut state = model.notification_state(self.id)?;
                    if self.last.is_none() {
                        self.last = Some(state.notification_revision);
                        if mode == StreamMode::StaleSync {
                            state.decision_revision = 0;
                            state.conditions_revision = 0;
                            state.trust_serial = 0;
                        }
                        model.notifications.sync_sent = model
                            .notifications
                            .sync_sent
                            .checked_add(1)
                            .assured("bounded fixture sync count");
                        return Some(event(Notification::Sync(state)));
                    }
                    if mode != StreamMode::Withhold
                        && self.last != Some(state.notification_revision)
                    {
                        self.last = Some(state.notification_revision);
                        let hint = Notification::Decision(DecisionNotification {
                            notification_revision: state.notification_revision,
                            decision_revision: state.decision_revision,
                            reason: "override".to_owned(),
                        });
                        if model.notifications.duplicate {
                            self.repeat = Some(hint.clone());
                        }
                        model.notifications.hints_sent = model
                            .notifications
                            .hints_sent
                            .checked_add(1)
                            .assured("bounded fixture hint count");
                        return Some(event(hint));
                    }
                }
            }
            nervix_primitives::select! {
                _ = self.stop.cancelled() => return None,
                changed = self.admission.updates.changed() => if changed.is_err() { return None; },
                _ = sleep(KEEP_ALIVE_INTERVAL), if mode != StreamMode::Blocked => {
                    return Some(Event::default().comment("keep-alive"));
                },
            }
        }
    }
}
fn event(notification: Notification) -> Event {
    Event::default()
        .event(notification.kind())
        .id(notification.revision().to_string())
        .data(
            notification
                .encode()
                .assured("typed bounded fixture notification serializes"),
        )
}

pub(crate) async fn events(
    State(state): State<ServerState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> axum::response::Response {
    if !*state.online.borrow() {
        return crate::lost_response();
    }
    let admission = match state.model.lock().await.open_stream(id, &headers) {
        Ok(admission) => admission,
        Err(error) => return problem(error.current_context().0.clone(), None),
    };
    let session = Session {
        model: state.model,
        id,
        admission,
        stop: state.stop,
        last: None,
        repeat: None,
    };
    let output = futures_util::stream::unfold(session, |mut session| async move {
        session
            .next()
            .await
            .map(|event| (Ok::<_, Infallible>(event), session))
    });
    Sse::new(output).into_response()
}

#[cfg(test)]
mod tests {
    use nervix_licensing_protocol::{
        conditions::ActivationKind, document::DocumentSigner, requests::*,
    };

    use super::*;

    async fn common(model: &mut Model, cluster_uid: Uuid) -> RequestCommon {
        let response = model.challenge(
            &serde_json::to_vec(&ChallengeRequest { cluster_uid })
                .assured("fixture challenge JSON"),
        );
        let body = axum::body::to_bytes(response.into_body(), crate::MAX_BODY_BYTES)
            .await
            .assured("bounded challenge body");
        let challenge: ChallengeResponse =
            serde_json::from_slice(&body).assured("current challenge response");
        RequestCommon {
            protocol: 1,
            request_id: super::super::id(),
            challenge: challenge.challenge,
            sent_at: model.now,
            aud: model.address.clone(),
            cluster_uid,
            display_name: "test".to_owned(),
            client: ClientDescription {
                product: ClientProduct::NervixServer,
                version: "test".to_owned(),
            },
            key_generation: 1,
            trust_held: TrustHeld {
                working_keys: Vec::new(),
                revocation_serials: Vec::new(),
                succession_serials: Vec::new(),
            },
        }
    }

    async fn activated() -> (Model, DocumentSigner, Uuid) {
        let now = chrono::DateTime::parse_from_rfc3339("2026-11-10T12:00:00Z")
            .assured("fixed fixture time")
            .with_timezone(&chrono::Utc);
        let mut model = Model::new(
            url::Url::parse("https://127.0.0.1:12345").assured("fixed URL"),
            now,
        )
        .assured("fixture model");
        let signer = super::super::signer().assured("fixture cluster signer");
        let request = Request::Create {
            common: common(&mut model, super::super::id()).await,
            license_key: model.license_key.clone(),
            kind: ActivationKind::Persistent,
            cluster_key: signer.public_key().clone(),
        };
        let compact = signer.sign(&request, limits()).assured("signed creation");
        let (response, _) = model.answer("/v1/activations", compact.as_bytes());
        assert_eq!(response.status(), 201);
        let id = *model
            .activations
            .keys()
            .next()
            .assured("one activation installed");
        (model, signer, id)
    }

    #[nervix_primitives::test]
    async fn stream_tickets_are_bound_single_use_and_preserve_reconnect_identity() {
        let (mut model, signer, id) = activated().await;
        let cluster = model.activations[&id].binding.cluster_uid;
        let common = common(&mut model, cluster).await;
        let ticket = StreamTicket {
            common,
            activation_id: id,
            state: CommittedState {
                applied_decision_revision: 1,
                applied_conditions_revision: 1,
                licensing_time: model.now,
            },
        };
        let compact = signer
            .sign(&ticket, limits())
            .assured("signed stream ticket");
        let mut headers = HeaderMap::new();
        headers
            .typed_insert(Authorization::bearer(&compact).assured("compact JWS is a header token"));
        headers.insert("last-event-id", "7".parse().assured("numeric header"));
        let admission = model
            .open_stream(id, &headers)
            .assured("bound fresh ticket opens");
        assert_eq!(model.active_notification_streams(), 1);
        let observations = model.notification_observations();
        assert_eq!(observations[0].request_id, ticket.common.request_id);
        assert_eq!(observations[0].last_event, Some(7));
        assert!(
            model
                .open_stream(id, &headers)
                .is_err_and(|error| error.current_context().0 == ProblemKind::RequestConflict)
        );
        drop(admission);
        assert_eq!(model.active_notification_streams(), 0);
        assert!(
            model
                .open_stream(super::super::id(), &headers)
                .is_err_and(|error| error.current_context().0 == ProblemKind::ActivationUnknown)
        );
        assert!(
            model
                .open_stream(id, &HeaderMap::new())
                .is_err_and(|error| error.current_context().0 == ProblemKind::SignatureInvalid)
        );
    }

    #[nervix_primitives::test]
    async fn stream_refusals_retain_signature_and_cursor_causes() {
        let (mut model, signer, id) = activated().await;
        let mut headers = HeaderMap::new();
        headers.typed_insert(Authorization::bearer("invalid").assured("header token"));
        assert!(model.open_stream(id, &headers).is_err_and(|error| {
            error.current_context().0 == ProblemKind::SignatureInvalid
                && error.contains::<nervix_licensing_protocol::document::DocumentError>()
        }));
        let cluster = model.activations[&id].binding.cluster_uid;
        let ticket = StreamTicket {
            common: common(&mut model, cluster).await,
            activation_id: id,
            state: CommittedState {
                applied_decision_revision: 1,
                applied_conditions_revision: 1,
                licensing_time: model.now,
            },
        };
        let compact = signer.sign(&ticket, limits()).assured("signed ticket");
        headers.typed_insert(Authorization::bearer(&compact).assured("signed token"));
        headers.insert(
            "last-event-id",
            "not-a-number".parse().assured("header value"),
        );
        assert!(model.open_stream(id, &headers).is_err_and(|error| {
            error.current_context().0 == ProblemKind::InvalidRequest
                && error.contains::<std::num::ParseIntError>()
        }));
    }
}
