//! One leadership-scoped authenticated licensing stream, independent of polling.
//!
//! Layer: control plane.
//! - **Owns.** Ticket setup, bounded SSE parsing, idle waits, reconnects and coalesced hints.
//! - **Depends on.** The licensing client, maintained SSE parser and committed observations.
//! - **Must not know.** Private keys, permission derivation or graph execution.

use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use eventsource_stream::Eventsource as _;
use futures_util::StreamExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_consensus::{ClusterLicensingState, LicensingMode};
use nervix_execution::MemoryClass;
use nervix_licensing_protocol::{notifications::Notification, requests::Operation};
use nervix_primitives::{
    sync::watch,
    time::{sleep, timeout},
};
use thiserror::Error;

use super::{
    client::{LicensingClient, LicensingRequestError},
    clock,
    notifications::{CommittedRevisions, NotificationCursor},
};
use crate::{
    application::session_service::SessionServiceImpl,
    licensing::{PolicyHandle, RequestEvent},
};

#[derive(PartialEq, Eq)]
struct StreamBinding {
    activation_id: uuid::Uuid,
    key_generation: std::num::NonZeroU64,
}

#[derive(Default)]
pub(super) struct StreamTasks {
    pub(super) tasks: nervix_primitives::task::JoinSet<()>,
    binding: Option<StreamBinding>,
}
impl StreamTasks {
    pub(super) fn reset(&mut self) {
        self.binding = None;
    }

    pub(super) async fn stop(&mut self) {
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {
            nervix_primitives::task::consume_budget().await;
        }
        self.binding = None;
    }

    pub(super) async fn reconcile(
        &mut self,
        service: &SessionServiceImpl,
        leading: bool,
        hints: &watch::Sender<()>,
    ) {
        let desired = if leading {
            let state = service
                .inner
                .consensus
                .observer()
                .licensing_snapshot()
                .initialized;
            match state {
                Some(state) => match &state.record.mode {
                    LicensingMode::Activated(activation) => Some(StreamBinding {
                        activation_id: activation.binding.activation_id,
                        key_generation: state.identity.key_generation,
                    }),
                    LicensingMode::Free { .. } | LicensingMode::Released(_) => None,
                },
                None => None,
            }
        } else {
            None
        };
        if self.binding == desired {
            return;
        }
        self.stop().await;
        self.binding = desired;
        if self.binding.is_some() {
            self.tasks.spawn(run(service.clone(), hints.clone()));
        }
    }
}

#[derive(Debug, Error)]
enum StreamError {
    #[error("licensing notification stream ended")]
    Ended,
    #[error("licensing notification stream was rejected")]
    Read,
    #[error("licensing notification identifier differs from its payload")]
    Identity,
}

/// Its lifetime is the admitted response, including cancellation by the coordinator.
struct Connected(PolicyHandle);
impl Connected {
    fn new(handle: PolicyHandle) -> Self {
        handle.set_stream_connected(true);
        tracing::info!("licensing notification stream connected");
        Self(handle)
    }
}
impl Drop for Connected {
    fn drop(&mut self) {
        self.0.set_stream_connected(false);
        tracing::info!("licensing notification stream closed");
    }
}

/// This task is aborted and joined before another tenure/activation starts its stream.
pub(super) async fn run(service: SessionServiceImpl, hints: watch::Sender<()>) {
    let mut cursor = NotificationCursor::default();
    let mut backoff = service.cluster_parameters().licensing_request_timeout();
    loop {
        nervix_primitives::task::consume_budget().await;
        let result = connect(&service, &hints, &mut cursor, &mut backoff).await;
        if let Err(error) = result {
            tracing::warn!(error = %format_args!("{error:#}"), "licensing notification stream interrupted");
        }
        let parameters = service.cluster_parameters();
        let maximum = parameters.licensing_retry_backoff_max();
        backoff = backoff.min(maximum);
        let nanos = u64::try_from(backoff.as_nanos())
            .assured("validated u32-seconds duration fits u64 nanoseconds");
        sleep(Duration::from_nanos(nervix_crypto::random::u64() % nanos)).await;
        backoff = backoff
            .checked_mul(2)
            .assured("doubling the validated duration fits Duration")
            .min(maximum);
    }
}

async fn connect(
    service: &SessionServiceImpl,
    hints: &watch::Sender<()>,
    cursor: &mut NotificationCursor,
    backoff: &mut Duration,
) -> error_stack::Result<(), StreamError> {
    let Some(initialized) = service
        .inner
        .consensus
        .observer()
        .licensing_snapshot()
        .initialized
    else {
        return Err(Report::new(StreamError::Ended));
    };
    let client = LicensingClient::new(service)
        .await
        .change_context(StreamError::Read)?;
    let response = match client.subscribe(&initialized, cursor.last).await {
        Ok(response) => response,
        Err(error) => {
            let outcome = match error.current_context() {
                LicensingRequestError::Timeout => Some(RequestEvent::Timeout),
                LicensingRequestError::Transport => Some(RequestEvent::Unreachable),
                LicensingRequestError::Refused { kind } => {
                    Some(RequestEvent::RefusedByService(kind.code()))
                }
                LicensingRequestError::ContentType
                | LicensingRequestError::Response
                | LicensingRequestError::Bounds => Some(RequestEvent::RejectedByCluster),
                _ => None,
            };
            if let Some(outcome) = outcome {
                service.inner.licensing.record_request(
                    Operation::Subscribe,
                    outcome,
                    clock::observe(service.inner.runtime.fault_injection()),
                );
            }
            return Err(error.change_context(StreamError::Read));
        }
    };
    let executor = service.inner.runtime.executor();
    let _reservation = executor
        .try_reserve(
            MemoryClass::Management,
            executor.limits().management_event_bytes.as_u64(),
        )
        .change_context(StreamError::Read)?;
    let _connected = Connected::new(service.inner.licensing.clone());
    // Bound all bytes admitted during one parser lifetime. Renewal retains the cursor and sync
    // reconciles every gap; no unfinished line or data field can grow beyond this reservation.
    let bytes = futures_util::stream::try_unfold(
        (response, client.environment.limits().bytes.get()),
        move |(mut response, remaining)| async move {
            nervix_primitives::task::consume_budget().await;
            let idle = service.cluster_parameters().licensing_stream_idle_timeout();
            let chunk = timeout(idle, response.chunk())
                .await
                .change_context(LicensingRequestError::Timeout)?
                .map_err(|error| {
                    Report::new(error.without_url())
                        .change_context(LicensingRequestError::Transport)
                })?;
            let Some(chunk) = chunk else {
                return Ok::<_, Report<LicensingRequestError>>(None);
            };
            let remaining = remaining
                .checked_sub(chunk.len())
                .ok_or_else(|| Report::new(LicensingRequestError::Bounds))?;
            Ok(Some((chunk, (response, remaining))))
        },
    );
    let events = bytes.eventsource();
    let mut events = std::pin::pin!(events);
    while let Some(event) = events.next().await {
        nervix_primitives::task::consume_budget().await;
        let event = match event {
            Ok(event) => event,
            Err(eventsource_stream::EventStreamError::Transport(error)) => {
                return Err(error.change_context(StreamError::Read));
            }
            Err(error) => {
                return Err(Report::new(FramingFailure(error))
                    .change_context(LicensingRequestError::Response)
                    .change_context(StreamError::Read));
            }
        };
        let id = event
            .id
            .parse::<u64>()
            .change_context(StreamError::Identity)?;
        let hint =
            Notification::decode(&event.event, &event.data).change_context(StreamError::Read)?;
        if hint.revision() != id {
            return Err(Report::new(StreamError::Identity));
        }
        *backoff = service.cluster_parameters().licensing_request_timeout();
        let Some(committed) = service
            .inner
            .consensus
            .observer()
            .licensing_snapshot()
            .initialized
        else {
            return Err(Report::new(StreamError::Ended));
        };
        let Some(revisions) = revisions(&committed) else {
            return Err(Report::new(StreamError::Ended));
        };
        if cursor.observe(&hint, &revisions) {
            hints.send_replace(());
        }
    }
    Err(Report::new(StreamError::Ended))
}

/// Retain the concrete parser failure for inspection without rendering untrusted event contents.
struct FramingFailure(eventsource_stream::EventStreamError<Report<LicensingRequestError>>);
impl std::fmt::Debug for FramingFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}
impl std::fmt::Display for FramingFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            eventsource_stream::EventStreamError::Utf8(_) => {
                formatter.write_str("licensing event stream is not UTF-8")
            }
            eventsource_stream::EventStreamError::Parser(_) => {
                formatter.write_str("licensing event stream has invalid framing")
            }
            eventsource_stream::EventStreamError::Transport(_) => {
                formatter.write_str("licensing event stream transport failed")
            }
        }
    }
}
impl std::error::Error for FramingFailure {}

fn revisions(state: &ClusterLicensingState) -> Option<CommittedRevisions> {
    let LicensingMode::Activated(activation) = &state.record.mode else {
        return None;
    };
    Some(CommittedRevisions {
        decision_revision: activation.decision.revision,
        conditions_revision: activation.decision.conditions_revision,
        trust_serial: state
            .record
            .trust
            .iter()
            .map(|document| document.serial)
            .max()
            .unwrap_or(0),
    })
}
