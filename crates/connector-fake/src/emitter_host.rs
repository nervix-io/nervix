//! Count-recording services for tests of native emitter owners.
//!
//! Layer: test harness.
//! - **Owns.** A retained test host and observations of its native payload preparations.
//! - **Depends on.** Connector host services, the bounded executor and selected primitives.
//! - **Must not know.** Metric registries, graph execution, real drivers or server runtime types.

use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use meticulous::OptionExt as _;
use nervix_connector::{
    ConnectorEventCounter, ConnectorEventProvider, EmitterBoundedExecution, EmitterEventReporter,
    EmitterGeneralErrorHandler, EmitterHost, EmitterPayloadMeasurement, EmitterPayloadObserver,
    EmitterRecordAcknowledgements, EmitterStagingDirectory, EmitterTransientErrorStatus,
    UndeclaredConnectorEvent,
};
use nervix_execution::Executor;
use nervix_models::{ConnectorEvent, DeclaredEvents};
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
    blocking::Mutex,
};

#[derive(Debug, Default, Clone)]
pub struct EmitterHostObservations {
    pub readiness: Vec<bool>,
    pub payloads: Vec<EmitterPayloadMeasurement>,
    pub subdivisions: u64,
    pub events: BTreeMap<ConnectorEvent, u64>,
}

#[derive(Clone)]
pub struct FakeEmitterHost {
    executor: Option<Executor>,
    observations: Arc<Mutex<EmitterHostObservations>>,
    declared_events: DeclaredEvents,
    events: BTreeMap<ConnectorEvent, Arc<AtomicU64>>,
}

impl Default for FakeEmitterHost {
    fn default() -> Self {
        Self {
            executor: None,
            observations: Arc::new(Mutex::new(EmitterHostObservations::default())),
            declared_events: DeclaredEvents::NONE,
            events: BTreeMap::new(),
        }
    }
}

impl FakeEmitterHost {
    pub fn on(executor: Executor) -> Self {
        Self {
            executor: Some(executor),
            ..Self::default()
        }
    }

    /// A test host whose connector may record exactly these declared identities.
    pub fn declaring(declared_events: DeclaredEvents) -> Self {
        let events = declared_events
            .as_slice()
            .iter()
            .map(|event| (*event, Arc::new(AtomicU64::new(0))))
            .collect();
        Self {
            declared_events,
            events,
            ..Self::default()
        }
    }

    pub fn host(&self) -> EmitterHost {
        EmitterHost::new(self.clone())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "test assertions snapshot the numeric preparation log outside emitter \
                      execution"
        )
    )]
    pub fn observations(&self) -> EmitterHostObservations {
        let mut observations = self.observations.lock().clone();
        observations.events = self
            .events
            .iter()
            .map(|(event, counter)| (*event, counter.load(Ordering::Relaxed)))
            .collect();
        observations
    }
}

impl ConnectorEventProvider for FakeEmitterHost {
    fn event_counter(
        &self,
        event: ConnectorEvent,
    ) -> error_stack::Result<ConnectorEventCounter, UndeclaredConnectorEvent> {
        let Some(counter) = self.events.get(&event).cloned() else {
            return Err(UndeclaredConnectorEvent { event }.into());
        };
        ConnectorEventCounter::new(self.declared_events, event, move || {
            counter.fetch_add(1, Ordering::Relaxed);
        })
    }
}

impl EmitterPayloadObserver for FakeEmitterHost {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            key = "one native emitter observation log",
            bound = "one synchronous caller-bounded measurement append; no guard crosses await",
            reason = "the harness records a count-only preparation before its test inspects it"
        )
    )]
    fn record_prepared_payload(&self, measurement: EmitterPayloadMeasurement) {
        self.observations.lock().payloads.push(measurement);
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            key = "one native emitter observation log",
            bound = "one synchronous scalar addition; no guard crosses await",
            reason = "the harness records the divider's count without changing its outcome"
        )
    )]
    fn record_payload_subdivisions(&self, subdivisions: u64) {
        let mut observations = self.observations.lock();
        observations.subdivisions = observations
            .subdivisions
            .checked_add(subdivisions)
            .assured("the fixture records a bounded number of candidate subdivisions");
    }
}

impl EmitterTransientErrorStatus for FakeEmitterHost {
    fn record_transient_error(&self, _reason: String, _retry_after: Duration) {}
    fn clear_transient_error(&self) {}
}

impl nervix_connector::EmitterReadiness for FakeEmitterHost {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the harness records this host's readiness transition",
            key = "one fake host readiness log",
            bound = "one boolean append under one mutex guard; the callback never awaits"
        )
    )]
    fn mark_ready(&self) {
        self.observations.lock().readiness.push(true);
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the harness records this host's readiness transition",
            key = "one fake host readiness log",
            bound = "one boolean append under one mutex guard; the callback never awaits"
        )
    )]
    fn mark_unready(&self) {
        self.observations.lock().readiness.push(false);
    }
}

impl EmitterEventReporter for FakeEmitterHost {
    fn report_error(&self, _message: String) {}
}

impl EmitterStagingDirectory for FakeEmitterHost {
    fn staging_directory(&self) -> PathBuf {
        std::env::temp_dir()
    }
}

impl EmitterGeneralErrorHandler for FakeEmitterHost {
    fn handle_general_error(&self, _acks: &EmitterRecordAcknowledgements, _reason: String) {}
}

impl EmitterBoundedExecution for FakeEmitterHost {
    fn executor(&self) -> Executor {
        self.executor
            .as_ref()
            .assured("an execution-using fixture supplies its executor")
            .clone()
    }
}
