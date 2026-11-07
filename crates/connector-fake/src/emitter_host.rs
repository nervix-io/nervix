//! Count-recording services for tests of native emitter owners.
//!
//! Layer: test harness.
//! - **Owns.** A retained test host and observations of its native payload preparations.
//! - **Depends on.** Connector host services, the bounded executor and selected primitives.
//! - **Must not know.** Metric registries, graph execution, real drivers or server runtime types.

use std::{path::PathBuf, time::Duration};

use meticulous::OptionExt as _;
use nervix_connector::{
    EmitterBoundedExecution, EmitterEventReporter, EmitterGeneralErrorHandler, EmitterHost,
    EmitterPayloadMeasurement, EmitterPayloadObserver, EmitterRecordAcknowledgements,
    EmitterStagingDirectory, EmitterTransientErrorStatus,
};
use nervix_execution::Executor;
use nervix_primitives::sync::{Arc, blocking::Mutex};

#[derive(Debug, Default, Clone)]
pub struct EmitterHostObservations {
    pub payloads: Vec<EmitterPayloadMeasurement>,
    pub subdivisions: u64,
}

#[derive(Clone, Default)]
pub struct FakeEmitterHost {
    executor: Option<Executor>,
    observations: Arc<Mutex<EmitterHostObservations>>,
}

impl FakeEmitterHost {
    pub fn on(executor: Executor) -> Self {
        Self {
            executor: Some(executor),
            observations: Arc::new(Mutex::new(EmitterHostObservations::default())),
        }
    }

    pub fn host(&self) -> EmitterHost {
        EmitterHost::new(self.clone())
    }

    pub fn observations(&self) -> EmitterHostObservations {
        self.observations.lock().clone()
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
