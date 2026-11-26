//! Scripted host services for tests of native emitter owners.
//!
//! Layer: test harness.
//! - **Owns.** A retained test host, the services a test puts behind its handle, and observations
//!   of what an emitter did through it.
//! - **Depends on.** Connector host services, the bounded executor, the node resolver and selected
//!   primitives.
//! - **Must not know.** Metric registries, graph execution, real drivers or server runtime types.

use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use meticulous::OptionExt as _;
use nervix_connector::{
    ConnectorEventCounter, ConnectorEventProvider, EmitterBoundedExecution, EmitterEventReporter,
    EmitterGeneralErrorHandler, EmitterHost, EmitterNameResolution, EmitterOutwardTls,
    EmitterPayloadMeasurement, EmitterPayloadObserver, EmitterRecordAcknowledgements,
    EmitterStagingDirectory, EmitterTaskEnd, EmitterTransientErrorStatus, OutwardTlsPolicy,
    PooledClient, PooledClientLease, UndeclaredConnectorEvent,
};
use nervix_dns::DnsResolver;
use nervix_execution::Executor;
use nervix_models::{ConnectorEvent, DeclaredEvents, MessageErrorClass};
use nervix_primitives::sync::{
    Arc, CancellationToken,
    atomic::{AtomicU64, Ordering},
    blocking::Mutex,
};

#[derive(Debug, Default, Clone)]
pub struct EmitterHostObservations {
    pub readiness: Vec<bool>,
    pub payloads: Vec<EmitterPayloadMeasurement>,
    pub subdivisions: u64,
    pub events: BTreeMap<ConnectorEvent, u64>,
    /// The transient status the emitter shows now, as `DESCRIBE EMITTER` would.
    pub transient_error: Option<String>,
    /// Every error event the emitter reported, in order.
    pub reported_errors: Vec<String>,
    /// Every failure the emitter handed to the node's general error policy, in order.
    pub general_errors: Vec<(MessageErrorClass, String)>,
}

/// A test host for one emitter instance.
///
/// A test puts behind the handle only the services its emitter reaches; a service it left out
/// answers as a node that does not provide it would, with the handle's typed refusal.
#[derive(Clone)]
pub struct FakeEmitterHost {
    executor: Option<Executor>,
    dns: Option<DnsResolver>,
    tls_policy: OutwardTlsPolicy,
    task_end: CancellationToken,
    observations: Arc<Mutex<EmitterHostObservations>>,
    declared_events: DeclaredEvents,
    events: BTreeMap<ConnectorEvent, Arc<AtomicU64>>,
}

impl Default for FakeEmitterHost {
    fn default() -> Self {
        Self {
            executor: None,
            dns: None,
            tls_policy: OutwardTlsPolicy::default(),
            task_end: CancellationToken::new(),
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

    /// This host, with `executor` as its node's bounded executor.
    pub fn with_executor(mut self, executor: Executor) -> Self {
        self.executor = Some(executor);
        self
    }

    /// This host, with `dns` as its node's resolver.
    pub fn with_dns(mut self, dns: DnsResolver) -> Self {
        self.dns = Some(dns);
        self
    }

    /// This host, with `tls_policy` as its node's committed outward TLS policy.
    pub fn with_tls_policy(mut self, tls_policy: OutwardTlsPolicy) -> Self {
        self.tls_policy = tls_policy;
        self
    }

    pub fn host(&self) -> EmitterHost {
        EmitterHost::new(self.clone())
    }

    /// The handle of an emitter whose host leased `lease` for it before opening it.
    pub fn leasing<K: PooledClient>(&self, lease: PooledClientLease<K>) -> EmitterHost {
        EmitterHost::leasing(self.clone(), lease)
    }

    /// Ends the emitter's task as the host ending it does, which cancels the token the handle
    /// hands its emitter.
    pub fn end_task(&self) {
        self.task_end.cancel();
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
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            key = "one native emitter observation log",
            bound = "one synchronous status replacement; no guard crosses await",
            reason = "the harness records the status the emitter shows before its test inspects it"
        )
    )]
    fn record_transient_error(&self, reason: String, _retry_after: Duration) {
        self.observations.lock().transient_error = Some(reason);
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            key = "one native emitter observation log",
            bound = "one synchronous status removal; no guard crosses await",
            reason = "the harness records that the emitter cleared its status"
        )
    )]
    fn clear_transient_error(&self) {
        self.observations.lock().transient_error = None;
    }
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
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            key = "one native emitter observation log",
            bound = "one synchronous append; no guard crosses await",
            reason = "the harness records the error event before its test inspects it"
        )
    )]
    fn report_error(&self, message: String) {
        self.observations.lock().reported_errors.push(message);
    }
}

impl EmitterStagingDirectory for FakeEmitterHost {
    fn staging_directory(&self) -> PathBuf {
        std::env::temp_dir()
    }
}

impl EmitterGeneralErrorHandler for FakeEmitterHost {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            key = "one native emitter observation log",
            bound = "one synchronous append; no guard crosses await",
            reason = "the harness records the failure handed to the general error policy"
        )
    )]
    fn handle_general_error(
        &self,
        _acks: &EmitterRecordAcknowledgements,
        class: MessageErrorClass,
        reason: String,
    ) {
        self.observations
            .lock()
            .general_errors
            .push((class, reason));
    }
}

impl EmitterNameResolution for FakeEmitterHost {
    fn dns(&self) -> Option<DnsResolver> {
        self.dns.clone()
    }
}

impl EmitterOutwardTls for FakeEmitterHost {
    fn tls_policy(&self) -> OutwardTlsPolicy {
        self.tls_policy.clone()
    }
}

impl EmitterTaskEnd for FakeEmitterHost {
    fn cancellation(&self) -> CancellationToken {
        self.task_end.clone()
    }
}

impl EmitterBoundedExecution for FakeEmitterHost {
    fn executor(&self) -> Executor {
        self.executor
            .as_ref()
            .assured("an execution-using fixture supplies its executor")
            .clone()
    }
}
