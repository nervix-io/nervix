//! Scripted host services for tests of ingestor connectors and of the hosts that drive them.
//!
//! Layer: test harness.
//! - **Owns.** A test host whose handle an ingestor connector is opened, resumed and read with,
//!   the services a test puts behind that handle, and what a connector did through it.
//! - **Depends on.** Connector host services, the bounded executor, the node resolver and selected
//!   primitives.
//! - **Must not know.** Metric registries, graph execution, real drivers or server runtime types.

use std::{any::Any, collections::BTreeMap, time::Duration};

use meticulous::OptionExt as _;
use nervix_connector::{
    ConnectorEventCounter, ConnectorEventProvider, DomainOffsets, IngestorHost,
    IngestorHostServices, IngestorReadAheadOverflow, OutwardTlsPolicy, UndeclaredConnectorEvent,
};
use nervix_dns::DnsResolver;
use nervix_execution::Executor;
use nervix_models::{ConnectorEvent, DeclaredEvents};
use nervix_primitives::sync::{
    Arc, CancellationToken, StdArc,
    atomic::{AtomicU64, Ordering},
    blocking::Mutex,
};

/// What an ingestor connector did through the handle of one [`FakeIngestorHost`].
#[derive(Debug, Default, Clone)]
pub struct IngestorHostObservations {
    /// The transient status the instance shows now, as `DESCRIBE INGESTOR` would.
    pub transient_error: Option<String>,
    /// Every transient status the connector published, with the retry it announced, in order.
    pub transient_errors: Vec<(String, Duration)>,
    /// How often the connector withdrew its transient status.
    pub transient_clears: usize,
    /// Every error event the connector reported, in order.
    pub reported_errors: Vec<String>,
    /// How often the connector recorded each event its entity declares.
    pub events: BTreeMap<ConnectorEvent, u64>,
    /// How many payloads the connector's connections dropped for lack of read-ahead room.
    pub dropped_payloads: u64,
}

/// A test host for one ingestor connector instance.
///
/// A test puts behind the handle only the services its connector reaches; a service it left out
/// answers as a node that does not provide it would, with the handle's typed refusal.
#[derive(Clone)]
pub struct FakeIngestorHost {
    executor: Option<Executor>,
    dns: Option<DnsResolver>,
    tls_policy: OutwardTlsPolicy,
    domain_offsets: Option<DomainOffsets>,
    /// A trait object takes the standard reference count, because the boundary's `Arc` cannot be
    /// coerced to one.
    signaling_protocol: Option<StdArc<dyn Any + Send + Sync>>,
    declared_events: DeclaredEvents,
    events: BTreeMap<ConnectorEvent, Arc<AtomicU64>>,
    dropped_payloads: Arc<AtomicU64>,
    instance_end: CancellationToken,
    observations: Arc<Mutex<IngestorHostObservations>>,
}

impl Default for FakeIngestorHost {
    fn default() -> Self {
        Self {
            executor: None,
            dns: None,
            tls_policy: OutwardTlsPolicy::default(),
            domain_offsets: None,
            signaling_protocol: None,
            declared_events: DeclaredEvents::NONE,
            events: BTreeMap::new(),
            dropped_payloads: Arc::new(AtomicU64::new(0)),
            instance_end: CancellationToken::new(),
            observations: Arc::new(Mutex::new(IngestorHostObservations::default())),
        }
    }
}

impl FakeIngestorHost {
    /// A test host whose connector admits its synchronous work through `executor`.
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

    /// This host, keeping `offsets` as the domain-owned offsets of its ingestor.
    pub fn with_domain_offsets(mut self, offsets: DomainOffsets) -> Self {
        self.domain_offsets = Some(offsets);
        self
    }

    /// This host, with `protocol` as the signaling protocol it compiled for its ingestor: the
    /// shared value its connector reads.
    pub fn with_signaling_protocol<P: Any + Send + Sync>(mut self, protocol: P) -> Self {
        self.signaling_protocol = Some(StdArc::new(protocol));
        self
    }

    /// The handle a connector is opened, resumed and read with.
    pub fn host(&self) -> IngestorHost {
        IngestorHost::new(self.clone())
    }

    /// Ends the instance as the host task that drives it ending does, which cancels the token the
    /// handle hands its connector.
    pub fn end_instance(&self) {
        self.instance_end.cancel();
    }

    /// The token the handle hands its connector, which the owner of the instance cancels when it
    /// lets the instance go.
    pub fn instance_end(&self) -> CancellationToken {
        self.instance_end.clone()
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "test assertions snapshot what the connector did outside its execution"
        )
    )]
    pub fn observations(&self) -> IngestorHostObservations {
        let mut observations = self.observations.lock().clone();
        observations.events = self
            .events
            .iter()
            .map(|(event, counter)| (*event, counter.load(Ordering::Relaxed)))
            .collect();
        observations.dropped_payloads = self.dropped_payloads.load(Ordering::Relaxed);
        observations
    }
}

impl ConnectorEventProvider for FakeIngestorHost {
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

impl IngestorHostServices for FakeIngestorHost {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            key = "one fake ingestor host observation log",
            bound = "one synchronous status replacement and append; no guard crosses await",
            reason = "the harness records the status the instance shows before its test inspects \
                      it"
        )
    )]
    fn record_transient_error(&self, reason: String, retry_after: Duration) {
        let mut observations = self.observations.lock();
        observations.transient_error = Some(reason.clone());
        observations.transient_errors.push((reason, retry_after));
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            key = "one fake ingestor host observation log",
            bound = "one synchronous status removal and count; no guard crosses await",
            reason = "the harness records that the connector withdrew its status"
        )
    )]
    fn clear_transient_error(&self) {
        let mut observations = self.observations.lock();
        observations.transient_error = None;
        observations.transient_clears = observations
            .transient_clears
            .checked_add(1)
            .assured("a test withdraws a bounded number of statuses");
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            key = "one fake ingestor host observation log",
            bound = "one synchronous append; no guard crosses await",
            reason = "the harness records the error event before its test inspects it"
        )
    )]
    fn report_error(&self, message: String) {
        self.observations.lock().reported_errors.push(message);
    }

    fn executor(&self) -> Executor {
        self.executor
            .as_ref()
            .assured("an execution-using fixture supplies its executor")
            .clone()
    }

    fn dns(&self) -> Option<DnsResolver> {
        self.dns.clone()
    }

    fn tls_policy(&self) -> OutwardTlsPolicy {
        self.tls_policy.clone()
    }

    fn cancellation(&self) -> CancellationToken {
        self.instance_end.clone()
    }

    fn read_ahead_overflow(&self) -> IngestorReadAheadOverflow {
        let dropped = self.dropped_payloads.clone();
        IngestorReadAheadOverflow::new(move || {
            dropped.fetch_add(1, Ordering::Relaxed);
        })
    }

    fn domain_offsets(&self) -> Option<DomainOffsets> {
        self.domain_offsets.clone()
    }

    fn signaling_protocol(&self) -> Option<&(dyn Any + Send + Sync)> {
        match &self.signaling_protocol {
            Some(protocol) => Some(protocol.as_ref()),
            None => None,
        }
    }
}
