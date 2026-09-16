use dashmap::mapref::entry::Entry as DashMapEntry;
use nervix_models::{DomainName, NodeRef};

use super::{message_error::MessageErrorHandlingError, *};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct MessageErrorRouteKey {
    pub(super) domain: DomainName,
    pub(super) node: NodeRef,
    pub(super) source_route: Option<RelayName>,
    pub(super) error_relay: RelayName,
}

#[derive(Clone)]
pub(super) struct MessageErrorRouteTarget {
    pub(super) registry: RelayRegistry,
    pub(super) services: Arc<RelayBoundaryServices>,
}

pub(super) struct MessageErrorDelivery {
    pub(super) batch: RelayRecordBatch,
    pub(super) source_acks: Vec<AckSet>,
}

struct PendingMessageErrorDelivery {
    deliveries: Vec<MessageErrorDelivery>,
    estimated_bytes: u64,
    flush_timer: BranchBufferTimer,
}

impl PendingMessageErrorDelivery {
    fn ack_source_alive(&self) {
        for delivery in &self.deliveries {
            delivery.ack_source_alive();
        }
    }
}

impl MessageErrorDelivery {
    fn ack_source_alive(&self) {
        for ack in &self.source_acks {
            ack.ack_alive();
        }
    }

    fn merged_source_acks(&self) -> AckSet {
        AckSet::merged(self.source_acks.iter().cloned())
    }
}

pub(super) struct MessageErrorRouteRuntime {
    sender: mpsc::Sender<MessageErrorDelivery>,
    shutdown: watch::Sender<bool>,
    task: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

struct MessageErrorRouteTask {
    runtime: Runtime,
    route: MessageErrorRouteKey,
    target: MessageErrorRouteTarget,
    flush_policy: RuntimeFlushPolicy,
    pending: HashMap<Option<BranchKey>, PendingMessageErrorDelivery>,
}

/// Resolves the output route a message-error policy belongs to.
///
/// Both passes walk the node's own declared routes, whose order is part of the Model, and each
/// call resolves a single route while an error route is compiled. Building an index would cost the
/// same walk it replaces.
pub(super) fn matching_message_error_output<'a>(
    outputs: &'a nervix_models::ProcessorOutputs,
    source_route: Option<&RelayName>,
    error_relay: &RelayName,
    assignments: &[Assignment],
) -> Option<&'a ProcessorOutput> {
    if let Some(route) = source_route
        && let Some(output) = outputs.routes.iter().find(|output| &output.relay == route)
    {
        return Some(output);
    }
    outputs.routes.iter().find(|output| {
        if let MessageErrorPolicy::Dlq {
            relay,
            assignments: configured,
        } = &output.message_error_policy
        {
            relay == error_relay && configured == assignments
        } else {
            false
        }
    })
}

impl MessageErrorRouteRuntime {
    fn new(
        runtime: Runtime,
        route: MessageErrorRouteKey,
        target: MessageErrorRouteTarget,
        flush_policy: RuntimeFlushPolicy,
    ) -> Arc<Self> {
        let (sender, input) = mpsc::channel(1);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let route_runtime = Arc::new(Self {
            sender,
            shutdown,
            task: parking_lot::Mutex::new(None),
        });
        // A route owes the force-flush generations of the node whose messages failed. The
        // obligation is registered before the task starts, so a generation requested between the
        // spawn and the first poll is still owed by this route rather than missed.
        let force_flush = runtime.force_flush_participant(
            &route.domain,
            runtime.node_quiesce_counters(&route.domain, route.node.clone()),
        );
        let task = tokio::spawn(
            MessageErrorRouteTask {
                runtime,
                route,
                target,
                flush_policy,
                pending: HashMap::default(),
            }
            .run(input, shutdown_rx, force_flush),
        );
        *route_runtime.task.lock() = Some(task);
        route_runtime
    }

    async fn shutdown(&self) {
        self.shutdown.send_replace(true);
        let task = self.task.lock().take();
        if let Some(task) = task {
            task.join_after_shutdown("message error delivery").await;
        }
    }
}

impl MessageErrorRouteTask {
    fn report_failure(&self, acks: &[AckSet], reason: String) {
        self.runtime.events().report_error(reason.clone());
        warn!(
            domain = self.route.domain.as_str(),
            node_kind = self.route.node.kind.as_str(),
            node = self.route.node.identifier.as_str(),
            error_relay = self.route.error_relay.as_str(),
            reason = %reason,
            "runtime node failed to flush message errors"
        );
        for ack in acks {
            ack.no_ack(reason.clone());
        }
    }

    fn ack_pending_alive(&self) {
        for pending in self.pending.values() {
            pending.ack_source_alive();
        }
    }

    fn pending_acks(&self) -> AckSet {
        AckSet::merged(
            self.pending
                .values()
                .flat_map(|pending| &pending.deliveries)
                .flat_map(|delivery| delivery.source_acks.iter().cloned()),
        )
    }

    async fn flush_key(&mut self, key: &Option<BranchKey>) {
        let pending_acks = self.pending_acks();
        let Some(pending) = self.pending.remove(key) else {
            return;
        };
        let mut batches = Vec::with_capacity(pending.deliveries.len());
        let mut source_acks = Vec::new();
        for delivery in pending.deliveries {
            tokio::task::consume_budget().await;
            pending_acks.ack_alive();
            batches.push(delivery.batch);
            source_acks.extend(delivery.source_acks);
        }
        let batch = match RelayRecordBatch::concat(batches) {
            Ok(batch) => batch,
            Err(error) => {
                self.report_failure(
                    &source_acks,
                    format!(
                        "{} '{}' failed to concatenate buffered message errors for relay '{}' in \
                         domain '{}': {}",
                        self.route.node.kind.as_str(),
                        self.route.node.identifier.as_str(),
                        self.route.error_relay.as_str(),
                        self.route.domain.as_str(),
                        error
                    ),
                );
                return;
            }
        };
        if await_message_error_ack_alive(
            &pending_acks,
            self.runtime.ingest_stream_boundary_message(
                &self.route.domain,
                &self.route.error_relay,
                &self.target.registry,
                &self.target.services,
                &batch,
            ),
        )
        .await
        .is_err()
        {
            self.report_failure(
                &source_acks,
                format!(
                    "{} '{}' failed to flush message errors to relay '{}' in domain '{}'",
                    self.route.node.kind.as_str(),
                    self.route.node.identifier.as_str(),
                    self.route.error_relay.as_str(),
                    self.route.domain.as_str()
                ),
            );
            return;
        }
        for ack in source_acks {
            ack.ack_success();
        }
    }

    async fn accept(&mut self, delivery: MessageErrorDelivery, domain_clock: &DomainClock) {
        let key = delivery.batch.key.clone();
        let estimated_bytes = delivery.batch.estimated_bytes();
        if !self.pending.contains_key(&key) {
            let snapshot = match domain_clock.snapshot() {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    self.report_failure(
                        &delivery.source_acks,
                        format!(
                            "message-error route for '{}' in domain '{}' could not read its flush \
                             clock: {error}",
                            self.route.node.identifier.as_str(),
                            self.route.domain.as_str(),
                        ),
                    );
                    return;
                }
            };
            let mut flush_timer = BranchBufferTimer::default();
            if let Err(error) = flush_timer.arm_flush(self.flush_policy, domain_clock, &snapshot) {
                self.report_failure(
                    &delivery.source_acks,
                    format!(
                        "message-error route for '{}' in domain '{}' could not start its flush \
                         deadline: {error}",
                        self.route.node.identifier.as_str(),
                        self.route.domain.as_str(),
                    ),
                );
                return;
            }
            self.pending.insert(
                key.clone(),
                PendingMessageErrorDelivery {
                    deliveries: Vec::new(),
                    estimated_bytes: 0,
                    flush_timer,
                },
            );
        }
        let pending = self
            .pending
            .get_mut(&key)
            .verified("the message-error buffer is inserted above when it is absent");
        pending.estimated_bytes = pending
            .estimated_bytes
            .checked_add(estimated_bytes)
            .assured("both counts estimate bytes of batches this node already holds in memory");
        pending.deliveries.push(delivery);
        if self
            .flush_policy
            .size_boundary_reached(pending.estimated_bytes)
        {
            self.flush_key(&key).await;
        }
    }

    async fn flush_due(&mut self, domain_clock: &DomainClock) -> BranchBufferTimingResult<()> {
        let snapshot = domain_clock
            .snapshot()
            .map_err(|error| error.change_context(BranchBufferTimingError::LogicalDeadline))?;
        let mut keys = Vec::new();
        for (key, pending) in &self.pending {
            if pending.flush_timer.is_due(domain_clock, &snapshot)? {
                keys.push(key.clone());
            }
        }
        for key in keys {
            tokio::task::consume_budget().await;
            self.flush_key(&key).await;
        }
        Ok(())
    }

    async fn flush_all(&mut self) {
        let keys = self.pending.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            tokio::task::consume_budget().await;
            self.flush_key(&key).await;
        }
    }

    fn flush_deadlines(&self) -> Vec<BranchBufferDeadline> {
        self.pending
            .values()
            .filter_map(|pending| pending.flush_timer.deadline())
            .collect()
    }

    /// Releases every buffered message error for one force-flush generation.
    ///
    /// The generation covers the deliveries the channel holds when it arrives: they are accepted
    /// into their branch buffers before those buffers are released. Deliveries that land later
    /// belong to the next generation, so a failing source cannot extend one flush indefinitely.
    async fn force_flush(
        &mut self,
        input: &mut mpsc::Receiver<MessageErrorDelivery>,
        domain_clock: &DomainClock,
    ) {
        let ready = input.len();
        for _ in 0..ready {
            tokio::task::consume_budget().await;
            let Ok(delivery) = input.try_recv() else {
                break;
            };
            self.accept(delivery, domain_clock).await;
        }
        self.flush_all().await;
    }

    /// Accepts everything already sent to this route, then releases every buffer.
    ///
    /// Closing the channel first is what bounds the drain. A node that fails a message afterwards
    /// sees a stopped route and reports the failure itself, rather than adding to a buffer that
    /// nothing will publish.
    async fn drain_and_flush(
        &mut self,
        input: &mut mpsc::Receiver<MessageErrorDelivery>,
        domain_clock: &DomainClock,
    ) {
        input.close();
        while let Some(delivery) = input.recv().await {
            tokio::task::consume_budget().await;
            self.accept(delivery, domain_clock).await;
        }
        self.flush_all().await;
    }

    async fn run(
        mut self,
        mut input: mpsc::Receiver<MessageErrorDelivery>,
        mut shutdown_rx: watch::Receiver<bool>,
        mut force_flush: DomainForceFlushParticipant,
    ) {
        let domain_clock = match self.runtime.bind_domain_clock(&self.route.domain) {
            Ok(clock) => clock,
            Err(error) => {
                self.runtime.events().report_error(format!(
                    "message-error route for '{}' in domain '{}' could not bind its flush clock: \
                     {error}",
                    self.route.node.identifier.as_str(),
                    self.route.domain.as_str(),
                ));
                return;
            }
        };
        loop {
            tokio::task::consume_budget().await;
            let flush_deadlines = self.flush_deadlines();
            let has_flush_deadlines = !flush_deadlines.is_empty();
            tokio::select! {
                biased;
                // A signalled stop and a dropped sender both mean the owner is gone, and this
                // arm drains and finishes either way, so the outcome carries nothing to read.
                _ = shutdown_rx.changed() => {
                    self.drain_and_flush(&mut input, &domain_clock).await;
                    break;
                }
                completion = force_flush.changed() => {
                    let Ok(completion) = completion else {
                        // The domain's coordinator is gone, so the domain is being torn down and no
                        // later generation can arrive. Release everything this route holds now.
                        self.drain_and_flush(&mut input, &domain_clock).await;
                        break;
                    };
                    self.force_flush(&mut input, &domain_clock).await;
                    completion.complete();
                }
                result = wait_for_branch_buffer_deadlines(&domain_clock, flush_deadlines),
                    if has_flush_deadlines =>
                {
                    if let Err(error) = result {
                        let acks = self.pending_acks();
                        self.report_failure(
                            &[acks],
                            format!(
                                "message-error route for '{}' in domain '{}' could not wait for \
                                 its flush deadline: {error}",
                                self.route.node.identifier.as_str(),
                                self.route.domain.as_str(),
                            ),
                        );
                        self.flush_all().await;
                        break;
                    }
                    if let Err(error) = self.flush_due(&domain_clock).await {
                        let acks = self.pending_acks();
                        self.report_failure(
                            &[acks],
                            format!(
                                "message-error route for '{}' in domain '{}' could not inspect \
                                 its flush deadline: {error}",
                                self.route.node.identifier.as_str(),
                                self.route.domain.as_str(),
                            ),
                        );
                        self.flush_all().await;
                        break;
                    }
                }
                _ = sleep(REMOTE_ACK_ALIVE_INTERVAL), if has_flush_deadlines => {
                    self.ack_pending_alive();
                }
                delivery = input.recv() => {
                    let Some(delivery) = delivery else {
                        self.flush_all().await;
                        break;
                    };
                    self.accept(delivery, &domain_clock).await;
                }
            }
        }
    }
}

async fn await_message_error_ack_alive<F>(acks: &AckSet, future: F) -> F::Output
where
    F: std::future::Future,
{
    tokio::pin!(future);
    loop {
        tokio::task::consume_budget().await;
        acks.ack_alive();
        tokio::select! {
            biased;
            result = &mut future => return result,
            _ = sleep(REMOTE_ACK_ALIVE_INTERVAL) => {}
        }
    }
}

impl Runtime {
    pub(super) async fn enqueue_message_error_delivery(
        &self,
        route: MessageErrorRouteKey,
        target: MessageErrorRouteTarget,
        flush_policy: RuntimeFlushPolicy,
        delivery: MessageErrorDelivery,
    ) -> error_stack::Result<(), MessageErrorHandlingError> {
        let failure_route = route.clone();
        let route_runtime = match self.inner.message_error_routes.entry(route.clone()) {
            DashMapEntry::Occupied(entry) => entry.get().clone(),
            DashMapEntry::Vacant(entry) => {
                let route_runtime =
                    MessageErrorRouteRuntime::new(self.clone(), route, target, flush_policy);
                entry.insert(route_runtime.clone());
                route_runtime
            }
        };
        let source_acks = delivery.merged_source_acks();
        await_message_error_ack_alive(&source_acks, route_runtime.sender.send(delivery))
            .await
            .map_err(|_| {
                error_stack::Report::new(MessageErrorHandlingError::DeliveryStopped {
                    route: failure_route,
                })
            })
    }

    pub(super) async fn stop_message_error_routes_for_domain(&self, domain: &DomainName) {
        let keys = self
            .inner
            .message_error_routes
            .iter()
            .filter_map(|entry| (&entry.key().domain == domain).then_some(entry.key().clone()))
            .collect::<Vec<_>>();
        for key in keys {
            tokio::task::consume_budget().await;
            if let Some((_, route)) = self.inner.message_error_routes.remove(&key) {
                route.shutdown().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named<N>(raw: &str) -> N
    where
        N: for<'a> TryFrom<&'a str>,
        for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
    {
        N::try_from(raw).expect("valid name")
    }

    fn test_delivery() -> (MessageErrorDelivery, AckCompletion) {
        let schema = Arc::new(compile_schema(&nervix_models::CreateSchema {
            name: named("message_error"),
            fields: Vec::new(),
        }));
        let batch = RelayRecordBatch::single(schema, None, test_runtime_row([]), AckSet::empty())
            .expect("message-error batch must build");
        let (source_acks, completion) = AckSet::root();
        (
            MessageErrorDelivery {
                batch,
                source_acks: vec![source_acks],
            },
            completion,
        )
    }

    fn test_task(
        flush_policy: RuntimeFlushPolicy,
        fanout: RelayBoundaryFanout,
    ) -> (MessageErrorRouteTask, RelayOwnerTask) {
        let runtime = Runtime::default();
        let domain = DomainName::try_from("test").expect("valid domain");
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            unpaced_domain_state(domain.as_str()),
        )]));
        let task = MessageErrorRouteTask {
            runtime,
            route: MessageErrorRouteKey {
                domain,
                node: NodeRef::new(ModelKind::Emitter, named::<ModelName>("notifications")),
                source_route: None,
                error_relay: named("emitter_errors"),
            },
            target: MessageErrorRouteTarget {
                registry: RelayRegistry::new(),
                services: Arc::new(RelayBoundaryServices::new(fanout, 0, 0, Vec::new(), None)),
            },
            flush_policy,
            pending: HashMap::default(),
        };
        let owner_task = task.runtime.spawn_relay_owner_task(
            &task.route.domain,
            &task.route.error_relay,
            task.target.registry.clone(),
            task.target.services.clone(),
            RelayRetention::default(),
        );
        (task, owner_task)
    }

    #[tokio::test]
    async fn buffered_message_error_refreshes_source_ack_before_flush_deadline() {
        let interval = REMOTE_ACK_ALIVE_INTERVAL * 4;
        let fanout = RelayBoundaryFanout::direct_with_capacity(
            NonZeroUsize::new(1).expect("non-zero test capacity"),
        );
        let (task, owner_task) = test_task(
            RuntimeFlushPolicy::Each {
                interval,
                max_batch_size: u64::MAX,
            },
            fanout,
        );
        let (sender, input) = mpsc::channel(1);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let force_flush = task.runtime.force_flush_participant(
            &task.route.domain,
            task.runtime
                .node_quiesce_counters(&task.route.domain, task.route.node.clone()),
        );
        let task = tokio::spawn(task.run(input, shutdown_rx, force_flush));
        let (delivery, mut completion) = test_delivery();

        sender
            .send(delivery)
            .await
            .expect("message-error task must accept delivery");

        assert_eq!(
            tokio::time::timeout(
                REMOTE_ACK_ALIVE_INTERVAL * 2,
                completion.wait_for_progress(),
            )
            .await
            .expect("pending message error must refresh its source ACK before flushing"),
            AckProgress::Alive
        );

        shutdown.send_replace(true);
        task.await.expect("message-error task must stop cleanly");
        assert_eq!(completion.wait().await, AckOutcome::Ack);
        owner_task
            .stop(Duration::from_secs(1))
            .await
            .expect("relay owner should stop");
    }

    #[tokio::test]
    async fn force_flush_releases_a_buffered_message_error_before_its_cadence() {
        let runtime = Runtime::default();
        let domain = DomainName::try_from("test").expect("valid domain");
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            unpaced_domain_state(domain.as_str()),
        )]));
        let route = MessageErrorRouteKey {
            domain: domain.clone(),
            node: NodeRef::new(ModelKind::Junction, named::<ModelName>("route_orders")),
            source_route: None,
            error_relay: named("route_errors"),
        };
        let target = MessageErrorRouteTarget {
            registry: RelayRegistry::new(),
            services: Arc::new(RelayBoundaryServices::new(
                RelayBoundaryFanout::direct_with_capacity(
                    NonZeroUsize::new(1).expect("non-zero test capacity"),
                ),
                0,
                0,
                Vec::new(),
                None,
            )),
        };
        let owner_task = runtime.spawn_relay_owner_task(
            &domain,
            &route.error_relay,
            target.registry.clone(),
            target.services.clone(),
            RelayRetention::default(),
        );
        let route_runtime = MessageErrorRouteRuntime::new(
            runtime.clone(),
            route,
            target,
            RuntimeFlushPolicy::Each {
                interval: Duration::from_secs(3600),
                max_batch_size: u64::MAX,
            },
        );
        let (delivery, completion) = test_delivery();
        route_runtime
            .sender
            .send(delivery)
            .await
            .expect("message-error route must accept delivery");

        runtime.force_flush_domain(&domain);

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), completion.wait())
                .await
                .expect("a force flush must publish a message error its cadence still holds"),
            AckOutcome::Ack
        );
        route_runtime.shutdown().await;
        owner_task
            .stop(Duration::from_secs(1))
            .await
            .expect("relay owner should stop");
    }

    #[tokio::test]
    async fn blocked_message_error_relay_delivery_refreshes_source_ack() {
        let fanout = RelayBoundaryFanout::direct_with_capacity(
            NonZeroUsize::new(1).expect("non-zero test capacity"),
        );
        let gate = fanout.dispatch_gate();
        let gate_token = gate.engage(
            Instant::now() + Duration::from_secs(2),
            "block message-error relay delivery",
        );
        let (task, owner_task) = test_task(RuntimeFlushPolicy::Immediate, fanout);
        let (sender, input) = mpsc::channel(1);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let force_flush = task.runtime.force_flush_participant(
            &task.route.domain,
            task.runtime
                .node_quiesce_counters(&task.route.domain, task.route.node.clone()),
        );
        let task = tokio::spawn(task.run(input, shutdown_rx, force_flush));
        let (delivery, mut completion) = test_delivery();

        sender
            .send(delivery)
            .await
            .expect("message-error task must accept delivery");

        assert_eq!(
            tokio::time::timeout(
                REMOTE_ACK_ALIVE_INTERVAL * 2,
                completion.wait_for_progress(),
            )
            .await
            .expect("blocked message-error relay must refresh its source ACK"),
            AckProgress::Alive
        );

        gate.release(gate_token);
        assert_eq!(completion.wait().await, AckOutcome::Ack);
        shutdown.send_replace(true);
        task.await.expect("message-error task must stop cleanly");
        owner_task
            .stop(Duration::from_secs(1))
            .await
            .expect("relay owner should stop");
    }

    #[tokio::test]
    async fn enqueue_reports_when_the_error_route_has_stopped() {
        let runtime = Runtime::default();
        let domain = DomainName::try_from("test").expect("valid domain");
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            unpaced_domain_state(domain.as_str()),
        )]));
        let route = MessageErrorRouteKey {
            domain,
            node: NodeRef::new(ModelKind::Junction, named::<ModelName>("route_orders")),
            source_route: None,
            error_relay: named("route_errors"),
        };
        let target = MessageErrorRouteTarget {
            registry: RelayRegistry::new(),
            services: Arc::new(RelayBoundaryServices::new(
                RelayBoundaryFanout::direct_with_capacity(
                    NonZeroUsize::new(1).expect("non-zero test capacity"),
                ),
                0,
                0,
                Vec::new(),
                None,
            )),
        };
        let route_runtime = MessageErrorRouteRuntime::new(
            runtime.clone(),
            route.clone(),
            target.clone(),
            RuntimeFlushPolicy::Immediate,
        );
        route_runtime.shutdown().await;
        runtime
            .inner
            .message_error_routes
            .insert(route.clone(), route_runtime);
        let (delivery, _) = test_delivery();

        let error = runtime
            .enqueue_message_error_delivery(
                route.clone(),
                target,
                RuntimeFlushPolicy::Immediate,
                delivery,
            )
            .await
            .expect_err("a stopped error route must reject new delivery");

        assert!(matches!(
            error.current_context(),
            MessageErrorHandlingError::DeliveryStopped { route: failed } if failed == &route
        ));
    }
}
