//! Buffered delivery and acknowledgement of prepared message-error routes.
//!
//! Layer: data plane.
//! - **Owns.** Each route's branch buffers, flush cadence, relay delivery and source ACK lease.
//! - **Depends on.** Bound route plans, relay services and the domain clock.
//! - **Must not know.** Scheduled Models, error-route selection or VM compilation.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "message-error route installation binds its retained execution handle"
    )
)]

use nervix_models::DomainName;

use super::{message_error::MessageErrorHandlingError, *};

#[derive(Clone)]
pub(super) struct MessageErrorRouteTarget {
    pub(super) services: Arc<RelayBoundaryServices>,
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "a routed message error retains and observes its own acknowledgement roots"
    )
)]
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
    retired: CancellationToken,
    sender: mpsc::Sender<MessageErrorDelivery>,
    shutdown: watch::Sender<bool>,
    task: nervix_primitives::sync::blocking::Mutex<MessageErrorRouteState>,
}

enum MessageErrorRouteState {
    Prepared {
        input: mpsc::Receiver<MessageErrorDelivery>,
        shutdown: watch::Receiver<bool>,
    },
    Running(JoinHandle<()>),
    Ended,
}

struct MessageErrorRouteTask {
    runtime: Runtime,
    route: MessageErrorRouteKey,
    target: MessageErrorRouteTarget,
    flush_policy: RuntimeFlushPolicy,
    pending: HashMap<Option<BranchKey>, PendingMessageErrorDelivery>,
}

impl MessageErrorRouteRuntime {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "install or terminate the retained message error route task"
        )
    )]
    pub(super) fn prepare() -> Arc<Self> {
        let (sender, input) = mpsc::channel(1);
        let (shutdown, shutdown_rx) = watch::channel(false);
        Arc::new(Self {
            retired: CancellationToken::new(),
            sender,
            shutdown,
            task: nervix_primitives::sync::blocking::Mutex::new(MessageErrorRouteState::Prepared {
                input,
                shutdown: shutdown_rx,
            }),
        })
    }

    #[cfg(test)]
    fn new(runtime: Runtime, plan: &BoundMessageErrorRoute) -> Arc<Self> {
        let route = Self::prepare();
        assert!(route.activate(runtime, plan));
        route
    }

    /// Starts only at successful running publication. An unchanged bound handle starts once.
    fn activate(&self, runtime: Runtime, plan: &BoundMessageErrorRoute) -> bool {
        let mut state = self.task.lock();
        if !matches!(*state, MessageErrorRouteState::Prepared { .. }) {
            return false;
        }
        let MessageErrorRouteState::Prepared {
            input,
            shutdown: shutdown_rx,
        } = std::mem::replace(&mut *state, MessageErrorRouteState::Ended)
        else {
            unreachable!("the prepared state was checked while holding its lifecycle lock")
        };
        // A route owes the force-flush generations of the node whose messages failed. The
        // obligation is registered before the task starts, so a generation requested between the
        // spawn and the first poll is still owed by this route rather than missed.
        let force_flush = runtime.force_flush_participant(
            &plan.key.domain,
            runtime.node_quiesce_counters(&plan.key.domain, plan.key.node.clone()),
        );
        let flush_policy = plan
            .flush_policy
            .assured("only a buffered prepared message-error route creates a delivery task");
        let task = nervix_primitives::task::spawn(
            MessageErrorRouteTask {
                runtime,
                route: plan.key.clone(),
                target: plan.target.clone(),
                flush_policy,
                pending: HashMap::default(),
            }
            .run(input, shutdown_rx, force_flush),
        );
        *state = MessageErrorRouteState::Running(task);
        true
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "install or terminate the retained message error route task"
        )
    )]
    async fn shutdown(&self) {
        self.retired.cancel();
        self.shutdown.send_replace(true);
        let state = std::mem::replace(&mut *self.task.lock(), MessageErrorRouteState::Ended);
        if let MessageErrorRouteState::Running(task) = state {
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
            nervix_primitives::task::consume_budget().await;
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
            nervix_primitives::task::consume_budget().await;
            self.flush_key(&key).await;
        }
        Ok(())
    }

    async fn flush_all(&mut self) {
        let keys = self.pending.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            nervix_primitives::task::consume_budget().await;
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
            nervix_primitives::task::consume_budget().await;
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
            nervix_primitives::task::consume_budget().await;
            self.accept(delivery, domain_clock).await;
        }
        self.flush_all().await;
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "the retained message error route processes each admitted error delivery and \
                      flush poll"
        )
    )]
    async fn run(
        mut self,
        mut input: mpsc::Receiver<MessageErrorDelivery>,
        mut shutdown_rx: watch::Receiver<bool>,
        mut force_flush: DomainForceFlushParticipant,
    ) {
        let domain_clock = match nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "this task or concrete branch transition attaches its retained domain clock before \
             executing in the selected lifetime",
            self.runtime.bind_domain_clock(&self.route.domain)
        ) {
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
            nervix_primitives::task::consume_budget().await;
            let flush_deadlines = self.flush_deadlines();
            let has_flush_deadlines = !flush_deadlines.is_empty();
            nervix_primitives::select! {
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

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "a routed message error retains and observes its own acknowledgement roots"
    )
)]
async fn await_message_error_ack_alive<F>(acks: &AckSet, future: F) -> F::Output
where
    F: std::future::Future,
{
    tokio::pin!(future);
    loop {
        nervix_primitives::task::consume_budget().await;
        acks.ack_alive();
        nervix_primitives::select! {
            biased;
            result = &mut future => return result,
            _ = sleep(REMOTE_ACK_ALIVE_INTERVAL) => {}
        }
    }
}

impl Runtime {
    /// Prepared routes bind their worker before the domain publishes the revision. Replacement
    /// ends the exact preceding worker and drains what it already accepted.
    pub(super) fn install_message_error_route(
        &self,
        plan: &BoundMessageErrorRoute,
    ) -> Arc<MessageErrorRouteRuntime> {
        let route = plan
            .delivery
            .as_ref()
            .assured("binding prepared the buffered route handle")
            .clone();
        if !route.activate(self.clone(), plan) {
            return route;
        }
        if let Some(predecessor) = self.inner.message_error_routes.get(&plan.key) {
            predecessor.retired.cancel();
            predecessor.shutdown.send_replace(true);
        }
        let predecessor = self
            .inner
            .message_error_routes
            .insert(plan.key.clone(), route.clone());
        if let Some(predecessor) = predecessor {
            nervix_primitives::task::spawn(async move {
                predecessor.shutdown().await;
            });
        }
        route
    }

    pub(super) fn retire_unselected_message_error_routes(
        &self,
        domain: &DomainName,
        selected: &HashMap<MessageErrorRouteKey, Arc<BoundMessageErrorRoute>>,
    ) {
        let keys = self
            .inner
            .message_error_routes
            .iter()
            .filter_map(|entry| {
                (&entry.key().domain == domain
                    && selected
                        .get(entry.key())
                        .is_none_or(|plan| plan.flush_policy.is_none()))
                .then_some(entry.key().clone())
            })
            .collect::<Vec<_>>();
        for key in keys {
            if let Some((_, route)) = self.inner.message_error_routes.remove(&key) {
                route.retired.cancel();
                route.shutdown.send_replace(true);
                nervix_primitives::task::spawn(async move {
                    route.shutdown().await;
                });
            }
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "each failed record sends directly to the worker retained by its prepared \
                      route"
        )
    )]
    pub(super) async fn enqueue_message_error_delivery(
        &self,
        plan: Arc<BoundMessageErrorRoute>,
        delivery: MessageErrorDelivery,
    ) -> error_stack::Result<(), MessageErrorHandlingError> {
        let route = plan.delivery.as_ref().ok_or_else(|| {
            Report::new(MessageErrorHandlingError::PreparedRouteUnavailable {
                route: plan.key.clone(),
            })
        })?;
        let source_acks = delivery.merged_source_acks();
        let admitted = async {
            nervix_primitives::select! {
                biased;
                _ = route.retired.cancelled() => Err(()),
                sent = route.sender.send(delivery) => sent.map_err(|_| ()),
            }
        };
        await_message_error_ack_alive(&source_acks, admitted)
            .await
            .map_err(|()| {
                Report::new(MessageErrorHandlingError::DeliveryStopped {
                    route: plan.key.clone(),
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
            nervix_primitives::task::consume_budget().await;
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

    fn test_plan(
        route: MessageErrorRouteKey,
        target: MessageErrorRouteTarget,
        flush_policy: RuntimeFlushPolicy,
    ) -> Arc<BoundMessageErrorRoute> {
        let schema = Arc::new(compile_schema(&nervix_models::CreateSchema {
            name: named("message_error"),
            fields: Vec::new(),
        }));
        let materialized = HashMap::default();
        let lookups = HashMap::default();
        let branching = ResolvedBranching::unbranched();
        let lowered = lower_route_construction(
            &RouteConstruction::default(),
            SemanticScopePolicy::read_write("error_output", "error_output"),
        )
        .expect("the empty test error SET lowers");
        let program = compile_message_error_set_program(
            &route.node.identifier,
            &lowered,
            schema.clone(),
            MessageErrorCompileSchemas::default(),
            RuntimeVmCompileContext {
                available_materialized_streams: &materialized,
                available_lookups: &lookups,
                current_branching: &branching,
                udfs: None,
            },
        )
        .expect("an empty test error record compiles");
        Arc::new(BoundMessageErrorRoute {
            key: route,
            schema,
            target,
            branching,
            program,
            flush_policy: Some(flush_policy),
            delivery: Some(MessageErrorRouteRuntime::prepare()),
        })
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
                services: Arc::new(RelayBoundaryServices::new(
                    fanout,
                    0,
                    0,
                    Vec::new(),
                    None,
                    Arc::new(BranchPresence::new()),
                )),
            },
            flush_policy,
            pending: HashMap::default(),
        };
        let owner_task = task.runtime.spawn_relay_owner_task(
            &task.route.domain,
            &task.route.error_relay,
            task.target.services.clone(),
            RelayRetention::default(),
        );
        (task, owner_task)
    }

    #[nervix_primitives::test]
    async fn prepared_error_workers_activate_once_and_end_when_the_plan_becomes_immediate() {
        let fanout = RelayBoundaryFanout::direct_with_capacity(
            NonZeroUsize::new(1).expect("non-zero test capacity"),
        );
        let (task, owner_task) = test_task(RuntimeFlushPolicy::Immediate, fanout);
        let mut plan = test_plan(
            task.route.clone(),
            task.target,
            RuntimeFlushPolicy::Immediate,
        );
        let worker = plan
            .delivery
            .as_ref()
            .expect("buffered plan owns its worker")
            .clone();
        assert!(matches!(
            *worker.task.lock(),
            MessageErrorRouteState::Prepared { .. }
        ));
        assert!(task.runtime.inner.message_error_routes.is_empty());
        assert!(Arc::ptr_eq(
            &worker,
            &task.runtime.install_message_error_route(&plan)
        ));
        assert!(matches!(
            *worker.task.lock(),
            MessageErrorRouteState::Running(_)
        ));
        assert!(Arc::ptr_eq(
            &worker,
            &task.runtime.install_message_error_route(&plan)
        ));
        assert_eq!(task.runtime.inner.message_error_routes.len(), 1);

        let bound = Arc::get_mut(&mut plan).expect("the fixture exclusively owns its plan");
        bound.flush_policy = None;
        bound.delivery = None;
        let mut selected = HashMap::default();
        selected.insert(task.route.clone(), plan.clone());
        task.runtime
            .retire_unselected_message_error_routes(&task.route.domain, &selected);
        assert!(worker.retired.is_cancelled());
        assert!(task.runtime.inner.message_error_routes.is_empty());
        worker.shutdown().await;
        assert!(matches!(*worker.task.lock(), MessageErrorRouteState::Ended));
        assert!(!worker.activate(task.runtime, &plan));
        owner_task
            .stop(Duration::from_secs(1))
            .await
            .expect("relay owner stops");
    }

    #[nervix_primitives::test]
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
        let task = nervix_primitives::task::spawn(task.run(input, shutdown_rx, force_flush));
        let (delivery, mut completion) = test_delivery();

        sender
            .send(delivery)
            .await
            .expect("message-error task must accept delivery");

        assert_eq!(
            nervix_primitives::time::timeout(
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

    #[nervix_primitives::test]
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
            services: Arc::new(RelayBoundaryServices::new(
                RelayBoundaryFanout::direct_with_capacity(
                    NonZeroUsize::new(1).expect("non-zero test capacity"),
                ),
                0,
                0,
                Vec::new(),
                None,
                Arc::new(BranchPresence::new()),
            )),
        };
        let owner_task = runtime.spawn_relay_owner_task(
            &domain,
            &route.error_relay,
            target.services.clone(),
            RelayRetention::default(),
        );
        let plan = test_plan(
            route,
            target,
            RuntimeFlushPolicy::Each {
                interval: Duration::from_secs(3600),
                max_batch_size: u64::MAX,
            },
        );
        let route_runtime = MessageErrorRouteRuntime::new(runtime.clone(), &plan);
        let (delivery, completion) = test_delivery();
        route_runtime
            .sender
            .send(delivery)
            .await
            .expect("message-error route must accept delivery");

        runtime.force_flush_domain(&domain);

        assert_eq!(
            nervix_primitives::time::timeout(Duration::from_secs(2), completion.wait())
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

    #[nervix_primitives::test]
    async fn route_replacement_drains_pending_errors_and_uses_the_new_cadence() {
        let runtime = Runtime::default();
        let domain = DomainName::try_from("test").expect("valid domain");
        runtime.sync_domains(&BTreeMap::from([(
            domain.clone(),
            unpaced_domain_state(domain.as_str()),
        )]));
        let route = MessageErrorRouteKey {
            domain: domain.clone(),
            node: NodeRef::new(ModelKind::Junction, named::<ModelName>("route_orders")),
            source_route: Some(named("out")),
            error_relay: named("route_errors"),
        };
        let target = MessageErrorRouteTarget {
            services: Arc::new(RelayBoundaryServices::new(
                RelayBoundaryFanout::direct_with_capacity(
                    NonZeroUsize::new(2).expect("non-zero test capacity"),
                ),
                0,
                0,
                Vec::new(),
                None,
                Arc::new(BranchPresence::new()),
            )),
        };
        let owner_task = runtime.spawn_relay_owner_task(
            &domain,
            &route.error_relay,
            target.services.clone(),
            RelayRetention::default(),
        );
        let mut previous = test_plan(
            route.clone(),
            target.clone(),
            RuntimeFlushPolicy::Each {
                interval: Duration::from_secs(3600),
                max_batch_size: u64::MAX,
            },
        );
        let mut replacement = test_plan(route.clone(), target, RuntimeFlushPolicy::Immediate);
        let worker = runtime.install_message_error_route(&previous);
        Arc::get_mut(&mut previous)
            .assured("the fixture owns its prepared plan")
            .delivery = Some(worker);
        let (first, first_completion) = test_delivery();
        runtime
            .enqueue_message_error_delivery(previous, first)
            .await
            .expect("the preceding route accepts its error");
        let worker = runtime.install_message_error_route(&replacement);
        Arc::get_mut(&mut replacement)
            .assured("the fixture owns its prepared plan")
            .delivery = Some(worker);
        let (second, second_completion) = test_delivery();
        runtime
            .enqueue_message_error_delivery(replacement.clone(), second)
            .await
            .expect("the replacement route accepts its error");

        assert_eq!(
            nervix_primitives::time::timeout(Duration::from_secs(2), first_completion.wait())
                .await
                .expect("the preceding route drains on replacement"),
            AckOutcome::Ack
        );
        assert_eq!(
            nervix_primitives::time::timeout(Duration::from_secs(2), second_completion.wait())
                .await
                .expect("the replacement applies its immediate cadence"),
            AckOutcome::Ack
        );
        let installed = runtime
            .inner
            .message_error_routes
            .get(&route)
            .expect("the replacement is installed");
        assert!(Arc::ptr_eq(
            installed.value(),
            replacement
                .delivery
                .as_ref()
                .assured("the fixture installed its route")
        ));
        drop(installed);
        runtime.stop_message_error_routes_for_domain(&domain).await;
        owner_task
            .stop(Duration::from_secs(1))
            .await
            .expect("relay owner should stop");
    }

    #[nervix_primitives::test]
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
        let task = nervix_primitives::task::spawn(task.run(input, shutdown_rx, force_flush));
        let (delivery, mut completion) = test_delivery();

        sender
            .send(delivery)
            .await
            .expect("message-error task must accept delivery");

        assert_eq!(
            nervix_primitives::time::timeout(
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

    #[nervix_primitives::test]
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
            services: Arc::new(RelayBoundaryServices::new(
                RelayBoundaryFanout::direct_with_capacity(
                    NonZeroUsize::new(1).expect("non-zero test capacity"),
                ),
                0,
                0,
                Vec::new(),
                None,
                Arc::new(BranchPresence::new()),
            )),
        };
        let mut plan = test_plan(route.clone(), target, RuntimeFlushPolicy::Immediate);
        let route_runtime = MessageErrorRouteRuntime::new(runtime.clone(), &plan);
        route_runtime.shutdown().await;
        Arc::get_mut(&mut plan)
            .assured("the fixture owns its prepared plan")
            .delivery = Some(route_runtime.clone());
        runtime
            .inner
            .message_error_routes
            .insert(route.clone(), route_runtime);
        let (delivery, _) = test_delivery();

        let error = runtime
            .enqueue_message_error_delivery(plan, delivery)
            .await
            .expect_err("a stopped error route must reject new delivery");

        assert!(matches!(
            error.current_context(),
            MessageErrorHandlingError::DeliveryStopped { route: failed } if failed == &route
        ));
    }
}
