use dashmap::mapref::entry::Entry as DashMapEntry;

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct MessageErrorRouteKey {
    pub(super) domain: Domain,
    pub(super) node_kind: String,
    pub(super) node: Identifier,
    pub(super) source_route: Option<Identifier>,
    pub(super) error_relay: Identifier,
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
    flush_at: Timestamp,
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

pub(super) fn matching_message_error_output<'a>(
    outputs: &'a nervix_models::ProcessorOutputs,
    source_route: Option<&Identifier>,
    error_relay: &Identifier,
    assignments: &[Assignment],
) -> Option<&'a ProcessorOutput> {
    source_route
        .and_then(|route| outputs.routes.iter().find(|output| &output.relay == route))
        .or_else(|| {
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
        let task = tokio::spawn(
            MessageErrorRouteTask {
                runtime,
                route,
                target,
                flush_policy,
                pending: HashMap::default(),
            }
            .run(input, shutdown_rx),
        );
        *route_runtime.task.lock() = Some(task);
        route_runtime
    }

    async fn shutdown(&self) {
        let _ = self.shutdown.send(true);
        let task = self.task.lock().take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

impl MessageErrorRouteTask {
    fn now(&self) -> Timestamp {
        self.runtime
            .current_stream_expiration_time(&self.route.domain)
            .ok()
            .flatten()
            .unwrap_or_else(current_timestamp)
    }

    fn report_failure(&self, acks: &[AckSet], reason: String) {
        let _ = self
            .runtime
            .events
            .send(RuntimeEvent::Error(reason.clone()));
        warn!(
            domain = self.route.domain.as_str(),
            node_kind = self.route.node_kind.as_str(),
            node = self.route.node.as_str(),
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
                        self.route.node_kind,
                        self.route.node.as_str(),
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
                    self.route.node_kind,
                    self.route.node.as_str(),
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

    async fn accept(&mut self, delivery: MessageErrorDelivery) {
        let key = delivery.batch.key.clone();
        let estimated_bytes = delivery.batch.estimated_bytes();
        let now = self.now();
        let pending =
            self.pending
                .entry(key.clone())
                .or_insert_with(|| PendingMessageErrorDelivery {
                    deliveries: Vec::new(),
                    estimated_bytes: 0,
                    flush_at: checked_add_duration_to_timestamp(now, self.flush_policy.interval()),
                });
        pending.estimated_bytes = pending.estimated_bytes.saturating_add(estimated_bytes);
        pending.deliveries.push(delivery);
        if self
            .flush_policy
            .size_boundary_reached(pending.estimated_bytes)
        {
            self.flush_key(&key).await;
        }
    }

    async fn flush_due(&mut self, now: Timestamp) {
        let keys = self
            .pending
            .iter()
            .filter_map(|(key, pending)| (pending.flush_at <= now).then_some(key.clone()))
            .collect::<Vec<_>>();
        for key in keys {
            tokio::task::consume_budget().await;
            self.flush_key(&key).await;
        }
    }

    async fn flush_all(&mut self) {
        let keys = self.pending.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            tokio::task::consume_budget().await;
            self.flush_key(&key).await;
        }
    }

    fn next_flush(&self) -> Option<Timestamp> {
        self.pending.values().map(|pending| pending.flush_at).min()
    }

    async fn run(
        mut self,
        mut input: mpsc::Receiver<MessageErrorDelivery>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        loop {
            tokio::task::consume_budget().await;
            let now = self.now();
            let next_flush = self.next_flush();
            let flush_wait = next_flush
                .map(|deadline| {
                    wall_duration_until_domain_deadline(
                        &self.runtime,
                        &self.route.domain,
                        now,
                        deadline,
                    )
                })
                .unwrap_or(Duration::from_secs(86_400));
            tokio::select! {
                biased;
                changed = shutdown_rx.changed() => {
                    let _ = changed;
                    input.close();
                    while let Some(delivery) = input.recv().await {
                        tokio::task::consume_budget().await;
                        self.accept(delivery).await;
                    }
                    self.flush_all().await;
                    break;
                }
                _ = sleep(flush_wait), if next_flush.is_some() => {
                    self.flush_due(self.now()).await;
                }
                _ = sleep(REMOTE_ACK_ALIVE_INTERVAL), if next_flush.is_some() => {
                    self.ack_pending_alive();
                }
                delivery = input.recv() => {
                    let Some(delivery) = delivery else {
                        self.flush_all().await;
                        break;
                    };
                    self.accept(delivery).await;
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
    ) -> Result<(), String> {
        let failure_route = route.clone();
        let route_runtime = match self.message_error_routes.entry(route.clone()) {
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
                format!(
                    "message-error route for {} '{}' to relay '{}' is stopped",
                    failure_route.node_kind,
                    failure_route.node.as_str(),
                    failure_route.error_relay.as_str()
                )
            })
    }

    pub(super) async fn stop_message_error_routes_for_domain(&self, domain: &Domain) {
        let keys = self
            .message_error_routes
            .iter()
            .filter_map(|entry| (&entry.key().domain == domain).then_some(entry.key().clone()))
            .collect::<Vec<_>>();
        for key in keys {
            tokio::task::consume_budget().await;
            if let Some((_, route)) = self.message_error_routes.remove(&key) {
                route.shutdown().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identifier(value: &str) -> Identifier {
        Identifier::parse(value).expect("valid identifier")
    }

    fn test_delivery() -> (MessageErrorDelivery, AckCompletion) {
        let schema = Arc::new(compile_schema(&nervix_models::CreateSchema {
            name: identifier("message_error"),
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
        let task = MessageErrorRouteTask {
            runtime: Runtime::default(),
            route: MessageErrorRouteKey {
                domain: Domain::try_from("test").expect("valid domain"),
                node_kind: "emitter".to_string(),
                node: identifier("notifications"),
                source_route: None,
                error_relay: identifier("emitter_errors"),
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
        let interval = REMOTE_ACK_ALIVE_INTERVAL.saturating_mul(4);
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
        let task = tokio::spawn(task.run(input, shutdown_rx));
        let (delivery, mut completion) = test_delivery();

        sender
            .send(delivery)
            .await
            .expect("message-error task must accept delivery");

        assert_eq!(
            tokio::time::timeout(
                REMOTE_ACK_ALIVE_INTERVAL.saturating_mul(2),
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
        let task = tokio::spawn(task.run(input, shutdown_rx));
        let (delivery, mut completion) = test_delivery();

        sender
            .send(delivery)
            .await
            .expect("message-error task must accept delivery");

        assert_eq!(
            tokio::time::timeout(
                REMOTE_ACK_ALIVE_INTERVAL.saturating_mul(2),
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
}
