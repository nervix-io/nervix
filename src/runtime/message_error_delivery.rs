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

    async fn flush_key(&mut self, key: &Option<BranchKey>) {
        let Some(pending) = self.pending.remove(key) else {
            return;
        };
        let mut batches = Vec::with_capacity(pending.deliveries.len());
        let mut source_acks = Vec::new();
        for delivery in pending.deliveries {
            tokio::task::consume_budget().await;
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
        if self
            .runtime
            .ingest_stream_boundary_message(
                &self.route.domain,
                &self.route.error_relay,
                &self.target.registry,
                &self.target.services,
                &batch,
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
        route_runtime.sender.send(delivery).await.map_err(|_| {
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
