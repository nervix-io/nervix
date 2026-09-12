//! Read-only filtered views a session attaches to a relay.
//!
//! Layer: control plane.
//!
//! - **Owns.** Subscription creation and deletion, the per-session subscription set, cluster-wide
//!   subscription interest, and the sampling and delivery each subscription asks for.
//! - **Depends on.** The registry for schemas and schedules, the runtime for relay receivers, and
//!   the interconnect to make interest visible on every node.
//! - **Must not know.** Construction, inheritance, values or any other side effect a processor has.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc as StdArc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use ahash::{HashMap, HashMapExt};
use blake3::Hasher;
use futures_util::{StreamExt, stream::FuturesUnordered};
use meticulous::OptionExt as _;
use nervix_approx_into::ApproxInto;
use nervix_consensus::ReplicatedTransaction;
use nervix_interconnect::SubscriptionInterestVisibilityRequest as RemoteSubscriptionInterestVisibilityRequest;
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeName, CreateBranch, CreateRelay, CreateSchema, DomainName,
    FieldName, Model, ModelKind, ModelName, NodeRef, ParseAsType, RelayName, ScheduledModel,
    SubscriptionBinding, SubscriptionDeliveryBehavior, SubscriptionLiteral, SubscriptionName,
    UserName,
};
use nervix_nspl::client_statement::{ClientStatement, ParsedClientStatement};
use nervix_recovery::NoReceiver;
use sorted_vec::SortedSet;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tonic::Status;
use triomphe::Arc;

#[cfg(test)]
use super::authentication::DEFAULT_USER;
use super::{
    domain_clock::current_timestamp,
    model_mutation::{
        append_command_result, command_batch_result, command_error, command_ok,
        command_results_message,
    },
    session_service::SessionServiceImpl,
    transaction::transaction_status,
};
use crate::{
    proto,
    proto::{
        CommandResult, CommandResultKind, Diagnostic, ServerEvent, ServerEventLevel,
        SessionResponse,
    },
    runtime::{
        CompiledProgramWithMaterializedInterest, RelayMessage, RelayRecordBatch,
        RelaySubscriptionReceiver, RelaySubscriptionRecvError, Runtime,
        RuntimeMaterializedRelaySpec, RuntimeVmCompileContext, compile_session_filter_map_program,
        execute_filter_map_on_record, scheduled_relay_owner_nodes,
    },
    runtime_schema,
    task_shutdown::JoinShutdown,
};
static SESSION_SAMPLE_COUNTER: AtomicU64 = AtomicU64::new(0);

struct SessionSubscription {
    domain: DomainName,
    relay: RelayName,
    active: Arc<AtomicBool>,
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

#[derive(Clone)]
pub(in crate::application) struct SubscriptionFilter {
    bindings: Vec<SubscriptionMatcher>,
}

#[derive(Clone)]
struct SubscriptionMatcher {
    field: FieldName,
    expected: runtime_schema::RuntimeValue,
}

pub(in crate::application) struct SessionSubscriptions {
    subscriptions: HashMap<SubscriptionName, SessionSubscription>,
    pub(in crate::application) user: UserName,
    pub(in crate::application) session_id: String,
    transaction_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(in crate::application) struct PendingSessionCommand {
    pub(in crate::application) source: String,
    pub(in crate::application) statement: ClientStatement,
    pub(in crate::application) domain: String,
}

#[derive(Debug)]
pub(in crate::application) enum SessionCommandOperation {
    Begin { domain: String },
    Queue(PendingSessionCommand),
    Commit,
    Revert,
    Execute(PendingSessionCommand),
}

struct SessionSubscriptionTaskConfig {
    filter_map: Option<CompiledProgramWithMaterializedInterest>,
    sensitivity: nervix_vm::SchemaSensitivity,
    delivery_behavior: SubscriptionDeliveryBehavior,
    batch_sample_rate: Option<f64>,
    runtime: Runtime,
    materialized_stream_owner_nodes: HashMap<RelayName, Option<ClusterNodeName>>,
    receiver: RelaySubscriptionReceiver<RelayRecordBatch>,
    tx: mpsc::Sender<Result<SessionResponse, Status>>,
}

impl SessionSubscriptions {
    #[cfg(test)]
    pub(in crate::application) fn new() -> Self {
        Self::for_user(
            UserName::parse(DEFAULT_USER).expect("default user identifier must be valid"),
        )
    }

    pub(in crate::application) fn for_user(user: UserName) -> Self {
        Self {
            subscriptions: HashMap::new(),
            user,
            session_id: uuid::Uuid::now_v7().to_string(),
            transaction_id: None,
        }
    }

    pub(in crate::application) fn transaction_active(&self) -> bool {
        self.transaction_id.is_some()
    }

    pub(in crate::application) fn plan_commands(
        &self,
        statements: Vec<ParsedClientStatement>,
        query: &str,
        request_domain: &str,
    ) -> Result<Vec<SessionCommandOperation>, String> {
        let mut transaction_active = self.transaction_active();
        let multi_statement = statements.len() > 1;
        let mut operations = Vec::with_capacity(statements.len());

        for parsed in statements {
            let span = parsed.span.clone();
            match parsed.statement {
                ClientStatement::BeginTransaction => {
                    if transaction_active {
                        return Err("transaction is already active".to_string());
                    }
                    transaction_active = true;
                    operations.push(SessionCommandOperation::Begin {
                        domain: request_domain.to_string(),
                    });
                }
                ClientStatement::CommitTransaction => {
                    if !transaction_active {
                        return Err("COMMIT requires an active transaction".to_string());
                    }
                    transaction_active = false;
                    operations.push(SessionCommandOperation::Commit);
                }
                ClientStatement::RevertTransaction => {
                    if !transaction_active {
                        return Err("REVERT requires an active transaction".to_string());
                    }
                    transaction_active = false;
                    operations.push(SessionCommandOperation::Revert);
                }
                statement => {
                    let command = PendingSessionCommand {
                        source: query[span].to_string(),
                        statement,
                        domain: request_domain.to_string(),
                    };
                    if transaction_active {
                        operations.push(SessionCommandOperation::Queue(command));
                    } else if multi_statement {
                        return Err("multiple commands require BEGIN".to_string());
                    } else {
                        operations.push(SessionCommandOperation::Execute(command));
                    }
                }
            }
        }

        Ok(operations)
    }

    pub(in crate::application) fn bind_transaction(&mut self, id: String) {
        self.transaction_id = Some(id);
    }

    pub(in crate::application) fn transaction_id(&self) -> Option<&str> {
        self.transaction_id.as_deref()
    }

    pub(in crate::application) fn detach_transaction(&mut self) -> Option<String> {
        self.transaction_id.take()
    }

    fn insert(
        &mut self,
        name: SubscriptionName,
        domain: DomainName,
        relay: RelayName,
        config: SessionSubscriptionTaskConfig,
    ) {
        let SessionSubscriptionTaskConfig {
            filter_map,
            sensitivity,
            delivery_behavior,
            batch_sample_rate,
            runtime,
            materialized_stream_owner_nodes,
            receiver,
            tx,
        } = config;
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let active = Arc::new(AtomicBool::new(true));
        let task_active = active.clone();
        let task_domain = domain.clone();
        let event_name = name.clone();
        let event_stream = relay.clone();
        let task = tokio::spawn(async move {
            let mut receiver = receiver;
            'subscription_loop: loop {
                tokio::task::consume_budget().await;
                tokio::select! {
                    batch = receiver.recv() => {
                        match batch {
                            Ok(batch) => {
                                let messages = match batch.try_into_messages() {
                                    Ok(messages) => messages,
                                    Err(error_and_batch) => {
                                        let (error, _) = *error_and_batch;
                                        let event = SessionResponse {
                                            event: Some(proto::session_response::Event::Server(
                                                ServerEvent {
                                                    level: i32::from(ServerEventLevel::Error),
                                                    message: format!(
                                                        "session subscription '{}' failed to expand relay batch: {}",
                                                        event_name, error
                                                    ),
                                                },
                                            )),
                                        };
                                        if tx.send(Ok(event)).await.is_err() {
                                            break 'subscription_loop;
                                        }
                                        continue;
                                    }
                                };
                                for message in messages {
                                    tokio::task::consume_budget().await;
                                    let Some(message) = (match filter_map.as_ref() {
                                        Some(filter_map) => {
                                            let execution_snapshot = match runtime
                                                .domain_execution_snapshot(&task_domain)
                                            {
                                                Ok(snapshot) => snapshot,
                                                Err(error) => {
                                                    let event = SessionResponse {
                                                        event: Some(proto::session_response::Event::Server(
                                                            ServerEvent {
                                                                level: i32::from(ServerEventLevel::Error),
                                                                message: format!(
                                                                    "session subscription '{}' could not read domain execution time: {}",
                                                                    event_name, error
                                                                ),
                                                            },
                                                        )),
                                                    };
                                                    if tx.send(Ok(event)).await.is_err() {
                                                        break 'subscription_loop;
                                                    }
                                                    continue;
                                                }
                                            };
                                            let side_inputs = match runtime
                                                .load_materialized_side_inputs(
                                                    &task_domain,
                                                    &message.key,
                                                    &filter_map.materialized_interest,
                                                    &materialized_stream_owner_nodes,
                                                )
                                                .await
                                            {
                                                Ok(values) => values,
                                                Err(error) => {
                                                    let event = SessionResponse {
                                                        event: Some(proto::session_response::Event::Server(
                                                            ServerEvent {
                                                                level: i32::from(ServerEventLevel::Error),
                                                                message: format!(
                                                                    "session subscription '{}' failed to load materialized side inputs: {}",
                                                                    event_name, error
                                                                ),
                                                            },
                                                        )),
                                                    };
                                                    if tx.send(Ok(event)).await.is_err() {
                                                        break 'subscription_loop;
                                                    }
                                                    continue;
                                                }
                                            };
                                            match execute_filter_map_on_record(
                                                &event_name,
                                                filter_map,
                                                message.record.clone(),
                                                message.key.as_ref(),
                                                None,
                                                &side_inputs,
                                                execution_snapshot.now(),
                                            )
                                            .await
                                            {
                                            Ok(Some(record)) => Some(RelayMessage {
                                                key: message.key,
                                                record,
                                                acks: message.acks,
                                            }),
                                            Ok(None) => None,
                                            Err(error) => {
                                                let event = SessionResponse {
                                                    event: Some(proto::session_response::Event::Server(
                                                        ServerEvent {
                                                            level: i32::from(ServerEventLevel::Error),
                                                            message: format!(
                                                                "session subscription '{}' FILTER-MAP failed: {}",
                                                                event_name, error
                                                            ),
                                                        },
                                                    )),
                                                };
                                                if tx.send(Ok(event)).await.is_err() {
                                                    break 'subscription_loop;
                                                }
                                                continue;
                                            }
                                            }
                                        }
                                        None => Some(message),
                                    }) else {
                                        continue;
                                    };
                                    if !subscription_sample_passes(batch_sample_rate, &message) {
                                        continue;
                                    }
                                    let payload = format_stream_message(&message, &sensitivity);
                                    let event = SessionResponse {
                                        event: Some(proto::session_response::Event::Subscription(
                                            proto::SubscriptionEvent {
                                                subscription: event_name.as_str().to_string(),
                                                relay: event_stream.as_str().to_string(),
                                                payload,
                                            }
                                        )),
                                    };
                                    match delivery_behavior {
                                        SubscriptionDeliveryBehavior::Blocking => {
                                            if tx.send(Ok(event)).await.is_err() {
                                                break 'subscription_loop;
                                            }
                                        }
                                        SubscriptionDeliveryBehavior::Dropping => {
                                            match tx.try_send(Ok(event)) {
                                                Ok(()) => {}
                                                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {}
                                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                                    break 'subscription_loop;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Err(RelaySubscriptionRecvError::Closed) => {
                                let event = SessionResponse {
                                    event: Some(proto::session_response::Event::Server(
                                        ServerEvent {
                                            level: i32::from(ServerEventLevel::Error),
                                            message: format!(
                                                "session subscription '{}' was dropped because \
                                                 relay '{}' in domain '{}' was rebuilt after a \
                                                 schema or execution change; recreate the \
                                                 subscription against the current schema",
                                                event_name,
                                                event_stream,
                                                task_domain,
                                            ),
                                        },
                                    )),
                                };
                                tx.send(Ok(event))
                                    .await
                                    .means_peer_left("session subscription stream");
                                break 'subscription_loop;
                            }
                            Err(RelaySubscriptionRecvError::Overflowed(_)) => continue,
                        }
                    }
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break 'subscription_loop;
                        }
                    }
                }
            }
            task_active.store(false, Ordering::Release);
        });

        self.subscriptions.insert(
            name,
            SessionSubscription {
                domain,
                relay,
                active,
                stop_tx,
                task,
            },
        );
    }

    fn contains_domain_stream(&self, domain: &DomainName, relay: &RelayName) -> bool {
        self.subscriptions.values().any(|subscription| {
            subscription.active.load(Ordering::Acquire)
                && subscription.domain == *domain
                && subscription.relay == *relay
        })
    }

    fn contains_name(&self, name: &SubscriptionName) -> bool {
        self.subscriptions
            .get(name)
            .is_some_and(|subscription| subscription.active.load(Ordering::Acquire))
    }

    pub(in crate::application) fn matching_names(&self, prefix: &str) -> Vec<String> {
        let prefix = prefix.to_ascii_lowercase();
        self.subscriptions
            .keys()
            .filter(|name| {
                self.contains_name(name)
                    && (prefix.is_empty() || name.as_str().starts_with(&prefix))
            })
            .map(ToString::to_string)
            .collect()
    }

    async fn remove(&mut self, name: &SubscriptionName) -> Option<(DomainName, RelayName)> {
        let subscription = self.subscriptions.remove(name)?;
        subscription.stop_tx.send_replace(true);
        subscription
            .task
            .join_after_shutdown("session subscription")
            .await;
        Some((subscription.domain, subscription.relay))
    }

    pub(in crate::application) async fn stop_all(&mut self, service: &SessionServiceImpl) {
        for (_, subscription) in self.subscriptions.drain() {
            subscription.stop_tx.send_replace(true);
            subscription
                .task
                .join_after_shutdown("session subscription")
                .await;
            service
                .unregister_subscription_interest(&subscription.domain, &subscription.relay)
                .await;
        }
    }
}

pub(in crate::application) fn format_stream_message(
    message: &RelayMessage,
    sensitivity: &nervix_vm::SchemaSensitivity,
) -> String {
    let payload = message
        .record
        .to_json_string_masking(sensitivity)
        .unwrap_or_else(|error| format!("<invalid Arrow row: {error}>"));
    match message.key.as_ref() {
        Some(key) => format!("key={} payload={}", key.as_str(), payload),
        None => payload,
    }
}

pub(in crate::application) fn validate_subscription_bindings(
    relay: &RelayName,
    branching: &[FieldName],
    schema: &nervix_models::CreateSchema,
    bindings: &[SubscriptionBinding],
) -> Result<SubscriptionFilter, String> {
    if branching.is_empty() {
        if bindings.is_empty() {
            return Ok(SubscriptionFilter {
                bindings: Vec::new(),
            });
        }
        return Err(format!(
            "stream '{}' is not branched and does not accept WHERE bindings",
            relay.as_str()
        ));
    }

    if bindings.is_empty() {
        return Err(format!(
            "stream '{}' requires WHERE bindings for ({})",
            relay.as_str(),
            branching
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let mut fields = HashMap::new();
    for field in &schema.fields {
        fields.insert(field.name.clone(), field.ty.clone());
    }

    let mut bound = HashMap::new();
    for binding in bindings {
        if bound
            .insert(binding.field.clone(), binding.value.clone())
            .is_some()
        {
            return Err(format!(
                "subscription binding '{}' is specified more than once",
                binding.field.as_str()
            ));
        }
    }

    let expected = SortedSet::from_unsorted(branching.to_vec()).into_vec();
    let actual = SortedSet::from_unsorted(bound.keys().cloned().collect::<Vec<_>>()).into_vec();
    if expected != actual {
        return Err(format!(
            "subscription bindings for relay '{}' must exactly match ({})",
            relay.as_str(),
            branching
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let mut matchers = Vec::new();
    for field in branching {
        let ty = fields.get(field).ok_or_else(|| {
            format!(
                "branch field '{}' is missing from schema '{}'",
                field.as_str(),
                schema.name.as_str()
            )
        })?;
        let literal = bound
            .get(field)
            .verified("the check above requires the bound keys to match the branch fields exactly");
        let expected = parse_subscription_literal(field, ty, literal)?;
        matchers.push(SubscriptionMatcher {
            field: field.clone(),
            expected,
        });
    }

    Ok(SubscriptionFilter { bindings: matchers })
}

pub(in crate::application) fn branch_key_from_filter(
    branching: &[FieldName],
    filter: &SubscriptionFilter,
) -> Result<Option<crate::runtime::BranchKey>, String> {
    if branching.is_empty() {
        return Ok(None);
    }
    let mut fields = Vec::with_capacity(branching.len());
    for field in branching {
        let Some(binding) = filter
            .bindings
            .iter()
            .find(|binding| binding.field == *field)
        else {
            return Err(format!(
                "missing binding for branch field '{}'",
                field.as_str()
            ));
        };
        fields.push((field.clone(), binding.expected.clone()));
    }
    crate::runtime::BranchKey::from_fields(fields).map(Some)
}

pub(in crate::application) fn render_subscription_literal(literal: &SubscriptionLiteral) -> String {
    match literal {
        SubscriptionLiteral::String(value) => format!("'{}'", value.replace('\'', "''")),
        SubscriptionLiteral::Number(value) => value.clone(),
        SubscriptionLiteral::Bool(value) => value.to_string(),
    }
}

fn parse_subscription_batch_sample_rate(rate: Option<&str>) -> Result<Option<f64>, String> {
    let Some(rate) = rate else {
        return Ok(None);
    };
    let parsed = rate
        .parse::<f64>()
        .map_err(|error| format!("invalid batch sample rate '{rate}': {error}"))?;
    if (0.0..=1.0).contains(&parsed) {
        Ok(Some(parsed))
    } else {
        Err(format!(
            "invalid batch sample rate '{rate}': must be between 0.0 and 1.0"
        ))
    }
}

fn subscription_sample_passes(batch_sample_rate: Option<f64>, message: &RelayMessage) -> bool {
    let Some(rate) = batch_sample_rate else {
        return true;
    };
    if rate >= 1.0 {
        return true;
    }
    if rate <= 0.0 {
        return false;
    }

    let counter = SESSION_SAMPLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut hasher = Hasher::new();
    hasher.update(&counter.to_le_bytes());
    if let Some(key) = message.key.as_ref() {
        hasher.update(key.as_str().as_bytes());
    }
    let hash = hasher.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    let draw = u64::from_le_bytes(bytes).approx_into::<f64>() / u64::MAX.approx_into::<f64>();
    draw < rate
}

pub(in crate::application) fn parse_subscription_literal(
    field: &FieldName,
    ty: &ParseAsType,
    literal: &SubscriptionLiteral,
) -> Result<runtime_schema::RuntimeValue, String> {
    use runtime_schema::RuntimeValue;

    let bad = |expected: &str| {
        format!(
            "subscription binding '{}' expects {} literal for type {:?}",
            field.as_str(),
            expected,
            ty
        )
    };

    match (ty, literal) {
        (ParseAsType::String, SubscriptionLiteral::String(v)) => {
            Ok(RuntimeValue::String(v.clone()))
        }
        (ParseAsType::Datetime, SubscriptionLiteral::String(v)) => {
            chrono::DateTime::parse_from_rfc3339(v)
                .map(RuntimeValue::Datetime)
                .map_err(|_| bad("RFC3339 datetime string"))
        }
        (ParseAsType::Bool, SubscriptionLiteral::Bool(v)) => Ok(RuntimeValue::Bool(*v)),
        (ParseAsType::U8, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::U8).map_err(|_| bad("numeric"))
        }
        (ParseAsType::I8, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::I8).map_err(|_| bad("numeric"))
        }
        (ParseAsType::U16, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::U16).map_err(|_| bad("numeric"))
        }
        (ParseAsType::I16, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::I16).map_err(|_| bad("numeric"))
        }
        (ParseAsType::U32, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::U32).map_err(|_| bad("numeric"))
        }
        (ParseAsType::I32, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::I32).map_err(|_| bad("numeric"))
        }
        (ParseAsType::U64, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::U64).map_err(|_| bad("numeric"))
        }
        (ParseAsType::I64, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::I64).map_err(|_| bad("numeric"))
        }
        (ParseAsType::F32, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::F32).map_err(|_| bad("numeric"))
        }
        (ParseAsType::F64, SubscriptionLiteral::Number(v)) => {
            v.parse().map(RuntimeValue::F64).map_err(|_| bad("numeric"))
        }
        _ => Err(bad(match ty {
            ParseAsType::String | ParseAsType::Datetime => "string",
            ParseAsType::Bool => "boolean",
            ParseAsType::Array { .. } | ParseAsType::Vec { .. } => "array",
            _ => "numeric",
        })),
    }
}

/// One relay whose subscription interest this node advertises to the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(in crate::application) struct SubscriptionInterestKey {
    domain: DomainName,
    relay: RelayName,
}

/// The relay a subscription attaches to, as the cluster schedule describes it: the relay model,
/// the schema its records carry, and the branch key fields a subscription may bind.
pub(in crate::application) struct SubscriptionTarget {
    pub(in crate::application) relay: nervix_models::CreateRelay,
    pub(in crate::application) schema: nervix_models::CreateSchema,
    pub(in crate::application) branching: Vec<FieldName>,
}

impl SessionServiceImpl {
    async fn register_subscription_interest(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<(), String> {
        let key = SubscriptionInterestKey {
            domain: domain.clone(),
            relay: relay.clone(),
        };
        let first_interest = {
            let mut entry = self
                .inner
                .subscription_interest_counts
                .entry(key)
                .or_insert(0);
            *entry += 1;
            *entry == 1
        };
        if first_interest {
            self.inner
                .cluster
                .set_local_subscription_interest(domain.as_str(), relay.as_str(), true)
                .await;
        }
        if let Err(error) = self
            .wait_for_subscription_interest_visibility(domain, relay)
            .await
        {
            self.unregister_subscription_interest(domain, relay).await;
            return Err(error);
        }
        Ok(())
    }

    async fn wait_for_subscription_interest_visibility(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<(), String> {
        let subscriber = self.inner.cluster.local_node_identity().await;
        let target_nodes = self
            .inner
            .cluster
            .live_node_ids()
            .await
            .into_iter()
            .filter(|node_id| node_id != subscriber.node_id())
            .collect::<BTreeSet<_>>();
        let mut checks = target_nodes
            .iter()
            .map(|node_id| {
                let subscriber = subscriber.clone();
                async move {
                    let result = self
                        .wait_for_subscription_interest_visibility_on_node(
                            node_id,
                            &subscriber,
                            domain,
                            relay,
                        )
                        .await;
                    (node_id, result)
                }
            })
            .collect::<FuturesUnordered<_>>();
        let mut errors = BTreeMap::new();
        while let Some((node_id, result)) = checks.next().await {
            tokio::task::consume_budget().await;
            if let Err(error) = result {
                errors.insert(node_id.clone(), error);
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "subscription interest in relay '{}' in domain '{}' did not become visible on \
                 every live node: {:?}",
                relay.as_str(),
                domain.as_str(),
                errors,
            ))
        }
    }

    async fn wait_for_subscription_interest_visibility_on_node(
        &self,
        target_node_id: &ClusterNodeName,
        subscriber: &ClusterNodeIdentity,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<(), String> {
        let response = self
            .inner
            .interconnect
            .request(
                target_node_id,
                RemoteSubscriptionInterestVisibilityRequest {
                    subscriber: subscriber.clone(),
                    domain: domain.clone(),
                    relay: relay.clone(),
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        response.result
    }

    async fn unregister_subscription_interest(&self, domain: &DomainName, relay: &RelayName) {
        let key = SubscriptionInterestKey {
            domain: domain.clone(),
            relay: relay.clone(),
        };
        let mut should_clear = false;
        if let Some(mut entry) = self.inner.subscription_interest_counts.get_mut(&key) {
            if *entry <= 1 {
                should_clear = true;
            } else {
                *entry -= 1;
            }
        }
        if should_clear {
            self.inner.subscription_interest_counts.remove(&key);
            self.inner
                .cluster
                .set_local_subscription_interest(domain.as_str(), relay.as_str(), false)
                .await;
        }
    }

    pub(in crate::application) async fn scheduled_stream_owner_nodes(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Vec<ClusterNodeName>, String> {
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok(Vec::new());
        };
        Ok(scheduled_relay_owner_nodes(domain_schedule, relay))
    }

    async fn process_pending_session_commands(
        &self,
        commands: Vec<PendingSessionCommand>,
        tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
        explicit_batch: bool,
    ) -> CommandResult {
        let is_batch = explicit_batch || commands.len() > 1;
        let mut results = Vec::new();
        let mut commands = commands.into_iter().peekable();

        while let Some(command) = commands.next() {
            match command.statement {
                ClientStatement::Server(statement) if statement.is_model_mutation() => {
                    let domain = command.domain;
                    let mut sources = vec![command.source];
                    let mut statements = vec![statement];

                    while let Some(next) = commands.peek() {
                        if next.domain != domain {
                            break;
                        }
                        let ClientStatement::Server(statement) = &next.statement else {
                            break;
                        };
                        if !statement.is_model_mutation() {
                            break;
                        }
                        let next = commands.next().verified(
                            "the peek above observed this command and nothing consumed the \
                             iterator since",
                        );
                        let ClientStatement::Server(statement) = next.statement else {
                            unreachable!("peeked model mutation statement must be next");
                        };
                        sources.push(next.source);
                        statements.push(statement);
                    }

                    let mutation_query = sources.join("; ");
                    let result = self
                        .process_model_mutation_batch(statements, &mutation_query, &domain)
                        .await;
                    if !result.success {
                        return command_batch_result(results, result, is_batch);
                    }
                    append_command_result(&mut results, result);
                }
                statement => {
                    let result = self
                        .process_client_statement(
                            statement,
                            &command.source,
                            &command.domain,
                            tx,
                            subscriptions,
                        )
                        .await;
                    if !result.success {
                        return command_batch_result(results, result, is_batch);
                    }
                    append_command_result(&mut results, result);
                }
            }
        }

        if results.is_empty() {
            return command_error("empty command".to_string());
        }
        if !is_batch {
            return results
                .pop()
                .verified("the empty check above already returned");
        }

        CommandResult {
            success: true,
            message: command_results_message(&results),
            diagnostics: Vec::new(),
            kind: i32::from(CommandResultKind::Ok),
            results,
            ..Default::default()
        }
    }

    pub(in crate::application) async fn subscription_target_from_schedule(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Option<SubscriptionTarget>, String> {
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok(None);
        };
        let Some(ScheduledModel {
            config: ack_model,
            node: relay_node,
        }) = domain_schedule.scheduled::<CreateRelay>(relay)
        else {
            return Ok(None);
        };
        let Some(schema) = domain_schedule.configured::<CreateSchema>(&ack_model.schema) else {
            return Err(format!(
                "stream '{}' references missing scheduled schema '{}'",
                relay.as_str(),
                ack_model.schema.as_str()
            ));
        };
        Ok(Some(SubscriptionTarget {
            relay: ack_model.clone(),
            schema: schema.clone(),
            branching: relay_node.effective_branching.clone().unwrap_or_default(),
        }))
    }

    async fn subscription_stream_schema(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Option<nervix_models::CreateSchema>, String> {
        match self.inner.registry.get::<CreateRelay>(domain, relay) {
            Ok(Some(ack_model)) => {
                match self
                    .inner
                    .registry
                    .get::<CreateSchema>(domain, &ack_model.schema)
                {
                    Ok(Some(schema)) => Ok(Some(schema)),
                    Ok(None) => Err(format!(
                        "stream '{}' references missing schema '{}'",
                        relay.as_str(),
                        ack_model.schema.as_str()
                    )),
                    Err(err) => Err(format!(
                        "failed to resolve schema '{}' for relay '{}': {err}",
                        ack_model.schema.as_str(),
                        relay.as_str()
                    )),
                }
            }
            Ok(None) => self
                .subscription_target_from_schedule(domain, relay)
                .await
                .map(|resolved| resolved.map(|target| target.schema)),
            Err(err) => Err(format!(
                "failed to resolve relay '{}' for subscription: {err}",
                relay.as_str()
            )),
        }
    }

    async fn subscription_branch_schema(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Option<StdArc<arrow_schema::Schema>>, String> {
        match self.inner.registry.get::<CreateRelay>(domain, relay) {
            Ok(Some(relay_model)) => {
                let Some(branch_ref) = relay_model.branching.branch() else {
                    return Ok(None);
                };
                let branch = match self.inner.registry.get::<CreateBranch>(domain, branch_ref) {
                    Ok(Some(branch)) => branch,
                    Ok(None) => {
                        return Err(format!(
                            "stream '{}' references missing branch '{}'",
                            relay.as_str(),
                            branch_ref.as_str()
                        ));
                    }
                    Err(err) => {
                        return Err(format!(
                            "failed to resolve branch '{}' for relay '{}': {err}",
                            branch_ref.as_str(),
                            relay.as_str()
                        ));
                    }
                };
                match self
                    .inner
                    .registry
                    .get::<CreateSchema>(domain, &branch.schema)
                {
                    Ok(Some(schema)) => {
                        Ok(Some(runtime_schema::compile_schema(&schema).arrow_schema()))
                    }
                    Ok(None) => Err(format!(
                        "stream '{}' references missing branch schema '{}'",
                        relay.as_str(),
                        branch.schema.as_str()
                    )),
                    Err(err) => Err(format!(
                        "failed to resolve branch schema '{}' for relay '{}': {err}",
                        branch.schema.as_str(),
                        relay.as_str()
                    )),
                }
            }
            Ok(None) => {
                self.subscription_branch_schema_from_schedule(domain, relay)
                    .await
            }
            Err(err) => Err(format!(
                "failed to resolve relay '{}' for subscription: {err}",
                relay.as_str()
            )),
        }
    }

    async fn subscription_branch_schema_from_schedule(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Option<StdArc<arrow_schema::Schema>>, String> {
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok(None);
        };
        let Some(ScheduledModel {
            config: relay_model,
            node: relay_node,
        }) = domain_schedule.scheduled::<CreateRelay>(relay)
        else {
            return Ok(None);
        };
        if let Some(branch_ref) = relay_model.branching.branch() {
            let Some(branch) = domain_schedule.configured::<CreateBranch>(branch_ref) else {
                return Err(format!(
                    "stream '{}' references missing scheduled branch '{}'",
                    relay.as_str(),
                    branch_ref.as_str()
                ));
            };
            let Some(schema) = domain_schedule.configured::<CreateSchema>(&branch.schema) else {
                return Err(format!(
                    "stream '{}' references missing scheduled branch schema '{}'",
                    relay.as_str(),
                    branch.schema.as_str()
                ));
            };
            return Ok(Some(runtime_schema::compile_schema(schema).arrow_schema()));
        }

        let branching = relay_node
            .effective_branching
            .as_deref()
            .unwrap_or_default();
        if branching.is_empty() {
            return Ok(None);
        }
        let Some(schema) = domain_schedule.configured::<CreateSchema>(&relay_model.schema) else {
            return Err(format!(
                "stream '{}' references missing scheduled schema '{}'",
                relay.as_str(),
                relay_model.schema.as_str()
            ));
        };
        let mut fields = Vec::with_capacity(branching.len());
        for branch_field in branching {
            let Some(field) = schema
                .fields
                .iter()
                .find(|field| field.name == *branch_field)
            else {
                return Err(format!(
                    "stream '{}' inferred branch field '{}' from its branching, but the field is \
                     missing from schema '{}'",
                    relay.as_str(),
                    branch_field.as_str(),
                    schema.name.as_str()
                ));
            };
            fields.push(field.clone());
        }
        Ok(Some(
            runtime_schema::compile_schema(&nervix_models::CreateSchema {
                name: schema.name.clone(),
                fields,
            })
            .arrow_schema(),
        ))
    }

    async fn subscription_materialized_context(
        &self,
        domain: &DomainName,
    ) -> Result<
        (
            HashMap<RelayName, RuntimeMaterializedRelaySpec>,
            HashMap<RelayName, Option<ClusterNodeName>>,
        ),
        String,
    > {
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok((HashMap::default(), HashMap::default()));
        };

        let mut specs = HashMap::default();
        let mut owners = HashMap::default();
        for relay_node in domain_schedule
            .nodes
            .values()
            .filter(|node| node.kind() == ModelKind::Relay)
        {
            let Model::Relay(ack_model) = relay_node.config.as_ref() else {
                continue;
            };
            if ack_model.materialized_state.is_none() {
                continue;
            }
            let Some(schema_node) = domain_schedule.nodes.get(&NodeRef::new(
                ModelKind::Schema,
                ModelName::from(&ack_model.schema),
            )) else {
                return Err(format!(
                    "stream '{}' references missing scheduled schema '{}'",
                    ack_model.name.as_str(),
                    ack_model.schema.as_str()
                ));
            };
            let Model::Schema(schema) = schema_node.config.as_ref() else {
                return Err("scheduled schema node has invalid model kind".to_string());
            };
            let schema = runtime_schema::compile_schema(schema);
            specs.insert(
                ack_model.name.clone(),
                RuntimeMaterializedRelaySpec::new(
                    schema.arrow_schema(),
                    schema.vm_sensitivity(),
                    relay_node.effective_branching.clone().unwrap_or_default(),
                ),
            );
            owners.insert(ack_model.name.clone(), relay_node.primary_node().cloned());
        }

        Ok((specs, owners))
    }

    pub(in crate::application) async fn create_subscription(
        &self,
        domain: &DomainName,
        subscription: nervix_models::CreateSubscription,
        tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        if subscriptions.contains_name(&subscription.name) {
            return CommandResult {
                success: false,
                message: format!(
                    "session subscription '{}' already exists",
                    subscription.name
                ),
                diagnostics: vec![Diagnostic {
                    message: format!(
                        "session subscription '{}' already exists",
                        subscription.name
                    ),
                    span_start: 0,
                    span_end: 0,
                }],
                kind: i32::from(CommandResultKind::Error),
                ..Default::default()
            };
        }

        let batch_sample_rate =
            match parse_subscription_batch_sample_rate(subscription.batch_sample_rate.as_deref()) {
                Ok(rate) => rate,
                Err(err) => {
                    return CommandResult {
                        success: false,
                        message: format!(
                            "failed to subscribe session '{}': {err}",
                            subscription.name
                        ),
                        diagnostics: vec![Diagnostic {
                            message: err,
                            span_start: 0,
                            span_end: 0,
                        }],
                        kind: i32::from(CommandResultKind::Error),
                        ..Default::default()
                    };
                }
            };

        match self
            .inner
            .registry
            .contains(domain, ModelKind::Relay, &subscription.relay)
        {
            Ok(true) => {}
            Ok(false) => match self
                .subscription_target_from_schedule(domain, &subscription.relay)
                .await
            {
                Ok(Some(_)) => {}
                Ok(None) => {
                    return CommandResult {
                        success: false,
                        message: format!(
                            "stream '{}' does not exist in domain '{}'",
                            subscription.relay.as_str(),
                            domain.as_str()
                        ),
                        diagnostics: vec![Diagnostic {
                            message: format!("stream '{}' not found", subscription.relay.as_str()),
                            span_start: 0,
                            span_end: 0,
                        }],
                        kind: i32::from(CommandResultKind::Error),
                        ..Default::default()
                    };
                }
                Err(err) => {
                    return CommandResult {
                        success: false,
                        message: format!("failed to resolve relay for subscription: {err}"),
                        diagnostics: vec![Diagnostic {
                            message: format!("failed to resolve relay for subscription: {err}"),
                            span_start: 0,
                            span_end: 0,
                        }],
                        kind: i32::from(CommandResultKind::Error),
                        ..Default::default()
                    };
                }
            },
            Err(err) => {
                return CommandResult {
                    success: false,
                    message: format!("failed to resolve relay for subscription: {err}"),
                    diagnostics: vec![Diagnostic {
                        message: format!("failed to resolve relay for subscription: {err}"),
                        span_start: 0,
                        span_end: 0,
                    }],
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
        }

        let relay_target = self
            .subscription_target_from_schedule(domain, &subscription.relay)
            .await;
        let relay_branching = match relay_target {
            Ok(Some(target)) => target.branching,
            Ok(None) | Err(_) => Vec::new(),
        };
        let relay_branch_schema = match self
            .subscription_branch_schema(domain, &subscription.relay)
            .await
        {
            Ok(schema) => schema,
            Err(err) => {
                return CommandResult {
                    success: false,
                    message: format!(
                        "failed to resolve relay branch schema for subscription: {err}"
                    ),
                    diagnostics: vec![Diagnostic {
                        message: format!(
                            "failed to resolve relay branch schema for subscription: {err}"
                        ),
                        span_start: 0,
                        span_end: 0,
                    }],
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
        };
        let (materialized_stream_specs, materialized_stream_owner_nodes) =
            match self.subscription_materialized_context(domain).await {
                Ok(context) => context,
                Err(err) => {
                    return CommandResult {
                        success: false,
                        message: format!(
                            "failed to resolve materialized relays for subscription: {err}"
                        ),
                        diagnostics: vec![Diagnostic {
                            message: format!(
                                "failed to resolve materialized relays for subscription: {err}"
                            ),
                            span_start: 0,
                            span_end: 0,
                        }],
                        kind: i32::from(CommandResultKind::Error),
                        ..Default::default()
                    };
                }
            };
        let (filter_map, subscription_sensitivity) = match self
            .subscription_stream_schema(domain, &subscription.relay)
            .await
        {
            Ok(Some(schema)) => {
                let udfs = self.inner.runtime.udf_executor(domain);
                let schema = runtime_schema::compile_schema(&schema);
                let input_sensitivity = schema.vm_sensitivity();
                let filter_map = match compile_session_filter_map_program(
                    domain,
                    &subscription.relay,
                    subscription.where_clause.as_ref(),
                    schema.arrow_schema(),
                    input_sensitivity.clone(),
                    RuntimeVmCompileContext {
                        available_materialized_streams: &materialized_stream_specs,
                        available_lookups: &HashMap::default(),
                        current_branching: &relay_branching,
                        current_branch_schema: relay_branch_schema.as_ref(),
                        current_branch_sensitivity: None,
                        udfs: udfs.as_ref(),
                    },
                ) {
                    Ok(filter_map) => filter_map,
                    Err(err) => {
                        return CommandResult {
                            success: false,
                            message: format!(
                                "failed to compile session subscription '{}': {err}",
                                subscription.name
                            ),
                            diagnostics: vec![Diagnostic {
                                message: format!(
                                    "failed to compile session subscription '{}': {err}",
                                    subscription.name
                                ),
                                span_start: 0,
                                span_end: 0,
                            }],
                            kind: i32::from(CommandResultKind::Error),
                            ..Default::default()
                        };
                    }
                };
                let sensitivity = match filter_map.as_ref() {
                    Some(filter_map) => filter_map.output_sensitivity.clone(),
                    None => input_sensitivity,
                };
                (filter_map, sensitivity)
            }
            Ok(None) => {
                return CommandResult {
                    success: false,
                    message: format!(
                        "stream '{}' does not exist in domain '{}'",
                        subscription.relay.as_str(),
                        domain.as_str()
                    ),
                    diagnostics: vec![Diagnostic {
                        message: format!("stream '{}' not found", subscription.relay.as_str()),
                        span_start: 0,
                        span_end: 0,
                    }],
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
            Err(err) => {
                return CommandResult {
                    success: false,
                    message: format!("failed to resolve relay for subscription: {err}"),
                    diagnostics: vec![Diagnostic {
                        message: format!("failed to resolve relay for subscription: {err}"),
                        span_start: 0,
                        span_end: 0,
                    }],
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
        };

        let relay = subscription.relay.clone();
        let receiver = match self.inner.runtime.subscribe_stream(domain, &relay).await {
            Ok(receiver) => receiver,
            Err(err) => {
                return CommandResult {
                    success: false,
                    message: format!("failed to subscribe to relay '{}': {err}", relay.as_str()),
                    diagnostics: vec![Diagnostic {
                        message: format!(
                            "failed to subscribe to relay '{}': {err}",
                            relay.as_str()
                        ),
                        span_start: 0,
                        span_end: 0,
                    }],
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
        };

        if let Err(error) = self.register_subscription_interest(domain, &relay).await {
            return command_error(format!(
                "failed to register subscription interest for relay '{}' in domain '{}': {error}",
                relay.as_str(),
                domain.as_str(),
            ));
        }
        subscriptions.insert(
            subscription.name.clone(),
            domain.clone(),
            relay.clone(),
            SessionSubscriptionTaskConfig {
                filter_map,
                sensitivity: subscription_sensitivity,
                delivery_behavior: subscription.delivery_behavior,
                batch_sample_rate,
                runtime: self.inner.runtime.clone(),
                materialized_stream_owner_nodes,
                receiver,
                tx: tx.clone(),
            },
        );

        CommandResult {
            success: true,
            message: format!(
                "created subscription '{}' in domain '{}'",
                subscription.name,
                domain.as_str()
            ),
            diagnostics: Vec::new(),
            kind: i32::from(CommandResultKind::Ok),
            ..Default::default()
        }
    }

    pub(in crate::application) async fn delete_subscription(
        &self,
        subscription: nervix_models::DeleteSubscription,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        match subscriptions.remove(&subscription.name).await {
            Some((subscription_domain, relay)) => {
                if !subscriptions.contains_domain_stream(&subscription_domain, &relay.clone()) {
                    self.unregister_subscription_interest(&subscription_domain, &relay.clone())
                        .await;
                }
                CommandResult {
                    success: true,
                    message: format!(
                        "deleted subscription '{}' from domain '{}'",
                        subscription.name,
                        subscription_domain.as_str()
                    ),
                    diagnostics: Vec::new(),
                    kind: i32::from(CommandResultKind::Ok),
                    ..Default::default()
                }
            }
            None => CommandResult {
                success: false,
                message: format!(
                    "session subscription '{}' does not exist",
                    subscription.name
                ),
                diagnostics: vec![Diagnostic {
                    message: format!("session subscription '{}' not found", subscription.name),
                    span_start: 0,
                    span_end: 0,
                }],
                kind: i32::from(CommandResultKind::Error),
                ..Default::default()
            },
        }
    }
}

impl SessionServiceImpl {
    pub(in crate::application) async fn process_session_command_operations(
        &self,
        operations: Vec<SessionCommandOperation>,
        tx: &mpsc::Sender<Result<SessionResponse, Status>>,
        subscriptions: &mut SessionSubscriptions,
    ) -> CommandResult {
        let is_batch = operations.len() > 1;
        let mut results = Vec::new();
        let mut transaction = None;

        for operation in operations {
            tokio::task::consume_budget().await;
            let result = match operation {
                SessionCommandOperation::Begin { domain } => {
                    match self.resolve_transaction_domain(&domain).await {
                        Err(message) => command_error(message),
                        Ok(domain) => {
                            let id = uuid::Uuid::now_v7().to_string();
                            let transaction = ReplicatedTransaction::open(
                                id.clone(),
                                domain,
                                subscriptions.user.clone(),
                                current_timestamp(),
                            );
                            match self
                                .inner
                                .consensus
                                .open_transaction(transaction, self.inner.transaction_max_open)
                                .await
                            {
                                Ok(transaction) => {
                                    self.inner
                                        .transaction_bindings
                                        .insert(id.clone(), subscriptions.session_id.clone());
                                    subscriptions.bind_transaction(id.clone());
                                    let mut result =
                                        command_ok(format!("transaction started: id '{id}'"));
                                    result.transaction = Some(transaction_status(&transaction));
                                    result
                                }
                                Err(error) => {
                                    self.transaction_consensus_error_response(error).await
                                }
                            }
                        }
                    }
                }
                SessionCommandOperation::Queue(command) => {
                    self.queue_transaction_statement(command, subscriptions)
                        .await
                }
                SessionCommandOperation::Commit => {
                    self.commit_bound_transaction(tx, subscriptions).await
                }
                SessionCommandOperation::Revert => {
                    self.revert_bound_transaction(subscriptions).await
                }
                SessionCommandOperation::Execute(command) => {
                    self.process_pending_session_commands(vec![command], tx, subscriptions, false)
                        .await
                }
            };

            if result.transaction.is_some() {
                transaction.clone_from(&result.transaction);
            }
            if !result.success {
                let mut result = command_batch_result(results, result, is_batch);
                if result.transaction.is_none() {
                    result.transaction = transaction;
                }
                return result;
            }
            if !is_batch {
                return result;
            }
            append_command_result(&mut results, result);
        }

        if results.is_empty() {
            return command_error("empty command".to_string());
        }
        CommandResult {
            success: true,
            message: command_results_message(&results),
            diagnostics: Vec::new(),
            kind: i32::from(CommandResultKind::Ok),
            results,
            transaction,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;
    use nervix_models::{DomainName, SubscriptionDeliveryBehavior};
    use tokio::{sync::mpsc, time::Duration};

    use super::{
        super::test_fixtures::{named, string_branch_key},
        *,
    };
    use crate::{proto, proto::ServerEventLevel, runtime::Runtime};

    #[test]
    fn parse_subscription_literal_enforces_declared_types() {
        let field = named("created_at");
        assert!(matches!(
            parse_subscription_literal(
                &field,
                &ParseAsType::Datetime,
                &SubscriptionLiteral::String("2025-01-02T03:04:05+00:00".to_string())
            ),
            Ok(runtime_schema::RuntimeValue::Datetime(_))
        ));
        assert!(matches!(
            parse_subscription_literal(
                &named("active"),
                &ParseAsType::Bool,
                &SubscriptionLiteral::Bool(true)
            ),
            Ok(runtime_schema::RuntimeValue::Bool(true))
        ));
        let err = parse_subscription_literal(
            &named("user_id"),
            &ParseAsType::U32,
            &SubscriptionLiteral::String("42".to_string()),
        )
        .expect_err("string should not satisfy numeric field");
        assert!(err.contains("expects numeric literal"));
    }

    #[test]
    fn subscription_batch_sample_rate_is_validated() {
        assert_eq!(parse_subscription_batch_sample_rate(None), Ok(None));
        assert_eq!(
            parse_subscription_batch_sample_rate(Some("0.25")),
            Ok(Some(0.25))
        );
        assert!(parse_subscription_batch_sample_rate(Some("1.1")).is_err());
        assert!(parse_subscription_batch_sample_rate(Some("bad")).is_err());
    }

    #[test]
    fn subscription_sampling_respects_extreme_rates() {
        let message = RelayMessage {
            key: string_branch_key("tenant", "acme"),
            record: runtime_schema::test_runtime_row([]),
            acks: crate::runtime_ack::AckSet::empty(),
        };
        assert!(subscription_sample_passes(None, &message));
        assert!(subscription_sample_passes(Some(1.0), &message));
        assert!(!subscription_sample_passes(Some(0.0), &message));
    }

    #[tokio::test]
    async fn session_subscriptions_track_names_and_cleanup_tasks() {
        let mut subscriptions = SessionSubscriptions::new();
        let (tx, _rx) = mpsc::channel(4);
        let events = crate::runtime::RelayBroadcast::with_capacity(
            std::num::NonZeroUsize::new(4).expect("test relay capacity must be nonzero"),
        );
        let events_rx = events.new_receiver();
        subscriptions.insert(
            named("live_events"),
            DomainName::parse("default").expect("valid domain"),
            named("events"),
            SessionSubscriptionTaskConfig {
                filter_map: None,
                sensitivity: nervix_vm::SchemaSensitivity::default(),
                delivery_behavior: SubscriptionDeliveryBehavior::Blocking,
                batch_sample_rate: None,
                runtime: Runtime::default(),
                materialized_stream_owner_nodes: HashMap::default(),
                receiver: events_rx,
                tx,
            },
        );
        assert_eq!(
            subscriptions.matching_names("LIVE"),
            vec!["live_events".to_string()]
        );
        assert!(subscriptions.matching_names("missing").is_empty());

        let removed = subscriptions
            .remove(&named("live_events"))
            .await
            .expect("subscription should be removed");
        assert_eq!(removed.0.as_str(), "default");
        assert_eq!(removed.1.as_str(), "events");
        assert!(
            subscriptions
                .remove(&named("missing_events"))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn rebuilt_relay_drops_subscription_with_clear_session_error() {
        let mut subscriptions = SessionSubscriptions::new();
        let (tx, mut rx) = mpsc::channel(4);
        let events = crate::runtime::RelayBroadcast::with_capacity(
            std::num::NonZeroUsize::new(4).expect("test relay capacity must be nonzero"),
        );
        subscriptions.insert(
            named("live_events"),
            DomainName::parse("default").expect("valid domain"),
            named("events"),
            SessionSubscriptionTaskConfig {
                filter_map: None,
                sensitivity: nervix_vm::SchemaSensitivity::default(),
                delivery_behavior: SubscriptionDeliveryBehavior::Blocking,
                batch_sample_rate: None,
                runtime: Runtime::default(),
                materialized_stream_owner_nodes: HashMap::default(),
                receiver: events.new_receiver(),
                tx,
            },
        );

        drop(events);
        let response = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("subscription close error should arrive")
            .expect("session channel should remain open")
            .expect("session response should succeed");
        let Some(proto::session_response::Event::Server(event)) = response.event else {
            panic!("expected server error event");
        };
        assert_eq!(event.level, i32::from(ServerEventLevel::Error));
        assert!(
            event
                .message
                .contains("subscription 'live_events' was dropped")
        );
        assert!(event.message.contains("recreate the subscription"));
        tokio::task::yield_now().await;
        assert!(!subscriptions.contains_name(&named("live_events")));

        let _ = subscriptions
            .remove(&named("live_events"))
            .await
            .expect("closed subscription metadata should remain removable");
    }
}
