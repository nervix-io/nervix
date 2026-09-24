//! The command and completion pipeline every session request is served by, and the state one
//! Nervix server serves it from.
//!
//! Layer: edges.
//!
//! - **Owns.** The server's shared state, the session event bus, the command pipeline from a
//!   request's text to its typed result, and completion suggestions.
//! - **Depends on.** The control-plane use cases it dispatches to, and the parser for the text a
//!   client sends.
//! - **Must not know.** How a transport frames, correlates or delivers what the pipeline returns.

use std::sync::Arc as StdArc;

use ahash::RandomState;
use futures_util::future::BoxFuture;
use nervix_client_wire::{
    CommandRequest, NoticeLevel, ServerNotice, SuggestOutcome, SuggestRequest, Suggestion,
    SuggestionKind,
};
use nervix_consensus::{Administrator, CommandExecutionTransactionTarget, Observer, Proposer};
use nervix_execution::sync::DashMap;
use nervix_interconnect::Transport;
use nervix_models::{
    CommandExecutionReference, DomainName, ModelKind, ModelName, ResourceId, ResourceName,
    ResourceUploadKey, TransactionPosition,
};
use nervix_nspl::{
    Token, Word,
    client_statement::{
        ClientStatement, parse_client_statement_sources, suggest_client_statement,
        upload_resource_path_fragment,
    },
    lex,
    schema::{Diagnostic as ParseDiagnostic, ParseFromSourceError},
};
use nervix_recovery::Discarded;
use sorted_vec::SortedSet;
use tokio::{
    sync::{Mutex as AsyncMutex, broadcast},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use triomphe::Arc;

use super::{
    authentication::{AuthRateLimiter, BasicAuthCredentials},
    command_execution::{
        CommandAdmission, CommandExecutionOwners, CommandExecutionPolicy, PersistentCommandRequest,
    },
    command_result::{
        CommandDiagnostic, CommandDisposition, CommandOrigin, CommandResponse, CommandResult,
    },
    completion::{ApplicationRevisionPhase, wait_for_application_revision},
    describe_output::placement_runtime_node_ref_suggestions,
    model_mutation::command_error,
    resource::{
        completed_resource_version_suggestions, resource_named_before_version,
        resource_ref_suggestions, resource_version_suggestions,
    },
    runtime_admission::RuntimeAdmission,
    scheduling::RUNTIME_REVISION_READINESS_PROPAGATION_BOUND,
    service_tasks::ServiceTasks,
    session::admission::{CancelledBeforeAdmission, RequestAdmission},
    subscription::{
        SessionCommandOperation, SessionSubscriptions, SessionView, SubscriptionInterests,
    },
    tls::HttpsListenerCertificates,
    transaction::TransactionRecovery,
};
use crate::{
    cluster,
    registry::{Registry, RegistryError},
    resource::ResourceStore,
    runtime::Runtime,
};

/// How many events a session can fall behind before the bus drops the oldest.
pub(in crate::application) const SESSION_EVENT_CAPACITY: usize = 256;

/// The session event bus, and the one way a control-plane failure or a cluster transition reaches
/// the sessions attached to this node.
///
/// Unlike the runtime event bus this one carries no fan-out task, so its only subscribers are live
/// sessions, and a node serving none is the ordinary case rather than a startup window. An event
/// that finds no receiver is therefore expected, which is why publishing goes through these
/// methods: each one leaves a record that does not depend on anyone listening, and the send that
/// follows only offers the same fact to whoever is.
#[derive(Clone)]
pub(in crate::application) struct SessionEvents {
    sender: broadcast::Sender<ServerNotice>,
}

impl SessionEvents {
    pub(in crate::application) fn new(capacity: usize) -> Self {
        Self {
            sender: broadcast::channel(capacity).0,
        }
    }

    /// Report a control-plane failure this node recovered from.
    fn report_error(&self, message: impl Into<String>) {
        let message = message.into();
        warn!(error = %message, "server error reported to sessions");
        self.publish(NoticeLevel::Error, message);
    }

    /// Offer a transition the cluster or consensus bus has already recorded.
    ///
    /// Those buses write their own `info` line before handing the text here, so this is a relay
    /// rather than a report and it logs nothing of its own.
    pub(in crate::application) fn relay_info(&self, message: String) {
        self.publish(NoticeLevel::Info, message);
    }

    fn publish(&self, level: NoticeLevel, message: String) {
        self.sender
            .send(ServerNotice { level, message })
            .discarded("the record this event carries is written before it is offered");
    }

    pub(in crate::application) fn subscribe(&self) -> broadcast::Receiver<ServerNotice> {
        self.sender.subscribe()
    }
}

/// The handle every session, background reconciliation task, and HTTP server clones. It is one
/// `Arc` over the server's state, so handing the service to a spawned task costs a single refcount
/// rather than one per piece of state the server owns.
#[derive(Clone)]
pub struct SessionServiceImpl {
    pub(in crate::application) inner: Arc<SessionServiceInner>,
}

/// Everything one Nervix server owns for as long as it serves. These fields are reached only
/// through a `SessionServiceImpl` handle and therefore hold their values directly. The ones that
/// keep an `Arc` of their own have a second owner outside the service, and each names it.
pub(in crate::application) struct SessionServiceInner {
    /// Started and shut down by the application, which outlives the service handle.
    pub(in crate::application) cluster: Arc<cluster::ClusterHandle>,
    /// Proposal authority and local observation; leadership is checked for each operation.
    pub(in crate::application) consensus: Proposer,
    /// Membership changes requested by authenticated cluster commands.
    pub(in crate::application) consensus_administrator: Administrator,
    /// Also held by the application and by the registry reconciliation tasks it spawns.
    pub(in crate::application) registry: Arc<Registry>,
    /// Also held by the application startup that opened it, and published by the runtime.
    pub(in crate::application) resource_store: StdArc<ResourceStore>,
    /// Also held by the HTTPS server, which reads the current certificate on every accept, and by
    /// the schedule task that installs every admitted revision.
    pub(in crate::application) https_certificates: HttpsListenerCertificates,
    pub(in crate::application) runtime: Runtime,
    /// Also held by the initial schedule reconciliation task until process shutdown.
    pub(in crate::application) runtime_admission: Arc<RuntimeAdmission>,
    pub(in crate::application) replica_count: usize,
    /// Stops external sessions after this node has advertised termination.
    pub(in crate::application) admission_shutdown: CancellationToken,
    /// Keeps internal coordination available until admitted work has drained.
    pub(in crate::application) drain_support_shutdown: CancellationToken,
    pub(in crate::application) events: SessionEvents,
    /// Also held by every subscription delivery, whose interest lease releases into it.
    pub(in crate::application) subscription_interests: SubscriptionInterests,
    pub(in crate::application) interconnect: Transport,
    pub(in crate::application) service_tasks: ServiceTasks,
    pub(in crate::application) configured_basic_auth: Option<BasicAuthCredentials>,
    pub(in crate::application) auth_rate_limiter: AuthRateLimiter,
    pub(in crate::application) failed_auth_rate_limit_keys: DashMap<String, (), RandomState>,
    pub(in crate::application) transaction_idle_timeout: Duration,
    pub(in crate::application) transaction_tombstone_retention: Duration,
    pub(in crate::application) transaction_max_statements: usize,
    pub(in crate::application) transaction_max_source_bytes: u64,
    pub(in crate::application) transaction_max_open: usize,
    pub(in crate::application) transaction_bindings: DashMap<String, String, RandomState>,
    /// Retry validity and history capacity every durable command admission applies.
    pub(in crate::application) command_execution_policy: CommandExecutionPolicy,
    /// Requests with one durable execution reference join one application owner on this leader.
    pub(in crate::application) command_executions: CommandExecutionOwners,
    /// Calls adopting the same replicated transaction share one executor without serializing
    /// commits in independent domains.
    pub(in crate::application) transaction_executions:
        DashMap<String, StdArc<AsyncMutex<()>>, RandomState>,
    /// Bounded fair admission for leader-side recovery of durable COMMITTING work.
    pub(in crate::application) transaction_recovery: TransactionRecovery,
    /// Serializes destination preparation with authority reconciliation so a request from a
    /// superseded leader cannot race a current leader's preparation into the runtime.
    pub(in crate::application) ownership_handoff_operations: AsyncMutex<()>,
    /// Also held by a request while it installs. Calls with one durable identity share the lock,
    /// so only one of them can build and publish that assigned version on this leader.
    pub(in crate::application) resource_upload_executions:
        DashMap<ResourceUploadKey, StdArc<AsyncMutex<()>>, RandomState>,
    /// A reconciliation request holds this lock through download, verification and promotion.
    /// Repeated observations of the same missing version join that one installation.
    pub(in crate::application) resource_replication_executions:
        DashMap<ResourceId, StdArc<AsyncMutex<()>>, RandomState>,
}

/// The node-local handles that install the newest admitted runtime state.
#[derive(Clone, Copy)]
pub(in crate::application) struct RuntimeStateApplication<'a> {
    pub(in crate::application) runtime: &'a Runtime,
    pub(in crate::application) https_certificates: &'a HttpsListenerCertificates,
    pub(in crate::application) cluster: &'a cluster::ClusterHandle,
    pub(in crate::application) interconnect: &'a Transport,
    pub(in crate::application) registry: &'a Registry,
    pub(in crate::application) consensus: &'a Observer,
    pub(in crate::application) admission: &'a RuntimeAdmission,
    pub(in crate::application) shutdown: &'a CancellationToken,
}

/// Apply the newest admitted runtime state with a heap-backed future so command execution keeps a
/// bounded stack regardless of the preparation and readiness paths active inside this operation.
pub(in crate::application) fn apply_current_cluster_runtime_state(
    application: RuntimeStateApplication<'_>,
) -> BoxFuture<'_, Result<(), crate::runtime::RuntimeError>> {
    let RuntimeStateApplication {
        runtime,
        https_certificates,
        cluster,
        interconnect,
        registry,
        consensus,
        admission,
        shutdown,
    } = application;
    Box::pin(async move {
        let local_node_id = consensus.local_node_id();
        loop {
            tokio::task::consume_budget().await;
            let Some(installation) = admission.begin_installation(shutdown).await else {
                return Ok(());
            };
            let Some(state) = admission.runtime_state(consensus, shutdown).await else {
                return Ok(());
            };
            debug!(
                %local_node_id,
                revision = state.revision,
                "installing admitted cluster runtime state"
            );
            if let Err(error) = registry.synchronize_cluster_schedule(&state.schedule) {
                warn!(error = %error, "failed to synchronize registry from admitted cluster schedule");
            }
            let runtime_application = runtime
                .apply_cluster_state(
                    local_node_id,
                    state.revision,
                    &state.domains,
                    &state.domain_clock_authorities,
                    &state.schedule,
                )
                .await;
            // The listener presents the certificates of the same revision before this node reports
            // it prepared. A failed installation keeps the certificates already presented and is
            // reported to the command that changed them through its installation barrier.
            if let Err(error) = https_certificates
                .install(state.revision, &state.schedule)
                .await
            {
                warn!(
                    %local_node_id,
                    revision = state.revision,
                    error = format!("{error:#}"),
                    "failed to install the HTTPS listener TLS configuration"
                );
            }
            runtime_application?;
            cluster
                .set_local_runtime_revision_prepared(state.revision)
                .await;
            debug!(
                %local_node_id,
                revision = state.revision,
                "local runtime revision prepared"
            );
            drop(installation);

            let node_unavailability_timeout = cluster.node_unavailability_timeout();
            // Applying a revision gets one start-time operation budget: peer-failure detection followed
            // by readiness propagation. A peer that disconnects after this starts has only the remaining
            // portion of that budget.
            let Some(readiness_timeout) = node_unavailability_timeout
                .checked_add(RUNTIME_REVISION_READINESS_PROPAGATION_BOUND)
            else {
                return Err(
                    crate::runtime::RuntimeError::RuntimeRevisionReadinessDeadlineOverflow {
                        node_unavailability_timeout,
                        readiness_propagation_bound: RUNTIME_REVISION_READINESS_PROPAGATION_BOUND,
                    },
                );
            };
            let Some(deadline) = tokio::time::Instant::now().checked_add(readiness_timeout) else {
                return Err(
                    crate::runtime::RuntimeError::RuntimeRevisionReadinessDeadlineOverflow {
                        node_unavailability_timeout,
                        readiness_propagation_bound: RUNTIME_REVISION_READINESS_PROPAGATION_BOUND,
                    },
                );
            };
            let preparation = wait_for_application_revision(
                cluster,
                interconnect,
                state.revision,
                ApplicationRevisionPhase::RuntimePrepared,
                deadline,
            );
            tokio::pin!(preparation);
            let supersession = consensus.wait_for_runtime_revision_after(state.revision);
            tokio::pin!(supersession);
            let preparation_result = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(()),
                newer_revision = &mut supersession => {
                    let Some(newer_revision) = newer_revision else {
                        return Ok(());
                    };
                    debug!(
                        %local_node_id,
                        revision = state.revision,
                        newer_revision,
                        "superseding runtime revision during cluster preparation"
                    );
                    continue;
                }
                result = &mut preparation => result,
            };
            if let Err(timeout) = preparation_result {
                let current_revision = consensus.current_runtime_revision().await;
                if current_revision > state.revision {
                    debug!(
                        %local_node_id,
                        revision = state.revision,
                        newer_revision = current_revision,
                        "superseding runtime revision at cluster preparation deadline"
                    );
                    continue;
                }
                return Err(crate::runtime::RuntimeError::RuntimeRevisionPreparation {
                    revision: state.revision,
                    pending_nodes: timeout.pending_nodes,
                });
            }
            debug!(
                %local_node_id,
                revision = state.revision,
                "cluster runtime revision prepared"
            );

            let Some(activation) = admission.begin_installation(shutdown).await else {
                return Ok(());
            };
            let current_revision = consensus.current_runtime_revision().await;
            if current_revision > state.revision {
                debug!(
                    %local_node_id,
                    revision = state.revision,
                    newer_revision = current_revision,
                    "superseding runtime revision before source activation"
                );
                continue;
            }
            runtime.start_running_domain_ingestors().await?;
            debug!(
                %local_node_id,
                revision = state.revision,
                "started running-domain ingestors"
            );
            cluster
                .set_local_runtime_revision_ready(state.revision)
                .await;
            drop(activation);
            let readiness = wait_for_application_revision(
                cluster,
                interconnect,
                state.revision,
                ApplicationRevisionPhase::RuntimeReady,
                deadline,
            );
            tokio::pin!(readiness);
            let supersession = consensus.wait_for_runtime_revision_after(state.revision);
            tokio::pin!(supersession);
            let readiness_result = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(()),
                newer_revision = &mut supersession => {
                    let Some(newer_revision) = newer_revision else {
                        return Ok(());
                    };
                    debug!(
                        %local_node_id,
                        revision = state.revision,
                        newer_revision,
                        "superseding runtime revision during cluster readiness"
                    );
                    continue;
                }
                result = &mut readiness => result,
            };
            if let Err(timeout) = readiness_result {
                let current_revision = consensus.current_runtime_revision().await;
                if current_revision > state.revision {
                    debug!(
                        %local_node_id,
                        revision = state.revision,
                        newer_revision = current_revision,
                        "superseding runtime revision at cluster readiness deadline"
                    );
                    continue;
                }
                return Err(crate::runtime::RuntimeError::RuntimeRevisionReadiness {
                    revision: state.revision,
                    pending_nodes: timeout.pending_nodes,
                });
            }
            debug!(
                %local_node_id,
                revision = state.revision,
                "cluster runtime revision ready"
            );
            return Ok(());
        }
    })
}

pub(in crate::application) fn error_response(
    kind: &str,
    diagnostics: &[ParseDiagnostic],
) -> CommandResult {
    CommandResult {
        diagnostics: diagnostics.iter().map(map_diagnostic).collect(),
        ..CommandResult::new(CommandDisposition::Failed, kind.to_string())
    }
}

/// A failed result whose one diagnostic repeats its message at `span`.
fn failed_at(message: String, span: Option<std::ops::Range<usize>>) -> CommandResult {
    let diagnostic = CommandDiagnostic {
        message: message.clone(),
        span,
    };
    CommandResult {
        diagnostics: vec![diagnostic],
        ..CommandResult::new(CommandDisposition::Failed, message)
    }
}

pub(in crate::application) fn create_registry_error_response(
    query: &str,
    domain: &DomainName,
    model_id: &ModelName,
    err: &error_stack::Report<RegistryError>,
) -> CommandResult {
    match err.current_context() {
        RegistryError::AlreadyExists { .. } => {
            let diagnostic = CommandDiagnostic {
                message: format!("'{}' already exists", model_id.as_str()),
                span: find_identifier_span(query, model_id),
            };
            let message = format!(
                "{} '{}' already exists in domain '{}'",
                infer_kind_from_error_target(err, model_id).unwrap_or("model"),
                model_id.as_str(),
                domain.as_str()
            );
            CommandResult {
                diagnostics: vec![diagnostic],
                ..CommandResult::new(CommandDisposition::Failed, message)
            }
        }
        RegistryError::NotFound { .. }
        | RegistryError::StoredModelKindMismatch { .. }
        | RegistryError::DeleteInUse { .. }
        | RegistryError::InvalidModel { .. } => {
            failed_at(format!("{err}"), find_identifier_span(query, model_id))
        }
        RegistryError::MissingReference { reference, .. } => {
            // A reference that is not a model name has nothing to underline in the query, and a
            // diagnostic without a span is still the diagnostic the operator needs.
            let span = match ModelName::try_from(reference.as_str()) {
                Ok(id) => find_identifier_span(query, &id),
                Err(_) => None,
            };
            failed_at(format!("{err}"), span)
        }
        _ => failed_at(format!("{err}"), None),
    }
}

fn infer_kind_from_error_target(
    err: &error_stack::Report<RegistryError>,
    model_id: &ModelName,
) -> Option<&'static str> {
    match err.current_context() {
        RegistryError::AlreadyExists { identifier, .. } if identifier == model_id.as_str() => {
            Some("model")
        }
        _ => None,
    }
}

fn map_diagnostic(d: &ParseDiagnostic) -> CommandDiagnostic {
    CommandDiagnostic {
        message: d.message.clone(),
        span: Some(d.span.clone()),
    }
}

/// Where `identifier` appears in `query`, for a diagnostic that wants to underline it.
///
/// The query reached here because it failed validation, so it may also fail to lex. A diagnostic
/// without a span is still a diagnostic, which is why absence is the answer rather than an error.
pub(in crate::application) fn find_identifier_span(
    query: &str,
    identifier: &ModelName,
) -> Option<std::ops::Range<usize>> {
    let tokens = lex(query).ok()?;
    tokens.into_iter().find_map(|spanned| match spanned.token {
        Token::Word(Word::KnownWord { raw, .. }) | Token::Word(Word::UnknownWord(raw))
            if raw.eq_ignore_ascii_case(identifier.as_str()) =>
        {
            Some(spanned.span.into_range())
        }
        _ => None,
    })
}

pub(in crate::application) fn current_word_prefix(input: &str, cursor: usize) -> String {
    let end = cursor.min(input.len());
    let mut out = String::new();
    for ch in input[..end].chars().rev() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.insert(0, ch.to_ascii_lowercase());
        } else {
            break;
        }
    }
    out
}

/// Completion input split at the cursor: the source the grammar parses with the half-typed word
/// removed, where that word started, and the word itself for filtering the offers.
struct CompletionContext {
    grammar_input: String,
    grammar_cursor: usize,
    prefix: String,
}

fn completion_context(input: &str, cursor: usize) -> CompletionContext {
    let safe_cursor = cursor.min(input.len());
    let start = word_start(input, safe_cursor);
    let prefix = current_word_prefix(input, safe_cursor);

    let mut grammar_input = String::with_capacity(input.len() - (safe_cursor - start));
    grammar_input.push_str(&input[..start]);
    grammar_input.push_str(&input[safe_cursor..]);

    CompletionContext {
        grammar_input,
        grammar_cursor: start,
        prefix,
    }
}

pub(in crate::application) fn word_start(input: &str, cursor: usize) -> usize {
    let is_word = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let boundary = input[..cursor.min(input.len())]
        .char_indices()
        .rev()
        .find(|(_, c)| !is_word(*c));
    match boundary {
        Some((index, character)) => index + character.len_utf8(),
        None => 0,
    }
}

fn rebind_resource_before_for(input: &str, cursor: usize) -> Option<ResourceName> {
    let prefix = input.get(..cursor.min(input.len()))?;
    let words = prefix.split_whitespace().collect::<Vec<_>>();
    let rebind_index = words.windows(2).rposition(|pair| {
        pair[0].eq_ignore_ascii_case("REBIND") && pair[1].eq_ignore_ascii_case("RESOURCE")
    })?;
    let rebind_words = words.get(rebind_index..)?;
    let resource = rebind_words.get(2)?;
    let version_index = rebind_words.windows(2).position(|pair| {
        pair[0].eq_ignore_ascii_case("TO") && pair[1].eq_ignore_ascii_case("VERSION")
    })?;
    let after_version = rebind_words.get(version_index.checked_add(2)?..)?;
    let for_index = after_version
        .iter()
        .position(|word| word.eq_ignore_ascii_case("FOR"))?;
    if for_index == 0 {
        return None;
    }
    ResourceName::parse(resource).ok()
}

impl SessionServiceImpl {
    pub(in crate::application) async fn apply_current_cluster_state(
        &self,
    ) -> Result<(), crate::runtime::RuntimeError> {
        apply_current_cluster_runtime_state(RuntimeStateApplication {
            runtime: &self.inner.runtime,
            https_certificates: &self.inner.https_certificates,
            cluster: &self.inner.cluster,
            interconnect: &self.inner.interconnect,
            registry: &self.inner.registry,
            consensus: &self.inner.consensus,
            admission: &self.inner.runtime_admission,
            shutdown: &self.inner.drain_support_shutdown,
        })
        .await
    }

    /// Report a control-plane failure this node recovered from to the sessions attached to it.
    pub(in crate::application) fn broadcast_error(&self, message: impl Into<String>) {
        self.inner.events.report_error(message);
    }

    /// The completions at the request's cursor, read against the session as `session` last left
    /// it. The request's cursor is a byte offset on a character boundary of its input, which the
    /// request type guarantees.
    pub(in crate::application) async fn process_suggest(
        &self,
        req: SuggestRequest,
        session: &SessionView,
    ) -> SuggestOutcome {
        let cursor = req.cursor();
        let domain = req.domain().cloned();
        let queued = self
            .queued_configuration(session.binding(), domain.as_ref())
            .await;

        let CompletionContext {
            grammar_input,
            grammar_cursor,
            prefix,
        } = completion_context(req.input(), cursor);
        let grammar = suggest_client_statement(&grammar_input, grammar_cursor);

        let mut suggestions = Vec::new();
        let mut semantic_kinds = Vec::new();
        let mut expects_resource_ref = false;
        let mut expects_session_subscription_ref = false;
        let mut expects_runtime_node_ref = false;
        // `DESCRIBE RESOURCE` may name any published version, while a binding may name only a
        // completed one.
        let mut expects_resource_version = false;
        let mut expects_completed_resource_version = false;
        for item in &grammar {
            if let Some(kind) = ModelKind::from_completion_label(item) {
                semantic_kinds.push(kind);
            } else if item == "ref:resource" {
                expects_resource_ref = true;
            } else if item == "ref:session_subscription" {
                expects_session_subscription_ref = true;
            } else if item == "ref:runtime_node" {
                expects_runtime_node_ref = true;
            } else {
                if item == "resource_version" {
                    expects_resource_version = true;
                } else if item == "completed_resource_version" {
                    expects_completed_resource_version = true;
                }
                if prefix.is_empty()
                    || item
                        .to_ascii_lowercase()
                        .starts_with(&prefix.to_ascii_lowercase())
                {
                    suggestions.push(item.clone());
                }
            }
        }
        let expects_version = expects_resource_version || expects_completed_resource_version;
        let rebind_resource = rebind_resource_before_for(&grammar_input, grammar_cursor);

        for kind in &semantic_kinds {
            if let Some(domain) = &domain
                && self.inner.consensus.current_domain(domain).await.is_some()
            {
                if let Some(resource) = &rebind_resource {
                    if let Ok(models) = self.inner.registry.resulting_models(domain, &queued.models)
                    {
                        suggestions.extend(models.into_iter().filter_map(|model| {
                            if model.kind() == *kind
                                && model.binds_resource(resource)
                                && model.name().as_str().starts_with(&prefix)
                            {
                                Some(model.name().to_string())
                            } else {
                                None
                            }
                        }));
                    }
                } else if let Ok(ids) = self.inner.registry.resulting_identifiers(
                    domain,
                    *kind,
                    &prefix,
                    &queued.models,
                ) {
                    suggestions.extend(ids.into_iter().map(|id| id.to_string()));
                }
            }
        }

        if expects_session_subscription_ref {
            suggestions.extend(session.matching_subscription_names(&prefix));
        }

        if expects_runtime_node_ref
            && let Some(domain) = &domain
            && self.inner.consensus.current_domain(domain).await.is_some()
        {
            suggestions.extend(placement_runtime_node_ref_suggestions(
                &self.inner.registry,
                domain,
                &prefix,
                &queued.models,
            ));
        }

        if let Some(domain) = &domain
            && (expects_resource_ref || expects_version)
        {
            let resources = self.inner.consensus.current_resources().await;
            if expects_resource_ref {
                suggestions.extend(resource_ref_suggestions(&resources, domain, &prefix));
                suggestions.extend(queued.resource_suggestions(&prefix));
            }
            if let Some(resource) = resource_named_before_version(&grammar_input, grammar_cursor) {
                if expects_resource_version {
                    suggestions.extend(resource_version_suggestions(
                        &resources, domain, &resource, &prefix,
                    ));
                }
                if expects_completed_resource_version {
                    suggestions.extend(completed_resource_version_suggestions(
                        &resources, domain, &resource, &prefix,
                    ));
                }
            }
        }

        if grammar_input.contains("DOMAIN")
            || (semantic_kinds.is_empty()
                && !expects_resource_ref
                && !expects_session_subscription_ref
                && !expects_runtime_node_ref
                && !expects_version)
        {
            let domains = self.inner.consensus.current_domains().await;
            for id in domains.into_keys() {
                if prefix.is_empty() || id.as_str().starts_with(&prefix) {
                    suggestions.push(id.to_string());
                }
            }
        }

        let mut response_suggestions = SortedSet::from_unsorted(suggestions)
            .into_vec()
            .into_iter()
            .map(|value| Suggestion {
                value,
                kind: SuggestionKind::Text,
            })
            .collect::<Vec<_>>();

        if let Some(fragment) = upload_resource_path_fragment(req.input(), cursor) {
            response_suggestions.push(Suggestion {
                value: fragment.to_string(),
                kind: SuggestionKind::LocalDirectoryLookup,
            });
        }

        SuggestOutcome {
            suggestions: response_suggestions,
        }
    }

    /// Serves one command request of a session, from its text to its typed result.
    ///
    /// `admission` is decided immediately before the command's first effect: a request cancelled
    /// before that point returns [`CancelledBeforeAdmission`] having changed nothing.
    pub(in crate::application) async fn process_command(
        &self,
        req: CommandRequest,
        subscriptions: &mut SessionSubscriptions,
        admission: &RequestAdmission,
    ) -> Result<CommandResponse, CancelledBeforeAdmission> {
        let response =
            Box::pin(self.process_command_with_reference(req, subscriptions, admission)).await?;
        #[cfg(feature = "testing")]
        self.inner
            .runtime
            .pause_command_response_delivery_if_armed(self.inner.consensus.local_node_id())
            .await;
        Ok(response)
    }

    async fn process_command_with_reference(
        &self,
        req: CommandRequest,
        subscriptions: &mut SessionSubscriptions,
        admission: &RequestAdmission,
    ) -> Result<CommandResponse, CancelledBeforeAdmission> {
        let execution_reference = &req.execution_reference;
        let expected_transaction_position = req
            .expected_transaction_position
            .map(TransactionPosition::accepted_operations);
        let client_statements = match parse_client_statement_sources(&req.query) {
            Ok(statements) => statements,
            Err(ParseFromSourceError::Lex { diagnostics, .. }) => {
                let result = self
                    .command_with_transaction_status(
                        error_response("lex error", &diagnostics),
                        subscriptions,
                    )
                    .await;
                return Ok(CommandResponse::executed(result));
            }
            Err(ParseFromSourceError::Parse { diagnostics, .. }) => {
                let result = self
                    .command_with_transaction_status(
                        error_response("parse error", &diagnostics),
                        subscriptions,
                    )
                    .await;
                return Ok(CommandResponse::executed(result));
            }
        };

        // A request that only inspects a transaction reads it rather than changing it, so it
        // takes none of the durable admission, replay and queue-position fencing a transaction
        // request needs, even while the session has one attached.
        let inspects_transaction_only = matches!(
            client_statements.as_slice(),
            [parsed] if parsed.statement.inspects_transaction()
        );
        let is_transaction_request = !inspects_transaction_only
            && (expected_transaction_position.is_some()
                || subscriptions.transaction_active()
                || client_statements.iter().any(|parsed| {
                    matches!(
                        parsed.statement,
                        ClientStatement::BeginTransaction
                            | ClientStatement::CommitTransaction
                            | ClientStatement::RevertTransaction
                    )
                }));
        let mut execution_guard = None;
        if is_transaction_request {
            let leader = self.inner.consensus.current_leader().await;
            if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
                let result = self.not_leader_response(&req.query, leader).await;
                let result = self
                    .command_with_transaction_status(result, subscriptions)
                    .await;
                return Ok(CommandResponse::executed(result));
            }
            execution_guard = Some(
                self.inner
                    .command_executions
                    .lock(execution_reference.clone())
                    .await,
            );
            if let Some(execution) = self
                .inner
                .consensus
                .current_command_execution(execution_reference)
                .await
            {
                if execution.is_expired() {
                    return Ok(CommandResponse::executed(expired_reference(
                        execution_reference,
                    )));
                }
                let digest = match PersistentCommandRequest::transaction_digest(&req.query) {
                    Ok(digest) => digest,
                    Err(error) => {
                        return Ok(CommandResponse::executed(command_error(error.to_string())));
                    }
                };
                let expected_position = expected_transaction_position.map(TransactionPosition::new);
                if let Some(conflict) = execution.request_conflict(
                    &subscriptions.user,
                    req.domain.as_ref(),
                    expected_position,
                    digest,
                ) {
                    return Ok(CommandResponse::executed(conflicting_reference(
                        execution_reference,
                        conflict,
                    )));
                }
                let Some(target) = execution.transaction_target() else {
                    return Ok(CommandResponse::executed(conflicting_reference(
                        execution_reference,
                        nervix_consensus::CommandExecutionRequestConflict::Position,
                    )));
                };
                if let Some(bound) = subscriptions.transaction_id()
                    && bound != target.id()
                {
                    return Ok(CommandResponse::executed(conflicting_reference(
                        execution_reference,
                        nervix_consensus::CommandExecutionRequestConflict::Position,
                    )));
                }
                admission.admit()?;
                let result = self
                    .complete_persistent_command_request(execution, subscriptions)
                    .await;
                return Ok(CommandResponse {
                    result,
                    origin: CommandOrigin::Recovered,
                });
            }
            #[cfg(feature = "testing")]
            self.inner
                .runtime
                .pause_command_admission_if_armed(self.inner.consensus.local_node_id())
                .await;
            self.drop_transaction_bindings_if_armed();
            if subscriptions.transaction_active()
                && let Err(error) =
                    self.validate_session_transaction_binding(subscriptions.binding())
            {
                let result = self
                    .command_with_transaction_status(error.into_command_result(), subscriptions)
                    .await;
                return Ok(CommandResponse::executed(result));
            }
        }

        let operations = match subscriptions.plan_commands(
            client_statements,
            &req.query,
            req.domain.as_ref(),
            execution_reference,
            expected_transaction_position,
            req.expected_preview.clone(),
        ) {
            Ok(operations) => operations,
            Err(error) => {
                let result = self
                    .command_with_transaction_status(command_error(error), subscriptions)
                    .await;
                return Ok(CommandResponse::executed(result));
            }
        };

        let persistent_request = if is_transaction_request {
            let domain = match self.resolve_transaction_domain(req.domain.as_ref()).await {
                Ok(domain) => domain,
                Err(error) => return Ok(CommandResponse::executed(command_error(error))),
            };
            let target = if matches!(
                operations.first(),
                Some(SessionCommandOperation::Begin { .. })
            ) {
                CommandExecutionTransactionTarget::New {
                    id: uuid::Uuid::now_v7().to_string(),
                    activity: self.transaction_activity(),
                }
            } else {
                let Some(id) = subscriptions.transaction_id() else {
                    return Ok(CommandResponse::executed(command_error(
                        "transaction request has no durable transaction target".to_string(),
                    )));
                };
                CommandExecutionTransactionTarget::Existing {
                    id: id.to_string(),
                    activity: self.transaction_activity(),
                }
            };
            match PersistentCommandRequest::transaction(
                &operations,
                domain,
                &req.query,
                expected_transaction_position,
                target,
            ) {
                Ok(request) => Some(request),
                Err(error) => {
                    return Ok(CommandResponse::executed(command_error(error.to_string())));
                }
            }
        } else {
            match PersistentCommandRequest::from_operations(&operations, req.domain.as_ref()) {
                Ok(request) => request,
                Err(error) => {
                    return Ok(CommandResponse::executed(command_error(error.to_string())));
                }
            }
        };
        let mut persistent_execution = None;
        if let Some(request) = &persistent_request {
            let leader = self.inner.consensus.current_leader().await;
            if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
                let result = self.not_leader_response(&req.query, leader).await;
                return Ok(CommandResponse::executed(result));
            }
            #[cfg(feature = "testing")]
            if !is_transaction_request {
                self.inner
                    .runtime
                    .pause_command_admission_if_armed(self.inner.consensus.local_node_id())
                    .await;
            }
            let leader = self.inner.consensus.current_leader().await;
            if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
                let result = self.not_leader_response(&req.query, leader).await;
                return Ok(CommandResponse::executed(result));
            }
            if execution_guard.is_none() {
                execution_guard = Some(
                    self.inner
                        .command_executions
                        .lock(execution_reference.clone())
                        .await,
                );
            }
            admission.admit()?;
            let admitted = self
                .admit_persistent_command(
                    execution_reference.clone(),
                    subscriptions.user.clone(),
                    request,
                )
                .await;
            let admitted = match admitted {
                Ok(admitted) => admitted,
                Err(result) => return Ok(CommandResponse::executed(*result)),
            };
            #[cfg(feature = "testing")]
            self.inner
                .runtime
                .pause_command_after_durable_admission_if_armed(
                    self.inner.consensus.local_node_id(),
                )
                .await;
            persistent_execution = Some(admitted);
        }

        let response = match persistent_execution {
            Some(CommandAdmission::Admitted(execution)) => CommandResponse::executed(
                self.complete_persistent_command_request(execution, subscriptions)
                    .await,
            ),
            Some(CommandAdmission::Existing(execution)) => CommandResponse {
                result: self
                    .complete_persistent_command_request(execution, subscriptions)
                    .await,
                origin: CommandOrigin::Recovered,
            },
            None => {
                admission.admit()?;
                let result =
                    Box::pin(self.process_session_command_operations(operations, subscriptions))
                        .await;
                CommandResponse::executed(
                    self.command_with_transaction_status(result, subscriptions)
                        .await,
                )
            }
        };
        drop(execution_guard);
        Ok(response)
    }
}

/// The answer to a request whose execution reference aged out of execution history.
fn expired_reference(reference: &CommandExecutionReference) -> CommandResult {
    let message = format!("command execution reference '{reference}' has expired");
    CommandResult {
        diagnostics: vec![CommandDiagnostic::unlocated(message.clone())],
        ..CommandResult::new(CommandDisposition::ExecutionReferenceExpired, message)
    }
}

/// The answer to a request whose execution reference already identifies a different command.
pub(in crate::application) fn conflicting_reference(
    reference: &CommandExecutionReference,
    conflict: nervix_consensus::CommandExecutionRequestConflict,
) -> CommandResult {
    let message = format!("command execution reference '{reference}' conflicts by {conflict}");
    CommandResult {
        diagnostics: vec![CommandDiagnostic::unlocated(message.clone())],
        ..CommandResult::new(
            CommandDisposition::ExecutionReferenceConflict(conflict),
            message,
        )
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::{
        super::{
            subscription::SessionSubscriptions,
            test_fixtures::{
                TestService, build_test_service, named, queue_in_transaction, suggestion_values,
                test_execution_reference,
            },
            transaction::TransactionAttachment,
        },
        *,
    };

    #[test]
    fn completion_context_preserves_prefix_for_post_filtering() {
        let input = "CREATE SCHE";
        let CompletionContext {
            grammar_input,
            grammar_cursor,
            prefix,
        } = completion_context(input, input.len());

        assert_eq!(grammar_input, "CREATE ");
        assert_eq!(grammar_cursor, "CREATE ".len());
        assert_eq!(prefix, "sche");
    }

    #[test]
    fn keyword_completion_is_filtered_by_original_prefix() {
        let input = "CREATE SCHE";
        let CompletionContext {
            grammar_input,
            grammar_cursor,
            prefix,
        } = completion_context(input, input.len());
        let filtered = suggest_client_statement(&grammar_input, grammar_cursor)
            .into_iter()
            .filter(|item| {
                prefix.is_empty()
                    || item
                        .to_ascii_lowercase()
                        .starts_with(&prefix.to_ascii_lowercase())
            })
            .collect::<Vec<_>>();

        assert_eq!(filtered, vec!["SCHEMA".to_string()]);
    }

    #[test]
    fn rebind_member_completion_finds_the_resource_across_whitespace() {
        for input in [
            "REBIND RESOURCE bundle TO VERSION 2 FOR HASH MAP ",
            "REBIND\nRESOURCE\tbundle\nTO\tVERSION 2\nFOR\tHASH MAP ",
        ] {
            assert_eq!(
                rebind_resource_before_for(input, input.len())
                    .as_ref()
                    .map(ResourceName::as_str),
                Some("bundle")
            );
        }
        assert_eq!(
            rebind_resource_before_for("REBIND RESOURCE bundle TO VERSION 2 ", 36),
            None
        );
    }

    #[test]
    fn diagnostic_and_registry_error_helpers_map_spans() {
        let parse_diagnostic = ParseDiagnostic {
            message: "unexpected token".to_string(),
            span: 3..7,
        };
        let mapped = map_diagnostic(&parse_diagnostic);
        assert_eq!(mapped.message, "unexpected token");
        assert_eq!(mapped.span, Some(3..7));

        let response = error_response("parse error", std::slice::from_ref(&parse_diagnostic));
        assert!(!response.succeeded());
        assert_eq!(response.message, "parse error");
        assert_eq!(response.diagnostics, vec![mapped]);

        let query = "CREATE RELAY orders SCHEMA notification UNBRANCHED;";
        let identifier = named("orders");
        assert_eq!(find_identifier_span(query, &identifier), Some(13..19));

        let domain = DomainName::parse("default").expect("valid domain");
        let err = error_stack::Report::new(RegistryError::AlreadyExists {
            domain: "default".to_string(),
            identifier: "orders".to_string(),
        });
        let registry_response = create_registry_error_response(query, &domain, &identifier, &err);
        assert!(!registry_response.succeeded());
        assert!(registry_response.message.contains("orders"));
        assert_eq!(registry_response.diagnostics.len(), 1);
        assert_eq!(registry_response.diagnostics[0].span, Some(13..19));
        assert_eq!(
            infer_kind_from_error_target(&err, &identifier),
            Some("model")
        );

        let missing_target = error_stack::Report::new(RegistryError::NotFound {
            domain: "default".to_string(),
            identifier: "other".to_string(),
        });
        assert_eq!(
            infer_kind_from_error_target(&missing_target, &identifier),
            None
        );
    }

    #[tokio::test]
    async fn placement_member_completion_expands_all_schedulable_runtime_names() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let mut subscriptions = SessionSubscriptions::new();
        let configured = service
            .test_command(
                CommandRequest {
                    query: "BEGIN; CREATE SCHEMA placement_event ( id I64 ); CREATE RELAY \
                            plain_input SCHEMA placement_event UNBRANCHED; CREATE RELAY \
                            eligible_state SCHEMA placement_event UNBRANCHED WITH MATERIALIZED \
                            STATE LAST BY TIMESTAMP; CREATE RELAY plain_output SCHEMA \
                            placement_event UNBRANCHED; CREATE JUNCTION eligible_processor FROM \
                            plain_input UNBRANCHED TO plain_output INHERIT ALL FLUSH IMMEDIATE ON \
                            MESSAGE ERROR LOG; COMMIT;"
                        .to_string(),
                    domain: Some(named("default")),
                    execution_reference: test_execution_reference(),
                    expected_transaction_position: None,
                    expected_preview: None,
                },
                &mut subscriptions,
            )
            .await;
        assert!(
            configured.succeeded(),
            "placement completion fixture should configure: {configured:?}"
        );

        let input = "CREATE PLACEMENT policy FROM ";
        let request = SuggestRequest::new(input.to_string(), input.len(), Some(named("default")))
            .assured("the end of the input is a character boundary");
        let response = service
            .process_suggest(request, &subscriptions.view())
            .await;
        let values = response
            .suggestions
            .into_iter()
            .map(|suggestion| suggestion.value)
            .collect::<Vec<_>>();

        assert!(
            values.contains(&"eligible_processor".to_string()),
            "{values:?}"
        );
        assert!(values.contains(&"eligible_state".to_string()), "{values:?}");
        assert!(values.contains(&"plain_input".to_string()), "{values:?}");
        assert!(values.contains(&"plain_output".to_string()), "{values:?}");
        assert!(
            !values.contains(&"ref:runtime_node".to_string()),
            "{values:?}"
        );

        subscriptions.stop_all().await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn completion_offers_models_queued_in_the_open_transaction() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let mut subscriptions = SessionSubscriptions::new();

        queue_in_transaction(&service, &mut subscriptions, "BEGIN;").await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            "CREATE SCHEMA queued_order ( order_id I64 );",
        )
        .await;

        let values =
            suggestion_values(&service, &subscriptions, "CREATE RELAY orders SCHEMA ").await;
        assert!(values.contains(&"queued_order".to_string()), "{values:?}");

        subscriptions.stop_all().await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn completion_hides_models_dropped_in_the_open_transaction() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let mut subscriptions = SessionSubscriptions::new();

        queue_in_transaction(
            &service,
            &mut subscriptions,
            "BEGIN; CREATE SCHEMA committed_order ( order_id I64 ); CREATE RELAY committed_orders \
             SCHEMA committed_order UNBRANCHED; COMMIT;",
        )
        .await;

        let committed = suggestion_values(&service, &subscriptions, "DROP RELAY ").await;
        assert!(
            committed.contains(&"committed_orders".to_string()),
            "{committed:?}"
        );

        queue_in_transaction(&service, &mut subscriptions, "BEGIN;").await;
        queue_in_transaction(&service, &mut subscriptions, "DROP RELAY committed_orders;").await;

        let dropped = suggestion_values(&service, &subscriptions, "DROP RELAY ").await;
        assert!(
            !dropped.contains(&"committed_orders".to_string()),
            "{dropped:?}"
        );

        queue_in_transaction(
            &service,
            &mut subscriptions,
            "CREATE RELAY committed_orders SCHEMA committed_order UNBRANCHED;",
        )
        .await;

        let recreated = suggestion_values(&service, &subscriptions, "DROP RELAY ").await;
        assert!(
            recreated.contains(&"committed_orders".to_string()),
            "{recreated:?}"
        );

        subscriptions.stop_all().await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn completion_keeps_queued_models_out_of_other_sessions() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let mut writer = SessionSubscriptions::new();
        let mut observer = SessionSubscriptions::new();

        queue_in_transaction(&service, &mut writer, "BEGIN;").await;
        queue_in_transaction(
            &service,
            &mut writer,
            "CREATE SCHEMA isolated_order ( order_id I64 );",
        )
        .await;

        let bound = suggestion_values(&service, &writer, "CREATE RELAY orders SCHEMA ").await;
        assert!(bound.contains(&"isolated_order".to_string()), "{bound:?}");

        let unbound = suggestion_values(&service, &observer, "CREATE RELAY orders SCHEMA ").await;
        assert!(
            !unbound.contains(&"isolated_order".to_string()),
            "{unbound:?}"
        );

        writer.stop_all().await;
        observer.stop_all().await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn completion_drops_queued_models_until_a_detached_transaction_is_attached() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let mut subscriptions = SessionSubscriptions::new();

        queue_in_transaction(&service, &mut subscriptions, "BEGIN;").await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            "CREATE SCHEMA detached_order ( order_id I64 );",
        )
        .await;
        let transaction_id = subscriptions
            .transaction_id()
            .expect("BEGIN must bind a transaction")
            .to_string();

        let bound =
            suggestion_values(&service, &subscriptions, "CREATE RELAY orders SCHEMA ").await;
        assert!(bound.contains(&"detached_order".to_string()), "{bound:?}");

        // A leadership change leaves the replicated transaction intact while the leader-local
        // binding is gone, which is what the session observes until it attaches again.
        service.inner.transaction_bindings.remove(&transaction_id);

        let detached =
            suggestion_values(&service, &subscriptions, "CREATE RELAY orders SCHEMA ").await;
        assert!(
            !detached.contains(&"detached_order".to_string()),
            "{detached:?}"
        );

        let attached = service
            .attach_transaction(transaction_id.clone(), &mut subscriptions)
            .await;
        assert!(
            matches!(attached, TransactionAttachment::Attached { .. }),
            "reattach must succeed: {attached:?}"
        );

        let reattached =
            suggestion_values(&service, &subscriptions, "CREATE RELAY orders SCHEMA ").await;
        assert!(
            reattached.contains(&"detached_order".to_string()),
            "{reattached:?}"
        );

        subscriptions.stop_all().await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn completion_moves_queued_models_to_the_session_that_takes_the_transaction_over() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let mut first = SessionSubscriptions::new();
        let mut second = SessionSubscriptions::new();

        queue_in_transaction(&service, &mut first, "BEGIN;").await;
        queue_in_transaction(
            &service,
            &mut first,
            "CREATE SCHEMA takeover_order ( order_id I64 );",
        )
        .await;
        let transaction_id = first
            .transaction_id()
            .expect("BEGIN must bind a transaction")
            .to_string();

        let attached = service
            .attach_transaction(transaction_id.clone(), &mut second)
            .await;
        assert!(
            matches!(attached, TransactionAttachment::Attached { .. }),
            "takeover must succeed: {attached:?}"
        );

        let displaced = suggestion_values(&service, &first, "CREATE RELAY orders SCHEMA ").await;
        assert!(
            !displaced.contains(&"takeover_order".to_string()),
            "{displaced:?}"
        );

        let holder = suggestion_values(&service, &second, "CREATE RELAY orders SCHEMA ").await;
        assert!(holder.contains(&"takeover_order".to_string()), "{holder:?}");

        first.stop_all().await;
        second.stop_all().await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn placement_member_completion_expands_queued_runtime_names() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let mut subscriptions = SessionSubscriptions::new();

        queue_in_transaction(&service, &mut subscriptions, "BEGIN;").await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            "CREATE SCHEMA queued_event ( id I64 );",
        )
        .await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            "CREATE RELAY queued_state SCHEMA queued_event UNBRANCHED WITH MATERIALIZED STATE \
             LAST BY TIMESTAMP;",
        )
        .await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            "CREATE RELAY queued_plain SCHEMA queued_event UNBRANCHED;",
        )
        .await;

        let values =
            suggestion_values(&service, &subscriptions, "CREATE PLACEMENT policy FROM ").await;
        assert!(values.contains(&"queued_state".to_string()), "{values:?}");
        assert!(values.contains(&"queued_plain".to_string()), "{values:?}");

        subscriptions.stop_all().await;
        let _ = std::fs::remove_dir_all(&path);
    }
}
