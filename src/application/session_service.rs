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

use std::time::Duration;

use ahash::RandomState;
use error_stack::{Report, ResultExt as _};
use futures_util::future::BoxFuture;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    Choice, ChoiceLookupRequest, ChoiceOutcome, ChoicePresentation, ChoiceSelection, ChoiceStatus,
    ChoiceTarget, ChoiceValue, CommandRequest, DomainPaceChoice, NoticeLevel, ServerNotice,
    SuggestOutcome, SuggestRequest, Suggestion, SuggestionKind, SuggestionStatus, TextEdit,
};
use nervix_consensus::{Administrator, CommandExecutionTransactionTarget, Observer, Proposer};
use nervix_interconnect::Transport;
use nervix_models::{
    BuiltinFunctionScope, CommandExecutionReference, DomainName, Model, ModelName, PlacementPolicy,
    RequestedResourceVersion, ResourceId, ResourceName, ResourceUploadKey, ResourceVersionStatus,
    SemanticReference, TransactionPosition,
};
use nervix_nspl::{
    Token, Word,
    client_statement::{
        ClientStatement, CompletionExpectation, local_path_fragment,
        parse_client_statement_sources, suggest_client_expectations,
    },
    lex,
    schema::{Diagnostic as ParseDiagnostic, ParseFromSourceError},
};
use nervix_primitives::{
    collections::DashMap,
    sync::{Arc, CancellationToken, Mutex as AsyncMutex, StdArc, broadcast},
};
use nervix_recovery::Discarded;
use nervix_vm::program::FunctionName;
use tracing::{debug, warn};

use super::{
    authentication::AuthRateLimiter,
    backup::{CaptureSectionKey, CapturedSectionStage, ServerRetainedBackups},
    client_consumers::ClientConsumerRouter,
    client_producers::ClientProducerRouter,
    command_execution::{
        CommandAdmission, CommandExecutionOwners, CommandExecutionPolicy, PersistentCommandRequest,
    },
    command_result::{
        CommandDiagnostic, CommandDisposition, CommandOrigin, CommandResponse, CommandResult,
    },
    completion::{ApplicationRevisionPhase, wait_for_application_revision},
    configured_choices::{ConfiguredChoices, ConfiguredQuery},
    describe_output::placement_runtime_node_ref_suggestions,
    model_mutation::command_error,
    resource::{
        completed_resource_version_suggestions, resource_named_before_version,
        resource_ref_suggestions, resource_version_suggestions,
    },
    restore::ServerRestoreArchives,
    runtime_admission::RuntimeAdmission,
    scheduling::RUNTIME_REVISION_READINESS_PROPAGATION_BOUND,
    service_tasks::ServiceTasks,
    session::admission::{CancelledBeforeAdmission, RequestAdmission},
    subscription::{
        SessionCommandOperation, SessionSubscriptions, SessionView, SubscriptionInterests,
        SubscriptionSampler,
    },
    tls::HttpsListenerCertificates,
    transaction::{SessionTransactionBindingError, TransactionRecovery},
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
    /// The draws every subscription on this node samples its rows with.
    pub(in crate::application) subscription_sampler: SubscriptionSampler,
    pub(in crate::application) interconnect: Transport,
    /// Attaches the producers of this node's sessions, locally or through the node that executes
    /// their ingestor.
    pub(in crate::application) client_producers: ClientProducerRouter,
    /// Routes each client emitter consumer to its current execution owner.
    pub(in crate::application) client_consumers: ClientConsumerRouter,
    pub(in crate::application) service_tasks: ServiceTasks,
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
    /// The archives this node's backups assembled, until a download collects each or its retry
    /// validity ends.
    pub(in crate::application) retained_backups: ServerRetainedBackups,
    /// Node-local staged state sections awaiting the coordinator's bulk fetch.
    pub(in crate::application) captured_backup_sections:
        DashMap<CaptureSectionKey, CapturedSectionStage, RandomState>,
    /// Quota-charged, partially received guest saves awaiting a verified restore install.
    pub(in crate::application) restored_state_uploads: DashMap<
        nervix_models::CoordinationIdentity,
        crate::application::backup::interconnect::RestoreUploadEntry,
        RandomState,
    >,
    /// The verified archives this node's restores read, until each restore finishes or its retry
    /// validity ends.
    pub(in crate::application) restore_archives: ServerRestoreArchives,
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
) -> BoxFuture<'_, error_stack::Result<(), crate::runtime::RuntimeError>> {
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
            nervix_primitives::task::consume_budget().await;
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
            let runtime_application = admission
                .apply_planned_cluster_state(runtime, local_node_id, &state)
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
            runtime_application.change_context(crate::runtime::RuntimeError::ApplyRevision {
                revision: state.revision,
            })?;
            #[cfg(feature = "testing")]
            runtime
                .pause_runtime_preparation_if_armed(local_node_id)
                .await;
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
            let deadline_overflow = || {
                Report::new(
                    crate::runtime::RuntimeError::RuntimeRevisionReadinessDeadlineOverflow {
                        node_unavailability_timeout,
                        readiness_propagation_bound: RUNTIME_REVISION_READINESS_PROPAGATION_BOUND,
                    },
                )
            };
            let Some(readiness_timeout) = node_unavailability_timeout
                .checked_add(RUNTIME_REVISION_READINESS_PROPAGATION_BOUND)
            else {
                return Err(deadline_overflow());
            };
            let Some(deadline) =
                nervix_primitives::time::Instant::now().checked_add(readiness_timeout)
            else {
                return Err(deadline_overflow());
            };
            let preparation = wait_for_application_revision(
                cluster,
                consensus,
                interconnect,
                state.revision,
                ApplicationRevisionPhase::RuntimePrepared,
                deadline,
            );
            tokio::pin!(preparation);
            let supersession = consensus.wait_for_runtime_revision_after(state.revision);
            tokio::pin!(supersession);
            let preparation_result = nervix_primitives::select! {
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
                return Err(Report::new(
                    crate::runtime::RuntimeError::RuntimeRevisionPreparation {
                        revision: state.revision,
                        pending_nodes: timeout.pending_nodes,
                    },
                ));
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
            runtime
                .start_running_domain_ingestors()
                .await
                .change_context(crate::runtime::RuntimeError::StartIngestors {
                    revision: state.revision,
                })?;
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
                consensus,
                interconnect,
                state.revision,
                ApplicationRevisionPhase::RuntimeReady,
                deadline,
            );
            tokio::pin!(readiness);
            let supersession = consensus.wait_for_runtime_revision_after(state.revision);
            tokio::pin!(supersession);
            let readiness_result = nervix_primitives::select! {
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
                return Err(Report::new(
                    crate::runtime::RuntimeError::RuntimeRevisionReadiness {
                        revision: state.revision,
                        pending_nodes: timeout.pending_nodes,
                    },
                ));
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

/// The failed result for a request whose source the language rejected.
///
/// The message names the stage that rejected it, and every diagnostic keeps the span the parser
/// located in the submitted source, so a client can underline it in the text it sent.
pub(in crate::application) fn rejected_source_response(
    rejection: &ParseFromSourceError,
) -> CommandResult {
    match rejection {
        ParseFromSourceError::Lex { diagnostics, .. } => error_response("lex error", diagnostics),
        ParseFromSourceError::Parse { diagnostics, .. } => {
            error_response("parse error", diagnostics)
        }
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
    replacement: std::ops::Range<usize>,
}

impl CompletionContext {
    fn literal_range(&self, input: &str, candidate: &str) -> Option<std::ops::Range<usize>> {
        let words = candidate.split_ascii_whitespace().collect::<Vec<_>>();
        for current in (0..words.len()).rev() {
            if !words[current]
                .to_ascii_lowercase()
                .starts_with(&self.prefix)
            {
                continue;
            }
            let mut start = self.replacement.start;
            let mut preceding_matches = true;
            for expected in words[..current].iter().rev() {
                let head = input.get(..start)?;
                let trimmed = head.trim_end_matches(char::is_whitespace);
                if trimmed.len() == head.len() {
                    preceding_matches = false;
                    break;
                }
                let previous_start = word_start(trimmed, trimmed.len());
                if !trimmed[previous_start..].eq_ignore_ascii_case(expected) {
                    preceding_matches = false;
                    break;
                }
                start = previous_start;
            }
            if !preceding_matches {
                continue;
            }
            let mut end = self.replacement.end;
            for expected in &words[current + 1..] {
                let suffix = input.get(end..)?;
                let trimmed = suffix.trim_start_matches(char::is_whitespace);
                if trimmed.len() == suffix.len() {
                    break;
                }
                let following_start = end
                    .checked_add(suffix.len() - trimmed.len())
                    .assured("the skipped whitespace is within the completion input");
                let following_end = word_end(input, following_start);
                if !input[following_start..following_end].eq_ignore_ascii_case(expected) {
                    break;
                }
                end = following_end;
            }
            return Some(start..end);
        }
        None
    }
}

/// One captured basis for every semantic question in a completion request.
struct CompletionSnapshot<'a> {
    domain: Option<&'a DomainName>,
    models: &'a [Model<RequestedResourceVersion>],
    resources: &'a ResourceVersionStatus,
    domains: &'a [DomainName],
    queued_resources: &'a [String],
    subscriptions: &'a [String],
}

impl CompletionSnapshot<'_> {
    fn resolve(
        &self,
        reference: SemanticReference,
        prefix: &str,
        selected_resource: Option<&ResourceName>,
        rebound_resource: Option<&ResourceName>,
    ) -> Vec<String> {
        let prefix = prefix.to_ascii_lowercase();
        if reference == SemanticReference::Domain {
            return self
                .domains
                .iter()
                .filter(|domain| domain.as_str().starts_with(&prefix))
                .map(ToString::to_string)
                .collect();
        }
        if reference == SemanticReference::SessionSubscription {
            return self
                .subscriptions
                .iter()
                .filter(|name| name.starts_with(&prefix))
                .cloned()
                .collect();
        }
        let Some(domain) = self.domain else {
            return Vec::new();
        };
        match reference {
            SemanticReference::Model(kind) => self
                .models
                .iter()
                .filter(|model| {
                    model.kind() == kind
                        && model.name().as_str().starts_with(&prefix)
                        && rebound_resource.is_none_or(|resource| model.binds_resource(resource))
                })
                .map(|model| model.name().to_string())
                .collect(),
            SemanticReference::SchemaField(schema_name) => self
                .models
                .iter()
                .filter_map(|model| match model {
                    Model::Schema(schema) if schema.name == schema_name => Some(schema),
                    _ => None,
                })
                .flat_map(|schema| &schema.fields)
                .filter(|field| field.name.as_str().starts_with(&prefix))
                .map(|field| field.name.to_string())
                .collect(),
            SemanticReference::WireSchemaField(kind, schema_name) => self
                .models
                .iter()
                .filter(|model| model.kind() == kind)
                .flat_map(|model| match model {
                    Model::WireJsonSchema(schema) if schema.name == schema_name => schema
                        .fields
                        .iter()
                        .map(|field| field.name.to_string())
                        .collect::<Vec<_>>(),
                    Model::WireCborSchema(schema) if schema.name == schema_name => schema
                        .fields
                        .iter()
                        .map(|field| field.name.to_string())
                        .collect::<Vec<_>>(),
                    Model::WireAvroSchema(schema) if schema.name == schema_name => schema
                        .fields
                        .iter()
                        .map(|field| field.name.to_string())
                        .collect::<Vec<_>>(),
                    _ => Vec::new(),
                })
                .filter(|name| name.starts_with(&prefix))
                .collect(),
            SemanticReference::RelayField(relay_name) => {
                let Some(schema_name) = self.models.iter().find_map(|model| match model {
                    Model::Relay(relay) if relay.name == relay_name => Some(&relay.schema),
                    _ => None,
                }) else {
                    return Vec::new();
                };
                self.models
                    .iter()
                    .filter_map(|model| match model {
                        Model::Schema(schema) if &schema.name == schema_name => Some(schema),
                        _ => None,
                    })
                    .flat_map(|schema| &schema.fields)
                    .filter(|field| field.name.as_str().starts_with(&prefix))
                    .map(|field| field.name.to_string())
                    .collect()
            }
            SemanticReference::BuiltinFunction(scope) => {
                let mut names = FunctionName::ordinary_completion_names();
                match scope {
                    BuiltinFunctionScope::Ordinary => {}
                    BuiltinFunctionScope::IngestSource(kind) if kind.reads_headers() => {
                        names.push(FunctionName::ReadHeader.as_str().to_string());
                        names.push(FunctionName::ReadHeaders.as_str().to_string());
                    }
                    BuiltinFunctionScope::EmitterInvocation(kind)
                        if kind.capabilities().writes_headers() =>
                    {
                        names.push(FunctionName::WriteHeader.as_str().to_string());
                    }
                    BuiltinFunctionScope::IngestSource(_)
                    | BuiltinFunctionScope::EmitterInvocation(_) => {}
                }
                names
                    .into_iter()
                    .filter(|name| name.starts_with(&prefix))
                    .collect()
            }
            SemanticReference::Resource => {
                let mut candidates = resource_ref_suggestions(self.resources, domain, &prefix);
                candidates.extend(
                    self.queued_resources
                        .iter()
                        .filter(|name| name.starts_with(&prefix))
                        .cloned(),
                );
                candidates
            }
            SemanticReference::ResourceVersion => {
                let Some(resource) = selected_resource else {
                    return Vec::new();
                };
                resource_version_suggestions(self.resources, domain, resource, &prefix)
            }
            SemanticReference::CompletedResourceVersion => {
                let Some(resource) = selected_resource else {
                    return Vec::new();
                };
                completed_resource_version_suggestions(self.resources, domain, resource, &prefix)
            }
            SemanticReference::RuntimeNode => {
                placement_runtime_node_ref_suggestions(self.models, &prefix)
            }
            SemanticReference::Domain | SemanticReference::SessionSubscription => Vec::new(),
        }
    }
}

struct CompletionPageBasis {
    revision: u64,
    query_digest: String,
}

/// The revision and typed query a page cursor is bound to.
struct ChoicePageBasis {
    revision: u64,
    query_digest: String,
    content_digest: Option<String>,
}

impl ChoicePageBasis {
    fn new(request: &ChoiceLookupRequest, revision: u64) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&[match request.target() {
            ChoiceTarget::DomainPace => 0,
            ChoiceTarget::PlacementPolicy => 1,
            ChoiceTarget::Schema => 2,
            ChoiceTarget::Branch => 3,
            ChoiceTarget::Relay => 4,
            ChoiceTarget::RelayField => 5,
            ChoiceTarget::WireJsonSchema => 6,
            ChoiceTarget::WireCborSchema => 7,
            ChoiceTarget::WireAvroSchema => 8,
            ChoiceTarget::Resource => 9,
            ChoiceTarget::CompletedResourceVersion => 10,
            ChoiceTarget::Vhost => 11,
            ChoiceTarget::SignalingProtocol => 12,
            ChoiceTarget::Codec => 13,
            ChoiceTarget::CodecField => 14,
            ChoiceTarget::IngestHttpSource => 15,
            ChoiceTarget::IngestKafkaSource => 16,
            ChoiceTarget::IngestPulsarSource => 17,
            ChoiceTarget::IngestMqttSource => 18,
            ChoiceTarget::IngestNatsSource => 19,
            ChoiceTarget::IngestRabbitMqSource => 20,
            ChoiceTarget::IngestRedisPubSubSource => 21,
            ChoiceTarget::IngestPrometheusSource => 22,
            ChoiceTarget::IngestZeroMqSource => 23,
            ChoiceTarget::IngestSqsSource => 24,
            ChoiceTarget::IngestEndpointSource => 25,
            ChoiceTarget::IngestWebsocketsSource => 26,
            ChoiceTarget::IngestSyslogSource => 27,
            ChoiceTarget::IngestCodec => 28,
            ChoiceTarget::IngestUnbranchedRelay => 29,
            ChoiceTarget::IngestBranchedRelay => 30,
            ChoiceTarget::BranchField => 31,
            ChoiceTarget::ProcessorCompatibleInputRelay => 32,
            ChoiceTarget::ProcessorInputBranchRelay => 33,
            ChoiceTarget::ProcessorMaterializedRelay => 34,
        }]);
        hash_choice_text(&mut hasher, request.search());
        for dependency in request.dependencies() {
            hash_choice_value(&mut hasher, &dependency.value);
        }
        Self {
            revision,
            query_digest: hasher.finalize().to_hex().to_string(),
            content_digest: None,
        }
    }

    fn with_content_digest(mut self, digest: String) -> Self {
        self.content_digest = Some(digest);
        self
    }

    fn page(&self, request: &ChoiceLookupRequest, candidates: Vec<Choice>) -> ChoiceOutcome {
        let mut candidate_hasher = blake3::Hasher::new();
        hash_optional_choice_text(&mut candidate_hasher, self.content_digest.as_deref());
        for candidate in &candidates {
            hash_choice_value(&mut candidate_hasher, &candidate.value);
            hash_choice_text(&mut candidate_hasher, &candidate.presentation.label);
            hash_optional_choice_text(
                &mut candidate_hasher,
                candidate.presentation.detail.as_deref(),
            );
            hash_optional_choice_text(
                &mut candidate_hasher,
                candidate.presentation.group.as_deref(),
            );
        }
        let candidate_digest = candidate_hasher.finalize().to_hex().to_string();
        let offset = match request.page_cursor() {
            Some(cursor) => {
                let mut parts = cursor.split(':');
                let revision = parts.next().and_then(|value| value.parse::<u64>().ok());
                let offset = parts.next().and_then(|value| value.parse::<usize>().ok());
                let query = parts.next();
                let candidates = parts.next();
                if parts.next().is_some()
                    || revision != Some(self.revision)
                    || query != Some(self.query_digest.as_str())
                    || candidates != Some(candidate_digest.as_str())
                {
                    return stale_choice_outcome();
                }
                let Some(offset) = offset else {
                    return stale_choice_outcome();
                };
                offset
            }
            None => 0,
        };
        if offset > candidates.len() {
            return stale_choice_outcome();
        }
        let available = candidates.len() - offset;
        let take = request.page_size().min(available);
        let end = offset
            .checked_add(take)
            .assured("a choice page cannot extend beyond its bounded candidates");
        let page_cursor = if end < candidates.len() {
            Some(format!(
                "{}:{end}:{}:{candidate_digest}",
                self.revision, self.query_digest
            ))
        } else {
            None
        };
        ChoiceOutcome {
            status: ChoiceStatus::Ready,
            choices: candidates[offset..end].to_vec(),
            page_cursor,
        }
    }
}

pub(in crate::application) fn hash_choice_text(hasher: &mut blake3::Hasher, value: &str) {
    let length =
        u64::try_from(value.len()).assured("choice text fits the bounded session transfer limit");
    hasher.update(&length.to_le_bytes());
    hasher.update(value.as_bytes());
}

fn hash_optional_choice_text(hasher: &mut blake3::Hasher, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hash_choice_text(hasher, value);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn hash_choice_value(hasher: &mut blake3::Hasher, value: &ChoiceValue) {
    match value {
        ChoiceValue::DomainPace(value) => {
            hasher.update(&[
                0,
                match value {
                    DomainPaceChoice::Unpaced => 0,
                    DomainPaceChoice::Paced => 1,
                },
            ]);
        }
        ChoiceValue::PlacementPolicy(value) => {
            hasher.update(&[
                1,
                match value {
                    PlacementPolicy::RequireColocation => 0,
                    PlacementPolicy::PreferColocation => 1,
                    PlacementPolicy::Neutral => 2,
                    PlacementPolicy::SuggestSeparation => 3,
                },
            ]);
        }
        ChoiceValue::Domain(domain) => {
            hasher.update(&[2]);
            hash_choice_text(hasher, domain.as_str());
        }
        ChoiceValue::Resource(resource) => {
            hasher.update(&[3]);
            hash_choice_text(hasher, resource.as_str());
        }
        ChoiceValue::ResourceVersion(version) => {
            hasher.update(&[6]);
            hash_choice_text(hasher, &version.to_string());
        }
        ChoiceValue::Model(node) => {
            hasher.update(&[4]);
            hash_choice_text(hasher, node.kind.as_str());
            hash_choice_text(hasher, node.identifier.as_str());
        }
        ChoiceValue::Field(field) => {
            hasher.update(&[5]);
            hash_choice_text(hasher, field.as_str());
        }
    }
}

fn stale_choice_outcome() -> ChoiceOutcome {
    ChoiceOutcome {
        status: ChoiceStatus::StaleContext,
        choices: Vec::new(),
        page_cursor: None,
    }
}

fn choices_for(request: &ChoiceLookupRequest) -> Result<Vec<Choice>, ChoiceStatus> {
    let mut choices = match request.target() {
        ChoiceTarget::DomainPace if request.dependencies().is_empty() => vec![
            Choice {
                value: ChoiceValue::DomainPace(DomainPaceChoice::Unpaced),
                presentation: ChoicePresentation {
                    label: "UNPACED".to_string(),
                    detail: Some("Advance only when the domain receives progress".to_string()),
                    group: Some("Domain clock".to_string()),
                },
            },
            Choice {
                value: ChoiceValue::DomainPace(DomainPaceChoice::Paced),
                presentation: ChoicePresentation {
                    label: "PACED".to_string(),
                    detail: Some("Advance from wall time using a period and skew".to_string()),
                    group: Some("Domain clock".to_string()),
                },
            },
        ],
        ChoiceTarget::PlacementPolicy
            if matches!(
                request.dependencies(),
                [ChoiceSelection {
                    value: ChoiceValue::DomainPace(_),
                }]
            ) =>
        {
            vec![
                (
                    PlacementPolicy::RequireColocation,
                    "REQUIRE COLOCATION",
                    "Place all domain work together",
                ),
                (
                    PlacementPolicy::PreferColocation,
                    "PREFER COLOCATION",
                    "Prefer placing domain work together",
                ),
                (
                    PlacementPolicy::Neutral,
                    "NEUTRAL",
                    "Apply no placement preference",
                ),
                (
                    PlacementPolicy::SuggestSeparation,
                    "SUGGEST SEPARATION",
                    "Prefer spreading domain work across nodes",
                ),
            ]
            .into_iter()
            .map(|(value, label, detail)| Choice {
                value: ChoiceValue::PlacementPolicy(value),
                presentation: ChoicePresentation {
                    label: label.to_string(),
                    detail: Some(detail.to_string()),
                    group: Some("Placement".to_string()),
                },
            })
            .collect()
        }
        ChoiceTarget::DomainPace
        | ChoiceTarget::PlacementPolicy
        | ChoiceTarget::Schema
        | ChoiceTarget::Branch
        | ChoiceTarget::Relay
        | ChoiceTarget::RelayField => {
            return Err(ChoiceStatus::MissingContext);
        }
        ChoiceTarget::WireJsonSchema
        | ChoiceTarget::WireCborSchema
        | ChoiceTarget::WireAvroSchema
        | ChoiceTarget::Resource
        | ChoiceTarget::CompletedResourceVersion => {
            return Err(ChoiceStatus::MissingContext);
        }
        ChoiceTarget::Vhost
        | ChoiceTarget::SignalingProtocol
        | ChoiceTarget::Codec
        | ChoiceTarget::CodecField
        | ChoiceTarget::IngestHttpSource
        | ChoiceTarget::IngestKafkaSource
        | ChoiceTarget::IngestPulsarSource
        | ChoiceTarget::IngestMqttSource
        | ChoiceTarget::IngestNatsSource
        | ChoiceTarget::IngestRabbitMqSource
        | ChoiceTarget::IngestRedisPubSubSource
        | ChoiceTarget::IngestPrometheusSource
        | ChoiceTarget::IngestZeroMqSource
        | ChoiceTarget::IngestSqsSource
        | ChoiceTarget::IngestEndpointSource
        | ChoiceTarget::IngestWebsocketsSource
        | ChoiceTarget::IngestSyslogSource
        | ChoiceTarget::IngestCodec
        | ChoiceTarget::IngestUnbranchedRelay
        | ChoiceTarget::IngestBranchedRelay
        | ChoiceTarget::BranchField
        | ChoiceTarget::ProcessorCompatibleInputRelay
        | ChoiceTarget::ProcessorInputBranchRelay
        | ChoiceTarget::ProcessorMaterializedRelay => {
            return Err(ChoiceStatus::MissingContext);
        }
    };
    let search = request.search().to_ascii_lowercase();
    choices.retain(|choice| {
        search.is_empty()
            || choice
                .presentation
                .label
                .to_ascii_lowercase()
                .contains(&search)
            || choice
                .presentation
                .detail
                .as_deref()
                .is_some_and(|detail| detail.to_ascii_lowercase().contains(&search))
    });
    Ok(choices)
}

impl CompletionPageBasis {
    fn new(request: &SuggestRequest, revision: u64) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(request.input().as_bytes());
        let cursor = u64::try_from(request.cursor())
            .assured("a session completion input fits the wire's u32 cursor range");
        hasher.update(&cursor.to_le_bytes());
        if let Some(domain) = request.domain() {
            hasher.update(domain.as_str().as_bytes());
        }
        let query_digest = hasher.finalize().to_hex().to_string();
        Self {
            revision,
            query_digest,
        }
    }

    fn page(&self, request: &SuggestRequest, candidates: Vec<Suggestion>) -> SuggestOutcome {
        let mut candidate_hasher = blake3::Hasher::new();
        for candidate in &candidates {
            let bytes = candidate.value.as_bytes();
            let length = u64::try_from(bytes.len())
                .assured("a candidate value fits the bounded session frame");
            candidate_hasher.update(&length.to_le_bytes());
            candidate_hasher.update(bytes);
            candidate_hasher.update(&candidate.edit.start.to_le_bytes());
            candidate_hasher.update(&candidate.edit.end.to_le_bytes());
            let replacement = candidate.edit.replacement.as_bytes();
            let replacement_length = u64::try_from(replacement.len())
                .assured("an edit replacement fits the bounded session frame");
            candidate_hasher.update(&replacement_length.to_le_bytes());
            candidate_hasher.update(replacement);
            candidate_hasher.update(&[match candidate.kind {
                SuggestionKind::Text => 0,
                SuggestionKind::LocalDirectoryLookup => 1,
            }]);
        }
        let candidate_digest = candidate_hasher.finalize().to_hex().to_string();
        let offset = match request.continuation() {
            Some(continuation) => {
                let mut parts = continuation.split(':');
                let revision = parts.next().and_then(|value| value.parse::<u64>().ok());
                let offset = parts.next().and_then(|value| value.parse::<usize>().ok());
                let digest = parts.next();
                let candidates = parts.next();
                if parts.next().is_some()
                    || revision != Some(self.revision)
                    || digest != Some(self.query_digest.as_str())
                    || candidates != Some(candidate_digest.as_str())
                {
                    return SuggestOutcome {
                        status: SuggestionStatus::StaleContext,
                        suggestions: Vec::new(),
                        continuation: None,
                    };
                }
                let Some(offset) = offset else {
                    return SuggestOutcome {
                        status: SuggestionStatus::StaleContext,
                        suggestions: Vec::new(),
                        continuation: None,
                    };
                };
                offset
            }
            None => 0,
        };
        if offset > candidates.len() {
            return SuggestOutcome {
                status: SuggestionStatus::StaleContext,
                suggestions: Vec::new(),
                continuation: None,
            };
        }
        let available = candidates.len() - offset;
        let take = request.page_size().min(available);
        let end = offset
            .checked_add(take)
            .assured("the page take cannot exceed the candidates after its offset");
        let continuation = if end < candidates.len() {
            Some(format!(
                "{}:{end}:{}:{candidate_digest}",
                self.revision, self.query_digest
            ))
        } else {
            None
        };
        SuggestOutcome {
            status: SuggestionStatus::Ready,
            suggestions: candidates[offset..end].to_vec(),
            continuation,
        }
    }
}

fn completion_context(input: &str, cursor: usize) -> CompletionContext {
    let safe_cursor = cursor.min(input.len());
    let start = word_start(input, safe_cursor);
    let end = word_end(input, safe_cursor);
    let prefix = current_word_prefix(input, safe_cursor);

    let mut grammar_input = String::with_capacity(input.len() - (end - start));
    grammar_input.push_str(&input[..start]);
    grammar_input.push_str(&input[end..]);

    CompletionContext {
        grammar_input,
        grammar_cursor: start,
        prefix,
        replacement: start..end,
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

fn word_end(input: &str, cursor: usize) -> usize {
    let suffix = &input[cursor..];
    let within = suffix
        .find(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .unwrap_or(suffix.len());
    cursor
        .checked_add(within)
        .assured("the suffix offset is within the completion input")
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
    ) -> error_stack::Result<(), crate::runtime::RuntimeError> {
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

    /// Resolves one structured control against a single observed application revision.
    pub(in crate::application) async fn process_choice(
        &self,
        request: ChoiceLookupRequest,
        session: &SessionView,
    ) -> ChoiceOutcome {
        let revision = self.inner.consensus.current_revision().await;
        let basis = ChoicePageBasis::new(&request, revision);
        let resolved = match request.target() {
            ChoiceTarget::DomainPace | ChoiceTarget::PlacementPolicy => {
                choices_for(&request).map(|choices| (choices, None))
            }
            ChoiceTarget::Schema
            | ChoiceTarget::Branch
            | ChoiceTarget::Relay
            | ChoiceTarget::RelayField
            | ChoiceTarget::WireJsonSchema
            | ChoiceTarget::WireCborSchema
            | ChoiceTarget::WireAvroSchema
            | ChoiceTarget::Resource
            | ChoiceTarget::CompletedResourceVersion => {
                self.configured_choices_for(&request, session).await
            }
            ChoiceTarget::Vhost
            | ChoiceTarget::SignalingProtocol
            | ChoiceTarget::Codec
            | ChoiceTarget::CodecField
            | ChoiceTarget::IngestHttpSource
            | ChoiceTarget::IngestKafkaSource
            | ChoiceTarget::IngestPulsarSource
            | ChoiceTarget::IngestMqttSource
            | ChoiceTarget::IngestNatsSource
            | ChoiceTarget::IngestRabbitMqSource
            | ChoiceTarget::IngestRedisPubSubSource
            | ChoiceTarget::IngestPrometheusSource
            | ChoiceTarget::IngestZeroMqSource
            | ChoiceTarget::IngestSqsSource
            | ChoiceTarget::IngestEndpointSource
            | ChoiceTarget::IngestWebsocketsSource
            | ChoiceTarget::IngestSyslogSource
            | ChoiceTarget::IngestCodec
            | ChoiceTarget::IngestUnbranchedRelay
            | ChoiceTarget::IngestBranchedRelay
            | ChoiceTarget::BranchField
            | ChoiceTarget::ProcessorCompatibleInputRelay
            | ChoiceTarget::ProcessorInputBranchRelay
            | ChoiceTarget::ProcessorMaterializedRelay => {
                self.configured_choices_for(&request, session).await
            }
        };
        let (choices, content_digest) = match resolved {
            Ok(resolved) => resolved,
            Err(status) => {
                return ChoiceOutcome {
                    status,
                    choices: Vec::new(),
                    page_cursor: None,
                };
            }
        };
        if self.inner.consensus.current_revision().await != revision {
            return stale_choice_outcome();
        }
        let basis = match content_digest {
            Some(digest) => basis.with_content_digest(digest),
            None => basis,
        };
        basis.page(&request, choices)
    }

    /// Answers a question about the configuration of the domain `request` depends on, read with
    /// the session's attached transaction prefix applied, so a model staged earlier in that
    /// transaction is offered before commit.
    async fn configured_choices_for(
        &self,
        request: &ChoiceLookupRequest,
        session: &SessionView,
    ) -> Result<(Vec<Choice>, Option<String>), ChoiceStatus> {
        let Some(ConfiguredQuery { domain, question }) = ConfiguredQuery::of(request) else {
            return Err(ChoiceStatus::MissingContext);
        };
        let domains = self.inner.consensus.current_domains().await;
        if !domains.contains_key(domain) {
            return Err(ChoiceStatus::MissingContext);
        }
        let queued = self
            .queued_configuration(session.binding(), Some(domain))
            .await
            .map_err(|_| ChoiceStatus::StaleContext)?;
        let models = self
            .inner
            .registry
            .resulting_models(domain, &queued.models)
            .map_err(|_| ChoiceStatus::LookupFailed)?;
        let resources = self.inner.consensus.current_resources().await;
        let staged_resources = queued.resource_suggestions("");
        let resolved = ConfiguredChoices::new(domain.clone(), models, resources, staged_resources)
            .resolve(&question, request.search())?;
        Ok((resolved.choices, Some(resolved.content_digest)))
    }

    /// The completions at the request's cursor, read against the session as `session` last left
    /// it. The request's cursor is a byte offset on a character boundary of its input, which the
    /// request type guarantees.
    pub(in crate::application) async fn process_suggest(
        &self,
        req: SuggestRequest,
        session: &SessionView,
    ) -> SuggestOutcome {
        let revision_before = self.inner.consensus.current_revision().await;
        let page_basis = CompletionPageBasis::new(&req, revision_before);
        let cursor = req.cursor();
        let domain = req.domain().cloned();
        let queued = match self
            .queued_configuration(session.binding(), domain.as_ref())
            .await
        {
            Ok(queued) => queued,
            Err(_) => {
                return SuggestOutcome {
                    status: SuggestionStatus::StaleContext,
                    continuation: None,
                    suggestions: Vec::new(),
                };
            }
        };

        let context = completion_context(req.input(), cursor);
        let grammar = suggest_client_expectations(&context.grammar_input, context.grammar_cursor);

        let mut suggestions = Vec::new();
        let mut references = Vec::new();
        for item in grammar {
            match item {
                CompletionExpectation::Semantic(reference) => references.push(reference),
                CompletionExpectation::Literal(item) => {
                    if let Some(range) = context.literal_range(req.input(), &item) {
                        suggestions.push((item, range));
                    }
                }
            }
        }
        if !references.is_empty() {
            let domains = self.inner.consensus.current_domains().await;
            let selected_exists = domain
                .as_ref()
                .is_some_and(|selected| domains.contains_key(selected));
            if !selected_exists
                && references.iter().any(|reference| {
                    !matches!(
                        reference,
                        SemanticReference::Domain | SemanticReference::SessionSubscription
                    )
                })
            {
                return SuggestOutcome {
                    status: SuggestionStatus::MissingContext,
                    continuation: None,
                    suggestions: Vec::new(),
                };
            }
            let models = if selected_exists {
                match domain.as_ref() {
                    Some(selected) => match self
                        .inner
                        .registry
                        .resulting_models(selected, &queued.models)
                    {
                        Ok(models) => models,
                        Err(_) => {
                            return SuggestOutcome {
                                status: SuggestionStatus::LookupFailed,
                                continuation: None,
                                suggestions: Vec::new(),
                            };
                        }
                    },
                    None => Vec::new(),
                }
            } else {
                Vec::new()
            };
            let resources = self.inner.consensus.current_resources().await;
            let domain_names = domains.into_keys().collect::<Vec<_>>();
            let queued_resources = queued.resource_suggestions("");
            let subscriptions = session.matching_subscription_names("");
            let snapshot = CompletionSnapshot {
                domain: if selected_exists {
                    domain.as_ref()
                } else {
                    None
                },
                models: &models,
                resources: &resources,
                domains: &domain_names,
                queued_resources: &queued_resources,
                subscriptions: &subscriptions,
            };
            let selected_resource =
                resource_named_before_version(&context.grammar_input, context.grammar_cursor);
            let rebound_resource =
                rebind_resource_before_for(&context.grammar_input, context.grammar_cursor);
            for reference in references {
                let values = snapshot.resolve(
                    reference,
                    &context.prefix,
                    selected_resource.as_ref(),
                    rebound_resource.as_ref(),
                );
                suggestions.extend(
                    values
                        .into_iter()
                        .map(|value| (value, context.replacement.clone())),
                );
            }
        }

        suggestions.sort_by(|left, right| left.0.cmp(&right.0));
        suggestions.dedup_by(|left, right| left.0 == right.0);
        let mut response_suggestions = suggestions
            .into_iter()
            .map(|(value, range)| Suggestion {
                edit: TextEdit {
                    start: u32::try_from(range.start).assured(
                        "a completion source fits the session frame limit below 2^32 bytes",
                    ),
                    end: u32::try_from(range.end).assured(
                        "a completion source fits the session frame limit below 2^32 bytes",
                    ),
                    replacement: value.clone(),
                },
                value,
                kind: SuggestionKind::Text,
            })
            .collect::<Vec<_>>();

        if let Some(local_path) = local_path_fragment(req.input(), cursor) {
            response_suggestions.push(Suggestion {
                value: local_path.fragment.to_string(),
                kind: SuggestionKind::LocalDirectoryLookup,
                edit: TextEdit {
                    start: u32::try_from(local_path.range.start)
                        .assured("the local path fragment is a slice of the bounded request"),
                    end: u32::try_from(local_path.range.end).assured(
                        "a completion source fits the session frame limit below 2^32 bytes",
                    ),
                    replacement: local_path.fragment.to_string(),
                },
            });
        }

        response_suggestions.sort_by(|left, right| {
            left.value.cmp(&right.value).then_with(|| {
                let left_path = left.kind == SuggestionKind::LocalDirectoryLookup;
                let right_path = right.kind == SuggestionKind::LocalDirectoryLookup;
                left_path.cmp(&right_path)
            })
        });
        if self.inner.consensus.current_revision().await != revision_before {
            return SuggestOutcome {
                status: SuggestionStatus::StaleContext,
                continuation: None,
                suggestions: Vec::new(),
            };
        }
        page_basis.page(&req, response_suggestions)
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
            Err(report) => {
                let result = self
                    .command_with_transaction_status(
                        rejected_source_response(report.current_context()),
                        subscriptions,
                    )
                    .await;
                return Ok(CommandResponse::executed(result));
            }
        };

        // A request that only inspects a transaction reads it rather than changing it, so it
        // takes none of the durable admission, replay and queue-position fencing a transaction
        // request needs, even while the session has one attached.
        let reads_transaction_state_only = matches!(
            client_statements.as_slice(),
            [parsed] if parsed.statement.reads_transaction_state()
        );
        let is_transaction_request = !reads_transaction_state_only
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
                    .command_with_transaction_status(
                        SessionTransactionBindingError::command_result(&error),
                        subscriptions,
                    )
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
                    .command_with_transaction_status(
                        command_error(error.to_string()),
                        subscriptions,
                    )
                    .await;
                return Ok(CommandResponse::executed(result));
            }
        };

        let persistent_request = if is_transaction_request {
            let domain = match self.resolve_transaction_domain(req.domain.as_ref()).await {
                Ok(domain) => domain,
                Err(error) => {
                    return Ok(CommandResponse::executed(command_error(error.to_string())));
                }
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
pub(in crate::application) fn expired_reference(
    reference: &CommandExecutionReference,
) -> CommandResult {
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
    use nervix_models::{ModelKind, NodeRef};

    use super::{
        super::{
            subscription::SessionSubscriptions,
            test_fixtures::{
                TestService, build_test_service, build_test_service_inner, named,
                queue_in_transaction, suggestion_values, test_execution_reference,
            },
            transaction::TransactionAttachment,
        },
        *,
    };

    #[test]
    fn completion_pages_cover_candidates_and_reject_a_changed_basis() {
        let request = SuggestRequest::new("SHOW ".to_string(), 5, None)
            .assured("the test cursor is at a character boundary")
            .with_page(2, None)
            .assured("two suggestions fit the bounded page size");
        let basis = CompletionPageBasis::new(&request, 42);
        let candidates = ["ALPHA", "BETA", "GAMMA"]
            .into_iter()
            .map(|value| Suggestion {
                value: value.to_string(),
                kind: SuggestionKind::Text,
                edit: TextEdit {
                    start: 5,
                    end: 5,
                    replacement: value.to_string(),
                },
            })
            .collect::<Vec<_>>();
        let first = basis.page(&request, candidates.clone());
        assert_eq!(first.status, SuggestionStatus::Ready);
        assert_eq!(first.suggestions.len(), 2);
        let next = first.continuation.assured("a third candidate remains");
        let next_request = request
            .with_page(2, Some(next))
            .assured("the second page uses the same bounded size");
        let second = basis.page(&next_request, candidates.clone());
        assert_eq!(second.suggestions.len(), 1);
        assert_eq!(second.suggestions[0].value, "GAMMA");
        assert!(second.continuation.is_none());

        let mut changed_candidates = candidates.clone();
        changed_candidates[2].value = "DELTA".to_string();
        assert_eq!(
            basis.page(&next_request, changed_candidates).status,
            SuggestionStatus::StaleContext
        );

        let changed = CompletionPageBasis::new(&next_request, 43);
        assert_eq!(
            changed.page(&next_request, candidates).status,
            SuggestionStatus::StaleContext
        );
    }

    #[test]
    fn choice_pages_bind_typed_dependencies_search_candidates_and_revision() {
        let request = |pace, search: &str, cursor| {
            ChoiceLookupRequest::new(
                ChoiceTarget::PlacementPolicy,
                vec![ChoiceSelection {
                    value: ChoiceValue::DomainPace(pace),
                }],
                search.to_string(),
            )
            .with_page(3, cursor)
            .assured("three choices fit the bounded page size")
        };
        let first_request = request(DomainPaceChoice::Paced, "", None);
        let basis = ChoicePageBasis::new(&first_request, 42);
        let candidates =
            choices_for(&first_request).assured("a placement lookup carries its pace dependency");
        let first = basis.page(&first_request, candidates.clone());
        assert_eq!(first.status, ChoiceStatus::Ready);
        assert_eq!(first.choices.len(), 3);
        let cursor = first.page_cursor.assured("the fourth choice remains");

        let next_request = request(DomainPaceChoice::Paced, "", Some(cursor.clone()));
        let second = basis.page(&next_request, candidates);
        assert_eq!(second.status, ChoiceStatus::Ready);
        assert_eq!(
            second.choices[0].value,
            ChoiceValue::PlacementPolicy(PlacementPolicy::SuggestSeparation)
        );
        assert!(second.page_cursor.is_none());

        let schema_content_request = ChoiceLookupRequest::new(
            ChoiceTarget::Schema,
            vec![ChoiceSelection {
                value: ChoiceValue::Domain(DomainName::parse("tenant").assured("valid domain")),
            }],
            String::new(),
        )
        .with_page(1, None)
        .assured("one choice fits the bounded page size");
        let schema_candidates = vec![
            Choice {
                value: ChoiceValue::Model(NodeRef::new(
                    ModelKind::Schema,
                    ModelName::parse("first").assured("valid schema name"),
                )),
                presentation: ChoicePresentation {
                    label: "first".to_string(),
                    detail: Some("1 fields".to_string()),
                    group: Some("Schema".to_string()),
                },
            },
            Choice {
                value: ChoiceValue::Model(NodeRef::new(
                    ModelKind::Schema,
                    ModelName::parse("second").assured("valid schema name"),
                )),
                presentation: ChoicePresentation {
                    label: "second".to_string(),
                    detail: Some("1 fields".to_string()),
                    group: Some("Schema".to_string()),
                },
            },
        ];
        let first = ChoicePageBasis::new(&schema_content_request, 42)
            .with_content_digest("first schema shape".to_string())
            .page(&schema_content_request, schema_candidates.clone());
        let continued = ChoiceLookupRequest::new(
            ChoiceTarget::Schema,
            schema_content_request.dependencies().to_vec(),
            String::new(),
        )
        .with_page(1, first.page_cursor)
        .assured("one choice fits the bounded page size");
        assert_eq!(
            ChoicePageBasis::new(&continued, 42)
                .with_content_digest("changed schema shape".to_string())
                .page(&continued, schema_candidates)
                .status,
            ChoiceStatus::StaleContext
        );

        let changed_dependency = request(DomainPaceChoice::Unpaced, "", Some(cursor.clone()));
        assert_eq!(
            ChoicePageBasis::new(&changed_dependency, 42)
                .page(
                    &changed_dependency,
                    choices_for(&changed_dependency)
                        .assured("the changed request still has a typed pace dependency"),
                )
                .status,
            ChoiceStatus::StaleContext
        );
        assert_eq!(
            ChoicePageBasis::new(&next_request, 43)
                .page(
                    &next_request,
                    choices_for(&next_request)
                        .assured("the continued request has a typed pace dependency"),
                )
                .status,
            ChoiceStatus::StaleContext
        );

        let searched = request(DomainPaceChoice::Paced, "spreading", None);
        let searched =
            choices_for(&searched).assured("the searched request has a typed pace dependency");
        assert_eq!(searched.len(), 1);
        assert_eq!(searched[0].presentation.label, "SUGGEST SEPARATION");
    }

    #[test]
    fn choice_lookup_reports_missing_typed_dependencies() {
        let placement =
            ChoiceLookupRequest::new(ChoiceTarget::PlacementPolicy, Vec::new(), String::new());
        assert_eq!(choices_for(&placement), Err(ChoiceStatus::MissingContext));

        let pace = ChoiceLookupRequest::new(
            ChoiceTarget::DomainPace,
            vec![ChoiceSelection {
                value: ChoiceValue::PlacementPolicy(PlacementPolicy::Neutral),
            }],
            String::new(),
        );
        assert_eq!(choices_for(&pace), Err(ChoiceStatus::MissingContext));
    }

    #[test]
    fn completion_context_preserves_prefix_for_post_filtering() {
        let input = "CREATE SCHE";
        let CompletionContext {
            grammar_input,
            grammar_cursor,
            prefix,
            ..
        } = completion_context(input, input.len());

        assert_eq!(grammar_input, "CREATE ");
        assert_eq!(grammar_cursor, "CREATE ".len());
        assert_eq!(prefix, "sche");

        let input = "SHOW CLUSTR;";
        let CompletionContext {
            grammar_input,
            grammar_cursor,
            prefix,
            ..
        } = completion_context(input, "SHOW CLU".len());
        assert_eq!(grammar_input, "SHOW ;");
        assert_eq!(grammar_cursor, "SHOW ".len());
        assert_eq!(prefix, "clu");
        assert_eq!(word_end(input, "SHOW CLU".len()), "SHOW CLUSTR".len());
    }

    #[test]
    fn keyword_completion_is_filtered_by_original_prefix() {
        let input = "CREATE SCHE";
        let context = completion_context(input, input.len());
        let filtered = suggest_client_expectations(&context.grammar_input, context.grammar_cursor)
            .into_iter()
            .filter_map(|item| {
                let CompletionExpectation::Literal(item) = item else {
                    return None;
                };
                context.literal_range(input, &item).map(|_| item)
            })
            .collect::<Vec<_>>();

        assert_eq!(filtered, vec!["SCHEMA".to_string()]);
    }

    #[test]
    fn phrase_completion_replaces_matching_words_around_the_cursor() {
        let input = "TIME RA";
        let context = completion_context(input, input.len());
        assert_eq!(
            context.literal_range(input, "TIME RATE"),
            Some(0..input.len())
        );

        let input = "TIME RATE;";
        let context = completion_context(input, 2);
        assert_eq!(context.literal_range(input, "TIME RATE"), Some(0..9));

        let input = "OTHER RA";
        let context = completion_context(input, input.len());
        assert_eq!(context.literal_range(input, "TIME RATE"), None);
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

    #[test]
    fn a_rejected_source_names_its_stage_and_keeps_every_span() {
        let query = "USE demo;\nCREATE SCHEMA broken (id BOGUS);";
        let parse = nervix_nspl::client_statement::parse_client_statement_sources(query)
            .expect_err("BOGUS is not a type");
        let response = rejected_source_response(parse.current_context());
        assert!(!response.succeeded());
        assert_eq!(response.message, "parse error");
        assert_eq!(response.diagnostics.len(), 1);
        assert_eq!(response.diagnostics[0].span, Some(35..40));

        let query = "USE demo;\nCREATE SCHEMA broken (id STRING @);";
        let lex = nervix_nspl::client_statement::parse_client_statement_sources(query)
            .expect_err("`@` starts no token");
        let response = rejected_source_response(lex.current_context());
        assert!(!response.succeeded());
        assert_eq!(response.message, "lex error");
        assert_eq!(response.diagnostics.len(), 1);
        assert_eq!(response.diagnostics[0].span, Some(42..43));
    }

    #[nervix_primitives::test]
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

    #[nervix_primitives::test]
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

    #[nervix_primitives::test]
    async fn schema_field_completion_uses_ordered_queued_alterations() {
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
            "CREATE SCHEMA queued_order ( first I64, secret STRING );",
        )
        .await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            "ALTER SCHEMA queued_order DROP FIELD first, ADD FIELD current I64;",
        )
        .await;

        let values = suggestion_values(
            &service,
            &subscriptions,
            "ALTER SCHEMA queued_order DROP FIELD ",
        )
        .await;
        assert_eq!(values, vec!["current", "secret"]);

        queue_in_transaction(
            &service,
            &mut subscriptions,
            "CREATE WIRE JSON SCHEMA queued_wire MODE STRICT ( value integer, secret string );",
        )
        .await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            "ALTER WIRE JSON SCHEMA queued_wire RENAME FIELD value TO current;",
        )
        .await;
        let wire_values = suggestion_values(
            &service,
            &subscriptions,
            "ALTER WIRE JSON SCHEMA queued_wire DROP FIELD ",
        )
        .await;
        assert_eq!(wire_values, vec!["current", "secret"]);

        queue_in_transaction(
            &service,
            &mut subscriptions,
            "CREATE WIRE CBOR SCHEMA queued_cbor MODE STRICT ( value integer );",
        )
        .await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            "CREATE WIRE AVRO SCHEMA queued_avro MODE STRICT ( value long );",
        )
        .await;
        let cbor_values = suggestion_values(
            &service,
            &subscriptions,
            "ALTER WIRE CBOR SCHEMA queued_cbor DROP FIELD ",
        )
        .await;
        assert_eq!(cbor_values, vec!["value"]);
        let avro_values = suggestion_values(
            &service,
            &subscriptions,
            "ALTER WIRE AVRO SCHEMA queued_avro DROP FIELD ",
        )
        .await;
        assert_eq!(avro_values, vec!["value"]);

        subscriptions.stop_all().await;
        std::fs::remove_dir_all(&path)
            .discarded("the temporary test fixture is already isolated from the next test");
    }

    #[nervix_primitives::test]
    async fn route_expression_completion_resolves_queued_relay_fields_and_vm_builtins() {
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
            "CREATE SCHEMA event ( value I64, other STRING );",
        )
        .await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            "CREATE RELAY incoming SCHEMA event UNBRANCHED;",
        )
        .await;
        queue_in_transaction(
            &service,
            &mut subscriptions,
            "CREATE RELAY outgoing SCHEMA event UNBRANCHED;",
        )
        .await;

        let prefix = "CREATE JUNCTION normalizer FROM incoming UNBRANCHED TO outgoing SET value = ";
        let functions = suggestion_values(&service, &subscriptions, &format!("{prefix}co")).await;
        assert!(functions.contains(&"coalesce".to_string()), "{functions:?}");
        assert!(!functions.contains(&"write_header".to_string()));

        let fields =
            suggestion_values(&service, &subscriptions, &format!("{prefix}input.va")).await;
        assert_eq!(fields, vec!["value"]);

        let endpoint = suggestion_values(
            &service,
            &subscriptions,
            "CREATE INGESTOR source FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON QUIESCE \
             SUSPEND DECODE USING codec TO outgoing SET value = read_",
        )
        .await;
        assert_eq!(endpoint, vec!["read_header", "read_headers"]);
        let mqtt = suggestion_values(
            &service,
            &subscriptions,
            "CREATE INGESTOR source FROM MQTT broker TOPIC events MODE NO_ACK SEQUENTIAL ON \
             QUIESCE SUSPEND DECODE USING codec TO outgoing SET value = read_",
        )
        .await;
        assert!(mqtt.is_empty(), "{mqtt:?}");

        let kafka = suggestion_values(
            &service,
            &subscriptions,
            "CREATE EMITTER sink FROM incoming TO KAFKA broker TOPIC events MODE NO_ACK RETRY \
             POLICY BACKOFF 250ms MAX 30s ENCODE USING codec INVOKE write_",
        )
        .await;
        assert_eq!(kafka, vec!["write_header"]);
        let sentry = suggestion_values(
            &service,
            &subscriptions,
            "CREATE EMITTER sink FROM incoming TO SENTRY client MODE ACK RETRY POLICY BACKOFF \
             250ms MAX 30s ENCODE USING codec INVOKE write_",
        )
        .await;
        assert!(sentry.is_empty(), "{sentry:?}");

        subscriptions.stop_all().await;
        std::fs::remove_dir_all(&path)
            .discarded("the temporary test fixture is already isolated from the next test");
    }

    #[nervix_primitives::test]
    async fn completion_reports_stale_context_when_an_attached_transaction_loses_its_domain() {
        let TestService { service, path, .. } = build_test_service(true).await;
        let mut subscriptions = SessionSubscriptions::new();
        queue_in_transaction(&service, &mut subscriptions, "BEGIN;").await;

        let input = "DROP SCHEMA ";
        for domain in [None, Some(named("another_domain"))] {
            let request = SuggestRequest::new(input.to_string(), input.len(), domain)
                .assured("the test cursor is at the end of the input");
            let outcome = service
                .process_suggest(request, &subscriptions.view())
                .await;
            assert_eq!(outcome.status, SuggestionStatus::StaleContext);
            assert!(outcome.suggestions.is_empty());
        }

        subscriptions.stop_all().await;
        std::fs::remove_dir_all(&path)
            .discarded("the temporary test fixture is already isolated from the next test");
    }

    #[nervix_primitives::test]
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

    #[nervix_primitives::test]
    async fn completion_keeps_queued_models_out_of_other_sessions() {
        let (
            TestService {
                service,
                registry,
                path,
            },
            consensus,
        ) = {
            #[cfg(feature = "testing")]
            {
                build_test_service_inner(true, None, Runtime::new()).await
            }
            #[cfg(not(feature = "testing"))]
            {
                build_test_service_inner(true, Runtime::new()).await
            }
        };
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
        nervix_primitives::time::timeout(Duration::from_secs(30), async {
            service.inner.runtime.shutdown().await;
            consensus.shutdown().await;
        })
        .await
        .expect("the completion fixture joins its runtime and consensus storage before teardown");
        drop(writer);
        drop(observer);
        drop(service);
        drop(registry);
        drop(consensus);
        std::fs::remove_dir_all(&path)
            .expect("the stopped completion fixture directory is removed");
    }

    #[nervix_primitives::test]
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

    #[nervix_primitives::test]
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

    #[nervix_primitives::test]
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
