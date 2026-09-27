//! Structured creation of the small entities operators need before a graph can be configured.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser-local create drafts, typed choice presentation, canonical previews, dialog
//!   accessibility, and the state shown for one durable submission.
//! - **Depends on.** Public semantic models, the session choice contract, and the console request
//!   dispatcher owned by the parent module.
//! - **Must not know.** Registry state, command execution internals, or how the server resolves a
//!   choice.

use std::collections::BTreeMap;

use error_stack::{Report, ResultExt as _};
use futures_channel::mpsc::UnboundedSender;
use leptos::{ev, prelude::*};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    Choice, ChoiceLookupRequest, ChoiceOutcome, ChoiceSelection, ChoiceStatus, ChoiceTarget,
    ChoiceValue, DomainPaceChoice,
};
use nervix_models::{
    CreateDomain, CreateResource, CreateStatement, CreateUser, DomainClockPeriod, DomainClockSkew,
    DomainConfig, DomainName, DomainPace, Model, ModelKind, PlacementPolicy, ResourceName,
    SchemaName, Statement, UserName,
};
use nervix_recovery::Discarded as _;
use thiserror::Error;
use wasm_bindgen::JsCast as _;

use super::{ConsoleConnectionState, ConsoleRequest};

mod schema_draft;
mod schema_editor;

use schema_draft::{SchemaDraftError, StructuredDrafts, WireFormat};
use schema_editor::{BranchEditor, SchemaEditor, WireSchemaEditor};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CreateKind {
    Domain,
    User,
    Resource,
    Schema,
    WireJsonSchema,
    WireCborSchema,
    WireAvroSchema,
    Branch,
}

impl CreateKind {
    fn label(self) -> &'static str {
        match self {
            Self::Domain => "domain",
            Self::User => "user",
            Self::Resource => "resource",
            Self::Schema => "schema",
            Self::WireJsonSchema => "wire JSON schema",
            Self::WireCborSchema => "wire CBOR schema",
            Self::WireAvroSchema => "wire AVRO schema",
            Self::Branch => "branch",
        }
    }

    fn domain_scoped(self) -> bool {
        !matches!(self, Self::Domain | Self::User)
    }

    fn wire_format(self) -> Option<WireFormat> {
        match self {
            Self::WireJsonSchema => Some(WireFormat::Json),
            Self::WireCborSchema => Some(WireFormat::Cbor),
            Self::WireAvroSchema => Some(WireFormat::Avro),
            Self::Domain | Self::User | Self::Resource | Self::Schema | Self::Branch => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ChoiceControl {
    DomainPace,
    PlacementPolicy,
    BranchSchema,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChoiceRequestContext {
    pub(crate) control: ChoiceControl,
    pub(crate) draft_revision: u64,
    pub(crate) session_generation: u64,
    pub(crate) append: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ChoiceLoad {
    Waiting,
    Loading,
    Ready {
        choices: Vec<Choice>,
        page_cursor: Option<String>,
    },
    Empty,
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CreateProgress {
    Editing,
    Submitting,
    QueuedReconnect,
    QueuedTransaction { position: usize },
    Completed,
    Failed(String),
}

impl CreateProgress {
    fn label(&self) -> String {
        match self {
            Self::Editing => "Editing".to_string(),
            Self::Submitting => "Submitting".to_string(),
            Self::QueuedReconnect => "Queued until reconnect".to_string(),
            Self::QueuedTransaction { position } => {
                format!("Queued in transaction · position {position}")
            }
            Self::Completed => "Completed".to_string(),
            Self::Failed(_) => "Failed".to_string(),
        }
    }

    fn is_pending(&self) -> bool {
        matches!(self, Self::Submitting | Self::QueuedReconnect)
    }

    fn blocks_repeat_submit(&self) -> bool {
        matches!(
            self,
            Self::Submitting
                | Self::QueuedReconnect
                | Self::QueuedTransaction { .. }
                | Self::Completed
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DomainDraft {
    name: String,
    if_not_exists: bool,
    pace: DomainPaceChoice,
    period: String,
    skew: String,
    placement: PlacementPolicy,
}

impl Default for DomainDraft {
    fn default() -> Self {
        Self {
            name: String::new(),
            if_not_exists: false,
            pace: DomainPaceChoice::Unpaced,
            period: "1s".to_string(),
            skew: "0s".to_string(),
            placement: PlacementPolicy::Neutral,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct UserDraft {
    name: String,
    password: String,
    if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct ResourceDraft {
    name: String,
    if_not_exists: bool,
}

#[derive(Debug, Error, PartialEq, Eq)]
enum CreateDraftError {
    #[error("Choose what to create")]
    KindRequired,
    #[error("Domain name is invalid")]
    DomainName,
    #[error("Period is invalid")]
    Period,
    #[error("Skew is invalid")]
    Skew,
    #[error("User name is invalid")]
    UserName,
    #[error("Password is required")]
    PasswordRequired,
    #[error("Select a domain before creating a resource")]
    ResourceDomainRequired,
    #[error("Resource name is invalid")]
    ResourceName,
    #[error("Select a domain before creating this entity")]
    ScopedDomainRequired,
    #[error("{0}")]
    Structured(#[from] SchemaDraftError),
    #[error("Canonical NSPL could not be rendered")]
    CanonicalNspl,
}

#[derive(Debug, Clone)]
pub(crate) struct CreateSubmission {
    pub(crate) kind: CreateKind,
    pub(crate) query: String,
    pub(crate) presentation: String,
    pub(crate) domain: Option<DomainName>,
    pub(crate) resource: Option<String>,
    pub(crate) created_domain: Option<DomainName>,
}

#[derive(Debug, Clone)]
pub(crate) struct CreateCommandContext {
    pub(crate) attempt: u64,
    pub(crate) draft_revision: u64,
    pub(crate) kind: CreateKind,
    pub(crate) presentation: String,
    pub(crate) domain: Option<DomainName>,
    pub(crate) resource: Option<String>,
    pub(crate) created_domain: Option<DomainName>,
}

#[derive(Clone, Copy)]
pub(crate) struct CreateSignals {
    open: RwSignal<Option<CreateKind>>,
    return_focus: RwSignal<Option<String>>,
    captured_domain: RwSignal<Option<DomainName>>,
    captured_scopes: RwSignal<BTreeMap<CreateKind, Option<DomainName>>>,
    revision: RwSignal<u64>,
    next_attempt: RwSignal<u64>,
    active_attempt: RwSignal<Option<(u64, u64)>>,
    progress: RwSignal<CreateProgress>,
    validation: RwSignal<Option<String>>,
    domain: RwSignal<DomainDraft>,
    user: RwSignal<UserDraft>,
    resource: RwSignal<ResourceDraft>,
    structured: RwSignal<StructuredDrafts>,
    pace_search: RwSignal<String>,
    placement_search: RwSignal<String>,
    schema_search: RwSignal<String>,
    pace_choices: RwSignal<ChoiceLoad>,
    placement_choices: RwSignal<ChoiceLoad>,
    schema_choices: RwSignal<ChoiceLoad>,
}

impl CreateSignals {
    pub(crate) fn new() -> Self {
        Self {
            open: RwSignal::new(None),
            return_focus: RwSignal::new(None),
            captured_domain: RwSignal::new(None),
            captured_scopes: RwSignal::new(BTreeMap::new()),
            revision: RwSignal::new(0),
            next_attempt: RwSignal::new(0),
            active_attempt: RwSignal::new(None),
            progress: RwSignal::new(CreateProgress::Editing),
            validation: RwSignal::new(None),
            domain: RwSignal::new(DomainDraft::default()),
            user: RwSignal::new(UserDraft::default()),
            resource: RwSignal::new(ResourceDraft::default()),
            structured: RwSignal::new(StructuredDrafts::default()),
            pace_search: RwSignal::new(String::new()),
            placement_search: RwSignal::new(String::new()),
            schema_search: RwSignal::new(String::new()),
            pace_choices: RwSignal::new(ChoiceLoad::Waiting),
            placement_choices: RwSignal::new(ChoiceLoad::Waiting),
            schema_choices: RwSignal::new(ChoiceLoad::Waiting),
        }
    }

    pub(crate) fn open(
        self,
        kind: CreateKind,
        domain: Option<DomainName>,
        return_focus: &'static str,
    ) {
        if kind.domain_scoped() {
            let captured = match self.captured_scopes.get_untracked().get(&kind) {
                Some(captured) => captured.clone(),
                None => {
                    self.captured_scopes.update(|scopes| {
                        scopes.insert(kind, domain.clone());
                    });
                    domain
                }
            };
            self.captured_domain.set(captured);
        } else {
            self.captured_domain.set(None);
        }
        self.return_focus.set(Some(return_focus.to_string()));
        self.progress.set(CreateProgress::Editing);
        self.validation.set(None);
        self.open.set(Some(kind));
        self.advance_revision();
    }

    fn close(self) {
        self.open.set(None);
        self.advance_revision();
        let focus_id = self.return_focus.get_untracked();
        self.return_focus.set(None);
        if let Some(focus_id) = focus_id
            && let Some(document) = web_sys::window().and_then(|window| window.document())
            && let Some(element) = document.get_element_by_id(&focus_id)
            && let Ok(element) = element.dyn_into::<web_sys::HtmlElement>()
        {
            element
                .focus()
                .discarded("the create trigger remains focusable while its dialog closes");
        }
    }

    fn edit(self) {
        self.progress.set(CreateProgress::Editing);
        self.active_attempt.set(None);
        self.validation.set(None);
        self.advance_revision();
    }

    fn change_scope(self, domain: Option<DomainName>) {
        let Some(kind) = self.open.get_untracked() else {
            return;
        };
        if !kind.domain_scoped() {
            return;
        }
        if kind == CreateKind::Branch && self.captured_domain.get_untracked() != domain {
            self.structured.update(|drafts| {
                if drafts.branch.schema.is_some() {
                    drafts.branch.schema_valid = false;
                }
            });
        }
        self.captured_scopes.update(|scopes| {
            scopes.insert(kind, domain.clone());
        });
        self.captured_domain.set(domain);
        self.edit();
    }

    fn advance_revision(self) {
        self.revision.update(|revision| {
            *revision = revision
                .checked_add(1)
                .assured("a browser draft cannot be edited 2^64 times");
        });
    }

    pub(crate) fn begin_submission(self, connected: bool) -> (u64, u64) {
        self.next_attempt.update(|attempt| {
            *attempt = attempt
                .checked_add(1)
                .assured("a browser cannot submit 2^64 create attempts");
        });
        let attempt = self.next_attempt.get_untracked();
        let revision = self.revision.get_untracked();
        self.active_attempt.set(Some((attempt, revision)));
        self.progress.set(if connected {
            CreateProgress::Submitting
        } else {
            CreateProgress::QueuedReconnect
        });
        (attempt, revision)
    }

    pub(crate) fn queued_reconnect(self, attempt: u64, revision: u64) {
        if self.active_attempt.get_untracked() == Some((attempt, revision)) {
            self.progress.set(CreateProgress::QueuedReconnect);
        }
    }

    pub(crate) fn connection_lost(self) {
        if self.progress.get_untracked() == CreateProgress::Submitting {
            self.progress.set(CreateProgress::QueuedReconnect);
        }
    }

    pub(crate) fn completed(self, attempt: u64, revision: u64) -> bool {
        if self.active_attempt.get_untracked() != Some((attempt, revision)) {
            return false;
        }
        self.progress.set(CreateProgress::Completed);
        true
    }

    pub(crate) fn queued_transaction(self, attempt: u64, revision: u64, position: usize) {
        if self.active_attempt.get_untracked() == Some((attempt, revision)) {
            self.progress
                .set(CreateProgress::QueuedTransaction { position });
        }
    }

    pub(crate) fn failed(self, attempt: u64, revision: u64, reason: String) {
        if self.active_attempt.get_untracked() == Some((attempt, revision)) {
            self.progress.set(CreateProgress::Failed(reason));
        }
    }

    pub(crate) fn apply_choice(
        self,
        context: ChoiceRequestContext,
        current_generation: u64,
        outcome: ChoiceOutcome,
    ) {
        let relevant = match context.control {
            ChoiceControl::DomainPace | ChoiceControl::PlacementPolicy => {
                self.open.get_untracked() == Some(CreateKind::Domain)
            }
            ChoiceControl::BranchSchema => self.open.get_untracked() == Some(CreateKind::Branch),
        };
        if !relevant
            || self.revision.get_untracked() != context.draft_revision
            || current_generation != context.session_generation
        {
            return;
        }
        let target = self.choice_load(context.control);
        match outcome.status {
            ChoiceStatus::Ready if outcome.choices.is_empty() && !context.append => {
                target.set(ChoiceLoad::Empty);
            }
            ChoiceStatus::Ready => {
                let mut choices = if context.append {
                    match target.get_untracked() {
                        ChoiceLoad::Ready { choices, .. } => choices,
                        ChoiceLoad::Waiting
                        | ChoiceLoad::Loading
                        | ChoiceLoad::Empty
                        | ChoiceLoad::Failed(_) => Vec::new(),
                    }
                } else {
                    Vec::new()
                };
                choices.extend(outcome.choices);
                target.set(ChoiceLoad::Ready {
                    choices,
                    page_cursor: outcome.page_cursor,
                });
            }
            ChoiceStatus::MissingContext => target.set(ChoiceLoad::Failed(
                "Choose the fields this control depends on".to_string(),
            )),
            ChoiceStatus::StaleContext => {
                target.set(ChoiceLoad::Failed(
                    "The form context changed; retry".to_string(),
                ));
            }
            ChoiceStatus::LookupFailed => {
                target.set(ChoiceLoad::Failed(
                    "Choices could not be loaded".to_string(),
                ));
            }
        }
    }

    pub(crate) fn fail_choice(
        self,
        context: ChoiceRequestContext,
        current_generation: u64,
        reason: String,
    ) {
        if self.revision.get_untracked() == context.draft_revision
            && current_generation == context.session_generation
        {
            self.choice_load(context.control)
                .set(ChoiceLoad::Failed(reason));
        }
    }

    fn choice_load(self, control: ChoiceControl) -> RwSignal<ChoiceLoad> {
        match control {
            ChoiceControl::DomainPace => self.pace_choices,
            ChoiceControl::PlacementPolicy => self.placement_choices,
            ChoiceControl::BranchSchema => self.schema_choices,
        }
    }

    fn submission(self) -> error_stack::Result<CreateSubmission, CreateDraftError> {
        build_submission(
            self.open
                .get_untracked()
                .ok_or_else(|| Report::new(CreateDraftError::KindRequired))?,
            &self.domain.get_untracked(),
            &self.user.get_untracked(),
            &self.resource.get_untracked(),
            &self.structured.get_untracked(),
            self.captured_domain.get_untracked(),
        )
    }
}

fn build_submission(
    kind: CreateKind,
    domain_draft: &DomainDraft,
    user_draft: &UserDraft,
    resource_draft: &ResourceDraft,
    structured_drafts: &StructuredDrafts,
    captured_domain: Option<DomainName>,
) -> error_stack::Result<CreateSubmission, CreateDraftError> {
    match kind {
        CreateKind::Domain => {
            let name = DomainName::parse(domain_draft.name.trim())
                .map_err(|_| Report::new(CreateDraftError::DomainName))?;
            let pace = match domain_draft.pace {
                DomainPaceChoice::Unpaced => DomainPace::Unpaced,
                DomainPaceChoice::Paced => DomainPace::Paced {
                    period: domain_draft
                        .period
                        .trim()
                        .parse::<DomainClockPeriod>()
                        .map_err(|_| Report::new(CreateDraftError::Period))?,
                    skew: domain_draft
                        .skew
                        .trim()
                        .parse::<DomainClockSkew>()
                        .map_err(|_| Report::new(CreateDraftError::Skew))?,
                },
            };
            let statement = Statement::CreateDomain(CreateStatement::new(
                CreateDomain {
                    id: name.clone(),
                    config: DomainConfig {
                        pace,
                        placement: domain_draft.placement,
                    },
                },
                domain_draft.if_not_exists,
            ));
            let query = statement
                .to_canonical_nspl()
                .change_context(CreateDraftError::CanonicalNspl)?;
            Ok(CreateSubmission {
                kind,
                presentation: query.clone(),
                query,
                domain: None,
                resource: None,
                created_domain: Some(name),
            })
        }
        CreateKind::User => {
            let name = UserName::parse(user_draft.name.trim())
                .map_err(|_| Report::new(CreateDraftError::UserName))?;
            if user_draft.password.is_empty() {
                return Err(Report::new(CreateDraftError::PasswordRequired));
            }
            let statement = Statement::CreateUser(CreateStatement::new(
                CreateUser {
                    name: name.clone(),
                    password: user_draft.password.clone(),
                },
                user_draft.if_not_exists,
            ));
            let presentation_statement = Statement::CreateUser(CreateStatement::new(
                CreateUser {
                    name,
                    password: "********".to_string(),
                },
                user_draft.if_not_exists,
            ));
            Ok(CreateSubmission {
                kind,
                query: statement
                    .to_canonical_nspl()
                    .change_context(CreateDraftError::CanonicalNspl)?,
                presentation: presentation_statement
                    .to_canonical_nspl()
                    .change_context(CreateDraftError::CanonicalNspl)?,
                domain: None,
                resource: None,
                created_domain: None,
            })
        }
        CreateKind::Resource => {
            let scope = captured_domain
                .ok_or_else(|| Report::new(CreateDraftError::ResourceDomainRequired))?;
            let name = ResourceName::parse(resource_draft.name.trim())
                .map_err(|_| Report::new(CreateDraftError::ResourceName))?;
            let statement = Statement::CreateResource(CreateStatement::new(
                CreateResource {
                    identifier: name.clone(),
                },
                resource_draft.if_not_exists,
            ));
            let query = statement
                .to_canonical_nspl()
                .change_context(CreateDraftError::CanonicalNspl)?;
            Ok(CreateSubmission {
                kind,
                presentation: query.clone(),
                query,
                domain: Some(scope),
                resource: Some(name.to_string()),
                created_domain: None,
            })
        }
        CreateKind::Schema
        | CreateKind::WireJsonSchema
        | CreateKind::WireCborSchema
        | CreateKind::WireAvroSchema
        | CreateKind::Branch => {
            let scope = captured_domain
                .ok_or_else(|| Report::new(CreateDraftError::ScopedDomainRequired))?;
            let (model, if_not_exists) = match kind {
                CreateKind::Schema => (
                    Model::Schema(
                        structured_drafts
                            .schema
                            .build()
                            .map_err(|error| Report::new(CreateDraftError::Structured(error)))?,
                    ),
                    structured_drafts.schema.if_not_exists,
                ),
                CreateKind::Branch => (
                    Model::Branch(
                        structured_drafts
                            .branch
                            .build()
                            .map_err(|error| Report::new(CreateDraftError::Structured(error)))?,
                    ),
                    structured_drafts.branch.if_not_exists,
                ),
                _ => {
                    let format = kind.wire_format().assured(
                        "the domain-owned structured kind is a wire schema after schema and branch",
                    );
                    let draft = structured_drafts.wire(format);
                    (
                        draft
                            .build(format)
                            .map_err(|error| Report::new(CreateDraftError::Structured(error)))?,
                        draft.if_not_exists,
                    )
                }
            };
            let statement =
                Statement::Create(CreateStatement::new(Box::new(model.into()), if_not_exists));
            let query = statement
                .to_canonical_nspl()
                .change_context(CreateDraftError::CanonicalNspl)?;
            Ok(CreateSubmission {
                kind,
                presentation: query.clone(),
                query,
                domain: Some(scope),
                resource: None,
                created_domain: None,
            })
        }
    }
}

#[component]
pub(crate) fn CreateMenu(
    signals: CreateSignals,
    active_domain: RwSignal<Option<DomainName>>,
) -> impl IntoView {
    let menu_open = RwSignal::new(false);
    let choose = move |kind| {
        signals.open(kind, active_domain.get_untracked(), "global-create-button");
        menu_open.set(false);
    };
    view! {
        <div
            class="menu-wrap create-menu-wrap"
            on:keydown=move |event: ev::KeyboardEvent| {
                if event.key() == "Escape" {
                    menu_open.set(false);
                }
            }
        >
            <button
                id="global-create-button"
                class="create-menu-button"
                type="button"
                aria-haspopup="menu"
                aria-expanded=move || menu_open.get().to_string()
                on:click=move |_| menu_open.update(|open| *open = !*open)
            >
                <span aria-hidden="true">"＋"</span>
                <span>"Create"</span>
            </button>
            <div class="popup-menu create-menu" class:open=move || menu_open.get() role="menu">
                <button type="button" role="menuitem" data-create-kind="domain" on:click=move |_| choose(CreateKind::Domain)>
                    <span>"Domain"</span><em>"Clock and placement"</em>
                </button>
                <button type="button" role="menuitem" data-create-kind="user" on:click=move |_| choose(CreateKind::User)>
                    <span>"User"</span><em>"Login credential"</em>
                </button>
                <button type="button" role="menuitem" data-create-kind="resource" on:click=move |_| choose(CreateKind::Resource)>
                    <span>"Resource"</span><em>"Catalog and upload"</em>
                </button>
                <button type="button" role="menuitem" data-create-kind="schema" on:click=move |_| choose(CreateKind::Schema)>
                    <span>"Schema"</span><em>"Internal record fields"</em>
                </button>
                <button type="button" role="menuitem" data-create-kind="wire-json-schema" on:click=move |_| choose(CreateKind::WireJsonSchema)>
                    <span>"Wire JSON schema"</span><em>"JSON payload fields"</em>
                </button>
                <button type="button" role="menuitem" data-create-kind="wire-cbor-schema" on:click=move |_| choose(CreateKind::WireCborSchema)>
                    <span>"Wire CBOR schema"</span><em>"CBOR payload fields"</em>
                </button>
                <button type="button" role="menuitem" data-create-kind="wire-avro-schema" on:click=move |_| choose(CreateKind::WireAvroSchema)>
                    <span>"Wire AVRO schema"</span><em>"AVRO payload fields"</em>
                </button>
                <button type="button" role="menuitem" data-create-kind="branch" on:click=move |_| choose(CreateKind::Branch)>
                    <span>"Branch"</span><em>"Key schema and lifetime"</em>
                </button>
            </div>
        </div>
    }
}

fn request_choices(
    signals: CreateSignals,
    control: ChoiceControl,
    request_tx: RwSignal<Option<UnboundedSender<ConsoleRequest>>>,
    generation: u64,
    append: bool,
) {
    let (target, dependencies, search, page_size, cursor) = match control {
        ChoiceControl::DomainPace => (
            ChoiceTarget::DomainPace,
            Vec::new(),
            signals.pace_search.get_untracked(),
            2,
            if append {
                page_cursor(signals.pace_choices)
            } else {
                None
            },
        ),
        ChoiceControl::PlacementPolicy => (
            ChoiceTarget::PlacementPolicy,
            vec![ChoiceSelection {
                value: ChoiceValue::DomainPace(signals.domain.get_untracked().pace),
            }],
            signals.placement_search.get_untracked(),
            3,
            if append {
                page_cursor(signals.placement_choices)
            } else {
                None
            },
        ),
        ChoiceControl::BranchSchema => {
            let Some(domain) = signals.captured_domain.get_untracked() else {
                signals.schema_choices.set(ChoiceLoad::Failed(
                    "Select a domain before choosing a schema".to_string(),
                ));
                return;
            };
            (
                ChoiceTarget::Schema,
                vec![ChoiceSelection {
                    value: ChoiceValue::Domain(domain),
                }],
                signals.schema_search.get_untracked(),
                20,
                if append {
                    page_cursor(signals.schema_choices)
                } else {
                    None
                },
            )
        }
    };
    if append && cursor.is_none() {
        return;
    }
    let request = ChoiceLookupRequest::new(target, dependencies, search)
        .with_page(page_size, cursor)
        .assured("the create dialog uses a bounded choice page size");
    let context = ChoiceRequestContext {
        control,
        draft_revision: signals.revision.get_untracked(),
        session_generation: generation,
        append,
    };
    if !append {
        signals.choice_load(control).set(ChoiceLoad::Loading);
    }
    let Some(request_tx) = request_tx.get_untracked() else {
        signals.choice_load(control).set(ChoiceLoad::Failed(
            "The session is not available".to_string(),
        ));
        return;
    };
    if request_tx
        .unbounded_send(ConsoleRequest::Choice { request, context })
        .is_err()
    {
        signals.choice_load(control).set(ChoiceLoad::Failed(
            "The session channel is closed".to_string(),
        ));
    }
}

fn page_cursor(load: RwSignal<ChoiceLoad>) -> Option<String> {
    match load.get_untracked() {
        ChoiceLoad::Ready { page_cursor, .. } => page_cursor,
        ChoiceLoad::Waiting | ChoiceLoad::Loading | ChoiceLoad::Empty | ChoiceLoad::Failed(_) => {
            None
        }
    }
}

#[component]
pub(crate) fn CreateDialog(
    signals: CreateSignals,
    active_domain: RwSignal<Option<DomainName>>,
    connection_state: RwSignal<ConsoleConnectionState>,
    session_generation: RwSignal<u64>,
    request_tx: RwSignal<Option<UnboundedSender<ConsoleRequest>>>,
    submit: impl Fn(CreateSubmission, u64, u64) + Copy + Send + Sync + 'static,
) -> impl IntoView {
    let name_input = NodeRef::<leptos::html::Input>::new();
    Effect::new(move |_| {
        let open = signals.open.get();
        if open.is_some()
            && let Some(input) = name_input.get()
        {
            input
                .focus()
                .discarded("the create name input may already hold focus");
        }
    });
    Effect::new(move |_| {
        let open = signals.open.get();
        let revision = signals.revision.get();
        let generation = session_generation.get();
        let connected = connection_state.get() == ConsoleConnectionState::Connected;
        if open == Some(CreateKind::Domain) && connected {
            request_choices(
                signals,
                ChoiceControl::DomainPace,
                request_tx,
                generation,
                false,
            );
            request_choices(
                signals,
                ChoiceControl::PlacementPolicy,
                request_tx,
                generation,
                false,
            );
        } else if open == Some(CreateKind::Domain) {
            signals.pace_choices.set(ChoiceLoad::Waiting);
            signals.placement_choices.set(ChoiceLoad::Waiting);
        } else if open == Some(CreateKind::Branch) && connected {
            request_choices(
                signals,
                ChoiceControl::BranchSchema,
                request_tx,
                generation,
                false,
            );
        } else if open == Some(CreateKind::Branch) {
            signals.schema_choices.set(ChoiceLoad::Waiting);
        }
        let _ = revision;
    });
    let scope_changed = move || {
        signals.captured_domain.get() != active_domain.get()
            && signals.open.get().is_some_and(CreateKind::domain_scoped)
    };
    let submit_form = move |event: ev::SubmitEvent| {
        event.prevent_default();
        match signals.submission() {
            Ok(submission) => {
                signals.validation.set(None);
                let (attempt, revision) = signals.begin_submission(
                    connection_state.get_untracked() == ConsoleConnectionState::Connected,
                );
                submit(submission, attempt, revision);
            }
            Err(error) => signals
                .validation
                .set(Some(error.current_context().to_string())),
        }
    };
    view! {
        <Show when=move || signals.open.get().is_some() fallback=|| ()>
            <div
                class="modal-scrim create-scrim"
                on:click=move |_| signals.close()
                on:keydown=move |event: ev::KeyboardEvent| {
                    if event.key() == "Escape" {
                        event.prevent_default();
                        signals.close();
                    }
                }
            >
                <section
                    class="create-dialog"
                    role="dialog"
                    aria-modal="true"
                    aria-labelledby="create-dialog-title"
                    on:click=move |event| event.stop_propagation()
                >
                    <header class="create-head">
                        <div>
                            <span>"Create"</span>
                            <h2 id="create-dialog-title">{move || {
                                let label = match signals.open.get() {
                                    Some(kind) => kind.label(),
                                    None => "entity",
                                };
                                format!("Create {label}")
                            }}</h2>
                        </div>
                        <button class="dialog-close create-close" type="button" title="Close" aria-label="Close create dialog" on:click=move |_| signals.close()>"×"</button>
                    </header>
                    <form on:submit=submit_form>
                        <div class="create-scope-row">
                            <span>"Scope"</span>
                            <strong class="create-scope">{move || match signals.open.get() {
                                Some(kind) if kind.domain_scoped() => match signals.captured_domain.get() {
                                    Some(domain) => domain.to_string(),
                                    None => "No domain selected".to_string(),
                                },
                                Some(CreateKind::Domain | CreateKind::User) | None => "Cluster".to_string(),
                                Some(_) => "Cluster".to_string(),
                            }}</strong>
                            <Show when=scope_changed fallback=|| ()>
                                <button
                                    class="create-scope-change"
                                    type="button"
                                    on:click=move |_| {
                                        signals.change_scope(active_domain.get_untracked());
                                    }
                                >
                                    "Use current domain"
                                </button>
                            </Show>
                        </div>

                        <Show when=move || signals.open.get() == Some(CreateKind::Domain) fallback=|| ()>
                            <label class="create-field">
                                <span>"Domain name"</span>
                                <input
                                    node_ref=name_input
                                    class="create-name"
                                    type="text"
                                    autocomplete="off"
                                    prop:value=move || signals.domain.get().name
                                    disabled=move || signals.progress.get().is_pending()
                                    on:input=move |event| {
                                        signals.domain.update(|draft| draft.name = event_target_value(&event));
                                        signals.edit();
                                    }
                                />
                            </label>
                            <ChoiceGroup
                                class_name="create-pace-options"
                                label="Pace"
                                control=ChoiceControl::DomainPace
                                signals=signals
                                request_tx=request_tx
                                session_generation=session_generation
                            />
                            <Show when=move || signals.domain.get().pace == DomainPaceChoice::Paced fallback=|| ()>
                                <div class="create-field-row">
                                    <label class="create-field">
                                        <span>"Period"</span>
                                        <input class="create-period" type="text" prop:value=move || signals.domain.get().period on:input=move |event| {
                                            signals.domain.update(|draft| draft.period = event_target_value(&event));
                                            signals.edit();
                                        } />
                                    </label>
                                    <label class="create-field">
                                        <span>"Skew"</span>
                                        <input class="create-skew" type="text" prop:value=move || signals.domain.get().skew on:input=move |event| {
                                            signals.domain.update(|draft| draft.skew = event_target_value(&event));
                                            signals.edit();
                                        } />
                                    </label>
                                </div>
                            </Show>
                            <ChoiceGroup
                                class_name="create-placement-options"
                                label="Placement"
                                control=ChoiceControl::PlacementPolicy
                                signals=signals
                                request_tx=request_tx
                                session_generation=session_generation
                            />
                        </Show>

                        <Show when=move || signals.open.get() == Some(CreateKind::User) fallback=|| ()>
                            <label class="create-field">
                                <span>"User name"</span>
                                <input node_ref=name_input class="create-name" type="text" autocomplete="off" prop:value=move || signals.user.get().name disabled=move || signals.progress.get().is_pending() on:input=move |event| {
                                    signals.user.update(|draft| draft.name = event_target_value(&event));
                                    signals.edit();
                                } />
                            </label>
                            <label class="create-field">
                                <span>"Password"</span>
                                <input class="create-password" type="password" autocomplete="new-password" prop:value=move || signals.user.get().password disabled=move || signals.progress.get().is_pending() on:input=move |event| {
                                    signals.user.update(|draft| draft.password = event_target_value(&event));
                                    signals.edit();
                                } />
                            </label>
                        </Show>

                        <Show when=move || signals.open.get() == Some(CreateKind::Resource) fallback=|| ()>
                            <label class="create-field">
                                <span>"Resource name"</span>
                                <input node_ref=name_input class="create-name" type="text" autocomplete="off" prop:value=move || signals.resource.get().name disabled=move || signals.progress.get().is_pending() on:input=move |event| {
                                    signals.resource.update(|draft| draft.name = event_target_value(&event));
                                    signals.edit();
                                } />
                            </label>
                        </Show>

                        <Show when=move || signals.open.get() == Some(CreateKind::Schema) fallback=|| ()>
                            <SchemaEditor signals=signals name_input=name_input />
                        </Show>
                        <Show when=move || signals.open.get() == Some(CreateKind::WireJsonSchema) fallback=|| ()>
                            <WireSchemaEditor signals=signals name_input=name_input format=WireFormat::Json />
                        </Show>
                        <Show when=move || signals.open.get() == Some(CreateKind::WireCborSchema) fallback=|| ()>
                            <WireSchemaEditor signals=signals name_input=name_input format=WireFormat::Cbor />
                        </Show>
                        <Show when=move || signals.open.get() == Some(CreateKind::WireAvroSchema) fallback=|| ()>
                            <WireSchemaEditor signals=signals name_input=name_input format=WireFormat::Avro />
                        </Show>
                        <Show when=move || signals.open.get() == Some(CreateKind::Branch) fallback=|| ()>
                            <BranchEditor signals=signals name_input=name_input request_tx=request_tx session_generation=session_generation />
                        </Show>

                        <label class="create-check">
                            <input
                                class="create-if-not-exists"
                                type="checkbox"
                                prop:checked=move || match signals.open.get() {
                                    Some(CreateKind::Domain) => signals.domain.get().if_not_exists,
                                    Some(CreateKind::User) => signals.user.get().if_not_exists,
                                    Some(CreateKind::Resource) => signals.resource.get().if_not_exists,
                                    Some(CreateKind::Schema) => signals.structured.get().schema.if_not_exists,
                                    Some(CreateKind::WireJsonSchema) => signals.structured.get().wire_json.if_not_exists,
                                    Some(CreateKind::WireCborSchema) => signals.structured.get().wire_cbor.if_not_exists,
                                    Some(CreateKind::WireAvroSchema) => signals.structured.get().wire_avro.if_not_exists,
                                    Some(CreateKind::Branch) => signals.structured.get().branch.if_not_exists,
                                    None => false,
                                }
                                disabled=move || signals.progress.get().is_pending()
                                on:change=move |event| {
                                    let checked = event_target_checked(&event);
                                    match signals.open.get_untracked() {
                                        Some(CreateKind::Domain) => signals.domain.update(|draft| draft.if_not_exists = checked),
                                        Some(CreateKind::User) => signals.user.update(|draft| draft.if_not_exists = checked),
                                        Some(CreateKind::Resource) => signals.resource.update(|draft| draft.if_not_exists = checked),
                                        Some(CreateKind::Schema) => signals.structured.update(|draft| draft.schema.if_not_exists = checked),
                                        Some(CreateKind::WireJsonSchema) => signals.structured.update(|draft| draft.wire_json.if_not_exists = checked),
                                        Some(CreateKind::WireCborSchema) => signals.structured.update(|draft| draft.wire_cbor.if_not_exists = checked),
                                        Some(CreateKind::WireAvroSchema) => signals.structured.update(|draft| draft.wire_avro.if_not_exists = checked),
                                        Some(CreateKind::Branch) => signals.structured.update(|draft| draft.branch.if_not_exists = checked),
                                        None => {}
                                    }
                                    signals.edit();
                                }
                            />
                            <span>"If not exists"</span>
                        </label>

                        <div class="create-preview-block">
                            <span>"Canonical NSPL preview"</span>
                            <code class="create-preview">{move || {
                                let _revision = signals.revision.get();
                                match signals.submission() {
                                    Ok(submission) => submission.presentation,
                                    Err(_) => String::new(),
                                }
                            }}</code>
                        </div>
                        <Show when=move || signals.validation.get().is_some() fallback=|| ()>
                            <p class="create-validation" role="alert">{move || signals.validation.get().unwrap_or_default()}</p>
                        </Show>
                        <p class="create-status" aria-live="polite">{move || signals.progress.get().label()}</p>
                        <Show when=move || matches!(signals.progress.get(), CreateProgress::Failed(_)) fallback=|| ()>
                            <p class="create-error" role="alert">{move || match signals.progress.get() {
                                CreateProgress::Failed(reason) => reason,
                                _ => String::new(),
                            }}</p>
                        </Show>
                        <footer class="create-actions">
                            <button class="create-cancel" type="button" on:click=move |_| signals.close()>"Close"</button>
                            <button class="create-submit" type="submit" disabled=move || signals.progress.get().blocks_repeat_submit()>"Create"</button>
                        </footer>
                    </form>
                </section>
            </div>
        </Show>
    }
}

#[component]
fn ChoiceGroup(
    class_name: &'static str,
    label: &'static str,
    control: ChoiceControl,
    signals: CreateSignals,
    request_tx: RwSignal<Option<UnboundedSender<ConsoleRequest>>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let load = signals.choice_load(control);
    let search = match control {
        ChoiceControl::DomainPace => signals.pace_search,
        ChoiceControl::PlacementPolicy => signals.placement_search,
        ChoiceControl::BranchSchema => signals.schema_search,
    };
    view! {
        <fieldset class=format!("create-choice-group {class_name}")>
            <legend>{label}</legend>
            <label class="create-choice-search-label">
                <span class="sr-only">{format!("Search {label}")}</span>
                <input
                    class="create-choice-search"
                    type="search"
                    placeholder=format!("Search {}", label.to_ascii_lowercase())
                    prop:value=move || search.get()
                    on:input=move |event| {
                        search.set(event_target_value(&event));
                        signals.edit();
                    }
                />
            </label>
            <Show when=move || matches!(load.get(), ChoiceLoad::Loading | ChoiceLoad::Waiting) fallback=|| ()>
                <p class="create-choice-state">{move || if load.get() == ChoiceLoad::Waiting { "Waiting for connection" } else { "Loading choices" }}</p>
            </Show>
            <Show when=move || load.get() == ChoiceLoad::Empty fallback=|| ()>
                <p class="create-choice-state">"No choices"</p>
            </Show>
            <Show when=move || matches!(load.get(), ChoiceLoad::Failed(_)) fallback=|| ()>
                <p class="create-choice-state choice-failed" role="alert">{move || match load.get() {
                    ChoiceLoad::Failed(reason) => reason,
                    _ => String::new(),
                }}</p>
            </Show>
            <div class="create-choice-buttons">
                <For
                    each=move || match load.get() {
                        ChoiceLoad::Ready { choices, .. } => choices,
                        ChoiceLoad::Waiting | ChoiceLoad::Loading | ChoiceLoad::Empty | ChoiceLoad::Failed(_) => Vec::new(),
                    }
                    key=|choice| choice.presentation.label.clone()
                    children=move |choice| {
                        let label = choice.presentation.label.clone();
                        let detail = choice.presentation.detail.clone().unwrap_or_default();
                        let selected_value = choice.value.clone();
                        let selected_for_class = selected_value.clone();
                        view! {
                            <button
                                type="button"
                                data-value=label.clone()
                                class:active=move || selected_choice(signals, &selected_for_class)
                                title=detail
                                on:click=move |_| {
                                    select_choice(signals, selected_value.clone());
                                    signals.edit();
                                }
                            >
                                {label.clone()}
                            </button>
                        }
                    }
                />
            </div>
            <Show when=move || matches!(load.get(), ChoiceLoad::Ready { page_cursor: Some(_), .. }) fallback=|| ()>
                <button class="create-choice-more" type="button" on:click=move |_| request_choices(
                    signals,
                    control,
                    request_tx,
                    session_generation.get_untracked(),
                    true,
                )>"Load more"</button>
            </Show>
        </fieldset>
    }
}

fn selected_choice(signals: CreateSignals, value: &ChoiceValue) -> bool {
    match value {
        ChoiceValue::DomainPace(value) => signals.domain.get().pace == *value,
        ChoiceValue::PlacementPolicy(value) => signals.domain.get().placement == *value,
        ChoiceValue::Model(node) if node.kind == ModelKind::Schema => {
            let branch = signals.structured.get();
            branch.branch.schema_valid
                && branch
                    .branch
                    .schema
                    .as_ref()
                    .is_some_and(|schema| schema.as_str() == node.identifier.as_str())
        }
        ChoiceValue::Domain(_) | ChoiceValue::Resource(_) | ChoiceValue::Model(_) => false,
    }
}

fn select_choice(signals: CreateSignals, value: ChoiceValue) {
    match value {
        ChoiceValue::DomainPace(value) => signals.domain.update(|draft| draft.pace = value),
        ChoiceValue::PlacementPolicy(value) => {
            signals.domain.update(|draft| draft.placement = value);
        }
        ChoiceValue::Model(node) if node.kind == ModelKind::Schema => {
            if let Ok(name) = SchemaName::parse(node.identifier.as_str()) {
                signals.structured.update(|drafts| {
                    drafts.branch.schema = Some(name);
                    drafts.branch.schema_valid = true;
                });
            }
        }
        ChoiceValue::Domain(_) | ChoiceValue::Resource(_) | ChoiceValue::Model(_) => {}
    }
}

fn event_target_value(event: &ev::Event) -> String {
    event_target::<web_sys::HtmlInputElement>(event).value()
}

fn event_target_checked(event: &ev::Event) -> bool {
    event_target::<web_sys::HtmlInputElement>(event).checked()
}

#[cfg(test)]
mod tests {
    use futures_channel::mpsc::unbounded;
    use leptos::prelude::{
        GetUntracked as _, Owner, RenderHtml as _, RwSignal, Set as _, Update as _,
    };
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_client_wire::{
        Choice, ChoiceOutcome, ChoicePresentation, ChoiceStatus, ChoiceValue, DomainPaceChoice,
    };
    use nervix_models::{
        AvroType, DomainName, JsonType, ModelKind, ModelName, NodeRef, ParseAsType,
        PlacementPolicy, SchemaName, WireSchemaStrictness,
    };

    use super::{
        super::ConsoleRequest,
        ChoiceControl, ChoiceLoad, ChoiceRequestContext, CreateDialog, CreateDialogProps,
        CreateDraftError, CreateKind, CreateMenu, CreateMenuProps, CreateProgress, CreateSignals,
        DomainDraft, ResourceDraft, UserDraft, build_submission, request_choices,
        schema_draft::{
            SchemaFieldDraft, SchemaTypeDraft, StructuredDrafts, WireFieldDraft, WireFieldType,
        },
        select_choice, selected_choice,
    };

    #[test]
    fn typed_drafts_render_the_canonical_statements_the_dispatcher_sends() {
        let domain = DomainDraft {
            name: "orders".to_string(),
            if_not_exists: true,
            placement: nervix_models::PlacementPolicy::PreferColocation,
            ..DomainDraft::default()
        };
        let submission = build_submission(
            CreateKind::Domain,
            &domain,
            &UserDraft::default(),
            &ResourceDraft::default(),
            &StructuredDrafts::default(),
            None,
        )
        .assured("the domain draft is valid");
        assert_eq!(
            submission.query,
            "CREATE IF NOT EXISTS UNPACED DOMAIN orders PLACEMENT PREFER COLOCATION;"
        );

        let resource = ResourceDraft {
            name: "bundle".to_string(),
            if_not_exists: false,
        };
        let scope = DomainName::parse("orders").assured("the scope is a valid domain name");
        let submission = build_submission(
            CreateKind::Resource,
            &DomainDraft::default(),
            &UserDraft::default(),
            &resource,
            &StructuredDrafts::default(),
            Some(scope.clone()),
        )
        .assured("the resource draft and scope are valid");
        assert_eq!(submission.query, "CREATE RESOURCE bundle;");
        assert_eq!(submission.domain, Some(scope));
    }

    #[test]
    fn credential_presentation_is_masked_while_the_submitted_statement_keeps_the_secret() {
        let user = UserDraft {
            name: "operator".to_string(),
            password: "it's-secret".to_string(),
            if_not_exists: false,
        };
        let submission = build_submission(
            CreateKind::User,
            &DomainDraft::default(),
            &user,
            &ResourceDraft::default(),
            &StructuredDrafts::default(),
            None,
        )
        .assured("the user draft is valid");
        assert!(submission.query.contains("it's-secret"));
        assert!(!submission.presentation.contains("it's-secret"));
        assert_eq!(
            submission.presentation,
            "CREATE USER operator WITH PASSWORD '********';"
        );
    }

    #[test]
    fn a_resource_requires_the_scope_captured_when_the_dialog_opened() {
        let resource = ResourceDraft {
            name: "bundle".to_string(),
            if_not_exists: false,
        };
        let error = build_submission(
            CreateKind::Resource,
            &DomainDraft::default(),
            &UserDraft::default(),
            &resource,
            &StructuredDrafts::default(),
            None,
        )
        .expect_err("resource creation needs a selected domain");
        assert_eq!(
            error.current_context(),
            &CreateDraftError::ResourceDomainRequired
        );
    }

    #[test]
    fn independent_choice_controls_discard_older_drafts_and_session_generations() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            signals.open(CreateKind::Domain, None, "trigger");
            let revision = signals.revision.get_untracked();
            let pace_context = ChoiceRequestContext {
                control: ChoiceControl::DomainPace,
                draft_revision: revision,
                session_generation: 7,
                append: false,
            };
            let placement_context = ChoiceRequestContext {
                control: ChoiceControl::PlacementPolicy,
                ..pace_context
            };
            let outcome = |value, label: &str| ChoiceOutcome {
                status: ChoiceStatus::Ready,
                choices: vec![Choice {
                    value,
                    presentation: ChoicePresentation {
                        label: label.to_string(),
                        detail: None,
                        group: None,
                    },
                }],
                page_cursor: None,
            };

            signals.apply_choice(
                placement_context,
                7,
                outcome(
                    ChoiceValue::PlacementPolicy(PlacementPolicy::Neutral),
                    "NEUTRAL",
                ),
            );
            signals.apply_choice(
                pace_context,
                7,
                outcome(
                    ChoiceValue::DomainPace(DomainPaceChoice::Unpaced),
                    "UNPACED",
                ),
            );
            assert!(matches!(
                signals.pace_choices.get_untracked(),
                ChoiceLoad::Ready { choices, .. } if choices[0].presentation.label == "UNPACED"
            ));
            assert!(matches!(
                signals.placement_choices.get_untracked(),
                ChoiceLoad::Ready { choices, .. } if choices[0].presentation.label == "NEUTRAL"
            ));

            signals.edit();
            signals.apply_choice(
                pace_context,
                7,
                outcome(ChoiceValue::DomainPace(DomainPaceChoice::Paced), "PACED"),
            );
            signals.apply_choice(
                ChoiceRequestContext {
                    draft_revision: signals.revision.get_untracked(),
                    ..placement_context
                },
                8,
                outcome(
                    ChoiceValue::PlacementPolicy(PlacementPolicy::RequireColocation),
                    "REQUIRE COLOCATION",
                ),
            );
            assert!(matches!(
                signals.pace_choices.get_untracked(),
                ChoiceLoad::Ready { choices, .. } if choices[0].presentation.label == "UNPACED"
            ));
            assert!(matches!(
                signals.placement_choices.get_untracked(),
                ChoiceLoad::Ready { choices, .. } if choices[0].presentation.label == "NEUTRAL"
            ));
        });
    }

    #[test]
    fn a_finished_attempt_requires_an_edit_before_it_can_be_submitted_again() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            signals.open(CreateKind::Resource, None, "trigger");
            let (attempt, revision) = signals.begin_submission(true);
            assert!(signals.progress.get_untracked().blocks_repeat_submit());

            signals.queued_transaction(attempt, revision, 1);
            assert_eq!(
                signals.progress.get_untracked(),
                CreateProgress::QueuedTransaction { position: 1 }
            );
            assert!(signals.progress.get_untracked().blocks_repeat_submit());

            signals.edit();
            assert_eq!(signals.progress.get_untracked(), CreateProgress::Editing);
            assert!(!signals.progress.get_untracked().blocks_repeat_submit());
        });
    }

    #[test]
    fn submission_state_changes_only_for_the_active_attempt() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            signals.open(CreateKind::User, None, "trigger");
            let (attempt, revision) = signals.begin_submission(true);
            assert_eq!(signals.progress.get_untracked().label(), "Submitting");

            assert!(!signals.completed(0, revision));
            signals.queued_reconnect(0, revision);
            assert_eq!(signals.progress.get_untracked(), CreateProgress::Submitting);

            signals.connection_lost();
            assert_eq!(
                signals.progress.get_untracked().label(),
                "Queued until reconnect"
            );
            signals.queued_reconnect(attempt, revision);
            assert!(signals.completed(attempt, revision));
            assert_eq!(signals.progress.get_untracked().label(), "Completed");

            signals.queued_transaction(attempt, revision, 2);
            assert_eq!(
                signals.progress.get_untracked().label(),
                "Queued in transaction · position 2"
            );
        });
    }

    #[test]
    fn choice_outcomes_cover_empty_append_and_each_failure_state() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            signals.open(CreateKind::Domain, None, "trigger");
            let revision = signals.revision.get_untracked();
            let context = ChoiceRequestContext {
                control: ChoiceControl::DomainPace,
                draft_revision: revision,
                session_generation: 3,
                append: false,
            };
            let outcome = |status, choices, page_cursor| ChoiceOutcome {
                status,
                choices,
                page_cursor,
            };
            signals.apply_choice(context, 3, outcome(ChoiceStatus::Ready, Vec::new(), None));
            assert_eq!(signals.pace_choices.get_untracked(), ChoiceLoad::Empty);

            let choice = |pace, label: &str| Choice {
                value: ChoiceValue::DomainPace(pace),
                presentation: ChoicePresentation {
                    label: label.to_string(),
                    detail: None,
                    group: None,
                },
            };
            signals.apply_choice(
                context,
                3,
                outcome(
                    ChoiceStatus::Ready,
                    vec![choice(DomainPaceChoice::Unpaced, "UNPACED")],
                    Some("next".to_string()),
                ),
            );
            signals.apply_choice(
                ChoiceRequestContext {
                    append: true,
                    ..context
                },
                3,
                outcome(
                    ChoiceStatus::Ready,
                    vec![choice(DomainPaceChoice::Paced, "PACED")],
                    None,
                ),
            );
            assert!(matches!(
                signals.pace_choices.get_untracked(),
                ChoiceLoad::Ready { choices, page_cursor: None } if choices.len() == 2
            ));

            for (status, message) in [
                (
                    ChoiceStatus::MissingContext,
                    "Choose the fields this control depends on",
                ),
                (
                    ChoiceStatus::StaleContext,
                    "The form context changed; retry",
                ),
                (ChoiceStatus::LookupFailed, "Choices could not be loaded"),
            ] {
                signals.apply_choice(context, 3, outcome(status, Vec::new(), None));
                assert_eq!(
                    signals.pace_choices.get_untracked(),
                    ChoiceLoad::Failed(message.to_string())
                );
            }
            signals.fail_choice(context, 3, "transport ended".to_string());
            assert_eq!(
                signals.pace_choices.get_untracked(),
                ChoiceLoad::Failed("transport ended".to_string())
            );
            signals.fail_choice(context, 4, "stale transport".to_string());
            assert_eq!(
                signals.pace_choices.get_untracked(),
                ChoiceLoad::Failed("transport ended".to_string())
            );
        });
    }

    #[test]
    fn paced_domain_and_password_validation_report_the_owning_field() {
        let mut domain = DomainDraft {
            name: "orders".to_string(),
            pace: DomainPaceChoice::Paced,
            period: "2s".to_string(),
            skew: "500ms".to_string(),
            ..DomainDraft::default()
        };
        let submission = build_submission(
            CreateKind::Domain,
            &domain,
            &UserDraft::default(),
            &ResourceDraft::default(),
            &StructuredDrafts::default(),
            None,
        )
        .assured("the paced domain draft is valid");
        assert!(submission.query.contains("PACED"));
        assert!(submission.query.contains("PERIOD 2s"));

        domain.period = "soon".to_string();
        assert_eq!(
            build_submission(
                CreateKind::Domain,
                &domain,
                &UserDraft::default(),
                &ResourceDraft::default(),
                &StructuredDrafts::default(),
                None,
            )
            .expect_err("an invalid period is rejected")
            .current_context(),
            &CreateDraftError::Period
        );
        domain.period = "2s".to_string();
        domain.skew = "later".to_string();
        assert_eq!(
            build_submission(
                CreateKind::Domain,
                &domain,
                &UserDraft::default(),
                &ResourceDraft::default(),
                &StructuredDrafts::default(),
                None,
            )
            .expect_err("an invalid skew is rejected")
            .current_context(),
            &CreateDraftError::Skew
        );

        let user = UserDraft {
            name: "operator".to_string(),
            password: String::new(),
            if_not_exists: false,
        };
        assert_eq!(
            build_submission(
                CreateKind::User,
                &DomainDraft::default(),
                &user,
                &ResourceDraft::default(),
                &StructuredDrafts::default(),
                None,
            )
            .expect_err("an empty password is rejected")
            .current_context(),
            &CreateDraftError::PasswordRequired
        );
    }

    #[test]
    fn choice_requests_preserve_control_context_and_report_unavailable_channels() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            signals.open(CreateKind::Domain, None, "trigger");
            signals.pace_search.set("wall".to_string());
            let (sender, mut receiver) = unbounded();
            let request_tx = RwSignal::new(Some(sender));
            request_choices(signals, ChoiceControl::DomainPace, request_tx, 8, false);
            let ConsoleRequest::Choice { request, context } = receiver
                .try_recv()
                .assured("the choice channel remains open")
            else {
                panic!("the create dialog sends a typed choice request");
            };
            assert_eq!(
                request.target(),
                nervix_client_wire::ChoiceTarget::DomainPace
            );
            assert_eq!(request.search(), "wall");
            assert!(request.dependencies().is_empty());
            assert_eq!(context.session_generation, 8);
            assert!(!context.append);

            signals.pace_choices.set(ChoiceLoad::Ready {
                choices: Vec::new(),
                page_cursor: Some("page-two".to_string()),
            });
            request_choices(signals, ChoiceControl::DomainPace, request_tx, 8, true);
            let ConsoleRequest::Choice { request, context } = receiver
                .try_recv()
                .assured("the choice channel remains open")
            else {
                panic!("the create dialog sends a typed choice request");
            };
            assert_eq!(request.page_cursor(), Some("page-two"));
            assert!(context.append);

            request_choices(
                signals,
                ChoiceControl::PlacementPolicy,
                request_tx,
                8,
                false,
            );
            let ConsoleRequest::Choice { request, .. } = receiver
                .try_recv()
                .assured("the choice channel remains open")
            else {
                panic!("the create dialog sends a typed choice request");
            };
            assert_eq!(request.dependencies().len(), 1);

            let unavailable = RwSignal::new(None);
            request_choices(signals, ChoiceControl::DomainPace, unavailable, 8, false);
            assert_eq!(
                signals.pace_choices.get_untracked(),
                ChoiceLoad::Failed("The session is not available".to_string())
            );

            let (closed_sender, closed_receiver) = unbounded();
            drop(closed_receiver);
            request_choices(
                signals,
                ChoiceControl::DomainPace,
                RwSignal::new(Some(closed_sender)),
                8,
                false,
            );
            assert_eq!(
                signals.pace_choices.get_untracked(),
                ChoiceLoad::Failed("The session channel is closed".to_string())
            );

            signals.pace_choices.set(ChoiceLoad::Empty);
            request_choices(signals, ChoiceControl::DomainPace, request_tx, 8, true);
            assert!(receiver.try_recv().is_err());
        });
    }

    #[test]
    fn typed_choice_selection_updates_only_supported_create_fields() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            select_choice(signals, ChoiceValue::DomainPace(DomainPaceChoice::Paced));
            select_choice(
                signals,
                ChoiceValue::PlacementPolicy(PlacementPolicy::RequireColocation),
            );
            assert_eq!(signals.domain.get_untracked().pace, DomainPaceChoice::Paced);
            assert_eq!(
                signals.domain.get_untracked().placement,
                PlacementPolicy::RequireColocation
            );
            let domain = DomainName::parse("orders").assured("the test domain is valid");
            select_choice(signals, ChoiceValue::Domain(domain));
            assert_eq!(signals.domain.get_untracked().pace, DomainPaceChoice::Paced);
        });
    }

    #[test]
    fn branch_scope_change_keeps_the_draft_and_requires_reselecting_its_schema() {
        Owner::new().with(|| {
            let first = DomainName::parse("first").assured("valid domain");
            let second = DomainName::parse("second").assured("valid domain");
            let schema = SchemaName::parse("tenant_key").assured("valid schema");
            let signals = CreateSignals::new();

            signals.change_scope(Some(first.clone()));
            assert_eq!(signals.captured_domain.get_untracked(), None);
            signals.open(CreateKind::Domain, None, "trigger");
            signals.change_scope(Some(first.clone()));
            assert_eq!(signals.captured_domain.get_untracked(), None);

            signals.open(CreateKind::Branch, Some(first.clone()), "trigger");
            signals.structured.update(|drafts| {
                drafts.branch.name = "by_tenant".to_string();
                drafts.branch.schema = Some(schema.clone());
                drafts.branch.schema_valid = true;
                drafts.branch.ttl = "5m".to_string();
            });
            signals.change_scope(Some(first.clone()));
            assert!(signals.structured.get_untracked().branch.schema_valid);

            signals.change_scope(Some(second.clone()));
            let retained = signals.structured.get_untracked().branch;
            assert_eq!(retained.schema, Some(schema));
            assert!(!retained.schema_valid);
            assert_eq!(retained.ttl, "5m");
            assert_eq!(
                signals.captured_domain.get_untracked(),
                Some(second.clone())
            );

            signals.open(CreateKind::Branch, Some(first), "trigger");
            assert_eq!(signals.captured_domain.get_untracked(), Some(second));
            assert_eq!(signals.structured.get_untracked().branch.name, "by_tenant");
        });
    }

    #[test]
    fn branch_schema_requests_bind_the_captured_domain_and_page_cursor() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            let (sender, mut receiver) = unbounded();
            let request_tx = RwSignal::new(Some(sender));
            signals.open(CreateKind::Branch, None, "trigger");
            request_choices(signals, ChoiceControl::BranchSchema, request_tx, 9, false);
            assert_eq!(
                signals.schema_choices.get_untracked(),
                ChoiceLoad::Failed("Select a domain before choosing a schema".to_string())
            );
            assert!(receiver.try_recv().is_err());

            let domain = DomainName::parse("orders").assured("valid domain");
            signals.change_scope(Some(domain.clone()));
            signals.schema_search.set("tenant".to_string());
            request_choices(signals, ChoiceControl::BranchSchema, request_tx, 9, false);
            let ConsoleRequest::Choice { request, context } = receiver
                .try_recv()
                .assured("the schema picker requests a typed page")
            else {
                panic!("the schema picker must send a choice request");
            };
            assert_eq!(request.target(), nervix_client_wire::ChoiceTarget::Schema);
            assert_eq!(request.search(), "tenant");
            assert_eq!(request.page_size(), 20);
            assert_eq!(request.dependencies()[0].value, ChoiceValue::Domain(domain));
            assert_eq!(context.control, ChoiceControl::BranchSchema);

            signals.schema_choices.set(ChoiceLoad::Ready {
                choices: Vec::new(),
                page_cursor: Some("next-page".to_string()),
            });
            request_choices(signals, ChoiceControl::BranchSchema, request_tx, 9, true);
            let ConsoleRequest::Choice { request, context } = receiver
                .try_recv()
                .assured("a page cursor requests the next schema page")
            else {
                panic!("the schema picker must send a choice request");
            };
            assert_eq!(request.page_cursor(), Some("next-page"));
            assert!(context.append);

            signals.schema_choices.set(ChoiceLoad::Empty);
            request_choices(signals, ChoiceControl::BranchSchema, request_tx, 9, true);
            assert!(receiver.try_recv().is_err());
        });
    }

    #[test]
    fn branch_schema_selection_is_exact_and_does_not_clear_an_invalid_reference() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            let node = NodeRef::new(
                ModelKind::Schema,
                ModelName::parse("tenant_key").assured("valid model name"),
            );
            let choice = ChoiceValue::Model(node);
            assert!(!selected_choice(signals, &choice));
            select_choice(signals, choice.clone());
            assert!(selected_choice(signals, &choice));

            signals
                .structured
                .update(|drafts| drafts.branch.schema_valid = false);
            assert!(!selected_choice(signals, &choice));
            assert_eq!(
                signals.structured.get_untracked().branch.schema,
                Some(SchemaName::parse("tenant_key").assured("valid schema"))
            );

            select_choice(
                signals,
                ChoiceValue::Model(NodeRef::new(
                    ModelKind::Relay,
                    ModelName::parse("tenant_key").assured("valid model name"),
                )),
            );
            assert!(!signals.structured.get_untracked().branch.schema_valid);
        });
    }

    #[test]
    fn creation_components_render_each_typed_draft_and_accessible_status() {
        super::super::initialize_test_executor();
        Owner::new().with(|| {
            let scope = DomainName::parse("orders").assured("the test scope is valid");
            let active_domain = RwSignal::new(Some(scope.clone()));
            let connection_state = RwSignal::new(super::super::ConsoleConnectionState::Waiting);
            let generation = RwSignal::new(4);
            let request_tx = RwSignal::new(None);
            let signals = CreateSignals::new();

            let menu = CreateMenu(
                CreateMenuProps::builder()
                    .signals(signals)
                    .active_domain(active_domain)
                    .build(),
            );
            let menu_markup = menu.to_html();
            assert!(menu_markup.contains("global-create-button"));
            assert!(menu_markup.contains("Create"));
            assert!(menu_markup.contains("Resource"));

            signals.domain.update(|draft| {
                draft.name = "analytics".to_string();
                draft.if_not_exists = true;
                draft.placement = PlacementPolicy::PreferColocation;
            });
            signals.open(CreateKind::Domain, None, "global-create-button");
            let render = || {
                let dialog = CreateDialog(
                    CreateDialogProps::builder()
                        .signals(signals)
                        .active_domain(active_domain)
                        .connection_state(connection_state)
                        .session_generation(generation)
                        .request_tx(request_tx)
                        .submit(|_, _, _| {})
                        .build(),
                );
                any_spawner::Executor::poll_local();
                if signals.open.get_untracked() == Some(CreateKind::Domain) {
                    signals.pace_choices.set(ChoiceLoad::Ready {
                        choices: vec![Choice {
                            value: ChoiceValue::DomainPace(DomainPaceChoice::Unpaced),
                            presentation: ChoicePresentation {
                                label: "UNPACED".to_string(),
                                detail: Some("No domain clock".to_string()),
                                group: None,
                            },
                        }],
                        page_cursor: Some("more-pace".to_string()),
                    });
                    signals.placement_choices.set(ChoiceLoad::Ready {
                        choices: vec![Choice {
                            value: ChoiceValue::PlacementPolicy(PlacementPolicy::PreferColocation),
                            presentation: ChoicePresentation {
                                label: "PREFER COLOCATION".to_string(),
                                detail: None,
                                group: None,
                            },
                        }],
                        page_cursor: None,
                    });
                }
                dialog.to_html()
            };
            let domain_markup = render();
            assert!(domain_markup.contains("role=\"dialog\""));
            assert!(domain_markup.contains("Create domain"));
            assert!(domain_markup.contains(
                "CREATE IF NOT EXISTS UNPACED DOMAIN analytics PLACEMENT PREFER COLOCATION;"
            ));
            assert!(domain_markup.contains("UNPACED"));
            assert!(domain_markup.contains("Load more"));

            signals.user.update(|draft| {
                draft.name = "operator".to_string();
                draft.password = "never-render-me".to_string();
            });
            signals.open(CreateKind::User, None, "global-create-button");
            let user_markup = render();
            assert!(user_markup.contains("Create user"));
            assert!(user_markup.contains("********"));
            assert!(!user_markup.contains("never-render-me"));

            signals
                .resource
                .update(|draft| draft.name = "bundle".to_string());
            signals.open(
                CreateKind::Resource,
                Some(scope.clone()),
                "sidebar-create-resource",
            );
            let (attempt, revision) = signals.begin_submission(false);
            let resource_markup = render();
            assert!(resource_markup.contains("Create resource"));
            assert!(resource_markup.contains("Queued until reconnect"));
            assert!(resource_markup.contains("CREATE RESOURCE bundle;"));

            signals.failed(attempt, revision, "resource already exists".to_string());
            let failed_markup = render();
            assert!(failed_markup.contains("Failed"));
            assert!(failed_markup.contains("resource already exists"));

            signals.structured.update(|drafts| {
                drafts.schema.name = "visual_record".to_string();
                drafts.schema.fields.push(SchemaFieldDraft {
                    name: "tenant".to_string(),
                    ty: SchemaTypeDraft {
                        scalar: Some(ParseAsType::U32),
                        ..SchemaTypeDraft::default()
                    },
                    optional: true,
                    sensitive: true,
                });
            });
            signals.open(
                CreateKind::Schema,
                Some(scope.clone()),
                "global-create-button",
            );
            let schema_markup = render();
            assert!(schema_markup.contains("Create schema"));
            assert!(schema_markup.contains("tenant U32 OPTIONAL SENSITIVE"));
            assert!(schema_markup.contains("Wrap in fixed array"));

            for (kind, field_type, format_name) in [
                (
                    CreateKind::WireJsonSchema,
                    WireFieldType::Json(JsonType::String),
                    "JSON",
                ),
                (
                    CreateKind::WireCborSchema,
                    WireFieldType::Json(JsonType::String),
                    "CBOR",
                ),
                (
                    CreateKind::WireAvroSchema,
                    WireFieldType::Avro(AvroType::String),
                    "AVRO",
                ),
            ] {
                signals.structured.update(|drafts| {
                    let wire =
                        drafts.wire_mut(kind.wire_format().assured("a wire kind has a format"));
                    wire.name = "visual_wire".to_string();
                    wire.mode = Some(WireSchemaStrictness::Loose);
                    wire.fields.push(WireFieldDraft {
                        name: "payload".to_string(),
                        ty: Some(field_type),
                        optional: true,
                    });
                });
                signals.open(kind, Some(scope.clone()), "global-create-button");
                let wire_markup = render();
                assert!(
                    wire_markup.contains(&format!("CREATE WIRE {format_name} SCHEMA visual_wire"))
                );
                assert!(wire_markup.contains("payload STRING OPTIONAL"));
            }

            signals.structured.update(|drafts| {
                drafts.branch.name = "by_tenant".to_string();
                drafts.branch.schema =
                    Some(SchemaName::parse("visual_record").assured("valid name"));
                drafts.branch.schema_valid = true;
                drafts.branch.ttl = "5m".to_string();
            });
            signals.open(CreateKind::Branch, Some(scope), "global-create-button");
            let branch_markup = render();
            assert!(branch_markup.contains("Create branch"));
            assert!(branch_markup.contains("Selected schema: visual_record"));
            assert!(branch_markup.contains("CREATE BRANCH by_tenant SCHEMA visual_record TTL 5m;"));
        });
    }
}
