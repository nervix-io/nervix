//! Structured creation of the entities operators configure before and around a graph.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser-local create drafts, typed choice presentation, canonical previews, dialog
//!   accessibility, the state shown for one submission, and how a completed draft is dispatched:
//!   as a durable command or as a session subscription.
//! - **Depends on.** Public semantic models, the canonical client statement renderer, the session
//!   choice contract, and the console request dispatcher owned by the parent module.
//! - **Must not know.** Registry state, command execution internals, subscription tabs, or how the
//!   server resolves a choice.

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
    CanonicalNsplError, CreateDomain, CreateResource, CreateStatement, CreateSubscription,
    CreateUser, DomainClockPeriod, DomainClockSkew, DomainConfig, DomainName, DomainPace, Model,
    ModelKind, ModelName, PlacementPolicy, RelayName, RequestedResourceVersion, ResourceName,
    Statement, SubscriptionName, UserName,
};
use nervix_nspl::client_statement::ClientStatement;
use nervix_recovery::Discarded as _;
use thiserror::Error;
use wasm_bindgen::JsCast as _;

use super::{ConsoleConnectionState, ConsoleRequest};

mod choice_group;
mod codec_draft;
mod codec_editor;
mod relay_draft;
mod relay_editor;
mod resource_binding_draft;
mod resource_binding_editor;
mod schema_draft;
mod schema_editor;
mod signaling_draft;
mod signaling_editor;
mod subscription_draft;
mod subscription_editor;
#[cfg(test)]
mod visual_forms_tests;

use choice_group::ChoiceGroup;
#[cfg(test)]
use choice_group::{ChoiceGroupProps, select_choice, selected_choice};
use codec_draft::{CodecDraft, CodecDraftError, CodecFormatDraft, CodecFormatKind};
use codec_editor::CodecEditor;
use relay_draft::{RelayDraft, RelayDraftError};
use relay_editor::RelayEditor;
use schema_draft::{SchemaDraftError, StructuredDrafts, WireFormat};
use schema_editor::{BranchEditor, SchemaEditor, WireSchemaEditor};
use signaling_draft::{SignalingDraft, SignalingDraftError, SignalingFormatDraft};
use signaling_editor::SignalingEditor;
use subscription_draft::{SubscriptionDraft, SubscriptionDraftError};
use subscription_editor::SubscriptionEditor;

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
    Relay,
    Subscription,
    Codec,
    SignalingProtocol,
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
            Self::Relay => "relay",
            Self::Subscription => "subscription",
            Self::Codec => "codec",
            Self::SignalingProtocol => "signaling protocol",
        }
    }

    fn domain_scoped(self) -> bool {
        !matches!(self, Self::Domain | Self::User)
    }

    /// Whether the created statement takes `IF NOT EXISTS`. A session subscription is not a stored
    /// entity, so its statement has no such modifier.
    fn takes_if_not_exists(self) -> bool {
        match self {
            Self::Subscription => false,
            Self::Domain
            | Self::User
            | Self::Resource
            | Self::Schema
            | Self::WireJsonSchema
            | Self::WireCborSchema
            | Self::WireAvroSchema
            | Self::Branch
            | Self::Relay
            | Self::Codec
            | Self::SignalingProtocol => true,
        }
    }

    fn wire_format(self) -> Option<WireFormat> {
        match self {
            Self::WireJsonSchema => Some(WireFormat::Json),
            Self::WireCborSchema => Some(WireFormat::Cbor),
            Self::WireAvroSchema => Some(WireFormat::Avro),
            Self::Domain
            | Self::User
            | Self::Resource
            | Self::Schema
            | Self::Branch
            | Self::Relay
            | Self::Subscription
            | Self::Codec
            | Self::SignalingProtocol => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ChoiceControl {
    DomainPace,
    PlacementPolicy,
    BranchSchema,
    RelaySchema,
    RelayBranch,
    SubscriptionRelay,
    /// Inserts typed references to the selected relay's fields into the subscription filter.
    SubscriptionField,
    CodecSchema,
    CodecWireSchema,
    CodecResource,
    CodecVersion,
    SignalingResource,
    SignalingVersion,
}

impl ChoiceControl {
    /// The form whose draft this control edits.
    fn form(self) -> CreateKind {
        match self {
            Self::DomainPace | Self::PlacementPolicy => CreateKind::Domain,
            Self::BranchSchema => CreateKind::Branch,
            Self::RelaySchema | Self::RelayBranch => CreateKind::Relay,
            Self::SubscriptionRelay | Self::SubscriptionField => CreateKind::Subscription,
            Self::CodecSchema
            | Self::CodecWireSchema
            | Self::CodecResource
            | Self::CodecVersion => CreateKind::Codec,
            Self::SignalingResource | Self::SignalingVersion => CreateKind::SignalingProtocol,
        }
    }
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
    MissingPrerequisite(&'static str),
    StaleContext,
    Failed(String),
}

/// The search text and the loaded choices of one structured control.
#[derive(Clone, Copy)]
struct ChoiceControlSignals {
    search: RwSignal<String>,
    load: RwSignal<ChoiceLoad>,
}

impl ChoiceControlSignals {
    fn new() -> Self {
        Self {
            search: RwSignal::new(String::new()),
            load: RwSignal::new(ChoiceLoad::Waiting),
        }
    }
}

/// Every structured control's signals, one set per control so each owns its own latest request.
#[derive(Clone, Copy)]
struct ChoiceControls {
    domain_pace: ChoiceControlSignals,
    placement_policy: ChoiceControlSignals,
    branch_schema: ChoiceControlSignals,
    relay_schema: ChoiceControlSignals,
    relay_branch: ChoiceControlSignals,
    subscription_relay: ChoiceControlSignals,
    subscription_field: ChoiceControlSignals,
    codec_schema: ChoiceControlSignals,
    codec_wire_schema: ChoiceControlSignals,
    codec_resource: ChoiceControlSignals,
    codec_version: ChoiceControlSignals,
    signaling_resource: ChoiceControlSignals,
    signaling_version: ChoiceControlSignals,
}

impl ChoiceControls {
    fn new() -> Self {
        Self {
            domain_pace: ChoiceControlSignals::new(),
            placement_policy: ChoiceControlSignals::new(),
            branch_schema: ChoiceControlSignals::new(),
            relay_schema: ChoiceControlSignals::new(),
            relay_branch: ChoiceControlSignals::new(),
            subscription_relay: ChoiceControlSignals::new(),
            subscription_field: ChoiceControlSignals::new(),
            codec_schema: ChoiceControlSignals::new(),
            codec_wire_schema: ChoiceControlSignals::new(),
            codec_resource: ChoiceControlSignals::new(),
            codec_version: ChoiceControlSignals::new(),
            signaling_resource: ChoiceControlSignals::new(),
            signaling_version: ChoiceControlSignals::new(),
        }
    }

    fn of(self, control: ChoiceControl) -> ChoiceControlSignals {
        match control {
            ChoiceControl::DomainPace => self.domain_pace,
            ChoiceControl::PlacementPolicy => self.placement_policy,
            ChoiceControl::BranchSchema => self.branch_schema,
            ChoiceControl::RelaySchema => self.relay_schema,
            ChoiceControl::RelayBranch => self.relay_branch,
            ChoiceControl::SubscriptionRelay => self.subscription_relay,
            ChoiceControl::SubscriptionField => self.subscription_field,
            ChoiceControl::CodecSchema => self.codec_schema,
            ChoiceControl::CodecWireSchema => self.codec_wire_schema,
            ChoiceControl::CodecResource => self.codec_resource,
            ChoiceControl::CodecVersion => self.codec_version,
            ChoiceControl::SignalingResource => self.signaling_resource,
            ChoiceControl::SignalingVersion => self.signaling_version,
        }
    }
}

/// A reference a picker selected, kept visible after the context it was chosen in changes.
///
/// A draft that moves to another domain keeps showing the name, but no longer offers it to its
/// Model until the operator selects it again.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SelectedReference<N> {
    name: N,
    /// Whether the draft still holds the captured domain the reference was selected in.
    current: bool,
}

impl<N> SelectedReference<N> {
    fn chosen(name: N) -> Self {
        Self {
            name,
            current: true,
        }
    }

    fn name(&self) -> &N {
        &self.name
    }

    fn is_current(&self) -> bool {
        self.current
    }

    /// The selected name, while it still belongs to the draft's context.
    fn current_name(&self) -> Option<&N> {
        if self.current { Some(&self.name) } else { None }
    }

    fn invalidate(&mut self) {
        self.current = false;
    }
}

impl<N> SelectedReference<N>
where
    for<'a> &'a N: Into<ModelName>,
{
    /// Whether this is a current selection of `node`, a Model of `kind`.
    fn selects(&self, kind: ModelKind, node: &nervix_models::NodeRef) -> bool {
        self.current && *node == nervix_models::NodeRef::new(kind, &self.name)
    }
}

/// The question a control asks the session, or why it cannot ask one yet.
struct ChoiceQuery {
    target: ChoiceTarget,
    dependencies: Vec<ChoiceSelection>,
    page_size: u16,
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

impl DomainDraft {
    fn submission(&self) -> error_stack::Result<CreateSubmission, CreateDraftError> {
        let name = DomainName::parse(self.name.trim())
            .map_err(|_| Report::new(CreateDraftError::DomainName))?;
        let pace = match self.pace {
            DomainPaceChoice::Unpaced => DomainPace::Unpaced,
            DomainPaceChoice::Paced => DomainPace::Paced {
                period: self
                    .period
                    .trim()
                    .parse::<DomainClockPeriod>()
                    .map_err(|_| Report::new(CreateDraftError::Period))?,
                skew: self
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
                    placement: self.placement,
                },
            },
            self.if_not_exists,
        ));
        let query = statement
            .to_canonical_nspl()
            .change_context(CreateDraftError::CanonicalNspl)?;
        Ok(CreateSubmission {
            kind: CreateKind::Domain,
            presentation: query.clone(),
            dispatch: CreateDispatch::Command(CommandDispatch {
                query,
                domain: None,
                resource: None,
                created_domain: Some(name),
            }),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct UserDraft {
    name: String,
    password: String,
    if_not_exists: bool,
}

impl UserDraft {
    /// The submitted statement carries the password; its presentation masks it, so the cleartext
    /// never reaches the preview or the terminal.
    fn submission(&self) -> error_stack::Result<CreateSubmission, CreateDraftError> {
        let name = UserName::parse(self.name.trim())
            .map_err(|_| Report::new(CreateDraftError::UserName))?;
        if self.password.is_empty() {
            return Err(Report::new(CreateDraftError::PasswordRequired));
        }
        let statement = Statement::CreateUser(CreateStatement::new(
            CreateUser {
                name: name.clone(),
                password: self.password.clone(),
            },
            self.if_not_exists,
        ));
        let presentation_statement = Statement::CreateUser(CreateStatement::new(
            CreateUser {
                name,
                password: "********".to_string(),
            },
            self.if_not_exists,
        ));
        Ok(CreateSubmission {
            kind: CreateKind::User,
            presentation: presentation_statement
                .to_canonical_nspl()
                .change_context(CreateDraftError::CanonicalNspl)?,
            dispatch: CreateDispatch::Command(CommandDispatch {
                query: statement
                    .to_canonical_nspl()
                    .change_context(CreateDraftError::CanonicalNspl)?,
                domain: None,
                resource: None,
                created_domain: None,
            }),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct ResourceDraft {
    name: String,
    if_not_exists: bool,
}

impl ResourceDraft {
    fn submission(
        &self,
        captured_domain: Option<DomainName>,
    ) -> error_stack::Result<CreateSubmission, CreateDraftError> {
        let scope =
            captured_domain.ok_or_else(|| Report::new(CreateDraftError::ResourceDomainRequired))?;
        let name = ResourceName::parse(self.name.trim())
            .map_err(|_| Report::new(CreateDraftError::ResourceName))?;
        let statement = Statement::CreateResource(CreateStatement::new(
            CreateResource {
                identifier: name.clone(),
            },
            self.if_not_exists,
        ));
        let query = statement
            .to_canonical_nspl()
            .change_context(CreateDraftError::CanonicalNspl)?;
        Ok(CreateSubmission {
            kind: CreateKind::Resource,
            presentation: query.clone(),
            dispatch: CreateDispatch::Command(CommandDispatch {
                query,
                domain: Some(scope),
                resource: Some(name.to_string()),
                created_domain: None,
            }),
        })
    }
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
    #[error("{0}")]
    Relay(#[from] RelayDraftError),
    #[error("{0}")]
    Subscription(#[from] SubscriptionDraftError),
    #[error("{0}")]
    Codec(#[from] CodecDraftError),
    #[error("{0}")]
    Signaling(#[from] SignalingDraftError),
    #[error("Canonical NSPL could not be rendered")]
    CanonicalNspl,
}

/// Carries a draft's own error as the create dialog's, keeping the draft's report beneath it.
fn draft_error<E>(error: Report<E>) -> Report<CreateDraftError>
where
    E: error_stack::Context + Clone + Into<CreateDraftError>,
{
    let context = error.current_context().clone().into();
    error.change_context(context)
}

/// A completed draft, the statement it previews, and how it is dispatched.
#[derive(Debug, Clone)]
pub(crate) struct CreateSubmission {
    pub(crate) kind: CreateKind,
    /// The canonical statement as the preview and the terminal show it, with secrets masked.
    pub(crate) presentation: String,
    pub(crate) dispatch: CreateDispatch,
}

/// Where a completed draft goes: a persistent statement runs on the durable command path, and a
/// session subscription opens a tab under the subscription lifecycle.
#[derive(Debug, Clone)]
pub(crate) enum CreateDispatch {
    Command(CommandDispatch),
    Subscription(SubscriptionDispatch),
}

#[derive(Debug, Clone)]
pub(crate) struct CommandDispatch {
    pub(crate) query: String,
    pub(crate) domain: Option<DomainName>,
    pub(crate) resource: Option<String>,
    pub(crate) created_domain: Option<DomainName>,
}

/// A session subscription, the canonical statement that opens it, and the domain it reads.
#[derive(Debug, Clone)]
pub(crate) struct SubscriptionDispatch {
    pub(crate) domain: DomainName,
    pub(crate) subscription: CreateSubscription,
    pub(crate) statement: String,
}

impl SubscriptionDispatch {
    /// Renders `subscription` as the canonical client statement a subscribe request carries.
    pub(crate) fn new(
        domain: DomainName,
        subscription: CreateSubscription,
    ) -> error_stack::Result<Self, CanonicalNsplError> {
        let statement =
            ClientStatement::CreateSubscription(subscription.clone()).to_canonical_nspl()?;
        Ok(Self {
            domain,
            subscription,
            statement,
        })
    }
}

impl CreateSubmission {
    /// A domain-owned Model created through the durable command path.
    fn domain_model(
        kind: CreateKind,
        model: Model,
        if_not_exists: bool,
        scope: DomainName,
    ) -> error_stack::Result<Self, CreateDraftError> {
        Self::domain_requested_model(kind, model.into(), if_not_exists, scope)
    }

    fn domain_requested_model(
        kind: CreateKind,
        model: Model<RequestedResourceVersion>,
        if_not_exists: bool,
        scope: DomainName,
    ) -> error_stack::Result<Self, CreateDraftError> {
        let statement = Statement::Create(CreateStatement::new(Box::new(model), if_not_exists));
        let query = statement
            .to_canonical_nspl()
            .change_context(CreateDraftError::CanonicalNspl)?;
        Ok(Self {
            kind,
            presentation: query.clone(),
            dispatch: CreateDispatch::Command(CommandDispatch {
                query,
                domain: Some(scope),
                resource: None,
                created_domain: None,
            }),
        })
    }
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
    relay: RwSignal<RelayDraft>,
    subscription: RwSignal<SubscriptionDraft>,
    codec: RwSignal<CodecDraft>,
    signaling: RwSignal<SignalingDraft>,
    /// The number the next generated subscription name carries. Numbers only increase, so no two
    /// generated names of one console coincide.
    next_subscription_name: RwSignal<u64>,
    choices: ChoiceControls,
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
            relay: RwSignal::new(RelayDraft::default()),
            subscription: RwSignal::new(SubscriptionDraft::named(
                SubscriptionName::parse("web_console_subscription_1")
                    .assured("the first generated subscription name is a valid name"),
            )),
            codec: RwSignal::new(CodecDraft::default()),
            signaling: RwSignal::new(SignalingDraft::default()),
            next_subscription_name: RwSignal::new(2),
            choices: ChoiceControls::new(),
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

    /// Opens a new subscription draft that reads `relay` in `domain`, as the graph's relay action
    /// does. The action names what the operator wants to read, so it replaces a retained
    /// subscription draft rather than editing it, under a newly generated name.
    pub(crate) fn open_subscription(
        self,
        domain: DomainName,
        relay: RelayName,
        return_focus: &'static str,
    ) {
        let name = self.generate_subscription_name();
        self.subscription
            .set(SubscriptionDraft::for_relay(name, relay));
        self.captured_scopes.update(|scopes| {
            scopes.insert(CreateKind::Subscription, Some(domain.clone()));
        });
        self.open(CreateKind::Subscription, Some(domain), return_focus);
    }

    fn generate_subscription_name(self) -> SubscriptionName {
        let number = self.next_subscription_name.get_untracked();
        self.next_subscription_name.set(
            number
                .checked_add(1)
                .assured("a console cannot generate 2^64 subscription names"),
        );
        SubscriptionName::parse(&format!("web_console_subscription_{number}"))
            .assured("lower-case letters, underscores and at most 20 digits form a valid name")
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

    /// Moves the open draft to `domain`. References the draft selected in its previous domain stay
    /// visible but must be selected again before the draft completes.
    fn change_scope(self, domain: Option<DomainName>) {
        let Some(kind) = self.open.get_untracked() else {
            return;
        };
        if !kind.domain_scoped() {
            return;
        }
        if self.captured_domain.get_untracked() != domain {
            match kind {
                CreateKind::Branch => self
                    .structured
                    .update(|drafts| drafts.branch.invalidate_references()),
                CreateKind::Relay => self.relay.update(RelayDraft::invalidate_references),
                CreateKind::Subscription => self
                    .subscription
                    .update(SubscriptionDraft::invalidate_references),
                CreateKind::Codec => self.codec.update(CodecDraft::invalidate_references),
                CreateKind::SignalingProtocol => {
                    self.signaling.update(SignalingDraft::invalidate_references)
                }
                CreateKind::Domain
                | CreateKind::User
                | CreateKind::Resource
                | CreateKind::Schema
                | CreateKind::WireJsonSchema
                | CreateKind::WireCborSchema
                | CreateKind::WireAvroSchema => {}
            }
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
        if self.open.get_untracked() != Some(context.control.form())
            || self.revision.get_untracked() != context.draft_revision
            || current_generation != context.session_generation
        {
            return;
        }
        let target = self.choices.of(context.control).load;
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
                        | ChoiceLoad::MissingPrerequisite(_)
                        | ChoiceLoad::StaleContext
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
            ChoiceStatus::MissingContext => target.set(ChoiceLoad::MissingPrerequisite(
                "Choose the fields this control depends on",
            )),
            ChoiceStatus::StaleContext => target.set(ChoiceLoad::StaleContext),
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
            self.choices
                .of(context.control)
                .load
                .set(ChoiceLoad::Failed(reason));
        }
    }

    /// The typed question `control` asks, or why it cannot ask one while the draft lacks what the
    /// question depends on.
    fn choice_query(self, control: ChoiceControl) -> Result<ChoiceQuery, &'static str> {
        match control {
            ChoiceControl::DomainPace => Ok(ChoiceQuery {
                target: ChoiceTarget::DomainPace,
                dependencies: Vec::new(),
                page_size: 2,
            }),
            ChoiceControl::PlacementPolicy => Ok(ChoiceQuery {
                target: ChoiceTarget::PlacementPolicy,
                dependencies: vec![ChoiceSelection {
                    value: ChoiceValue::DomainPace(self.domain.get_untracked().pace),
                }],
                page_size: 3,
            }),
            ChoiceControl::BranchSchema | ChoiceControl::RelaySchema => self.domain_question(
                ChoiceTarget::Schema,
                "Select a domain before choosing a schema",
            ),
            ChoiceControl::CodecSchema => self.domain_question(
                ChoiceTarget::Schema,
                "Select a domain before choosing a schema",
            ),
            ChoiceControl::CodecWireSchema => {
                let target = match self
                    .codec
                    .get_untracked()
                    .format
                    .as_ref()
                    .map(CodecFormatDraft::kind)
                {
                    Some(CodecFormatKind::WireJson) => ChoiceTarget::WireJsonSchema,
                    Some(CodecFormatKind::WireCbor) => ChoiceTarget::WireCborSchema,
                    Some(CodecFormatKind::WireAvro) => ChoiceTarget::WireAvroSchema,
                    _ => return Err("Choose a wire schema format first"),
                };
                self.domain_question(target, "Select a domain before choosing a wire schema")
            }
            ChoiceControl::CodecResource | ChoiceControl::SignalingResource => self
                .domain_question(
                    ChoiceTarget::Resource,
                    "Select a domain before choosing a resource",
                ),
            ChoiceControl::CodecVersion | ChoiceControl::SignalingVersion => {
                let Some(domain) = self.captured_domain.get_untracked() else {
                    return Err("Select a domain before choosing a resource version");
                };
                let resource = match control {
                    ChoiceControl::CodecVersion => self
                        .codec
                        .get_untracked()
                        .binding()
                        .and_then(|binding| binding.current_resource().cloned()),
                    ChoiceControl::SignalingVersion => self
                        .signaling
                        .get_untracked()
                        .binding()
                        .and_then(|binding| binding.current_resource().cloned()),
                    _ => None,
                };
                let Some(resource) = resource else {
                    return Err("Select a resource to list its completed versions");
                };
                Ok(ChoiceQuery {
                    target: ChoiceTarget::CompletedResourceVersion,
                    dependencies: vec![
                        ChoiceSelection {
                            value: ChoiceValue::Domain(domain),
                        },
                        ChoiceSelection {
                            value: ChoiceValue::Resource(resource),
                        },
                    ],
                    page_size: 100,
                })
            }
            ChoiceControl::RelayBranch => self.domain_question(
                ChoiceTarget::Branch,
                "Select a domain before choosing a branch",
            ),
            ChoiceControl::SubscriptionRelay => self.domain_question(
                ChoiceTarget::Relay,
                "Select a domain before choosing a relay",
            ),
            ChoiceControl::SubscriptionField => {
                let Some(domain) = self.captured_domain.get_untracked() else {
                    return Err("Select a domain before choosing a relay");
                };
                let draft = self.subscription.get_untracked();
                let Some(relay) = draft.current_relay() else {
                    return Err("Select a relay to list the fields of its records");
                };
                Ok(ChoiceQuery {
                    target: ChoiceTarget::RelayField,
                    dependencies: vec![
                        ChoiceSelection {
                            value: ChoiceValue::Domain(domain),
                        },
                        ChoiceSelection {
                            value: ChoiceValue::Model(nervix_models::NodeRef::new(
                                ModelKind::Relay,
                                relay,
                            )),
                        },
                    ],
                    page_size: 100,
                })
            }
        }
    }

    /// A lookup of the models of the draft's captured domain.
    fn domain_question(
        self,
        target: ChoiceTarget,
        missing_domain: &'static str,
    ) -> Result<ChoiceQuery, &'static str> {
        let Some(domain) = self.captured_domain.get_untracked() else {
            return Err(missing_domain);
        };
        Ok(ChoiceQuery {
            target,
            dependencies: vec![ChoiceSelection {
                value: ChoiceValue::Domain(domain),
            }],
            page_size: 20,
        })
    }

    fn submission(self) -> error_stack::Result<CreateSubmission, CreateDraftError> {
        let kind = self
            .open
            .get_untracked()
            .ok_or_else(|| Report::new(CreateDraftError::KindRequired))?;
        let captured_domain = self.captured_domain.get_untracked();
        match kind {
            CreateKind::Domain => self.domain.get_untracked().submission(),
            CreateKind::User => self.user.get_untracked().submission(),
            CreateKind::Resource => self.resource.get_untracked().submission(captured_domain),
            CreateKind::Schema
            | CreateKind::WireJsonSchema
            | CreateKind::WireCborSchema
            | CreateKind::WireAvroSchema
            | CreateKind::Branch => {
                let scope = captured_domain
                    .ok_or_else(|| Report::new(CreateDraftError::ScopedDomainRequired))?;
                let drafts = self.structured.get_untracked();
                let (model, if_not_exists) = match kind {
                    CreateKind::Schema => (
                        Model::Schema(drafts.schema.build().map_err(draft_error)?),
                        drafts.schema.if_not_exists,
                    ),
                    CreateKind::Branch => (
                        Model::Branch(drafts.branch.build().map_err(draft_error)?),
                        drafts.branch.if_not_exists,
                    ),
                    _ => {
                        let format = kind.wire_format().assured(
                            "the structured kinds left after schema and branch are wire schemas",
                        );
                        let draft = drafts.wire(format);
                        (
                            draft.build(format).map_err(draft_error)?,
                            draft.if_not_exists,
                        )
                    }
                };
                CreateSubmission::domain_model(kind, model, if_not_exists, scope)
            }
            CreateKind::Relay => {
                let scope = captured_domain
                    .ok_or_else(|| Report::new(CreateDraftError::ScopedDomainRequired))?;
                let draft = self.relay.get_untracked();
                let relay = draft.build().map_err(draft_error)?;
                CreateSubmission::domain_model(
                    kind,
                    Model::Relay(relay),
                    draft.if_not_exists,
                    scope,
                )
            }
            CreateKind::Codec => {
                let scope = captured_domain
                    .ok_or_else(|| Report::new(CreateDraftError::ScopedDomainRequired))?;
                let draft = self.codec.get_untracked();
                let codec = draft.build().map_err(draft_error)?;
                CreateSubmission::domain_requested_model(
                    kind,
                    Model::Codec(codec),
                    draft.if_not_exists,
                    scope,
                )
            }
            CreateKind::SignalingProtocol => {
                let scope = captured_domain
                    .ok_or_else(|| Report::new(CreateDraftError::ScopedDomainRequired))?;
                let draft = self.signaling.get_untracked();
                let protocol = draft.build().map_err(draft_error)?;
                CreateSubmission::domain_requested_model(
                    kind,
                    Model::SignalingProtocol(protocol),
                    draft.if_not_exists,
                    scope,
                )
            }
            CreateKind::Subscription => {
                let scope = captured_domain
                    .ok_or_else(|| Report::new(CreateDraftError::ScopedDomainRequired))?;
                let subscription = self
                    .subscription
                    .get_untracked()
                    .build()
                    .map_err(draft_error)?;
                let dispatch = SubscriptionDispatch::new(scope, subscription)
                    .change_context(CreateDraftError::CanonicalNspl)?;
                Ok(CreateSubmission {
                    kind,
                    presentation: dispatch.statement.clone(),
                    dispatch: CreateDispatch::Subscription(dispatch),
                })
            }
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
                <button type="button" role="menuitem" data-create-kind="relay" on:click=move |_| choose(CreateKind::Relay)>
                    <span>"Relay"</span><em>"Schema, branching and capacity"</em>
                </button>
                <button type="button" role="menuitem" data-create-kind="subscription" on:click=move |_| choose(CreateKind::Subscription)>
                    <span>"Subscription"</span><em>"Read-only relay tab"</em>
                </button>
                <button type="button" role="menuitem" data-create-kind="codec" on:click=move |_| choose(CreateKind::Codec)>
                    <span>"Codec"</span><em>"Wire format and schema mapping"</em>
                </button>
                <button type="button" role="menuitem" data-create-kind="signaling-protocol" on:click=move |_| choose(CreateKind::SignalingProtocol)>
                    <span>"Signaling protocol"</span><em>"Ordered connection handshake"</em>
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
    let control_signals = signals.choices.of(control);
    let query = match signals.choice_query(control) {
        Ok(query) => query,
        Err(reason) => {
            control_signals
                .load
                .set(ChoiceLoad::MissingPrerequisite(reason));
            return;
        }
    };
    let cursor = if append {
        page_cursor(control_signals.load)
    } else {
        None
    };
    if append && cursor.is_none() {
        return;
    }
    let request = ChoiceLookupRequest::new(
        query.target,
        query.dependencies,
        control_signals.search.get_untracked(),
    )
    .with_page(query.page_size, cursor)
    .assured("the create dialog uses a bounded choice page size");
    let context = ChoiceRequestContext {
        control,
        draft_revision: signals.revision.get_untracked(),
        session_generation: generation,
        append,
    };
    if !append {
        control_signals.load.set(ChoiceLoad::Loading);
    }
    let Some(request_tx) = request_tx.get_untracked() else {
        control_signals.load.set(ChoiceLoad::Failed(
            "The session is not available".to_string(),
        ));
        return;
    };
    if request_tx
        .unbounded_send(ConsoleRequest::Choice { request, context })
        .is_err()
    {
        control_signals.load.set(ChoiceLoad::Failed(
            "The session channel is closed".to_string(),
        ));
    }
}

fn page_cursor(load: RwSignal<ChoiceLoad>) -> Option<String> {
    match load.get_untracked() {
        ChoiceLoad::Ready { page_cursor, .. } => page_cursor,
        ChoiceLoad::Waiting
        | ChoiceLoad::Loading
        | ChoiceLoad::Empty
        | ChoiceLoad::MissingPrerequisite(_)
        | ChoiceLoad::StaleContext
        | ChoiceLoad::Failed(_) => None,
    }
}

/// The controls the open form asks the session about. Each asks again whenever its draft changes,
/// so a reply to an older draft or connection never fills it.
fn open_form_controls(signals: CreateSignals, kind: CreateKind) -> Vec<ChoiceControl> {
    match kind {
        CreateKind::Domain => vec![ChoiceControl::DomainPace, ChoiceControl::PlacementPolicy],
        CreateKind::Branch => vec![ChoiceControl::BranchSchema],
        CreateKind::Relay => {
            let mut controls = vec![ChoiceControl::RelaySchema];
            if signals.relay.get_untracked().branching.is_branched() {
                controls.push(ChoiceControl::RelayBranch);
            }
            controls
        }
        CreateKind::Subscription => vec![
            ChoiceControl::SubscriptionRelay,
            ChoiceControl::SubscriptionField,
        ],
        CreateKind::Codec => {
            let mut controls = vec![ChoiceControl::CodecSchema];
            match signals.codec.get_untracked().format {
                Some(CodecFormatDraft::Wire { .. }) => {
                    controls.push(ChoiceControl::CodecWireSchema)
                }
                Some(CodecFormatDraft::Protobuf { .. }) => {
                    controls.push(ChoiceControl::CodecResource);
                    controls.push(ChoiceControl::CodecVersion);
                }
                _ => {}
            }
            controls
        }
        CreateKind::SignalingProtocol => {
            if matches!(
                signals.signaling.get_untracked().format,
                Some(SignalingFormatDraft::Protobuf { .. })
            ) {
                vec![
                    ChoiceControl::SignalingResource,
                    ChoiceControl::SignalingVersion,
                ]
            } else {
                Vec::new()
            }
        }
        CreateKind::User
        | CreateKind::Resource
        | CreateKind::Schema
        | CreateKind::WireJsonSchema
        | CreateKind::WireCborSchema
        | CreateKind::WireAvroSchema => Vec::new(),
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
        signals.revision.track();
        let generation = session_generation.get();
        let connected = connection_state.get() == ConsoleConnectionState::Connected;
        let Some(kind) = open else {
            return;
        };
        for control in open_form_controls(signals, kind) {
            if connected {
                request_choices(signals, control, request_tx, generation, false);
            } else {
                signals.choices.of(control).load.set(ChoiceLoad::Waiting);
            }
        }
    });
    let scope_changed = move || {
        signals.captured_domain.get() != active_domain.get()
            && signals.open.get().is_some_and(CreateKind::domain_scoped)
    };
    // The drafts own validation and report it inline, so the form is `novalidate`: the browser's
    // constraint checks, such as a number input's minimum, never keep a submit from reaching them.
    let submit_form = move |event: ev::SubmitEvent| {
        event.prevent_default();
        // A submitted draft is not submitted again until it is edited. The handler holds the rule
        // the disabled button shows, because a form can be submitted without clicking it.
        if signals.progress.get_untracked().blocks_repeat_submit() {
            return;
        }
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
                    <form novalidate on:submit=submit_form>
                        <div class="create-scope-row">
                            <span>"Scope"</span>
                            <strong class="create-scope">{move || match signals.open.get() {
                                Some(kind) if kind.domain_scoped() => match signals.captured_domain.get() {
                                    Some(domain) => domain.to_string(),
                                    None => "No domain selected".to_string(),
                                },
                                Some(_) | None => "Cluster".to_string(),
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
                        <Show when=move || signals.open.get() == Some(CreateKind::Relay) fallback=|| ()>
                            <RelayEditor signals=signals name_input=name_input request_tx=request_tx session_generation=session_generation />
                        </Show>
                        <Show when=move || signals.open.get() == Some(CreateKind::Subscription) fallback=|| ()>
                            <SubscriptionEditor signals=signals name_input=name_input request_tx=request_tx session_generation=session_generation />
                        </Show>
                        <Show when=move || signals.open.get() == Some(CreateKind::Codec) fallback=|| ()>
                            <CodecEditor signals=signals name_input=name_input request_tx=request_tx session_generation=session_generation />
                        </Show>
                        <Show when=move || signals.open.get() == Some(CreateKind::SignalingProtocol) fallback=|| ()>
                            <SignalingEditor signals=signals name_input=name_input request_tx=request_tx session_generation=session_generation />
                        </Show>

                        <Show when=move || signals.open.get().is_some_and(CreateKind::takes_if_not_exists) fallback=|| ()>
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
                                        Some(CreateKind::Relay) => signals.relay.get().if_not_exists,
                                        Some(CreateKind::Codec) => signals.codec.get().if_not_exists,
                                        Some(CreateKind::SignalingProtocol) => signals.signaling.get().if_not_exists,
                                        Some(CreateKind::Subscription) | None => false,
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
                                            Some(CreateKind::Relay) => signals.relay.update(|draft| draft.if_not_exists = checked),
                                            Some(CreateKind::Codec) => signals.codec.update(|draft| draft.if_not_exists = checked),
                                            Some(CreateKind::SignalingProtocol) => signals.signaling.update(|draft| draft.if_not_exists = checked),
                                            Some(CreateKind::Subscription) | None => {}
                                        }
                                        signals.edit();
                                    }
                                />
                                <span>"If not exists"</span>
                            </label>
                        </Show>

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

fn event_target_value(event: &ev::Event) -> String {
    event_target::<web_sys::HtmlInputElement>(event).value()
}

fn event_target_textarea_value(event: &ev::Event) -> String {
    event_target::<web_sys::HtmlTextAreaElement>(event).value()
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
        Choice, ChoiceOutcome, ChoicePresentation, ChoiceStatus, ChoiceTarget, ChoiceValue,
        DomainPaceChoice,
    };
    use nervix_models::{
        AvroType, DomainName, FieldName, JsonType, MaterializedRelayState, ModelKind, ModelName,
        NodeRef, ParseAsType, PlacementPolicy, RelayName, SchemaName, SubscriptionDeliveryBehavior,
        WireSchemaStrictness,
    };

    use super::{
        super::ConsoleRequest,
        ChoiceControl, ChoiceGroup, ChoiceGroupProps, ChoiceLoad, ChoiceRequestContext,
        CreateDialog, CreateDialogProps, CreateDispatch, CreateDraftError, CreateKind, CreateMenu,
        CreateMenuProps, CreateProgress, CreateSignals, CreateSubmission, DomainDraft,
        ResourceDraft, SelectedReference, UserDraft, open_form_controls, request_choices,
        schema_draft::{SchemaFieldDraft, SchemaTypeDraft, WireFieldDraft, WireFieldType},
        select_choice, selected_choice,
    };

    fn domain(name: &str) -> DomainName {
        DomainName::parse(name).assured("the test domain names are valid")
    }

    fn node(kind: ModelKind, name: &str) -> NodeRef {
        NodeRef::new(
            kind,
            ModelName::parse(name).assured("the test names are valid"),
        )
    }

    fn command_query(submission: &CreateSubmission) -> &str {
        let CreateDispatch::Command(command) = &submission.dispatch else {
            panic!("the submission runs on the durable command path");
        };
        &command.query
    }

    fn command_domain(submission: &CreateSubmission) -> Option<&DomainName> {
        let CreateDispatch::Command(command) = &submission.dispatch else {
            panic!("the submission runs on the durable command path");
        };
        command.domain.as_ref()
    }

    fn outcome(value: ChoiceValue, label: &str) -> ChoiceOutcome {
        ChoiceOutcome {
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
        }
    }

    #[test]
    fn typed_drafts_render_the_canonical_statements_the_dispatcher_sends() {
        let domain_draft = DomainDraft {
            name: "orders".to_string(),
            if_not_exists: true,
            placement: PlacementPolicy::PreferColocation,
            ..DomainDraft::default()
        };
        let submission = domain_draft
            .submission()
            .assured("the domain draft is valid");
        assert_eq!(
            command_query(&submission),
            "CREATE IF NOT EXISTS UNPACED DOMAIN orders PLACEMENT PREFER COLOCATION;"
        );

        let resource = ResourceDraft {
            name: "bundle".to_string(),
            if_not_exists: false,
        };
        let scope = domain("orders");
        let submission = resource
            .submission(Some(scope.clone()))
            .assured("the resource draft and scope are valid");
        assert_eq!(command_query(&submission), "CREATE RESOURCE bundle;");
        assert_eq!(command_domain(&submission), Some(&scope));
    }

    #[test]
    fn credential_presentation_is_masked_while_the_submitted_statement_keeps_the_secret() {
        let user = UserDraft {
            name: "operator".to_string(),
            password: "it's-secret".to_string(),
            if_not_exists: false,
        };
        let submission = user.submission().assured("the user draft is valid");
        assert!(command_query(&submission).contains("it's-secret"));
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
        let error = resource
            .submission(None)
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
            let pace = signals.choices.domain_pace.load;
            let placement = signals.choices.placement_policy.load;

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
                pace.get_untracked(),
                ChoiceLoad::Ready { choices, .. } if choices[0].presentation.label == "UNPACED"
            ));
            assert!(matches!(
                placement.get_untracked(),
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
                pace.get_untracked(),
                ChoiceLoad::Ready { choices, .. } if choices[0].presentation.label == "UNPACED"
            ));
            assert!(matches!(
                placement.get_untracked(),
                ChoiceLoad::Ready { choices, .. } if choices[0].presentation.label == "NEUTRAL"
            ));
        });
    }

    #[test]
    fn a_reply_fills_only_a_control_of_the_open_form() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            signals.open(CreateKind::Relay, Some(domain("orders")), "trigger");
            let context = |control| ChoiceRequestContext {
                control,
                draft_revision: signals.revision.get_untracked(),
                session_generation: 1,
                append: false,
            };
            let relay = ChoiceValue::Model(node(ModelKind::Relay, "orders"));
            signals.apply_choice(
                context(ChoiceControl::SubscriptionRelay),
                1,
                outcome(relay.clone(), "orders"),
            );
            assert_eq!(
                signals.choices.subscription_relay.load.get_untracked(),
                ChoiceLoad::Waiting
            );
            let schema = ChoiceValue::Model(node(ModelKind::Schema, "order_record"));
            signals.apply_choice(
                context(ChoiceControl::RelaySchema),
                1,
                outcome(schema, "order_record"),
            );
            assert!(matches!(
                signals.choices.relay_schema.load.get_untracked(),
                ChoiceLoad::Ready { .. }
            ));

            signals.open(CreateKind::Subscription, Some(domain("orders")), "trigger");
            signals.apply_choice(
                context(ChoiceControl::SubscriptionRelay),
                1,
                outcome(relay, "orders"),
            );
            assert!(matches!(
                signals.choices.subscription_relay.load.get_untracked(),
                ChoiceLoad::Ready { .. }
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
    fn choice_outcomes_keep_missing_stale_and_failed_states_distinct() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            signals.open(CreateKind::Domain, None, "trigger");
            let pace = signals.choices.domain_pace.load;
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
            assert_eq!(pace.get_untracked(), ChoiceLoad::Empty);

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
                pace.get_untracked(),
                ChoiceLoad::Ready { choices, page_cursor: None } if choices.len() == 2
            ));

            for (status, expected) in [
                (
                    ChoiceStatus::MissingContext,
                    ChoiceLoad::MissingPrerequisite("Choose the fields this control depends on"),
                ),
                (ChoiceStatus::StaleContext, ChoiceLoad::StaleContext),
                (
                    ChoiceStatus::LookupFailed,
                    ChoiceLoad::Failed("Choices could not be loaded".to_string()),
                ),
            ] {
                signals.apply_choice(context, 3, outcome(status, Vec::new(), None));
                assert_eq!(pace.get_untracked(), expected);
            }
            signals.fail_choice(context, 3, "transport ended".to_string());
            assert_eq!(
                pace.get_untracked(),
                ChoiceLoad::Failed("transport ended".to_string())
            );
            signals.fail_choice(context, 4, "stale transport".to_string());
            assert_eq!(
                pace.get_untracked(),
                ChoiceLoad::Failed("transport ended".to_string())
            );
        });
    }

    #[test]
    fn choice_states_render_hints_and_failures_with_their_own_presentation() {
        super::super::initialize_test_executor();
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            let load = signals.choices.domain_pace.load;
            let request_tx = RwSignal::new(None);
            let session_generation = RwSignal::new(1);
            let render = || {
                ChoiceGroup(
                    ChoiceGroupProps::builder()
                        .class_name("create-pace-options")
                        .label("Pace")
                        .control(ChoiceControl::DomainPace)
                        .signals(signals)
                        .request_tx(request_tx)
                        .session_generation(session_generation)
                        .build(),
                )
                .to_html()
            };

            load.set(ChoiceLoad::MissingPrerequisite("Choose a domain"));
            let missing = render();
            assert!(missing.contains("create-choice-missing"));
            assert!(missing.contains("Choose a domain"));

            load.set(ChoiceLoad::StaleContext);
            let stale = render();
            assert!(stale.contains("create-choice-stale"));
            assert!(stale.contains("create-choice-retry"));
            assert!(stale.contains("The form context changed"));

            load.set(ChoiceLoad::Failed(
                "Choices could not be loaded".to_string(),
            ));
            let failed = render();
            assert!(failed.contains("choice-failed"));
            assert!(failed.contains("role=\"alert\""));
            assert!(failed.contains("Choices could not be loaded"));
        });
    }

    #[test]
    fn paced_domain_and_password_validation_report_the_owning_field() {
        let mut draft = DomainDraft {
            name: "orders".to_string(),
            pace: DomainPaceChoice::Paced,
            period: "2s".to_string(),
            skew: "500ms".to_string(),
            ..DomainDraft::default()
        };
        let submission = draft
            .submission()
            .assured("the paced domain draft is valid");
        assert!(command_query(&submission).contains("PACED"));
        assert!(command_query(&submission).contains("PERIOD 2s"));

        draft.period = "soon".to_string();
        assert_eq!(
            draft
                .submission()
                .expect_err("an invalid period is rejected")
                .current_context(),
            &CreateDraftError::Period
        );
        draft.period = "2s".to_string();
        draft.skew = "later".to_string();
        assert_eq!(
            draft
                .submission()
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
            user.submission()
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
            let pace = signals.choices.domain_pace;
            pace.search.set("wall".to_string());
            let (sender, mut receiver) = unbounded();
            let request_tx = RwSignal::new(Some(sender));
            request_choices(signals, ChoiceControl::DomainPace, request_tx, 8, false);
            let ConsoleRequest::Choice { request, context } = receiver
                .try_recv()
                .assured("the choice channel remains open")
            else {
                panic!("the create dialog sends a typed choice request");
            };
            assert_eq!(request.target(), ChoiceTarget::DomainPace);
            assert_eq!(request.search(), "wall");
            assert!(request.dependencies().is_empty());
            assert_eq!(context.session_generation, 8);
            assert!(!context.append);

            pace.load.set(ChoiceLoad::Ready {
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
                pace.load.get_untracked(),
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
                pace.load.get_untracked(),
                ChoiceLoad::Failed("The session channel is closed".to_string())
            );

            pace.load.set(ChoiceLoad::Empty);
            request_choices(signals, ChoiceControl::DomainPace, request_tx, 8, true);
            assert!(receiver.try_recv().is_err());
        });
    }

    #[test]
    fn typed_choice_selection_updates_only_the_draft_its_control_edits() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            select_choice(
                signals,
                ChoiceControl::DomainPace,
                ChoiceValue::DomainPace(DomainPaceChoice::Paced),
            );
            select_choice(
                signals,
                ChoiceControl::PlacementPolicy,
                ChoiceValue::PlacementPolicy(PlacementPolicy::RequireColocation),
            );
            assert_eq!(signals.domain.get_untracked().pace, DomainPaceChoice::Paced);
            assert_eq!(
                signals.domain.get_untracked().placement,
                PlacementPolicy::RequireColocation
            );
            select_choice(
                signals,
                ChoiceControl::DomainPace,
                ChoiceValue::Domain(domain("orders")),
            );
            assert_eq!(signals.domain.get_untracked().pace, DomainPaceChoice::Paced);

            let schema = ChoiceValue::Model(node(ModelKind::Schema, "order_record"));
            select_choice(signals, ChoiceControl::RelaySchema, schema.clone());
            assert!(selected_choice(
                signals,
                ChoiceControl::RelaySchema,
                &schema
            ));
            assert!(!selected_choice(
                signals,
                ChoiceControl::BranchSchema,
                &schema
            ));
            assert!(signals.structured.get_untracked().branch.schema.is_none());

            let branch = ChoiceValue::Model(node(ModelKind::Branch, "by_tenant"));
            select_choice(signals, ChoiceControl::RelayBranch, branch.clone());
            assert!(selected_choice(
                signals,
                ChoiceControl::RelayBranch,
                &branch
            ));

            let relay = ChoiceValue::Model(node(ModelKind::Relay, "orders"));
            select_choice(signals, ChoiceControl::SubscriptionRelay, relay.clone());
            assert!(selected_choice(
                signals,
                ChoiceControl::SubscriptionRelay,
                &relay
            ));

            let field = ChoiceValue::Field(FieldName::parse("amount").assured("valid field"));
            select_choice(signals, ChoiceControl::SubscriptionField, field.clone());
            select_choice(signals, ChoiceControl::SubscriptionField, field.clone());
            assert_eq!(
                signals.subscription.get_untracked().filter,
                "input.amount input.amount"
            );
            assert!(!selected_choice(
                signals,
                ChoiceControl::SubscriptionField,
                &field
            ));
            select_choice(signals, ChoiceControl::RelaySchema, field);
            assert!(selected_choice(
                signals,
                ChoiceControl::RelaySchema,
                &schema
            ));
        });
    }

    #[test]
    fn branch_scope_change_keeps_the_draft_and_requires_reselecting_its_schema() {
        Owner::new().with(|| {
            let first = domain("first");
            let second = domain("second");
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
                drafts.branch.schema = Some(SelectedReference::chosen(schema.clone()));
                drafts.branch.ttl = "5m".to_string();
            });
            signals.change_scope(Some(first.clone()));
            assert!(
                signals
                    .structured
                    .get_untracked()
                    .branch
                    .schema
                    .assured("the schema stays selected")
                    .is_current()
            );

            signals.change_scope(Some(second.clone()));
            let retained = signals.structured.get_untracked().branch;
            let retained_schema = retained.schema.assured("the schema stays selected");
            assert_eq!(retained_schema.name(), &schema);
            assert!(!retained_schema.is_current());
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
    fn relay_and_subscription_scope_changes_invalidate_their_references() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            signals.open(CreateKind::Relay, Some(domain("first")), "trigger");
            signals.relay.update(|draft| {
                draft.select_schema(&node(ModelKind::Schema, "order_record"));
                draft.select_branch(&node(ModelKind::Branch, "by_tenant"));
            });
            signals.change_scope(Some(domain("second")));
            let relay = signals.relay.get_untracked();
            assert!(!relay.selects_schema(&node(ModelKind::Schema, "order_record")));
            assert!(!relay.selects_branch(&node(ModelKind::Branch, "by_tenant")));

            signals.open_subscription(
                domain("first"),
                RelayName::parse("orders").assured("valid relay"),
                "trigger",
            );
            signals.change_scope(Some(domain("first")));
            assert!(
                signals
                    .subscription
                    .get_untracked()
                    .current_relay()
                    .is_some()
            );
            signals.change_scope(Some(domain("second")));
            assert!(
                signals
                    .subscription
                    .get_untracked()
                    .current_relay()
                    .is_none()
            );
            assert_eq!(
                signals
                    .submission()
                    .expect_err("the relay must be selected again")
                    .current_context()
                    .to_string(),
                "The selected relay belongs to a changed context; select it again"
            );
        });
    }

    #[test]
    fn branch_schema_requests_bind_the_captured_domain_and_page_cursor() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            let branch_schema = signals.choices.branch_schema;
            let (sender, mut receiver) = unbounded();
            let request_tx = RwSignal::new(Some(sender));
            signals.open(CreateKind::Branch, None, "trigger");
            request_choices(signals, ChoiceControl::BranchSchema, request_tx, 9, false);
            assert_eq!(
                branch_schema.load.get_untracked(),
                ChoiceLoad::MissingPrerequisite("Select a domain before choosing a schema")
            );
            assert!(receiver.try_recv().is_err());

            let scope = domain("orders");
            signals.change_scope(Some(scope.clone()));
            branch_schema.search.set("tenant".to_string());
            request_choices(signals, ChoiceControl::BranchSchema, request_tx, 9, false);
            let ConsoleRequest::Choice { request, context } = receiver
                .try_recv()
                .assured("the schema picker requests a typed page")
            else {
                panic!("the schema picker must send a choice request");
            };
            assert_eq!(request.target(), ChoiceTarget::Schema);
            assert_eq!(request.search(), "tenant");
            assert_eq!(request.page_size(), 20);
            assert_eq!(request.dependencies()[0].value, ChoiceValue::Domain(scope));
            assert_eq!(context.control, ChoiceControl::BranchSchema);

            branch_schema.load.set(ChoiceLoad::Ready {
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

            branch_schema.load.set(ChoiceLoad::Empty);
            request_choices(signals, ChoiceControl::BranchSchema, request_tx, 9, true);
            assert!(receiver.try_recv().is_err());
        });
    }

    #[test]
    fn relay_and_subscription_controls_ask_typed_questions_of_the_captured_domain() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            let (sender, mut receiver) = unbounded();
            let request_tx = RwSignal::new(Some(sender));
            let mut next_request = || {
                let ConsoleRequest::Choice { request, context } = receiver
                    .try_recv()
                    .assured("the control sends a typed choice request")
                else {
                    panic!("a control sends a choice request");
                };
                (request, context)
            };

            signals.open(CreateKind::Subscription, None, "trigger");
            for (control, reason) in [
                (
                    ChoiceControl::RelaySchema,
                    "Select a domain before choosing a schema",
                ),
                (
                    ChoiceControl::RelayBranch,
                    "Select a domain before choosing a branch",
                ),
                (
                    ChoiceControl::SubscriptionRelay,
                    "Select a domain before choosing a relay",
                ),
                (
                    ChoiceControl::SubscriptionField,
                    "Select a domain before choosing a relay",
                ),
            ] {
                request_choices(signals, control, request_tx, 2, false);
                assert_eq!(
                    signals.choices.of(control).load.get_untracked(),
                    ChoiceLoad::MissingPrerequisite(reason)
                );
            }

            let scope = domain("orders");
            signals.change_scope(Some(scope.clone()));
            request_choices(
                signals,
                ChoiceControl::SubscriptionField,
                request_tx,
                2,
                false,
            );
            assert_eq!(
                signals.choices.subscription_field.load.get_untracked(),
                ChoiceLoad::MissingPrerequisite("Select a relay to list the fields of its records")
            );
            for (control, target) in [
                (ChoiceControl::RelaySchema, ChoiceTarget::Schema),
                (ChoiceControl::RelayBranch, ChoiceTarget::Branch),
                (ChoiceControl::SubscriptionRelay, ChoiceTarget::Relay),
            ] {
                request_choices(signals, control, request_tx, 2, false);
                let (request, context) = next_request();
                assert_eq!(request.target(), target);
                assert_eq!(request.page_size(), 20);
                assert_eq!(
                    request.dependencies()[0].value,
                    ChoiceValue::Domain(scope.clone())
                );
                assert_eq!(context.control, control);
            }

            signals.subscription.update(|draft| {
                draft.select_relay(&node(ModelKind::Relay, "orders"));
            });
            request_choices(
                signals,
                ChoiceControl::SubscriptionField,
                request_tx,
                2,
                false,
            );
            let (request, _) = next_request();
            assert_eq!(request.target(), ChoiceTarget::RelayField);
            assert_eq!(request.page_size(), 100);
            assert_eq!(
                request.dependencies()[1].value,
                ChoiceValue::Model(node(ModelKind::Relay, "orders"))
            );
        });
    }

    #[test]
    fn branch_schema_selection_is_exact_and_does_not_clear_an_invalid_reference() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            let choice = ChoiceValue::Model(node(ModelKind::Schema, "tenant_key"));
            assert!(!selected_choice(
                signals,
                ChoiceControl::BranchSchema,
                &choice
            ));
            select_choice(signals, ChoiceControl::BranchSchema, choice.clone());
            assert!(selected_choice(
                signals,
                ChoiceControl::BranchSchema,
                &choice
            ));

            signals
                .structured
                .update(|drafts| drafts.branch.invalidate_references());
            assert!(!selected_choice(
                signals,
                ChoiceControl::BranchSchema,
                &choice
            ));
            assert_eq!(
                signals
                    .structured
                    .get_untracked()
                    .branch
                    .schema
                    .assured("the schema stays selected")
                    .name(),
                &SchemaName::parse("tenant_key").assured("valid schema")
            );

            select_choice(
                signals,
                ChoiceControl::BranchSchema,
                ChoiceValue::Model(node(ModelKind::Relay, "tenant_key")),
            );
            assert!(!selected_choice(
                signals,
                ChoiceControl::BranchSchema,
                &choice
            ));
        });
    }

    #[test]
    fn a_contextual_subscription_opens_a_fresh_named_draft_for_its_relay() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            assert_eq!(
                signals.subscription.get_untracked().name,
                "web_console_subscription_1"
            );
            signals.subscription.update(|draft| {
                draft.filter = "input.amount > 1".to_string();
            });
            let scope = domain("orders");
            let relay = RelayName::parse("orders").assured("valid relay");
            signals.open_subscription(scope.clone(), relay.clone(), "graph-search");
            assert_eq!(signals.open.get_untracked(), Some(CreateKind::Subscription));
            assert_eq!(signals.captured_domain.get_untracked(), Some(scope.clone()));
            let draft = signals.subscription.get_untracked();
            assert_eq!(draft.name, "web_console_subscription_2");
            assert_eq!(draft.current_relay(), Some(&relay));
            assert!(draft.filter.is_empty());
            assert_eq!(draft.delivery, SubscriptionDeliveryBehavior::Blocking);

            let submission = signals
                .submission()
                .assured("a contextual draft is complete");
            let CreateDispatch::Subscription(dispatch) = submission.dispatch else {
                panic!("a subscription opens under the subscription lifecycle");
            };
            assert_eq!(dispatch.domain, scope);
            assert_eq!(
                dispatch.statement,
                "CREATE SUBSCRIPTION web_console_subscription_2 TO orders;"
            );
            assert_eq!(submission.presentation, dispatch.statement);

            signals.open_subscription(scope, relay, "graph-search");
            assert_eq!(
                signals.subscription.get_untracked().name,
                "web_console_subscription_3"
            );
        });
    }

    #[test]
    fn domain_scoped_forms_need_their_captured_domain_and_run_where_they_belong() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            signals.relay.update(|draft| {
                draft.name = "orders".to_string();
                draft.select_schema(&node(ModelKind::Schema, "order_record"));
                draft.choose_unbranched();
                draft.materialized_state = Some(MaterializedRelayState::LastByTimestamp);
                draft.if_not_exists = true;
            });
            signals.open(CreateKind::Relay, None, "trigger");
            assert_eq!(
                signals
                    .submission()
                    .expect_err("a relay needs a domain")
                    .current_context(),
                &CreateDraftError::ScopedDomainRequired
            );
            signals.open(CreateKind::Subscription, None, "trigger");
            assert_eq!(
                signals
                    .submission()
                    .expect_err("a subscription needs a domain")
                    .current_context(),
                &CreateDraftError::ScopedDomainRequired
            );

            let scope = domain("orders");
            signals.open(CreateKind::Relay, Some(scope.clone()), "trigger");
            assert_eq!(signals.captured_domain.get_untracked(), None);
            signals.change_scope(Some(scope.clone()));
            signals.relay.update(|draft| {
                draft.select_schema(&node(ModelKind::Schema, "order_record"));
            });
            let submission = signals.submission().assured("the relay draft is complete");
            assert_eq!(
                command_query(&submission),
                "CREATE IF NOT EXISTS RELAY orders SCHEMA order_record UNBRANCHED CAPACITY 1 WITH \
                 MATERIALIZED STATE LAST BY TIMESTAMP;"
            );
            assert_eq!(command_domain(&submission), Some(&scope));

            signals
                .relay
                .update(|draft| draft.capacity = "none".to_string());
            assert_eq!(
                signals
                    .submission()
                    .expect_err("capacity must be a count")
                    .current_context()
                    .to_string(),
                "Capacity must be a positive integer"
            );
            assert!(CreateKind::Relay.takes_if_not_exists());
            assert!(!CreateKind::Subscription.takes_if_not_exists());
            assert_eq!(
                ChoiceControl::SubscriptionField.form(),
                CreateKind::Subscription
            );
            assert_eq!(ChoiceControl::RelayBranch.form(), CreateKind::Relay);
        });
    }

    #[test]
    fn each_form_asks_only_the_questions_its_draft_needs() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            assert_eq!(
                open_form_controls(signals, CreateKind::Domain),
                [ChoiceControl::DomainPace, ChoiceControl::PlacementPolicy]
            );
            assert_eq!(
                open_form_controls(signals, CreateKind::Branch),
                [ChoiceControl::BranchSchema]
            );
            assert_eq!(
                open_form_controls(signals, CreateKind::Relay),
                [ChoiceControl::RelaySchema]
            );
            signals.relay.update(|draft| draft.choose_branched());
            assert_eq!(
                open_form_controls(signals, CreateKind::Relay),
                [ChoiceControl::RelaySchema, ChoiceControl::RelayBranch]
            );
            assert_eq!(
                open_form_controls(signals, CreateKind::Subscription),
                [
                    ChoiceControl::SubscriptionRelay,
                    ChoiceControl::SubscriptionField
                ]
            );
            for kind in [
                CreateKind::User,
                CreateKind::Resource,
                CreateKind::Schema,
                CreateKind::WireJsonSchema,
            ] {
                assert!(open_form_controls(signals, kind).is_empty());
            }
        });
    }

    #[test]
    fn creation_components_render_each_typed_draft_and_accessible_status() {
        super::super::initialize_test_executor();
        Owner::new().with(|| {
            let scope = domain("orders");
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
            assert!(menu_markup.contains("data-create-kind=\"relay\""));
            assert!(menu_markup.contains("data-create-kind=\"subscription\""));

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
                    signals.choices.domain_pace.load.set(ChoiceLoad::Ready {
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
                    signals
                        .choices
                        .placement_policy
                        .load
                        .set(ChoiceLoad::Ready {
                            choices: vec![Choice {
                                value: ChoiceValue::PlacementPolicy(
                                    PlacementPolicy::PreferColocation,
                                ),
                                presentation: ChoicePresentation {
                                    label: "PREFER COLOCATION".to_string(),
                                    detail: None,
                                    group: None,
                                },
                            }],
                            page_cursor: None,
                        });
                }
                if signals.open.get_untracked() == Some(CreateKind::Subscription) {
                    signals
                        .choices
                        .subscription_field
                        .load
                        .set(ChoiceLoad::Ready {
                            choices: vec![Choice {
                                value: ChoiceValue::Field(
                                    FieldName::parse("amount").assured("valid field"),
                                ),
                                presentation: ChoicePresentation {
                                    label: "amount".to_string(),
                                    detail: Some("I64 OPTIONAL".to_string()),
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
                drafts.branch.schema = Some(SelectedReference::chosen(
                    SchemaName::parse("visual_record").assured("valid name"),
                ));
                drafts.branch.ttl = "5m".to_string();
            });
            signals.open(
                CreateKind::Branch,
                Some(scope.clone()),
                "global-create-button",
            );
            let branch_markup = render();
            assert!(branch_markup.contains("Create branch"));
            assert!(branch_markup.contains("Selected schema: visual_record"));
            assert!(branch_markup.contains("CREATE BRANCH by_tenant SCHEMA visual_record TTL 5m;"));

            signals.relay.update(|draft| {
                draft.name = "visual_orders".to_string();
                draft.select_schema(&node(ModelKind::Schema, "visual_record"));
                draft.select_branch(&node(ModelKind::Branch, "by_tenant"));
                draft.capacity = "4".to_string();
            });
            signals.open(
                CreateKind::Relay,
                Some(scope.clone()),
                "global-create-button",
            );
            let relay_markup = render();
            assert!(relay_markup.contains("Create relay"));
            assert!(relay_markup.contains("Selected schema: visual_record"));
            assert!(relay_markup.contains("Selected branch: by_tenant"));
            assert!(relay_markup.contains("LAST BY TIMESTAMP"));
            assert!(relay_markup.contains(
                "CREATE RELAY visual_orders SCHEMA visual_record BRANCHED BY by_tenant CAPACITY 4;"
            ));
            signals.change_scope(Some(domain("elsewhere")));
            let changed_markup = render();
            assert!(changed_markup.contains("domain changed. Select a schema again."));
            assert!(changed_markup.contains("domain changed. Select a branch again."));

            signals.open_subscription(
                scope,
                RelayName::parse("visual_orders").assured("valid relay"),
                "graph-search",
            );
            signals.subscription.update(|draft| {
                draft.filter = "input.amount=1".to_string();
                draft.delivery = SubscriptionDeliveryBehavior::Dropping;
                draft.sampled = true;
                draft.sample_rate = "0.5".to_string();
            });
            let subscription_markup = render();
            assert!(subscription_markup.contains("Create subscription"));
            assert!(subscription_markup.contains("Selected relay: visual_orders"));
            assert!(subscription_markup.contains("Reads as WHERE input.amount = 1"));
            assert!(subscription_markup.contains("I64 OPTIONAL"));
            assert!(!subscription_markup.contains("create-if-not-exists"));
            assert!(subscription_markup.contains(
                "TO visual_orders DROPPING BATCH SAMPLE RATE 0.5 WHERE input.amount = 1;"
            ));
            signals
                .subscription
                .update(|draft| draft.filter = "input.amount =".to_string());
            let invalid_markup = render();
            assert!(invalid_markup.contains("create-filter-reading invalid"));
        });
    }
}
