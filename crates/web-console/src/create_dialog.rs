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

use super::{ConsoleConnectionState, ConsoleRequest, request_handoff::RequestSender};

mod choice_group;
mod client_draft;
mod client_editor;
mod codec_draft;
mod codec_editor;
mod endpoint_draft;
mod endpoint_editor;
mod hash_map_draft;
mod hash_map_editor;
mod relay_draft;
mod relay_editor;
mod resource_binding_draft;
mod resource_binding_editor;
mod resource_pin_draft;
mod schema_draft;
mod schema_editor;
mod signaling_draft;
mod signaling_editor;
mod subscription_draft;
mod subscription_editor;
mod udf_draft;
mod udf_editor;
mod vhost_draft;
mod vhost_editor;
#[cfg(test)]
mod visual_forms_tests;

use choice_group::ChoiceGroup;
#[cfg(test)]
use choice_group::{ChoiceGroupProps, select_choice, selected_choice};
use client_draft::{ClientDraft, ClientDraftError, ClientTransport};
use client_editor::ClientEditor;
use codec_draft::{CodecDraft, CodecDraftError, CodecFormatDraft, CodecFormatKind};
use codec_editor::CodecEditor;
use endpoint_draft::{EndpointDraft, EndpointDraftError};
use endpoint_editor::EndpointEditor;
use hash_map_draft::{HashMapDraft, HashMapDraftError};
use hash_map_editor::HashMapEditor;
use relay_draft::{RelayDraft, RelayDraftError};
use relay_editor::RelayEditor;
use schema_draft::{SchemaDraftError, StructuredDrafts, WireFormat};
use schema_editor::{BranchEditor, SchemaEditor, WireSchemaEditor};
use signaling_draft::{SignalingDraft, SignalingDraftError, SignalingFormatDraft};
use signaling_editor::SignalingEditor;
use subscription_draft::{SubscriptionDraft, SubscriptionDraftError};
use subscription_editor::SubscriptionEditor;
use udf_draft::{UdfDraft, UdfDraftError};
use udf_editor::UdfEditor;
use vhost_draft::{VhostDraft, VhostDraftError};
use vhost_editor::VhostEditor;

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
    Client,
    Vhost,
    Endpoint,
    HashMap,
    Udf,
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
            Self::Client => "client",
            Self::Vhost => "VHOST",
            Self::Endpoint => "endpoint",
            Self::HashMap => "hash map",
            Self::Udf => "Roto UDF",
        }
    }

    fn domain_scoped(self) -> bool {
        !matches!(self, Self::Domain | Self::User)
    }

    /// Whether the created statement takes `IF NOT EXISTS`. A session subscription is not a stored
    /// entity, so its statement has no such modifier.
    fn takes_if_not_exists(self) -> bool {
        self != Self::Subscription
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
            | Self::SignalingProtocol
            | Self::Client
            | Self::Vhost
            | Self::Endpoint => None,
            Self::HashMap | Self::Udf => None,
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
    ClientResource,
    ClientVersion,
    ClientSignaling,
    VhostResource,
    VhostVersion,
    EndpointVhost,
    EndpointSignaling,
    HashResource,
    HashVersion,
    HashCodec,
    HashKey,
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
            Self::ClientResource | Self::ClientVersion | Self::ClientSignaling => {
                CreateKind::Client
            }
            Self::VhostResource | Self::VhostVersion => CreateKind::Vhost,
            Self::EndpointVhost | Self::EndpointSignaling => CreateKind::Endpoint,
            Self::HashResource | Self::HashVersion | Self::HashCodec | Self::HashKey => {
                CreateKind::HashMap
            }
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
    client_resource: ChoiceControlSignals,
    client_version: ChoiceControlSignals,
    client_signaling: ChoiceControlSignals,
    vhost_resource: ChoiceControlSignals,
    vhost_version: ChoiceControlSignals,
    endpoint_vhost: ChoiceControlSignals,
    endpoint_signaling: ChoiceControlSignals,
    hash_resource: ChoiceControlSignals,
    hash_version: ChoiceControlSignals,
    hash_codec: ChoiceControlSignals,
    hash_key: ChoiceControlSignals,
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
            client_resource: ChoiceControlSignals::new(),
            client_version: ChoiceControlSignals::new(),
            client_signaling: ChoiceControlSignals::new(),
            vhost_resource: ChoiceControlSignals::new(),
            vhost_version: ChoiceControlSignals::new(),
            endpoint_vhost: ChoiceControlSignals::new(),
            endpoint_signaling: ChoiceControlSignals::new(),
            hash_resource: ChoiceControlSignals::new(),
            hash_version: ChoiceControlSignals::new(),
            hash_codec: ChoiceControlSignals::new(),
            hash_key: ChoiceControlSignals::new(),
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
            ChoiceControl::ClientResource => self.client_resource,
            ChoiceControl::ClientVersion => self.client_version,
            ChoiceControl::ClientSignaling => self.client_signaling,
            ChoiceControl::VhostResource => self.vhost_resource,
            ChoiceControl::VhostVersion => self.vhost_version,
            ChoiceControl::EndpointVhost => self.endpoint_vhost,
            ChoiceControl::EndpointSignaling => self.endpoint_signaling,
            ChoiceControl::HashResource => self.hash_resource,
            ChoiceControl::HashVersion => self.hash_version,
            ChoiceControl::HashCodec => self.hash_codec,
            ChoiceControl::HashKey => self.hash_key,
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
    #[error("{0}")]
    Client(#[from] ClientDraftError),
    #[error("{0}")]
    Vhost(#[from] VhostDraftError),
    #[error("{0}")]
    Endpoint(#[from] EndpointDraftError),
    #[error("{0}")]
    HashMap(#[from] HashMapDraftError),
    #[error("{0}")]
    Udf(#[from] UdfDraftError),
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
        let presentation = model.clone();
        Self::domain_requested_model_with_presentation(
            kind,
            model,
            presentation,
            if_not_exists,
            scope,
        )
    }

    fn domain_requested_model_with_presentation(
        kind: CreateKind,
        model: Model<RequestedResourceVersion>,
        presentation_model: Model<RequestedResourceVersion>,
        if_not_exists: bool,
        scope: DomainName,
    ) -> error_stack::Result<Self, CreateDraftError> {
        let statement = Statement::Create(CreateStatement::new(Box::new(model), if_not_exists));
        let query = statement
            .to_canonical_nspl()
            .change_context(CreateDraftError::CanonicalNspl)?;
        let presentation = Statement::Create(CreateStatement::new(
            Box::new(presentation_model),
            if_not_exists,
        ))
        .to_canonical_nspl()
        .change_context(CreateDraftError::CanonicalNspl)?;
        Ok(Self {
            kind,
            presentation,
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
    client: RwSignal<ClientDraft>,
    vhost: RwSignal<VhostDraft>,
    endpoint: RwSignal<EndpointDraft>,
    hash_map: RwSignal<HashMapDraft>,
    udf: RwSignal<UdfDraft>,
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
            client: RwSignal::new(ClientDraft::default()),
            vhost: RwSignal::new(VhostDraft::default()),
            endpoint: RwSignal::new(EndpointDraft::default()),
            hash_map: RwSignal::new(HashMapDraft::default()),
            udf: RwSignal::new(UdfDraft::default()),
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
                CreateKind::Client => self.client.update(ClientDraft::invalidate_references),
                CreateKind::Vhost => self.vhost.update(VhostDraft::invalidate_references),
                CreateKind::Endpoint => self.endpoint.update(EndpointDraft::invalidate_references),
                CreateKind::HashMap => self.hash_map.update(HashMapDraft::invalidate_references),
                CreateKind::Udf => {}
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
            ChoiceControl::CodecResource
            | ChoiceControl::SignalingResource
            | ChoiceControl::ClientResource
            | ChoiceControl::VhostResource
            | ChoiceControl::HashResource => self.domain_question(
                ChoiceTarget::Resource,
                "Select a domain before choosing a resource",
            ),
            ChoiceControl::CodecVersion
            | ChoiceControl::SignalingVersion
            | ChoiceControl::ClientVersion
            | ChoiceControl::VhostVersion
            | ChoiceControl::HashVersion => {
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
                    ChoiceControl::ClientVersion => {
                        self.client.get_untracked().current_resource().cloned()
                    }
                    ChoiceControl::VhostVersion => {
                        self.vhost.get_untracked().current_resource().cloned()
                    }
                    ChoiceControl::HashVersion => self
                        .hash_map
                        .get_untracked()
                        .pin
                        .current_resource()
                        .cloned(),
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
            ChoiceControl::ClientSignaling | ChoiceControl::EndpointSignaling => self
                .domain_question(
                    ChoiceTarget::SignalingProtocol,
                    "Select a domain before choosing a signaling protocol",
                ),
            ChoiceControl::EndpointVhost => self.domain_question(
                ChoiceTarget::Vhost,
                "Select a domain before choosing a VHOST",
            ),
            ChoiceControl::HashCodec => self.domain_question(
                ChoiceTarget::Codec,
                "Select a domain before choosing a codec",
            ),
            ChoiceControl::HashKey => {
                let Some(domain) = self.captured_domain.get_untracked() else {
                    return Err("Select a domain before choosing a key field");
                };
                let draft = self.hash_map.get_untracked();
                let Some(codec) = draft.current_codec() else {
                    return Err("Select a codec to list the fields of its output schema");
                };
                Ok(ChoiceQuery {
                    target: ChoiceTarget::CodecField,
                    dependencies: vec![
                        ChoiceSelection {
                            value: ChoiceValue::Domain(domain),
                        },
                        ChoiceSelection {
                            value: ChoiceValue::Model(nervix_models::NodeRef::new(
                                ModelKind::Codec,
                                codec,
                            )),
                        },
                    ],
                    page_size: 100,
                })
            }
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
            CreateKind::Client => {
                let scope = captured_domain
                    .ok_or_else(|| Report::new(CreateDraftError::ScopedDomainRequired))?;
                let draft = self.client.get_untracked();
                let completed = draft.build().map_err(draft_error)?;
                CreateSubmission::domain_requested_model_with_presentation(
                    kind,
                    completed.actual,
                    completed.presentation,
                    draft.if_not_exists,
                    scope,
                )
            }
            CreateKind::Vhost => {
                let scope = captured_domain
                    .ok_or_else(|| Report::new(CreateDraftError::ScopedDomainRequired))?;
                let draft = self.vhost.get_untracked();
                let vhost = draft.build().map_err(draft_error)?;
                CreateSubmission::domain_requested_model(
                    kind,
                    Model::Vhost(vhost),
                    draft.if_not_exists,
                    scope,
                )
            }
            CreateKind::Endpoint => {
                let scope = captured_domain
                    .ok_or_else(|| Report::new(CreateDraftError::ScopedDomainRequired))?;
                let draft = self.endpoint.get_untracked();
                let endpoint = draft.build().map_err(draft_error)?;
                CreateSubmission::domain_model(
                    kind,
                    Model::Endpoint(endpoint),
                    draft.if_not_exists,
                    scope,
                )
            }
            CreateKind::HashMap => {
                let scope = captured_domain
                    .ok_or_else(|| Report::new(CreateDraftError::ScopedDomainRequired))?;
                let draft = self.hash_map.get_untracked();
                let lookup = draft.build().map_err(draft_error)?;
                CreateSubmission::domain_requested_model(
                    kind,
                    Model::Lookup(lookup),
                    draft.if_not_exists,
                    scope,
                )
            }
            CreateKind::Udf => {
                let scope = captured_domain
                    .ok_or_else(|| Report::new(CreateDraftError::ScopedDomainRequired))?;
                let draft = self.udf.get_untracked();
                let udf = draft.build().map_err(draft_error)?;
                CreateSubmission::domain_model(kind, Model::Udf(udf), draft.if_not_exists, scope)
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
                <button type="button" role="menuitem" data-create-kind="client" on:click=move |_| choose(CreateKind::Client)>
                    <span>"Client"</span><em>"External transport configuration"</em>
                </button>
                <button type="button" role="menuitem" data-create-kind="vhost" on:click=move |_| choose(CreateKind::Vhost)>
                    <span>"VHOST"</span><em>"Hostnames and optional TLS"</em>
                </button>
                <button type="button" role="menuitem" data-create-kind="endpoint" on:click=move |_| choose(CreateKind::Endpoint)>
                    <span>"Endpoint"</span><em>"HTTP or WebSocket path"</em>
                </button>
                <button type="button" role="menuitem" data-create-kind="hash-map" on:click=move |_| choose(CreateKind::HashMap)>
                    <span>"Hash map"</span><em>"Resource-backed lookup"</em>
                </button>
                <button type="button" role="menuitem" data-create-kind="udf" on:click=move |_| choose(CreateKind::Udf)>
                    <span>"Roto UDF"</span><em>"Typed function and source tests"</em>
                </button>
            </div>
        </div>
    }
}

fn request_choices(
    signals: CreateSignals,
    control: ChoiceControl,
    request_tx: RwSignal<Option<RequestSender>>,
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
    if let Err(refusal) = request_tx.send(ConsoleRequest::Choice { request, context }) {
        control_signals
            .load
            .set(ChoiceLoad::Failed(refusal.current_context().to_string()));
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
        CreateKind::Client => {
            let draft = signals.client.get_untracked();
            let mut controls = Vec::new();
            if draft.mount_enabled {
                controls.push(ChoiceControl::ClientResource);
                controls.push(ChoiceControl::ClientVersion);
            }
            if draft.transport.is_some_and(ClientTransport::websockets) {
                controls.push(ChoiceControl::ClientSignaling);
            }
            controls
        }
        CreateKind::Vhost => {
            if signals.vhost.get_untracked().tls_enabled {
                vec![ChoiceControl::VhostResource, ChoiceControl::VhostVersion]
            } else {
                Vec::new()
            }
        }
        CreateKind::Endpoint => {
            let mut controls = vec![ChoiceControl::EndpointVhost];
            if signals.endpoint.get_untracked().endpoint_type
                == Some(nervix_models::EndpointType::Websockets)
            {
                controls.push(ChoiceControl::EndpointSignaling);
            }
            controls
        }
        CreateKind::HashMap => vec![
            ChoiceControl::HashResource,
            ChoiceControl::HashVersion,
            ChoiceControl::HashCodec,
            ChoiceControl::HashKey,
        ],
        CreateKind::Udf => Vec::new(),
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
    request_tx: RwSignal<Option<RequestSender>>,
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
                        <Show when=move || signals.open.get() == Some(CreateKind::Client) fallback=|| ()>
                            <ClientEditor signals=signals name_input=name_input request_tx=request_tx session_generation=session_generation />
                        </Show>
                        <Show when=move || signals.open.get() == Some(CreateKind::Vhost) fallback=|| ()>
                            <VhostEditor signals=signals name_input=name_input request_tx=request_tx session_generation=session_generation />
                        </Show>
                        <Show when=move || signals.open.get() == Some(CreateKind::Endpoint) fallback=|| ()>
                            <EndpointEditor signals=signals name_input=name_input request_tx=request_tx session_generation=session_generation />
                        </Show>
                        <Show when=move || signals.open.get() == Some(CreateKind::HashMap) fallback=|| ()>
                            <HashMapEditor signals=signals name_input=name_input request_tx=request_tx session_generation=session_generation />
                        </Show>
                        <Show when=move || signals.open.get() == Some(CreateKind::Udf) fallback=|| ()>
                            <UdfEditor signals=signals name_input=name_input />
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
                                        Some(CreateKind::Client) => signals.client.get().if_not_exists,
                                        Some(CreateKind::Vhost) => signals.vhost.get().if_not_exists,
                                        Some(CreateKind::Endpoint) => signals.endpoint.get().if_not_exists,
                                        Some(CreateKind::HashMap) => signals.hash_map.get().if_not_exists,
                                        Some(CreateKind::Udf) => signals.udf.get().if_not_exists,
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
                                            Some(CreateKind::Client) => signals.client.update(|draft| draft.if_not_exists = checked),
                                            Some(CreateKind::Vhost) => signals.vhost.update(|draft| draft.if_not_exists = checked),
                                            Some(CreateKind::Endpoint) => signals.endpoint.update(|draft| draft.if_not_exists = checked),
                                            Some(CreateKind::HashMap) => signals.hash_map.update(|draft| draft.if_not_exists = checked),
                                            Some(CreateKind::Udf) => signals.udf.update(|draft| draft.if_not_exists = checked),
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
mod tests;
