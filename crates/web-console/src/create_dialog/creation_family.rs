//! The creation families of the visual create dialog.
//!
//! Layer: edges.
//!
//! - **Owns.** The families the console creates: each statement outside the Model vocabulary and
//!   each Model kind, their keys, names and Create-menu presentation, and which shared form a
//!   family uses.
//! - **Depends on.** The vocabulary's Model kinds and the form families the dialog's drafts define.
//! - **Must not know.** Draft contents, choice lookups, statement dispatch, or browser transport.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use nervix_models::ModelKind;

use super::{processor_draft::ProcessorFamily, schema_draft::WireFormat};

/// A creation family the console has a form for. The statements outside the Model vocabulary
/// have their own variants, and every Model kind is one family, so the dispatch is keyed by the
/// vocabulary's own identity: a kind added to it does not compile here until the console decides
/// its form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CreateKind {
    Domain,
    User,
    Resource,
    /// A session subscription, created by the client statement rather than stored.
    Subscription,
    Model(ModelKind),
}

impl Ord for CreateKind {
    /// Orders families by their menu key, a name, rather than by the declaration order of
    /// [`ModelKind`], which the vocabulary deliberately leaves unordered.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key().cmp(&other.key())
    }
}

impl PartialOrd for CreateKind {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl CreateKind {
    /// Every family the Create menu offers, in its order: each statement outside the Model
    /// vocabulary and each [`ModelKind`] exactly once.
    pub(crate) const MENU: [Self; 29] = [
        Self::Domain,
        Self::Model(ModelKind::Placement),
        Self::User,
        Self::Resource,
        Self::Model(ModelKind::Schema),
        Self::Model(ModelKind::WireJsonSchema),
        Self::Model(ModelKind::WireCborSchema),
        Self::Model(ModelKind::WireAvroSchema),
        Self::Model(ModelKind::Branch),
        Self::Model(ModelKind::Relay),
        Self::Subscription,
        Self::Model(ModelKind::Codec),
        Self::Model(ModelKind::SignalingProtocol),
        Self::Model(ModelKind::Client),
        Self::Model(ModelKind::Vhost),
        Self::Model(ModelKind::Endpoint),
        Self::Model(ModelKind::Lookup),
        Self::Model(ModelKind::Udf),
        Self::Model(ModelKind::Ingestor),
        Self::Model(ModelKind::Emitter),
        Self::Model(ModelKind::Junction),
        Self::Model(ModelKind::Deduplicator),
        Self::Model(ModelKind::Reorderer),
        Self::Model(ModelKind::WindowProcessor),
        Self::Model(ModelKind::Correlator),
        Self::Model(ModelKind::Reingestor),
        Self::Model(ModelKind::Generator),
        Self::Model(ModelKind::Inferencer),
        Self::Model(ModelKind::WasmProcessor),
    ];

    /// The key a menu item and a contextual action carry for this family: a Model kind's keyword
    /// phrase in lower case, words joined by hyphens.
    pub(crate) fn key(self) -> String {
        match self {
            Self::Domain => "domain".to_string(),
            Self::User => "user".to_string(),
            Self::Resource => "resource".to_string(),
            Self::Subscription => "subscription".to_string(),
            Self::Model(kind) => kind.keyword_phrase().to_ascii_lowercase().replace(' ', "-"),
        }
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Domain => "domain",
            Self::User => "user",
            Self::Resource => "resource",
            Self::Subscription => "subscription",
            Self::Model(kind) => Self::model_label(kind),
        }
    }

    /// The name a Model kind's family uses in its form's title and actions.
    fn model_label(kind: ModelKind) -> &'static str {
        match kind {
            ModelKind::Placement => "placement",
            ModelKind::Schema => "schema",
            ModelKind::WireJsonSchema => "wire JSON schema",
            ModelKind::WireCborSchema => "wire CBOR schema",
            ModelKind::WireAvroSchema => "wire AVRO schema",
            ModelKind::Branch => "branch",
            ModelKind::Relay => "relay",
            ModelKind::Codec => "codec",
            ModelKind::SignalingProtocol => "signaling protocol",
            ModelKind::Client => "client",
            ModelKind::Vhost => "VHOST",
            ModelKind::Endpoint => "endpoint",
            ModelKind::Lookup => "hash map",
            ModelKind::Udf => "Roto UDF",
            ModelKind::Ingestor => "ingestor",
            ModelKind::Emitter => "emitter",
            ModelKind::Junction => "junction",
            ModelKind::Deduplicator => "deduplicator",
            ModelKind::Reorderer => "reorderer",
            ModelKind::WindowProcessor => "window processor",
            ModelKind::Correlator => "correlator",
            ModelKind::Reingestor => "reingestor",
            ModelKind::Generator => "generator",
            ModelKind::Inferencer => "inferencer",
            ModelKind::WasmProcessor => "WASM processor",
        }
    }

    /// The name and the one-line summary the Create menu shows for this family.
    pub(crate) fn menu_entry(self) -> MenuEntry {
        match self {
            Self::Domain => MenuEntry {
                name: "Domain",
                summary: "Clock and lifecycle",
            },
            Self::User => MenuEntry {
                name: "User",
                summary: "Login credential",
            },
            Self::Resource => MenuEntry {
                name: "Resource",
                summary: "Catalog and upload",
            },
            Self::Subscription => MenuEntry {
                name: "Subscription",
                summary: "Read-only relay tab",
            },
            Self::Model(kind) => Self::model_menu_entry(kind),
        }
    }

    /// The name and the one-line summary the Create menu shows for a Model kind's family.
    fn model_menu_entry(kind: ModelKind) -> MenuEntry {
        let (name, summary) = match kind {
            ModelKind::Placement => ("Placement", "Domain-wide or corridor policy"),
            ModelKind::Schema => ("Schema", "Internal record fields"),
            ModelKind::WireJsonSchema => ("Wire JSON schema", "JSON payload fields"),
            ModelKind::WireCborSchema => ("Wire CBOR schema", "CBOR payload fields"),
            ModelKind::WireAvroSchema => ("Wire AVRO schema", "AVRO payload fields"),
            ModelKind::Branch => ("Branch", "Key schema and lifetime"),
            ModelKind::Relay => ("Relay", "Schema, branching and capacity"),
            ModelKind::Codec => ("Codec", "Wire format and schema mapping"),
            ModelKind::SignalingProtocol => ("Signaling protocol", "Ordered connection handshake"),
            ModelKind::Client => ("Client", "External transport configuration"),
            ModelKind::Vhost => ("VHOST", "Hostnames and optional TLS"),
            ModelKind::Endpoint => ("Endpoint", "HTTP or WebSocket path"),
            ModelKind::Lookup => ("Hash map", "Resource-backed lookup"),
            ModelKind::Udf => ("Roto UDF", "Typed function and source tests"),
            ModelKind::Ingestor => ("Ingestor", "External source and ordered routes"),
            ModelKind::Emitter => ("Emitter", "External publication and client output"),
            ModelKind::Junction => ("Junction", "Transform and fan out relay records"),
            ModelKind::Deduplicator => ("Deduplicator", "Suppress repeated keys in each branch"),
            ModelKind::Reorderer => ("Reorderer", "Release each branch in key order"),
            ModelKind::WindowProcessor => (
                "Window processor",
                "Aggregate each branch over WIDTH and STEP",
            ),
            ModelKind::Correlator => ("Correlator", "Match left and right records in each branch"),
            ModelKind::Reingestor => ("Reingestor", "Transform records across branch boundaries"),
            ModelKind::Generator => ("Generator", "Generate from materialized state on a cadence"),
            ModelKind::Inferencer => ("Inferencer", "Score each branch with an ONNX model"),
            ModelKind::WasmProcessor => {
                ("WASM processor", "Run a guest with fuel and memory limits")
            }
        };
        MenuEntry { name, summary }
    }

    pub(super) fn domain_scoped(self) -> bool {
        !matches!(self, Self::Domain | Self::User)
    }

    /// Whether the created statement takes `IF NOT EXISTS`. A session subscription is not a stored
    /// entity, so its statement has no such modifier.
    pub(super) fn takes_if_not_exists(self) -> bool {
        self != Self::Subscription
    }

    pub(super) fn wire_format(self) -> Option<WireFormat> {
        match self {
            Self::Model(ModelKind::WireJsonSchema) => Some(WireFormat::Json),
            Self::Model(ModelKind::WireCborSchema) => Some(WireFormat::Cbor),
            Self::Model(ModelKind::WireAvroSchema) => Some(WireFormat::Avro),
            Self::Domain | Self::User | Self::Resource | Self::Subscription => None,
            Self::Model(
                ModelKind::Placement
                | ModelKind::Schema
                | ModelKind::Branch
                | ModelKind::Relay
                | ModelKind::Codec
                | ModelKind::SignalingProtocol
                | ModelKind::Client
                | ModelKind::Vhost
                | ModelKind::Endpoint
                | ModelKind::Lookup
                | ModelKind::Udf
                | ModelKind::Ingestor
                | ModelKind::Emitter
                | ModelKind::Junction
                | ModelKind::Deduplicator
                | ModelKind::Reorderer
                | ModelKind::WindowProcessor
                | ModelKind::Correlator
                | ModelKind::Reingestor
                | ModelKind::Generator
                | ModelKind::Inferencer
                | ModelKind::WasmProcessor,
            ) => None,
        }
    }

    /// The processor family the shared processor form creates for this kind.
    pub(super) fn processor_family(self) -> Option<ProcessorFamily> {
        match self {
            Self::Model(ModelKind::Junction) => Some(ProcessorFamily::Junction),
            Self::Model(ModelKind::Deduplicator) => Some(ProcessorFamily::Deduplicator),
            Self::Model(ModelKind::Reorderer) => Some(ProcessorFamily::Reorderer),
            Self::Model(ModelKind::WindowProcessor) => Some(ProcessorFamily::Window),
            Self::Model(ModelKind::Correlator) => Some(ProcessorFamily::Correlator),
            Self::Model(ModelKind::Reingestor) => Some(ProcessorFamily::Reingestor),
            Self::Model(ModelKind::Generator) => Some(ProcessorFamily::Generator),
            Self::Model(ModelKind::Inferencer) => Some(ProcessorFamily::Inferencer),
            Self::Model(ModelKind::WasmProcessor) => Some(ProcessorFamily::Wasm),
            Self::Domain | Self::User | Self::Resource | Self::Subscription => None,
            Self::Model(
                ModelKind::Placement
                | ModelKind::Schema
                | ModelKind::WireJsonSchema
                | ModelKind::WireCborSchema
                | ModelKind::WireAvroSchema
                | ModelKind::Branch
                | ModelKind::Relay
                | ModelKind::Codec
                | ModelKind::SignalingProtocol
                | ModelKind::Client
                | ModelKind::Vhost
                | ModelKind::Endpoint
                | ModelKind::Lookup
                | ModelKind::Udf
                | ModelKind::Ingestor
                | ModelKind::Emitter,
            ) => None,
        }
    }
}

/// What the Create menu shows for one family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MenuEntry {
    pub(crate) name: &'static str,
    pub(crate) summary: &'static str,
}
