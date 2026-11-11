//! Shared scope and metadata for bounded state observations.
//!
//! Layer: vocabulary.
//! - **Owns.** Complete state counts, captured owner origins, read selectors, cursor positions and refusals.
//! - **Depends on.** Node, branch and execution-unit identities and serialization.
//! - **Must not know.** Row encodings, storage, runtime tasks, sessions or Plexus grammar.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{BranchKeyFingerprint, ClusterNodeIdentity, DomainNodeRef, Unit};

/// A cluster observation has no fabricated domain or graph-node identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateInspectionEntity {
    DomainNode(DomainNodeRef),
    ClusterLicense,
}

impl From<DomainNodeRef> for StateInspectionEntity {
    fn from(entity: DomainNodeRef) -> Self {
        Self::DomainNode(entity)
    }
}

impl StateInspectionEntity {
    pub fn domain_node(&self) -> Option<&DomainNodeRef> {
        match self {
            Self::DomainNode(entity) => Some(entity),
            Self::ClusterLicense => None,
        }
    }
}

/// Counts describe the complete selected state, including rows omitted from this page.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct StateInspectionCounts {
    /// Concrete branched keys in the selection; an unbranched row adds no branch.
    pub branches: u64,
    pub rows: u64,
    /// The inspected state owner's byte charge: logical payloads or bounded live window state.
    pub bytes: u64,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct StateInspectionOrigin {
    pub node: ClusterNodeIdentity,
    /// Exact immutable cut, including every branch-local source and captured row watermark.
    pub fingerprint: [u8; 32],
    /// Membership and revision high watermarks across this unit's captured sources.
    pub generation: u64,
    pub revision: u64,
    pub stage: StateInspectionStage,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    strum::AsRefStr,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum StateInspectionStage {
    Live,
    Stored,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    strum::AsRefStr,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "the inspection protocol returns a fixed typed read refusal; internal causes are \
                  reported where the response is constructed"
    )
)]
pub enum StateInspectionRejection {
    UnknownEntity,
    NotMaterialized,
    InvalidSelection,
    InvalidLimit,
    StaleContinuation,
    OwnerUnavailable,
    NotReady,
    Deadline,
    ReplyTooLarge,
}

/// Typed sections of the current stateful processor kinds, including metadata and inference tensors.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    strum::AsRefStr,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum ProcessorStateSection {
    Pending,
    Left,
    Right,
    Keys,
    Rows,
    Header,
    Accumulators,
    /// One declared inferencer STATE entry, in declaration order from zero.
    Tensor(u32),
}

impl ProcessorStateSection {
    /// Reorderers, correlators, deduplicators and windows have fixed layouts; the window's
    /// rows, header and accumulator facts give their maximum.
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "the fixed processor layouts contain at most the window's three sections"
        )
    )]
    pub const FIXED_SECTION_COUNT: usize = 3;
    /// Stable section names do not collide with arbitrary model port names.
    pub fn name(self) -> String {
        use meticulous::OptionExt as _;
        match self {
            Self::Tensor(index) => {
                let ordinal = u64::from(index)
                    .checked_add(1)
                    .assured("a u32 tensor index plus one fits u64");
                format!("tensor_{ordinal}")
            }
            _ => self.as_ref().to_string(),
        }
    }

    /// Fixed processor layouts have at most three sections. Tensor layouts have their header
    /// first and one section per declared entry, so their index is addressed directly even for
    /// a large model. The identity check also rejects malformed peer section identifiers.
    pub fn index_in<T>(self, sections: &[T], identity: impl Fn(&T) -> Self) -> Option<usize> {
        let index = match self {
            Self::Tensor(index) => usize::try_from(index).ok()?.checked_add(1)?,
            Self::Header
                if sections
                    .first()
                    .is_some_and(|value| identity(value) == self) =>
            {
                0
            }
            _ => sections
                .iter()
                .take(Self::FIXED_SECTION_COUNT)
                .position(|value| identity(value) == self)?,
        };
        let found = sections.get(index)?;
        if identity(found) == self {
            Some(index)
        } else {
            None
        }
    }

    /// The schemas of native inference observations; tensor values inherit the sensitivity of
    /// every input and branch-key field, while shape, port names and lifecycle facts are public.
    pub fn inferencer_fields(self, sensitive: bool) -> Vec<crate::SchemaField> {
        use crate::ParseAsType;
        match self {
            Self::Header => vec![
                metadata_field("branch_fingerprint", ParseAsType::String, true),
                metadata_field("incarnation", ParseAsType::U64, false),
                metadata_field("publication_revision", ParseAsType::U64, false),
                metadata_field("persisted_revision", ParseAsType::U64, false),
                metadata_field("state_bytes", ParseAsType::U64, false),
                metadata_field("confirmed_replicas", ParseAsType::U64, false),
                metadata_field("replica_count", ParseAsType::U64, false),
                metadata_field("stage", ParseAsType::String, false),
            ],
            Self::Tensor(_) => {
                let mut values = metadata_field(
                    "values",
                    ParseAsType::Vec {
                        element: Box::new(ParseAsType::F32),
                    },
                    false,
                );
                values.sensitive = sensitive;
                vec![
                    metadata_field("input", ParseAsType::String, false),
                    metadata_field("output", ParseAsType::String, false),
                    metadata_field(
                        "shape",
                        ParseAsType::Vec {
                            element: Box::new(ParseAsType::U64),
                        },
                        false,
                    ),
                    values,
                ]
            }
            _ => Vec::new(),
        }
    }
    /// Headers and accumulator facts describe a branch's retained state without consuming the
    /// row limit or advancing a continuation within its data rows.
    pub const fn is_metadata(self) -> bool {
        matches!(self, Self::Header | Self::Accumulators)
    }

    /// The fixed, non-sensitive facts a window metadata section publishes.
    pub fn metadata_fields(self) -> Vec<crate::SchemaField> {
        use crate::ParseAsType;
        match self {
            Self::Header => vec![
                metadata_field("publication_revision", ParseAsType::U64, false),
                metadata_field("rows", ParseAsType::U64, false),
                metadata_field("first_sequence", ParseAsType::U64, true),
                metadata_field("next_sequence", ParseAsType::U64, false),
                metadata_field("incarnation", ParseAsType::U64, true),
                metadata_field("due_at", ParseAsType::Datetime, true),
                metadata_field("state_bytes", ParseAsType::U64, false),
            ],
            Self::Accumulators => vec![
                metadata_field("structure", ParseAsType::U32, false),
                metadata_field("kind", ParseAsType::String, false),
                metadata_field("count", ParseAsType::U64, false),
            ],
            Self::Pending
            | Self::Left
            | Self::Right
            | Self::Keys
            | Self::Rows
            | Self::Tensor(_) => Vec::new(),
        }
    }
}

fn metadata_field(name: &str, ty: crate::ParseAsType, optional: bool) -> crate::SchemaField {
    use meticulous::ResultExt as _;
    crate::SchemaField {
        name: crate::FieldName::try_from(name)
            .assured("fixed inspection field names satisfy the name contract"),
        ty,
        optional,
        sensitive: false,
    }
}

/// A resolved inspection selector retains no sensitive branch value.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum StateInspectionBranchSelection {
    All,
    Unbranched,
    Branch(BranchKeyFingerprint),
}

impl StateInspectionBranchSelection {
    /// Whether this selector admits the execution `branch` names; `None` is the unbranched
    /// execution.
    pub fn admits(&self, branch: Option<&BranchKeyFingerprint>) -> bool {
        match (self, branch) {
            (Self::All, _) | (Self::Unbranched, None) => true,
            (Self::Branch(selected), Some(branch)) => selected == branch,
            (Self::Unbranched, Some(_)) | (Self::Branch(_), None) => false,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum StateInspectionPosition {
    AfterUnit(Unit),
    Rows {
        unit: Unit,
        branch: Option<BranchKeyFingerprint>,
        offset: u64,
    },
}
