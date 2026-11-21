//! Resolved branch declaration operations.
//!
//! Layer: vocabulary.
//! - **Owns.** Branch identity, key-schema comparison and declared unit accessors.
//! - **Depends on.** Typed branch declarations, schema definitions and vocabulary names.
//! - **Must not know.** Registry validation, scheduling decisions or runtime tasks.

use super::{FieldName, ParametrizerName, ResolvedBranching, ResolvedParametrizerPartitions, Unit};

impl ResolvedBranching {
    pub fn unbranched() -> Self {
        Self::Unbranched
    }

    pub fn awaiting_topology(
        parametrizer: ParametrizerName,
        schema: impl Into<crate::SchemaDefinition>,
    ) -> Self {
        Self::AwaitingTopology {
            parametrizer,
            schema: schema.into(),
        }
    }

    pub fn is_awaiting_topology(&self) -> bool {
        matches!(self, Self::AwaitingTopology { .. })
    }

    pub fn with_partitions(self, partitions: ResolvedParametrizerPartitions) -> Self {
        match self {
            Self::Unbranched => Self::Unbranched,
            Self::AwaitingTopology {
                parametrizer,
                schema,
            }
            | Self::Branched {
                parametrizer,
                schema,
                ..
            } => Self::Branched {
                parametrizer,
                schema,
                partitions,
            },
        }
    }

    pub fn branched(
        parametrizer: ParametrizerName,
        schema: impl Into<crate::SchemaDefinition>,
        partitions: ResolvedParametrizerPartitions,
    ) -> Self {
        Self::Branched {
            parametrizer,
            schema: schema.into(),
            partitions,
        }
    }

    pub fn parametrizer(&self) -> Option<&ParametrizerName> {
        match self {
            Self::Unbranched => None,
            Self::Branched { parametrizer, .. } | Self::AwaitingTopology { parametrizer, .. } => {
                Some(parametrizer)
            }
        }
    }

    /// Whether `other` names the same branch with the same key schema, whatever partition count
    /// each places keys by: the keys and their values are the same under either count.
    pub fn same_key_as(&self, other: &Self) -> bool {
        self.parametrizer() == other.parametrizer() && self.schema() == other.schema()
    }

    /// The partitions of the branch, when the declaration is branched.
    pub fn partitions(&self) -> Option<ResolvedParametrizerPartitions> {
        match self {
            Self::Unbranched | Self::AwaitingTopology { .. } => None,
            Self::Branched { partitions, .. } => Some(*partitions),
        }
    }

    /// The units a relay of this declaration is scheduled as: one `Single` unit when it is
    /// unbranched or its branch has one partition, and one `Partition` unit per partition
    /// otherwise.
    pub fn units(&self) -> Vec<Unit> {
        match self {
            Self::Unbranched => vec![Unit::SINGLE],
            Self::AwaitingTopology { .. } => Vec::new(),
            Self::Branched { partitions, .. } => partitions.units(),
        }
    }

    pub fn schema(&self) -> Option<&crate::SchemaDefinition> {
        match self {
            Self::Unbranched => None,
            Self::Branched { schema, .. } | Self::AwaitingTopology { schema, .. } => Some(schema),
        }
    }

    pub fn fields(&self) -> &[crate::SchemaField] {
        match self {
            Self::Unbranched => &[],
            Self::Branched { schema, .. } | Self::AwaitingTopology { schema, .. } => &schema.fields,
        }
    }

    pub fn field_names(&self) -> impl Iterator<Item = &FieldName> {
        self.fields().iter().map(|field| &field.name)
    }

    pub fn is_unbranched(&self) -> bool {
        matches!(self, Self::Unbranched)
    }
}
