//! Resolved branch declaration operations.
//!
//! Layer: vocabulary.
//! - **Owns.** Branch identity, key-schema comparison and declared unit accessors.
//! - **Depends on.** Typed branch declarations, schema definitions and vocabulary names.
//! - **Must not know.** Registry validation, scheduling decisions or runtime tasks.

use super::{BranchName, FieldName, ResolvedBranchPartitions, ResolvedBranching, Unit};

impl ResolvedBranching {
    pub fn unbranched() -> Self {
        Self::Unbranched
    }

    pub fn awaiting_topology(
        branch: BranchName,
        schema: impl Into<crate::SchemaDefinition>,
    ) -> Self {
        Self::AwaitingTopology {
            branch,
            schema: schema.into(),
        }
    }

    pub fn is_awaiting_topology(&self) -> bool {
        matches!(self, Self::AwaitingTopology { .. })
    }

    pub fn with_partitions(self, partitions: ResolvedBranchPartitions) -> Self {
        match self {
            Self::Unbranched => Self::Unbranched,
            Self::AwaitingTopology { branch, schema } | Self::Branched { branch, schema, .. } => {
                Self::Branched {
                    branch,
                    schema,
                    partitions,
                }
            }
        }
    }

    pub fn branched(
        branch: BranchName,
        schema: impl Into<crate::SchemaDefinition>,
        partitions: ResolvedBranchPartitions,
    ) -> Self {
        Self::Branched {
            branch,
            schema: schema.into(),
            partitions,
        }
    }

    pub fn branch(&self) -> Option<&BranchName> {
        match self {
            Self::Unbranched => None,
            Self::Branched { branch, .. } | Self::AwaitingTopology { branch, .. } => Some(branch),
        }
    }

    /// Whether `other` names the same branch with the same key schema, whatever partition count
    /// each places keys by: the keys and their values are the same under either count.
    pub fn same_key_as(&self, other: &Self) -> bool {
        self.branch() == other.branch() && self.schema() == other.schema()
    }

    /// The partitions of the branch, when the declaration is branched.
    pub fn partitions(&self) -> Option<ResolvedBranchPartitions> {
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
