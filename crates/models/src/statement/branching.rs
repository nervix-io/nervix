//! Resolved branch declaration operations.
//!
//! Layer: vocabulary.
//! - **Owns.** Branch identity, key-schema comparison and declared unit accessors.
//! - **Depends on.** Typed branch declarations, schema definitions and vocabulary names.
//! - **Must not know.** Registry validation, scheduling decisions or runtime tasks.

use super::{BranchName, BranchPartitions, FieldName, ResolvedBranching, Unit};

impl ResolvedBranching {
    pub fn unbranched() -> Self {
        Self::Unbranched
    }

    pub fn branched(
        branch: BranchName,
        schema: impl Into<crate::SchemaDefinition>,
        partitions: BranchPartitions,
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
            Self::Branched { branch, .. } => Some(branch),
        }
    }

    /// Whether `other` names the same branch with the same key schema, whatever partition count
    /// each places keys by: the keys and their values are the same under either count.
    pub fn same_key_as(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Branched { branch, schema, .. },
                Self::Branched {
                    branch: other_branch,
                    schema: other_schema,
                    ..
                },
            ) => branch == other_branch && schema == other_schema,
            (Self::Unbranched, Self::Unbranched) => true,
            (Self::Unbranched, Self::Branched { .. })
            | (Self::Branched { .. }, Self::Unbranched) => false,
        }
    }

    /// The partitions of the branch, when the declaration is branched.
    pub fn partitions(&self) -> Option<BranchPartitions> {
        match self {
            Self::Unbranched => None,
            Self::Branched { partitions, .. } => Some(*partitions),
        }
    }

    /// The units a relay of this declaration is scheduled as: one `Single` unit when it is
    /// unbranched or its branch has one partition, and one `Partition` unit per partition
    /// otherwise.
    pub fn units(&self) -> Vec<Unit> {
        match self {
            Self::Unbranched => vec![Unit::SINGLE],
            Self::Branched { partitions, .. } => partitions.units(),
        }
    }

    pub fn schema(&self) -> Option<&crate::SchemaDefinition> {
        match self {
            Self::Unbranched => None,
            Self::Branched { schema, .. } => Some(schema),
        }
    }

    pub fn fields(&self) -> &[crate::SchemaField] {
        match self {
            Self::Unbranched => &[],
            Self::Branched { schema, .. } => &schema.fields,
        }
    }

    pub fn field_names(&self) -> impl Iterator<Item = &FieldName> {
        self.fields().iter().map(|field| &field.name)
    }

    pub fn is_unbranched(&self) -> bool {
        matches!(self, Self::Unbranched)
    }
}
