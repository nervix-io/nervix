//! Exact metadata carried by every domain Model.
//!
//! Layer: vocabulary.
//! - **Owns.** Label syntax, unique ordered maps, mutations, complete-set limits and inspection.
//! - **Depends on.** Existing entity identities, committed parameter values and serialization.
//! - **Must not know.** Parsing, registry state, persistence policy or running execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

mod decoding;
mod inspection;
#[cfg(test)]
mod tests;

use std::{
    borrow::Borrow,
    collections::{BTreeMap, BTreeSet},
};

use error_stack::Report;
pub use inspection::*;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ClusterParameterKey, ClusterParameterValues};

/// A label key is exact ASCII; it is never normalized as an entity name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Archive, RkyvSerialize)]
#[serde(transparent)]
#[rkyv(derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash))]
pub struct LabelKey(String);

impl LabelKey {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for LabelKey {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl TryFrom<String> for LabelKey {
    type Error = Report<EntityLabelError>;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let mut bytes = value.bytes();
        let first = bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit());
        let rest = bytes.all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'.' | b'_' | b'/' | b'-')
        });
        if !first || !rest {
            return Err(Report::new(EntityLabelError::InvalidKey { key: value }));
        }
        Ok(Self(value))
    }
}

/// One authored assignment, before duplicate detection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelAssignment {
    pub key: LabelKey,
    pub value: String,
}

/// The one current metadata map. Its iterator and encodings use lexicographic key order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Archive, RkyvSerialize)]
#[serde(transparent)]
pub struct EntityLabels(BTreeMap<LabelKey, String>);

impl EntityLabels {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&LabelKey, &str)> {
        self.0.iter().map(|(key, value)| (key, value.as_str()))
    }

    fn insert_unique(
        &mut self,
        key: LabelKey,
        value: String,
    ) -> Result<(), Report<EntityLabelError>> {
        match self.0.entry(key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(value);
                Ok(())
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                Err(Report::new(EntityLabelError::DuplicateKey {
                    key: entry.key().as_str().to_owned(),
                }))
            }
        }
    }

    /// Check the complete resulting map under the caller's captured committed policy.
    pub fn validate(&self, limits: &EntityLabelLimits) -> Result<(), Report<EntityLabelError>> {
        check_limit(
            ClusterParameterKey::EntityLabelsMaxCount,
            self.0.len(),
            limits.count,
        )?;
        let mut bytes = 0_u64;
        for (key, value) in &self.0 {
            check_limit(
                ClusterParameterKey::EntityLabelsMaxKeyBytes,
                key.as_str().len(),
                limits.key_bytes,
            )?;
            check_limit(
                ClusterParameterKey::EntityLabelsMaxValueBytes,
                value.len(),
                limits.value_bytes,
            )?;
            for length in [key.as_str().len(), value.len()] {
                let length = u64::try_from(length)
                    .map_err(|_| Report::new(EntityLabelError::ByteCountOverflow))?;
                bytes = bytes
                    .checked_add(length)
                    .ok_or_else(|| Report::new(EntityLabelError::ByteCountOverflow))?;
            }
        }
        if bytes > limits.bytes {
            return Err(Report::new(EntityLabelError::Limit {
                parameter: ClusterParameterKey::EntityLabelsMaxBytes,
                actual: bytes,
                maximum: limits.bytes,
            }));
        }
        Ok(())
    }

    /// Apply syntax-validated operands. The candidate owner validates the complete atomic run.
    pub fn apply(&mut self, mutation: &EntityLabelMutation) {
        match mutation {
            EntityLabelMutation::Set(labels) => {
                for (key, value) in &labels.0 {
                    self.0.insert(key.clone(), value.clone());
                }
            }
            EntityLabelMutation::Drop(keys) => {
                for key in &keys.0 {
                    self.0.remove(key);
                }
            }
        }
    }

    pub fn updated(
        &self,
        mutation: &EntityLabelMutation,
        limits: &EntityLabelLimits,
    ) -> Result<Self, Report<EntityLabelError>> {
        let mut candidate = self.clone();
        candidate.apply(mutation);
        candidate.validate(limits)?;
        Ok(candidate)
    }
}

impl TryFrom<Vec<LabelAssignment>> for EntityLabels {
    type Error = Report<EntityLabelError>;
    fn try_from(entries: Vec<LabelAssignment>) -> Result<Self, Self::Error> {
        let mut labels = Self::default();
        for entry in entries {
            labels.insert_unique(entry.key, entry.value)?;
        }
        Ok(labels)
    }
}

/// An authored set of distinct keys to remove.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Archive, RkyvSerialize)]
#[serde(transparent)]
pub struct EntityLabelKeys(BTreeSet<LabelKey>);

impl EntityLabelKeys {
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &LabelKey> {
        self.0.iter()
    }
}

impl TryFrom<Vec<LabelKey>> for EntityLabelKeys {
    type Error = Report<EntityLabelError>;
    fn try_from(keys: Vec<LabelKey>) -> Result<Self, Self::Error> {
        let mut unique = BTreeSet::new();
        for key in keys {
            if !unique.insert(key.clone()) {
                return Err(Report::new(EntityLabelError::DuplicateKey { key: key.0 }));
            }
        }
        Ok(Self(unique))
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum EntityLabelMutation {
    Set(EntityLabels),
    Drop(EntityLabelKeys),
}

/// A complete policy projection; callers obtain it from one committed parameter record.
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
#[serde(deny_unknown_fields)]
pub struct EntityLabelLimits {
    pub count: u64,
    pub key_bytes: u64,
    pub value_bytes: u64,
    pub bytes: u64,
}

impl From<&ClusterParameterValues> for EntityLabelLimits {
    fn from(values: &ClusterParameterValues) -> Self {
        Self {
            count: values.entity_labels_max_count(),
            key_bytes: values.entity_labels_max_key_bytes(),
            value_bytes: values.entity_labels_max_value_bytes(),
            bytes: values.entity_labels_max_bytes(),
        }
    }
}

#[derive(Debug, Error)]
pub enum EntityLabelError {
    #[error("invalid label key {key:?}: expected [a-z0-9][a-z0-9._/-]*")]
    InvalidKey { key: String },
    #[error("duplicate label key {key:?}")]
    DuplicateKey { key: String },
    #[error("label set exceeds {parameter}: {actual} > {maximum}")]
    Limit {
        parameter: ClusterParameterKey,
        actual: u64,
        maximum: u64,
    },
    #[error("archived label keys are not in strictly increasing order")]
    NonCanonicalKeys,
    #[error("label byte count exceeds the representable bound")]
    ByteCountOverflow,
}

fn check_limit(
    parameter: ClusterParameterKey,
    length: usize,
    maximum: u64,
) -> Result<(), Report<EntityLabelError>> {
    let actual =
        u64::try_from(length).map_err(|_| Report::new(EntityLabelError::ByteCountOverflow))?;
    if actual > maximum {
        return Err(Report::new(EntityLabelError::Limit {
            parameter,
            actual,
            maximum,
        }));
    }
    Ok(())
}

/// The metadata projection implemented by every current Model payload.
pub trait EntityMetadata {
    fn labels(&self) -> &EntityLabels;
    fn labels_mut(&mut self) -> &mut EntityLabels;
}
