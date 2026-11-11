//! Concrete key origins in one bounded processor inspection census.
//!
//! Layer: vocabulary.
//! - **Owns.** Requested key membership and inherited origin witnesses for copy selection.
//! - **Depends on.** Opaque key fingerprints and committed inherited origins.
//! - **Must not know.** Runtime actors, checkpoints, coordinator policy or transport.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns inspection census vocabulary")
)]

use std::collections::{BTreeMap, BTreeSet, btree_map, btree_set};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{BranchKeyFingerprint, InheritedPartitionOrigin};

/// Direct censuses keep their fields-only set. Only inherited keys require origin witnesses.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum StateInspectionKeyCatalog {
    #[default]
    Unrequested,
    Direct(BTreeSet<BranchKeyFingerprint>),
    Inherited(Box<BTreeMap<BranchKeyFingerprint, Option<InheritedPartitionOrigin>>>),
}

impl StateInspectionKeyCatalog {
    pub fn requested() -> Self {
        Self::Direct(BTreeSet::new())
    }

    pub fn is_requested(&self) -> bool {
        !matches!(self, Self::Unrequested)
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Unrequested => 0,
            Self::Direct(keys) => keys.len(),
            Self::Inherited(keys) => keys.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn insert(&mut self, key: BranchKeyFingerprint, origin: Option<InheritedPartitionOrigin>) {
        match self {
            Self::Unrequested => {
                *self = Self::requested();
                self.insert(key, origin);
            }
            Self::Direct(keys) if origin.is_none() => {
                keys.insert(key);
            }
            Self::Direct(keys) => {
                let mut positions = std::mem::take(keys)
                    .into_iter()
                    .map(|key| (key, None))
                    .collect::<BTreeMap<_, _>>();
                positions.insert(key, origin);
                *self = Self::Inherited(Box::new(positions));
            }
            Self::Inherited(keys) => {
                keys.insert(key, origin);
            }
        }
    }

    pub fn origin(&self, key: &BranchKeyFingerprint) -> Option<InheritedPartitionOrigin> {
        match self {
            Self::Inherited(keys) => keys.get(key).copied().flatten(),
            Self::Unrequested | Self::Direct(_) => None,
        }
    }

    pub fn fingerprints(&self) -> StateInspectionFingerprints<'_> {
        match self {
            Self::Unrequested => StateInspectionFingerprints::Unrequested,
            Self::Direct(keys) => StateInspectionFingerprints::Direct(keys.iter()),
            Self::Inherited(keys) => StateInspectionFingerprints::Inherited(keys.keys()),
        }
    }
}

pub enum StateInspectionFingerprints<'a> {
    Unrequested,
    Direct(btree_set::Iter<'a, BranchKeyFingerprint>),
    Inherited(btree_map::Keys<'a, BranchKeyFingerprint, Option<InheritedPartitionOrigin>>),
}

impl<'a> Iterator for StateInspectionFingerprints<'a> {
    type Item = &'a BranchKeyFingerprint;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Unrequested => None,
            Self::Direct(keys) => keys.next(),
            Self::Inherited(keys) => keys.next(),
        }
    }
}
