//! The models of one domain, keyed by the node each one configures.
//!
//! A domain's configuration is a set of [`Model`]s that are addressed by [`NodeRef`]. Keeping them
//! in a plain map lets a key and the model stored under it disagree, and every reader then has to
//! answer for a schema stored under a relay key. [`ModelIndex`] reads the key from the model
//! itself, so that disagreement has no way in, and readers ask for the shape they need instead of
//! checking what they were handed.

use ahash_compile_time::{HashMap, HashMapExt};
use meticulous::OptionExt as _;

use crate::{Model, ModelKind, ModelName, NodeRef, UniquelyKindedModel};

/// The models of one domain, keyed by the node each one configures.
///
/// `Version` is the type the indexed models bind resource versions with, matching [`Model`]:
/// stored configuration binds numbers, and configuration a transaction has queued but not planned
/// keeps the versions its statements wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelIndex<Version = u64> {
    by_node: HashMap<NodeRef, Model<Version>>,
}

impl<Version> Default for ModelIndex<Version> {
    fn default() -> Self {
        Self {
            by_node: HashMap::new(),
        }
    }
}

impl<Version> ModelIndex<Version> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stores `model` under the node it configures, replacing the model already there.
    pub fn insert(&mut self, model: Model<Version>) -> Option<Model<Version>> {
        self.by_node.insert(model.node_ref(), model)
    }

    /// Removes the model configuring `node`.
    pub fn remove(&mut self, node: &NodeRef) -> Option<Model<Version>> {
        self.by_node.remove(node)
    }

    /// The model configuring `node`, whatever its kind.
    ///
    /// Readers that already name the kind use [`ModelIndex::configured`] and receive its shape.
    pub fn get(&self, node: &NodeRef) -> Option<&Model<Version>> {
        self.by_node.get(node)
    }

    pub fn contains(&self, node: &NodeRef) -> bool {
        self.by_node.contains_key(node)
    }

    pub fn len(&self) -> usize {
        self.by_node.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_node.is_empty()
    }

    /// The nodes this index configures, in no particular order.
    pub fn nodes(&self) -> impl Iterator<Item = &NodeRef> {
        self.by_node.keys()
    }

    /// The models this index holds, in no particular order.
    pub fn models(&self) -> impl Iterator<Item = &Model<Version>> {
        self.by_node.values()
    }

    /// Each node with the model configuring it, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = (&NodeRef, &Model<Version>)> {
        self.by_node.iter()
    }
}

impl ModelIndex {
    /// The `M` named `identifier`, when the domain configures one.
    pub fn configured<M: UniquelyKindedModel>(
        &self,
        identifier: impl Into<ModelName>,
    ) -> Option<&M> {
        let model = self.by_node.get(&NodeRef::new(M::KIND, identifier))?;
        Some(M::from_model_ref(model).assured(Self::KEYED_BY_MODEL))
    }

    /// The `M` named `identifier`, to be altered in place.
    pub fn configured_mut<M: UniquelyKindedModel>(
        &mut self,
        identifier: impl Into<ModelName>,
    ) -> Option<&mut M> {
        self.narrowed_mut(M::KIND, identifier, M::from_model_mut)
    }

    /// The model of `kind` named `identifier`, narrowed by `narrow` to the shape that kind stores.
    ///
    /// The JSON and CBOR wire schemas are one shape under two kinds, so neither kind names a shape
    /// on its own and neither goes through [`Self::configured`]. `narrow` says which of the two
    /// the caller means, and the key still decides whether a model is there at all.
    pub fn narrowed<'a, T>(
        &'a self,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
        narrow: impl FnOnce(&'a Model) -> Option<&'a T>,
    ) -> Option<&'a T> {
        let model = self.by_node.get(&NodeRef::new(kind, identifier))?;
        Some(narrow(model).assured(Self::KEYED_BY_MODEL))
    }

    /// The mutable form of [`Self::narrowed`], for altering a stored model in place.
    pub fn narrowed_mut<'a, T>(
        &'a mut self,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
        narrow: impl FnOnce(&'a mut Model) -> Option<&'a mut T>,
    ) -> Option<&'a mut T> {
        let model = self.by_node.get_mut(&NodeRef::new(kind, identifier))?;
        Some(narrow(model).assured(Self::KEYED_BY_MODEL))
    }

    const KEYED_BY_MODEL: &'static str =
        "a model index keys every model by the kind the model itself reports";
}

impl<Version> FromIterator<Model<Version>> for ModelIndex<Version> {
    fn from_iter<I: IntoIterator<Item = Model<Version>>>(models: I) -> Self {
        let mut index = Self::new();
        for model in models {
            index.insert(model);
        }
        index
    }
}

impl<'a, Version> IntoIterator for &'a ModelIndex<Version> {
    type Item = (&'a NodeRef, &'a Model<Version>);
    type IntoIter = std::collections::hash_map::Iter<'a, NodeRef, Model<Version>>;

    fn into_iter(self) -> Self::IntoIter {
        self.by_node.iter()
    }
}

impl<Version> IntoIterator for ModelIndex<Version> {
    type Item = (NodeRef, Model<Version>);
    type IntoIter = std::collections::hash_map::IntoIter<NodeRef, Model<Version>>;

    fn into_iter(self) -> Self::IntoIter {
        self.by_node.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CreateRelay, CreateSchema, RelayBranching, RelayName, SchemaName};

    fn relay(name: &str) -> Model {
        Model::Relay(CreateRelay {
            name: RelayName::parse(name).expect("valid relay name"),
            schema: SchemaName::parse("events").expect("valid schema name"),
            buffer: nonzero_ext::nonzero!(4usize),
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        })
    }

    #[test]
    fn a_model_is_keyed_by_the_node_it_configures() {
        let mut index = ModelIndex::new();
        index.insert(relay("events_in"));

        assert_eq!(
            index
                .configured::<CreateRelay>(RelayName::parse("events_in").expect("valid relay name"))
                .map(|relay| relay.name.as_str()),
            Some("events_in")
        );
        assert!(
            index
                .configured::<CreateSchema>(
                    SchemaName::parse("events_in").expect("valid schema name")
                )
                .is_none(),
            "a name shared across kinds resolves per kind"
        );
    }

    #[test]
    fn two_kinds_sharing_an_identifier_stay_separate() {
        let index = ModelIndex::from_iter([
            relay("shared"),
            Model::Schema(CreateSchema {
                name: SchemaName::parse("shared").expect("valid schema name"),
                fields: Vec::new(),
            }),
        ]);

        assert_eq!(index.len(), 2);
        assert!(
            index
                .configured::<CreateRelay>(RelayName::parse("shared").expect("valid relay name"))
                .is_some()
        );
        assert!(
            index
                .configured::<CreateSchema>(SchemaName::parse("shared").expect("valid schema name"))
                .is_some()
        );
    }
}
