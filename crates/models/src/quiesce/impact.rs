//! Typed transaction, operation and execution-step impact reports.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** Stable impact identities, scopes, attribution, topology and planned/actual effects.
//! - **Depends on.** Quiescence vocabulary and self-contained Model identities.
//! - **Must not know.** Parsing, planning, persistence, storage, runtime execution or presentation.

use std::{collections::BTreeMap, num::NonZeroUsize};

use error_stack::Report;
use meticulous::OptionExt as _;
use rkyv::{
    Archive, Deserialize as RkyvDeserialize, Place, Serialize as RkyvSerialize,
    rancor::Fallible,
    ser::{Allocator, Writer},
    vec::{ArchivedVec, VecResolver},
    with::{ArchiveWith, DeserializeWith, SerializeWith},
};
use serde::{Deserialize, Serialize};
use sorted_vec::SortedSet;
use strum::AsRefStr;
use thiserror::Error;

use super::{ModelChangeAspect, QuiesceLevel, StatePurge};
#[cfg(test)]
use crate::ModelName;
use crate::{
    BranchName, ClusterNodeName, DomainName, ModelKind, NodeRef, RelayName,
    RequestedResourceVersion, ResourceName,
};

/// The one-based public identity of an accepted operation in a transaction.
///
/// Transaction controls, inspection requests and rejected admission are not operations and never
/// construct one of these numbers. Keeping the number non-zero makes the public convention part of
/// the value instead of something every renderer has to remember.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(transparent)]
pub struct TransactionOperationNumber(NonZeroUsize);

impl TransactionOperationNumber {
    pub const fn new(number: NonZeroUsize) -> Self {
        Self(number)
    }

    /// Converts the internal zero-based statement position into its public operation number.
    pub fn from_index(index: usize) -> Result<Self, Report<ImpactReportError>> {
        let Some(number) = index.checked_add(1) else {
            return Err(Report::new(ImpactReportError::OperationNumberOverflow));
        };
        let number = NonZeroUsize::new(number)
            .assured("adding one to an index that did not overflow produces a non-zero number");
        Ok(Self(number))
    }

    pub const fn get(self) -> usize {
        self.0.get()
    }

    /// The internal zero-based position named by this public number.
    pub fn index(self) -> usize {
        self.get()
            .checked_sub(1)
            .assured("a transaction operation number is non-zero")
    }
}

impl std::fmt::Display for TransactionOperationNumber {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.get().fmt(formatter)
    }
}

/// The accepted prefix a transaction report describes.
///
/// This is a count rather than the next operation number, so zero identifies an empty transaction
/// without inventing an operation zero.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(transparent)]
pub struct TransactionPosition(usize);

impl TransactionPosition {
    pub const fn new(accepted_operations: usize) -> Self {
        Self(accepted_operations)
    }

    pub const fn accepted_operations(self) -> usize {
        self.0
    }

    pub fn next_operation(self) -> Result<TransactionOperationNumber, Report<ImpactReportError>> {
        TransactionOperationNumber::from_index(self.0)
    }
}

/// The inclusive, consecutive operation range executed as one atomic step.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct TransactionOperationRange {
    first: TransactionOperationNumber,
    last: TransactionOperationNumber,
}

impl TransactionOperationRange {
    pub fn new(
        first: TransactionOperationNumber,
        last: TransactionOperationNumber,
    ) -> Result<Self, Report<ImpactReportError>> {
        if last < first {
            return Err(Report::new(ImpactReportError::OperationRangeReversed {
                first,
                last,
            }));
        }
        Ok(Self { first, last })
    }

    pub fn from_index_and_count(
        first_index: usize,
        operation_count: usize,
    ) -> Result<Self, Report<ImpactReportError>> {
        let Some(operation_count) = NonZeroUsize::new(operation_count) else {
            return Err(Report::new(ImpactReportError::EmptyOperationRange));
        };
        let first = TransactionOperationNumber::from_index(first_index)?;
        let offset = operation_count
            .get()
            .checked_sub(1)
            .assured("an operation range count is non-zero");
        let Some(last_index) = first_index.checked_add(offset) else {
            return Err(Report::new(ImpactReportError::OperationNumberOverflow));
        };
        let last = TransactionOperationNumber::from_index(last_index)?;
        Ok(Self { first, last })
    }

    pub const fn first(self) -> TransactionOperationNumber {
        self.first
    }

    pub const fn last(self) -> TransactionOperationNumber {
        self.last
    }

    pub fn first_index(self) -> usize {
        self.first.index()
    }

    /// The zero-based exclusive end used to slice the queued statement sequence.
    pub const fn end_index(self) -> usize {
        self.last.get()
    }

    pub fn operation_count(self) -> NonZeroUsize {
        let distance =
            self.last.get().checked_sub(self.first.get()).assured(
                "an operation range is constructed with its last number at or after first",
            );
        let count = distance
            .checked_add(1)
            .assured("the distance between two usize operation numbers is below usize::MAX");
        NonZeroUsize::new(count).assured("adding one to a range distance produces a non-zero count")
    }

    pub fn contains(self, operation: TransactionOperationNumber) -> bool {
        self.first <= operation && operation <= self.last
    }

    pub fn operations(self) -> impl Iterator<Item = TransactionOperationNumber> {
        (self.first.get()..=self.last.get()).map(|number| {
            let number = NonZeroUsize::new(number)
                .assured("an inclusive range beginning at a non-zero operation stays non-zero");
            TransactionOperationNumber::new(number)
        })
    }
}

/// An opaque identity for the coherent control-plane inputs used to plan a report.
///
/// The producer hashes only relevant domain, Model, resource, schedule and topology inputs. Raft
/// writes unrelated to those inputs therefore do not change this identity.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(transparent)]
pub struct ImpactPlanningBasis([u8; 32]);

impl ImpactPlanningBasis {
    pub const fn new(fingerprint: [u8; 32]) -> Self {
        Self(fingerprint)
    }

    pub const fn fingerprint(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A set whose in-memory and serialized representation is sorted and duplicate-free.
///
/// Impact report projections all consume the same order. Its archive is a vector so archived
/// identity types do not need their own second ordering implementation.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(transparent)]
pub struct CanonicalImpactSet<T: Ord>(#[rkyv(with = SortedSetAsVec)] SortedSet<T>);

#[derive(Debug)]
struct SortedSetAsVec;

impl<T> ArchiveWith<SortedSet<T>> for SortedSetAsVec
where
    T: Archive + Ord,
{
    type Archived = ArchivedVec<T::Archived>;
    type Resolver = VecResolver;

    fn resolve_with(field: &SortedSet<T>, resolver: Self::Resolver, out: Place<Self::Archived>) {
        ArchivedVec::resolve_from_len(field.len(), resolver, out);
    }
}

impl<T, S> SerializeWith<SortedSet<T>, S> for SortedSetAsVec
where
    T: RkyvSerialize<S> + Ord,
    S: Fallible + Allocator + Writer + ?Sized,
{
    fn serialize_with(
        field: &SortedSet<T>,
        serializer: &mut S,
    ) -> Result<Self::Resolver, S::Error> {
        ArchivedVec::<T::Archived>::serialize_from_iter::<T, _, _>(field.iter(), serializer)
    }
}

impl<T, D> DeserializeWith<ArchivedVec<T::Archived>, SortedSet<T>, D> for SortedSetAsVec
where
    T: Archive + Ord,
    T::Archived: RkyvDeserialize<T, D>,
    D: Fallible + ?Sized,
{
    fn deserialize_with(
        field: &ArchivedVec<T::Archived>,
        deserializer: &mut D,
    ) -> Result<SortedSet<T>, D::Error> {
        let mut values = SortedSet::new();
        for archived in field.iter() {
            values.find_or_insert(archived.deserialize(deserializer)?);
        }
        Ok(values)
    }
}

impl<T: Ord> Default for CanonicalImpactSet<T> {
    fn default() -> Self {
        Self(SortedSet::new())
    }
}

impl<T: Ord> CanonicalImpactSet<T> {
    pub fn new(values: impl IntoIterator<Item = T>) -> Self {
        Self(values.into_iter().collect())
    }

    pub fn as_slice(&self) -> &[T] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn extend(&mut self, values: impl IntoIterator<Item = T>) {
        self.0.extend(values);
    }
}

impl<T: Ord> FromIterator<T> for CanonicalImpactSet<T> {
    fn from_iter<I: IntoIterator<Item = T>>(values: I) -> Self {
        Self::new(values)
    }
}

impl<T: Ord> IntoIterator for CanonicalImpactSet<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T: Ord> IntoIterator for &'a CanonicalImpactSet<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'de, T> Deserialize<'de> for CanonicalImpactSet<T>
where
    T: Deserialize<'de> + Ord,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let values = Vec::<T>::deserialize(deserializer)?;
        Ok(Self::new(values))
    }
}

/// The class of a non-sensitive diagnostic attached to an impact report.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum ImpactDiagnosticKind {
    Planning,
    Topology,
    Quiescence,
    Ownership,
    Activation,
    Application,
    Recovery,
}

/// A report diagnostic. Its message describes control-plane state only and must never contain a
/// payload, a branch-key value, or a sensitive Model.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ImpactDiagnostic {
    pub kind: ImpactDiagnosticKind,
    pub operation: Option<TransactionOperationNumber>,
    pub message: String,
}

/// Whether every section of an impact report was planned from a coherent input snapshot.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(tag = "status", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ImpactReportCompleteness {
    Complete,
    Incomplete { diagnostics: Vec<ImpactDiagnostic> },
}

impl ImpactReportCompleteness {
    pub fn incomplete(
        diagnostics: Vec<ImpactDiagnostic>,
    ) -> Result<Self, Report<ImpactReportError>> {
        if diagnostics.is_empty() {
            return Err(Report::new(ImpactReportError::IncompleteWithoutDiagnostic));
        }
        Ok(Self::Incomplete { diagnostics })
    }

    pub const fn is_complete(&self) -> bool {
        matches!(self, Self::Complete)
    }

    pub fn diagnostics(&self) -> &[ImpactDiagnostic] {
        match self {
            Self::Complete => &[],
            Self::Incomplete { diagnostics } => diagnostics,
        }
    }
}

/// Every accepted operation that contributes to one shared impact item.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(transparent)]
pub struct ImpactAttribution(CanonicalImpactSet<TransactionOperationNumber>);

impl ImpactAttribution {
    pub fn new(
        operations: impl IntoIterator<Item = TransactionOperationNumber>,
    ) -> Result<Self, Report<ImpactReportError>> {
        let operations = CanonicalImpactSet::new(operations);
        if operations.is_empty() {
            return Err(Report::new(ImpactReportError::EmptyAttribution));
        }
        Ok(Self(operations))
    }

    pub fn single(operation: TransactionOperationNumber) -> Self {
        Self(CanonicalImpactSet::new([operation]))
    }

    pub fn for_range(operations: TransactionOperationRange) -> Self {
        Self(operations.operations().collect())
    }

    pub fn operations(&self) -> &[TransactionOperationNumber] {
        self.0.as_slice()
    }

    fn merge(&mut self, other: &Self) {
        self.0.extend(other.0.as_slice().iter().copied());
    }
}

/// An opaque, non-payload identity for one concrete branch key.
///
/// Report producers fingerprint the canonical typed key. Raw branch fields are deliberately not
/// report content because they may contain sensitive values.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(transparent)]
#[rkyv(derive(PartialEq, Eq, PartialOrd, Ord))]
pub struct BranchKeyFingerprint([u8; 32]);

impl BranchKeyFingerprint {
    pub const fn new(fingerprint: [u8; 32]) -> Self {
        Self(fingerprint)
    }

    pub const fn fingerprint(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Which concrete executions of a logical node participate in an impact.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConcreteBranchCoverage {
    /// Every concrete execution of the logical node participates. This is used when the node's
    /// own configuration already carries its declared branch identity.
    All,
    /// The node has no branch key and its sole unbranched execution participates.
    Unbranched,
    /// Every current and subsequently appearing concrete key of this declared branch participates.
    AllOfBranch { branch: BranchName },
    /// Only the named concrete keys participate.
    Selected {
        branch: BranchName,
        keys: CanonicalImpactSet<BranchKeyFingerprint>,
    },
}

impl ConcreteBranchCoverage {
    pub fn selected(
        branch: BranchName,
        keys: impl IntoIterator<Item = BranchKeyFingerprint>,
    ) -> Result<Self, Report<ImpactReportError>> {
        let keys = CanonicalImpactSet::new(keys);
        if keys.is_empty() {
            return Err(Report::new(ImpactReportError::EmptyConcreteBranchCoverage));
        }
        Ok(Self::Selected { branch, keys })
    }
}

/// One logical node and the concrete branch executions covered by an impact.
///
/// Configuration-only nodes carry `None`; an execution node always carries an explicit coverage
/// value, including [`ConcreteBranchCoverage::Unbranched`].
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ImpactNodeCoverage {
    pub node: NodeRef,
    pub branches: Option<ConcreteBranchCoverage>,
}

impl ImpactNodeCoverage {
    pub fn configuration(node: NodeRef) -> Self {
        Self {
            node,
            branches: None,
        }
    }

    pub fn execution(node: NodeRef, branches: ConcreteBranchCoverage) -> Self {
        Self {
            node,
            branches: Some(branches),
        }
    }

    pub fn all_executions(node: NodeRef) -> Self {
        Self::execution(node, ConcreteBranchCoverage::All)
    }
}

/// A relay boundary at which new admission into a paused subgraph is gated.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ImpactGateBoundary {
    pub relay: RelayName,
    pub branches: ConcreteBranchCoverage,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct AttributedImpactNode {
    pub coverage: ImpactNodeCoverage,
    pub attribution: ImpactAttribution,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct AttributedGateBoundary {
    pub boundary: ImpactGateBoundary,
    pub attribution: ImpactAttribution,
}

/// The exact named subgraph and admission boundaries that must be paused together.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct QuiesceSubgraph {
    domain: DomainName,
    nodes: CanonicalImpactSet<AttributedImpactNode>,
    gate_boundaries: CanonicalImpactSet<AttributedGateBoundary>,
}

impl QuiesceSubgraph {
    pub fn new(
        domain: DomainName,
        nodes: impl IntoIterator<Item = AttributedImpactNode>,
        gate_boundaries: impl IntoIterator<Item = AttributedGateBoundary>,
    ) -> Self {
        let nodes = Self::merge_nodes(nodes);
        let gate_boundaries = Self::merge_gate_boundaries(gate_boundaries);
        Self {
            domain,
            nodes,
            gate_boundaries,
        }
    }

    pub fn domain(&self) -> &DomainName {
        &self.domain
    }

    pub fn nodes(&self) -> &[AttributedImpactNode] {
        self.nodes.as_slice()
    }

    pub fn gate_boundaries(&self) -> &[AttributedGateBoundary] {
        self.gate_boundaries.as_slice()
    }

    fn merge_nodes(
        nodes: impl IntoIterator<Item = AttributedImpactNode>,
    ) -> CanonicalImpactSet<AttributedImpactNode> {
        let mut by_coverage = BTreeMap::<ImpactNodeCoverage, ImpactAttribution>::new();
        for node in nodes {
            match by_coverage.get_mut(&node.coverage) {
                Some(attribution) => attribution.merge(&node.attribution),
                None => {
                    by_coverage.insert(node.coverage, node.attribution);
                }
            }
        }
        CanonicalImpactSet::new(by_coverage.into_iter().map(|(coverage, attribution)| {
            AttributedImpactNode {
                coverage,
                attribution,
            }
        }))
    }

    fn merge_gate_boundaries(
        boundaries: impl IntoIterator<Item = AttributedGateBoundary>,
    ) -> CanonicalImpactSet<AttributedGateBoundary> {
        let mut by_boundary = BTreeMap::<ImpactGateBoundary, ImpactAttribution>::new();
        for gate in boundaries {
            match by_boundary.get_mut(&gate.boundary) {
                Some(attribution) => attribution.merge(&gate.attribution),
                None => {
                    by_boundary.insert(gate.boundary, gate.attribution);
                }
            }
        }
        CanonicalImpactSet::new(by_boundary.into_iter().map(|(boundary, attribution)| {
            AttributedGateBoundary {
                boundary,
                attribution,
            }
        }))
    }
}

/// The required coordination before an execution step may apply.
///
/// There is no separately stored level: presentation derives it from this enum, so a report cannot
/// claim `DOMAIN_PAUSE` while carrying an entity scope (or the reverse).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PauseRequirement {
    NoPause,
    Subgraph { scope: QuiesceSubgraph },
    Domain { domain: DomainName },
}

impl PauseRequirement {
    pub const fn level(&self) -> QuiesceLevel {
        match self {
            Self::NoPause => QuiesceLevel::Dynamic,
            Self::Subgraph { .. } => QuiesceLevel::EntityPause,
            Self::Domain { .. } => QuiesceLevel::DomainPause,
        }
    }

    pub fn domain(&self) -> Option<&DomainName> {
        match self {
            Self::NoPause => None,
            Self::Subgraph { scope } => Some(scope.domain()),
            Self::Domain { domain } => Some(domain),
        }
    }

    pub fn combined(self, other: Self) -> Result<Self, Report<ImpactReportError>> {
        match (self, other) {
            (Self::NoPause, requirement) | (requirement, Self::NoPause) => Ok(requirement),
            (Self::Domain { domain: left }, Self::Domain { domain: right }) => {
                ensure_same_impact_domain(&left, &right)?;
                Ok(Self::Domain { domain: left })
            }
            (Self::Domain { domain }, Self::Subgraph { scope })
            | (Self::Subgraph { scope }, Self::Domain { domain }) => {
                ensure_same_impact_domain(&domain, scope.domain())?;
                Ok(Self::Domain { domain })
            }
            (Self::Subgraph { scope: left }, Self::Subgraph { scope: right }) => {
                ensure_same_impact_domain(left.domain(), right.domain())?;
                let nodes = left.nodes.into_iter().chain(right.nodes);
                let gates = left
                    .gate_boundaries
                    .into_iter()
                    .chain(right.gate_boundaries);
                Ok(Self::Subgraph {
                    scope: QuiesceSubgraph::new(left.domain, nodes, gates),
                })
            }
        }
    }
}

fn ensure_same_impact_domain(
    expected: &DomainName,
    actual: &DomainName,
) -> Result<(), Report<ImpactReportError>> {
    if expected == actual {
        return Ok(());
    }
    Err(Report::new(ImpactReportError::DomainMismatch {
        expected: expected.clone(),
        actual: actual.clone(),
    }))
}

/// The non-sensitive identity of one accepted NSPL operation.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TransactionOperation {
    CreateConfiguration {
        domain: DomainName,
        node: NodeRef,
    },
    AlterConfiguration {
        domain: DomainName,
        node: NodeRef,
    },
    DropConfiguration {
        domain: DomainName,
        node: NodeRef,
    },
    AlterDomain {
        domain: DomainName,
    },
    StartDomain {
        domain: DomainName,
    },
    StopDomain {
        domain: DomainName,
    },
    CreateResource {
        domain: DomainName,
        resource: ResourceName,
    },
    RebindResource {
        domain: DomainName,
        resource: ResourceName,
        requested: RequestedResourceVersion,
        version: u64,
    },
}

impl TransactionOperation {
    pub fn domain(&self) -> &DomainName {
        match self {
            Self::CreateConfiguration { domain, .. }
            | Self::AlterConfiguration { domain, .. }
            | Self::DropConfiguration { domain, .. }
            | Self::AlterDomain { domain }
            | Self::StartDomain { domain }
            | Self::StopDomain { domain }
            | Self::CreateResource { domain, .. }
            | Self::RebindResource { domain, .. } => domain,
        }
    }
}

/// One ordered reason contributed by an accepted operation. ALTER clauses append reasons in their
/// written order; sets elsewhere in the report never replace this sequence.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperationImpactReason {
    Configuration {
        node: NodeRef,
        aspect: ModelChangeAspect,
    },
    DomainPlacement,
    DomainStart,
    DomainStop,
    ResourceCatalog {
        resource: ResourceName,
    },
    ResourceRebinding {
        node: NodeRef,
        resource: ResourceName,
        from_version: u64,
        to_version: u64,
    },
}

/// The semantic relation of an edge in affected topology. Parallel relations between the same two
/// nodes stay distinct because the relation is part of the edge identity.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum ImpactEdgeKind {
    ConfigurationDependency,
    Dataflow,
    MessageError,
    CorrelationTimeout,
    MaterializedState,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ImpactTopologyEdge {
    pub source: ImpactNodeCoverage,
    pub target: ImpactNodeCoverage,
    pub kind: ImpactEdgeKind,
    pub attribution: ImpactAttribution,
}

/// One side of affected topology, canonically ordered for every projection.
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
pub struct ImpactTopology {
    pub nodes: CanonicalImpactSet<AttributedImpactNode>,
    pub edges: CanonicalImpactSet<ImpactTopologyEdge>,
}

/// The topology immediately before and immediately after an execution step.
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
pub struct AffectedTopology {
    pub before: ImpactTopology,
    pub after: ImpactTopology,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConfigurationTransition {
    Created { node: NodeRef },
    Changed { node: NodeRef },
    Dropped { node: NodeRef },
}

impl ConfigurationTransition {
    /// The configuration node the transition creates, changes, or drops.
    pub const fn node(&self) -> &NodeRef {
        match self {
            Self::Created { node } | Self::Changed { node } | Self::Dropped { node } => node,
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ConfigurationImpact {
    pub transition: ConfigurationTransition,
    pub attribution: ImpactAttribution,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum ResourceCatalogAction {
    Create,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ResourceCatalogImpact {
    pub resource: ResourceName,
    pub action: ResourceCatalogAction,
    pub attribution: ImpactAttribution,
}

/// The resource version a created or changed model binds. `requested` keeps the version as the
/// statement wrote it, so a `LATEST` binding reports the number it resolved to beside the request.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ResourceBindingImpact {
    pub node: NodeRef,
    pub resource: ResourceName,
    pub requested: RequestedResourceVersion,
    pub version: u64,
    pub attribution: ImpactAttribution,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum DomainLifecycleAction {
    Start,
    Stop,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct DomainLifecycleImpact {
    pub domain: DomainName,
    pub action: DomainLifecycleAction,
    pub attribution: ImpactAttribution,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct OwnershipMoveImpact {
    pub node: ImpactNodeCoverage,
    pub source: ClusterNodeName,
    pub destination: ClusterNodeName,
    pub attribution: ImpactAttribution,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum ActivationAction {
    Activate,
    Deactivate,
    /// Every live node's HTTPS listener installs the VHOST's new certificate. Established
    /// connections keep their negotiated session, and no execution node pauses or restarts.
    RefreshHttpsListener,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ActivationImpact {
    pub node: ImpactNodeCoverage,
    pub action: ActivationAction,
    pub attribution: ImpactAttribution,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum RebuildReason {
    Configuration,
    Ownership,
    Recovery,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct RebuildImpact {
    pub node: ImpactNodeCoverage,
    pub reason: RebuildReason,
    pub attribution: ImpactAttribution,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct StateResetImpact {
    pub node: ImpactNodeCoverage,
    pub state: StatePurge,
    pub attribution: ImpactAttribution,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ForceFlushImpact {
    pub node: ImpactNodeCoverage,
    pub attribution: ImpactAttribution,
}

/// Every semantic effect of an operation contribution or an execution step. Sequence-valued
/// reasons live on the operation report; these sets are canonical because their order is not
/// semantic.
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
pub struct ImpactEffects {
    pub changed_configuration: CanonicalImpactSet<ConfigurationImpact>,
    pub topology: AffectedTopology,
    pub ownership_moves: CanonicalImpactSet<OwnershipMoveImpact>,
    pub lifecycle: CanonicalImpactSet<DomainLifecycleImpact>,
    pub activations: CanonicalImpactSet<ActivationImpact>,
    pub rebuilds: CanonicalImpactSet<RebuildImpact>,
    pub state_resets: CanonicalImpactSet<StateResetImpact>,
    pub force_flushes: CanonicalImpactSet<ForceFlushImpact>,
    pub resource_catalog: CanonicalImpactSet<ResourceCatalogImpact>,
    pub resource_bindings: CanonicalImpactSet<ResourceBindingImpact>,
}

impl ImpactEffects {
    /// Whether these effects create, change, or drop a configuration node of `kind`. The changed
    /// configuration is one step's own diff, so this walks it once rather than looking a key up.
    pub fn changes_configuration_of(&self, kind: ModelKind) -> bool {
        self.changed_configuration
            .as_slice()
            .iter()
            .any(|change| change.transition.node().kind == kind)
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PlannedExecutionStepImpact {
    pub completeness: ImpactReportCompleteness,
    pub pause: PauseRequirement,
    pub effects: ImpactEffects,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QuiescenceOutcome {
    Requested,
    Confirmed,
    Failed { diagnostic: ImpactDiagnostic },
    Uncertain { diagnostic: ImpactDiagnostic },
    Released,
}

/// The ordered engagement history for one actual pause scope. A recovery expansion is another
/// entry with its wider requirement; it never rewrites what was originally planned.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ActualQuiescence {
    pub requirement: PauseRequirement,
    pub outcomes: Vec<QuiescenceOutcome>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[serde(tag = "status", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionStepOutcome {
    Unattempted,
    Applying,
    Applied,
    Failed { diagnostic: ImpactDiagnostic },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ActualExecutionStepImpact {
    pub outcome: ExecutionStepOutcome,
    pub quiescence: Vec<ActualQuiescence>,
    pub effects: ImpactEffects,
}

impl ActualExecutionStepImpact {
    pub fn unattempted() -> Self {
        Self {
            outcome: ExecutionStepOutcome::Unattempted,
            quiescence: Vec::new(),
            effects: ImpactEffects::default(),
        }
    }

    pub fn applying() -> Self {
        Self {
            outcome: ExecutionStepOutcome::Applying,
            quiescence: Vec::new(),
            effects: ImpactEffects::default(),
        }
    }

    /// The greatest scope that was confirmed, may have engaged, or was later released.
    /// A request followed only by a definitive failure did not interrupt execution.
    pub fn quiesce_level(&self) -> QuiesceLevel {
        self.quiescence
            .iter()
            .filter(|engagement| {
                engagement.outcomes.iter().any(|outcome| {
                    matches!(
                        outcome,
                        QuiescenceOutcome::Confirmed
                            | QuiescenceOutcome::Uncertain { .. }
                            | QuiescenceOutcome::Released
                    )
                })
            })
            .map(|engagement| engagement.requirement.level())
            .max()
            .unwrap_or(QuiesceLevel::Dynamic)
    }
}

/// The effective impact of one atomic execution step, distinct from each operation's contribution.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ExecutionStepImpactReport {
    operations: TransactionOperationRange,
    planned: PlannedExecutionStepImpact,
    actual: ActualExecutionStepImpact,
}

impl ExecutionStepImpactReport {
    pub fn new(
        operations: TransactionOperationRange,
        planned: PlannedExecutionStepImpact,
        actual: ActualExecutionStepImpact,
    ) -> Self {
        Self {
            operations,
            planned,
            actual,
        }
    }

    pub const fn operations(&self) -> TransactionOperationRange {
        self.operations
    }

    pub const fn planned(&self) -> &PlannedExecutionStepImpact {
        &self.planned
    }

    pub const fn actual(&self) -> &ActualExecutionStepImpact {
        &self.actual
    }

    pub fn actual_mut(&mut self) -> &mut ActualExecutionStepImpact {
        &mut self.actual
    }

    pub fn actual_quiesce_level(&self) -> QuiesceLevel {
        self.actual.quiesce_level()
    }
}

/// What one accepted operation contributed, and the effective step that contains it.
///
/// A contribution deliberately has no quiesce level or actual execution outcome. Those are facts
/// about the complete atomic step and live only in [`ExecutionStepImpactReport`].
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct OperationImpactReport {
    pub number: TransactionOperationNumber,
    pub operation: TransactionOperation,
    pub execution_step: TransactionOperationRange,
    pub completeness: ImpactReportCompleteness,
    pub reasons: Vec<OperationImpactReason>,
    pub contribution: ImpactEffects,
}

/// The transaction-wide requirement derived from its effective execution steps.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionImpactSummary {
    pause: PauseRequirement,
}

impl TransactionImpactSummary {
    pub const fn pause(&self) -> &PauseRequirement {
        &self.pause
    }

    pub const fn level(&self) -> QuiesceLevel {
        self.pause.level()
    }
}

/// The one semantic transaction-impact report consumed by planners, persistence and presentation.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionImpactReport {
    domain: DomainName,
    position: TransactionPosition,
    planning_basis: ImpactPlanningBasis,
    completeness: ImpactReportCompleteness,
    operations: Vec<OperationImpactReport>,
    execution_steps: Vec<ExecutionStepImpactReport>,
    summary: TransactionImpactSummary,
}

impl TransactionImpactReport {
    pub fn new(
        domain: DomainName,
        position: TransactionPosition,
        planning_basis: ImpactPlanningBasis,
        completeness: ImpactReportCompleteness,
        operations: Vec<OperationImpactReport>,
        execution_steps: Vec<ExecutionStepImpactReport>,
    ) -> Result<Self, Report<ImpactReportError>> {
        Self::validate_operations(&domain, position, &operations, &execution_steps)?;
        let pause = Self::summarize_pause(&domain, &execution_steps)?;
        Ok(Self {
            domain,
            position,
            planning_basis,
            completeness,
            operations,
            execution_steps,
            summary: TransactionImpactSummary { pause },
        })
    }

    pub fn domain(&self) -> &DomainName {
        &self.domain
    }

    pub const fn position(&self) -> TransactionPosition {
        self.position
    }

    pub const fn planning_basis(&self) -> ImpactPlanningBasis {
        self.planning_basis
    }

    pub const fn completeness(&self) -> &ImpactReportCompleteness {
        &self.completeness
    }

    pub fn operations(&self) -> &[OperationImpactReport] {
        &self.operations
    }

    pub fn execution_steps(&self) -> &[ExecutionStepImpactReport] {
        &self.execution_steps
    }

    pub const fn summary(&self) -> &TransactionImpactSummary {
        &self.summary
    }

    fn validate_operations(
        domain: &DomainName,
        position: TransactionPosition,
        operations: &[OperationImpactReport],
        execution_steps: &[ExecutionStepImpactReport],
    ) -> Result<(), Report<ImpactReportError>> {
        if position.accepted_operations() != operations.len() {
            return Err(Report::new(ImpactReportError::PositionMismatch {
                position: position.accepted_operations(),
                operations: operations.len(),
            }));
        }

        let mut operation_index = 0usize;
        for step in execution_steps {
            let range = step.operations();
            if range.first_index() != operation_index {
                return Err(Report::new(ImpactReportError::ExecutionStepSequence {
                    expected_index: operation_index,
                    actual: range.first(),
                }));
            }
            ensure_requirement_domain(domain, &step.planned.pause)?;
            for engagement in &step.actual.quiescence {
                ensure_requirement_domain(domain, &engagement.requirement)?;
            }
            for expected_number in range.operations() {
                let operation = operations.get(operation_index).ok_or(
                    ImpactReportError::ExecutionStepPastPosition {
                        operation: expected_number,
                        position: position.accepted_operations(),
                    },
                )?;
                if operation.number != expected_number {
                    return Err(Report::new(ImpactReportError::OperationSequence {
                        expected: expected_number,
                        actual: operation.number,
                    }));
                }
                ensure_same_impact_domain(domain, operation.operation.domain())?;
                if operation.execution_step != range {
                    return Err(Report::new(ImpactReportError::OperationStepMismatch {
                        operation: operation.number,
                        expected: range,
                        actual: operation.execution_step,
                    }));
                }
                operation_index = operation_index
                    .checked_add(1)
                    .assured("an operation index cannot exceed the report's Vec length");
            }
        }
        if operation_index != operations.len() {
            let expected = TransactionOperationNumber::from_index(operation_index)?;
            return Err(Report::new(ImpactReportError::ExecutionStepSequence {
                expected_index: operation_index,
                actual: expected,
            }));
        }
        Ok(())
    }

    fn summarize_pause(
        domain: &DomainName,
        execution_steps: &[ExecutionStepImpactReport],
    ) -> Result<PauseRequirement, Report<ImpactReportError>> {
        let mut summary = PauseRequirement::NoPause;
        for step in execution_steps {
            ensure_requirement_domain(domain, &step.planned.pause)?;
            summary = summary.combined(step.planned.pause.clone())?;
        }
        Ok(summary)
    }
}

fn ensure_requirement_domain(
    domain: &DomainName,
    requirement: &PauseRequirement,
) -> Result<(), Report<ImpactReportError>> {
    let Some(requirement_domain) = requirement.domain() else {
        return Ok(());
    };
    ensure_same_impact_domain(domain, requirement_domain)
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ImpactReportError {
    #[error("transaction operation number exceeds the target's addressable range")]
    OperationNumberOverflow,
    #[error("an execution-step operation range cannot be empty")]
    EmptyOperationRange,
    #[error("operation range {first} through {last} is reversed")]
    OperationRangeReversed {
        first: TransactionOperationNumber,
        last: TransactionOperationNumber,
    },
    #[error("an incomplete impact report requires at least one diagnostic")]
    IncompleteWithoutDiagnostic,
    #[error("an impact item must be attributed to at least one operation")]
    EmptyAttribution,
    #[error("selected concrete branch coverage requires at least one branch key")]
    EmptyConcreteBranchCoverage,
    #[error("impact report domain '{actual}' does not match owning domain '{expected}'")]
    DomainMismatch {
        expected: DomainName,
        actual: DomainName,
    },
    #[error(
        "transaction position records {position} accepted operation(s), but the report contains \
         {operations}"
    )]
    PositionMismatch { position: usize, operations: usize },
    #[error("expected operation {expected}, found {actual}")]
    OperationSequence {
        expected: TransactionOperationNumber,
        actual: TransactionOperationNumber,
    },
    #[error(
        "execution-step sequence expected a range beginning at zero-based operation index \
         {expected_index}, found operation {actual}"
    )]
    ExecutionStepSequence {
        expected_index: usize,
        actual: TransactionOperationNumber,
    },
    #[error("execution step reaches operation {operation}, past transaction position {position}")]
    ExecutionStepPastPosition {
        operation: TransactionOperationNumber,
        position: usize,
    },
    #[error(
        "operation {operation} names execution step {actual:?}, but is contained by {expected:?}"
    )]
    OperationStepMismatch {
        operation: TransactionOperationNumber,
        expected: TransactionOperationRange,
        actual: TransactionOperationRange,
    },
}

#[cfg(test)]
mod impact_report_tests {
    use meticulous::ResultExt as _;
    use rkyv::{from_bytes, rancor::Error, to_bytes};

    use super::*;

    fn named<N>(raw: &str) -> N
    where
        N: for<'a> TryFrom<&'a str>,
        for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
    {
        N::try_from(raw).assured("the test passes a valid Nervix name")
    }

    fn operation(number: usize, step: TransactionOperationRange) -> OperationImpactReport {
        let number = TransactionOperationNumber::from_index(
            number
                .checked_sub(1)
                .assured("test operation numbers are one-based"),
        )
        .assured("test operation numbers fit the target");
        let node = NodeRef::new(ModelKind::Relay, named::<ModelName>("events"));
        let contribution = ImpactEffects {
            changed_configuration: CanonicalImpactSet::new([ConfigurationImpact {
                transition: ConfigurationTransition::Changed { node: node.clone() },
                attribution: ImpactAttribution::single(number),
            }]),
            ..ImpactEffects::default()
        };
        OperationImpactReport {
            number,
            operation: TransactionOperation::AlterConfiguration {
                domain: named("tenant"),
                node: node.clone(),
            },
            execution_step: step,
            completeness: ImpactReportCompleteness::Complete,
            reasons: vec![
                OperationImpactReason::Configuration {
                    node: node.clone(),
                    aspect: ModelChangeAspect::RelayCapacity,
                },
                OperationImpactReason::Configuration {
                    node: node.clone(),
                    aspect: ModelChangeAspect::RelayMaterializedState,
                },
            ],
            contribution,
        }
    }

    fn shared_subgraph(step: TransactionOperationRange) -> PauseRequirement {
        let first = step.first();
        let last = step.last();
        let relay = named::<RelayName>("events");
        let boundary = ImpactGateBoundary {
            relay: relay.clone(),
            branches: ConcreteBranchCoverage::Unbranched,
        };
        let relay_node = ImpactNodeCoverage::execution(
            NodeRef::new(ModelKind::Relay, relay),
            ConcreteBranchCoverage::Unbranched,
        );
        PauseRequirement::Subgraph {
            scope: QuiesceSubgraph::new(
                named("tenant"),
                [
                    AttributedImpactNode {
                        coverage: relay_node.clone(),
                        attribution: ImpactAttribution::single(first),
                    },
                    AttributedImpactNode {
                        coverage: relay_node,
                        attribution: ImpactAttribution::single(last),
                    },
                ],
                [
                    AttributedGateBoundary {
                        boundary: boundary.clone(),
                        attribution: ImpactAttribution::single(last),
                    },
                    AttributedGateBoundary {
                        boundary,
                        attribution: ImpactAttribution::single(first),
                    },
                ],
            ),
        }
    }

    fn two_operation_report() -> TransactionImpactReport {
        let step = TransactionOperationRange::from_index_and_count(0, 2)
            .assured("two test operations form one addressable range");
        let attribution = ImpactAttribution::for_range(step);
        let relay = NodeRef::new(ModelKind::Relay, named::<ModelName>("events"));
        let junction = NodeRef::new(ModelKind::Junction, named::<ModelName>("normalize"));
        let relay_coverage =
            ImpactNodeCoverage::execution(relay.clone(), ConcreteBranchCoverage::Unbranched);
        let junction_coverage =
            ImpactNodeCoverage::execution(junction.clone(), ConcreteBranchCoverage::Unbranched);
        let before = ImpactTopology {
            nodes: CanonicalImpactSet::new([AttributedImpactNode {
                coverage: relay_coverage.clone(),
                attribution: attribution.clone(),
            }]),
            edges: CanonicalImpactSet::default(),
        };
        let after = ImpactTopology {
            nodes: CanonicalImpactSet::new([
                AttributedImpactNode {
                    coverage: relay_coverage.clone(),
                    attribution: attribution.clone(),
                },
                AttributedImpactNode {
                    coverage: junction_coverage.clone(),
                    attribution: attribution.clone(),
                },
            ]),
            edges: CanonicalImpactSet::new([ImpactTopologyEdge {
                source: relay_coverage.clone(),
                target: junction_coverage,
                kind: ImpactEdgeKind::Dataflow,
                attribution: attribution.clone(),
            }]),
        };
        let effects = ImpactEffects {
            topology: AffectedTopology { before, after },
            rebuilds: CanonicalImpactSet::new([RebuildImpact {
                node: relay_coverage,
                reason: RebuildReason::Configuration,
                attribution,
            }]),
            ..ImpactEffects::default()
        };
        let planned = PlannedExecutionStepImpact {
            completeness: ImpactReportCompleteness::Complete,
            pause: shared_subgraph(step),
            effects,
        };
        TransactionImpactReport::new(
            named("tenant"),
            TransactionPosition::new(2),
            ImpactPlanningBasis::new([7; 32]),
            ImpactReportCompleteness::Complete,
            vec![operation(1, step), operation(2, step)],
            vec![ExecutionStepImpactReport::new(
                step,
                planned,
                ActualExecutionStepImpact::unattempted(),
            )],
        )
        .assured("the test report has a consecutive operation and execution-step sequence")
    }

    #[test]
    fn operation_numbers_are_one_based_while_an_empty_position_has_no_operation() {
        let empty = TransactionPosition::new(0);
        assert_eq!(empty.accepted_operations(), 0);
        assert_eq!(
            empty
                .next_operation()
                .assured("operation one fits every target usize")
                .get(),
            1
        );

        let report = two_operation_report();
        assert_eq!(report.position().accepted_operations(), 2);
        assert_eq!(report.operations()[0].number.get(), 1);
        assert_eq!(report.operations()[1].number.get(), 2);
        assert_eq!(report.execution_steps()[0].operations().first().get(), 1);
        assert_eq!(report.execution_steps()[0].operations().last().get(), 2);
    }

    #[test]
    fn joint_step_impact_keeps_shared_attribution_out_of_operation_contributions() {
        let report = two_operation_report();
        let PauseRequirement::Subgraph { scope } = report.summary().pause() else {
            panic!("the shared test step requires its named subgraph")
        };
        assert_eq!(report.summary().level(), QuiesceLevel::EntityPause);
        assert_eq!(scope.nodes().len(), 1);
        assert_eq!(scope.gate_boundaries().len(), 1);
        let expected = [
            TransactionOperationNumber::from_index(0)
                .assured("operation one fits every target usize"),
            TransactionOperationNumber::from_index(1)
                .assured("operation two fits every target usize"),
        ];
        assert_eq!(scope.nodes()[0].attribution.operations(), expected);
        assert_eq!(
            scope.gate_boundaries()[0].attribution.operations(),
            expected
        );
        let step = &report.execution_steps()[0];
        assert_eq!(step.planned().effects.rebuilds.len(), 1);
        assert_eq!(
            step.planned().effects.rebuilds.as_slice()[0]
                .attribution
                .operations(),
            expected
        );
        let first = &report.operations()[0].contribution;
        let second = &report.operations()[1].contribution;
        assert_eq!(first.changed_configuration.len(), 1);
        assert_eq!(second.changed_configuration.len(), 1);
        assert_eq!(
            first.changed_configuration.as_slice()[0]
                .attribution
                .operations(),
            &expected[..1]
        );
        assert_eq!(
            second.changed_configuration.as_slice()[0]
                .attribution
                .operations(),
            &expected[1..]
        );
        assert!(first.rebuilds.is_empty());
        assert!(second.rebuilds.is_empty());
    }

    #[test]
    fn actual_level_counts_confirmed_and_uncertain_engagement_but_not_a_definitive_rejection() {
        let step = TransactionOperationRange::from_index_and_count(0, 2)
            .assured("two test operations form one addressable range");
        let requirement = shared_subgraph(step);
        let mut impact = ActualExecutionStepImpact::applying();
        impact.quiescence.push(ActualQuiescence {
            requirement: requirement.clone(),
            outcomes: vec![
                QuiescenceOutcome::Requested,
                QuiescenceOutcome::Failed {
                    diagnostic: ImpactDiagnostic {
                        kind: ImpactDiagnosticKind::Quiescence,
                        operation: Some(step.first()),
                        message: "the local gate rejected the request".to_string(),
                    },
                },
            ],
        });
        assert_eq!(impact.quiesce_level(), QuiesceLevel::Dynamic);

        impact.quiescence.push(ActualQuiescence {
            requirement,
            outcomes: vec![
                QuiescenceOutcome::Requested,
                QuiescenceOutcome::Uncertain {
                    diagnostic: ImpactDiagnostic {
                        kind: ImpactDiagnosticKind::Quiescence,
                        operation: Some(step.first()),
                        message: "the remote response timed out".to_string(),
                    },
                },
                QuiescenceOutcome::Released,
            ],
        });
        assert_eq!(impact.quiesce_level(), QuiesceLevel::EntityPause);
    }

    #[test]
    fn actual_level_retains_a_confirmed_pause_after_drain_failure_and_release() {
        let step = TransactionOperationRange::from_index_and_count(0, 2)
            .assured("two test operations form one addressable range");
        let mut impact = ActualExecutionStepImpact::applying();
        impact.quiescence.push(ActualQuiescence {
            requirement: PauseRequirement::Domain {
                domain: named("tenant"),
            },
            outcomes: vec![
                QuiescenceOutcome::Requested,
                QuiescenceOutcome::Confirmed,
                QuiescenceOutcome::Failed {
                    diagnostic: ImpactDiagnostic {
                        kind: ImpactDiagnosticKind::Quiescence,
                        operation: Some(step.first()),
                        message: "the domain did not drain before its deadline".to_string(),
                    },
                },
                QuiescenceOutcome::Released,
            ],
        });

        assert_eq!(impact.quiesce_level(), QuiesceLevel::DomainPause);
    }

    #[test]
    fn pause_level_is_derived_from_the_explicit_scope() {
        let step = TransactionOperationRange::from_index_and_count(0, 2)
            .assured("two test operations form one addressable range");
        let subgraph = shared_subgraph(step);
        let domain = PauseRequirement::Domain {
            domain: named("tenant"),
        };
        assert_eq!(PauseRequirement::NoPause.level(), QuiesceLevel::Dynamic);
        assert_eq!(subgraph.level(), QuiesceLevel::EntityPause);
        assert_eq!(domain.level(), QuiesceLevel::DomainPause);
        assert_eq!(
            subgraph
                .combined(domain)
                .assured("both test requirements belong to tenant")
                .level(),
            QuiesceLevel::DomainPause
        );
    }

    #[test]
    fn incomplete_reports_carry_a_non_sensitive_diagnostic() {
        let operation = TransactionOperationNumber::from_index(0)
            .assured("operation one fits every target usize");
        let completeness = ImpactReportCompleteness::incomplete(vec![ImpactDiagnostic {
            kind: ImpactDiagnosticKind::Topology,
            operation: Some(operation),
            message: "the final model run has an unresolved relay reference".to_string(),
        }])
        .assured("the incomplete test report names what remains unresolved");
        assert!(!completeness.is_complete());
        assert_eq!(completeness.diagnostics()[0].operation, Some(operation));
    }

    #[test]
    fn canonical_sets_and_ordered_reasons_survive_json_and_rkyv_losslessly() {
        let report = two_operation_report();
        let json = serde_json::to_vec(&report)
            .assured("every field in an impact report has a JSON representation");
        let decoded: TransactionImpactReport =
            serde_json::from_slice(&json).assured("the JSON was produced from this report type");
        assert_eq!(decoded, report);
        assert!(matches!(
            decoded.operations()[0].reasons.as_slice(),
            [
                OperationImpactReason::Configuration {
                    aspect: ModelChangeAspect::RelayCapacity,
                    ..
                },
                OperationImpactReason::Configuration {
                    aspect: ModelChangeAspect::RelayMaterializedState,
                    ..
                }
            ]
        ));

        let one = TransactionOperationNumber::from_index(0)
            .assured("operation one fits every target usize");
        let two = TransactionOperationNumber::from_index(1)
            .assured("operation two fits every target usize");
        let ascending = ImpactAttribution::new([one, two])
            .assured("the stable-order test supplies two operations");
        let descending = ImpactAttribution::new([two, one])
            .assured("the stable-order test supplies two operations");
        assert_eq!(ascending, descending);
        let ascending_json =
            serde_json::to_vec(&ascending).assured("impact attribution has a JSON representation");
        let descending_json =
            serde_json::to_vec(&descending).assured("impact attribution has a JSON representation");
        assert_eq!(ascending_json, descending_json);

        let archived = to_bytes::<Error>(&report)
            .assured("every field in an impact report has an rkyv representation");
        let restored = from_bytes::<TransactionImpactReport, Error>(&archived)
            .assured("the archive was produced from this report type");
        assert_eq!(restored, report);
        let encoded_again = serde_json::to_vec(&restored)
            .assured("every restored report field has a JSON representation");
        assert_eq!(encoded_again, json);
    }

    #[test]
    fn lifecycle_effects_remain_explicit_with_no_pause() {
        let operation = TransactionOperationNumber::from_index(0)
            .assured("operation one fits every target usize");
        let effects = ImpactEffects {
            lifecycle: CanonicalImpactSet::new([
                DomainLifecycleImpact {
                    domain: named("tenant"),
                    action: DomainLifecycleAction::Stop,
                    attribution: ImpactAttribution::single(operation),
                },
                DomainLifecycleImpact {
                    domain: named("tenant"),
                    action: DomainLifecycleAction::Start,
                    attribution: ImpactAttribution::single(operation),
                },
            ]),
            ..ImpactEffects::default()
        };
        let planned = PlannedExecutionStepImpact {
            completeness: ImpactReportCompleteness::Complete,
            pause: PauseRequirement::NoPause,
            effects,
        };
        assert_eq!(planned.pause.level(), QuiesceLevel::Dynamic);
        assert_eq!(planned.effects.lifecycle.len(), 2);
        assert_eq!(
            planned
                .effects
                .lifecycle
                .as_slice()
                .iter()
                .map(|effect| effect.action)
                .collect::<Vec<_>>(),
            [DomainLifecycleAction::Start, DomainLifecycleAction::Stop]
        );
    }
}
