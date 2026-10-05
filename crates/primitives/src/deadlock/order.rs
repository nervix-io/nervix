//! Bounded source evidence for a historical cycle of lock instances.
//!
//! Layer: primitives.
//! - **Owns.** Lock lifetimes, directed order edges, acquisition witnesses and cyclic normalization.
//! - **Depends on.** The shared run-local identities and bounded source sites.
//! - **Must not know.** The application, review policy, recording or process exit.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroU64,
    time::SystemTime,
};

use super::{BlockedAttempt, BoundedText, LockSite, TrackedLockId, TrackedThreadId};

pub const MAX_ORDER_EDGES: usize = 64;
pub const MAX_ORDER_WITNESSES: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockLifetime {
    Live,
    Ended,
    /// The boundary never registered this instance.
    Unrecorded,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderLock {
    pub id: TrackedLockId,
    pub site: Option<LockSite>,
    pub lifetime: LockLifetime,
}

impl OrderLock {
    fn merge(&mut self, other: &Self) -> Result<bool, OrderOutOfBounds> {
        if self.id != other.id
            || (self.site.is_some() && other.site.is_some() && self.site != other.site)
        {
            return Err(OrderOutOfBounds::ConflictingInstance);
        }
        let before = self.clone();
        if self.site.is_none() {
            self.site = other.site.clone();
        }
        self.lifetime = match (self.lifetime, other.lifetime) {
            (LockLifetime::Ended, _) | (_, LockLifetime::Ended) => LockLifetime::Ended,
            (LockLifetime::Live, _) | (_, LockLifetime::Live) => LockLifetime::Live,
            _ => LockLifetime::Unrecorded,
        };
        Ok(*self != before)
    }
}

/// A source-level attempt made while holding another lock. Attempts include unsuccessful
/// nonblocking acquisitions; Deloxide, rather than this history, decides which edges form a cycle.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderWitness {
    pub thread: TrackedThreadId,
    pub name: Option<BoundedText>,
    pub held: BlockedAttempt,
    pub requested: BlockedAttempt,
    pub attempts: NonZeroU64,
    /// Identical live guards held at the recorded source site, including recursive shared reads.
    pub held_count: NonZeroU64,
}

impl OrderWitness {
    pub fn same_context(&self, other: &Self) -> bool {
        self.thread == other.thread
            && self.name == other.name
            && self.held == other.held
            && self.requested == other.requested
            && self.held_count == other.held_count
    }

    fn cmp_context(&self, other: &Self) -> Ordering {
        self.thread
            .cmp(&other.thread)
            .then(self.name.cmp(&other.name))
            .then(self.held.cmp(&other.held))
            .then(self.requested.cmp(&other.requested))
            .then(self.held_count.cmp(&other.held_count))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderEdge {
    pub before: OrderLock,
    pub after: OrderLock,
    witnesses: Vec<OrderWitness>,
    omitted_witnesses: u64,
}

impl OrderEdge {
    pub fn new(
        before: OrderLock,
        after: OrderLock,
        mut witnesses: Vec<OrderWitness>,
        omitted_witnesses: u64,
    ) -> Result<Self, OrderOutOfBounds> {
        if witnesses.len() > MAX_ORDER_WITNESSES {
            return Err(OrderOutOfBounds::TooManyWitnesses);
        }
        if omitted_witnesses > 0 && witnesses.len() < MAX_ORDER_WITNESSES {
            return Err(OrderOutOfBounds::OmittedWitnessesBelowBound);
        }
        witnesses.sort_by(OrderWitness::cmp_context);
        if witnesses
            .windows(2)
            .any(|adjacent| adjacent[0].same_context(&adjacent[1]))
        {
            return Err(OrderOutOfBounds::RepeatedWitnessContext);
        }
        Ok(Self {
            before,
            after,
            witnesses,
            omitted_witnesses,
        })
    }

    pub fn witnesses(&self) -> &[OrderWitness] {
        &self.witnesses
    }
    pub fn omitted_witnesses(&self) -> u64 {
        self.omitted_witnesses
    }

    pub fn has_complete_context(&self) -> bool {
        self.omitted_witnesses == 0
            && !self.witnesses.is_empty()
            && self
                .before
                .site
                .as_ref()
                .is_some_and(|site| !site.constructed_at.file.is_truncated())
            && self
                .after
                .site
                .as_ref()
                .is_some_and(|site| !site.constructed_at.file.is_truncated())
            && self.witnesses.iter().all(|witness| {
                !witness.held.at.file.is_truncated() && !witness.requested.at.file.is_truncated()
            })
            && self.before.lifetime != LockLifetime::Unrecorded
            && self.after.lifetime != LockLifetime::Unrecorded
    }

    /// Merge cumulative snapshots; counts take their maximum so delivery does not count the same
    /// attempts twice. Returns whether the source context changed and therefore needs fresh review.
    pub fn merge(&mut self, other: &Self) -> Result<bool, OrderOutOfBounds> {
        let mut combined = self.clone();
        let changed = combined.merge_context(other)?;
        *self = combined;
        Ok(changed)
    }

    fn merge_context(&mut self, other: &Self) -> Result<bool, OrderOutOfBounds> {
        if self.before.id != other.before.id || self.after.id != other.after.id {
            return Err(OrderOutOfBounds::Disconnected);
        }
        let mut changed = self.before.merge(&other.before)? | self.after.merge(&other.after)?;
        changed |= self.omitted_witnesses != other.omitted_witnesses;
        self.omitted_witnesses = self.omitted_witnesses.max(other.omitted_witnesses);
        for witness in &other.witnesses {
            // A finding holds at most MAX_ORDER_WITNESSES contexts, independent of workload size.
            if let Some(recorded) = self
                .witnesses
                .iter_mut()
                .find(|recorded| recorded.same_context(witness))
            {
                recorded.attempts = recorded.attempts.max(witness.attempts);
            } else {
                if self.witnesses.len() == MAX_ORDER_WITNESSES {
                    return Err(OrderOutOfBounds::TooManyWitnesses);
                }
                self.witnesses.push(witness.clone());
                changed = true;
            }
        }
        self.witnesses.sort_by(OrderWitness::cmp_context);
        Ok(changed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PotentialCycle {
    detected_at: SystemTime,
    edges: Vec<OrderEdge>,
    omitted_edges: u64,
}

impl PotentialCycle {
    /// Current representation: directed edges, no repeated closing node, rotated to the smallest
    /// instance identity for complete cycles. Direction stays distinct; merged snapshots retain
    /// every observed mode and source context up to the witness bound.
    pub fn new(
        detected_at: SystemTime,
        mut edges: Vec<OrderEdge>,
        omitted_edges: u64,
    ) -> Result<Self, OrderOutOfBounds> {
        if edges.is_empty() {
            return Err(OrderOutOfBounds::Empty);
        }
        if edges.len() > MAX_ORDER_EDGES {
            return Err(OrderOutOfBounds::TooManyEdges);
        }
        if omitted_edges > 0 && edges.len() < MAX_ORDER_EDGES {
            return Err(OrderOutOfBounds::OmittedEdgesBelowBound);
        }
        let mut instances = BTreeSet::new();
        let mut sites = BTreeMap::new();
        for edge in &edges {
            if !instances.insert(edge.before.id) {
                return Err(OrderOutOfBounds::RepeatedInstance);
            }
            for lock in [&edge.before, &edge.after] {
                let recorded = sites.entry(lock.id).or_insert(None);
                if recorded.is_some() && lock.site.is_some() && *recorded != lock.site.as_ref() {
                    return Err(OrderOutOfBounds::ConflictingInstance);
                }
                if recorded.is_none() {
                    *recorded = lock.site.as_ref();
                }
            }
        }
        for adjacent in edges.windows(2) {
            if adjacent[0].after.id != adjacent[1].before.id {
                return Err(OrderOutOfBounds::Disconnected);
            }
        }
        if omitted_edges == 0 {
            let last = &edges[edges.len() - 1];
            if last.after.id != edges[0].before.id {
                return Err(OrderOutOfBounds::Disconnected);
            }
            let first = edges
                .iter()
                .enumerate()
                .min_by_key(|(_, edge)| edge.before.id);
            if let Some((first, _)) = first {
                edges.rotate_left(first);
            }
        }
        Ok(Self {
            detected_at,
            edges,
            omitted_edges,
        })
    }

    pub fn detected_at(&self) -> SystemTime {
        self.detected_at
    }
    pub fn edges(&self) -> &[OrderEdge] {
        &self.edges
    }
    pub fn omitted_edges(&self) -> u64 {
        self.omitted_edges
    }
    pub fn has_complete_context(&self) -> bool {
        self.omitted_edges == 0 && self.edges.iter().all(OrderEdge::has_complete_context)
    }

    pub fn same_cycle(&self, other: &Self) -> bool {
        self.omitted_edges == 0
            && other.omitted_edges == 0
            && self.edges.len() == other.edges.len()
            && self.edges.iter().zip(&other.edges).all(|(left, right)| {
                left.before.id == right.before.id && left.after.id == right.after.id
            })
    }

    pub fn merge(&mut self, other: &Self) -> Result<bool, OrderOutOfBounds> {
        if !self.same_cycle(other) {
            return Err(OrderOutOfBounds::Disconnected);
        }
        let mut changed = false;
        let mut edges = self.edges.clone();
        for (recorded, incoming) in edges.iter_mut().zip(&other.edges) {
            changed |= recorded.merge(incoming)?;
        }
        self.edges = edges;
        self.detected_at = self.detected_at.max(other.detected_at);
        Ok(changed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderOutOfBounds {
    Empty,
    TooManyEdges,
    TooManyWitnesses,
    OmittedEdgesBelowBound,
    OmittedWitnessesBelowBound,
    Disconnected,
    RepeatedInstance,
    ConflictingInstance,
    RepeatedWitnessContext,
}

impl fmt::Display for OrderOutOfBounds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Empty => "an order cycle has no edges",
            Self::TooManyEdges => "an order cycle exceeds its edge capacity",
            Self::TooManyWitnesses => "an order edge exceeds its witness capacity",
            Self::OmittedEdgesBelowBound => "an order cycle omits edges below its capacity",
            Self::OmittedWitnessesBelowBound => "an order edge omits witnesses below its capacity",
            Self::Disconnected => "order edges do not form the declared directed cycle",
            Self::RepeatedInstance => "an order cycle repeats a lock instance",
            Self::ConflictingInstance => "one lock identity names conflicting construction sites",
            Self::RepeatedWitnessContext => "an order edge repeats a source context",
        })
    }
}

impl std::error::Error for OrderOutOfBounds {}
