//! Layer: engines and infrastructure.
//!
//! - **Owns.** Unit-owned allowance transactions, bounded keyed stores and allowance time conversion.
//! - **Depends on.** Governor's GCRA, the primitive boundary, expiry-map and typed time values.
//! - **Must not know.** Plexus, configuration Models, connectors, graph decisions or persistence.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns rate admission, not graph policy")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        key = "one execution unit and its written allowance clauses",
        bound = "the clause vector and each keyed clause's explicit shard count, TTL and MAX KEYS",
        reason = "exclusive admission owns each tentative charge until all clauses commit or \
                  refund"
    )
)]

mod cell;
mod clock;
mod generation;
mod keyed;
mod quota;

use std::{fmt, num::NonZeroU32, time::Duration};

use cell::{DirectCell, RelativeStore};
use clock::{AdmissionClock, ClockPoint};
use error_stack::Report;
pub use generation::{
    ClauseGeneration, Generation, KeyGeneration, UtcClauseGeneration, UtcGeneration,
    UtcKeyGeneration,
};
use governor::RateLimiter;
use keyed::{KeyLease, KeyedStore};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::time::Instant;
pub use quota::{KeyedOptions, Quota, QuotaError};
use thiserror::Error;

/// A caller's already-fingerprinted key; the engine never inspects or renders its original values.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyFingerprint([u8; 16]);

impl From<[u8; 16]> for KeyFingerprint {
    fn from(value: [u8; 16]) -> Self {
        Self(value)
    }
}

impl fmt::Debug for KeyFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("KeyFingerprint")
    }
}

/// An allowance declaration, separate from its mutable execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Clause {
    Direct { quota: Quota },
    Keyed { quota: Quota, keys: KeyedOptions },
}

impl Clause {
    pub fn direct(quota: Quota) -> Self {
        Self::Direct { quota }
    }
    pub fn keyed(quota: Quota, keys: KeyedOptions) -> Self {
        Self::Keyed { quota, keys }
    }
    fn quota(self) -> Quota {
        match self {
            Self::Direct { quota } | Self::Keyed { quota, .. } => quota,
        }
    }
}

/// A whole payload's cost in one clause. Zero costs are rejected before constructing this value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Charge {
    Direct(NonZeroU32),
    Keyed {
        key: KeyFingerprint,
        cost: NonZeroU32,
    },
}

impl Charge {
    fn cost(self) -> NonZeroU32 {
        match self {
            Self::Direct(cost) | Self::Keyed { cost, .. } => cost,
        }
    }
}

/// An ordinary decision: the caller owns waiting and the policy for an unadmittable payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Admitted,
    WaitUntil(Instant),
    NeverAdmissible {
        clause: usize,
        cost: NonZeroU32,
        burst: NonZeroU32,
    },
    KeyBudgetExhausted {
        clause: usize,
        limit: usize,
    },
}

#[derive(Debug, Error, PartialEq, Eq)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(outcome, reason = "pure admission input and state conversion refusals")
)]
pub enum AdmissionError {
    #[error("the number of charges does not match the number of clauses")]
    ChargeCount,
    #[error("charge {clause} has the wrong key kind")]
    ChargeKind { clause: usize },
    #[error("the monotonic clock moved behind the limiter's latest observation")]
    ClockRegressed,
    #[error("the admission or saved horizon does not fit the clock's range")]
    ClockRange,
    #[error("the keyed store has exhausted its visit sequence")]
    VisitRange,
    #[error("the allowance generation contains duplicate keys or invalid state")]
    GenerationState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct KeyStatistics {
    pub active: usize,
    pub evicted: u128,
}

enum Store {
    Direct(DirectCell),
    Keyed(KeyedStore),
}

struct Allowance {
    clause: Clause,
    store: Store,
}

/// One execution unit's ordered allowances. Its exclusive admission borrow is the transaction fence.
pub struct Limiter {
    clauses: Vec<Allowance>,
    epoch: Instant,
    latest: Instant,
}

impl fmt::Debug for Limiter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Limiter")
            .field("clauses", &self.clauses.len())
            .finish_non_exhaustive()
    }
}

impl Limiter {
    pub fn new(clauses: Vec<Clause>, epoch: Instant) -> Self {
        let clauses = clauses
            .into_iter()
            .map(|clause| {
                let store = match clause {
                    Clause::Direct { .. } => Store::Direct(DirectCell::new(0)),
                    Clause::Keyed { keys, .. } => Store::Keyed(KeyedStore::new(keys)),
                };
                Allowance { clause, store }
            })
            .collect();
        Self {
            clauses,
            epoch,
            latest: epoch,
        }
    }

    /// Install a validated generation on a fresh monotonic clock, preserving retained key ages.
    pub fn from_generation(
        generation: Generation,
        now: Instant,
    ) -> Result<Self, Report<AdmissionError>> {
        let plan = generation.prepare()?;
        let epoch = now
            .checked_sub(plan.maximum_age)
            .ok_or_else(|| Report::new(AdmissionError::ClockRange))?;
        let offset = now.duration_since(epoch);
        let mut limiter = Self::new(plan.declarations, epoch);
        limiter.latest = now;
        for (allowance, state) in limiter.clauses.iter_mut().zip(&generation.clauses) {
            match (&mut allowance.store, state) {
                (Store::Direct(cell), ClauseGeneration::Direct { remaining, .. }) => {
                    let horizon = match remaining {
                        Some(remaining) => offset
                            .checked_add(*remaining)
                            .ok_or_else(|| Report::new(AdmissionError::ClockRange))?,
                        None => offset,
                    };
                    let horizon = u64::try_from(horizon.as_nanos())
                        .map_err(|_| Report::new(AdmissionError::ClockRange))?;
                    *cell = DirectCell::new(horizon);
                }
                (Store::Keyed(store), ClauseGeneration::Keyed { keys, .. }) => {
                    store.restore(keys, offset)?
                }
                _ => {
                    unreachable!("the restore plan constructs each store from its generation kind")
                }
            }
        }
        Ok(limiter)
    }

    /// Read the selected node clock once; this never sleeps or follows a domain's logical clock.
    pub fn admit(&mut self, charges: &[Charge]) -> Result<Admission, Report<AdmissionError>> {
        self.admit_at(charges, Instant::now())
    }

    fn offset(&self, now: Instant) -> Result<u64, Report<AdmissionError>> {
        if now < self.latest {
            return Err(Report::new(AdmissionError::ClockRegressed));
        }
        u64::try_from(now.duration_since(self.epoch).as_nanos())
            .map_err(|_| Report::new(AdmissionError::ClockRange))
    }

    fn validate(&self, charges: &[Charge], now: Instant) -> Result<u64, Report<AdmissionError>> {
        if charges.len() != self.clauses.len() {
            return Err(Report::new(AdmissionError::ChargeCount));
        }
        let offset = self.offset(now)?;
        for (index, (allowance, charge)) in self.clauses.iter().zip(charges).enumerate() {
            match (&allowance.store, charge) {
                (Store::Direct(_), Charge::Direct(_)) | (Store::Keyed(_), Charge::Keyed { .. }) => {
                }
                _ => return Err(Report::new(AdmissionError::ChargeKind { clause: index })),
            }
            let horizon = allowance.clause.quota().horizon;
            let nanos =
                u64::try_from(horizon.as_nanos()).verified("quota bounds its horizon to u64");
            if offset.checked_add(nanos).is_none() || now.checked_add(horizon).is_none() {
                return Err(Report::new(AdmissionError::ClockRange));
            }
        }
        Ok(offset)
    }

    /// Injected-time form of the same production admission, for a caller that already read its clock.
    pub fn admit_at(
        &mut self,
        charges: &[Charge],
        now: Instant,
    ) -> Result<Admission, Report<AdmissionError>> {
        let offset = self.validate(charges, now)?;
        for (index, (allowance, charge)) in self.clauses.iter().zip(charges).enumerate() {
            let burst = allowance.clause.quota().burst();
            if charge.cost() > burst {
                return Ok(Admission::NeverAdmissible {
                    clause: index,
                    cost: charge.cost(),
                    burst,
                });
            }
        }
        self.latest = now;
        let mut leases = Vec::with_capacity(self.clauses.len());
        let mut wait: Option<Instant> = None;
        let mut exhausted = None;
        for (index, (allowance, charge)) in self.clauses.iter().zip(charges).enumerate() {
            let kind = match (&allowance.store, *charge) {
                (Store::Direct(cell), Charge::Direct(_)) => LeaseKind::Direct(cell),
                (Store::Keyed(store), Charge::Keyed { key, .. }) => {
                    let Some(lease) = store.prepare(key, Duration::from_nanos(offset))? else {
                        let Clause::Keyed { keys, .. } = allowance.clause else {
                            unreachable!("the store was built from its keyed declaration")
                        };
                        exhausted = Some(Admission::KeyBudgetExhausted {
                            clause: index,
                            limit: keys.max_keys,
                        });
                        continue;
                    };
                    LeaseKind::Keyed(lease)
                }
                _ => {
                    unreachable!("validation checked every charge's key kind before changing state")
                }
            };
            let lease = Lease::new(kind);
            let store = RelativeStore {
                cell: lease.cell(),
                now: offset,
            };
            let limiter: RateLimiter<_, _, _, governor::middleware::NoOpMiddleware<ClockPoint>> =
                RateLimiter::new(
                    allowance.clause.quota().governor,
                    store,
                    AdmissionClock(now),
                );
            match limiter.check_n(charge.cost()) {
                Ok(Ok(())) => {}
                Ok(Err(not_until)) => {
                    let deadline = not_until.earliest_possible().0;
                    wait = Some(match wait {
                        Some(previous) => previous.max(deadline),
                        None => deadline,
                    });
                }
                Err(_) => unreachable!("the burst precheck already refused every oversized charge"),
            }
            leases.push(lease);
        }
        if let Some(outcome) = exhausted {
            return Ok(outcome);
        }
        if let Some(deadline) = wait {
            return Ok(Admission::WaitUntil(deadline));
        }
        for lease in leases {
            lease.commit();
        }
        Ok(Admission::Admitted)
    }

    /// Expire inactive keys before reporting occupancy; TTL removals are not capacity evictions.
    pub fn key_statistics(
        &mut self,
        now: Instant,
    ) -> Result<KeyStatistics, Report<AdmissionError>> {
        let offset = Duration::from_nanos(self.offset(now)?);
        self.latest = now;
        let mut result = KeyStatistics::default();
        for allowance in &self.clauses {
            if let Store::Keyed(store) = &allowance.store {
                let current = store.statistics(offset);
                result.active = result
                    .active
                    .checked_add(current.active)
                    .verified("all retained entries fit the address space");
                result.evicted = result
                    .evicted
                    .checked_add(current.evicted)
                    .verified("at most usize::MAX stores each count at most u64::MAX visits");
            }
        }
        Ok(result)
    }

    pub fn generation(&self, now: Instant) -> Result<Generation, Report<AdmissionError>> {
        let offset = Duration::from_nanos(self.offset(now)?);
        let mut clauses = Vec::with_capacity(self.clauses.len());
        for allowance in &self.clauses {
            let state = match &allowance.store {
                Store::Direct(cell) => {
                    let horizon = Duration::from_nanos(cell.horizon());
                    let remaining = if horizon > offset {
                        Some(horizon.saturating_sub(offset))
                    } else {
                        None
                    };
                    ClauseGeneration::Direct {
                        quota: allowance.clause.quota(),
                        remaining,
                    }
                }
                Store::Keyed(store) => {
                    let Clause::Keyed {
                        quota,
                        keys: options,
                    } = allowance.clause
                    else {
                        unreachable!("a keyed store has a keyed declaration")
                    };
                    ClauseGeneration::Keyed {
                        quota,
                        options,
                        keys: store.generation(offset),
                    }
                }
            };
            clauses.push(state);
        }
        Ok(Generation { clauses })
    }
}

enum LeaseKind<'a> {
    Direct(&'a DirectCell),
    Keyed(KeyLease<'a>),
}

pub(crate) struct Lease<'a> {
    kind: LeaseKind<'a>,
    refund: Option<u64>,
}

impl<'a> Lease<'a> {
    fn new(kind: LeaseKind<'a>) -> Self {
        let mut lease = Self { kind, refund: None };
        lease.refund = Some(lease.cell().horizon());
        lease
    }
    fn cell(&self) -> &DirectCell {
        match &self.kind {
            LeaseKind::Direct(cell) => cell,
            LeaseKind::Keyed(lease) => lease.cell(),
        }
    }
    fn commit(mut self) {
        if let LeaseKind::Keyed(lease) = &mut self.kind {
            lease.commit();
        }
        self.refund = None;
    }
    fn rollback(&mut self) {
        if let Some(before) = self.refund.take() {
            self.cell().refund(before);
        }
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        self.rollback();
    }
}

#[cfg(all(
    test,
    not(any(feature = "loom", feature = "shuttle", feature = "deloxide"))
))]
mod tests;

#[cfg(all(test, feature = "deloxide"))]
mod diagnostic_tests;
#[cfg(all(test, feature = "loom"))]
mod loom_tests;
#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;
