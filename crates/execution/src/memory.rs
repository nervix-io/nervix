//! The byte budgets an operation is charged against before it allocates.

use std::{io, ops::Deref};

use arch_into::ArchInto as _;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::sync::{
    Arc, OwnedSemaphorePermit, Semaphore, StdArc, TryAcquireError,
    atomic::{AtomicU64, Ordering},
};
use thiserror::Error;

use crate::{CapacityWaits, MemoryClass, executor::SemaphoreRef};

/// The smallest charge an incremental writer takes. Charging every byte would put a semaphore
/// acquisition in the middle of a serializer's inner loop; charging at least this much keeps the
/// overhead bounded while a small output still wastes little.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "an accounting granule, which Node Settings keeps fixed: charging a growing \
                  buffer at least 4 KiB at a time bounds a writer to one budget acquisition per 4 \
                  KiB of output, while a small output is over-charged by less than 4 KiB"
    )
)]
const MINIMUM_GROWTH: u64 = 4 * 1024;

/// Why an operation was not charged. An operation that is refused here has allocated nothing.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AdmissionError {
    #[error(
        "an operation of {requested} bytes exceeds the whole {class} memory budget of {capacity} \
         bytes"
    )]
    ExceedsBudget {
        class: &'static str,
        requested: u64,
        capacity: u64,
    },
    #[error("the {class} memory budget has no room for another {requested} bytes")]
    BudgetExhausted { class: &'static str, requested: u64 },
    #[error("the {class} memory budget was closed")]
    BudgetClosed { class: &'static str },
    #[error("cannot merge a {first} reservation with a {second} reservation")]
    DifferentBudget {
        first: &'static str,
        second: &'static str,
    },
}

/// The whole charge an operation needed when its class had no room to grow its reservation to it.
///
/// A growth refusal attaches it to its [`AdmissionError::BudgetExhausted`], where it stays beneath
/// every context the failure gains on its way out, so the owner that meets the failure can give
/// back what the operation held and wait for exactly this charge before running it again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shortfall {
    pub class: MemoryClass,
    pub needed: u64,
}

impl Shortfall {
    /// The charge a growth refusal beneath `report` needed, when one is beneath it.
    pub fn of<C>(report: &Report<C>) -> Option<u64> {
        Self::beneath(report).map(|shortfall| shortfall.needed)
    }

    /// The shortfall beneath `report`, with the class it fell short in, when one is beneath it.
    pub fn beneath<C>(report: &Report<C>) -> Option<Self> {
        for frame in report.frames() {
            if let Some(shortfall) = frame.downcast_ref::<Self>() {
                return Some(*shortfall);
            }
        }
        None
    }
}

/// One class's ceiling, expressed as one permit per byte so that a reservation and its release are
/// the same operation the pool already performs for its workers.
///
/// An allowance is a budget too: a share of a class one operation waited for, whose own permits
/// every charge of that class the operation takes is drawn from.
#[derive(Debug)]
pub(crate) struct MemoryBudget {
    class: MemoryClass,
    capacity: u32,
    permits: SemaphoreRef,
    granted: AtomicU64,
    refused: AtomicU64,
    /// Present on an allowance: the share of the class it holds, for as long as anything drawn
    /// from it lives.
    allowance: Option<StdArc<AllowanceHold>>,
}

/// The share of a class an allowance holds, the most that was drawn from it at once, and the
/// largest share its operation was found to need, which its owner runs it again within.
#[derive(Debug)]
pub(crate) struct AllowanceHold {
    _share: Reservation,
    peak: AtomicU64,
    wanted: AtomicU64,
}

impl AllowanceHold {
    /// The capacity of the class the share was taken from: no share can be larger.
    fn class_capacity(&self) -> u32 {
        self._share.capacity
    }

    /// Record that `drawn` bytes are drawn now.
    fn drawn(&self, drawn: u64) {
        self.peak.fetch_max(drawn, Ordering::AcqRel);
    }

    /// The share an operation needs to take `more` bytes beyond the `drawn` it holds now, counting
    /// the most it held at once.
    fn needed(&self, drawn: u64, more: u64) -> u64 {
        nervix_primitives::expect_lint!(
            nervix::clamped_arithmetic,
            "a need beyond u64 bytes is asked for as the largest share, which no budget grants",
            drawn.saturating_add(more)
        )
        .max(self.peak.load(Ordering::Acquire))
    }

    /// Record that a charge the operation needed found the share too small, so its owner runs the
    /// operation again within a share of `needed` bytes however the operation reported the failure.
    fn wanted(&self, needed: u64) {
        self.wanted.fetch_max(needed, Ordering::AcqRel);
    }

    /// The largest share a charge the operation needed found missing, if any did.
    pub(crate) fn shortfall(&self) -> Option<u64> {
        match self.wanted.load(Ordering::Acquire) {
            0 => None,
            needed => Some(needed),
        }
    }
}

/// What one class of the budget currently holds, and how much it has admitted and refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryBudgetSnapshot {
    pub capacity_bytes: u64,
    pub reserved_bytes: u64,
    /// Charges this class has granted since the node started.
    pub granted: u64,
    /// Charges refused because the request was larger than the whole class, or because the class
    /// was closed. A request that waits for room is not refused and is not counted here.
    pub refused: u64,
}

impl MemoryBudget {
    pub(crate) fn new(class: MemoryClass, capacity: u32) -> Self {
        Self {
            class,
            capacity,
            permits: StdArc::new(Semaphore::new(capacity.arch_into())),
            granted: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            allowance: None,
        }
    }

    /// A budget over `share`, which an operation waited for, that every charge of its class the
    /// operation takes is drawn from.
    pub(crate) fn allowance(share: Reservation) -> Self {
        let class = share.class;
        let capacity = share.bytes;
        Self {
            class,
            capacity,
            permits: StdArc::new(Semaphore::new(capacity.arch_into())),
            granted: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            allowance: Some(StdArc::new(AllowanceHold {
                _share: share,
                peak: AtomicU64::new(0),
                wanted: AtomicU64::new(0),
            })),
        }
    }

    pub(crate) fn class(&self) -> MemoryClass {
        self.class
    }

    /// What is drawn from this budget now.
    fn drawn(&self) -> u64 {
        let available: u64 = self.permits.available_permits().arch_into();
        u64::from(self.capacity)
            .checked_sub(available)
            .verified("a budget never holds more permits than the capacity it was built with")
    }

    /// The refusal of `bytes` this budget has no room for now. An allowance's refusal carries
    /// the [`Shortfall`] its operation needs, so the operation's owner can give back everything
    /// and wait for a share that holds all of it; a charge the operation `required`, unlike an
    /// optional one, is also recorded on the allowance.
    fn exhausted(&self, bytes: u64, required: bool) -> Report<AdmissionError> {
        let refusal = Report::new(AdmissionError::BudgetExhausted {
            class: self.class.as_str(),
            requested: bytes,
        });
        match &self.allowance {
            Some(hold) => {
                let needed = hold.needed(self.drawn(), bytes);
                if required {
                    hold.wanted(needed);
                }
                refusal.attach(Shortfall {
                    class: self.class,
                    needed,
                })
            }
            None => refusal,
        }
    }

    /// The allowance this budget is, when it is one.
    pub(crate) fn allowance_hold(&self) -> Option<&StdArc<AllowanceHold>> {
        self.allowance.as_ref()
    }

    pub(crate) fn capacity_bytes(&self) -> u64 {
        u64::from(self.capacity)
    }

    pub(crate) fn snapshot(&self) -> MemoryBudgetSnapshot {
        let capacity_bytes = self.capacity.into();
        let available: u64 = self.permits.available_permits().try_into().verified(
            "the budget never holds more permits than the u32 capacity it was built with",
        );
        MemoryBudgetSnapshot {
            capacity_bytes,
            reserved_bytes: capacity_bytes
                .checked_sub(available)
                .verified("permits are only returned by the reservations this budget issued"),
            granted: self.granted.load(Ordering::Acquire),
            refused: self.refused.load(Ordering::Acquire),
        }
    }

    pub(crate) fn try_reserve(&self, bytes: u64) -> Result<Reservation, Report<AdmissionError>> {
        let requested = self.checked_request(bytes)?;
        match StdArc::clone(&self.permits).try_acquire_many_owned(requested) {
            Ok(permit) => Ok(self.reservation(requested, permit)),
            // An optional charge that finds no room now refuses nothing the node owes: its owner
            // proceeds without it, so it is not counted as a refusal.
            Err(TryAcquireError::NoPermits) => Err(self.exhausted(bytes, false)),
            Err(TryAcquireError::Closed) => Err(self.refusal(AdmissionError::BudgetClosed {
                class: self.class.as_str(),
            })),
        }
    }

    /// Charge `bytes`, waiting only when the class cannot satisfy the request outright.
    ///
    /// The immediate attempt comes first because the frequent case is a small frame against a
    /// class with room, and an `await` there costs a scheduler round trip per frame rather than
    /// the permits themselves. Taking room that is already free lets a request overtake one
    /// queued for more than the class currently has; within a class every request is bounded by
    /// the same operation limit, so what is overtaken is a request the class could not have
    /// admitted anyway.
    ///
    /// A charge that has to wait tells `waits`, when the caller has one, for as long as it waits.
    pub(crate) async fn reserve(
        &self,
        bytes: u64,
        waits: Option<&CapacityWaits>,
    ) -> Result<Reservation, Report<AdmissionError>> {
        let requested = self.checked_request(bytes)?;
        match StdArc::clone(&self.permits).try_acquire_many_owned(requested) {
            Ok(permit) => return Ok(self.reservation(requested, permit)),
            Err(TryAcquireError::Closed) => {
                return Err(self.refusal(AdmissionError::BudgetClosed {
                    class: self.class.as_str(),
                }));
            }
            // Within an allowance nothing waits: everything else drawn from it belongs to the same
            // operation, which would wait behind itself. Its owner waits for a larger share instead.
            Err(TryAcquireError::NoPermits) if self.allowance.is_some() => {
                return Err(self.exhausted(bytes, true));
            }
            Err(TryAcquireError::NoPermits) => {}
        }
        let room = StdArc::clone(&self.permits).acquire_many_owned(requested);
        let acquired = match waits {
            Some(waits) => waits.report(room).await,
            None => room.await,
        };
        let permit = acquired.map_err(|_| {
            self.refusal(AdmissionError::BudgetClosed {
                class: self.class.as_str(),
            })
        })?;
        Ok(self.reservation(requested, permit))
    }

    /// A reservation of no bytes, granted at once whatever the class holds.
    pub(crate) fn nothing(&self) -> Reservation {
        let permit = StdArc::clone(&self.permits)
            .try_acquire_many_owned(0)
            .assured("no permits are always available, and nothing closes a memory budget");
        self.reservation(0, permit)
    }

    /// Refuse an operation the class could never hold, rather than letting it wait for capacity
    /// that will never exist. Within an allowance, a charge beyond the share but within the class
    /// is a shortfall its operation's owner ends with a larger share.
    fn checked_request(&self, bytes: u64) -> Result<u32, Report<AdmissionError>> {
        let ceiling = match &self.allowance {
            Some(hold) => hold.class_capacity(),
            None => self.capacity,
        };
        let exceeds = || {
            self.refusal(AdmissionError::ExceedsBudget {
                class: self.class.as_str(),
                requested: bytes,
                capacity: ceiling.into(),
            })
        };
        let requested = u32::try_from(bytes).map_err(|_| exceeds())?;
        if requested > ceiling {
            return Err(exceeds());
        }
        if requested > self.capacity {
            return Err(self.exhausted(bytes, true));
        }
        Ok(requested)
    }

    /// Count one quota failure and carry its cause, so a node exposes how often a class turned
    /// work away rather than only how full it is at the moment it is scraped.
    fn refusal(&self, error: AdmissionError) -> Report<AdmissionError> {
        self.refused.fetch_add(1, Ordering::AcqRel);
        Report::new(error)
    }

    fn reservation(&self, bytes: u32, permit: OwnedSemaphorePermit) -> Reservation {
        self.granted.fetch_add(1, Ordering::AcqRel);
        if let Some(hold) = &self.allowance {
            hold.drawn(self.drawn());
        }
        Reservation {
            class: self.class,
            capacity: self.capacity,
            bytes,
            permits: StdArc::clone(&self.permits),
            permit,
            allowance: self.allowance.clone(),
        }
    }
}

/// Transient memory an operation has been charged for. Dropping it returns the bytes, so the
/// charge lasts exactly as long as the allocation it stands for.
///
/// A charge drawn from an allowance returns its bytes to the allowance, and keeps the allowance's
/// share of the class held for as long as it lives.
#[derive(Debug)]
pub struct Reservation {
    class: MemoryClass,
    capacity: u32,
    bytes: u32,
    permits: SemaphoreRef,
    permit: OwnedSemaphorePermit,
    allowance: Option<StdArc<AllowanceHold>>,
}

impl Reservation {
    pub fn class(&self) -> MemoryClass {
        self.class
    }

    pub fn bytes(&self) -> u64 {
        self.bytes.into()
    }

    /// Divide one already admitted allocation plan without returning either part to the budget.
    ///
    /// This is used when an owner must admit a compound operation atomically, then hand the
    /// encoded allocation and its decode/scratch overlap to different lifetimes. The two returned
    /// reservations always add up to the original charge.
    pub fn split(self, first_bytes: u64) -> Result<(Self, Self), Report<AdmissionError>> {
        let first = u32::try_from(first_bytes).map_err(|_| {
            Report::new(AdmissionError::ExceedsBudget {
                class: self.class.as_str(),
                requested: first_bytes,
                capacity: self.bytes.into(),
            })
        })?;
        if first > self.bytes {
            return Err(Report::new(AdmissionError::ExceedsBudget {
                class: self.class.as_str(),
                requested: first_bytes,
                capacity: self.bytes.into(),
            }));
        }

        let Self {
            class,
            capacity,
            bytes,
            permits,
            mut permit,
            allowance,
        } = self;
        let first_permit = permit
            .split(first.arch_into())
            .verified("the requested first part was checked against the reservation");
        let second = bytes
            .checked_sub(first)
            .verified("the requested first part was checked against the reservation");
        Ok((
            Self {
                class,
                capacity,
                bytes: first,
                permits: StdArc::clone(&permits),
                permit: first_permit,
                allowance: allowance.clone(),
            },
            Self {
                class,
                capacity,
                bytes: second,
                permits,
                permit,
                allowance,
            },
        ))
    }

    /// Rejoin two charges from the same memory class and budget.
    ///
    /// Relay admission uses this after receiving its encoded body: the body allocation then owns
    /// the decoded/scratch overlap reserved by its grant, so dropping the last body handle releases
    /// the whole operation atomically.
    pub fn merge(mut self, other: Self) -> Result<Self, Report<AdmissionError>> {
        if self.class != other.class || !StdArc::ptr_eq(&self.permits, &other.permits) {
            return Err(Report::new(AdmissionError::DifferentBudget {
                first: self.class.as_str(),
                second: other.class.as_str(),
            }));
        }
        let requested = u64::from(self.bytes)
            .checked_add(u64::from(other.bytes))
            .assured("adding two u32 reservation sizes fits in u64");
        let bytes = self.bytes.checked_add(other.bytes).ok_or_else(|| {
            Report::new(AdmissionError::ExceedsBudget {
                class: self.class.as_str(),
                requested,
                capacity: self.capacity.into(),
            })
        })?;
        let Self { permit, .. } = other;
        self.permit.merge(permit);
        self.bytes = bytes;
        Ok(self)
    }

    /// Charge the class for everything up to `bytes`, so an incremental writer knows it may grow
    /// to that size before it does. Growth that the class cannot back now fails here, with the
    /// writer still holding only what it had, and the [`Shortfall`] it needed attached: the
    /// writer's owner gives back what it holds and waits for that charge, rather than waiting
    /// while holding part of the class.
    pub fn grow_to(&mut self, bytes: u64) -> Result<(), Report<AdmissionError>> {
        self.grow(bytes, true)
    }

    /// Grow this charge to cover `bytes` when its budget has room for the growth now, as an
    /// optional extra its operation goes without otherwise. Unlike [`Self::grow_to`], a growth an
    /// allowance's share has no room for is not recorded on the allowance, so the operation's own
    /// answer without the extra stands and it does not run again within a larger share.
    pub fn try_grow_to(&mut self, bytes: u64) -> Result<(), Report<AdmissionError>> {
        self.grow(bytes, false)
    }

    /// Grow this charge to `bytes`. A growth the operation `required` that its allowance has no
    /// room for is recorded on the allowance, so its owner runs it again within a larger share.
    fn grow(&mut self, bytes: u64, required: bool) -> Result<(), Report<AdmissionError>> {
        // A charge drawn from an allowance can grow past the share by a larger share, but none can
        // grow past its class.
        let ceiling = self.ceiling();
        let capacity = ceiling.into();
        let target = u32::try_from(bytes).map_err(|_| {
            Report::new(AdmissionError::ExceedsBudget {
                class: self.class.as_str(),
                requested: bytes,
                capacity,
            })
        })?;
        if target <= self.bytes {
            return Ok(());
        }
        if target > ceiling {
            return Err(Report::new(AdmissionError::ExceedsBudget {
                class: self.class.as_str(),
                requested: bytes,
                capacity,
            }));
        }
        let additional = target.checked_sub(self.bytes).ok_or_else(|| {
            Report::new(AdmissionError::ExceedsBudget {
                class: self.class.as_str(),
                requested: bytes,
                capacity,
            })
        })?;
        match StdArc::clone(&self.permits).try_acquire_many_owned(additional) {
            Ok(extra) => {
                self.permit.merge(extra);
                self.bytes = target;
                if let Some(hold) = &self.allowance {
                    hold.drawn(self.drawn());
                }
                Ok(())
            }
            Err(TryAcquireError::NoPermits) => {
                // Drawn from an allowance, the operation needs a share holding everything it
                // draws; on its own, the charge needs room for its whole target.
                let needed = match &self.allowance {
                    Some(hold) => {
                        let needed = hold.needed(self.drawn(), additional.into());
                        if required {
                            hold.wanted(needed);
                        }
                        needed
                    }
                    None => target.into(),
                };
                Err(Report::new(AdmissionError::BudgetExhausted {
                    class: self.class.as_str(),
                    requested: additional.into(),
                })
                .attach(Shortfall {
                    class: self.class,
                    needed,
                }))
            }
            Err(TryAcquireError::Closed) => Err(Report::new(AdmissionError::BudgetClosed {
                class: self.class.as_str(),
            })),
        }
    }

    /// The most this charge can ever grow to: its class's capacity, even when it was drawn from an
    /// allowance of that class.
    fn ceiling(&self) -> u32 {
        match &self.allowance {
            Some(hold) => hold.class_capacity(),
            None => self.capacity,
        }
    }

    /// What is drawn from the budget this charge came from now.
    fn drawn(&self) -> u64 {
        let available: u64 = self.permits.available_permits().arch_into();
        u64::from(self.capacity)
            .checked_sub(available)
            .verified("a budget never holds more permits than the capacity it was built with")
    }

    /// Give back everything above `bytes`.
    ///
    /// A writer charges ahead of itself so it can grow without asking per byte, and most writers
    /// stop well short of what they asked for. Returning the difference is what keeps the charge a
    /// measure of the allocation rather than of the room the writer reserved to work in: a
    /// five-byte heartbeat that started with a four-kibibyte reservation must not hold four
    /// kibibytes of its class for as long as it is queued.
    ///
    /// Deliberately not public. Narrowing a charge is only correct alongside narrowing the
    /// allocation it stands for: `BudgetedBuffer::into_parts` narrows a writer's charge to what it
    /// wrote, and `ChargedBytes::keep_charge_for` narrows a body's charge to the body and the data
    /// decoded from it. Exposing this on its own would offer callers a way to make the budget stop
    /// measuring the memory it bounds.
    pub(crate) fn shrink_to(&mut self, bytes: u64) {
        let Ok(target) = u32::try_from(bytes) else {
            return;
        };
        let Some(excess) = self.bytes.checked_sub(target) else {
            return;
        };
        if excess == 0 {
            return;
        }
        // Dropping the split permits returns them to the class.
        if self.permit.split(excess.arch_into()).is_some() {
            self.bytes = target;
        }
    }
}

/// A charge for an operation that learns its size as it goes.
///
/// It counts what the operation holds apart from what its reservation holds. An operation that ran
/// short gives everything back and runs again under the larger charge it needed: counting its own
/// bytes from nothing again keeps that charge from being added to, rather than covering, what it
/// asks for the second time, and the charge never shrinks below what it started with, so the
/// operation keeps the room it waited for until it ends.
#[derive(Debug)]
pub struct GrowingCharge {
    reservation: Reservation,
    /// What the operation holds now.
    required: u64,
    /// The most the operation has held at once.
    peak: u64,
    /// What the charge was granted at the start, which it keeps until the operation ends.
    floor: u64,
}

impl GrowingCharge {
    /// Count an operation from nothing under `reservation`, which may already hold more.
    pub fn new(reservation: Reservation) -> Self {
        let floor = reservation.bytes();
        Self {
            reservation,
            required: 0,
            peak: 0,
            floor,
        }
    }

    /// What the operation holds now.
    pub fn required(&self) -> u64 {
        self.required
    }

    /// Count `bytes` more and grow the charge to cover what the operation holds. A class without
    /// room for it now fails with the [`Shortfall`] the operation needs, the most it has held at
    /// once included, and counts nothing.
    pub fn grow_by(&mut self, bytes: u64) -> Result<(), Report<AdmissionError>> {
        let Some(required) = self.required.checked_add(bytes) else {
            return Err(Report::new(AdmissionError::ExceedsBudget {
                class: self.reservation.class.as_str(),
                requested: u64::MAX,
                capacity: self.reservation.capacity.into(),
            }));
        };
        if let Err(refusal) = self.reservation.grow_to(required) {
            // Only a class without room now can grant the charge later; a charge larger than the
            // whole class, or one a closed class refuses, is the operation's answer as it is.
            if !matches!(
                refusal.current_context(),
                AdmissionError::BudgetExhausted { .. }
            ) {
                return Err(refusal);
            }
            return Err(refusal.attach(Shortfall {
                class: self.reservation.class,
                needed: required.max(self.peak),
            }));
        }
        self.required = required;
        self.peak = self.peak.max(required);
        Ok(())
    }

    /// The operation freed `bytes` it held. The charge gives back what it holds beyond what the
    /// operation still holds, but never less than it was granted at the start. A release of more
    /// than the operation holds leaves it holding nothing.
    pub fn release(&mut self, bytes: u64) {
        self.required = nervix_primitives::expect_lint!(
            nervix::clamped_arithmetic,
            "a release of more than the operation holds leaves it holding nothing",
            self.required.saturating_sub(bytes)
        );
        self.reservation.shrink_to(self.required.max(self.floor));
    }

    /// The charge, narrowed to what the operation holds.
    pub fn into_reservation(mut self) -> Reservation {
        self.reservation.shrink_to(self.required);
        self.reservation
    }
}

/// An incremental writer produced more than the operation it belongs to is allowed to.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("the output grew to {required} bytes, past the {limit} byte limit for this operation")]
pub struct BufferLimitExceeded {
    pub limit: u64,
    pub required: u64,
}

/// Why a budgeted buffer did not take more bytes.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum BufferGrowthError {
    /// The bytes would take the buffer past the limit of the operation it belongs to.
    #[error(transparent)]
    LimitExceeded(BufferLimitExceeded),
    /// The buffer's length would not fit an address.
    #[error("budgeted buffer length exceeds an addressable size")]
    Unaddressable,
    /// The buffer's class did not back the growth. The class's [`AdmissionError`] is the cause
    /// beneath this context.
    #[error("the budgeted buffer's memory class did not back its growth")]
    NotCharged,
}

/// Why one charge for more bytes failed, kept as the cause each caller's outcome is built from.
enum GrowthRefusal {
    Limit(BufferLimitExceeded),
    Unaddressable,
    Admission(Report<AdmissionError>),
}

impl GrowthRefusal {
    /// The refusal as the I/O error a writer through [`io::Write`] reports.
    fn into_io_error(self) -> io::Error {
        match self {
            Self::Limit(limit) => io::Error::other(limit),
            Self::Unaddressable => {
                io::Error::other("budgeted buffer length exceeds an addressable size")
            }
            Self::Admission(refusal) => io::Error::other(refusal),
        }
    }

    /// The refusal as a typed report, with the class's own refusal beneath when it made one.
    fn into_report(self) -> Report<BufferGrowthError> {
        match self {
            Self::Limit(limit) => Report::new(BufferGrowthError::LimitExceeded(limit)),
            Self::Unaddressable => Report::new(BufferGrowthError::Unaddressable),
            Self::Admission(refusal) => refusal.change_context(BufferGrowthError::NotCharged),
        }
    }
}

/// A byte buffer that never grows past what its class has been charged for. Serializers that write
/// incrementally build into it, so an output larger than its budget fails at the boundary instead
/// of after the allocation has already happened.
///
/// A synchronous writer cannot wait for room. When its class has none for the growth it needs, the
/// buffer refuses the write and records the charge it needed as its [`Self::shortfall`], so the
/// job's owner can give back everything the job held, wait for that charge, and run the job again
/// under it (see [`crate::Executor::run_cpu_growing`]).
#[derive(Debug)]
pub struct BudgetedBuffer {
    reservation: Reservation,
    limit: u64,
    bytes: Vec<u8>,
    shortfall: Option<u64>,
}

impl BudgetedBuffer {
    /// Start a buffer that may grow up to `reservation`, and further only while the class can back
    /// it.
    pub fn new(reservation: Reservation) -> Self {
        let limit = reservation.ceiling().into();
        Self::with_limit(reservation, limit)
    }

    /// Start a buffer that additionally refuses to grow past `limit`, so an operation with its own
    /// declared maximum fails at that maximum rather than at whatever its class happens to have
    /// free.
    pub fn with_limit(reservation: Reservation, limit: u64) -> Self {
        let capacity: usize = reservation.bytes.arch_into();
        Self {
            reservation,
            limit,
            bytes: Vec::with_capacity(capacity),
            shortfall: None,
        }
    }

    /// The charge this buffer needed and its class had no room for, once a write was refused for
    /// that reason. A writer that meets it gives back what it holds rather than waiting, because a
    /// wait while holding part of the class can stall behind the very bytes it holds.
    pub fn shortfall(&self) -> Option<u64> {
        self.shortfall
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// The written bytes and the charge that backs them, narrowed to each other.
    ///
    /// A writer reserves room to grow into and usually stops short of it. Both halves of that room
    /// are given back here: the vector drops to what it holds and the charge follows it. Releasing
    /// one without the other would break the property the budget exists for — a class that has
    /// released its charge but not its allocation stops measuring the memory it is bounding.
    pub fn into_parts(mut self) -> (Vec<u8>, Reservation) {
        self.bytes.shrink_to_fit();
        self.reservation.shrink_to(self.bytes.len().arch_into());
        (self.bytes, self.reservation)
    }

    /// Charge and append `len` zeroed bytes, returning the new region for a reader to fill. A
    /// reader that cannot be charged for the bytes it is about to receive never allocates room for
    /// them.
    pub fn extend_zeroed(&mut self, len: usize) -> io::Result<&mut [u8]> {
        self.charge(len)?;
        let start = self.bytes.len();
        self.bytes.resize(
            start
                .checked_add(len)
                .ok_or_else(|| io::Error::other("frame length exceeds an addressable size"))?,
            0,
        );
        Ok(&mut self.bytes[start..])
    }

    /// Append `bytes`, charging the growth they need to room the buffer's class has free.
    ///
    /// Writing through [`io::Write`] reports a refusal as an I/O error. This reports it typed, so
    /// a reader can tell a class without free room, an [`AdmissionError`] beneath
    /// [`BufferGrowthError::NotCharged`], from a buffer at its operation's limit.
    pub fn try_extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), Report<BufferGrowthError>> {
        self.try_charge(bytes.len())
            .map_err(GrowthRefusal::into_report)?;
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn charge(&mut self, additional: usize) -> io::Result<()> {
        self.try_charge(additional)
            .map_err(GrowthRefusal::into_io_error)
    }

    fn try_charge(&mut self, additional: usize) -> Result<(), GrowthRefusal> {
        let written: u64 = self.bytes.len().arch_into();
        let additional: u64 = additional.arch_into();
        let target = written
            .checked_add(additional)
            .ok_or(GrowthRefusal::Unaddressable)?;
        if target > self.limit {
            return Err(GrowthRefusal::Limit(BufferLimitExceeded {
                limit: self.limit,
                required: target,
            }));
        }
        if target <= self.reservation.bytes() {
            return Ok(());
        }
        // Grow geometrically, so a serializer that writes a megabyte in small pieces takes the
        // budget a logarithmic number of times rather than once per piece, and a frame that writes
        // a hundred bytes does not hold a granule it will never use. Doubling can overshoot the
        // limit or the whole class, which is where the charge stops; only the bytes the write
        // itself needs can exceed the class.
        let doubled = self
            .reservation
            .bytes()
            .checked_mul(2)
            .unwrap_or(self.limit);
        let rounded = target
            .checked_next_multiple_of(MINIMUM_GROWTH)
            .unwrap_or(target);
        let class_capacity = u64::from(self.reservation.ceiling());
        let charged = rounded
            .max(doubled)
            .min(self.limit)
            .min(class_capacity)
            .max(target);
        if let Err(refusal) = self.reservation.grow_to(charged) {
            if let AdmissionError::BudgetExhausted { .. } = refusal.current_context() {
                self.shortfall = Some(charged);
            }
            return Err(GrowthRefusal::Admission(refusal));
        }
        let reserve: usize = self.reservation.bytes.arch_into();
        if self.bytes.capacity() < reserve {
            let extra = reserve.checked_sub(self.bytes.len()).verified(
                "the charge just grew to at least the target, which covers the written bytes",
            );
            self.bytes.reserve_exact(extra);
        }
        Ok(())
    }
}

impl io::Write for BudgetedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.charge(buf.len())?;
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// One immutable allocation and the charge that backs it, shared by every holder.
///
/// Work that produces bytes many destinations send — an encoded body, a hashed archive chunk —
/// produces them once and hands out this handle. The class is charged for the allocation once, and
/// the charge is returned when the last holder drops it. A destination that additionally owes
/// bytes while its own delivery is outstanding reserves those separately.
#[derive(Clone, Debug)]
pub struct ChargedBytes {
    allocation: Arc<ChargedAllocation>,
    start: usize,
    end: usize,
}

#[derive(Debug)]
struct ChargedAllocation {
    bytes: Vec<u8>,
    // Held for exactly as long as `bytes` is. Its value is read only to give back room charged
    // beyond the bytes.
    reservation: Reservation,
}

impl ChargedBytes {
    /// Take an allocation the caller already holds under a charge for its size, without copying
    /// it. Used where bytes arrive from somewhere that produced them whole.
    pub fn from_owned(bytes: Vec<u8>, reservation: Reservation) -> Self {
        let end = bytes.len();
        Self {
            allocation: Arc::new(ChargedAllocation { bytes, reservation }),
            start: 0,
            end,
        }
    }

    /// Return to its class the charge this allocation holds beyond its own bytes and `additional`
    /// bytes more, when this handle is the allocation's only one.
    ///
    /// Bytes that arrived under a charge for an operation larger than themselves, such as a relay
    /// body whose grant also reserved room to decode it, give the room back this way once the
    /// operation knows how much of it the data kept beside the bytes takes. An allocation another
    /// handle still shares keeps its whole charge, since that handle may still need the room.
    pub fn keep_charge_for(&mut self, additional: u64) {
        let Some(allocation) = Arc::get_mut(&mut self.allocation) else {
            return;
        };
        let own: u64 = allocation.bytes.len().arch_into();
        // Room beyond what any charge can count is beyond this charge too.
        let Some(kept) = own.checked_add(additional) else {
            return;
        };
        allocation.reservation.shrink_to(kept);
    }

    /// Freeze what an incremental writer produced. The writer's unused room, and the charge that
    /// stood for it, are both released as the buffer is taken apart, so a queued frame holds its
    /// own size and nothing more.
    pub fn from_buffer(buffer: BudgetedBuffer) -> Self {
        let (bytes, reservation) = buffer.into_parts();
        Self::from_owned(bytes, reservation)
    }

    /// A window onto the same allocation, so a framed body is carried out of the frame it arrived
    /// in without copying it into a second one.
    pub fn slice(&self, start: usize, end: usize) -> Option<Self> {
        if start > end {
            return None;
        }
        let absolute_start = self.start.checked_add(start)?;
        let absolute_end = self.start.checked_add(end)?;
        if absolute_end > self.end {
            return None;
        }
        Some(Self {
            allocation: Arc::clone(&self.allocation),
            start: absolute_start,
            end: absolute_end,
        })
    }

    /// Whether two handles name the same allocation, which is how a fanout proves it encoded its
    /// body once and shared it rather than encoding it per destination.
    pub fn shares_allocation_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.allocation, &other.allocation)
    }

    pub fn len(&self) -> usize {
        self.end
            .checked_sub(self.start)
            .verified("every window is constructed with its start at or before its end")
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Deref for ChargedBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.allocation.bytes[self.start..self.end]
    }
}

impl AsRef<[u8]> for ChargedBytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

/// Two handles are equal when they carry the same bytes: the charge behind them is bookkeeping,
/// not part of the value.
impl PartialEq for ChargedBytes {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Eq for ChargedBytes {}
