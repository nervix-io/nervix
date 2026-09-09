//! The byte budgets an operation is charged against before it allocates.

use std::{io, ops::Deref, sync::Arc as StdArc};

use arch_into::ArchInto as _;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use triomphe::Arc;

use crate::{MemoryClass, SemaphoreRef};

/// The smallest charge an incremental writer takes. Charging every byte would put a semaphore
/// acquisition in the middle of a serializer's inner loop; charging at least this much keeps the
/// overhead bounded while a small output still wastes little.
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

/// One class's ceiling, expressed as one permit per byte so that a reservation and its release are
/// the same operation the pool already performs for its workers.
#[derive(Debug)]
pub(crate) struct MemoryBudget {
    class: MemoryClass,
    capacity: u32,
    permits: SemaphoreRef,
}

/// What one class of the budget currently holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryBudgetSnapshot {
    pub capacity_bytes: u64,
    pub reserved_bytes: u64,
}

impl MemoryBudget {
    pub(crate) fn new(class: MemoryClass, capacity: u32) -> Self {
        Self {
            class,
            capacity,
            permits: StdArc::new(Semaphore::new(capacity.arch_into())),
        }
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
        }
    }

    pub(crate) fn try_reserve(&self, bytes: u64) -> Result<Reservation, Report<AdmissionError>> {
        let requested = self.checked_request(bytes)?;
        match StdArc::clone(&self.permits).try_acquire_many_owned(requested) {
            Ok(permit) => Ok(self.reservation(requested, permit)),
            Err(TryAcquireError::NoPermits) => Err(Report::new(AdmissionError::BudgetExhausted {
                class: self.class.as_str(),
                requested: bytes,
            })),
            Err(TryAcquireError::Closed) => Err(Report::new(AdmissionError::BudgetClosed {
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
    pub(crate) async fn reserve(&self, bytes: u64) -> Result<Reservation, Report<AdmissionError>> {
        let requested = self.checked_request(bytes)?;
        match StdArc::clone(&self.permits).try_acquire_many_owned(requested) {
            Ok(permit) => return Ok(self.reservation(requested, permit)),
            Err(TryAcquireError::Closed) => {
                return Err(Report::new(AdmissionError::BudgetClosed {
                    class: self.class.as_str(),
                }));
            }
            Err(TryAcquireError::NoPermits) => {}
        }
        let permit = StdArc::clone(&self.permits)
            .acquire_many_owned(requested)
            .await
            .map_err(|_| {
                Report::new(AdmissionError::BudgetClosed {
                    class: self.class.as_str(),
                })
            })?;
        Ok(self.reservation(requested, permit))
    }

    /// Refuse an operation the class could never hold, rather than letting it wait for capacity
    /// that will never exist.
    fn checked_request(&self, bytes: u64) -> Result<u32, Report<AdmissionError>> {
        let capacity = self.capacity.into();
        let requested = u32::try_from(bytes).map_err(|_| {
            Report::new(AdmissionError::ExceedsBudget {
                class: self.class.as_str(),
                requested: bytes,
                capacity,
            })
        })?;
        if requested > self.capacity {
            return Err(Report::new(AdmissionError::ExceedsBudget {
                class: self.class.as_str(),
                requested: bytes,
                capacity,
            }));
        }
        Ok(requested)
    }

    fn reservation(&self, bytes: u32, permit: OwnedSemaphorePermit) -> Reservation {
        Reservation {
            class: self.class,
            capacity: self.capacity,
            bytes,
            permits: StdArc::clone(&self.permits),
            permit,
        }
    }
}

/// Transient memory an operation has been charged for. Dropping it returns the bytes, so the
/// charge lasts exactly as long as the allocation it stands for.
#[derive(Debug)]
pub struct Reservation {
    class: MemoryClass,
    capacity: u32,
    bytes: u32,
    permits: SemaphoreRef,
    permit: OwnedSemaphorePermit,
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
            },
            Self {
                class,
                capacity,
                bytes: second,
                permits,
                permit,
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
    /// to that size before it does. Growth that the class cannot back fails here, with the writer
    /// still holding only what it had.
    pub fn grow_to(&mut self, bytes: u64) -> Result<(), Report<AdmissionError>> {
        let capacity = self.capacity.into();
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
        if target > self.capacity {
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
                Ok(())
            }
            Err(TryAcquireError::NoPermits) => Err(Report::new(AdmissionError::BudgetExhausted {
                class: self.class.as_str(),
                requested: additional.into(),
            })),
            Err(TryAcquireError::Closed) => Err(Report::new(AdmissionError::BudgetClosed {
                class: self.class.as_str(),
            })),
        }
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
    /// allocation it stands for, and `BudgetedBuffer::into_parts` is the one place that does both;
    /// exposing this on its own would offer callers a way to make the budget stop measuring the
    /// memory it bounds.
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

/// An incremental writer produced more than the operation it belongs to is allowed to.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("the output grew to {required} bytes, past the {limit} byte limit for this operation")]
pub struct BufferLimitExceeded {
    pub limit: u64,
    pub required: u64,
}

/// A byte buffer that never grows past what its class has been charged for. Serializers that write
/// incrementally build into it, so an output larger than its budget fails at the boundary instead
/// of after the allocation has already happened.
#[derive(Debug)]
pub struct BudgetedBuffer {
    reservation: Reservation,
    limit: u64,
    bytes: Vec<u8>,
}

impl BudgetedBuffer {
    /// Start a buffer that may grow up to `reservation`, and further only while the class can back
    /// it.
    pub fn new(reservation: Reservation) -> Self {
        let limit = reservation.capacity.into();
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
        }
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

    fn charge(&mut self, additional: usize) -> io::Result<()> {
        let written = u64::try_from(self.bytes.len()).map_err(io::Error::other)?;
        let additional = u64::try_from(additional).map_err(io::Error::other)?;
        let target = written.checked_add(additional).ok_or_else(|| {
            io::Error::other("budgeted buffer length exceeds an addressable size")
        })?;
        if target > self.limit {
            return Err(io::Error::other(BufferLimitExceeded {
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
        // limit, which is where the charge stops.
        let doubled = self
            .reservation
            .bytes()
            .checked_mul(2)
            .unwrap_or(self.limit);
        let rounded = target
            .checked_next_multiple_of(MINIMUM_GROWTH)
            .unwrap_or(target);
        let charged = rounded.max(doubled).min(self.limit).max(target);
        self.reservation
            .grow_to(charged)
            .map_err(io::Error::other)?;
        let reserve: usize = self.reservation.bytes.arch_into();
        if self.bytes.capacity() < reserve {
            let extra = reserve
                .checked_sub(self.bytes.len())
                .ok_or_else(|| io::Error::other("charged length is below the written length"))?;
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
    // Held for exactly as long as `bytes` is, and never read: the charge is the reservation's
    // existence, not its value.
    _reservation: Reservation,
}

impl ChargedBytes {
    /// Take an allocation the caller already holds under a charge for its size, without copying
    /// it. Used where bytes arrive from somewhere that produced them whole.
    pub fn from_owned(bytes: Vec<u8>, reservation: Reservation) -> Self {
        let end = bytes.len();
        Self {
            allocation: Arc::new(ChargedAllocation {
                bytes,
                _reservation: reservation,
            }),
            start: 0,
            end,
        }
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
