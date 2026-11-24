//! The admission of one transport direction's requests: the general queue's places and the
//! requests in flight of every kind.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The general queue's places, held by requests without a reserved kind, and the count
//!   of admitted, unresolved requests of each kind in one direction.
//! - **Depends on.** The transport's declared incoming queue and the primitive boundary's semaphore
//!   and atomics.
//! - **Must not know.** What an admitted request carries, which peer it concerns, or which stream
//!   carries it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use meticulous::OptionExt as _;
use nervix_primitives::sync::{
    Arc, OwnedSemaphorePermit, Semaphore, StdArc,
    atomic::{AtomicUsize, Ordering},
};
use strum::EnumCount as _;

use super::RequestSubquota;

/// The requests one direction admitted. A request of a reserved kind is always admitted: the
/// connection to its peer reserves stream slots for its kind, and it waits for one of them until
/// its own deadline, so no count of its own stands beside them. Requests without a reserved kind
/// share the incoming queue's places, and one finding them all held is refused.
pub(crate) struct RequestAdmissions {
    /// The places requests without a reserved kind share: the declared incoming queue.
    shared: StdArc<Semaphore>,
    /// Requests admitted into each kind and not yet resolved, by subquota index, for observation.
    in_flight: [AtomicUsize; RequestSubquota::COUNT],
}

impl RequestAdmissions {
    pub(crate) fn new(options: &crate::TransportOptions) -> Self {
        Self {
            shared: StdArc::new(Semaphore::new(options.incoming_queue_capacity)),
            in_flight: std::array::from_fn(|_| AtomicUsize::new(0)),
        }
    }

    /// Admits one `subquota` request into `admissions`, or refuses a request without a reserved
    /// kind while every place of the incoming queue is held.
    pub(crate) fn try_admit(
        admissions: &Arc<Self>,
        subquota: RequestSubquota,
    ) -> Option<RequestAdmission> {
        let place = match subquota {
            RequestSubquota::Shared => {
                let place = StdArc::clone(&admissions.shared).try_acquire_owned().ok()?;
                Some(place)
            }
            RequestSubquota::Append
            | RequestSubquota::Resource
            | RequestSubquota::Snapshot
            | RequestSubquota::Discovery
            | RequestSubquota::Liveness
            | RequestSubquota::Progress
            | RequestSubquota::Admission
            | RequestSubquota::Cancellation
            | RequestSubquota::Terminal => None,
        };
        update(&admissions.in_flight[subquota.index()], |count| {
            count.checked_add(1)
        })
        .verified(
            "every request in flight holds a task and its charged bytes, far fewer than usize",
        );
        Some(RequestAdmission {
            admissions: Arc::clone(admissions),
            subquota,
            place,
        })
    }

    /// Requests admitted into each subquota and not yet resolved, by subquota index.
    pub(crate) fn in_flight(&self) -> [usize; RequestSubquota::COUNT] {
        std::array::from_fn(|index| self.in_flight[index].load(Ordering::Relaxed))
    }
}

/// One admitted request, counted in flight until it resolves.
pub(crate) struct RequestAdmission {
    admissions: Arc<RequestAdmissions>,
    subquota: RequestSubquota,
    /// The incoming queue's place a request without a reserved kind holds until it resolves.
    place: Option<OwnedSemaphorePermit>,
}

impl RequestAdmission {
    /// Whether this admission holds a place of the incoming queue. The place bounds what its
    /// request holds uncharged while an encoding or decoding waits for room. A request of a
    /// reserved kind holds no place, so the stream it leases bounds that instead.
    pub(crate) fn holds_a_place(&self) -> bool {
        self.place.is_some()
    }
}

impl Drop for RequestAdmission {
    fn drop(&mut self) {
        update(&self.admissions.in_flight[self.subquota.index()], |count| {
            count.checked_sub(1)
        })
        .verified("an admission resolves exactly the one request it counted");
    }
}

/// Replaces `counter`'s value with what `change` makes of it, unless `change` refuses the value it
/// read, and returns the value it replaced.
fn update(counter: &AtomicUsize, change: impl Fn(usize) -> Option<usize>) -> Option<usize> {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = change(current)?;
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(replaced) => return Some(replaced),
            Err(observed) => current = observed,
        }
    }
}
