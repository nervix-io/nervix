//! How one connection's HTTP/2 stream capacity is partitioned between its reserved classes.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** HTTP/2 builder settings and the stream-slot partition each traffic class runs under,
//!   the reserved subquotas
//!   inside the management, replication and bulk partitions, the leases taken from them, and the
//!   drain that takes every slot back when a connection retires.
//! - **Depends on.** The traffic classes and the reserved request subquotas they are divided by.
//! - **Must not know.** Connection lifetime, request dispatch, or what any message means.
//!
//! Every partition is checked against its class capacity at compile time, so a reserved class can
//! never be given capacity the connection does not have and the shared remainder is always what
//! the reservations leave behind.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use nervix_primitives::sync::atomic::{AtomicBool, Ordering};

use super::*;

/// One connection's stream-slot partition, and the fence that ends leasing from it once the
/// connection drains.
///
/// Tokio's owned permits retain a `std::sync::Arc` to their one semaphore after the connection
/// quota bundle is no longer borrowed. The surrounding `triomphe::Arc` keeps cloning a complete
/// quota bundle to one reference-count operation.
#[derive(Clone)]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "retained stream quotas are charged for each admitted request"
    )
)]
pub(super) struct StreamSlotQuotas {
    inner: Arc<StreamSlotQuotasInner>,
}

struct StreamSlotQuotasInner {
    /// Raised when the connection begins to drain and never lowered, because a drained connection
    /// is closed rather than reused.
    ///
    /// The semaphores decide what the drain waits for: it takes every permit back, so a lease
    /// whose slot came first is one the drain waits for, and a lease after the drain holds every
    /// permit finds none left. A lease also reads this after taking its slot and returns the slot
    /// when it is raised, which stops leasing as soon as the drain begins rather than once the
    /// drain holds every permit.
    draining: AtomicBool,
    partition: StreamSlotPartition,
    capacity: usize,
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "a retained stream partition admits and drains stream slots for recurring \
                  requests"
    )
)]
enum StreamSlotPartition {
    Management(ManagementStreamSlotQuotas),
    Replication(ReplicationStreamSlotQuotas),
    Bulk(BulkStreamSlotQuotas),
    Shared {
        capacity: usize,
        slots: StdArc<Semaphore>,
    },
}

struct ManagementStreamSlotQuotas {
    shared_capacity: usize,
    shared: StdArc<Semaphore>,
    discovery: StdArc<Semaphore>,
    liveness: StdArc<Semaphore>,
    progress: StdArc<Semaphore>,
    admission: StdArc<Semaphore>,
    cancellation: StdArc<Semaphore>,
    terminal: StdArc<Semaphore>,
}

struct BulkStreamSlotQuotas {
    shared_capacity: usize,
    shared: StdArc<Semaphore>,
    resource: StdArc<Semaphore>,
    snapshot: StdArc<Semaphore>,
}

/// The one ordered append stream a leader keeps open to a follower reserves its own slot, so the
/// ownership handoff requests that share this pool are never held behind it.
struct ReplicationStreamSlotQuotas {
    shared_capacity: usize,
    shared: StdArc<Semaphore>,
    append: StdArc<Semaphore>,
}

const MANAGEMENT_TOTAL_STREAMS: usize = PoolClass::Management.stream_slots_per_connection();
const MANAGEMENT_DISCOVERY_STREAMS: usize = 4;
const MANAGEMENT_LIVENESS_STREAMS: usize = 8;
pub(crate) const MANAGEMENT_PROGRESS_STREAMS: usize = MANAGEMENT_LIVENESS_STREAMS;
const MANAGEMENT_ADMISSION_STREAMS: usize = 4;
const MANAGEMENT_CANCELLATION_STREAMS: usize = 4;
const MANAGEMENT_TERMINAL_STREAMS: usize = 4;
const MANAGEMENT_RESERVED_STREAMS: usize = MANAGEMENT_DISCOVERY_STREAMS
    + MANAGEMENT_LIVENESS_STREAMS
    + MANAGEMENT_PROGRESS_STREAMS
    + MANAGEMENT_ADMISSION_STREAMS
    + MANAGEMENT_CANCELLATION_STREAMS
    + MANAGEMENT_TERMINAL_STREAMS;
const _: () = assert!(
    MANAGEMENT_RESERVED_STREAMS < MANAGEMENT_TOTAL_STREAMS,
    "reserved management stream quotas must leave shared capacity",
);
pub(crate) const MANAGEMENT_SHARED_STREAMS: usize =
    MANAGEMENT_TOTAL_STREAMS - MANAGEMENT_RESERVED_STREAMS;
const _: () = assert!(
    MANAGEMENT_SHARED_STREAMS + MANAGEMENT_RESERVED_STREAMS == MANAGEMENT_TOTAL_STREAMS,
    "management stream subquotas must exactly partition the HTTP/2 stream capacity",
);
const _: () = assert!(
    MANAGEMENT_DISCOVERY_STREAMS > 0
        && MANAGEMENT_LIVENESS_STREAMS > 0
        && MANAGEMENT_PROGRESS_STREAMS > 0
        && MANAGEMENT_ADMISSION_STREAMS > 0
        && MANAGEMENT_CANCELLATION_STREAMS > 0
        && MANAGEMENT_TERMINAL_STREAMS > 0,
    "every reserved management stream class must have capacity",
);
const REPLICATION_TOTAL_STREAMS: usize = PoolClass::Replication.stream_slots_per_connection();
/// One live append stream per follower, and room for its replacement while the live one is still
/// being torn down.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "one active ordered append stream and one replacement are independently retained \
                  per follower"
    )
)]
const REPLICATION_APPEND_STREAMS: usize = 2;
const _: () = assert!(
    REPLICATION_APPEND_STREAMS > 0 && REPLICATION_APPEND_STREAMS < REPLICATION_TOTAL_STREAMS,
    "the reserved append stream quota must leave shared replication capacity",
);
const REPLICATION_SHARED_STREAMS: usize = REPLICATION_TOTAL_STREAMS - REPLICATION_APPEND_STREAMS;
const _: () = assert!(
    REPLICATION_SHARED_STREAMS + REPLICATION_APPEND_STREAMS == REPLICATION_TOTAL_STREAMS,
    "replication stream subquotas must exactly partition the HTTP/2 stream capacity",
);
const BULK_TOTAL_STREAMS: usize = PoolClass::Bulk.stream_slots_per_connection();
const BULK_RESOURCE_STREAMS: usize = 2;
const BULK_SNAPSHOT_STREAMS: usize = 1;
const BULK_RESERVED_STREAMS: usize = BULK_RESOURCE_STREAMS + BULK_SNAPSHOT_STREAMS;
const _: () = assert!(
    BULK_RESERVED_STREAMS < BULK_TOTAL_STREAMS,
    "reserved bulk stream quotas must leave shared capacity",
);
const BULK_SHARED_STREAMS: usize = BULK_TOTAL_STREAMS - BULK_RESERVED_STREAMS;
const _: () = assert!(
    BULK_SHARED_STREAMS + BULK_RESERVED_STREAMS == BULK_TOTAL_STREAMS,
    "bulk stream subquotas must exactly partition the HTTP/2 stream capacity",
);

impl StreamSlotQuotas {
    pub(super) fn new(class: PoolClass) -> Self {
        Self {
            inner: Arc::new(StreamSlotQuotasInner {
                draining: AtomicBool::new(false),
                partition: StreamSlotPartition::new(class),
                capacity: class.stream_slots_per_connection(),
            }),
        }
    }

    /// Apply the peer's initial SETTINGS after the binding reply and before publication.
    /// Only shared capacity shrinks; every reserved traffic class keeps its guaranteed slots.
    pub(super) fn negotiate(
        &mut self,
        class: PoolClass,
        peer_limit: usize,
    ) -> Result<(), Report<TransportError>> {
        let inner = Arc::get_mut(&mut self.inner)
            .verified("binding owns the unpublished stream partition exclusively");
        let (shared, shared_capacity, reserved) = match &mut inner.partition {
            StreamSlotPartition::Management(quotas) => (
                &quotas.shared,
                &mut quotas.shared_capacity,
                MANAGEMENT_RESERVED_STREAMS,
            ),
            StreamSlotPartition::Replication(quotas) => (
                &quotas.shared,
                &mut quotas.shared_capacity,
                REPLICATION_APPEND_STREAMS,
            ),
            StreamSlotPartition::Bulk(quotas) => (
                &quotas.shared,
                &mut quotas.shared_capacity,
                BULK_RESERVED_STREAMS,
            ),
            StreamSlotPartition::Shared { slots, capacity } => (&*slots, capacity, 0),
        };
        let minimum = reserved + 1;
        if peer_limit < minimum {
            return Err(Report::new(TransportError::Http2StreamCapacity {
                class,
                advertised: peer_limit,
                minimum,
            }));
        }
        let capacity = peer_limit.min(inner.capacity);
        let remaining = capacity - reserved;
        let removed = *shared_capacity - remaining;
        let removed = u32::try_from(removed)
            .assured("the local stream partition fits HTTP/2's setting width");
        if removed > 0 {
            shared
                .try_acquire_many(removed)
                .verified("no stream leases exist before the binding is published")
                .forget();
        }
        *shared_capacity = remaining;
        inner.capacity = capacity;
        Ok(())
    }

    /// Lease one stream slot from `subquota`, unless every slot it reserves is already leased or
    /// the connection has begun to drain.
    pub(super) fn try_lease(&self, subquota: RequestSubquota) -> Option<OwnedSemaphorePermit> {
        let quota = self
            .inner
            .partition
            .for_subquota(subquota)
            .assured("reserved stream subquotas are assigned to their configured pool");
        let slot = StdArc::clone(quota).try_acquire_owned().ok()?;
        if self.inner.draining.load(Ordering::SeqCst) {
            drop(slot);
            return None;
        }
        Some(slot)
    }

    /// Stream slots currently leased on this connection: its class capacity, less what its
    /// subquotas still hold free.
    pub(super) fn leased(&self) -> usize {
        self.inner
            .capacity
            .checked_sub(self.inner.partition.available())
            .verified("stream permits are only taken and returned by this connection's leases")
    }

    /// Stop leasing from this connection, and return the wait for every slot leased before that.
    ///
    /// Leasing stops when this is called rather than when the wait is first polled, so a caller
    /// that races the wait against a deadline has fenced the connection either way. Once the wait
    /// completes no slot is leased, and none is leased afterwards.
    pub(super) fn drain(&self) -> impl Future<Output = ()> + Send + '_ {
        self.inner.draining.store(true, Ordering::SeqCst);
        self.inner.partition.drain()
    }
}

impl StreamSlotPartition {
    fn new(class: PoolClass) -> Self {
        if class == PoolClass::Management {
            return Self::Management(ManagementStreamSlotQuotas {
                shared_capacity: MANAGEMENT_SHARED_STREAMS,
                shared: StdArc::new(Semaphore::new(MANAGEMENT_SHARED_STREAMS)),
                discovery: StdArc::new(Semaphore::new(MANAGEMENT_DISCOVERY_STREAMS)),
                liveness: StdArc::new(Semaphore::new(MANAGEMENT_LIVENESS_STREAMS)),
                progress: StdArc::new(Semaphore::new(MANAGEMENT_PROGRESS_STREAMS)),
                admission: StdArc::new(Semaphore::new(MANAGEMENT_ADMISSION_STREAMS)),
                cancellation: StdArc::new(Semaphore::new(MANAGEMENT_CANCELLATION_STREAMS)),
                terminal: StdArc::new(Semaphore::new(MANAGEMENT_TERMINAL_STREAMS)),
            });
        }
        if class == PoolClass::Replication {
            return Self::Replication(ReplicationStreamSlotQuotas {
                shared_capacity: REPLICATION_SHARED_STREAMS,
                shared: StdArc::new(Semaphore::new(REPLICATION_SHARED_STREAMS)),
                append: StdArc::new(Semaphore::new(REPLICATION_APPEND_STREAMS)),
            });
        }
        if class == PoolClass::Bulk {
            return Self::Bulk(BulkStreamSlotQuotas {
                shared_capacity: BULK_SHARED_STREAMS,
                shared: StdArc::new(Semaphore::new(BULK_SHARED_STREAMS)),
                resource: StdArc::new(Semaphore::new(BULK_RESOURCE_STREAMS)),
                snapshot: StdArc::new(Semaphore::new(BULK_SNAPSHOT_STREAMS)),
            });
        }
        Self::Shared {
            capacity: class.stream_slots_per_connection(),
            slots: StdArc::new(Semaphore::new(class.stream_slots_per_connection())),
        }
    }

    fn for_subquota(&self, subquota: RequestSubquota) -> Option<&StdArc<Semaphore>> {
        match self {
            Self::Management(quotas) => quotas.for_subquota(subquota),
            Self::Replication(quotas) => quotas.for_subquota(subquota),
            Self::Bulk(quotas) => quotas.for_subquota(subquota),
            Self::Shared { slots, .. } => {
                if let RequestSubquota::Shared = subquota {
                    Some(slots)
                } else {
                    None
                }
            }
        }
    }

    /// Free slots across the whole partition.
    fn available(&self) -> usize {
        match self {
            Self::Management(quotas) => quotas.available(),
            Self::Replication(quotas) => quotas.available(),
            Self::Bulk(quotas) => quotas.available(),
            Self::Shared { slots, .. } => slots.available_permits(),
        }
    }

    async fn drain(&self) {
        match self {
            Self::Management(quotas) => quotas.drain().await,
            Self::Replication(quotas) => quotas.drain().await,
            Self::Bulk(quotas) => quotas.drain().await,
            Self::Shared { capacity, slots } => {
                let permits: u32 = (*capacity)
                    .try_into()
                    .assured("stream slot counts are much smaller than u32::MAX");
                let permit = StdArc::clone(slots)
                    .acquire_many_owned(permits)
                    .await
                    .assured("interconnect stream-slot semaphores are never closed");
                drop(permit);
            }
        }
    }
}

impl ManagementStreamSlotQuotas {
    fn available(&self) -> usize {
        available_permits([
            &self.shared,
            &self.discovery,
            &self.liveness,
            &self.progress,
            &self.admission,
            &self.cancellation,
            &self.terminal,
        ])
    }

    fn for_subquota(&self, subquota: RequestSubquota) -> Option<&StdArc<Semaphore>> {
        match subquota {
            RequestSubquota::Shared => Some(&self.shared),
            RequestSubquota::Discovery => Some(&self.discovery),
            RequestSubquota::Liveness => Some(&self.liveness),
            RequestSubquota::Progress => Some(&self.progress),
            RequestSubquota::Admission => Some(&self.admission),
            RequestSubquota::Cancellation => Some(&self.cancellation),
            RequestSubquota::Terminal => Some(&self.terminal),
            RequestSubquota::Append | RequestSubquota::Resource | RequestSubquota::Snapshot => None,
        }
    }

    async fn drain(&self) {
        let quotas = [
            (RequestSubquota::Shared, self.shared_capacity),
            (RequestSubquota::Discovery, MANAGEMENT_DISCOVERY_STREAMS),
            (RequestSubquota::Liveness, MANAGEMENT_LIVENESS_STREAMS),
            (RequestSubquota::Progress, MANAGEMENT_PROGRESS_STREAMS),
            (RequestSubquota::Admission, MANAGEMENT_ADMISSION_STREAMS),
            (
                RequestSubquota::Cancellation,
                MANAGEMENT_CANCELLATION_STREAMS,
            ),
            (RequestSubquota::Terminal, MANAGEMENT_TERMINAL_STREAMS),
        ];
        let mut drained = Vec::with_capacity(quotas.len());
        for (subquota, permits) in quotas {
            nervix_primitives::task::consume_budget().await;
            let permits: u32 = permits
                .try_into()
                .assured("management stream subquotas are much smaller than u32::MAX");
            let quota = self
                .for_subquota(subquota)
                .assured("the management drain list names only management subquotas");
            let permit = StdArc::clone(quota)
                .acquire_many_owned(permits)
                .await
                .assured("interconnect stream-slot semaphores are never closed");
            drained.push(permit);
        }
    }
}

impl ReplicationStreamSlotQuotas {
    fn available(&self) -> usize {
        available_permits([&self.shared, &self.append])
    }

    fn for_subquota(&self, subquota: RequestSubquota) -> Option<&StdArc<Semaphore>> {
        match subquota {
            RequestSubquota::Shared => Some(&self.shared),
            RequestSubquota::Append => Some(&self.append),
            RequestSubquota::Resource
            | RequestSubquota::Snapshot
            | RequestSubquota::Discovery
            | RequestSubquota::Liveness
            | RequestSubquota::Progress
            | RequestSubquota::Admission
            | RequestSubquota::Cancellation
            | RequestSubquota::Terminal => None,
        }
    }

    async fn drain(&self) {
        let quotas = [
            (RequestSubquota::Shared, self.shared_capacity),
            (RequestSubquota::Append, REPLICATION_APPEND_STREAMS),
        ];
        let mut drained = Vec::with_capacity(quotas.len());
        for (subquota, permits) in quotas {
            nervix_primitives::task::consume_budget().await;
            let permits: u32 = permits
                .try_into()
                .assured("replication stream subquotas are much smaller than u32::MAX");
            let quota = self
                .for_subquota(subquota)
                .assured("the replication drain list names only replication subquotas");
            let permit = StdArc::clone(quota)
                .acquire_many_owned(permits)
                .await
                .assured("interconnect stream-slot semaphores are never closed");
            drained.push(permit);
        }
    }
}

impl BulkStreamSlotQuotas {
    fn available(&self) -> usize {
        available_permits([&self.shared, &self.resource, &self.snapshot])
    }

    fn for_subquota(&self, subquota: RequestSubquota) -> Option<&StdArc<Semaphore>> {
        match subquota {
            RequestSubquota::Shared => Some(&self.shared),
            RequestSubquota::Resource => Some(&self.resource),
            RequestSubquota::Snapshot => Some(&self.snapshot),
            RequestSubquota::Append
            | RequestSubquota::Discovery
            | RequestSubquota::Liveness
            | RequestSubquota::Progress
            | RequestSubquota::Admission
            | RequestSubquota::Cancellation
            | RequestSubquota::Terminal => None,
        }
    }

    async fn drain(&self) {
        let quotas = [
            (RequestSubquota::Shared, self.shared_capacity),
            (RequestSubquota::Resource, BULK_RESOURCE_STREAMS),
            (RequestSubquota::Snapshot, BULK_SNAPSHOT_STREAMS),
        ];
        let mut drained = Vec::with_capacity(quotas.len());
        for (subquota, permits) in quotas {
            nervix_primitives::task::consume_budget().await;
            let permits: u32 = permits
                .try_into()
                .assured("bulk stream subquotas are much smaller than u32::MAX");
            let quota = self
                .for_subquota(subquota)
                .assured("the bulk drain list names only bulk subquotas");
            let permit = StdArc::clone(quota)
                .acquire_many_owned(permits)
                .await
                .assured("interconnect stream-slot semaphores are never closed");
            drained.push(permit);
        }
    }
}

/// Free slots across one connection's whole stream partition.
fn available_permits<const QUOTAS: usize>(quotas: [&StdArc<Semaphore>; QUOTAS]) -> usize {
    let mut available = 0;
    for quota in quotas {
        available = super::increment(available, quota.available_permits());
    }
    available
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_checks;

pub(super) fn configure_client_builder(
    builder: &mut client::Builder,
    state: &super::TransportState,
    class: PoolClass,
) -> Result<(), Report<TransportError>> {
    let stream_slots = class.stream_slots_per_connection();
    let streams = u32::try_from(stream_slots).map_err(|_| TransportError::InvalidOptions {
        reason: "pool stream slots exceed the HTTP/2 setting width".to_string(),
    })?;
    let windows = state.receive_windows(class);
    builder
        .initial_window_size(windows.stream)
        .initial_connection_window_size(windows.connection)
        .max_frame_size(
            u32::try_from(BODY_CHUNK_BYTES).assured("the default HTTP/2 frame fits u32"),
        )
        .max_header_list_size(state.options.max_header_bytes)
        .max_concurrent_streams(streams)
        .initial_max_send_streams(stream_slots)
        .max_local_error_reset_streams(Some(RESET_LIMIT))
        .max_pending_accept_reset_streams(RESET_LIMIT)
        .max_send_buffer_size(BODY_CHUNK_BYTES);
    Ok(())
}

pub(super) fn configure_server_builder(
    builder: &mut server::Builder,
    options: &TransportOptions,
) -> Result<(), Report<TransportError>> {
    let streams =
        u32::try_from(PoolClass::Management.stream_slots_per_connection()).map_err(|_| {
            TransportError::InvalidOptions {
                reason: "pool stream slots exceed the HTTP/2 setting width".to_string(),
            }
        })?;
    builder
        .initial_window_size(options.initial_stream_window_bytes)
        .initial_connection_window_size(options.initial_connection_window_bytes)
        .max_frame_size(
            u32::try_from(BODY_CHUNK_BYTES).assured("the default HTTP/2 frame fits u32"),
        )
        .max_header_list_size(options.max_header_bytes)
        .max_concurrent_streams(streams)
        .max_local_error_reset_streams(Some(RESET_LIMIT))
        .max_pending_accept_reset_streams(RESET_LIMIT)
        .max_send_buffer_size(BODY_CHUNK_BYTES);
    Ok(())
}

#[cfg(all(
    test,
    not(any(feature = "shuttle", feature = "loom", feature = "turmoil"))
))]
mod negotiation_tests {
    use super::*;

    #[nervix_primitives::test]
    async fn a_negotiated_partition_retains_reserves_and_drains_its_actual_capacity() {
        let mut quotas = StreamSlotQuotas::new(PoolClass::Management);
        quotas
            .negotiate(PoolClass::Management, MANAGEMENT_RESERVED_STREAMS + 2)
            .assured("the advertised streams hold every reserve and two shared requests");
        let first = quotas
            .try_lease(RequestSubquota::Shared)
            .assured("first shared slot");
        let second = quotas
            .try_lease(RequestSubquota::Shared)
            .assured("second shared slot");
        assert!(quotas.try_lease(RequestSubquota::Shared).is_none());
        let liveness = quotas
            .try_lease(RequestSubquota::Liveness)
            .assured("full shared capacity preserves liveness");
        let cancellation = quotas
            .try_lease(RequestSubquota::Cancellation)
            .assured("full shared capacity preserves cancellation");
        assert_eq!(quotas.leased(), 4);
        let drain = quotas.drain();
        tokio::pin!(drain);
        assert!(futures_util::poll!(&mut drain).is_pending());
        assert!(quotas.try_lease(RequestSubquota::Terminal).is_none());
        drop((first, second, liveness, cancellation));
        assert!(futures_util::poll!(&mut drain).is_ready());
        assert!(quotas.try_lease(RequestSubquota::Shared).is_none());
    }

    #[test]
    fn a_peer_cannot_consume_the_minimum_reserved_partition() {
        for (class, minimum) in [
            (PoolClass::Management, MANAGEMENT_RESERVED_STREAMS + 1),
            (PoolClass::Replication, REPLICATION_APPEND_STREAMS + 1),
            (PoolClass::Bulk, BULK_RESERVED_STREAMS + 1),
        ] {
            let mut quotas = StreamSlotQuotas::new(class);
            let refusal = quotas
                .negotiate(class, minimum - 1)
                .err()
                .assured("the peer's capacity cannot preserve every traffic class");
            assert!(
                matches!(refusal.current_context(), TransportError::Http2StreamCapacity { minimum: actual, .. } if *actual == minimum)
            );
            quotas
                .negotiate(class, minimum)
                .assured("the declared minimum leaves shared capacity");
            assert!(quotas.try_lease(RequestSubquota::Shared).is_some());
        }
    }
}
