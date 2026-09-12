//! How one connection's HTTP/2 stream capacity is partitioned between its reserved classes.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The stream-slot partition each traffic class runs under, the reserved subquotas
//!   inside the management, replication and bulk partitions, and the drain that takes every slot
//!   back when a connection retires.
//! - **Depends on.** The traffic classes and the reserved request subquotas they are divided by.
//! - **Must not know.** Connection lifetime, request dispatch, or what any message means.
//!
//! Every partition is checked against its class capacity at compile time, so a reserved class can
//! never be given capacity the connection does not have and the shared remainder is always what
//! the reservations leave behind.

use super::*;

/// Tokio's owned permits retain a `std::sync::Arc` to their one semaphore after the connection
/// quota bundle is no longer borrowed. The surrounding `triomphe::Arc` keeps cloning a complete
/// quota bundle to one reference-count operation.
pub(super) struct ManagementStreamSlotQuotas {
    shared: StdArc<Semaphore>,
    discovery: StdArc<Semaphore>,
    liveness: StdArc<Semaphore>,
    progress: StdArc<Semaphore>,
    admission: StdArc<Semaphore>,
    cancellation: StdArc<Semaphore>,
    terminal: StdArc<Semaphore>,
}

pub(super) struct BulkStreamSlotQuotas {
    shared: StdArc<Semaphore>,
    resource: StdArc<Semaphore>,
    snapshot: StdArc<Semaphore>,
}

/// The one ordered append stream a leader keeps open to a follower reserves its own slot, so the
/// ownership handoff requests that share this pool are never held behind it.
pub(super) struct ReplicationStreamSlotQuotas {
    shared: StdArc<Semaphore>,
    append: StdArc<Semaphore>,
}

#[derive(Clone)]
pub(super) enum StreamSlotQuotas {
    Management(Arc<ManagementStreamSlotQuotas>),
    Replication(Arc<ReplicationStreamSlotQuotas>),
    Bulk(Arc<BulkStreamSlotQuotas>),
    Shared {
        class: PoolClass,
        slots: StdArc<Semaphore>,
    },
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
        if class == PoolClass::Management {
            return Self::Management(Arc::new(ManagementStreamSlotQuotas {
                shared: StdArc::new(Semaphore::new(MANAGEMENT_SHARED_STREAMS)),
                discovery: StdArc::new(Semaphore::new(MANAGEMENT_DISCOVERY_STREAMS)),
                liveness: StdArc::new(Semaphore::new(MANAGEMENT_LIVENESS_STREAMS)),
                progress: StdArc::new(Semaphore::new(MANAGEMENT_PROGRESS_STREAMS)),
                admission: StdArc::new(Semaphore::new(MANAGEMENT_ADMISSION_STREAMS)),
                cancellation: StdArc::new(Semaphore::new(MANAGEMENT_CANCELLATION_STREAMS)),
                terminal: StdArc::new(Semaphore::new(MANAGEMENT_TERMINAL_STREAMS)),
            }));
        }
        if class == PoolClass::Replication {
            return Self::Replication(Arc::new(ReplicationStreamSlotQuotas {
                shared: StdArc::new(Semaphore::new(REPLICATION_SHARED_STREAMS)),
                append: StdArc::new(Semaphore::new(REPLICATION_APPEND_STREAMS)),
            }));
        }
        if class == PoolClass::Bulk {
            return Self::Bulk(Arc::new(BulkStreamSlotQuotas {
                shared: StdArc::new(Semaphore::new(BULK_SHARED_STREAMS)),
                resource: StdArc::new(Semaphore::new(BULK_RESOURCE_STREAMS)),
                snapshot: StdArc::new(Semaphore::new(BULK_SNAPSHOT_STREAMS)),
            }));
        }
        Self::Shared {
            class,
            slots: StdArc::new(Semaphore::new(class.stream_slots_per_connection())),
        }
    }

    pub(super) fn for_subquota(&self, subquota: RequestSubquota) -> Option<&StdArc<Semaphore>> {
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

    pub(super) async fn drain(&self) {
        match self {
            Self::Management(quotas) => quotas.drain().await,
            Self::Replication(quotas) => quotas.drain().await,
            Self::Bulk(quotas) => quotas.drain().await,
            Self::Shared { class, slots } => {
                let permits: u32 = class
                    .stream_slots_per_connection()
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
            (RequestSubquota::Shared, MANAGEMENT_SHARED_STREAMS),
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
            tokio::task::consume_budget().await;
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
            (RequestSubquota::Shared, REPLICATION_SHARED_STREAMS),
            (RequestSubquota::Append, REPLICATION_APPEND_STREAMS),
        ];
        let mut drained = Vec::with_capacity(quotas.len());
        for (subquota, permits) in quotas {
            tokio::task::consume_budget().await;
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
            (RequestSubquota::Shared, BULK_SHARED_STREAMS),
            (RequestSubquota::Resource, BULK_RESOURCE_STREAMS),
            (RequestSubquota::Snapshot, BULK_SNAPSHOT_STREAMS),
        ];
        let mut drained = Vec::with_capacity(quotas.len());
        for (subquota, permits) in quotas {
            tokio::task::consume_budget().await;
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
