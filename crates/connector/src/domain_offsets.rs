//! Domain-owned transport offsets, as the ingestor that reads through them sees the host's state.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The position a domain keeps for one partition of a partitioned transport in place of
//!   the transport's own consumer-group commit, where a resumed ingestor resolves its next
//!   assignment from, and the opaque handle through which it reads, replaces and commits those
//!   positions.
//! - **Depends on.** The vocabulary's timestamps and external partition assignments, `error-stack`,
//!   and shared ownership.
//! - **Must not know.** How the host stores, replicates, places or schedules offset state, or any
//!   driver.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use async_trait::async_trait;
use error_stack::Report;
use nervix_models::{ExternalPartitionAssignment, Timestamp};
use nervix_primitives::sync::Arc;
use thiserror::Error;

/// The next unread offset for one partition of a transport topic.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DomainOffsetPosition {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// Where a domain-offset ingestor resolves its next assignment from.
pub enum DomainOffsetStart {
    Resume {
        positions: Vec<DomainOffsetPosition>,
        missing_partition_timestamp: Option<Timestamp>,
    },
    At(Timestamp),
}

/// Host-owned state needed to initialize one domain-offset ingestor instance.
pub struct DomainOffsetInitialization {
    pub generation: u64,
    pub start: DomainOffsetStart,
    pub assignment: Option<ExternalPartitionAssignment>,
}

/// Why host-owned offset state could not serve the ingestor connector that reads `transport`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DomainOffsetError {
    #[error("failed to read host-owned {transport} offset state")]
    Read { transport: &'static str },
    #[error("failed to replace host-owned {transport} offsets")]
    Reset { transport: &'static str },
    #[error("failed to commit a host-owned {transport} offset")]
    Commit { transport: &'static str },
}

pub type DomainOffsetResult<T> = Result<T, Report<DomainOffsetError>>;

/// The host's implementation of domain-owned offsets for one ingestor.
#[async_trait]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the retained host service is invoked by each admitted driver operation"
    )
)]
pub trait DomainOffsetServices: Send + Sync + 'static {
    fn generation(&self) -> Option<u64>;

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "a resumed driver resolves its initial assigned offsets before consuming \
                      records"
        )
    )]
    async fn initialization(
        &self,
        partitions: &[i32],
    ) -> DomainOffsetResult<DomainOffsetInitialization>;

    /// Accepts the explicit generation start through persistence and its replica boundary.
    /// An already accepted start retains its canonical positions. Subsequent initialization of
    /// that same generation returns `Resume` with those recorded positions.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "an explicit start is selected and made durable before the driver polls \
                      records; normal acknowledgements call commit instead"
        )
    )]
    async fn reset(
        &self,
        generation: u64,
        positions: Vec<DomainOffsetPosition>,
    ) -> DomainOffsetResult<()>;

    async fn commit(&self, position: DomainOffsetPosition) -> DomainOffsetResult<()>;
}

struct DomainOffsetsInner {
    services: Box<dyn DomainOffsetServices>,
}

/// Opaque access to host-owned replicated offset state.
#[derive(Clone)]
pub struct DomainOffsets {
    inner: Arc<DomainOffsetsInner>,
}

impl DomainOffsets {
    pub fn new(services: impl DomainOffsetServices) -> Self {
        Self {
            inner: Arc::new(DomainOffsetsInner {
                services: Box::new(services),
            }),
        }
    }

    pub fn generation(&self) -> Option<u64> {
        self.inner.services.generation()
    }

    pub async fn initialization(
        &self,
        partitions: &[i32],
    ) -> DomainOffsetResult<DomainOffsetInitialization> {
        nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "a resume installs its exact domain-offset placement before polling; the ingestor \
             retains the resulting position",
            self.inner.services.initialization(partitions)
        )
        .await
    }

    pub async fn reset(
        &self,
        generation: u64,
        positions: Vec<DomainOffsetPosition>,
    ) -> DomainOffsetResult<()> {
        nervix_primitives::expect_lint!(
            nervix::lifecycle_call,
            "initialization selects one explicit starting cut before polling; recurring record \
             acknowledgements call commit instead",
            self.inner.services.reset(generation, positions)
        )
        .await
    }

    pub async fn commit(&self, position: DomainOffsetPosition) -> DomainOffsetResult<()> {
        self.inner.services.commit(position).await
    }
}
