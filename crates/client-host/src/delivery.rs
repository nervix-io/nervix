//! One delivery of a client emitter's output, as a host reads it.
//!
//! - **Owns.** The settlements a host reads, a delivery's identity, reference and metadata, its
//!   canonical Arrow IPC stream, the batch decoded from it, and acknowledging, retrying or
//!   rejecting it.
//! - **Depends on.** The Rust client's emitter delivery, the session it came from, and the batch.
//! - **Must not know.** Which attachment may settle it; the Rust delivery decides that, and a
//!   reference that expired with its attachment is refused there.
//!
//! Releasing a delivery settles nothing: an unsettled attempt stays with its consumer until the
//! emitter's ACK timeout or its attachment ends, and is then delivered again.

use meticulous::OptionExt as _;
use nervix_client_core::{EmitterDelivery, EmitterSettlement};
use nervix_primitives::sync::{Arc, blocking::OnceLock};
use nervix_recovery::Discarded as _;

use crate::{
    batch::Batch, cancel::Cancel, failure::Failure, schema::Schema, session::SessionRuntime,
};

/// What the server did with a settlement, with its stable values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Settlement {
    Confirmed = 1,
    StaleReference = 2,
    WrongConsumer = 3,
    InvalidReason = 4,
    ConsumerEnded = 5,
}

impl From<EmitterSettlement> for Settlement {
    fn from(settlement: EmitterSettlement) -> Self {
        match settlement {
            EmitterSettlement::Confirmed => Self::Confirmed,
            EmitterSettlement::StaleReference => Self::StaleReference,
            EmitterSettlement::WrongConsumer => Self::WrongConsumer,
            EmitterSettlement::InvalidReason => Self::InvalidReason,
            EmitterSettlement::ConsumerEnded => Self::ConsumerEnded,
        }
    }
}

/// One delivered attempt, shared by every reference a host holds.
pub struct Delivery {
    delivery: EmitterDelivery,
    schema: Schema,
    session: Arc<SessionRuntime>,
    /// The batch decoded from the attempt the first time a host reads it.
    batch: OnceLock<Result<Arc<Batch>, Failure>>,
}

impl Delivery {
    pub(crate) fn new(
        delivery: EmitterDelivery,
        schema: Schema,
        session: Arc<SessionRuntime>,
    ) -> Self {
        Self {
            delivery,
            schema,
            session,
            batch: OnceLock::new(),
        }
    }

    pub fn identity(&self) -> &[u8] {
        self.delivery.identity.as_bytes()
    }

    pub fn reference(&self) -> &[u8] {
        self.delivery.reference.as_bytes()
    }

    pub fn source_relay(&self) -> &str {
        self.delivery.source_relay.as_str()
    }

    pub fn branch_fingerprint(&self) -> Option<&[u8; 32]> {
        self.delivery.branch_fingerprint.as_ref()
    }

    pub fn members(&self) -> u32 {
        self.delivery.members
    }

    pub fn execution_now(&self) -> i64 {
        self.delivery.execution_now.unix_nanos()
    }

    pub fn ipc(&self) -> &[u8] {
        &self.delivery.batch
    }

    /// The batch the attempt carries, decoded the first time it is read and held to its
    /// consumer's schema and its member count. Every reader shares the batch that was kept.
    pub fn batch(&self) -> Result<&Arc<Batch>, Failure> {
        let decoded = match self.batch.get() {
            Some(decoded) => decoded,
            None => {
                let batch = match Batch::delivered(&self.delivery, self.schema.clone()) {
                    Ok(batch) => Ok(Arc::new(batch)),
                    Err(failure) => Err(failure),
                };
                self.batch.set(batch).discarded(
                    "a reader that decoded the same delivery concurrently kept its batch first",
                );
                self.batch
                    .get()
                    .verified("this reader or a concurrent one kept the batch just above")
            }
        };
        match decoded {
            Ok(batch) => Ok(batch),
            Err(failure) => Err(failure.clone()),
        }
    }

    pub fn ack(&self, cancel: Option<&Cancel>) -> Result<Settlement, Failure> {
        let settling = async { self.delivery.ack().await.map_err(Failure::from) };
        Ok(Settlement::from(self.session.block_on(cancel, settling)?))
    }

    pub fn retry(&self, cancel: Option<&Cancel>) -> Result<Settlement, Failure> {
        let settling = async { self.delivery.retry().await.map_err(Failure::from) };
        Ok(Settlement::from(self.session.block_on(cancel, settling)?))
    }

    pub fn reject(&self, reason: &str, cancel: Option<&Cancel>) -> Result<Settlement, Failure> {
        let settling = async { self.delivery.reject(reason).await.map_err(Failure::from) };
        Ok(Settlement::from(self.session.block_on(cancel, settling)?))
    }
}
