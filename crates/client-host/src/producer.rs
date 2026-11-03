//! A producer a host holds, opened on a session's client ingestor.
//!
//! - **Owns.** Opening a producer with the fields and credit a host asks for, submitting the
//!   batches a host built or wrote as canonical Arrow IPC, waiting for and taking their outcomes,
//!   listing and releasing the submissions it holds, reading its description, admission and state,
//!   and closing and releasing it.
//! - **Depends on.** The Rust client's producer, the session it was opened on, the batches, field
//!   descriptions and submission outcomes, and the endpoint vocabulary.
//! - **Must not know.** How a batch is admitted, sent again or restored; the Rust producer decides
//!   all of it, so this module keeps no state of its own about a submission or an attachment.

use std::num::NonZeroU64;

use bytes::Bytes;
use nervix_client_core::{
    ClientProducerAdmission, ClientProducerDescription, ClientProducerLimits, DomainName,
    IngestorName, PendingSubmission, ProducerBatch, ProducerEnd, SubmissionId,
};
use nervix_primitives::sync::Arc;

use crate::{
    batch::Batch,
    cancel::Cancel,
    endpoint::{EndpointState, Reopen},
    failure::Failure,
    fields::Fields,
    schema::Schema,
    session::{Session, SessionRuntime},
    submission::SubmissionOutcome,
};

/// Whether a producer's batches are admitted, with its stable values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Admission {
    Open = 1,
    Suspended = 2,
}

impl From<ClientProducerAdmission> for Admission {
    fn from(admission: ClientProducerAdmission) -> Self {
        match admission {
            ClientProducerAdmission::Open => Self::Open,
            ClientProducerAdmission::Suspended => Self::Suspended,
        }
    }
}

/// A producer, with the session its blocking calls run on and the schema of its batches.
pub struct Producer {
    producer: nervix_client_core::Producer,
    schema: Schema,
    session: Arc<SessionRuntime>,
}

impl Producer {
    /// Opens a producer on `ingestor` of `domain`, blocking until the server answers.
    pub fn open(
        session: &Session,
        domain: &str,
        ingestor: &str,
        fields: &Fields,
        limits: ClientProducerLimits,
        cancel: Option<&Cancel>,
    ) -> Result<Self, Failure> {
        let domain = match DomainName::try_from(domain) {
            Ok(domain) => domain,
            Err(error) => return Err(Failure::invalid_argument("domain", &error.to_string())),
        };
        let ingestor = match IngestorName::try_from(ingestor) {
            Ok(ingestor) => ingestor,
            Err(error) => return Err(Failure::invalid_argument("ingestor", &error.to_string())),
        };
        let expected = fields.schema_fields()?;
        let shared = session.shared().clone();
        let opening = async {
            let opened = shared
                .client()
                .open_ingestor(domain, ingestor, expected, limits)
                .await;
            opened.map_err(Failure::from)
        };
        let producer = shared.block_on(cancel, opening)?;
        let schema = Schema::of_fields(producer.description().fields.clone());
        Ok(Self {
            producer,
            schema,
            session: shared,
        })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// The producer the server opened: its fields, contract, generation, grant and policy.
    pub fn description(&self) -> &ClientProducerDescription {
        self.producer.description()
    }

    /// The submissions the producer holds, each with its outcome once it is known.
    pub fn pending_submissions(&self) -> Vec<PendingSubmission> {
        self.producer.pending_submissions()
    }

    pub fn state(&self) -> EndpointState {
        EndpointState::from(self.producer.connection())
    }

    pub fn admission(&self) -> Admission {
        Admission::from(self.producer.admission())
    }

    /// Why the producer has to be opened again, once it does.
    pub fn reopen(&self) -> Option<Reopen> {
        match self.producer.end() {
            Some(ProducerEnd::ReopenRequired(reason)) => Some(Reopen::from(&reason)),
            Some(ProducerEnd::Closed | ProducerEnd::Ended { .. } | ProducerEnd::SessionLost)
            | None => None,
        }
    }

    /// Waits for credit and submits a batch built for this producer's schema.
    pub fn submit(&self, batch: &Batch, cancel: Option<&Cancel>) -> Result<u64, Failure> {
        let submitting = async {
            let encoded = self
                .producer
                .batch(batch.record_batch())
                .map_err(Failure::from)?;
            self.producer.submit(encoded).await.map_err(Failure::from)
        };
        let id = self.session.block_on(cancel, submitting)?;
        Ok(id.get().get())
    }

    /// Waits for credit and submits a batch a host wrote as one canonical Arrow IPC stream. The
    /// stream is copied before anything waits.
    pub fn submit_ipc(&self, ipc: &[u8], cancel: Option<&Cancel>) -> Result<u64, Failure> {
        let batch = ProducerBatch::from_arrow_ipc(Bytes::copy_from_slice(ipc));
        let submitting = async { self.producer.submit(batch).await.map_err(Failure::from) };
        let id = self.session.block_on(cancel, submitting)?;
        Ok(id.get().get())
    }

    fn submission(id: u64) -> Result<SubmissionId, Failure> {
        let Some(id) = NonZeroU64::new(id) else {
            return Err(Failure::invalid_argument(
                "submission",
                "is zero, which no submission is",
            ));
        };
        Ok(SubmissionId::from(id))
    }

    /// Waits for a submission's outcome and takes it, which returns its credit.
    pub fn rejoin(&self, id: u64, cancel: Option<&Cancel>) -> Result<SubmissionOutcome, Failure> {
        let id = Self::submission(id)?;
        let rejoining = async { self.producer.rejoin(id).await.map_err(Failure::from) };
        let outcome = self.session.block_on(cancel, rejoining)?;
        Ok(SubmissionOutcome::new(outcome))
    }

    /// Lets go of a submission, returning its outcome when it has one already.
    pub fn release(&self, id: u64) -> Result<Option<SubmissionOutcome>, Failure> {
        let id = Self::submission(id)?;
        let released = self.producer.release(id).map_err(Failure::from)?;
        match released {
            Some(outcome) => Ok(Some(SubmissionOutcome::new(outcome))),
            None => Ok(None),
        }
    }

    /// Stops the producer's admission and waits until the server released it.
    pub fn close(&self, cancel: Option<&Cancel>) -> Result<(), Failure> {
        let closing = async { self.producer.close().await.map_err(Failure::from) };
        self.session.block_on(cancel, closing)
    }

    /// Releases the producer, closing it without waiting when it is still open.
    pub fn free(self) {
        let Self {
            producer,
            schema,
            session,
        } = self;
        drop(schema);
        session.release(producer);
    }
}
