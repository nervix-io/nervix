//! A producer a host holds: `nx_producer`, opened on a session's client ingestor.
//!
//! - **Owns.** Opening a producer with the fields and credit a host asks for, submitting the
//!   batches a host built or wrote as canonical Arrow IPC, waiting for and taking their outcomes,
//!   listing and releasing the submissions it holds, reading its description, admission and state,
//!   and closing and releasing it.
//! - **Depends on.** The Rust client's producer, the session it was opened on, the batch, field
//!   and submission outcome handles, and the endpoint vocabulary.
//! - **Must not know.** How a batch is admitted, sent again or restored; the Rust producer decides
//!   all of it, so this module keeps no state of its own about a submission or an attachment.

use std::{
    num::{NonZeroU32, NonZeroU64},
    time::Duration,
};

use bytes::Bytes;
use meticulous::ResultExt as _;
use nervix_client_core::{
    ClientProducerAdmission, ClientProducerLimits, DomainName, IngestorName, ProducerBatch,
    ProducerEnd, SubmissionId,
};
use triomphe::Arc;

use crate::{
    abi,
    batch::Batch,
    cancel::Cancel,
    endpoint::{EndpointState, OpenRefusal, Reopen, ReopenReason, Window, WindowKind},
    failure::Failure,
    fields::Fields,
    schema::Schema,
    session::{Session, SessionRuntime},
    submission::SubmissionOutcome,
};

/// Whether a producer's batches are admitted, with the header's values.
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

/// The credit one endpoint asks for: how many batches and bytes may be outstanding at once.
pub(crate) fn credit(batches: u32, bytes: u64) -> Result<(NonZeroU32, NonZeroU64), Failure> {
    let Some(batches) = NonZeroU32::new(batches) else {
        return Err(Failure::invalid_argument("batches", "must be positive"));
    };
    let Some(bytes) = NonZeroU64::new(bytes) else {
        return Err(Failure::invalid_argument("bytes", "must be positive"));
    };
    Ok((batches, bytes))
}

/// A duration the wire carried, in nanoseconds.
pub(crate) fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos())
        .verified("the wire carries every endpoint duration as u64 nanoseconds")
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
    fn free(self) {
        let Self {
            producer,
            schema,
            session,
        } = self;
        drop(schema);
        session.release(producer);
    }
}

/// # Safety
///
/// `session` is a live session; `domain` and `ingestor` address their lengths in readable bytes;
/// `fields` is a live field list; a non-null `cancel` is a live token; a non-null `out` is
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_session_open_ingestor(
    session: *const Session,
    domain: *const u8,
    domain_len: usize,
    ingestor: *const u8,
    ingestor_len: usize,
    fields: *const Fields,
    batches: u32,
    bytes: u64,
    cancel: *const Cancel,
    out: *mut *mut Producer,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe {
        write_opened(
            session,
            domain,
            domain_len,
            ingestor,
            ingestor_len,
            fields,
            batches,
            bytes,
            cancel,
            out,
        )
    })
}

/// # Safety
///
/// As [`nx_session_open_ingestor`].
#[expect(
    clippy::too_many_arguments,
    reason = "the C ABI passes each string as a pointer and a length"
)]
unsafe fn write_opened(
    session: *const Session,
    domain: *const u8,
    domain_len: usize,
    ingestor: *const u8,
    ingestor_len: usize,
    fields: *const Fields,
    batches: u32,
    bytes: u64,
    cancel: *const Cancel,
    out: *mut *mut Producer,
) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live session, readable names, live fields and a live token
    // or null.
    let (session, domain, ingestor, fields, cancel) = unsafe {
        (
            abi::handle(session, "session")?,
            abi::text(domain, domain_len, "domain")?,
            abi::text(ingestor, ingestor_len, "ingestor")?,
            abi::handle(fields, "fields")?,
            cancel.as_ref(),
        )
    };
    let (batches, bytes) = credit(batches, bytes)?;
    let limits = ClientProducerLimits { batches, bytes };
    let producer = Producer::open(session, domain, ingestor, fields, limits, cancel)?;
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(producer)) };
    Ok(())
}

/// # Safety
///
/// `producer` is a live producer; a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_producer_schema(
    producer: *const Producer,
    out: *mut *mut Schema,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_schema(producer, out) })
}

/// # Safety
///
/// As [`nx_producer_schema`].
unsafe fn write_schema(producer: *const Producer, out: *mut *mut Schema) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live producer.
    let schema = unsafe { abi::handle(producer, "producer") }?
        .schema()
        .clone();
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(schema)) };
    Ok(())
}

/// # Safety
///
/// `producer` is a live producer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_producer_generation(producer: *const Producer) -> u64 {
    // SAFETY: the header requires a live producer.
    unsafe { abi::accessor(producer) }
        .producer
        .description()
        .generation
}

/// # Safety
///
/// `producer` is a live producer; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_producer_contract(
    producer: *const Producer,
    digest: *mut *const u8,
    digest_len: *mut usize,
) {
    // SAFETY: the header requires a live producer and writable out-parameters.
    unsafe {
        let producer = abi::accessor(producer);
        let contract = producer.producer.description().contract.as_digest();
        abi::write_bytes(digest, digest_len, contract);
    }
}

/// # Safety
///
/// `producer` is a live producer; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_producer_grant(
    producer: *const Producer,
    batches: *mut u32,
    bytes: *mut u64,
    max_batch_rows: *mut u32,
    max_batch_bytes: *mut u64,
) {
    // SAFETY: the header requires a live producer and writable out-parameters.
    unsafe {
        let grant = abi::accessor(producer).producer.description().grant;
        abi::write(batches, grant.batches.get());
        abi::write(bytes, grant.bytes.get());
        abi::write(max_batch_rows, grant.max_batch_rows.get());
        abi::write(max_batch_bytes, grant.max_batch_bytes.get());
    }
}

/// # Safety
///
/// `producer` is a live producer; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_producer_policy(
    producer: *const Producer,
    window: *mut WindowKind,
    outstanding: *mut u64,
    ack_timeout_nanos: *mut u64,
    retry_backoff_nanos: *mut u64,
    retry_max_backoff_nanos: *mut u64,
) {
    // SAFETY: the header requires a live producer and writable out-parameters.
    unsafe {
        let policy = abi::accessor(producer).producer.description().policy;
        let acknowledged = Window::from(policy.window);
        abi::write(window, acknowledged.kind);
        abi::write(outstanding, acknowledged.outstanding);
        abi::write(ack_timeout_nanos, nanos(policy.ack_timeout));
        abi::write(retry_backoff_nanos, nanos(policy.retry_backoff));
        abi::write(retry_max_backoff_nanos, nanos(policy.retry_max_backoff));
    }
}

/// # Safety
///
/// `producer` is a live producer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_producer_admission(producer: *const Producer) -> Admission {
    // SAFETY: the header requires a live producer.
    unsafe { abi::accessor(producer) }.admission()
}

/// # Safety
///
/// `producer` is a live producer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_producer_state(producer: *const Producer) -> EndpointState {
    // SAFETY: the header requires a live producer.
    unsafe { abi::accessor(producer) }.state()
}

/// # Safety
///
/// `producer` is a live producer; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_producer_reopen_reason(
    producer: *const Producer,
    reason: *mut ReopenReason,
    refusal: *mut OpenRefusal,
) -> bool {
    // SAFETY: the header requires a live producer.
    let Some(reopen) = unsafe { abi::accessor(producer) }.reopen() else {
        return false;
    };
    // SAFETY: the header requires writable out-parameters.
    unsafe {
        abi::write(reason, reopen.reason());
        if let Some(refused) = reopen.refusal() {
            abi::write(refusal, refused);
        }
    }
    true
}

/// # Safety
///
/// `producer` and `batch` are live, a non-null `cancel` is a live token, and a non-null
/// `submission` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_producer_submit(
    producer: *const Producer,
    batch: *const Batch,
    cancel: *const Cancel,
    submission: *mut u64,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_submitted(producer, batch, cancel, submission) })
}

/// # Safety
///
/// As [`nx_producer_submit`].
unsafe fn write_submitted(
    producer: *const Producer,
    batch: *const Batch,
    cancel: *const Cancel,
    submission: *mut u64,
) -> Result<(), Failure> {
    abi::require_out(submission, "submission")?;
    // SAFETY: the caller guarantees a live producer and batch, and a live token or null.
    let (producer, batch, cancel) = unsafe {
        (
            abi::handle(producer, "producer")?,
            abi::handle(batch, "batch")?,
            cancel.as_ref(),
        )
    };
    let id = producer.submit(batch, cancel)?;
    // SAFETY: `submission` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(submission, id) };
    Ok(())
}

/// # Safety
///
/// `producer` is live, a non-null `ipc` addresses `ipc_len` readable bytes, a non-null `cancel`
/// is a live token, and a non-null `submission` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_producer_submit_ipc(
    producer: *const Producer,
    ipc: *const u8,
    ipc_len: usize,
    cancel: *const Cancel,
    submission: *mut u64,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_submitted_ipc(producer, ipc, ipc_len, cancel, submission) })
}

/// # Safety
///
/// As [`nx_producer_submit_ipc`].
unsafe fn write_submitted_ipc(
    producer: *const Producer,
    ipc: *const u8,
    ipc_len: usize,
    cancel: *const Cancel,
    submission: *mut u64,
) -> Result<(), Failure> {
    abi::require_out(submission, "submission")?;
    // SAFETY: the caller guarantees a live producer, a readable stream and a live token or null.
    let (producer, ipc, cancel) = unsafe {
        (
            abi::handle(producer, "producer")?,
            abi::input_slice(ipc, ipc_len, "ipc")?,
            cancel.as_ref(),
        )
    };
    let id = producer.submit_ipc(ipc, cancel)?;
    // SAFETY: `submission` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(submission, id) };
    Ok(())
}

/// # Safety
///
/// `producer` is live, a non-null `cancel` is a live token, and a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_producer_rejoin(
    producer: *const Producer,
    submission: u64,
    cancel: *const Cancel,
    out: *mut *mut SubmissionOutcome,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_rejoined(producer, submission, cancel, out) })
}

/// # Safety
///
/// As [`nx_producer_rejoin`].
unsafe fn write_rejoined(
    producer: *const Producer,
    submission: u64,
    cancel: *const Cancel,
    out: *mut *mut SubmissionOutcome,
) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live producer and a live token or null.
    let (producer, cancel) = unsafe { (abi::handle(producer, "producer")?, cancel.as_ref()) };
    let outcome = producer.rejoin(submission, cancel)?;
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(outcome)) };
    Ok(())
}

/// # Safety
///
/// `producer` is live; a non-null `submissions` and a non-null `resolved` each address
/// `capacity` writable entries; a non-null `count` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_producer_pending(
    producer: *const Producer,
    submissions: *mut u64,
    resolved: *mut bool,
    capacity: usize,
    count: *mut usize,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_pending(producer, submissions, resolved, capacity, count) })
}

/// # Safety
///
/// As [`nx_producer_pending`].
unsafe fn write_pending(
    producer: *const Producer,
    submissions: *mut u64,
    resolved: *mut bool,
    capacity: usize,
    count: *mut usize,
) -> Result<(), Failure> {
    abi::require_out(count, "count")?;
    // SAFETY: the caller guarantees a live producer.
    let producer = unsafe { abi::handle(producer, "producer") }?;
    let pending = producer.producer.pending_submissions();
    // SAFETY: `count` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(count, pending.len()) };
    let written = pending.len().min(capacity);
    if !submissions.is_null() {
        // SAFETY: `submissions` is non-null and the caller guarantees `capacity` writable entries,
        // of which `written` are written.
        let targets = unsafe { abi::output_slice(submissions, written, "submissions") }?;
        for (target, submission) in targets.iter_mut().zip(&pending) {
            *target = submission.id.get().get();
        }
    }
    if !resolved.is_null() {
        // SAFETY: `resolved` is non-null and the caller guarantees `capacity` writable entries, of
        // which `written` are written.
        let targets = unsafe { abi::output_slice(resolved, written, "resolved") }?;
        for (target, submission) in targets.iter_mut().zip(&pending) {
            *target = submission.outcome.is_some();
        }
    }
    Ok(())
}

/// # Safety
///
/// `producer` is live and a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_producer_release(
    producer: *const Producer,
    submission: u64,
    out: *mut *mut SubmissionOutcome,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_released(producer, submission, out) })
}

/// # Safety
///
/// As [`nx_producer_release`].
unsafe fn write_released(
    producer: *const Producer,
    submission: u64,
    out: *mut *mut SubmissionOutcome,
) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live producer.
    let released = unsafe { abi::handle(producer, "producer") }?.release(submission)?;
    let handle = match released {
        Some(outcome) => abi::into_handle(outcome),
        None => std::ptr::null_mut(),
    };
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, handle) };
    Ok(())
}

/// # Safety
///
/// `producer` is live and a non-null `cancel` is a live token.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_producer_close(
    producer: *const Producer,
    cancel: *const Cancel,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { closed(producer, cancel) })
}

/// # Safety
///
/// As [`nx_producer_close`].
unsafe fn closed(producer: *const Producer, cancel: *const Cancel) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live producer and a live token or null.
    let (producer, cancel) = unsafe { (abi::handle(producer, "producer")?, cancel.as_ref()) };
    producer.close(cancel)
}

/// # Safety
///
/// A non-null `producer` is a producer this library returned that has not been freed, and no
/// other thread is using it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_producer_free(producer: *mut Producer) {
    if producer.is_null() {
        return;
    }
    // SAFETY: the header requires an unreleased producer no other thread uses.
    let producer = unsafe { Box::from_raw(producer) };
    producer.free();
}
