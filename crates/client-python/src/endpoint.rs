//! Producers and consumers of client endpoints as a Python host holds them, the deliveries a
//! consumer hands out, and what became of a submitted batch.
//!
//! - **Owns.** Opening a producer or a consumer with the fields and credit a host asks for,
//!   submitting a batch a host built, wrote as Arrow IPC or brought as Arrow data, taking and
//!   releasing outcomes, reading descriptions and states, waiting for deliveries, settling them,
//!   and closing an endpoint.
//! - **Depends on.** The host session runtime's producer, consumer, delivery and submission
//!   outcome, the session, field, batch and token classes, and the Arrow exchange.
//! - **Must not know.** How a batch is admitted, sent again or restored, or how output is
//!   assigned or redelivered; the Rust client decides all of it.
//!
//! An endpoint keeps its session running until it is released, and a delivery its consumer's.
//! Producers, consumers and deliveries are context managers: leaving the `with` block releases the
//! object as collecting it does, a producer or consumer is then closed without waiting, and every
//! later call raises `ClosedError`. A call another thread is in keeps its own reference until it
//! returns, so a release never waits for it. Releasing or collecting a delivery settles nothing: an
//! unsettled attempt is delivered again once the emitter's ACK timeout passes or its attachment
//! ends.

use std::{borrow::Cow, ffi::c_int, time::Duration};

use nervix_client_core::{
    AckWindow, ClientConsumerLimits, ClientProducerLimits, PendingSubmission as HeldSubmission,
};
use nervix_client_host as host;
use nervix_primitives::sync::Arc;
use pyo3::{
    ffi,
    prelude::*,
    types::{PyBytes, PyMemoryView},
};

use crate::{
    arrow,
    batch::Batch,
    enums::{
        Admission, BatchDefect, EndpointState, OpenRefusal, ProcessingFailure, ReopenReason,
        Settlement, SubmissionRefusal, SubmissionResult, Uncertainty, WindowKind,
    },
    errors,
    schema::{Fields, Schema},
    session::{Cancel, token},
    values,
};

/// The credit an endpoint was granted, and the largest batch it takes.
#[pyclass(frozen, eq, module = "nervix_client")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Grant {
    batches: u32,
    bytes: u64,
    max_batch_rows: u32,
    max_batch_bytes: u64,
}

#[pymethods]
impl Grant {
    /// How many batches may be outstanding at once.
    #[getter]
    fn batches(&self) -> u32 {
        self.batches
    }

    /// How many bytes may be outstanding at once.
    #[getter]
    fn bytes(&self) -> u64 {
        self.bytes
    }

    #[getter]
    fn max_batch_rows(&self) -> u32 {
        self.max_batch_rows
    }

    #[getter]
    fn max_batch_bytes(&self) -> u64 {
        self.max_batch_bytes
    }
}

/// How an endpoint's batches are acknowledged and retried. Every duration is in nanoseconds.
#[pyclass(frozen, eq, module = "nervix_client")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Policy {
    window: WindowKind,
    outstanding: u64,
    ack_timeout_nanos: u64,
    retry_backoff_nanos: u64,
    retry_max_backoff_nanos: u64,
}

impl Policy {
    fn of(
        window: AckWindow,
        ack_timeout: Duration,
        retry_backoff: Duration,
        retry_max_backoff: Duration,
    ) -> Self {
        let window = host::Window::from(window);
        Self {
            window: WindowKind::from(window.kind),
            outstanding: window.outstanding,
            ack_timeout_nanos: host::nanos(ack_timeout),
            retry_backoff_nanos: host::nanos(retry_backoff),
            retry_max_backoff_nanos: host::nanos(retry_max_backoff),
        }
    }
}

#[pymethods]
impl Policy {
    #[getter]
    fn window(&self) -> WindowKind {
        self.window
    }

    /// How many acknowledgements may be outstanding at once: one for a sequential window.
    #[getter]
    fn outstanding(&self) -> u64 {
        self.outstanding
    }

    #[getter]
    fn ack_timeout_nanos(&self) -> u64 {
        self.ack_timeout_nanos
    }

    #[getter]
    fn retry_backoff_nanos(&self) -> u64 {
        self.retry_backoff_nanos
    }

    #[getter]
    fn retry_max_backoff_nanos(&self) -> u64 {
        self.retry_max_backoff_nanos
    }
}

/// Why an endpoint has to be opened again, with the refusal of a restoration the server refused
/// for good.
#[pyclass(frozen, eq, module = "nervix_client")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Reopen {
    reason: ReopenReason,
    refusal: Option<OpenRefusal>,
}

impl Reopen {
    fn of(reopen: host::Reopen) -> Self {
        Self {
            reason: ReopenReason::from(reopen.reason()),
            refusal: reopen.refusal().map(OpenRefusal::from),
        }
    }
}

#[pymethods]
impl Reopen {
    #[getter]
    fn reason(&self) -> ReopenReason {
        self.reason
    }

    #[getter]
    fn refusal(&self) -> Option<OpenRefusal> {
        self.refusal
    }
}

/// A submission a producer holds, and whether its outcome is known.
#[pyclass(frozen, eq, module = "nervix_client")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingSubmission {
    id: u64,
    resolved: bool,
}

#[pymethods]
impl PendingSubmission {
    #[getter]
    fn id(&self) -> u64 {
        self.id
    }

    #[getter]
    fn resolved(&self) -> bool {
        self.resolved
    }
}

impl PendingSubmission {
    fn of(submission: &HeldSubmission) -> Self {
        Self {
            id: submission.id.get().get(),
            resolved: submission.outcome.is_some(),
        }
    }
}

/// What became of one submitted batch.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct SubmissionOutcome {
    outcome: host::SubmissionOutcome,
}

#[pymethods]
impl SubmissionOutcome {
    #[getter]
    fn result(&self) -> SubmissionResult {
        SubmissionResult::from(self.outcome.result())
    }

    /// The server's message, empty for a completed batch.
    #[getter]
    fn message(&self) -> &str {
        self.outcome.message()
    }

    /// Why a batch that was not admitted was refused.
    #[getter]
    fn refusal(&self, py: Python<'_>) -> PyResult<SubmissionRefusal> {
        match self.outcome.refusal() {
            Ok(refusal) => Ok(SubmissionRefusal::from(refusal)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// What made a batch refused as invalid invalid.
    #[getter]
    fn defect(&self, py: Python<'_>) -> PyResult<BatchDefect> {
        match self.outcome.defect() {
            Ok(defect) => Ok(BatchDefect::from(defect)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Why an admitted batch's acknowledgement failed.
    #[getter]
    fn failure(&self, py: Python<'_>) -> PyResult<ProcessingFailure> {
        match self.outcome.failure() {
            Ok(failure) => Ok(ProcessingFailure::from(failure)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Why no terminal result could be established for the batch.
    #[getter]
    fn uncertainty(&self, py: Python<'_>) -> PyResult<Uncertainty> {
        match self.outcome.uncertainty() {
            Ok(uncertainty) => Ok(Uncertainty::from(uncertainty)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }
}

/// A producer opened on a client ingestor.
#[pyclass(module = "nervix_client", weakref)]
pub(crate) struct Producer {
    /// The producer, until its `with` block releases it.
    producer: Option<Arc<host::Producer>>,
}

impl Producer {
    #[expect(
        clippy::too_many_arguments,
        reason = "opening takes the session, the endpoint, its fields, its credit and a token"
    )]
    pub(crate) fn open(
        py: Python<'_>,
        session: &host::Session,
        domain: &str,
        ingestor: &str,
        fields: &Fields,
        batches: u32,
        bytes: u64,
        cancel: Option<&host::Cancel>,
    ) -> PyResult<Self> {
        let (batches, bytes) = match host::credit(batches, bytes) {
            Ok(credit) => credit,
            Err(failure) => return Err(errors::raise(py, failure)),
        };
        let limits = ClientProducerLimits { batches, bytes };
        let fields = fields.host();
        let opened =
            py.detach(|| host::Producer::open(session, domain, ingestor, fields, limits, cancel));
        match opened {
            Ok(producer) => Ok(Self {
                producer: Some(Arc::new(producer)),
            }),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// The producer a call that does not wait reads.
    fn held(&self, py: Python<'_>) -> PyResult<&host::Producer> {
        match &self.producer {
            Some(producer) => Ok(producer),
            None => Err(errors::released(py, "producer")),
        }
    }

    /// The producer a call that waits runs on, as a reference of its own: a `with` block another
    /// thread leaves while the call waits releases the object without waiting for the call.
    fn shared(slf: &Bound<'_, Self>) -> PyResult<Arc<host::Producer>> {
        let producer = slf.try_borrow()?;
        match &producer.producer {
            Some(held) => Ok(held.clone()),
            None => Err(errors::released(slf.py(), "producer")),
        }
    }
}

/// The batch `data` stands for on `producer`: one a host built, the canonical Arrow IPC stream of
/// one, or Arrow data held to the producer's schema.
fn submitted<'a>(
    py: Python<'_>,
    producer: &host::Producer,
    data: &'a Bound<'_, PyAny>,
) -> PyResult<Submitted<'a>> {
    if let Ok(batch) = data.cast::<Batch>() {
        return Ok(Submitted::Built(batch.get().host().clone()));
    }
    if arrow::exports(data)? {
        let batch = arrow::import(py, data, producer.schema().clone())?;
        return Ok(Submitted::Built(Arc::new(batch)));
    }
    Ok(Submitted::Ipc(values::host_bytes(data)?))
}

/// A batch a host submits, as the producer sends it: a batch, or the canonical Arrow IPC stream
/// of one, borrowed from a `bytes` object and copied from any other buffer.
enum Submitted<'a> {
    Built(Arc<host::Batch>),
    Ipc(Cow<'a, [u8]>),
}

impl Submitted<'_> {
    /// Submits the batch on `producer`, waiting for credit, and returns its submission id.
    fn send(
        &self,
        producer: &host::Producer,
        cancel: Option<&host::Cancel>,
    ) -> Result<u64, host::Failure> {
        match self {
            Self::Built(batch) => producer.submit(batch, cancel),
            Self::Ipc(ipc) => producer.submit_ipc(ipc, cancel),
        }
    }
}

#[pymethods]
impl Producer {
    /// The producer itself, which leaving the block releases.
    fn __enter__(slf: Bound<'_, Self>) -> PyResult<Bound<'_, Self>> {
        Self::shared(&slf)?;
        Ok(slf)
    }

    /// Releases the producer, closing it without waiting when it is still open, as collecting it
    /// does. An exception raised in the block is never suppressed.
    fn __exit__(
        &mut self,
        _exception_type: &Bound<'_, PyAny>,
        _exception: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> bool {
        self.producer = None;
        false
    }

    /// The schema of the producer's batches.
    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<Schema> {
        Ok(Schema::new(self.held(py)?.schema().clone()))
    }

    #[getter]
    fn state(&self, py: Python<'_>) -> PyResult<EndpointState> {
        Ok(EndpointState::from(self.held(py)?.state()))
    }

    #[getter]
    fn admission(&self, py: Python<'_>) -> PyResult<Admission> {
        Ok(Admission::from(self.held(py)?.admission()))
    }

    #[getter]
    fn generation(&self, py: Python<'_>) -> PyResult<u64> {
        Ok(self.held(py)?.description().generation)
    }

    /// The digest of the contract the producer was opened under.
    #[getter]
    fn contract<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let producer = self.held(py)?;
        Ok(PyBytes::new(
            py,
            producer.description().contract.as_digest(),
        ))
    }

    #[getter]
    fn grant(&self, py: Python<'_>) -> PyResult<Grant> {
        let grant = self.held(py)?.description().grant;
        Ok(Grant {
            batches: grant.batches.get(),
            bytes: grant.bytes.get(),
            max_batch_rows: grant.max_batch_rows.get(),
            max_batch_bytes: grant.max_batch_bytes.get(),
        })
    }

    #[getter]
    fn policy(&self, py: Python<'_>) -> PyResult<Policy> {
        let policy = self.held(py)?.description().policy;
        Ok(Policy::of(
            policy.window,
            policy.ack_timeout,
            policy.retry_backoff,
            policy.retry_max_backoff,
        ))
    }

    /// Why the producer has to be opened again, or `None` while it does not.
    #[getter]
    fn reopen(&self, py: Python<'_>) -> PyResult<Option<Reopen>> {
        let Some(reopen) = self.held(py)?.reopen() else {
            return Ok(None);
        };
        Ok(Some(Reopen::of(reopen)))
    }

    /// Submits a batch, waiting for credit, and returns its submission id. `data` is a batch a
    /// `BatchBuilder` finished, the canonical Arrow IPC stream of one as bytes, or Arrow data that
    /// exports itself through the Arrow PyCapsule interface, held to the producer's schema.
    #[pyo3(signature = (data, *, cancel = None))]
    fn submit(
        slf: &Bound<'_, Self>,
        data: &Bound<'_, PyAny>,
        cancel: Option<PyRef<'_, Cancel>>,
    ) -> PyResult<u64> {
        let py = slf.py();
        let producer = Self::shared(slf)?;
        let submitted = submitted(py, &producer, data)?;
        let cancel = token(cancel.as_ref());
        let sent = py.detach(|| submitted.send(&producer, cancel));
        sent.map_err(|failure| errors::raise(py, failure))
    }

    /// Waits for the outcome of `submission` and takes it.
    #[pyo3(signature = (submission, *, cancel = None))]
    fn rejoin(
        slf: &Bound<'_, Self>,
        submission: u64,
        cancel: Option<PyRef<'_, Cancel>>,
    ) -> PyResult<SubmissionOutcome> {
        let py = slf.py();
        let producer = Self::shared(slf)?;
        let cancel = token(cancel.as_ref());
        let rejoined = py.detach(|| producer.rejoin(submission, cancel));
        match rejoined {
            Ok(outcome) => Ok(SubmissionOutcome { outcome }),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Lets `submission` go, returning its outcome when it is already known.
    fn release(&self, py: Python<'_>, submission: u64) -> PyResult<Option<SubmissionOutcome>> {
        match self.held(py)?.release(submission) {
            Ok(Some(outcome)) => Ok(Some(SubmissionOutcome { outcome })),
            Ok(None) => Ok(None),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// The submissions the producer holds, in submission order.
    fn pending(&self, py: Python<'_>) -> PyResult<Vec<PendingSubmission>> {
        let held = self.held(py)?.pending_submissions();
        Ok(held.iter().map(PendingSubmission::of).collect())
    }

    /// Closes the producer, waiting until the server has.
    #[pyo3(signature = (*, cancel = None))]
    fn close(slf: &Bound<'_, Self>, cancel: Option<PyRef<'_, Cancel>>) -> PyResult<()> {
        let py = slf.py();
        let producer = Self::shared(slf)?;
        let cancel = token(cancel.as_ref());
        let closed = py.detach(|| producer.close(cancel));
        closed.map_err(|failure| errors::raise(py, failure))
    }
}

/// A consumer opened on a client emitter.
#[pyclass(module = "nervix_client", weakref)]
pub(crate) struct Consumer {
    /// The consumer, until its `with` block releases it.
    consumer: Option<Arc<host::Consumer>>,
}

impl Consumer {
    #[expect(
        clippy::too_many_arguments,
        reason = "opening takes the session, the endpoint, its fields, its credit and a token"
    )]
    pub(crate) fn open(
        py: Python<'_>,
        session: &host::Session,
        domain: &str,
        emitter: &str,
        fields: &Fields,
        batches: u32,
        bytes: u64,
        cancel: Option<&host::Cancel>,
    ) -> PyResult<Self> {
        let (batches, bytes) = match host::credit(batches, bytes) {
            Ok(credit) => credit,
            Err(failure) => return Err(errors::raise(py, failure)),
        };
        let limits = ClientConsumerLimits { batches, bytes };
        let fields = fields.host();
        let opened =
            py.detach(|| host::Consumer::open(session, domain, emitter, fields, limits, cancel));
        match opened {
            Ok(consumer) => Ok(Self {
                consumer: Some(Arc::new(consumer)),
            }),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// The consumer a call that does not wait reads.
    fn held(&self, py: Python<'_>) -> PyResult<&host::Consumer> {
        match &self.consumer {
            Some(consumer) => Ok(consumer),
            None => Err(errors::released(py, "consumer")),
        }
    }

    /// The consumer a call that waits runs on, as a reference of its own: a `with` block another
    /// thread leaves while the call waits releases the object without waiting for the call.
    fn shared(slf: &Bound<'_, Self>) -> PyResult<Arc<host::Consumer>> {
        let consumer = slf.try_borrow()?;
        match &consumer.consumer {
            Some(held) => Ok(held.clone()),
            None => Err(errors::released(slf.py(), "consumer")),
        }
    }
}

#[pymethods]
impl Consumer {
    /// The consumer itself, which leaving the block releases.
    fn __enter__(slf: Bound<'_, Self>) -> PyResult<Bound<'_, Self>> {
        Self::shared(&slf)?;
        Ok(slf)
    }

    /// Releases the consumer, closing it without waiting when it is still open, as collecting it
    /// does. Its deliveries keep its session running until they are released too. An exception
    /// raised in the block is never suppressed.
    fn __exit__(
        &mut self,
        _exception_type: &Bound<'_, PyAny>,
        _exception: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> bool {
        self.consumer = None;
        false
    }

    /// The schema of the consumer's batches.
    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<Schema> {
        Ok(Schema::new(self.held(py)?.schema().clone()))
    }

    #[getter]
    fn state(&self, py: Python<'_>) -> PyResult<EndpointState> {
        Ok(EndpointState::from(self.held(py)?.state()))
    }

    #[getter]
    fn generation(&self, py: Python<'_>) -> PyResult<u64> {
        Ok(self.held(py)?.description().generation)
    }

    /// The digest of the contract the consumer was opened under.
    #[getter]
    fn contract<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let consumer = self.held(py)?;
        Ok(PyBytes::new(
            py,
            consumer.description().contract.as_digest(),
        ))
    }

    #[getter]
    fn grant(&self, py: Python<'_>) -> PyResult<Grant> {
        let description = self.held(py)?.description();
        Ok(Grant {
            batches: description.granted.batches.get(),
            bytes: description.granted.bytes.get(),
            max_batch_rows: description.max_batch_rows,
            max_batch_bytes: description.max_batch_bytes,
        })
    }

    #[getter]
    fn policy(&self, py: Python<'_>) -> PyResult<Policy> {
        let description = self.held(py)?.description();
        Ok(Policy::of(
            description.window,
            description.ack_timeout,
            description.retry_backoff,
            description.retry_max_backoff,
        ))
    }

    /// Why the consumer has to be opened again, or `None` while it does not.
    #[getter]
    fn reopen(&self, py: Python<'_>) -> PyResult<Option<Reopen>> {
        let Some(reopen) = self.held(py)?.reopen() else {
            return Ok(None);
        };
        Ok(Some(Reopen::of(reopen)))
    }

    /// Waits for the next delivery.
    #[pyo3(signature = (*, cancel = None))]
    fn next(slf: &Bound<'_, Self>, cancel: Option<PyRef<'_, Cancel>>) -> PyResult<Delivery> {
        let py = slf.py();
        let consumer = Self::shared(slf)?;
        let cancel = token(cancel.as_ref());
        let read = py.detach(|| consumer.next(cancel));
        match read {
            Ok(delivery) => Ok(Delivery {
                delivery: Some(Arc::new(delivery)),
            }),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Closes the consumer, waiting until the server has.
    #[pyo3(signature = (*, cancel = None))]
    fn close(slf: &Bound<'_, Self>, cancel: Option<PyRef<'_, Cancel>>) -> PyResult<()> {
        let py = slf.py();
        let consumer = Self::shared(slf)?;
        let cancel = token(cancel.as_ref());
        let closed = py.detach(|| consumer.close(cancel));
        closed.map_err(|failure| errors::raise(py, failure))
    }
}

/// One delivery of a client emitter's output.
///
/// It exports its canonical Arrow IPC stream through the buffer protocol: `memoryview(delivery)`,
/// or `delivery.ipc`, borrows it without a copy and keeps the delivery alive, and keeps the stream
/// valid after the delivery's `with` block released it.
#[pyclass(module = "nervix_client", weakref)]
pub(crate) struct Delivery {
    /// The delivery, until its `with` block releases it. Every exported view holds a reference of
    /// its own until the view is released.
    delivery: Option<Arc<host::Delivery>>,
}

impl Delivery {
    /// The delivery a call that does not wait reads.
    fn held(&self, py: Python<'_>) -> PyResult<&host::Delivery> {
        match &self.delivery {
            Some(delivery) => Ok(delivery),
            None => Err(errors::released(py, "delivery")),
        }
    }

    /// The delivery a call that waits, or a view, holds as a reference of its own: a `with` block
    /// another thread leaves while the call waits releases the object without waiting for it.
    fn shared(slf: &Bound<'_, Self>) -> PyResult<Arc<host::Delivery>> {
        let delivery = slf.try_borrow()?;
        match &delivery.delivery {
            Some(held) => Ok(held.clone()),
            None => Err(errors::released(slf.py(), "delivery")),
        }
    }
}

#[pymethods]
impl Delivery {
    /// The delivery itself, which leaving the block releases.
    fn __enter__(slf: Bound<'_, Self>) -> PyResult<Bound<'_, Self>> {
        Self::shared(&slf)?;
        Ok(slf)
    }

    /// Releases the delivery, settling nothing, as collecting it does. A view of its stream keeps
    /// the stream valid until the view is released. An exception raised in the block is never
    /// suppressed.
    fn __exit__(
        &mut self,
        _exception_type: &Bound<'_, PyAny>,
        _exception: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> bool {
        self.delivery = None;
        false
    }

    /// The identity every attempt of the same output keeps.
    #[getter]
    fn identity<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        Ok(PyBytes::new(py, self.held(py)?.identity()))
    }

    /// The reference this attempt is settled by.
    #[getter]
    fn reference<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        Ok(PyBytes::new(py, self.held(py)?.reference()))
    }

    #[getter]
    fn source_relay(&self, py: Python<'_>) -> PyResult<&str> {
        Ok(self.held(py)?.source_relay())
    }

    /// The fingerprint of the concrete branch the output came from, or `None` when it is
    /// unbranched.
    #[getter]
    fn branch_fingerprint<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let Some(fingerprint) = self.held(py)?.branch_fingerprint() else {
            return Ok(None);
        };
        Ok(Some(PyBytes::new(py, fingerprint)))
    }

    /// The rows the delivery carries.
    #[getter]
    fn members(&self, py: Python<'_>) -> PyResult<u32> {
        Ok(self.held(py)?.members())
    }

    /// The domain's logical time when the output was produced, in nanoseconds since the Unix
    /// epoch.
    #[getter]
    fn execution_now(&self, py: Python<'_>) -> PyResult<i64> {
        Ok(self.held(py)?.execution_now())
    }

    /// The delivery's canonical Arrow IPC stream, borrowed without a copy. The view keeps the
    /// delivery alive.
    #[getter]
    fn ipc<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        PyMemoryView::from(slf.as_any())
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let delivery = Self::shared(&slf)?;
        // SAFETY: Python passes a view to fill; the stream is owned by the delivery whose
        // reference the view holds below until it is released.
        unsafe { values::export_bytes(slf.as_any(), delivery.ipc(), view, flags)? };
        // The view keeps its own reference to the delivery, so a `with` block that releases the
        // object leaves the stream the view borrows valid until `__releasebuffer__`.
        //
        // SAFETY: `view` is the view Python asked this object to fill, `export_bytes` filled it,
        // and its `internal` slot belongs to the exporter.
        unsafe {
            (*view).internal = Arc::into_raw(delivery).cast_mut().cast();
        }
        Ok(())
    }

    unsafe fn __releasebuffer__(slf: Bound<'_, Self>, view: *mut ffi::Py_buffer) {
        drop(slf);
        // SAFETY: Python passes the view this object filled, once, when the view is released, and
        // `__getbuffer__` left in its `internal` slot the reference it took.
        let internal = unsafe { (*view).internal };
        if internal.is_null() {
            return;
        }
        // SAFETY: `internal` is the pointer `Arc::into_raw` returned for this view's reference,
        // which nothing else reclaims.
        drop(unsafe { Arc::from_raw(internal.cast::<host::Delivery>().cast_const()) });
    }

    /// The batch the delivery carries, decoded the first time it is read and held to the
    /// consumer's schema and the delivery's row count.
    #[getter]
    fn batch(&self, py: Python<'_>) -> PyResult<Batch> {
        match self.held(py)?.batch() {
            Ok(batch) => Ok(Batch::new(batch.clone())),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Acknowledges the delivery.
    #[pyo3(signature = (*, cancel = None))]
    fn ack(slf: &Bound<'_, Self>, cancel: Option<PyRef<'_, Cancel>>) -> PyResult<Settlement> {
        let py = slf.py();
        let delivery = Self::shared(slf)?;
        let cancel = token(cancel.as_ref());
        let settled = py.detach(|| delivery.ack(cancel));
        match settled {
            Ok(settlement) => Ok(Settlement::from(settlement)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Asks for the delivery again, after the emitter's retry backoff.
    #[pyo3(signature = (*, cancel = None))]
    fn retry(slf: &Bound<'_, Self>, cancel: Option<PyRef<'_, Cancel>>) -> PyResult<Settlement> {
        let py = slf.py();
        let delivery = Self::shared(slf)?;
        let cancel = token(cancel.as_ref());
        let settled = py.detach(|| delivery.retry(cancel));
        match settled {
            Ok(settlement) => Ok(Settlement::from(settlement)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Rejects the delivery for `reason`, which the emitter's message error policy handles.
    #[pyo3(signature = (reason, *, cancel = None))]
    fn reject(
        slf: &Bound<'_, Self>,
        reason: &str,
        cancel: Option<PyRef<'_, Cancel>>,
    ) -> PyResult<Settlement> {
        let py = slf.py();
        let delivery = Self::shared(slf)?;
        let cancel = token(cancel.as_ref());
        let settled = py.detach(|| delivery.reject(reason, cancel));
        match settled {
            Ok(settlement) => Ok(Settlement::from(settlement)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }
}

/// Adds the producer, consumer and delivery classes to the module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Producer>()?;
    module.add_class::<Consumer>()?;
    module.add_class::<Delivery>()?;
    module.add_class::<SubmissionOutcome>()?;
    module.add_class::<Grant>()?;
    module.add_class::<Policy>()?;
    module.add_class::<Reopen>()?;
    module.add_class::<PendingSubmission>()?;
    Ok(())
}
