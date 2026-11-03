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
//! An endpoint keeps its session running until Python collects it, and a delivery its consumer's.
//! Collecting a delivery settles nothing: an unsettled attempt is delivered again once the
//! emitter's ACK timeout passes or its attachment ends.

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
    session::{Cancel, Session, token},
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
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct Producer {
    producer: host::Producer,
}

impl Producer {
    #[expect(
        clippy::too_many_arguments,
        reason = "opening takes the session, the endpoint, its fields, its credit and a token"
    )]
    pub(crate) fn open(
        py: Python<'_>,
        session: &Session,
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
        let session = session.host();
        let opened =
            py.detach(|| host::Producer::open(session, domain, ingestor, fields, limits, cancel));
        match opened {
            Ok(producer) => Ok(Self { producer }),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// The batch `data` stands for: one a host built, the canonical Arrow IPC stream of one, or
    /// Arrow data held to the producer's schema.
    fn submitted<'a>(&self, py: Python<'_>, data: &'a Bound<'_, PyAny>) -> PyResult<Submitted<'a>> {
        if let Ok(batch) = data.cast::<Batch>() {
            return Ok(Submitted::Built(batch.get().host().clone()));
        }
        if arrow::exports(data)? {
            let batch = arrow::import(py, data, self.producer.schema().clone())?;
            return Ok(Submitted::Built(Arc::new(batch)));
        }
        Ok(Submitted::Ipc(values::host_bytes(data)?))
    }
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
    /// The schema of the producer's batches.
    #[getter]
    fn schema(&self) -> Schema {
        Schema::new(self.producer.schema().clone())
    }

    #[getter]
    fn state(&self) -> EndpointState {
        EndpointState::from(self.producer.state())
    }

    #[getter]
    fn admission(&self) -> Admission {
        Admission::from(self.producer.admission())
    }

    #[getter]
    fn generation(&self) -> u64 {
        self.producer.description().generation
    }

    /// The digest of the contract the producer was opened under.
    #[getter]
    fn contract<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.producer.description().contract.as_digest())
    }

    #[getter]
    fn grant(&self) -> Grant {
        let grant = self.producer.description().grant;
        Grant {
            batches: grant.batches.get(),
            bytes: grant.bytes.get(),
            max_batch_rows: grant.max_batch_rows.get(),
            max_batch_bytes: grant.max_batch_bytes.get(),
        }
    }

    #[getter]
    fn policy(&self) -> Policy {
        let policy = self.producer.description().policy;
        Policy::of(
            policy.window,
            policy.ack_timeout,
            policy.retry_backoff,
            policy.retry_max_backoff,
        )
    }

    /// Why the producer has to be opened again, or `None` while it does not.
    #[getter]
    fn reopen(&self) -> Option<Reopen> {
        let reopen = self.producer.reopen()?;
        Some(Reopen::of(reopen))
    }

    /// Submits a batch, waiting for credit, and returns its submission id. `data` is a batch a
    /// `BatchBuilder` finished, the canonical Arrow IPC stream of one as bytes, or Arrow data that
    /// exports itself through the Arrow PyCapsule interface, held to the producer's schema.
    #[pyo3(signature = (data, *, cancel = None))]
    fn submit(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        cancel: Option<PyRef<'_, Cancel>>,
    ) -> PyResult<u64> {
        let submitted = self.submitted(py, data)?;
        let cancel = token(cancel.as_ref());
        let sent = py.detach(|| submitted.send(&self.producer, cancel));
        sent.map_err(|failure| errors::raise(py, failure))
    }

    /// Waits for the outcome of `submission` and takes it.
    #[pyo3(signature = (submission, *, cancel = None))]
    fn rejoin(
        &self,
        py: Python<'_>,
        submission: u64,
        cancel: Option<PyRef<'_, Cancel>>,
    ) -> PyResult<SubmissionOutcome> {
        let cancel = token(cancel.as_ref());
        let rejoined = py.detach(|| self.producer.rejoin(submission, cancel));
        match rejoined {
            Ok(outcome) => Ok(SubmissionOutcome { outcome }),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Lets `submission` go, returning its outcome when it is already known.
    fn release(&self, py: Python<'_>, submission: u64) -> PyResult<Option<SubmissionOutcome>> {
        match self.producer.release(submission) {
            Ok(Some(outcome)) => Ok(Some(SubmissionOutcome { outcome })),
            Ok(None) => Ok(None),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// The submissions the producer holds, in submission order.
    fn pending(&self) -> Vec<PendingSubmission> {
        self.producer
            .pending_submissions()
            .iter()
            .map(PendingSubmission::of)
            .collect()
    }

    /// Closes the producer, waiting until the server has.
    #[pyo3(signature = (*, cancel = None))]
    fn close(&self, py: Python<'_>, cancel: Option<PyRef<'_, Cancel>>) -> PyResult<()> {
        let cancel = token(cancel.as_ref());
        let closed = py.detach(|| self.producer.close(cancel));
        closed.map_err(|failure| errors::raise(py, failure))
    }
}

/// A consumer opened on a client emitter.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct Consumer {
    consumer: host::Consumer,
}

impl Consumer {
    #[expect(
        clippy::too_many_arguments,
        reason = "opening takes the session, the endpoint, its fields, its credit and a token"
    )]
    pub(crate) fn open(
        py: Python<'_>,
        session: &Session,
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
        let session = session.host();
        let opened =
            py.detach(|| host::Consumer::open(session, domain, emitter, fields, limits, cancel));
        match opened {
            Ok(consumer) => Ok(Self { consumer }),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }
}

#[pymethods]
impl Consumer {
    /// The schema of the consumer's batches.
    #[getter]
    fn schema(&self) -> Schema {
        Schema::new(self.consumer.schema().clone())
    }

    #[getter]
    fn state(&self) -> EndpointState {
        EndpointState::from(self.consumer.state())
    }

    #[getter]
    fn generation(&self) -> u64 {
        self.consumer.description().generation
    }

    /// The digest of the contract the consumer was opened under.
    #[getter]
    fn contract<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.consumer.description().contract.as_digest())
    }

    #[getter]
    fn grant(&self) -> Grant {
        let description = self.consumer.description();
        Grant {
            batches: description.granted.batches.get(),
            bytes: description.granted.bytes.get(),
            max_batch_rows: description.max_batch_rows,
            max_batch_bytes: description.max_batch_bytes,
        }
    }

    #[getter]
    fn policy(&self) -> Policy {
        let description = self.consumer.description();
        Policy::of(
            description.window,
            description.ack_timeout,
            description.retry_backoff,
            description.retry_max_backoff,
        )
    }

    /// Why the consumer has to be opened again, or `None` while it does not.
    #[getter]
    fn reopen(&self) -> Option<Reopen> {
        let reopen = self.consumer.reopen()?;
        Some(Reopen::of(reopen))
    }

    /// Waits for the next delivery.
    #[pyo3(signature = (*, cancel = None))]
    fn next(&self, py: Python<'_>, cancel: Option<PyRef<'_, Cancel>>) -> PyResult<Delivery> {
        let cancel = token(cancel.as_ref());
        let read = py.detach(|| self.consumer.next(cancel));
        match read {
            Ok(delivery) => Ok(Delivery { delivery }),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Closes the consumer, waiting until the server has.
    #[pyo3(signature = (*, cancel = None))]
    fn close(&self, py: Python<'_>, cancel: Option<PyRef<'_, Cancel>>) -> PyResult<()> {
        let cancel = token(cancel.as_ref());
        let closed = py.detach(|| self.consumer.close(cancel));
        closed.map_err(|failure| errors::raise(py, failure))
    }
}

/// One delivery of a client emitter's output.
///
/// It exports its canonical Arrow IPC stream through the buffer protocol: `memoryview(delivery)`,
/// or `delivery.ipc`, borrows it without a copy and keeps the delivery alive.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct Delivery {
    delivery: host::Delivery,
}

#[pymethods]
impl Delivery {
    /// The identity every attempt of the same output keeps.
    #[getter]
    fn identity<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.delivery.identity())
    }

    /// The reference this attempt is settled by.
    #[getter]
    fn reference<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.delivery.reference())
    }

    #[getter]
    fn source_relay(&self) -> &str {
        self.delivery.source_relay()
    }

    /// The fingerprint of the concrete branch the output came from, or `None` when it is
    /// unbranched.
    #[getter]
    fn branch_fingerprint<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        let fingerprint = self.delivery.branch_fingerprint()?;
        Some(PyBytes::new(py, fingerprint))
    }

    /// The rows the delivery carries.
    #[getter]
    fn members(&self) -> u32 {
        self.delivery.members()
    }

    /// The domain's logical time when the output was produced, in nanoseconds since the Unix
    /// epoch.
    #[getter]
    fn execution_now(&self) -> i64 {
        self.delivery.execution_now()
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
        let ipc = slf.get().delivery.ipc();
        // SAFETY: Python passes a view to fill; the stream is owned by the delivery `slf` holds,
        // which the view keeps alive until it is released.
        unsafe { values::export_bytes(slf.as_any(), ipc, view, flags) }
    }

    /// The batch the delivery carries, decoded the first time it is read and held to the
    /// consumer's schema and the delivery's row count.
    #[getter]
    fn batch(&self, py: Python<'_>) -> PyResult<Batch> {
        match self.delivery.batch() {
            Ok(batch) => Ok(Batch::new(batch.clone())),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Acknowledges the delivery.
    #[pyo3(signature = (*, cancel = None))]
    fn ack(&self, py: Python<'_>, cancel: Option<PyRef<'_, Cancel>>) -> PyResult<Settlement> {
        let cancel = token(cancel.as_ref());
        let settled = py.detach(|| self.delivery.ack(cancel));
        match settled {
            Ok(settlement) => Ok(Settlement::from(settlement)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Asks for the delivery again, after the emitter's retry backoff.
    #[pyo3(signature = (*, cancel = None))]
    fn retry(&self, py: Python<'_>, cancel: Option<PyRef<'_, Cancel>>) -> PyResult<Settlement> {
        let cancel = token(cancel.as_ref());
        let settled = py.detach(|| self.delivery.retry(cancel));
        match settled {
            Ok(settlement) => Ok(Settlement::from(settlement)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Rejects the delivery for `reason`, which the emitter's message error policy handles.
    #[pyo3(signature = (reason, *, cancel = None))]
    fn reject(
        &self,
        py: Python<'_>,
        reason: &str,
        cancel: Option<PyRef<'_, Cancel>>,
    ) -> PyResult<Settlement> {
        let cancel = token(cancel.as_ref());
        let settled = py.detach(|| self.delivery.reject(reason, cancel));
        match settled {
            Ok(settlement) => Ok(Settlement::from(settlement)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }
}
