//! A session a Python host holds, the commands it prepares and runs, and what became of them.
//!
//! Layer: edges.
//! - **Owns.** Opening a session with the connection options a host passes, preparing and
//!   executing commands, waiting for subscription and domain clock events, reading the clock of a
//!   followed domain, completion pages, opening producers and consumers, and reading a command's
//!   outcome, diagnostics, upload failure, subscription, backup archive and restore report.
//! - **Depends on.** The host session runtime's session, outcome and completion page, the
//!   cancellation token, and the event, clock, schema and endpoint classes it returns.
//! - **Must not know.** How the session reaches its server, retries or recovers; the Rust client
//!   decides all of it through the host session runtime.
//!
//! Every call that waits releases the GIL for as long as it waits, so another Python thread runs
//! meanwhile and may cancel it through its token.

use std::time::Duration;

use nervix_client_core::{CommandExecutionReference, ConnectOptions, DomainName, SourceSpan};
use nervix_client_host as host;
use pyo3::{prelude::*, pybacked::PyBackedBytes};
use url::Url;

use crate::{
    clock::{ClockEvent, DomainClock},
    endpoint::{Consumer, Producer},
    enums::{CompletionKind, CompletionStatus, Disposition, Tls, UploadFailure},
    errors,
    event::{Event, Subscription},
    schema::{Fields, Schema},
};

/// A cancellation token, with the deadline it expires at when it has one.
///
/// A token is triggered from any thread, and every call it was passed to returns with
/// `CancelledError`, or with `DeadlineError` once its deadline passes. Cancelling a wait never
/// rolls back admitted work.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct Cancel {
    cancel: host::Cancel,
}

impl Cancel {
    pub(crate) fn token(&self) -> &host::Cancel {
        &self.cancel
    }
}

/// The token a call takes, borrowed from the Python object the host passed.
pub(crate) fn token<'a>(cancel: Option<&'a PyRef<'_, Cancel>>) -> Option<&'a host::Cancel> {
    cancel.map(|cancel| cancel.token())
}

/// A duration a host passed in seconds.
pub(crate) fn seconds(py: Python<'_>, argument: &'static str, seconds: f64) -> PyResult<Duration> {
    match Duration::try_from_secs_f64(seconds) {
        Ok(duration) => Ok(duration),
        Err(error) => Err(errors::invalid_argument(
            py,
            argument,
            &format!("must be a finite number of seconds that is not negative: {error}"),
        )),
    }
}

#[pymethods]
impl Cancel {
    /// A token, which also expires `deadline` seconds from now when it is given.
    #[new]
    #[pyo3(signature = (*, deadline = None))]
    fn new(py: Python<'_>, deadline: Option<f64>) -> PyResult<Self> {
        let Some(deadline) = deadline else {
            return Ok(Self {
                cancel: host::Cancel::default(),
            });
        };
        let deadline = seconds(py, "deadline", deadline)?;
        match host::Cancel::with_deadline(deadline) {
            Ok(cancel) => Ok(Self { cancel }),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Makes every call waiting on the token, now or later, return `CancelledError`.
    fn trigger(&self) {
        self.cancel.trigger();
    }
}

/// An open session.
///
/// It is released when Python collects it; its producers, consumers and deliveries keep it
/// running until they are collected too.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct Session {
    session: host::Session,
}

/// One command, with the durable execution identity it keeps across every attempt.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct Execution {
    execution: host::Execution,
}

#[pymethods]
impl Execution {
    /// The command's durable execution reference.
    #[getter]
    fn reference(&self) -> &str {
        self.execution.reference()
    }
}

/// The connection options a host passed, as the Rust client takes them.
struct Connection<'a> {
    username: Option<&'a str>,
    password: Option<&'a str>,
    tls: Option<Tls>,
    ca_certificate: Option<PyBackedBytes>,
    seeds: Vec<String>,
    connect_timeout: Option<f64>,
    request_timeout: Option<f64>,
    retry_timeout: Option<f64>,
    backup_wait_timeout: Option<f64>,
}

impl Connection<'_> {
    /// The Rust client's options: its own default for every option the host left out.
    fn options(self, py: Python<'_>) -> PyResult<ConnectOptions> {
        let mut options = ConnectOptions::default();
        match (self.username, self.password) {
            (Some(username), Some(password)) => {
                options = options.with_basic_auth(username, password);
            }
            (None, None) => {}
            (Some(_), None) | (None, Some(_)) => {
                return Err(errors::invalid_argument(
                    py,
                    "username",
                    "and `password` must both be present or both be absent",
                ));
            }
        }
        if let Some(tls) = self.tls {
            options.tls_requirement = Some(tls.into());
        }
        if let Some(pem) = self.ca_certificate {
            options.ca_certificate_pem = Some(pem.to_vec());
        }
        let mut seeds = Vec::with_capacity(self.seeds.len());
        for seed in &self.seeds {
            match Url::parse(seed) {
                Ok(url) => seeds.push(url),
                Err(error) => {
                    return Err(errors::invalid_argument(
                        py,
                        "seeds",
                        &format!("holds '{seed}', which is not a URL: {error}"),
                    ));
                }
            }
        }
        options.seed_servers = seeds;
        if let Some(timeout) = self.connect_timeout {
            options.connect_timeout = seconds(py, "connect_timeout", timeout)?;
        }
        if let Some(timeout) = self.request_timeout {
            options.request_timeout = seconds(py, "request_timeout", timeout)?;
        }
        if let Some(timeout) = self.retry_timeout {
            options.retry_timeout = seconds(py, "retry_timeout", timeout)?;
        }
        if let Some(timeout) = self.backup_wait_timeout {
            options.backup_wait_timeout = seconds(py, "backup_wait_timeout", timeout)?;
        }
        Ok(options)
    }
}

impl Session {
    pub(crate) fn host(&self) -> &host::Session {
        &self.session
    }
}

#[pymethods]
impl Session {
    /// Opens a session on `server`, blocking until it is open.
    ///
    /// Every option the host leaves out takes the Rust client's default. `seeds` are further
    /// servers the session may reach, and the timeouts are in seconds.
    #[new]
    #[pyo3(signature = (
        server,
        *,
        domain = None,
        username = None,
        password = None,
        tls = None,
        ca_certificate = None,
        seeds = Vec::new(),
        connect_timeout = None,
        request_timeout = None,
        retry_timeout = None,
        backup_wait_timeout = None,
        cancel = None,
    ))]
    #[expect(
        clippy::too_many_arguments,
        reason = "a Python constructor takes every connection option as its own keyword"
    )]
    fn connect(
        py: Python<'_>,
        server: &str,
        domain: Option<&str>,
        username: Option<&str>,
        password: Option<&str>,
        tls: Option<Tls>,
        ca_certificate: Option<PyBackedBytes>,
        seeds: Vec<String>,
        connect_timeout: Option<f64>,
        request_timeout: Option<f64>,
        retry_timeout: Option<f64>,
        backup_wait_timeout: Option<f64>,
        cancel: Option<PyRef<'_, Cancel>>,
    ) -> PyResult<Self> {
        let connection = Connection {
            username,
            password,
            tls,
            ca_certificate,
            seeds,
            connect_timeout,
            request_timeout,
            retry_timeout,
            backup_wait_timeout,
        };
        let options = connection.options(py)?;
        let cancel = token(cancel.as_ref());
        let connected = py.detach(|| host::Session::connect(server, domain, options, cancel));
        match connected {
            Ok(session) => Ok(Self { session }),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Captures `query` and a fresh execution identity before anything is sent.
    #[pyo3(signature = (query, *, cancel = None))]
    fn prepare(
        &self,
        py: Python<'_>,
        query: &str,
        cancel: Option<PyRef<'_, Cancel>>,
    ) -> PyResult<Execution> {
        let cancel = token(cancel.as_ref());
        let prepared = py.detach(|| self.session.prepare(query, cancel));
        match prepared {
            Ok(execution) => Ok(Execution { execution }),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Runs a prepared command. A failure that leaves its outcome open names its execution
    /// reference, and running the same execution again recovers that outcome instead of running
    /// the command twice.
    #[pyo3(signature = (execution, *, cancel = None))]
    fn execute(
        &self,
        py: Python<'_>,
        execution: PyRef<'_, Execution>,
        cancel: Option<PyRef<'_, Cancel>>,
    ) -> PyResult<Outcome> {
        let cancel = token(cancel.as_ref());
        let execution = &execution.execution;
        let executed = py.detach(|| self.session.execute(execution, cancel));
        match executed {
            Ok(outcome) => Ok(Outcome { outcome }),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Waits for the next event of any subscription the session holds, continuing across a lost
    /// session.
    #[pyo3(signature = (*, cancel = None))]
    fn next_event(&self, py: Python<'_>, cancel: Option<PyRef<'_, Cancel>>) -> PyResult<Event> {
        let cancel = token(cancel.as_ref());
        let waited = py.detach(|| self.session.next_event(cancel));
        match waited {
            Ok(event) => Ok(Event::new(event)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Waits for the next event about the domain clocks the session follows.
    #[pyo3(signature = (*, cancel = None))]
    fn next_clock_event(
        &self,
        py: Python<'_>,
        cancel: Option<PyRef<'_, Cancel>>,
    ) -> PyResult<ClockEvent> {
        let cancel = token(cancel.as_ref());
        let waited = py.detach(|| self.session.next_clock_event(cancel));
        match waited {
            Ok(event) => Ok(ClockEvent::new(event)),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// The clock of `domain` as the session last received it, or `None` when it follows no clock
    /// of that domain. It reads what the session already holds, so it never waits.
    fn domain_clock(&self, py: Python<'_>, domain: &str) -> PyResult<Option<DomainClock>> {
        let domain = match DomainName::try_from(domain) {
            Ok(domain) => domain,
            Err(error) => {
                return Err(errors::invalid_argument(py, "domain", &error.to_string()));
            }
        };
        let clock = self.session.domain_clock(&domain);
        match clock {
            Some(clock) => Ok(Some(DomainClock::new(clock))),
            None => Ok(None),
        }
    }

    /// Reads one page of completion candidates for `input` at `cursor` from the session's
    /// current context, continuing an earlier page when `continuation` is given.
    #[pyo3(signature = (input, cursor, page_size, continuation = None, *, cancel = None))]
    fn suggest(
        &self,
        py: Python<'_>,
        input: &str,
        cursor: usize,
        page_size: u16,
        continuation: Option<&str>,
        cancel: Option<PyRef<'_, Cancel>>,
    ) -> PyResult<Suggestions> {
        let cancel = token(cancel.as_ref());
        let suggested = py.detach(|| {
            self.session
                .suggest_page(input, cursor, page_size, continuation, cancel)
        });
        match suggested {
            Ok(outcome) => Ok(Suggestions {
                page: host::Suggestions::new(outcome),
            }),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Opens a producer on client ingestor `ingestor` of `domain`, expecting exactly `fields`
    /// and asking for credit of `batches` batches and `bytes` bytes.
    #[pyo3(signature = (domain, ingestor, fields, *, batches, bytes, cancel = None))]
    #[expect(
        clippy::too_many_arguments,
        reason = "a Python method takes the endpoint, its fields and its credit as keywords"
    )]
    fn open_ingestor(
        &self,
        py: Python<'_>,
        domain: &str,
        ingestor: &str,
        fields: PyRef<'_, Fields>,
        batches: u32,
        bytes: u64,
        cancel: Option<PyRef<'_, Cancel>>,
    ) -> PyResult<Producer> {
        Producer::open(
            py,
            self,
            domain,
            ingestor,
            &fields,
            batches,
            bytes,
            token(cancel.as_ref()),
        )
    }

    /// Opens a consumer on client emitter `emitter` of `domain`, expecting exactly `fields` and
    /// asking for credit of `batches` batches and `bytes` bytes.
    #[pyo3(signature = (domain, emitter, fields, *, batches, bytes, cancel = None))]
    #[expect(
        clippy::too_many_arguments,
        reason = "a Python method takes the endpoint, its fields and its credit as keywords"
    )]
    fn subscribe_emitter(
        &self,
        py: Python<'_>,
        domain: &str,
        emitter: &str,
        fields: PyRef<'_, Fields>,
        batches: u32,
        bytes: u64,
        cancel: Option<PyRef<'_, Cancel>>,
    ) -> PyResult<Consumer> {
        Consumer::open(
            py,
            self,
            domain,
            emitter,
            &fields,
            batches,
            bytes,
            token(cancel.as_ref()),
        )
    }
}

/// What became of a command.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct Outcome {
    outcome: host::Outcome,
}

/// One diagnostic of a command, with the span of the source it concerns when it names one.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct Diagnostic {
    message: String,
    span: Option<SourceSpan>,
}

#[pymethods]
impl Diagnostic {
    #[getter]
    fn message(&self) -> &str {
        &self.message
    }

    /// The `(start, end)` byte offsets of the source the diagnostic concerns, or `None`.
    #[getter]
    fn span(&self) -> Option<(u32, u32)> {
        let span = self.span?;
        Some((span.start(), span.end()))
    }
}

/// The archive a backup command left to download.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct Backup {
    total_bytes: u64,
    digest: [u8; 32],
}

#[pymethods]
impl Backup {
    /// The exact size of the archive.
    #[getter]
    fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// The archive's SHA-256 digest.
    #[getter]
    fn digest(&self) -> &[u8] {
        &self.digest
    }
}

/// What a restore command did, or would do for a dry run.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct Restore {
    summary: host::RestoreSummary,
}

#[pymethods]
impl Restore {
    #[getter]
    fn dry_run(&self) -> bool {
        self.summary.dry_run
    }

    #[getter]
    fn domains(&self) -> u64 {
        self.summary.domains
    }

    #[getter]
    fn resource_versions(&self) -> u64 {
        self.summary.resource_versions
    }

    #[getter]
    fn models(&self) -> u64 {
        self.summary.models
    }

    /// Whether a step of the restore failed.
    #[getter]
    fn failed(&self) -> bool {
        self.summary.failed
    }
}

#[pymethods]
impl Outcome {
    #[getter]
    fn disposition(&self) -> Disposition {
        Disposition::from(self.outcome.disposition())
    }

    /// The typed upload failure, or `None` for a completed upload or another command.
    #[getter]
    fn upload_failure(&self) -> Option<UploadFailure> {
        self.outcome.upload_failure().map(UploadFailure::from)
    }

    #[getter]
    fn message(&self) -> &str {
        &self.outcome.command().message
    }

    /// The command's execution reference, when the server reported one.
    #[getter]
    fn execution_reference(&self) -> Option<&str> {
        let reference: &CommandExecutionReference =
            self.outcome.command().execution_reference.as_ref()?;
        Some(reference.as_str())
    }

    #[getter]
    fn diagnostics(&self) -> Vec<Diagnostic> {
        self.outcome
            .command()
            .diagnostics
            .iter()
            .map(|diagnostic| Diagnostic {
                message: diagnostic.message.clone(),
                span: diagnostic.span,
            })
            .collect()
    }

    /// The subscription the command opened, or `None`.
    #[getter]
    fn subscription(&self) -> Option<Subscription> {
        let opened = self.outcome.command().subscription.as_ref()?;
        Some(Subscription::of(&opened.subscription))
    }

    /// The schema of the subscription the command opened, or `None`.
    #[getter]
    fn schema(&self) -> Option<Schema> {
        let schema = self.outcome.opened_schema()?;
        Some(Schema::new(schema.clone()))
    }

    /// The archive a backup command left to download, or `None`.
    #[getter]
    fn backup(&self) -> Option<Backup> {
        let summary = self.outcome.command().backup.as_ref()?;
        Some(Backup {
            total_bytes: summary.total_bytes.get(),
            digest: *summary.digest.as_bytes(),
        })
    }

    /// What a restore command did, or `None`.
    #[getter]
    fn restore(&self) -> Option<Restore> {
        let summary = self.outcome.restore_summary()?;
        Some(Restore { summary })
    }
}

/// One page of completion candidates.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct Suggestions {
    page: host::Suggestions,
}

/// One completion candidate, and the edit of the input that applies it.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct Suggestion {
    value: String,
    kind: CompletionKind,
    start: u32,
    end: u32,
    replacement: String,
}

#[pymethods]
impl Suggestion {
    #[getter]
    fn value(&self) -> &str {
        &self.value
    }

    #[getter]
    fn kind(&self) -> CompletionKind {
        self.kind
    }

    /// The `(start, end)` byte offsets of the input the candidate replaces.
    #[getter]
    fn span(&self) -> (u32, u32) {
        (self.start, self.end)
    }

    #[getter]
    fn replacement(&self) -> &str {
        &self.replacement
    }
}

#[pymethods]
impl Suggestions {
    #[getter]
    fn status(&self) -> CompletionStatus {
        CompletionStatus::from(host::CompletionStatus::from(self.page.outcome().status))
    }

    #[getter]
    fn suggestions(&self) -> Vec<Suggestion> {
        self.page
            .outcome()
            .suggestions
            .iter()
            .map(|suggestion| Suggestion {
                value: suggestion.value.clone(),
                kind: CompletionKind::from(host::CompletionKind::from(suggestion.kind)),
                start: suggestion.edit.start,
                end: suggestion.edit.end,
                replacement: suggestion.edit.replacement.clone(),
            })
            .collect()
    }

    /// The token that continues with the next page, or `None` on the last page.
    #[getter]
    fn continuation(&self) -> Option<&str> {
        self.page.outcome().continuation.as_deref()
    }
}

/// Adds the session, command, outcome and completion classes to the module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Cancel>()?;
    module.add_class::<Session>()?;
    module.add_class::<Execution>()?;
    module.add_class::<Outcome>()?;
    module.add_class::<Diagnostic>()?;
    module.add_class::<Backup>()?;
    module.add_class::<Restore>()?;
    module.add_class::<Suggestions>()?;
    module.add_class::<Suggestion>()?;
    Ok(())
}
