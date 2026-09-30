//! A consumer a host holds: `nx_consumer`, opened on a session's client emitter.
//!
//! - **Owns.** Opening a consumer with the fields and credit a host asks for, waiting for the
//!   next delivery, reading its description and state, and closing and releasing it.
//! - **Depends on.** The Rust client's emitter consumer, the session it was opened on, the field
//!   and delivery handles, and the endpoint vocabulary.
//! - **Must not know.** How output is assigned, redelivered or restored; the Rust consumer decides
//!   all of it, so this module keeps no state of its own about a delivery or an attachment.

use nervix_client_core::{
    ClientConsumerLimits, DomainName, EmitterConsumer, EmitterName, wire::EmitterOpened,
};
use triomphe::Arc;

use crate::{
    abi,
    cancel::Cancel,
    delivery::Delivery,
    endpoint::{EndpointState, OpenRefusal, Reopen, ReopenReason, Window, WindowKind},
    failure::{Failure, FailureKind},
    fields::Fields,
    producer::{credit, nanos},
    schema::Schema,
    session::{Session, SessionRuntime},
};

/// A consumer, with the session its blocking calls run on and the schema of its batches.
pub struct Consumer {
    consumer: EmitterConsumer,
    schema: Schema,
    session: Arc<SessionRuntime>,
}

impl Consumer {
    /// Opens a consumer on `emitter` of `domain`, blocking until the server answers.
    pub fn open(
        session: &Session,
        domain: &str,
        emitter: &str,
        fields: &Fields,
        limits: ClientConsumerLimits,
        cancel: Option<&Cancel>,
    ) -> Result<Self, Failure> {
        let domain = match DomainName::try_from(domain) {
            Ok(domain) => domain,
            Err(error) => return Err(Failure::invalid_argument("domain", &error.to_string())),
        };
        let emitter = match EmitterName::try_from(emitter) {
            Ok(emitter) => emitter,
            Err(error) => return Err(Failure::invalid_argument("emitter", &error.to_string())),
        };
        let expected = fields.schema_fields()?;
        let shared = session.shared().clone();
        let opening = async {
            let opened = shared
                .client()
                .subscribe_emitter(domain, emitter, expected, limits)
                .await;
            opened.map_err(Failure::from)
        };
        let consumer = shared.block_on(cancel, opening)?;
        let schema = Schema::of_fields(consumer.description().fields.clone());
        Ok(Self {
            consumer,
            schema,
            session: shared,
        })
    }

    fn description(&self) -> &EmitterOpened {
        self.consumer.description()
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn state(&self) -> EndpointState {
        EndpointState::from(self.consumer.connection())
    }

    /// Why the consumer has to be opened again, once it does.
    pub fn reopen(&self) -> Option<Reopen> {
        self.consumer.reopen_reason().as_ref().map(Reopen::from)
    }

    /// Waits for the next delivery. A read the wait abandons stays with the consumer, and a later
    /// call receives what it reads.
    pub fn next(&self, cancel: Option<&Cancel>) -> Result<Delivery, Failure> {
        let reading = async { self.consumer.next_batch().await.map_err(Failure::from) };
        let read = self.session.block_on(cancel, reading)?;
        let Some(delivery) = read else {
            return Err(Failure::new(FailureKind::Closed, "the consumer was closed"));
        };
        Ok(Delivery::new(
            delivery,
            self.schema.clone(),
            self.session.clone(),
        ))
    }

    /// Closes the consumer and waits until the server released its attachment.
    pub fn close(&self, cancel: Option<&Cancel>) -> Result<(), Failure> {
        let closing = async { self.consumer.close().await.map_err(Failure::from) };
        self.session.block_on(cancel, closing)?;
        Ok(())
    }

    /// Releases the consumer, closing it without waiting when it is still open.
    fn free(self) {
        let Self {
            consumer,
            schema,
            session,
        } = self;
        drop(schema);
        session.release(consumer);
    }
}

/// # Safety
///
/// `session` is a live session; `domain` and `emitter` address their lengths in readable bytes;
/// `fields` is a live field list; a non-null `cancel` is a live token; a non-null `out` is
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_session_subscribe_emitter(
    session: *const Session,
    domain: *const u8,
    domain_len: usize,
    emitter: *const u8,
    emitter_len: usize,
    fields: *const Fields,
    batches: u32,
    bytes: u64,
    cancel: *const Cancel,
    out: *mut *mut Consumer,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe {
        write_opened(
            session,
            domain,
            domain_len,
            emitter,
            emitter_len,
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
/// As [`nx_session_subscribe_emitter`].
#[expect(
    clippy::too_many_arguments,
    reason = "the C ABI passes each string as a pointer and a length"
)]
unsafe fn write_opened(
    session: *const Session,
    domain: *const u8,
    domain_len: usize,
    emitter: *const u8,
    emitter_len: usize,
    fields: *const Fields,
    batches: u32,
    bytes: u64,
    cancel: *const Cancel,
    out: *mut *mut Consumer,
) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live session, readable names, live fields and a live token
    // or null.
    let (session, domain, emitter, fields, cancel) = unsafe {
        (
            abi::handle(session, "session")?,
            abi::text(domain, domain_len, "domain")?,
            abi::text(emitter, emitter_len, "emitter")?,
            abi::handle(fields, "fields")?,
            cancel.as_ref(),
        )
    };
    let (batches, bytes) = credit(batches, bytes)?;
    let limits = ClientConsumerLimits { batches, bytes };
    let consumer = Consumer::open(session, domain, emitter, fields, limits, cancel)?;
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(consumer)) };
    Ok(())
}

/// # Safety
///
/// `consumer` is a live consumer; a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_consumer_schema(
    consumer: *const Consumer,
    out: *mut *mut Schema,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_schema(consumer, out) })
}

/// # Safety
///
/// As [`nx_consumer_schema`].
unsafe fn write_schema(consumer: *const Consumer, out: *mut *mut Schema) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live consumer.
    let schema = unsafe { abi::handle(consumer, "consumer") }?
        .schema()
        .clone();
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(schema)) };
    Ok(())
}

/// # Safety
///
/// `consumer` is a live consumer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_consumer_generation(consumer: *const Consumer) -> u64 {
    // SAFETY: the header requires a live consumer.
    unsafe { abi::accessor(consumer) }.description().generation
}

/// # Safety
///
/// `consumer` is a live consumer; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_consumer_contract(
    consumer: *const Consumer,
    digest: *mut *const u8,
    digest_len: *mut usize,
) {
    // SAFETY: the header requires a live consumer and writable out-parameters.
    unsafe {
        let contract = abi::accessor(consumer).description().contract.as_digest();
        abi::write_bytes(digest, digest_len, contract);
    }
}

/// # Safety
///
/// `consumer` is a live consumer; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_consumer_grant(
    consumer: *const Consumer,
    batches: *mut u32,
    bytes: *mut u64,
    max_batch_rows: *mut u32,
    max_batch_bytes: *mut u64,
) {
    // SAFETY: the header requires a live consumer and writable out-parameters.
    unsafe {
        let description = abi::accessor(consumer).description();
        abi::write(batches, description.granted.batches.get());
        abi::write(bytes, description.granted.bytes.get());
        abi::write(max_batch_rows, description.max_batch_rows);
        abi::write(max_batch_bytes, description.max_batch_bytes);
    }
}

/// # Safety
///
/// `consumer` is a live consumer; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_consumer_policy(
    consumer: *const Consumer,
    window: *mut WindowKind,
    outstanding: *mut u64,
    ack_timeout_nanos: *mut u64,
    retry_backoff_nanos: *mut u64,
    retry_max_backoff_nanos: *mut u64,
) {
    // SAFETY: the header requires a live consumer and writable out-parameters.
    unsafe {
        let description = abi::accessor(consumer).description();
        let acknowledged = Window::from(description.window);
        abi::write(window, acknowledged.kind);
        abi::write(outstanding, acknowledged.outstanding);
        abi::write(ack_timeout_nanos, nanos(description.ack_timeout));
        abi::write(retry_backoff_nanos, nanos(description.retry_backoff));
        abi::write(
            retry_max_backoff_nanos,
            nanos(description.retry_max_backoff),
        );
    }
}

/// # Safety
///
/// `consumer` is a live consumer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_consumer_state(consumer: *const Consumer) -> EndpointState {
    // SAFETY: the header requires a live consumer.
    unsafe { abi::accessor(consumer) }.state()
}

/// # Safety
///
/// `consumer` is a live consumer; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_consumer_reopen_reason(
    consumer: *const Consumer,
    reason: *mut ReopenReason,
    refusal: *mut OpenRefusal,
) -> bool {
    // SAFETY: the header requires a live consumer.
    let Some(reopen) = unsafe { abi::accessor(consumer) }.reopen() else {
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
/// `consumer` is live, a non-null `cancel` is a live token, and a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_consumer_next(
    consumer: *const Consumer,
    cancel: *const Cancel,
    out: *mut *mut Delivery,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_next(consumer, cancel, out) })
}

/// # Safety
///
/// As [`nx_consumer_next`].
unsafe fn write_next(
    consumer: *const Consumer,
    cancel: *const Cancel,
    out: *mut *mut Delivery,
) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live consumer and a live token or null.
    let (consumer, cancel) = unsafe { (abi::handle(consumer, "consumer")?, cancel.as_ref()) };
    let delivery = consumer.next(cancel)?;
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, delivery.into_shared()) };
    Ok(())
}

/// # Safety
///
/// `consumer` is live and a non-null `cancel` is a live token.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_consumer_close(
    consumer: *const Consumer,
    cancel: *const Cancel,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { closed(consumer, cancel) })
}

/// # Safety
///
/// As [`nx_consumer_close`].
unsafe fn closed(consumer: *const Consumer, cancel: *const Cancel) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live consumer and a live token or null.
    let (consumer, cancel) = unsafe { (abi::handle(consumer, "consumer")?, cancel.as_ref()) };
    consumer.close(cancel)
}

/// # Safety
///
/// A non-null `consumer` is a consumer this library returned that has not been freed, and no
/// other thread is using it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_consumer_free(consumer: *mut Consumer) {
    if consumer.is_null() {
        return;
    }
    // SAFETY: the header requires an unreleased consumer no other thread uses.
    let consumer = unsafe { Box::from_raw(consumer) };
    consumer.free();
}
