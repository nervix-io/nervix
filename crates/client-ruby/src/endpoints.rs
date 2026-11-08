//! Ruby producers, consumers, deliveries and their outcomes.
//!
//! Layer: edges.
//! - **Owns.** Exact endpoint opens, owned blocking inputs, metadata and settlement projections.
//! - **Depends on.** The host endpoint owners, typed batches, schemas and Ruby cancellation.
//! - **Must not know.** Routing, credit accounting, retries, restoration or acknowledgement policy.

use std::time::Duration;

use magnus::{
    DataTypeFunctions, Error, RArray, RClass, RHash, RModule, RString, Ruby, TypedData, Value,
    method,
    prelude::*,
    scan_args::{get_kwargs, scan_args},
    typed_data::Obj,
};
use meticulous::ResultExt as _;
use nervix_client_core::{AckWindow, ClientConsumerLimits, ClientProducerLimits};
use nervix_client_host as host;
use nervix_primitives::sync::Arc;

use crate::{
    batches::Batch,
    enums, errors, gvl,
    schema::{Fields, Schema},
    sessions::{Cancel, Session},
    values,
};

struct OpenArguments {
    domain: String,
    entity: String,
    fields: Arc<host::Fields>,
    batches: u32,
    bytes: u64,
    cancel: Option<host::Cancel>,
}

impl OpenArguments {
    fn parse(args: &[Value]) -> Result<Self, Error> {
        let args = scan_args::<(String, String, Obj<Fields>), (), (), (), RHash, ()>(args)?;
        let (domain, entity, fields) = args.required;
        let fields = fields.snapshot()?;
        let kwargs = get_kwargs::<_, (u32, u64), (Option<Option<Obj<Cancel>>>,), ()>(
            args.keywords,
            &["batches", "bytes"],
            &["cancel"],
        )?;
        let (batches, bytes) = kwargs.required;
        let cancel = kwargs.optional.0.flatten().map(|cancel| cancel.child());
        Ok(Self {
            domain,
            entity,
            fields,
            batches,
            bytes,
            cancel,
        })
    }
}

impl Session {
    fn open_ingestor(&self, args: &[Value]) -> Result<Producer, Error> {
        let OpenArguments {
            domain,
            entity,
            fields,
            batches,
            bytes,
            cancel,
        } = OpenArguments::parse(args)?;
        let (batches, bytes) = host::credit(batches, bytes).map_err(errors::raise)?;
        let limits = ClientProducerLimits { batches, bytes };
        let token = cancel;
        let session = self.held()?;
        let producer = gvl::without_gvl(token, |cancel| {
            host::Producer::open(&session, &domain, &entity, &fields, limits, Some(cancel))
        })?;
        Ok(Producer {
            producer: Arc::new(producer),
        })
    }
    fn subscribe_emitter(&self, args: &[Value]) -> Result<Consumer, Error> {
        let OpenArguments {
            domain,
            entity,
            fields,
            batches,
            bytes,
            cancel,
        } = OpenArguments::parse(args)?;
        let (batches, bytes) = host::credit(batches, bytes).map_err(errors::raise)?;
        let limits = ClientConsumerLimits { batches, bytes };
        let token = cancel;
        let session = self.held()?;
        let consumer = gvl::without_gvl(token, |cancel| {
            host::Consumer::open(&session, &domain, &entity, &fields, limits, Some(cancel))
        })?;
        Ok(Consumer {
            consumer: Arc::new(consumer),
        })
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Grant", free_immediately)]
struct Grant {
    batches: u32,
    bytes: u64,
    max_batch_rows: u32,
    max_batch_bytes: u64,
}
impl Grant {
    fn batches(&self) -> u32 {
        self.batches
    }
    fn bytes(&self) -> u64 {
        self.bytes
    }
    fn max_batch_rows(&self) -> u32 {
        self.max_batch_rows
    }
    fn max_batch_bytes(&self) -> u64 {
        self.max_batch_bytes
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Policy", free_immediately)]
struct Policy {
    window: host::Window,
    ack_timeout: u64,
    retry_backoff: u64,
    retry_max_backoff: u64,
}
impl Policy {
    fn new(
        window: AckWindow,
        ack_timeout: Duration,
        retry_backoff: Duration,
        retry_max_backoff: Duration,
    ) -> Self {
        Self {
            window: window.into(),
            ack_timeout: host::nanos(ack_timeout),
            retry_backoff: host::nanos(retry_backoff),
            retry_max_backoff: host::nanos(retry_max_backoff),
        }
    }
    fn window(&self) -> i32 {
        enums::WindowKind::code(self.window.kind)
    }
    fn outstanding(&self) -> u64 {
        self.window.outstanding
    }
    fn ack_timeout_nanos(&self) -> u64 {
        self.ack_timeout
    }
    fn retry_backoff_nanos(&self) -> u64 {
        self.retry_backoff
    }
    fn retry_max_backoff_nanos(&self) -> u64 {
        self.retry_max_backoff
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Reopen", free_immediately)]
struct Reopen {
    reopen: host::Reopen,
}
impl Reopen {
    fn new(reopen: host::Reopen) -> Self {
        Self { reopen }
    }
    fn reason(&self) -> i32 {
        enums::ReopenReason::code(self.reopen.reason())
    }
    fn refusal(&self) -> Option<i32> {
        self.reopen.refusal().map(enums::OpenRefusal::code)
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::PendingSubmission", free_immediately)]
struct PendingSubmission {
    id: u64,
    resolved: bool,
}
impl PendingSubmission {
    fn id(&self) -> u64 {
        self.id
    }
    fn resolved(&self) -> bool {
        self.resolved
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::SubmissionOutcome", free_immediately)]
struct SubmissionOutcome {
    outcome: host::SubmissionOutcome,
}
impl SubmissionOutcome {
    fn new(outcome: host::SubmissionOutcome) -> Self {
        Self { outcome }
    }
    fn result(&self) -> i32 {
        enums::SubmissionResult::code(self.outcome.result())
    }
    fn message(&self) -> &str {
        self.outcome.message()
    }
    fn refusal(&self) -> Result<i32, Error> {
        Ok(enums::SubmissionRefusal::code(
            self.outcome.refusal().map_err(errors::raise)?,
        ))
    }
    fn defect(&self) -> Result<i32, Error> {
        Ok(enums::BatchDefect::code(
            self.outcome.defect().map_err(errors::raise)?,
        ))
    }
    fn failure(&self) -> Result<i32, Error> {
        Ok(enums::ProcessingFailure::code(
            self.outcome.failure().map_err(errors::raise)?,
        ))
    }
    fn uncertainty(&self) -> Result<i32, Error> {
        Ok(enums::Uncertainty::code(
            self.outcome.uncertainty().map_err(errors::raise)?,
        ))
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Producer", free_immediately)]
pub(crate) struct Producer {
    // Every admitted wait takes its own owner before releasing the GVL.
    producer: Arc<host::Producer>,
}
impl Producer {
    fn schema(&self) -> Schema {
        Schema::new(self.producer.schema().clone())
    }
    fn state(&self) -> i32 {
        enums::EndpointState::code(self.producer.state())
    }
    fn admission(&self) -> i32 {
        enums::Admission::code(self.producer.admission())
    }
    fn generation(&self) -> u64 {
        self.producer.description().generation
    }
    fn contract(&self) -> RString {
        Ruby::get()
            .assured("a native Ruby method holds the GVL")
            .str_from_slice(self.producer.description().contract.as_digest())
    }
    fn grant(&self) -> Grant {
        let grant = self.producer.description().grant;
        Grant {
            batches: grant.batches.get(),
            bytes: grant.bytes.get(),
            max_batch_rows: grant.max_batch_rows.get(),
            max_batch_bytes: grant.max_batch_bytes.get(),
        }
    }
    fn policy(&self) -> Policy {
        let policy = self.producer.description().policy;
        Policy::new(
            policy.window,
            policy.ack_timeout,
            policy.retry_backoff,
            policy.retry_max_backoff,
        )
    }
    fn reopen(&self) -> Option<Reopen> {
        self.producer.reopen().map(Reopen::new)
    }
    fn submit(&self, args: &[Value]) -> Result<u64, Error> {
        let args = scan_args::<(Obj<Batch>,), (), (), (), RHash, ()>(args)?;
        let (batch,) = args.required;
        let batch = batch.batch.clone();
        let token = Cancel::keyword(args.keywords)?;
        let producer = self.producer.clone();
        gvl::without_gvl(token, |cancel| producer.submit(&batch, Some(cancel)))
    }
    fn submit_ipc(&self, args: &[Value]) -> Result<u64, Error> {
        let args = scan_args::<(RString,), (), (), (), RHash, ()>(args)?;
        let (ipc,) = args.required;
        let bytes = values::bytes(ipc);
        let token = Cancel::keyword(args.keywords)?;
        let producer = self.producer.clone();
        gvl::without_gvl(token, |cancel| producer.submit_ipc(&bytes, Some(cancel)))
    }
    fn rejoin(&self, args: &[Value]) -> Result<SubmissionOutcome, Error> {
        let args = scan_args::<(u64,), (), (), (), RHash, ()>(args)?;
        let (id,) = args.required;
        let token = Cancel::keyword(args.keywords)?;
        let producer = self.producer.clone();
        let outcome = gvl::without_gvl(token, |cancel| producer.rejoin(id, Some(cancel)))?;
        Ok(SubmissionOutcome::new(outcome))
    }
    fn release(&self, id: u64) -> Result<Option<SubmissionOutcome>, Error> {
        Ok(self
            .producer
            .release(id)
            .map_err(errors::raise)?
            .map(SubmissionOutcome::new))
    }
    fn pending(&self) -> RArray {
        let ruby = Ruby::get().assured("a native Ruby method holds the GVL");
        ruby.ary_from_iter(
            self.producer
                .pending_submissions()
                .iter()
                .map(|submission| PendingSubmission {
                    id: submission.id.get().get(),
                    resolved: submission.outcome.is_some(),
                }),
        )
    }
    fn close(&self, args: &[Value]) -> Result<(), Error> {
        let args = scan_args::<(), (), (), (), RHash, ()>(args)?;
        let token = Cancel::keyword(args.keywords)?;
        let producer = self.producer.clone();
        gvl::without_gvl(token, |cancel| producer.close(Some(cancel)))
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Consumer", free_immediately)]
pub(crate) struct Consumer {
    consumer: Arc<host::Consumer>,
}
impl Consumer {
    fn schema(&self) -> Schema {
        Schema::new(self.consumer.schema().clone())
    }
    fn state(&self) -> i32 {
        enums::EndpointState::code(self.consumer.state())
    }
    fn generation(&self) -> u64 {
        self.consumer.description().generation
    }
    fn contract(&self) -> RString {
        Ruby::get()
            .assured("a native Ruby method holds the GVL")
            .str_from_slice(self.consumer.description().contract.as_digest())
    }
    fn grant(&self) -> Grant {
        let description = self.consumer.description();
        Grant {
            batches: description.granted.batches.get(),
            bytes: description.granted.bytes.get(),
            max_batch_rows: description.max_batch_rows,
            max_batch_bytes: description.max_batch_bytes,
        }
    }
    fn policy(&self) -> Policy {
        let description = self.consumer.description();
        Policy::new(
            description.window,
            description.ack_timeout,
            description.retry_backoff,
            description.retry_max_backoff,
        )
    }
    fn reopen(&self) -> Option<Reopen> {
        self.consumer.reopen().map(Reopen::new)
    }
    fn next(&self, args: &[Value]) -> Result<Delivery, Error> {
        let args = scan_args::<(), (), (), (), RHash, ()>(args)?;
        let token = Cancel::keyword(args.keywords)?;
        let consumer = self.consumer.clone();
        let delivery = gvl::without_gvl(token, |cancel| consumer.next(Some(cancel)))?;
        Ok(Delivery {
            delivery: Arc::new(delivery),
        })
    }
    fn close(&self, args: &[Value]) -> Result<(), Error> {
        let args = scan_args::<(), (), (), (), RHash, ()>(args)?;
        let token = Cancel::keyword(args.keywords)?;
        let consumer = self.consumer.clone();
        gvl::without_gvl(token, |cancel| consumer.close(Some(cancel)))
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Delivery", free_immediately)]
struct Delivery {
    delivery: Arc<host::Delivery>,
}
impl Delivery {
    fn identity(&self) -> RString {
        Ruby::get()
            .assured("a native Ruby method holds the GVL")
            .str_from_slice(self.delivery.identity())
    }
    fn reference(&self) -> RString {
        Ruby::get()
            .assured("a native Ruby method holds the GVL")
            .str_from_slice(self.delivery.reference())
    }
    fn source_relay(&self) -> &str {
        self.delivery.source_relay()
    }
    fn branch_fingerprint(&self) -> Option<RString> {
        let fingerprint = self.delivery.branch_fingerprint()?;
        Some(
            Ruby::get()
                .assured("a native Ruby method holds the GVL")
                .str_from_slice(fingerprint),
        )
    }
    fn members(&self) -> u32 {
        self.delivery.members()
    }
    fn execution_now(&self) -> i64 {
        self.delivery.execution_now()
    }
    fn ipc(this: Obj<Self>) -> Result<Value, Error> {
        // SAFETY: Delivery has no explicit release operation. This exact Ruby object owns the
        // immutable attempt until the buffer and its slices release their reference to it.
        unsafe { values::buffer(this.as_value(), this.delivery.ipc()) }
    }
    fn batch(&self) -> Result<Batch, Error> {
        let delivery = self.delivery.clone();
        let batch = gvl::without_gvl(None, |_| delivery.batch().cloned())?;
        Ok(Batch::new(batch))
    }
    fn ack(&self, args: &[Value]) -> Result<i32, Error> {
        let args = scan_args::<(), (), (), (), RHash, ()>(args)?;
        let token = Cancel::keyword(args.keywords)?;
        let delivery = self.delivery.clone();
        let result = gvl::without_gvl(token, |cancel| delivery.ack(Some(cancel)))?;
        Ok(enums::Settlement::code(result))
    }
    fn retry(&self, args: &[Value]) -> Result<i32, Error> {
        let args = scan_args::<(), (), (), (), RHash, ()>(args)?;
        let token = Cancel::keyword(args.keywords)?;
        let delivery = self.delivery.clone();
        let result = gvl::without_gvl(token, |cancel| delivery.retry(Some(cancel)))?;
        Ok(enums::Settlement::code(result))
    }
    fn reject(&self, args: &[Value]) -> Result<i32, Error> {
        let args = scan_args::<(String,), (), (), (), RHash, ()>(args)?;
        let (reason,) = args.required;
        let token = Cancel::keyword(args.keywords)?;
        let delivery = self.delivery.clone();
        let result = gvl::without_gvl(token, |cancel| delivery.reject(&reason, Some(cancel)))?;
        Ok(enums::Settlement::code(result))
    }
}

pub(crate) fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
    let session: RClass = module.const_get("Session")?;
    session.define_method("open_ingestor", method!(Session::open_ingestor, -1))?;
    session.define_method("subscribe_emitter", method!(Session::subscribe_emitter, -1))?;
    let grant = crate::class(ruby, module, "Grant")?;
    grant.define_method("batches", method!(Grant::batches, 0))?;
    grant.define_method("bytes", method!(Grant::bytes, 0))?;
    grant.define_method("max_batch_rows", method!(Grant::max_batch_rows, 0))?;
    grant.define_method("max_batch_bytes", method!(Grant::max_batch_bytes, 0))?;
    let policy = crate::class(ruby, module, "Policy")?;
    policy.define_method("window", method!(Policy::window, 0))?;
    policy.define_method("outstanding", method!(Policy::outstanding, 0))?;
    policy.define_method("ack_timeout_nanos", method!(Policy::ack_timeout_nanos, 0))?;
    policy.define_method(
        "retry_backoff_nanos",
        method!(Policy::retry_backoff_nanos, 0),
    )?;
    policy.define_method(
        "retry_max_backoff_nanos",
        method!(Policy::retry_max_backoff_nanos, 0),
    )?;
    let reopen = crate::class(ruby, module, "Reopen")?;
    reopen.define_method("reason", method!(Reopen::reason, 0))?;
    reopen.define_method("refusal", method!(Reopen::refusal, 0))?;
    let pending = crate::class(ruby, module, "PendingSubmission")?;
    pending.define_method("id", method!(PendingSubmission::id, 0))?;
    pending.define_method("resolved", method!(PendingSubmission::resolved, 0))?;
    let outcome = crate::class(ruby, module, "SubmissionOutcome")?;
    outcome.define_method("result", method!(SubmissionOutcome::result, 0))?;
    outcome.define_method("message", method!(SubmissionOutcome::message, 0))?;
    outcome.define_method("refusal", method!(SubmissionOutcome::refusal, 0))?;
    outcome.define_method("defect", method!(SubmissionOutcome::defect, 0))?;
    outcome.define_method("failure", method!(SubmissionOutcome::failure, 0))?;
    outcome.define_method("uncertainty", method!(SubmissionOutcome::uncertainty, 0))?;
    let producer = crate::class(ruby, module, "Producer")?;
    producer.define_method("schema", method!(Producer::schema, 0))?;
    producer.define_method("state", method!(Producer::state, 0))?;
    producer.define_method("admission", method!(Producer::admission, 0))?;
    producer.define_method("generation", method!(Producer::generation, 0))?;
    producer.define_method("contract", method!(Producer::contract, 0))?;
    producer.define_method("grant", method!(Producer::grant, 0))?;
    producer.define_method("policy", method!(Producer::policy, 0))?;
    producer.define_method("reopen", method!(Producer::reopen, 0))?;
    producer.define_method("submit", method!(Producer::submit, -1))?;
    producer.define_method("submit_ipc", method!(Producer::submit_ipc, -1))?;
    producer.define_method("rejoin", method!(Producer::rejoin, -1))?;
    producer.define_method("release", method!(Producer::release, 1))?;
    producer.define_method("pending", method!(Producer::pending, 0))?;
    producer.define_method("close", method!(Producer::close, -1))?;
    let consumer = crate::class(ruby, module, "Consumer")?;
    consumer.define_method("schema", method!(Consumer::schema, 0))?;
    consumer.define_method("state", method!(Consumer::state, 0))?;
    consumer.define_method("generation", method!(Consumer::generation, 0))?;
    consumer.define_method("contract", method!(Consumer::contract, 0))?;
    consumer.define_method("grant", method!(Consumer::grant, 0))?;
    consumer.define_method("policy", method!(Consumer::policy, 0))?;
    consumer.define_method("reopen", method!(Consumer::reopen, 0))?;
    consumer.define_method("next", method!(Consumer::next, -1))?;
    consumer.define_method("close", method!(Consumer::close, -1))?;
    let delivery = crate::class(ruby, module, "Delivery")?;
    delivery.define_method("identity", method!(Delivery::identity, 0))?;
    delivery.define_method("reference", method!(Delivery::reference, 0))?;
    delivery.define_method("source_relay", method!(Delivery::source_relay, 0))?;
    delivery.define_method(
        "branch_fingerprint",
        method!(Delivery::branch_fingerprint, 0),
    )?;
    delivery.define_method("members", method!(Delivery::members, 0))?;
    delivery.define_method("execution_now", method!(Delivery::execution_now, 0))?;
    delivery.define_method("ipc", method!(Delivery::ipc, 0))?;
    delivery.define_method("batch", method!(Delivery::batch, 0))?;
    delivery.define_method("ack", method!(Delivery::ack, -1))?;
    delivery.define_method("retry", method!(Delivery::retry, -1))?;
    delivery.define_method("reject", method!(Delivery::reject, -1))?;
    Ok(())
}
