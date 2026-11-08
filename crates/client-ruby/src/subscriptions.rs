//! Ruby subscription events and their verified row frames.
//!
//! Layer: edges.
//! - **Owns.** Event metadata, typed/bulk columns and immutable owner-retaining frame views.
//! - **Depends on.** The shared host event, schema vocabulary and Ruby value projection.
//! - **Must not know.** Subscription routing, event queues, restoration or the server.

use std::time::Duration;

use magnus::{
    DataTypeFunctions, Error, RArray, RModule, RString, Ruby, TypedData, Value, method, prelude::*,
    scan_args::scan_args, typed_data::Obj,
};
use meticulous::ResultExt as _;
use nervix_client_core::{SubscriptionEvent, SubscriptionHandle};
use nervix_client_host as host;

use crate::{enums, errors, schema::Schema, values};

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Subscription", free_immediately)]
pub(crate) struct Subscription {
    name: String,
    generation: u64,
}

impl Subscription {
    pub(crate) fn new(handle: &SubscriptionHandle) -> Self {
        Self {
            name: handle.name.as_str().to_owned(),
            generation: handle.generation.get(),
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn generation(&self) -> u64 {
        self.generation
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::RestorationFailure", free_immediately)]
pub(crate) struct RestorationFailure {
    cause: host::RestorationCause,
    message: String,
    retry_after: Duration,
}

impl RestorationFailure {
    pub(crate) fn new(cause: host::RestorationCause, message: &str, retry_after: Duration) -> Self {
        Self {
            cause,
            message: message.to_owned(),
            retry_after,
        }
    }
    fn cause(&self) -> i32 {
        enums::RestorationCause::code(self.cause)
    }
    fn message(&self) -> &str {
        &self.message
    }
    fn retry_after_nanos(&self) -> u64 {
        host::nanos(self.retry_after)
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Event", free_immediately)]
pub(crate) struct Event {
    event: host::Event,
}

impl Event {
    pub(crate) fn new(event: host::Event) -> Self {
        Self { event }
    }
    fn kind(&self) -> i32 {
        enums::EventKind::code(self.event.kind())
    }
    fn row_count(&self) -> u64 {
        self.event.row_count()
    }
    fn subscription(&self) -> Subscription {
        Subscription::new(self.event.subscription_event().subscription())
    }
    fn end_reason(&self) -> Result<i32, Error> {
        let reason = self.event.end_reason().map_err(errors::raise)?;
        Ok(enums::EndReason::code(reason))
    }
    fn restoration_failure(&self) -> Option<RestorationFailure> {
        let SubscriptionEvent::RestorationFailed(failure) = self.event.subscription_event() else {
            return None;
        };
        Some(RestorationFailure::new(
            failure.cause.into(),
            &failure.message,
            failure.retry_after,
        ))
    }
    fn schema(&self) -> Result<Schema, Error> {
        Ok(Schema::new(self.event.schema().map_err(errors::raise)?))
    }
    fn frame(this: Obj<Self>) -> Result<Value, Error> {
        let bytes = this.event.frame().map_err(errors::raise)?;
        // SAFETY: This immutable event owns its verified frame and cannot release it explicitly.
        // The buffer keeps this exact Ruby object alive until its last derived view is gone.
        unsafe { values::buffer(this.as_value(), bytes) }
    }
    fn column_args(args: &[Value]) -> Result<(usize, host::Part), Error> {
        let args = scan_args::<(usize,), (Option<i32>,), (), (), (), ()>(args)?;
        let (column,) = args.required;
        let (part,) = args.optional;
        let part = part.unwrap_or(enums::Part::code(host::Part::Rows));
        let part = host::Part::from_repr(part)
            .ok_or_else(|| errors::invalid("part", "is not ROWS or BRANCH_KEY"))?;
        Ok((column, part))
    }
    fn column(&self, args: &[Value]) -> Result<RArray, Error> {
        let (column, part) = Self::column_args(args)?;
        let cells = self.event.cells(part, column).map_err(errors::raise)?;
        let ruby = Ruby::get().assured("a native Ruby method holds the GVL");
        let values = ruby.ary_new_capa(cells.len());
        for cell in cells {
            values.push(values::cell(&ruby, cell)?)?;
        }
        Ok(values)
    }
    fn states(&self, args: &[Value]) -> Result<Vec<u8>, Error> {
        let (column, part) = Self::column_args(args)?;
        let cells = self.event.cells(part, column).map_err(errors::raise)?;
        let mut states = Vec::with_capacity(cells.len());
        for cell in cells {
            states.push(u8::from(host::CellState::from(cell)));
        }
        Ok(states)
    }
    fn fixed(&self, args: &[Value]) -> Result<RString, Error> {
        let (column, part) = Self::column_args(args)?;
        let len = self.event.fixed_len(part, column).map_err(errors::raise)?;
        // SAFETY: The immutable host accessor only fills bytes and the GVL stays held.
        unsafe { values::binary(len, |output| self.event.column_fixed(part, column, output)) }
    }
    fn varlen(&self, args: &[Value]) -> Result<(Vec<u64>, RString), Error> {
        let (column, part) = Self::column_args(args)?;
        let cells = self.event.cells(part, column).map_err(errors::raise)?;
        let ends = cells
            .len()
            .checked_add(1)
            .ok_or_else(|| errors::invalid("column", "overflows"))?;
        let len = self
            .event
            .column_varlen_len(part, column)
            .map_err(errors::raise)?;
        let mut offsets = vec![0; ends];
        // SAFETY: The host fills these owned outputs synchronously without calling Ruby.
        let bytes = unsafe {
            values::binary(len, |output| {
                self.event.column_varlen(part, column, &mut offsets, output)
            })
        }?;
        Ok((offsets, bytes))
    }
}

pub(crate) fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
    let subscription = crate::class(ruby, module, "Subscription")?;
    subscription.define_method("name", method!(Subscription::name, 0))?;
    subscription.define_method("generation", method!(Subscription::generation, 0))?;
    let failure = crate::class(ruby, module, "RestorationFailure")?;
    failure.define_method("cause", method!(RestorationFailure::cause, 0))?;
    failure.define_method("message", method!(RestorationFailure::message, 0))?;
    failure.define_method(
        "retry_after_nanos",
        method!(RestorationFailure::retry_after_nanos, 0),
    )?;
    let event = crate::class(ruby, module, "Event")?;
    event.define_method("kind", method!(Event::kind, 0))?;
    event.define_method("row_count", method!(Event::row_count, 0))?;
    event.define_method("subscription", method!(Event::subscription, 0))?;
    event.define_method("end_reason", method!(Event::end_reason, 0))?;
    event.define_method(
        "restoration_failure",
        method!(Event::restoration_failure, 0),
    )?;
    event.define_method("schema", method!(Event::schema, 0))?;
    event.define_method("frame", method!(Event::frame, 0))?;
    event.define_method("column", method!(Event::column, -1))?;
    event.define_method("states", method!(Event::states, -1))?;
    event.define_method("fixed", method!(Event::fixed, -1))?;
    event.define_method("varlen", method!(Event::varlen, -1))?;
    Ok(())
}
