//! A consumer a host holds, opened on a session's client emitter.
//!
//! - **Owns.** Opening a consumer with the fields and credit a host asks for, waiting for the
//!   next delivery, reading its description and state, and closing and releasing it.
//! - **Depends on.** The Rust client's emitter consumer, the session it was opened on, the field
//!   descriptions and deliveries, and the endpoint vocabulary.
//! - **Must not know.** How output is assigned, redelivered or restored; the Rust consumer decides
//!   all of it, so this module keeps no state of its own about a delivery or an attachment.

use meticulous::OptionExt as _;
use nervix_client_core::{
    ClientConsumerLimits, DomainName, EmitterConsumer, EmitterName, wire::EmitterOpened,
};
use nervix_primitives::sync::Arc;

use crate::{
    cancel::Cancel,
    delivery::Delivery,
    endpoint::{EndpointState, Reopen},
    failure::{Failure, FailureKind},
    fields::Fields,
    schema::Schema,
    session::{Session, SessionRuntime},
};

/// A consumer, with the session its blocking calls run on and the schema of its batches.
pub struct Consumer {
    /// Taken only when the consumer is dropped, which releases it inside its session's runtime.
    consumer: Option<EmitterConsumer>,
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
            consumer: Some(consumer),
            schema,
            session: shared,
        })
    }

    pub fn description(&self) -> &EmitterOpened {
        self.consumer().description()
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn state(&self) -> EndpointState {
        EndpointState::from(self.consumer().connection())
    }

    /// Why the consumer has to be opened again, once it does.
    pub fn reopen(&self) -> Option<Reopen> {
        self.consumer().reopen_reason().as_ref().map(Reopen::from)
    }

    /// Waits for the next delivery. A read the wait abandons stays with the consumer, and a later
    /// call receives what it reads.
    pub fn next(&self, cancel: Option<&Cancel>) -> Result<Delivery, Failure> {
        let reading = async { self.consumer().next_batch().await.map_err(Failure::from) };
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
        let closing = async { self.consumer().close().await.map_err(Failure::from) };
        self.session.block_on(cancel, closing)?;
        Ok(())
    }

    fn consumer(&self) -> &EmitterConsumer {
        self.consumer
            .as_ref()
            .assured("the consumer is taken only when it is dropped")
    }
}

impl Drop for Consumer {
    /// Releases the consumer inside its session's runtime, closing it without waiting when it is
    /// still open, so a host's finalizer never blocks on it.
    fn drop(&mut self) {
        if let Some(consumer) = self.consumer.take() {
            self.session.release(consumer);
        }
    }
}
