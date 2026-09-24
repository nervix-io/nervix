//! The session subscribers of one relay and the definition they were attached under.
//!
//! Layer: data plane.
//!
//! - **Owns.** The subscription fan-out of one relay, the row definition its subscribers were
//!   attached under, and ending every subscriber before a batch of any other definition can reach
//!   it.
//! - **Depends on.** The relay fan-out channel, compiled schemas, and the resolved branching the
//!   vocabulary describes.
//! - **Must not know.** Sessions, clients, the wire encoding of rows, or why a relay was redefined.
//!
//! A subscriber announces the rows it delivers once, when it opens, and reads every later batch
//! positionally against that announcement. The relay fan-out outlives rebuilds, so without a fence
//! a relay redeclared with other fields, another branching, or a field that became sensitive would
//! hand its new batches to subscribers that describe and redact them by the old definition.
//! Declaring a different definition, or withdrawing the relay, therefore closes every subscriber
//! first; each drains what it already received and then sees its receiver end.

use std::sync::atomic::fence;

use super::*;

/// What the rows a subscriber announces depend on: the relay's payload fields with their types,
/// nullability and sensitivity, and its branching with the fields of its branch key.
#[derive(Debug, Clone)]
pub(crate) struct RelaySubscriptionDefinition {
    payload: Arc<CompiledSchema>,
    branching: ResolvedBranching,
}

impl RelaySubscriptionDefinition {
    pub(crate) fn new(payload: Arc<CompiledSchema>, branching: ResolvedBranching) -> Self {
        Self { payload, branching }
    }

    pub(in crate::runtime) fn is_branched(&self) -> bool {
        !self.branching.is_unbranched()
    }
}

impl PartialEq for RelaySubscriptionDefinition {
    fn eq(&self, other: &Self) -> bool {
        *self.payload == *other.payload && self.branching == other.branching
    }
}

/// Why a subscriber could not attach to a relay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum RelaySubscriptionRefusal {
    /// This node does not declare the relay.
    NotDeclared,
    /// The relay is declared with a different definition than the subscriber describes, or it was
    /// redeclared while the subscriber attached.
    Redefined,
}

/// The session subscribers of one relay.
#[derive(Debug)]
pub(in crate::runtime) struct RelaySubscriptions {
    receivers: RelayBroadcast<RelayRecordBatch>,
    /// The definition every current subscriber was attached under. `None` while this node does
    /// not declare the relay, when no subscriber can attach.
    definition: ArcSwapOption<RelaySubscriptionDefinition>,
    /// Advances each time the definition changes, before the subscribers attached under the
    /// previous one are closed.
    ///
    /// An attachment reads it before it checks the definition and again after it registers. A
    /// change and an attachment each put a sequentially consistent fence between their own write
    /// and the read of the other's, so at least one of them observes the other: either the change
    /// closes the new receiver, or the attachment sees the epoch move and gives the receiver back.
    epoch: AtomicU64,
}

impl RelaySubscriptions {
    pub(in crate::runtime) fn new() -> Self {
        Self {
            receivers: RelayBroadcast::with_capacity(NonZeroUsize::MIN),
            definition: ArcSwapOption::empty(),
            epoch: AtomicU64::new(0),
        }
    }

    /// Attaches a subscriber that describes the relay by `expected`.
    pub(in crate::runtime) fn attach(
        &self,
        expected: &RelaySubscriptionDefinition,
    ) -> Result<RelaySubscriptionReceiver<RelayRecordBatch>, RelaySubscriptionRefusal> {
        let epoch = self.epoch.load(Ordering::SeqCst);
        let declared = self.definition.load();
        let Some(declared) = declared.as_deref() else {
            return Err(RelaySubscriptionRefusal::NotDeclared);
        };
        if declared != expected {
            return Err(RelaySubscriptionRefusal::Redefined);
        }
        let receiver = self.receivers.new_receiver();
        fence(Ordering::SeqCst);
        if self.epoch.load(Ordering::SeqCst) != epoch {
            return Err(RelaySubscriptionRefusal::Redefined);
        }
        Ok(receiver)
    }

    /// Declares the definition the relay executes with on this node. A definition that differs
    /// from the one the current subscribers were attached under closes all of them.
    pub(in crate::runtime) fn declare(&self, definition: RelaySubscriptionDefinition) {
        let declared = self.definition.load();
        if declared.as_deref() == Some(&definition) {
            return;
        }
        self.definition.store(Some(StdArc::new(definition)));
        self.close_attached();
    }

    /// Records that this node no longer declares the relay, and closes every subscriber.
    pub(in crate::runtime) fn withdraw(&self) {
        if self.definition.load().is_none() {
            return;
        }
        self.definition.store(None);
        self.close_attached();
    }

    fn close_attached(&self) {
        self.epoch
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |epoch| {
                epoch.checked_add(1)
            })
            .assured("a relay is not redeclared 2^64 times on one node");
        fence(Ordering::SeqCst);
        self.receivers.close_receivers();
    }

    pub(in crate::runtime) fn receiver_count(&self) -> usize {
        self.receivers.receiver_count()
    }

    pub(in crate::runtime) async fn broadcast(&self, batch: RelayRecordBatch) {
        self.receivers
            .broadcast(batch)
            .await
            .means_peer_left("relay subscription");
    }

    /// The fan-out itself, for tests that attach receivers without a declared definition or
    /// observe its capacity and waiting publishers.
    #[cfg(test)]
    pub(in crate::runtime) fn receivers(&self) -> &RelayBroadcast<RelayRecordBatch> {
        &self.receivers
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{CreateSchema, SchemaField};

    use super::*;

    fn definition(sensitive: bool) -> RelaySubscriptionDefinition {
        let schema = CreateSchema {
            name: named("event"),
            fields: vec![SchemaField {
                name: named("secret"),
                ty: ParseAsType::String,
                optional: false,
                sensitive,
            }],
        };
        RelaySubscriptionDefinition::new(
            Arc::new(compile_schema(&schema)),
            ResolvedBranching::unbranched(),
        )
    }

    fn batch(value: &str) -> RelayRecordBatch {
        let schema = test_schema(&[("secret", ParseAsType::String)]);
        RelayRecordBatch::single(
            schema,
            None,
            test_runtime_row([(
                "secret".to_string(),
                RuntimeValue::String(value.to_string()),
            )]),
            AckSet::empty(),
        )
        .assured("a one-field test row matches its one-field test schema")
    }

    #[tokio::test]
    async fn a_subscriber_attaches_only_under_the_declared_definition() {
        let subscriptions = RelaySubscriptions::new();
        assert_eq!(
            subscriptions.attach(&definition(false)).err(),
            Some(RelaySubscriptionRefusal::NotDeclared)
        );

        subscriptions.declare(definition(false));
        assert_eq!(
            subscriptions.attach(&definition(true)).err(),
            Some(RelaySubscriptionRefusal::Redefined),
            "a subscriber describing the field as public cannot attach once it is sensitive"
        );
        let mut attached = subscriptions
            .attach(&definition(false))
            .assured("the subscriber describes the declared definition");
        subscriptions.broadcast(batch("first")).await;
        assert!(attached.recv().await.is_some());
    }

    #[tokio::test]
    async fn redeclaring_the_same_definition_keeps_subscribers_attached() {
        let subscriptions = RelaySubscriptions::new();
        subscriptions.declare(definition(false));
        let mut attached = subscriptions
            .attach(&definition(false))
            .assured("the subscriber describes the declared definition");

        subscriptions.declare(definition(false));
        subscriptions.broadcast(batch("after a rebuild")).await;
        assert!(
            attached.recv().await.is_some(),
            "a rebuild that keeps the definition keeps its subscribers"
        );
    }

    #[tokio::test]
    async fn a_new_definition_closes_subscribers_before_its_first_batch() {
        let subscriptions = RelaySubscriptions::new();
        subscriptions.declare(definition(false));
        let mut attached = subscriptions
            .attach(&definition(false))
            .assured("the subscriber describes the declared definition");
        subscriptions.broadcast(batch("public")).await;

        subscriptions.declare(definition(true));
        subscriptions.broadcast(batch("secret")).await;
        assert!(
            attached.recv().await.is_some(),
            "a subscriber drains what it received under its own definition"
        );
        assert!(
            attached.recv().await.is_none(),
            "no batch of the new definition reaches a subscriber attached under the old one"
        );
        assert_eq!(
            subscriptions.attach(&definition(false)).err(),
            Some(RelaySubscriptionRefusal::Redefined)
        );
    }

    #[tokio::test]
    async fn withdrawing_the_relay_closes_its_subscribers() {
        let subscriptions = RelaySubscriptions::new();
        subscriptions.declare(definition(false));
        let mut attached = subscriptions
            .attach(&definition(false))
            .assured("the subscriber describes the declared definition");

        subscriptions.withdraw();
        assert!(attached.recv().await.is_none());
        assert_eq!(
            subscriptions.attach(&definition(false)).err(),
            Some(RelaySubscriptionRefusal::NotDeclared)
        );
        subscriptions.withdraw();
    }
}
