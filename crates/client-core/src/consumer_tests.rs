//! A read's attachment step, driven through the consumer's production owner in the order in which
//! the end of its session can interleave with the read.

use std::{
    num::{NonZeroU32, NonZeroU64},
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{ConsumerId, EmitterOpened, OpenEmitterRequest, RequestId};
use nervix_models::{
    AckWindow, ClientConsumerLimits, ClientEndpointContract, DomainName, EmitterName, FieldName,
    ParseAsType, SchemaField,
};
use nervix_primitives::sync::{
    Arc, StdArc, atomic::AtomicBool, blocking::Mutex as SyncMutex, mpsc,
};

use super::{
    ConsumerAttachment, ConsumerHandle, ConsumerLifecycle, ConsumerPhase, EmitterConsumer,
    ParkedReads,
};
use crate::{
    ClientError,
    exchange::{ExchangeRequests, PendingReplies},
    tests::test_client,
};

/// Long enough for any prompt answer on a loaded machine; a read that waits for a restoration
/// nobody answers never ends within it.
const DEADLINE: Duration = Duration::from_secs(10);

fn domain() -> DomainName {
    DomainName::parse("tenant").assured("literal domain")
}

fn emitter() -> EmitterName {
    EmitterName::parse("app_output").assured("literal emitter")
}

fn limits() -> ClientConsumerLimits {
    ClientConsumerLimits {
        batches: NonZeroU32::new(2).assured("literal batches are nonzero"),
        bytes: NonZeroU64::new(4096).assured("literal bytes are nonzero"),
    }
}

fn fields() -> Vec<SchemaField> {
    vec![SchemaField {
        name: FieldName::parse("id").assured("literal field name"),
        ty: ParseAsType::String,
        optional: false,
        sensitive: false,
    }]
}

fn description() -> EmitterOpened {
    EmitterOpened {
        domain: domain(),
        emitter: emitter(),
        fields: fields(),
        generation: 4,
        contract: ClientEndpointContract::from_digest([7; 32]),
        window: AckWindow::Sequential,
        ack_timeout: Duration::from_secs(30),
        retry_backoff: Duration::from_millis(100),
        retry_max_backoff: Duration::from_secs(1),
        granted: limits(),
        max_batch_bytes: 4096,
        max_batch_rows: 16,
    }
}

/// A consumer the client registered as desired, attached on an exchange of the returned
/// generation that no server answers.
struct AttachedConsumer {
    handle: StdArc<ConsumerHandle>,
    generation: Arc<()>,
}

impl AttachedConsumer {
    fn new() -> Self {
        let client = test_client("tenant");
        let generation = Arc::new(());
        let (frames, _unanswered) = mpsc::channel(1);
        let id = ConsumerId::opened_by(RequestId::new(NonZeroU64::MIN));
        let attachment = Arc::new(ConsumerAttachment {
            id,
            #[cfg(feature = "arrow")]
            description: description(),
            exchange: Arc::new(ExchangeRequests {
                frames,
                pending: Arc::new(SyncMutex::new(PendingReplies::new())),
            }),
            generation: generation.clone(),
            closed: AtomicBool::new(false),
        });
        let registry = client.inner.events.sinks.consumers.clone();
        let handle = StdArc::new(ConsumerHandle {
            client,
            request: OpenEmitterRequest {
                domain: domain(),
                emitter: emitter(),
                expected_fields: fields(),
                limits: limits(),
            },
            pinned: description(),
            registry: registry.clone(),
            initial_id: id,
            lifecycle: ConsumerLifecycle::new(ConsumerPhase::Active(attachment)),
            parked: ParkedReads::default(),
        });
        registry.register(&handle);
        Self { handle, generation }
    }
}

/// The exchange ends after a read found no gap to report and before it chose the attachment to
/// read through. The read reports the gap then, before it recovers the session or restores the
/// consumer, as it would had the exchange ended a moment earlier.
#[nervix_primitives::test]
async fn a_read_whose_exchange_ends_before_it_attaches_reports_the_gap_before_restoring() {
    let AttachedConsumer { handle, generation } = AttachedConsumer::new();

    handle.exchange_ended(&generation);
    let attached = nervix_primitives::time::timeout(DEADLINE, handle.attachment()).await;

    let Ok(attached) = attached else {
        panic!("the read waited for a restoration instead of reporting the gap its exchange left");
    };
    let Err(gap) = attached else {
        panic!("the read attached again without reporting the gap its exchange left");
    };
    assert!(
        matches!(gap.current_context(), ClientError::ConsumerInterrupted),
        "the read reported {gap:?} instead of the gap its exchange left"
    );
    assert!(
        !handle.lifecycle.take_interruption(),
        "the gap was left for a later read after this read reported it"
    );
}

/// A read of a consumer its application closed ends without a delivery.
#[nervix_primitives::test]
async fn a_read_of_a_closed_consumer_ends_without_a_delivery() {
    let AttachedConsumer { handle, .. } = AttachedConsumer::new();
    let released = handle.lifecycle.close();
    assert!(
        released.is_some(),
        "closing an attached consumer releases its attachment"
    );
    let consumer = EmitterConsumer { inner: handle };

    let read = nervix_primitives::time::timeout(DEADLINE, consumer.next_batch()).await;

    let Ok(read) = read else {
        panic!("a read of a closed consumer waited instead of ending");
    };
    assert!(
        matches!(read, Ok(None)),
        "a read of a closed consumer did not end without a delivery"
    );
}

/// The consumer closes once a read has reported its gap and set out to restore it: the read ends
/// with the closed session instead of restoring a consumer nobody holds.
#[nervix_primitives::test]
async fn a_restoring_read_of_a_consumer_that_closes_ends_with_its_session_closed() {
    let AttachedConsumer { handle, generation } = AttachedConsumer::new();
    handle.exchange_ended(&generation);
    assert!(
        handle.lifecycle.take_interruption(),
        "the end of the exchange leaves a gap for the next read"
    );
    let released = handle.lifecycle.close();
    assert!(
        released.is_none(),
        "an interrupted consumer holds no attachment to release"
    );

    let attached = nervix_primitives::time::timeout(DEADLINE, handle.attachment()).await;

    let Ok(attached) = attached else {
        panic!("the restoring read of a closed consumer waited instead of ending");
    };
    let Err(closed) = attached else {
        panic!("the restoring read of a closed consumer attached again");
    };
    assert!(
        matches!(closed.current_context(), ClientError::SessionClosed),
        "the restoring read of a closed consumer reported {closed:?}"
    );
}
