//! The Kafka producer's delivery reports: which records the producer still holds, and the report a
//! confirmed publish awaits for each record it handed over.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The producer context through which librdkafka's polling thread serves the delivery
//!   report of every record the producer took, the count of records whose report has not been
//!   served yet, and the delivery opaque that carries each record's retention and awaited report.
//! - **Depends on.** `rust-rdkafka`'s client and producer contexts and its delivery opaque, and the
//!   boundary's shared ownership, atomics and oneshot channel.
//! - **Must not know.** The sink contract, how a report becomes a broker verdict, the host, or any
//!   other connector implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use meticulous::OptionExt as _;
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
    oneshot,
};
use nervix_recovery::NoReceiver as _;
use rdkafka::{
    ClientContext,
    error::KafkaError,
    producer::{DeliveryResult, ProducerContext},
};

/// What librdkafka's delivery report says about one record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KafkaDeliveryReport {
    /// The broker took the record.
    Delivered,
    /// The record's delivery ended with this error: the broker refused the record, or the producer
    /// gave it up.
    Failed(KafkaError),
}

/// The producer's context, through which librdkafka's polling thread serves the delivery report of
/// every record the producer took.
///
/// Logging, errors and statistics keep rust-rdkafka's default handling. Each record carries a
/// [`HandedOverRecord`] as its delivery opaque, which librdkafka gives back with the record's
/// report.
pub(crate) struct KafkaProducerContext {
    /// Shared with the retention of every record librdkafka holds, which outlives any borrow of
    /// the producer inside the delivery opaque librdkafka keeps.
    retained: Arc<RetainedRecords>,
}

impl KafkaProducerContext {
    pub(crate) fn new() -> Self {
        Self {
            retained: Arc::new(RetainedRecords {
                count: AtomicU64::new(0),
            }),
        }
    }

    /// The delivery opaque of a record about to be handed to the producer, which retains the
    /// record from now on. `answer` is where a confirmed publish awaits the record's report.
    pub(crate) fn retain(
        &self,
        answer: Option<oneshot::Sender<KafkaDeliveryReport>>,
    ) -> Box<HandedOverRecord> {
        Box::new(HandedOverRecord {
            answer,
            retention: RetainedRecord::new(&self.retained),
        })
    }

    /// How many records the producer took whose delivery report librdkafka has not served yet.
    pub(crate) fn retained_records(&self) -> u64 {
        self.retained.count()
    }
}

impl ClientContext for KafkaProducerContext {}

impl ProducerContext for KafkaProducerContext {
    type DeliveryOpaque = Box<HandedOverRecord>;

    fn delivery(&self, delivery_result: &DeliveryResult<'_>, record: Box<HandedOverRecord>) {
        let report = match delivery_result {
            Ok(_message) => KafkaDeliveryReport::Delivered,
            Err((error, _message)) => KafkaDeliveryReport::Failed(error.clone()),
        };
        (*record).report(report);
    }
}

/// One record the producer took, carried as its delivery opaque from its hand-over until
/// librdkafka serves its delivery report.
///
/// librdkafka gives the opaque back exactly once: with the record when the producer refuses to take
/// it, or to the polling thread with the record's report. It is dropped there, and dropping it ends
/// the record's retention.
pub(crate) struct HandedOverRecord {
    /// Where a confirmed publish awaits the record's report. A `NO ACK` publish awaits none.
    answer: Option<oneshot::Sender<KafkaDeliveryReport>>,
    retention: RetainedRecord,
}

impl HandedOverRecord {
    /// Ends the record's retention with its delivery report, and gives the report to the publish
    /// awaiting it.
    ///
    /// The retention ends first, so a publish that reads the report finds the record no longer
    /// retained, and a finish after that publish does not count it.
    fn report(self, report: KafkaDeliveryReport) {
        let Self { answer, retention } = self;
        drop(retention);
        let Some(answer) = answer else {
            return;
        };
        answer
            .send(report)
            .means_peer_left("confirmed publish that stopped waiting for the record's report");
    }
}

/// How many records the producer took whose delivery report librdkafka has not served yet.
///
/// librdkafka's own `rd_kafka_outq_len` adds every event waiting in the producer's queue, its
/// errors, statistics and unserved delivery reports among them, so it counts more than the records
/// the producer holds. This counts records alone: a [`RetainedRecord`] counts one record from before
/// its hand-over until it is dropped. A record is retained no longer than librdkafka holds it, so
/// the producer's `queue.buffering.max.messages` bounds the count.
struct RetainedRecords {
    count: AtomicU64,
}

impl RetainedRecords {
    /// The count now.
    ///
    /// The read is a read-modify-write, so it reads the last count written before it: a retention
    /// that librdkafka's polling thread ended before the read is not counted.
    fn count(&self) -> u64 {
        self.count.fetch_add(0, Ordering::Relaxed)
    }
}

/// One record's place in [`RetainedRecords`], from before its hand-over until it is dropped.
struct RetainedRecord {
    /// The producer's count, which this record's delivery opaque keeps until librdkafka gives the
    /// opaque back.
    records: Arc<RetainedRecords>,
}

impl RetainedRecord {
    /// Starts one record's retention in `records`.
    fn new(records: &Arc<RetainedRecords>) -> Self {
        let previous = records.count.fetch_add(1, Ordering::Relaxed);
        previous.checked_add(1).assured(
            "every retained record keeps an allocation of its own alive, so far fewer than \
             u64::MAX are retained at once",
        );
        Self {
            records: Arc::clone(records),
        }
    }
}

impl Drop for RetainedRecord {
    fn drop(&mut self) {
        let previous = self.records.count.fetch_sub(1, Ordering::Relaxed);
        previous.checked_sub(1).assured(
            "this record's own retention counted it, and only dropping that retention uncounts it",
        );
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[path = "delivery_shuttle_tests.rs"]
mod shuttle_tests;
