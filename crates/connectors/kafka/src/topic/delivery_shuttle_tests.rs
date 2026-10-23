//! The Kafka producer's retained-record count explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The invariants the count is held to while the delivery reports of the records a emitter
//!   handed over are served concurrently with further hand-overs and counts: every record counts
//!   from before its hand-over until its report is served, and a publish that reads a record's
//!   report never finds that record still counted.
//! - **Depends on.** The producer context's retention and delivery opaque, a channel as the
//!   driver's queue of records, a task as its polling thread, and the model harness's Shuttle
//!   runner.
//! - **Must not know.** librdkafka, a broker, or the emitter contract.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_model_harness::shuttle::check_interleavings;
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
    mpsc, oneshot,
};

use super::{HandedOverRecord, KafkaDeliveryReport, KafkaProducerContext};

const CHECK_TASK_JOINS: &str =
    "a check task that panics fails the execution before its join returns";

/// How many records a check hands over.
const RECORDS: u64 = 3;

/// Serves the delivery report of every record `driver` gives it, as librdkafka's polling thread
/// does, counting each one in `served` before serving it.
async fn serve_reports(
    mut driver: mpsc::UnboundedReceiver<Box<HandedOverRecord>>,
    served: Arc<AtomicU64>,
) {
    loop {
        nervix_primitives::task::consume_budget().await;
        let Some(record) = driver.recv().await else {
            return;
        };
        served.fetch_add(1, Ordering::SeqCst);
        (*record).report(KafkaDeliveryReport::Delivered);
    }
}

fn the_count_holds_every_record_until_its_report_is_served() {
    shuttle::future::block_on(async {
        let context = KafkaProducerContext::new();
        let (driver, taken) = mpsc::unbounded_channel();
        let served = Arc::new(AtomicU64::new(0));
        let polling = nervix_primitives::task::spawn(serve_reports(taken, Arc::clone(&served)));
        for handed_over in 1..=RECORDS {
            let record = context.retain(None);
            if driver.send(record).is_err() {
                panic!("the polling task takes records until the check stops handing them over");
            }
            let counted = context.retained_records();
            let served_by_now = served.load(Ordering::SeqCst);
            assert!(
                counted <= handed_over,
                "counted {counted} records after handing {handed_over} over"
            );
            let retired = handed_over
                .checked_sub(counted)
                .verified("the assertion above bounds the count by the records handed over");
            assert!(
                retired <= served_by_now,
                "{retired} of {handed_over} records left the count, but only {served_by_now} \
                 reports were being served"
            );
        }
        drop(driver);
        polling.await.assured(CHECK_TASK_JOINS);
        assert_eq!(
            context.retained_records(),
            0,
            "every record's retention ended with its served report"
        );
    });
}

#[test]
fn shuttle_the_count_holds_every_record_until_its_report_is_served() {
    check_interleavings(the_count_holds_every_record_until_its_report_is_served);
}

fn a_publish_that_reads_a_report_finds_its_record_no_longer_retained() {
    shuttle::future::block_on(async {
        let context = KafkaProducerContext::new();
        let (answer, report) = oneshot::channel();
        let record = context.retain(Some(answer));
        let polling = nervix_primitives::task::spawn(async move {
            (*record).report(KafkaDeliveryReport::Delivered);
        });
        // The publish counts before, while or after the polling task serves the report.
        let counted_before_the_answer = context.retained_records();
        assert!(
            counted_before_the_answer <= 1,
            "counted {counted_before_the_answer} records with one handed over"
        );
        let delivered = report
            .await
            .assured("the polling task serves the record's report before it ends");
        assert_eq!(delivered, KafkaDeliveryReport::Delivered);
        assert_eq!(
            context.retained_records(),
            0,
            "a publish that read the record's report found the record still retained"
        );
        polling.await.assured(CHECK_TASK_JOINS);
    });
}

#[test]
fn shuttle_a_publish_that_reads_a_report_finds_its_record_no_longer_retained() {
    check_interleavings(a_publish_that_reads_a_report_finds_its_record_no_longer_retained);
}
