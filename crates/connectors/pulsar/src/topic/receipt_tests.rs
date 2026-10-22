//! The bounded NO ACK receipt owner against an independent completion/count model.
//!
//! Layer: test harness.
//! - **Owns.** Valid bounded scripts of completed, failed and unanswered receipts, the finish
//!   outcome their model predicts, and a direct check of backpressure at the production bound.
//! - **Depends on.** The production receipt owner and the primitive runtime and channels.
//! - **Must not know.** A broker's wire protocol or the host's acknowledgement tracking.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{future::Future, pin::Pin, time::Duration};

use arch_into::ArchInto as _;
use meticulous::ResultExt as _;
use nervix_connector::{SinkFinish, SinkFinishError};
use nervix_primitives::{runtime::Builder, sync::oneshot, time::Instant};

use super::{
    CommandSendReceipt, PULSAR, PULSAR_OUTBOUND_CAPACITY, PulsarConnectionError, PulsarError,
    PulsarProducerError, UnconfirmedPulsarPublication,
};

type DriverReceipt = Result<CommandSendReceipt, PulsarError>;
type ScriptedReceipt = Pin<Box<dyn Future<Output = DriverReceipt> + Send>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, bolero::TypeGenerator)]
enum Answer {
    Confirmed,
    Failed,
    Canceled,
    Unanswered,
}

impl Answer {
    fn receipt(self) -> ScriptedReceipt {
        match self {
            Self::Confirmed => Box::pin(futures_util::future::ready(Ok(Self::confirmation()))),
            Self::Failed => Box::pin(futures_util::future::ready(Err(Self::failure()))),
            Self::Canceled => Box::pin(futures_util::future::ready(Err(PulsarError::Producer(
                PulsarProducerError::Connection(PulsarConnectionError::Canceled),
            )))),
            Self::Unanswered => Box::pin(futures_util::future::pending()),
        }
    }

    fn confirmation() -> CommandSendReceipt {
        CommandSendReceipt {
            producer_id: 1,
            sequence_id: 1,
            message_id: Some(pulsar::message::proto::MessageIdData {
                ledger_id: 1,
                entry_id: 1,
                ..Default::default()
            }),
            highest_sequence_id: None,
        }
    }

    fn failure() -> PulsarError {
        PulsarError::Producer(PulsarProducerError::Connection(
            PulsarConnectionError::Disconnected,
        ))
    }
}

#[derive(Debug, Clone, Copy, bolero::TypeGenerator)]
struct HandOver {
    answer: Answer,
    collect: bool,
}

#[derive(Debug, bolero::TypeGenerator)]
struct ReceiptScript {
    #[generator(bolero::generator::produce_with::<Vec<HandOver>>().len(0_usize..=PULSAR_OUTBOUND_CAPACITY))]
    records: Vec<HandOver>,
    wait_before_expiry: bool,
}

impl ReceiptScript {
    async fn check(&self) {
        let mut owner = UnconfirmedPulsarPublication::<ScriptedReceipt>::default();
        let mut unanswered = 0_usize;
        let mut first_failure = None;
        let mut uncollected_answer = false;
        for record in &self.records {
            nervix_primitives::task::consume_budget().await;
            owner.make_room().await;
            owner.retain(record.answer.receipt());
            match record.answer {
                Answer::Confirmed => uncollected_answer = true,
                Answer::Failed | Answer::Canceled => {
                    first_failure.get_or_insert(record.answer);
                    uncollected_answer = true;
                }
                Answer::Unanswered => {
                    unanswered += 1;
                    uncollected_answer = false;
                }
            }
            if record.collect {
                owner.collect_ready();
                uncollected_answer = false;
            }
            let expected_held = unanswered + usize::from(uncollected_answer);
            assert_eq!(owner.receipts.len(), expected_held);
            assert!(owner.receipts.len() <= PULSAR_OUTBOUND_CAPACITY);
        }
        let had_receipts = unanswered > 0 || uncollected_answer;
        let deadline = if self.wait_before_expiry {
            Instant::now() + Duration::from_millis(1)
        } else {
            Instant::now()
        };
        let finished = owner.finish(deadline).await;
        assert_eq!(owner.receipts.len(), unanswered);
        if unanswered > 0 || first_failure.is_some() {
            let Err(failure) = finished else {
                panic!("an unanswered or failed receipt must fail finish");
            };
            if unanswered > 0 {
                assert_eq!(
                    *failure.current_context(),
                    SinkFinishError::Unsent {
                        sink: PULSAR.keyword(),
                        records: unanswered.arch_into(),
                    }
                );
            } else {
                assert_eq!(
                    *failure.current_context(),
                    SinkFinishError::Flush {
                        sink: PULSAR.keyword()
                    }
                );
            }
            let driver_cause = failure.downcast_ref::<PulsarError>();
            assert_eq!(driver_cause.is_some(), first_failure.is_some());
            if let Some(driver_cause) = driver_cause {
                let actual = match driver_cause {
                    PulsarError::Producer(PulsarProducerError::Connection(
                        PulsarConnectionError::Disconnected,
                    )) => Answer::Failed,
                    PulsarError::Producer(PulsarProducerError::Connection(
                        PulsarConnectionError::Canceled,
                    )) => Answer::Canceled,
                    error => panic!("the driver cause changed at finish: {error:?}"),
                };
                assert_eq!(Some(actual), first_failure);
            }
        } else if had_receipts {
            assert_eq!(
                finished.assured("every retained receipt confirmed"),
                SinkFinish::Flushed
            );
        } else {
            assert_eq!(
                finished.assured("there is no outstanding receipt or failure"),
                SinkFinish::NothingHeld
            );
        }
    }
}

#[test]
fn bolero_receipts_keep_exact_unanswered_counts_and_the_first_failure() {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .assured("the test process can create its current-thread runtime");
    for wait_before_expiry in [false, true] {
        for answer in [
            Answer::Confirmed,
            Answer::Failed,
            Answer::Canceled,
            Answer::Unanswered,
        ] {
            let boundary = ReceiptScript {
                records: vec![
                    HandOver {
                        answer,
                        collect: false
                    };
                    PULSAR_OUTBOUND_CAPACITY
                ],
                wait_before_expiry,
            };
            runtime.block_on(boundary.check());
        }
    }
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .with_type::<ReceiptScript>()
        .for_each(|script| runtime.block_on(script.check()));
}

#[nervix_primitives::test]
async fn a_full_receipt_set_waits_for_any_answer_before_another_hand_over() {
    let mut owner = UnconfirmedPulsarPublication::<ScriptedReceipt>::default();
    let mut answers = Vec::new();
    for _ in 0..PULSAR_OUTBOUND_CAPACITY {
        nervix_primitives::task::consume_budget().await;
        owner.make_room().await;
        let (answer, receipt) = oneshot::channel::<DriverReceipt>();
        let receipt: ScriptedReceipt = Box::pin(async move {
            match receipt.await {
                Ok(received) => received,
                Err(_) => Err(Answer::failure()),
            }
        });
        owner.retain(receipt);
        answers.push(answer);
    }
    use futures_util::FutureExt as _;
    assert!(owner.make_room().now_or_never().is_none());
    let mut waiting = Box::pin(owner.make_room());
    assert!(waiting.as_mut().now_or_never().is_none());
    answers
        .remove(PULSAR_OUTBOUND_CAPACITY / 2)
        .send(Ok(Answer::confirmation()))
        .assured("the production owner still retains the chosen receipt");
    waiting.await;
    assert_eq!(owner.receipts.len(), PULSAR_OUTBOUND_CAPACITY - 1);
    owner.retain(Answer::Unanswered.receipt());
    assert_eq!(owner.receipts.len(), PULSAR_OUTBOUND_CAPACITY);
}
