//! A dataflow node status answer through the production typed-request payload codec.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current dataflow node status answers, including an emitter's
//!   misconfiguration and the time left before it gives up its oldest retried unit, and their
//!   full-value oracle.
//! - **Depends on.** The dataflow node status request and answer, the typed-request payload codec
//!   and the bounded executor.
//! - **Must not know.** Which node answers or how `DESCRIBE` renders the status.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::ResultExt as _;
use nervix_execution::Executor;

use crate::{
    DataflowNodeStatusEnvelope, DataflowNodeStatusRequest, DataflowNodeStatusResponse,
    InterconnectRequest as _, request::RkyvMessage,
};

/// The status word a node reports.
#[derive(Debug, Clone, Copy, bolero::TypeGenerator)]
enum StatusWord {
    Ok,
    Waiting,
    Error,
}

impl StatusWord {
    fn text(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::Waiting => "WAITING",
            Self::Error => "ERROR",
        }
    }
}

/// The values one generated case varies a status answer by.
#[derive(Debug, bolero::TypeGenerator)]
struct StatusWireCase {
    status: StatusWord,
    detail: Option<String>,
    transient_error: Option<String>,
    misconfigured: bool,
    reconnect_backoff: Option<String>,
    reconnect_wait_millis: Option<u64>,
    give_up_wait_millis: Option<u64>,
}

impl StatusWireCase {
    fn response(&self) -> DataflowNodeStatusResponse {
        DataflowNodeStatusResponse {
            result: Ok(DataflowNodeStatusEnvelope {
                status: self.status.text().to_owned(),
                detail: self.detail.clone(),
                transient_error: self.transient_error.clone(),
                misconfigured: self.misconfigured,
                reconnect_backoff: self.reconnect_backoff.clone(),
                reconnect_wait_millis: self.reconnect_wait_millis,
                give_up_wait_millis: self.give_up_wait_millis,
            }),
        }
    }
}

/// Encodes `message` as the transport encodes the answer to a status request, and decodes it the
/// way the asking node does.
async fn round_trip<M>(executor: &Executor, message: M) -> M
where
    M: RkyvMessage,
{
    let class = DataflowNodeStatusRequest::CLASS;
    let (encoded, _reservation) = message
        .encode_rkyv(executor.clone(), class, class.payload_limit(executor))
        .await
        .assured("a bounded current status answer encodes within the management payload limit");
    let (decoded, _reservation) = M::decode_rkyv(executor.clone(), class, encoded)
        .await
        .assured("a status answer the transport encoded decodes");
    decoded
}

/// A dataflow node status answer keeps its complete value through the production typed-request
/// payload codec: its status, detail, transient error and whether that error is a
/// misconfiguration, its reconnect backoff and wait, and the time left before the node gives up
/// its oldest retried unit, each present or absent.
#[test]
fn bolero_dataflow_node_status_answers_round_trip() {
    let runtime = nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the ordinary test runtime builds");
    bolero::check!()
        .with_iterations(128)
        .with_max_len(256)
        .with_type::<StatusWireCase>()
        .for_each(|case| {
            runtime.block_on(async {
                let executor = Executor::default();
                let response = case.response();
                let decoded = round_trip(&executor, response.clone()).await;
                assert_eq!(decoded, response);
            });
        });
}
