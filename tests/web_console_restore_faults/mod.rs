//! Steps that make the web console's next restore attempt lose the reply the leader sent once it
//! ran the restore, after which a later attempt cannot read the archive or receives an unrecorded
//! failure, and that read the reply the page lost.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Installing the bounded restore fault on the scenario's console page, reading its
//!   record, and decoding the reply it kept from the console.
//! - **Depends on.** The scenario's browser page and placeholders, and the client wire's restore
//!   codec.
//! - **Must not know.** How the console streams a restore or decides its outcome, or how a node
//!   serves it.

mod unrecorded_failure;

use std::time::Duration;

use cucumber::{given, then};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    CommandDisposition, RestoreDisposition, RestoreReply, SessionLimits,
    websocket::{CONSOLE_RESTORE_PATH, ClientRestoreWebSocketCodec, WebSocketData},
};
use nervix_primitives::time::Instant;
use serde::Deserialize;

use super::ScenarioWorld;

/// The fault, parameterized by the restore path and the later attempt's failure.
const LOSE_RESTORE_REPLY: &str = include_str!("lose_restore_reply.js");

/// Reads the fault's record.
const READ_RECORD: &str = "() => window.__nervixRestoreFault.record()";

/// How long the restore may take to stream, apply and answer, and the console to pause and stream
/// it again, before the page has lost the reply and encountered the later failure. Scenarios run
/// many at a time, so the bound is generous; each wait ends as soon as its condition holds.
const LOST_REPLY_TIMEOUT: Duration = Duration::from_secs(60);

/// What the fault saw on the page.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RestoreFaultRecord {
    /// The bytes of the reply the fault kept from the console, once it kept one.
    lost_reply: Option<Vec<u8>>,
    /// Lost candidates, including valid uncertain replies before the selected completion.
    lost_replies: usize,
    /// The first attempt's start, retained only for the unrecorded-failure witness.
    start: Option<Vec<u8>>,
    /// How many valid unrecorded failures replaced a later reply.
    substituted_failures: usize,
    /// How many restore connections the page opened since the fault was installed.
    restore_connections: usize,
    /// How many reads of a file's bytes failed once the reply was lost.
    failed_reads: usize,
    /// What went wrong in the fault itself, when something did.
    failure: Option<String>,
}

impl RestoreFaultRecord {
    /// Whether the page lost the reply and then failed to read the archive.
    fn lost_then_unreadable(&self) -> bool {
        self.lost_reply.is_some() && self.failed_reads > 0
    }
}

fn browser_page(world: &ScenarioWorld) -> &playwright_rs::Page {
    world
        .browser_page
        .as_ref()
        .assured("the scenario opened the console before it installs the restore fault")
}

async fn read_record(page: &playwright_rs::Page) -> RestoreFaultRecord {
    page.evaluate::<(), RestoreFaultRecord>(READ_RECORD, None::<&()>)
        .await
        .assured("the scenario installed the restore fault before reading its record")
}

#[given(
    "the web console's next restore attempt loses its reply, after which its archive file cannot \
     be read"
)]
async fn given_next_restore_attempt_loses_its_reply(world: &mut ScenarioWorld) {
    install_fault(world, "unreadable").await;
}

async fn install_fault(world: &ScenarioWorld, later_failure: &str) {
    let configuration = serde_json::json!({
        "restorePath": CONSOLE_RESTORE_PATH,
        "laterFailure": later_failure,
        "replyFrameBytes": SessionLimits::DEFAULT.frame_bytes(),
    });
    let installed = browser_page(world)
        .evaluate::<_, bool>(LOSE_RESTORE_REPLY, Some(&configuration))
        .await
        .assured("the console page runs the restore fault");
    assert!(installed, "the restore fault reports its installation");
}

#[then(
    expr = "the web console's lost restore reply completed the restore, whose execution reference \
            is saved as placeholder {string}"
)]
async fn then_lost_restore_reply_completed_the_restore(
    world: &mut ScenarioWorld,
    placeholder: String,
) {
    let page = browser_page(world);
    let deadline = Instant::now() + LOST_REPLY_TIMEOUT;
    let record = loop {
        nervix_primitives::task::consume_budget().await;
        let record = read_record(page).await;
        if let Some(failure) = &record.failure {
            panic!("the restore fault failed: {failure}; its record: {record:?}");
        }
        if record.lost_then_unreadable() {
            break record;
        }
        assert!(
            Instant::now() < deadline,
            "the web console did not lose its restore reply and then fail to read the archive \
             within {LOST_REPLY_TIMEOUT:?}; the fault's record: {record:?}"
        );
        nervix_primitives::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(
        record.restore_connections >= 2,
        "the console streamed the restore again after it lost the reply: {record:?}"
    );
    let lost_reply = record
        .lost_reply
        .verified("the wait above returned only once the fault kept the reply");
    let codec = ClientRestoreWebSocketCodec::new(SessionLimits::DEFAULT);
    let frame = codec
        .decode(WebSocketData::Binary(lost_reply.into()))
        .assured("the leader answers a restore stream with one restore reply frame");
    let reply =
        RestoreReply::decode(&frame).assured("the leader's restore reply frame decodes as a reply");
    let RestoreDisposition::Outcome(outcome) = reply.disposition else {
        panic!("the lost reply refused the stream instead of running the restore: {reply:?}");
    };
    assert!(
        matches!(outcome.disposition, CommandDisposition::Completed { .. }),
        "the lost reply is the outcome of the restore the leader ran: {outcome:?}"
    );
    world
        .placeholders
        .insert(placeholder, outcome.execution_reference.to_string());
}
