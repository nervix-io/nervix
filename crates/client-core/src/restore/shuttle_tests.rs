//! The last archive frame racing the ending of its production restore attempt.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Whether a final frame can still be forwarded after ending reported no handover.
//! - **Depends on.** The production queued-frame predicate, handover fence, and central runner.
//! - **Must not know.** Network scheduling, server admission, or reference-count ordering.

use meticulous::ResultExt as _;
use nervix_model_harness::shuttle::check_dfs;
use nervix_primitives::task;

use super::*;

#[test]
fn shuttle_finishing_a_restore_attempt_fences_its_last_frame() {
    check_dfs(
        || {
            shuttle::future::block_on(async {
                let handover = Arc::new(RestoreHandover::new());
                let frame = RestoreQueuedFrame {
                    frame: RestoreChunk::encode(&[0x5a], &SESSION_LIMITS)
                        .assured("one archive byte fits the current frame"),
                    completion: Some(handover.clone()),
                };
                let sending =
                    task::spawn(async move { frame.can_handover().then(|| frame.into_frame()) });
                let archive_may_arrive = handover.finish();
                let forwarded = sending.await.assured("the final-frame decision completes");
                assert_eq!(
                    archive_may_arrive,
                    forwarded.is_some(),
                    "an ending without handover must fence the final frame; an allowed final \
                     frame must retain uncertainty even if its forwarding finishes after ending"
                );
            });
        },
        Some(64),
    );
}
