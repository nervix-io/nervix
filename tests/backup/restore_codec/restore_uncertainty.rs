//! Public restore uncertainty after a completed attempt loses its reply and its local archive.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Native and CLI observations of a later archive-open failure and exact recovery.
//! - **Depends on.** The public restore fixture, clients, and current archive digest contract.
//! - **Must not know.** Client attempt-history internals or the server's command records.

use super::*;

#[when(
    expr = "the {string} client loses the completed restore answer for archive {string} and \
            cannot reopen it on node {string}"
)]
async fn when_a_later_archive_is_missing(
    world: &mut ScenarioWorld,
    kind: String,
    archive: String,
    node: String,
) {
    let path = archive_path(world, &archive);
    let bytes = tokio::fs::read(&path)
        .await
        .assured("the scenario's backup exists before the first restore attempt");
    when_restore_reply_is_unreadable(
        world,
        kind,
        archive,
        "missing archive after completion".to_string(),
        node,
    )
    .await;
    assert!(
        !tokio::fs::try_exists(&path)
            .await
            .assured("the scenario can inspect its archive path"),
        "the archive stayed unavailable until the client's restore returned"
    );
    let observation = world
        .restore_codec_result
        .as_ref()
        .assured("the peer observed a completed real restore");
    assert_eq!(
        observation.received.archive.digest.as_bytes(),
        blake3::hash(&bytes).as_bytes()
    );
    assert_eq!(
        observation.received.archive.total_bytes.get(),
        u64::try_from(bytes.len()).assured("the bounded backup size fits u64")
    );
    tokio::fs::write(path, bytes)
        .await
        .assured("recovery restores exactly the archived bytes under the same path");
}

#[then("a later archive failure left its restore uncertain under the original execution reference")]
fn then_a_later_archive_failure_is_uncertain(world: &mut ScenarioWorld) {
    then_restore_outcome_is_uncertain(world);
}
