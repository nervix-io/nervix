//! The contract of task handles, which every mode's backend keeps.
//!
//! The ordinary build runs the script on Tokio's runtime, where the abort-on-drop handle is Tokio
//! Util's, and the Shuttle build inside a Shuttle execution, where it is this crate's own over
//! Shuttle's join handle.

use meticulous::ResultExt as _;

use crate::{
    sync::oneshot,
    task::{AbortOnDropHandle, spawn},
};

/// An abort-on-drop handle hands over its task's output, ends its task on request, and ends it when
/// the handle is dropped.
pub(super) async fn abort_on_drop_handles_end_their_tasks() {
    let finished = AbortOnDropHandle::new(spawn(async { 7_u8 }));
    assert_eq!(finished.await.assured("the task returns a constant"), 7);

    let requested = AbortOnDropHandle::new(spawn(std::future::pending::<u8>()));
    requested.abort();
    let ended = requested.await;
    assert!(ended.is_err_and(|error| error.is_cancelled()));

    // The task holds the sender until it ends, so the receiver learns when the task's future is
    // dropped.
    let (mut held, released) = oneshot::channel::<()>();
    let dropped = AbortOnDropHandle::new(spawn(async move { held.closed().await }));
    drop(dropped);
    assert!(released.await.is_err());
}
