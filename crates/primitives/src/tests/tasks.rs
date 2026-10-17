//! The contract of task handles and of reads outside the cooperative budget, which every mode's
//! backend keeps.
//!
//! The ordinary build runs the scripts on Tokio's runtime, where the abort-on-drop handle is Tokio
//! Util's, and the Shuttle build inside a Shuttle execution, where it is this crate's own over
//! Shuttle's join handle.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use meticulous::ResultExt as _;

use crate::{
    sync::oneshot,
    task::{AbortOnDropHandle, spawn, unconstrained},
};

/// More delivered values than Tokio's cooperative budget lets one task poll read.
const READS_BEYOND_ONE_BUDGET: usize = 300;

/// `READS_BEYOND_ONE_BUDGET` oneshot receivers, the `n`th already holding `n`.
fn delivered_values() -> Vec<oneshot::Receiver<usize>> {
    let mut receivers = Vec::with_capacity(READS_BEYOND_ONE_BUDGET);
    for value in 0..READS_BEYOND_ONE_BUDGET {
        let (sender, receiver) = oneshot::channel();
        sender
            .send(value)
            .assured("the receiver is held below, so the value is delivered");
        receivers.push(receiver);
    }
    receivers
}

/// Whether `read` has a value now, polled once without waiting for one.
#[cfg(not(feature = "shuttle"))]
fn is_ready<F: Future + Unpin>(read: &mut F) -> bool {
    let mut context = Context::from_waker(Waker::noop());
    Pin::new(read).poll(&mut context).is_ready()
}

/// In one synchronous pass, a task that reads more delivered values than its cooperative budget
/// allows finds a later value not ready, though it has arrived.
///
/// This is the control for [`unconstrained_reads_take_every_delivered_value`]: it shows that the
/// pass below spends the runtime's budget, so that check does not pass for want of one. Only a
/// runtime that keeps a budget runs it, and Shuttle keeps none.
#[cfg(not(feature = "shuttle"))]
pub(super) async fn a_spent_budget_leaves_delivered_values_unread() {
    let mut receivers = delivered_values();
    let mut refused_a_delivered_value = false;
    for receiver in &mut receivers {
        if !is_ready(receiver) {
            refused_a_delivered_value = true;
        }
    }
    assert!(
        refused_a_delivered_value,
        "the pass spends the task's cooperative budget"
    );
}

/// A future polled outside the cooperative budget reads a value that has arrived, however many
/// reads the task made before it in the same poll.
pub(super) async fn unconstrained_reads_take_every_delivered_value() {
    let mut receivers = delivered_values();
    for (expected, receiver) in receivers.iter_mut().enumerate() {
        let mut read = unconstrained(receiver);
        let mut context = Context::from_waker(Waker::noop());
        let Poll::Ready(value) = Pin::new(&mut read).poll(&mut context) else {
            panic!("value {expected} arrived before the read, which reads it outside the budget");
        };
        assert_eq!(
            value.assured("the value was sent before its sender was dropped"),
            expected
        );
    }
}

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
