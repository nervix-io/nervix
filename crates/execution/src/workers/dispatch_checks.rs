//! Ordinary executor drain regressions over the production pool.
//!
//! Layer: test harness.
//! - **Owns.** Backlog dispatch, callback panic isolation and interrupted-worker assertions.
//! - **Depends on.** The production execution owner and its deterministic worker hold.
//! - **Must not know.** Graph, transport or domain behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the owner regressions declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(outside, reason = "ordinary owner regression harness")
)]

use std::{future::Future as _, task::Poll};

use super::*;
use crate::{CpuClass, Executor, MemoryClass};

#[nervix_primitives::test]
async fn an_interrupted_worker_closes_its_queued_jobs_and_returns_the_class() {
    let pool = WorkerPool::new(
        WorkerClassName::Cpu(CpuClass::Control),
        NonZeroUsize::MIN,
        NonZeroUsize::new(2).assured("two queue slots are nonzero"),
    );
    let executor = Executor::default();
    let mut held = HeldWorker::new(&pool);
    let charge = executor
        .try_reserve(MemoryClass::Management, 1)
        .assured("one byte fits in the management budget");
    let mut queued = Box::pin(pool.run(QueueAdmission::RefuseWhenFull, charge, |_, _| ()));
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(queued.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    let charge = executor
        .try_reserve(MemoryClass::Management, 1)
        .assured("a second byte fits in the management budget");
    let mut successor = Box::pin(pool.run(QueueAdmission::RefuseWhenFull, charge, |_, _| ()));
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(successor.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    held.0.advance();
    assert_eq!(pool.snapshot().pending, 1);
    // Transfer the selected callback into its production destructor without executing it. This
    // is the transition when a runtime drops a worker before its next callback. Its successor
    // remains queued and must lose its start channel as the selected job loses its result.
    let interrupted = Worker {
        pool: Arc::clone(&held.0.pool),
        state: std::mem::replace(&mut held.0.state, WorkerState::Idle),
    };
    drop(held);
    drop(interrupted);
    let error = queued
        .await
        .expect_err("the interrupted worker closes its selected result channel");
    assert!(matches!(
        error.current_context(),
        ExecutionError::PoolClosed {
            class: "control_cpu"
        }
    ));
    let error = successor
        .await
        .expect_err("the interrupted worker closes its queued start channel");
    assert!(matches!(
        error.current_context(),
        ExecutionError::PoolClosed {
            class: "control_cpu"
        }
    ));
    assert_eq!(pool.snapshot().running, 0);
    assert_eq!(pool.snapshot().pending, 0);
    assert_eq!(executor.snapshot().management_memory.reserved_bytes, 0);
    let charge = executor
        .try_reserve(MemoryClass::Management, 1)
        .assured("the interrupted job returned its charge");
    assert_eq!(
        pool.run(QueueAdmission::RefuseWhenFull, charge, |_, _| 7)
            .await
            .assured("the returned active place admits a replacement"),
        7
    );
}

#[nervix_primitives::test]
async fn a_panicking_job_reports_its_own_failure_and_the_worker_drains_its_successor() {
    let pool = WorkerPool::new(
        WorkerClassName::Cpu(CpuClass::Control),
        NonZeroUsize::MIN,
        NonZeroUsize::new(2).assured("two queue slots are nonzero"),
    );
    let executor = Executor::default();
    let held = HeldWorker::new(&pool);
    let charge = executor
        .try_reserve(MemoryClass::Management, 1)
        .assured("one byte fits in the management budget");
    let mut panicking = Box::pin(pool.run(QueueAdmission::RefuseWhenFull, charge, |_, _| {
        panic!("the callback's deliberate panic")
    }));
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(panicking.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    let charge = executor
        .try_reserve(MemoryClass::Management, 1)
        .assured("a second byte fits in the management budget");
    let mut succeeding = Box::pin(pool.run(QueueAdmission::RefuseWhenFull, charge, |_, _| 7));
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(succeeding.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    drop(held);
    let failure = panicking
        .await
        .expect_err("the deliberate panic is reported");
    assert!(matches!(
        failure.current_context(),
        ExecutionError::JobPanicked {
            class: "control_cpu"
        }
    ));
    assert_eq!(
        succeeding
            .await
            .assured("the worker survives the first callback's panic"),
        7
    );
    let drained = pool.snapshot();
    assert_eq!(drained.completed, 2);
    assert_eq!(drained.running, 0);
    assert_eq!(drained.pending, 0);
    assert_eq!(executor.snapshot().management_memory.reserved_bytes, 0);
}

#[nervix_primitives::test]
async fn a_single_worker_drains_its_backlog_without_polling_submitters_again() {
    let pool = WorkerPool::new(
        WorkerClassName::Cpu(CpuClass::Control),
        NonZeroUsize::MIN,
        NonZeroUsize::new(8).assured("eight queue slots are nonzero"),
    );
    let executor = Executor::default();
    let (release, released) = nervix_primitives::sync::oneshot::channel();
    let mut gate = Some(released);
    let (completed, mut observed) = nervix_primitives::sync::mpsc::channel(8);
    let mut submissions = Vec::new();
    for index in 0..8 {
        let charge = executor
            .try_reserve(MemoryClass::Management, 1)
            .assured("eight bytes fit in the untouched management budget");
        let release = gate.take();
        let completed = completed.clone();
        let mut submission =
            Box::pin(
                pool.run(QueueAdmission::RefuseWhenFull, charge, move |_, _| {
                    if let Some(release) = release {
                        release
                            .blocking_recv()
                            .assured("the test releases the first job");
                    }
                    completed
                        .try_send(index)
                        .assured("the channel holds all eight results");
                }),
            );
        let state = std::future::poll_fn(|cx| Poll::Ready(submission.as_mut().poll(cx))).await;
        assert!(state.is_pending(), "the first job holds the class's worker");
        submissions.push(submission);
    }
    release
        .send(())
        .assured("the first job retains its release receiver");

    // No submission is polled while the backlog drains: the worker itself must take each
    // admitted successor, without an async wake-up to dispatch that successor.
    nervix_primitives::time::timeout(Duration::from_secs(30), async {
        for index in 0..8 {
            nervix_primitives::task::consume_budget().await;
            assert_eq!(observed.recv().await, Some(index));
        }
    })
    .await
    .assured("one dispatched worker drains every admitted job without waking submitters");
    for submission in submissions {
        nervix_primitives::task::consume_budget().await;
        submission
            .await
            .assured("each completed job returns its result");
    }
    assert_eq!(pool.snapshot().completed, 8);
    assert_eq!(pool.inner.dispatches.load(Ordering::Relaxed), 1);
    assert_eq!(pool.snapshot().pending, 0);
    assert_eq!(executor.snapshot().management_memory.reserved_bytes, 0);
}
