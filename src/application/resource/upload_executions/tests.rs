//! Resource upload execution ownership under cancellation, retirement and concurrent admission.
//!
//! Layer: test harness.
//! - **Owns.** Assertions that a live or waiting attempt keeps one execution per upload key and
//!   that the last attempt retires its entry.
//! - **Depends on.** The production execution owner and the selected execution primitives.
//! - **Must not know.** Resource archives, consensus outcomes or client transports.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use futures_util::poll;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    DomainName, ResourceName, ResourceUploadIdentity, ResourceUploadKey, UserName,
};

use super::{ResourceUploadExecution, ResourceUploadExecutions};

fn key(identity: &str) -> ResourceUploadKey {
    ResourceUploadKey::new(
        UserName::parse("alice").assured("the test owner is valid"),
        DomainName::parse("tenant").assured("the test domain is valid"),
        ResourceName::parse("bundle").assured("the test resource is valid"),
        ResourceUploadIdentity::parse(identity).assured("the test identity is valid"),
    )
}

#[nervix_primitives::test]
async fn waiting_attempts_share_the_execution_until_the_last_guard_retires_it() {
    let executions = ResourceUploadExecutions::default();
    let key = key("one-upload");
    let first = executions.lock(&key).await;
    assert!(executions.contains_key(&key));
    assert!(executions.try_lock(&key).is_none());

    let mut second = Box::pin(executions.lock(&key));
    assert!(poll!(second.as_mut()).is_pending());
    drop(first);
    assert!(
        executions.contains_key(&key),
        "a waiting attempt keeps its entry"
    );

    let second = second.await;
    let mut third = Box::pin(executions.lock(&key));
    assert!(poll!(third.as_mut()).is_pending());
    drop(second);
    let third = third.await;
    assert!(executions.contains_key(&key));
    drop(third);
    assert!(!executions.contains_key(&key));

    let repeated = executions.lock(&key).await;
    drop(repeated);
    assert!(!executions.contains_key(&key));
}

#[nervix_primitives::test]
async fn cancelled_waiters_and_failed_try_locks_leave_no_idle_entry() {
    let executions = ResourceUploadExecutions::default();
    let key = key("cancelled-upload");
    let first = executions.lock(&key).await;
    let mut cancelled = Box::pin(executions.lock(&key));
    assert!(poll!(cancelled.as_mut()).is_pending());
    assert!(executions.try_lock(&key).is_none());
    drop(cancelled);
    assert!(executions.contains_key(&key));
    drop(first);
    assert!(!executions.contains_key(&key));

    let available = executions
        .try_lock(&key)
        .assured("the previous execution and its waiter have ended");
    drop(available);
    assert!(!executions.contains_key(&key));
}

#[nervix_primitives::test]
async fn recovery_probe_keeps_an_entry_acquired_between_unlock_and_retirement() {
    let executions = ResourceUploadExecutions::default();
    let key = key("handover-upload");
    let first = executions.lock(&key).await;
    let ResourceUploadExecution {
        _guard: guard,
        _lease: lease,
    } = first;
    drop(guard);

    let probe = executions
        .try_lock(&key)
        .assured("the previous guard unlocked before its lease retires");
    drop(lease);
    let remained_registered = executions.contains_key(&key);
    drop(probe);

    assert!(
        remained_registered,
        "the acquired probe keeps its execution registered"
    );
    assert!(!executions.contains_key(&key));
}

#[nervix_primitives::test]
async fn independent_uploads_can_execute_together_and_retire_separately() {
    let executions = ResourceUploadExecutions::default();
    let first_key = key("first-upload");
    let second_key = key("second-upload");
    let first = executions.lock(&first_key).await;
    let second = executions
        .try_lock(&second_key)
        .assured("no attempt holds this independent upload key");
    drop(first);
    assert!(!executions.contains_key(&first_key));
    assert!(executions.contains_key(&second_key));
    drop(second);
    assert!(!executions.contains_key(&second_key));
}

#[cfg(feature = "shuttle")]
#[test]
fn shuttle_waiter_cancellation_and_retirement_keep_one_execution_per_upload() {
    use nervix_model_harness::shuttle::check_dfs;
    use nervix_primitives::{
        runtime::Builder,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            oneshot,
        },
        task,
    };

    check_dfs(
        || {
            let runtime = Builder::new_current_thread()
                .build()
                .assured("the model runtime builds");
            runtime.block_on(async {
                let executions = ResourceUploadExecutions::default();
                let key = key("racing-upload");
                let active = Arc::new(AtomicBool::new(false));
                let first = executions.lock(&key).await;
                assert!(!active.swap(true, Ordering::SeqCst));
                let mut cancelled = Box::pin(executions.lock(&key));
                assert!(poll!(cancelled.as_mut()).is_pending());
                drop(cancelled);
                assert!(executions.try_lock(&key).is_none());

                let (registered, waiting) = oneshot::channel();
                let waiter = task::spawn({
                    let executions = executions.clone();
                    let key = key.clone();
                    let active = active.clone();
                    async move {
                        let mut attempt = Box::pin(executions.lock(&key));
                        assert!(poll!(attempt.as_mut()).is_pending());
                        registered
                            .send(())
                            .assured("the admission observer is waiting");
                        let guard = attempt.await;
                        assert!(
                            !active.swap(true, Ordering::SeqCst),
                            "two attempts execute one upload"
                        );
                        task::yield_now().await;
                        assert!(active.swap(false, Ordering::SeqCst));
                        drop(guard);
                    }
                });
                waiting
                    .await
                    .assured("the waiter registered before retirement races admission");
                let arriving = task::spawn({
                    let executions = executions.clone();
                    let key = key.clone();
                    let active = active.clone();
                    async move {
                        let guard = match executions.try_lock(&key) {
                            Some(guard) => guard,
                            None => executions.lock(&key).await,
                        };
                        assert!(
                            !active.swap(true, Ordering::SeqCst),
                            "retirement split the upload execution"
                        );
                        task::yield_now().await;
                        assert!(active.swap(false, Ordering::SeqCst));
                        drop(guard);
                    }
                });
                assert!(active.swap(false, Ordering::SeqCst));
                drop(first);
                waiter.await.assured("the registered retry finishes");
                arriving.await.assured("the racing retry finishes");
                assert!(!active.load(Ordering::SeqCst));
                assert!(
                    !executions.contains_key(&key),
                    "the final attempt retires the entry"
                );
            });
        },
        Some(256),
    );
}
