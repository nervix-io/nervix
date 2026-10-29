//! Admission and cancellation through the production offline client inspector.
//!
//! Layer: test harness.
//! - **Owns.** Real occupied executor capacities and the inspector's resulting outcomes.
//! - **Depends on.** The application fixture, the production inspector and bounded executor.
//! - **Must not know.** Restore planning, session transports or a second admission algorithm.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::task::Poll;

use meticulous::ResultExt as _;
use nervix_execution::{AdmissionError, ExecutionError, Executor, QueueAdmission};
use nervix_models::ClientName;
use nervix_primitives::{
    sync::{blocking::mpsc, oneshot},
    task::AbortOnDropHandle,
};

use super::*;
use crate::application::test_fixtures::{TestService, build_test_service_with_executor};

struct InspectionFixture {
    service: TestService,
    domain: DomainName,
    name: ClientName,
    config: [ClientConfigEntry; 2],
}

impl InspectionFixture {
    async fn new(executor: Executor) -> Self {
        Self {
            service: build_test_service_with_executor(false, executor).await,
            domain: DomainName::parse("inspection")
                .assured("inspection is a nonempty ASCII domain-name literal"),
            name: ClientName::parse("api").assured("api is a nonempty ASCII client-name literal"),
            config: [
                ClientConfigEntry {
                    key: "endpoint".into(),
                    value: "http://127.0.0.1:19080".into(),
                },
                ClientConfigEntry {
                    key: "timeout_ms".into(),
                    value: "5000".into(),
                },
            ],
        }
    }

    async fn inspect(
        &self,
        admission: QueueAdmission,
    ) -> error_stack::Result<(), ModelBindingValidationError> {
        self.service
            .service
            .validate_client_configuration(
                &self.domain,
                ConfiguredClientRef {
                    connector: ConnectorKind::Http,
                    name: &self.name,
                    mount: None,
                    config: &self.config,
                },
                admission,
            )
            .await
    }

    fn close(self) -> anyhow::Result<()> {
        let path = self.service.path.clone();
        drop(self);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }
}

struct FullFilesystem {
    release: mpsc::Sender<()>,
    running: AbortOnDropHandle<()>,
    waiting: AbortOnDropHandle<()>,
}

impl FullFilesystem {
    async fn fill(executor: &Executor) -> Self {
        let (release, held) = mpsc::channel();
        let (started, running) = oneshot::channel();
        let holder = executor.clone();
        let running_job = AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
            let charge = holder
                .try_reserve(MemoryClass::Commands, 0)
                .assured("zero bytes fit the live command-memory budget");
            holder
                .run_storage(
                    StorageClass::Filesystem,
                    charge,
                    move |_charge, _cancellation| {
                        started.send(()).assured(
                            "fill retains the worker-start receiver until this signal arrives",
                        );
                        match held.recv() {
                            Ok(()) | Err(mpsc::RecvError) => {}
                        }
                    },
                )
                .await
                .assured("the live executor runs a closure that handles both channel outcomes");
        }));
        running
            .await
            .assured("the retained holding task signals before waiting for its release");
        let waiter = executor.clone();
        let waiting_job = AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
            let charge = waiter
                .try_reserve(MemoryClass::Commands, 0)
                .assured("zero bytes fit the live command-memory budget");
            waiter
                .run_storage(
                    StorageClass::Filesystem,
                    charge,
                    |_charge, _cancellation| (),
                )
                .await
                .assured("the live executor runs the queued infallible closure after release");
        }));
        while executor.snapshot().filesystem_storage.pending != 1 {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::task::yield_now().await;
        }
        Self {
            release,
            running: running_job,
            waiting: waiting_job,
        }
    }

    async fn release(self) {
        drop(self.release);
        self.running
            .await
            .assured("dropping the release sender ends the held closure without panic");
        self.waiting
            .await
            .assured("the queued closure returns unit and has no failure operation");
    }
}

#[nervix_primitives::test]
async fn accepted_inspection_waits_for_memory_and_cancellation_returns_its_place()
-> anyhow::Result<()> {
    let executor = crate::runtime::single_worker_executor();
    let fixture = InspectionFixture::new(executor.clone()).await;
    let memory = executor.memory(MemoryClass::Commands);
    assert_eq!(memory.reserved_bytes, 0);
    let held = executor
        .try_reserve(MemoryClass::Commands, memory.capacity_bytes)
        .verified("the empty budget was asserted above and this reserves its exact capacity");
    let refused = fixture
        .inspect(QueueAdmission::RefuseWhenFull)
        .await
        .expect_err("request preflight refuses a full budget");
    assert!(matches!(
        refused.downcast_ref::<AdmissionError>(),
        Some(AdmissionError::BudgetExhausted { .. })
    ));
    assert_eq!(
        client_validation_message(&refused).as_deref(),
        Some(
            "invalid client 'api' in domain 'inspection': client configuration inspection could \
             not be admitted"
        )
    );
    let mut cancelled = Box::pin(fixture.inspect(QueueAdmission::WaitForPlace));
    assert!(matches!(
        futures_util::poll!(cancelled.as_mut()),
        Poll::Pending
    ));
    drop(cancelled);
    let mut accepted = Box::pin(fixture.inspect(QueueAdmission::WaitForPlace));
    assert!(matches!(
        futures_util::poll!(accepted.as_mut()),
        Poll::Pending
    ));
    drop(held);
    accepted
        .await
        .assured("the fixture config has no external inputs and its memory was released");
    assert_eq!(executor.memory(MemoryClass::Commands).reserved_bytes, 0);
    fixture.close()?;
    Ok(())
}

#[nervix_primitives::test]
async fn accepted_inspection_waits_for_a_full_filesystem_queue_and_cancellation_returns_its_charge()
-> anyhow::Result<()> {
    let executor = crate::runtime::single_worker_executor();
    let fixture = InspectionFixture::new(executor.clone()).await;
    let full = FullFilesystem::fill(&executor).await;
    let refused = fixture
        .inspect(QueueAdmission::RefuseWhenFull)
        .await
        .expect_err("request preflight refuses a full filesystem queue");
    assert!(matches!(
        refused.downcast_ref::<ExecutionError>(),
        Some(ExecutionError::QueueFull { .. })
    ));
    assert_eq!(executor.memory(MemoryClass::Commands).reserved_bytes, 0);
    let mut cancelled = Box::pin(fixture.inspect(QueueAdmission::WaitForPlace));
    assert!(matches!(
        futures_util::poll!(cancelled.as_mut()),
        Poll::Pending
    ));
    assert!(executor.memory(MemoryClass::Commands).reserved_bytes > 0);
    drop(cancelled);
    assert_eq!(executor.memory(MemoryClass::Commands).reserved_bytes, 0);
    let mut accepted = Box::pin(fixture.inspect(QueueAdmission::WaitForPlace));
    assert!(matches!(
        futures_util::poll!(accepted.as_mut()),
        Poll::Pending
    ));
    full.release().await;
    accepted
        .await
        .assured("the fixture config has no external inputs and both held jobs finished");
    assert_eq!(executor.memory(MemoryClass::Commands).reserved_bytes, 0);
    fixture.close()?;
    Ok(())
}
