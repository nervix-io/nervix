//! Connector task status publication.
//!
//! Layer: data plane.
//! - **Owns.** The task's and retained connector's coherent failure and retry status, published on
//!   their independent transitions.
//! - **Depends on.** The primitive publication boundary and connector retry vocabulary.
//! - **Must not know.** Shared registries, graph planning, payloads or transport drivers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use nervix_primitives::{
    publication::ArcSwapOption,
    sync::{Arc, StdArc},
};

/// The safe error and its retry are observed together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TaskFailure<R> {
    pub(super) error: String,
    pub(super) retry: Option<R>,
}

#[derive(Clone, Copy)]
enum FailureOwner {
    Task,
    Connector,
}

/// Absence of this publication means both writers are healthy.
#[derive(Debug)]
pub(super) enum TaskFailures<R> {
    Task(Arc<TaskFailure<R>>),
    Connector(Arc<TaskFailure<R>>),
    Both {
        task: Arc<TaskFailure<R>>,
        connector: Arc<TaskFailure<R>>,
    },
}

impl<R> TaskFailures<R> {
    fn owned(&self, owner: FailureOwner) -> Option<&Arc<TaskFailure<R>>> {
        match (self, owner) {
            (Self::Task(failure), FailureOwner::Task)
            | (Self::Connector(failure), FailureOwner::Connector) => Some(failure),
            (Self::Both { task, .. }, FailureOwner::Task) => Some(task),
            (Self::Both { connector, .. }, FailureOwner::Connector) => Some(connector),
            _ => None,
        }
    }

    fn replace(
        current: Option<&Self>,
        owner: FailureOwner,
        failure: Option<Arc<TaskFailure<R>>>,
    ) -> Option<StdArc<Self>> {
        let mut task = current
            .and_then(|current| current.owned(FailureOwner::Task))
            .cloned();
        let mut connector = current
            .and_then(|current| current.owned(FailureOwner::Connector))
            .cloned();
        match owner {
            FailureOwner::Task => task = failure,
            FailureOwner::Connector => connector = failure,
        }
        let failures = match (task, connector) {
            (None, None) => return None,
            (Some(task), None) => Self::Task(task),
            (None, Some(connector)) => Self::Connector(connector),
            (Some(task), Some(connector)) => Self::Both { task, connector },
        };
        Some(StdArc::new(failures))
    }

    /// Selects the failure used by retry scheduling and drain checks.
    /// A pending task retry takes precedence. Otherwise any connector retry stays visible to
    /// drains, ahead of a task error without a retry. With no retry, the task error takes precedence.
    pub(super) fn selected(&self) -> Arc<TaskFailure<R>> {
        match self {
            Self::Task(failure) | Self::Connector(failure) => failure.clone(),
            Self::Both { task, connector } => {
                if task.retry.is_some() || connector.retry.is_none() {
                    task.clone()
                } else {
                    connector.clone()
                }
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct TaskStatus<R> {
    failures: ArcSwapOption<TaskFailures<R>>,
}

impl<R> Default for TaskStatus<R> {
    fn default() -> Self {
        Self {
            failures: ArcSwapOption::empty(),
        }
    }
}

impl<R: Clone + PartialEq> TaskStatus<R> {
    /// Retains every failure in one immutable publication for operator inspection.
    pub(super) fn failures(&self) -> Option<StdArc<TaskFailures<R>>> {
        self.failures.load_full()
    }

    pub(super) fn snapshot(&self) -> Option<Arc<TaskFailure<R>>> {
        self.failures
            .load()
            .as_ref()
            .map(|failures| failures.selected())
    }

    pub(super) fn fail(&self, error: String, retry: Option<R>) {
        self.fail_owned(FailureOwner::Task, error, retry);
    }

    pub(super) fn fail_connector(&self, error: String, retry: Option<R>) {
        self.fail_owned(FailureOwner::Connector, error, retry);
    }

    fn fail_owned(&self, owner: FailureOwner, error: String, retry: Option<R>) {
        let failure = TaskFailure { error, retry };
        let unchanged = self
            .failures
            .load()
            .as_ref()
            .and_then(|current| current.owned(owner))
            .is_some_and(|current| current.as_ref() == &failure);
        if unchanged {
            return;
        }
        let failure = Arc::new(failure);
        self.failures.rcu(|current| {
            if current
                .as_ref()
                .and_then(|current| current.owned(owner))
                .is_some_and(|current| current.as_ref() == failure.as_ref())
            {
                return current.clone();
            }
            TaskFailures::replace(current.as_deref(), owner, Some(failure.clone()))
        });
    }

    pub(super) fn record_error(&self, error: String) {
        let unchanged = self
            .failures
            .load()
            .as_ref()
            .and_then(|current| current.owned(FailureOwner::Task))
            .is_some_and(|current| current.error == error);
        if unchanged {
            return;
        }
        // Reporting without a new retry preserves this task's active retry. The connector's
        // independently retained retry stays visible until that writer clears it.
        self.failures.rcu(|current| {
            let task = current
                .as_ref()
                .and_then(|current| current.owned(FailureOwner::Task));
            if task.is_some_and(|task| task.error == error) {
                return current.clone();
            }
            let retry = task.and_then(|task| task.retry.clone());
            let failure = Arc::new(TaskFailure {
                error: error.clone(),
                retry,
            });
            TaskFailures::replace(current.as_deref(), FailureOwner::Task, Some(failure))
        });
    }

    /// Healthy record paths only read. Success clears this task's failure and retains a failure
    /// reported by its connector's independent background task.
    pub(super) fn clear(&self) {
        self.clear_owned(FailureOwner::Task);
    }

    pub(super) fn clear_connector(&self) {
        self.clear_owned(FailureOwner::Connector);
    }

    fn clear_owned(&self, owner: FailureOwner) {
        let present = self
            .failures
            .load()
            .as_ref()
            .is_some_and(|current| current.owned(owner).is_some());
        if !present {
            return;
        }
        self.failures.rcu(|current| {
            if current
                .as_ref()
                .is_some_and(|current| current.owned(owner).is_some())
            {
                TaskFailures::replace(current.as_deref(), owner, None)
            } else {
                current.clone()
            }
        });
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::OptionExt as _;

    use super::*;

    #[test]
    fn bolero_status_sequences_publish_complete_transitions() {
        bolero::check!().with_type::<[u8; 64]>().for_each(|steps| {
            let status = TaskStatus::<u8>::default();
            let mut task: Option<TaskFailure<u8>> = None;
            let mut connector: Option<TaskFailure<u8>> = None;
            let mut snapshots = Vec::new();
            for step in steps {
                let error = format!("failure-{}", step % 4);
                match step % 5 {
                    0 => {
                        status.clear();
                        task = None;
                    }
                    1 => {
                        let retry = Some(step % 8);
                        status.fail(error.clone(), retry);
                        task = Some(TaskFailure { error, retry });
                    }
                    2 => {
                        status.record_error(error.clone());
                        let retry = task.as_ref().and_then(|failure| failure.retry);
                        task = Some(TaskFailure { error, retry });
                    }
                    3 => {
                        let retry = Some(step % 8);
                        status.fail_connector(error.clone(), retry);
                        connector = Some(TaskFailure { error, retry });
                    }
                    _ => {
                        status.clear_connector();
                        connector = None;
                    }
                }
                let expected = match (&task, &connector) {
                    (Some(task), Some(connector))
                        if task.retry.is_some() || connector.retry.is_none() =>
                    {
                        Some(task)
                    }
                    (Some(_), Some(connector)) => Some(connector),
                    (Some(task), None) => Some(task),
                    (None, connector) => connector.as_ref(),
                };
                let snapshot = status.snapshot();
                assert_eq!(snapshot.as_deref(), expected);
                let publication = status.failures();
                assert_eq!(
                    publication
                        .as_deref()
                        .and_then(|failures| failures.owned(FailureOwner::Task))
                        .map(|failure| failure.as_ref()),
                    task.as_ref()
                );
                assert_eq!(
                    publication
                        .as_deref()
                        .and_then(|failures| failures.owned(FailureOwner::Connector))
                        .map(|failure| failure.as_ref()),
                    connector.as_ref()
                );
                snapshots.push((snapshot, expected.cloned()));
            }
            status.clear();
            assert_eq!(status.snapshot().as_deref(), connector.as_ref());
            status.clear_connector();
            assert!(status.snapshot().is_none());
            for (snapshot, expected) in snapshots {
                assert_eq!(snapshot.as_deref(), expected.as_ref());
            }
        });
    }

    #[cfg(feature = "shuttle")]
    #[test]
    fn shuttle_status_observers_read_coherent_error_and_retry() {
        use meticulous::ResultExt as _;
        use nervix_primitives::sync::Arc;

        struct FailureSequence {
            first: &'static str,
            second: &'static str,
            retry: u8,
        }

        nervix_model_harness::shuttle::check_random(
            || {
                shuttle::future::block_on(async {
                    let status = Arc::new(TaskStatus::<u8>::default());
                    let mut writers = Vec::new();
                    for owner in [FailureOwner::Task, FailureOwner::Connector] {
                        let writer = status.clone();
                        writers.push(nervix_primitives::task::spawn(async move {
                            let sequence = match owner {
                                FailureOwner::Task => FailureSequence {
                                    first: "task-first",
                                    second: "task-second",
                                    retry: 1,
                                },
                                FailureOwner::Connector => FailureSequence {
                                    first: "connector-first",
                                    second: "connector-second",
                                    retry: 3,
                                },
                            };
                            writer.fail_owned(owner, sequence.first.into(), Some(sequence.retry));
                            nervix_primitives::task::yield_now().await;
                            writer.clear_owned(owner);
                            nervix_primitives::task::yield_now().await;
                            writer.fail_owned(
                                owner,
                                sequence.second.into(),
                                Some(sequence.retry + 1),
                            );
                        }));
                    }
                    let reader = status.clone();
                    let reading = nervix_primitives::task::spawn(async move {
                        for _ in 0..5 {
                            if let Some(failure) = reader.snapshot() {
                                let retry = match failure.error.as_str() {
                                    "task-first" => 1,
                                    "task-second" => 2,
                                    "connector-first" => 3,
                                    "connector-second" => 4,
                                    other => panic!("unexpected published failure: {other}"),
                                };
                                assert_eq!(failure.retry, Some(retry));
                            }
                            if let Some(publication) = reader.failures() {
                                for owner in [FailureOwner::Task, FailureOwner::Connector] {
                                    if let Some(failure) = publication.owned(owner) {
                                        let retry = match (owner, failure.error.as_str()) {
                                            (FailureOwner::Task, "task-first") => 1,
                                            (FailureOwner::Task, "task-second") => 2,
                                            (FailureOwner::Connector, "connector-first") => 3,
                                            (FailureOwner::Connector, "connector-second") => 4,
                                            (_, other) => {
                                                panic!("unexpected owned failure: {other}")
                                            }
                                        };
                                        assert_eq!(failure.retry, Some(retry));
                                    }
                                }
                            }
                            nervix_primitives::task::yield_now().await;
                        }
                    });
                    for writing in writers {
                        writing.await.assured("status transitions do not panic");
                    }
                    reading.await.assured("status observations do not panic");
                    let publication = status
                        .failures()
                        .assured("both owners retain their final failures");
                    let published_task = publication
                        .owned(FailureOwner::Task)
                        .assured("the task published its final failure");
                    let published_connector = publication
                        .owned(FailureOwner::Connector)
                        .assured("the connector published its final failure");
                    assert_eq!(published_task.error, "task-second");
                    assert_eq!(published_connector.error, "connector-second");
                    let task = status
                        .snapshot()
                        .assured("the task failure remains published");
                    assert_eq!(task.error, "task-second");
                    assert_eq!(task.retry, Some(2));
                    status.clear();
                    let connector = status
                        .snapshot()
                        .assured("clearing task status preserves the connector failure");
                    assert_eq!(connector.error, "connector-second");
                    assert_eq!(connector.retry, Some(4));
                    status.clear_connector();
                    assert!(status.snapshot().is_none());
                    assert_eq!(task.retry, Some(2));
                    assert_eq!(connector.retry, Some(4));
                    assert_eq!(published_task.retry, Some(2));
                    assert_eq!(published_connector.retry, Some(4));
                });
            },
            1_000,
        );
    }

    #[test]
    fn status_transitions_keep_error_and_retry_together() {
        let status = TaskStatus::<u64>::default();
        for _ in 0..100 {
            status.clear();
        }
        assert!(status.snapshot().is_none());
        status.fail("unavailable".into(), Some(7));
        let first = status.snapshot().assured("failure is published");
        status.fail("unavailable".into(), Some(7));
        assert!(Arc::ptr_eq(
            &first,
            &status.snapshot().assured("failure remains")
        ));
        status.record_error("retry failed".into());
        let failure = status.snapshot().assured("the error retains its retry");
        assert_eq!(
            *failure,
            TaskFailure {
                error: "retry failed".into(),
                retry: Some(7)
            }
        );
        status.clear();
        assert!(status.snapshot().is_none());
        assert_eq!(
            first.retry,
            Some(7),
            "observers keep their immutable snapshot"
        );
    }
    #[test]
    fn a_task_retry_survives_a_connector_failure_and_recovery() {
        let status = TaskStatus::<u8>::default();
        status.fail("publish unanswered".into(), Some(7));
        let task = status.snapshot().assured("the task failure is published");
        status.fail_connector("connection unavailable".into(), Some(9));
        assert!(Arc::ptr_eq(
            &task,
            &status.snapshot().assured("the task retry takes precedence")
        ));
        status.clear_connector();
        assert!(Arc::ptr_eq(
            &task,
            &status
                .snapshot()
                .assured("the task retry survives connection recovery")
        ));
        status.clear();
        assert!(status.snapshot().is_none());
    }

    #[test]
    fn a_task_error_keeps_the_connector_retry_visible_until_its_owner_clears_it() {
        let status = TaskStatus::<u8>::default();
        status.fail_connector("connection unavailable".into(), Some(9));
        status.record_error("request refused".into());
        let connector = status
            .snapshot()
            .assured("the connector retry stays visible");
        assert_eq!(connector.error, "connection unavailable");
        assert_eq!(connector.retry, Some(9));
        status.clear_connector();
        let task = status.snapshot().assured("the task error remains");
        assert_eq!(task.error, "request refused");
        assert_eq!(task.retry, None);
        status.clear();
        assert!(status.snapshot().is_none());
        assert_eq!(connector.retry, Some(9));
    }
}
