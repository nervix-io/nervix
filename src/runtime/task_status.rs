//! Connector task status publication.
//!
//! Layer: data plane.
//! - **Owns.** One connector's coherent failure and retry status, published on transitions.
//! - **Depends on.** The primitive publication boundary and connector retry vocabulary.
//! - **Must not know.** Shared registries, graph planning, payloads or transport drivers.

use nervix_primitives::{publication::ArcSwapOption, sync::StdArc};

/// Healthy tasks publish no failure. The error and its retry are observed together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TaskFailure<R> {
    pub(super) error: String,
    pub(super) retry: Option<R>,
}

#[derive(Debug)]
pub(super) struct TaskStatus<R> {
    failure: ArcSwapOption<TaskFailure<R>>,
}

impl<R> Default for TaskStatus<R> {
    fn default() -> Self {
        Self {
            failure: ArcSwapOption::empty(),
        }
    }
}

impl<R: Clone + PartialEq> TaskStatus<R> {
    pub(super) fn snapshot(&self) -> Option<StdArc<TaskFailure<R>>> {
        self.failure.load_full()
    }

    pub(super) fn fail(&self, error: String, retry: Option<R>) {
        let failure = TaskFailure { error, retry };
        if self.failure.load().as_deref() != Some(&failure) {
            self.failure.store(Some(StdArc::new(failure)));
        }
    }

    pub(super) fn record_error(&self, error: String) {
        if let Some(current) = self.failure.load().as_ref()
            && current.error == error
        {
            return;
        }
        // Reporting without a new retry preserves the active retry's drain obligation.
        self.failure.rcu(|current| {
            let retry = match current {
                Some(current) => current.retry.clone(),
                None => None,
            };
            let failure = TaskFailure {
                error: error.clone(),
                retry,
            };
            if current.as_deref() == Some(&failure) {
                return current.clone();
            }
            Some(StdArc::new(failure))
        });
    }

    /// The successful record path reads a retained publication. Healthy polls never write it.
    pub(super) fn clear(&self) {
        if self.failure.load().is_some() {
            self.failure.store(None);
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::OptionExt as _;

    use super::*;

    #[test]
    fn bolero_status_sequences_publish_complete_transitions() {
        bolero::check!().with_type::<[u8; 64]>().for_each(|steps| {
            let status = TaskStatus::<u8>::default();
            let mut expected: Option<TaskFailure<u8>> = None;
            let mut snapshots = Vec::new();
            for step in steps {
                let error = format!("failure-{}", step % 4);
                match step % 3 {
                    0 => {
                        status.clear();
                        expected = None;
                    }
                    1 => {
                        let retry = Some(step % 8);
                        status.fail(error.clone(), retry);
                        expected = Some(TaskFailure { error, retry });
                    }
                    _ => {
                        status.record_error(error.clone());
                        let retry = match &expected {
                            Some(failure) => failure.retry,
                            None => None,
                        };
                        expected = Some(TaskFailure { error, retry });
                    }
                }
                let snapshot = status.snapshot();
                assert_eq!(snapshot.as_deref(), expected.as_ref());
                snapshots.push((snapshot, expected.clone()));
            }
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

        nervix_model_harness::shuttle::check_random(
            || {
                shuttle::future::block_on(async {
                    let status = Arc::new(TaskStatus::<u8>::default());
                    let writer = status.clone();
                    let writing = nervix_primitives::task::spawn(async move {
                        writer.fail("first".into(), Some(1));
                        nervix_primitives::task::yield_now().await;
                        writer.clear();
                        nervix_primitives::task::yield_now().await;
                        writer.fail("second".into(), Some(2));
                    });
                    let reader = status.clone();
                    let reading = nervix_primitives::task::spawn(async move {
                        for _ in 0..3 {
                            if let Some(failure) = reader.snapshot() {
                                match failure.error.as_str() {
                                    "first" => assert_eq!(failure.retry, Some(1)),
                                    "second" => assert_eq!(failure.retry, Some(2)),
                                    other => panic!("unexpected published failure: {other}"),
                                }
                            }
                            nervix_primitives::task::yield_now().await;
                        }
                    });
                    writing.await.assured("status transitions do not panic");
                    reading.await.assured("status observations do not panic");
                    assert_eq!(
                        status
                            .snapshot()
                            .assured("the final failure is published")
                            .retry,
                        Some(2)
                    );
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
        assert!(StdArc::ptr_eq(
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
}
