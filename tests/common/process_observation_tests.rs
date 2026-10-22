//! Regressions for observing a running process and its host through procfs.
//!
//! Layer: test harness.
//! - **Owns.** Stand-in child processes in known states, and synthetic thread groups beyond the
//!   bounds of a description.
//! - **Depends on.** The process observation owner, the phase deadline, Tokio's process module and
//!   signal delivery.
//! - **Must not know.** What a server process runs or why a scenario observes it.

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use tokio::process::{Child, Command};

use crate::{
    phase_deadline::PhaseDeadline,
    process_observation::{
        ProcessObservation, ThreadName, ThreadState, ThreadWait, ThreadWaits, WaitChannel,
    },
};

/// How long a stand-in child may take to reach the state a test waits for. The wait ends as soon
/// as it does, so the bound only has to hold on a machine running every harness test at once.
const STATE_BUDGET: Duration = Duration::from_secs(60);
const STATE_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// A child process the test owns.
struct StandInChild {
    /// Held so that dropping the stand-in kills the process.
    _child: Child,
    pid: u32,
}

impl StandInChild {
    fn sleeping() -> Self {
        let child = Command::new("sleep")
            .arg("600")
            .kill_on_drop(true)
            .spawn()
            .assured("the test host runs sleep");
        let pid = child
            .id()
            .assured("a child that was just spawned has a process id");
        Self { _child: child, pid }
    }

    fn signal(&self, signal: Signal) {
        let pid = i32::try_from(self.pid).assured("Linux process ids fit in an i32");
        kill(Pid::from_raw(pid), signal).assured("the test may signal its own child");
    }

    /// Observes the child until its description contains every one of `fragments`, and returns
    /// that description.
    async fn observe_until(&self, fragments: &[&str]) -> String {
        let deadline = PhaseDeadline::after(STATE_BUDGET);
        loop {
            nervix_primitives::task::consume_budget().await;
            let description = ProcessObservation::of(self.pid).await.to_string();
            let shows_every_fragment = fragments
                .iter()
                .all(|fragment| description.contains(fragment));
            if shows_every_fragment {
                return description;
            }
            if deadline.has_passed() {
                panic!(
                    "process {} never showed {fragments:?}:\n{description}",
                    self.pid
                );
            }
            deadline.pause(STATE_POLL_INTERVAL).await;
        }
    }

    /// Observes the child until it has executed `sleep` and sleeps in it.
    async fn observe_asleep(&self) -> String {
        self.observe_until(&[": sleeping, ", "sleep ×1"]).await
    }
}

#[nervix_primitives::test]
async fn a_sleeping_child_is_described_with_its_thread_and_its_host() {
    let child = StandInChild::sleeping();

    let description = child.observe_asleep().await;

    assert!(
        description.starts_with(&format!("process {}: sleeping, 1 threads, ", child.pid)),
        "{description}"
    );
    assert!(
        description.contains("major and"),
        "a description counts page faults: {description}"
    );
    assert!(
        description.contains("[1 threads sleeping in "),
        "a sleeping thread waits in a kernel function: {description}"
    );
    assert!(description.contains("\nhost: load "), "{description}");
    assert!(description.contains("; io pressure "), "{description}");
}

#[nervix_primitives::test]
async fn a_stopped_child_is_described_as_stopped() {
    let child = StandInChild::sleeping();
    child.observe_asleep().await;

    child.signal(Signal::SIGSTOP);
    let description = child.observe_until(&[": stopped, "]).await;

    assert!(
        description.contains("[1 threads stopped in "),
        "the stopped thread is counted as stopped: {description}"
    );
    child.signal(Signal::SIGCONT);
}

#[nervix_primitives::test]
async fn a_reaped_child_is_described_as_ended() {
    let mut child = Command::new("true")
        .spawn()
        .assured("the test host runs true");
    let pid = child
        .id()
        .assured("a child that was just spawned has a process id");
    let status = child
        .wait()
        .await
        .assured("the test waits for its own child");
    assert!(status.success());

    let description = ProcessObservation::of(pid).await.to_string();

    assert!(
        description.starts_with(&format!("process {pid} had ended before it was observed")),
        "{description}"
    );
}

#[nervix_primitives::test]
async fn a_description_lists_a_bounded_number_of_groups_and_names() {
    let mut waits = ThreadWaits::default();
    for group in 0..40 {
        for name in 0..6 {
            waits.count(
                ThreadWait {
                    state: ThreadState::Sleeping,
                    channel: WaitChannel::Named(format!("wait_{group:02}")),
                },
                ThreadName::Named(format!("worker-{name}")),
            );
        }
    }
    waits.add_run_queue_wait(Duration::from_millis(1500));

    let description = waits.to_string();

    assert!(
        description.starts_with("threads by state and kernel wait, 1.50s of run-queue wait"),
        "{description}"
    );
    assert_eq!(
        description.matches(" [6 threads sleeping in ").count(),
        16,
        "{description}"
    );
    assert!(
        description.ends_with(" and 24 smaller groups"),
        "{description}"
    );
    assert_eq!(
        description.matches(" and 2 other names]").count(),
        16,
        "{description}"
    );
}

#[nervix_primitives::test]
async fn a_description_orders_groups_from_the_largest() {
    let mut waits = ThreadWaits::default();
    waits.count(
        ThreadWait {
            state: ThreadState::DiskSleep,
            channel: WaitChannel::Named("folio_wait_bit_common".to_string()),
        },
        ThreadName::Named("nervix-server".to_string()),
    );
    for _ in 0..3 {
        waits.count(
            ThreadWait {
                state: ThreadState::Sleeping,
                channel: WaitChannel::Named("futex_do_wait".to_string()),
            },
            ThreadName::Named("tokio-rt-worker".to_string()),
        );
    }

    let description = waits.to_string();

    let Some(futex) = description.find("[3 threads sleeping in futex_do_wait: tokio-rt-worker ×3]")
    else {
        panic!("the description lists the group of three sleeping workers: {description}");
    };
    let Some(disk) =
        description.find("[1 threads disk sleep in folio_wait_bit_common: nervix-server ×1]")
    else {
        panic!("the description lists the thread in disk sleep: {description}");
    };
    assert!(futex < disk, "{description}");
}
