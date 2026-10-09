//! Active deadlocks among tracked locks, each found in a disposable process.
//!
//! The detector is process-wide and installs once, and a process that deadlocks ends, so every
//! probe here starts this test binary again as a child that runs one workload. The child starts a
//! diagnostic run with the evidence directory its probe chose, runs the workload, and either ends
//! cleanly or is ended by the run's recorder. The probe bounds the child with a watchdog that kills
//! it, then asserts its exit status, what it wrote, and the evidence it recorded: a deadlock that
//! leaves no finding, a finding that leaves no evidence, and a child that never ends all fail.
//!
//! The workloads deadlock on purpose and only here, in a test binary of this crate; no product
//! binary has a way to deadlock on request. The diagnostic lane's supervision qualification also
//! starts the `workload` entry point directly, with workloads that must fail its supervisor in each
//! of the ways a diagnostic process can fail.

use std::{
    env,
    io::Read,
    path::Path,
    process::{Command, ExitStatus, Stdio},
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_deadlock::{
    ACTIVE_DEADLOCK_EXIT_STATUS, DIAGNOSTIC_FAILURE_EXIT_STATUS, DeadlockEvidence, DiagnosticError,
    DiagnosticRun, EvidenceDirectory, RecordedFinding as Finding,
};
#[cfg(feature = "deloxide-order")]
use nervix_deadlock::{PotentialTriage, ProofBasis, TriageProof};
use nervix_primitives::{
    deadlock::{Access, ActiveCycle, LockKind},
    sync::{
        Arc,
        blocking::{Barrier, Condvar, Mutex, RwLock, mpsc},
    },
    thread,
};

/// The workload a child runs, named by its probe.
const WORKLOAD: &str = "NERVIX_DEADLOCK_PROBE_WORKLOAD";
/// The evidence directory a child records in, when its probe gives one.
const EVIDENCE: &str = "NERVIX_DEADLOCK_PROBE_EVIDENCE";

/// How long a child may run before its watchdog kills it. Every workload ends within milliseconds
/// on an idle machine; the bound only decides when a child that never ends fails its probe.
const CHILD_BOUND: Duration = Duration::from_secs(120);

/// Every source site a finding names here is in this file.
const THIS_FILE: &str = "active_cycles.rs";

/// What the deadlock detector prints when it starts, which no output of a diagnostic process may
/// contain.
const DETECTOR_BANNER: &str = "▄ ▄▖▖ ▄▖▖▖▄▖▄";

const PARTICIPANT_JOINS: &str = "a participant panics only when an assertion fails";

/// The child's side: run the workload its probe named. A run without a probe returns at once.
#[test]
#[ignore = "a disposable workload; its probe runs it in a child process"]
fn workload() {
    let Ok(workload) = env::var(WORKLOAD) else {
        return;
    };
    if workload == "a_lock_before_the_run" {
        let early = Mutex::new(0_u8);
        drop(early);
        return;
    }
    let directory = env::var_os(EVIDENCE).map(EvidenceDirectory::new);
    if workload == "a_missing_evidence_directory" {
        let refused = DiagnosticRun::start(directory);
        let Err(refusal) = refused else {
            panic!("a run started with an evidence directory that does not exist");
        };
        assert_eq!(*refusal.current_context(), DiagnosticError::RecordStart);
        return;
    }
    let selection = nervix_primitives::deadlock::DiagnosticSelection::for_build(matches!(
        workload.as_str(),
        "instrumented_active_only" | "instrumented_self_wait"
    ));
    let run = DiagnosticRun::start_selected(directory, selection)
        .assured("the child starts its run once");
    match workload.as_str() {
        "two_mutexes_in_opposite_orders" => two_mutexes_in_opposite_orders(),
        "a_mutex_relocked_by_its_holder" => a_mutex_relocked_by_its_holder(),
        "a_read_lock_upgraded_by_its_holder" => a_read_lock_upgraded_by_its_holder(),
        "a_writer_and_a_reader_across_two_locks" => a_writer_and_a_reader_across_two_locks(),
        "a_notified_waiter_and_its_notifier" => a_notified_waiter_and_its_notifier(),
        "two_mutexes_in_one_order" => two_mutexes_in_one_order(),
        "serial_inversion" => serial_inversion(run),
        "many_worker_contexts" => many_worker_contexts(run),
        "instrumented_active_only" => serial_orders(),
        "instrumented_self_wait" => a_mutex_relocked_by_its_holder(),
        "shared_order_inversion" => shared_order_inversion(),
        "mixed_order_inversion" => mixed_order_inversion(run),
        "separate_lifetimes" => separate_lifetimes(),
        "held_guard_overload" => held_guard_overload(),
        "witness_overload" => witness_overload(),
        "history_overload" => history_overload(),
        "retention_overload" => retention_overload(),
        "blocked_report_output" => retention_overload(),
        "failed_report_output" => {
            serial_orders();
            thread::park();
        }
        "a_condition_handed_between_threads" => a_condition_handed_between_threads(),
        "readers_sharing_a_lock" => readers_sharing_a_lock(),
        #[cfg(feature = "deloxide-stress")]
        "nested_acquisitions_under_stress" => nested_acquisitions_under_stress(),
        "quiet_standard_output" => println!("probe output after the detector started"),
        // The diagnostic lane's supervision qualification starts these two directly: a process
        // that never ends on a wait the detector does not track, and one a signal ends.
        "an_untracked_wait_that_never_ends" => loop {
            thread::park();
        },
        "an_aborted_process" => std::process::abort(),
        "a_second_run" => {
            let refused = DiagnosticRun::start(None);
            let Err(refusal) = refused else {
                panic!("a second run started in one process");
            };
            assert_eq!(*refusal.current_context(), DiagnosticError::Install);
            assert!(DiagnosticRun::current().is_some_and(|current| std::ptr::eq(current, run)));
        }
        "evidence_that_cannot_be_recorded" => {
            let file = run
                .evidence_file()
                .assured("the probe gives an evidence directory");
            let directory = file
                .parent()
                .assured("an evidence file is in its directory");
            std::fs::remove_dir_all(directory).assured("the probe's directory can be removed");
            std::fs::write(directory, b"not a directory").assured("the path can become a file");
            a_mutex_relocked_by_its_holder();
        }
        "a_run_without_an_evidence_directory" => a_mutex_relocked_by_its_holder(),
        unknown => panic!("no workload is named {unknown}"),
    }
}

fn two_mutexes_in_opposite_orders() {
    let first = Arc::new(Mutex::new(()));
    let second = Arc::new(Mutex::new(()));
    let both_hold_one = Arc::new(Barrier::new(2));
    let mut participants = Vec::new();
    for (name, held, wanted) in [
        ("probe-first", Arc::clone(&first), Arc::clone(&second)),
        ("probe-second", Arc::clone(&second), Arc::clone(&first)),
    ] {
        let both_hold_one = Arc::clone(&both_hold_one);
        let participant = thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                let _held = held.lock();
                both_hold_one.wait();
                let _wanted = wanted.lock();
            })
            .assured("the child can start its participants");
        participants.push(participant);
    }
    for participant in participants {
        participant.join().assured(PARTICIPANT_JOINS);
    }
}

fn a_mutex_relocked_by_its_holder() {
    let (mutex, constructed_at) = (Mutex::new(()), line!());
    println!("constructed at line {constructed_at}");
    let _held = mutex.lock();
    println!("waits at line {}", line!() + 1);
    let _again = mutex.lock();
}

fn a_read_lock_upgraded_by_its_holder() {
    let lock = RwLock::new(0_u8);
    let _reading = lock.read();
    let _writing = lock.write();
}

fn a_writer_and_a_reader_across_two_locks() {
    let shared = Arc::new(RwLock::new(0_u8));
    let exclusive = Arc::new(Mutex::new(0_u8));
    let both_hold_one = Arc::new(Barrier::new(2));
    let writer = {
        let (shared, exclusive, both_hold_one) = (
            Arc::clone(&shared),
            Arc::clone(&exclusive),
            Arc::clone(&both_hold_one),
        );
        thread::Builder::new()
            .name("probe-writer".to_string())
            .spawn(move || {
                let _writing = shared.write();
                both_hold_one.wait();
                let _exclusive = exclusive.lock();
            })
            .assured("the child can start its participants")
    };
    let reader = thread::Builder::new()
        .name("probe-reader".to_string())
        .spawn(move || {
            let _exclusive = exclusive.lock();
            both_hold_one.wait();
            let _reading = shared.read();
        })
        .assured("the child can start its participants");
    writer.join().assured(PARTICIPANT_JOINS);
    reader.join().assured(PARTICIPANT_JOINS);
}

/// The waiter holds a lock the notifier wants next, and the notifier holds the waiter's mutex while
/// it notifies: the woken waiter waits for its mutex, and the notifier for the waiter's lock.
fn a_notified_waiter_and_its_notifier() {
    let state = Arc::new((Mutex::new(Signal::default()), Condvar::new()));
    let held_by_waiter = Arc::new(Mutex::new(()));
    let waiter = {
        let (state, held_by_waiter) = (Arc::clone(&state), Arc::clone(&held_by_waiter));
        thread::Builder::new()
            .name("probe-waiter".to_string())
            .spawn(move || {
                let _held = held_by_waiter.lock();
                let (signal, changed) = &*state;
                let mut signal = signal.lock();
                signal.waiting = true;
                while !signal.sent {
                    changed.wait(&mut signal);
                }
            })
            .assured("the child can start its participants")
    };
    let notifier = thread::Builder::new()
        .name("probe-notifier".to_string())
        .spawn(move || {
            let (signal, changed) = &*state;
            loop {
                let mut signal = signal.lock();
                // The waiter marks itself under the mutex and releases it only by waiting.
                if signal.waiting {
                    signal.sent = true;
                    assert_eq!(changed.notify_all(), 1);
                    let _wanted = held_by_waiter.lock();
                    return;
                }
                drop(signal);
                thread::yield_now();
            }
        })
        .assured("the child can start its participants");
    waiter.join().assured(PARTICIPANT_JOINS);
    notifier.join().assured(PARTICIPANT_JOINS);
}

#[derive(Default)]
struct Signal {
    waiting: bool,
    sent: bool,
}

const CONTROL_ROUNDS: u32 = 2_000;

fn serial_orders() {
    let first = Mutex::new(());
    let second = Mutex::new(());
    {
        let _first = first.lock();
        let _second = second.lock();
    }
    {
        let _second = second.lock();
        let _first = first.lock();
    }
}

fn serial_inversion(run: &DiagnosticRun) {
    serial_orders();
    wait_for_potential(run);
}

const WORKER_CONTEXTS: usize = 512;

fn many_worker_contexts(run: &DiagnosticRun) {
    let locks = Arc::new([Mutex::new(()), Mutex::new(())]);
    for _ in 0..WORKER_CONTEXTS {
        let locks = Arc::clone(&locks);
        thread::spawn(move || {
            let _first = locks[0].lock();
            let _second = locks[1].lock();
        })
        .join()
        .assured(PARTICIPANT_JOINS);
    }
    {
        let _second = locks[1].lock();
        let _first = locks[0].lock();
    }
    wait_for_potential(run);
}

fn wait_for_potential(run: &DiagnosticRun) {
    let file = run
        .evidence_file()
        .assured("the probe provides an evidence directory");
    let deadline = nervix_primitives::time::Instant::now() + Duration::from_secs(10);
    loop {
        let bytes = std::fs::read(file).assured("the probe's evidence is readable");
        let evidence = DeadlockEvidence::decode(&bytes).assured("the run writes valid evidence");
        if !evidence.findings().is_empty() {
            println!("workload continued after a potential finding");
            return;
        }
        assert!(
            nervix_primitives::time::Instant::now() < deadline,
            "serial inversion produced no potential evidence"
        );
        thread::yield_now();
    }
}

#[cfg(feature = "deloxide-order")]
#[test]
fn serial_inverse_order_retains_potential_evidence_and_the_workload_continues() {
    let directory = tempfile::tempdir().assured("a temporary directory can be created");
    let child = run_child("serial_inversion", Some(directory.path()));
    assert_eq!(child.exit_code(), 0, "{}", child.describe());
    let evidence = only_evidence(directory.path(), &child);
    let [
        Finding::Potential {
            cycle,
            repetitions,
            triage,
        },
    ] = evidence.findings()
    else {
        panic!("a retained potential cycle: {evidence:?}")
    };
    assert_eq!(repetitions.get(), 1);
    assert_eq!(*triage, PotentialTriage::Unreviewed);
    assert_eq!(cycle.edges().len(), 2);
    assert!(cycle.has_complete_context(), "{cycle:?}");
    for edge in cycle.edges() {
        assert_ne!(edge.before.id, edge.after.id);
        assert!(
            edge.before
                .site
                .as_ref()
                .assured("construction retained")
                .constructed_at
                .file
                .as_str()
                .ends_with(THIS_FILE)
        );
        for witness in edge.witnesses() {
            assert_eq!(witness.held.access, Access::Exclusive);
            assert_eq!(witness.requested.access, Access::Exclusive);
            assert_eq!(witness.held_count.get(), 1);
            assert!(witness.held.at.file.as_str().ends_with(THIS_FILE));
            assert!(witness.requested.at.file.as_str().ends_with(THIS_FILE));
        }
    }
    assert!(!evidence.qualifies());
    assert!(
        child.stderr.contains("potential lock-order"),
        "{}",
        child.describe()
    );
    assert!(
        child
            .stdout
            .contains("workload continued after a potential finding"),
        "{}",
        child.describe()
    );
}

fn shared_order_inversion() {
    let first = RwLock::new(());
    let second = RwLock::new(());
    {
        let _first: Vec<_> = (0..2).map(|_| first.read()).collect();
        let _second = second.read();
    }
    {
        let _second = second.read();
        let _first = first.read();
    }
}

#[cfg(feature = "deloxide-order")]
#[test]
fn a_historical_edge_retains_every_distinct_worker_context() {
    let directory = tempfile::tempdir().assured("temporary evidence directory");
    let child = run_child("many_worker_contexts", Some(directory.path()));
    assert_eq!(child.exit_code(), 0, "{}", child.describe());
    let evidence = only_evidence(directory.path(), &child);
    let [Finding::Potential { cycle, .. }] = evidence.findings() else {
        panic!("one complete historical cycle: {evidence:?}")
    };
    assert!(cycle.has_complete_context(), "{cycle:?}");
    let mut counts: Vec<_> = cycle
        .edges()
        .iter()
        .map(|edge| edge.witnesses().len())
        .collect();
    counts.sort_unstable();
    assert_eq!(counts, [1, WORKER_CONTEXTS]);
    for edge in cycle.edges() {
        let mut threads: Vec<_> = edge
            .witnesses()
            .iter()
            .map(|witness| witness.thread)
            .collect();
        threads.sort_unstable();
        threads.dedup();
        assert_eq!(threads.len(), edge.witnesses().len());
        assert!(edge.witnesses().iter().all(|witness| {
            witness.held.at.file.as_str().ends_with(THIS_FILE)
                && witness.requested.at.file.as_str().ends_with(THIS_FILE)
                && witness.held.access == Access::Exclusive
                && witness.requested.access == Access::Exclusive
                && witness.held_count.get() == 1
        }));
    }
    assert!(!evidence.qualifies());
}

fn mixed_order_inversion(run: &DiagnosticRun) {
    let first = RwLock::new(());
    let second = Mutex::new(());
    {
        let _first: Vec<_> = (0..2).map(|_| first.read()).collect();
        let _second = second.lock();
    }
    {
        let _second = second.lock();
        let _first = first.write();
    }
    wait_for_potential(run);
}

fn separate_lifetimes() {
    fn pair() -> [Mutex<()>; 2] {
        [Mutex::new(()), Mutex::new(())]
    }
    {
        let locks = pair();
        let _first = locks[0].lock();
        let _second = locks[1].lock();
    }
    {
        let locks = pair();
        let _second = locks[1].lock();
        let _first = locks[0].lock();
    }
}

fn held_guard_overload() {
    let locks: Vec<_> = (0..65).map(|_| Mutex::new(())).collect();
    let _held: Vec<_> = locks.iter().map(Mutex::lock).collect();
    thread::park();
}

fn witness_overload() {
    let locks = Arc::new([Mutex::new(()), Mutex::new(())]);
    for _ in 0..=nervix_primitives::deadlock::MAX_ORDER_WITNESSES {
        let locks = Arc::clone(&locks);
        thread::spawn(move || {
            let _first = locks[0].lock();
            let _second = locks[1].lock();
        })
        .join()
        .assured(PARTICIPANT_JOINS);
    }
    thread::park();
}

fn history_overload() {
    // 182 choose 2 yields 16,471 directed pairs, exceeding the bounded 16,384-edge history.
    let locks: Vec<_> = (0..182).map(|_| Mutex::new(())).collect();
    for (index, first) in locks.iter().enumerate() {
        for second in &locks[index + 1..] {
            let _first = first.lock();
            let _second = second.lock();
        }
    }
    thread::park();
}

fn retention_overload() {
    let locks: Vec<_> = (0..32).map(|_| Mutex::new(())).collect();
    for pair in locks.as_chunks::<2>().0 {
        {
            let _first = pair[0].lock();
            let _second = pair[1].lock();
        }
        {
            let _second = pair[1].lock();
            let _first = pair[0].lock();
        }
    }
    thread::park();
}

#[cfg(feature = "deloxide-order")]
#[test]
fn shared_read_orders_complete_without_an_order_finding() {
    completed("shared_order_inversion");
}

#[cfg(feature = "deloxide-order")]
#[test]
fn mixed_order_history_retains_modes_multiplicity_and_a_serial_non_overlap_proof() {
    let directory = tempfile::tempdir().assured("temporary evidence directory");
    let child = run_child("mixed_order_inversion", Some(directory.path()));
    assert_eq!(child.exit_code(), 0, "{}", child.describe());
    let mut evidence = only_evidence(directory.path(), &child);
    let [Finding::Potential { cycle, .. }] = evidence.findings() else {
        panic!("shared order history: {evidence:?}")
    };
    assert!(
        cycle
            .edges()
            .iter()
            .flat_map(|edge| edge.witnesses())
            .any(|witness| witness.held.access == Access::Shared && witness.held_count.get() == 2)
    );
    assert!(
        cycle
            .edges()
            .iter()
            .flat_map(|edge| edge.witnesses())
            .all(|witness| witness.requested.access == Access::Exclusive)
    );
    assert_eq!(
        cycle
            .edges()
            .iter()
            .map(|edge| edge
                .witnesses()
                .iter()
                .map(|witness| witness.held_count.get())
                .sum::<u64>())
            .max(),
        Some(2)
    );
    let reference = "active_cycles::mixed_order_history_retains_modes_multiplicity_and_a_serial_non_overlap_proof";
    let readers = TriageProof::new(
        ProofBasis::SharedReaders,
        "These acquisitions share reads.",
        reference,
    )
    .assured("a bounded review");
    assert!(
        evidence.triage(0, readers).is_err(),
        "exclusive access cannot receive a reader proof"
    );
    let proof = TriageProof::new(
        ProofBasis::NonOverlap,
        "The probe owns all acquisitions on one thread, in sequential scopes, with no other \
         participants.",
        reference,
    )
    .assured("specific proof and retained regression");
    evidence
        .triage(0, proof)
        .assured("complete source evidence supports explicit review");
    assert!(evidence.qualifies());
}

#[cfg(feature = "deloxide-order")]
#[test]
fn reused_construction_sites_keep_separate_lock_lifetimes() {
    completed("separate_lifetimes");
}

#[cfg(feature = "deloxide-order")]
#[test]
fn runtime_active_only_reports_the_instrumented_selection() {
    let directory = tempfile::tempdir().assured("temporary evidence directory");
    let child = run_child("instrumented_active_only", Some(directory.path()));
    assert_eq!(child.exit_code(), 0, "{}", child.describe());
    let evidence = only_evidence(directory.path(), &child);
    assert_eq!(
        evidence.process().selection,
        nervix_primitives::deadlock::DiagnosticSelection::OrderInstrumentedActiveOnly
    );
    assert!(evidence.findings().is_empty());
    let (_, evidence) = deadlocked("instrumented_self_wait");
    assert_eq!(
        evidence.process().selection,
        nervix_primitives::deadlock::DiagnosticSelection::OrderInstrumentedActiveOnly
    );
    assert_eq!(only_cycle(&evidence).threads().len(), 1);
}

#[cfg(feature = "deloxide-order")]
#[test]
fn order_context_and_retention_overload_leave_failure_evidence() {
    for workload in [
        "held_guard_overload",
        "witness_overload",
        "history_overload",
        "retention_overload",
    ] {
        let directory = tempfile::tempdir().assured("temporary evidence directory");
        let child = run_child(workload, Some(directory.path()));
        assert_eq!(
            child.exit_code(),
            DIAGNOSTIC_FAILURE_EXIT_STATUS,
            "{}",
            child.describe()
        );
        let evidence = only_evidence(directory.path(), &child);
        assert!(
            evidence
                .findings()
                .iter()
                .any(|finding| matches!(finding, Finding::Overflow { .. })),
            "{evidence:?}"
        );
        assert!(!evidence.qualifies());
        assert!(child.stderr.contains("overload"), "{}", child.describe());
        if matches!(workload, "history_overload" | "witness_overload") {
            assert!(evidence.findings().iter().any(|finding| matches!(
                finding,
                Finding::Overflow {
                    source: nervix_deadlock::EvidenceLossSource::OrderHistory,
                    ..
                }
            )));
        }
    }
}

fn two_mutexes_in_one_order() {
    let first = Arc::new(Mutex::new(0_u32));
    let second = Arc::new(Mutex::new(0_u32));
    let mut participants = Vec::new();
    for name in ["probe-one", "probe-two"] {
        let (first, second) = (Arc::clone(&first), Arc::clone(&second));
        let participant = thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                for _ in 0..CONTROL_ROUNDS {
                    let mut first = first.lock();
                    let mut second = second.lock();
                    *first = first
                        .checked_add(1)
                        .assured("two rounds of counts fit a u32");
                    *second = second
                        .checked_add(1)
                        .assured("two rounds of counts fit a u32");
                }
            })
            .assured("the child can start its participants");
        participants.push(participant);
    }
    for participant in participants {
        participant.join().assured(PARTICIPANT_JOINS);
    }
    let rounds = CONTROL_ROUNDS
        .checked_mul(2)
        .assured("two rounds fit a u32");
    assert_eq!(*first.lock(), rounds);
    assert_eq!(*second.lock(), rounds);
}

fn a_condition_handed_between_threads() {
    let state = Arc::new((Mutex::new(Turn::default()), Condvar::new()));
    let producer = {
        let state = Arc::clone(&state);
        thread::Builder::new()
            .name("probe-producer".to_string())
            .spawn(move || {
                let (turn, changed) = &*state;
                for value in 1..=CONTROL_ROUNDS {
                    let mut turn = turn.lock();
                    while turn.value.is_some() {
                        changed.wait(&mut turn);
                    }
                    turn.value = Some(value);
                    changed.notify_all();
                }
            })
            .assured("the child can start its participants")
    };
    let (turn, changed) = &*state;
    for expected in 1..=CONTROL_ROUNDS {
        let mut turn = turn.lock();
        loop {
            if let Some(value) = turn.value.take() {
                assert_eq!(value, expected);
                break;
            }
            changed.wait(&mut turn);
        }
        changed.notify_all();
    }
    producer.join().assured(PARTICIPANT_JOINS);
}

#[derive(Default)]
struct Turn {
    value: Option<u32>,
}

fn readers_sharing_a_lock() {
    let lock = Arc::new(RwLock::new(5_u8));
    let all_read = Arc::new(Barrier::new(3));
    let (sender, received) = mpsc::channel();
    let mut readers = Vec::new();
    for name in ["probe-reader-1", "probe-reader-2", "probe-reader-3"] {
        let (lock, all_read, sender) = (Arc::clone(&lock), Arc::clone(&all_read), sender.clone());
        let reader = thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                let reading = lock.read();
                all_read.wait();
                sender
                    .send(*reading)
                    .assured("the child keeps its receiver until the readers end");
            })
            .assured("the child can start its participants");
        readers.push(reader);
    }
    drop(sender);
    for reader in readers {
        reader.join().assured(PARTICIPANT_JOINS);
    }
    let values: Vec<u8> = received.iter().collect();
    assert_eq!(values, [5, 5, 5]);
}

/// How many nested acquisitions the stress probe makes.
#[cfg(feature = "deloxide-stress")]
const STRESS_ROUNDS: u32 = 20_000;

/// One thread that takes an inner mutex while it holds an outer one, for every round, and must
/// spend at least the shortest delay for half the rounds the lane's configuration expects to
/// preempt. With a probability of one twentieth over twenty thousand rounds, a thousand preemptions
/// are expected and falling short of half of them is sixteen standard deviations away, so only an
/// absent disturbance fails it; load only lengthens the time it measures. The bound, ten
/// milliseconds, is twice what the same rounds take undisturbed on an idle host.
#[cfg(feature = "deloxide-stress")]
fn nested_acquisitions_under_stress() {
    use nervix_primitives::{
        deadlock::{PREEMPTION_SCALE, StressConfiguration},
        time::Instant,
    };

    let outer = Mutex::new(0_u32);
    let inner = Mutex::new(0_u32);
    let started = Instant::now();
    for _ in 0..STRESS_ROUNDS {
        let mut outer = outer.lock();
        let mut inner = inner.lock();
        *outer = outer.checked_add(1).assured("the rounds fit a u32");
        *inner = inner.checked_add(1).assured("the rounds fit a u32");
    }
    let elapsed = started.elapsed();
    assert_eq!(*outer.lock(), STRESS_ROUNDS);
    assert_eq!(*inner.lock(), STRESS_ROUNDS);
    let lane = StressConfiguration::LANE;
    let expected = u64::from(STRESS_ROUNDS)
        .checked_mul(u64::from(lane.preemptions_per_million().get()))
        .assured("rounds times millionths fit a u64")
        / u64::from(PREEMPTION_SCALE);
    let preempted_at_least =
        u32::try_from(expected / 2).assured("at most half the rounds fit a u32");
    let shortest = lane.shortest_delay();
    let waited_at_least = shortest
        .checked_mul(preempted_at_least)
        .assured("half the expected preemptions of the shortest delay fit a duration");
    assert!(
        elapsed >= waited_at_least,
        "{STRESS_ROUNDS} nested acquisitions took {elapsed:?}, less than the {waited_at_least:?} \
         {preempted_at_least} preemptions of {shortest:?} would wait"
    );
}

#[cfg(feature = "deloxide-stress")]
#[test]
fn stress_delays_nested_acquisitions_and_records_its_configuration() {
    let directory = tempfile::tempdir().assured("temporary evidence directory");
    let child = run_child("nested_acquisitions_under_stress", Some(directory.path()));
    assert_eq!(child.exit_code(), 0, "{}", child.describe());
    let evidence = only_evidence(directory.path(), &child);
    assert_eq!(
        evidence.process().selection,
        nervix_primitives::deadlock::DiagnosticSelection::StressedActiveOnly(
            nervix_primitives::deadlock::StressConfiguration::LANE
        )
    );
    assert!(evidence.findings().is_empty(), "{:?}", evidence.findings());
}

/// What a child left behind.
struct Child {
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

/// Run `workload` in a child of this test binary, recording in `evidence` when it is given.
fn run_child(workload: &str, evidence: Option<&Path>) -> Child {
    let executable = env::current_exe().assured("a test binary knows its own path");
    let mut command = Command::new(executable);
    command
        .args([
            "--exact",
            "workload",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(WORKLOAD, workload)
        .env_remove(EVIDENCE)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(evidence) = evidence {
        command.env(EVIDENCE, evidence);
    }
    if workload == "failed_report_output" {
        command.stderr(Stdio::from(
            std::fs::OpenOptions::new()
                .write(true)
                .open("/dev/full")
                .assured("the diagnostic failure fixture exists"),
        ));
    }
    let mut child = command.spawn().assured("the test binary can start itself");
    let stdout = read_in_background(child.stdout.take().assured("stdout is piped"));
    let stderr = match child.stderr.take() {
        Some(stderr) if workload == "blocked_report_output" => {
            nix::fcntl::fcntl(&stderr, nix::fcntl::FcntlArg::F_SETPIPE_SZ(4096))
                .assured("the test bounds the output pipe");
            StderrCapture::Stalled(stderr)
        }
        Some(stderr) => StderrCapture::Reading(read_in_background(stderr)),
        None => StderrCapture::Uncaptured,
    };
    let process = nix::unistd::Pid::from_raw(
        i32::try_from(child.id()).assured("process identifiers fit a pid_t"),
    );
    let (ended, ending) = mpsc::channel();
    let waiter = thread::spawn(move || {
        let status = child.wait().assured("a started child can be waited for");
        ended
            .send(status)
            .assured("the probe keeps its receiver until the child ends");
    });
    let status = match ending.recv_timeout(CHILD_BOUND) {
        Ok(status) => status,
        Err(_) => {
            let killed = nix::sys::signal::kill(process, nix::sys::signal::Signal::SIGKILL);
            killed.assured("a child that has not ended can be killed");
            waiter.join().assured(PARTICIPANT_JOINS);
            let stdout = stdout.join().assured(PARTICIPANT_JOINS);
            let stderr = stderr.finish();
            panic!(
                "workload {workload} did not end within \
                 {CHILD_BOUND:?}\nstdout:\n{stdout}\nstderr:\n{stderr}"
            );
        }
    };
    waiter.join().assured(PARTICIPANT_JOINS);
    let child = Child {
        status,
        stdout: stdout.join().assured(PARTICIPANT_JOINS),
        stderr: stderr.finish(),
    };
    if let Some(root) = env::var_os("NERVIX_DEADLOCK_PROBE_ARTIFACTS") {
        let artifact = std::path::PathBuf::from(root).join(workload);
        std::fs::create_dir_all(&artifact).assured("probe artifacts have an owned directory");
        std::fs::write(artifact.join("output.log"), child.describe())
            .assured("probe output is retained");
        if let Some(evidence) = evidence
            && evidence.is_dir()
        {
            for file in EvidenceDirectory::new(evidence)
                .files()
                .assured("probe evidence is listable")
            {
                let name = file.file_name().assured("each evidence file has a name");
                std::fs::copy(&file, artifact.join(name))
                    .assured("probe evidence is retained before its fixture ends");
            }
        }
    }
    child
}

enum StderrCapture {
    Reading(thread::JoinHandle<String>),
    Stalled(std::process::ChildStderr),
    Uncaptured,
}

impl StderrCapture {
    fn finish(self) -> String {
        match self {
            Self::Reading(reader) => reader.join().assured(PARTICIPANT_JOINS),
            Self::Stalled(mut stream) => {
                let mut output = String::new();
                stream
                    .read_to_string(&mut output)
                    .assured("the exited child closed its output");
                output
            }
            Self::Uncaptured => String::new(),
        }
    }
}

#[cfg(feature = "deloxide-order")]
#[test]
fn diagnostic_output_failure_and_a_blocked_descriptor_end_with_failure() {
    for workload in ["failed_report_output", "blocked_report_output"] {
        let directory = tempfile::tempdir().assured("temporary evidence directory");
        let child = run_child(workload, Some(directory.path()));
        assert_eq!(
            child.exit_code(),
            DIAGNOSTIC_FAILURE_EXIT_STATUS,
            "{}",
            child.describe()
        );
        assert!(!only_evidence(directory.path(), &child).qualifies());
    }
}

fn read_in_background<Stream>(mut stream: Stream) -> thread::JoinHandle<String>
where
    Stream: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut text = String::new();
        stream
            .read_to_string(&mut text)
            .assured("a child writes UTF-8");
        text
    })
}

impl Child {
    fn exit_code(&self) -> i32 {
        let code = self.status.code();
        code.assured("a child ends by exiting: its watchdog kills only children it then fails")
    }

    fn describe(&self) -> String {
        format!(
            "status {:?}\nstdout:\n{}\nstderr:\n{}",
            self.status, self.stdout, self.stderr
        )
    }
}

/// The child's evidence: one file, of the child's own process.
fn only_evidence(directory: &Path, child: &Child) -> DeadlockEvidence {
    let evidence = EvidenceDirectory::new(directory)
        .read_all()
        .assured("the probe's evidence directory is readable");
    assert_eq!(evidence.len(), 1, "one evidence file: {}", child.describe());
    let evidence = evidence.into_iter().next().assured("one file was found");
    let program = evidence.process().program.as_ref();
    let program = program.assured("a test binary is named by its first argument");
    assert!(program.as_str().starts_with("active_cycles"), "{program}");
    evidence
}

/// The one active cycle `evidence` records.
fn only_cycle(evidence: &DeadlockEvidence) -> &ActiveCycle {
    let [Finding::ActiveCycle(cycle)] = evidence.findings() else {
        panic!("one active cycle is recorded: {:?}", evidence.findings());
    };
    assert_eq!(cycle.omitted_threads(), 0);
    cycle
}

/// A probe whose workload deadlocks: the child reports the cycle, records it and ends with the
/// active deadlock status.
fn deadlocked(workload: &str) -> (Child, DeadlockEvidence) {
    let directory = tempfile::tempdir().assured("a temporary directory can be created");
    let child = run_child(workload, Some(directory.path()));
    assert_eq!(
        child.exit_code(),
        ACTIVE_DEADLOCK_EXIT_STATUS,
        "{}",
        child.describe()
    );
    assert!(
        child
            .stderr
            .contains("nervix deadlock detector: active deadlock among"),
        "{}",
        child.describe()
    );
    assert!(
        child.stderr.contains("evidence recorded at"),
        "{}",
        child.describe()
    );
    let evidence = only_evidence(directory.path(), &child);
    (child, evidence)
}

/// A probe whose workload ends: the child ends cleanly and its evidence records no finding.
fn completed(workload: &str) -> Child {
    let directory = tempfile::tempdir().assured("a temporary directory can be created");
    let child = run_child(workload, Some(directory.path()));
    assert_eq!(child.exit_code(), 0, "{}", child.describe());
    let evidence = only_evidence(directory.path(), &child);
    assert!(evidence.findings().is_empty(), "{:?}", evidence.findings());
    child
}

/// Every thread of `cycle` waits for a lock of `kind` constructed in this file, at an acquisition
/// with `access` in this file. Returns the threads' names.
fn threads_wait_here(cycle: &ActiveCycle, waits: &[(LockKind, Access)]) -> Vec<String> {
    assert_eq!(cycle.threads().len(), waits.len(), "{cycle:?}");
    let mut names = Vec::new();
    let mut seen = Vec::new();
    for thread in cycle.threads() {
        let lock = thread
            .waits_for
            .as_ref()
            .assured("the detector names the lock");
        let site = lock
            .site
            .as_ref()
            .assured("an adapter constructed the lock");
        let attempt = thread
            .attempt
            .as_ref()
            .assured("the waiting acquisition was recorded");
        assert!(
            site.constructed_at.file.as_str().ends_with(THIS_FILE),
            "{site:?}"
        );
        assert!(attempt.at.file.as_str().ends_with(THIS_FILE), "{attempt:?}");
        seen.push((site.kind, attempt.access));
        let name = thread.name.as_ref().assured("every participant is named");
        names.push(name.as_str().to_string());
    }
    let mut expected = waits.to_vec();
    let key = |wait: &(LockKind, Access)| format!("{wait:?}");
    expected.sort_by_key(key);
    seen.sort_by_key(key);
    assert_eq!(seen, expected, "{cycle:?}");
    names.sort();
    names
}

#[test]
fn two_threads_locking_two_mutexes_in_opposite_orders_report_one_cycle() {
    let (_, evidence) = deadlocked("two_mutexes_in_opposite_orders");
    let cycle = only_cycle(&evidence);
    let names = threads_wait_here(
        cycle,
        &[
            (LockKind::Mutex, Access::Exclusive),
            (LockKind::Mutex, Access::Exclusive),
        ],
    );
    assert_eq!(names, ["probe-first", "probe-second"]);
    let first = cycle.threads()[0]
        .waits_for
        .as_ref()
        .assured("checked above");
    let second = cycle.threads()[1]
        .waits_for
        .as_ref()
        .assured("checked above");
    assert_ne!(
        first.id, second.id,
        "each thread waits for the other's mutex"
    );
}

#[test]
fn a_thread_relocking_its_own_mutex_reports_a_cycle_of_one() {
    let (child, evidence) = deadlocked("a_mutex_relocked_by_its_holder");
    let cycle = only_cycle(&evidence);
    let [thread] = cycle.threads() else {
        panic!("a cycle of one thread: {cycle:?}");
    };
    let lock = thread
        .waits_for
        .as_ref()
        .assured("the detector names the lock");
    let site = lock
        .site
        .as_ref()
        .assured("an adapter constructed the lock");
    let attempt = thread
        .attempt
        .as_ref()
        .assured("the waiting acquisition was recorded");
    assert_eq!(site.kind, LockKind::Mutex);
    assert_eq!(attempt.access, Access::Exclusive);
    let constructed = format!("constructed at line {}", site.constructed_at.line);
    let waits = format!("waits at line {}", attempt.at.line);
    assert!(child.stdout.contains(&constructed), "{}", child.describe());
    assert!(child.stdout.contains(&waits), "{}", child.describe());
    assert!(
        child
            .stderr
            .contains("the thread waits for a lock it holds itself"),
        "{}",
        child.describe()
    );
}

#[test]
fn a_thread_upgrading_its_own_read_lock_reports_a_cycle_of_one() {
    let (_, evidence) = deadlocked("a_read_lock_upgraded_by_its_holder");
    threads_wait_here(
        only_cycle(&evidence),
        &[(LockKind::RwLock, Access::Exclusive)],
    );
}

#[test]
fn a_writer_and_a_reader_across_two_locks_report_one_cycle() {
    let (_, evidence) = deadlocked("a_writer_and_a_reader_across_two_locks");
    let names = threads_wait_here(
        only_cycle(&evidence),
        &[
            (LockKind::Mutex, Access::Exclusive),
            (LockKind::RwLock, Access::Shared),
        ],
    );
    assert_eq!(names, ["probe-reader", "probe-writer"]);
}

#[test]
fn a_notified_waiter_and_a_notifier_holding_its_mutex_report_one_cycle() {
    let (_, evidence) = deadlocked("a_notified_waiter_and_its_notifier");
    let names = threads_wait_here(
        only_cycle(&evidence),
        &[
            (LockKind::Mutex, Access::Exclusive),
            (LockKind::Mutex, Access::Exclusive),
        ],
    );
    assert_eq!(names, ["probe-notifier", "probe-waiter"]);
}

#[test]
fn threads_taking_two_mutexes_in_one_order_complete_without_findings() {
    completed("two_mutexes_in_one_order");
}

#[test]
fn a_condition_handed_between_threads_completes_without_findings() {
    completed("a_condition_handed_between_threads");
}

#[test]
fn readers_sharing_a_read_write_lock_complete_without_findings() {
    completed("readers_sharing_a_lock");
}

#[test]
fn the_detector_keeps_its_start_up_output_off_standard_output() {
    let child = completed("quiet_standard_output");
    assert!(
        child
            .stdout
            .contains("probe output after the detector started"),
        "{}",
        child.describe()
    );
    assert!(
        !child.stdout.contains(DETECTOR_BANNER),
        "{}",
        child.describe()
    );
    assert!(
        !child.stderr.contains(DETECTOR_BANNER),
        "{}",
        child.describe()
    );
}

#[test]
fn a_process_starts_one_run() {
    completed("a_second_run");
}

#[test]
fn a_tracked_lock_before_the_run_fails_the_process() {
    let child = run_child("a_lock_before_the_run", None);
    assert_ne!(child.exit_code(), 0, "{}", child.describe());
    assert!(
        child.stderr.contains(
            "a tracked lock was constructed before this process installed its deadlock detector"
        ),
        "{}",
        child.describe()
    );
}

#[test]
fn a_run_refuses_an_evidence_directory_that_does_not_exist() {
    let directory = tempfile::tempdir().assured("a temporary directory can be created");
    let missing = directory.path().join("missing");
    let child = run_child("a_missing_evidence_directory", Some(&missing));
    assert_eq!(child.exit_code(), 0, "{}", child.describe());
    assert!(!missing.exists());
}

#[test]
fn a_deadlock_whose_evidence_cannot_be_recorded_fails_the_diagnostic_run() {
    let directory = tempfile::tempdir().assured("a temporary directory can be created");
    let evidence = directory.path().join("evidence");
    std::fs::create_dir(&evidence).assured("a directory can be created");
    let child = run_child("evidence_that_cannot_be_recorded", Some(&evidence));
    assert_eq!(
        child.exit_code(),
        DIAGNOSTIC_FAILURE_EXIT_STATUS,
        "{}",
        child.describe()
    );
    assert!(
        child
            .stderr
            .contains("nervix deadlock detector: active deadlock among 1 thread"),
        "{}",
        child.describe()
    );
    assert!(
        child.stderr.contains("the evidence could not be recorded"),
        "{}",
        child.describe()
    );
}

#[test]
fn a_run_without_an_evidence_directory_reports_on_standard_error() {
    let child = run_child("a_run_without_an_evidence_directory", None);
    assert_eq!(
        child.exit_code(),
        ACTIVE_DEADLOCK_EXIT_STATUS,
        "{}",
        child.describe()
    );
    assert!(
        child
            .stderr
            .contains("nervix deadlock detector: active deadlock among 1 thread"),
        "{}",
        child.describe()
    );
    assert!(
        !child.stderr.contains("evidence recorded at"),
        "{}",
        child.describe()
    );
}
