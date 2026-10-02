//! The boundary's conformance: which backend each mode selects, and that every operation of the
//! surface behaves as its library defines it under that backend.
//!
//! `just test-primitives` runs this module once per mode. Each mode's checks run the same
//! [`exercise_the_atomic_surface`] and the same scripts of the native families, so a backend that
//! lacks an operation fails to compile here and a backend that answers one differently fails here,
//! rather than in the first owner that uses it. The Shuttle checks also show that the scheduler
//! reaches the races its adapters exist for, and the Turmoil check that sockets, name lookup,
//! timers and admitted CPU jobs belong to the simulated host that uses them.

#[cfg(all(feature = "native", not(feature = "loom")))]
mod families;
#[cfg(all(feature = "native", not(feature = "loom")))]
mod notification;
#[cfg(all(feature = "native", feature = "shuttle", not(feature = "loom")))]
mod shuttle_races;
#[cfg(all(feature = "native", feature = "turmoil"))]
mod simulated_host;
#[cfg(all(feature = "native", not(feature = "loom")))]
mod tasks;
#[cfg(all(
    feature = "native",
    any(feature = "shuttle", feature = "test-util"),
    not(feature = "loom")
))]
mod timers;
#[cfg(all(feature = "native", not(feature = "loom")))]
mod watch_channel;

use std::{any::TypeId, ptr};

use crate::sync::atomic::{
    AtomicBool, AtomicI64, AtomicPtr, AtomicU64, AtomicUsize, Ordering, fence,
};

fn is_same_type<Selected: 'static, Expected: 'static>() -> bool {
    TypeId::of::<Selected>() == TypeId::of::<Expected>()
}

/// Every operation of the atomic surface, each with the result the standard library defines.
#[allow(deprecated)] // until try_update is released to stable
fn exercise_the_atomic_surface() {
    let counter = AtomicU64::new(5);
    assert_eq!(counter.load(Ordering::Acquire), 5);
    counter.store(6, Ordering::Release);
    assert_eq!(counter.swap(7, Ordering::AcqRel), 6);
    assert_eq!(
        counter.compare_exchange(7, 8, Ordering::AcqRel, Ordering::Acquire),
        Ok(7)
    );
    assert_eq!(
        counter.compare_exchange(7, 9, Ordering::AcqRel, Ordering::Acquire),
        Err(8)
    );
    // A weak exchange may fail spuriously on some hardware, so it is retried until it succeeds.
    let mut expected = 8;
    loop {
        match counter.compare_exchange_weak(expected, 12, Ordering::AcqRel, Ordering::Acquire) {
            Ok(previous) => {
                assert_eq!(previous, 8);
                break;
            }
            Err(actual) => expected = actual,
        }
    }
    assert_eq!(counter.fetch_add(4, Ordering::AcqRel), 12);
    assert_eq!(counter.fetch_sub(6, Ordering::AcqRel), 16);
    assert_eq!(counter.fetch_and(0b1100, Ordering::AcqRel), 10);
    assert_eq!(counter.fetch_or(0b0011, Ordering::AcqRel), 8);
    assert_eq!(counter.fetch_xor(0b0101, Ordering::AcqRel), 11);
    assert_eq!(counter.fetch_nand(0b1111, Ordering::AcqRel), 14);
    assert_eq!(counter.fetch_max(3, Ordering::AcqRel), u64::MAX - 14);
    assert_eq!(counter.fetch_min(3, Ordering::AcqRel), u64::MAX - 14);
    assert_eq!(
        counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| current
            .checked_add(1)),
        Ok(3)
    );
    assert_eq!(
        counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |_| None),
        Err(4)
    );
    assert_eq!(counter.into_inner(), 4);

    let signed = AtomicI64::new(1);
    assert_eq!(signed.fetch_sub(3, Ordering::AcqRel), 1);
    assert_eq!(signed.load(Ordering::Acquire), -2);

    let flag = AtomicBool::new(false);
    assert!(!flag.swap(true, Ordering::AcqRel));
    assert_eq!(
        flag.compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire),
        Ok(true)
    );
    assert!(!flag.fetch_or(true, Ordering::AcqRel));
    assert!(flag.fetch_and(false, Ordering::AcqRel));
    assert!(!flag.fetch_xor(true, Ordering::AcqRel));
    assert!(flag.fetch_nand(true, Ordering::AcqRel));
    assert!(!flag.load(Ordering::Acquire));
    flag.store(true, Ordering::Release);
    assert!(flag.into_inner());

    assert_eq!(AtomicUsize::default().into_inner(), 0);
    assert_eq!(AtomicUsize::from(3).into_inner(), 3);

    let mut target = 7_u8;
    let target_address: *mut u8 = &mut target;
    let pointer = AtomicPtr::new(ptr::null_mut());
    pointer.store(target_address, Ordering::Release);
    assert_eq!(pointer.load(Ordering::Acquire), target_address);
    assert_eq!(
        pointer.compare_exchange(
            target_address,
            ptr::null_mut(),
            Ordering::AcqRel,
            Ordering::Acquire
        ),
        Ok(target_address)
    );
    assert!(pointer.swap(target_address, Ordering::AcqRel).is_null());

    fence(Ordering::Acquire);
    fence(Ordering::Release);
    fence(Ordering::AcqRel);
    fence(Ordering::SeqCst);
}

/// A thread's result reaches the thread that joins it.
#[cfg(feature = "native")]
fn exercise_threads() {
    use meticulous::ResultExt as _;

    let participant = crate::thread::spawn(|| 5_u8);
    let result = participant
        .join()
        .assured("the participant returns a constant without panicking");
    assert_eq!(result, 5);
}

/// Shared ownership is the library's own type in every mode and on every target: no model checker
/// counts references, so no mode substitutes a type of its own.
#[test]
fn shared_ownership_is_the_same_library_type_in_every_mode() {
    assert!(is_same_type::<crate::sync::Arc<u8>, triomphe::Arc<u8>>());
    assert!(is_same_type::<crate::sync::StdArc<u8>, std::sync::Arc<u8>>());
    assert!(is_same_type::<crate::sync::StdWeak<u8>, std::sync::Weak<u8>>());
}

/// The `futures` families the unmodeled surface offers the browser console are that crate's own.
#[test]
fn the_unmodeled_futures_families_are_the_futures_crates_own() {
    assert!(is_same_type::<
        crate::unmodeled::futures::mpsc::UnboundedSender<u8>,
        futures_channel::mpsc::UnboundedSender<u8>,
    >());
    assert!(is_same_type::<
        crate::unmodeled::futures::AbortHandle,
        futures_util::future::AbortHandle,
    >());
}

#[cfg(not(any(feature = "loom", feature = "shuttle")))]
mod ordinary {
    use std::sync::atomic as standard;

    use super::*;
    use crate::{sync::atomic as selected, unmodeled::sync::atomic as unmodeled};

    #[test]
    fn ordinary_execution_selects_the_standard_library_items_themselves() {
        assert!(is_same_type::<selected::AtomicBool, standard::AtomicBool>());
        assert!(is_same_type::<selected::AtomicI8, standard::AtomicI8>());
        assert!(is_same_type::<selected::AtomicI16, standard::AtomicI16>());
        assert!(is_same_type::<selected::AtomicI32, standard::AtomicI32>());
        assert!(is_same_type::<selected::AtomicI64, standard::AtomicI64>());
        assert!(is_same_type::<selected::AtomicIsize, standard::AtomicIsize>());
        assert!(is_same_type::<
            selected::AtomicPtr<u8>,
            standard::AtomicPtr<u8>,
        >());
        assert!(is_same_type::<selected::AtomicU8, standard::AtomicU8>());
        assert!(is_same_type::<selected::AtomicU16, standard::AtomicU16>());
        assert!(is_same_type::<selected::AtomicU32, standard::AtomicU32>());
        assert!(is_same_type::<selected::AtomicU64, standard::AtomicU64>());
        assert!(is_same_type::<selected::AtomicUsize, standard::AtomicUsize>());
        assert!(is_same_type::<selected::Ordering, standard::Ordering>());
    }

    #[test]
    fn unmodeled_atomics_are_the_standard_library_items() {
        assert!(is_same_type::<unmodeled::AtomicBool, standard::AtomicBool>());
        assert!(is_same_type::<unmodeled::AtomicU64, standard::AtomicU64>());
        assert!(is_same_type::<unmodeled::AtomicUsize, standard::AtomicUsize>());
        assert!(is_same_type::<unmodeled::Ordering, selected::Ordering>());
    }

    #[test]
    fn the_atomic_surface_behaves_as_the_standard_library_defines() {
        exercise_the_atomic_surface();
    }

    #[cfg(feature = "native")]
    #[test]
    fn native_threads_are_operating_system_threads() {
        exercise_threads();
    }

    /// Ordinary execution selects each library's own items: no wrapper, dispatch or scheduling
    /// point stands between a caller and the primitive.
    #[cfg(feature = "native")]
    #[test]
    fn ordinary_execution_selects_each_librarys_own_items() {
        assert!(is_same_type::<crate::sync::Notify, tokio::sync::Notify>());
        assert!(is_same_type::<
            crate::sync::AtomicWaker,
            futures_util::task::AtomicWaker,
        >());
        assert!(is_same_type::<crate::sync::Semaphore, tokio::sync::Semaphore>());
        assert!(is_same_type::<
            crate::sync::watch::Sender<u8>,
            tokio::sync::watch::Sender<u8>,
        >());
        assert!(is_same_type::<
            crate::sync::mpsc::Sender<u8>,
            tokio::sync::mpsc::Sender<u8>,
        >());
        assert!(is_same_type::<
            crate::sync::CancellationToken,
            tokio_util::sync::CancellationToken,
        >());
        assert!(is_same_type::<
            crate::sync::blocking::Mutex<u8>,
            parking_lot::Mutex<u8>,
        >());
        assert!(is_same_type::<
            crate::sync::blocking::Condvar,
            parking_lot::Condvar,
        >());
        assert!(is_same_type::<
            crate::sync::blocking::OnceLock<u8>,
            std::sync::OnceLock<u8>,
        >());
        assert!(is_same_type::<
            crate::task::JoinHandle<u8>,
            tokio::task::JoinHandle<u8>,
        >());
        assert!(is_same_type::<
            crate::task::AbortOnDropHandle<u8>,
            tokio_util::task::AbortOnDropHandle<u8>,
        >());
        assert!(is_same_type::<
            crate::runtime::Runtime,
            tokio::runtime::Runtime,
        >());
        assert!(is_same_type::<
            crate::__private::runtime::Builder,
            tokio::runtime::Builder,
        >());
        assert!(is_same_type::<
            crate::collections::DashMap<u8, u8>,
            dashmap::DashMap<u8, u8>,
        >());
        assert!(is_same_type::<
            crate::collections::ConcurrentQueue<u8>,
            concurrent_queue::ConcurrentQueue<u8>,
        >());
        assert!(is_same_type::<
            crate::publication::ArcSwap<u8>,
            arc_swap::ArcSwap<u8>,
        >());
        assert!(is_same_type::<
            crate::stream::wrappers::ReceiverStream<u8>,
            tokio_stream::wrappers::ReceiverStream<u8>,
        >());
        assert!(is_same_type::<
            crate::thread::JoinHandle<u8>,
            std::thread::JoinHandle<u8>,
        >());
        assert!(is_same_type::<crate::time::Instant, tokio::time::Instant>());
        assert!(is_same_type::<crate::time::Sleep, tokio::time::Sleep>());
        assert!(is_same_type::<crate::time::Interval, tokio::time::Interval>());
    }

    /// Outside Turmoil the sockets are Tokio's, over the operating system's network.
    #[cfg(all(feature = "native", not(feature = "turmoil")))]
    #[test]
    fn ordinary_execution_selects_tokios_sockets() {
        assert!(is_same_type::<
            crate::net::TcpListener,
            tokio::net::TcpListener,
        >());
        assert!(is_same_type::<crate::net::TcpStream, tokio::net::TcpStream>());
        assert!(is_same_type::<crate::net::UdpSocket, tokio::net::UdpSocket>());
        assert!(is_same_type::<
            crate::net::tcp::OwnedReadHalf,
            tokio::net::tcp::OwnedReadHalf,
        >());
    }

    /// An admitted CPU job runs on the blocking pool, off the thread of the task that submitted it.
    #[cfg(all(feature = "native", not(feature = "turmoil")))]
    #[crate::test]
    async fn an_admitted_cpu_job_runs_on_the_blocking_pool() {
        use meticulous::ResultExt as _;

        let submitter = crate::thread::current().id();
        let job = crate::task::spawn_cpu(|| crate::thread::current().id())
            .await
            .assured("the job returns its thread without panicking");
        assert_ne!(job, submitter);
    }

    #[cfg(all(feature = "native", feature = "test-util"))]
    #[crate::test(start_paused = true)]
    async fn timers_measure_the_runtime_clock() {
        super::timers::timers_measure_the_runtime_clock().await;
    }

    #[cfg(feature = "native")]
    #[test]
    fn notify_keeps_its_registration_contract() {
        super::notification::keeps_the_registration_contract();
    }

    #[cfg(feature = "native")]
    #[test]
    fn the_watch_channel_keeps_its_contract() {
        super::watch_channel::keeps_the_channel_contract();
    }

    #[cfg(feature = "native")]
    #[test]
    fn the_other_native_families_keep_their_contracts() {
        super::families::keep_their_contracts();
    }

    /// The test attribute builds the selected runtime and passes its arguments through, so a task
    /// the test spawns runs on the worker threads it asked for.
    #[cfg(feature = "native")]
    #[crate::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_test_attribute_runs_the_selected_runtime() {
        use meticulous::ResultExt as _;

        let spawned = crate::task::spawn(async { 7_u8 });
        assert_eq!(spawned.await.assured("the task returns a constant"), 7);
        let runtime = crate::runtime::Handle::current();
        assert_eq!(runtime.metrics().num_workers(), 2);
    }

    #[cfg(feature = "native")]
    #[crate::test]
    async fn an_abort_on_drop_handle_keeps_its_contract() {
        super::tasks::abort_on_drop_handles_end_their_tasks().await;
    }
}

#[cfg(feature = "shuttle")]
mod shuttle_mode {
    use shuttle::current::context_switches;

    use super::*;
    use crate::{sync::atomic as selected, unmodeled::sync::atomic as unmodeled};

    #[test]
    fn shuttle_selects_shuttles_atomics() {
        assert!(is_same_type::<
            selected::AtomicBool,
            shuttle::sync::atomic::AtomicBool,
        >());
        assert!(is_same_type::<
            selected::AtomicU64,
            shuttle::sync::atomic::AtomicU64,
        >());
        assert!(is_same_type::<
            selected::AtomicUsize,
            shuttle::sync::atomic::AtomicUsize,
        >());
        assert!(is_same_type::<
            selected::AtomicPtr<u8>,
            shuttle::sync::atomic::AtomicPtr<u8>,
        >());
    }

    #[test]
    fn the_atomic_surface_behaves_as_the_standard_library_defines_under_shuttle() {
        shuttle::check_random(exercise_the_atomic_surface, 1);
    }

    /// Shuttle counts every point at which it could switch threads, including the ones where it
    /// keeps running the same thread, so an operation it observes always advances the count.
    #[test]
    fn every_selected_atomic_operation_is_a_shuttle_scheduling_point() {
        shuttle::check_random(
            || {
                let flag = AtomicBool::new(false);
                let before_store = context_switches();
                flag.store(true, Ordering::Release);
                assert!(context_switches() > before_store);
                let before_load = context_switches();
                assert!(flag.load(Ordering::Acquire));
                assert!(context_switches() > before_load);
                let counter = AtomicU64::new(0);
                let before_add = context_switches();
                counter.fetch_add(1, Ordering::AcqRel);
                assert!(context_switches() > before_add);
            },
            1,
        );
    }

    #[test]
    fn an_unmodeled_atomic_stays_outside_the_shuttle_schedule() {
        shuttle::check_random(
            || {
                let flag = unmodeled::AtomicBool::new(false);
                let before = context_switches();
                flag.store(true, unmodeled::Ordering::Release);
                assert!(flag.load(unmodeled::Ordering::Acquire));
                assert_eq!(context_switches(), before);
            },
            1,
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn native_threads_are_shuttle_threads() {
        shuttle::check_random(exercise_threads, 1);
    }

    /// Shuttle's modeled Tokio, Tokio Util, Tokio Stream and `parking_lot` supply the families this
    /// crate does not adapt, and every endpoint of a channel family comes from one of them.
    #[cfg(feature = "native")]
    #[test]
    fn shuttle_selects_the_modeled_libraries() {
        assert!(is_same_type::<
            crate::sync::Semaphore,
            shuttle_tokio::sync::Semaphore,
        >());
        assert!(is_same_type::<
            crate::sync::mpsc::Sender<u8>,
            shuttle_tokio::sync::mpsc::Sender<u8>,
        >());
        assert!(is_same_type::<
            crate::sync::blocking::Mutex<u8>,
            shuttle_parking_lot::Mutex<u8>,
        >());
        assert!(is_same_type::<
            crate::sync::blocking::mpsc::Sender<u8>,
            shuttle::sync::mpsc::Sender<u8>,
        >());
        assert!(is_same_type::<
            crate::task::JoinHandle<u8>,
            shuttle_tokio::task::JoinHandle<u8>,
        >());
        assert!(is_same_type::<
            crate::runtime::Runtime,
            shuttle_tokio::runtime::Runtime,
        >());
        assert!(is_same_type::<
            crate::__private::runtime::Builder,
            shuttle_tokio::runtime::Builder,
        >());
        assert!(is_same_type::<
            crate::stream::wrappers::ReceiverStream<u8>,
            shuttle_tokio_stream::wrappers::ReceiverStream<u8>,
        >());
        assert!(is_same_type::<
            crate::thread::JoinHandle<u8>,
            shuttle::thread::JoinHandle<u8>,
        >());
        assert!(is_same_type::<crate::time::Sleep, shuttle_tokio::time::Sleep>());
        assert!(is_same_type::<
            crate::time::Interval,
            shuttle_tokio::time::Interval,
        >());
    }

    /// No model checker simulates a network: a Shuttle build's sockets are Tokio's, outside every
    /// model.
    #[cfg(feature = "native")]
    #[test]
    fn shuttle_takes_tokios_sockets_outside_every_model() {
        assert!(is_same_type::<crate::net::TcpStream, tokio::net::TcpStream>());
        assert!(is_same_type::<crate::net::UdpSocket, tokio::net::UdpSocket>());
    }

    /// A socket created inside a Shuttle execution registers with a Tokio reactor the execution does
    /// not have, so it fails the check instead of reaching the network unobserved.
    #[cfg(feature = "native")]
    #[test]
    #[should_panic(expected = "no reactor running")]
    fn a_socket_created_inside_a_shuttle_execution_fails_the_check() {
        shuttle::check_random(
            || {
                let bound = shuttle::future::block_on(crate::net::UdpSocket::bind("127.0.0.1:0"));
                drop(bound);
            },
            1,
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn timers_are_scheduling_points_and_the_check_decides_every_timeout() {
        shuttle::check_random(
            || {
                shuttle::future::block_on(
                    super::timers::timers_are_scheduling_points_and_the_check_decides_every_timeout(
                    ),
                );
            },
            1,
        );
    }

    #[cfg(feature = "native")]
    #[test]
    fn notify_keeps_its_registration_contract_under_shuttle() {
        shuttle::check_random(super::notification::keeps_the_registration_contract, 1);
    }

    #[cfg(feature = "native")]
    #[test]
    fn the_watch_channel_keeps_its_contract_under_shuttle() {
        shuttle::check_random(super::watch_channel::keeps_the_channel_contract, 1);
    }

    #[cfg(feature = "native")]
    #[test]
    fn the_other_native_families_keep_their_contracts_under_shuttle() {
        shuttle::check_random(super::families::keep_their_contracts, 1);
    }

    /// The script spawns tasks, so it runs under several schedules: its outcomes hold in each.
    #[cfg(feature = "native")]
    #[test]
    fn an_abort_on_drop_handle_keeps_its_contract_under_shuttle() {
        shuttle::check_random(
            || shuttle::future::block_on(super::tasks::abort_on_drop_handles_end_their_tasks()),
            16,
        );
    }

    /// Each registration and notification of `Notify`, each registration, take and wake of an
    /// atomic waker, and each read of the watch channel's version that registers or decides, lets
    /// the scheduler choose the next task. A drop does not.
    #[cfg(feature = "native")]
    #[test]
    fn every_registration_and_notification_is_a_scheduling_point() {
        use std::pin::pin;

        use crate::sync::{AtomicWaker, Notify, watch};

        shuttle::check_random(
            || {
                let notify = Notify::new();
                let before = context_switches();
                let mut created = pin!(notify.notified());
                assert!(context_switches() > before);
                let before = context_switches();
                assert!(!created.as_mut().enable());
                assert!(context_switches() > before);
                let before = context_switches();
                notify.notify_one();
                assert!(context_switches() > before);
                let before = context_switches();
                notify.notify_waiters();
                assert!(context_switches() > before);
                let unregistered = Box::pin(notify.notified());
                let before = context_switches();
                drop(unregistered);
                assert_eq!(context_switches(), before);

                let registration = AtomicWaker::new();
                let before = context_switches();
                registration.register(std::task::Waker::noop());
                assert!(context_switches() > before);
                let before = context_switches();
                assert!(registration.take().is_some());
                assert!(context_switches() > before);
                let before = context_switches();
                registration.wake();
                assert!(context_switches() > before);

                let (sender, receiver) = watch::channel(0_u8);
                let before = context_switches();
                let subscribed = sender.subscribe();
                assert!(context_switches() > before);
                let before = context_switches();
                assert!(receiver.has_changed().is_ok());
                assert!(context_switches() > before);
                let before = context_switches();
                drop(subscribed);
                assert_eq!(context_switches(), before);
            },
            1,
        );
    }
}

#[cfg(feature = "loom")]
mod loom_mode {
    use super::*;
    use crate::sync::atomic as selected;

    #[test]
    fn loom_selects_looms_atomics() {
        assert!(is_same_type::<
            selected::AtomicBool,
            loom::sync::atomic::AtomicBool,
        >());
        assert!(is_same_type::<
            selected::AtomicU64,
            loom::sync::atomic::AtomicU64,
        >());
        assert!(is_same_type::<
            selected::AtomicUsize,
            loom::sync::atomic::AtomicUsize,
        >());
        assert!(is_same_type::<
            selected::AtomicPtr<u8>,
            loom::sync::atomic::AtomicPtr<u8>,
        >());
    }

    #[test]
    fn the_atomic_surface_behaves_as_the_standard_library_defines_under_loom() {
        loom::model(exercise_the_atomic_surface);
    }

    #[cfg(feature = "native")]
    #[test]
    fn native_threads_are_loom_threads() {
        loom::model(exercise_threads);
    }

    /// Loom models the threads a model spawns, joins, parks and yields, and nothing of the async,
    /// thread-blocking, collection or publication families, which a Loom build takes from the
    /// ordinary libraries, outside every model.
    #[cfg(feature = "native")]
    #[test]
    fn loom_takes_the_ordinary_libraries_for_the_families_it_does_not_model() {
        assert!(is_same_type::<
            crate::thread::JoinHandle<u8>,
            loom::thread::JoinHandle<u8>,
        >());
        assert!(is_same_type::<crate::thread::Builder, loom::thread::Builder>());
        assert!(is_same_type::<crate::sync::Notify, tokio::sync::Notify>());
        assert!(is_same_type::<
            crate::sync::AtomicWaker,
            futures_util::task::AtomicWaker,
        >());
        assert!(is_same_type::<
            crate::sync::watch::Sender<u8>,
            tokio::sync::watch::Sender<u8>,
        >());
        assert!(is_same_type::<
            crate::sync::blocking::Mutex<u8>,
            parking_lot::Mutex<u8>,
        >());
        assert!(is_same_type::<
            crate::task::JoinHandle<u8>,
            tokio::task::JoinHandle<u8>,
        >());
        assert!(is_same_type::<
            crate::collections::DashMap<u8, u8>,
            dashmap::DashMap<u8, u8>,
        >());
        assert!(is_same_type::<
            crate::publication::ArcSwap<u8>,
            arc_swap::ArcSwap<u8>,
        >());
        assert!(is_same_type::<crate::time::Instant, tokio::time::Instant>());
        assert!(is_same_type::<crate::time::Sleep, tokio::time::Sleep>());
        assert!(is_same_type::<crate::net::TcpStream, tokio::net::TcpStream>());
    }
}

#[cfg(all(feature = "turmoil", not(any(feature = "loom", feature = "shuttle"))))]
mod turmoil_mode {
    use super::is_same_type;

    /// A Turmoil build's sockets are the simulated network's; its timers are Tokio's, which follow
    /// the clock of the simulated host that polls them.
    #[cfg(feature = "native")]
    #[test]
    fn turmoil_selects_simulated_sockets_and_tokios_timers() {
        assert!(is_same_type::<
            crate::net::TcpListener,
            turmoil::net::TcpListener,
        >());
        assert!(is_same_type::<crate::net::TcpStream, turmoil::net::TcpStream>());
        assert!(is_same_type::<crate::net::UdpSocket, turmoil::net::UdpSocket>());
        assert!(is_same_type::<
            crate::net::tcp::OwnedReadHalf,
            turmoil::net::tcp::OwnedReadHalf,
        >());
        assert!(is_same_type::<crate::time::Instant, tokio::time::Instant>());
    }
}
