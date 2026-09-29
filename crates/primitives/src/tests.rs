//! The boundary's conformance: which backend each mode selects, and that every operation of the
//! surface behaves as the standard library defines it under that backend.
//!
//! `just test-primitives` runs this module once per mode. Each mode's checks run the same
//! [`exercise_the_atomic_surface`], so a backend that lacks an operation fails to compile here and a
//! backend that answers one differently fails here, rather than in the first owner that uses it.

use std::{any::TypeId, ptr};

use crate::sync::atomic::{
    AtomicBool, AtomicI64, AtomicPtr, AtomicU64, AtomicUsize, Ordering, fence,
};

fn is_same_type<Selected: 'static, Expected: 'static>() -> bool {
    TypeId::of::<Selected>() == TypeId::of::<Expected>()
}

/// Every operation of the atomic surface, each with the result the standard library defines.
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
}
