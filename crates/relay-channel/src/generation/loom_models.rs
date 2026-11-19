//! Closing and waiting while a publication completes.
//!
//! Layer: test harness.
//! - **Owns.** The observations made by the reader and publisher at a membership boundary.
//! - **Depends on.** The production log generation and Loom.
//! - **Must not know.** Item meaning or caller identity.

use std::num::NonZeroU64;

use meticulous::ResultExt as _;
use nervix_model_harness::{
    InvariantId,
    loom::{explore, spawn},
};
use nervix_primitives::sync::Arc;

use super::{Enter, Generation, Next, Position, ReaderSignal};
use crate::log::{Arena, Entry, Writer};

#[test]
fn loom_sealed_generation_never_hides_an_admitted_item() {
    explore(
        InvariantId::new("relay-channel.generation.finish-publication"),
        || {
            let arena = Arc::new(Arena::new());
            let generation = Generation::new(1, &arena, Box::new([ReaderSignal::new()]));
            let mut reader = Position::new(generation.clone());
            let publishing = generation.clone();
            let sending = spawn(move || {
                let Enter::Live(append) = publishing.publication.try_enter() else {
                    return false;
                };
                let mut writer = Writer::new(arena);
                append.publish(
                    &mut writer,
                    Entry {
                        item: 7,
                        weight: NonZeroU64::MIN,
                    },
                );
                true
            });
            generation.seal(None);
            let ended = matches!(reader.next(true, None), Next::Ended);
            let published = sending.join().assured("the producer finished");
            assert!(
                !(ended && published),
                "the sealed generation hid an admitted item"
            );
        },
    );
}

#[test]
fn loom_a_reader_waiting_for_a_link_observes_it_or_is_woken() {
    explore(
        InvariantId::new("relay-channel.generation.reader-wake"),
        || {
            let arena = Arc::new(Arena::new());
            let generation = Generation::new(1, &arena, Box::new([ReaderSignal::new()]));
            let mut reader = Position::new(generation.clone());
            let publishing = generation.clone();
            let sending = spawn(move || {
                let Enter::Live(append) = publishing.publication.try_enter() else {
                    panic!("the generation stays open");
                };
                let mut writer = Writer::new(arena);
                append.publish(
                    &mut writer,
                    Entry {
                        item: 7,
                        weight: NonZeroU64::MIN,
                    },
                );
                // Observe the same completion that Append::drop calls. The test consumes the
                // lease here, after publishing, so it can inspect the wake decision directly.
                std::mem::forget(append);
                publishing.publication.end()
            });
            generation.publication.arm_wait();
            let observed = matches!(reader.next(true, None), Next::Item(_));
            let would_wake = sending.join().assured("the sender completed");
            assert!(
                observed || would_wake,
                "the forward link and the waiting-reader notification were both missed"
            );
        },
    );
}
