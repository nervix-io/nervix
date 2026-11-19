//! The approved pause between the tail exchange and its link.
//!
//! Layer: test harness.
//! - **Owns.** The controlled publication-gap check.
//! - **Depends on.** The production log and Shuttle.
//! - **Must not know.** The runtime producer identity.

use std::num::{NonZeroU64, NonZeroUsize};

use meticulous::ResultExt as _;
use nervix_model_harness::shuttle::check_random_and_pct;
use nervix_primitives::{runtime::Builder, sync::Arc, task::spawn};

use super::{Arena, Entry, Writer};
use crate::generation::{Enter, Generation, Next, Position, ReaderSignal};

#[test]
fn shuttle_a_later_completed_link_cannot_hide_pending_delivery() {
    check_random_and_pct(|| {
        Builder::new_current_thread()
            .build()
            .assured("the model starts a runtime")
            .block_on(async {
                let arena = Arc::new(Arena::new());
                let mut first = Writer::new(arena.clone());
                let generation = Generation::new(0, &arena, Box::new([ReaderSignal::new()]));
                let mut reader = Position::new(generation.clone());
                let Enter::Live(earlier) = generation.publication.try_enter() else {
                    panic!("the first sender enters the open generation");
                };
                let log = earlier.log();
                let slot = first.claim_slot(
                    Entry {
                        item: 1,
                        weight: NonZeroU64::MIN,
                    },
                    NonZeroUsize::MIN,
                );
                let previous = log.exchange(slot);
                let later_generation = generation.clone();
                let later = spawn(async move {
                    let mut second = Writer::new(arena);
                    let Enter::Live(append) = later_generation.publication.try_enter() else {
                        panic!("the second sender enters before the seal");
                    };
                    append.publish(
                        &mut second,
                        Entry {
                            item: 2,
                            weight: NonZeroU64::MIN,
                        },
                    );
                });
                assert!(matches!(reader.next(true, None), Next::Publishing));
                later
                    .await
                    .assured("the second producer publishes during the pause");
                generation.seal(None);
                assert!(matches!(reader.next(true, None), Next::Publishing));
                log.link(previous, slot);
                drop(earlier);
                for expected in [1, 2] {
                    let Next::Item(result) = reader.next(true, None) else {
                        panic!("the linked item can now be read");
                    };
                    assert_eq!(result.item, Some(expected));
                }
                assert!(matches!(reader.next(true, None), Next::Ended));
            });
    });
}
