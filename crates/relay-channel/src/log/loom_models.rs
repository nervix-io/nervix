//! Publication and retirement of live log entries.
//!
//! Layer: test harness.
//! - **Owns.** The relaxed observations ordered by actual forward links and reader releases.
//! - **Depends on.** The production log and the Loom harness.
//! - **Must not know.** How a sender gets a reservation.

use std::num::{NonZeroU64, NonZeroUsize};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_model_harness::{
    InvariantId,
    loom::{explore, spawn},
};
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::{Arena, Entry, Log, Read, Writer};

#[test]
fn loom_forward_link_publishes_the_prepared_entry() {
    explore(
        InvariantId::new("relay-channel.log.link-publication"),
        || {
            let arena = Arc::new(Arena::new());
            let log = Arc::new(Log::new(arena.clone(), NonZeroUsize::MIN));
            let mut cursor = Log::start(&log).assured("the log owns this reader position");
            let publisher = log.clone();
            let witness = Arc::new(AtomicUsize::new(0));
            let observing = witness.clone();
            let sending = spawn(move || {
                let mut writer = Writer::new(arena);
                witness.store(1, Ordering::Relaxed);
                publisher.append(
                    &mut writer,
                    Entry {
                        item: witness,
                        weight: NonZeroU64::MIN,
                    },
                );
            });
            if let Read::Item(_) = cursor.try_read(true) {
                assert_eq!(
                    observing.load(Ordering::Relaxed),
                    1,
                    "the link did not publish the ready item"
                );
            }
            sending.join().assured("the publisher finished its model");
        },
    );
}

struct CopyWitness(Arc<AtomicUsize>);

impl Clone for CopyWitness {
    fn clone(&self) -> Self {
        self.0.fetch_add(1, Ordering::Relaxed);
        Self(self.0.clone())
    }
}

#[test]
fn loom_last_reader_acquires_every_earlier_read() {
    explore(
        InvariantId::new("relay-channel.log.reader-retirement"),
        || {
            let arena = Arc::new(Arena::new());
            let readers = NonZeroUsize::new(2).assured("this model has two readers");
            let log = Arc::new(Log::new(arena.clone(), readers));
            let mut first = Log::start(&log).assured("the first reader has a position");
            let mut second = Log::start(&log).assured("the second reader has a position");
            let witness = Arc::new(AtomicUsize::new(0));
            log.append(
                &mut Writer::new(arena),
                Entry {
                    item: CopyWitness(witness.clone()),
                    weight: NonZeroU64::MIN,
                },
            );
            let remote = witness.clone();
            let taking = spawn(move || {
                let Read::Item(read) = first.try_read(true) else {
                    panic!("the first reader has a ready entry");
                };
                if read.finished.is_some() {
                    assert_eq!(
                        remote.load(Ordering::Relaxed),
                        2,
                        "last reader omitted an earlier read"
                    );
                }
            });
            let Read::Item(read) = second.try_read(true) else {
                panic!("the second reader has a ready entry");
            };
            if read.finished.is_some() {
                assert_eq!(
                    witness.load(Ordering::Relaxed),
                    2,
                    "last reader omitted an earlier read"
                );
            }
            taking.join().assured("the other reader finished");
        },
    );
}
