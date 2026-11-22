//! Numbers that choose a duration, a capacity, a channel capacity or a permit count outside a
//! declared setting, a derivation from typed input or a fixed contract.
use std::{num::NonZeroUsize, time::Duration as Wait};

use nervix_lint_fixtures::{Semaphore, mpsc};

type Bytes = u64;

pub const TIMEOUT: Wait = Wait::from_secs(5);
pub const LIMIT: Bytes = 1024;
pub const MINIMUM: NonZeroUsize = NonZeroUsize::MIN;

pub struct Pool;
impl Pool {
    pub const WORKERS: usize = 4;
}

pub trait Cadence {
    const EVERY: Wait = Wait::from_millis(100);
}

pub fn buffer() -> Vec<u8> {
    Vec::with_capacity(512)
}

pub fn retry() -> Wait {
    Wait::from_millis(2 * 125)
}

pub fn queue() -> mpsc::Receiver<u8> {
    let (_sender, receiver) = mpsc::channel(8);
    receiver
}

pub fn widen(shared: &Semaphore) {
    shared.add_permits(2);
}

pub fn gate() -> Semaphore {
    Semaphore::new(4)
}

macro_rules! cadence {
    () => {
        Wait::from_secs(1)
    };
}

pub fn expanded() -> Wait {
    cadence!()
}

macro_rules! named_limit {
    ($name:ident) => {
        pub const $name: usize = 64;
    };
}

// An authored `macro_rules!` body stays checked in every expansion, also when its caller names
// the constant it declares.
named_limit!(SPLICED);
