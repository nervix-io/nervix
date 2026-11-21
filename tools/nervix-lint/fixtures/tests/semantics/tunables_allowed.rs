//! Fixed contracts, setting definitions and values derived from typed input carry no hidden
//! numeric policy.
use std::time::Duration;

use nervix_lint_fixtures::settings::{Setting, SettingDefinition};
use nervix_lint_fixtures::{Semaphore, watch};

#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = "the frame ABI stores its tag in four bytes"))]
pub const TAG_BYTES: usize = 4;

pub struct Frame;
impl Frame {
    #[cfg_attr(nervix_lint, nervix::constant(fixed, reason = "the frame ABI stores its length in eight bytes"))]
    pub const LENGTH_BYTES: usize = 8;
}

pub trait Wire {
    #[cfg_attr(nervix_lint, nervix::constant(fixed, reason = "the frame ABI stores its version in two bytes"))]
    const VERSION_BYTES: usize = 2;
    /// A declaration without a value decides nothing; each implementation supplies one.
    const DECLARED: u32;
}

/// A fixed constant's initializer is part of that one contract, not a second policy call.
#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = "the fixture protocol fixes its keep-alive at one second"))]
pub const KEEP_ALIVE: Duration = Duration::from_secs(1);

pub struct Deadline;
impl SettingDefinition for Deadline {
    type Value = Duration;
    fn default_value() -> Duration {
        Duration::from_secs(30)
    }
}

pub struct Group {
    pub deadline: Setting<Deadline>,
}
impl Default for Group {
    fn default() -> Self {
        Self {
            deadline: Setting::new(Duration::from_millis(500)),
        }
    }
}

pub fn derived(header: &[u8]) -> Vec<u8> {
    Vec::with_capacity(header.len() + size_of::<u32>())
}

pub fn named() -> Vec<u8> {
    Vec::with_capacity(TAG_BYTES)
}

pub fn configured(seconds: u64) -> Duration {
    Duration::from_secs(seconds)
}

pub fn shared(permits: usize) -> Semaphore {
    Semaphore::new(permits)
}

/// The same names on an owner that is not a duration choose nothing.
pub struct Ledger;
impl Ledger {
    pub fn from_secs(_: u64) -> Self {
        Self
    }
}
pub fn unrelated() -> Ledger {
    Ledger::from_secs(5)
}

/// A watch channel's argument is its initial value, not a capacity.
pub fn current() -> watch::Receiver<u32> {
    let (_sender, receiver) = watch::channel(0);
    receiver
}
