//! Audit fixture: functions whose whole body forwards their parameters to one local call.
#![cfg_attr(
    nervix_lint,
    nervix::context(lifecycle, reason = "the audit calibration runs these examples once")
)]

pub struct Store {
    values: Vec<u64>,
}

impl Store {
    fn append(&mut self, value: u64) {
        self.values.push(value);
    }

    /// Forwards its receiver and parameter unchanged.
    pub fn push(&mut self, value: u64) {
        self.append(value)
    }

    /// Adds work, so it is not a forwarder.
    pub fn push_twice(&mut self, value: u64) {
        self.append(value);
        self.append(value);
    }

    /// Reorders nothing but transforms its argument.
    pub fn push_next(&mut self, value: u64) {
        self.append(value + 1)
    }
}

fn combine(left: u64, right: u64) -> u64 {
    left ^ right
}

/// Forwards both parameters in order.
pub fn combined(left: u64, right: u64) -> u64 {
    combine(left, right)
}

/// Swapping parameters is a decision, not forwarding.
pub fn swapped(left: u64, right: u64) -> u64 {
    combine(right, left)
}

/// Forwarding to a library function is an adapter, not a local wrapper.
pub fn largest(left: u64, right: u64) -> u64 {
    std::cmp::max(left, right)
}

/// A newtype constructor builds a value; it forwards to no function.
pub struct Identifier(u64);

impl Identifier {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

pub fn callers(store: &mut Store) -> u64 {
    store.push(1);
    combined(1, 2) + combined(3, 4) + Identifier::new(5).value()
}
