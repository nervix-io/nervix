//! Resolution sees governed types through glob imports, reexported renames and associated types,
//! which spelling alone cannot follow.
mod units {
    pub type Count = usize;
    pub use std::time::Duration as Wait;
}

use units::*;

pub trait Measure {
    type Unit;
}
pub struct Bytes;
impl Measure for Bytes {
    type Unit = u64;
}

pub const WORKERS: Count = 8;
pub const TIMEOUT: units::Wait = units::Wait::from_secs(5);
pub const CAPACITY: <Bytes as Measure>::Unit = 4096;
