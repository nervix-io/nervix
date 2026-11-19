//! The limit an operator puts on how long a waiting operation may wait.
//!
//! An operation an operator requests that waits for work to finish, such as a `STOP` waiting for
//! the messages in motion, has no time limit of its own. Its statement may name one with a clause:
//! `ENFORCE AFTER <duration>` forces the operation through once the time has passed, and
//! `CANCEL AFTER <duration>` calls it off. A statement carries at most one of the two.

use std::time::Duration;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// What happens to a waiting operation once the stated time has passed since it was requested.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum WaitLimit {
    /// `ENFORCE AFTER <duration>`: the operation is forced through, cutting what it still waits for.
    Enforce { after: Duration },
    /// `CANCEL AFTER <duration>`: the operation is called off, and what it changed so far is
    /// undone.
    Cancel { after: Duration },
}

impl WaitLimit {
    /// How long after its request the operation waits before the limit acts.
    pub const fn after(self) -> Duration {
        match self {
            Self::Enforce { after } | Self::Cancel { after } => after,
        }
    }

    /// Whether the limit forces the operation through rather than calling it off.
    pub const fn enforces(self) -> bool {
        matches!(self, Self::Enforce { .. })
    }
}

impl std::fmt::Display for WaitLimit {
    /// The clause as canonical Plexus writes it, such as `ENFORCE AFTER 30s`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&crate::canonical::wait_limit_clause(*self))
    }
}
