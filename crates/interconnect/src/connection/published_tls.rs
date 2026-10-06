//! The credentials new interconnect connections authenticate with, and their generation.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Publishing a credential bundle together with its generation, and each replacement's
//!   next generation.
//! - **Depends on.** The TLS configuration bundle and the primitive boundary's publication.
//! - **Must not know.** Connections, slots, certificate files, or how a replacement is detected.

use meticulous::OptionExt as _;
use nervix_primitives::{publication::ArcSwap, sync::StdArc};

use super::TlsConfigBundle;

/// The credentials new connections authenticate with, published without a lock.
///
/// Every connection records the generation it authenticated under and drains once the published
/// generation moves past it. The bundle and its generation are published as one value, so a
/// connection always records the generation of exactly the credentials it used, and each
/// replacement publishes under a generation of its own, also when two replacements race.
pub(super) struct PublishedTls {
    current: ArcSwap<ActiveTls>,
}

/// The credentials one connection authenticates with, and the generation it records for them.
#[derive(Clone)]
pub(super) struct ActiveTls {
    pub(super) generation: u64,
    pub(super) bundle: StdArc<TlsConfigBundle>,
}

impl PublishedTls {
    pub(super) fn new(bundle: TlsConfigBundle) -> Self {
        Self {
            current: ArcSwap::from_pointee(ActiveTls {
                generation: 1,
                bundle: StdArc::new(bundle),
            }),
        }
    }

    pub(super) fn generation(&self) -> u64 {
        self.current.load().generation
    }

    pub(super) fn current(&self) -> ActiveTls {
        ActiveTls::clone(&self.current.load())
    }

    /// Publishes `bundle` under the generation after the current one.
    pub(super) fn replace(&self, bundle: TlsConfigBundle) {
        let bundle = StdArc::new(bundle);
        self.current.rcu(|current| ActiveTls {
            generation: current
                .generation
                .checked_add(1)
                .assured("a node cannot replace its TLS credentials 2^64 times"),
            bundle: StdArc::clone(&bundle),
        });
    }
}
