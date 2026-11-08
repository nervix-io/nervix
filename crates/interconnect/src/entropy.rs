//! The unpredictable values one transport process allocates.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The source of the process epoch and of relay grant identifiers.
//! - **Depends on.** The cryptography boundary's secure random values.
//! - **Must not know.** What an identifier fences, or how a peer uses it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::fmt;

use nervix_primitives::sync::StdArc;

/// The source of the unpredictable values a transport allocates: its process epoch, which fences
/// work addressed to an earlier incarnation of the same node, and its relay grant identifiers.
///
/// Production draws both from AWS-LC through the cryptography boundary. A controlled environment,
/// such as a simulation, supplies its own source so the same scenario allocates the same identities.
/// Cryptographic randomness inside TLS is not drawn from here.
#[derive(Clone)]
pub struct TransportEntropy {
    source: StdArc<dyn Fn() -> u64 + Send + Sync>,
}

impl TransportEntropy {
    /// Draw every value from AWS-LC's operating-system-seeded secure random source.
    pub fn operating_system() -> Self {
        Self {
            source: StdArc::new(nervix_crypto::random::u64),
        }
    }

    /// Draw every value from `source`, owned by the environment that constructs the transport.
    pub fn from_source(source: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        Self {
            source: StdArc::new(source),
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the transport constructor supplies the environment-owned \
                                   entropy callback; no internal runtime ownership is inferred")
    )]
    pub(crate) fn next_u64(&self) -> u64 {
        (self.source)()
    }
}

impl fmt::Debug for TransportEntropy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TransportEntropy")
    }
}
