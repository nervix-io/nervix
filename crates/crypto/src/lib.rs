//! Cryptography Nervix obtains from AWS-LC.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** TLS providers, SHA-2 digests, secure randomness, password derivation and Ed25519 signing.
//! - **Depends on.** AWS-LC, rustls, primitive ownership/initialization and typed reports.
//! - **Must not know.** Models, graphs, connectors, credentials formats or runtime policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the AWS-LC cryptography boundary")
)]

pub mod digest;
pub mod ed25519;
pub mod password;
pub mod random;
pub mod tls;

#[cfg(test)]
mod digest_tests;
