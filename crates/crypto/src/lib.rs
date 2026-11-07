//! Cryptography Nervix obtains from AWS-LC.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Purpose-specific TLS providers and Ed25519 key preparation and signing.
//! - **Depends on.** AWS-LC, rustls, primitive ownership/initialization and typed reports.
//! - **Must not know.** Models, graphs, connectors, credentials formats or runtime policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the AWS-LC cryptography boundary")
)]

pub mod ed25519;
pub mod tls;
