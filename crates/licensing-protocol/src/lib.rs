//! The signed licensing contract shared by Nervix, its service and its test service.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Licensing documents, signature profile, trust, validation and condition arithmetic.
//! - **Depends on.** The cryptography boundary and maintained JSON, JWS and identity encodings.
//! - **Must not know.** Models, graphs, consensus, databases, network I/O or the physical clock.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the shared licensing protocol")
)]

pub mod conditions;
pub mod document;
pub mod documents;
pub mod impact;
pub mod keys;
pub mod problems;
pub mod requests;
pub mod trust;
pub mod validation;

#[cfg(test)]
mod probe_tests;
#[cfg(test)]
mod signature_vector_tests;
#[cfg(test)]
mod test_fixture;
#[cfg(test)]
mod verification_tests;

#[cfg(test)]
mod policy_tests;

#[cfg(test)]
mod encoding_tests;

#[cfg(test)]
mod representation_properties;

#[cfg(test)]
mod policy_properties;

#[cfg(test)]
mod trust_properties;
