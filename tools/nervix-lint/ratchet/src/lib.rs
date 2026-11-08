//! Repository tooling, outside the product layer order.
//! Owns: complete compiler-analysis evidence and review audits, compilation lifetimes and memory admission.
//! Depends on: the report vocabulary, selected primitives, Cargo and operating-system processes.
//! Must not know: product graph policy, runtime execution or manually curated source findings.

pub mod admission;
pub mod admission_queue;
pub mod analysis;
pub mod applicability;
pub mod audit;
pub mod cache;
pub mod configuration;
pub mod contracts;
pub mod crap;
pub mod error;
pub mod evidence;
pub mod gate;
pub mod host_memory;
pub mod identity;
pub mod io;
pub mod matrix;
pub mod measurement;
pub mod process;
pub mod render;
pub mod runner;
pub mod wrapper;

#[cfg(test)]
mod representations;

#[cfg(test)]
mod test_support;
