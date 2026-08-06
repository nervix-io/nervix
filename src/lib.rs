#![recursion_limit = "256"]

#[cfg(all(feature = "testing", not(debug_assertions)))]
compile_error!(
    "the `testing` feature lowers Argon2 password hashing parameters and must not be compiled \
     with release-like profiles"
);

pub mod application;
pub mod cluster;
pub mod jaq_program;
pub mod memory_pressure;
pub mod metrics;
pub(crate) mod registry;
pub mod resource;
pub mod runtime;
pub mod runtime_ack;
pub mod runtime_schema;

pub use nervix_proto as proto;
#[cfg(feature = "testing")]
pub use registry::SchedulerMode;
