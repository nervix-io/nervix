//! Source-contract compiler fixture; no runtime behavior is asserted.
//!
//! Crate metadata keeps no attributes for an `extern` block, so each foreign item below takes its
//! contracts from the module that holds its block.

#![cfg_attr(nervix_lint, nervix::layer(edge, reason = "foreign interface adapter"))]

#[cfg_attr(nervix_lint, nervix::layer(language, reason = "language implementation"))]
pub mod language {
    unsafe extern "C" {
        pub fn parse_foreign() -> u32;
    }
}

#[cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "the caller installs the driver once"))]
pub mod installation {
    unsafe extern "C" {
        pub fn install_foreign();
    }
}

pub mod native {
    unsafe extern "C" {
        pub fn count_foreign() -> u32;
    }
}
