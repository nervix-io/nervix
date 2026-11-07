//! Test harness for codec-head parsing and composed-session completion.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Execution of the codec-head assertions kept beside their grammar.
//! - **Depends on.** The public Plexus parser and completion APIs.
//! - **Must not know.** Registry installation or codec execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test harness stays outside product execution")
)]

use nervix_plexus::{client_statement, codec::parse_create_codec};

#[path = "../src/codec/kinded_heads.rs"]
mod kinded_heads;
