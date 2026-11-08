//! Embedded Ruby representation checks. Each test executable owns exactly one interpreter.
//!
//! Outside the layer order: a test harness.
//! - **Owns.** The complete logical-value oracle through the native Ruby builder, reader and IPC.
//! - **Depends on.** The binding registration, embedded Ruby and bounded Bolero inputs.
//! - **Must not know.** Server execution, native handle layouts or Ruby's collector implementation.

use magnus::prelude::*;
use meticulous::ResultExt as _;

#[test]
fn bolero_columns_round_trip_through_ruby() {
    // SAFETY: This executable has one test and initializes Ruby once on the calling thread.
    // Every Ruby value is dropped before the cleanup guard ends the interpreter.
    let ruby = unsafe { magnus::embed::init() };
    nervix_client_ruby::register(&ruby).assured("the binding registers in the fresh interpreter");
    ruby.eval::<magnus::Value>(include_str!("properties.rb"))
        .assured("the complete Ruby property oracle loads");
    let oracle: magnus::RModule = ruby
        .eval("RubyProperties")
        .assured("the script declares its oracle module");
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .for_each(|input: &[u8]| {
            oracle
                .funcall::<_, _, magnus::Value>("columns", (ruby.str_from_slice(input),))
                .assured("complete logical values survive Ruby builder, reader and IPC calls");
        });
}
