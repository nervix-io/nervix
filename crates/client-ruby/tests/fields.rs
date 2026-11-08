//! Embedded Ruby field-description checks, with one interpreter owned by this executable.
//!
//! Outside the layer order: a test harness.
//! - **Owns.** Complete field-description and immutable snapshot oracles through the native gem.
//! - **Depends on.** The binding registration, embedded Ruby and bounded Bolero inputs.
//! - **Must not know.** Endpoint admission, native handle layouts or Ruby's collector implementation.

use magnus::prelude::*;
use meticulous::ResultExt as _;

#[test]
fn bolero_field_descriptions_round_trip_through_ruby() {
    // SAFETY: This executable has one test, initializes Ruby once and keeps every Ruby value
    // inside the interpreter's lifetime. No library worker calls Ruby.
    let ruby = unsafe { magnus::embed::init() };
    nervix_client_ruby::register(&ruby).assured("the binding registers in the fresh interpreter");
    ruby.eval::<magnus::Value>(include_str!("properties.rb"))
        .assured("the complete Ruby field oracle loads");
    let oracle: magnus::RModule = ruby
        .eval("RubyProperties")
        .assured("the script declares its oracle module");
    bolero::check!()
        .with_iterations(256)
        .with_max_len(256)
        .for_each(|input: &[u8]| {
            oracle
                .funcall::<_, _, magnus::Value>("fields", (ruby.str_from_slice(input),))
                .assured("every field name, level, length, flag and order survives Ruby");
        });
}
