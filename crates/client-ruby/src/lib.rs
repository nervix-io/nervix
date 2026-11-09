//! Ruby's native Nervix client gem, on magnus over the shared host session runtime.
//!
//! Layer: edges.
//! - **Owns.** Ruby objects, exceptions, typed columns, owner-retaining buffers and GVL release.
//! - **Depends on.** Magnus, rb-sys, the client vocabulary and the shared host session runtime.
//! - **Must not know.** The server, the C ABI, or how sessions route, recover or decide outcomes.
//!
//! Only the calling Ruby thread enters Ruby. A wait holds owned native inputs with the GVL
//! released; collection drops handles without waiting. Read-only frame views retain their owner.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the native Ruby client edge")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "Ruby calls and GVL release belong to the client host edge"
    )
)]

mod batches;
mod clocks;
mod endpoints;
mod enums;
mod errors;
mod gvl;
mod inspection;
mod schema;
mod sessions;
mod subscriptions;
mod value_object;
mod values;

use magnus::{Error, RClass, RModule, Ruby, prelude::*};

/// Register the native classes in an interpreter the caller owns, as the gem loader does.
pub fn register(ruby: &Ruby) -> Result<(), Error> {
    let nervix = ruby.define_module("Nervix")?;
    let client = nervix.define_module("Client")?;
    errors::register(ruby, client)?;
    enums::register(client)?;
    schema::register(ruby, client)?;
    batches::register(ruby, client)?;
    sessions::register(ruby, client)?;
    inspection::register(ruby, client)?;
    subscriptions::register(ruby, client)?;
    clocks::register(ruby, client)?;
    endpoints::register(ruby, client)?;
    client.const_set("RUBY_ABI", env!("NERVIX_RUBY_ABI"))?;
    client.const_set("VERSION", env!("CARGO_PKG_VERSION"))?;
    client.const_set("REDACTED", ruby.to_symbol("redacted"))?;
    Ok(())
}

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    register(ruby)
}

/// Native constructors initialize their own Rust value; Object's allocator cannot create one.
fn class(ruby: &Ruby, module: RModule, name: &str) -> Result<RClass, Error> {
    let class = module.define_class(name, ruby.class_object())?;
    class.undef_default_alloc_func();
    Ok(class)
}

#[cfg(all(test, feature = "ruby-tests"))]
mod tests;
