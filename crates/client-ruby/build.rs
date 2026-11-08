//! Build tooling for the Ruby edge's immutable external-buffer flag combination.
//!
//! Outside the layer order: build tooling.
//! - **Owns.** Compiling the C bitmask shim against the selected Ruby's headers.
//! - **Depends on.** rb-sys's exported interpreter configuration and the C compiler.
//! - **Must not know.** Client sessions or the server.

use meticulous::{OptionExt as _, ResultExt as _};

fn main() {
    let ruby = rb_sys_env::load().assured("rb-sys provides the selected Ruby configuration");
    ruby.print_cargo_rustc_cfg();
    // Linux resolves extension symbols in its Ruby host; embedded tests link Ruby through
    // rb-sys's link-ruby feature. An interpreter's application DLDFLAGS do not belong on this
    // Rust library (distribution metadata flags may not be understood by its linker).
    // Other platforms need the loader/link configuration rb-sys exports for extensions.
    #[cfg(not(target_os = "linux"))]
    ruby.print_encoded_cargo_args();
    assert!(
        ruby.ruby_major_minor() >= (3, 3),
        "the Nervix gem requires Ruby 3.3 or later"
    );
    let headers = ruby
        .get_rbconfig_value("RUBYHDRDIR")
        .assured("Ruby declares its header directory");
    let architecture = ruby
        .get_rbconfig_value("RUBYARCHHDRDIR")
        .assured("Ruby declares its architecture headers");
    let abi = ruby
        .get_rbconfig_value("RUBY_VERSION")
        .assured("Ruby declares its extension ABI");
    println!("cargo:rustc-env=NERVIX_RUBY_ABI={abi}");
    // An embedded executable must also locate a Ruby installed outside the system loader's
    // paths. The restored unit runner provisions the same interpreter as its build job.
    #[cfg(unix)]
    if std::env::var_os("CARGO_FEATURE_RUBY_TESTS").is_some() {
        let library = ruby
            .get_rbconfig_value("LIBDIR")
            .assured("Ruby declares its library directory");
        println!("cargo:rustc-link-arg=-Wl,-rpath,{library}");
    }
    cc::Build::new()
        .include(headers)
        .include(architecture)
        .file("src/buffer.c")
        .compile("nervix_ruby_buffer");
    println!("cargo:rerun-if-changed=src/buffer.c");
}
