use std::{env, fs, path::PathBuf};

use meticulous::OptionExt as _;

/// Every shape of the protocol lives in this file. The root files beside it only name frame roots
/// for other languages' code generators, so Rust generation reads this file alone.
const SCHEMA: &str = "schema/session.fbs";
const ROOT_SCHEMAS: [&str; 4] = [
    "schema/client_message.fbs",
    "schema/server_message.fbs",
    "schema/upload_message.fbs",
    "schema/upload_reply.fbs",
];

/// The lints flatc's Rust output trips, each admitted with the reason it cannot be avoided in code
/// this crate does not write. The wrapper module carries them so the generated text stays as flatc
/// produced it.
const GENERATED_MODULE_HEADER: &str = "\
#[expect(
    clippy::as_conversions,
    reason = \"flatc copies fixed-layout structs through pointer casts\"
)]
#[expect(
    clippy::derivable_impls,
    reason = \"flatc writes the zeroed Default of a fixed-layout struct by hand\"
)]
#[expect(
    clippy::extra_unused_lifetimes,
    reason = \"flatc declares a lifetime on verifier impls that do not use it\"
)]
#[allow(
    unused_imports,
    reason = \"some flatc versions emit root imports used only by nested modules\"
)]
mod generated {
";

fn main() {
    println!("cargo:rerun-if-changed={SCHEMA}");
    for root in ROOT_SCHEMAS {
        println!("cargo:rerun-if-changed={root}");
    }
    println!("cargo:rerun-if-env-changed=FLATC_PATH");

    let out_dir =
        PathBuf::from(env::var_os("OUT_DIR").assured("cargo sets OUT_DIR for every build script"));
    let generated_dir = out_dir.join("flatbuffers");
    if let Err(error) = fs::create_dir_all(&generated_dir) {
        panic!("failed to create the FlatBuffers output directory: {error}");
    }
    let compiler = match env::var_os("FLATC_PATH") {
        Some(path) => flatc_rust::Flatc::from_path(path),
        None => flatc_rust::Flatc::from_env_path(),
    };
    let result = match compiler.check() {
        Ok(()) => compiler.run(flatc_rust::Args {
            inputs: &[SCHEMA.as_ref()],
            out_dir: &generated_dir,
            ..Default::default()
        }),
        Err(error) => Err(error),
    };
    if let Err(error) = result {
        panic!(
            "failed to generate the Rust FlatBuffers bindings: {error}; install flatc or set \
             FLATC_PATH"
        );
    }

    let generated = match fs::read_to_string(generated_dir.join("session_generated.rs")) {
        Ok(generated) => generated,
        Err(error) => panic!("failed to read the generated FlatBuffers bindings: {error}"),
    };
    let module = format!("{GENERATED_MODULE_HEADER}{generated}\n}}\n");
    if let Err(error) = fs::write(generated_dir.join("session_module.rs"), module) {
        panic!("failed to write the generated FlatBuffers module: {error}");
    }
}
