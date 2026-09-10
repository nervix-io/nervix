use std::{env, fs, path::PathBuf};

use meticulous::OptionExt as _;

const SCHEMA: &str = "schema/nervix_wasm.fbs";

fn main() {
    println!("cargo:rerun-if-changed={SCHEMA}");
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
}
