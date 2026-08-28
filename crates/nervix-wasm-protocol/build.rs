use std::{env, fs, path::PathBuf};

const SCHEMA: &str = "schema/nervix_wasm.fbs";

fn main() {
    println!("cargo:rerun-if-changed={SCHEMA}");
    println!("cargo:rerun-if-env-changed=FLATC_PATH");

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo must provide OUT_DIR"));
    let generated_dir = out_dir.join("flatbuffers");
    fs::create_dir_all(&generated_dir).expect("failed to create FlatBuffers output directory");
    let compiler = env::var_os("FLATC_PATH").map_or_else(
        flatc_rust::Flatc::from_env_path,
        flatc_rust::Flatc::from_path,
    );
    compiler
        .check()
        .and_then(|()| {
            compiler.run(flatc_rust::Args {
                inputs: &[SCHEMA.as_ref()],
                out_dir: &generated_dir,
                ..Default::default()
            })
        })
        .expect("failed to generate Rust FlatBuffers bindings; install flatc or set FLATC_PATH");
}
