use std::{
    env, fs,
    path::{Path, PathBuf},
};

const SCHEMA: &str = "schema/nervix_wasm.fbs";
const GENERATED_FILE: &str = "nervix_wasm_generated.rs";
const REDUNDANT_ROOT_IMPORTS: &str = concat!(
    "use core::mem;\n",
    "use core::cmp::Ordering;\n\n",
    "extern crate flatbuffers;\n",
    "use self::flatbuffers::{EndianScalar, Follow};\n\n",
);

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

    normalize_bindings(&generated_dir.join(GENERATED_FILE));
}

fn normalize_bindings(path: &Path) {
    let generated = fs::read_to_string(path).expect("failed to read generated FlatBuffers binding");

    // Older compatible flatc releases emit unused imports at the file root,
    // omit explicit lifetimes from root accessors, and attach unused lifetimes
    // to enum verification and lifetime-free argument implementations. Newer
    // output is already normalized, so these replacements leave it unchanged.
    let normalized = generated
        .replacen(REDUNDANT_ROOT_IMPORTS, "", 1)
        .replace(
            "Result<Message, flatbuffers::InvalidFlatbuffer>",
            "Result<Message<'_>, flatbuffers::InvalidFlatbuffer>",
        )
        .replace("(buf: &[u8]) -> Message {", "(buf: &[u8]) -> Message<'_> {")
        .replace(
            "impl<'a> flatbuffers::Verifiable for ",
            "impl flatbuffers::Verifiable for ",
        )
        .replace(
            "impl<'a> Default for OutputColumnRefArgs {",
            "impl Default for OutputColumnRefArgs {",
        )
        .replace(
            "impl<'a> Default for MessageArgs {",
            "impl Default for MessageArgs {",
        );

    fs::write(path, normalized).expect("failed to normalize generated FlatBuffers binding");
}
