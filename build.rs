//! Build tooling outside the product layers.
//! Owns validation of the prepared ONNX Runtime and its system link dependencies.
//! May depend on the artifact manifest and Cargo's target environment.
//! Must not download dependencies or make runtime decisions.

use std::{env, error::Error, fs, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-env-changed=ORT_LIB_PATH");
    println!("cargo:rerun-if-changed=scripts/onnxruntime/manifest.toml");
    let library_dir = PathBuf::from(env::var_os("ORT_LIB_PATH").ok_or(
        "ORT_LIB_PATH is required; fetch the published static artifact from R2 with just \
         fetch-onnxruntime",
    )?);
    let archive = library_dir.join("libonnxruntime.a");
    if !archive.is_file() {
        return Err(format!("missing static ONNX Runtime archive: {}", archive.display()).into());
    }
    let package = library_dir
        .parent()
        .ok_or("ORT_LIB_PATH must name the prepared package's lib directory")?;
    let manifest_path = package.join("manifest.json");
    println!("cargo:rerun-if-changed={}", manifest_path.display());
    println!("cargo:rerun-if-changed={}", archive.display());
    let manifest: serde_json::Value = serde_json::from_str(&fs::read_to_string(manifest_path)?)?;
    let pinned: toml::Value =
        toml::from_str(&fs::read_to_string("scripts/onnxruntime/manifest.toml")?)?;
    if manifest["identity"]["configuration"] != serde_json::to_value(&pinned)? {
        return Err("prepared ONNX Runtime does not match the pinned configuration".into());
    }
    let target = env::var("TARGET")?;
    let platform = match target.as_str() {
        "x86_64-unknown-linux-gnu" => "linux/amd64",
        "aarch64-unknown-linux-gnu" => "linux/arm64",
        "aarch64-apple-darwin" => "darwin/arm64",
        _ => return Err(format!("unsupported ONNX Runtime target: {target}").into()),
    };
    if manifest["identity"]["platform"].as_str() != Some(platform) {
        return Err(format!("prepared ONNX Runtime does not match Cargo target {target}").into());
    }
    if platform == "darwin/arm64" {
        println!("cargo:rustc-link-lib=iconv");
    } else {
        for library in [
            "libonnxruntime_providers_shared.so",
            "libonnxruntime_providers_cuda.so",
            "libcudnn_graph.so.9",
        ] {
            if !package.join("runtime/lib").join(library).is_file() {
                return Err(format!("Linux ONNX Runtime package is missing {library}").into());
            }
        }
        for library in ["dl", "pthread", "m", "rt", "atomic"] {
            println!("cargo:rustc-link-lib={library}");
        }
    }
    Ok(())
}
