//! Lets the package's tests, which embed CPython, find the Python library they link when it lies
//! outside the loader's default path, as the library of a pyenv or uv interpreter does.
//!
//! An extension module built for Python links no Python library, so it gets no path.

use std::env;

fn main() {
    println!("cargo::rerun-if-env-changed=PYO3_BUILD_EXTENSION_MODULE");
    if env::var_os("PYO3_BUILD_EXTENSION_MODULE").is_some() {
        return;
    }
    if let Some(lib_dir) = pyo3_build_config::get().lib_dir() {
        println!("cargo::rustc-link-arg=-Wl,-rpath,{lib_dir}");
    }
}
