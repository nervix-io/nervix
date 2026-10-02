//! Compiler fixture outside the product layer order.
//! Owns: a generated acquisition whose exclusion is reported explicitly.
//! Depends on: build-time environment and filesystem I/O.
//! Must not know: policy scopes or runtime state.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    std::fs::write(
        output.join("acquisition.rs"),
        "pub fn generated_acquisition(map: &nervix_primitives::collections::DashMap<u32, u32>) { \
         drop(map.get(&1)); }\n",
    )?;
    Ok(())
}
