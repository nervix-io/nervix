//! A collection owns its failure outcomes.
pub fn run(result: std::io::Result<()>) { std::mem::drop(vec![result]); }
