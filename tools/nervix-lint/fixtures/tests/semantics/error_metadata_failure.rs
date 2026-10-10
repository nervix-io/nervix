//! A bare operation failure declares no outcome contract.
#[derive(Debug)]
pub struct Failure;
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("failed") }
}
impl std::error::Error for Failure {}
