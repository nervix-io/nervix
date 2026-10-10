//! Exact error classifications cross compiler metadata.
#[derive(Debug)]
#[cfg_attr(nervix_lint, nervix::error_boundary(outcome, reason = "the wire contract returns its typed ordinary refusal"))]
pub struct Refusal;
impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("refused") }
}
impl std::error::Error for Refusal {}
