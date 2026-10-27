//! Exact outcome types and return contracts are exported through compiler metadata.
#[derive(Debug)]
#[cfg_attr(nervix_lint, nervix::error_boundary(outcome, reason = "this field carries a semantic refusal, not an operation diagnostic"))]
pub struct Refusal(pub u8);
impl std::fmt::Display for Refusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "refusal {}", self.0)
    }
}
impl std::error::Error for Refusal {}

#[cfg_attr(nervix_lint, nervix::error_boundary(outcome, reason = "the producer's exact return is an ordinary validation refusal"))]
pub fn refusal() -> std::io::Result<()> {
    Err(std::io::Error::other("refusal"))
}

pub fn informative() -> std::io::Result<()> {
    Err(std::io::Error::other("informative failure"))
}
