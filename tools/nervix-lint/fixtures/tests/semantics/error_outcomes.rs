//! Source-owned semantic and foreign-library boundaries preserve their exact contracts.
#[derive(Debug)]
#[cfg_attr(nervix_lint, nervix::error_boundary(outcome, reason = "one row's semantic refusal is not an operation failure"))]
pub struct RowError;
pub fn row() -> Result<(), RowError> { Err(RowError) }
#[derive(Debug)]
#[cfg_attr(nervix_lint, nervix::error_boundary(library, reason = "the modeled channel must preserve the external library's closed-channel contract"))]
pub struct ChannelError;
pub async fn changed() -> Result<(), ChannelError> { Err(ChannelError) }
pub struct MerelyNamedError;
pub fn ordinary() -> Result<(), MerelyNamedError> { Err(MerelyNamedError) }
impl std::fmt::Display for RowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("typed outcome") }
}
impl std::error::Error for RowError {}
impl std::fmt::Display for ChannelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("typed outcome") }
}
impl std::error::Error for ChannelError {}
