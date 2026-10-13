#![cfg_attr(nervix_lint, nervix::layer(edge, reason="public language adapter"))]
#[cfg_attr(nervix_lint, nervix::value(model, reason="configuration union"))]
pub enum Configuration { Ready }
pub use Configuration as Renamed;
#[cfg_attr(nervix_lint, nervix::layer(language, reason="language implementation"))]
mod language { pub fn parse() -> u32 { 0 } }
pub use language::parse;
