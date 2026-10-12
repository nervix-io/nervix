#![cfg_attr(nervix_lint, nervix::layer(edge, reason="parser adapter"))]
#[cfg_attr(nervix_lint, nervix::layer(language, reason="language implementation"))]
mod parser { pub struct Token; }
pub type Input = parser::Token;
#[cfg_attr(nervix_lint, nervix::layer(engine, reason="execution engine"))]
pub fn execute(_: Option<Input>) {}
