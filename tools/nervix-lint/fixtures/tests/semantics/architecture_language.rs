//! Resolved language provenance survives an import alias.

#[cfg_attr(nervix_lint, nervix::layer(language, reason = "owns parsing"))]
pub mod language {
    pub struct Token;
}

#[cfg_attr(nervix_lint, nervix::layer(engine, reason = "consumes structured vocabulary"))]
pub mod engine {
    use super::language::Token as Input;
    pub fn consume(_: Input) {}
}
