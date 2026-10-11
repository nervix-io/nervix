#[cfg_attr(nervix_lint, nervix::layer(language, reason="parser owner"))]
mod parser {
    pub trait Parse { fn parse(&self) -> u32; }
}
#[cfg_attr(nervix_lint, nervix::layer(engine, reason="expression evaluator"))]
pub fn execute(value: &impl parser::Parse) -> u32 { value.parse() }
