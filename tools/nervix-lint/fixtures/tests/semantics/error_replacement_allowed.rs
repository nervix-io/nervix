//! Preserved causes, semantic projections and exact producer contracts are distinct controls.
#![cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "compiler calibration exercises these boundaries once"))]

use nervix_lint_fixtures::{FixtureFailure, error_stack::{Report, ResultExt as _}};

pub fn foreign(failure: std::io::Result<()>) -> Result<(), Report<FixtureFailure>> {
    failure.change_context(FixtureFailure)
}

pub fn bound_preserved_callback(failure: std::io::Result<()>) -> Result<(), Report<FixtureFailure>> {
    let convert = |cause: std::io::Error| Report::new(cause).change_context(FixtureFailure);
    let borrowed = &convert;
    failure.map_err(borrowed)
}

pub fn captured_cause(failure: std::io::Result<()>) -> Result<(), Report<FixtureFailure>> {
    failure.map_err(|cause| (|| Report::new(cause).change_context(FixtureFailure))())
}

pub fn nested(failure: Result<(), Report<FixtureFailure>>) -> Result<(), Report<FixtureFailure>> {
    failure.change_context(FixtureFailure)
}

pub fn matched(failure: std::io::Result<()>) -> Result<(), Report<FixtureFailure>> {
    match failure {
        Ok(value) => Ok(value),
        Err(error) => Err(Report::new(error).change_context(FixtureFailure)),
    }
}

#[derive(Debug)]
#[cfg_attr(nervix_lint, nervix::error_boundary(outcome, reason = "this field carries one semantic row refusal"))]
pub struct Refusal(pub u8);

impl std::fmt::Display for Refusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "semantic refusal {}", self.0)
    }
}
impl std::error::Error for Refusal {}

pub fn typed_outcome(failure: Result<(), Refusal>) -> Result<(), Report<FixtureFailure>> {
    failure.map_err(|_| Report::new(FixtureFailure))
}

pub fn semantic_projection(failure: std::io::Result<()>) -> Result<(), Refusal> {
    failure.map_err(|_| Refusal(1))
}

#[cfg_attr(nervix_lint, nervix::error_boundary(outcome, reason = "the calibration producer declares a fixed semantic refusal"))]
pub fn refusal() -> std::io::Result<()> {
    Err(std::io::Error::other("refusal"))
}

pub fn return_contract() -> Result<(), Report<FixtureFailure>> {
    let answer = refusal();
    answer.map_err(|_| Report::new(FixtureFailure))
}

pub fn mapped_return_contract() -> Result<u32, Report<FixtureFailure>> {
    let mapped = refusal().map(|()| 7);
    mapped.map_err(|_| Report::new(FixtureFailure))
}

pub fn qualified_mapped_return_contract() -> Result<u32, Report<FixtureFailure>> {
    let mapped = Result::map(refusal(), |()| 7);
    mapped.map_err(|_| Report::new(FixtureFailure))
}

#[cfg_attr(nervix_lint, nervix::error_boundary(outcome, reason = "the asynchronous producer declares the same fixed semantic refusal"))]
pub async fn async_refusal() -> std::io::Result<()> {
    Err(std::io::Error::other("refusal"))
}

pub async fn asynchronous_return_contract() -> Result<(), Report<FixtureFailure>> {
    async_refusal().await.map_err(|_| Report::new(FixtureFailure))
}

pub fn probes(failure: std::io::Result<()>) -> bool {
    match failure {
        Ok(()) => true,
        Err(_) => false,
    }
}

pub fn zero_sized(failure: Result<(), ()>) -> Result<(), Report<FixtureFailure>> {
    failure.map_err(|_| Report::new(FixtureFailure))
}

pub fn integer_range(value: i64) -> Result<u8, Report<FixtureFailure>> {
    u8::try_from(value).map_err(|_| Report::new(FixtureFailure))
}

pub fn closed_channel(value: Result<(), nervix_lint_fixtures::UnsentValue<u32>>) -> Result<(), Report<FixtureFailure>> {
    value.map_err(|_| Report::new(FixtureFailure))
}

pub fn permit(value: Result<(), nervix_lint_fixtures::PermitRefusal>) -> Result<(), Report<FixtureFailure>> {
    value.map_err(|_| Report::new(FixtureFailure))
}

pub fn missing_key(values: &[u32], key: u32) -> Result<usize, Report<FixtureFailure>> {
    values.binary_search(&key).map_err(|_| Report::new(FixtureFailure))
}

pub fn selected_comparison(value: &nervix_lint_fixtures::Counter) -> Result<u64, Report<FixtureFailure>> {
    use nervix_lint_fixtures::MemoryOrdering;
    value.compare_exchange(0, 1, MemoryOrdering::Relaxed, MemoryOrdering::Relaxed)
        .map_err(|_| Report::new(FixtureFailure))
}

pub fn formatter_projection(value: Result<(), nervix_lint_fixtures::error_stack::Report<FixtureFailure>>) -> std::fmt::Result {
    value.map_err(|_| std::fmt::Error)
}

pub struct OtherResult;
impl OtherResult {
    pub fn map_err(&self, _convert: impl FnOnce(std::io::Error) -> Report<FixtureFailure>) {}
}

pub fn custom(value: OtherResult) {
    value.map_err(|_| Report::new(FixtureFailure));
}
