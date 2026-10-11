//! A renamed error retains its source-owned classification.
use callee::Refusal as Response;
pub async fn answer() -> Result<(), Response> { Err(Response) }
