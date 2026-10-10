//! Renaming a foreign-crate local failure does not erase ownership.
use callee::Failure as Response;
pub fn answer() -> Result<(), Response> { Err(Response) }
