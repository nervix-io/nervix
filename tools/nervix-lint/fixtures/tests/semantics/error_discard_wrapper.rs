//! Field ownership exposes an outcome behind a wrapper.
pub struct Wrapper<T>(pub T);
pub fn run(result: std::io::Result<()>) { let _ = Wrapper(Some(result)); }
