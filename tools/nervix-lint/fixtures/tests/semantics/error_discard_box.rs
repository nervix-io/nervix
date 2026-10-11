//! Owning storage does not make a discarded result an ordinary value.
pub fn run(result: std::io::Result<()>) { let _ = Box::new(result); }
