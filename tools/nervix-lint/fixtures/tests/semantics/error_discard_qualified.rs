//! A renamed qualified drop still discards the resolved outcome.
pub fn run(result: std::io::Result<()>) {
    use std::mem::drop as release;
    release(result);
}
