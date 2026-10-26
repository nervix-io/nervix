//! A named Result remains a failure outcome when a wildcard binding drops it.
pub fn operation(result: Result<(), std::io::Error>) {
    let _ = result;
}
