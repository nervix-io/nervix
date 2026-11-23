//! One expectation cannot hide two policy choices.
use std::time::Duration;

pub fn two() -> (Vec<u8>, Duration) {
    #[cfg_attr(nervix_lint, expect(nervix::hidden_tunable, reason = "two unrelated values"))]
    let pair = (Vec::with_capacity(4), Duration::from_secs(1));
    pair
}
