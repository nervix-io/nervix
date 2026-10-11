//! Resource owners and lifetime guards are ordinary values, even when they retain observations.
use nervix_lint_fixtures::{FixtureFailure, error_stack::Report, nervix_recovery::Discarded};

pub struct Observation {
    pub attempts: u32,
    pub latest: Option<Report<FixtureFailure>>,
}

pub struct Guard(Option<Report<FixtureFailure>>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.take().discarded("ending this guard consumes its retained observation");
    }
}

pub fn run() {
    let _ = Observation { attempts: 0, latest: None };
    let _ = Guard(None);
    let _ = Vec::<u32>::new();
    let _ = std::io::Cursor::new(Vec::<u32>::new());
}
