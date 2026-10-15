//! Observes a generator's logical cadence through public subscription rows.
//!
//! Layer: test harness.
//! - **Owns.** A bounded subscription observation, exact source-field preservation, branch keys,
//!   per-branch timestamp order and complete occurrences sharing one execution timestamp.
//! - **Depends on.** Public JSON rows, typed timestamps and the harness's monotonic timers.
//! - **Must not know.** Generator tasks, materialized-state installation or runtime clock owners.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::Timestamp;
use nervix_primitives::time::{Instant, timeout_at};
use serde_json::{Map, Value};

/// One sample of complete logical occurrences, independent of how quickly its rows are read.
#[derive(Debug)]
pub(crate) struct GeneratorCadenceObservation {
    expected_rows: BTreeMap<String, Map<String, Value>>,
    branch_field: String,
    timestamp_field: String,
    logical_cadence: Duration,
    expected_occurrences: usize,
    branches_by_timestamp: BTreeMap<Timestamp, BTreeSet<String>>,
    latest_by_branch: BTreeMap<String, Timestamp>,
    observed: Vec<String>,
}

impl GeneratorCadenceObservation {
    pub(crate) fn new(
        rows: Vec<Value>,
        branch_field: String,
        timestamp_field: String,
        logical_cadence: Duration,
        expected_occurrences: usize,
    ) -> Self {
        assert!(
            expected_occurrences >= 4,
            "a cadence sample needs at least four occurrences"
        );
        assert!(
            !logical_cadence.is_zero(),
            "the logical cadence must be positive"
        );
        let mut expected_rows = BTreeMap::new();
        for row in rows {
            let Value::Object(fields) = row else {
                panic!("an expected generator row must be a JSON object");
            };
            let Some(Value::String(branch)) = fields.get(&branch_field) else {
                panic!("an expected row must have a string branch field '{branch_field}'");
            };
            let branch = branch.clone();
            assert!(
                expected_rows.insert(branch, fields).is_none(),
                "each expected generator branch must be unique"
            );
        }
        assert!(
            expected_rows.len() >= 2,
            "a cadence sample needs at least two branches"
        );
        Self {
            expected_rows,
            branch_field,
            timestamp_field,
            logical_cadence,
            expected_occurrences,
            branches_by_timestamp: BTreeMap::new(),
            latest_by_branch: BTreeMap::new(),
            observed: Vec::new(),
        }
    }

    /// Waits for a sample under a liveness budget; row timestamps decide its cadence verdict.
    pub(crate) async fn observe(
        &mut self,
        budget: Duration,
        mut next: impl AsyncFnMut(Duration) -> io::Result<Option<String>>,
    ) {
        let deadline = Instant::now()
            .checked_add(budget)
            .assured("the scenario's bounded observation budget fits a monotonic instant");
        loop {
            nervix_primitives::task::consume_budget().await;
            if self.is_complete() {
                return;
            }
            let now = Instant::now();
            assert!(now < deadline, "generator observation timed out: {self:?}");
            let remaining = deadline
                .checked_duration_since(now)
                .verified("the deadline comparison established remaining observation time");
            let payload = match timeout_at(deadline, next(remaining)).await {
                Ok(Ok(Some(payload))) => payload,
                Ok(Ok(None)) | Err(_) => panic!("generator observation timed out: {self:?}"),
                Ok(Err(error)) => panic!("generator subscription failed: {error}; {self:?}"),
            };
            self.record(payload);
        }
    }

    pub(crate) fn last_payload(&self) -> Option<&String> {
        self.observed.last()
    }

    fn record(&mut self, payload: String) {
        let Some((key, row)) = payload.split_once(" payload=") else {
            panic!("generator output has no concrete branch key: {payload}");
        };
        let Some(key) = key.strip_prefix("key=") else {
            panic!("generator output has no concrete branch key: {payload}");
        };
        let key = match serde_json::from_str::<Value>(key) {
            Ok(key) => key,
            Err(error) => {
                panic!("generator output has an invalid JSON branch key: {error}; {payload}")
            }
        };
        let row = match serde_json::from_str::<Value>(row) {
            Ok(row) => row,
            Err(error) => panic!("generator output has an invalid JSON row: {error}; {payload}"),
        };
        let Value::Object(mut fields) = row else {
            panic!("generator output must be a JSON object: {payload}");
        };
        let Some(Value::String(branch)) = fields.get(&self.branch_field) else {
            panic!("generator output has no string branch field: {payload}");
        };
        let branch = branch.clone();
        let Some(expected) = self.expected_rows.get(&branch) else {
            panic!("generator produced an unexpected branch: {payload}");
        };
        let expected_key = Value::Object(Map::from_iter([(
            self.branch_field.clone(),
            Value::String(branch.clone()),
        )]));
        assert_eq!(
            key, expected_key,
            "generator output did not preserve its branch key: {payload}"
        );
        let Some(Value::String(timestamp)) = fields.remove(&self.timestamp_field) else {
            panic!("generator output has no string execution timestamp: {payload}");
        };
        let timestamp = match timestamp.parse::<Timestamp>() {
            Ok(timestamp) => timestamp,
            Err(error) => panic!("generator execution timestamp is invalid: {error:#}; {payload}"),
        };
        assert_eq!(
            &fields, expected,
            "generator output did not preserve its source fields: {payload}"
        );
        if let Some(previous) = self.latest_by_branch.insert(branch.clone(), timestamp) {
            assert!(
                timestamp > previous,
                "generator timestamps did not increase for branch '{branch}': {self:?}; {payload}"
            );
        }
        self.branches_by_timestamp
            .entry(timestamp)
            .or_default()
            .insert(branch);
        self.observed.push(payload);
    }

    fn is_complete(&self) -> bool {
        let mut complete = Vec::with_capacity(self.expected_occurrences);
        for (timestamp, branches) in &self.branches_by_timestamp {
            if branches.len() == self.expected_rows.len() {
                complete.push(*timestamp);
                if complete.len() == self.expected_occurrences {
                    break;
                }
            }
        }
        if complete.len() < self.expected_occurrences {
            return false;
        }
        let first = complete
            .first()
            .verified("a complete sample has at least four occurrences");
        let last = complete
            .last()
            .verified("a complete sample has at least four occurrences");
        let span = last
            .duration_since(*first)
            .verified("complete occurrences are ordered by their execution timestamps");
        // One occurrence can finish after its due boundary, followed by one coalesced occurrence;
        // the next boundary can also be arbitrarily close. Each further occurrence requires a
        // whole logical interval. Four complete occurrences must therefore span one interval.
        let intervals = self
            .expected_occurrences
            .checked_sub(3)
            .verified("the constructor requires at least four occurrences");
        let intervals =
            u32::try_from(intervals).assured("the scenario's literal occurrence count fits u32");
        let minimum_span = self
            .logical_cadence
            .checked_mul(intervals)
            .assured("the scenario's bounded cadence and sample count fit a duration");
        assert!(
            span >= minimum_span,
            "generator occurrences formed a catch-up burst instead of following logical cadence: \
             span {span:?}, minimum {minimum_span:?}; {self:?}"
        );
        true
    }
}
