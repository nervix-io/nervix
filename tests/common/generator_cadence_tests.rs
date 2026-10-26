//! Regressions for observing logical generator cadence independently of subscription read time.
//!
//! Layer: test harness.
//! - **Owns.** Controlled subscription rows, delayed reads and cadence/field/order discriminators.
//! - **Depends on.** The subscription observation owner and primitive test clocks.
//! - **Must not know.** Generator implementation, graph placement or materialized-state internals.

use std::{collections::VecDeque, future, io, time::Duration};

use meticulous::ResultExt as _;
use nervix_models::Timestamp;
use nervix_primitives::time::{Instant, sleep};
use rstest::{fixture, rstest};
use serde_json::{Value, json};

use crate::generator_cadence::{GeneratorCadenceObservation, GeneratorSubscription};

const BUDGET: Duration = Duration::from_secs(30);

struct ScriptedSubscription {
    rows: VecDeque<String>,
    read_delay: Duration,
    read_budgets: Vec<Duration>,
}

impl ScriptedSubscription {
    fn new(rows: VecDeque<String>) -> Self {
        Self {
            rows,
            read_delay: Duration::ZERO,
            read_budgets: Vec::new(),
        }
    }
}

impl GeneratorSubscription for ScriptedSubscription {
    async fn next(&mut self, budget: Duration) -> io::Result<Option<String>> {
        self.read_budgets.push(budget);
        sleep(self.read_delay).await;
        Ok(self.rows.pop_front())
    }
}

struct StalledSubscription;

impl GeneratorSubscription for StalledSubscription {
    async fn next(&mut self, _budget: Duration) -> io::Result<Option<String>> {
        future::pending().await
    }
}

struct FailedSubscription;

impl GeneratorSubscription for FailedSubscription {
    async fn next(&mut self, _budget: Duration) -> io::Result<Option<String>> {
        Err(io::Error::other("disconnected"))
    }
}

#[derive(Clone, Copy)]
struct SourceRow {
    tenant: &'static str,
    user_id: i64,
    amount: i64,
}

const ACME: SourceRow = SourceRow {
    tenant: "acme",
    user_id: 42,
    amount: 7,
};
const BETA: SourceRow = SourceRow {
    tenant: "beta",
    user_id: 84,
    amount: 9,
};

impl SourceRow {
    fn expected(self) -> Value {
        json!({"tenant": self.tenant, "user_id": self.user_id, "amount": self.amount})
    }

    fn payload(self, elapsed_millis: u64) -> String {
        let timestamp = Timestamp::from_unix_nanos(0)
            .checked_add(Duration::from_millis(elapsed_millis))
            .assured("the fixture's small offsets fit signed Unix nanoseconds");
        let key = json!({"tenant": self.tenant});
        let row = json!({
            "tenant": self.tenant, "user_id": self.user_id, "amount": self.amount,
            "generated_at": timestamp.to_rfc3339(),
        });
        format!("key={key} payload={row}")
    }
}

#[fixture]
fn observation() -> GeneratorCadenceObservation {
    GeneratorCadenceObservation::new(
        vec![ACME.expected(), BETA.expected()],
        "tenant".to_string(),
        "generated_at".to_string(),
        Duration::from_millis(200),
        4,
    )
}

fn occurrences(times: [u64; 4]) -> VecDeque<String> {
    let mut rows = VecDeque::new();
    for (index, time) in times.into_iter().enumerate() {
        let sources = if index % 2 == 0 {
            [ACME, BETA]
        } else {
            [BETA, ACME]
        };
        for source in sources {
            rows.push_back(source.payload(time));
        }
    }
    rows
}

#[rstest]
#[nervix_primitives::test(start_paused = true)]
async fn interleaved_rows_preserve_fields_and_share_execution_timestamps(
    mut observation: GeneratorCadenceObservation,
) {
    let mut subscription = ScriptedSubscription::new(occurrences([0, 200, 400, 600]));
    observation.observe(BUDGET, &mut subscription).await;
    assert!(subscription.rows.is_empty());
    assert_eq!(observation.last_payload(), Some(&ACME.payload(600)));
}

#[rstest]
#[nervix_primitives::test(start_paused = true)]
async fn delayed_reads_and_late_branch_readiness_do_not_change_the_cadence_verdict(
    mut observation: GeneratorCadenceObservation,
) {
    let mut rows = VecDeque::new();
    // The first branch can generate while the runner is still establishing the second branch.
    for index in 0..11 {
        rows.push_back(ACME.payload(index * 200));
    }
    // Missing periods may coalesce. Complete occurrences need neither adjacent logical instants
    // nor adjacent delivery, and all field and timestamp assertions still apply to early rows.
    rows.extend(occurrences([4_000, 8_000, 16_000, 32_000]));
    let started = Instant::now();
    let mut subscription = ScriptedSubscription::new(rows);
    subscription.read_delay = Duration::from_secs(1);
    observation.observe(BUDGET, &mut subscription).await;
    assert!(subscription.rows.is_empty());
    assert_eq!(started.elapsed(), Duration::from_secs(19));
    assert_eq!(subscription.read_budgets.len(), 19);
    assert_eq!(subscription.read_budgets.first(), Some(&BUDGET));
    assert_eq!(
        subscription.read_budgets.last(),
        Some(&Duration::from_secs(12))
    );
}

#[rstest]
#[nervix_primitives::test(start_paused = true)]
async fn a_pending_execution_and_coalescing_do_not_require_equal_execution_gaps(
    mut observation: GeneratorCadenceObservation,
) {
    let mut subscription = ScriptedSubscription::new(occurrences([0, 1, 2, 202]));
    observation.observe(BUDGET, &mut subscription).await;
    assert!(subscription.rows.is_empty());
}

#[rstest]
#[case::physical_cadence_at_one_tenth_rate([0, 20, 40, 60])]
#[case::catch_up_burst([0, 1, 2, 3])]
#[nervix_primitives::test(start_paused = true)]
#[should_panic(expected = "catch-up burst")]
async fn occurrences_must_span_the_logical_cadence(
    mut observation: GeneratorCadenceObservation,
    #[case] times: [u64; 4],
) {
    let mut subscription = ScriptedSubscription::new(occurrences(times));
    observation.observe(BUDGET, &mut subscription).await;
}

#[rstest]
#[case::user_id("user_id", json!(420))]
#[case::amount("amount", json!(70))]
#[nervix_primitives::test(start_paused = true)]
#[should_panic(expected = "did not preserve its source fields")]
async fn every_source_field_is_checked(
    mut observation: GeneratorCadenceObservation,
    #[case] field: &str,
    #[case] replacement: Value,
) {
    let mut row = ACME.expected();
    row[field] = replacement;
    row["generated_at"] = json!("1970-01-01T00:00:00Z");
    let payload = format!("key={{\"tenant\":\"acme\"}} payload={row}");
    let mut subscription = ScriptedSubscription::new(VecDeque::from([payload]));
    observation.observe(BUDGET, &mut subscription).await;
}

#[rstest]
#[nervix_primitives::test(start_paused = true)]
#[should_panic(expected = "did not preserve its branch key")]
async fn the_subscription_branch_key_must_match_the_row(
    mut observation: GeneratorCadenceObservation,
) {
    let payload =
        ACME.payload(0)
            .replacen("key={\"tenant\":\"acme\"}", "key={\"tenant\":\"beta\"}", 1);
    let mut subscription = ScriptedSubscription::new(VecDeque::from([payload]));
    observation.observe(BUDGET, &mut subscription).await;
}

#[rstest]
#[case::repeated(200)]
#[case::regressed(100)]
#[nervix_primitives::test(start_paused = true)]
#[should_panic(expected = "did not increase for branch")]
async fn each_branch_has_strictly_ordered_timestamps(
    mut observation: GeneratorCadenceObservation,
    #[case] next_millis: u64,
) {
    let rows = VecDeque::from([
        ACME.payload(200),
        BETA.payload(200),
        ACME.payload(next_millis),
    ]);
    let mut subscription = ScriptedSubscription::new(rows);
    observation.observe(BUDGET, &mut subscription).await;
}

#[rstest]
#[nervix_primitives::test(start_paused = true)]
#[should_panic(expected = "generator observation timed out")]
async fn independently_increasing_branch_timestamps_do_not_form_complete_occurrences(
    mut observation: GeneratorCadenceObservation,
) {
    let mut rows = VecDeque::new();
    for time in [0, 200, 400, 600] {
        rows.push_back(ACME.payload(time));
        rows.push_back(BETA.payload(time + 1));
    }
    let mut subscription = ScriptedSubscription::new(rows);
    observation.observe(BUDGET, &mut subscription).await;
}

#[rstest]
#[nervix_primitives::test(start_paused = true)]
#[should_panic(expected = "generator observation timed out")]
async fn a_stalled_read_still_ends_at_the_liveness_deadline(
    mut observation: GeneratorCadenceObservation,
) {
    observation.observe(BUDGET, &mut StalledSubscription).await;
}

#[rstest]
#[nervix_primitives::test(start_paused = true)]
#[should_panic(expected = "generator subscription failed: disconnected")]
async fn a_subscription_failure_is_retained(mut observation: GeneratorCadenceObservation) {
    observation.observe(BUDGET, &mut FailedSubscription).await;
}

#[rstest]
#[case::missing_key(r#"{"tenant":"acme"}"#)]
#[case::unknown_branch(
    r#"key={"tenant":"gamma"} payload={"tenant":"gamma","generated_at":"1970-01-01T00:00:00Z"}"#
)]
#[case::missing_timestamp(r#"key={"tenant":"acme"} payload={"tenant":"acme"}"#)]
#[case::non_object(r#"key={"tenant":"acme"} payload=[]"#)]
#[case::missing_branch(r#"key={"tenant":"acme"} payload={"amount":7}"#)]
#[nervix_primitives::test(start_paused = true)]
#[should_panic]
async fn invalid_public_rows_fail_the_observation(
    mut observation: GeneratorCadenceObservation,
    #[case] payload: &str,
) {
    let mut subscription = ScriptedSubscription::new(VecDeque::from([payload.to_string()]));
    observation.observe(BUDGET, &mut subscription).await;
}
