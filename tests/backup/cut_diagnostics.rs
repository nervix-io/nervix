//! Public attribution of the phase that exhausts a backup cut's budget.
//!
//! Layer: test harness.
//! - **Owns.** Checking a CLI timeout's active phase, its elapsed cut budget and its completed
//!   drain measurement.
//! - **Depends on.** The public CLI JSON failure and the shared duration reader.
//! - **Must not know.** The coordinator's progress state or mutation lease representation.

use super::*;

#[then("the CLI capture timeout reports the elapsed completed drain")]
fn then_capture_timeout_reports_completed_drain(world: &mut ScenarioWorld) {
    let report = cli_json(last_cli_output(world));
    let message = report["error"]["message"]
        .as_str()
        .assured("the public refusal carries its diagnostic");
    assert!(message.contains("phase=capture"), "{message}");
    let (_, completed) = message
        .split_once("completed=[")
        .assured("the timeout lists completed phases separately from its active phase");
    let (completed, _) = completed
        .split_once(']')
        .assured("the completed phase list ends");
    let mut drain = None;
    for entry in completed.split(", ") {
        let (phase, elapsed) = entry
            .split_once('=')
            .assured("each completed phase has an elapsed duration");
        let elapsed = parse_duration_text(elapsed).assured("a completed phase reports a duration");
        if phase == "drain" {
            assert!(drain.replace(elapsed).is_none(), "drain is reported once");
        }
    }
    let drain = drain.assured("the cut completed its drain before entering capture");
    assert!(
        !drain.is_zero(),
        "the completed drain has a measured duration"
    );
}

/// A drain that cannot finish is refused at its cut's deadline, which the cut's budget counts from
/// before its lease, so the refusal reports at least the whole budget. A drain that measured its
/// budget afresh once the held pause was applied would report that hold on top of the budget
/// instead. The hold is longer than any poll round a loaded runner takes, so the one round that
/// overshoots the deadline stays inside it.
#[then(
    expr = "the CLI drain timeout reports at least its {int} second budget elapsed and less than \
            {int} seconds beyond it"
)]
fn then_drain_timeout_ends_at_cut_deadline(world: &mut ScenarioWorld, budget: u64, beyond: u64) {
    let report = cli_json(last_cli_output(world));
    let message = report["error"]["message"]
        .as_str()
        .assured("the public refusal carries its diagnostic");
    let (_, elapsed) = message
        .split_once("phase=drain, elapsed=")
        .assured("the timeout attributes the cut's elapsed time to its drain");
    let (elapsed, _) = elapsed
        .split_once(',')
        .assured("the cut's elapsed time precedes the drain's own measurement");
    let elapsed = parse_duration_text(elapsed).assured("the cut reports its elapsed duration");
    let budget = Duration::from_secs(budget);
    let limit = budget
        .checked_add(Duration::from_secs(beyond))
        .assured("a scenario's budget and margin are a few seconds each");
    assert!(
        elapsed >= budget,
        "the drain gave up before its cut's budget ended: {message}"
    );
    assert!(
        elapsed < limit,
        "the drain outlasted its cut's deadline by a budget measured after the pause: {message}"
    );
}
