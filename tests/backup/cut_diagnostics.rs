//! Public attribution of the phase that exhausts a backup cut's budget.
//!
//! Layer: test harness.
//! - **Owns.** Checking a CLI timeout's active phase and completed drain measurement.
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
