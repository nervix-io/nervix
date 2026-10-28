//! Failed-body fixtures for backup cut gate cleanup.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The deliberate fixture failure and finishing retained public backup work before
//!   its cluster stops, within the cluster's existing cleanup deadline.
//! - **Depends on.** Cucumber, the scenario's background tasks and the harness phase deadline.
//! - **Must not know.** Private backup coordination, leases or archive contents.

use cucumber::{then, when};
use meticulous::ResultExt as _;
use nervix_primitives::task::{self, AbortOnDropHandle};

use super::ScenarioWorld;
use crate::{
    append_cucumber_log_line,
    common::{
        phase_deadline::{BeforeDeadline, PhaseDeadline},
        raw_session::{WireOutcome as _, fresh_execution_reference, outcome_awaits_leader},
    },
    current_leader_node, docstring, expand_placeholders, learn_command_outcome,
};

#[then("the backup cleanup qualification body fails deliberately")]
fn fail_backup_cleanup_body(_world: &mut ScenarioWorld) {
    panic!("backup-cut cleanup qualification: deliberate body failure");
}

#[when("a backup command request begins in the background for cleanup qualification")]
async fn begin_cleanup_backup_request(world: &mut ScenarioWorld, #[step] step: &crate::Step) {
    assert!(world.background_command_result.is_none());
    let query = expand_placeholders(world, docstring(step));
    let leader = current_leader_node(world).await;
    let mut session = world
        .cluster()
        .open_session(&leader, &world.domain)
        .await
        .assured("the cleanup fixture opens its backup session");
    let routes = world.cluster().leader_routes();
    let reference = fresh_execution_reference();
    let scope = session.scope();
    world.background_command_result = Some(AbortOnDropHandle::new(task::spawn(async move {
        let mut outcome = session
            .run_command_result_with_reference(&query, &reference)
            .await?;
        if outcome_awaits_leader(&outcome) {
            let learned = learn_command_outcome(&routes, &scope, &query, &reference, outcome).await;
            outcome = learned.outcome;
        }
        Ok(outcome)
    })));
}

impl ScenarioWorld {
    /// Once gates release, let retained public backup work finish while its peers still serve it.
    /// Node shutdown receives the same deadline, so this never adds another cleanup budget.
    pub(crate) async fn finish_background_backups(&mut self, deadline: PhaseDeadline) {
        let identity = self.scenario_identity();
        if let Some(task) = self.background_plexus.take() {
            match deadline.bound(task).await {
                BeforeDeadline::Finished(Ok(Ok(output))) => {
                    self.last_command_error = None;
                    self.last_command_output = Some(output);
                    append_cucumber_log_line(&format!(
                        "backup cleanup finished: {identity} kind=plexus"
                    ));
                }
                BeforeDeadline::Finished(Ok(Err(error))) => {
                    self.last_command_output = None;
                    self.last_command_error = Some(error);
                    append_cucumber_log_line(&format!(
                        "backup cleanup failed: {identity} kind=plexus command failed"
                    ));
                }
                BeforeDeadline::Finished(Err(error)) => append_cucumber_log_line(&format!(
                    "backup cleanup failed: {identity} kind=plexus task={error}"
                )),
                BeforeDeadline::Passed => append_cucumber_log_line(&format!(
                    "backup cleanup unfinished: {identity} kind=plexus deadline passed"
                )),
            }
        }
        if let Some(task) = self.background_command_result.take() {
            match deadline.bound(task).await {
                BeforeDeadline::Finished(Ok(Ok(outcome))) => {
                    let succeeded = outcome.succeeded();
                    if succeeded {
                        self.last_command_error = None;
                        self.last_command_output = Some(outcome.message);
                    } else {
                        self.last_command_output = None;
                        self.last_command_error = Some(outcome.message);
                    }
                    append_cucumber_log_line(&format!(
                        "backup cleanup finished: {identity} kind=command succeeded={succeeded}"
                    ));
                }
                BeforeDeadline::Finished(Ok(Err(error))) => append_cucumber_log_line(&format!(
                    "backup cleanup unfinished: {identity} kind=command transport={error}"
                )),
                BeforeDeadline::Finished(Err(error)) => append_cucumber_log_line(&format!(
                    "backup cleanup failed: {identity} kind=command task={error}"
                )),
                BeforeDeadline::Passed => append_cucumber_log_line(&format!(
                    "backup cleanup unfinished: {identity} kind=command deadline passed"
                )),
            }
        }
        if let Some(task) = self.background_backup.take() {
            match deadline.bound(task).await {
                BeforeDeadline::Finished(Ok(output)) => {
                    let succeeded = output.status.success();
                    self.last_cli_output = Some(output);
                    append_cucumber_log_line(&format!(
                        "backup cleanup finished: {identity} kind=cli succeeded={succeeded}"
                    ));
                }
                BeforeDeadline::Finished(Err(error)) => append_cucumber_log_line(&format!(
                    "backup cleanup failed: {identity} kind=cli task={error}"
                )),
                BeforeDeadline::Passed => append_cucumber_log_line(&format!(
                    "backup cleanup unfinished: {identity} kind=cli deadline passed"
                )),
            }
        }
    }
}
