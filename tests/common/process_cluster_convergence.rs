//! The convergence a real-process cluster's members report before a scenario sends it commands.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Asking every member for its public cluster status, deciding whether the members
//!   agree on their leader and their voters, and waiting for that within one budget, after a
//!   start or after one member is relaunched.
//! - **Depends on.** Bounded status requests and the phase deadline.
//! - **Must not know.** How a member is launched, signalled or restarted, or how a server decides
//!   leadership.

use std::{collections::BTreeMap, future::Future, io, time::Duration};

use super::{
    phase_deadline::PhaseDeadline,
    status_request::{STATUS_REQUEST_TIMEOUT, StatusEndpoint},
};

const MEMBERSHIP_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The members of one real-process cluster, by node name, and how long they have to converge.
pub(crate) struct ClusterConvergence {
    members: BTreeMap<String, StatusEndpoint>,
    budget: Duration,
}

impl ClusterConvergence {
    pub(crate) fn new(members: BTreeMap<String, StatusEndpoint>, budget: Duration) -> Self {
        Self { members, budget }
    }

    /// Relaunches one member through `relaunch`, then waits until every member reports the same
    /// leader and all configured voters.
    pub(crate) async fn relaunch<F>(&self, _member: &str, relaunch: F) -> io::Result<()>
    where
        F: Future<Output = io::Result<()>>,
    {
        relaunch.await?;
        self.wait().await
    }

    /// Waits until every member reports the same leader and all configured voters.
    pub(crate) async fn wait(&self) -> io::Result<()> {
        let deadline = PhaseDeadline::after(self.budget);
        let mut last_statuses = BTreeMap::new();
        loop {
            nervix_primitives::task::consume_budget().await;
            let mut leader: Option<String> = None;
            let mut ready = true;
            for (node_id, endpoint) in &self.members {
                nervix_primitives::task::consume_budget().await;
                let status = endpoint
                    .cluster_status(deadline.nested(STATUS_REQUEST_TIMEOUT))
                    .await;
                let status = match status {
                    Ok(status) => status,
                    Err(error) => {
                        last_statuses.insert(node_id.clone(), error.to_string());
                        ready = false;
                        continue;
                    }
                };
                let current_leader = status
                    .lines()
                    .find_map(|line| line.strip_prefix("raft.current_leader: "));
                if let Some(current_leader) = current_leader
                    && current_leader != "(none)"
                    && self.members.contains_key(current_leader)
                {
                    match leader.as_deref() {
                        Some(existing) if existing != current_leader => ready = false,
                        None => leader = Some(current_leader.to_string()),
                        _ => {}
                    }
                } else {
                    ready = false;
                }
                for member in self.members.keys() {
                    if !status.contains(&format!("- {member} [voter]")) {
                        ready = false;
                    }
                }
                last_statuses.insert(node_id.clone(), status);
            }
            if ready {
                return Ok(());
            }
            if deadline.has_passed() {
                return Err(io::Error::other(format!(
                    "process cluster did not converge on its voters within {}; last statuses: \
                     {last_statuses:?}",
                    humantime::format_duration(self.budget)
                )));
            }
            deadline.pause(MEMBERSHIP_POLL_INTERVAL).await;
        }
    }
}
