//! Three real `nervix-server` processes with separate durable stores and shared membership.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Starting, killing and restarting every persisted voter, signalling, freezing and
//!   restarting one of them, and observable membership and leader readiness before a scenario
//!   sends commands.
//! - **Depends on.** The single-process fixture, its test certificate authority, and bounded
//!   public cluster-status requests.
//! - **Must not know.** Server state beyond the command and status surfaces each process exposes.
//!
//! A member stopped by SIGSTOP keeps its sockets open and answers nothing, so the cluster asks
//! only the members it has not frozen which node leads.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    os::unix::process::ExitStatusExt as _,
    process::ExitStatus,
    time::Duration,
};

use nix::sys::signal::Signal;
use tempfile::TempDir;

use super::{
    cluster::{InterconnectTestCa, TestSession},
    phase_deadline::PhaseDeadline,
    server_process::{ServerProcess, ServerProcessLaunch, ServerProcessOption, describe_exit},
    status_request::STATUS_REQUEST_TIMEOUT,
};

const NODE_COUNT: usize = 3;
const CLUSTER_READY_TIMEOUT: Duration = Duration::from_secs(120);
const MEMBERSHIP_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// A whole real-process cluster. Each member retains its ports and database across a restart.
#[derive(Debug)]
pub(crate) struct ServerProcessCluster {
    nodes: BTreeMap<String, ServerProcess>,
    /// Members stopped by SIGSTOP and not continued since.
    frozen: BTreeSet<String>,
    _root: TempDir,
}

impl ServerProcessCluster {
    pub(crate) async fn start(options: &[ServerProcessOption]) -> io::Result<Self> {
        let root = tempfile::Builder::new()
            .prefix("nervix-server-process-cluster-")
            .tempdir()?;
        let certificate_authority = InterconnectTestCa::new(&root)?;
        let bootstrap_root = tempfile::Builder::new()
            .prefix("node-1-")
            .tempdir_in(root.path())?;
        let mut bootstrap = ServerProcess::start_cluster_member(
            bootstrap_root,
            &certificate_authority,
            "node-1",
            None,
            ServerProcessLaunch::Direct,
            options,
        )?;
        bootstrap.wait_until_ready().await?;
        let bootstrap_host = bootstrap.interconnect_endpoint();
        let mut nodes = BTreeMap::new();
        nodes.insert("node-1".to_string(), bootstrap);
        for index in 2..=NODE_COUNT {
            tokio::task::consume_budget().await;
            let node_id = format!("node-{index}");
            let node_root = tempfile::Builder::new()
                .prefix(&format!("{node_id}-"))
                .tempdir_in(root.path())?;
            let mut process = ServerProcess::start_cluster_member(
                node_root,
                &certificate_authority,
                &node_id,
                Some(&bootstrap_host),
                ServerProcessLaunch::Direct,
                options,
            )?;
            process.wait_until_ready().await?;
            nodes.insert(node_id, process);
        }
        let cluster = Self {
            nodes,
            frozen: BTreeSet::new(),
            _root: root,
        };
        cluster.wait_for_voters().await?;
        Ok(cluster)
    }

    pub(crate) fn node_ids(&self) -> Vec<String> {
        self.nodes.keys().cloned().collect()
    }

    pub(crate) fn grpc_uri(&self, node_id: &str) -> io::Result<String> {
        Ok(self.member(node_id)?.grpc_uri())
    }

    /// Delivers `signal` to one member. SIGSTOP freezes it with its sockets open, so peers hear
    /// nothing from it and see no reset, until SIGCONT lets it run again.
    pub(crate) fn signal(&mut self, node_id: &str, signal: Signal) -> io::Result<()> {
        self.member(node_id)?.send_signal(signal)?;
        match signal {
            Signal::SIGSTOP => {
                self.frozen.insert(node_id.to_string());
            }
            Signal::SIGCONT => {
                self.frozen.remove(node_id);
            }
            _ => {}
        }
        Ok(())
    }

    /// Waits until one member's process exits, and says how it did.
    pub(crate) async fn wait_for_exit(&mut self, node_id: &str) -> io::Result<ExitStatus> {
        self.member_mut(node_id)?.wait_for_exit().await
    }

    /// Starts an exited member again from its own database and ports, then waits until every
    /// member reports the same leader and all three voters.
    pub(crate) async fn restart(&mut self, node_id: &str) -> io::Result<()> {
        self.member_mut(node_id)?.restart().await?;
        self.wait_for_voters().await
    }

    /// The node the running members report as leader.
    pub(crate) async fn leader_id(&self) -> io::Result<String> {
        Ok(self.leader_entry().await?.0.clone())
    }

    fn member(&self, node_id: &str) -> io::Result<&ServerProcess> {
        self.nodes.get(node_id).ok_or_else(|| {
            io::Error::other(format!("the process cluster has no member {node_id:?}"))
        })
    }

    fn member_mut(&mut self, node_id: &str) -> io::Result<&mut ServerProcess> {
        self.nodes.get_mut(node_id).ok_or_else(|| {
            io::Error::other(format!("the process cluster has no member {node_id:?}"))
        })
    }

    pub(crate) async fn run_commands(&self, domain: &str, commands: &str) -> io::Result<String> {
        self.leader().await?.run_commands(domain, commands).await
    }

    pub(crate) async fn open_session(&self, domain: &str) -> io::Result<TestSession> {
        self.leader().await?.open_session(domain).await
    }

    /// Delivers SIGKILL to every voter before waiting on any child. An exiting leader therefore
    /// cannot hand its work to a survivor that the scenario has not yet killed.
    pub(crate) async fn kill_all(&mut self) -> io::Result<()> {
        for process in self.nodes.values() {
            process.send_signal(Signal::SIGKILL)?;
        }
        for (node_id, process) in &mut self.nodes {
            tokio::task::consume_budget().await;
            let status = process.wait_for_exit().await?;
            if status.signal() != Some(nix::libc::SIGKILL) {
                return Err(io::Error::other(format!(
                    "{node_id} exited with {}, not SIGKILL",
                    describe_exit(status)
                )));
            }
        }
        Ok(())
    }

    /// Starts all persisted voters before awaiting readiness, then observes their common leader
    /// and voter set through each process's public status endpoint.
    pub(crate) async fn restart_all(&mut self) -> io::Result<()> {
        for process in self.nodes.values_mut() {
            process.restart_without_waiting()?;
        }
        for process in self.nodes.values_mut() {
            tokio::task::consume_budget().await;
            process.wait_until_ready().await?;
        }
        self.wait_for_voters().await
    }

    async fn leader(&self) -> io::Result<&ServerProcess> {
        Ok(self.leader_entry().await?.1)
    }

    /// Asks the members that run, in node order, which node leads, and takes the first answer
    /// that names a running member. A killed or frozen member is neither asked nor taken.
    async fn leader_entry(&self) -> io::Result<(&String, &ServerProcess)> {
        let mut answers = Vec::new();
        for (node_id, process) in &self.nodes {
            tokio::task::consume_budget().await;
            if process.has_exited() || self.frozen.contains(node_id) {
                continue;
            }
            let status = process
                .status_endpoint()
                .cluster_status(PhaseDeadline::after(STATUS_REQUEST_TIMEOUT))
                .await;
            let status = match status {
                Ok(status) => status,
                Err(error) => {
                    answers.push(format!("{node_id}: {error}"));
                    continue;
                }
            };
            let leader_id = status
                .lines()
                .find_map(|line| line.strip_prefix("raft.current_leader: "));
            let Some(leader_id) = leader_id else {
                answers.push(format!("{node_id}: the status omitted its current leader"));
                continue;
            };
            if let Some((leader_id, leader)) = self.nodes.get_key_value(leader_id)
                && !leader.has_exited()
                && !self.frozen.contains(leader_id)
            {
                return Ok((leader_id, leader));
            }
            answers.push(format!("{node_id}: named leader {leader_id:?}"));
        }
        Err(io::Error::other(format!(
            "no running member named a running member as leader: {answers:?}"
        )))
    }

    async fn wait_for_voters(&self) -> io::Result<()> {
        let deadline = PhaseDeadline::after(CLUSTER_READY_TIMEOUT);
        let mut last_statuses = BTreeMap::new();
        loop {
            tokio::task::consume_budget().await;
            let mut leader: Option<String> = None;
            let mut ready = true;
            for (node_id, process) in &self.nodes {
                tokio::task::consume_budget().await;
                let status = process
                    .status_endpoint()
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
                    && self.nodes.contains_key(current_leader)
                {
                    match leader.as_deref() {
                        Some(existing) if existing != current_leader => ready = false,
                        None => leader = Some(current_leader.to_string()),
                        _ => {}
                    }
                } else {
                    ready = false;
                }
                for index in 1..=NODE_COUNT {
                    if !status.contains(&format!("- node-{index} [voter]")) {
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
                    "process cluster did not converge on three voters within {}; last statuses: \
                     {last_statuses:?}",
                    humantime::format_duration(CLUSTER_READY_TIMEOUT)
                )));
            }
            deadline.pause(MEMBERSHIP_POLL_INTERVAL).await;
        }
    }
}
