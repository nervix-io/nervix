//! One or three real `nervix-server` processes with separate durable stores and shared membership.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Starting, killing and restarting every persisted voter, signalling, freezing and
//!   restarting one of them, the budget its members have to converge before a scenario sends
//!   commands, and, in a diagnostic build, retaining the members' deadlock evidence and logs,
//!   whether the cluster started or its start failed.
//! - **Depends on.** The single-process fixture, its test certificate authority, bounded public
//!   cluster-status requests, and the convergence its members report.
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
    process_cluster_convergence::ClusterConvergence,
    server_process::{ServerProcess, ServerProcessLaunch, ServerProcessOption, describe_exit},
    status_request::STATUS_REQUEST_TIMEOUT,
};

const NODE_COUNT: usize = 3;
/// The directory, under a diagnostic cluster's root, its members record deadlock evidence in.
#[cfg(feature = "deloxide")]
const DEADLOCK_EVIDENCE_DIRECTORY: &str = "deadlock-evidence";
const CLUSTER_READY_TIMEOUT: Duration = Duration::from_secs(120);

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
        Self::start_sized(NODE_COUNT, options).await
    }

    pub(crate) async fn start_sized(
        node_count: usize,
        options: &[ServerProcessOption],
    ) -> io::Result<Self> {
        let root = tempfile::Builder::new()
            .prefix("nervix-server-process-cluster-")
            .tempdir()?;
        Self::start_in(root, node_count, options).await
    }

    /// Starts a one- or three-node cluster of diagnostic nodes, which a scenario binary built for
    /// the `deloxide` mode spawns. Every member records its deadlock evidence in one directory
    /// under the cluster's root, each in a file of its own.
    #[cfg(feature = "deloxide")]
    pub(crate) async fn start_diagnostic(node_count: usize) -> io::Result<Self> {
        let root = tempfile::Builder::new()
            .prefix("nervix-diagnostic-process-cluster-")
            .tempdir()?;
        let evidence = root.path().join(DEADLOCK_EVIDENCE_DIRECTORY);
        std::fs::create_dir(&evidence)?;
        let option = ServerProcessOption::Written {
            option: "--deadlock-evidence".to_string(),
            value: evidence.display().to_string(),
        };
        Self::start_in(root, node_count, &[option]).await
    }

    /// The directory every member of a cluster [`Self::start_diagnostic`] started records its
    /// deadlock evidence in.
    #[cfg(feature = "deloxide")]
    pub(crate) fn deadlock_evidence(&self) -> nervix_deadlock::EvidenceDirectory {
        nervix_deadlock::EvidenceDirectory::new(self._root.path().join(DEADLOCK_EVIDENCE_DIRECTORY))
    }

    /// Retain evidence and logs before temporary process stores are deleted, including failures.
    #[cfg(feature = "deloxide")]
    pub(crate) fn retain_deadlock_diagnostics(&self) -> io::Result<()> {
        self.retain_deadlock_diagnostics_in_directory()?;
        Ok(())
    }

    /// Copies every member's evidence and log into a directory of its own under the diagnostic
    /// evidence the run keeps, and returns that directory. A cluster that records no evidence, or
    /// a run that keeps none, retains nothing.
    #[cfg(feature = "deloxide")]
    fn retain_deadlock_diagnostics_in_directory(&self) -> io::Result<Option<std::path::PathBuf>> {
        let Some(base) = std::env::var_os(crate::DEADLOCK_EVIDENCE_ENV) else {
            return Ok(None);
        };
        let evidence = self.deadlock_evidence();
        if !evidence.path().is_dir() {
            return Ok(None);
        }
        let base = std::path::PathBuf::from(base).join("process-clusters");
        std::fs::create_dir_all(&base)?;
        let destination = tempfile::Builder::new()
            .prefix("cluster-")
            .tempdir_in(base)?
            .keep();
        for entry in std::fs::read_dir(evidence.path())? {
            let entry = entry?;
            std::fs::copy(entry.path(), destination.join(entry.file_name()))?;
        }
        for (node, process) in &self.nodes {
            std::fs::write(destination.join(format!("{node}.log")), process.log()?)?;
        }
        Ok(Some(destination))
    }

    /// Retains what a cluster whose start failed leaves behind, before its processes are killed
    /// and their stores deleted: every launched member's evidence and whole log, as for a cluster
    /// that started, and the failure. The failure is returned either way, with the retention's own
    /// failure added when the retention fails.
    #[cfg(feature = "deloxide")]
    fn retain_failed_start(&self, failure: io::Error) -> io::Error {
        let retained = self.retain_deadlock_diagnostics_in_directory();
        let written = match retained {
            Ok(Some(destination)) => {
                std::fs::write(destination.join("start-failure.txt"), failure.to_string())
            }
            Ok(None) => Ok(()),
            Err(error) => Err(error),
        };
        match written {
            Ok(()) => failure,
            Err(error) => io::Error::other(format!(
                "{failure}\nthe failed start's diagnostics could not be retained: {error}"
            )),
        }
    }

    /// What one member logged since its current launch.
    #[cfg(feature = "deloxide")]
    pub(crate) fn log(&self, node_id: &str) -> io::Result<String> {
        self.member(node_id)?.log()
    }

    async fn start_in(
        root: TempDir,
        node_count: usize,
        options: &[ServerProcessOption],
    ) -> io::Result<Self> {
        assert!(matches!(node_count, 1 | NODE_COUNT));
        let certificate_authority = InterconnectTestCa::new(&root)?;
        let mut cluster = Self {
            nodes: BTreeMap::new(),
            frozen: BTreeSet::new(),
            _root: root,
        };
        let started = cluster
            .start_members(&certificate_authority, node_count, options)
            .await;
        match started {
            Ok(()) => Ok(cluster),
            #[cfg(feature = "deloxide")]
            Err(failure) => Err(cluster.retain_failed_start(failure)),
            #[cfg(not(feature = "deloxide"))]
            Err(failure) => Err(failure),
        }
    }

    /// Launches the bootstrap member and then each joining member, waiting until each accepts
    /// commands before launching the next, and then until all of them agree on their voters. A
    /// member joins the cluster before its readiness wait, so a failed start still holds it.
    async fn start_members(
        &mut self,
        certificate_authority: &InterconnectTestCa,
        node_count: usize,
        options: &[ServerProcessOption],
    ) -> io::Result<()> {
        let bootstrap_root = tempfile::Builder::new()
            .prefix("node-1-")
            .tempdir_in(self._root.path())?;
        let bootstrap = ServerProcess::start_cluster_member(
            bootstrap_root,
            certificate_authority,
            "node-1",
            None,
            ServerProcessLaunch::Direct,
            options,
        )
        .await?;
        let bootstrap_host = bootstrap.interconnect_endpoint();
        self.nodes.insert("node-1".to_string(), bootstrap);
        self.member_mut("node-1")?.wait_until_ready().await?;
        for index in 2..=node_count {
            nervix_primitives::task::consume_budget().await;
            let node_id = format!("node-{index}");
            let node_root = tempfile::Builder::new()
                .prefix(&format!("{node_id}-"))
                .tempdir_in(self._root.path())?;
            let process = ServerProcess::start_cluster_member(
                node_root,
                certificate_authority,
                &node_id,
                Some(&bootstrap_host),
                ServerProcessLaunch::Direct,
                options,
            )
            .await?;
            self.nodes.insert(node_id.clone(), process);
            self.member_mut(&node_id)?.wait_until_ready().await?;
        }
        self.convergence().wait().await
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
    /// member reports the same leader and all configured voters.
    pub(crate) async fn restart(&mut self, node_id: &str) -> io::Result<()> {
        let convergence = self.convergence();
        let member = self.member_mut(node_id)?;
        convergence.relaunch(node_id, member.restart()).await
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
            nervix_primitives::task::consume_budget().await;
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
            nervix_primitives::task::consume_budget().await;
            process.restart_without_waiting().await?;
        }
        for process in self.nodes.values_mut() {
            nervix_primitives::task::consume_budget().await;
            process.wait_until_ready().await?;
        }
        self.convergence().wait().await
    }

    async fn leader(&self) -> io::Result<&ServerProcess> {
        Ok(self.leader_entry().await?.1)
    }

    /// Asks the members that run, in node order, which node leads, and takes the first answer
    /// that names a running member. A killed or frozen member is neither asked nor taken.
    async fn leader_entry(&self) -> io::Result<(&String, &ServerProcess)> {
        let mut answers = Vec::new();
        for (node_id, process) in &self.nodes {
            nervix_primitives::task::consume_budget().await;
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

    /// What every member must report before a scenario continues, and the budget it has.
    fn convergence(&self) -> ClusterConvergence {
        let mut members = BTreeMap::new();
        for (node_id, process) in &self.nodes {
            members.insert(node_id.clone(), process.status_endpoint());
        }
        ClusterConvergence::new(members, CLUSTER_READY_TIMEOUT)
    }
}
