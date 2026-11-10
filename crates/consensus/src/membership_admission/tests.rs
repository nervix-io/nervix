//! Deterministic learner admission against absent and partially replicated peers.
//!
//! Outside the layer order: a harness.
//! - **Owns.** The short-log first-confirmation regression and the current catch-up lag gate.
//! - **Depends on.** The production catch-up owner, OpenRaft and the consensus store.
//! - **Must not know.** Licensing requests, graph Models or network timing.

use std::{collections::BTreeMap, io, time::Duration};

use nervix_primitives::sync::{StdArc, watch};
use openraft::{
    RaftNetworkFactory,
    entry::RaftEntry as _,
    error::{RPCError, ReplicationClosed, StreamingError, Unreachable},
    network::{RPCOption, RaftNetworkV2},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    type_config::async_runtime::watch::WatchReceiver as _,
};

use crate::{
    ClusterNodeName, CommittedLeaderIdOf, Config, FjallStore, LicensingEnvironment, NervixRaft,
    Node, RaftOperationSettings, SealedSnapshot, Snapshot, TypeConfig, VoteOf,
};

#[derive(Clone)]
enum MembershipPeer {
    Unresponsive,
    FirstEntryOnly,
    FirstEntryAfter(watch::Receiver<bool>),
}

fn unavailable() -> Unreachable<TypeConfig> {
    Unreachable::new(&io::Error::from(io::ErrorKind::ConnectionRefused))
}

impl RaftNetworkFactory<TypeConfig> for MembershipPeer {
    type Network = Self;

    async fn new_client(&mut self, _: ClusterNodeName, _: &Node) -> Self::Network {
        self.clone()
    }
}

impl RaftNetworkV2<TypeConfig> for MembershipPeer {
    type SnapshotData = SealedSnapshot;

    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        match self {
            Self::Unresponsive => return Err(unavailable().into()),
            Self::FirstEntryOnly => {}
            Self::FirstEntryAfter(ready) => {
                if ready.wait_for(|confirmed| *confirmed).await.is_err() {
                    return Err(unavailable().into());
                }
            }
        }
        if let Some(prefix) = request.prev_log_id {
            if prefix.index == 0 {
                return Ok(AppendEntriesResponse::PartialSuccess(Some(prefix)));
            }
        } else if let Some(entry) = request.entries.first() {
            let prefix = entry.log_id();
            if prefix.index == 0 {
                return Ok(AppendEntriesResponse::PartialSuccess(Some(prefix)));
            }
        } else {
            return Ok(AppendEntriesResponse::PartialSuccess(None));
        }
        Ok(AppendEntriesResponse::Conflict)
    }

    async fn vote(
        &mut self,
        _: VoteRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        Err(unavailable().into())
    }

    async fn full_snapshot(
        &mut self,
        _: VoteOf,
        _: Snapshot<CommittedLeaderIdOf<TypeConfig>, ClusterNodeName, Node, SealedSnapshot>,
        _: impl std::future::Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
        _: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        Err(unavailable().into())
    }
}

#[nervix_primitives::test]
async fn an_unconfirmed_learner_never_completes_catch_up_on_a_short_log()
-> Result<(), Box<dyn std::error::Error>> {
    let node = ClusterNodeName::parse("node-1")?;
    let learner = ClusterNodeName::parse("node-2")?;
    let store = FjallStore::from_database(
        crate::tests::temp_database(),
        nervix_execution::Executor::default(),
        RaftOperationSettings::default(),
        LicensingEnvironment::default(),
    )
    .await?;
    let raft = NervixRaft::new(
        node.clone(),
        StdArc::new(Config::default().validate()?),
        MembershipPeer::Unresponsive,
        store.clone(),
        store,
    )
    .await?;
    raft.initialize(BTreeMap::from([(node.clone(), Node::new("127.0.0.1:1"))]))
        .await?;
    raft.wait(Some(Duration::from_secs(10)))
        .metrics(
            |metrics| {
                metrics.current_leader.as_ref() == Some(&node)
                    && metrics.last_applied.as_ref().map(|entry| entry.index)
                        == metrics.last_log_index
            },
            "the sole voter has applied its leadership",
        )
        .await?;
    let admitting_raft = raft.clone();
    let admitting_learner = learner.clone();
    let mut admission = nervix_primitives::task::spawn(async move {
        super::catch_up_learner(&admitting_raft, admitting_learner, "127.0.0.1:2".to_owned()).await
    });
    raft.wait(Some(Duration::from_secs(3)))
        .metrics(
            |metrics| {
                metrics
                    .membership_config
                    .membership()
                    .get_node(&learner)
                    .is_some()
                    && metrics.last_applied.as_ref().map(|entry| entry.index)
                        == metrics.last_log_index
            },
            "the learner registration is applied before checking its confirmation",
        )
        .await?;
    let outcome = nervix_primitives::time::timeout(Duration::from_secs(1), &mut admission).await;
    if outcome.is_err() {
        admission.abort();
        let _ = admission.await;
    }
    let metrics = raft.metrics().borrow_watched().clone();
    raft.shutdown().await?;
    assert!(
        metrics
            .replication
            .as_ref()
            .is_some_and(|peers| { peers.get(&learner).is_some_and(Option::is_none) }),
        "the learner has acknowledged no log position: {metrics:?}"
    );
    assert!(
        outcome.is_err(),
        "a short log cannot turn an unconfirmed learner into a caught-up one: {outcome:?}"
    );
    assert_eq!(
        metrics
            .membership_config
            .membership()
            .voter_ids()
            .collect::<Vec<_>>(),
        vec![node],
        "the unresponsive peer stays outside the voting quorum"
    );
    Ok(())
}

#[nervix_primitives::test]
async fn a_confirmed_prefix_keeps_the_existing_catch_up_lag_allowance()
-> Result<(), Box<dyn std::error::Error>> {
    let node = ClusterNodeName::parse("node-1")?;
    let learner = ClusterNodeName::parse("node-2")?;
    let store = FjallStore::from_database(
        crate::tests::temp_database(),
        nervix_execution::Executor::default(),
        RaftOperationSettings::default(),
        LicensingEnvironment::default(),
    )
    .await?;
    let raft = NervixRaft::new(
        node.clone(),
        StdArc::new(Config::default().validate()?),
        MembershipPeer::FirstEntryOnly,
        store.clone(),
        store,
    )
    .await?;
    raft.initialize(BTreeMap::from([(node.clone(), Node::new("127.0.0.1:1"))]))
        .await?;
    raft.wait(Some(Duration::from_secs(10)))
        .metrics(
            |metrics| {
                metrics.current_leader.as_ref() == Some(&node)
                    && metrics.last_applied.as_ref().map(|entry| entry.index)
                        == metrics.last_log_index
            },
            "the sole voter has applied its leadership",
        )
        .await?;
    let admission = nervix_primitives::time::timeout(
        Duration::from_secs(3),
        super::catch_up_learner(&raft, learner.clone(), "127.0.0.1:2".to_owned()),
    )
    .await;
    let metrics = raft.metrics().borrow_watched().clone();
    raft.shutdown().await?;
    assert!(
        matches!(admission, Ok(Ok(_))),
        "a confirmed prefix within the existing lag allowance completes admission: {admission:?}"
    );
    let peers = metrics.replication.expect("the sole voter still leads");
    let matched = peers
        .get(&learner)
        .expect("the learner has replication state")
        .as_ref()
        .expect("the learner confirmed the first entry");
    assert_eq!(matched.index, 0);
    assert!(
        metrics
            .last_log_index
            .is_some_and(|last| last > matched.index),
        "confirmation does not require zero lag"
    );
    Ok(())
}

#[nervix_primitives::test]
async fn log_growth_while_confirmation_waits_keeps_the_current_lag_gate()
-> Result<(), Box<dyn std::error::Error>> {
    let node = ClusterNodeName::parse("node-1")?;
    let learner = ClusterNodeName::parse("node-2")?;
    let (confirm, ready) = watch::channel(false);
    let store = FjallStore::from_database(
        crate::tests::temp_database(),
        nervix_execution::Executor::default(),
        RaftOperationSettings::default(),
        LicensingEnvironment::default(),
    )
    .await?;
    let config = Config {
        replication_lag_threshold: 4,
        ..Config::default()
    };
    let raft = NervixRaft::new(
        node.clone(),
        StdArc::new(config.validate()?),
        MembershipPeer::FirstEntryAfter(ready),
        store.clone(),
        store,
    )
    .await?;
    raft.initialize(BTreeMap::from([(node.clone(), Node::new("127.0.0.1:1"))]))
        .await?;
    raft.wait(Some(Duration::from_secs(10)))
        .metrics(
            |metrics| {
                metrics.current_leader.as_ref() == Some(&node)
                    && metrics.last_applied.as_ref().map(|entry| entry.index)
                        == metrics.last_log_index
            },
            "the sole voter has applied its leadership",
        )
        .await?;
    let admitting_raft = raft.clone();
    let admitting_learner = learner.clone();
    let mut admission = nervix_primitives::task::spawn(async move {
        super::catch_up_learner(&admitting_raft, admitting_learner, "127.0.0.1:2".to_owned()).await
    });
    raft.wait(Some(Duration::from_secs(3)))
        .metrics(
            |metrics| {
                metrics
                    .membership_config
                    .membership()
                    .get_node(&learner)
                    .is_some()
                    && metrics.last_applied.as_ref().map(|entry| entry.index)
                        == metrics.last_log_index
            },
            "the first learner is registered before its confirmation",
        )
        .await?;
    // Learner registration writes real committed entries without changing the sole voter's quorum.
    for index in 3..8 {
        nervix_primitives::task::consume_budget().await;
        let next = ClusterNodeName::parse(&format!("node-{index}"))?;
        raft.add_learner(next, Node::new(format!("127.0.0.1:{index}")), false)
            .await?;
    }
    confirm.send_replace(true);
    raft.wait(Some(Duration::from_secs(3)))
        .metrics(
            |metrics| {
                if let Some(peers) = metrics.replication.as_ref() {
                    matches!(peers.get(&learner), Some(Some(prefix)) if prefix.index == 0)
                } else {
                    false
                }
            },
            "the first prefix is confirmed after the log grew beyond the allowance",
        )
        .await?;
    let outcome = nervix_primitives::time::timeout(Duration::from_secs(1), &mut admission).await;
    if outcome.is_err() {
        admission.abort();
        let _ = admission.await;
    }
    let metrics = raft.metrics().borrow_watched().clone();
    raft.shutdown().await?;
    assert!(metrics.last_log_index.is_some_and(|last| last > 4));
    assert!(
        outcome.is_err(),
        "a first confirmation must not bypass the current catch-up lag: {outcome:?}"
    );
    Ok(())
}
