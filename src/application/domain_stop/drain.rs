//! Draining one stopping domain, acting on its requests' clauses, and collecting its report.
//!
//! Layer: control plane.
//! - **Owns.** The rounds of a stop's drain across the cluster, the decision that its work in
//!   motion rests, when each request's clause acts, and the collection of every node's report.
//! - **Depends on.** The committed stopping record, committed licensing time, the stop drain and
//!   report requests, and the domain's schedule.
//! - **Must not know.** How a node classifies or tears down its work.
//!
//! A round asks every node that can hold the domain's work for its view. While any node holds work
//! in motion, each such node releases it with a force-flush generation. Once no node holds any,
//! every node starts the generation that confirms nothing is still moving, and the drain rests
//! only when every node's participants that still take work completed it, no node holds work in
//! motion, and no relay of any node admitted a batch between the confirming view and the last. A
//! node that has not applied the stop yet starts no generation and answers with its view alone, in
//! which each of its ingestors that still admits holds the drain open. Held work never holds the
//! drain open: the stop does not wait for it, and its teardown cuts it.

use std::{collections::BTreeMap, time::Duration};

use error_stack::{Report, ResultExt as _};
use nervix_interconnect::{
    RemoteOperationFailure,
    domain_stop::{
        StopDrainAction, StopDrainRequest, StopDrainStatus, StopReportAnswer, StopReportRequest,
    },
};
use nervix_models::{
    ClusterNodeName, CoordinationIdentity, DomainName, DomainStopReport, DomainStopping,
    NodeStopReport, StopEnding, StopRequestId, Timestamp,
};
use nervix_primitives::time::sleep;
use thiserror::Error;

use super::{CancelledStop, NodeStopWaits, StopWaits};
use crate::application::{
    backup::cut_reach::CutReach, domain_clock::current_timestamp,
    session_service::SessionServiceImpl,
};

#[derive(Debug, Error)]
enum StopDrainError {
    #[error("the stop of domain '{domain}' could not allocate the identity of its drain")]
    Coordination { domain: DomainName },
    #[error("node '{node}' could not report the stopping domain '{domain}'")]
    Request {
        domain: DomainName,
        node: ClusterNodeName,
    },
    #[error("node '{node}' refused to report the stopping domain '{domain}': {failure:?}")]
    Remote {
        domain: DomainName,
        node: ClusterNodeName,
        failure: RemoteOperationFailure,
    },
    #[error("the stop of domain '{domain}' could not commit its ending")]
    End { domain: DomainName },
}

/// What one pass of a stop's drain found.
enum DrainPass {
    /// The drain goes on from this round.
    Continue(DrainRound),
    /// A confirming round found every node at rest, every obligation complete and nothing moved
    /// since the round before it.
    Rested,
}

/// Where a drain stands between two rounds.
enum DrainRound {
    /// Work in motion was visible at the last round, or a confirmation failed.
    Releasing,
    /// No node held work in motion, and every node started the confirming generation; each
    /// node's motion as its confirming view counted it.
    Confirming {
        motion: BTreeMap<ClusterNodeName, u64>,
    },
}

/// What a round found.
struct RoundViews {
    views: BTreeMap<ClusterNodeName, Option<StopDrainStatus>>,
}

impl RoundViews {
    fn waits(&self) -> StopWaits {
        let mut waits = StopWaits::default();
        for (node, view) in &self.views {
            let node_waits = match view {
                Some(view) => NodeStopWaits::Observed(view.entries.clone()),
                None => NodeStopWaits::Unanswered,
            };
            waits.nodes.insert(node.clone(), node_waits);
        }
        waits
    }

    /// Whether every node answered and none holds work in motion.
    fn quiet(&self) -> bool {
        self.views
            .values()
            .all(|view| view.as_ref().is_some_and(|view| !view.moving()))
    }

    fn obligations_complete(&self) -> bool {
        self.views.values().all(|view| {
            view.as_ref()
                .is_some_and(|view| view.flush_obligations == 0)
        })
    }

    fn motion(&self) -> BTreeMap<ClusterNodeName, u64> {
        let mut motion = BTreeMap::new();
        for (node, view) in &self.views {
            if let Some(view) = view {
                motion.insert(node.clone(), view.motion);
            }
        }
        motion
    }
}

impl SessionServiceImpl {
    /// The nodes a stop's drain asks: every node the cluster reports live, this leader, and every
    /// node the schedule assigns the domain's work to, so a scheduled node that stops answering
    /// holds the drain open instead of leaving its work unobserved.
    async fn stop_drain_nodes(&self, domain: &DomainName) -> Vec<ClusterNodeName> {
        let schedule = self.inner.consensus.current_schedule().await;
        let reach = CutReach::of(schedule.domain(domain));
        let mut nodes = self.cut_live_nodes().await;
        nodes.extend(reach.executors().cloned());
        nodes.into_iter().collect()
    }

    async fn stop_drain_view(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
        coordination: &CoordinationIdentity,
        action: StopDrainAction,
    ) -> Result<StopDrainStatus, Report<StopDrainError>> {
        let request = StopDrainRequest {
            coordination: coordination.clone(),
            domain: domain.clone(),
            action,
        };
        let answer = if node == self.inner.consensus.local_node_id() {
            self.handle_stop_drain_request(node, request).await
        } else {
            self.inner
                .interconnect
                .request(node, request)
                .await
                .change_context_lazy(|| StopDrainError::Request {
                    domain: domain.clone(),
                    node: node.clone(),
                })?
        };
        answer.map_err(|failure| {
            Report::new(StopDrainError::Remote {
                domain: domain.clone(),
                node: node.clone(),
                failure,
            })
        })
    }

    async fn stop_drain_round(
        &self,
        domain: &DomainName,
        coordination: &CoordinationIdentity,
        action: StopDrainAction,
    ) -> RoundViews {
        let mut views = BTreeMap::new();
        for node in self.stop_drain_nodes(domain).await {
            nervix_primitives::task::consume_budget().await;
            let view = match self
                .stop_drain_view(&node, domain, coordination, action)
                .await
            {
                Ok(view) => Some(view),
                Err(error) => {
                    tracing::debug!(
                        domain = domain.as_str(),
                        error = %format_args!("{error:#}"),
                        "a node did not answer a stop's drain round"
                    );
                    None
                }
            };
            views.insert(node, view);
        }
        RoundViews { views }
    }

    /// One pass of the drain from `round`: the round it asks for and, after a quiet releasing
    /// round, the confirming round.
    async fn stop_drain_pass(
        &self,
        domain: &DomainName,
        coordination: &CoordinationIdentity,
        round: DrainRound,
    ) -> DrainPass {
        let action = match &round {
            DrainRound::Releasing => StopDrainAction::FlushMovingWork,
            DrainRound::Confirming { .. } => StopDrainAction::Observe,
        };
        let views = self.stop_drain_round(domain, coordination, action).await;
        self.inner.domain_stops.record_waits(domain, views.waits());
        let quiet = views.quiet();
        match round {
            DrainRound::Confirming { motion } if quiet => {
                if !views.obligations_complete() {
                    DrainPass::Continue(DrainRound::Confirming { motion })
                } else if views.motion() == motion {
                    DrainPass::Rested
                } else {
                    DrainPass::Continue(DrainRound::Releasing)
                }
            }
            DrainRound::Releasing if quiet => {
                let confirming = self
                    .stop_drain_round(domain, coordination, StopDrainAction::Confirm)
                    .await;
                if confirming.quiet() {
                    DrainPass::Continue(DrainRound::Confirming {
                        motion: confirming.motion(),
                    })
                } else {
                    DrainPass::Continue(DrainRound::Releasing)
                }
            }
            DrainRound::Confirming { .. } | DrainRound::Releasing => {
                DrainPass::Continue(DrainRound::Releasing)
            }
        }
    }

    /// The committed stopping record of `domain`, while it is still the stop that began at
    /// `began_at_revision`.
    async fn current_stop(
        &self,
        domain: &DomainName,
        began_at_revision: u64,
    ) -> Option<DomainStopping> {
        let state = self.inner.consensus.current_domain(domain).await?;
        let stopping = state.stopping()?;
        (stopping.began_at_revision == began_at_revision).then(|| stopping.clone())
    }

    /// The committed licensing time, against which the licensing request's fixed time runs.
    fn committed_licensing_time(&self) -> Option<Timestamp> {
        let snapshot = self.inner.consensus.observer().licensing_snapshot();
        Some(snapshot.initialized?.record.licensing_time)
    }
}

/// The request whose `ENFORCE` acts by now: an operator's, at `now` in the clock their requests
/// were read with, or the licensing request's, at the committed licensing time. When both are due,
/// the one whose instant came first enforces.
fn due_enforcement(
    stopping: &DomainStopping,
    now: Timestamp,
    licensing_time: Option<Timestamp>,
) -> Option<StopRequestId> {
    let operator = stopping
        .earliest_operator_enforcement()
        .filter(|(at, _)| *at <= now);
    let licensing = match (stopping.licensing_enforcement(), licensing_time) {
        (Some(at), Some(time)) if at <= time => Some(at),
        _ => None,
    };
    match (operator, licensing) {
        (Some((operator_at, id)), Some(licensing_at)) => {
            if licensing_at < operator_at {
                Some(StopRequestId::Licensing)
            } else {
                Some(id)
            }
        }
        (Some((_, id)), None) => Some(id),
        (None, Some(_)) => Some(StopRequestId::Licensing),
        (None, None) => None,
    }
}

/// How long until a request's clause acts, if one is still ahead: an operator's clause at its
/// instant, and the licensing request's fixed time as the physical clock places it. Only
/// committed licensing time decides that the licensing stop is due, so once the physical clock
/// has passed that time the owner waits for the licensing time it commits instead.
fn until_next_clause(stopping: &DomainStopping, now: Timestamp) -> Option<Duration> {
    let operator = stopping.next_operator_limit();
    let licensing = match stopping.licensing_enforcement() {
        Some(at) if at > now => Some(at),
        Some(_) | None => None,
    };
    let next = match (operator, licensing) {
        (Some(operator), Some(licensing)) => operator.min(licensing),
        (Some(next), None) | (None, Some(next)) => next,
        (None, None) => return None,
    };
    let remaining = next.unix_nanos().checked_sub(now.unix_nanos())?;
    let remaining = u64::try_from(remaining.max(0)).ok()?;
    Some(Duration::from_nanos(remaining))
}

/// Drains the stop of `domain` that began at `began_at_revision` until it ends, is called off, or
/// is no longer this leader's.
pub(super) async fn drain_stopping_domain(
    service: SessionServiceImpl,
    domain: DomainName,
    began_at_revision: u64,
) {
    let observer = service.inner.consensus.observer();
    let mut applied = observer.subscribe_applied();
    let coordination = loop {
        nervix_primitives::task::consume_budget().await;
        match service.inner.interconnect.next_coordination_identity() {
            Ok(coordination) => break coordination,
            Err(error) => {
                let error = error.change_context(StopDrainError::Coordination {
                    domain: domain.clone(),
                });
                tracing::warn!(error = %format_args!("{error:#}"), "stop drain waits to begin");
                sleep(service.cluster_parameters().domain_drain_poll_interval()).await;
            }
        }
    };
    let mut round = DrainRound::Releasing;
    loop {
        nervix_primitives::task::consume_budget().await;
        let Some(stopping) = service.current_stop(&domain, began_at_revision).await else {
            return;
        };
        let now = current_timestamp();
        let cancelled = stopping.cancelled_by(now);
        if !cancelled.is_empty() {
            let waits = service
                .inner
                .domain_stops
                .waits(&domain)
                .unwrap_or_default();
            let mut withdrawn = true;
            for reference in cancelled {
                service.inner.domain_stops.record_cancelled(
                    &reference,
                    CancelledStop {
                        domain: domain.clone(),
                        waits: waits.clone(),
                    },
                );
                if let Err(error) = service
                    .inner
                    .consensus
                    .withdraw_domain_stop_request(domain.clone(), reference.clone())
                    .await
                {
                    tracing::warn!(
                        domain = domain.as_str(),
                        request = reference.as_str(),
                        error = %format_args!("{error:#}"),
                        "the CANCEL of a stop request could not be committed; it is retried"
                    );
                    withdrawn = false;
                }
            }
            if !withdrawn {
                sleep(service.cluster_parameters().domain_drain_poll_interval()).await;
            }
            continue;
        }
        if let Some(by) = due_enforcement(&stopping, now, service.committed_licensing_time()) {
            let ended = end_stop(
                &service,
                &domain,
                began_at_revision,
                StopEnding::Enforced { by },
            )
            .await;
            match ended {
                Ok(()) => return,
                Err(error) => {
                    report_unended_stop(&domain, &error);
                    sleep(service.cluster_parameters().domain_drain_poll_interval()).await;
                    continue;
                }
            }
        }
        // A clause acts at its time even while a pass waits on a node that answers late: the pass
        // is abandoned, and the loop reads the stop again and acts on the clause. An abandoned
        // pass proves nothing, so the drain starts over from a releasing round.
        let next_clause = until_next_clause(&stopping, now);
        let pass = Box::pin(service.stop_drain_pass(&domain, &coordination, round));
        let pass = match next_clause {
            Some(until) => nervix_primitives::select! {
                pass = pass => Some(pass),
                () = sleep(until) => None,
            },
            None => Some(pass.await),
        };
        let Some(pass) = pass else {
            round = DrainRound::Releasing;
            continue;
        };
        round = match pass {
            DrainPass::Rested => {
                let ended =
                    end_stop(&service, &domain, began_at_revision, StopEnding::Completed).await;
                match ended {
                    Ok(()) => return,
                    // The owner keeps the stop: it drains again from a releasing round and ends
                    // the stop once a round rests again.
                    Err(error) => {
                        report_unended_stop(&domain, &error);
                        DrainRound::Releasing
                    }
                }
            }
            DrainPass::Continue(next) => next,
        };
        let poll = service.cluster_parameters().domain_drain_poll_interval();
        let wait = match until_next_clause(&stopping, current_timestamp()) {
            Some(until) => poll.min(until),
            None => poll,
        };
        nervix_primitives::select! {
            changed = applied.changed() => if changed.is_err() { return; },
            () = sleep(wait) => {}
        }
    }
}

/// Commits the end of the stop that began at `began_at_revision`. A conflict means the stop
/// changed under the owner, whose next look decides again.
///
/// The owner applies nothing it commits. Every node, this leader among them, tears the stopped
/// domain down through its own application of the committed ending, as it applies every committed
/// change. The owner loop ends this task as soon as the ending is applied, wherever the task then
/// is, so a teardown this task ran could be cut short with it: the node would no longer hold the
/// domain's execution, and no later application of the same state could end its tasks, write its
/// final snapshot or complete its report.
async fn end_stop(
    service: &SessionServiceImpl,
    domain: &DomainName,
    began_at_revision: u64,
    ending: StopEnding,
) -> Result<(), Report<StopDrainError>> {
    service
        .inner
        .consensus
        .end_domain_stop(domain.clone(), began_at_revision, ending)
        .await
        .change_context_lazy(|| StopDrainError::End {
            domain: domain.clone(),
        })
}

/// Reports an ending the owner could not commit. The owner keeps the stop and ends it again, since
/// the owner loop runs a stop's owner anew only when a revision is applied or the topology changes,
/// which a cluster that has nothing else to do may never do.
fn report_unended_stop(domain: &DomainName, error: &Report<StopDrainError>) {
    tracing::warn!(
        domain = domain.as_str(),
        error = %format_args!("{error:#}"),
        "the end of a domain's stop could not be committed; its owner tries again"
    );
}

/// Collects every node's report of the teardown of the stop that ended `domain` at
/// `start_version`, and commits it. A node that left the cluster, or restarted, before its report
/// was collected is reported unreported.
pub(super) async fn collect_stop_report(
    service: SessionServiceImpl,
    domain: DomainName,
    start_version: u64,
) {
    let coordination = loop {
        nervix_primitives::task::consume_budget().await;
        match service.inner.interconnect.next_coordination_identity() {
            Ok(coordination) => break coordination,
            Err(_) => sleep(service.cluster_parameters().domain_drain_poll_interval()).await,
        }
    };
    let mut report = DomainStopReport::default();
    loop {
        nervix_primitives::task::consume_budget().await;
        let live = service.cut_live_nodes().await;
        let nodes = service.stop_drain_nodes(&domain).await;
        let mut pending = false;
        for node in nodes {
            nervix_primitives::task::consume_budget().await;
            if report.nodes.contains_key(&node) {
                continue;
            }
            if !live.contains(&node) {
                report.nodes.insert(node, NodeStopReport::Unreported);
                continue;
            }
            let request = StopReportRequest {
                coordination: coordination.clone(),
                domain: domain.clone(),
                start_version,
            };
            let answer = if &node == service.inner.consensus.local_node_id() {
                service.handle_stop_report_request(&node, request).await
            } else {
                match service.inner.interconnect.request(&node, request).await {
                    Ok(answer) => answer,
                    Err(_) => {
                        pending = true;
                        continue;
                    }
                }
            };
            match answer {
                Ok(StopReportAnswer::Reported(cuts)) => {
                    report.nodes.insert(node, NodeStopReport::Reported { cuts });
                }
                Ok(StopReportAnswer::Unknown) => {
                    report.nodes.insert(node, NodeStopReport::Unreported);
                }
                Ok(StopReportAnswer::Pending) | Err(_) => pending = true,
            }
        }
        if !pending {
            match service
                .inner
                .consensus
                .record_domain_stop_report(domain.clone(), start_version, report.clone())
                .await
            {
                Ok(()) => return,
                Err(error) => tracing::warn!(
                    domain = domain.as_str(),
                    error = %format_args!("{error:#}"),
                    "the report of a domain's stop could not be committed; it is retried"
                ),
            }
        }
        sleep(service.cluster_parameters().domain_drain_poll_interval()).await;
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::time::Duration;

    use nervix_models::{
        CommandExecutionReference, LicensingStopReason, OperatorStopRequest, WaitLimit,
    };

    use super::*;

    fn at(seconds: i64) -> Timestamp {
        Timestamp::from_unix_nanos(seconds * 1_000_000_000)
    }

    fn reference(value: &str) -> CommandExecutionReference {
        CommandExecutionReference::parse(value).expect("the test reference is a valid literal")
    }

    fn stopping(
        operators: &[(&str, i64, Option<WaitLimit>)],
        licensing_expiry: Option<i64>,
    ) -> DomainStopping {
        let mut stopping = DomainStopping {
            began_at_revision: 1,
            operators: std::collections::BTreeMap::new(),
            licensing: licensing_expiry.map(|expiry| LicensingStopReason::EphemeralExpired {
                expired_at: at(expiry),
            }),
        };
        for (name, requested_at, limit) in operators {
            stopping.operators.insert(
                reference(name),
                OperatorStopRequest {
                    requested_at: at(*requested_at),
                    limit: *limit,
                },
            );
        }
        stopping
    }

    fn enforce(seconds: u64) -> Option<WaitLimit> {
        Some(WaitLimit::Enforce {
            after: Duration::from_secs(seconds),
        })
    }

    #[test]
    fn a_plain_stop_is_never_enforced() {
        let plain = stopping(&[("plain", 10, None)], None);
        assert_eq!(
            due_enforcement(&plain, at(1_000_000), Some(at(1_000_000))),
            None
        );
        assert_eq!(until_next_clause(&plain, at(10)), None);
    }

    #[test]
    fn an_operator_enforce_acts_at_its_instant_and_not_before() {
        let enforcing = stopping(&[("enforcing", 10, enforce(30))], None);
        assert_eq!(due_enforcement(&enforcing, at(39), None), None);
        assert_eq!(
            due_enforcement(&enforcing, at(40), None),
            Some(StopRequestId::Operator(reference("enforcing")))
        );
        assert_eq!(
            until_next_clause(&enforcing, at(25)),
            Some(Duration::from_secs(15))
        );
        assert_eq!(
            until_next_clause(&enforcing, at(50)),
            Some(Duration::ZERO),
            "a clause already due is due at once"
        );
    }

    #[test]
    fn the_licensing_stop_is_forced_a_fixed_minute_after_the_expiry_in_licensing_time() {
        let licensing = stopping(&[], Some(100));
        // The physical clock decides nothing: only committed licensing time does.
        assert_eq!(due_enforcement(&licensing, at(1_000), Some(at(159))), None);
        assert_eq!(due_enforcement(&licensing, at(1_000), None), None);
        assert_eq!(
            due_enforcement(&licensing, at(0), Some(at(160))),
            Some(StopRequestId::Licensing)
        );
    }

    #[test]
    fn the_licensing_stops_fixed_time_bounds_the_owners_wait_until_the_physical_clock_passes_it() {
        let licensing = stopping(&[], Some(100));
        assert_eq!(
            until_next_clause(&licensing, at(130)),
            Some(Duration::from_secs(30))
        );
        // Past it, only the licensing time the cluster commits decides, and its commit wakes the
        // owner.
        assert_eq!(until_next_clause(&licensing, at(160)), None);
        let both = stopping(&[("operator", 100, enforce(90))], Some(100));
        assert_eq!(
            until_next_clause(&both, at(120)),
            Some(Duration::from_secs(40)),
            "the licensing stop's fixed time comes before the operator's ENFORCE"
        );
    }

    #[test]
    fn clauses_that_act_at_the_same_instant_end_the_stop_once_by_the_operators_request() {
        let together = stopping(&[("operator", 100, enforce(60))], Some(100));
        assert_eq!(
            due_enforcement(&together, at(160), Some(at(160))),
            Some(StopRequestId::Operator(reference("operator"))),
            "an operator's ENFORCE that acts with the licensing stop's fixed time names the ending"
        );
    }

    /// The leader's owner loop retires a stop's drain task as soon as the ending that task committed
    /// is applied, wherever the task then is. A teardown it had started on that node would be cut
    /// short with it: the node no longer holds the domain's execution, so no later application of
    /// the same committed state ends its tasks, writes its final snapshot or completes its report,
    /// and the stop's `STOP` never answers. The test drives the drain task as that loop does and
    /// retires it at the worst moment it can reach, once this node's teardown holds the domain's
    /// execution, and then lets the node apply the committed ending as its reconciliation does.
    #[cfg(feature = "testing")]
    #[nervix_primitives::test]
    async fn retiring_the_owner_once_its_ending_commits_leaves_the_node_a_whole_teardown() {
        use meticulous::{OptionExt as _, ResultExt as _};
        use nervix_interconnect::domain_stop::StopReportAnswer;
        use nervix_models::{DomainStatus, StopRequest};
        use nervix_recovery::Discarded as _;

        use crate::application::{
            subscription::SessionSubscriptions,
            test_fixtures::{
                TestService, build_test_service, test_command_request, test_execution_reference,
            },
        };

        let TestService {
            service,
            registry,
            path,
        } = build_test_service(true).await;
        let domain =
            DomainName::parse("default").assured("the fixture domain name is a valid literal");
        let mut subscriptions = SessionSubscriptions::new();
        // Two relays and a junction between them: an execution whose tasks the teardown waits for.
        let configured = service
            .test_command(
                test_command_request(
                    "BEGIN; CREATE SCHEMA event ( id I64 ); CREATE RELAY events SCHEMA event \
                     UNBRANCHED; CREATE RELAY forwarded SCHEMA event UNBRANCHED; CREATE JUNCTION \
                     forward FROM events UNBRANCHED TO forwarded INHERIT ALL FLUSH IMMEDIATE ON \
                     MESSAGE ERROR LOG; COMMIT;",
                    "default",
                ),
                &mut subscriptions,
            )
            .await;
        assert!(
            configured.succeeded(),
            "the fixture graph is created: {configured:?}"
        );
        let started = service
            .test_command(
                test_command_request("START;", "default"),
                &mut subscriptions,
            )
            .await;
        assert!(
            started.succeeded(),
            "the fixture domain starts: {started:?}"
        );
        subscriptions.stop_all().await;

        // The stop is requested and its intake stop applied, as a `STOP` does before it waits.
        service
            .inner
            .consensus
            .request_domain_stop(
                domain.clone(),
                StopRequest::Operator {
                    reference: test_execution_reference(),
                    request: OperatorStopRequest {
                        requested_at: current_timestamp(),
                        limit: None,
                    },
                },
                None,
            )
            .await
            .assured("the leader of the fixture's single-node consensus commits the request");
        service
            .apply_current_cluster_state()
            .await
            .assured("the fixture node applies the stopping domain");
        let stopping = service
            .inner
            .consensus
            .current_domain(&domain)
            .await
            .assured("the fixture domain exists");
        let began_at_revision = stopping
            .stopping()
            .verified("the request above began the domain's stop")
            .began_at_revision;
        let start_version = stopping.start_version;

        let mut owner = Box::pin(drain_stopping_domain(
            service.clone(),
            domain.clone(),
            began_at_revision,
        ));
        let retired = nervix_primitives::time::timeout(Duration::from_secs(60), async {
            loop {
                nervix_primitives::task::consume_budget().await;
                if futures_util::poll!(owner.as_mut()).is_ready() {
                    return;
                }
                if service.inner.runtime.stop_teardown_holds_execution(&domain) {
                    return;
                }
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        assert!(
            retired.is_ok(),
            "the drain of a domain with nothing in motion rests and commits its ending"
        );
        drop(owner);
        let ended = service
            .inner
            .consensus
            .current_domain(&domain)
            .await
            .assured("the fixture domain exists");
        assert_eq!(
            ended.status,
            DomainStatus::Stopped,
            "the owner committed the stop's ending before it was retired"
        );

        service
            .apply_current_cluster_state()
            .await
            .assured("the fixture node applies the stopped domain");
        assert_eq!(
            service.inner.runtime.stop_report(&domain, start_version),
            StopReportAnswer::Reported(Vec::new()),
            "the node's teardown ran whole, so it reports the stop, which cut nothing"
        );

        drop(service);
        drop(registry);
        std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
    }

    /// The owner loop runs a stop's owner again only when it wakes for an applied revision or a
    /// topology change, so an owner that gave up once its ending failed to commit would leave the
    /// stop with no owner in a cluster where nothing else changes, and its `STOP` would never
    /// answer. A leader whose lease lapsed under load refuses the ending once, as it refuses every
    /// proposal until its lease recovers; the owner keeps the stop and ends it with the next
    /// attempt, both a completed ending after its drain rested and an enforced one whose `ENFORCE
    /// AFTER` has passed.
    #[cfg(feature = "testing")]
    #[rstest::rstest]
    #[case::completed(None)]
    #[case::enforced(Some(WaitLimit::Enforce { after: Duration::ZERO }))]
    #[nervix_primitives::test]
    async fn an_owner_whose_ending_fails_to_commit_ends_the_stop_again(
        #[case] limit: Option<WaitLimit>,
    ) {
        use meticulous::{OptionExt as _, ResultExt as _};
        use nervix_consensus::ConsensusTestProbe;
        use nervix_models::{DomainStatus, StopRequest};
        use nervix_recovery::Discarded as _;

        use crate::application::{
            subscription::SessionSubscriptions,
            test_fixtures::{
                TestService, build_test_service_with_probe, test_command_request,
                test_execution_reference,
            },
        };

        let probe = ConsensusTestProbe::default();
        let TestService {
            service,
            registry,
            path,
        } = build_test_service_with_probe(true, probe.clone()).await;
        let domain =
            DomainName::parse("default").assured("the fixture domain name is a valid literal");
        let mut subscriptions = SessionSubscriptions::new();
        let started = service
            .test_command(
                test_command_request("START;", "default"),
                &mut subscriptions,
            )
            .await;
        assert!(
            started.succeeded(),
            "the fixture domain starts: {started:?}"
        );
        subscriptions.stop_all().await;
        service
            .inner
            .consensus
            .request_domain_stop(
                domain.clone(),
                StopRequest::Operator {
                    reference: test_execution_reference(),
                    request: OperatorStopRequest {
                        requested_at: current_timestamp(),
                        limit,
                    },
                },
                None,
            )
            .await
            .assured("the leader of the fixture's single-node consensus commits the request");
        service
            .apply_current_cluster_state()
            .await
            .assured("the fixture node applies the stopping domain");
        let began_at_revision = service
            .inner
            .consensus
            .current_domain(&domain)
            .await
            .assured("the fixture domain exists")
            .stopping()
            .verified("the request above began the domain's stop")
            .began_at_revision;

        probe
            .refuse_next_domain_stop_ending_as_lapsed_lease(domain.clone())
            .await;
        let owned = nervix_primitives::time::timeout(
            Duration::from_secs(60),
            drain_stopping_domain(service.clone(), domain.clone(), began_at_revision),
        )
        .await;
        assert!(
            owned.is_ok(),
            "the owner of a domain with nothing in motion ends its stop"
        );
        assert!(
            probe
                .domain_stop_ending_refused_as_lapsed_lease(&domain)
                .await,
            "the owner's first ending met the refusal"
        );
        let ended = service
            .inner
            .consensus
            .current_domain(&domain)
            .await
            .assured("the fixture domain exists");
        assert_eq!(
            ended.status,
            DomainStatus::Stopped,
            "the owner kept the stop until its ending committed"
        );

        drop(service);
        drop(registry);
        std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
    }

    #[test]
    fn the_earliest_enforce_ends_the_stop_for_every_request() {
        let both = stopping(&[("operator", 100, enforce(5))], Some(100));
        assert_eq!(
            due_enforcement(&both, at(200), Some(at(200))),
            Some(StopRequestId::Operator(reference("operator"))),
            "the operator's ENFORCE came due before the licensing stop's fixed time"
        );
        let licensing_first = stopping(&[("operator", 100, enforce(120))], Some(100));
        assert_eq!(
            due_enforcement(&licensing_first, at(300), Some(at(300))),
            Some(StopRequestId::Licensing)
        );
    }
}
