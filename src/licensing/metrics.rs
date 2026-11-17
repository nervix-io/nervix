//! Prometheus projection of the node's single licensing publication.
//!
//! Layer: edges.
//! - **Owns.** Licensing gauge/request families and their fixed label sets.
//! - **Depends on.** The published policy, leadership observation and the shared metrics registry.
//! - **Must not know.** Policy evaluation, admission decisions or service orchestration.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns licensing metric exposition")
)]

use meticulous::ResultExt as _;
use nervix_approx_into::ApproxInto as _;
use nervix_consensus::Observer;
use nervix_licensing_protocol::requests::Operation;
use nervix_licensing_vocabulary::QuotaName;
use prometheus::{
    core::{Collector, Desc},
    proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType},
};

use super::{LicensingPolicy, PolicyHandle};
use crate::metrics::RuntimeMetrics;

const STATES: [&str; 8] = [
    "free",
    "licensed_fresh",
    "licensed_due",
    "licensed_overdue",
    "lapsed",
    "stopped",
    "releasing",
    "degraded",
];
const FAMILIES: [(&str, &str, &[&str]); 8] = [
    (
        "nervix_licensing_state",
        "Current published licensing state.",
        &["state"],
    ),
    (
        "nervix_licensing_decision_revision",
        "Committed licensing decision revision, absent in free mode.",
        &[],
    ),
    (
        "nervix_licensing_deadline_timestamp_seconds",
        "Existing committed licensing deadlines as Unix seconds.",
        &["deadline"],
    ),
    (
        "nervix_licensing_licensing_time_seconds",
        "Committed licensing time as Unix seconds.",
        &[],
    ),
    (
        "nervix_licensing_requests_total",
        "Licensing requests by operation and outcome on the leader.",
        &["operation", "outcome"],
    ),
    (
        "nervix_licensing_stream_connected",
        "Whether the leader holds a licensing event stream.",
        &[],
    ),
    (
        "nervix_licensing_transitions_active",
        "Committed licensing transitions with a future deadline.",
        &[],
    ),
    (
        "nervix_licensing_over_limit",
        "Whether committed usage exceeds its effective quota after grace.",
        &["condition"],
    ),
];

struct LicensingCollector {
    policy: PolicyHandle,
    observer: Observer,
    descs: Vec<Desc>,
}

pub(crate) fn register(metrics: &RuntimeMetrics, policy: PolicyHandle, observer: Observer) {
    let descs = FAMILIES
        .iter()
        .map(|(name, help, labels)| {
            Desc::new(
                (*name).to_string(),
                (*help).to_string(),
                labels.iter().map(|label| (*label).to_string()).collect(),
                // Prometheus fixes this argument's map type; the descriptor has no constant labels.
                Default::default(),
            )
            .assured("licensing family and label declarations satisfy Prometheus rules")
        })
        .collect();
    metrics
        .register_collector(Box::new(LicensingCollector {
            policy,
            observer,
            descs,
        }))
        .assured("licensing families register once per node");
}

impl Collector for LicensingCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }
    fn collect(&self) -> Vec<MetricFamily> {
        let current = self.policy.current();
        let leader =
            self.observer.current_leader_now().as_ref() == Some(self.observer.local_node_id());
        families(&current, leader, Some(&self.policy))
    }
}

fn family(index: usize, kind: MetricType) -> MetricFamily {
    let (name, help, _) = FAMILIES[index];
    let mut family = MetricFamily::default();
    family.set_name(name.to_string());
    family.set_help(help.to_string());
    family.set_field_type(kind);
    family
}

fn sample(family: &mut MetricFamily, labels: &[(&str, &str)], value: f64) {
    let mut metric = Metric::default();
    let mut pairs = Vec::with_capacity(labels.len());
    for (name, value) in labels {
        let mut label = LabelPair::default();
        label.set_name((*name).to_string());
        label.set_value((*value).to_string());
        pairs.push(label);
    }
    metric.set_label(pairs);
    if family.get_field_type() == MetricType::COUNTER {
        let mut counter = Counter::default();
        counter.set_value(value);
        metric.set_counter(counter);
    } else {
        let mut gauge = Gauge::default();
        gauge.set_value(value);
        metric.set_gauge(gauge);
    }
    family.mut_metric().push(metric);
}

fn families(
    policy: &LicensingPolicy,
    leader: bool,
    observations: Option<&PolicyHandle>,
) -> Vec<MetricFamily> {
    let mut output = Vec::with_capacity(FAMILIES.len());
    let mut state = family(0, MetricType::GAUGE);
    for name in STATES {
        sample(
            &mut state,
            &[("state", name)],
            if name == policy.state_name() {
                1.0
            } else {
                0.0
            },
        );
    }
    output.push(state);
    let mut revision = family(1, MetricType::GAUGE);
    let mut deadlines = family(2, MetricType::GAUGE);
    if let Some(initialized) = &policy.snapshot.initialized {
        if let nervix_consensus::LicensingMode::Activated(activation) = &initialized.record.mode {
            sample(
                &mut revision,
                &[],
                activation.decision.revision.approx_into(),
            );
        }
        if let Some(Ok(verified)) = &initialized.verification {
            let timing = &verified.decision.timing;
            for (name, at) in [
                ("refresh_after", Some(timing.refresh_after)),
                ("overdue_at", Some(timing.overdue_at)),
                ("valid_until", timing.valid_until),
                ("term_ends_at", Some(timing.term_ends_at)),
            ] {
                if let Some(at) = at {
                    let seconds: f64 = at.timestamp().approx_into();
                    sample(&mut deadlines, &[("deadline", name)], seconds);
                }
            }
        }
        if let nervix_consensus::LicensingMode::Activated(activation) = &initialized.record.mode
            && let Some(expiry) = activation.binding.ephemeral_expires_at
        {
            let nanos: f64 = expiry.unix_nanos().approx_into();
            sample(
                &mut deadlines,
                &[("deadline", "ephemeral_expires_at")],
                nanos / 1_000_000_000.0,
            );
        }
        if let nervix_consensus::LicensingMode::Released(release) = &initialized.record.mode {
            sample(
                &mut revision,
                &[],
                release.activation.decision.revision.approx_into(),
            );
        }
    }
    if let Some(initialized) = &policy.snapshot.initialized
        && let Some(next) = policy
            .transitions()
            .iter()
            .map(|(_, until, _)| *until)
            .filter(|at| *at > initialized.record.licensing_time)
            .min()
    {
        let seconds: f64 = next.into_datetime().timestamp().approx_into();
        sample(&mut deadlines, &[("deadline", "next_grant")], seconds);
    }
    output.push(revision);
    output.push(deadlines);
    let mut time = family(3, MetricType::GAUGE);
    if let Some(initialized) = &policy.snapshot.initialized {
        let nanos: f64 = initialized.record.licensing_time.unix_nanos().approx_into();
        sample(&mut time, &[], nanos / 1_000_000_000.0);
    }
    output.push(time);
    if leader {
        let mut requests = family(4, MetricType::COUNTER);
        for operation in Operation::ALL {
            for outcome in super::RequestOutcome::ALL {
                sample(
                    &mut requests,
                    &[
                        ("operation", operation.as_str()),
                        ("outcome", outcome.as_str()),
                    ],
                    match observations {
                        Some(handle) => handle.request_count(*operation, outcome).approx_into(),
                        None => 0.0,
                    },
                );
            }
        }
        output.push(requests);
        let mut connected = family(5, MetricType::GAUGE);
        let value = match observations {
            Some(handle) if handle.stream_connected() => 1.0,
            _ => 0.0,
        };
        sample(&mut connected, &[], value);
        output.push(connected);
    }
    let mut transitions = family(6, MetricType::GAUGE);
    let active_transitions = policy.active_transition_count();
    sample(&mut transitions, &[], active_transitions.approx_into());
    output.push(transitions);
    let mut over = family(7, MetricType::GAUGE);
    for name in QuotaName::ALL {
        let used = match name {
            QuotaName::ClusterNodes => policy.snapshot.usage.cluster_nodes,
            QuotaName::ClusterDomains => policy.snapshot.usage.cluster_domains,
        };
        sample(
            &mut over,
            &[("condition", name.as_str())],
            if policy.effective_quota(name).admits(used) {
                0.0
            } else {
                1.0
            },
        );
    }
    output.push(over);
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_follower_omits_connectivity_and_uninitialized_values() {
        let policy =
            crate::licensing::policy::evaluate_policy(nervix_consensus::LicensingSnapshot {
                initialized: None,
                revision: None,
                usage: nervix_licensing_protocol::impact::UsageSnapshot {
                    cluster_nodes: 0,
                    cluster_domains: 0,
                },
            });
        let follower = families(&policy, false, None);
        for name in [
            "nervix_licensing_requests_total",
            "nervix_licensing_stream_connected",
        ] {
            assert!(!follower.iter().any(|family| family.name() == name));
            assert!(
                families(&policy, true, None)
                    .iter()
                    .any(|family| family.name() == name && !family.get_metric().is_empty())
            );
        }
        for name in [
            "nervix_licensing_decision_revision",
            "nervix_licensing_deadline_timestamp_seconds",
            "nervix_licensing_licensing_time_seconds",
        ] {
            assert!(
                follower
                    .iter()
                    .any(|family| family.name() == name && family.get_metric().is_empty())
            );
        }
    }
}
