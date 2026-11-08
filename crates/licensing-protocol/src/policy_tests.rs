//! Deterministic condition-resolution and incremental-grant evidence.

use meticulous::{OptionExt as _, ResultExt as _};

use crate::{conditions::*, documents::*, impact::*, test_fixture::*};

pub fn usage(nodes: Quota, domains: Quota) -> UsageConditions {
    let mut usage = FREE_POLICY.usage();
    usage
        .quotas
        .insert(CLUSTER_NODES.to_owned(), ConditionValue::Known(nodes));
    usage
        .quotas
        .insert(CLUSTER_DOMAINS.to_owned(), ConditionValue::Known(domains));
    usage
}

pub fn context(from: u64, to: u64, started: i64) -> ImpactContext {
    ImpactContext {
        from_conditions_revision: from,
        to_conditions_revision: to,
        started_at: at(started),
        trigger: TransitionTrigger::Decision {
            decision_id: id(100),
        },
        reason: TransitionReason::PlanChanged,
    }
}

pub fn policy() -> TransitionPolicy {
    TransitionPolicy {
        quota_reduction_grace_seconds: 100,
        capability_removal_grace_seconds: 50,
    }
}

#[test]
fn resolution_overrides_every_map_and_derives_status_without_a_clock() {
    let mut plan = plan();
    plan.usage = usage(Quota::Bounded { value: 10 }, Quota::Bounded { value: 4 });
    let mut overrides = empty_conditions();
    overrides.usage.quotas.insert(
        CLUSTER_NODES.to_owned(),
        ConditionValue::Known(Quota::Bounded { value: 0 }),
    );
    overrides.operating.quotas.insert(
        PERSISTENT_ACTIVATIONS.to_owned(),
        ConditionValue::Known(Quota::Unlimited),
    );
    let active = plan
        .resolve(&overrides, resolution_facts(), limits().bytes)
        .assured("complete plan and partial overrides");
    assert_eq!(active.status(), DecisionStatus::Active);
    assert_eq!(
        active
            .values()
            .usage
            .quota(CLUSTER_NODES)
            .assured("resolved nodes"),
        Quota::Bounded { value: 0 }
    );
    assert_eq!(
        active.values().operating.quotas[PERSISTENT_ACTIVATIONS],
        ConditionValue::Known(Quota::Unlimited)
    );
    assert_eq!(
        active.conditions_digest(),
        &active
            .values()
            .digest(limits().bytes)
            .assured("same canonical digest")
    );
    let mut facts = resolution_facts();
    for (activation_status, expected) in [
        (ActivationStatus::Released, DecisionStatus::Released),
        (ActivationStatus::Revoked, DecisionStatus::Revoked),
        (ActivationStatus::Transferred, DecisionStatus::Transferred),
    ] {
        facts.activation_status = activation_status;
        let resolved = plan
            .resolve(&overrides, facts, limits().bytes)
            .assured("terminal activation has free usage");
        assert_eq!(resolved.status(), expected);
        assert_eq!(resolved.values().usage, FREE_POLICY.usage());
        assert_eq!(resolved.values().operating, active.values().operating);
    }
    facts = resolution_facts();
    for (license_status, expected) in [
        (LicenseStatus::Suspended, DecisionStatus::Suspended),
        (LicenseStatus::Revoked, DecisionStatus::Revoked),
    ] {
        facts.license_status = license_status;
        assert_eq!(
            plan.resolve(&overrides, facts, limits().bytes)
                .assured("nonactive license")
                .status(),
            expected
        );
    }
    facts = resolution_facts();
    facts.at = facts.term_ends_at;
    assert_eq!(
        plan.resolve(&overrides, facts, limits().bytes)
            .assured("term boundary")
            .status(),
        DecisionStatus::TermEnded
    );
    facts.lifetime = ActivationLifetime::Ephemeral {
        expires_at: facts.at,
    };
    let expired = plan
        .resolve(&overrides, facts, limits().bytes)
        .assured("ephemeral stop takes precedence");
    assert_eq!(expired.status(), DecisionStatus::Expired);
    assert!(expired.values().usage.quotas.is_empty());
    assert!(expired.values().usage.capabilities.is_empty());
}

#[test]
fn resolver_refuses_missing_unknown_misplaced_and_inconsistent_values() {
    let plan = plan();
    let mut overrides = empty_conditions();
    overrides.operating.durations.insert(
        OFFLINE_ALLOWANCE.to_owned(),
        ConditionValue::Known(ConditionDuration::Bounded { seconds: 179 }),
    );
    assert!(
        plan.resolve(&overrides, resolution_facts(), limits().bytes)
            .is_err()
    );
    overrides.operating.durations.insert(
        OFFLINE_ALLOWANCE.to_owned(),
        ConditionValue::Known(ConditionDuration::Bounded { seconds: 180 }),
    );
    assert!(
        plan.resolve(&overrides, resolution_facts(), limits().bytes)
            .is_ok()
    );
    overrides.usage.quotas.insert(
        "unknown.quota".to_owned(),
        ConditionValue::Known(Quota::Unlimited),
    );
    assert!(
        plan.resolve(&overrides, resolution_facts(), limits().bytes)
            .is_err()
    );
    let mut incomplete = plan.clone();
    incomplete.operating.sets.clear();
    assert!(
        incomplete
            .resolve(&empty_conditions(), resolution_facts(), limits().bytes)
            .is_err()
    );
    let mut misplaced = plan.clone();
    misplaced.usage.quotas.insert(
        PERSISTENT_ACTIVATIONS.to_owned(),
        ConditionValue::Known(Quota::Unlimited),
    );
    assert!(Catalog::validate(&misplaced, true, false, false).is_err());
    let mut facts = resolution_facts();
    facts.at = at(-4_000);
    assert!(
        plan.resolve(&empty_conditions(), facts, limits().bytes)
            .is_err()
    );
}

#[test]
fn further_reductions_protect_only_new_amounts_and_keep_each_deadline() {
    let free = FREE_POLICY.usage();
    let four = usage(Quota::Bounded { value: 4 }, Quota::Unlimited);
    let three = usage(Quota::Bounded { value: 3 }, Quota::Unlimited);
    let counts = UsageSnapshot {
        cluster_nodes: 7,
        cluster_domains: 0,
    };
    let first = assess_impact(
        EffectiveUsage {
            conditions: &free,
            transitions: &[],
        },
        ImpactTarget::Usage(&four),
        counts,
        policy(),
        context(1, 2, 0),
        limits().bytes,
    )
    .assured("first reduction");
    assert_eq!(first.len(), 1);
    assert_eq!(
        first[0].affected[0]
            .additional_allowance()
            .assured("first grant amount"),
        3
    );
    let second = assess_impact(
        EffectiveUsage {
            conditions: &four,
            transitions: &first,
        },
        ImpactTarget::Usage(&three),
        counts,
        policy(),
        context(2, 3, 10),
        limits().bytes,
    )
    .assured("further reduction");
    assert_eq!(
        second[0].affected[0]
            .additional_allowance()
            .assured("only newly affected amount"),
        1
    );
    assert_eq!(first[0].deadline, at(100));
    assert_eq!(second[0].deadline, at(110));
    let grants = [first[0].clone(), second[0].clone()];
    let current = EffectiveUsage {
        conditions: &three,
        transitions: &grants,
    };
    assert_eq!(
        current
            .quota(CLUSTER_NODES, at(99))
            .assured("both grants active"),
        Quota::Bounded { value: 7 }
    );
    assert_eq!(
        current
            .quota(CLUSTER_NODES, at(100))
            .assured("earlier amount expired"),
        Quota::Bounded { value: 4 }
    );
    assert_eq!(
        current
            .quota(CLUSTER_NODES, at(110))
            .assured("all grace expired"),
        Quota::Bounded { value: 3 }
    );
    assert!(
        assess_impact(
            current,
            ImpactTarget::Usage(&three),
            counts,
            policy(),
            context(3, 3, 20),
            limits().bytes
        )
        .assured("refresh cannot restart grace")
        .is_empty()
    );
}

#[test]
fn improvements_safe_reductions_expiry_and_deadline_overflow_are_explicit() {
    let free = FREE_POLICY.usage();
    let bounded = usage(Quota::Bounded { value: 2 }, Quota::Bounded { value: 3 });
    let counts = UsageSnapshot {
        cluster_nodes: 2,
        cluster_domains: 3,
    };
    assert!(
        assess_impact(
            EffectiveUsage {
                conditions: &free,
                transitions: &[]
            },
            ImpactTarget::Usage(&bounded),
            counts,
            policy(),
            context(1, 2, 0),
            limits().bytes
        )
        .assured("usage fits")
        .is_empty()
    );
    assert!(
        assess_impact(
            EffectiveUsage {
                conditions: &bounded,
                transitions: &[]
            },
            ImpactTarget::Usage(&free),
            counts,
            policy(),
            context(1, 2, 0),
            limits().bytes
        )
        .assured("improvement")
        .is_empty()
    );
    assert!(
        assess_impact(
            EffectiveUsage {
                conditions: &free,
                transitions: &[]
            },
            ImpactTarget::Expired,
            counts,
            policy(),
            context(1, 2, 0),
            limits().bytes
        )
        .assured("expiry is firm")
        .is_empty()
    );
    let mut overflow = policy();
    overflow.quota_reduction_grace_seconds = u64::MAX;
    assert!(
        assess_impact(
            EffectiveUsage {
                conditions: &free,
                transitions: &[]
            },
            ImpactTarget::Usage(&bounded),
            UsageSnapshot {
                cluster_nodes: 3,
                cluster_domains: 4
            },
            overflow,
            context(1, 2, 0),
            limits().bytes
        )
        .is_err()
    );
    let transition = assess_impact(
        EffectiveUsage {
            conditions: &free,
            transitions: &[],
        },
        ImpactTarget::Usage(&bounded),
        UsageSnapshot {
            cluster_nodes: 3,
            cluster_domains: 4,
        },
        policy(),
        context(1, 2, 0),
        limits().bytes,
    )
    .assured("both affected quotas share one deadline")
    .pop()
    .assured("one quota transition");
    assert_eq!(transition.affected.len(), 2);
    round_trip(&transition);
}

#[test]
fn transition_policy_comes_from_complete_bounded_signed_operating_values() {
    let mut operating = plan().operating;
    let policy = TransitionPolicy::from_operating(&operating).assured("signed bounded durations");
    assert_eq!(policy.quota_reduction_grace_seconds, 900);
    assert_eq!(policy.capability_removal_grace_seconds, 300);
    operating.durations.insert(
        QUOTA_GRACE.to_owned(),
        ConditionValue::Known(ConditionDuration::Unbounded),
    );
    assert!(TransitionPolicy::from_operating(&operating).is_err());
    operating.durations.remove(QUOTA_GRACE);
    assert!(TransitionPolicy::from_operating(&operating).is_err());
}
