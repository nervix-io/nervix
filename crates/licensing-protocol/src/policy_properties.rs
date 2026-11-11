//! Bounded resolution, impact and logical retry identity through production owners.

use meticulous::{OptionExt as _, ResultExt as _};

use crate::{
    conditions::*,
    impact::*,
    policy_tests::{context, policy, usage},
    representation_properties::{number, text},
    requests::*,
    test_fixture::*,
};

#[test]
fn bolero_conditions_resolution() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(256)
        .for_each(|input| {
            let value = number(input);
            let mut plan = plan();
            plan.usage = usage(Quota::Bounded { value }, Quota::Unlimited);
            let mut overrides = empty_conditions();
            overrides.usage.quotas.insert(
                CLUSTER_DOMAINS.to_owned(),
                ConditionValue::Known(Quota::Bounded { value }),
            );
            let resolved = plan
                .resolve(&overrides, resolution_facts(), limits().bytes)
                .assured("complete bounded plan");
            let mut expected = plan.clone();
            expected.usage.quotas.extend(overrides.usage.quotas);
            assert_eq!(resolved.values(), &expected);
            assert_eq!(
                resolved.conditions_digest(),
                &expected
                    .digest(limits().bytes)
                    .assured("complete canonical encoding")
            );
            round_trip(resolved.values());
            let mut suspended_facts = resolution_facts();
            suspended_facts.organization_status = OrganizationStatus::Suspended;
            let suspended = plan
                .resolve(&empty_conditions(), suspended_facts, limits().bytes)
                .assured("organization suspension is an explicit shared input");
            assert_eq!(
                suspended.status(),
                crate::documents::DecisionStatus::Suspended
            );
            assert_eq!(suspended.values().usage, FREE_POLICY.usage());
            assert_eq!(suspended.values().operating, plan.operating);
            // Reversing insertion history cannot change the canonical digest.
            let mut reordered = empty_conditions();
            for (name, value) in expected.usage.quotas.iter().rev() {
                reordered.usage.quotas.insert(name.clone(), value.clone());
            }
            reordered.operating = expected.operating.clone();
            assert_eq!(
                reordered.digest(limits().bytes).assured("sorted maps"),
                *resolved.conditions_digest()
            );
        });
}

#[test]
fn bolero_impact_assessment() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(256)
        .for_each(|input| {
            // Small counts let the independent expected arithmetic remain explicit and exhaustive
            // at zero and equality, while the checked overflow boundaries have deterministic units.
            let value = number(input) % 1_000;
            let previous = value.checked_add(2).assured("value is below 1000");
            let count = value.checked_add(1).assured("value is below 1000");
            let current = usage(Quota::Bounded { value: previous }, Quota::Unlimited);
            let target = usage(Quota::Bounded { value }, Quota::Unlimited);
            let snapshot = UsageSnapshot {
                cluster_nodes: count,
                cluster_domains: 0,
            };
            let trigger = match value % 3 {
                0 => crate::documents::TransitionTrigger::Decision {
                    decision_id: id(100),
                },
                1 => crate::documents::TransitionTrigger::Tick {
                    decision_id: id(100),
                },
                _ => crate::documents::TransitionTrigger::Release {
                    request_id: id(100),
                },
            };
            let mut impact = context(1, if value.is_multiple_of(3) { 2 } else { 1 }, 0);
            impact.trigger = trigger;
            let transitions = assess_impact(
                EffectiveUsage {
                    conditions: &current,
                    transitions: &[],
                },
                ImpactTarget::Usage(&target),
                snapshot,
                policy(),
                impact.clone(),
                limits().bytes,
            )
            .assured("bounded reduction");
            assert_eq!(transitions.len(), 1);
            assert_eq!(transitions[0].affected.len(), 1);
            assert_eq!(
                transitions[0].affected[0]
                    .additional_allowance()
                    .assured("new allowance"),
                1
            );
            assert_eq!(transitions[0].deadline, at(100));
            round_trip(&transitions);
            let repeated = assess_impact(
                EffectiveUsage {
                    conditions: &current,
                    transitions: &[],
                },
                ImpactTarget::Usage(&target),
                snapshot,
                policy(),
                impact.clone(),
                limits().bytes,
            )
            .assured("same replicated inputs");
            assert_eq!(repeated, transitions);
            assert_eq!(
                EffectiveUsage {
                    conditions: &target,
                    transitions: &transitions
                }
                .quota(CLUSTER_NODES, at(99))
                .assured("temporary amount"),
                Quota::Bounded { value: count }
            );
            assert_eq!(
                EffectiveUsage {
                    conditions: &target,
                    transitions: &transitions
                }
                .quota(CLUSTER_NODES, at(100))
                .assured("deadline removes amount"),
                Quota::Bounded { value }
            );
        });
}

#[test]
fn bolero_request_digest() {
    let fixture = Fixture::new();
    bolero::check!()
        .with_iterations(128)
        .with_max_len(256)
        .for_each(|input| {
            for mut request in fixture.requests() {
                request.common_mut().display_name = text(input);
                let digest = request
                    .digest(limits().bytes)
                    .assured("original logical request");
                let mut retry = request.clone();
                retry.common_mut().challenge = Secret::new(text(input));
                retry.common_mut().sent_at = at(100);
                assert_eq!(
                    retry
                        .digest(limits().bytes)
                        .assured("fresh challenge and timestamp"),
                    digest
                );
                let mut original = serde_json::to_value(&request).assured("complete request");
                // Every other top-level field contributes, including the operation and optional
                // committed state. Each mutation remains a valid current request.
                original["display_name"] =
                    serde_json::json!(format!("{}!", request.common().display_name));
                let changed: Request =
                    serde_json::from_value(original).assured("valid changed request");
                assert_ne!(
                    changed.digest(limits().bytes).assured("different name"),
                    digest
                );
                for changed in meaningful_changes(&request, &fixture) {
                    assert_ne!(
                        changed
                            .digest(limits().bytes)
                            .assured("different logical field"),
                        digest
                    );
                }
            }
            let ticket = StreamTicket {
                common: fixture.common(),
                activation_id: id(1),
                state: fixture.committed(),
            };
            let digest = ticket.digest(limits().bytes).assured("ticket identity");
            let mut retry = ticket.clone();
            retry.common.challenge = Secret::new(text(input));
            retry.common.sent_at = at(100);
            assert_eq!(retry.digest(limits().bytes).assured("ticket retry"), digest);
            retry.activation_id = id(99);
            assert_ne!(
                retry.digest(limits().bytes).assured("different activation"),
                digest
            );
        });
}

fn meaningful_changes(request: &Request, fixture: &Fixture) -> Vec<Request> {
    let mut changes = Vec::new();
    for name in [
        "protocol",
        "request_id",
        "aud",
        "cluster_uid",
        "client",
        "key_generation",
        "trust_held",
    ] {
        let mut value = serde_json::to_value(request).assured("complete request");
        value[name] = match name {
            "protocol" => serde_json::json!(2),
            "request_id" | "cluster_uid" => serde_json::json!(id(99)),
            "aud" => serde_json::json!("https://other.example"),
            "client" => serde_json::json!({"product":"nervix-server","version":"changed"}),
            "key_generation" => serde_json::json!(2),
            "trust_held" => {
                serde_json::json!({"working_keys":[],"revocation_serials":[],"succession_serials":[]})
            }
            _ => unreachable!("the local field array is closed"),
        };
        changes.push(serde_json::from_value(value).assured("current changed common field"));
    }
    struct Mutation {
        name: &'static str,
        value: serde_json::Value,
    }
    let mut mutations = vec![];
    if !matches!(request, Request::Create { .. }) {
        mutations.push(Mutation {
            name: "activation_id",
            value: serde_json::json!(id(99)),
        });
    }
    match request {
        Request::Create { .. } => {
            mutations.push(Mutation {
                name: "license_key",
                value: serde_json::json!("different-key"),
            });
            mutations.push(Mutation {
                name: "kind",
                value: serde_json::json!(ActivationKind::Ephemeral),
            });
            mutations.push(Mutation {
                name: "cluster_key",
                value: serde_json::to_value(fixture.working.public_key())
                    .assured("current public key"),
            });
        }
        Request::Refresh { state, .. } => {
            if let RefreshState::Adoption = state {
                let mut committed = request.clone();
                let Request::Refresh { state, .. } = &mut committed else {
                    unreachable!("the matched operation is refresh");
                };
                *state = RefreshState::Committed(fixture.committed());
                changes.push(committed);
            }
            mutations.push(Mutation {
                name: "transitions",
                value: serde_json::json!([fixture_transition(
                    crate::documents::TransitionTrigger::Decision {
                        decision_id: id(100)
                    }
                )]),
            });
        }
        Request::Release { .. } => {
            mutations.push(Mutation {
                name: "reason",
                value: serde_json::json!("different reason"),
            });
            mutations.push(Mutation {
                name: "grants_until",
                value: serde_json::json!(at(1_000)),
            });
        }
        Request::RotateKey { .. } => {
            mutations.push(Mutation {
                name: "new_key",
                value: serde_json::to_value(fixture.cluster.public_key())
                    .assured("current public key"),
            });
            mutations.push(Mutation {
                name: "proof",
                value: serde_json::json!("different-proof"),
            });
        }
    }
    let original = serde_json::to_value(request).assured("complete request");
    if original.get("applied_decision_revision").is_some() {
        mutations.push(Mutation {
            name: "applied_decision_revision",
            value: serde_json::json!(99),
        });
        mutations.push(Mutation {
            name: "applied_conditions_revision",
            value: serde_json::json!(99),
        });
        mutations.push(Mutation {
            name: "licensing_time",
            value: serde_json::json!(at(99)),
        });
    }
    for mutation in mutations {
        let mut value = original.clone();
        value[mutation.name] = mutation.value;
        changes
            .push(serde_json::from_value(value).assured("one meaningful operation field changed"));
    }
    changes
}
