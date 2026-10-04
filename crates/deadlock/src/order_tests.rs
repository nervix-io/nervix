//! Current order evidence, deduplication and review contracts, through the reporting owner.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    time::{Duration, UNIX_EPOCH},
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::Entropy;
use nervix_primitives::deadlock::{
    Access, BlockedAttempt, BoundedText, LockKind, LockLifetime, LockSite, MAX_ORDER_EDGES,
    MAX_ORDER_WITNESSES, OrderEdge, OrderLock, OrderOutOfBounds, OrderWitness, PotentialCycle,
    SourceSite, TrackedLockId, TrackedThreadId,
};

use crate::{
    DeadlockEvidence, PotentialTriage, ProcessRecord, ProofBasis, RecordedFinding, TriageProof,
};

fn number(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).assured("test identities start from one")
}

fn source(line: u32) -> SourceSite {
    SourceSite {
        file: BoundedText::new("src/owner.rs"),
        line,
        column: 4,
    }
}

pub(super) fn cycle(offset: u64, access: Access) -> PotentialCycle {
    let lock = |id| OrderLock {
        id: TrackedLockId::new(number(id)),
        site: Some(LockSite {
            kind: LockKind::RwLock,
            constructed_at: source(10),
        }),
        lifetime: LockLifetime::Live,
    };
    let witness = OrderWitness {
        thread: TrackedThreadId::new(number(1)),
        name: Some(BoundedText::new("worker")),
        held: BlockedAttempt {
            access,
            at: source(20),
        },
        requested: BlockedAttempt {
            access,
            at: source(30),
        },
        attempts: number(1),
        held_count: number(2),
    };
    let first = lock(offset + 1);
    let second = lock(offset + 2);
    let edges = vec![
        OrderEdge::new(first.clone(), second.clone(), vec![witness.clone()], 0)
            .assured("one witness fits"),
        OrderEdge::new(second, first, vec![witness], 0).assured("one witness fits"),
    ];
    PotentialCycle::new(UNIX_EPOCH, edges, 0).assured("a complete cycle of two fits")
}

fn evidence(cycle: PotentialCycle) -> DeadlockEvidence {
    DeadlockEvidence::new(
        ProcessRecord {
            selection: nervix_primitives::deadlock::DiagnosticSelection::OrderAnalysis,
            ..super::tests::process()
        },
        vec![RecordedFinding::from(
            nervix_primitives::deadlock::Finding::PotentialCycle(cycle),
        )],
    )
    .assured("one finding fits")
}

fn proof(basis: ProofBasis) -> TriageProof {
    TriageProof::new(
        basis,
        "the owner serializes these operations before acquiring either lock",
        "order_tests::potential_evidence_requires_a_review_and_keeps_its_cycle_after_review",
    )
    .assured("the proof and regression are present")
}

#[test]
fn potential_evidence_requires_a_review_and_keeps_its_cycle_after_review() {
    let original = cycle(0, Access::Exclusive);
    let mut evidence = evidence(original.clone());
    assert!(!evidence.qualifies());
    evidence
        .triage(0, proof(ProofBasis::NonOverlap))
        .assured("the complete cycle can be reviewed");
    assert!(evidence.qualifies());
    let [
        RecordedFinding::Potential {
            cycle,
            triage: PotentialTriage::Reviewed(recorded),
            ..
        },
    ] = evidence.findings()
    else {
        panic!("reviewed potential evidence was retained")
    };
    assert_eq!(cycle, &original);
    assert_eq!(recorded, &proof(ProofBasis::NonOverlap));
    let bytes = evidence.encode().assured("reviewed evidence encodes");
    assert_eq!(
        DeadlockEvidence::decode(&bytes).assured("current evidence decodes"),
        evidence
    );
}

#[test]
fn rotated_cycles_deduplicate_without_losing_mode_or_multiplicity() {
    let original = cycle(0, Access::Shared);
    let mut edges = original.edges().to_vec();
    edges.rotate_left(1);
    let rotated = PotentialCycle::new(UNIX_EPOCH, edges, 0).assured("rotation preserves a cycle");
    assert_eq!(original, rotated);
    let evidence = evidence(original.clone())
        .with_finding(RecordedFinding::from(
            nervix_primitives::deadlock::Finding::PotentialCycle(rotated),
        ))
        .assured("a repeated cycle fits");
    let [
        RecordedFinding::Potential {
            cycle, repetitions, ..
        },
    ] = evidence.findings()
    else {
        panic!("one normalized cycle")
    };
    assert_eq!(cycle, &original);
    assert_eq!(*repetitions, number(2));
    assert_eq!(cycle.edges()[0].witnesses()[0].held_count, number(2));
}

#[test]
fn lock_instances_constructed_at_one_source_site_have_distinct_findings() {
    let evidence = evidence(cycle(0, Access::Exclusive))
        .with_finding(RecordedFinding::from(
            nervix_primitives::deadlock::Finding::PotentialCycle(cycle(10, Access::Exclusive)),
        ))
        .assured("two findings fit");
    assert_eq!(evidence.findings().len(), 2);
}

#[test]
fn a_shared_reader_proof_checks_every_recorded_mode() {
    let mut readers = evidence(cycle(0, Access::Shared));
    readers
        .triage(0, proof(ProofBasis::SharedReaders))
        .assured("every acquisition is shared");
    assert!(readers.qualifies());
    let mut writers = evidence(cycle(0, Access::Exclusive));
    assert!(writers.triage(0, proof(ProofBasis::SharedReaders)).is_err());
    assert!(!writers.qualifies());
}

#[test]
fn a_proof_requires_a_reason_and_retained_regression() {
    assert!(TriageProof::new(ProofBasis::NonOverlap, "", "test::regression").is_err());
    assert!(TriageProof::new(ProofBasis::Lifecycle, "the generations are separate", " ").is_err());
    assert!(TriageProof::new(ProofBasis::Lifecycle, &"x".repeat(513), "test::regression").is_err());
    let mut evidence = evidence(cycle(0, Access::Exclusive));
    assert!(evidence.triage(7, proof(ProofBasis::Lifecycle)).is_err());
}

#[test]
fn selected_artifacts_retain_loss_and_cannot_qualify_the_source_process() {
    let mut original = evidence(cycle(0, Access::Shared));
    original
        .triage(0, proof(ProofBasis::SharedReaders))
        .assured("complete reader proof");
    let selected = original.selected(crate::FindingSelection::Active);
    assert!(selected.findings().is_empty());
    assert!(!selected.qualifies());
    assert_eq!(
        DeadlockEvidence::decode(&selected.encode().assured("selected evidence encodes"))
            .assured("current evidence decodes"),
        selected
    );
    assert_eq!(original.selected(crate::FindingSelection::All), original);
    let loss = RecordedFinding::Overflow {
        lost: number(1),
        source: crate::EvidenceLossSource::OrderHistory,
    };
    let original = original
        .with_finding(loss.clone())
        .assured("two records fit");
    assert_eq!(
        original
            .selected(crate::FindingSelection::Active)
            .findings(),
        &[loss]
    );
    assert!(!original.qualifies());
}

#[test]
fn new_source_context_revokes_a_review_and_retains_both_modes() {
    let original = cycle(0, Access::Shared);
    let mut reviewed = evidence(original.clone());
    reviewed
        .triage(0, proof(ProofBasis::SharedReaders))
        .assured("complete reader proof");
    let merged = reviewed
        .with_finding(RecordedFinding::from(
            nervix_primitives::deadlock::Finding::PotentialCycle(cycle(0, Access::Exclusive)),
        ))
        .assured("two contexts fit");
    assert!(!merged.qualifies());
    let [
        RecordedFinding::Potential {
            cycle,
            triage: PotentialTriage::Unreviewed,
            ..
        },
    ] = merged.findings()
    else {
        panic!("new context requires review")
    };
    for edge in cycle.edges() {
        assert_eq!(edge.witnesses().len(), 2);
        assert!(
            edge.witnesses()
                .iter()
                .any(|witness| witness.held.access == Access::Shared)
        );
        assert!(
            edge.witnesses()
                .iter()
                .any(|witness| witness.requested.access == Access::Exclusive)
        );
    }
    assert_eq!(
        DeadlockEvidence::decode(&merged.encode().assured("merged evidence encodes"))
            .assured("current evidence decodes"),
        merged
    );
}

#[test]
fn missing_or_omitted_context_remains_visible_and_cannot_be_reviewed() {
    let original = cycle(0, Access::Exclusive);
    let mut edges = original.edges().to_vec();
    edges[0].before.site = None;
    edges[0].before.lifetime = LockLifetime::Unrecorded;
    let missing = PotentialCycle::new(UNIX_EPOCH, edges, 0)
        .assured("missing context is represented explicitly");
    let mut incomplete = evidence(missing);
    assert!(incomplete.triage(0, proof(ProofBasis::Lifecycle)).is_err());
    let rendered = crate::render_finding(&incomplete.findings()[0]);
    assert!(rendered.contains("not recorded"), "{rendered}");
    assert!(!incomplete.qualifies());
    let mut edge = original.edges()[0].clone();
    let mut missing = edge.clone();
    missing.before.site = None;
    missing.before.lifetime = LockLifetime::Unrecorded;
    edge.merge(&missing)
        .assured("missing context never replaces known construction");
    assert_eq!(edge, original.edges()[0]);
    let active = RecordedFinding::ActiveCycle(
        nervix_primitives::deadlock::ActiveCycle::new(
            UNIX_EPOCH,
            vec![nervix_primitives::deadlock::BlockedThread {
                thread: TrackedThreadId::new(number(1)),
                name: None,
                waits_for: None,
                attempt: None,
            }],
            0,
        )
        .assured("a bounded active cycle"),
    );
    let mut active =
        DeadlockEvidence::new(super::tests::process(), vec![active]).assured("one record fits");
    assert!(active.triage(0, proof(ProofBasis::NonOverlap)).is_err());
}

/// Current bounded values, including missing context, lifecycle states, modes and multiplicity.
fn generated_cycle(entropy: &mut Entropy<'_>, complete: bool) -> PotentialCycle {
    let count = entropy
        .positive_count(NonZeroUsize::new(MAX_ORDER_EDGES).assured("the capacity is positive"));
    let omitted = if !complete && count == MAX_ORDER_EDGES {
        entropy.any_u64()
    } else {
        0
    };
    let lock_count = count + usize::from(omitted > 0);
    let base = entropy
        .up_to(u64::MAX - u64::try_from(MAX_ORDER_EDGES).assured("the fixed bound fits") - 2)
        + 1;
    let mut locks = Vec::with_capacity(lock_count);
    for index in 0..lock_count {
        locks.push(OrderLock {
            id: TrackedLockId::new(number(
                base + u64::try_from(index).assured("the fixed bound fits"),
            )),
            site: if complete || entropy.flag() {
                Some(LockSite {
                    kind: entropy.pick([LockKind::Mutex, LockKind::RwLock, LockKind::CondvarState]),
                    constructed_at: generated_site(entropy),
                })
            } else {
                None
            },
            lifetime: if complete {
                entropy.pick([LockLifetime::Live, LockLifetime::Ended])
            } else {
                entropy.pick([
                    LockLifetime::Live,
                    LockLifetime::Ended,
                    LockLifetime::Unrecorded,
                ])
            },
        });
    }
    let mut edges = Vec::with_capacity(count);
    for index in 0..count {
        let witnesses = if complete {
            entropy.positive_count(
                NonZeroUsize::new(MAX_ORDER_WITNESSES).assured("the witness capacity is positive"),
            )
        } else {
            entropy.count(MAX_ORDER_WITNESSES)
        };
        let omitted_witnesses = if !complete && witnesses == MAX_ORDER_WITNESSES {
            entropy.any_u64()
        } else {
            0
        };
        let mut contexts = Vec::with_capacity(witnesses);
        let thread_base = entropy
            .up_to(u64::MAX - u64::try_from(MAX_ORDER_WITNESSES).assured("the bound fits"))
            + 1;
        for context in 0..witnesses {
            contexts.push(OrderWitness {
                thread: TrackedThreadId::new(number(
                    thread_base + u64::try_from(context).assured("the context index fits"),
                )),
                name: if entropy.flag() {
                    Some(generated_text(entropy))
                } else {
                    None
                },
                held: BlockedAttempt {
                    access: entropy.pick([Access::Shared, Access::Exclusive]),
                    at: generated_site(entropy),
                },
                requested: BlockedAttempt {
                    access: entropy.pick([Access::Shared, Access::Exclusive]),
                    at: generated_site(entropy),
                },
                attempts: generated_number(entropy),
                held_count: generated_number(entropy),
            });
        }
        edges.push(
            OrderEdge::new(
                locks[index].clone(),
                locks[(index + 1) % lock_count].clone(),
                contexts,
                omitted_witnesses,
            )
            .assured("the drawn witnesses respect the capacity"),
        );
    }
    PotentialCycle::new(
        UNIX_EPOCH + Duration::from_nanos(entropy.any_u64()),
        edges,
        omitted,
    )
    .assured("a bounded directed cycle or explicitly partial prefix")
}

fn generated_number(entropy: &mut Entropy<'_>) -> NonZeroU64 {
    number(entropy.boundary_biased(1..=u64::MAX))
}
fn generated_text(entropy: &mut Entropy<'_>) -> BoundedText {
    let count = entropy.count(520);
    let mut text = String::new();
    while text.len() < count {
        text.push(entropy.pick(['a', '/', 'é', '✓', '🦀']));
    }
    BoundedText::new(&text)
}
fn generated_site(entropy: &mut Entropy<'_>) -> SourceSite {
    SourceSite {
        file: generated_text(entropy),
        line: u32::try_from(entropy.up_to(u64::from(u32::MAX))).assured("drawn within u32"),
        column: u32::try_from(entropy.up_to(u64::from(u32::MAX))).assured("drawn within u32"),
    }
}

pub(super) fn generated_finding(entropy: &mut Entropy<'_>) -> RecordedFinding {
    let complete = entropy.flag();
    let cycle = generated_cycle(entropy, complete);
    let mut evidence = evidence(cycle);
    assert!(!evidence.qualifies(), "unreviewed order never qualifies");
    if let RecordedFinding::Potential { cycle, .. } = &evidence.findings()[0]
        && cycle.has_complete_context()
        && entropy.flag()
    {
        let basis = entropy.pick([
            ProofBasis::Correction,
            ProofBasis::NonOverlap,
            ProofBasis::Lifecycle,
        ]);
        evidence
            .triage(0, proof(basis))
            .assured("bounded complete proof");
    }
    let RecordedFinding::Potential { cycle, triage, .. } = evidence.findings()[0].clone() else {
        panic!("one potential record")
    };
    RecordedFinding::Potential {
        cycle,
        triage,
        repetitions: generated_number(entropy),
    }
}

#[test]
fn bolero_order_normalization_and_dedup_preserve_complete_evidence() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let mut entropy = Entropy::new(bytes);
            let original = generated_cycle(&mut entropy, true);
            let mut edges = original.edges().to_vec();
            let rotation = entropy.count(edges.len() - 1);
            edges.rotate_left(rotation);
            let normalized = PotentialCycle::new(original.detected_at(), edges, 0)
                .assured("cyclic rotation preserves direction");
            assert_eq!(normalized, original);
            let merged = evidence(original.clone())
                .with_finding(RecordedFinding::from(
                    nervix_primitives::deadlock::Finding::PotentialCycle(normalized),
                ))
                .assured("the same complete contexts fit");
            let [
                RecordedFinding::Potential {
                    cycle,
                    repetitions,
                    triage,
                    ..
                },
            ] = merged.findings()
            else {
                panic!("one deduplicated cycle")
            };
            assert_eq!(cycle, &original);
            assert_eq!(*repetitions, number(2));
            assert_eq!(*triage, PotentialTriage::Unreviewed);
            assert_eq!(
                DeadlockEvidence::decode(&merged.encode().assured("bounded evidence encodes"))
                    .assured("its complete encoding decodes"),
                merged
            );
        });
}

#[test]
fn bolero_order_evidence_outside_its_bounds_is_refused() {
    use crate::{
        order_wire::{BasisWire, OrderCycleWire, ProofWire, TriageWire},
        wire::{EvidenceWire, FindingWire, encode_wire},
    };
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .for_each(|bytes: &[u8]| {
            let mut entropy = Entropy::new(bytes);
            let mut wire = EvidenceWire::try_from(&evidence(cycle(0, Access::Exclusive)))
                .assured("the current complete fixture encodes");
            let mut cycle = OrderCycleWire::try_from(&cycle(0, Access::Exclusive))
                .assured("current order encodes");
            let mut triage = TriageWire::Unreviewed;
            let mut repetitions = 1;
            let review = |basis| {
                TriageWire::Reviewed(ProofWire {
                    basis,
                    reason: crate::wire::TextWire {
                        text: "proof".into(),
                        original_bytes: 5,
                    },
                    regression: crate::wire::TextWire {
                        text: "owner::regression".into(),
                        original_bytes: 17,
                    },
                })
            };
            match entropy.byte() % 18 {
                0 => cycle.edges.clear(),
                1 => cycle.edges = vec![cycle.edges[0].clone(); MAX_ORDER_EDGES + 1],
                2 => cycle.edges[0].after.lock.id = 9,
                3 => cycle.edges[1].before.lock.id = cycle.edges[0].before.lock.id,
                4 => cycle.omitted_edges = 1,
                5 => {
                    cycle.edges[0].witnesses =
                        vec![cycle.edges[0].witnesses[0].clone(); MAX_ORDER_WITNESSES + 1]
                }
                6 => cycle.edges[0].omitted_witnesses = 1,
                7 => cycle.edges[0].witnesses[0].held_count = 0,
                8 => cycle.edges[0].witnesses[0].attempts = 0,
                9 => cycle.edges[0].witnesses[0].thread = 0,
                10 => cycle.edges[0].before.lock.id = 0,
                11 => {
                    triage = review(BasisWire::Lifecycle);
                    if let TriageWire::Reviewed(proof) = &mut triage {
                        proof.reason.text.clear();
                        proof.reason.original_bytes = 0;
                    }
                }
                12 => triage = review(BasisWire::SharedReaders),
                13 => repetitions = 0,
                14 => {
                    cycle.edges[0].before.lifetime = crate::order_wire::LifetimeWire::Unrecorded;
                    triage = review(BasisWire::Lifecycle);
                }
                15 => cycle.edges[0].witnesses = vec![cycle.edges[0].witnesses[0].clone(); 2],
                16 => {
                    cycle.edges[0]
                        .before
                        .lock
                        .site
                        .as_mut()
                        .assured("fixture construction is present")
                        .constructed_at
                        .line = 999;
                }
                _ => {
                    cycle.edges[0].witnesses[0].held.at.file.text = "x".repeat(513);
                    cycle.edges[0].witnesses[0].held.at.file.original_bytes = 513;
                }
            }
            wire.findings = vec![FindingWire::Potential {
                cycle,
                repetitions,
                triage,
            }];
            let bytes = encode_wire(&wire).assured("bounded malformed current values encode");
            assert!(DeadlockEvidence::decode(&bytes).is_err());
        });
}

#[test]
fn a_refused_context_merge_preserves_the_complete_preceding_snapshot() {
    let original = cycle(0, Access::Shared);
    let edge = &original.edges()[1];
    let contexts: Vec<_> = (1..=MAX_ORDER_WITNESSES)
        .map(|thread| {
            let mut witness = edge.witnesses()[0].clone();
            witness.thread =
                TrackedThreadId::new(number(u64::try_from(thread).assured("the bound is small")));
            witness
        })
        .collect();
    let full = OrderEdge::new(edge.before.clone(), edge.after.clone(), contexts.clone(), 0)
        .assured("exactly the witness bound fits");
    let mut next = contexts[0].clone();
    next.thread = TrackedThreadId::new(number(99));
    let mut repeated = contexts[0].clone();
    repeated.attempts = number(10);
    let incoming = OrderEdge::new(
        edge.before.clone(),
        edge.after.clone(),
        vec![repeated, next],
        0,
    )
    .assured("two incoming witnesses fit");
    let mut refused = full.clone();
    assert_eq!(
        refused.merge(&incoming),
        Err(OrderOutOfBounds::TooManyWitnesses)
    );
    assert_eq!(refused, full);
    let mut preceding = PotentialCycle::new(UNIX_EPOCH, vec![original.edges()[0].clone(), full], 0)
        .assured("a complete current cycle fits");
    let snapshot = preceding.clone();
    let changed = cycle(0, Access::Exclusive);
    let incoming = PotentialCycle::new(UNIX_EPOCH, vec![changed.edges()[0].clone(), incoming], 0)
        .assured("a complete incoming cycle fits");
    assert_eq!(
        preceding.merge(&incoming),
        Err(OrderOutOfBounds::TooManyWitnesses)
    );
    assert_eq!(preceding, snapshot);
}

#[test]
fn order_boundaries_report_the_broken_invariant() {
    let original = cycle(0, Access::Exclusive);
    assert_eq!(
        PotentialCycle::new(UNIX_EPOCH, Vec::new(), 0),
        Err(OrderOutOfBounds::Empty)
    );
    assert_eq!(
        PotentialCycle::new(UNIX_EPOCH, original.edges().to_vec(), 1),
        Err(OrderOutOfBounds::OmittedEdgesBelowBound)
    );
    let mut conflict = original.edges()[0].clone();
    conflict
        .before
        .site
        .as_mut()
        .assured("known construction")
        .constructed_at
        .line = 11;
    let mut edge = original.edges()[0].clone();
    assert_eq!(
        edge.merge(&conflict),
        Err(OrderOutOfBounds::ConflictingInstance)
    );
    let mut independent = cycle(10, Access::Exclusive);
    assert_eq!(
        independent.merge(&original),
        Err(OrderOutOfBounds::Disconnected)
    );
    for error in [
        OrderOutOfBounds::Empty,
        OrderOutOfBounds::TooManyEdges,
        OrderOutOfBounds::TooManyWitnesses,
        OrderOutOfBounds::OmittedEdgesBelowBound,
        OrderOutOfBounds::OmittedWitnessesBelowBound,
        OrderOutOfBounds::Disconnected,
        OrderOutOfBounds::RepeatedInstance,
        OrderOutOfBounds::ConflictingInstance,
        OrderOutOfBounds::RepeatedWitnessContext,
    ] {
        assert!(!error.to_string().is_empty());
    }
}
