//! Repository test harness, outside the product layer order.
//! Owns: bounded complete-report round trips, identity oracles and every rendering variant.
//! Depends on: the current production audit representation and maintained JSON encoding.
//! Must not know: Git processes, Cargo or the network.

use std::collections::BTreeMap;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_lint_report::{
    Acquisition, Context,
    audit::{AuditPattern, Construct, ErrorInformation},
};

use super::{
    AttributedCandidate, Attribution, Collector, Encoding, Report, Summary, evidence, lookup, span,
};
use crate::audit::{
    candidates::{Candidate, CandidateFacts, Pattern, Relocated},
    history::Commit,
    report::{Committed, Introduction, Level, Origin, Rewrite, RuleEvidence},
};

fn report(bytes: &[u8; 32]) -> Report {
    let text = format!("owner-{}-λ-😀-\"\\\u{7f}", bytes[0]);
    let source = span(
        "src/example.rs",
        u32::from(bytes[1])
            .checked_add(1)
            .assured("bounded line fits"),
        &text,
        0,
    );
    let patterns = [
        Pattern::Resolved(lookup("src/example.rs", 1, 0).pattern),
        Pattern::Resolved(AuditPattern::KeyAllocation {
            collection: text.clone(),
            operation: "get".into(),
            allocation: text.clone(),
            key_type: "String".into(),
        }),
        Pattern::Resolved(AuditPattern::BorrowedCopy {
            operation: text.clone(),
            value_type: "String".into(),
        }),
        Pattern::Resolved(AuditPattern::DuplicatedBranch {
            construct: if bytes[2].is_multiple_of(2) {
                Construct::If
            } else {
                Construct::Match
            },
            statement: text.clone(),
            occurrences: vec![source.clone(), source.clone()],
        }),
        Pattern::Resolved(AuditPattern::ForwardingWrapper {
            callee: text.clone(),
            callers: u32::from(bytes[3]),
            contract: bytes[4].is_multiple_of(2),
            implementation: bytes[5].is_multiple_of(2),
            relocated: bytes[6].is_multiple_of(2),
        }),
        Pattern::Resolved(AuditPattern::DiscardedError {
            operation: text.clone(),
            error_type: text.clone(),
            information: match bytes[7] % 3 {
                0 => ErrorInformation::Report,
                1 => ErrorInformation::Value,
                _ => ErrorInformation::Generic,
            },
        }),
        Pattern::Resolved(AuditPattern::DisguisedClamp {
            operation: text.clone(),
            fallback: text.clone(),
        }),
        Pattern::Relocated(Relocated::RelocatedAcquisition {
            receiver: text.clone(),
            operation: text.clone(),
            acquisition: match bytes[8] % 4 {
                0 => Acquisition::Shared,
                1 => Acquisition::Exclusive,
                2 => Acquisition::TryShared,
                _ => Acquisition::TryExclusive,
            },
        }),
    ];
    let rule = RuleEvidence {
        rule: "data_plane_lock_acquisitions".into(),
        event: if bytes[9].is_multiple_of(2) {
            Some(text.clone())
        } else {
            None
        },
        spelling_removed: bytes[10].is_multiple_of(2),
        file_spelling_removed: bytes[11].is_multiple_of(2),
        commit_spelling_removed: bytes[12].is_multiple_of(2),
        written_while_counted: bytes[13].is_multiple_of(2),
        predates_rule: bytes[14].is_multiple_of(2),
        pull_request_rewrite: if bytes[15].is_multiple_of(2) {
            Some(Rewrite {
                sha: "b".repeat(40),
                removed: vec![text.clone()],
                added: vec![text.clone()],
            })
        } else {
            None
        },
    };
    let mut facts = Vec::new();
    for pattern in patterns {
        facts.push(CandidateFacts {
            owner: text.clone(),
            contexts: vec![
                None,
                Some(Context::Recurring {
                    reason: text.clone(),
                }),
                Some(Context::Bounded {
                    reason: text.clone(),
                    key: text.clone(),
                    bound: text.clone(),
                }),
                Some(Context::Lifecycle {
                    reason: text.clone(),
                }),
                Some(Context::Observer {
                    reason: text.clone(),
                }),
                Some(Context::Outside {
                    reason: text.clone(),
                }),
            ],
            configurations: vec![text.clone(), "ordinary:example".into()],
            pattern,
            span: source.clone(),
            expansion: vec![nervix_lint_report::Expansion {
                macro_name: text.clone(),
                call_site: text.clone(),
                definition: if bytes[16].is_multiple_of(2) {
                    Some(text.clone())
                } else {
                    None
                },
            }],
        });
    }
    let mut candidates = Vec::new();
    for (index, candidate) in Candidate::identify(facts, &Encoding::new())
        .assured("current bounded facts encode")
        .into_iter()
        .enumerate()
    {
        let attribution = if index == 0 {
            Attribution::uncommitted()
        } else {
            let level = match index % 4 {
                0 => Level::Suspected,
                1 => Level::Possible,
                2 => Level::Unattributed,
                _ => Level::PreExisting,
            };
            Attribution::committed(
                level,
                Committed {
                    commit: Commit {
                        sha: "a".repeat(40),
                        date: "2026-09-24".into(),
                        subject: text.clone(),
                    },
                    commits: vec!["a".repeat(12), "c".repeat(12)],
                    before: vec![text.clone()],
                    after: vec![text.clone()],
                    rules: vec![rule.clone()],
                    origin: if bytes[17].is_multiple_of(2) {
                        Some(Origin {
                            sha: "c".repeat(40),
                            date: "2026-09-23".into(),
                            subject: text.clone(),
                            rules: vec![rule.clone()],
                        })
                    } else {
                        None
                    },
                },
            )
        };
        candidates.push(AttributedCandidate {
            candidate,
            attribution,
        });
    }
    Report {
        revision: "a".repeat(40),
        source_identity: text.clone(),
        matrix_complete: bytes[18].is_multiple_of(2),
        configurations: vec![text.clone()],
        pull_requests: bytes[19].is_multiple_of(2),
        rules: BTreeMap::from([(
            text,
            Introduction {
                introduced: "b".repeat(40),
                date: "2026-09-20".into(),
            },
        )]),
        summary: Summary::of(&candidates),
        candidates,
        complete: (),
    }
}

fn round_trip(bytes: &[u8; 32]) {
    let report = report(bytes);
    let encoded = Encoding::new()
        .report(&report)
        .assured("current reports encode");
    let decoded: Report = serde_json::from_slice(&encoded).assured("current reports decode");
    assert_eq!(decoded, report);
    assert_eq!(Summary::of(&decoded.candidates), report.summary);
    assert_eq!(
        Encoding::new()
            .report(&decoded)
            .assured("decoded reports encode"),
        encoded
    );
    assert_eq!(
        crate::audit::render::markdown(&decoded),
        crate::audit::render::markdown(&report)
    );
}

#[test]
fn every_report_variant_round_trips_and_renders() {
    for seed in 0..24_u8 {
        round_trip(&[seed; 32]);
    }
}

#[test]
fn bolero_complete_audit_report_round_trip() {
    bolero::check!()
        .with_type::<[u8; 32]>()
        .for_each(round_trip);
}

#[test]
fn semantic_identity_oracles_keep_unicode_and_control_spelling() {
    for (text, written, expected) in [
        ("plain", "'plain'", "87ac72cf5715"),
        ("café", "'café'", "dc950c5377e5"),
        ("λ", "'λ'", "e97b2af2f412"),
        ("😀", "'😀'", "635ecf96f32b"),
        ("\u{7f}", "'\\x7f'", "21913fb957b4"),
        ("\0\t\n\r", "'\\x00\\t\\n\\r'", "783d61affa0c"),
        ("\u{2028}\u{2029}", "'\\u2028\\u2029'", "69a00a705c1f"),
        ("\u{1c}", "'\\x1c'", "1bdad6130ff7"),
    ] {
        let path = format!("src/{text}.rs");
        let mut raw = lookup(&path, 1, 0);
        raw.owner = "crate::f".into();
        raw.span.source = text.into();
        if let AuditPattern::RepeatedLookup {
            receiver,
            key,
            operations,
            ..
        } = &mut raw.pattern
        {
            *receiver = "self.map".into();
            *key = text.into();
            operations[0].operation = "get".into();
            operations[0].span.source = format!("self.map.get({written})");
            operations[1].span.source = "self.map.insert(key, value)".into();
        }
        let candidates = Collector::new()
            .collect(vec![evidence("ordinary", vec![raw])])
            .assured("oracle candidate collects");
        assert_eq!(candidates[0].id, expected);
    }
}

#[test]
fn candidate_projections_must_agree_with_their_typed_facts() {
    let original = report(&[0; 32]);
    for field in ["kind", "path", "line", "column", "source", "id"] {
        let mut encoded = serde_json::to_value(&original).assured("fixture report encodes");
        encoded["candidates"][0][field] = match field {
            "line" | "column" => serde_json::json!(99999),
            "id" => serde_json::json!(""),
            _ => serde_json::json!("inconsistent"),
        };
        assert!(
            serde_json::from_value::<Report>(encoded).is_err(),
            "{field} must agree"
        );
    }
}
