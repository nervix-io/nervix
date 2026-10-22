//! Pure current-shape report, source contract, API identity and aggregation properties.

use std::collections::BTreeMap;

use meticulous::{OptionExt as _, ResultExt as _};

use super::{
    audit::{AuditCandidate, AuditPattern, Construct, ErrorInformation, KeyedOperation},
    *,
};

fn report(seed: &[u8; 32]) -> CompilerReport {
    let mut findings = Vec::new();
    for index in 0..seed[0] % 5 {
        let mut finding_seed = *seed;
        finding_seed.rotate_left(usize::from(index) * 3);
        findings.push(finding(&finding_seed, index));
    }
    let mut arguments = Vec::new();
    for index in 0..seed[1] % 5 {
        arguments.push(format!("{index}:{}", text(seed)));
    }
    let mut audit = Vec::new();
    for index in 0..seed[2] % 5 {
        let mut candidate_seed = *seed;
        candidate_seed.rotate_left(usize::from(index) * 5 + 1);
        audit.push(audit_candidate(&candidate_seed, index));
    }
    CompilerReport {
        compiler: text(seed),
        identity: text(seed),
        configuration: "ordinary".into(),
        crate_name: text(seed),
        crate_source: text(seed),
        arguments,
        audit,
        findings,
        excluded_generated: integer(seed, 6),
        excluded_external: integer(seed, 7),
        complete: seed[14].is_multiple_of(2),
    }
}

fn text(seed: &[u8; 32]) -> String {
    let code_point = integer(seed, 20) % 0x11_0000;
    let character = char::from_u32(code_point).unwrap_or(char::REPLACEMENT_CHARACTER);
    format!("symbol_{}\"\\\n\r\t\0é🦀{character}", seed[0])
}

fn integer(seed: &[u8; 32], offset: usize) -> u32 {
    let bytes = std::array::from_fn(|index| seed[(offset + index) % seed.len()]);
    match seed[(offset + 4) % seed.len()] % 5 {
        0 => 0,
        1 => u32::MAX,
        2 => 1,
        3 => u32::MAX - 1,
        _ => u32::from_le_bytes(bytes),
    }
}

fn finding(seed: &[u8; 32], index: u8) -> Finding {
    let text = text(seed);
    let start = integer(seed, 1);
    let end = integer(seed, 2).max(start);
    let line = integer(seed, 3);
    let column = integer(seed, 4);
    let source = IngestorSpan {
        site: SiteId {
            path: text.clone(),
            start,
        },
        end,
        line,
        column,
        end_line: integer(seed, 8).max(line),
        end_column: integer(seed, 9).max(column),
        source: text.clone(),
    };
    let mut expansions = Vec::new();
    for expansion in 0..seed[5] % 5 {
        expansions.push(Expansion {
            macro_name: format!("{expansion}:{text}"),
            call_site: text.clone(),
            definition: seed[usize::from(expansion) + 6]
                .is_multiple_of(2)
                .then(|| text.clone()),
        });
    }
    Finding {
        rule: text.clone(),
        receiver: text.clone(),
        receiver_type: text.clone(),
        receiver_expression: text.clone(),
        operation: text.clone(),
        definition: text.clone(),
        owner: format!("owner_{index}:{text}"),
        context: seed[13].is_multiple_of(2).then(|| context(seed)),
        acquisition: match seed[10] % 4 {
            0 => Acquisition::Shared,
            1 => Acquisition::Exclusive,
            2 => Acquisition::TryShared,
            _ => Acquisition::TryExclusive,
        },
        span: source,
        expansion: expansions,
    }
}

fn audit_candidate(seed: &[u8; 32], index: u8) -> AuditCandidate {
    let finding = finding(seed, index);
    let mut span = finding.span;
    // Patterns with two ordered operations need room for the second offset.
    if matches!(seed[19] % 7, 0 | 3) {
        span.site.start &= 0x00ff_ffff;
        span.end = span.site.start + u32::from(seed[17]);
    }
    AuditCandidate {
        owner: format!("example::owner::{{closure#{index}}}:{}", text(seed)),
        context: seed[15].is_multiple_of(2).then(|| context(seed)),
        pattern: pattern(seed, &span),
        span,
        expansion: finding.expansion,
    }
}

/// Every current audit pattern, with seed-selected values in each of its fields.
fn pattern(seed: &[u8; 32], source: &IngestorSpan) -> AuditPattern {
    let name = format!("value_{}", seed[16]);
    let mut later = source.clone();
    if matches!(seed[19] % 7, 0 | 3) {
        later.site.start = source.site.start + u32::from(seed[17]) + 1;
        later.end = later.end.max(later.site.start);
    }
    let information = match seed[18] % 3 {
        0 => ErrorInformation::Report,
        1 => ErrorInformation::Value,
        _ => ErrorInformation::Generic,
    };
    match seed[19] % 7 {
        0 => AuditPattern::RepeatedLookup {
            collection: "std::collections::hash::map::HashMap".into(),
            receiver: format!("self.{name}"),
            key: name.clone(),
            guarded: seed[20].is_multiple_of(2),
            operations: vec![
                KeyedOperation {
                    operation: "get_mut".into(),
                    span: source.clone(),
                },
                KeyedOperation {
                    operation: "insert".into(),
                    span: later,
                },
            ],
        },
        1 => AuditPattern::KeyAllocation {
            collection: "alloc::collections::btree::map::BTreeMap".into(),
            operation: "get".into(),
            allocation: "to_string".into(),
            key_type: name,
        },
        2 => AuditPattern::BorrowedCopy {
            operation: "clone".into(),
            value_type: name,
        },
        3 => AuditPattern::DuplicatedBranch {
            construct: if seed[20].is_multiple_of(2) {
                Construct::If
            } else {
                Construct::Match
            },
            statement: name,
            occurrences: vec![source.clone(), later],
        },
        4 => AuditPattern::ForwardingWrapper {
            callee: format!("example::{name}"),
            callers: u32::from(seed[21]),
            contract: seed[20].is_multiple_of(2),
            implementation: seed[21].is_multiple_of(2),
            relocated: seed[22].is_multiple_of(2),
        },
        5 => AuditPattern::DiscardedError {
            operation: "Result::ok".into(),
            error_type: name,
            information,
        },
        _ => AuditPattern::DisguisedClamp {
            operation: "checked_sub".into(),
            fallback: name,
        },
    }
}

fn context(seed: &[u8; 32]) -> Context {
    let kind = match seed[8] % 5 {
        0 => "recurring",
        1 => "lifecycle",
        2 => "observer",
        3 => "outside",
        _ => "bounded",
    };
    let (key, bound) = if kind == "bounded" {
        (
            Some(format!("attempt_{}", seed[9])),
            Some(format!("{} admitted attempts", u16::from(seed[11]) + 1)),
        )
    } else {
        (None, None)
    };
    Context::from_parts(
        kind,
        format!("owner_{} executes this path", seed[12]),
        key,
        bound,
    )
    .assured("the property constructs complete current contracts")
}

#[test]
fn generated_reports_reach_empty_and_multiple_items_and_integer_boundaries() {
    let empty = report(&[0; 32]);
    assert!(empty.findings.is_empty());
    assert!(empty.arguments.is_empty());
    assert!(empty.audit.is_empty());
    let multiple = report(&[4; 32]);
    assert_eq!(multiple.findings.len(), 4);
    assert_eq!(multiple.arguments.len(), 4);
    assert_eq!(multiple.audit.len(), 4);
    for finding in &multiple.findings {
        assert_eq!(finding.expansion.len(), 4);
        assert!(finding.span.source.contains('🦀'));
        assert!(finding.span.source.contains('"'));
        assert!(finding.span.source.contains('\0'));
    }
    let boundary = report(&[1; 32]);
    assert_eq!(boundary.findings[0].span.site.start, u32::MAX);
    assert_eq!(boundary.excluded_generated, u32::MAX);
    assert_eq!(boundary.excluded_external, u32::MAX);
}

#[test]
fn bolero_compiler_reports_round_trip() {
    bolero::check!().with_type::<[u8; 32]>().for_each(|seed| {
        let original = report(seed);
        let bytes = serde_json::to_vec(&original)
            .assured("the property constructs bounded, complete current values");
        let decoded: CompilerReport = serde_json::from_slice(&bytes)
            .assured("the property constructs bounded, complete current values");
        assert_eq!(decoded, original);
        for finding in &original.findings {
            let encoded = serde_json::to_value(finding)
                .assured("a current finding has its complete field set");
            let fields = encoded.as_object().assured("findings encode as objects");
            for field in fields.keys() {
                let mut incomplete = fields.clone();
                incomplete.remove(field);
                let result =
                    serde_json::from_value::<Finding>(serde_json::Value::Object(incomplete));
                assert!(
                    result.is_err(),
                    "every current finding field is required: {field}"
                );
            }
        }
        for audit in &original.audit {
            let encoded = serde_json::to_value(audit)
                .assured("a current audit candidate has its complete field set");
            let candidate = encoded.as_object().assured("candidates encode as objects");
            for field in candidate.keys() {
                let mut incomplete = candidate.clone();
                incomplete.remove(field);
                let result =
                    serde_json::from_value::<AuditCandidate>(serde_json::Value::Object(incomplete));
                assert!(
                    result.is_err(),
                    "every current candidate field is required: {field}"
                );
            }
            let pattern = candidate["pattern"]
                .as_object()
                .assured("patterns encode as tagged objects");
            for field in pattern.keys() {
                let mut incomplete = pattern.clone();
                incomplete.remove(field);
                let result =
                    serde_json::from_value::<AuditPattern>(serde_json::Value::Object(incomplete));
                assert!(
                    result.is_err(),
                    "every current pattern field is required: {field}"
                );
            }
        }
    });
}

#[test]
fn bolero_source_contracts_round_trip() {
    bolero::check!().with_type::<[u8; 32]>().for_each(|seed| {
        let original = context(seed);
        let bytes = serde_json::to_vec(&original).assured("the generated contract is serializable");
        let decoded: Context =
            serde_json::from_slice(&bytes).assured("the generated contract is valid");
        assert_eq!(decoded, original);
        assert_eq!(
            decoded.is_recurring(),
            matches!(
                original,
                Context::Recurring { .. } | Context::Bounded { .. }
            )
        );
        assert_eq!(
            decoded.permits_acquisition(),
            !matches!(original, Context::Recurring { .. })
        );
        assert_eq!(
            decoded.is_lifecycle(),
            matches!(original, Context::Lifecycle { .. })
        );
    });
}

#[test]
fn bolero_authored_sites_preserve_every_configuration() {
    bolero::check!().with_type::<[u8; 32]>().for_each(|seed| {
        let mut ordinary = report(seed);
        if !ordinary.complete {
            let failure =
                aggregate(&[ordinary.clone()]).expect_err("an unfinished pass cannot qualify");
            assert!(matches!(
                failure.current_context(),
                ReportError::IncompleteReport { .. }
            ));
        }
        ordinary.complete = true;
        let mut modeled = ordinary.clone();
        modeled.configuration = "shuttle".into();
        for finding in &mut modeled.findings {
            finding.receiver = format!("modeled:{}", finding.receiver);
            finding.owner = format!("modeled:{}", finding.owner);
        }
        let reports = vec![ordinary.clone(), ordinary.clone(), modeled.clone()];
        let aggregated =
            aggregate(&reports).assured("the property constructs bounded, complete current values");
        let mut expected: BTreeMap<SiteId, BTreeMap<String, Vec<Finding>>> = BTreeMap::new();
        for report in [&ordinary, &modeled] {
            for finding in &report.findings {
                let configurations = expected.entry(finding.span.site.clone()).or_default();
                let instances = configurations
                    .entry(format!("{}:{}", report.configuration, report.crate_name))
                    .or_default();
                instances.push(finding.clone());
            }
        }
        let expected: Vec<_> = expected
            .into_iter()
            .map(|(site, configurations)| AuthoredSite {
                site,
                configurations,
            })
            .collect();
        assert_eq!(aggregated, expected);
    });
}

#[test]
fn every_source_contract_argument_is_validated() {
    struct Case {
        kind: &'static str,
        reason: &'static str,
        key: Option<&'static str>,
        bound: Option<&'static str>,
        problem: ContractProblem,
    }
    for case in [
        Case {
            kind: "recurring",
            reason: "",
            key: None,
            bound: None,
            problem: ContractProblem::EmptyReason,
        },
        Case {
            kind: "bounded",
            reason: "admission",
            key: None,
            bound: Some("one attempt"),
            problem: ContractProblem::MissingKey,
        },
        Case {
            kind: "bounded",
            reason: "admission",
            key: Some("attempt"),
            bound: None,
            problem: ContractProblem::MissingBound,
        },
        Case {
            kind: "bounded",
            reason: "admission",
            key: Some(""),
            bound: Some("one attempt"),
            problem: ContractProblem::EmptyCoordinate,
        },
        Case {
            kind: "bounded",
            reason: "admission",
            key: Some("attempt"),
            bound: Some(""),
            problem: ContractProblem::EmptyCoordinate,
        },
        Case {
            kind: "lifecycle",
            reason: "installation",
            key: Some("attempt"),
            bound: None,
            problem: ContractProblem::UnexpectedCoordinate,
        },
        Case {
            kind: "unknown",
            reason: "installation",
            key: None,
            bound: None,
            problem: ContractProblem::UnknownKind {
                kind: "unknown".into(),
            },
        },
    ] {
        let report = Context::from_parts(
            case.kind,
            case.reason.into(),
            case.key.map(str::to_owned),
            case.bound.map(str::to_owned),
        )
        .err()
        .assured("each case violates a required source contract field");
        let ReportError::InvalidContract { problem } = report.current_context() else {
            panic!("invalid contract fields must retain their typed classification");
        };
        assert_eq!(problem, &case.problem);
    }
}

#[test]
fn recognition_requires_actual_definition_and_receiver_identity() {
    use crate::rules::{acquisition, may_acquire};
    for (receiver, defining_crate, operation, expected) in [
        (
            "lock_api::mutex::Mutex",
            "lock_api",
            "lock",
            Acquisition::Exclusive,
        ),
        (
            "lock_api::rwlock::RwLock",
            "lock_api",
            "read",
            Acquisition::Shared,
        ),
        (
            "tokio::sync::mutex::Mutex",
            "tokio",
            "try_lock",
            Acquisition::TryExclusive,
        ),
        (
            "shuttle_tokio_impl_inner::sync::rwlock::RwLock",
            "shuttle_tokio_impl_inner",
            "try_read",
            Acquisition::TryShared,
        ),
        (
            "loom::sync::mutex::Mutex",
            "loom",
            "lock",
            Acquisition::Exclusive,
        ),
        (
            "std::sync::poison::rwlock::RwLock",
            "std",
            "write",
            Acquisition::Exclusive,
        ),
        ("dashmap::DashMap", "dashmap", "get", Acquisition::Shared),
        (
            "shuttle_dashmap_impl::DashMap",
            "shuttle_dashmap_impl",
            "try_entry",
            Acquisition::TryExclusive,
        ),
        (
            "nervix_primitives::collections::scheduled::DashMap",
            "nervix_primitives",
            "into_iter",
            Acquisition::Shared,
        ),
        (
            "nervix_primitives::sync::blocking::tracked::Mutex",
            "nervix_primitives",
            "lock",
            Acquisition::Exclusive,
        ),
        (
            "nervix_primitives::sync::blocking::tracked::Mutex",
            "nervix_primitives",
            "try_lock",
            Acquisition::TryExclusive,
        ),
        (
            "nervix_primitives::sync::blocking::tracked::RwLock",
            "nervix_primitives",
            "read",
            Acquisition::Shared,
        ),
        (
            "nervix_primitives::sync::blocking::tracked::RwLock",
            "nervix_primitives",
            "try_write",
            Acquisition::TryExclusive,
        ),
    ] {
        assert_eq!(
            acquisition(receiver, defining_crate, operation),
            Some(expected)
        );
        assert!(may_acquire(operation));
        assert_eq!(acquisition(receiver, "application", operation), None);
        assert_eq!(
            acquisition("application::Collection", defining_crate, operation),
            None
        );
        assert_eq!(acquisition(receiver, defining_crate, "borrow_mut"), None);
    }
    assert!(!may_acquire("borrow_mut"));
}

#[test]
fn audit_catalogs_resolve_owners_and_conversions() {
    use crate::audit::{
        KeyedAccess, checked_operation, copies_text, copies_value, fallback, keyed_access,
        preserves_value,
    };
    for (collection, operation, expected) in [
        (
            "std::collections::hash::map::HashMap",
            "get_mut",
            KeyedAccess::Lookup,
        ),
        (
            "alloc::collections::btree::map::BTreeMap",
            "contains_key",
            KeyedAccess::Lookup,
        ),
        (
            "indexmap::map::IndexMap",
            "swap_remove",
            KeyedAccess::Removal,
        ),
        ("dashmap::DashMap", "entry", KeyedAccess::Entry),
        (
            "std::collections::hash::set::HashSet",
            "insert",
            KeyedAccess::Insertion,
        ),
        ("serde_json::map::Map", "index", KeyedAccess::Lookup),
    ] {
        assert_eq!(keyed_access(collection, operation), Some(expected));
        // An unrelated owner with the same method names is not a keyed collection.
        assert_eq!(keyed_access("example::Registry", operation), None);
    }
    assert_eq!(
        keyed_access("std::collections::hash::map::HashMap", "len"),
        None
    );
    assert!(KeyedAccess::Lookup.borrows_key());
    assert!(KeyedAccess::Removal.borrows_key());
    assert!(!KeyedAccess::Insertion.borrows_key());
    assert!(!KeyedAccess::Entry.borrows_key());
    for operation in ["clone", "to_owned", "to_vec"] {
        assert!(copies_value(operation));
        assert!(!copies_text(operation));
        assert!(preserves_value(operation));
    }
    // Formatting a number or converting a value is not a copy of existing text.
    for operation in ["to_string", "into"] {
        assert!(copies_text(operation));
        assert!(!copies_value(operation));
    }
    for operation in ["as_str", "borrow", "copied"] {
        assert!(preserves_value(operation));
        assert!(!copies_value(operation));
        assert!(!copies_text(operation));
    }
    assert!(!preserves_value("next"));
    assert!(checked_operation("checked_sub"));
    assert!(!checked_operation("saturating_sub"));
    assert!(fallback("unwrap_or_default"));
    assert!(!fallback("unwrap"));
}

#[test]
fn completion_and_expansion_instances_are_preserved() {
    let mut seed = [1; 32];
    seed[14] = 0;
    let first = report(&seed);
    let mut second = first.clone();
    second.findings[0].owner = "example::first_callback".into();
    let sites = aggregate(&[first.clone(), second.clone(), first.clone()])
        .assured("the two reports have complete current values");
    assert_eq!(
        sites[0]
            .configurations
            .values()
            .next()
            .assured("one configuration was inserted"),
        &vec![first.findings[0].clone(), second.findings[0].clone()]
    );
    let mut conflicting = first.clone();
    conflicting.findings[0].acquisition = Acquisition::Shared;
    assert!(matches!(
        aggregate(&[first.clone(), conflicting])
            .err()
            .assured("one instance reports contradictory acquisition semantics")
            .current_context(),
        ReportError::ConflictingFinding { .. }
    ));
    second.complete = false;
    assert!(matches!(
        aggregate(&[second])
            .err()
            .assured("the compiler pass did not finish")
            .current_context(),
        ReportError::IncompleteReport { .. }
    ));
    assert!(
        aggregate(&[])
            .assured("the empty report slice has no incomplete report")
            .is_empty()
    );
}
