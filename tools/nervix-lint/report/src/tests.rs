//! Pure current-shape report, source contract, API identity and aggregation properties.

use std::collections::BTreeMap;

use meticulous::{OptionExt as _, ResultExt as _};

use super::*;

fn report(seed: &[u8; 32]) -> CompilerReport {
    let text = format!("symbol_{}", seed[0]);
    let source = SourceSpan {
        site: SiteId {
            path: "src/owner.rs".into(),
            start: u32::from(seed[1]),
        },
        end: u32::from(seed[1]) + u32::from(seed[2]),
        line: u32::from(seed[3]) + 1,
        column: u32::from(seed[4]),
        end_line: u32::from(seed[3]) + 1,
        end_column: u32::from(seed[4]) + u32::from(seed[2]),
        source: text.clone(),
    };
    let mut expansions = Vec::new();
    if seed[5].is_multiple_of(2) {
        expansions.push(Expansion {
            macro_name: text.clone(),
            call_site: text.clone(),
            definition: Some(text.clone()),
        });
    }
    CompilerReport {
        compiler: text.clone(),
        identity: text.clone(),
        configuration: "ordinary".into(),
        crate_name: text.clone(),
        crate_source: "src/lib.rs".into(),
        arguments: vec![text.clone()],
        findings: vec![Finding {
            rule: "data_plane_lock_acquisitions".into(),
            receiver: text.clone(),
            receiver_type: text.clone(),
            receiver_expression: text.clone(),
            operation: "get".into(),
            definition: text.clone(),
            owner: "example::owner".into(),
            context: seed[13].is_multiple_of(2).then(|| context(seed)),
            acquisition: match seed[10] % 4 {
                0 => Acquisition::Shared,
                1 => Acquisition::Exclusive,
                2 => Acquisition::TryShared,
                _ => Acquisition::TryExclusive,
            },
            span: source,
            expansion: expansions,
        }],
        excluded_generated: u32::from(seed[6]),
        excluded_external: u32::from(seed[7]),
        complete: true,
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
fn bolero_compiler_reports_round_trip() {
    bolero::check!().with_type::<[u8; 32]>().for_each(|seed| {
        let original = report(seed);
        let bytes = serde_json::to_vec(&original)
            .assured("the property constructs bounded, complete current values");
        let decoded: CompilerReport = serde_json::from_slice(&bytes)
            .assured("the property constructs bounded, complete current values");
        assert_eq!(decoded, original);
        let encoded = serde_json::to_value(&original.findings[0])
            .assured("a current finding has its complete field set");
        let fields = encoded.as_object().assured("findings encode as objects");
        for field in fields.keys() {
            let mut incomplete = fields.clone();
            incomplete.remove(field);
            let result = serde_json::from_value::<Finding>(serde_json::Value::Object(incomplete));
            assert!(
                result.is_err(),
                "every current finding field is required: {field}"
            );
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
        let ordinary = report(seed);
        let mut modeled = ordinary.clone();
        modeled.configuration = "shuttle".into();
        modeled.findings[0].receiver = "selected modeled receiver".into();
        modeled.findings[0].owner = "example::modeled_callback".into();
        let reports = vec![ordinary.clone(), ordinary.clone(), modeled.clone()];
        let aggregated =
            aggregate(&reports).assured("the property constructs bounded, complete current values");
        assert_eq!(aggregated.len(), 1);
        assert_eq!(aggregated[0].site, ordinary.findings[0].span.site);
        assert_eq!(
            aggregated[0].configurations,
            BTreeMap::from([
                (
                    format!("ordinary:{}", ordinary.crate_name),
                    vec![ordinary.findings[0].clone()]
                ),
                (
                    format!("shuttle:{}", modeled.crate_name),
                    vec![modeled.findings[0].clone()]
                ),
            ])
        );
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
fn completion_and_expansion_instances_are_preserved() {
    let first = report(&[0; 32]);
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
    conflicting.findings[0].acquisition = Acquisition::Exclusive;
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
