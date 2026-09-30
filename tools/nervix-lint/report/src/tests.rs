//! Pure current-shape report, catalog, scope and aggregation properties.

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

fn scope(report: &CompilerReport, seed: &[u8; 32]) -> Scope {
    let finding = &report.findings[0];
    let disposition = match seed[8] % 6 {
        0 => Disposition::Lifecycle,
        1 => Disposition::Observer,
        2 => Disposition::RetainedState,
        3 => Disposition::BoundedProtocol {
            key: "attempt identity".into(),
            bound: "one admitted attempt".into(),
        },
        4 => Disposition::OutsideDataPlane {
            boundary: "client session".into(),
        },
        _ => Disposition::Debt {
            delivery: "owner repair".into(),
        },
    };
    Scope {
        site: finding.span.site.clone(),
        source_sha256: "qualified source hash".into(),
        operation: finding.operation.clone(),
        owners: BTreeSet::from([finding.owner.clone()]),
        frequency: "per batch".into(),
        rationale: "reviewed owner and call chain".into(),
        disposition,
    }
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
    });
}

#[test]
fn bolero_catalog_and_scopes_round_trip() {
    bolero::check!().with_type::<[u8; 32]>().for_each(|seed| {
        let original_report = report(seed);
        let original_scope = scope(&original_report, seed);
        let bytes = serde_json::to_vec(&vec![original_scope.clone()])
            .assured("the property constructs bounded, complete current values");
        let parsed = parse_scopes(&bytes)
            .assured("the property constructs bounded, complete current values");
        assert_eq!(parsed.get(&original_scope.site), Some(&original_scope));
        let catalog = Catalog {
            apis: vec![Api {
                receivers: vec![format!("receiver_{}", seed[9])],
                defining_crates: vec!["engine".into()],
                operations: BTreeMap::from([(
                    "get".into(),
                    original_report.findings[0].acquisition,
                )]),
                rationale: "read guard".into(),
            }],
        };
        let bytes = serde_json::to_vec(&catalog)
            .assured("the property constructs bounded, complete current values");
        let decoded = Catalog::parse(&bytes)
            .assured("the property constructs bounded, complete current values");
        assert_eq!(decoded, catalog);
        assert_eq!(
            decoded.acquisition(&catalog.apis[0].receivers[0], "engine", "get"),
            Some(original_report.findings[0].acquisition)
        );
        assert_eq!(
            decoded.acquisition(&catalog.apis[0].receivers[0], "custom", "get"),
            None
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
        let mut reviewed = scope(&ordinary, seed);
        reviewed.owners.insert(modeled.findings[0].owner.clone());
        let input = PolicyInput {
            reports,
            scopes: vec![reviewed.clone()],
            source_sha256: BTreeMap::from([(
                reviewed.site.path.clone(),
                reviewed.source_sha256.clone(),
            )]),
        };
        let classified = input
            .classify()
            .assured("the property constructs bounded, complete current values");
        assert_eq!(classified.len(), 1);
        assert_eq!(classified[0].scope, reviewed);
    });
}

#[test]
fn malformed_catalog_and_scope_are_rejected() {
    assert!(Catalog::parse(b"{}").is_err());
    let seed = [0; 32];
    let report = report(&seed);
    let mut reviewed = scope(&report, &seed);
    reviewed.rationale.clear();
    assert!(reviewed.validate().is_err());
    reviewed.rationale = "a bounded acquisition".into();
    reviewed.disposition = Disposition::BoundedProtocol {
        key: String::new(),
        bound: "one attempt".into(),
    };
    assert!(reviewed.validate().is_err());
}

#[test]
fn catalog_definition_identity_and_duplicate_contracts() {
    let api = Api {
        receivers: vec!["engine::Map".into()],
        defining_crates: vec!["engine".into()],
        operations: BTreeMap::from([("get".into(), Acquisition::Shared)]),
        rationale: "borrowed read guard".into(),
    };
    let catalog = Catalog {
        apis: vec![api.clone()],
    };
    assert_eq!(catalog.acquisition("owned::Map", "engine", "get"), None);
    assert_eq!(catalog.acquisition("engine::Map", "engine", "custom"), None);
    let repeated = Catalog {
        apis: vec![api.clone(), api],
    };
    let bytes =
        serde_json::to_vec(&repeated).assured("the current catalog has only serializable fields");
    assert!(matches!(
        Catalog::parse(&bytes)
            .err()
            .assured("the catalog repeats one API")
            .current_context(),
        ReportError::DuplicateApi { .. }
    ));
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

#[test]
fn every_acquisition_requires_a_current_reviewed_scope() {
    let compiler = report(&[0; 32]);
    let reviewed = scope(&compiler, &[0; 32]);
    let mut input = PolicyInput {
        reports: vec![compiler.clone()],
        scopes: vec![],
        source_sha256: BTreeMap::from([(
            reviewed.site.path.clone(),
            reviewed.source_sha256.clone(),
        )]),
    };
    assert!(matches!(
        input
            .classify()
            .err()
            .assured("a new authored acquisition has no review")
            .current_context(),
        ReportError::MissingScope { .. }
    ));
    input.scopes = vec![reviewed.clone(), reviewed.clone()];
    assert!(matches!(
        input
            .classify()
            .err()
            .assured("the authored scope is duplicated")
            .current_context(),
        ReportError::DuplicateScope { .. }
    ));
    input.scopes = vec![reviewed.clone()];
    input.scopes[0].owners = BTreeSet::from(["example::another_owner".into()]);
    assert!(matches!(
        input
            .classify()
            .err()
            .assured("the reviewed owner changed")
            .current_context(),
        ReportError::StaleScope { .. }
    ));
    input.scopes = vec![reviewed.clone()];
    input.source_sha256.clear();
    assert!(matches!(
        input
            .classify()
            .err()
            .assured("the source fingerprint is missing")
            .current_context(),
        ReportError::StaleScope { .. }
    ));
    input
        .source_sha256
        .insert(reviewed.site.path.clone(), reviewed.source_sha256.clone());
    input.scopes[0].operation = "write".into();
    assert!(matches!(
        input
            .classify()
            .err()
            .assured("the operation no longer matches its review")
            .current_context(),
        ReportError::StaleScope { .. }
    ));
    input.scopes = vec![reviewed.clone()];
    input.reports.clear();
    assert!(matches!(
        input
            .classify()
            .err()
            .assured("the declared acquisition was not compiled")
            .current_context(),
        ReportError::UnobservedScope { .. }
    ));
    input.reports.push(compiler);
    let sites = input
        .classify()
        .assured("the current authored site matches its review");
    assert_eq!(sites[0].scope, reviewed);
    assert!(!sites[0].scope.disposition.is_debt());
    assert!(
        Disposition::Debt {
            delivery: "owner repair".into()
        }
        .is_debt()
    );
}

#[test]
fn scope_encoding_and_each_required_review_field_are_checked() {
    assert!(parse_scopes(b"{}").is_err());
    for field in 0..3 {
        let compiler = report(&[0; 32]);
        let mut reviewed = scope(&compiler, &[0; 32]);
        match field {
            0 => reviewed.owners.clear(),
            1 => reviewed.frequency.clear(),
            _ => reviewed.rationale.clear(),
        }
        assert!(matches!(
            reviewed
                .validate()
                .err()
                .assured("one required review field is empty")
                .current_context(),
            ReportError::IncompleteScope { .. }
        ));
    }
    let compiler = report(&[0; 32]);
    let mut reviewed = scope(&compiler, &[0; 32]);
    reviewed.disposition = Disposition::BoundedProtocol {
        key: "attempt".into(),
        bound: String::new(),
    };
    assert!(matches!(
        reviewed
            .validate()
            .err()
            .assured("the protocol review lacks its bound")
            .current_context(),
        ReportError::UnboundedProtocol { .. }
    ));
    reviewed.disposition = Disposition::OutsideDataPlane {
        boundary: String::new(),
    };
    assert!(matches!(
        reviewed
            .validate()
            .err()
            .assured("the external boundary must be named")
            .current_context(),
        ReportError::IncompleteScope { .. }
    ));
}
