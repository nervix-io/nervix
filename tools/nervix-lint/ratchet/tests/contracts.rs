//! Repository assertion harness, outside the product layer order.
//! Owns: the semantic fixture, diagnostic span and metadata assertions.
//! Depends on: evidence prepared by the canonical recipes and current source bytes.
//! Must not know: compiler/Cargo execution or host scheduling.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_ratchet::evidence::Ending;
use serde_json::Value;

#[path = "contracts/catalog.rs"]
mod catalog;

#[path = "contracts/audit.rs"]
mod audit;

struct Case {
    directory: PathBuf,
}

impl Case {
    fn new(name: &str) -> Self {
        Self::mode(name, "ordinary")
    }

    fn mode(name: &str, mode: &str) -> Self {
        let target = std::env::var_os("NERVIX_RATCHET_TARGET_DIR")
            .assured("the preparation recipe supplies the evidence root");
        let name = if mode == "ordinary" {
            name.to_owned()
        } else {
            format!("{name}-{mode}")
        };
        Self {
            directory: PathBuf::from(target)
                .join("typed-ratchet/contracts")
                .join(name),
        }
    }

    fn ending(&self) -> Ending {
        nervix_ratchet::io::read(&self.directory.join("ending.json"))
            .assured("the recipe prepared this fixture's ending")
    }
    fn stderr(&self) -> String {
        fs::read_to_string(self.directory.join("stderr"))
            .assured("the recipe retained the complete diagnostics")
    }

    fn allowed(&self) {
        assert!(self.ending().succeeded(), "{}", self.stderr());
    }

    fn refused(&self, diagnostic: &str) {
        assert!(
            !self.ending().succeeded(),
            "the refused fixture unexpectedly compiled"
        );
        assert!(
            self.stderr()
                .replace('-', "_")
                .contains(&diagnostic.replace('-', "_")),
            "{}",
            self.stderr()
        );
        assert!(
            !self.stderr().contains("panicked at"),
            "the fixture must fail by its expected diagnostic"
        );
    }

    fn diagnostics(&self, code: &str) -> Vec<Value> {
        let mut diagnostics = Vec::new();
        for line in self.stderr().lines() {
            if !line.starts_with('{') {
                continue;
            }
            let value: Value =
                serde_json::from_str(line).assured("a structured rustc diagnostic is valid JSON");
            if value["code"]["code"].as_str() == Some(code) {
                diagnostics.push(value);
            }
        }
        diagnostics
    }

    fn snippets(&self, code: &str) -> Vec<String> {
        let mut snippets = Vec::new();
        for diagnostic in self.diagnostics(code) {
            let spans = diagnostic["spans"]
                .as_array()
                .assured("a rustc diagnostic has its span array");
            let primary: Vec<_> = spans
                .iter()
                .filter(|span| span["is_primary"] == true)
                .collect();
            assert_eq!(primary.len(), 1);
            let span = primary[0];
            let path = PathBuf::from(
                span["file_name"]
                    .as_str()
                    .assured("the primary span names its source"),
            );
            let source = fs::read(path).assured("the compiler's authored source remains available");
            let start = usize::try_from(
                span["byte_start"]
                    .as_u64()
                    .assured("a span has its byte start"),
            )
            .assured("the source fits this host's address coordinate");
            let end = usize::try_from(span["byte_end"].as_u64().assured("a span has its byte end"))
                .assured("the source fits this host's address coordinate");
            assert!(end > start);
            snippets.push(
                String::from_utf8(source[start..end].to_vec())
                    .assured("the primary span contains whole UTF-8 tokens"),
            );
        }
        snippets.sort();
        snippets
    }
}

#[test]
fn allowed_controls_and_required_refusals_keep_their_exact_verdicts() {
    for name in catalog::ALLOWED {
        Case::new(name).allowed();
    }
    for refusal in catalog::REFUSED {
        Case::new(refusal.name).refused(refusal.diagnostic);
    }
}

#[test]
fn fixed_contract_diagnostics_keep_cardinality_and_primary_lines() {
    let case = Case::new("constant_invalid");
    case.refused("nervix::invalid_contract");
    let findings = case.diagnostics("nervix::invalid_contract");
    assert_eq!(findings.len(), 5);
    let mut lines = BTreeSet::new();
    for finding in findings {
        for span in finding["spans"]
            .as_array()
            .assured("the diagnostic has spans")
        {
            if span["is_primary"] == true {
                lines.insert(
                    span["line_start"]
                        .as_u64()
                        .assured("the primary line is present"),
                );
            }
        }
    }
    assert_eq!(lines, BTreeSet::from([3, 6, 9, 12, 13]));
}

#[test]
fn arithmetic_combinator_and_invalid_contract_spans_keep_their_exact_operations() {
    assert_eq!(
        Case::new("api_arithmetic").snippets("nervix::clamped_arithmetic"),
        ["saturating_mul", "wrapping_add"]
    );
    assert_eq!(
        Case::new("api_combinator").snippets("nervix::combinator_control_flow"),
        [
            "Option::unwrap_or(Option::map(value, |item| item + 1), 0)",
            "value.map(|item| { let next = item + 1; next })"
        ]
    );
    assert_eq!(
        Case::new("api_invalid")
            .diagnostics("nervix::invalid_contract")
            .len(),
        2
    );
}

#[test]
fn informative_error_replacements_keep_every_primary_span() {
    let case = Case::new("error_replacement");
    case.refused("nervix::replaced_error");
    let snippets = case.snippets("nervix::replaced_error");
    assert_eq!(snippets.len(), 16);
    for snippet in snippets {
        assert!(
            [
                "map_err",
                "or_else",
                "unwrap_or_else",
                "Err(_)",
                "Ok(value)"
            ]
            .iter()
            .any(|token| snippet.contains(token)),
            "{snippet}"
        );
    }
}

#[test]
fn report_fields_are_diagnosed_at_the_whole_authored_value() {
    assert_eq!(
        Case::new("error_report_field").snippets("nervix::partial_report_field"),
        [
            "boxed",
            "current",
            "error",
            "failure",
            "self.held",
            "shared",
            "wrapped"
        ]
    );
}

#[test]
fn foreign_items_take_the_contract_of_their_actual_owning_module() {
    let case = Case::new("architecture_foreign_use");
    case.refused("nervix::parser_dependency");
    assert_eq!(
        case.snippets("nervix::parser_dependency"),
        [
            "callee::language::parse_foreign",
            "callee::language::parse_foreign()"
        ]
    );
    assert_eq!(
        case.snippets("nervix::lifecycle_call"),
        ["callee::installation::install_foreign()"]
    );
}

#[test]
fn metadata_aliases_and_source_moves_keep_their_contracts() {
    for (name, diagnostic) in [
        ("architecture_metadata_use", "nervix::model_in_data_plane"),
        (
            "error_replacement_metadata_rejected",
            "nervix::replaced_error",
        ),
        ("error_metadata_rejected", "nervix::bare_error_signature"),
        ("cross_crate_caller", "nervix::lifecycle_call"),
        ("changed_source_hot", "nervix::sync_acquisition"),
        ("changed_source_moved", "nervix::sync_acquisition"),
        ("changed_caller_refused", "nervix::lifecycle_call"),
    ] {
        Case::new(name).refused(diagnostic);
    }
    Case::new("architecture_metadata_use").refused("nervix::parser_dependency");
    assert!(
        Case::new("changed_source_moved")
            .stderr()
            .contains("relocated.rs")
    );
}

#[test]
fn explicit_warn_deny_and_forbidden_caps_keep_normal_lint_policy() {
    let warned = Case::new("recurring_warn");
    warned.allowed();
    let diagnostics = warned.diagnostics("nervix::sync_acquisition");
    assert!(!diagnostics.is_empty());
    assert!(
        diagnostics
            .iter()
            .all(|diagnostic| diagnostic["level"] == "warning")
    );
    Case::new("recurring_deny").refused("nervix::sync_acquisition");
    for name in ["recurring_allow", "warnings_allow", "cap_allow", "cap_warn"] {
        Case::new(name).refused("rejects lint caps and blanket allow");
    }
}
