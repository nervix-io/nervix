//! Repository assertion harness, outside the product layer order.
//! Owns: the audit fixture's complete resolved-pattern assertions.
//! Depends on: compiler-prepared current reports.
//! Must not know: compiler execution, Git attribution or host state.

use super::*;

fn candidates(name: &str) -> Vec<Value> {
    let case = Case::new(name);
    case.allowed();
    let report: nervix_lint_report::CompilerReport =
        nervix_ratchet::io::read(&case.directory.join("contract.json"))
            .assured("the compiler prepared the current audit report");
    assert!(report.complete);
    report
        .audit
        .into_iter()
        .map(|candidate| {
            serde_json::to_value(candidate).assured("a typed audit candidate serializes")
        })
        .collect()
}

fn by_owner<'a>(candidates: &'a [Value], kind: &str) -> BTreeMap<String, Vec<&'a Value>> {
    let mut owners = BTreeMap::<String, Vec<&Value>>::new();
    for candidate in candidates {
        if candidate["pattern"]["kind"].as_str() != Some(kind) {
            continue;
        }
        let owner = candidate["owner"]
            .as_str()
            .assured("an audit candidate names its owner")
            .rsplit("::")
            .next()
            .assured("splitting a string always yields a final segment");
        owners.entry(owner.into()).or_default().push(candidate);
    }
    owners
}

#[test]
fn repeated_lookups_follow_the_resolved_receiver_key_and_branches() {
    let candidates = candidates("audit_lookups");
    let lookups = by_owner(&candidates, "repeated_lookup");
    assert_eq!(
        lookups.keys().map(String::as_str).collect::<Vec<_>>(),
        ["admit", "qualified", "shared", "total"]
    );
    let admit = lookups["admit"][0];
    assert_eq!(
        admit["pattern"]["collection"],
        "alloc::collections::btree::map::BTreeMap"
    );
    assert_eq!(admit["pattern"]["receiver"], "self.panes");
    assert_eq!(admit["pattern"]["key"], "pane");
    assert_eq!(admit["pattern"]["guarded"], true);
    let operations = admit["pattern"]["operations"]
        .as_array()
        .assured("the repeated lookup records every operation");
    assert_eq!(
        operations
            .iter()
            .map(|operation| operation["operation"].as_str())
            .collect::<Vec<_>>(),
        [Some("get_mut"), Some("insert")]
    );
    assert_eq!(
        operations
            .iter()
            .map(|operation| operation["span"]["source"].as_str())
            .collect::<Vec<_>>(),
        [
            Some("self.panes.get_mut(&pane)"),
            Some("self.panes.insert(pane, values)")
        ]
    );
    assert_eq!(admit["context"]["context"], "recurring");
    let total = &lookups["total"][0]["pattern"];
    assert_eq!(
        total["operations"]
            .as_array()
            .assured("the total lookup records its operations")
            .iter()
            .map(|operation| operation["operation"].as_str())
            .collect::<Vec<_>>(),
        [Some("contains_key"), Some("index")]
    );
    assert_eq!(total["guarded"], true);
    assert_eq!(
        lookups["shared"][0]["pattern"]["collection"],
        "dashmap::DashMap"
    );
    assert_eq!(lookups["shared"][0]["context"]["context"], "lifecycle");
    assert_eq!(
        lookups["qualified"][0]["pattern"]["collection"],
        "std::collections::hash::map::HashMap"
    );
    let duplicated = by_owner(&candidates, "duplicated_branch");
    assert_eq!(
        duplicated.keys().map(String::as_str).collect::<Vec<_>>(),
        ["admit"]
    );
    assert_eq!(
        duplicated["admit"][0]["pattern"]["statement"],
        "values.push(value)"
    );
    assert_eq!(
        duplicated["admit"][0]["pattern"]["occurrences"]
            .as_array()
            .assured("the duplicate retains its occurrences")
            .len(),
        2
    );
}

#[test]
fn allocations_and_borrowed_copies_keep_their_resolved_types() {
    let candidates = candidates("audit_allocations");
    let allocations = by_owner(&candidates, "key_allocation");
    assert_eq!(
        allocations.keys().map(String::as_str).collect::<Vec<_>>(),
        ["counted", "formatted"]
    );
    assert_eq!(
        allocations["formatted"][0]["pattern"]["operation"],
        "contains_key"
    );
    assert_eq!(
        allocations["formatted"][0]["pattern"]["allocation"],
        "to_string"
    );
    assert_eq!(
        allocations["formatted"][0]["pattern"]["key_type"],
        "std::string::String"
    );
    assert_eq!(allocations["counted"][0]["pattern"]["operation"], "entry");
    assert_eq!(allocations["counted"][0]["pattern"]["allocation"], "clone");
    let copies = by_owner(&candidates, "borrowed_copy");
    assert_eq!(
        copies.keys().map(String::as_str).collect::<Vec<_>>(),
        ["measured"]
    );
    let mut operations: Vec<_> = copies["measured"]
        .iter()
        .map(|candidate| {
            candidate["pattern"]["operation"]
                .as_str()
                .assured("a borrowed copy names its operation")
        })
        .collect();
    operations.sort();
    assert_eq!(operations, ["clone", "to_owned", "to_string", "to_string"]);
    let mut stringified = Vec::new();
    for candidate in &copies["measured"] {
        if candidate["pattern"]["operation"] == "to_string" {
            stringified.push(
                candidate["pattern"]["value_type"]
                    .as_str()
                    .assured("a stringification names its resolved type"),
            );
        }
    }
    assert_eq!(stringified, ["std::string::String", "std::string::String"]);
    for kind in ["repeated_lookup", "discarded_error", "disguised_clamp"] {
        assert!(!by_owner(&candidates, kind).contains_key("borrowed"));
    }
}

#[test]
fn discarded_failures_and_disguised_clamps_keep_their_information_controls() {
    let candidates = candidates("audit_errors");
    let discarded = by_owner(&candidates, "discarded_error");
    assert_eq!(
        discarded.keys().map(String::as_str).collect::<Vec<_>>(),
        ["dropped"]
    );
    let mut operations: Vec<_> = discarded["dropped"]
        .iter()
        .map(|candidate| {
            candidate["pattern"]["operation"]
                .as_str()
                .assured("a discarded error records its operation")
        })
        .collect();
    operations.sort();
    assert_eq!(
        operations,
        [
            "Result::map_err",
            "Result::ok",
            "Result::unwrap_or_default",
            "if let Ok",
            "let Ok else",
            "match Err(_)"
        ]
    );
    for candidate in &discarded["dropped"] {
        assert_eq!(
            candidate["pattern"]["error_type"],
            "std::num::ParseIntError"
        );
        assert_eq!(candidate["pattern"]["information"], "value");
    }
    let clamps = by_owner(&candidates, "disguised_clamp");
    assert_eq!(
        clamps.keys().map(String::as_str).collect::<Vec<_>>(),
        ["clamped"]
    );
    let mut operations = BTreeMap::new();
    for candidate in &clamps["clamped"] {
        operations.insert(
            candidate["pattern"]["operation"]
                .as_str()
                .assured("a clamp names its operation"),
            candidate["pattern"]["fallback"]
                .as_str()
                .assured("a clamp records its fallback"),
        );
    }
    assert_eq!(
        operations,
        BTreeMap::from([("checked_sub", "0"), ("try_from", "u64::MAX")])
    );
}

#[test]
fn duplicate_branches_cover_every_exhaustive_arm_and_exact_statement() {
    let candidates = candidates("audit_branches");
    let duplicated = by_owner(&candidates, "duplicated_branch");
    assert_eq!(
        duplicated.keys().map(String::as_str).collect::<Vec<_>>(),
        ["classify", "record"]
    );
    assert_eq!(duplicated["record"][0]["pattern"]["construct"], "if");
    assert_eq!(
        duplicated["record"][0]["pattern"]["statement"],
        "self.entries.push(value)"
    );
    assert_eq!(duplicated["classify"][0]["pattern"]["construct"], "match");
    assert_eq!(
        duplicated["classify"][0]["pattern"]["occurrences"]
            .as_array()
            .assured("every duplicate occurrence is recorded")
            .len(),
        3
    );
}

#[test]
fn forwarding_wrappers_keep_parameter_identity_and_resolved_implementation() {
    let candidates = candidates("audit_wrappers");
    let wrappers = by_owner(&candidates, "forwarding_wrapper");
    assert_eq!(
        wrappers.keys().map(String::as_str).collect::<Vec<_>>(),
        ["combined", "push"]
    );
    assert!(
        wrappers["push"][0]["pattern"]["callee"]
            .as_str()
            .assured("the wrapper names its resolved callee")
            .ends_with("::append")
    );
    assert_eq!(wrappers["push"][0]["pattern"]["callers"], 1);
    assert_eq!(wrappers["push"][0]["pattern"]["implementation"], false);
    assert_eq!(wrappers["push"][0]["pattern"]["relocated"], false);
    assert_eq!(wrappers["push"][0]["pattern"]["contract"], false);
    assert!(
        wrappers["combined"][0]["pattern"]["callee"]
            .as_str()
            .assured("the wrapper names its resolved callee")
            .ends_with("::combine")
    );
    assert_eq!(wrappers["combined"][0]["pattern"]["callers"], 1);
}
