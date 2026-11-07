//! Repository test harness, outside the product layer order.
//! Owns: current candidate identities and provenance over real, private Git histories.
//! Depends on: the production audit owners and current compiler report vocabulary.
//! Must not know: a compiler, Cargo invocation or product execution.

use std::{collections::BTreeMap, fs, path::Path, process::Command};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_lint_report::{
    Acquisition, AuthoredSite, CompilerReport, Context, Finding, SiteId, SourceSpan,
    audit::{AuditCandidate, AuditPattern, ErrorInformation, KeyedOperation},
};
use serde_json::json;
use tempfile::TempDir;

use super::{
    attribution::Attributor,
    candidates::Collector,
    encoding::Encoding,
    git::Git,
    history::History,
    hunks::Hunk,
    report::{AttributedCandidate, Attribution, Report, Summary},
};
use crate::{
    configuration::Configuration,
    evidence::{Completion, MatrixReport},
};

fn span(path: &str, line: u32, source: &str, start: u32) -> SourceSpan {
    let bytes = u32::try_from(source.len()).assured("the fixture source is bounded");
    SourceSpan {
        site: SiteId {
            path: path.into(),
            start,
        },
        end: start
            .checked_add(bytes)
            .assured("the fixture offsets are bounded"),
        line,
        column: 4,
        end_line: line,
        end_column: 4_u32
            .checked_add(bytes)
            .assured("the fixture column is bounded"),
        source: source.into(),
    }
}

fn lookup(path: &str, line: u32, start: u32) -> AuditCandidate {
    AuditCandidate {
        owner: "crate::Panes::admit".into(),
        context: Some(Context::Recurring {
            reason: "every row".into(),
        }),
        span: span(path, line, "self.panes.get_mut(&pane)", start),
        expansion: Vec::new(),
        pattern: AuditPattern::RepeatedLookup {
            collection: "alloc::collections::btree::map::BTreeMap".into(),
            receiver: "self.panes".into(),
            key: "pane".into(),
            guarded: true,
            operations: vec![
                KeyedOperation {
                    operation: "get_mut".into(),
                    span: span(path, line, "self.panes.get_mut(&pane)", start),
                },
                KeyedOperation {
                    operation: "insert".into(),
                    span: span(
                        path,
                        line.checked_add(2).assured("fixture line fits"),
                        "self.panes.insert(pane, values)",
                        start.checked_add(80).assured("fixture offset fits"),
                    ),
                },
            ],
        },
    }
}

fn evidence(configuration: &str, candidates: Vec<AuditCandidate>) -> Completion {
    Completion {
        configuration: Configuration {
            name: configuration.into(),
            manifest: "Cargo.toml".into(),
            packages: vec!["example".into()],
            kinds: vec!["lib".into()],
            features: Vec::new(),
            target: None,
        },
        reports: BTreeMap::from([(
            "crate::lib".into(),
            CompilerReport {
                compiler: "fixture compiler".into(),
                identity: "fixture identity".into(),
                configuration: configuration.into(),
                crate_name: "crate".into(),
                crate_source: "src/lib.rs".into(),
                arguments: Vec::new(),
                findings: Vec::new(),
                audit: candidates,
                excluded_generated: 0,
                excluded_external: 0,
                complete: true,
            },
        )]),
        expected: BTreeMap::new(),
        files: BTreeMap::new(),
        complete: true,
        build_directory: "target/build".into(),
        report_directory: "target/reports".into(),
        cache_namespace: "fixture".into(),
        cargo_log: "target/cargo.jsonl".into(),
        compilations: BTreeMap::new(),
    }
}

#[test]
fn configurations_merge_and_identities_ignore_offsets() {
    let raw = lookup("src/panes.rs", 10, 0);
    let merged = Collector::new()
        .collect(vec![
            evidence("ordinary", vec![raw.clone()]),
            evidence("testing", vec![raw]),
        ])
        .assured("complete current candidates merge");
    assert_eq!(merged.len(), 1);
    assert_eq!(
        merged[0].configurations,
        ["ordinary:crate", "testing:crate"]
    );
    assert_eq!(
        merged[0].contexts,
        [Some(Context::Recurring {
            reason: "every row".into()
        })]
    );
    let moved = Collector::new()
        .collect(vec![evidence(
            "ordinary",
            vec![lookup("src/panes.rs", 40, 900)],
        )])
        .assured("moved current candidates merge");
    assert_eq!(moved[0].id, merged[0].id);
    let mut other = lookup("src/panes.rs", 10, 0);
    other.owner = "crate::Panes::evict".into();
    let other = Collector::new()
        .collect(vec![evidence("ordinary", vec![other])])
        .assured("another owner is a valid current candidate");
    assert_ne!(other[0].id, merged[0].id);
}

#[test]
fn repeated_patterns_in_one_owner_keep_distinct_identities() {
    let merged = Collector::new()
        .collect(vec![evidence(
            "ordinary",
            vec![
                lookup("src/panes.rs", 10, 0),
                lookup("src/panes.rs", 30, 500),
            ],
        )])
        .assured("distinct sites merge");
    assert_eq!(merged.len(), 2);
    assert_eq!(merged[1].id, format!("{}-1", merged[0].id));
}

#[test]
fn identity_json_keeps_ascii_escapes_utf16_pairs_and_separator_spaces() {
    let value = json!({"letter":"λ", "emoji":"😀", "del":"\u{7f}", "controls":"\t\n", "slash":"/"});
    let encoded = Encoding::new()
        .identity(&value)
        .assured("current identity values encode");
    assert_eq!(
        encoded,
        r#"{"controls": "\t\n", "del": "\u007f", "emoji": "\ud83d\ude00", "letter": "\u03bb", "slash": "/"}"#
    );
}

#[test]
fn an_excerpt_keeps_the_lines_around_the_candidate_and_the_shorter_sides_end() {
    let written = Hunk {
        start: 1,
        length: 200,
        before: Vec::new(),
        after: (1..=200).map(|number| format!("line {number}")).collect(),
    };
    assert_eq!(
        written.excerpt(100, 2).after,
        ["line 98", "line 99", "line 100", "line 101", "line 102"]
    );
    let replaced = Hunk {
        start: 10,
        length: 3,
        before: vec!["initial a".into(), "initial b".into()],
        after: vec!["current a".into(), "current b".into(), "current c".into()],
    };
    assert_eq!(replaced.excerpt(11, 2).before, ["initial a", "initial b"]);
    assert_eq!(
        replaced.excerpt(11, 2).after,
        ["current a", "current b", "current c"]
    );
    let shorter = Hunk {
        before: (1..=10).map(|number| format!("initial {number}")).collect(),
        ..written
    };
    assert_eq!(
        shorter.excerpt(150, 2).before,
        [
            "initial 6",
            "initial 7",
            "initial 8",
            "initial 9",
            "initial 10"
        ]
    );
}

struct Repository {
    directory: TempDir,
}

impl Repository {
    fn new() -> Self {
        let repository = Self {
            directory: tempfile::tempdir().assured("private audit fixture opens"),
        };
        repository.command(&["init", "--quiet", "--initial-branch=main"], None);
        fs::create_dir_all(repository.root().join("src/runtime"))
            .assured("fixture source directories open");
        repository.commit(
            "2026-09-01",
            "initial code",
            &[(
                "src/runtime/initial.rs",
                "fn initial(map: &mut M) {\n    map.insert(1, 2);\n    map.get(&1);\n}\n",
            )],
        );
        repository.commit(
            "2026-09-14",
            "count lock acquisitions",
            &[
                (
                    "debt-baseline.json",
                    r#"{"data_plane_lock_acquisitions":3}"#,
                ),
                (
                    "src/runtime/panes.rs",
                    "fn admit(map: &mut M, pane: u64) {\n    \
                     map.entry(pane).or_default().push(1);\n}\n",
                ),
            ],
        );
        repository.commit(
            "2026-09-20",
            "lower the lock count",
            &[
                (
                    "debt-baseline.json",
                    r#"{"data_plane_lock_acquisitions":2}"#,
                ),
                (
                    "src/runtime/panes.rs",
                    "fn admit(map: &mut M, pane: u64) {\n    if let Some(values) = \
                     map.get_mut(&pane) {\n        values.push(1);\n    } else {\n        \
                     map.insert(pane, vec![1]);\n    }\n}\n",
                ),
                (
                    "src/registry.rs",
                    "fn helper(shared: &Shared) {\n    shared.lock();\n}\n",
                ),
            ],
        );
        repository.commit(
            "2026-09-25",
            "unrelated feature",
            &[(
                "src/later.rs",
                "fn later(map: &mut M) {\n    map.remove(&1);\n    map.insert(1, 2);\n}\n",
            )],
        );
        repository
    }

    fn root(&self) -> &Path {
        self.directory.path()
    }

    fn command(&self, arguments: &[&str], date: Option<&str>) -> String {
        let mut command = Command::new("git");
        command.args(arguments).current_dir(self.root());
        command.envs([
            ("GIT_AUTHOR_NAME", "Audit"),
            ("GIT_AUTHOR_EMAIL", "audit@example.invalid"),
            ("GIT_COMMITTER_NAME", "Audit"),
            ("GIT_COMMITTER_EMAIL", "audit@example.invalid"),
        ]);
        if let Some(date) = date {
            let date = format!("{date}T12:00:00+00:00");
            command
                .env("GIT_AUTHOR_DATE", &date)
                .env("GIT_COMMITTER_DATE", date);
        }
        let output = command.output().assured("fixture Git runs");
        assert!(
            output.status.success(),
            "fixture Git {arguments:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).assured("fixture Git emits UTF-8")
    }

    fn commit(&self, date: &str, subject: &str, files: &[(&str, &str)]) {
        for (name, content) in files {
            fs::write(self.root().join(name), content).assured("fixture source writes");
        }
        self.command(&["add", "--all"], None);
        self.command(&["commit", "--quiet", "-m", subject], Some(date));
    }

    fn candidate(&self, path: &str, first: u32, second: u32) -> super::candidates::Candidate {
        let lines = fs::read_to_string(self.root().join(path)).assured("fixture source reads");
        let lines: Vec<_> = lines.lines().collect();
        let first_index =
            usize::try_from(first.checked_sub(1).assured("fixture lines start at one"))
                .assured("fixture line index fits");
        let second_index =
            usize::try_from(second.checked_sub(1).assured("fixture lines start at one"))
                .assured("fixture line index fits");
        let mut raw = lookup(path, first, 0);
        raw.span = span(path, first, lines[first_index].trim(), 0);
        if let AuditPattern::RepeatedLookup { operations, .. } = &mut raw.pattern {
            operations[0].span = raw.span.clone();
            operations[1].span = span(path, second, lines[second_index].trim(), 80);
        }
        Collector::new()
            .collect(vec![evidence("ordinary", vec![raw])])
            .assured("fixture candidate collects")
            .pop()
            .assured("one fixture candidate exists")
    }

    fn attribute(&self, candidate: &super::candidates::Candidate) -> Attribution {
        let mut attribution =
            Attributor::new(Git::new(self.root()), false).assured("fixture history loads");
        attribution
            .prepare(std::slice::from_ref(candidate))
            .assured("fixture provenance loads");
        attribution
            .attribute(candidate)
            .assured("fixture provenance attributes")
    }
}

#[test]
fn history_records_introductions_and_lowerings() {
    let repository = Repository::new();
    let history =
        History::load(&Git::new(repository.root())).assured("fixture baseline history loads");
    let introduced = &history.introduced["data_plane_lock_acquisitions"];
    assert_eq!(introduced.date, "2026-09-14");
    assert_eq!(introduced.subject, "count lock acquisitions");
    let lowering = history
        .commits
        .values()
        .find(|commit| commit.subject == "lower the lock count")
        .assured("fixture lowering exists");
    assert_eq!(
        history.event(&lowering.sha, "data_plane_lock_acquisitions"),
        Some("lowered 3->2".into())
    );
    assert!(history.lowered(&lowering.sha, "data_plane_lock_acquisitions"));
    assert!(history.predates("2026-09-01", "data_plane_lock_acquisitions"));
    assert!(!history.predates("2026-09-20", "data_plane_lock_acquisitions"));
}

#[test]
fn lines_from_one_commit_anchor_the_excerpt_at_the_first_candidate_line() {
    let repository = Repository::new();
    let lines: Vec<_> = (1..=80)
        .map(|number| format!("statement {number}\n"))
        .collect();
    let source = lines.concat();
    repository.commit(
        "2026-09-26",
        "add a multi-line candidate",
        &[("src/long.rs", &source)],
    );
    let candidate = repository.candidate("src/long.rs", 30, 60);
    let result = repository.attribute(&candidate);
    let data = result
        .data()
        .assured("the complete candidate was committed");
    let expected: Vec<_> = (18..=42)
        .map(|number| format!("statement {number}"))
        .collect();
    assert_eq!(data.after, expected);
}

#[test]
fn a_lowering_that_removed_the_governed_spelling_is_suspected() {
    let repository = Repository::new();
    let result = repository.attribute(&repository.candidate("src/runtime/panes.rs", 2, 5));
    let value = serde_json::to_value(result).assured("attribution serializes");
    assert_eq!(value["suggestion"], "suspected");
    assert_eq!(value["commit"]["subject"], "lower the lock count");
    assert_eq!(
        value["before"],
        json!(["    map.entry(pane).or_default().push(1);"])
    );
    assert!(
        value["after"]
            .as_array()
            .assured("after is an array")
            .contains(&json!("        map.insert(pane, vec![1]);"))
    );
    let rule = &value["rules"][0];
    assert_eq!(rule["event"], "lowered 3->2");
    assert_eq!(rule["spelling_removed"], true);
    assert_eq!(rule["predates_rule"], false);
}

#[test]
fn code_before_the_rule_and_unrelated_later_code_keep_their_suggestions() {
    let repository = Repository::new();
    for (path, expected) in [
        ("src/runtime/initial.rs", "pre-existing"),
        ("src/later.rs", "unattributed"),
    ] {
        let result = repository.attribute(&repository.candidate(path, 2, 3));
        assert_eq!(
            serde_json::to_value(result).assured("attribution serializes")["suggestion"],
            expected
        );
    }
}

#[test]
fn uncommitted_lines_have_no_attribution() {
    let repository = Repository::new();
    let path = repository.root().join("src/later.rs");
    let source = fs::read_to_string(&path).assured("fixture source reads");
    fs::write(
        path,
        source.replace("map.remove(&1);", "map.remove(&1).is_some();"),
    )
    .assured("fixture edit writes");
    let result = repository.attribute(&repository.candidate("src/later.rs", 2, 3));
    assert_eq!(
        serde_json::to_value(result).assured("attribution serializes"),
        json!({"suggestion":"uncommitted", "commit":null, "rules":[]})
    );
}

#[test]
fn a_scope_move_can_explain_a_lowering_from_another_file() {
    let repository = Repository::new();
    repository.commit(
        "2026-09-26",
        "count an entry",
        &[
            (
                "debt-baseline.json",
                r#"{"data_plane_lock_acquisitions":3}"#,
            ),
            (
                "src/runtime/initial.rs",
                "fn initial(map: &mut M) {\n    map.entry(1).or_insert(2);\n}\n",
            ),
        ],
    );
    repository.commit(
        "2026-09-27",
        "port offsets",
        &[
            (
                "debt-baseline.json",
                r#"{"data_plane_lock_acquisitions":2}"#,
            ),
            (
                "src/runtime/initial.rs",
                "fn initial(map: &mut M) {\n    map.insert(1, 2);\n}\n",
            ),
            (
                "src/runtime/ported.rs",
                "fn ported(map: &mut M, key: u64) {\n    if let Some(value) = map.get_mut(&key) \
                 {\n        *value += 1;\n    } else {\n        map.insert(key, 1);\n    }\n}\n",
            ),
        ],
    );
    let result = repository.attribute(&repository.candidate("src/runtime/ported.rs", 2, 5));
    assert_eq!(result.name(), "suspected");
    let rule = &result.data().assured("committed attribution").rules[0];
    assert_eq!(rule.event.as_deref(), Some("lowered 3->2"));
    assert!(!rule.spelling_removed);
    assert!(rule.commit_spelling_removed);
}

#[test]
fn a_later_change_that_carries_the_pattern_keeps_its_origin() {
    let repository = Repository::new();
    let source = fs::read_to_string(repository.root().join("src/runtime/panes.rs"))
        .assured("fixture source reads");
    repository.commit(
        "2026-09-26",
        "tune admission",
        &[(
            "src/runtime/panes.rs",
            &source
                .replace("values.push(1);", "values.push(2);")
                .replace("map.get_mut(&pane)", "map.get_mut(&(pane))"),
        )],
    );
    let result = repository.attribute(&repository.candidate("src/runtime/panes.rs", 2, 5));
    let data = result.data().assured("committed attribution");
    assert_eq!(result.name(), "suspected");
    assert_eq!(data.commit.subject, "tune admission");
    assert_eq!(
        data.origin
            .as_ref()
            .assured("a carried workaround has its origin")
            .subject,
        "lower the lock count"
    );
}

fn other_pattern(
    repository: &Repository,
    path: &str,
    line: u32,
    pattern: AuditPattern,
) -> super::candidates::Candidate {
    let source = fs::read_to_string(repository.root().join(path)).assured("fixture source reads");
    let index = usize::try_from(line.checked_sub(1).assured("fixture line starts at one"))
        .assured("fixture index fits");
    let source = source.lines().nth(index).assured("fixture line exists");
    let raw = AuditCandidate {
        owner: "crate::helper".into(),
        context: None,
        span: span(path, line, source.trim(), 0),
        expansion: Vec::new(),
        pattern,
    };
    Collector::new()
        .collect(vec![evidence("ordinary", vec![raw])])
        .assured("fixture candidate collects")
        .pop()
        .assured("fixture contains one candidate")
}

#[test]
fn a_string_failure_removal_explains_only_its_own_file() {
    let repository = Repository::new();
    repository.commit("2026-09-26", "count string errors", &[
        ("debt-baseline.json", r#"{"data_plane_lock_acquisitions":2,"result_string_errors":2}"#),
        ("src/convert.rs", "fn convert(text: &str) -> Result<u8, String> {\n    let trimmed = text.trim();\n    let length = trimmed.len();\n    trimmed.parse().map_err(|error| error.to_string())\n}\n"),
        ("src/parse.rs", "fn parse(text: &str) -> Result<u8, String> {\n    text.parse().map_err(|error| error.to_string())\n}\n"),
    ]);
    repository.commit(
        "2026-09-27",
        "type conversion failures",
        &[
            (
                "debt-baseline.json",
                r#"{"data_plane_lock_acquisitions":2,"result_string_errors":1}"#,
            ),
            (
                "src/convert.rs",
                "fn convert(text: &str) -> Result<u8, Report<ConvertError>> {\n    let trimmed = \
                 text.trim();\n    let length = trimmed.len();\n    trimmed.parse().map_err(|_| \
                 Report::new(ConvertError))\n}\n",
            ),
            (
                "src/other.rs",
                "fn other(text: &str) -> Option<u8> {\n    text.parse().map_err(|_| \
                 Missing).ok()\n}\n",
            ),
        ],
    );
    for (path, line, suggestion, file_removed) in [
        ("src/convert.rs", 4, "suspected", true),
        ("src/other.rs", 2, "possible", false),
    ] {
        let candidate = other_pattern(
            &repository,
            path,
            line,
            AuditPattern::DiscardedError {
                operation: "map_err".into(),
                error_type: "std::num::ParseIntError".into(),
                information: ErrorInformation::Value,
            },
        );
        let result = repository.attribute(&candidate);
        assert_eq!(result.name(), suggestion);
        let rule = result
            .data()
            .assured("committed attribution")
            .rules
            .iter()
            .find(|rule| rule.rule == "result_string_errors")
            .assured("error owner includes the string rule");
        assert!(!rule.spelling_removed);
        assert_eq!(rule.file_spelling_removed, file_removed);
        assert!(rule.commit_spelling_removed);
    }
}

#[test]
fn a_forwarding_wrapper_written_by_a_complexity_lowering_is_suspected() {
    let repository = Repository::new();
    repository.commit(
        "2026-09-26",
        "count complex functions",
        &[
            (
                "debt-baseline.json",
                r#"{"data_plane_lock_acquisitions":2,"functions_over_complexity_30":2}"#,
            ),
            (
                "src/route.rs",
                "fn route(value: u32) -> u32 {\n    if value > 1 { value } else { 0 }\n}\n",
            ),
        ],
    );
    repository.commit(
        "2026-09-27",
        "split routing",
        &[
            (
                "debt-baseline.json",
                r#"{"data_plane_lock_acquisitions":2,"functions_over_complexity_30":1}"#,
            ),
            (
                "src/route.rs",
                "fn route(value: u32) -> u32 {\n    route_branches(value)\n}\n\nfn \
                 route_branches(value: u32) -> u32 {\n    if value > 1 { value } else { 0 }\n}\n",
            ),
        ],
    );
    let candidate = other_pattern(
        &repository,
        "src/route.rs",
        2,
        AuditPattern::ForwardingWrapper {
            callee: "crate::route_branches".into(),
            callers: 1,
            contract: false,
            implementation: false,
            relocated: false,
        },
    );
    let result = repository.attribute(&candidate);
    assert_eq!(result.name(), "suspected");
    let data = result.data().assured("committed attribution");
    assert_eq!(data.commit.subject, "split routing");
    let rule = data
        .rules
        .iter()
        .find(|rule| rule.rule == "functions_over_complexity_30")
        .assured("wrapper owner includes complexity");
    assert_eq!(rule.event.as_deref(), Some("lowered 2->1"));
    assert!(!rule.predates_rule);
}

#[test]
fn signature_spelling_names_a_plain_failure() {
    let catalog = super::catalog::Catalog::new();
    let rule = catalog
        .rules()
        .find(|rule| rule.name == "bare_error_signatures")
        .assured("signature rule exists");
    for source in [
        "fn parse() -> Result<u8, ParseError> {",
        "fn parse() -> std::result::Result<u8, crate::ParseError> {",
    ] {
        assert_eq!(rule.count(source), 1);
    }
    for source in [
        "fn parse() -> Result<u8, Report<ParseError>> {",
        "fn parse() -> Result<u8, String> {",
    ] {
        assert_eq!(rule.count(source), 0);
    }
    assert_eq!(
        rule.count("fn parse() -> Result<u8, Pa\u{301}rseError> {"),
        0
    );
    let casts = catalog
        .rules()
        .find(|rule| rule.name == "as_casts")
        .assured("cast rule exists");
    assert_eq!(casts.count("λas u32"), 0);
    assert_eq!(casts.count("as u32λ"), 0);
    assert_eq!(casts.count("\u{301}as\u{1c}u32\u{301}"), 1);
    let strings = catalog
        .rules()
        .find(|rule| rule.name == "result_string_errors")
        .assured("string rule exists");
    assert_eq!(strings.count("λResult<Result<u8, String>, String>"), 1);
}

#[test]
fn counted_files_only_implicate_code_written_during_the_textual_period() {
    let repository = Repository::new();
    let source = "fn admit(map: &mut M, key: u64) {\n    if let Some(value) = map.get_mut(&key) \
                  {\n        *value += 1;\n    } else {\n        map.insert(key, 1);\n    }\n}\n";
    repository.commit(
        "2026-09-26",
        "add counted admission",
        &[("src/runtime/counted.rs", source)],
    );
    repository.commit(
        "2026-09-27",
        "resolve the lock count",
        &[(
            "debt-baseline.json",
            r#"{"data_plane_lock_acquisitions":1}"#,
        )],
    );
    let resolution = repository
        .command(&["rev-parse", "HEAD"], None)
        .trim()
        .to_owned();
    repository.commit(
        "2026-09-28",
        "add resolved admission",
        &[("src/runtime/resolved.rs", source)],
    );
    for (path, suggestion, counted) in [
        ("src/runtime/counted.rs", "possible", true),
        ("src/runtime/resolved.rs", "unattributed", false),
    ] {
        let candidate = repository.candidate(path, 2, 5);
        let mut attributor =
            Attributor::new(Git::new(repository.root()), false).assured("fixture history loads");
        attributor.history.resolved_commit = resolution.clone();
        attributor
            .prepare(std::slice::from_ref(&candidate))
            .assured("fixture provenance loads");
        let result = attributor
            .attribute(&candidate)
            .assured("fixture attribution completes");
        assert_eq!(result.name(), suggestion);
        assert_eq!(
            result.data().assured("committed attribution").rules[0].written_while_counted,
            counted
        );
    }
}

#[test]
fn relocated_acquisitions_require_a_lowering_outside_the_counted_files() {
    let repository = Repository::new();
    let mut sites = Vec::new();
    for path in ["src/registry.rs", "src/runtime/panes.rs", "src/later.rs"] {
        let finding = Finding {
            rule: "data_plane_lock_acquisitions".into(),
            owner: "crate::helper".into(),
            receiver: "lock_api::mutex::Mutex".into(),
            receiver_type: "Mutex".into(),
            receiver_expression: "shared".into(),
            operation: "lock".into(),
            definition: "lock_api::Mutex::lock".into(),
            acquisition: Acquisition::Exclusive,
            context: None,
            expansion: Vec::new(),
            span: span(path, 2, "lock", 0),
        };
        sites.push(AuthoredSite {
            site: finding.span.site.clone(),
            configurations: BTreeMap::from([("ordinary:crate".into(), vec![finding])]),
        });
    }
    let mut attribution =
        Attributor::new(Git::new(repository.root()), false).assured("fixture history loads");
    let relocated = attribution
        .relocated(sites)
        .assured("fixture relocations collect");
    assert_eq!(relocated.len(), 1);
    assert_eq!(relocated[0].path(), "src/registry.rs");
    assert_eq!(relocated[0].configurations, ["ordinary:crate"]);
    assert_eq!(
        serde_json::to_value(&relocated[0]).assured("candidate serializes")["pattern"]["receiver"],
        "lock_api::mutex::Mutex"
    );
    assert!(
        attribution
            .history
            .lock_scope("src/runtime/a.rs", "2026-01-01")
    );
    assert!(
        !attribution
            .history
            .lock_scope("crates/connectors/example/src/lib.rs", "2026-09-25")
    );
    attribution.history.counted_commit = repository
        .command(&["rev-parse", "HEAD~1"], None)
        .trim()
        .into();
    assert!(
        !attribution
            .history
            .lock_scope("crates/connectors/example/src/lib.rs", "2026-09-19")
    );
    assert!(
        attribution
            .history
            .lock_scope("crates/connectors/example/src/lib.rs", "2026-09-20")
    );
}

fn matrix(repository: &Repository, candidates: Vec<AuditCandidate>) -> MatrixReport {
    let completion = evidence("ordinary", candidates);
    MatrixReport {
        identity: "fixture".into(),
        source_identity: "0123456789abcdef0123".into(),
        compiler: "fixture".into(),
        root: repository.root().into(),
        configurations: vec![completion.configuration.clone()],
        evidence: vec![completion],
        findings: Vec::new(),
        complete: true,
        matrix_complete: true,
        diagnostics_checked: true,
    }
}

#[test]
fn one_complete_matrix_publishes_the_full_report_and_markdown() {
    let repository = Repository::new();
    let mut raw = lookup("src/runtime/panes.rs", 2, 0);
    let actual = repository.candidate("src/runtime/panes.rs", 2, 5);
    raw.pattern = match actual.pattern.clone() {
        super::candidates::Pattern::Resolved(pattern) => pattern,
        _ => unreachable!("the fixture uses compiler candidates"),
    };
    raw.owner = "crate::f::{closure|0}".into();
    let report = super::build(
        Git::new(repository.root()),
        matrix(&repository, vec![raw]),
        false,
    )
    .assured("complete matrix attributes");
    let target = repository.root().join("target/lint-audit/0123456789abcdef");
    super::publish(&target, &report).assured("complete report publishes");
    let bytes = fs::read(target.join("audit.json")).assured("report reads");
    let decoded: Report = serde_json::from_slice(&bytes).assured("every report field decodes");
    assert_eq!(decoded, report);
    assert_eq!(
        decoded.rules["data_plane_lock_acquisitions"].date,
        "2026-09-14"
    );
    assert_eq!(decoded.summary.0["repeated_lookup"].recurring, 1);
    assert_eq!(decoded.summary.0["repeated_lookup"].suspected, 1);
    let markdown = fs::read_to_string(target.join("audit.md")).assured("Markdown reads");
    assert!(markdown.contains("closure\\|0"));
    assert!(markdown.contains("get_mut -> insert (guarded)"));
    assert!(markdown.contains("| repeated_lookup | 1 | 1 | 1 | 0 | 0 | 0 |"));
    assert!(markdown.contains(
        "data_plane_lock_acquisitions: lowered 3->2, hunk removed spelling, written while counted"
    ));
    assert_eq!(markdown, super::render::markdown(&report));
}

#[test]
fn an_incomplete_matrix_cannot_publish_an_audit() {
    let repository = Repository::new();
    let mut matrix = matrix(&repository, Vec::new());
    matrix.complete = false;
    let error = match super::build(Git::new(repository.root()), matrix, false) {
        Err(error) => error,
        Ok(_) => panic!("incomplete analysis must be refused"),
    };
    assert!(matches!(
        error.current_context(),
        crate::error::Error::AuditIncomplete
    ));
}

mod github;
mod representations;

#[test]
fn audit_inventory_selection_and_output_failures_keep_the_command_contract() {
    let directory = tempfile::tempdir().assured("private audit directory opens");
    for (inventory, configuration, expected_inventory) in [
        (false, Vec::new(), false),
        (true, Vec::new(), true),
        (false, vec!["ordinary".into()], true),
    ] {
        let options = super::Options {
            root: Some(directory.path().into()),
            target_dir: Some(directory.path().join("target")),
            inventory,
            configuration,
            pull_requests: false,
            output_dir: None,
        };
        let analysis = options
            .analysis()
            .assured("audit options resolve without Cargo");
        assert_eq!(analysis.inventory, expected_inventory);
        assert_eq!(analysis.configuration, options.configuration);
        let path = if expected_inventory {
            "lint-audit/inventory.json"
        } else {
            "typed-ratchet/gate.json"
        };
        assert_eq!(
            analysis.output,
            Some(directory.path().join("target").join(path))
        );
    }
    let repository = Repository::new();
    let report = super::build(
        Git::new(repository.root()),
        matrix(&repository, Vec::new()),
        false,
    )
    .assured("empty complete evidence attributes");
    let path = directory.path().join("occupied");
    fs::write(&path, "occupied").assured("fixture output obstruction writes");
    assert!(super::publish(&path, &report).is_err());
    let git = Git::new(directory.path());
    let result = git.text(&["rev-parse", "HEAD"]);
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("a non-repository has no revision"),
    };
    assert!(matches!(
        error.current_context(),
        crate::error::Error::AuditGit { .. }
    ));
}

#[test]
fn git_read_groups_join_every_input_and_keep_declared_order() {
    use nervix_primitives::sync::{
        atomic::{AtomicUsize, Ordering},
        blocking::Barrier,
    };
    let directory = tempfile::tempdir().assured("private read group opens");
    let git = Git::new(directory.path());
    let items: Vec<_> = (0..16).collect();
    let active = AtomicUsize::new(0);
    let maximum = AtomicUsize::new(0);
    let barrier = Barrier::new(8);
    let result = git
        .read_all(&items, |index| {
            let previous = active.fetch_add(1, Ordering::SeqCst);
            let count = previous
                .checked_add(1)
                .assured("at most eight scoped workers enter");
            maximum.fetch_max(count, Ordering::SeqCst);
            barrier.wait();
            active.fetch_sub(1, Ordering::SeqCst);
            barrier.wait();
            Ok(*index)
        })
        .assured("finite read groups join");
    assert_eq!(result, items);
    assert_eq!(maximum.load(Ordering::SeqCst), 8);
    assert_eq!(active.load(Ordering::SeqCst), 0);
}
