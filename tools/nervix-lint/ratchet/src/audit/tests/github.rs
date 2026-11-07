//! Repository test harness, outside the product layer order.
//! Owns: squash-folded provenance and the gh reader's exact retry boundary witnesses.
//! Depends on: current recorded GitHub pages and the production audit owners.
//! Must not know: the real network or Cargo.

use std::{
    collections::BTreeMap,
    os::unix::process::ExitStatusExt as _,
    process::{ExitStatus, Output},
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use serde_json::json;

use super::{Attributor, Git, Repository};
use crate::{
    audit::pull_requests::{Api, GithubRead, PullRequests},
    error::{Error, Result},
};

struct Pages {
    pages: BTreeMap<String, Vec<u8>>,
}

impl GithubRead for Pages {
    fn read(&mut self, path: &str) -> Result<Vec<u8>> {
        if let Some(bytes) = self.pages.get(path) {
            return Ok(bytes.clone());
        }
        assert!(path.ends_with("/pulls"), "unexpected GitHub read {path}");
        Ok(b"[[]]".to_vec())
    }
}

fn pulls(rewritten: &str) -> PullRequests {
    let a = "a".repeat(40);
    let b = "b".repeat(40);
    let c = "c".repeat(40);
    let pages = BTreeMap::from([
        (
            "repos/example/project/pulls/7/commits".into(),
            json!([[{"sha":a,"parents":[{}]}, {"sha":b,"parents":[{}]}, {"sha":c,"parents":[{},{}]}]]),
        ),
        (
            format!("repos/example/project/commits/{a}"),
            json!([{"files":[{"filename":"src/runtime/sketches.rs", "patch":"@@ -0,0 +1,3 @@\n+fn admit(map: &mut M, pane: i64) {\n+    map.entry(pane).or_insert_with(S::new).add(1);\n+}"}]}]),
        ),
        (
            format!("repos/example/project/commits/{b}"),
            json!([{"files":[{"filename":"src/runtime/sketches.rs", "patch":format!("@@ -2 +2,5 @@\r\n-    map.entry(pane).or_insert_with(S::new).add(1);\r\n+{rewritten}\r\n")}]}]),
        ),
        (
            format!("repos/example/project/commits/{}/pulls", "d".repeat(40)),
            json!([[{"number":3,"merged_at":null},{"number":9,"merged_at":""}], [{"number":10,"merged_at":"2026-09-23T12:00:00Z"}]]),
        ),
    ]);
    let pages = pages
        .into_iter()
        .map(|(path, value)| {
            (
                path,
                serde_json::to_vec(&value).assured("fixture pages encode"),
            )
        })
        .collect();
    PullRequests::new("example/project".into(), Box::new(Pages { pages }))
}

fn output(status: i32, text: &str) -> Output {
    Output {
        status: ExitStatus::from_raw(status.checked_mul(256).assured("fixture status fits")),
        stdout: if status == 0 {
            text.as_bytes().into()
        } else {
            Vec::new()
        },
        stderr: if status == 0 {
            Vec::new()
        } else {
            text.as_bytes().into()
        },
    }
}

#[test]
fn transient_github_failures_retry_with_the_exact_backoff() {
    let mut reads = [
        output(1, "gh: HTTP 500\n"),
        output(1, "read: connection reset by peer\n"),
        output(0, "[]"),
    ]
    .into_iter();
    let mut delays = Vec::new();
    let result = Api::new()
        .read_using(
            "repos/example/project/commits/a",
            || Ok(reads.next().assured("three recorded responses exist")),
            |delay| delays.push(delay),
        )
        .assured("the third read succeeds");
    assert_eq!(result, b"[]");
    assert_eq!(delays, [Duration::from_secs(2), Duration::from_secs(4)]);
    assert!(reads.next().is_none());
}

#[test]
fn a_persisting_github_failure_ends_after_four_reads() {
    let mut reads = 0_usize;
    let mut delays = Vec::new();
    let result = Api::new().read_using(
        "repos/example/project/commits/a",
        || {
            reads = reads.checked_add(1).assured("four reads fit");
            Ok(output(1, "gh: HTTP 502\n"))
        },
        |delay| delays.push(delay),
    );
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("persistent failure must end the audit"),
    };
    assert!(matches!(
        error.current_context(),
        Error::AuditGithub { attempts: 4, .. }
    ));
    assert_eq!(reads, 4);
    assert_eq!(
        delays,
        [
            Duration::from_secs(2),
            Duration::from_secs(4),
            Duration::from_secs(8)
        ]
    );
    assert!(format!("{error:?}").contains("gh: HTTP 502"));
}

#[test]
fn a_terminal_github_failure_does_not_retry() {
    let mut reads = 0_usize;
    let result = Api::new().read_using(
        "repos/example/project/commits/a",
        || {
            reads = reads.checked_add(1).assured("one read fits");
            Ok(output(1, "gh: Not Found (HTTP 404)\n"))
        },
        |_| panic!("a terminal failure schedules no wait"),
    );
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("terminal failure must end the audit"),
    };
    assert!(matches!(
        error.current_context(),
        Error::AuditGithub { attempts: 1, .. }
    ));
    assert_eq!(reads, 1);
}

#[test]
fn a_folded_spelling_rewrite_matches_this_candidates_own_lines() {
    for (rewritten, suggestion, folded) in [
        (
            "    if let Some(sketch) = map.get_mut(&pane) {",
            "suspected",
            true,
        ),
        ("    let other = map.len();", "possible", false),
    ] {
        let repository = Repository::new();
        repository.commit(
            "2026-09-26",
            "feat(window): add pane sketches (#7)",
            &[(
                "src/runtime/sketches.rs",
                "fn admit(map: &mut M, pane: i64) {\n    if let Some(sketch) = map.get_mut(&pane) \
                 {\n        sketch.add(1);\n    } else {\n        map.insert(pane, S::new());\n    \
                 }\n}\n",
            )],
        );
        let candidate = repository.candidate("src/runtime/sketches.rs", 2, 5);
        let mut attribution =
            Attributor::new(Git::new(repository.root()), false).assured("fixture history loads");
        let mut reader = pulls(rewritten);
        assert_eq!(
            reader
                .number("Unicode decimal title (#٧)\u{1c}", "unused")
                .assured("decimal spelling normalizes"),
            Some(7)
        );
        assert_eq!(
            reader
                .number("another title", &"d".repeat(40))
                .assured("recorded merged pulls decode"),
            Some(9)
        );
        assert_eq!(
            reader
                .number("pushed directly", &"e".repeat(40))
                .assured("empty pulls decode"),
            None
        );
        attribution.pull_requests = Some(reader);
        attribution
            .prepare(std::slice::from_ref(&candidate))
            .assured("fixture provenance loads");
        let result = attribution
            .attribute(&candidate)
            .assured("folded provenance attributes");
        assert_eq!(result.name(), suggestion);
        let rule = &result.data().assured("committed candidate").rules[0];
        assert!(rule.written_while_counted);
        assert_eq!(rule.pull_request_rewrite.is_some(), folded);
        if let Some(rewrite) = &rule.pull_request_rewrite {
            assert_eq!(rewrite.sha, "b".repeat(40));
            assert_eq!(
                rewrite.removed,
                ["    map.entry(pane).or_insert_with(S::new).add(1);"]
            );
        }
    }
}
