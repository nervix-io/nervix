//! Repository tooling, outside the product layer order.
//! Owns: the identity and evidence of counted branch-ownership declarations.
//! Depends on: the verified compiler matrix and the branch ownership baseline entries.
//! Must not know: collection syntax, runtime code or any permitted collection.

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    path::Path,
    process::Command,
};

use anyhow::{Context as _, Result, bail, ensure};

use super::Site;
use crate::evidence::MatrixReport;

pub(super) fn collect(report: &MatrixReport) -> Result<BTreeMap<String, Site>> {
    ensure!(
        report.complete && report.matrix_complete && report.diagnostics_checked,
        "branch ownership requires a complete, checked compiler matrix"
    );
    let mut sites = BTreeMap::new();
    for configuration in &report.evidence {
        for compilation in configuration.reports.values() {
            for violation in &compilation.branch_inversions {
                let declaration = format!("{}::{}", violation.owner, violation.declaration);
                let reason = violation
                    .expectation_reason
                    .as_deref()
                    .filter(|reason| nervix_lint_report::branch_repair_task(reason).is_some())
                    .with_context(|| {
                        format!(
                            "{}:{}: branch collection {declaration} requires one expectation that \
                             names its ClickUp repair task",
                            violation.span.site.path, violation.span.line
                        )
                    })?;
                let site = Site {
                    path: violation.span.site.path.clone(),
                    line: usize::try_from(violation.span.line)?,
                    detail: format!("{declaration} ({reason})"),
                };
                match sites.entry(declaration) {
                    Entry::Vacant(entry) => {
                        entry.insert((violation.span.site.clone(), site));
                    }
                    Entry::Occupied(entry) => {
                        ensure!(
                            entry.get() == &(violation.span.site.clone(), site.clone()),
                            "branch declarations share the same identity: {} and {}",
                            entry.get().1.render(),
                            site.render()
                        );
                    }
                }
            }
        }
    }
    Ok(sites
        .into_iter()
        .map(|(name, (_, site))| (name, site))
        .collect())
}

/// A declaration's identity with the compiler's impl disambiguators taken out.
///
/// The compiler numbers a module's impl blocks in source order, so adding or removing an unrelated
/// impl block earlier in the module renumbers every declaration after it, `{impl#14}` becoming
/// `{impl#13}`, without changing which declaration it is. Declarations are compared by this stable
/// form and counted, so a renumbered declaration keeps its pin while one more declaration under an
/// equal stable form still counts as added.
pub(super) fn stable_identity(identity: &str) -> String {
    const OPEN: &str = "{impl#";
    let mut stable = String::with_capacity(identity.len());
    let mut rest = identity;
    while let Some(start) = rest.find(OPEN) {
        let (before, after) = rest.split_at(start);
        stable.push_str(before);
        let numbered = &after[OPEN.len()..];
        let digits = numbered
            .find(|character: char| !character.is_ascii_digit())
            .unwrap_or(numbered.len());
        let numbered_impl = digits > 0 && numbered[digits..].starts_with('}');
        if numbered_impl {
            stable.push_str("{impl}");
            rest = &numbered[digits + 1..];
        } else {
            stable.push_str(OPEN);
            rest = numbered;
        }
    }
    stable.push_str(rest);
    stable
}

/// How many of `identities` share each stable form.
fn stable_counts<'a>(
    identities: impl IntoIterator<Item = &'a String>,
) -> Result<BTreeMap<String, usize>> {
    let mut counts = BTreeMap::<String, usize>::new();
    for identity in identities {
        let count = counts.entry(stable_identity(identity)).or_default();
        *count = count
            .checked_add(1)
            .context("a branch ownership inventory holds fewer declarations than usize counts")?;
    }
    Ok(counts)
}

/// The identities among `current` whose stable form occurs more often there than in `accepted`:
/// every declaration `accepted` does not account for, even when an impl block renumbered the ones
/// it does.
pub(super) fn added<'a>(
    current: impl IntoIterator<Item = &'a String> + Clone,
    accepted: &BTreeSet<String>,
) -> Result<Vec<&'a String>> {
    let current_counts = stable_counts(current.clone())?;
    let accepted_counts = stable_counts(accepted)?;
    let mut added = Vec::new();
    for identity in current {
        let form = stable_identity(identity);
        let held = current_counts.get(&form).copied().unwrap_or_default();
        let pinned = accepted_counts.get(&form).copied().unwrap_or_default();
        if held > pinned {
            added.push(identity);
        }
    }
    Ok(added)
}

pub(super) fn verify(sites: &BTreeMap<String, Site>, pinned: &BTreeSet<String>) -> Result<()> {
    let mut rendered = Vec::new();
    for identity in added(sites.keys(), pinned)? {
        rendered.push(sites[identity].render());
    }
    if !rendered.is_empty() {
        bail!(
            "new branch ownership expectations are forbidden:\n{}",
            rendered.join("\n")
        );
    }
    Ok(())
}

pub(super) fn verify_previous_baseline(root: &Path, current: &BTreeSet<String>) -> Result<()> {
    let merge_ref = std::env::var("GITHUB_REF")
        .is_ok_and(|value| value.starts_with("refs/pull/") && value.ends_with("/merge"));
    let revision = if merge_ref { "HEAD^1" } else { "origin/main" };
    let git = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["show", &format!("{revision}:debt-baseline.json")])
        .output()
        .context("cannot read the accepted debt baseline")?;
    if !git.status.success() && !merge_ref {
        return Ok(());
    }
    ensure!(
        git.status.success(),
        "cannot read the pull request's base debt baseline: {}",
        String::from_utf8_lossy(&git.stderr)
    );
    let previous: serde_json::Value = serde_json::from_slice(&git.stdout)?;
    // The introducing pull request is the only one whose accepted base has no branch inventory.
    if let Some(declarations) = previous.get("branch_inversion_sites") {
        let accepted: BTreeSet<String> = serde_json::from_value(declarations.clone())?;
        let added = added(current, &accepted)?;
        let mut rendered = Vec::new();
        for identity in added {
            rendered.push(identity.as_str());
        }
        ensure!(
            rendered.is_empty(),
            "the debt baseline cannot add branch ownership declarations:\n{}",
            rendered.join("\n")
        );
    }
    Ok(())
}
