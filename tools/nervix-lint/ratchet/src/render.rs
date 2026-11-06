//! Repository tooling, outside the product layer order.
//! Owns: the human-readable authored-finding listing.
//! Depends on: aggregated report vocabulary.
//! Must not know: compiler execution, gate decisions or product graph policy.

use std::{
    collections::BTreeSet,
    io::{self, Write},
};

use meticulous::ResultExt as _;
use nervix_lint_report::AuthoredSite;
use serde::Serialize as _;

struct Spaced;
impl serde_json::ser::Formatter for Spaced {
    fn begin_object_key<W: ?Sized + Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        if !first {
            writer.write_all(b", ")?;
        }
        Ok(())
    }
    fn begin_object_value<W: ?Sized + Write>(&mut self, writer: &mut W) -> io::Result<()> {
        writer.write_all(b": ")
    }
    fn begin_array_value<W: ?Sized + Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        if !first {
            writer.write_all(b", ")?;
        }
        Ok(())
    }
}

fn json<T: serde::Serialize>(value: &T) -> String {
    let sorted = serde_json::to_value(value).assured("report values serialize into JSON");
    let mut bytes = Vec::new();
    sorted
        .serialize(&mut serde_json::Serializer::with_formatter(
            &mut bytes, Spaced,
        ))
        .assured("report JSON serializes into an in-memory buffer");
    String::from_utf8(bytes).assured("serde_json emits UTF-8")
}

pub fn finding(site: &AuthoredSite) {
    let mut operations = BTreeSet::new();
    let mut owners = BTreeSet::new();
    let mut contracts = BTreeSet::new();
    let mut origins = BTreeSet::new();
    let mut first = None;
    for findings in site.configurations.values() {
        for finding in findings {
            if first.is_none() {
                first = Some(finding);
            }
            operations.insert(format!(
                "{}.{} ({})",
                finding.receiver_type,
                finding.operation,
                json(&finding.acquisition).trim_matches('"')
            ));
            owners.insert(finding.owner.clone());
            contracts.insert(json(&finding.context));
            for origin in &finding.expansion {
                origins.insert(format!("{} at {}", origin.macro_name, origin.call_site));
            }
        }
    }
    let Some(first) = first else {
        return;
    };
    let configurations: Vec<_> = site.configurations.keys().cloned().collect();
    let mut text = format!(
        "synchronization: {}; owner {}; source contracts {}; configurations {}",
        operations.into_iter().collect::<Vec<_>>().join(", "),
        owners.into_iter().collect::<Vec<_>>().join(", "),
        contracts.into_iter().collect::<Vec<_>>().join(", "),
        configurations.join(", ")
    );
    if !origins.is_empty() {
        text.push_str("; expansions ");
        text.push_str(&origins.into_iter().collect::<Vec<_>>().join(", "));
    }
    println!(
        "{}:{}:{}: {text}",
        site.site.path,
        first.span.line,
        first.span.column + 1
    );
}
