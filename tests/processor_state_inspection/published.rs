//! Public checks of published window headers and count summaries.
//!
//! Layer: test harness.
//! - **Owns.** Header/deadline/charge consistency and metadata-only empty-window pages.
//! - **Depends on.** Public typed inspection frames and command outcomes.
//! - **Must not know.** Runtime publications, accumulator internals or private snapshot bytes.

use super::*;

#[then(expr = "the last processor state redacts {string} in section {string}")]
fn redacted_cells(world: &mut ScenarioWorld, field: String, section_name: String) {
    let state = last_state(world);
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("the fixture is a page");
    };
    let mut observed = 0_usize;
    for unit in &page.units {
        for section in &unit.sections {
            if section.name != section_name {
                continue;
            }
            let index = section
                .schema
                .fields
                .iter()
                .position(|candidate| candidate.name.as_str() == field)
                .assured("the section declares the inspected field");
            assert!(section.schema.fields[index].sensitive);
            for frame in &section.frames {
                for row in frame.batch().rows() {
                    assert!(matches!(row.get(index), Some(CellView::Redacted)));
                    observed = observed
                        .checked_add(1)
                        .assured("a bounded returned row count");
                }
            }
        }
    }
    assert!(observed > 0);
}

#[then(expr = "the last deduplicator state dates every visible key")]
fn seen_times(world: &mut ScenarioWorld) {
    let state = last_state(world);
    let times = section_times(state, "keys", "seen_at");
    assert_eq!(times.len(), section_rows(state, "keys").len());
    assert!(!times.is_empty());
    assert!(
        times.iter().all(|(_, time)| *time > 0),
        "the fixture ingests in the current domain time"
    );
}

#[then(expr = "the last window state has {int} branch headers for {int} retained rows each")]
fn window_headers(world: &mut ScenarioWorld, branches: usize, rows: u64) {
    let state = last_state(world);
    let headers = section_rows(state, "header");
    assert_eq!(headers.len(), branches);
    let mut identities = std::collections::BTreeSet::new();
    let mut total_bytes = 0_u64;
    for header in headers {
        assert!(
            identities.insert(header["branch"].to_string()),
            "one header per branch"
        );
        let fields = &header["fields"];
        assert_eq!(fields["rows"], rows);
        assert_eq!(fields["first_sequence"], 0);
        assert_eq!(fields["next_sequence"], rows);
        assert!(fields["incarnation"].as_u64().is_some());
        assert!(!fields["due_at"].is_null());
        let bytes = fields["state_bytes"]
            .as_u64()
            .assured("an exact byte charge");
        assert!(
            bytes > 0 && bytes <= 1_048_576,
            "the admitted window fits MAX STATE SIZE"
        );
        total_bytes = total_bytes
            .checked_add(bytes)
            .assured("the selected charge fits u64");
    }
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("the fixture is a page");
    };
    assert_eq!(
        page.counts.bytes, total_bytes,
        "complete byte counts use the header charge"
    );
    let mut earliest = BTreeMap::<String, i64>::new();
    for (branch, at) in section_times(state, "rows", "received_at") {
        let first = earliest.entry(branch).or_insert(at);
        *first = (*first).min(at);
    }
    for (branch, due) in section_times(state, "header", "due_at") {
        let first = earliest
            .get(&branch)
            .assured("every populated branch has retained input");
        assert_eq!(
            due - first,
            3_600_000_000_000_i64,
            "the fixture has a one-hour width"
        );
    }
    let structures = section_rows(state, "accumulators");
    assert_eq!(
        structures.len(),
        branches
            .checked_mul(3)
            .assured("three structures per fixture branch")
    );
    for structure in structures {
        let fields = &structure["fields"];
        assert_eq!(fields["count"], rows);
        match fields["structure"].as_u64().assured("a demand index") {
            0 => assert_eq!(fields["kind"], "counter"),
            1 => assert_eq!(fields["kind"], "sum"),
            2 => assert_eq!(fields["kind"], "sequence"),
            other => panic!("the fixture declares three demands, got {other}"),
        }
    }
}

#[then(expr = "the last processor description matches the last inspected counts")]
fn described_counts(world: &mut ScenarioWorld) {
    let state = last_state(world);
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("the fixture is a page");
    };
    let output = world
        .last_command_output
        .as_ref()
        .assured("DESCRIBE completed");
    let label = if processor_kind(state) == nervix_models::ModelKind::Deduplicator {
        "keys"
    } else {
        "rows"
    };
    for line in [
        format!("branches: {}", page.counts.branches),
        format!("{label}: {}", page.counts.rows),
        format!("state bytes: {}", page.counts.bytes),
    ] {
        assert!(output.contains(&line), "missing {line:?} in {output}");
    }
}

#[then(expr = "the last window state keeps an empty branch header after sequence {int}")]
fn empty_header(world: &mut ScenarioWorld, next: u64) {
    let state = last_state(world);
    let headers = section_rows(state, "header");
    assert_eq!(headers.len(), 1);
    let fields = &headers[0]["fields"];
    assert_eq!(fields["rows"], 0);
    assert!(fields["first_sequence"].is_null());
    assert_eq!(fields["next_sequence"], next);
    assert!(fields["incarnation"].as_u64().is_some());
    assert!(fields["due_at"].is_null());
    assert_eq!(fields["state_bytes"], 1024);
    let structures = section_rows(state, "accumulators");
    assert_eq!(structures.len(), 3);
    for structure in structures {
        assert_eq!(structure["fields"]["count"], 0);
    }
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("the fixture is a page");
    };
    assert!(page.continuation.is_none());
    assert_eq!(page.counts.bytes, 1024);
}
