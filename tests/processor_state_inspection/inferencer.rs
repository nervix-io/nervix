//! Layer: test harness.
//! Owns: public inference publication, persistence and replica-progress header assertions.
//! Depends on: verified inspection envelopes and their schema-aware Row presentation.
//! Must not know: native generations, checkpoint owners or the inspection implementation.

use super::*;

#[then(
    expr = "within {string} client {string} observes a new publication of inferencer {string} for \
            tenant {string}"
)]
async fn published(
    world: &mut ScenarioWorld,
    duration: String,
    client: String,
    name: String,
    tenant: String,
) {
    let duration = parse_duration_text(&duration).assured("the scenario names a valid bound");
    let name = expand_placeholders(world, &name);
    let statement = format!(
        "SHOW INFERENCER {name} STATE FOR BRANCH VALUES {{ tenant = '{tenant}' }} FORMAT JSON;"
    );
    let deadline = Instant::now() + duration;
    loop {
        nervix_primitives::task::consume_budget().await;
        let outcome = nervix_primitives::time::timeout_at(
            deadline,
            execute(world, &client, statement.clone()),
        )
        .await
        .assured("the public publication read completes within its bound");
        if let Some((fingerprint, revision)) = outcome.state.as_deref().and_then(publication) {
            let key = (name.clone(), fingerprint);
            let publications = &mut world.processor_state_observations.inference_publications;
            if publications
                .get(&key)
                .is_none_or(|previous| revision > *previous)
            {
                publications.insert(key, revision);
                record(world, outcome);
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "inferencer {name:?} did not publish tenant {tenant:?} within the bound; last: {}",
            outcome.message
        );
        sleep(Duration::from_millis(100)).await;
    }
}

fn publication(state: &StateInspection) -> Option<(String, u64)> {
    let StateInspectionResult::Page(page) = &state.result else {
        return None;
    };
    if page.counts.branches != 1 {
        return None;
    }
    let rows = section_rows(state, "header");
    let [row] = rows.as_slice() else {
        return None;
    };
    let revision = row["fields"]["publication_revision"].as_u64()?;
    if revision == 0 {
        return None;
    }
    Some((
        row["fields"]["branch_fingerprint"].as_str()?.to_owned(),
        revision,
    ))
}

#[then("the last inference headers identify each returned tensor publication")]
fn publication_headers(world: &mut ScenarioWorld) {
    let state = last_state(world);
    assert_eq!(processor_kind(state), nervix_models::ModelKind::Inferencer);
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("a successful inference read returns a page");
    };
    let mut headers = 0_u64;
    for unit in &page.units {
        for section in unit
            .sections
            .iter()
            .filter(|section| section.name == "header")
        {
            for row in displayed_rows(section) {
                headers = headers.checked_add(1).assured("fixture headers fit u64");
                let fields = &row.fields;
                let fingerprint = fields["branch_fingerprint"]
                    .as_str()
                    .assured("a branched header names its fingerprint");
                assert_eq!(fingerprint.len(), 64);
                assert!(fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit()));
                assert!(fields["incarnation"].as_u64().assured("typed incarnation") > 0);
                let revision = fields["publication_revision"]
                    .as_u64()
                    .assured("typed revision");
                assert!(revision > 0 && revision <= unit.origin.generation);
                assert!(
                    fields["persisted_revision"]
                        .as_u64()
                        .assured("typed persistence revision")
                        <= revision
                );
                let confirmed = fields["confirmed_replicas"]
                    .as_u64()
                    .assured("typed replica progress");
                let replicas = fields["replica_count"]
                    .as_u64()
                    .assured("typed replica count");
                assert!(confirmed <= replicas);
                assert_eq!(fields["state_bytes"].as_u64(), Some(8));
                assert!(matches!(
                    fields["stage"].as_str(),
                    Some("PUBLISHED" | "PERSISTED" | "REPLICA_CONFIRMED")
                ));
            }
        }
    }
    assert_eq!(headers, page.counts.branches);
}
