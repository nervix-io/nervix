//! Regressions for the gate a scenario stands in front of the Iceberg REST catalog.
//!
//! Layer: test harness.
//! - **Owns.** A stand-in catalog behind the gate, the requests a client sends through the gate, and
//!   what the stand-in received of them.
//! - **Depends on.** The Iceberg catalog gate, the HTTP receiver as the stand-in catalog, Reqwest
//!   and the primitives boundary's tasks and monotonic clock.
//! - **Must not know.** Scenario state, Nervix, or the Iceberg REST protocol beyond a table commit.

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::time::timeout;

use crate::{
    http_receiver::{HttpReceiver, ReceiverResponse, ReceiverTransport},
    iceberg_catalog_gate::IcebergCatalogGate,
};

/// How long a regression waits for an outcome the gate must produce. Generous, because each wait
/// ends as soon as its outcome arrives.
const OUTCOME_BOUND: Duration = Duration::from_secs(30);

/// The table the gate holds commits to, and its resource.
const TABLE: &str = "orders";
const TABLE_RESOURCE: &str = "/v1/namespaces/sales/tables/orders";

fn answer(line: &str) -> ReceiverResponse {
    line.parse()
        .assured("every regression's stand-in answer is a valid script line")
}

/// A stand-in catalog that answers every request `200` with a body that names it.
async fn catalog() -> HttpReceiver {
    let catalog = HttpReceiver::start(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        ReceiverTransport::Plain,
    )
    .await
    .assured("a loopback receiver binds an ephemeral port");
    catalog.answer_unscripted_requests_with(answer("respond 200; body from-catalog"));
    catalog
}

async fn gate(catalog: &HttpReceiver) -> IcebergCatalogGate {
    let origin = url::Url::parse(&catalog.origin()).assured("a receiver's origin is a URL");
    IcebergCatalogGate::start(origin, TABLE)
        .await
        .assured("a loopback gate binds an ephemeral port")
}

#[nervix_primitives::test]
async fn every_request_but_a_commit_to_the_gated_table_reaches_the_catalog_unchanged() {
    let catalog = catalog().await;
    let gate = gate(&catalog).await;
    let client = reqwest::Client::new();

    let loaded = client
        .get(format!("{}{TABLE_RESOURCE}?snapshots=all", gate.uri()))
        .header("X-Iceberg-Access-Delegation", "vended-credentials")
        .send()
        .await
        .assured("the gate forwards loading the table");
    assert_eq!(loaded.status(), 200);
    assert_eq!(
        loaded
            .text()
            .await
            .assured("the catalog's body is complete"),
        "from-catalog"
    );
    let other_table = client
        .post(format!(
            "{}/v1/namespaces/sales/tables/orders_archive",
            gate.uri()
        ))
        .body(r#"{"updates":[]}"#)
        .send()
        .await
        .assured("the gate forwards a commit to another table");
    assert_eq!(other_table.status(), 200);
    let metrics = client
        .post(format!("{}{TABLE_RESOURCE}/metrics", gate.uri()))
        .body(r#"{"report-type":"commit-report"}"#)
        .send()
        .await
        .assured("the gate forwards the table's metrics report");
    assert_eq!(metrics.status(), 200);

    let received = catalog
        .wait_for_requests(3, OUTCOME_BOUND)
        .await
        .assured("the catalog answered all three requests, so it received them first");
    let mut lines = Vec::new();
    for request in &received {
        lines.push(request.request_line());
    }
    assert_eq!(
        lines,
        [
            format!("GET {TABLE_RESOURCE}?snapshots=all"),
            "POST /v1/namespaces/sales/tables/orders_archive".to_string(),
            format!("POST {TABLE_RESOURCE}/metrics"),
        ]
    );
    let delegation = received
        .first()
        .verified("the catalog received three requests")
        .header_values("X-Iceberg-Access-Delegation");
    assert_eq!(delegation, [b"vended-credentials".as_slice()]);
    let commit = received
        .get(1)
        .verified("the catalog received three requests");
    assert_eq!(commit.body, br#"{"updates":[]}"#);
}

#[nervix_primitives::test]
async fn a_held_commit_waits_for_the_scenario_and_a_refused_one_never_reaches_the_catalog() {
    let catalog = catalog().await;
    let gate = gate(&catalog).await;
    let client = reqwest::Client::new();
    assert!(
        gate.refuse_held_commit().is_none(),
        "a gate that holds no commit has none to refuse"
    );
    assert!(
        !gate.pass_held_commit(),
        "a gate that holds no commit has none to pass"
    );

    let commit = {
        let request = client
            .post(format!("{}{TABLE_RESOURCE}", gate.uri()))
            .body(r#"{"updates":[{"action":"add-snapshot"}]}"#);
        nervix_primitives::task::spawn(request.send())
    };
    let held = timeout(OUTCOME_BOUND, gate.held_commit())
        .await
        .assured("the gate holds the commit it was sent")
        .assured("the gate keeps its commits while a regression waits for one");
    assert_eq!(held.previous_refusal, None);
    let refused_at = gate
        .refuse_held_commit()
        .verified("the gate reported the commit it holds");
    let refused = commit
        .await
        .assured("the commit's request task completes")
        .assured("the gate answers a refused commit");
    assert_eq!(refused.status(), 409);
    let refusal = refused
        .text()
        .await
        .assured("the refusal's body is complete");
    assert!(refusal.contains("CommitFailedException"), "{refusal}");

    let retried = {
        let request = client
            .post(format!("{}{TABLE_RESOURCE}", gate.uri()))
            .body(r#"{"updates":[{"action":"add-snapshot"}]}"#);
        nervix_primitives::task::spawn(request.send())
    };
    let held_again = timeout(OUTCOME_BOUND, gate.held_commit())
        .await
        .assured("the gate holds the commit that follows a refusal")
        .assured("the gate keeps its commits while a regression waits for one");
    assert_eq!(held_again.previous_refusal, Some(refused_at));
    assert!(held_again.arrived_at >= refused_at);
    assert!(
        catalog.captured().is_empty(),
        "neither the refused commit nor the held one reached the catalog"
    );
    assert!(gate.pass_held_commit());
    let passed = retried
        .await
        .assured("the commit's request task completes")
        .assured("the catalog answers a passed commit");
    assert_eq!(passed.status(), 200);

    gate.pass_commits();
    let unheld = client
        .post(format!("{}{TABLE_RESOURCE}", gate.uri()))
        .body(r#"{"updates":[{"action":"add-snapshot"}]}"#)
        .send()
        .await
        .assured("a gate that passes every commit forwards it at once");
    assert_eq!(unheld.status(), 200);
    let received = catalog
        .wait_for_requests(2, OUTCOME_BOUND)
        .await
        .assured("the catalog answered both passed commits, so it received them first");
    assert_eq!(
        received.len(),
        2,
        "only the passed commits reached the catalog"
    );
    for request in received {
        assert_eq!(request.request_line(), format!("POST {TABLE_RESOURCE}"));
    }
}
