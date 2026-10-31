//! Outside the layer order: the scenario's immediate Pulsar dependency readiness assertion.
//!
//! - **Owns.** Observing the default namespace before a scenario constructs its product graph.
//! - **Depends on.** The published admin endpoint and the Pulsar admin HTTP API.
//! - **Must not know.** Connector execution, retry policy or cluster lifecycle decisions.

use std::time::Duration;

use cucumber::then;

use crate::{ScenarioWorld, common::dependencies::PULSAR_ADMIN_ADDR};

#[then("Pulsar serves its default namespace")]
async fn then_pulsar_serves_its_default_namespace(world: &mut ScenarioWorld) {
    let admin = world
        .dependencies
        .endpoints()
        .get(PULSAR_ADMIN_ADDR)
        .expect("the Pulsar dependency should have published its admin endpoint");
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .expect("the harness admin client should build");
    let response = client
        .get(format!("{admin}/admin/v2/namespaces/public/default"))
        .send()
        .await
        .expect("the first Pulsar namespace observation should finish");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "Pulsar must serve public/default when its dependency startup returns"
    );
}
