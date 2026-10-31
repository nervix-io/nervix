//! Outside the layer order: regression coverage of the Pulsar dependency's readiness boundary.
//!
//! - **Owns.** A fresh standalone broker and an immediate default-namespace observation.
//! - **Depends on.** The dependency environment, its published endpoints and the admin HTTP API.
//! - **Must not know.** Nervix connector plans, graph construction or runtime behavior.

use std::{io, time::Duration};

use nervix_test_environment::{ContainerMode, DependencyEnvironment, PULSAR_ADMIN_ADDR};

#[nervix_primitives::test]
async fn fresh_pulsar_serves_its_default_namespace_before_publishing_endpoints() {
    let mut environment = DependencyEnvironment::new(
        format!("pulsar-readiness-{}", uuid::Uuid::now_v7()),
        ContainerMode::Ephemeral,
    );
    let observation = async {
        environment.start_pulsar().await?;
        let admin = environment.endpoints().get(PULSAR_ADMIN_ADDR)?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(io::Error::other)?;
        let response = client
            .get(format!("{admin}/admin/v2/namespaces/public/default"))
            .send()
            .await
            .map_err(io::Error::other)?;
        Ok::<_, io::Error>(response.status())
    }
    .await;
    let cleanup = environment.shutdown().await;
    assert_eq!(
        observation.expect("Pulsar startup and its first namespace observation should finish"),
        reqwest::StatusCode::OK,
        "a published Pulsar endpoint must already serve public/default"
    );
    cleanup.expect("the fresh Pulsar environment should stop cleanly");
}
