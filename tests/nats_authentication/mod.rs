//! NATS authentication through public Nervix commands and real enforcing brokers.
//!
//! Layer: test harness.
//! - **Owns.** Public test identities, mounted credentials and authenticated message publication.
//! - **Depends on.** The scenario harness, test environment and NATS driver.
//! - **Must not know.** Private connector state or authentication implementation.

use std::time::Duration;

use cucumber::{gherkin::Step, given, then};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::time::Instant;
use nervix_test_environment::{NATS_CREDENTIALS_ADDR, NATS_NKEY_ADDR, NATS_TOKEN_ADDR};

use super::{
    ScenarioWorld, docstring, expand_placeholders, initialize_scenario_identity,
    parse_duration_text, refresh_dependency_configuration, try_capture_any_subscription_payload,
};

const TOKEN: &str = "nervix-test-token";
const NKEY: &str = include_str!("../../docker/nats/auth-user.nk");
const CREDENTIALS: &str = include_str!("../../docker/nats/auth-user.creds");

#[given("NATS authentication servers are running")]
async fn authentication_servers(world: &mut ScenarioWorld) {
    initialize_scenario_identity(world);
    world
        .dependencies
        .start_nats_auth(&world.test_id)
        .await
        .assured("the three authentication-enforcing NATS servers start");
    refresh_dependency_configuration(world);
    world.placeholders.insert("nats_token".into(), TOKEN.into());
    world
        .placeholders
        .insert("nats_seed".into(), NKEY.trim().into());
}

#[given(expr = "node {string} has NATS credentials in resource directory {string}")]
async fn mounted_credentials(world: &mut ScenarioWorld, node: String, placeholder: String) {
    let directory = world
        .cluster()
        .node_base_dir(&node)
        .assured("the node has its own fixture directory")
        .join("fixtures")
        .join(&placeholder);
    std::fs::create_dir_all(&directory)
        .assured("the NATS credentials fixture directory is writable");
    std::fs::write(directory.join("user.creds"), CREDENTIALS)
        .assured("the public test credentials are written before upload");
    world
        .placeholders
        .insert(placeholder, directory.display().to_string());
}

#[then(
    expr = "within {string} repeatedly publishing an authenticated NATS {word} message to subject \
            {string} yields a relay subscription payload"
)]
async fn authenticated_publication(
    world: &mut ScenarioWorld,
    duration: String,
    method: String,
    subject: String,
    #[step] step: &Step,
) {
    let duration = parse_duration_text(&duration).assured("the scenario supplies a valid duration");
    let (endpoint, options) = match method.as_str() {
        "token" => (
            NATS_TOKEN_ADDR,
            async_nats::ConnectOptions::new().token(TOKEN.into()),
        ),
        "nkey" => (
            NATS_NKEY_ADDR,
            async_nats::ConnectOptions::new().nkey(NKEY.trim().into()),
        ),
        "credentials" => (
            NATS_CREDENTIALS_ADDR,
            async_nats::ConnectOptions::with_credentials(CREDENTIALS)
                .assured("the fixture holds an account-signed JWT and its user seed"),
        ),
        _ => panic!("the scenario names one of the three declared authentication methods"),
    };
    let address = world
        .dependencies
        .endpoints()
        .get(endpoint)
        .assured("the enforcing server is running");
    let client = options
        .connect(address)
        .await
        .assured("the fixture client authenticates to the real broker");
    let subject = expand_placeholders(world, &subject);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now()
        .checked_add(duration)
        .assured("the scenario deadline fits the clock");
    loop {
        nervix_primitives::task::consume_budget().await;
        client
            .publish(subject.clone(), payload.clone().into())
            .await
            .assured("the authenticated publisher queues the fixture");
        client
            .flush()
            .await
            .assured("the enforcing broker receives the fixture");
        if try_capture_any_subscription_payload(world, Duration::from_millis(500)).await {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "authenticated {method} publication did not traverse the Nervix ingestor and emitter"
        );
    }
}
