//! A fixture's real TLS transport must remain usable across a scripted outage.

use meticulous::ResultExt as _;
use nervix_licensing_protocol::requests::{ChallengeRequest, ChallengeResponse};

use super::*;

#[nervix_primitives::test]
async fn https_challenges_reconnect_to_the_same_address_after_a_transport_outage() {
    let now = DateTime::parse_from_rfc3339("2026-11-10T12:00:00Z")
        .assured("fixed fixture time")
        .with_timezone(&Utc);
    let mut service = Service::start(now)
        .await
        .assured("the HTTPS fixture starts");
    let pem = std::fs::read(service.ca_file()).assured("fixture retains its CA file");
    let root = reqwest::Certificate::from_pem(&pem).assured("rcgen emitted a valid CA");
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .add_root_certificate(root)
        .timeout(STOP_BUDGET)
        .build()
        .assured("native HTTPS client builds");
    let url = service
        .address()
        .join("v1/challenges")
        .assured("absolute fixture route");
    let body = ChallengeRequest {
        cluster_uid: model::id(),
    };
    let response = client
        .post(url.clone())
        .json(&body)
        .send()
        .await
        .assured("HTTPS handshake and request succeed");
    assert_eq!(response.status(), 200);
    let challenge: ChallengeResponse = response
        .json()
        .await
        .assured("shared challenge shape decodes");
    assert_eq!(challenge.expires_at, now + chrono::TimeDelta::seconds(300));
    assert!(
        service.requests().await.is_empty(),
        "challenges are unauthenticated and are not activation requests"
    );
    service.set_online(false);
    let unavailable = client.post(url.clone()).json(&body).send().await;
    if let Ok(response) = unavailable {
        assert!(
            response.bytes().await.is_err(),
            "an established connection cannot answer through the outage"
        );
    }
    service.set_online(true);
    let response = client
        .post(url)
        .json(&body)
        .send()
        .await
        .assured("same address accepts a new connection");
    assert_eq!(response.status(), 200);
    let _: ChallengeResponse = response
        .json()
        .await
        .assured("reconnected response is complete");
    service
        .stop()
        .await
        .assured("fixture joins every connection within its bound");
}
