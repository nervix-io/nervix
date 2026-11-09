//! Public password checks through HTTP, WebSocket and the browser console.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Credential attempts through real listeners and a browser login.
//! - **Depends on.** Public HTTP/WebSocket clients and the scenario cluster.
//! - **Must not know.** Password derivation or stored credential representations.

use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;

use super::*;

fn console_url(
    world: &ScenarioWorld,
    node: &str,
    user: &str,
    password: &str,
) -> anyhow::Result<url::Url> {
    let address = world.cluster().web_console_url(node)?;
    let mut address = url::Url::parse(&address)?;
    address.set_query(None);
    let token = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
    address.query_pairs_mut().append_pair("auth", &token);
    Ok(address)
}

#[then(expr = "user {string} with password {string} is {word} by the HTTP and WebSocket listeners")]
async fn listeners_judge_credentials(
    world: &mut ScenarioWorld,
    user: String,
    password: String,
    judgment: String,
) -> anyhow::Result<()> {
    let accepted = match judgment.as_str() {
        "accepted" => true,
        "refused" => false,
        _ => panic!("expected accepted or refused"),
    };
    let leader = current_leader_node(world).await;
    let mut address = console_url(world, &leader, &user, &password)?;
    address.set_path("/console/auth");
    let response = reqwest::Client::new()
        .get(address.clone())
        .basic_auth(&user, Some(&password))
        .send()
        .await?;
    let expected = if accepted {
        reqwest::StatusCode::NO_CONTENT
    } else {
        reqwest::StatusCode::UNAUTHORIZED
    };
    assert_eq!(response.status(), expected);
    address.set_path("/console/ws");
    address
        .set_scheme("ws")
        .map_err(|()| anyhow::anyhow!("the console URL cannot select WebSocket"))?;
    let request = address.as_str().into_client_request()?;
    match tokio_tungstenite::connect_async(request).await {
        Ok((mut socket, _)) => {
            assert!(accepted, "wrong credentials must not upgrade");
            socket.close(None).await?;
        }
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            assert!(!accepted, "valid credentials must upgrade");
            assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

#[when(expr = "the web console is opened as user {string} with password {string}")]
async fn console_authenticates_user(
    world: &mut ScenarioWorld,
    user: String,
    password: String,
) -> anyhow::Result<()> {
    close_browser(world).await;
    let leader = current_leader_node(world).await;
    let address = console_url(world, &leader, &user, &password)?;
    open_web_console_page(world, address.as_str())
        .await
        .map_err(anyhow::Error::msg)?;
    Ok(())
}
