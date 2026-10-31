//! Outside the layer order: HTTP readiness of an external test dependency.
//!
//! - **Owns.** Waiting for an exact successful HTTP response within one dependency probe budget.
//! - **Depends on.** The dependency's mapped loopback port, HTTP and the primitive timer boundary.
//! - **Must not know.** Product clients, connector retries, graph state or container replacement.

use std::{io, time::Duration};

pub(super) struct HttpReadiness {
    url: String,
}

impl HttpReadiness {
    pub(super) fn new(host_port: u16, path: &str) -> Self {
        Self {
            url: format!("http://127.0.0.1:{host_port}{path}"),
        }
    }

    pub(super) fn client() -> io::Result<reqwest::Client> {
        reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(io::Error::other)
    }

    pub(super) async fn wait(self, budget: Duration) -> io::Result<()> {
        let client = Self::client()?;
        let mut last_observation = None;
        let wait = async {
            loop {
                nervix_primitives::task::consume_budget().await;
                let observation = client.get(&self.url).send().await;
                match observation {
                    Ok(response) => {
                        if response.status() == reqwest::StatusCode::OK {
                            return;
                        }
                        last_observation = Some(Ok(response.status()));
                    }
                    Err(error) => last_observation = Some(Err(error)),
                }
                nervix_primitives::time::sleep(Duration::from_millis(200)).await;
            }
        };
        if nervix_primitives::time::timeout(budget, wait).await.is_ok() {
            return Ok(());
        }
        let last_failure = match last_observation {
            Some(Ok(status)) => format!("last response was HTTP {status}"),
            Some(Err(error)) => format!("last request failed: {error}"),
            None => "no request completed".to_string(),
        };
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "GET {} did not return HTTP 200 OK within {budget:?}; {last_failure}",
                self.url
            ),
        ))
    }
}

#[cfg(test)]
mod tests;
