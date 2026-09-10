//! Public HTTP and subscription probes for logical ingestion admission.
//!
//! Layer: test harness.
//! - **Owns.** Observing timing through configured ingestors and their public subscriptions.
//! - **Depends on.** The scenario cluster and timestamp vocabulary.
//! - **Must not know.** Runtime clock storage or admission implementation.

use cucumber::then;
use nervix_models::{DomainClockPeriod, Timestamp};

use super::*;

struct IngestionProbe {
    host: String,
    client: reqwest::Client,
    base_url: url::Url,
}

impl IngestionProbe {
    fn new(world: &ScenarioWorld, host: &str) -> Self {
        Self {
            host: expand_placeholders(world, host),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .assured("the probe uses the default HTTP client configuration"),
            base_url: world
                .cluster()
                .http_uri("node-1", "/")
                .assured("the scenario starts node-1")
                .parse()
                .assured("the cluster supplies an HTTP URL"),
        }
    }

    async fn publish(&self, path: &str, sequence: i64, event: Timestamp) {
        let payload = serde_json::json!({"sequence": sequence, "occurred_at": event.into_datetime().to_rfc3339()});
        let response = self
            .client
            .post(
                self.base_url
                    .join(path)
                    .assured("probe paths are absolute URL paths"),
            )
            .header("Host", &self.host)
            .json(&payload)
            .send()
            .await
            .unwrap_or_else(|error| panic!("ingestion probe request failed: {error}"));
        assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
        response
            .bytes()
            .await
            .unwrap_or_else(|error| panic!("ingestion probe response failed: {error}"));
    }

    async fn observation(
        &self,
        world: &mut ScenarioWorld,
        wait: Duration,
    ) -> Option<serde_json::Value> {
        let session = world
            .active_session
            .as_mut()
            .assured("the probe has a public subscription");
        let event = session
            .try_next_subscription(wait)
            .await
            .assured("the public subscription remains connected")?;
        Some(serde_json::from_str(&event.payload).assured("the unbranched observation is JSON"))
    }

    async fn clock(&self, world: &mut ScenarioWorld) -> Timestamp {
        self.publish("/clock", 0, Timestamp::from_unix_nanos(0))
            .await;
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(5))
            .assured("five seconds fits the monotonic clock");
        loop {
            tokio::task::consume_budget().await;
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .assured("clock probe responds within five seconds");
            let event = self
                .observation(world, remaining)
                .await
                .assured("clock probe must reach its relay");
            if event["sequence"] == 0 {
                return event["observed_at"]
                    .as_str()
                    .assured("clock output declares DATETIME")
                    .parse()
                    .assured("clock output is a supported timestamp");
            }
        }
    }
}

#[then(
    expr = "within {string} the clock at host {string} advances by {string} and is saved as \
            {string}"
)]
async fn clock_advances(
    world: &mut ScenarioWorld,
    timeout: String,
    host: String,
    advance: String,
    placeholder: String,
) {
    let probe = IngestionProbe::new(world, &host);
    let advance = humantime::parse_duration(&advance).assured("scenario duration is valid");
    let target = probe
        .clock(world)
        .await
        .checked_add(advance)
        .assured("fixture advance fits historical time");
    let deadline = Instant::now()
        .checked_add(humantime::parse_duration(&timeout).assured("scenario timeout is valid"))
        .assured("scenario timeout fits the monotonic clock");
    loop {
        tokio::task::consume_budget().await;
        let now = probe.clock(world).await;
        if now >= target {
            world
                .placeholders
                .insert(placeholder, now.into_datetime().to_rfc3339());
            return;
        }
        assert!(
            Instant::now() < deadline,
            "logical clock did not reach {target}; observed {now}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[then(
    expr = "within {string} admission at host {string} retains 256 positions from {string} with \
            period {string}"
)]
async fn retained_admission(
    world: &mut ScenarioWorld,
    timeout: String,
    host: String,
    origin: String,
    period: String,
) {
    let probe = IngestionProbe::new(world, &host);
    let origin: Timestamp = origin.parse().assured("scenario origin is supported");
    let period: DomainClockPeriod = period
        .parse()
        .assured("scenario period is positive and supported");
    let period_nanos = u128::from(period.as_nanos());
    let deadline = Instant::now()
        .checked_add(humantime::parse_duration(&timeout).assured("scenario timeout is valid"))
        .assured("scenario timeout fits the monotonic clock");
    loop {
        tokio::task::consume_budget().await;
        assert!(
            Instant::now() < deadline,
            "could not check retained admission within one reached position"
        );
        let before = probe.clock(world).await;
        let elapsed = before
            .duration_since(origin)
            .assured("logical observation follows origin")
            .as_nanos();
        let frontier = elapsed / period_nanos;
        // Start near the beginning of a position so the public round trips can finish before
        // the frontier moves. Both example rates provide 100ms of physical time per position.
        if frontier < 256 || elapsed % period_nanos > period_nanos / 8 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        }
        let earliest_offset = frontier
            .checked_sub(256)
            .assured("frontier has reached 256")
            .checked_mul(period_nanos)
            .assured("fixture elapsed is bounded by its timeout");
        let latest_offset = frontier
            .checked_add(1)
            .assured("fixture frontier is bounded by its timeout")
            .checked_mul(period_nanos)
            .assured("fixture elapsed is bounded by its timeout");
        let earliest = origin
            .checked_add(Duration::from_nanos(
                u64::try_from(earliest_offset).assured("fixture elapsed fits u64"),
            ))
            .assured("fixture timestamp fits the supported range");
        let latest = origin
            .checked_add(Duration::from_nanos(
                u64::try_from(latest_offset).assured("fixture elapsed fits u64"),
            ))
            .assured("fixture timestamp fits the supported range");
        struct Candidate {
            timestamp: Timestamp,
            accepted: bool,
        }
        let candidates = [
            Candidate {
                timestamp: earliest,
                accepted: true,
            },
            Candidate {
                timestamp: Timestamp::from_unix_nanos(
                    earliest
                        .unix_nanos()
                        .checked_sub(1)
                        .assured("fixture is far from minimum time"),
                ),
                accepted: false,
            },
            Candidate {
                timestamp: latest,
                accepted: true,
            },
            Candidate {
                timestamp: latest
                    .checked_add(Duration::from_nanos(1))
                    .assured("fixture is far from maximum time"),
                accepted: false,
            },
        ];
        struct Observation {
            candidate: Candidate,
            sequence: i64,
            event: Option<serde_json::Value>,
        }
        let mut observations = Vec::new();
        for (index, candidate) in candidates.into_iter().enumerate() {
            tokio::task::consume_budget().await;
            let sequence = i64::try_from(index)
                .assured("there are four candidates")
                .checked_add(1)
                .assured("four candidates fit i64");
            probe
                .publish("/events", sequence, candidate.timestamp)
                .await;
            let event = probe.observation(world, Duration::from_millis(20)).await;
            observations.push(Observation {
                candidate,
                sequence,
                event,
            });
        }
        let after = probe.clock(world).await;
        if after
            .duration_since(origin)
            .assured("logical observation follows origin")
            .as_nanos()
            / period_nanos
            != frontier
        {
            continue;
        }
        for Observation {
            candidate,
            sequence,
            event,
        } in observations
        {
            assert_eq!(
                event.is_some(),
                candidate.accepted,
                "frontier={frontier}, event={}, observation={event:?}",
                candidate.timestamp
            );
            if let Some(event) = event {
                assert_eq!(event["sequence"], sequence);
                let source: Timestamp = event["occurred_at"]
                    .as_str()
                    .assured("source timestamp is preserved")
                    .parse()
                    .assured("source timestamp is supported");
                assert_eq!(source, candidate.timestamp);
            }
        }
        return;
    }
}
