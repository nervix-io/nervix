//! Public HTTP and subscription probes for logical ingestion admission.
//!
//! Layer: test harness.
//! - **Owns.** Observing timing through configured ingestors and their public subscriptions.
//! - **Depends on.** The scenario cluster and timestamp vocabulary.
//! - **Must not know.** Runtime clock storage or admission implementation.

use cucumber::{given, then};
use nervix_models::{DomainAdmissionWindow, DomainClockPeriod, DomainName, Timestamp};

use super::*;

/// A liveness watchdog for public HTTP and session events; no clock-position arithmetic depends on
/// this duration.
const PUBLIC_PROBE_TIMEOUT: Duration = Duration::from_secs(10);

struct IngestionProbe {
    host: String,
    client: reqwest::Client,
    base_url: url::Url,
}

#[given(
    expr = "domain clock for domain {string} starts with its complete retained admission history \
            for period {string}"
)]
async fn domain_clock_starts_with_complete_retained_admission_history(
    world: &mut ScenarioWorld,
    domain: String,
    period: String,
) {
    let domain = DomainName::parse(&expand_placeholders(world, &domain))
        .assured("the scenario domain placeholder is a valid domain name");
    let period: DomainClockPeriod = period
        .parse()
        .assured("scenario period is positive and supported");
    let retained_position_count =
        u32::try_from(DomainAdmissionWindow::RETAINED_POSITION_COUNT)
            .assured("the retained position count fits Duration's multiplier");
    let elapsed = period
        .as_duration()
        .checked_mul(retained_position_count)
        .assured("the scenario period and retained position count fit Duration");
    world
        .fault_injection
        .set_domain_clock_initial_elapsed(domain, elapsed);
}

impl IngestionProbe {
    fn new(world: &ScenarioWorld, host: &str) -> Self {
        Self {
            host: expand_placeholders(world, host),
            client: reqwest::Client::builder()
                .timeout(PUBLIC_PROBE_TIMEOUT)
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
            .checked_add(PUBLIC_PROBE_TIMEOUT)
            .assured("the public probe timeout fits the monotonic clock");
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
    expr = "admission at host {string} retains 256 positions from {string} with period {string}"
)]
async fn retained_admission(
    world: &mut ScenarioWorld,
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
    let upper_position = DomainAdmissionWindow::RETAINED_POSITION_COUNT
        .checked_add(1)
        .assured("the retained position count is far below u64::MAX");
    let upper_offset = u128::from(upper_position)
        .checked_mul(period_nanos)
        .assured("the fixture period and retained history fit u128");
    let upper = origin
        .checked_add(Duration::from_nanos(
            u64::try_from(upper_offset).assured("the fixture retained history fits Duration"),
        ))
        .assured("the fixture timestamp fits the supported range");

    enum ExpectedAdmission {
        Accepted,
        Rejected,
    }

    struct Candidate {
        timestamp: Timestamp,
        expected: ExpectedAdmission,
    }

    // At frontier `RETAINED_POSITION_COUNT` with one-period skew, position one is the first
    // retained center. Its lower inclusive edge is the origin. The last reached center is at the
    // retained count, so its upper inclusive edge is one further period from the origin.
    let candidates = [
        Candidate {
            timestamp: origin,
            expected: ExpectedAdmission::Accepted,
        },
        Candidate {
            timestamp: origin
                .checked_sub(Duration::from_nanos(1))
                .assured("the fixture origin is far from the minimum timestamp"),
            expected: ExpectedAdmission::Rejected,
        },
        Candidate {
            timestamp: upper,
            expected: ExpectedAdmission::Accepted,
        },
        Candidate {
            timestamp: upper
                .checked_add(Duration::from_nanos(1))
                .assured("the fixture upper edge is far from the maximum timestamp"),
            expected: ExpectedAdmission::Rejected,
        },
    ];

    for (index, candidate) in candidates.into_iter().enumerate() {
        tokio::task::consume_budget().await;
        let sequence = i64::try_from(index)
            .assured("there are four candidates")
            .checked_add(1)
            .assured("four candidates fit i64");
        probe
            .publish("/events", sequence, candidate.timestamp)
            .await;
        match candidate.expected {
            ExpectedAdmission::Accepted => {
                let event = probe
                    .observation(world, PUBLIC_PROBE_TIMEOUT)
                    .await
                    .assured("an inclusive admission edge reaches the public subscription");
                assert_eq!(event["sequence"], sequence);
                let source: Timestamp = event["occurred_at"]
                    .as_str()
                    .assured("source timestamp is preserved")
                    .parse()
                    .assured("source timestamp is supported");
                assert_eq!(source, candidate.timestamp);
            }
            ExpectedAdmission::Rejected => {
                let error = world
                    .active_session
                    .as_mut()
                    .assured("the probe has a public session")
                    .try_next_server_error(PUBLIC_PROBE_TIMEOUT)
                    .await
                    .assured("the public server-error stream remains connected")
                    .assured("an event outside the admission edge produces a server error");
                assert!(
                    error
                        .message
                        .contains("outside any reached logical tick window"),
                    "unexpected server error after rejected timestamp {}: {}",
                    candidate.timestamp,
                    error.message
                );
            }
        }
    }
}
