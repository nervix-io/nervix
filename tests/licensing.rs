//! Public observations of the cluster's licensing policy.
//!
//! Layer: test harness.
//! - **Owns.** Assertions over the public licensing description and quota fixtures.
//! - **Depends on.** Scenario sessions, cluster fixtures and public observations.
//! - **Must not know.** Policy evaluation, stored records or admission implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the public licensing scenarios")
)]

use std::time::Duration;

use cucumber::{gherkin::Step, given, then, when};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_core::Client;
use nervix_primitives::time::{sleep, timeout};

use super::{
    ScenarioWorld, common::status_request::STATUS_WAIT_BUDGET, docstring, expand_placeholders,
};
mod notifications;

#[given(expr = "the free licensing quota {string} is limited to {int}")]
fn limit_quota(world: &mut ScenarioWorld, condition: String, limit: u64) {
    let name = match condition.as_str() {
        "cluster.nodes" => nervix_server::licensing::QuotaName::ClusterNodes,
        "cluster.domains" => nervix_server::licensing::QuotaName::ClusterDomains,
        _ => panic!("the scenario names an undeclared cluster quota: {condition}"),
    };
    world.fault_injection.set_licensing_quota(name, limit);
}

#[then(expr = "node {string} eventually describes this license without a selected domain")]
async fn observe_license(world: &mut ScenarioWorld, node: String, #[step] step: &Step) {
    let node = expand_placeholders(world, &node);
    let expected = expand_placeholders(world, docstring(step));
    let uri = world
        .cluster()
        .grpc_uri(&node)
        .assured("the scenario started this node");
    let client = Client::connect_with_options(
        &uri,
        None,
        super::client_connect_options(&uri).assured("the fixture provides native TLS"),
    )
    .await
    .assured("the public native client connects without selecting a domain");
    let mut last = None;
    let observed = timeout(STATUS_WAIT_BUDGET, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let outcome = client
                .execute("DESCRIBE LICENSE;")
                .await
                .assured("the licensing request completes");
            let matches = outcome.succeeded()
                && expected
                    .lines()
                    .filter(|line| !line.trim().is_empty())
                    .all(|line| outcome.message.lines().any(|actual| actual == line));
            last = Some(outcome);
            if matches {
                break;
            }
            nervix_primitives::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    let outcome = last.verified("at least one bounded description completed");
    assert!(
        observed.is_ok(),
        "license description did not contain {expected:?}: {}",
        outcome.message
    );
    world.last_command_output = Some(outcome.message.clone());
    world.last_client_outcome = Some(outcome);
}

#[then("the last license inspection has typed cluster scope")]
fn typed_scope(world: &mut ScenarioWorld) {
    use nervix_client_wire::{StateInspectionEntity, StateInspectionResult};
    let outcome = world
        .last_client_outcome
        .as_ref()
        .verified("a native description completed");
    let state = outcome
        .state
        .as_ref()
        .verified("the server supplied its typed observation");
    assert_eq!(state.entity, StateInspectionEntity::ClusterLicense);
    assert_eq!(
        state
            .display_text()
            .assured("the typed observation renders"),
        outcome.message
    );
    let host = nervix_client_host::Inspection::new(state.as_ref().clone());
    assert_eq!(host.domain(), None);
    assert_eq!(host.entity_kind(), "LICENSE");
    assert_eq!(host.entity_name(), None);
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("the completed licensing observation is a page");
    };
    assert_eq!(page.unit_count, 1);
    assert_eq!(page.counts.rows, 3);
    assert_eq!(page.counts.branches, 0);
    assert_eq!(page.units[0].unit, nervix_models::Unit::SINGLE);
    assert_eq!(page.units[0].origin.node.node_id(), &page.units[0].owner);
    for section in &page.units[0].sections {
        assert!(section.schema.branch.is_none());
        for frame in &section.frames {
            frame
                .conform(&section.schema)
                .assured("every typed frame conforms");
        }
    }
}

#[then("the license description contains these lines")]
fn description_contains(world: &mut ScenarioWorld, #[step] step: &Step) {
    let expected = expand_placeholders(world, docstring(step));
    let output = world
        .last_command_output
        .as_deref()
        .verified("the preceding public description produced output");
    for line in expected.lines().filter(|line| !line.trim().is_empty()) {
        assert!(
            output.lines().any(|actual| actual == line),
            "missing licensing line {line:?} in {output}"
        );
    }
}

/// Check the complete free-mode metric contract from one successful public scrape.
/// A missing sample differs from a zero, including the optional committed decision and deadlines.
#[then(expr = "node {string} licensing metrics match the free policy")]
async fn free_metrics(world: &mut ScenarioWorld, node: String) {
    let node = expand_placeholders(world, &node);
    let url = world
        .cluster()
        .observability_metrics_url(&node)
        .assured("the scenario started this metrics listener");
    let client = reqwest::Client::new();
    let mut last = String::new();
    let outcome = timeout(STATUS_WAIT_BUDGET, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let response = client
                .get(&url)
                .send()
                .await
                .assured("the public metrics listener answers")
                .error_for_status()
                .assured("the public metrics scrape succeeds");
            last = response.text().await.assured("the scrape is valid text");
            if free_metric_contract(&last) {
                break;
            }
            nervix_primitives::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        outcome.is_ok(),
        "free licensing metric contract missing from {last}"
    );
}

fn free_metric_contract(body: &str) -> bool {
    let sample = |name: &str| {
        body.lines().find_map(|line| {
            line.strip_prefix(name)
                .and_then(|suffix| suffix.strip_prefix(' '))
                .and_then(|value| value.parse::<f64>().ok())
        })
    };
    for state in [
        "free",
        "licensed_fresh",
        "licensed_due",
        "licensed_overdue",
        "lapsed",
        "stopped",
        "releasing",
        "degraded",
    ] {
        if sample(&format!("nervix_licensing_state{{state=\"{state}\"}}"))
            != Some(if state == "free" { 1.0 } else { 0.0 })
        {
            return false;
        }
    }
    for condition in ["cluster.nodes", "cluster.domains"] {
        if sample(&format!(
            "nervix_licensing_over_limit{{condition=\"{condition}\"}}"
        )) != Some(0.0)
        {
            return false;
        }
        for state in ["within_grace", "over_limit", "lapsed", "stopped"] {
            if sample(&format!(
                "nervix_licensing_admissions_refused_total{{condition=\"{condition}\",state=\"\
                 {state}\"}}"
            )) != Some(0.0)
            {
                return false;
            }
        }
    }
    for operation in [
        "activation.create",
        "activation.refresh",
        "activation.release",
        "activation.rotate_key",
        "events.subscribe",
    ] {
        for outcome in [
            "committed",
            "rejected_by_cluster",
            "refused_by_service",
            "unreachable",
            "timeout",
        ] {
            if sample(&format!(
                "nervix_licensing_requests_total{{operation=\"{operation}\",outcome=\"{outcome}\"\
                 }}"
            )) != Some(0.0)
            {
                return false;
            }
        }
    }
    sample("nervix_licensing_stream_connected") == Some(0.0)
        && sample("nervix_licensing_transitions_active") == Some(0.0)
        && sample("nervix_licensing_licensing_time_seconds")
            .is_some_and(|value| value.is_finite() && value > 0.0)
        && !body.lines().any(|line| {
            line.starts_with("nervix_licensing_decision_revision ")
                || line.starts_with("nervix_licensing_deadline_timestamp_seconds{")
        })
}

#[given("an HTTPS licensing test service is available")]
async fn start_licensing_service(world: &mut ScenarioWorld) {
    let service = nervix_licensing_test_service::Service::start(chrono::Utc::now())
        .await
        .assured("real signed HTTPS fixture starts");
    world.cluster_config.licensing_test_support =
        nervix_server::ConfiguredLicensingTestSupport::new(
            service.address().clone(),
            service.trust_bundle().await,
            service
                .ca_file()
                .to_str()
                .verified("fixture path is UTF-8")
                .to_owned(),
        );
    world.licensing_service = Some(service);
}

#[then("the licensing service has received no requests")]
async fn no_licensing_requests(world: &mut ScenarioWorld) {
    assert_eq!(
        world
            .licensing_service
            .as_ref()
            .verified("fixture started")
            .request_count()
            .await,
        0
    );
}

async fn leader_license_client(world: &ScenarioWorld) -> Client {
    let node = world
        .cluster()
        .wait_for_leader_among_running()
        .await
        .assured("cluster has a writable leader");
    let uri = world
        .cluster()
        .grpc_uri(&node)
        .assured("leader has a native endpoint");
    Client::connect_with_options(
        &uri,
        None,
        super::client_connect_options(&uri).assured("native TLS options"),
    )
    .await
    .assured("native licensing session connects")
}

async fn current_license(world: &ScenarioWorld) -> std::collections::BTreeMap<String, String> {
    let client = leader_license_client(world).await;
    let outcome = client
        .execute("DESCRIBE LICENSE;")
        .await
        .assured("public license inspection completes");
    assert!(outcome.succeeded(), "{}", outcome.message);
    inspected_license_fields(
        &outcome.message,
        outcome
            .state
            .as_ref()
            .verified("inspection includes typed rows"),
    )
}

fn inspected_license_fields(
    message: &str,
    state: &nervix_client_wire::StateInspection,
) -> std::collections::BTreeMap<String, String> {
    let mut fields = license_fields(message);
    let nervix_client_wire::StateInspectionResult::Page(page) = &state.result else {
        panic!("license inspection returned a page");
    };
    for unit in &page.units {
        for section in &unit.sections {
            for frame in &section.frames {
                for row in frame.batch().rows() {
                    for (index, field) in section.schema.fields.iter().enumerate() {
                        let value = match row.get(index) {
                            Some(nervix_client_wire::CellView::Datetime(value)) => {
                                Some(value.into_datetime().to_rfc3339())
                            }
                            Some(nervix_client_wire::CellView::U64(value)) => {
                                Some(value.to_string())
                            }
                            Some(nervix_client_wire::CellView::String(value)) => {
                                Some(value.to_owned())
                            }
                            _ => None,
                        };
                        if let Some(value) = value {
                            fields.insert(field.name.as_str().replace('_', " "), value);
                        }
                    }
                }
            }
        }
    }
    fields
}

fn license_fields(message: &str) -> std::collections::BTreeMap<String, String> {
    message
        .lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect()
}

#[then(expr = "every node has the license decision saved as placeholder {string}")]
async fn every_node_license_matches(world: &mut ScenarioWorld, name: String) {
    for node in world.cluster().node_ids() {
        node_license_matches(world, node, name.clone()).await;
    }
}

#[then(expr = "node {string} reports the licensing service refusal {string}")]
async fn observe_service_refusal(world: &mut ScenarioWorld, node: String, code: String) {
    let uri = world
        .cluster()
        .grpc_uri(&node)
        .assured("observer endpoint exists");
    let client = Client::connect_with_options(
        &uri,
        None,
        super::client_connect_options(&uri).assured("native TLS options"),
    )
    .await
    .assured("observer session connects");
    timeout(STATUS_WAIT_BUDGET, async {
        loop {
            let outcome = client
                .execute("DESCRIBE LICENSE;")
                .await
                .assured("public inspection completes");
            assert!(outcome.succeeded(), "{}", outcome.message);
            let fields = license_fields(&outcome.message);
            if fields
                .get("connectivity")
                .is_some_and(|value| value.starts_with(&format!("refusing {code} since ")))
            {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .assured("typed refusal reaches the leader or its follower publication");
}

/// The inspection helper normalizes its typed UTC datetime cells, independently of display text.
fn public_license_timestamp(value: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(value)
        .assured("the typed public timestamp was normalized as RFC3339")
        .with_timezone(&chrono::Utc)
}

#[when("a persistent license is activated through the running quorum")]
async fn activate_running_quorum(world: &mut ScenarioWorld) {
    let client = leader_license_client(world).await;
    let outcome = client
        .execute("ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';")
        .await
        .assured("activation reaches the running quorum");
    assert!(outcome.succeeded(), "{}", outcome.message);
}

#[then(expr = "the committed license decision is saved as placeholder {string}")]
async fn save_decision(world: &mut ScenarioWorld, name: String) {
    let license = timeout(STATUS_WAIT_BUDGET, async {
        loop {
            let license = current_license(world).await;
            if license.get("verification").map(String::as_str) == Some("verified") {
                break license;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .assured("committed verified activation reaches public inspection");
    world.placeholders.insert(
        name,
        serde_json::to_string(&license).assured("observation serializes"),
    );
}

fn saved_license(world: &ScenarioWorld, name: &str) -> std::collections::BTreeMap<String, String> {
    serde_json::from_str(
        world
            .placeholders
            .get(name)
            .verified("license observation was saved"),
    )
    .assured("saved observation parses")
}

#[when("a persistent license is activated with a stable reference")]
async fn stable_activation(world: &mut ScenarioWorld) {
    let client = leader_license_client(world).await;
    let execution = client
        .prepare_execution("ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';")
        .await;
    let outcome = client
        .execute_prepared(&execution)
        .await
        .assured("prepared activation completes");
    assert!(outcome.succeeded(), "{}", outcome.message);
    world.placeholders.insert(
        "license_activation_reference".to_owned(),
        execution.reference().to_string(),
    );
    let count = world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .requests()
        .await
        .len();
    world.placeholders.insert(
        "license_activation_request_count".to_owned(),
        count.to_string(),
    );
}

#[when("the license activation with the same reference is repeated")]
async fn repeat_stable_activation(world: &mut ScenarioWorld) {
    let reference = nervix_models::CommandExecutionReference::parse(
        world
            .placeholders
            .get("license_activation_reference")
            .verified("activation reference was retained")
            .clone(),
    )
    .assured("retained public reference parses");
    let client = leader_license_client(world).await;
    let execution = client
        .prepare_execution("ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';")
        .await
        .with_reference(reference);
    let outcome = client
        .execute_prepared(&execution)
        .await
        .assured("repeated activation completes");
    assert!(outcome.succeeded(), "{}", outcome.message);
    world.placeholders.insert(
        "license_activation_recovered".to_owned(),
        (outcome.origin == Some(nervix_client_core::OutcomeOrigin::Recovered)).to_string(),
    );
}

#[then("the repeated activation recovers its outcome without a service request")]
async fn repeated_activation_is_recovered(world: &mut ScenarioWorld) {
    assert_eq!(
        world
            .placeholders
            .get("license_activation_recovered")
            .map(String::as_str),
        Some("true")
    );
    let before = world
        .placeholders
        .get("license_activation_request_count")
        .verified("first activation request count was retained")
        .parse::<usize>()
        .assured("count is an integer");
    assert_eq!(
        world
            .licensing_service
            .as_ref()
            .verified("fixture started")
            .requests()
            .await
            .len(),
        before
    );
}

#[then(expr = "the committed license decision matches placeholder {string}")]
async fn unchanged_decision(world: &mut ScenarioWorld, name: String) {
    let saved = saved_license(world, &name);
    let current = current_license(world).await;
    assert_same_decision(&current, &saved);
}

#[then(
    expr = "the committed license decision matches placeholder {string} with a revoked signing key"
)]
async fn unchanged_decision_with_revoked_key(world: &mut ScenarioWorld, name: String) {
    let mut saved = saved_license(world, &name);
    let decision = saved
        .get("decision")
        .verified("the saved activation has a decision")
        .strip_suffix(" verified")
        .verified("the saved decision was verified before revocation");
    saved.insert(
        "decision".into(),
        format!("{decision} verified (signing key revoked)"),
    );
    saved.insert(
        "verification".into(),
        "verified (signing key revoked)".into(),
    );
    let current = current_license(world).await;
    assert_same_decision(&current, &saved);
}

fn assert_same_decision(
    current: &std::collections::BTreeMap<String, String>,
    saved: &std::collections::BTreeMap<String, String>,
) {
    for field in [
        "decision",
        "verification",
        "activation",
        "decision revision",
        "conditions revision",
    ] {
        assert_eq!(
            current.get(field),
            saved.get(field),
            "{field} changed after rejection"
        );
    }
}

#[then(expr = "the committed license decision is newer than placeholder {string}")]
async fn newer_decision(world: &mut ScenarioWorld, name: String) {
    let saved = saved_license(world, &name);
    let revision = |observation: &std::collections::BTreeMap<String, String>| {
        observation
            .get("decision revision")
            .verified("activated revision")
            .parse::<u64>()
            .assured("typed revision renders as u64")
    };
    let current = timeout(STATUS_WAIT_BUDGET, async {
        loop {
            let current = current_license(world).await;
            if current.get("decision revision").is_some_and(|value| {
                value.parse::<u64>().assured("typed revision") > revision(&saved)
            }) {
                break current;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .assured("newer committed decision reaches public inspection");
    assert_eq!(current.get("activation"), saved.get("activation"));
    assert_eq!(
        current.get("verification").map(String::as_str),
        Some("verified")
    );
}

#[given(expr = "the next licensing response has {string} tampering")]
async fn tamper_response(world: &mut ScenarioWorld, name: String) {
    use nervix_licensing_test_service::{Fault, Tamper};
    let tamper = match name.as_str() {
        "signature" => Tamper::Signature,
        "unauthorized_key" => Tamper::UnauthorizedKey,
        "revoked_key" => Tamper::RevokedKey,
        "cluster" => Tamper::Cluster,
        "key" => Tamper::Key,
        "generation" => Tamper::Generation,
        "activation" => Tamper::Activation,
        "revision" => Tamper::Revision,
        "timing" => Tamper::Timing,
        "service" => Tamper::Service,
        "request" => Tamper::Request,
        "earlier_revision" => Tamper::PriorDecision,
        _ => panic!("undeclared signed tampering fixture {name}"),
    };
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| model.script(Fault::Tamper(tamper)))
        .await;
}

#[when("an explicit license refresh is refused by decision verification")]
async fn refused_refresh(world: &mut ScenarioWorld) {
    let client = leader_license_client(world).await;
    let outcome = client
        .execute("REFRESH LICENSE;")
        .await
        .assured("refresh has a terminal public outcome");
    assert!(
        !outcome.succeeded(),
        "tampered decision was accepted: {}",
        outcome.message
    );
    assert!(
        outcome
            .message
            .contains("licensing response was rejected by the cluster"),
        "{}",
        outcome.message
    );
    world.last_command_output = Some(outcome.message.clone());
    world.last_client_outcome = Some(outcome);
}

#[then(expr = "the licensing request outcome {string} was reported")]
async fn request_outcome(world: &mut ScenarioWorld, outcome: String) {
    let node = world
        .cluster()
        .wait_for_leader_among_running()
        .await
        .assured("writable leader");
    let url = world
        .cluster()
        .observability_metrics_url(&node)
        .assured("metrics endpoint");
    let text = reqwest::get(url)
        .await
        .assured("public metrics scrape")
        .text()
        .await
        .assured("metrics body");
    assert!(
        text.lines()
            .filter(|line| line.starts_with("nervix_licensing_requests_total{")
                && line.contains(&format!("outcome=\"{outcome}\"")))
            .any(|line| line
                .rsplit_once(' ')
                .is_some_and(|(_, value)| value.parse::<f64>().is_ok_and(|value| value >= 1.0))),
        "outcome {outcome} absent in {text}"
    );
}

#[given("an HTTPS licensing test service has bounded offline allowance")]
async fn bounded_service(world: &mut ScenarioWorld) {
    use nervix_licensing_protocol::conditions::{
        ConditionDuration, ConditionValue, OFFLINE_ALLOWANCE,
    };
    start_licensing_service(world).await;
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| {
            let mut plan = model.conditions();
            plan.operating.durations.insert(
                OFFLINE_ALLOWANCE.to_owned(),
                ConditionValue::Known(ConditionDuration::Bounded { seconds: 2_400 }),
            );
            model.plan(plan);
        })
        .await;
}

#[when("the licensing service becomes unreachable")]
async fn offline(world: &mut ScenarioWorld) {
    world.licensing_notice_client = Some(leader_license_client(world).await);
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .set_online(false);
}

#[when("the licensing service becomes reachable")]
fn online(world: &mut ScenarioWorld) {
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .set_online(true);
}

#[when(expr = "the cluster licensing clock passes the {string} deadline")]
async fn pass_deadline(world: &mut ScenarioWorld, name: String) {
    let license = current_license(world).await;
    let field = match name.as_str() {
        "overdue" => "overdue at",
        "valid_until" => "valid until",
        "refresh_after" => "refresh after",
        "ephemeral" => "ephemeral expires at",
        "grants_until" => "grants until",
        _ => panic!("undeclared licensing deadline"),
    };
    let deadline =
        public_license_timestamp(license.get(field).verified("signed deadline is present"));
    let after = deadline
        .checked_add_signed(chrono::TimeDelta::seconds(1))
        .verified("fixture deadline fits UTC");
    world.licensing_clock_offset = after
        .signed_duration_since(chrono::Utc::now())
        .to_std()
        .unwrap_or(Duration::ZERO);
    world
        .fault_injection
        .set_licensing_clock_offset(world.licensing_clock_offset);
}

#[when("both licensing clocks pass the refresh deadline")]
async fn automatic_refresh_deadline(world: &mut ScenarioWorld) {
    let license = current_license(world).await;
    let deadline = public_license_timestamp(
        license
            .get("refresh after")
            .verified("signed refresh deadline is present"),
    )
    .checked_add_signed(chrono::TimeDelta::seconds(1))
    .verified("fixture deadline fits UTC");
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| model.set_time(deadline))
        .await;
    world.licensing_clock_offset = deadline
        .signed_duration_since(chrono::Utc::now())
        .to_std()
        .unwrap_or(Duration::ZERO);
    world
        .fault_injection
        .set_licensing_clock_offset(world.licensing_clock_offset);
}

#[when("the licensing service clock catches up with the cluster")]
async fn service_clock_catches_up(world: &mut ScenarioWorld) {
    let now = chrono::Utc::now()
        .checked_add_signed(
            chrono::TimeDelta::from_std(world.licensing_clock_offset)
                .assured("bounded fixture offset"),
        )
        .verified("test time fits UTC");
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| model.set_time(now))
        .await;
}

#[then(expr = "the signed license deadlines match placeholder {string}")]
async fn unchanged_deadlines(world: &mut ScenarioWorld, name: String) {
    let saved = saved_license(world, &name);
    let current = current_license(world).await;
    for field in [
        "refresh after",
        "overdue at",
        "valid until",
        "decision revision",
    ] {
        assert_eq!(
            current.get(field),
            saved.get(field),
            "failed requests moved {field}"
        );
    }
}

#[then(expr = "a licensing Warning notice saying {string} is observed")]
async fn licensing_warning(world: &mut ScenarioWorld, text: String) {
    let client = world
        .licensing_notice_client
        .as_ref()
        .verified("notice session opened before the deadline crossing");
    timeout(STATUS_WAIT_BUDGET, async {
        loop {
            let event = client
                .next_server_event()
                .await
                .assured("public notice stream remains open");
            if event.level == nervix_client_wire::NoticeLevel::Warning
                && event.kind == nervix_client_wire::NoticeKind::Licensing
                && event.message.contains(&text)
            {
                break;
            }
        }
    })
    .await
    .assured("typed licensing warning arrives");
}

#[given(expr = "the licensing service refuses with {string}")]
#[when(expr = "the licensing service refuses with {string}")]
async fn refuse_service(world: &mut ScenarioWorld, name: String) {
    use nervix_licensing_protocol::problems::ProblemKind;
    let refusal = match name.as_str() {
        "license_key_invalid" => ProblemKind::LicenseKeyInvalid,
        "license_key_revoked" => ProblemKind::LicenseKeyRevoked,
        "kind_not_allowed" => ProblemKind::KindNotAllowed,
        "license_not_active" => ProblemKind::LicenseNotActive,
        "quota_exhausted" => ProblemKind::QuotaExhausted,
        _ => panic!("undeclared refusal {name}"),
    };
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| model.refuse(Some(refusal)))
        .await;
}

#[given("the first activation response is lost after processing")]
#[given("the next licensing operation response is lost after processing")]
async fn lose_activation_response(world: &mut ScenarioWorld) {
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| model.script(nervix_licensing_test_service::Fault::LoseResponse))
        .await;
}

#[given("the next licensing rotation is refused")]
async fn refuse_rotation(world: &mut ScenarioWorld) {
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| {
            model.script(nervix_licensing_test_service::Fault::Refuse(
                nervix_licensing_protocol::problems::ProblemKind::ProofInvalid,
            ));
        })
        .await;
}

#[then(
    expr = "the rotation preserves identity and every signed deadline from placeholder {string}"
)]
async fn preserved_rotation(world: &mut ScenarioWorld, name: String) {
    let saved = saved_license(world, &name);
    let current = current_license(world).await;
    for field in [
        "cluster uid",
        "activation",
        "ephemeral expires at",
        "refresh after",
        "overdue at",
        "valid until",
    ] {
        assert_eq!(
            current.get(field),
            saved.get(field),
            "rotation moved {field}"
        );
    }
    assert_ne!(
        current.get("cluster key"),
        saved.get("cluster key"),
        "rotation must replace its public key"
    );
    let generation = |values: &std::collections::BTreeMap<String, String>| {
        values
            .get("cluster key generation")
            .verified("typed generation")
            .parse::<u64>()
            .assured("generation is u64")
    };
    assert_eq!(generation(&current), generation(&saved) + 1);
}

#[then("the licensing service replayed one rotation request identity")]
async fn rotation_replay(world: &mut ScenarioWorld) {
    let requests = world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .requests()
        .await;
    let rotations = requests
        .iter()
        .filter(|request| {
            matches!(
                request,
                nervix_licensing_protocol::requests::Request::RotateKey { .. }
            )
        })
        .collect::<Vec<_>>();
    assert!(rotations.len() >= 2);
    for request in &rotations {
        assert_eq!(request, &rotations[0]);
    }
}

#[when("the licensing service revokes its working key")]
async fn revoke_working_key(world: &mut ScenarioWorld) {
    let refreshes = world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .requests()
        .await
        .into_iter()
        .filter_map(|request| match request {
            nervix_licensing_protocol::requests::Request::Refresh { common, .. } => {
                Some(common.request_id)
            }
            _ => None,
        })
        .collect::<std::collections::BTreeSet<_>>();
    world.placeholders.insert(
        "refreshes_before_revocation".to_owned(),
        refreshes.len().to_string(),
    );
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| model.revoke_working_key().assured("real signed revocation"))
        .await;
}

#[then("the revocation triggers a background refresh before its ordinary deadline")]
async fn revocation_triggers_refresh(world: &mut ScenarioWorld) {
    let before = world
        .placeholders
        .get("refreshes_before_revocation")
        .verified("pre-revocation requests were saved")
        .parse::<usize>()
        .assured("saved count");
    timeout(STATUS_WAIT_BUDGET, async {
        loop {
            let requests = world
                .licensing_service
                .as_ref()
                .verified("fixture started")
                .requests()
                .await;
            let refreshes = requests
                .into_iter()
                .filter_map(|request| match request {
                    nervix_licensing_protocol::requests::Request::Refresh { common, .. } => {
                        Some(common.request_id)
                    }
                    _ => None,
                })
                .collect::<std::collections::BTreeSet<_>>();
            if refreshes.len() >= before + 2 {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .assured("revocation adds a background refresh beside the explicit request");
    let current = current_license(world).await;
    assert!(
        public_license_timestamp(current.get("licensing time").verified("committed time"))
            < public_license_timestamp(
                current
                    .get("refresh after")
                    .verified("ordinary refresh deadline")
            )
    );
}

#[when("the licensing service installs a replacement working key")]
async fn replace_working_key(world: &mut ScenarioWorld) {
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| {
            model
                .replace_working_key()
                .assured("real signed authorization")
        })
        .await;
}

#[when(expr = "the licensing service advances its root epoch with succession {string}")]
async fn succeed_roots(world: &mut ScenarioWorld, supplied: String) {
    let supplied = match supplied.as_str() {
        "provided" => true,
        "missing" => false,
        _ => panic!("declared succession choice"),
    };
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| {
            model
                .succeed_roots(supplied)
                .assured("real root and working key change")
        })
        .await;
}

#[when("the licensing administrator transfers the activation")]
async fn transfer_activation(world: &mut ScenarioWorld) {
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| model.transfer_activations())
        .await;
}

#[given("the licensing service allows one persistent activation")]
async fn one_persistent_slot(world: &mut ScenarioWorld) {
    use nervix_licensing_protocol::conditions::*;
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| {
            let mut plan = model.conditions();
            plan.operating.quotas.insert(
                PERSISTENT_ACTIVATIONS.to_owned(),
                ConditionValue::Known(Quota::Bounded { value: 1 }),
            );
            model.plan(plan);
        })
        .await;
}

#[when("the transferred installation is rebuilt and activates anew")]
async fn rebuilt_activation(world: &mut ScenarioWorld) {
    let mut config = world.cluster_config.clone();
    config.node_name_prefix = "rebuilt".to_owned();
    let mut rebuilt = super::common::cluster::Cluster::start_with_config(
        world.cluster().node_ids().len(),
        nervix_server::FaultInjection::default(),
        config,
        world.scenario_identity(),
    )
    .await
    .assured("clean independent cluster starts");
    let result = async {
        let leader = rebuilt.wait_for_leader_among_running().await?;
        let uri = rebuilt.grpc_uri(&leader)?;
        let client = Client::connect_with_options(&uri, None, super::client_connect_options(&uri)?)
            .await
            .map_err(std::io::Error::other)?;
        client
            .execute("ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';")
            .await
            .map_err(std::io::Error::other)
    }
    .await;
    rebuilt
        .shutdown()
        .await
        .assured("rebuilt fixture is joined before asserting");
    let outcome = result.assured("new activation completes");
    assert!(outcome.succeeded(), "{}", outcome.message);
}

#[then("the licensing service holds one active activation after transfer")]
async fn one_active_after_transfer(world: &mut ScenarioWorld) {
    let (all, active) = world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| (model.activation_count(), model.active_activation_count()))
        .await;
    assert_eq!(all, 2);
    assert_eq!(active, 1);
}

#[when("the licensing service reduces the domain quota with a transition")]
async fn reduce_domain_quota(world: &mut ScenarioWorld) {
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| model.reduce_domain_quota_with_grace())
        .await;
}

#[then("the released cluster retains a grant until its signed deadline")]
async fn release_has_grant(world: &mut ScenarioWorld) {
    let current = current_license(world).await;
    assert_eq!(current.get("mode").map(String::as_str), Some("released"));
    assert!(
        current.contains_key("grants until"),
        "release must retain the transition deadline: {current:?}"
    );
    let requests = world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .requests()
        .await;
    assert!(requests.iter().any(|request| matches!(request, nervix_licensing_protocol::requests::Request::Release { grants_until, .. }
        if chrono::DateTime::parse_from_rfc3339(current.get("grants until").verified("typed grant deadline")).map(|time| time.with_timezone(&chrono::Utc) == *grants_until).unwrap_or(false))));
}

#[given("the licensing service has already processed the activation when it answers")]
async fn adoption_response(world: &mut ScenarioWorld) {
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| model.script(nervix_licensing_test_service::Fault::Adopt))
        .await;
}

#[then("the licensing service holds one activation and one creation request identity")]
async fn one_activation(world: &mut ScenarioWorld) {
    let service = world.licensing_service.as_ref().verified("fixture started");
    assert_eq!(service.configure(|model| model.activation_count()).await, 1);
    let ids: std::collections::BTreeSet<_> = service
        .requests()
        .await
        .into_iter()
        .filter_map(|request| match request {
            nervix_licensing_protocol::requests::Request::Create { common, .. } => {
                Some(common.request_id)
            }
            _ => None,
        })
        .collect();
    assert_eq!(ids.len(), 1);
}

#[then("the licensing service received an adoption refresh without committed revisions")]
async fn adopted(world: &mut ScenarioWorld) {
    assert!(
        world
            .licensing_service
            .as_ref()
            .verified("fixture started")
            .requests()
            .await
            .iter()
            .any(|request| matches!(
                request,
                nervix_licensing_protocol::requests::Request::Refresh {
                    state: nervix_licensing_protocol::requests::RefreshState::Adoption,
                    ..
                }
            ))
    );
}

#[then(expr = "node {string} has the license decision saved as placeholder {string}")]
async fn node_license_matches(world: &mut ScenarioWorld, node: String, name: String) {
    let node = expand_placeholders(world, &node);
    let expected = saved_license(world, &name);
    let uri = world.cluster().grpc_uri(&node).assured("node endpoint");
    let client = Client::connect_with_options(
        &uri,
        None,
        super::client_connect_options(&uri).assured("TLS options"),
    )
    .await
    .assured("public session connects");
    timeout(STATUS_WAIT_BUDGET, async {
        loop {
            let outcome = client
                .execute("DESCRIBE LICENSE;")
                .await
                .assured("inspection completes");
            let current = inspected_license_fields(
                &outcome.message,
                outcome.state.as_ref().verified("typed inspection"),
            );
            if [
                "decision",
                "activation",
                "verification",
                "cluster uid",
                "cluster key",
                "cluster key generation",
            ]
            .iter()
            .all(|field| current.get(*field) == expected.get(*field))
            {
                break;
            }
            nervix_primitives::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .assured("node retains and verifies the same signed decision");
}

#[when("the cluster restarts with a damaged stored license decision")]
async fn damage_stored_license(world: &mut ScenarioWorld) {
    world
        .cluster_mut()
        .restart_inspecting_stopped_databases(|databases| {
            for database in databases {
                nervix_consensus::ConsensusTestProbe::rewrite_stopped_licensing_record(
                    &database.path.join("consensus"),
                    |record| {
                        let nervix_consensus::LicensingMode::Activated(activation) =
                            &mut record.mode
                        else {
                            panic!("fixture activated before damage");
                        };
                        activation.decision.bytes.push('!');
                    },
                )
                .assured("current stored record is edited while stopped");
            }
        })
        .await
        .assured("semantic signed damage does not prevent node startup");
}

#[when("two three-node clusters race for the last persistent activation slot")]
async fn last_activation_slot(world: &mut ScenarioWorld) {
    use nervix_licensing_protocol::conditions::{ConditionValue, PERSISTENT_ACTIVATIONS, Quota};

    use super::common::cluster::Cluster;
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| {
            let mut plan = model.conditions();
            plan.operating.quotas.insert(
                PERSISTENT_ACTIVATIONS.to_owned(),
                ConditionValue::Known(Quota::Bounded { value: 1 }),
            );
            model.plan(plan);
        })
        .await;
    let mut config = world.cluster_config.clone();
    config.node_name_prefix = "peer".to_owned();
    let mut peer = Cluster::start_with_config(
        3,
        nervix_server::FaultInjection::default(),
        config,
        world.scenario_identity(),
    )
    .await
    .assured("second independent quorum starts");
    let result = async {
        let first = leader_license_client(world).await;
        let leader = peer.wait_for_leader_among_running().await?;
        let uri = peer.grpc_uri(&leader)?;
        let second = Client::connect_with_options(&uri, None, super::client_connect_options(&uri)?)
            .await
            .map_err(std::io::Error::other)?;
        let command = "ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-TESTABCD-fixture-secret';";
        let (one, two) = futures_util::join!(first.execute(command), second.execute(command));
        Ok::<_, std::io::Error>((
            one.map_err(std::io::Error::other)?,
            two.map_err(std::io::Error::other)?,
        ))
    }
    .await;
    let shutdown = peer.shutdown().await;
    shutdown.assured("second quorum ends before asserting the race");
    let (one, two) = result.assured("both public activation requests complete");
    assert_ne!(
        one.succeeded(),
        two.succeeded(),
        "exactly one activation must commit: {}, {}",
        one.message,
        two.message
    );
    let refused = if one.succeeded() { &two } else { &one };
    assert!(
        refused.message.contains("quota_exhausted"),
        "{}",
        refused.message
    );
    assert_eq!(
        world
            .licensing_service
            .as_ref()
            .verified("fixture started")
            .configure(|model| model.activation_count())
            .await,
        1
    );
}

#[when("the cluster restarts with an edited signed ephemeral expiry")]
async fn edit_signed_expiry(world: &mut ScenarioWorld) {
    use base64::Engine as _;
    world
        .cluster_mut()
        .restart_inspecting_stopped_databases(|databases| {
            for database in databases {
                nervix_consensus::ConsensusTestProbe::rewrite_stopped_licensing_record(
                    &database.path.join("consensus"),
                    |record| {
                        let nervix_consensus::LicensingMode::Activated(activation) =
                            &mut record.mode
                        else {
                            panic!("fixture activated before editing");
                        };
                        let fields: Vec<_> = activation.decision.bytes.split('.').collect();
                        assert_eq!(fields.len(), 3);
                        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                            .decode(fields[1])
                            .assured("current JWS payload is base64url");
                        let mut payload: serde_json::Value =
                            serde_json::from_slice(&bytes).assured("current signed decision JSON");
                        payload["activation"]["ephemeral_expires_at"] =
                            serde_json::json!("2200-01-01T00:00:00Z");
                        payload["timing"]["ephemeral_expires_at"] =
                            serde_json::json!("2200-01-01T00:00:00Z");
                        let edited = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
                            serde_json::to_vec(&payload).assured("edited current payload encodes"),
                        );
                        activation.decision.bytes = format!("{}.{edited}.{}", fields[0], fields[2]);
                    },
                )
                .assured("signed payload edited without its signature or independent binding");
            }
        })
        .await
        .assured("semantically edited bytes load with a derived refusal");
}

#[then(expr = "the independent activation binding matches placeholder {string}")]
async fn binding_matches(world: &mut ScenarioWorld, name: String) {
    let expected = saved_license(world, &name);
    let current = current_license(world).await;
    assert_eq!(current.get("activation"), expected.get("activation"));
    assert_eq!(
        current.get("ephemeral expires at"),
        expected.get("ephemeral expires at")
    );
}

#[then(expr = "the independent activation expiry from placeholder {string} has been reached")]
async fn binding_expiry_reached(world: &mut ScenarioWorld, name: String) {
    let saved = saved_license(world, &name);
    let expiry = public_license_timestamp(
        saved
            .get("ephemeral expires at")
            .verified("independent expiry was recorded"),
    );
    timeout(STATUS_WAIT_BUDGET, async {
        loop {
            let current = current_license(world).await;
            let time = public_license_timestamp(
                current
                    .get("licensing time")
                    .verified("committed licensing time exists"),
            );
            if time >= expiry {
                assert_eq!(current.get("activation"), saved.get("activation"));
                assert_eq!(
                    current.get("ephemeral expires at"),
                    saved.get("ephemeral expires at")
                );
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .assured(
        "the independent timer commits the original expiry despite unverifiable decision bytes",
    );
}

#[then(expr = "node {string} eventually has cluster key generation {int}")]
async fn key_generation(world: &mut ScenarioWorld, node: String, generation: u64) {
    let uri = world
        .cluster()
        .grpc_uri(&expand_placeholders(world, &node))
        .assured("node endpoint");
    let client = Client::connect_with_options(
        &uri,
        None,
        super::client_connect_options(&uri).assured("TLS options"),
    )
    .await
    .assured("native session connects");
    timeout(STATUS_WAIT_BUDGET, async {
        loop {
            let outcome = client
                .execute("DESCRIBE LICENSE;")
                .await
                .assured("public inspection completes");
            let fields = inspected_license_fields(
                &outcome.message,
                outcome.state.as_ref().verified("typed inspection"),
            );
            if fields.get("cluster key generation") == Some(&generation.to_string()) {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .assured("the typed generation reaches this node");
}
