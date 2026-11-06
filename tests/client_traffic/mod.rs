//! Steps that observe the traffic a node's client listeners carry.
//!
//! Layer: test harness.
//! - **Owns.** Calls a scenario makes with credentials that do not authenticate, the credentials
//!   work a refused attempt costs its node, the node a session is served by, and failing one
//!   accept of a public listener and observing that the listener still serves.
//! - **Depends on.** The scenario cluster, the harness's session client and the shared fault
//!   injector.
//! - **Must not know.** How a node authenticates, counts or accepts.

use std::{collections::BTreeMap, time::Duration};

use cucumber::{then, when};
use meticulous::OptionExt as _;
use nervix_approx_into::CheckedApproxInto as _;
use nervix_primitives::time::Instant;

use super::{ScenarioWorld, expand_placeholders};
use crate::common::{
    cluster::{
        NodeListener, basic_authorization_as, test_basic_authorization,
        test_basic_authorization_for_password,
    },
    raw_session::open_session_as,
};

/// How long a scenario waits for a call it makes to a node's listener to be answered.
const CALL_WAIT: Duration = Duration::from_secs(30);

/// The credentials work a node's executor reports: the jobs its credentials worker completed and
/// the reservations its credentials memory granted.
const CREDENTIALS_JOBS: &str = "nervix_execution_jobs_completed_total";
const CREDENTIALS_JOBS_CLASS: &str = "class=\"credentials_cpu\"";
const CREDENTIALS_RESERVATIONS: &str = "nervix_execution_memory_reservations_total";
const CREDENTIALS_RESERVATIONS_CLASS: &str = "class=\"credentials\"";

/// The family a node counts its client listeners' authentications in.
const CLIENT_AUTHENTICATIONS: &str = "nervix_client_authentications_total";

/// The client listeners and authentication outcomes the family is labelled by.
const TRANSPORTS: [&str; 2] = ["grpc", "console"];
const OUTCOMES: [&str; 4] = ["authenticated", "missing", "rejected", "unavailable"];

/// One series of the authentication family.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct AuthenticationSeries {
    transport: String,
    outcome: String,
}

impl AuthenticationSeries {
    fn labels(&self) -> Vec<String> {
        vec![
            format!("outcome=\"{}\"", self.outcome),
            format!("transport=\"{}\"", self.transport),
        ]
    }
}

/// The authentication counts one node exported before a scenario's attempts. Every node of a
/// cluster refuses the harness's own calls while it starts, before the cluster has its first user,
/// so a scenario measures the attempts it makes from what the node counted before them.
#[derive(Debug, Default)]
pub(crate) struct AuthenticationBaseline {
    node: Option<String>,
    counts: BTreeMap<AuthenticationSeries, i64>,
}

/// Credentials a scenario presents that do not authenticate their call.
#[derive(Debug, Clone, Copy)]
enum RefusedCredentials<'a> {
    /// The test user's name with a password that is not its own.
    WrongPassword,
    /// A name no user of the cluster has.
    UnknownUser,
    /// No Basic credentials at all.
    Missing,
    /// A scenario-selected name with a password that is not its own.
    NamedUser(&'a str),
}

impl RefusedCredentials<'_> {
    fn authorization(self) -> String {
        match self {
            Self::WrongPassword => test_basic_authorization_for_password("not-the-password"),
            Self::UnknownUser => basic_authorization_as("nobody", "not-the-password"),
            Self::Missing => String::new(),
            Self::NamedUser(username) => basic_authorization_as(username, "not-the-password"),
        }
    }
}

/// Opens a native session on `node` with `credentials` and asserts the node refused it with
/// `expected`.
async fn assert_session_refused(
    world: &ScenarioWorld,
    node: &str,
    credentials: RefusedCredentials<'_>,
    expected: &str,
) {
    let node = expand_placeholders(world, node);
    let server = world
        .cluster()
        .grpc_uri(&node)
        .unwrap_or_else(|error| panic!("node '{node}' has no gRPC address: {error}"));
    let opened = nervix_primitives::time::timeout(
        CALL_WAIT,
        open_session_as(&server, &world.domain, &credentials.authorization()),
    )
    .await
    .unwrap_or_else(|_| panic!("node '{node}' did not answer a session with {credentials:?}"))
    .unwrap_or_else(|error| {
        panic!("a session with {credentials:?} could not be attempted: {error}")
    });
    let Err(status) = opened else {
        panic!("node '{node}' opened a session with {credentials:?}");
    };
    assert_eq!(
        format!("{:?}", status.code()),
        expected,
        "a session with {credentials:?} was refused with {status:?}"
    );
}

#[then(
    expr = "a native session opened on node {string} with a wrong password is refused with status \
            {string}"
)]
async fn wrong_password_session_is_refused(
    world: &mut ScenarioWorld,
    node: String,
    status: String,
) {
    assert_session_refused(world, &node, RefusedCredentials::WrongPassword, &status).await;
}

#[then(
    expr = "a native session opened on node {string} as an unknown user is refused with status \
            {string}"
)]
async fn unknown_user_session_is_refused(world: &mut ScenarioWorld, node: String, status: String) {
    assert_session_refused(world, &node, RefusedCredentials::UnknownUser, &status).await;
}

#[then(
    expr = "a burst of {int} refused native sessions as {string} on node {string} takes at least \
            {int} seconds"
)]
async fn refused_sessions_are_paced(
    world: &mut ScenarioWorld,
    attempts: usize,
    username: String,
    node: String,
    seconds: u64,
) {
    let credentials = RefusedCredentials::NamedUser(&username);
    let uppercase = username.to_ascii_uppercase();
    // The first refusal establishes pacing; measure only the later attempts it governs.
    assert_session_refused(world, &node, credentials, "Unauthenticated").await;
    let started = Instant::now();
    nervix_primitives::time::timeout(CALL_WAIT, async {
        for attempt in 0..attempts {
            nervix_primitives::task::consume_budget().await;
            let presented = if attempt.is_multiple_of(2) {
                &username
            } else {
                &uppercase
            };
            assert_session_refused(
                world,
                &node,
                RefusedCredentials::NamedUser(presented),
                "Unauthenticated",
            )
            .await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("node '{node}' did not finish {attempts} refused sessions"));
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_secs(seconds),
        "{attempts} refused sessions as {username:?} on '{node}' took {elapsed:?}; pacing must \
         delay the burst by at least {seconds}s"
    );
}

#[then(
    expr = "a native session opened on node {string} without credentials is refused with status \
            {string}"
)]
async fn credentialless_session_is_refused(
    world: &mut ScenarioWorld,
    node: String,
    status: String,
) {
    assert_session_refused(world, &node, RefusedCredentials::Missing, &status).await;
}

#[then(
    expr = "a native session opened on the server process with a wrong password is refused with \
            status {string}"
)]
async fn server_process_wrong_password_session_is_refused(
    world: &mut ScenarioWorld,
    expected: String,
) {
    let server = world
        .server_process
        .as_ref()
        .unwrap_or_else(|| panic!("a nervix-server process must be started first"))
        .grpc_uri();
    let credentials = RefusedCredentials::WrongPassword;
    let opened = nervix_primitives::time::timeout(
        CALL_WAIT,
        open_session_as(&server, &world.domain, &credentials.authorization()),
    )
    .await
    .unwrap_or_else(|_| panic!("the server process did not answer a session with {credentials:?}"))
    .unwrap_or_else(|error| {
        panic!("a session with {credentials:?} could not be attempted: {error}")
    });
    let Err(status) = opened else {
        panic!("the server process opened a session with {credentials:?}");
    };
    assert_eq!(
        format!("{:?}", status.code()),
        expected,
        "a session with {credentials:?} was refused with {status:?}"
    );
}

#[then(
    expr = "a console authentication check on node {string} with a wrong password is refused with \
            status {int}"
)]
async fn console_check_with_wrong_password_is_refused(
    world: &mut ScenarioWorld,
    node: String,
    expected: u16,
) {
    let node = expand_placeholders(world, &node);
    let console = world
        .cluster()
        .public_listener_addr(&node, NodeListener::Console)
        .unwrap_or_else(|error| panic!("node '{node}' has no console address: {error}"));
    let response = reqwest::Client::new()
        .get(format!("http://{console}/console/auth"))
        .header(
            reqwest::header::AUTHORIZATION,
            RefusedCredentials::WrongPassword.authorization(),
        )
        .timeout(CALL_WAIT)
        .send()
        .await
        .unwrap_or_else(|error| panic!("node '{node}' did not answer the console check: {error}"));
    assert_eq!(
        response.status().as_u16(),
        expected,
        "the console check with a wrong password was answered with {}",
        response.status()
    );
}

/// One credentials figure of a node's executor, as a whole count.
async fn credentials_figure(world: &ScenarioWorld, node: &str, metric: &str, class: &str) -> i64 {
    let value = world
        .cluster()
        .read_observability_metric(node, metric, &[class.to_string()])
        .await
        .unwrap_or_else(|error| panic!("node '{node}' reported no {metric}{{{class}}}: {error}"));
    value
        .checked_approx_into()
        .unwrap_or_else(|| panic!("{metric}{{{class}}} on node '{node}' is not a count: {value}"))
}

/// Opens a native session on `node` with `credentials`, asserts it was refused, and asserts the
/// refusal cost the node exactly one credentials job and one credentials memory reservation.
async fn assert_refused_after_one_verification(
    world: &ScenarioWorld,
    node: &str,
    credentials: RefusedCredentials<'_>,
) {
    let node = expand_placeholders(world, node);
    let jobs = credentials_figure(world, &node, CREDENTIALS_JOBS, CREDENTIALS_JOBS_CLASS).await;
    let reservations = credentials_figure(
        world,
        &node,
        CREDENTIALS_RESERVATIONS,
        CREDENTIALS_RESERVATIONS_CLASS,
    )
    .await;
    assert_session_refused(world, &node, credentials, "Unauthenticated").await;
    let expected_jobs = jobs
        .checked_add(1)
        .assured("a node completes far fewer than i64::MAX credentials jobs");
    let expected_reservations = reservations
        .checked_add(1)
        .assured("a node grants far fewer than i64::MAX credentials reservations");
    world
        .cluster()
        .wait_for_observability_metric_value(
            &node,
            CREDENTIALS_JOBS,
            &[CREDENTIALS_JOBS_CLASS.to_string()],
            expected_jobs,
            None,
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "a session with {credentials:?} must cost node '{node}' one credentials job: \
                 {error}"
            )
        });
    world
        .cluster()
        .wait_for_observability_metric_value(
            &node,
            CREDENTIALS_RESERVATIONS,
            &[CREDENTIALS_RESERVATIONS_CLASS.to_string()],
            expected_reservations,
            None,
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "a session with {credentials:?} must cost node '{node}' one credentials memory \
                 reservation: {error}"
            )
        });
}

#[then(
    expr = "a native session opened on node {string} as an unknown user is refused after one \
            credentials verification"
)]
async fn unknown_user_is_refused_after_one_verification(world: &mut ScenarioWorld, node: String) {
    assert_refused_after_one_verification(world, &node, RefusedCredentials::UnknownUser).await;
}

#[then(
    expr = "a native session opened on node {string} with a wrong password is refused after one \
            credentials verification"
)]
async fn wrong_password_is_refused_after_one_verification(world: &mut ScenarioWorld, node: String) {
    assert_refused_after_one_verification(world, &node, RefusedCredentials::WrongPassword).await;
}

#[then(expr = "the node serving the active session is saved as placeholder {string}")]
fn node_serving_active_session_is_saved(world: &mut ScenarioWorld, placeholder: String) {
    let node = world
        .active_session_node
        .clone()
        .unwrap_or_else(|| panic!("no step recorded the node serving the active session"));
    world.placeholders.insert(placeholder, node);
}

fn node_listener(name: &str) -> NodeListener {
    name.parse()
        .unwrap_or_else(|error| panic!("a scenario names a public listener: {error}"))
}

#[when(expr = "the next accept of the {word} listener on node {string} fails")]
async fn next_listener_accept_fails(world: &mut ScenarioWorld, listener: String, node: String) {
    let node = expand_placeholders(world, &node);
    let address = world
        .cluster()
        .public_listener_addr(&node, node_listener(&listener))
        .unwrap_or_else(|error| panic!("node '{node}' has no {listener} listener: {error}"));
    world.fault_injection.fail_next_listener_accept(address);
    nervix_primitives::time::timeout(
        CALL_WAIT,
        world.fault_injection.listener_accept_failure(address),
    )
    .await
    .unwrap_or_else(|_| {
        panic!("the {listener} listener of node '{node}' never met the accept failure armed for it")
    });
}

#[then(expr = "the {word} listener on node {string} serves a request")]
async fn listener_serves_a_request(world: &mut ScenarioWorld, listener: String, node: String) {
    let node = expand_placeholders(world, &node);
    let listener = node_listener(&listener);
    let address = world
        .cluster()
        .public_listener_addr(&node, listener)
        .unwrap_or_else(|error| panic!("node '{node}' has no {listener:?} listener: {error}"));
    let client = reqwest::Client::new();
    match listener {
        NodeListener::Grpc => {
            let server = world
                .cluster()
                .grpc_uri(&node)
                .unwrap_or_else(|error| panic!("node '{node}' has no gRPC address: {error}"));
            let opened = nervix_primitives::time::timeout(
                CALL_WAIT,
                open_session_as(&server, &world.domain, &test_basic_authorization()),
            )
            .await
            .unwrap_or_else(|_| panic!("node '{node}' did not answer a session"))
            .unwrap_or_else(|error| panic!("a session could not reach node '{node}': {error}"));
            assert!(
                opened.is_ok(),
                "node '{node}' refused a session with the test credentials: {opened:?}",
                opened = opened.err()
            );
        }
        NodeListener::Http => {
            let response = client
                .get(format!("http://{address}/"))
                .timeout(CALL_WAIT)
                .send()
                .await;
            assert!(
                response.is_ok(),
                "node '{node}' HTTP listener answered nothing: {:?}",
                response.err()
            );
        }
        NodeListener::Observability => {
            let response = client
                .get(format!("http://{address}/livez"))
                .timeout(CALL_WAIT)
                .send()
                .await
                .unwrap_or_else(|error| {
                    panic!("node '{node}' observability listener answered nothing: {error}")
                });
            assert_eq!(
                response.status().as_u16(),
                200,
                "/livez answered {}",
                response.status()
            );
        }
        NodeListener::Console => {
            let response = client
                .get(format!("http://{address}/console/auth"))
                .header(reqwest::header::AUTHORIZATION, test_basic_authorization())
                .timeout(CALL_WAIT)
                .send()
                .await
                .unwrap_or_else(|error| {
                    panic!("node '{node}' console listener answered nothing: {error}")
                });
            assert_eq!(
                response.status().as_u16(),
                204,
                "the console check with the test credentials was answered with {}",
                response.status()
            );
        }
    }
}

/// One series of a node's authentication family, as a whole count.
async fn authentication_count(
    world: &ScenarioWorld,
    node: &str,
    series: &AuthenticationSeries,
) -> i64 {
    let value = world
        .cluster()
        .read_observability_metric(node, CLIENT_AUTHENTICATIONS, &series.labels())
        .await
        .unwrap_or_else(|error| panic!("node '{node}' reported no {series:?}: {error}"));
    value
        .checked_approx_into()
        .unwrap_or_else(|| panic!("{series:?} on node '{node}' is not a count: {value}"))
}

#[when(expr = "the client authentication counts of node {string} are remembered")]
async fn client_authentication_counts_are_remembered(world: &mut ScenarioWorld, node: String) {
    let node = expand_placeholders(world, &node);
    let mut counts = BTreeMap::new();
    for transport in TRANSPORTS {
        for outcome in OUTCOMES {
            let series = AuthenticationSeries {
                transport: transport.to_string(),
                outcome: outcome.to_string(),
            };
            let count = authentication_count(world, &node, &series).await;
            counts.insert(series, count);
        }
    }
    world.client_authentication_baseline = AuthenticationBaseline {
        node: Some(node),
        counts,
    };
}

#[then(expr = "node {string} counted {int} more {word} authentication(s) on its {word} listener")]
async fn node_counted_more_authentications(
    world: &mut ScenarioWorld,
    node: String,
    more: i64,
    outcome: String,
    transport: String,
) {
    let node = expand_placeholders(world, &node);
    let baseline = &world.client_authentication_baseline;
    assert_eq!(
        baseline.node.as_deref(),
        Some(node.as_str()),
        "the scenario remembers node '{node}' counts before it compares them"
    );
    let series = AuthenticationSeries { transport, outcome };
    let before = *baseline
        .counts
        .get(&series)
        .unwrap_or_else(|| panic!("no {series:?} count was remembered for node '{node}'"));
    let expected = before
        .checked_add(more)
        .assured("a node counts far fewer than i64::MAX authentications");
    world
        .cluster()
        .wait_for_observability_metric_value(
            &node,
            CLIENT_AUTHENTICATIONS,
            &series.labels(),
            expected,
            None,
        )
        .await
        .unwrap_or_else(|error| {
            panic!("node '{node}' must count {more} more {series:?} than before: {error}")
        });
}
