//! Steps that attach sessions and clients to domain clocks and read what they observe.
//!
//! Layer: test harness.
//! - **Owns.** The clock session a scenario attaches with, the requests it sends by name, the
//!   window a `START AT NOW` ran in, and the events a client posts at times its attached clock
//!   computes.
//! - **Depends on.** The harness's own session client, the Rust client, and the scenario cluster.
//! - **Must not know.** How the server observes, installs or delivers a domain clock.

use cucumber::{given, then, when};
use nervix_client_wire::{
    DomainClockAttachDisposition, DomainClockDetachDisposition, ReplyBody, RequestId,
};
use nervix_models::{
    DomainAdmissionWindow, DomainClockObservation, DomainClockObservedState, DomainClockPeriod,
    DomainClockSkew, DomainName, DomainTimeRate, Timestamp,
};

use super::*;
use crate::common::raw_session::{TestClockFrame, TestClockLogEntry};

/// How long a step waits for the reply to a clock request the scenario just sent.
const CLOCK_REPLY_TIMEOUT: Duration = Duration::from_secs(60);

/// The UTC instants just before a `START AT NOW` was sent and just after it completed, which bound
/// the mapping that start established.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ClockStartWindow {
    before: Timestamp,
    after: Timestamp,
}

fn clock_session(world: &mut ScenarioWorld) -> &mut TestSession {
    world
        .clock_session
        .as_mut()
        .expect("a clock session must be opened first")
}

fn scenario_domain(world: &ScenarioWorld, raw: &str) -> DomainName {
    DomainName::parse(&expand_placeholders(world, raw)).expect("scenario domains are valid names")
}

fn named_clock_request(world: &ScenarioWorld, name: &str) -> RequestId {
    *world
        .clock_session_requests
        .get(name)
        .unwrap_or_else(|| panic!("request '{name}' was never sent on the clock session"))
}

async fn clock_reply_to_named(world: &mut ScenarioWorld, name: &str) -> ReplyBody {
    let request_id = named_clock_request(world, name);
    let reply = tokio::time::timeout(
        CLOCK_REPLY_TIMEOUT,
        clock_session(world).reply_to(request_id),
    )
    .await
    .unwrap_or_else(|_| panic!("request '{name}' was not answered in time"));
    reply.unwrap_or_else(|error| panic!("request '{name}' was not answered: {error}"))
}

fn last_clock_reply(world: &ScenarioWorld) -> &ReplyBody {
    world
        .last_clock_reply
        .as_ref()
        .expect("the clock session must have sent a request")
}

fn period(raw: &str) -> DomainClockPeriod {
    raw.parse().expect("scenario periods are valid")
}

fn skew(raw: &str) -> DomainClockSkew {
    raw.parse().expect("scenario skews are valid")
}

fn rate(raw: &str) -> DomainTimeRate {
    raw.parse().expect("scenario rates are positive and finite")
}

#[given(expr = "a clock session is opened on the {word} node")]
async fn given_clock_session_is_opened(world: &mut ScenarioWorld, role: String) {
    let leader = current_leader_node(world).await;
    let node = match role.as_str() {
        "leader" => leader,
        "follower" => world
            .cluster()
            .node_other_than(&leader)
            .expect("the cluster has a follower"),
        other => panic!("unknown node role '{other}'"),
    };
    let session = world
        .cluster()
        .open_session(&node, &world.domain)
        .await
        .unwrap_or_else(|error| panic!("a clock session could not open on '{node}': {error}"));
    world.clock_session = Some(session);
}

#[given(expr = "a clock session is opened on node {string}")]
async fn given_clock_session_is_opened_on_node(world: &mut ScenarioWorld, node: String) {
    let session = world
        .cluster()
        .open_session(&node, &world.domain)
        .await
        .unwrap_or_else(|error| panic!("a clock session could not open on '{node}': {error}"));
    world.clock_session = Some(session);
}

#[when(expr = "the clock session attaches to the clock of domain {string}")]
async fn when_clock_session_attaches(world: &mut ScenarioWorld, domain: String) {
    let domain = scenario_domain(world, &domain);
    let session = clock_session(world);
    let request_id = session
        .send_domain_clock_attach(domain)
        .await
        .unwrap_or_else(|error| panic!("the attach request was not sent: {error}"));
    let reply = session
        .reply_to(request_id)
        .await
        .unwrap_or_else(|error| panic!("the attach request was not answered: {error}"));
    world.last_clock_reply = Some(reply);
}

#[when(expr = "the clock session detaches from the clock of domain {string}")]
async fn when_clock_session_detaches(world: &mut ScenarioWorld, domain: String) {
    let domain = scenario_domain(world, &domain);
    let session = clock_session(world);
    let request_id = session
        .send_domain_clock_detach(domain)
        .await
        .unwrap_or_else(|error| panic!("the detach request was not sent: {error}"));
    let reply = session
        .reply_to(request_id)
        .await
        .unwrap_or_else(|error| panic!("the detach request was not answered: {error}"));
    world.last_clock_reply = Some(reply);
}

/// The clock an attach reply carries, after checking it attached the scenario domain.
fn attached_clock(world: &ScenarioWorld, reply: &ReplyBody) -> DomainClockObservation {
    let ReplyBody::DomainClockAttach(outcome) = reply else {
        panic!("the attach request was answered with {reply:?}");
    };
    let DomainClockAttachDisposition::Attached { domain, clock } = &outcome.disposition else {
        panic!("the session did not attach: {outcome:?}");
    };
    assert_eq!(domain.as_str(), world.domain, "another domain was attached");
    assert!(
        outcome.message.starts_with(&format!(
            "attached to the clock of domain '{domain}': {clock}"
        )),
        "the reply describes the clock it carries: {}",
        outcome.message
    );
    clock.clone()
}

#[then(
    expr = "the clock session is attached at generation {int} to a paced clock with period \
            {string}, skew {string}, logical origin {string} and time rate {string}"
)]
async fn then_clock_session_is_attached_to_paced_clock(
    world: &mut ScenarioWorld,
    generation: u64,
    expected_period: String,
    expected_skew: String,
    origin: String,
    time_rate: String,
) {
    let clock = attached_clock(world, last_clock_reply(world));
    assert_eq!(clock.generation, generation, "{clock:?}");
    let DomainClockObservedState::Paced(paced) = &clock.state else {
        panic!("the attached clock is not paced: {clock:?}");
    };
    assert_eq!(paced.period, period(&expected_period));
    assert_eq!(paced.skew, skew(&expected_skew));
    assert_eq!(
        paced.mapping.logical_start(),
        origin
            .parse::<Timestamp>()
            .expect("scenario origins are RFC 3339")
    );
    assert_eq!(paced.mapping.time_rate(), rate(&time_rate));
}

#[then(expr = "the clock session is attached at generation {int} to a stopped clock")]
async fn then_clock_session_is_attached_to_stopped_clock(
    world: &mut ScenarioWorld,
    generation: u64,
) {
    let clock = attached_clock(world, last_clock_reply(world));
    assert_eq!(
        clock,
        DomainClockObservation {
            generation,
            state: DomainClockObservedState::Stopped,
        }
    );
}

#[then(
    expr = "the clock session is refused because it already follows the clock of domain {string}"
)]
async fn then_clock_session_already_follows(world: &mut ScenarioWorld, domain: String) {
    let domain = scenario_domain(world, &domain);
    let reply = last_clock_reply(world);
    let ReplyBody::DomainClockAttach(outcome) = reply else {
        panic!("the attach request was answered with {reply:?}");
    };
    assert_eq!(
        outcome.disposition,
        DomainClockAttachDisposition::AlreadyAttached(domain),
        "{outcome:?}"
    );
}

#[then(expr = "the clock session is detached from the clock of domain {string}")]
async fn then_clock_session_is_detached(world: &mut ScenarioWorld, domain: String) {
    let domain = scenario_domain(world, &domain);
    let reply = last_clock_reply(world);
    let ReplyBody::DomainClockDetach(outcome) = reply else {
        panic!("the detach request was answered with {reply:?}");
    };
    assert_eq!(
        outcome.disposition,
        DomainClockDetachDisposition::Detached(domain),
        "{outcome:?}"
    );
}

#[then(
    expr = "the clock session is refused because it does not follow the clock of domain {string}"
)]
async fn then_clock_session_does_not_follow(world: &mut ScenarioWorld, domain: String) {
    let domain = scenario_domain(world, &domain);
    let reply = last_clock_reply(world);
    let ReplyBody::DomainClockDetach(outcome) = reply else {
        panic!("the detach request was answered with {reply:?}");
    };
    assert_eq!(
        outcome.disposition,
        DomainClockDetachDisposition::NotAttached(domain),
        "{outcome:?}"
    );
}

/// Takes the next clock frame, which must be about `domain`.
async fn next_clock_frame(
    world: &mut ScenarioWorld,
    duration: &str,
    domain: &DomainName,
) -> TestClockFrame {
    let duration = humantime::parse_duration(duration).expect("step durations are valid durations");
    let frame = clock_session(world)
        .try_next_clock_frame(duration)
        .await
        .unwrap_or_else(|error| panic!("failed while waiting for a clock frame: {error}"))
        .unwrap_or_else(|| panic!("no clock frame arrived within {duration:?}"));
    assert_eq!(
        frame.domain(),
        domain,
        "a frame about another domain: {frame:?}"
    );
    frame
}

/// Ignores ticks already queued before a lifecycle change and returns its state frame.
async fn next_observed_clock(
    world: &mut ScenarioWorld,
    duration: &str,
    domain: &DomainName,
) -> DomainClockObservation {
    loop {
        nervix_primitives::task::consume_budget().await;
        match next_clock_frame(world, duration, domain).await {
            TestClockFrame::Observed(observed) => return observed.clock,
            TestClockFrame::Ticked(_) => {}
            frame @ TestClockFrame::Ended(_) => {
                panic!("the attachment ended instead of observing a clock: {frame:?}")
            }
        }
    }
}

#[then(
    expr = "within {string} the clock session receives {int} increasing ticks for generation \
            {int} with period {string} and time rate {string}"
)]
async fn then_clock_session_receives_increasing_ticks(
    world: &mut ScenarioWorld,
    duration: String,
    count: usize,
    generation: u64,
    expected_period: String,
    expected_rate: String,
) {
    let domain = scenario_domain(world, &world.domain);
    let attached = attached_clock(world, last_clock_reply(world));
    let DomainClockObservedState::Paced(paced) = attached.state else {
        panic!("the attached clock must be paced");
    };
    let period = period(&expected_period);
    assert_eq!(paced.period, period);
    let minimum_wall_spacing = period.as_duration().div_f64(rate(&expected_rate).get());
    let mut previous: Option<nervix_models::DomainClockTickObservation> = None;
    for _ in 0..count {
        let ticked = match next_clock_frame(world, &duration, &domain).await {
            TestClockFrame::Ticked(ticked) => ticked,
            other => panic!("expected a paced tick, received {other:?}"),
        };
        let tick = ticked.tick;
        assert_eq!(tick.generation, generation);
        assert!(tick.tick_id > 0);
        let elapsed_periods = u32::try_from(tick.tick_id - 1)
            .expect("a scenario observes fewer than 2^32 paced ticks");
        let offset = period
            .as_duration()
            .checked_mul(elapsed_periods)
            .expect("the scenario tick offset fits in a duration");
        let expected = paced
            .mapping
            .logical_start()
            .checked_add(offset)
            .expect("the scenario origin leaves room for its ticks");
        assert_eq!(tick.logical_boundary, expected);
        assert!(tick.serving_logical >= tick.logical_boundary);
        if let Some(prior) = &previous {
            assert!(tick.tick_id > prior.tick_id);
            let spacing = tick
                .authority_utc
                .duration_since(prior.authority_utc)
                .expect("accepted ticks have increasing authority observations");
            assert!(
                spacing >= minimum_wall_spacing,
                "authority observations were {spacing:?} apart, below {minimum_wall_spacing:?}"
            );
        }
        previous = Some(tick);
    }
}

#[then(
    expr = "within {string} the clock session receives a tick for generation {int} after its \
            state frame"
)]
async fn then_tick_follows_state(world: &mut ScenarioWorld, duration: String, generation: u64) {
    let domain = scenario_domain(world, &world.domain);
    loop {
        nervix_primitives::task::consume_budget().await;
        let frame = next_clock_frame(world, &duration, &domain).await;
        match frame {
            TestClockFrame::Ticked(ticked) if ticked.tick.generation == generation => {
                let log = clock_session(world).clock_log();
                let tick_position = log.len() - 1;
                let state_position = log.iter().position(|entry| {
                    matches!(entry, TestClockLogEntry::Frame(TestClockFrame::Observed(observed))
                        if observed.clock.generation == generation)
                });
                assert!(state_position.is_some_and(|position| position < tick_position));
                return;
            }
            TestClockFrame::Ticked(_) => {}
            other => panic!("expected a generation {generation} tick, received {other:?}"),
        }
    }
}

#[then(
    expr = "within {string} the clock session observes domain {string} stopped at generation {int}"
)]
async fn then_clock_session_observes_stopped(
    world: &mut ScenarioWorld,
    duration: String,
    domain: String,
    generation: u64,
) {
    let domain = scenario_domain(world, &domain);
    let clock = next_observed_clock(world, &duration, &domain).await;
    assert_eq!(
        clock,
        DomainClockObservation {
            generation,
            state: DomainClockObservedState::Stopped,
        }
    );
}

#[then(
    expr = "within {string} the clock session receives a tick with an id higher than before owner \
            loss"
)]
async fn then_clock_tick_resumes_after_owner_loss(world: &mut ScenarioWorld, duration: String) {
    let domain = scenario_domain(world, &world.domain);
    let before_loss = clock_session(world)
        .clock_log()
        .iter()
        .filter_map(|entry| match entry {
            TestClockLogEntry::Frame(TestClockFrame::Ticked(ticked)) => Some(ticked.tick.tick_id),
            _ => None,
        })
        .max()
        .expect("the attachment read a tick before owner loss");
    let ticked = match next_clock_frame(world, &duration, &domain).await {
        TestClockFrame::Ticked(ticked) => ticked,
        other => panic!("expected progress after owner loss, received {other:?}"),
    };
    assert!(ticked.tick.tick_id > before_loss);
    let attached = attached_clock(world, last_clock_reply(world));
    let DomainClockObservedState::Paced(paced) = attached.state else {
        panic!("the attached clock must remain paced");
    };
    assert_eq!(ticked.tick.generation, attached.generation);
    let elapsed_periods = u32::try_from(ticked.tick.tick_id - 1)
        .expect("a scenario observes fewer than 2^32 paced ticks");
    let offset = paced
        .period
        .as_duration()
        .checked_mul(elapsed_periods)
        .expect("the tick offset fits in a duration");
    assert_eq!(
        ticked.tick.logical_boundary,
        paced
            .mapping
            .logical_start()
            .checked_add(offset)
            .expect("the original mapping leaves room for the resumed tick")
    );
}

#[when(expr = "the domain clock is started at now with time rate {string} on the leader node")]
async fn when_domain_clock_is_started_at_now(world: &mut ScenarioWorld, time_rate: String) {
    let leader = current_leader_node(world).await;
    let command = format!("START AT NOW TIME RATE {time_rate};");
    let before = Timestamp::now();
    world
        .cluster()
        .run_command(&leader, &world.domain, &command)
        .await
        .unwrap_or_else(|error| panic!("{command} failed on '{leader}': {error}"));
    let after = Timestamp::now();
    world.clock_start_window = Some(ClockStartWindow { before, after });
}

#[then(
    expr = "within {string} the clock session observes domain {string} at generation {int} as the \
            paced clock that start established with period {string} and skew {string}"
)]
async fn then_clock_session_observes_started_clock(
    world: &mut ScenarioWorld,
    duration: String,
    domain: String,
    generation: u64,
    expected_period: String,
    expected_skew: String,
) {
    let domain = scenario_domain(world, &domain);
    let window = world
        .clock_start_window
        .expect("the domain clock must have been started at now");
    let clock = next_observed_clock(world, &duration, &domain).await;
    assert_eq!(clock.generation, generation, "{clock:?}");
    let DomainClockObservedState::Paced(paced) = &clock.state else {
        panic!("the started clock is not paced: {clock:?}");
    };
    assert_eq!(paced.period, period(&expected_period));
    assert_eq!(paced.skew, skew(&expected_skew));
    let anchor = paced.mapping.wall_started_at();
    assert!(
        window.before <= anchor && anchor <= window.after,
        "START AT NOW anchors the mapping at the UTC it ran at: {anchor} outside {window:?}"
    );
    assert_eq!(
        paced.mapping.logical_start(),
        anchor,
        "START AT NOW starts logical time at its UTC anchor"
    );
    assert_eq!(paced.mapping.time_rate(), DomainTimeRate::ONE);
}

#[then(expr = "the clock session receives no frame about domain {string} within {string}")]
async fn then_clock_session_receives_no_frame(
    world: &mut ScenarioWorld,
    domain: String,
    duration: String,
) {
    let domain = scenario_domain(world, &domain);
    let duration =
        humantime::parse_duration(&duration).expect("step durations are valid durations");
    let session = clock_session(world);
    let reply = session
        .clock_log()
        .iter()
        .rposition(|entry| matches!(entry, TestClockLogEntry::Reply(_)))
        .assured("the detach reply was read before checking its frames");
    assert!(
        session.clock_log()[reply + 1..].iter().all(|entry| {
            !matches!(entry, TestClockLogEntry::Frame(frame) if frame.domain() == &domain)
        }),
        "a frame about the detached clock followed its reply on the wire"
    );
    // Waiting for the reply filed any earlier ticks in the unread queue. Only frames read after
    // the reply are relevant to this assertion, so begin the timed wait with that queue drained.
    session.discard_queued_clock_frames_for(&domain);
    let deadline = Instant::now() + duration;
    loop {
        nervix_primitives::task::consume_budget().await;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let frame = clock_session(world)
            .try_next_clock_frame(remaining)
            .await
            .unwrap_or_else(|error| panic!("failed while reading clock frames: {error}"));
        let Some(frame) = frame else {
            return;
        };
        assert_ne!(
            frame.domain(),
            &domain,
            "a frame about a detached clock arrived: {frame:?}"
        );
    }
}

#[when(
    expr = "the clock session sends request {string} attaching the clock of domain {string} and \
            then request {string} with this NSPL command"
)]
async fn when_clock_session_attaches_and_commands(
    world: &mut ScenarioWorld,
    attach: String,
    domain: String,
    command: String,
    #[step] step: &Step,
) {
    let domain = scenario_domain(world, &domain);
    let query = expand_placeholders(world, docstring(step));
    let session = clock_session(world);
    let attach_id = session
        .send_domain_clock_attach(domain)
        .await
        .unwrap_or_else(|error| panic!("failed to send request '{attach}': {error}"));
    let reference = crate::common::raw_session::fresh_execution_reference();
    let command_id = session
        .send_command_request_with_reference(&query, &reference)
        .await
        .unwrap_or_else(|error| panic!("failed to send request '{command}': {error}"));
    world.clock_session_requests.insert(attach, attach_id);
    world.clock_session_requests.insert(command, command_id);
}

#[when(
    expr = "the clock session sends request {string} detaching the clock of domain {string} and \
            then request {string} with this NSPL command"
)]
async fn when_clock_session_detaches_and_commands(
    world: &mut ScenarioWorld,
    detach: String,
    domain: String,
    command: String,
    #[step] step: &Step,
) {
    let domain = scenario_domain(world, &domain);
    let query = expand_placeholders(world, docstring(step));
    let session = clock_session(world);
    let detach_id = session
        .send_domain_clock_detach(domain)
        .await
        .unwrap_or_else(|error| panic!("failed to send request '{detach}': {error}"));
    let reference = crate::common::raw_session::fresh_execution_reference();
    let command_id = session
        .send_command_request_with_reference(&query, &reference)
        .await
        .unwrap_or_else(|error| panic!("failed to send request '{command}': {error}"));
    world.clock_session_requests.insert(detach, detach_id);
    world.clock_session_requests.insert(command, command_id);
}

#[when(expr = "the clock session sends request {string} attaching the clock of domain {string}")]
async fn when_clock_session_sends_attach(world: &mut ScenarioWorld, name: String, domain: String) {
    let domain = scenario_domain(world, &domain);
    let request_id = clock_session(world)
        .send_domain_clock_attach(domain)
        .await
        .unwrap_or_else(|error| panic!("failed to send request '{name}': {error}"));
    world.clock_session_requests.insert(name, request_id);
}

#[when(expr = "the clock session sends request {string} detaching the clock of domain {string}")]
async fn when_clock_session_sends_detach(world: &mut ScenarioWorld, name: String, domain: String) {
    let domain = scenario_domain(world, &domain);
    let request_id = clock_session(world)
        .send_domain_clock_detach(domain)
        .await
        .unwrap_or_else(|error| panic!("failed to send request '{name}': {error}"));
    world.clock_session_requests.insert(name, request_id);
}

#[when("the clock session executes this NSPL command")]
async fn when_clock_session_executes(world: &mut ScenarioWorld, #[step] step: &Step) {
    let query = expand_placeholders(world, docstring(step));
    let outcome = clock_session(world)
        .run_command_result(&query)
        .await
        .unwrap_or_else(|error| panic!("{query} was not answered: {error}"));
    assert!(
        crate::common::raw_session::outcome_succeeded(&outcome),
        "{query} must succeed: {outcome:?}"
    );
}

#[then(
    expr = "request {string} of the clock session attached the clock of domain {string} as \
            unpaced at generation {int}"
)]
async fn then_named_request_attached_unpaced(
    world: &mut ScenarioWorld,
    name: String,
    domain: String,
    generation: u64,
) {
    let domain = scenario_domain(world, &domain);
    let reply = clock_reply_to_named(world, &name).await;
    let ReplyBody::DomainClockAttach(outcome) = reply else {
        panic!("request '{name}' was answered with {reply:?}");
    };
    assert_eq!(
        outcome.disposition,
        DomainClockAttachDisposition::Attached {
            domain,
            clock: DomainClockObservation {
                generation,
                state: DomainClockObservedState::Unpaced,
            },
        },
        "{outcome:?}"
    );
}

#[then(expr = "request {string} of the clock session detached the clock of domain {string}")]
async fn then_named_request_detached(world: &mut ScenarioWorld, name: String, domain: String) {
    let domain = scenario_domain(world, &domain);
    let reply = clock_reply_to_named(world, &name).await;
    let ReplyBody::DomainClockDetach(outcome) = reply else {
        panic!("request '{name}' was answered with {reply:?}");
    };
    assert_eq!(
        outcome.disposition,
        DomainClockDetachDisposition::Detached(domain),
        "{outcome:?}"
    );
}

#[then(expr = "request {string} of the clock session completed")]
async fn then_named_request_completed(world: &mut ScenarioWorld, name: String) {
    let reply = clock_reply_to_named(world, &name).await;
    let ReplyBody::Command(outcome) = reply else {
        panic!("request '{name}' was answered with {reply:?}");
    };
    assert!(
        crate::common::raw_session::outcome_succeeded(&outcome),
        "request '{name}' must succeed: {outcome:?}"
    );
}

#[then(
    expr = "request {string} of the clock session is refused because the session holds a \
            transaction"
)]
async fn then_named_request_refused_in_transaction(world: &mut ScenarioWorld, name: String) {
    let reply = clock_reply_to_named(world, &name).await;
    let (failed, message) = match &reply {
        ReplyBody::DomainClockAttach(outcome) => (
            outcome.disposition == DomainClockAttachDisposition::Failed,
            &outcome.message,
        ),
        ReplyBody::DomainClockDetach(outcome) => (
            outcome.disposition == DomainClockDetachDisposition::Failed,
            &outcome.message,
        ),
        other => panic!("request '{name}' was answered with {other:?}"),
    };
    assert!(failed, "request '{name}' must be refused: {reply:?}");
    assert_eq!(
        message,
        "session-scoped and client-local statements cannot be queued in a transaction"
    );
}

/// Where the reply to request `name` sits in the clock session's log.
fn reply_position(world: &ScenarioWorld, name: &str) -> usize {
    let request_id = named_clock_request(world, name);
    let session = world
        .clock_session
        .as_ref()
        .expect("a clock session must be opened first");
    session
        .clock_log()
        .iter()
        .position(|entry| matches!(entry, TestClockLogEntry::Reply(id) if *id == request_id))
        .unwrap_or_else(|| panic!("the reply to request '{name}' was never read"))
}

/// The positions of every frame about `domain` in the clock session's log.
fn frame_positions(world: &ScenarioWorld, domain: &DomainName) -> Vec<usize> {
    let session = world
        .clock_session
        .as_ref()
        .expect("a clock session must be opened first");
    let mut positions = Vec::new();
    for (position, entry) in session.clock_log().iter().enumerate() {
        if let TestClockLogEntry::Frame(frame) = entry
            && frame.domain() == domain
        {
            positions.push(position);
        }
    }
    positions
}

#[then(
    expr = "the clock session received the reply to request {string} before every frame about \
            domain {string}"
)]
async fn then_reply_precedes_frames(world: &mut ScenarioWorld, name: String, domain: String) {
    let domain = scenario_domain(world, &domain);
    let reply = reply_position(world, &name);
    let frames = frame_positions(world, &domain);
    assert!(!frames.is_empty(), "no frame about the domain was read");
    assert!(
        frames.iter().all(|frame| *frame > reply),
        "a frame about the domain preceded the attach reply: reply at {reply}, frames at \
         {frames:?}"
    );
}

#[then(
    expr = "the clock session received no frame about domain {string} after the reply to request \
            {string}"
)]
async fn then_no_frame_follows_reply(world: &mut ScenarioWorld, domain: String, name: String) {
    let domain = scenario_domain(world, &domain);
    let reply = reply_position(world, &name);
    let frames = frame_positions(world, &domain);
    assert!(
        frames.iter().all(|frame| *frame < reply),
        "a frame about the domain followed the detach reply: reply at {reply}, frames at \
         {frames:?}"
    );
}

/// The attached clock of the scenario domain as client `name` last observed it.
fn client_attached_clock(
    world: &ScenarioWorld,
    name: &str,
) -> nervix_client_core::AttachedDomainClock {
    let client = world
        .transaction_clients
        .get(name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"));
    let domain = DomainName::parse(&world.domain).expect("scenario domains are valid names");
    client
        .domain_clock(&domain)
        .unwrap_or_else(|| panic!("client '{name}' does not follow the clock of '{domain}'"))
}

/// The admission window client `name` computes from its attached clock now.
fn client_admission_window(world: &ScenarioWorld, name: &str) -> DomainAdmissionWindow {
    let attached = client_attached_clock(world, name);
    attached
        .admission_window(Timestamp::now())
        .unwrap_or_else(|error| panic!("client '{name}' cannot read its attached clock: {error}"))
        .unwrap_or_else(|| panic!("client '{name}' follows a clock without an admission window"))
}

#[then(expr = "within {string} client {string} receives a tick for its attached domain clock")]
async fn then_client_receives_tick(world: &mut ScenarioWorld, duration: String, name: String) {
    let duration = humantime::parse_duration(&duration).expect("step durations are valid");
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    let domain = scenario_domain(world, &world.domain);
    let ticked = tokio::time::timeout(duration, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let event = client
                .next_domain_clock_event()
                .await
                .expect("the client reads its clock events");
            match event {
                nervix_client_core::DomainClockEvent::Ticked(ticked) if ticked.domain == domain => {
                    break ticked;
                }
                nervix_client_core::DomainClockEvent::Observed(_) => {}
                other => panic!("the client did not receive a tick: {other:?}"),
            }
        }
    })
    .await
    .expect("the attached clock delivers a tick within the scenario deadline");
    let attached = client_attached_clock(world, &name);
    let latest = attached.latest_tick().expect("the helper retains the tick");
    assert!(latest.tick_id >= ticked.tick.tick_id);
    assert_eq!(attached.frontier(), Some(latest.logical_boundary));
}

#[then(
    expr = "within {string} client {string} reports an interruption of its attached domain clock"
)]
async fn then_client_reports_clock_interruption(
    world: &mut ScenarioWorld,
    duration: String,
    name: String,
) {
    let duration = humantime::parse_duration(&duration).expect("step durations are valid");
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    let domain = scenario_domain(world, &world.domain);
    tokio::time::timeout(duration, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            match client
                .next_domain_clock_event()
                .await
                .expect("the client reads clock events")
            {
                nervix_client_core::DomainClockEvent::Interrupted(interrupted)
                    if interrupted.domain == domain =>
                {
                    break;
                }
                nervix_client_core::DomainClockEvent::Observed(_)
                | nervix_client_core::DomainClockEvent::Ticked(_) => {}
                other => panic!("unexpected clock event while waiting for interruption: {other:?}"),
            }
        }
    })
    .await
    .expect("the clock reports its session gap within the scenario deadline");
}

async fn post_timestamped_event(
    world: &mut ScenarioWorld,
    host: &str,
    path: &str,
    sequence: i64,
    occurred_at: Timestamp,
) {
    let host = expand_placeholders(world, host);
    let path = expand_placeholders(world, path);
    let payload = serde_json::json!({
        "sequence": sequence,
        "occurred_at": occurred_at.to_rfc3339(),
    })
    .to_string();
    append_cucumber_log_line(&format!(
        "http publish: node=node-1 host={host} path={path} payload={payload}"
    ));
    world
        .cluster()
        .publish_http("node-1", &host, &path, &payload)
        .await
        .unwrap_or_else(|error| panic!("event {sequence} was not accepted: {error}"));
}

#[when(
    expr = "client {string} posts event {int} to host {string} path {string} at the newest tick \
            center its attached clock admits"
)]
async fn when_client_posts_event_at_newest_center(
    world: &mut ScenarioWorld,
    name: String,
    sequence: i64,
    host: String,
    path: String,
) {
    let window = client_admission_window(world, &name);
    let occurred_at = window.latest_center();
    assert!(window.contains(occurred_at));
    post_timestamped_event(world, &host, &path, sequence, occurred_at).await;
}

#[when(
    expr = "client {string} posts event {int} to host {string} path {string} one nanosecond past \
            the skew after its attached clock's frontier"
)]
async fn when_client_posts_event_past_skew(
    world: &mut ScenarioWorld,
    name: String,
    sequence: i64,
    host: String,
    path: String,
) {
    let window = client_admission_window(world, &name);
    let past_skew = window
        .skew()
        .as_duration()
        .checked_add(Duration::from_nanos(1))
        .expect("a scenario skew leaves room for one more nanosecond");
    let occurred_at = window
        .latest_center()
        .checked_add(past_skew)
        .expect("the scenario frontier is far from the timestamp maximum");
    assert!(!window.contains(occurred_at));
    post_timestamped_event(world, &host, &path, sequence, occurred_at).await;
}

#[then(expr = "within {string} client {string} observes a server error containing")]
async fn then_client_observes_server_error(
    world: &mut ScenarioWorld,
    duration: String,
    name: String,
    #[step] step: &Step,
) {
    let expected = expand_placeholders(world, docstring(step));
    let duration =
        humantime::parse_duration(&duration).expect("step durations are valid durations");
    let client = world
        .transaction_clients
        .get(&name)
        .unwrap_or_else(|| panic!("client '{name}' must be connected"))
        .clone();
    let deadline = Instant::now() + duration;
    let mut seen = Vec::new();
    loop {
        nervix_primitives::task::consume_budget().await;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let event = tokio::time::timeout(remaining, client.next_server_event()).await;
        let event = match event {
            Ok(event) => {
                event.unwrap_or_else(|error| panic!("client '{name}' lost its events: {error}"))
            }
            Err(_) => panic!(
                "client '{name}' observed no server error containing {expected:?} within \
                 {duration:?}; it observed {seen:?}"
            ),
        };
        if event.level == nervix_client_core::NoticeLevel::Error
            && event.message.contains(expected.trim())
        {
            return;
        }
        seen.push(event.message);
    }
}
