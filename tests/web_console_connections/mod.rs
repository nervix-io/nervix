//! Steps that observe the WebSocket connections a web console page makes, and that make the page
//! lose its session connection.
//!
//! Layer: test harness.
//! - **Owns.** Starting a console page with its connection recorder and optional bounded initial
//!   frame gate, reading the page's record, releasing the gate, closing the session connection the
//!   page holds, and checking the record against the scenario's consoles and reported leaders.
//! - **Depends on.** The scenario's browser page and cluster, and the console connection record
//!   and its checks.
//! - **Must not know.** How the console drives its session, or how a node serves it.

use cucumber::{then, when};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::websocket::CONSOLE_SESSION_PATH;

use super::{
    ScenarioWorld, append_cucumber_log_line, assert_graph_probe, current_leader_node,
    expand_placeholders, open_recorded_web_console_page,
};
use crate::common::console_connections::{
    ConsoleAddress, ConsoleConnectionRecord, ReportedLeaders,
};

/// What a console page runs before any script of its own.
const RECORDER: &str = include_str!("recorder.js");

/// Whether the initial session frames reach the page immediately or at the scenario's release.
#[derive(Clone, Copy)]
pub(super) enum InitialSession {
    Deliver,
    Hold,
}

/// A bounded gate installed after the recorder and before the console's own scripts.
const HOLD_INITIAL_SESSION: &str = include_str!("hold_initial_session.js");

/// Reads the page's record.
const READ_RECORD: &str = "() => window.__nervixConsoleConnections.record()";

/// Closes the newest connection the page holds at the path it is given.
const LOSE_CONNECTION: &str = "(path) => window.__nervixConsoleConnections.lose(path)";

/// Starts `page` with the connection recorder, so its record holds every connection the console
/// makes, from its first.
pub(super) async fn record_console_connections(
    page: &playwright_rs::Page,
    initial_session: InitialSession,
) -> playwright_rs::Result<()> {
    let script = match initial_session {
        InitialSession::Deliver => RECORDER.to_string(),
        InitialSession::Hold => {
            let path = serde_json::to_string(CONSOLE_SESSION_PATH)
                .assured("the public session path serializes as JavaScript text");
            let gate = HOLD_INITIAL_SESSION.replace("__NERVIX_CONSOLE_SESSION_PATH__", &path);
            format!("{RECORDER}\n{gate}")
        }
    };
    page.add_init_script(&script).await
}

#[when("the web console is opened on the leader node with its initial session frames held")]
async fn when_web_console_opens_with_held_initial_session(world: &mut ScenarioWorld) {
    super::close_browser(world).await;
    let leader = current_leader_node(world).await;
    let url = world
        .cluster()
        .web_console_url(&leader)
        .assured("the started leader serves its console");
    open_recorded_web_console_page(world, &url, InitialSession::Hold)
        .await
        .assured("the console starts with its initial session gate");
    assert_graph_probe(
        browser_page(world),
        "() => window.__nervixInitialSession.pending() > 0 ? 'OK' : 'no session frame has reached \
         the gate'",
        "the initial session frames to reach the browser gate",
    )
    .await;
}

#[when("the held initial web console session frames are released")]
async fn when_initial_web_console_session_is_released(world: &mut ScenarioWorld) {
    let released = browser_page(world)
        .evaluate::<(), usize>("() => window.__nervixInitialSession.release()", None::<&()>)
        .await
        .assured("the bounded initial session gate releases every frame in receive order");
    assert!(released > 0, "the gate must hold an actual session frame");
    append_cucumber_log_line(&format!(
        "initial console session: released {released} frames for domain={}",
        world.domain
    ));
}

fn browser_page(world: &ScenarioWorld) -> &playwright_rs::Page {
    world
        .browser_page
        .as_ref()
        .assured("the scenario opened the console before observing its connections")
}

async fn read_record(page: &playwright_rs::Page) -> ConsoleConnectionRecord {
    page.evaluate::<(), ConsoleConnectionRecord>(READ_RECORD, None::<&()>)
        .await
        .assured("every console page starts with the connection recorder")
}

/// The leaders the nodes reported on the page's connections.
fn reported_leaders(record: &ConsoleConnectionRecord) -> ReportedLeaders {
    match record.reported_leaders() {
        Ok(leaders) => leaders,
        Err(error) => panic!("{error:#}; the web console's connections:{record}"),
    }
}

/// The console `node_id` serves, as the connections the page opens to it name it.
fn console_of(world: &ScenarioWorld, node_id: &str) -> ConsoleAddress {
    let node_id = expand_placeholders(world, node_id);
    let url = match world.cluster().web_console_url(&node_id) {
        Ok(url) => url,
        Err(error) => {
            panic!("node '{node_id}' serves no console of the scenario's cluster: {error}")
        }
    };
    ConsoleAddress::try_from(url).assured("the harness builds every node's console URL")
}

#[when("the web console loses its session connection")]
async fn when_web_console_loses_its_session_connection(world: &mut ScenarioWorld) {
    let page = browser_page(world);
    let lost = page
        .evaluate::<&str, bool>(LOSE_CONNECTION, Some(&CONSOLE_SESSION_PATH))
        .await
        .assured("every console page starts with the connection recorder");
    if !lost {
        let record = read_record(page).await;
        panic!("the web console held no session connection to lose; its connections:{record}");
    }
}

#[then("the web console's session reconnected where it lost its connection")]
async fn then_web_console_session_reconnected_where_it_was_lost(world: &mut ScenarioWorld) {
    let record = read_record(browser_page(world)).await;
    if let Err(violation) = record.session_reconnected_where_it_was_lost() {
        panic!("{violation}; the web console's connections:{record}");
    }
}

#[then("the web console never held two session connections at once")]
async fn then_web_console_held_one_session_connection_at_a_time(world: &mut ScenarioWorld) {
    let record = read_record(browser_page(world)).await;
    if let Err(violation) = record.one_session_connection_at_a_time() {
        panic!("{violation}; the web console's connections:{record}");
    }
}

#[then(
    expr = "the web console's session reached node {string} until it was redirected to node \
            {string}, and then only nodes reported to it as the leader"
)]
async fn then_web_console_session_followed_reported_leaders(
    world: &mut ScenarioWorld,
    origin: String,
    leader: String,
) {
    let origin = console_of(world, &origin);
    let leader = console_of(world, &leader);
    let record = read_record(browser_page(world)).await;
    let leaders = reported_leaders(&record);
    if let Err(violation) = record.session_followed_reported_leaders(&leaders, &origin, &leader) {
        panic!(
            "{violation}; the web console's connections:{record}\nthe leaders reported to \
             it:{leaders}"
        );
    }
}

#[then(
    "the web console's backup downloads and restores reached only nodes reported to it as the \
     leader"
)]
async fn then_web_console_backup_streams_reached_reported_leaders(world: &mut ScenarioWorld) {
    let record = read_record(browser_page(world)).await;
    let leaders = reported_leaders(&record);
    if let Err(violation) = record.backup_streams_reached_reported_leaders(&leaders) {
        panic!(
            "{violation}; the web console's connections:{record}\nthe leaders reported to \
             it:{leaders}"
        );
    }
}
