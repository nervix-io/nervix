//! Steps that observe the WebSocket connections a web console page makes, and that make the page
//! lose its session connection.
//!
//! Layer: test harness.
//! - **Owns.** Starting a console page with the connection recorder in `recorder.js`, reading the
//!   page's record, closing the session connection the page holds, and the steps that check the
//!   record against the consoles of the scenario's nodes and the leaders the nodes reported on it.
//! - **Depends on.** The scenario's browser page and cluster, and the console connection record
//!   and its checks.
//! - **Must not know.** How the console drives its session, or how a node serves it.

use cucumber::{then, when};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::websocket::CONSOLE_SESSION_PATH;

use super::{ScenarioWorld, expand_placeholders};
use crate::common::console_connections::{
    ConsoleAddress, ConsoleConnectionRecord, ReportedLeaders,
};

/// What a console page runs before any script of its own.
const RECORDER: &str = include_str!("recorder.js");

/// Reads the page's record.
const READ_RECORD: &str = "() => window.__nervixConsoleConnections.record()";

/// Closes the newest connection the page holds at the path it is given.
const LOSE_CONNECTION: &str = "(path) => window.__nervixConsoleConnections.lose(path)";

/// Starts `page` with the connection recorder, so its record holds every connection the console
/// makes, from its first.
pub(super) async fn record_console_connections(
    page: &playwright_rs::Page,
) -> playwright_rs::Result<()> {
    page.add_init_script(RECORDER).await
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
