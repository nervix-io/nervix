//! Public ClickHouse connection-policy evidence against the real test server.
//! Included by the scenario harness.
//!
//! Layer: test harness.
//! - **Owns.** Provisioning named databases and observing the server's idle connection limit.
//! - **Depends on.** Public ClickHouse SQL, scenario dependencies and the physical test clock.
//! - **Must not know.** The emitter's HTTP pool or its internal connection state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns public ClickHouse idle-connection evidence")
)]

use super::*;

#[given(expr = "ClickHouse database {string} exists")]
async fn given_clickhouse_database_exists(world: &mut ScenarioWorld, database: String) {
    let database = expand_placeholders(world, &database);
    assert!(
        !database.is_empty()
            && database
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        "the scenario database is one alphanumeric identifier"
    );
    clickhouse_post(
        world.dependencies.endpoints(),
        &format!("CREATE DATABASE IF NOT EXISTS `{database}`"),
    )
    .await
    .assured("the scenario explicitly provisions the ClickHouse database");
}

#[when("the ClickHouse server's idle connection timeout has elapsed")]
async fn when_clickhouse_idle_timeout_has_elapsed(world: &mut ScenarioWorld) {
    let seconds = clickhouse_post(
        world.dependencies.endpoints(),
        "SELECT value FROM system.server_settings WHERE name = 'keep_alive_timeout' FORMAT \
         TabSeparated",
    )
    .await
    .assured("the real ClickHouse server exposes its keep-alive setting")
    .trim()
    .parse::<u64>()
    .assured("keep_alive_timeout is a whole number of seconds");
    assert!(seconds > 0, "the server must retain idle HTTP connections");
    let interval = Duration::from_secs(seconds)
        .checked_add(Duration::from_secs(1))
        .assured("the server's test setting fits a physical duration");
    // This delay is the event under test: the next insert must start after the actual server's
    // idle limit. Scheduling delay only increases that interval; no silence assertion races it.
    nervix_primitives::time::sleep(interval).await;
}
