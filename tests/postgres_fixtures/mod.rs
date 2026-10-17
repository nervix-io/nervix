//! Outside the layer order: regression scenarios for the Postgres observation fixture.
//!
//! - **Owns.** Concurrent verification-client demand, controlled lock holders, and complete query
//!   result assertions.
//! - **Depends on.** The scenario's Postgres client fixture and the SQLx driver.
//! - **Must not know.** Production connection-pool ownership or emitter retry policy.

use super::*;

pub(super) struct Observations {
    expected_count: usize,
    expected_tls: bool,
    queries: Vec<ObservedQuery>,
}

struct ObservedQuery {
    observer: usize,
    round: usize,
    text: String,
    bytes: Vec<u8>,
    number: i64,
    encrypted: bool,
}

struct ObserverOutput {
    clients: Vec<SqlxPgPool>,
    queries: Vec<ObservedQuery>,
}

pub(super) struct HeldLocks {
    fixtures: Vec<HeldLock>,
}

struct HeldLock {
    release: nervix_primitives::sync::oneshot::Sender<()>,
    table: String,
}

fn transport_uses_tls(transport: &str) -> bool {
    match transport {
        "plaintext" => false,
        "TLS" => true,
        other => panic!("unsupported Postgres fixture transport {other}"),
    }
}

#[given(expr = "two other Postgres fixtures hold table locks over {word}")]
async fn two_fixtures_hold_locks(world: &mut ScenarioWorld, transport: String) {
    let tls = transport_uses_tls(&transport);
    let client = world
        .dependencies
        .postgres_client(tls)
        .expect("Postgres is running before lock setup");
    let mut fixtures = Vec::new();
    for index in 0..2 {
        let table = format!("fixture_lock_{}_{index}", world.test_id);
        sqlx::raw_sql(SqlxAssertSqlSafe(format!(
            "CREATE TABLE {table} (id BIGINT)"
        )))
        .execute(&client)
        .await
        .expect("the controlled table must exist");
        let mut fixture = ScenarioWorld {
            dependencies: world.dependencies.clone(),
            postgres_table: Some(table.clone()),
            postgres_tls: tls,
            ..ScenarioWorld::default()
        };
        // Exercise the same lock owner used by client-pool contention scenarios.
        given_postgres_table_is_locked(&mut fixture).await;
        let release = fixture
            .postgres_lock_release
            .take()
            .expect("the real fixture lock owner installed its release handle");
        fixtures.push(HeldLock { release, table });
    }
    world.postgres_fixture_locks = Some(HeldLocks { fixtures });
}

#[when(expr = "another Postgres fixture observer queries over {word}")]
async fn observer_queries_with_held_locks(world: &mut ScenarioWorld, transport: String) {
    let tls = transport_uses_tls(&transport);
    let held = world
        .postgres_fixture_locks
        .take()
        .expect("both controlled lock holders are ready");
    let client = world
        .dependencies
        .postgres_client(tls)
        .expect("Postgres is running before observation");
    let marker = "observer-0-round-0";
    let observed = nervix_primitives::time::timeout(
        Duration::from_secs(30),
        sqlx::query(
            "SELECT $1::text, $2::bytea, $3::bigint, ssl FROM pg_stat_ssl WHERE pid = \
             pg_backend_pid()",
        )
        .bind(marker)
        .bind(marker.as_bytes())
        .bind(0_i64)
        .fetch_one(&client),
    )
    .await;
    let mut tables = Vec::new();
    for fixture in held.fixtures {
        fixture
            .release
            .send(())
            .expect("the controlled lock owner is still alive");
        tables.push(fixture.table);
    }
    for table in tables {
        sqlx::raw_sql(SqlxAssertSqlSafe(format!("DROP TABLE {table}")))
            .execute(&client)
            .await
            .expect("released fixture tables must be removed");
    }
    let row = observed
        .expect("held fixture locks must leave observation capacity")
        .expect("the observer query must succeed while both locks are held");
    world.postgres_fixture_observations = Some(Observations {
        expected_count: 1,
        expected_tls: tls,
        queries: vec![ObservedQuery {
            observer: 0,
            round: 0,
            text: row.get(0),
            bytes: row.get(1),
            number: row.get(2),
            encrypted: row.get(3),
        }],
    });
}

#[when(expr = "{int} concurrent Postgres fixture observers retain {int} clients over {word}")]
async fn concurrent_observers(
    world: &mut ScenarioWorld,
    observers: usize,
    rounds: usize,
    transport: String,
) {
    let tls = transport_uses_tls(&transport);
    let dependencies = &world.dependencies;
    let mut pending = Vec::with_capacity(observers);
    for observer in 0..observers {
        pending.push(async move {
            let mut clients = Vec::new();
            let mut queries = Vec::new();
            for round in 0..rounds {
                nervix_primitives::task::consume_budget().await;
                let client = dependencies.postgres_client(tls)?;
                let marker = format!("observer-{observer}-round-{round}");
                let row = sqlx::query(
                    "SELECT $1::text, $2::bytea, $3::bigint, ssl FROM pg_stat_ssl WHERE pid = \
                     pg_backend_pid()",
                )
                .bind(&marker)
                .bind(marker.as_bytes())
                .bind(i64::try_from(observer).assured("the scenario declares few observers"))
                .fetch_one(&client)
                .await
                .map_err(std::io::Error::other)?;
                queries.push(ObservedQuery {
                    observer,
                    round,
                    text: row.get(0),
                    bytes: row.get(1),
                    number: row.get(2),
                    encrypted: row.get(3),
                });
                clients.push(client);
            }
            Ok::<_, std::io::Error>(ObserverOutput { clients, queries })
        });
    }
    let results = join_all(pending).await;
    let mut queries = Vec::new();
    for (observer, result) in results.into_iter().enumerate() {
        let output = result.unwrap_or_else(|error| {
            panic!("Postgres fixture observer {observer} failed over {transport}: {error}")
        });
        assert_eq!(output.clients.len(), rounds);
        queries.extend(output.queries);
    }
    world.postgres_fixture_observations = Some(Observations {
        expected_count: observers
            .checked_mul(rounds)
            .assured("the scenario declares few observations"),
        expected_tls: tls,
        queries,
    });
}

#[then("every Postgres fixture observer received its complete query results")]
async fn complete_observations(world: &mut ScenarioWorld) {
    let observations = world
        .postgres_fixture_observations
        .take()
        .assured("the preceding step recorded every observer's results");
    assert_eq!(observations.queries.len(), observations.expected_count);
    for observed in observations.queries {
        let marker = format!("observer-{}-round-{}", observed.observer, observed.round);
        assert_eq!(observed.text, marker);
        assert_eq!(observed.bytes, marker.as_bytes());
        assert_eq!(observed.encrypted, observations.expected_tls);
        assert_eq!(
            observed.number,
            i64::try_from(observed.observer).assured("the scenario declares few observers")
        );
    }
}
