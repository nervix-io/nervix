//! Outside the layer order: regression scenarios for the Postgres observation fixture.
//!
//! - **Owns.** Concurrent verification-client demand, controlled lock holders, complete query
//!   result assertions, and the checks that held locks leave the observers' connections in place
//!   and that a new connection is established in a few polls.
//! - **Depends on.** The scenario's Postgres client fixture and the SQLx driver.
//! - **Must not know.** Production connection-pool ownership or emitter retry policy.

use super::*;

pub(super) struct Observations {
    expected_count: usize,
    expected_tls: bool,
    queries: Vec<ObservedQuery>,
    /// For each lock holder found on the locked tables while the observer queried, whether the
    /// observer's connection was established before that holder's. Only an observer beside held
    /// locks records it.
    observer_predates_lock_holders: Option<Vec<bool>>,
}

/// How many times establishing one connection was polled before it completed.
pub(super) struct ConnectionEstablishment {
    polls: u32,
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
    let mut tables = Vec::new();
    for fixture in &held.fixtures {
        tables.push(fixture.table.clone());
    }
    let observed = nervix_primitives::time::timeout(Duration::from_secs(30), async {
        let mut connection = client.acquire().await?;
        let row = sqlx::query(
            "SELECT $1::text, $2::bytea, $3::bigint, ssl FROM pg_stat_ssl WHERE pid = \
             pg_backend_pid()",
        )
        .bind(marker)
        .bind(marker.as_bytes())
        .bind(0_i64)
        .fetch_one(&mut *connection)
        .await?;
        // The holders are found by the tables they lock, which only this scenario names.
        let holders = sqlx::query(
            "SELECT observer.backend_start < holder.backend_start FROM pg_stat_activity AS \
             observer CROSS JOIN pg_locks AS lock JOIN pg_class AS class ON class.oid = \
             lock.relation JOIN pg_stat_activity AS holder ON holder.pid = lock.pid WHERE \
             observer.pid = pg_backend_pid() AND class.relname::text = ANY($1) AND lock.granted \
             AND lock.mode = 'ExclusiveLock'",
        )
        .bind(&tables)
        .fetch_all(&mut *connection)
        .await?;
        Ok::<_, sqlx::Error>(ObservedBesideLocks { row, holders })
    })
    .await;
    for fixture in held.fixtures {
        fixture
            .release
            .send(())
            .expect("the controlled lock owner is still alive");
    }
    for table in tables {
        sqlx::raw_sql(SqlxAssertSqlSafe(format!("DROP TABLE {table}")))
            .execute(&client)
            .await
            .expect("released fixture tables must be removed");
    }
    let observed = observed
        .expect("held fixture locks must leave observation capacity")
        .expect("the observer query must succeed while both locks are held");
    let mut observer_predates_lock_holders = Vec::new();
    for holder in &observed.holders {
        observer_predates_lock_holders.push(holder.get(0));
    }
    world.postgres_fixture_observations = Some(Observations {
        expected_count: 1,
        expected_tls: tls,
        queries: vec![ObservedQuery {
            observer: 0,
            round: 0,
            text: observed.row.get(0),
            bytes: observed.row.get(1),
            number: observed.row.get(2),
            encrypted: observed.row.get(3),
        }],
        observer_predates_lock_holders: Some(observer_predates_lock_holders),
    });
}

/// The rows one observer read over a single borrowed connection while two locks were held.
struct ObservedBesideLocks {
    row: sqlx::postgres::PgRow,
    holders: Vec<sqlx::postgres::PgRow>,
}

#[then(
    "the Postgres fixture observer queried over a connection established before both lock holders \
     connected"
)]
async fn observer_connection_predates_lock_holders(world: &mut ScenarioWorld) {
    let observations = world
        .postgres_fixture_observations
        .as_ref()
        .assured("the preceding step recorded the observer's results");
    let predates = observations
        .observer_predates_lock_holders
        .as_ref()
        .assured("an observer beside held locks records how its connection compares with theirs");
    assert_eq!(
        predates.len(),
        2,
        "the observer found {} holders of the two locked tables",
        predates.len()
    );
    assert!(
        !predates.contains(&false),
        "the observer queried over a connection established after a lock holder's, so holding the \
         locks cost the observation pool a connection it had to establish again: {predates:?}"
    );
}

#[when(expr = "a new Postgres fixture connection is established over {word}")]
async fn new_fixture_connection(world: &mut ScenarioWorld, transport: String) {
    let tls = transport_uses_tls(&transport);
    let options = world
        .dependencies
        .postgres_client(tls)
        .expect("Postgres is running before a connection is established")
        .connect_options();
    let mut establishing = std::pin::pin!(sqlx::postgres::PgConnection::connect_with(&options));
    let mut polls: u32 = 0;
    let connection = std::future::poll_fn(|context| {
        polls = polls
            .checked_add(1)
            .assured("Postgres caps a verifier at i32::MAX rounds, under u32::MAX polls");
        establishing.as_mut().poll(context)
    })
    .await
    .expect("the environment's observation options establish a connection");
    connection
        .close()
        .await
        .expect("the established fixture connection closes");
    world.postgres_fixture_connection = Some(ConnectionEstablishment { polls });
}

#[then(expr = "the Postgres fixture connection was established within {int} polls")]
async fn connection_established_within(world: &mut ScenarioWorld, limit: u32) {
    let establishment = world
        .postgres_fixture_connection
        .take()
        .assured("the preceding step recorded how the connection was established");
    assert!(
        establishment.polls <= limit,
        "establishing a Postgres fixture connection took {} polls, more than {limit}",
        establishment.polls
    );
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
        observer_predates_lock_holders: None,
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
