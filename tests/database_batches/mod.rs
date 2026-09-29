//! Database batch targets: the tables and collections the database emitter batching scenarios
//! write to, what each write carried, and the rows they finally hold.
//!
//! Layer: test harness.
//! - **Owns.** Provisioning one batch table or collection per scenario target, reading the row
//!   count of every write the destination recorded in the order it took them, and projecting the
//!   stored rows onto the JSON the scenarios compare.
//! - **Depends on.** The scenario world's dependency endpoints and each database's own client.
//! - **Must not know.** How the emitter packs, measures or retries its writes.

use cucumber::{given, then};

use super::*;

/// How long a table must keep exactly what a step expects once it holds it: a write the emitter
/// must never repeat never arrives, so the re-read only strengthens the assertion.
const STABLE_READ_DELAY: Duration = Duration::from_millis(500);

/// The database a batch scenario writes to, named as its `<sink>` examples column spells it.
#[derive(Debug, Clone, Copy)]
enum DatabaseSink {
    ClickHouse,
    Postgres,
    MySql,
    MongoDb,
}

/// What a batch table enforces besides holding the rows written to it.
#[derive(Debug, Clone, Copy)]
enum BatchTableRule {
    /// Every write is recorded with the number of rows it carried.
    RecordsWrites,
    /// `seq` is unique, which a conflict policy targets.
    KeyedBySeq,
    /// A row whose `note` is `poison` is refused by the database itself.
    RejectsPoisonNotes,
}

impl DatabaseSink {
    fn parse(name: &str) -> Self {
        match name {
            "ClickHouse" => Self::ClickHouse,
            "Postgres" => Self::Postgres,
            "MySQL" => Self::MySql,
            "MongoDB" => Self::MongoDb,
            other => panic!("unknown database batch sink {other:?}"),
        }
    }

    /// Creates `table` with the columns every batch scenario maps: `seq`, `note`, the nested list
    /// `tags`, the fixed-size array `pair`, and the bytes `raw`.
    async fn create_table(self, world: &ScenarioWorld, table: &str, rule: BatchTableRule) {
        let dependencies = world.dependencies.endpoints();
        match self {
            Self::ClickHouse => {
                let constraint = match rule {
                    BatchTableRule::RecordsWrites => "",
                    BatchTableRule::RejectsPoisonNotes => {
                        ", CONSTRAINT reject_poison CHECK note != 'poison'"
                    }
                    BatchTableRule::KeyedBySeq => {
                        panic!(
                            "a ClickHouse emitter declares no conflict policy to key a table for"
                        )
                    }
                };
                clickhouse_post(dependencies, &format!("DROP TABLE IF EXISTS {table}"))
                    .await
                    .expect("failed to drop the ClickHouse batch table");
                // Merges stay stopped, so every insert remains the one part it created.
                clickhouse_post(
                    dependencies,
                    &format!(
                        "CREATE TABLE {table} (seq Int64, note String, tags Array(Array(Int64)), \
                         pair Array(Int64), raw String{constraint}) ENGINE = MergeTree ORDER BY \
                         tuple()"
                    ),
                )
                .await
                .expect("failed to create the ClickHouse batch table");
                clickhouse_post(dependencies, &format!("SYSTEM STOP MERGES {table}"))
                    .await
                    .expect("failed to stop ClickHouse batch table merges");
            }
            Self::Postgres => {
                let constraint = match rule {
                    BatchTableRule::RecordsWrites => "",
                    BatchTableRule::KeyedBySeq => ", PRIMARY KEY (seq)",
                    BatchTableRule::RejectsPoisonNotes => {
                        ", CONSTRAINT reject_poison CHECK (note <> 'poison')"
                    }
                };
                let client = postgres_client(dependencies, false)
                    .await
                    .expect("failed to connect to Postgres");
                sqlx::raw_sql(SqlxAssertSqlSafe(format!(
                    "DROP TABLE IF EXISTS {table};
                     DROP TABLE IF EXISTS {table}_writes;
                     CREATE TABLE {table} (seq bigint, note text, tags jsonb, pair jsonb, raw \
                     bytea{constraint});
                     CREATE TABLE {table}_writes (id bigserial PRIMARY KEY, row_count bigint NOT \
                     NULL);
                     CREATE FUNCTION {table}_record_write() RETURNS trigger AS $$
                     BEGIN
                       INSERT INTO {table}_writes (row_count) SELECT count(*) FROM written_rows;
                       RETURN NULL;
                     END;
                     $$ LANGUAGE plpgsql;
                     CREATE TRIGGER record_write AFTER INSERT ON {table}
                     REFERENCING NEW TABLE AS written_rows
                     FOR EACH STATEMENT EXECUTE FUNCTION {table}_record_write();"
                )))
                .execute(&client)
                .await
                .expect("failed to create the Postgres batch table");
            }
            Self::MySql => {
                // A MySQL check constraint name is unique across its database, so the one this
                // table rejects poison notes with is left to MySQL to name.
                let constraint = match rule {
                    BatchTableRule::RecordsWrites => "",
                    BatchTableRule::KeyedBySeq => ", PRIMARY KEY (seq)",
                    BatchTableRule::RejectsPoisonNotes => ", CHECK (note <> 'poison')",
                };
                let pool = mysql_root_pool(dependencies).expect("failed to build MySQL root pool");
                let mut conn = pool
                    .get_conn()
                    .await
                    .expect("failed to connect to MySQL as root");
                conn.query_drop(format!("DROP TABLE IF EXISTS nervix.`{table}`"))
                    .await
                    .expect("failed to drop the MySQL batch table");
                conn.query_drop(format!(
                    "CREATE TABLE nervix.`{table}` (seq bigint NOT NULL, note longtext, tags \
                     json, pair json, raw longblob{constraint})"
                ))
                .await
                .expect("failed to create the MySQL batch table");
                // The general log records every statement the emitter executes, values included,
                // which is where the rows of each write are counted.
                conn.query_drop("SET GLOBAL log_output = 'TABLE'")
                    .await
                    .expect("failed to direct the MySQL general log to its table");
                conn.query_drop("SET GLOBAL general_log = 'ON'")
                    .await
                    .expect("failed to enable the MySQL general log");
                drop(conn);
                pool.disconnect()
                    .await
                    .expect("failed to disconnect the MySQL root pool");
            }
            Self::MongoDb => {
                let client = mongodb_client(dependencies, false)
                    .await
                    .expect("failed to connect to MongoDB");
                let database = client.database("nervix");
                database
                    .collection::<MongoDbDocument>(table)
                    .drop()
                    .await
                    .expect("failed to drop the MongoDB batch collection");
                database
                    .create_collection(table)
                    .await
                    .expect("failed to create the MongoDB batch collection");
                match rule {
                    BatchTableRule::RecordsWrites => {
                        // Profiling every command records each insert with the documents it
                        // inserted.
                        database
                            .run_command(mongodb_doc! {
                                "profile": 2,
                                "slowms": 0,
                                "sampleRate": 1.0,
                            })
                            .await
                            .expect("failed to enable MongoDB command profiling");
                    }
                    BatchTableRule::KeyedBySeq => {
                        database
                            .run_command(mongodb_doc! {
                                "createIndexes": table,
                                "indexes": [{
                                    "key": { "seq": 1 },
                                    "name": "seq_unique",
                                    "unique": true,
                                }],
                            })
                            .await
                            .expect("failed to create the MongoDB unique seq index");
                    }
                    BatchTableRule::RejectsPoisonNotes => {
                        database
                            .run_command(mongodb_doc! {
                                "collMod": table,
                                "validator": { "note": { "$ne": "poison" } },
                                "validationLevel": "strict",
                                "validationAction": "error",
                            })
                            .await
                            .expect("failed to install the MongoDB poison-note validator");
                    }
                }
            }
        }
    }

    /// The rows each write the destination recorded for `table` carried, in the order it took
    /// them.
    async fn recorded_writes(self, world: &ScenarioWorld, table: &str) -> Vec<u64> {
        let dependencies = world.dependencies.endpoints();
        match self {
            Self::ClickHouse => {
                // Each insert is one part while merges are stopped, and block numbers order parts
                // the way their inserts arrived.
                let parts = clickhouse_post(
                    dependencies,
                    &format!(
                        "SELECT rows FROM system.parts WHERE database = currentDatabase() AND \
                         table = '{table}' AND active ORDER BY min_block_number FORMAT \
                         TabSeparated"
                    ),
                )
                .await
                .expect("failed to read the ClickHouse batch table parts");
                parts
                    .lines()
                    .map(|line| {
                        line.trim().parse::<u64>().unwrap_or_else(|error| {
                            panic!("ClickHouse part row count {line:?} is not a number: {error}")
                        })
                    })
                    .collect()
            }
            Self::Postgres => {
                let client = postgres_client(dependencies, false)
                    .await
                    .expect("failed to connect to Postgres");
                let rows = sqlx::query(SqlxAssertSqlSafe(format!(
                    "SELECT row_count FROM {table}_writes ORDER BY id"
                )))
                .fetch_all(&client)
                .await
                .expect("failed to read the Postgres batch table writes");
                rows.iter()
                    .map(|row| {
                        let count: i64 = row.get(0);
                        u64::try_from(count).expect("a statement inserts a non-negative row count")
                    })
                    .collect()
            }
            Self::MySql => {
                let pool = mysql_root_pool(dependencies).expect("failed to build MySQL root pool");
                let mut conn = pool
                    .get_conn()
                    .await
                    .expect("failed to connect to MySQL as root");
                let statements = conn
                    .exec::<Vec<u8>, _, _>(
                        "SELECT argument FROM mysql.general_log WHERE command_type = 'Execute' \
                         AND argument LIKE ? ORDER BY event_time",
                        (format!("INSERT INTO `{table}` %"),),
                    )
                    .await
                    .expect("failed to read the MySQL general log");
                drop(conn);
                pool.disconnect()
                    .await
                    .expect("failed to disconnect the MySQL root pool");
                statements
                    .iter()
                    .map(|statement| mysql_inserted_rows(statement))
                    .collect()
            }
            Self::MongoDb => {
                let client = mongodb_client(dependencies, false)
                    .await
                    .expect("failed to connect to MongoDB");
                let mut cursor = client
                    .database("nervix")
                    .collection::<MongoDbDocument>("system.profile")
                    .find(mongodb_doc! { "op": "insert", "ns": format!("nervix.{table}") })
                    .sort(mongodb_doc! { "ts": 1 })
                    .await
                    .expect("failed to query the MongoDB profile");
                let mut writes = Vec::new();
                while let Some(entry) = cursor
                    .try_next()
                    .await
                    .expect("failed to read the MongoDB profile")
                {
                    let inserted = match entry.get("ninserted") {
                        Some(MongoDbBson::Int32(value)) => u64::try_from(*value).ok(),
                        Some(MongoDbBson::Int64(value)) => u64::try_from(*value).ok(),
                        _ => None,
                    };
                    let inserted = inserted.unwrap_or_else(|| {
                        panic!("MongoDB profile entry has no inserted count: {entry}")
                    });
                    writes.push(inserted);
                }
                writes
            }
        }
    }

    /// Every row `table` holds, projected onto the columns `columns` names, as JSON objects keyed
    /// in sorted order so that equal rows have equal text.
    async fn rows(
        self,
        world: &ScenarioWorld,
        table: &str,
        columns: &BTreeSet<String>,
    ) -> Vec<String> {
        let dependencies = world.dependencies.endpoints();
        let mut rows = Vec::new();
        match self {
            Self::ClickHouse => {
                let body = clickhouse_post(
                    dependencies,
                    &format!(
                        "SELECT seq, note, tags, pair, lower(hex(raw)) AS raw FROM {table} FORMAT \
                         JSONEachRow SETTINGS output_format_json_quote_64bit_integers = 0"
                    ),
                )
                .await
                .expect("failed to read the ClickHouse batch table");
                for line in body.lines().filter(|line| !line.trim().is_empty()) {
                    let row: BTreeMap<String, serde_json::Value> = serde_json::from_str(line)
                        .unwrap_or_else(|error| {
                            panic!("ClickHouse row {line:?} is not a JSON object: {error}")
                        });
                    rows.push(row);
                }
            }
            Self::Postgres => {
                let client = postgres_client(dependencies, false)
                    .await
                    .expect("failed to connect to Postgres");
                let stored = sqlx::query(SqlxAssertSqlSafe(format!(
                    "SELECT seq, note, tags::text, pair::text, encode(raw, 'hex') FROM {table}"
                )))
                .fetch_all(&client)
                .await
                .expect("failed to read the Postgres batch table");
                for row in &stored {
                    let seq: Option<i64> = row.get(0);
                    let note: Option<String> = row.get(1);
                    let tags: Option<String> = row.get(2);
                    let pair: Option<String> = row.get(3);
                    let raw: Option<String> = row.get(4);
                    rows.push(BTreeMap::from([
                        ("seq".to_string(), serde_json::json!(seq)),
                        ("note".to_string(), serde_json::json!(note)),
                        ("tags".to_string(), stored_json(tags)),
                        ("pair".to_string(), stored_json(pair)),
                        ("raw".to_string(), serde_json::json!(raw)),
                    ]));
                }
            }
            Self::MySql => {
                let pool = mysql_pool(dependencies, false).expect("failed to build the MySQL pool");
                let mut conn = pool.get_conn().await.expect("failed to connect to MySQL");
                let stored = conn
                    .query::<(
                        Option<i64>,
                        Option<String>,
                        Option<String>,
                        Option<String>,
                        Option<String>,
                    ), _>(format!(
                        "SELECT seq, note, CAST(tags AS CHAR), CAST(pair AS CHAR), \
                         LOWER(HEX(raw)) FROM `{table}`"
                    ))
                    .await
                    .expect("failed to read the MySQL batch table");
                drop(conn);
                pool.disconnect()
                    .await
                    .expect("failed to disconnect the MySQL pool");
                for (seq, note, tags, pair, raw) in stored {
                    rows.push(BTreeMap::from([
                        ("seq".to_string(), serde_json::json!(seq)),
                        ("note".to_string(), serde_json::json!(note)),
                        ("tags".to_string(), stored_json(tags)),
                        ("pair".to_string(), stored_json(pair)),
                        ("raw".to_string(), serde_json::json!(raw)),
                    ]));
                }
            }
            Self::MongoDb => {
                let client = mongodb_client(dependencies, false)
                    .await
                    .expect("failed to connect to MongoDB");
                let documents = client
                    .database("nervix")
                    .collection::<MongoDbDocument>(table)
                    .find(mongodb_doc! {})
                    .await
                    .expect("failed to query the MongoDB batch collection")
                    .try_collect::<Vec<_>>()
                    .await
                    .expect("failed to read the MongoDB batch collection");
                for document in documents {
                    let mut row = BTreeMap::new();
                    for column in ["seq", "note", "tags", "pair", "raw"] {
                        let value = match document.get(column) {
                            Some(value) => bson_json(value),
                            None => serde_json::Value::Null,
                        };
                        row.insert(column.to_string(), value);
                    }
                    rows.push(row);
                }
            }
        }
        let mut projected = Vec::with_capacity(rows.len());
        for mut row in rows {
            row.retain(|column, _| columns.contains(column));
            projected.push(serde_json::to_string(&row).expect("a JSON object serializes back"));
        }
        projected.sort();
        projected
    }

    async fn row_count(self, world: &ScenarioWorld, table: &str) -> u64 {
        let dependencies = world.dependencies.endpoints();
        match self {
            Self::ClickHouse => {
                let count = clickhouse_post(
                    dependencies,
                    &format!("SELECT count() FROM {table} FORMAT TabSeparated"),
                )
                .await
                .expect("failed to count the ClickHouse batch table rows");
                count.trim().parse().unwrap_or_else(|error| {
                    panic!("ClickHouse count {count:?} is invalid: {error}")
                })
            }
            Self::Postgres => {
                let client = postgres_client(dependencies, false)
                    .await
                    .expect("failed to connect to Postgres");
                let count: i64 =
                    sqlx::query(SqlxAssertSqlSafe(format!("SELECT count(*) FROM {table}")))
                        .fetch_one(&client)
                        .await
                        .expect("failed to count the Postgres batch table rows")
                        .get(0);
                u64::try_from(count).expect("a table holds a non-negative number of rows")
            }
            Self::MySql => {
                let pool = mysql_pool(dependencies, false).expect("failed to build the MySQL pool");
                let mut conn = pool.get_conn().await.expect("failed to connect to MySQL");
                let count = conn
                    .query_first::<u64, _>(format!("SELECT COUNT(*) FROM `{table}`"))
                    .await
                    .expect("failed to count the MySQL batch table rows")
                    .expect("COUNT(*) returns one row");
                drop(conn);
                pool.disconnect()
                    .await
                    .expect("failed to disconnect the MySQL pool");
                count
            }
            Self::MongoDb => {
                let client = mongodb_client(dependencies, false)
                    .await
                    .expect("failed to connect to MongoDB");
                client
                    .database("nervix")
                    .collection::<MongoDbDocument>(table)
                    .count_documents(mongodb_doc! {})
                    .await
                    .expect("failed to count the MongoDB batch collection documents")
            }
        }
    }
}

/// A JSON column read back as text, or null for a SQL NULL.
fn stored_json(text: Option<String>) -> serde_json::Value {
    let Some(text) = text else {
        return serde_json::Value::Null;
    };
    serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("stored JSON column {text:?} does not parse: {error}"))
}

/// A stored BSON value as the JSON the scenarios compare: numbers, strings and arrays as they
/// are, and binary data as lowercase hexadecimal octets.
fn bson_json(value: &MongoDbBson) -> serde_json::Value {
    match value {
        MongoDbBson::Int32(value) => serde_json::json!(i64::from(*value)),
        MongoDbBson::Int64(value) => serde_json::json!(*value),
        MongoDbBson::Double(value) => serde_json::json!(*value),
        MongoDbBson::String(value) => serde_json::json!(value),
        MongoDbBson::Boolean(value) => serde_json::json!(value),
        MongoDbBson::Null => serde_json::Value::Null,
        MongoDbBson::Array(items) => {
            serde_json::Value::Array(items.iter().map(bson_json).collect())
        }
        MongoDbBson::Binary(binary) => {
            let mut hex = String::with_capacity(binary.bytes.len() * 2);
            for byte in &binary.bytes {
                hex.push_str(&format!("{byte:02x}"));
            }
            serde_json::Value::String(hex)
        }
        other => panic!("MongoDB batch document holds an unexpected value {other}"),
    }
}

/// How many rows one logged `INSERT ... VALUES (...), (...)` statement carried: the parenthesized
/// row groups the `VALUES` list holds, skipping everything inside quoted values. The list ends at
/// the first character outside a group that is neither a separator nor the start of another
/// group, which is where a duplicate-key clause begins.
fn mysql_inserted_rows(statement: &[u8]) -> u64 {
    let statement = String::from_utf8_lossy(statement);
    let Some((_, values)) = statement.split_once(" VALUES ") else {
        panic!("logged MySQL insert has no VALUES clause: {statement}");
    };
    let mut rows = 0_u64;
    let mut depth = 0_u32;
    let mut quoted = false;
    let mut escaped = false;
    for character in values.chars() {
        if quoted {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '\'' {
                quoted = false;
            }
            continue;
        }
        if character == '\'' {
            quoted = true;
            continue;
        }
        if depth > 0 {
            if character == '(' {
                depth = depth
                    .checked_add(1)
                    .expect("a logged statement nests few groups");
            } else if character == ')' {
                depth = depth
                    .checked_sub(1)
                    .expect("a logged statement closes what it opens");
            }
            continue;
        }
        if character == '(' {
            rows = rows
                .checked_add(1)
                .expect("a logged statement holds few rows");
            depth = 1;
        } else if character != ',' && !character.is_whitespace() {
            break;
        }
    }
    rows
}

#[given(expr = "the {string} batch table {string} recording its writes exists")]
async fn batch_table_recording_writes_exists(
    world: &mut ScenarioWorld,
    sink: String,
    table: String,
) {
    let table = expand_placeholders(world, &table);
    DatabaseSink::parse(&sink)
        .create_table(world, &table, BatchTableRule::RecordsWrites)
        .await;
}

#[given(expr = "the {string} batch table {string} keyed by seq exists")]
async fn batch_table_keyed_by_seq_exists(world: &mut ScenarioWorld, sink: String, table: String) {
    let table = expand_placeholders(world, &table);
    DatabaseSink::parse(&sink)
        .create_table(world, &table, BatchTableRule::KeyedBySeq)
        .await;
}

#[given(expr = "the {string} batch table {string} rejecting poison notes exists")]
async fn batch_table_rejecting_poison_notes_exists(
    world: &mut ScenarioWorld,
    sink: String,
    table: String,
) {
    let table = expand_placeholders(world, &table);
    DatabaseSink::parse(&sink)
        .create_table(world, &table, BatchTableRule::RejectsPoisonNotes)
        .await;
}

#[then(expr = "within {string} the {string} batch table {string} records writes of {string} rows")]
async fn batch_table_records_writes(
    world: &mut ScenarioWorld,
    duration: String,
    sink: String,
    table: String,
    expected: String,
) {
    let duration =
        humantime::parse_duration(&duration).expect("step duration must be a valid duration");
    let sink = DatabaseSink::parse(&sink);
    let table = expand_placeholders(world, &table);
    let expected = expected
        .split(',')
        .map(|count| {
            count
                .trim()
                .parse::<u64>()
                .unwrap_or_else(|error| panic!("write size {count:?} is not a number: {error}"))
        })
        .collect::<Vec<_>>();
    let deadline = Instant::now() + duration;
    loop {
        tokio::task::consume_budget().await;
        let observed = sink.recorded_writes(world, &table).await;
        if observed == expected {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {sink:?} table {table} to record writes of {expected:?} rows; \
             observed {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(STABLE_READ_DELAY).await;
    let observed = sink.recorded_writes(world, &table).await;
    assert_eq!(
        observed, expected,
        "{sink:?} table {table} recorded another write after the expected ones"
    );
}

#[then(expr = "the {string} batch table {string} eventually holds exactly these rows")]
async fn batch_table_holds_exactly_these_rows(
    world: &mut ScenarioWorld,
    sink: String,
    table: String,
    #[step] step: &Step,
) {
    let sink = DatabaseSink::parse(&sink);
    let table = expand_placeholders(world, &table);
    let mut columns = BTreeSet::new();
    let mut expected = Vec::new();
    for line in docstring(step).lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let row: BTreeMap<String, serde_json::Value> =
            serde_json::from_str(&expand_placeholders(world, line)).unwrap_or_else(|error| {
                panic!("expected row {line:?} is not a JSON object: {error}")
            });
        columns.extend(row.keys().cloned());
        expected.push(serde_json::to_string(&row).expect("a JSON object serializes back"));
    }
    expected.sort();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        tokio::task::consume_budget().await;
        let observed = sink.rows(world, &table, &columns).await;
        if observed == expected {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {sink:?} table {table} to hold exactly {expected:?}; observed \
             {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(STABLE_READ_DELAY).await;
    let observed = sink.rows(world, &table, &columns).await;
    assert_eq!(
        observed, expected,
        "{sink:?} table {table} changed after it held exactly the expected rows"
    );
}

#[then(expr = "the {string} batch table {string} eventually holds {int} rows")]
async fn batch_table_holds_rows(
    world: &mut ScenarioWorld,
    sink: String,
    table: String,
    expected: u64,
) {
    let sink = DatabaseSink::parse(&sink);
    let table = expand_placeholders(world, &table);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        tokio::task::consume_budget().await;
        let observed = sink.row_count(world, &table).await;
        if observed == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {sink:?} table {table} to hold {expected} rows; observed \
             {observed}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
