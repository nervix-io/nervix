//! Outside the layer order: MySQL insert isolation under contention for one shared connection.
//!
//! - **Owns.** Scenario table locks, server-observed insert waits, and complete emitted-row checks.
//! - **Depends on.** The scenario's MySQL endpoints, driver and public emitter observations.
//! - **Must not know.** Product connection handles, insert implementation or retry decisions.

use super::*;

/// An independent session holding one table lock until the scenario releases it.
pub(super) struct HeldTableLock {
    connection: mysql_async::Conn,
    pool: MySqlPool,
}

impl HeldTableLock {
    async fn release(mut self) {
        self.connection
            .query_drop("UNLOCK TABLES")
            .await
            .expect("the fixture session must release its MySQL table lock");
        drop(self.connection);
        self.pool
            .disconnect()
            .await
            .expect("the released MySQL lock session must disconnect");
    }
}

impl ScenarioWorld {
    /// Release locks before cluster teardown, including after a failed assertion, so no writer
    /// drains behind a lock whose release otherwise waits for this world to be dropped.
    pub(super) async fn release_mysql_table_locks(&mut self) {
        let locks = std::mem::take(&mut self.mysql_table_locks);
        for lock in locks.into_values() {
            nervix_primitives::task::consume_budget().await;
            lock.release().await;
        }
    }
}

#[given(expr = "MySQL table {string} rejecting poison actions exists")]
async fn poison_table(world: &mut ScenarioWorld, table: String) {
    given_mysql_table_recording_insert_commands_exists(world, table).await;
    let table = world
        .mysql_table
        .as_ref()
        .expect("table preparation records its name");
    let pool = mysql_root_pool(world.dependencies.endpoints()).expect("the root endpoint is valid");
    let mut connection = pool.get_conn().await.expect("MySQL is running");
    connection
        .query_drop(format!(
            "ALTER TABLE `{table}` ADD CHECK (mysql_action <> 'poison')"
        ))
        .await
        .expect("the poison table must enforce its row constraint");
    drop(connection);
    pool.disconnect().await.expect("the setup pool disconnects");
}

#[given(expr = "MySQL table {string} is locked against inserts")]
async fn lock_table(world: &mut ScenarioWorld, table: String) {
    let table = expand_placeholders(world, &table);
    let pool = mysql_root_pool(world.dependencies.endpoints()).expect("the root endpoint is valid");
    let mut connection = pool.get_conn().await.expect("MySQL is running");
    connection
        .query_drop(format!("LOCK TABLES `{table}` WRITE"))
        .await
        .expect("the fixture must acquire its MySQL table lock");
    let previous = world
        .mysql_table_locks
        .insert(table, HeldTableLock { connection, pool });
    assert!(
        previous.is_none(),
        "the scenario takes each table lock once"
    );
}

#[when(expr = "the lock on MySQL table {string} is released")]
async fn release_table(world: &mut ScenarioWorld, table: String) {
    let table = expand_placeholders(world, &table);
    let lock = world
        .mysql_table_locks
        .remove(&table)
        .expect("the scenario holds this table lock before releasing it");
    lock.release().await;
}

#[then(expr = "MySQL eventually reports an insert into table {string} waiting for its lock")]
async fn observe_insert_wait(world: &mut ScenarioWorld, table: String) {
    let table = expand_placeholders(world, &table);
    let pool = mysql_root_pool(world.dependencies.endpoints()).expect("the root endpoint is valid");
    let mut connection = pool.get_conn().await.expect("MySQL is running");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        nervix_primitives::task::consume_budget().await;
        let waiting: u64 = connection
            .exec_first(
                "SELECT COUNT(*) FROM performance_schema.metadata_locks WHERE OBJECT_SCHEMA = \
                 DATABASE() AND OBJECT_NAME = ? AND LOCK_STATUS = 'PENDING'",
                (&table,),
            )
            .await
            .expect("the fixture can inspect MySQL metadata lock waits")
            .expect("COUNT always returns one row");
        if waiting > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no insert into MySQL table {table} waited for its lock"
        );
        nervix_primitives::time::sleep(Duration::from_millis(100)).await;
    }
    drop(connection);
    pool.disconnect()
        .await
        .expect("the observer pool disconnects");
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize)]
struct EmittedRow {
    mysql_user_id: i64,
    mysql_action: String,
}

#[then(expr = "the MySQL table {string} eventually holds exactly these rows")]
async fn exact_rows(world: &mut ScenarioWorld, table: String, #[step] step: &Step) {
    let table = expand_placeholders(world, &table);
    let expected = expand_placeholders(world, docstring(step));
    let mut expected_rows = Vec::new();
    for line in expected.lines() {
        let line = line.trim();
        if !line.is_empty() {
            expected_rows.push(
                serde_json::from_str::<EmittedRow>(line)
                    .expect("each expected row is complete JSON"),
            );
        }
    }
    expected_rows.sort();
    let pool = mysql_root_pool(world.dependencies.endpoints()).expect("the root endpoint is valid");
    let mut connection = pool.get_conn().await.expect("MySQL is running");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        nervix_primitives::task::consume_budget().await;
        let rows: Vec<mysql_async::Row> = connection
            .query(format!(
                "SELECT mysql_user_id, mysql_now, mysql_action FROM `{table}`"
            ))
            .await
            .expect("the fixture can read the emitted rows");
        let mut observed = Vec::with_capacity(rows.len());
        for row in rows {
            let now: String = row
                .get("mysql_now")
                .expect("each mapped instant is present");
            chrono::DateTime::parse_from_rfc3339(&now)
                .expect("each mapped instant retains its timestamp");
            observed.push(EmittedRow {
                mysql_user_id: row
                    .get("mysql_user_id")
                    .expect("each mapped user is present"),
                mysql_action: row
                    .get("mysql_action")
                    .expect("each mapped action is present"),
            });
        }
        observed.sort();
        if observed == expected_rows {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "MySQL table {table} must hold exactly {expected_rows:?}; observed {observed:?}"
        );
        nervix_primitives::time::sleep(Duration::from_millis(100)).await;
    }
    drop(connection);
    pool.disconnect()
        .await
        .expect("the observer pool disconnects");
}

#[then(expr = "MySQL recorded {int} inserts into table {string} on one connection")]
async fn same_connection(world: &mut ScenarioWorld, expected: u64, table: String) {
    let table = expand_placeholders(world, &table);
    let pool = mysql_root_pool(world.dependencies.endpoints()).expect("the root endpoint is valid");
    let mut connection = pool.get_conn().await.expect("MySQL is running");
    let row: mysql_async::Row = connection
        .exec_first(
            "SELECT COUNT(*), COUNT(DISTINCT thread_id) FROM mysql.general_log WHERE command_type \
             = 'Execute' AND argument LIKE ?",
            (format!("INSERT INTO `{table}` %"),),
        )
        .await
        .expect("the fixture can read the MySQL insert log")
        .expect("COUNT always returns one row");
    let statements: u64 = row.get(0).expect("the insert count is present");
    let connections: u64 = row.get(1).expect("the connection count is present");
    assert_eq!(statements, expected, "the refused insert and its isolation");
    assert_eq!(connections, 1, "every insert used the same server session");
    drop(connection);
    pool.disconnect()
        .await
        .expect("the observer pool disconnects");
}
