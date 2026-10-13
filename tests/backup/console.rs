//! Steps that drive the web console's backup and restore controls: the archives a browser saves
//! from a console backup, the archives a console restore reads from a file input, and the pause
//! that holds a download before its archive streams.
//!
//! Layer: test harness.
//! - **Owns.** Saving the archive a console download hands the browser into the scenario's archive
//!   directory, handing a scenario archive to a file input, reloading the console while it
//!   downloads, writing and reading the pending backup record in the tab's session storage, and
//!   checking what the console reports against an archive's own size and digest.
//! - **Depends on.** The scenario's browser page, its archive directory, and the server's fault
//!   injection for backup downloads.
//! - **Must not know.** How the console verifies, assembles or streams an archive.

use cucumber::{given, then, when};
use playwright_rs::{EventWaiter, protocol::Download};

use super::*;

/// How long a console backup may take to reach the browser as a download, the backup itself
/// included. It bounds a wait for something to happen, so it is generous under parallel load.
const CONSOLE_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(120);

/// The session storage keys the console records the backup a reload resumes under.
const PENDING_REFERENCE_KEY: &str = "nervix.console.backup.reference";
const PENDING_QUERY_KEY: &str = "nervix.console.backup.query";
const PENDING_DOMAIN_KEY: &str = "nervix.console.backup.domain";
const PENDING_BACKUP_KEYS: [&str; 3] =
    [PENDING_REFERENCE_KEY, PENDING_QUERY_KEY, PENDING_DOMAIN_KEY];

/// How long a reloaded console may take to read the pending backup record. It bounds a wait for
/// something to happen, so it is generous under parallel load.
const CONSOLE_RECORD_TIMEOUT: Duration = Duration::from_secs(60);

fn browser_page(world: &ScenarioWorld) -> &playwright_rs::Page {
    world
        .browser_page
        .as_ref()
        .expect("a browser page must be opened before console backup actions")
}

/// The next download the console hands the browser. The waiter must exist before the action that
/// starts the download, or the download could arrive unobserved.
async fn expect_console_download(world: &ScenarioWorld) -> EventWaiter<Download> {
    let timeout_ms: u32 = CONSOLE_DOWNLOAD_TIMEOUT
        .as_millis()
        .try_into()
        .assured("the console download timeout is a few minutes of milliseconds");
    browser_page(world)
        .expect_download(Some(f64::from(timeout_ms)))
        .await
        .expect("the browser page accepts a download waiter")
}

/// What the console's backup dialog and terminal show, which explains a download that never came.
async fn console_backup_view(page: &playwright_rs::Page) -> String {
    let mut view = Vec::new();
    for selector in [".backup-dialog", ".terminal"] {
        let texts = page
            .locator(selector)
            .all_inner_texts()
            .await
            .unwrap_or_else(|error| vec![format!("<{selector} unreadable: {error}>")]);
        view.push(format!("{selector}:\n{}", texts.join("\n")));
    }
    view.join("\n")
}

/// Waits for the download `waiter` observes and saves it as the scenario archive at `archive`.
async fn save_console_download(
    page: &playwright_rs::Page,
    waiter: EventWaiter<Download>,
    archive: &Path,
) {
    let download = match waiter.wait().await {
        Ok(download) => download,
        Err(error) => {
            let view = console_backup_view(page).await;
            panic!("the web console handed the browser no download: {error}\n{view}");
        }
    };
    if let Some(failure) = download
        .failure()
        .await
        .expect("the download reports whether it failed")
    {
        panic!("the console's download failed in the browser: {failure}");
    }
    download
        .save_as(archive)
        .await
        .expect("the downloaded archive is saved into the scenario's archive directory");
}

#[when(
    expr = "selector {string} is clicked and the web console's download is saved as backup \
            archive {string}"
)]
async fn when_selector_is_clicked_and_download_is_saved(
    world: &mut ScenarioWorld,
    selector: String,
    file: String,
) {
    let archive = archive_path(world, &file);
    let selector = expand_placeholders(world, &selector);
    let waiter = expect_console_download(world).await;
    browser_page(world)
        .locator(&selector)
        .click(None)
        .await
        .expect("selector must be clickable");
    save_console_download(browser_page(world), waiter, &archive).await;
}

#[when(
    expr = "selector {string} is pressed with {string} and the web console's download is saved as \
            backup archive {string}"
)]
async fn when_selector_is_pressed_and_download_is_saved(
    world: &mut ScenarioWorld,
    selector: String,
    key: String,
    file: String,
) {
    let archive = archive_path(world, &file);
    let selector = expand_placeholders(world, &selector);
    let waiter = expect_console_download(world).await;
    browser_page(world)
        .locator(&selector)
        .press(&key, None)
        .await
        .expect("selector must accept the key press");
    save_console_download(browser_page(world), waiter, &archive).await;
}

#[when(
    expr = "the web console page is reloaded and its next download is saved as backup archive \
            {string}"
)]
async fn when_console_is_reloaded_and_download_is_saved(world: &mut ScenarioWorld, file: String) {
    let archive = archive_path(world, &file);
    let waiter = expect_console_download(world).await;
    browser_page(world)
        .reload(None)
        .await
        .expect("the web console page reloads");
    save_console_download(browser_page(world), waiter, &archive).await;
}

#[when("the web console page is reloaded")]
async fn when_console_is_reloaded(world: &mut ScenarioWorld) {
    browser_page(world)
        .reload(None)
        .await
        .expect("the web console page reloads");
}

#[when(
    expr = "a pending backup of {string} whose domain entry spells {string} in upper case is \
            written into the web console tab's session storage"
)]
async fn when_respelled_pending_backup_is_written(
    world: &mut ScenarioWorld,
    query: String,
    domain: String,
) {
    let query = expand_placeholders(world, &query);
    let domain = expand_placeholders(world, &domain).to_uppercase();
    // A retry identity minted now, as the console mints one, so the leader would admit the backup
    // again if the console resumed the record.
    let reference = uuid::Uuid::now_v7().to_string();
    let entries = serde_json::to_string(&[
        [PENDING_QUERY_KEY, query.as_str()],
        [PENDING_DOMAIN_KEY, domain.as_str()],
        [PENDING_REFERENCE_KEY, reference.as_str()],
    ])
    .assured("the record's entries serialize as JavaScript text");
    let script = format!(
        r#"() => {{
            for (const [key, value] of {entries}) {{
                sessionStorage.setItem(key, value);
            }}
            return true;
        }}"#
    );
    let written = browser_page(world)
        .evaluate::<(), bool>(&script, None::<&()>)
        .await
        .expect("the tab's session storage accepts the record");
    assert!(written, "the tab's session storage did not take the record");
}

#[then("the web console tab's session storage holds no pending backup")]
async fn then_no_pending_backup_is_recorded(world: &mut ScenarioWorld) {
    let keys = serde_json::to_string(&PENDING_BACKUP_KEYS)
        .assured("the record's keys serialize as JavaScript text");
    let script = format!("() => {keys}.filter((key) => sessionStorage.getItem(key) !== null)");
    let page = browser_page(world);
    let deadline = Instant::now() + CONSOLE_RECORD_TIMEOUT;
    loop {
        nervix_primitives::task::consume_budget().await;
        let recorded = page
            .evaluate::<(), Vec<String>>(&script, None::<&()>)
            .await
            .expect("the tab's session storage is readable");
        if recorded.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the reloaded console still records a pending backup under {recorded:?}"
        );
        nervix_primitives::time::sleep(Duration::from_millis(100)).await;
    }
}

#[when(expr = "selector {string} is given backup archive {string}")]
async fn when_file_input_is_given_backup_archive(
    world: &mut ScenarioWorld,
    selector: String,
    file: String,
) {
    let archive = archive_path(world, &file);
    let selector = expand_placeholders(world, &selector);
    browser_page(world)
        .locator(&selector)
        .set_input_files(&archive, None)
        .await
        .expect("the file input accepts the scenario's archive");
}

#[then(expr = "selector {string} reports backup archive {string} sent in full")]
async fn then_selector_reports_archive_sent_in_full(
    world: &mut ScenarioWorld,
    selector: String,
    file: String,
) {
    let archive = archive_path(world, &file);
    let length = std::fs::metadata(&archive)
        .expect("the scenario's archive exists")
        .len();
    then_selector_contains_text(world, selector, format!("{length} of {length} bytes")).await;
}

#[then(expr = "backup archive {string} has the size and digest selector {string} shows")]
async fn then_archive_has_the_size_and_digest_the_console_shows(
    world: &mut ScenarioWorld,
    file: String,
    selector: String,
) {
    let archive = archive_path(world, &file);
    let bytes = std::fs::read(&archive).expect("the scenario's archive is readable");
    let digest = blake3::hash(&bytes).to_hex().to_string();
    then_selector_contains_text(world, selector.clone(), format!("{} bytes", bytes.len())).await;
    then_selector_contains_text(world, selector, digest).await;
}

#[given(expr = "backup archive downloads on node {string} pause before the archive streams")]
async fn given_backup_download_pause(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .pause_backup_download_on(crate::common::cluster::node_name(&node_id));
}

#[then(expr = "the backup archive download pause on node {string} is reached")]
async fn then_backup_download_pause_is_reached(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    let node_name = crate::common::cluster::node_name(&node_id);
    let fault_injection = world.fault_injection.clone();
    nervix_primitives::select! {
        () = fault_injection.wait_for_backup_download_pause(&node_name) => {},
        () = nervix_primitives::time::sleep(CONSOLE_DOWNLOAD_TIMEOUT) => {
            panic!("the backup archive download pause on node '{node_id}' was not reached");
        }
    }
}

#[when(expr = "the backup archive download pause on node {string} is released")]
async fn when_backup_download_pause_is_released(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .release_backup_download_pause(&crate::common::cluster::node_name(&node_id));
}
