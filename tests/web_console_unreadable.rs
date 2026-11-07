//! Controlled unreadable replies at the real browser's transport boundary.
//!
//! Layer: test harness.
//! - **Owns.** A bounded one-shot reply substitution and observations of original references.
//! - **Depends on.** Public codecs, Playwright and the scenario's browser.
//! - **Must not know.** Console request queues, retry state or server command execution.

use std::{num::NonZeroUsize, time::Duration};

use cucumber::{given, then, when};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    Reply, ReplyBody, ReplyDelivery, RequestId, SessionLimitSettings, SessionLimits,
    SuggestOutcome, Suggestion, SuggestionKind, SuggestionStatus, TextEdit,
};
use serde::Deserialize;

use super::{ScenarioWorld, expand_placeholders};

const MAX_RECORDED_ATTEMPTS: usize = 32;

pub(super) fn fixture_script() -> String {
    let defaults = SessionLimits::DEFAULT;
    let limits = SessionLimits::try_from(SessionLimitSettings {
        frame_bytes: NonZeroUsize::new(1024).assured("a nonzero frame bound"),
        transfer_bytes: NonZeroUsize::new(defaults.transfer_bytes()).assured("nonzero bound"),
        nesting_depth: NonZeroUsize::new(defaults.nesting_depth()).assured("nonzero bound"),
        collection_entries: NonZeroUsize::new(defaults.collection_entries())
            .assured("nonzero bound"),
        string_bytes: NonZeroUsize::new(defaults.string_bytes()).assured("nonzero bound"),
    })
    .assured("one KiB is an admitted frame bound");
    let long = "x".repeat(2048);
    let reply = Reply {
        request_id: RequestId::new(std::num::NonZeroU64::MIN),
        body: ReplyBody::Suggest(SuggestOutcome {
            status: SuggestionStatus::Ready,
            continuation: None,
            suggestions: vec![Suggestion {
                value: long.clone(),
                kind: SuggestionKind::Text,
                edit: TextEdit {
                    start: 0,
                    end: 0,
                    replacement: long,
                },
            }],
        }),
    };
    let ReplyDelivery::Transfer(mut parts) =
        reply.encode(&limits).assured("the fixture fits a transfer")
    else {
        panic!("the fixture spans frames");
    };
    parts.next().assured("a transfer has a first part");
    let part = parts.next().assured("the fixture spans several parts");
    let bytes = Vec::from(part.into_bytes());
    let fixture = serde_json::to_string(&bytes).assured("fixture bytes serialize");
    let script = include_str!("web_console_unreadable/replies.js")
        .replace("__NERVIX_TRANSFER_PART__", &fixture);
    script.replace(
        "__NERVIX_MAX_RECORDED_ATTEMPTS__",
        &MAX_RECORDED_ATTEMPTS.to_string(),
    )
}

#[when("the web console is opened on the leader node with the reply fixture")]
async fn open(world: &mut ScenarioWorld) {
    super::close_browser(world).await;
    let leader = super::current_leader_node(world).await;
    let url = world
        .cluster()
        .web_console_url(&leader)
        .assured("the leader serves its console");
    super::open_recorded_web_console_page(
        world,
        &url,
        super::web_console_connections::InitialSession::UnreadableReplies,
    )
    .await
    .assured("the fixture starts before the console");
}

#[given(expr = "the next reply to console command {string} is unreadable as {string}")]
async fn arm(world: &mut ScenarioWorld, query: String, kind: String) {
    assert!(["decode", "split", "restore"].contains(&kind.as_str()));
    let query = expand_placeholders(world, &query);
    world
        .browser_page
        .as_ref()
        .assured("the console is open")
        .evaluate::<_, ()>(
            "([query, kind]) => window.__nervixUnreadableReply.arm(query, kind)",
            Some(&[query, kind]),
        )
        .await
        .assured("the one-shot fixture arms");
}

#[derive(Debug, Deserialize)]
struct Observed {
    references: Vec<String>,
    injected: usize,
    injected_at: Option<usize>,
    foreign: Option<String>,
}

#[then(
    expr = "the reply fixture observed at least {int} attempts from the unreadable reply under \
            the original execution reference"
)]
async fn observed(world: &mut ScenarioWorld, attempts: usize) {
    assert!((1..=MAX_RECORDED_ATTEMPTS).contains(&attempts));
    let page = world.browser_page.as_ref().assured("the console is open");
    let mut last_observation = None;
    let result = nervix_primitives::time::timeout(super::WEB_CONSOLE_ASSERTION_TIMEOUT, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let observed = page
                .evaluate::<(), Observed>(
                    "() => window.__nervixUnreadableReply.observed()",
                    None::<&()>,
                )
                .await
                .assured("the fixture retains its bounded observations");
            if observed.injected == 1
                && let Some(injected_at) = observed.injected_at
            {
                assert!(injected_at < MAX_RECORDED_ATTEMPTS);
                let needed = injected_at
                    .checked_add(attempts)
                    .assured("two fixture counts bounded by 32 fit usize");
                if observed.references.len() >= needed {
                    return observed;
                }
            }
            last_observation = Some(observed);
            nervix_primitives::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    let result = match result {
        Ok(result) => result,
        Err(error) => panic!(
            "expected {attempts} attempts from the unreadable reply: {error}; last observation: \
             {last_observation:?}"
        ),
    };
    assert_eq!(result.injected, 1);
    let injected_at = result
        .injected_at
        .assured("the observed reply was substituted");
    let original = result.references.first().assured("an attempt was observed");
    assert!(
        result
            .references
            .iter()
            .all(|reference| reference == original),
        "{result:?}"
    );
    world
        .placeholders
        .insert("own_execution".to_string(), original.clone());
    if let Some(foreign) = result.foreign {
        assert_eq!(
            result
                .references
                .len()
                .checked_sub(injected_at)
                .assured("the substitution belongs to an observed attempt"),
            1,
            "a restore stops after its unreadable reply"
        );
        assert_ne!(&foreign, original);
        world
            .placeholders
            .insert("foreign_execution".to_string(), foreign);
    }
}
