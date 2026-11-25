//! The consoles a backup download or a restore was sent to since it last paused.
//!
//! Layer: edges.
//! - **Owns.** Telling a redirect back to a console the transfer already reached since its last
//!   pause, which makes the transfer pause before it follows.
//! - **Depends on.** Console addresses as the redirects and the session name them.
//! - **Must not know.** Sockets, timers, frames or what a transfer carries.
//!
//! A transfer follows a redirect to the leader or to the node that retains an archive at once, so
//! a download that starts at one node and ends at another pays no wait. Two nodes whose views of
//! the leader or of the holder disagree for a moment can send a transfer back and forth, though,
//! and a transfer bounded by time rather than by a count of attempts would bounce between them
//! until its time ran out. A redirect back to a console reached since the last pause therefore
//! pauses first, as an election does, and the views have time to agree.

use std::collections::BTreeSet;

use url::Url;

/// The console `address` names, by its origin: a console is one node's listener, whatever path or
/// trailing slash its address is written with. Text that is no URL stands for itself.
fn console_of(address: &str) -> String {
    match Url::parse(address) {
        Ok(url) => url.origin().ascii_serialization(),
        Err(_) => address.to_owned(),
    }
}

/// When a transfer makes its next attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NextAttempt {
    /// At once, at a console the transfer has not reached since it last paused.
    AtOnce,
    /// After the pause between attempts.
    AfterPause,
}

/// The consoles a transfer reached since it last paused, its current one included. They are
/// consoles of the cluster's nodes, so the set holds at most one entry per node.
pub(crate) struct RedirectChain {
    reached: BTreeSet<String>,
}

impl RedirectChain {
    /// A chain whose transfer starts at `console`, when the console knows one.
    pub(crate) fn starting_at(console: Option<&str>) -> Self {
        let mut chain = Self {
            reached: BTreeSet::new(),
        };
        chain.paused_at(console);
        chain
    }

    /// The transfer was sent to `console`: at once when it has not reached that console since it
    /// last paused, and after a pause when it has.
    pub(crate) fn follow(&mut self, console: &str) -> NextAttempt {
        if self.reached.insert(console_of(console)) {
            return NextAttempt::AtOnce;
        }
        NextAttempt::AfterPause
    }

    /// The transfer paused and makes its next attempt at `console`, so the chain starts again
    /// there.
    pub(crate) fn paused_at(&mut self, console: Option<&str>) {
        self.reached.clear();
        if let Some(console) = console {
            self.reached.insert(console_of(console));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_chain_to_new_consoles_follows_at_once() {
        let mut chain = RedirectChain::starting_at(Some("http://node-1"));
        assert_eq!(chain.follow("http://node-2"), NextAttempt::AtOnce);
        assert_eq!(chain.follow("http://node-3"), NextAttempt::AtOnce);
    }

    #[test]
    fn a_redirect_back_to_a_reached_console_pauses_and_starts_the_chain_again() {
        let mut chain = RedirectChain::starting_at(Some("http://node-1"));
        assert_eq!(chain.follow("http://node-2"), NextAttempt::AtOnce);
        assert_eq!(chain.follow("http://node-1"), NextAttempt::AfterPause);
        chain.paused_at(Some("http://node-1"));
        assert_eq!(chain.follow("http://node-2"), NextAttempt::AtOnce);
        assert_eq!(chain.follow("http://node-2"), NextAttempt::AfterPause);
    }

    #[test]
    fn a_console_is_the_same_however_its_address_is_written() {
        let mut chain = RedirectChain::starting_at(Some("http://node-1:8080"));
        assert_eq!(chain.follow("http://node-1:8080/"), NextAttempt::AfterPause);
        chain.paused_at(Some("http://node-1:8080/console/"));
        assert_eq!(chain.follow("http://node-1:8080"), NextAttempt::AfterPause);
        assert_eq!(chain.follow("https://node-1:8080"), NextAttempt::AtOnce);
    }

    #[test]
    fn text_that_is_no_url_stands_for_itself() {
        let mut chain = RedirectChain::starting_at(Some("node-1"));
        assert_eq!(chain.follow("node-1"), NextAttempt::AfterPause);
        assert_eq!(chain.follow("node-1/"), NextAttempt::AtOnce);
    }

    #[test]
    fn a_chain_without_a_starting_console_counts_from_its_first_redirect() {
        let mut chain = RedirectChain::starting_at(None);
        assert_eq!(chain.follow("http://node-1"), NextAttempt::AtOnce);
        assert_eq!(chain.follow("http://node-1"), NextAttempt::AfterPause);
    }
}
