//! The statement grammar each runtime thread reads session requests with.
//!
//! Layer: edges.
//!
//! - **Owns.** The client statement grammar a runtime thread builds for the first command,
//!   subscription, restore or completion request it reads, and keeps for as long as it lives.
//! - **Depends on.** The language layer's client statement grammar, and thread-local storage from
//!   the primitives boundary.
//! - **Must not know.** What a request asks for, the session it belongs to, or how a transport
//!   carries it.
//!
//! Building the grammar constructs the parser of every Plexus statement, which costs far more than
//! reading a request with it, and a grammar cannot move between threads. A node reads its session
//! requests on the worker threads of its runtime and keeps each thread's grammar between requests,
//! so it holds at most one grammar on each of its worker threads.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use nervix_plexus::client_statement::ClientStatementGrammar;

nervix_primitives::thread_local! {
    /// The grammar this thread reads session requests with, built for the first one it reads.
    static STATEMENT_GRAMMAR: ClientStatementGrammar = ClientStatementGrammar::new();
}

/// Lends `read` the grammar this thread reads session requests with.
pub(in crate::application) fn with_statement_grammar<T>(
    read: impl FnOnce(&ClientStatementGrammar) -> T,
) -> T {
    STATEMENT_GRAMMAR.with(read)
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::ptr;

    use nervix_plexus::client_statement::parse_client_statement_sources;

    use super::*;

    /// A thread keeps the grammar it built for its first request and reads every later request
    /// with it, accepted or rejected, as a grammar built for that request would.
    #[test]
    fn a_thread_reads_every_request_with_the_grammar_it_built_for_its_first() {
        let first = with_statement_grammar(|grammar| ptr::from_ref(grammar).addr());
        for input in [
            "DESCRIBE DOMAIN;",
            "CREATE RELAY;",
            "USE analytics; LIST DOMAINS;",
            "CREATE SCHEMA broken ( value BOGUS );",
        ] {
            let held = with_statement_grammar(|grammar| {
                assert_eq!(ptr::from_ref(grammar).addr(), first, "{input}");
                grammar.parse_sources(input)
            });
            let built = parse_client_statement_sources(input);
            match (held, built) {
                (Ok(held), Ok(built)) => assert_eq!(held, built, "{input}"),
                (Err(held), Err(built)) => {
                    assert_eq!(held.current_context(), built.current_context(), "{input}");
                }
                (held, built) => {
                    panic!("{input}: the thread's grammar gave {held:?} and a built one {built:?}")
                }
            }
        }
    }
}
