//! ZEROMQ emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of ZEROMQ emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], ZeroMqPushPublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    unconfirmed_mode!(ZeroMqPushPublishingMode)
}

pub(super) fn push_body<'src>(
    context: EmitterContext,
) -> impl Parser<'src, &'src [Token], ParsedDestination<ZeroMqPushEmitter>, extra::Err<ParseError<'src>>>
+ Clone {
    encoded_destination(
        context,
        client_ref()
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .map(|(client, mode)| ZeroMqPushEmitter { client, mode }),
    )
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns ZeroMQ grammar conformance evidence")
)]
mod tests {
    //! Layer: test harness.
    //! - **Owns.** ZeroMQ emitter round trips, refusals and composed completion checks.
    //! - **Depends on.** The public grammar and canonical vocabulary rendering.
    //! - **Must not know.** Connector drivers or graph execution.

    use meticulous::ResultExt as _;

    use crate::{conformance_support::assert_offers_exactly, emitter::parse_create_emitter};

    #[test]
    fn zeromq_push_round_trips_optional_batching() {
        for batch in ["", "BATCH MAX MESSAGES 65536 MAX SIZE 1MiB"] {
            let text = format!(
                "CREATE ZEROMQ PUSH EMITTER events FROM input TO socket MODE NO ACK RETRY POLICY \
                 BACKOFF 1s MAX 30s ENCODE USING codec INHERIT ALL {batch} FLUSH IMMEDIATE ON \
                 MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
            );
            let parsed = parse_create_emitter(&text).assured("optional ZeroMQ batching parses");
            let canonical = parsed
                .to_canonical_plexus()
                .assured("a parsed ZeroMQ emitter has a canonical form");
            assert_eq!(
                parse_create_emitter(&canonical).assured("the canonical form parses"),
                parsed
            );
        }
    }

    #[test]
    fn zeromq_push_completion_stays_with_unacknowledged_publishing() {
        assert_offers_exactly(
            "CREATE ZEROMQ PUSH EMITTER events FROM input TO socket MODE ",
            &["NO ACK RETRY POLICY BACKOFF 1s MAX 30s"],
        );
    }

    #[test]
    fn zeromq_push_refuses_acknowledged_publishing() {
        let text = "CREATE ZEROMQ PUSH EMITTER events FROM input TO socket MODE ACK SEQUENTIAL \
                    ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING codec INHERIT ALL \
                    FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;";
        assert!(parse_create_emitter(text).is_err());
    }
}
