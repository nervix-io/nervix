//! Shared lexical clauses of emitter publication policies.
//!
//! Layer: language.
//! - **Owns.** NO ACK, acknowledged windows and request policies as reusable grammar units.
//! - **Depends on.** Shared language tokens and acknowledgement/retry vocabulary.
//! - **Must not know.** A connector's allowed policy set, clients or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) struct AcknowledgedPolicy {
    pub window: AckWindow,
    pub ack_timeout: String,
    pub retry_policy: RetryPolicy,
}

pub(super) fn unconfirmed_policy<'src>()
-> impl Parser<'src, &'src [Token], RetryPolicy, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::No, Identifier::Ack)
        .ignore_then(retry_policy())
        .boxed()
}

pub(super) fn acknowledged_policy<'src>()
-> impl Parser<'src, &'src [Token], AcknowledgedPolicy, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Ack)
        .ignore_then(acknowledged_policy_body())
        .boxed()
}

pub(super) fn acknowledged_policy_body<'src>()
-> impl Parser<'src, &'src [Token], AcknowledgedPolicy, extra::Err<ParseError<'src>>> + Clone {
    ack_window()
        .then(ack_timeout())
        .then(retry_policy())
        .map(|((window, ack_timeout), retry_policy)| AcknowledgedPolicy {
            window,
            ack_timeout,
            retry_policy,
        })
        .boxed()
}

pub(super) fn request_policy<'src>()
-> impl Parser<'src, &'src [Token], RetryPolicy, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Ack).ignore_then(retry_policy()).boxed()
}
