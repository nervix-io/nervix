//! SENTRY emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of SENTRY emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn mode<'src>()
-> impl Parser<'src, &'src [Token], SentryEventPublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    request_mode!(SentryEventPublishingMode)
}

pub(super) fn event_body<'src>(
    context: EmitterContext,
) -> impl Parser<
    'src,
    &'src [Token],
    ParsedDestination<SentryEventEmitter>,
    extra::Err<ParseError<'src>>,
> + Clone {
    encoded_destination(
        context,
        client_ref()
            .then(kw(Identifier::Platform).ignore_then(string_lit()).or_not())
            .then_ignore(kw(Identifier::Mode))
            .then(mode())
            .map(|((client, platform), mode)| SentryEventEmitter {
                client,
                mode,
                platform,
            }),
    )
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "qualifies Sentry's composed language contract")
)]
mod tests {
    use super::*;
    use crate::conformance_support::{assert_offers_exactly, created_model, refused};

    fn statement(option: &str) -> String {
        format!(
            "CREATE SENTRY EVENT EMITTER outgoing FROM errors TO client {option} MODE ACK RETRY \
             POLICY BACKOFF 100ms MAX 1s ENCODE USING event_codec INHERIT ALL FLUSH IMMEDIATE ON \
             MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
    }

    #[test]
    fn platform_preserves_its_spelling_and_whether_the_clause_was_written() {
        for (option, expected) in [
            ("", None),
            ("PLATFORM 'python'", Some("python")),
            ("pLaTfOrM 'JVM.東京'", Some("JVM.東京")),
            ("PLATFORM ''", Some("")),
        ] {
            let Some(Model::Emitter(emitter)) = created_model(&statement(option)) else {
                panic!("the declared Sentry platform must parse: {option}");
            };
            let EmitterEntity::Sentry(SentryEmitter::Event(event)) = emitter.connector.as_ref()
            else {
                panic!("the Sentry statement constructs a Sentry event emitter");
            };
            assert_eq!(event.platform.as_deref(), expected);
            let canonical = emitter.to_canonical_plexus();
            let Ok(canonical) = canonical else {
                panic!("the current event Model must render canonically");
            };
            assert_eq!(created_model(&canonical), Some(Model::Emitter(emitter)));
        }
    }

    #[test]
    fn platform_takes_one_string_literal_before_the_mode() {
        for option in [
            "PLATFORM python",
            "PLATFORM",
            "PLATFORM 'python' PLATFORM 'rust'",
        ] {
            assert!(refused(&statement(option)), "{option}");
        }
        let misplaced = statement("").replace("ENCODE USING", "PLATFORM 'python' ENCODE USING");
        assert!(refused(&misplaced));
    }

    #[test]
    fn platform_completion_follows_the_selected_entity_and_clause_order() {
        let reference = "CREATE SENTRY EVENT EMITTER outgoing FROM errors TO client ";
        assert_offers_exactly(reference, &["PLATFORM", "MODE"]);
        assert_offers_exactly(&format!("{reference}PLATFORM "), &["string_literal"]);
        assert_offers_exactly(&format!("{reference}PLATFORM 'python' "), &["MODE"]);
        assert_offers_exactly(
            &format!("{reference}PLATFORM 'python' MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s "),
            &["ENCODE USING"],
        );
    }
}
