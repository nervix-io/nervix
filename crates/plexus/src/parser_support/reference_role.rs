//! The role a codec or relay reference plays where a statement names it.
//!
//! Layer: language.
//! - **Owns.** The completion labels that name a codec reference's direction and a relay
//!   reference's role at its grammar position, the reference parsers that carry them, and the
//!   reference kind every diagnostic keeps naming for them.
//! - **Depends on.** Shared language tokens, the shared name parsers and the vocabulary's codec
//!   directions.
//! - **Must not know.** Which Models a domain configures or how completion answers a role.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the reference role labels declared above")
)]

use chumsky::prelude::*;
use nervix_models::{CodecDirection, CodecName, ModelKind, RelayName};

use super::{ParseError, parse_name, parse_name_excluding_reserved};
use crate::lexer::{Identifier, Token};

/// The part a relay reference plays where a statement names it. Its completion label states the
/// part, so completion can ask which relays agree with the ones the statement already names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelayRole {
    /// An input a processor or an emitter reads, in a `FROM` list or a correlator side's.
    Input,
    /// A relay whose materialized state a processor or an emitter reads.
    State,
    /// The relay a route of a processor that keeps its input's branch writes to.
    Output,
    /// The relay message errors are sent to.
    Error,
    /// The relay a correlator sends records of its left side to when they time out.
    LeftTimeout,
    /// The relay a correlator sends records of its right side to when they time out.
    RightTimeout,
}

impl RelayRole {
    const ALL: [Self; 6] = [
        Self::Input,
        Self::State,
        Self::Output,
        Self::Error,
        Self::LeftTimeout,
        Self::RightTimeout,
    ];

    /// The completion label of a relay reference in this role.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Input => "ref:input_relay",
            Self::State => "ref:state_relay",
            Self::Output => "ref:output_relay",
            Self::Error => "ref:error_relay",
            Self::LeftTimeout => "ref:left_timeout_relay",
            Self::RightTimeout => "ref:right_timeout_relay",
        }
    }

    /// The role `label` names, if it is a relay role label.
    pub(crate) fn of_label(label: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|role| role.label() == label)
    }
}

/// The completion label of a codec reference that a clause uses in `direction`.
pub(crate) const fn codec_label(direction: CodecDirection) -> &'static str {
    match direction {
        CodecDirection::Decoding => "ref:decoding_codec",
        CodecDirection::Encoding => "ref:encoding_codec",
    }
}

/// The direction `label` names, if it is a codec direction label.
pub(crate) fn codec_direction(label: &str) -> Option<CodecDirection> {
    [CodecDirection::Decoding, CodecDirection::Encoding]
        .into_iter()
        .find(|direction| codec_label(*direction) == label)
}

/// A relay named in `role`, under the words every relay reference reserves.
pub(crate) fn relay_in_role<'src>(
    role: RelayRole,
) -> impl Parser<'src, &'src [Token], RelayName, extra::Err<ParseError<'src>>> + Clone {
    parse_name_excluding_reserved(
        role.label(),
        &[Identifier::Message, Identifier::Branch],
        RelayName::parse,
    )
}

/// A codec named by a clause that uses it in `direction`.
pub(crate) fn codec_for<'src>(
    direction: CodecDirection,
) -> impl Parser<'src, &'src [Token], CodecName, extra::Err<ParseError<'src>>> + Clone {
    parse_name(codec_label(direction), CodecName::parse)
}

/// What a diagnostic calls the reference a role label expects: the reference's kind, as at every
/// other position of that kind. A role narrows completion and leaves what an error says unchanged.
pub(super) fn diagnostic_label(label: &str) -> Option<&'static str> {
    if RelayRole::of_label(label).is_some() {
        return Some(ModelKind::Relay.completion_label());
    }
    if codec_direction(label).is_some() {
        return Some(ModelKind::Codec.completion_label());
    }
    None
}

#[cfg(test)]
mod tests {
    use meticulous::OptionExt as _;
    use nervix_models::CodecDirection;

    use super::{RelayRole, codec_direction, codec_label, diagnostic_label};
    use crate::{client_statement::suggest_client_statement, statement::parse_statement};

    const CORRELATOR: &str = "CREATE CORRELATOR pairs LEFT FROM readings RIGHT FROM labels \
                              CORRELATE WHERE left.id = right.id MATCH EARLIEST MAX TIME 5s ON \
                              CORRELATION TIMEOUT";

    #[test]
    fn every_role_label_names_its_role_and_no_kind_label_names_one() {
        for role in RelayRole::ALL {
            assert_eq!(RelayRole::of_label(role.label()), Some(role));
            assert_eq!(diagnostic_label(role.label()), Some("ref:relay"));
        }
        for direction in [CodecDirection::Decoding, CodecDirection::Encoding] {
            assert_eq!(codec_direction(codec_label(direction)), Some(direction));
            assert_eq!(diagnostic_label(codec_label(direction)), Some("ref:codec"));
        }
        for label in ["ref:relay", "ref:codec", "ref:schema", "relay_name"] {
            assert_eq!(RelayRole::of_label(label), None, "{label}");
            assert_eq!(codec_direction(label), None, "{label}");
            assert_eq!(diagnostic_label(label), None, "{label}");
        }
    }

    #[test]
    fn a_statement_diagnostic_names_each_label_by_what_is_written_there() {
        let cases = [
            ("ref:expression_function", "function_name"),
            ("ref:expression_invocation", "function_name"),
            ("ref:expression_field", "field_name"),
            ("ref:expression_target", "field_name"),
            ("ref:ingestor_timestamp_metadata", "field_name"),
            ("ref:expression_udf", "udf_name"),
            ("ref:input_relay", "ref:relay"),
            ("ref:right_timeout_relay", "ref:relay"),
            ("ref:encoding_codec", "ref:codec"),
            ("ref:schema", "ref:schema"),
            ("relay_name", "relay_name"),
        ];
        for (label, named) in cases {
            assert_eq!(
                crate::parser_support::diagnostic_label(label),
                named,
                "{label}"
            );
        }
    }

    #[test]
    fn the_grammar_labels_each_position_with_its_role() {
        let cases = [
            ("CREATE JUNCTION copy FROM ", "ref:input_relay"),
            ("CREATE JUNCTION copy FROM readings, ", "ref:input_relay"),
            (
                "CREATE CORRELATOR pairs LEFT FROM readings RIGHT FROM ",
                "ref:input_relay",
            ),
            ("CREATE KAFKA TOPIC EMITTER sink FROM ", "ref:input_relay"),
            (
                "CREATE JUNCTION copy FROM readings UNBRANCHED USING MATERIALIZED STATE ",
                "ref:state_relay",
            ),
            (
                "CREATE KAFKA TOPIC EMITTER sink FROM readings USING MATERIALIZED STATE ",
                "ref:state_relay",
            ),
            (
                "CREATE JUNCTION copy FROM readings UNBRANCHED TO ",
                "ref:output_relay",
            ),
            (
                "CREATE JUNCTION copy FROM readings UNBRANCHED TO more INHERIT ALL FLUSH \
                 IMMEDIATE ON MESSAGE ERROR SEND TO ",
                "ref:error_relay",
            ),
            (&format!("{CORRELATOR} SEND TO "), "ref:left_timeout_relay"),
            (
                &format!("{CORRELATOR} DROP, SEND TO "),
                "ref:right_timeout_relay",
            ),
            (
                "ALTER HTTP ENDPOINT INGESTOR source SET DECODE USING ",
                "ref:decoding_codec",
            ),
            (
                "ALTER KAFKA TOPIC EMITTER sink SET ENCODE USING ",
                "ref:encoding_codec",
            ),
            ("CREATE REINGESTOR again FROM readings TO ", "ref:relay"),
            ("DROP RELAY ", "ref:relay"),
            ("DROP CODEC ", "ref:codec"),
        ];
        for (input, label) in cases {
            let labels = suggest_client_statement(input, input.len());
            assert!(labels.contains(&label.to_string()), "{input}: {labels:?}");
        }
    }

    #[test]
    fn a_diagnostic_names_a_role_position_by_its_reference_kind() {
        let too_long = "n".repeat(300);
        for (statement, expected) in [
            (
                "CREATE JUNCTION copy FROM ;".to_string(),
                "expected ref:relay",
            ),
            (
                format!("CREATE JUNCTION copy FROM {too_long};"),
                "expected ref:relay",
            ),
            (
                "CREATE KAFKA TOPIC EMITTER sink FROM readings USING MATERIALIZED STATE ;"
                    .to_string(),
                "expected ref:relay",
            ),
            (
                "ALTER HTTP ENDPOINT INGESTOR source SET DECODE USING ;".to_string(),
                "expected ref:codec",
            ),
            (
                "ALTER KAFKA TOPIC EMITTER sink SET ENCODE USING ;".to_string(),
                "expected ref:codec",
            ),
        ] {
            let rejection = parse_statement(&statement)
                .err()
                .verified("the statement misses or misnames its reference");
            let mut messages = Vec::new();
            for diagnostic in rejection.current_context().diagnostics() {
                messages.push(diagnostic.message.as_str());
            }
            let rendered = messages.join("; ");
            assert!(rendered.contains(expected), "{statement}: {rendered}");
            for role in RelayRole::ALL {
                assert!(!rendered.contains(role.label()), "{statement}: {rendered}");
            }
            for direction in [CodecDirection::Decoding, CodecDirection::Encoding] {
                assert!(
                    !rendered.contains(codec_label(direction)),
                    "{statement}: {rendered}"
                );
            }
        }
    }
}
