//! Domain lifecycle grammar.
//!
//! Layer: language.
//!
//! - **Owns.** Parsing domain creation, starts, pauses, resumes and stops.
//! - **Depends on.** Shared Plexus tokens and validated domain vocabulary models.
//! - **Must not know.** Registry state, consensus, runtime clocks or execution tasks.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{
    CreateDomain, CreateStatement, DomainClockSkew, DomainConfig, DomainPace, DomainStartPoint,
    DomainTimeRate, EntityLabels, StartDomain, StopDomain, Timestamp,
};

use crate::{
    labels::finish_creation,
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, ParseFromSourceError, completion_context, completion_tokens,
        domain_clock_period_lit, domain_name, duration_lit, if_not_exists_clause, into_parse_error,
        kw, kw_phrase2, lex_input, string_lit, suggestions_from_errors, tok, word_raw,
    },
    wait_limit::{WAIT_LIMIT_CLAUSES, wait_limit_clause},
};

fn float_lit<'src>()
-> impl Parser<'src, &'src [Token], DomainTimeRate, extra::Err<ParseError<'src>>> + Clone {
    choice((select! { Token::NumberLiteral(v) => v }, word_raw()))
        .labelled("time_rate")
        .try_map(|raw, span| {
            raw.parse::<DomainTimeRate>()
                .map_err(|err| Rich::custom(span, err.to_string()))
        })
}

fn timestamp_lit<'src>()
-> impl Parser<'src, &'src [Token], Timestamp, extra::Err<ParseError<'src>>> + Clone {
    choice((string_lit(), word_raw()))
        .labelled("timestamp")
        .try_map(|raw, span| {
            raw.parse::<Timestamp>().map_err(|err| {
                Rich::custom(span, format!("invalid start timestamp '{raw}': {err}"))
            })
        })
}

fn clock_skew_lit<'src>()
-> impl Parser<'src, &'src [Token], DomainClockSkew, extra::Err<ParseError<'src>>> + Clone {
    duration_lit().try_map(|raw, span| {
        raw.parse::<DomainClockSkew>()
            .map_err(|error| Rich::custom(span, error.to_string()))
    })
}

pub fn create_domain_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateDomain>, extra::Err<ParseError<'src>>> + Clone
{
    let default_unpaced =
        kw(Identifier::Domain)
            .ignore_then(domain_name())
            .map(|id| CreateDomain {
                id,
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                },
                labels: EntityLabels::default(),
            });
    let paced = kw_phrase2(Identifier::Paced, Identifier::Domain)
        .ignore_then(domain_name())
        .then_ignore(kw(Identifier::With))
        .then_ignore(kw(Identifier::Period))
        .then(domain_clock_period_lit())
        .then_ignore(kw(Identifier::Skew))
        .then(clock_skew_lit())
        .map(|((id, period), skew)| CreateDomain {
            id,
            config: DomainConfig {
                pace: DomainPace::Paced { period, skew },
            },
            labels: EntityLabels::default(),
        });
    let unpaced = kw_phrase2(Identifier::Unpaced, Identifier::Domain)
        .ignore_then(domain_name())
        .map(|id| CreateDomain {
            id,
            config: DomainConfig {
                pace: DomainPace::Unpaced,
            },
            labels: EntityLabels::default(),
        });

    finish_creation(
        kw(Identifier::Create)
            .ignore_then(if_not_exists_clause())
            .then(choice((paced, unpaced, default_unpaced)))
            .map(|(if_not_exists, create)| CreateStatement::new(create, if_not_exists)),
    )
}

pub fn start_domain_parser<'src>()
-> impl Parser<'src, &'src [Token], StartDomain, extra::Err<ParseError<'src>>> + Clone {
    let time_rate = kw_phrase2(Identifier::Time, Identifier::Rate)
        .ignore_then(float_lit())
        .or_not()
        .map(|time_rate| time_rate.unwrap_or(DomainTimeRate::ONE));
    let at_start = kw(Identifier::At).ignore_then(choice((
        kw(Identifier::Now)
            .ignore_then(time_rate.clone())
            .map(|time_rate| DomainStartPoint::Now { time_rate }),
        timestamp_lit()
            .then(time_rate)
            .map(|(timestamp, time_rate)| DomainStartPoint::At {
                timestamp,
                time_rate,
            }),
    )));

    let start_point = at_start
        .or_not()
        .map(|start| start.unwrap_or(DomainStartPoint::Resume));

    kw(Identifier::Start)
        .ignore_then(start_point)
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|start| StartDomain { start })
}

pub fn stop_domain_parser<'src>()
-> impl Parser<'src, &'src [Token], StopDomain, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Stop)
        .ignore_then(wait_limit_clause().or_not())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|wait_limit| StopDomain { wait_limit })
}

/// The clauses that may still follow a complete `STOP`, as completion offers them: a `STOP`
/// without a wait limit can still take one.
pub(crate) fn stop_domain_tail(stop: &StopDomain) -> Vec<String> {
    let mut tail = vec![";".to_string()];
    if stop.wait_limit.is_none() {
        tail.extend(WAIT_LIMIT_CLAUSES.map(str::to_string));
    }
    tail
}

pub fn parse_create_domain(
    input: &str,
) -> error_stack::Result<CreateStatement<CreateDomain>, ParseFromSourceError> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    let out = create_domain_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if out.has_errors() {
        Err(into_parse_error(
            source,
            &spanned_tokens,
            input.len(),
            out.into_errors(),
        ))
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn suggest_domain_statement(input: &str, cursor: usize) -> Vec<String> {
    let (source, prefix) = completion_context(input, cursor);
    let Some(tokens) = completion_tokens(&source) else {
        return Vec::new();
    };
    let out = choice((
        create_domain_parser().to(()),
        start_domain_parser().to(()),
        stop_domain_parser().to(()),
    ))
    .then_ignore(end())
    .parse(tokens.as_slice());
    if !out.has_errors() {
        return Vec::new();
    }
    suggestions_from_errors(out.into_errors(), &prefix)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use meticulous::ResultExt as _;
    use nervix_models::{
        DomainClockPeriod, DomainClockSkew, DomainPace, DomainStartPoint, DomainTimeRate,
        Statement, Timestamp, WaitLimit,
    };

    use crate::statement::{parse_statement, suggest_statement};

    #[test]
    fn parses_create_domain() {
        let parsed = parse_statement("CREATE PACED DOMAIN prod WITH PERIOD 30s SKEW 1s;")
            .expect("must parse");
        let Statement::CreateDomain(domain) = parsed else {
            panic!("expected create domain");
        };
        assert_eq!(domain.id.as_str(), "prod");
        assert_eq!(
            domain.config.pace,
            DomainPace::Paced {
                period: "30s"
                    .parse::<DomainClockPeriod>()
                    .assured("the fixture period is a positive duration"),
                skew: "1s"
                    .parse::<DomainClockSkew>()
                    .assured("the fixture skew fits the domain clock representation"),
            }
        );
    }

    #[test]
    fn parses_create_unpaced_domain() {
        let parsed = parse_statement("CREATE UNPACED DOMAIN prod;").expect("must parse");
        let Statement::CreateDomain(domain) = parsed else {
            panic!("expected create domain");
        };
        assert_eq!(domain.config.pace, DomainPace::Unpaced);
    }

    #[test]
    fn parses_create_domain_as_default_unpaced() {
        let parsed = parse_statement("CREATE DOMAIN prod;").expect("must parse");
        let Statement::CreateDomain(domain) = parsed else {
            panic!("expected create domain");
        };
        assert_eq!(domain.id.as_str(), "prod");
        assert_eq!(domain.config.pace, DomainPace::Unpaced);
    }

    #[test]
    fn parses_create_domain_without_trailing_semicolon() {
        let parsed = parse_statement("CREATE DOMAIN prod").expect("must parse");
        let Statement::CreateDomain(domain) = parsed else {
            panic!("expected create domain");
        };
        assert_eq!(domain.id.as_str(), "prod");
    }

    #[test]
    fn complete_domain_forms_offer_labels_and_statement_termination() {
        for input in [
            "CREATE DOMAIN prod ",
            "CREATE UNPACED DOMAIN prod ",
            "CREATE PACED DOMAIN prod WITH PERIOD 30s SKEW 1s ",
        ] {
            assert_eq!(suggest_statement(input, input.len()), [";", "LABELS"]);
            assert_eq!(
                crate::client_statement::suggest_client_statement(input, input.len()),
                [";", "LABELS"]
            );

            let labeled = format!("{input}LABELS {{ 'team' = 'Orders' }} ");
            assert_eq!(suggest_statement(&labeled, labeled.len()), [";"]);
            assert_eq!(
                crate::client_statement::suggest_client_statement(&labeled, labeled.len()),
                [";"]
            );
        }
    }

    #[test]
    fn parses_create_domain_with_if_not_exists() {
        let parsed = parse_statement("CREATE IF NOT EXISTS DOMAIN prod;").expect("must parse");
        let Statement::CreateDomain(domain) = parsed else {
            panic!("expected create domain");
        };
        assert!(domain.if_not_exists);
        assert_eq!(domain.id.as_str(), "prod");
        assert_eq!(domain.config.pace, DomainPace::Unpaced);
    }

    #[test]
    fn rejects_unpaced_domain_with_tick_config() {
        assert!(
            parse_statement("CREATE UNPACED DOMAIN prod WITH PERIOD 30s SKEW 1s;").is_err(),
            "unpaced domain must reject paced-only tick config"
        );
    }

    #[test]
    fn rejects_default_domain_with_tick_config() {
        assert!(
            parse_statement("CREATE DOMAIN prod WITH PERIOD 30s SKEW 1s;").is_err(),
            "default domain form must reject paced-only tick config"
        );
    }

    #[test]
    fn rejects_invalid_if_exists_domain_clause() {
        assert!(
            parse_statement("CREATE IF EXISTS DOMAIN prod;").is_err(),
            "CREATE DOMAIN only supports IF NOT EXISTS"
        );
    }

    #[test]
    fn parses_start_with_timestamp_and_rate() {
        let parsed =
            parse_statement("START AT '2026-04-06T12:00:00Z' TIME RATE 4.0;").expect("must parse");
        let Statement::StartDomain(command) = parsed else {
            panic!("expected start domain");
        };
        assert_eq!(
            command.start,
            DomainStartPoint::At {
                timestamp: "2026-04-06T12:00:00Z"
                    .parse::<Timestamp>()
                    .expect("fixture timestamp is representable"),
                time_rate: DomainTimeRate::try_from(4.0)
                    .expect("fixture rate is positive and finite")
            }
        );
    }

    #[test]
    fn parses_start_without_arguments_as_resume() {
        let parsed = parse_statement("START;").expect("must parse");
        let Statement::StartDomain(command) = parsed else {
            panic!("expected start domain");
        };
        assert_eq!(command.start, DomainStartPoint::Resume);
    }

    #[test]
    fn parses_start_without_trailing_semicolon() {
        let parsed = parse_statement("START").expect("must parse");
        let Statement::StartDomain(command) = parsed else {
            panic!("expected start domain");
        };
        assert_eq!(command.start, DomainStartPoint::Resume);
    }

    #[test]
    fn parses_stop_without_trailing_semicolon() {
        let parsed = parse_statement("STOP").expect("must parse");
        let Statement::StopDomain(stop) = parsed else {
            panic!("expected stop domain");
        };
        assert_eq!(stop.wait_limit, None);
    }

    #[rstest::rstest]
    #[case::enforce("STOP ENFORCE AFTER 30s;", WaitLimit::Enforce { after: Duration::from_secs(30) })]
    #[case::cancel("STOP CANCEL AFTER 2m;", WaitLimit::Cancel { after: Duration::from_secs(120) })]
    #[case::enforce_at_once("STOP ENFORCE AFTER 0s;", WaitLimit::Enforce { after: Duration::ZERO })]
    #[case::lower_case("stop cancel after 1500ms", WaitLimit::Cancel { after: Duration::from_millis(1500) })]
    #[case::spanned_duration(
        "STOP ENFORCE AFTER 1 min 30s;",
        WaitLimit::Enforce { after: Duration::from_secs(90) }
    )]
    fn parses_stop_with_a_wait_limit(#[case] source: &str, #[case] expected: WaitLimit) {
        let parsed = parse_statement(source).expect("must parse");
        let Statement::StopDomain(stop) = parsed else {
            panic!("expected stop domain");
        };
        assert_eq!(stop.wait_limit, Some(expected));
        let canonical = Statement::StopDomain(stop.clone())
            .to_canonical_plexus()
            .expect("a parsed stop renders");
        let Statement::StopDomain(reparsed) =
            parse_statement(&canonical).expect("the canonical stop must parse")
        else {
            panic!("expected stop domain");
        };
        assert_eq!(reparsed, stop);
    }

    #[rstest::rstest]
    #[case::both_clauses("STOP ENFORCE AFTER 1s CANCEL AFTER 2s;")]
    #[case::both_clauses_reversed("STOP CANCEL AFTER 2s ENFORCE AFTER 1s;")]
    #[case::enforce_without_after("STOP ENFORCE 1s;")]
    #[case::cancel_without_after("STOP CANCEL 1s;")]
    #[case::missing_duration("STOP ENFORCE AFTER;")]
    #[case::bare_word_duration("STOP CANCEL AFTER soon;")]
    #[case::negative_duration("STOP ENFORCE AFTER -1s;")]
    #[case::repeated_clause("STOP ENFORCE AFTER 1s ENFORCE AFTER 2s;")]
    #[case::clause_before_stop("ENFORCE AFTER 1s STOP;")]
    fn rejects_malformed_stop_wait_limits(#[case] source: &str) {
        assert!(
            parse_statement(source).is_err(),
            "{source} must not parse as a stop"
        );
    }

    #[test]
    fn suggests_wait_limits_after_stop() {
        for input in ["STOP ", "stop "] {
            assert_eq!(
                suggest_statement(input, input.len()),
                [";", "CANCEL AFTER", "ENFORCE AFTER"]
            );
            assert_eq!(
                crate::client_statement::suggest_client_statement(input, input.len()),
                [";", "CANCEL AFTER", "ENFORCE AFTER"]
            );
        }
    }

    #[test]
    fn a_stop_with_a_wait_limit_offers_only_its_termination() {
        for input in ["STOP ENFORCE AFTER 30s ", "STOP CANCEL AFTER 0s "] {
            assert_eq!(suggest_statement(input, input.len()), [";"]);
            assert_eq!(
                crate::client_statement::suggest_client_statement(input, input.len()),
                [";"]
            );
        }
    }

    #[test]
    fn completes_a_wait_limit_phrase_from_its_prefix() {
        assert_eq!(suggest_statement("STOP EN", 7), ["ENFORCE AFTER"]);
        assert_eq!(suggest_statement("STOP CAN", 8), ["CANCEL AFTER"]);
    }

    #[test]
    fn a_wait_limit_belongs_to_stop_and_leaks_into_no_other_lifecycle_statement() {
        assert!(!suggest_statement("START ", 6).contains(&"ENFORCE AFTER".to_string()));
        assert!(!suggest_statement("START ", 6).contains(&"CANCEL AFTER".to_string()));
        assert!(
            !suggest_statement("CREATE DOMAIN prod ", 19).contains(&"ENFORCE AFTER".to_string())
        );
    }

    #[test]
    fn parses_start_at_now_with_default_time_rate() {
        let parsed = parse_statement("START AT NOW;").expect("must parse");
        let Statement::StartDomain(command) = parsed else {
            panic!("expected start domain");
        };
        assert_eq!(
            command.start,
            DomainStartPoint::Now {
                time_rate: DomainTimeRate::ONE
            }
        );
    }

    #[test]
    fn rejects_start_timestamps_outside_signed_unix_nanoseconds() {
        for timestamp in [
            "1677-09-21T00:12:43.145224191Z",
            "2262-04-11T23:47:16.854775808Z",
        ] {
            let error = parse_statement(&format!("START AT '{timestamp}' TIME RATE 1.0;"))
                .expect_err("timestamp just outside the supported range must fail");
            assert!(
                error
                    .to_string()
                    .contains("outside the signed Unix-nanosecond range"),
                "unexpected diagnostic: {error}"
            );
        }
    }

    #[test]
    fn rejects_nonpositive_and_nonfinite_start_rates() {
        for rate in ["0", "-1", "NaN", "inf"] {
            assert!(
                parse_statement(&format!("START AT NOW TIME RATE {rate};")).is_err(),
                "rate {rate} must fail"
            );
        }
    }

    #[test]
    fn parses_tiny_and_large_finite_start_rates() {
        for rate in ["5e-324", "1.7976931348623157e308"] {
            let parsed = parse_statement(&format!("START AT NOW TIME RATE {rate};"))
                .expect("every positive finite f64 rate is supported");
            let canonical = parsed
                .to_canonical_plexus()
                .expect("a validated start command must render");
            assert_eq!(
                parse_statement(&canonical).expect("canonical start command must parse"),
                parsed
            );
            let Statement::StartDomain(command) = parsed else {
                panic!("expected start domain");
            };
            let DomainStartPoint::Now { time_rate } = command.start else {
                panic!("expected a NOW start point");
            };
            assert!(time_rate.get().is_finite());
            assert!(time_rate.get() > 0.0);
        }
    }

    #[test]
    fn rejects_start_now_with_time_rate() {
        let err = parse_statement("START NOW TIME RATE 4.0;")
            .expect_err("must reject time rate after NOW");
        let crate::parser_support::ParseFromSourceError::Parse { diagnostics, .. } =
            err.current_context()
        else {
            panic!("expected parse diagnostics");
        };
        assert!(!diagnostics.is_empty(), "must surface a parse diagnostic");
    }

    #[test]
    fn suggests_domain_heads_after_create() {
        let suggestions = suggest_statement("CREATE ", 7);
        assert!(suggestions.contains(&"DOMAIN".to_string()));
        assert!(suggestions.contains(&"PACED DOMAIN".to_string()));
        assert!(suggestions.contains(&"UNPACED DOMAIN".to_string()));
    }

    #[test]
    fn suggests_domain_without_cross_branch_leakage() {
        let suggestions = suggest_statement("CREATE DO", 9);
        assert!(suggestions.contains(&"DOMAIN".to_string()));
        assert!(!suggestions.contains(&"DROP".to_string()));
    }

    #[test]
    fn suggests_unpaced_without_cross_branch_leakage() {
        let suggestions = suggest_statement("CREATE UNP", 10);
        assert!(suggestions.contains(&"UNPACED DOMAIN".to_string()));
        assert!(!suggestions.contains(&"JUNCTION".to_string()));
    }

    #[test]
    fn suggests_domain_keyword_after_create_pace() {
        assert!(suggest_statement("CREATE PACED ", 13).contains(&"DOMAIN".to_string()));
    }

    #[test]
    fn rejects_use_domain_statement() {
        assert!(
            parse_statement("USE prod;").is_err(),
            "USE is a repl-only command"
        );
    }

    #[test]
    fn suggests_at_or_semicolon_after_start() {
        let suggestions = suggest_statement("START ", 6);
        assert!(suggestions.contains(&"AT".to_string()));
        assert!(suggestions.contains(&";".to_string()));
    }

    #[test]
    fn suggests_time_rate_phrase_after_start_at_timestamp() {
        assert!(
            suggest_statement("START AT '2026-04-06T12:00:00Z' ", 33)
                .contains(&"TIME RATE".to_string())
        );
    }
}
