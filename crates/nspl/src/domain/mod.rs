use chumsky::prelude::*;
use nervix_models::{
    AlterDomain, CreateDomain, CreateStatement, DomainConfig, DomainPace, DomainStartPoint,
    PlacementPolicy, StartDomain, StopDomain,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, completion_context, domain_name, duration_lit,
        if_not_exists_clause, into_parse_error, kw, kw_phrase2, lex_input, string_lit,
        suggestions_from_errors, tok, word_raw,
    },
};

fn float_lit<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    choice((select! { Token::NumberLiteral(v) => v }, word_raw()))
        .try_map(|raw, span| {
            raw.parse::<f64>()
                .map(|_| raw.clone())
                .map_err(|err| Rich::custom(span, format!("invalid time rate '{raw}': {err}")))
        })
        .labelled("time_rate")
}

fn timestamp_lit<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    choice((string_lit(), word_raw())).labelled("timestamp")
}

pub fn create_domain_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateDomain>, extra::Err<ParseError<'src>>> + Clone
{
    let placement = kw(Identifier::Placement)
        .ignore_then(crate::placement::placement_policy_parser())
        .or_not()
        .map(|policy| policy.unwrap_or(PlacementPolicy::Neutral));
    let default_unpaced = kw(Identifier::Domain)
        .ignore_then(domain_name())
        .then(placement.clone())
        .map(|(id, placement)| CreateDomain {
            id,
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                period: "0ms".to_string(),
                skew: "0ms".to_string(),
                placement,
            },
        });
    let paced = kw(Identifier::Paced)
        .ignore_then(kw(Identifier::Domain))
        .ignore_then(domain_name())
        .then_ignore(kw(Identifier::With))
        .then_ignore(kw(Identifier::Period))
        .then(duration_lit())
        .then_ignore(kw(Identifier::Skew))
        .then(duration_lit())
        .then(placement.clone())
        .map(|(((id, period), skew), placement)| CreateDomain {
            id,
            config: DomainConfig {
                pace: DomainPace::Paced,
                period,
                skew,
                placement,
            },
        });
    let unpaced = kw(Identifier::Unpaced)
        .ignore_then(kw(Identifier::Domain))
        .ignore_then(domain_name())
        .then(placement)
        .map(|(id, placement)| CreateDomain {
            id,
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                period: "0ms".to_string(),
                skew: "0ms".to_string(),
                placement,
            },
        });

    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then(choice((paced, unpaced, default_unpaced)))
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|(if_not_exists, create)| CreateStatement::new(create, if_not_exists))
}

pub fn alter_domain_parser<'src>()
-> impl Parser<'src, &'src [Token], AlterDomain, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Alter)
        .ignore_then(kw(Identifier::Domain))
        .ignore_then(kw(Identifier::Set))
        .ignore_then(kw(Identifier::Placement))
        .ignore_then(crate::placement::placement_policy_parser())
        .map(|policy| AlterDomain { policy })
        .then_ignore(tok(Token::Semicolon).or_not())
}

pub fn start_domain_parser<'src>()
-> impl Parser<'src, &'src [Token], StartDomain, extra::Err<ParseError<'src>>> + Clone {
    let time_rate = kw_phrase2(Identifier::Time, Identifier::Rate)
        .ignore_then(float_lit())
        .or_not()
        .map(|time_rate| time_rate.unwrap_or_else(|| "1.0".to_string()));
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
        .then_ignore(tok(Token::Semicolon).or_not())
        .to(StopDomain)
}

pub fn parse_create_domain(
    input: &str,
) -> Result<CreateStatement<CreateDomain>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
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
            .expect("successful parse must have output"))
    }
}

pub fn suggest_domain_statement(input: &str, cursor: usize) -> Vec<String> {
    let (source, prefix) = completion_context(input, cursor);
    let (_, _, tokens) = match lex_input(&source) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let out = choice((
        create_domain_parser().to(()),
        alter_domain_parser().to(()),
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
    use nervix_models::{DomainPace, DomainStartPoint, PlacementPolicy, Statement};

    use crate::statement::{parse_statement, suggest_statement};

    #[test]
    fn parses_create_domain() {
        let parsed = parse_statement("CREATE PACED DOMAIN prod WITH PERIOD 30s SKEW 1s;")
            .expect("must parse");
        let Statement::CreateDomain(domain) = parsed else {
            panic!("expected create domain");
        };
        assert_eq!(domain.id.as_str(), "prod");
        assert_eq!(domain.config.pace, DomainPace::Paced);
        assert_eq!(domain.config.period, "30s");
        assert_eq!(domain.config.skew, "1s");
        assert_eq!(domain.config.placement, PlacementPolicy::Neutral);
    }

    #[test]
    fn parses_domain_placement_on_every_create_form() {
        for (source, expected) in [
            (
                "CREATE DOMAIN plain PLACEMENT PREFER COLOCATION;",
                PlacementPolicy::PreferColocation,
            ),
            (
                "CREATE UNPACED DOMAIN unpaced PLACEMENT SUGGEST SEPARATION;",
                PlacementPolicy::SuggestSeparation,
            ),
            (
                "CREATE PACED DOMAIN paced WITH PERIOD 30s SKEW 1s PLACEMENT REQUIRE COLOCATION;",
                PlacementPolicy::RequireColocation,
            ),
        ] {
            let Statement::CreateDomain(domain) =
                parse_statement(source).expect("domain placement must parse")
            else {
                panic!("expected create domain");
            };
            assert_eq!(domain.config.placement, expected);
        }
    }

    #[test]
    fn parses_nameless_alter_domain_placement() {
        let Statement::AlterDomain(alter) =
            parse_statement("ALTER DOMAIN SET PLACEMENT NEUTRAL;").expect("must parse")
        else {
            panic!("expected alter domain");
        };
        assert_eq!(alter.policy, PlacementPolicy::Neutral);
        parse_statement("ALTER DOMAIN prod SET PLACEMENT NEUTRAL;")
            .expect_err("ALTER DOMAIN is nameless");
    }

    #[test]
    fn domain_placement_completion_is_policy_scoped() {
        let input = "ALTER DOMAIN SET PLACEMENT ";
        let suggestions = suggest_statement(input, input.len());
        for expected in [
            "REQUIRE COLOCATION",
            "PREFER COLOCATION",
            "NEUTRAL",
            "SUGGEST SEPARATION",
        ] {
            assert!(suggestions.contains(&expected.to_string()));
        }
        assert!(!suggestions.contains(&"REQUIRE SEPARATION".to_string()));
        assert!(!suggestions.contains(&"DOMAIN".to_string()));

        let input = "CREATE DOMAIN prod ";
        let suggestions = suggest_statement(input, input.len());
        assert!(suggestions.contains(&"PLACEMENT".to_string()));
        assert!(!suggestions.contains(&"RANK".to_string()));
    }

    #[test]
    fn parses_create_unpaced_domain() {
        let parsed = parse_statement("CREATE UNPACED DOMAIN prod;").expect("must parse");
        let Statement::CreateDomain(domain) = parsed else {
            panic!("expected create domain");
        };
        assert_eq!(domain.config.pace, DomainPace::Unpaced);
        assert_eq!(domain.config.period, "0ms");
        assert_eq!(domain.config.skew, "0ms");
    }

    #[test]
    fn parses_create_domain_as_default_unpaced() {
        let parsed = parse_statement("CREATE DOMAIN prod;").expect("must parse");
        let Statement::CreateDomain(domain) = parsed else {
            panic!("expected create domain");
        };
        assert_eq!(domain.id.as_str(), "prod");
        assert_eq!(domain.config.pace, DomainPace::Unpaced);
        assert_eq!(domain.config.period, "0ms");
        assert_eq!(domain.config.skew, "0ms");
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
        parse_statement("CREATE UNPACED DOMAIN prod WITH PERIOD 30s SKEW 1s;")
            .expect_err("unpaced domain must reject paced-only tick config");
    }

    #[test]
    fn rejects_default_domain_with_tick_config() {
        parse_statement("CREATE DOMAIN prod WITH PERIOD 30s SKEW 1s;")
            .expect_err("default domain form must reject paced-only tick config");
    }

    #[test]
    fn rejects_invalid_if_exists_domain_clause() {
        parse_statement("CREATE IF EXISTS DOMAIN prod;")
            .expect_err("CREATE DOMAIN only supports IF NOT EXISTS");
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
                timestamp: "2026-04-06T12:00:00Z".to_string(),
                time_rate: "4.0".to_string()
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
        let Statement::StopDomain(_) = parsed else {
            panic!("expected stop domain");
        };
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
                time_rate: "1.0".to_string()
            }
        );
    }

    #[test]
    fn rejects_start_now_with_time_rate() {
        let err = parse_statement("START NOW TIME RATE 4.0;")
            .expect_err("must reject time rate after NOW");
        let crate::parser_support::ParseFromSourceError::Parse { diagnostics, .. } = err else {
            panic!("expected parse diagnostics");
        };
        assert!(!diagnostics.is_empty(), "must surface a parse diagnostic");
    }

    #[test]
    fn suggests_pace_keywords_after_create() {
        let suggestions = suggest_statement("CREATE ", 7);
        assert!(suggestions.contains(&"DOMAIN".to_string()));
        assert!(suggestions.contains(&"PACED".to_string()));
        assert!(suggestions.contains(&"UNPACED".to_string()));
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
        assert!(suggestions.contains(&"UNPACED".to_string()));
        assert!(!suggestions.contains(&"JUNCTION".to_string()));
    }

    #[test]
    fn suggests_domain_keyword_after_create_pace() {
        assert!(suggest_statement("CREATE PACED ", 13).contains(&"DOMAIN".to_string()));
    }

    #[test]
    fn rejects_use_domain_statement() {
        parse_statement("USE prod;").expect_err("USE is a repl-only command");
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
