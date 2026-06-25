use chumsky::prelude::*;
use nervix_models::{
    AlterRelay, AlterRelayOperation, CreateRelay, CreateStatement, MaterializedRelayState,
    RelayParameterization, RelayParameters, default_relay_buffer,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, current_word_prefix, if_not_exists_clause,
        into_parse_error, kw, kw_phrase2, lex_input, relay_name, relay_ref, schema_ref,
        suggestions_from_errors, tok, word_raw,
    },
};

fn positive_usize<'src>()
-> impl Parser<'src, &'src [Token], usize, extra::Err<ParseError<'src>>> + Clone {
    choice((select! { Token::NumberLiteral(v) => v }, word_raw()))
        .try_map(|raw, span| {
            raw.parse::<usize>()
                .map_err(|_| Rich::custom(span, format!("invalid usize literal '{raw}'")))
                .and_then(|value| {
                    if value == 0 {
                        Err(Rich::custom(span, "capacity must be greater than 0"))
                    } else {
                        Ok(value)
                    }
                })
        })
        .labelled("relay_capacity")
}

pub fn create_relay_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateRelay>, extra::Err<ParseError<'src>>> + Clone
{
    let materialized_state = kw(Identifier::With)
        .ignore_then(kw(Identifier::Materialized))
        .ignore_then(kw(Identifier::State))
        .ignore_then(kw(Identifier::Last))
        .ignore_then(kw(Identifier::By))
        .ignore_then(kw(Identifier::Timestamp))
        .to(MaterializedRelayState::LastByTimestamp);

    let capacity = kw(Identifier::Capacity).ignore_then(positive_usize());

    let parameterized_tail = kw_phrase2(Identifier::Parameterized, Identifier::By)
        .ignore_then(schema_ref())
        .then(capacity.clone().or_not())
        .map(|(parameterized_by, buffer)| {
            (
                RelayParameterization::parameterized(RelayParameters::declared(parameterized_by)),
                buffer,
            )
        });

    let unparameterized_tail = kw(Identifier::Unparameterized)
        .ignore_then(capacity.clone().or_not())
        .map(|buffer| (RelayParameterization::unparameterized(), buffer));

    let default_tail = capacity.map(|buffer| {
        (
            RelayParameterization::parameterized(RelayParameters::inferred()),
            Some(buffer),
        )
    });
    let tail = choice((parameterized_tail, unparameterized_tail, default_tail)).or_not();

    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Relay))
        .then(relay_name())
        .then_ignore(kw(Identifier::Schema))
        .then(schema_ref())
        .then(tail)
        .then(materialized_state.or_not())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |((((if_not_exists, name), schema), tail), materialized_state)| {
                let (parameterization, buffer) = tail.unwrap_or_else(|| {
                    (
                        RelayParameterization::parameterized(RelayParameters::inferred()),
                        None,
                    )
                });
                CreateStatement::new(
                    CreateRelay {
                        name,
                        schema,
                        buffer: buffer.unwrap_or_else(default_relay_buffer),
                        parameterization,
                        materialized_state,
                    },
                    if_not_exists,
                )
            },
        )
}

pub fn alter_relay_parser<'src>()
-> impl Parser<'src, &'src [Token], AlterRelay, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Alter)
        .ignore_then(kw(Identifier::Relay))
        .ignore_then(relay_ref())
        .then_ignore(kw(Identifier::Set))
        .then_ignore(kw(Identifier::Capacity))
        .then(positive_usize())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(|(relay, capacity)| AlterRelay {
            relay,
            operation: AlterRelayOperation::SetCapacity { capacity },
        })
}

pub fn parse_create_stream_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateRelay>, Vec<ParseError<'_>>> {
    let out = create_relay_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_stream(
    input: &str,
) -> Result<CreateStatement<CreateRelay>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_stream_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn parse_alter_relay_tokens(tokens: &[Token]) -> Result<AlterRelay, Vec<ParseError<'_>>> {
    let out = alter_relay_parser().then_ignore(end()).parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_alter_relay(input: &str) -> Result<AlterRelay, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_alter_relay_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_stream(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_relay_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if !out.has_errors() {
        return Vec::new();
    }

    suggestions_from_errors(out.into_errors(), &prefix)
}

pub fn suggest_alter_relay(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = alter_relay_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if !out.has_errors() {
        return Vec::new();
    }

    suggestions_from_errors(out.into_errors(), &prefix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::lex;

    fn to_tokens(input: &str) -> Vec<Token> {
        lex(input)
            .expect("lexer should succeed")
            .into_iter()
            .map(|t| t.token)
            .collect()
    }

    #[test]
    fn parses_create_stream() {
        let tokens = to_tokens("CREATE RELAY notifications SCHEMA event_schema;");
        let parsed = parse_create_stream_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "notifications");
        assert_eq!(parsed.schema.as_str(), "event_schema");
        assert_eq!(parsed.buffer, 1);
        assert_eq!(
            parsed.parameterization,
            RelayParameterization::parameterized(RelayParameters::inferred())
        );
        assert_eq!(parsed.materialized_state, None);
    }

    #[test]
    fn parses_create_parameterized_stream() {
        let tokens = to_tokens(
            "CREATE RELAY notifications SCHEMA event_schema PARAMETERIZED BY tenant_branch;",
        );
        let parsed = parse_create_stream_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.parameterization,
            RelayParameterization::parameterized(RelayParameters::declared(
                nervix_models::Identifier::parse("tenant_branch").expect("valid identifier"),
            ))
        );
    }

    #[test]
    fn parses_create_stream_with_explicit_capacity() {
        let tokens = to_tokens("CREATE RELAY notifications SCHEMA event_schema CAPACITY 32;");
        let parsed = parse_create_stream_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.buffer, 32);
    }

    #[test]
    fn parses_alter_relay_set_capacity() {
        let tokens = to_tokens("ALTER RELAY notifications SET CAPACITY 32;");
        let parsed = parse_alter_relay_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.relay.as_str(), "notifications");
        assert_eq!(
            parsed.operation,
            AlterRelayOperation::SetCapacity { capacity: 32 }
        );
    }

    #[test]
    fn rejects_alter_relay_zero_capacity() {
        let error = parse_alter_relay("ALTER RELAY notifications SET CAPACITY 0;")
            .expect_err("parse should fail");

        let ParseFromSourceError::Parse { diagnostics, .. } = error else {
            panic!("expected parse error");
        };
        assert!(!diagnostics.is_empty());
    }

    #[test]
    fn parses_create_stream_with_materialized_state() {
        let tokens = to_tokens(
            "CREATE RELAY notifications SCHEMA event_schema WITH MATERIALIZED STATE LAST BY \
             TIMESTAMP;",
        );
        let parsed = parse_create_stream_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.materialized_state,
            Some(MaterializedRelayState::LastByTimestamp)
        );
    }

    #[test]
    fn rejects_zero_capacity() {
        let error =
            parse_create_stream("CREATE RELAY notifications SCHEMA event_schema CAPACITY 0;")
                .expect_err("parse should fail");

        let ParseFromSourceError::Parse { diagnostics, .. } = error else {
            panic!("expected parse error");
        };
        assert!(!diagnostics.is_empty());
    }

    #[test]
    fn parses_unparameterized_without_ttl() {
        let tokens = to_tokens("CREATE RELAY notifications SCHEMA event_schema UNPARAMETERIZED;");
        let parsed = parse_create_stream_tokens(&tokens).expect("parse should succeed");

        assert!(parsed.parameterization.is_unparameterized());
    }

    #[test]
    fn rejects_branch_as_relay_name() {
        let error = parse_create_stream("CREATE RELAY branch SCHEMA event_schema;")
            .expect_err("reserved branch namespace must not be accepted as relay name");

        let ParseFromSourceError::Parse { diagnostics, .. } = error else {
            panic!("expected parse error");
        };
        assert!(!diagnostics.is_empty(), "expected diagnostics");
    }

    #[test]
    fn rejects_branch_as_relay_reference() {
        let error = crate::statement::parse_statement(
            "CREATE REINGESTOR fw FROM branch TO projected PARAMETERIZED BY tenant_branch VALUES \
             { tenant = branch.tenant } TTL 5m FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        )
        .expect_err("reserved branch namespace must not be accepted as relay reference");

        let ParseFromSourceError::Parse { diagnostics, .. } = error else {
            panic!("expected parse error");
        };
        assert!(!diagnostics.is_empty(), "expected diagnostics");
    }

    #[test]
    fn rejects_unparameterized_with_ttl() {
        let error = parse_create_stream(
            "CREATE RELAY notifications SCHEMA event_schema UNPARAMETERIZED TTL 5m;",
        )
        .expect_err("parse should fail");

        let ParseFromSourceError::Parse { diagnostics, .. } = error else {
            panic!("expected parse error");
        };
        assert!(!diagnostics.is_empty());
    }

    #[test]
    fn suggests_schema_keyword_after_relay_name() {
        let input = "CREATE RELAY notifications ";
        let suggestions = suggest_create_stream(input, input.len());
        assert!(suggestions.contains(&"SCHEMA".to_string()));
    }

    #[test]
    fn suggests_relay_capacity_after_capacity_keyword_without_keyword_leakage() {
        let input = "CREATE RELAY notifications SCHEMA event_schema CAPACITY ";
        let suggestions = suggest_create_stream(input, input.len());
        assert!(suggestions.contains(&"relay_capacity".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn suggests_alter_relay_capacity_after_capacity_keyword_without_keyword_leakage() {
        let input = "ALTER RELAY notifications SET CAPACITY ";
        let suggestions = suggest_alter_relay(input, input.len());
        assert!(suggestions.contains(&"relay_capacity".to_string()));
        assert!(!suggestions.contains(&"JSON".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn does_not_suggest_ttl_after_capacity_value() {
        let input = "CREATE RELAY notifications SCHEMA event_schema CAPACITY 32 ";
        let suggestions = suggest_create_stream(input, input.len());
        assert!(!suggestions.contains(&"TTL".to_string()));
    }

    #[test]
    fn rejects_relay_ttl() {
        let input = "CREATE RELAY notifications SCHEMA event_schema TTL ";
        let error = parse_create_stream(input).expect_err("parse should fail");

        let ParseFromSourceError::Parse { diagnostics, .. } = error else {
            panic!("expected parse error");
        };
        assert!(!diagnostics.is_empty());
    }

    #[test]
    fn suggests_materialized_after_with() {
        let input = "CREATE RELAY notifications SCHEMA event_schema WITH ";
        let suggestions = suggest_create_stream(input, input.len());
        assert!(suggestions.contains(&"MATERIALIZED".to_string()));
    }

    #[test]
    fn does_not_suggest_ttl_after_unparameterized() {
        let input = "CREATE RELAY notifications SCHEMA event_schema UNPARAMETERIZED ";
        let suggestions = suggest_create_stream(input, input.len());
        assert!(!suggestions.contains(&"TTL".to_string()));
    }
}
