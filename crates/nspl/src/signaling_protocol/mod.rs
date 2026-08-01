use chumsky::prelude::*;
use nervix_models::{
    CreateSignalingProtocol, CreateStatement, SignalingPhase, SignalingProtobufConfig,
    SignalingProtocolOnConnect, SignalingWait, SignalingWireFormat,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, ParseFromSourceError, config_entries_block, duration_lit, if_not_exists_clause,
        into_parse_error, kw, lex_input, resource_ref, signaling_protocol_name, string_lit,
        suggest_from, tok, u64_value,
    },
};

fn jaq_program_list<'src>(
    label: &'static str,
) -> impl Parser<'src, &'src [Token], Vec<String>, extra::Err<ParseError<'src>>> + Clone {
    string_lit()
        .labelled(label)
        .separated_by(tok(Token::Comma))
        .at_least(1)
        .collect::<Vec<_>>()
}

/// What follows the first matcher of a `WAIT JAQ` clause.
#[derive(Debug, Clone)]
enum WaitTail {
    Matchers(Vec<String>),
    Modifiers {
        capture: Option<String>,
        accept_data: bool,
    },
}

impl WaitTail {
    fn into_waits(self, first: String) -> Vec<SignalingWait> {
        match self {
            Self::Matchers(rest) => std::iter::once(first)
                .chain(rest)
                .map(SignalingWait::new)
                .collect(),
            Self::Modifiers {
                capture,
                accept_data,
            } => vec![SignalingWait {
                matcher: first,
                capture,
                accept_data,
            }],
        }
    }
}

/// One written clause of the handshake, before clauses are grouped into ordered phases.
#[derive(Debug, Clone)]
enum SignalingClause {
    Send(Vec<String>),
    Wait(Vec<SignalingWait>),
}

fn signaling_clause<'src>()
-> impl Parser<'src, &'src [Token], SignalingClause, extra::Err<ParseError<'src>>> + Clone {
    let send = kw(Identifier::Send)
        .ignore_then(kw(Identifier::Jaq))
        .ignore_then(jaq_program_list("jaq_program"))
        .map(SignalingClause::Send);
    // Both modifiers describe the one frame that satisfied the matcher, so a wait either lists
    // several plain matchers or carries modifiers on a single one.
    let further_matchers = tok(Token::Comma)
        .ignore_then(string_lit().labelled("jaq_matcher"))
        .repeated()
        .at_least(1)
        .collect::<Vec<_>>()
        .map(WaitTail::Matchers);
    let modifiers = kw(Identifier::Capture)
        .ignore_then(string_lit().labelled("jaq_capture"))
        .or_not()
        .then(
            kw(Identifier::Accept)
                .ignore_then(kw(Identifier::Data))
                .or_not()
                .map(|accept| accept.is_some()),
        )
        .map(|(capture, accept_data)| WaitTail::Modifiers {
            capture,
            accept_data,
        });
    let wait = kw(Identifier::Wait)
        .ignore_then(kw(Identifier::Jaq))
        .ignore_then(string_lit().labelled("jaq_matcher"))
        .then(choice((further_matchers, modifiers)))
        .map(|(matcher, tail)| SignalingClause::Wait(tail.into_waits(matcher)));

    choice((send, wait)).boxed()
}

/// Group written clauses into ordered phases.
///
/// Sends that follow a wait begin a new phase, which is what holds them back until the preceding
/// wait is satisfied. Consecutive clauses of the same kind stay in one phase, so a protocol that
/// never interleaves keeps the single-phase behavior of sending everything and then waiting.
fn phases_from_clauses(clauses: Vec<SignalingClause>) -> Vec<SignalingPhase> {
    let mut phases: Vec<SignalingPhase> = Vec::new();
    for clause in clauses {
        match clause {
            SignalingClause::Send(sends) => match phases.last_mut() {
                Some(phase) if phase.waits.is_empty() => phase.sends.extend(sends),
                _ => phases.push(SignalingPhase::new(sends, Vec::new())),
            },
            SignalingClause::Wait(waits) => match phases.last_mut() {
                Some(phase) => phase.waits.extend(waits),
                None => phases.push(SignalingPhase::new(Vec::new(), waits)),
            },
        }
    }
    phases
}

fn signaling_wire_format<'src>()
-> impl Parser<'src, &'src [Token], SignalingWireFormat, extra::Err<ParseError<'src>>> + Clone {
    let native = choice((
        kw(Identifier::Json).to(SignalingWireFormat::Json),
        kw(Identifier::Yaml).to(SignalingWireFormat::Yaml),
        kw(Identifier::Toml).to(SignalingWireFormat::Toml),
        kw(Identifier::Xml).to(SignalingWireFormat::Xml),
        kw(Identifier::Cbor).to(SignalingWireFormat::Cbor),
        kw(Identifier::Raw).to(SignalingWireFormat::Raw),
    ));
    let protobuf = kw(Identifier::Protobuf)
        .ignore_then(kw(Identifier::Using))
        .ignore_then(kw(Identifier::Resource))
        .ignore_then(resource_ref())
        .then(kw(Identifier::Version).ignore_then(u64_value()).or_not())
        .then(config_entries_block())
        .boxed()
        .then_ignore(kw(Identifier::Send))
        .then_ignore(kw(Identifier::Message))
        .then(string_lit().labelled("protobuf_message"))
        .then_ignore(kw(Identifier::Wait))
        .then_ignore(kw(Identifier::Message))
        .then(string_lit().labelled("protobuf_message"))
        .map(
            |((((resource, resource_version), config), send_message), wait_message)| {
                SignalingWireFormat::Protobuf(SignalingProtobufConfig {
                    resource,
                    resource_version,
                    config,
                    send_message,
                    wait_message,
                })
            },
        )
        .boxed();
    choice((native, protobuf)).boxed()
}

pub fn create_signaling_protocol_parser<'src>() -> impl Parser<
    'src,
    &'src [Token],
    CreateStatement<CreateSignalingProtocol>,
    extra::Err<ParseError<'src>>,
> + Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Signaling))
        .then_ignore(kw(Identifier::Protocol))
        .then(signaling_protocol_name())
        .then_ignore(kw(Identifier::From))
        .then(signaling_wire_format())
        .boxed()
        .then_ignore(kw(Identifier::On))
        .then_ignore(kw(Identifier::Connect))
        .then(
            signaling_clause()
                .repeated()
                .at_least(1)
                .collect::<Vec<_>>()
                .map(phases_from_clauses),
        )
        .boxed()
        .then(
            kw(Identifier::Fail)
                .ignore_then(kw(Identifier::Jaq))
                .ignore_then(jaq_program_list("jaq_matcher"))
                .or_not()
                .map(Option::unwrap_or_default),
        )
        .then_ignore(kw(Identifier::Timeout))
        .then(duration_lit().try_map(|timeout, span| {
            humantime::parse_duration(&timeout)
                .map(|_| timeout.clone())
                .map_err(|error| {
                    Rich::custom(span, format!("invalid duration '{timeout}': {error}"))
                })
        }))
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |(((((if_not_exists, name), format), phases), fail_matchers), timeout)| {
                CreateStatement::new(
                    CreateSignalingProtocol {
                        name,
                        format,
                        on_connect: SignalingProtocolOnConnect {
                            phases,
                            fail_matchers,
                            timeout,
                        },
                    },
                    if_not_exists,
                )
            },
        )
        .boxed()
}

pub fn parse_create_signaling_protocol_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateSignalingProtocol>, Vec<ParseError<'_>>> {
    let out = create_signaling_protocol_parser()
        .then_ignore(end())
        .parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_signaling_protocol(
    input: &str,
) -> Result<CreateStatement<CreateSignalingProtocol>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_signaling_protocol_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_signaling_protocol(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, create_signaling_protocol_parser())
}

#[cfg(test)]
mod tests {
    use nervix_models::ClientConfigEntry;

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
    fn parses_create_signaling_protocol_from_json() {
        let tokens = to_tokens(
            r#"
            CREATE SIGNALING PROTOCOL binance_style
              FROM JSON
              ON CONNECT
              SEND JAQ '{method: "SUBSCRIBE", id: 1}', '{method: "SUBSCRIBE", id: 2}'
              WAIT JAQ '.id == 1 and .result == null', '.id == 2 and .result == null'
              TIMEOUT 5s;
            "#,
        );
        let parsed = parse_create_signaling_protocol_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "binance_style");
        assert_eq!(parsed.format, SignalingWireFormat::Json);
        assert_eq!(
            parsed.on_connect.phases,
            vec![SignalingPhase::new(
                vec![
                    r#"{method: "SUBSCRIBE", id: 1}"#.to_string(),
                    r#"{method: "SUBSCRIBE", id: 2}"#.to_string(),
                ],
                vec![
                    SignalingWait::new(".id == 1 and .result == null".to_string()),
                    SignalingWait::new(".id == 2 and .result == null".to_string()),
                ],
            )]
        );
        assert!(parsed.on_connect.fail_matchers.is_empty());
        assert_eq!(parsed.on_connect.timeout, "5s");
    }

    #[test]
    fn parses_create_signaling_protocol_with_fail_matchers() {
        let tokens = to_tokens(
            r#"
            CREATE IF NOT EXISTS SIGNALING PROTOCOL guarded
              FROM YAML
              ON CONNECT
              SEND JAQ '{op: "subscribe"}'
              WAIT JAQ '.success == true'
              FAIL JAQ '.success == false', '.error'
              TIMEOUT 10s;
            "#,
        );
        let parsed = parse_create_signaling_protocol_tokens(&tokens).expect("parse should succeed");

        assert!(parsed.if_not_exists);
        assert_eq!(parsed.format, SignalingWireFormat::Yaml);
        assert_eq!(
            parsed.on_connect.fail_matchers,
            vec![".success == false".to_string(), ".error".to_string()]
        );
        assert_eq!(parsed.on_connect.phases.len(), 1);
    }

    #[test]
    fn parses_interleaved_clauses_as_ordered_phases() {
        let tokens = to_tokens(
            r#"
            CREATE SIGNALING PROTOCOL authenticated
              FROM JSON
              ON CONNECT
              SEND JAQ '{op: "auth"}'
              WAIT JAQ '.authed' CAPTURE '{token: .token}'
              SEND JAQ '{op: "subscribe", token: $state.token, id: 1}'
              WAIT JAQ '.id == 1'
              TIMEOUT 5s;
            "#,
        );
        let parsed = parse_create_signaling_protocol_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.on_connect.phases,
            vec![
                SignalingPhase::new(
                    vec![r#"{op: "auth"}"#.to_string()],
                    vec![SignalingWait {
                        matcher: ".authed".to_string(),
                        capture: Some("{token: .token}".to_string()),
                        accept_data: false,
                    }],
                ),
                SignalingPhase::new(
                    vec![r#"{op: "subscribe", token: $state.token, id: 1}"#.to_string()],
                    vec![SignalingWait::new(".id == 1".to_string())],
                ),
            ]
        );
    }

    #[test]
    fn keeps_consecutive_clauses_of_one_kind_in_a_single_phase() {
        let tokens = to_tokens(
            r#"
            CREATE SIGNALING PROTOCOL batched
              FROM JSON
              ON CONNECT
              SEND JAQ '{id: 1}'
              SEND JAQ '{id: 2}'
              WAIT JAQ '.id == 1'
              WAIT JAQ '.id == 2'
              TIMEOUT 5s;
            "#,
        );
        let parsed = parse_create_signaling_protocol_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.on_connect.phases.len(), 1);
        assert_eq!(parsed.on_connect.phases[0].sends.len(), 2);
        assert_eq!(parsed.on_connect.phases[0].waits.len(), 2);
    }

    #[test]
    fn parses_a_leading_wait_as_a_phase_without_sends() {
        let tokens = to_tokens(
            r#"
            CREATE SIGNALING PROTOCOL challenge
              FROM JSON
              ON CONNECT
              WAIT JAQ '.challenge' CAPTURE '{nonce: .challenge}'
              SEND JAQ '{answer: $state.nonce}'
              WAIT JAQ '.accepted'
              TIMEOUT 5s;
            "#,
        );
        let parsed = parse_create_signaling_protocol_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.on_connect.phases.len(), 2);
        assert!(parsed.on_connect.phases[0].sends.is_empty());
        assert_eq!(
            parsed.on_connect.phases[0].waits[0].capture.as_deref(),
            Some("{nonce: .challenge}")
        );
    }

    #[test]
    fn rejects_capture_on_a_multi_matcher_wait() {
        let input = "CREATE SIGNALING PROTOCOL bad_capture FROM JSON ON CONNECT SEND JAQ '{id: \
                     1}' WAIT JAQ '.id == 1', '.id == 2' CAPTURE '{token: .token}' TIMEOUT 5s ;";

        assert!(parse_create_signaling_protocol(input).is_err());
    }

    #[test]
    fn parses_create_signaling_protocol_from_raw() {
        let tokens = to_tokens(
            r#"
            CREATE SIGNALING PROTOCOL plain
              FROM RAW
              ON CONNECT
              SEND JAQ '"HELLO"'
              WAIT JAQ '. == "WELCOME"'
              TIMEOUT 5s;
            "#,
        );
        let parsed = parse_create_signaling_protocol_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.format, SignalingWireFormat::Raw);
    }

    #[test]
    fn parses_create_signaling_protocol_from_cbor() {
        let tokens = to_tokens(
            r#"
            CREATE SIGNALING PROTOCOL binary
              FROM CBOR
              ON CONNECT
              SEND JAQ '{op: "subscribe"}'
              WAIT JAQ '.ok'
              TIMEOUT 5s;
            "#,
        );
        let parsed = parse_create_signaling_protocol_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.format, SignalingWireFormat::Cbor);
    }

    #[test]
    fn parses_create_signaling_protocol_from_protobuf() {
        let tokens = to_tokens(
            r#"
            CREATE SIGNALING PROTOCOL proto_handshake
              FROM PROTOBUF USING RESOURCE proto_bundle VERSION 2
                CONFIG {'file' = 'signaling.proto', 'include' = '.'}
                SEND MESSAGE 'nervix.test.Subscribe'
                WAIT MESSAGE 'nervix.test.Ack'
              ON CONNECT
              SEND JAQ '{id: 1}'
              WAIT JAQ '.id == 1'
              TIMEOUT 5s;
            "#,
        );
        let parsed = parse_create_signaling_protocol_tokens(&tokens).expect("parse should succeed");

        assert_eq!(
            parsed.format,
            SignalingWireFormat::Protobuf(SignalingProtobufConfig {
                resource: nervix_models::Identifier::parse("proto_bundle")
                    .expect("valid identifier"),
                resource_version: Some(2),
                config: vec![
                    ClientConfigEntry {
                        key: "file".to_string(),
                        value: "signaling.proto".to_string(),
                    },
                    ClientConfigEntry {
                        key: "include".to_string(),
                        value: ".".to_string(),
                    },
                ],
                send_message: "nervix.test.Subscribe".to_string(),
                wait_message: "nervix.test.Ack".to_string(),
            })
        );
    }

    #[test]
    fn parses_create_signaling_protocol_from_protobuf_without_version() {
        let tokens = to_tokens(
            r#"
            CREATE SIGNALING PROTOCOL proto_handshake
              FROM PROTOBUF USING RESOURCE proto_bundle
                CONFIG {'file' = 'signaling.proto'}
                SEND MESSAGE 'nervix.test.Subscribe'
                WAIT MESSAGE 'nervix.test.Ack'
              ON CONNECT
              SEND JAQ '{id: 1}'
              WAIT JAQ '.id == 1'
              TIMEOUT 5s;
            "#,
        );
        let parsed = parse_create_signaling_protocol_tokens(&tokens).expect("parse should succeed");

        let SignalingWireFormat::Protobuf(config) = &parsed.format else {
            panic!("expected protobuf signaling format");
        };
        assert_eq!(config.resource_version, None);
    }

    #[test]
    fn marks_the_matcher_that_opens_payload_ingestion() {
        let tokens = to_tokens(
            r#"
            CREATE SIGNALING PROTOCOL staged
              FROM JSON
              ON CONNECT
              SEND JAQ '{id: 1}', '{id: 2}'
              WAIT JAQ '.id == 1' ACCEPT DATA
              WAIT JAQ '.id == 2'
              TIMEOUT 5s;
            "#,
        );
        let parsed = parse_create_signaling_protocol_tokens(&tokens).expect("parse should succeed");

        // Both subscriptions stay in one phase, so they are still written together.
        assert_eq!(parsed.on_connect.phases.len(), 1);
        let waits = &parsed.on_connect.phases[0].waits;
        assert!(waits[0].accept_data);
        assert!(!waits[1].accept_data);
        assert!(parsed.on_connect.accepts_data_during_handshake());
    }

    #[test]
    fn accepts_capture_and_accept_data_on_one_matcher() {
        let tokens = to_tokens(
            r#"
            CREATE SIGNALING PROTOCOL both
              FROM JSON
              ON CONNECT
              SEND JAQ '{op: "auth"}'
              WAIT JAQ '.authed' CAPTURE '{token: .token}' ACCEPT DATA
              TIMEOUT 5s;
            "#,
        );
        let parsed = parse_create_signaling_protocol_tokens(&tokens).expect("parse should succeed");

        let wait = &parsed.on_connect.phases[0].waits[0];
        assert_eq!(wait.capture.as_deref(), Some("{token: .token}"));
        assert!(wait.accept_data);
    }

    #[test]
    fn omitting_accept_data_holds_payload_for_the_whole_handshake() {
        let tokens = to_tokens(
            r#"
            CREATE SIGNALING PROTOCOL held
              FROM JSON
              ON CONNECT
              SEND JAQ '{id: 1}'
              WAIT JAQ '.id == 1'
              TIMEOUT 5s;
            "#,
        );
        let parsed = parse_create_signaling_protocol_tokens(&tokens).expect("parse should succeed");

        assert!(!parsed.on_connect.accepts_data_during_handshake());
    }

    #[test]
    fn rejects_accept_data_on_a_matcher_list() {
        let input = "CREATE SIGNALING PROTOCOL bad FROM JSON ON CONNECT SEND JAQ '{id: 1}' WAIT \
                     JAQ '.id == 1', '.id == 2' ACCEPT DATA TIMEOUT 5s;";

        assert!(parse_create_signaling_protocol(input).is_err());
    }

    #[test]
    fn suggests_accept_after_a_single_matcher() {
        let input = "CREATE SIGNALING PROTOCOL live FROM JSON ON CONNECT SEND JAQ '{id: 1}' WAIT \
                     JAQ '.id == 1' ";
        let suggestions = suggest_create_signaling_protocol(input, input.len());

        assert!(suggestions.contains(&"ACCEPT".to_string()));
        assert!(suggestions.contains(&"CAPTURE".to_string()));
    }

    #[test]
    fn suggests_data_after_accept() {
        let input = "CREATE SIGNALING PROTOCOL live FROM JSON ON CONNECT SEND JAQ '{id: 1}' WAIT \
                     JAQ '.id == 1' ACCEPT ";
        let suggestions = suggest_create_signaling_protocol(input, input.len());

        assert!(suggestions.contains(&"DATA".to_string()));
    }

    #[test]
    fn rejects_legacy_send_body_syntax() {
        let input = r#"CREATE SIGNALING PROTOCOL legacy ON CONNECT SEND BODY '{"id":1}' WAIT BODY '{"id":1}' TIMEOUT 5s;"#;

        assert!(parse_create_signaling_protocol(input).is_err());
    }

    #[test]
    fn rejects_missing_wire_format() {
        let input = "CREATE SIGNALING PROTOCOL missing_format ON CONNECT SEND JAQ '{id: 1}' WAIT \
                     JAQ '.id == 1' TIMEOUT 5s;";

        assert!(parse_create_signaling_protocol(input).is_err());
    }

    #[test]
    fn rejects_missing_wait_matcher() {
        let input = "CREATE SIGNALING PROTOCOL missing_wait FROM JSON ON CONNECT SEND JAQ '{id: \
                     1}' WAIT JAQ TIMEOUT 5s;";

        assert!(parse_create_signaling_protocol(input).is_err());
    }

    #[test]
    fn rejects_missing_timeout() {
        let input = "CREATE SIGNALING PROTOCOL missing_timeout FROM JSON ON CONNECT SEND JAQ \
                     '{id: 1}' WAIT JAQ '.id == 1';";

        assert!(parse_create_signaling_protocol(input).is_err());
    }

    #[test]
    fn rejects_invalid_timeout_duration() {
        let input = "CREATE SIGNALING PROTOCOL bad_timeout FROM JSON ON CONNECT SEND JAQ '{id: \
                     1}' WAIT JAQ '.id == 1' TIMEOUT 5potatoes;";

        assert!(parse_create_signaling_protocol(input).is_err());
    }

    #[test]
    fn rejects_protobuf_without_config() {
        let input = "CREATE SIGNALING PROTOCOL proto_handshake FROM PROTOBUF USING RESOURCE \
                     proto_bundle SEND MESSAGE 'nervix.test.Subscribe' WAIT MESSAGE \
                     'nervix.test.Ack' ON CONNECT SEND JAQ '{id: 1}' WAIT JAQ '.id == 1' TIMEOUT \
                     5s;";

        assert!(parse_create_signaling_protocol(input).is_err());
    }

    #[test]
    fn rejects_protobuf_without_wait_message() {
        let input = "CREATE SIGNALING PROTOCOL proto_handshake FROM PROTOBUF USING RESOURCE \
                     proto_bundle CONFIG {'file' = 'signaling.proto'} SEND MESSAGE \
                     'nervix.test.Subscribe' ON CONNECT SEND JAQ '{id: 1}' WAIT JAQ '.id == 1' \
                     TIMEOUT 5s;";

        assert!(parse_create_signaling_protocol(input).is_err());
    }

    #[test]
    fn suggests_signaling_protocol_name_after_keyword() {
        let suggestions = suggest_create_signaling_protocol(
            "CREATE SIGNALING PROTOCOL ",
            "CREATE SIGNALING PROTOCOL ".len(),
        );
        assert!(suggestions.contains(&"signaling_protocol_name".to_string()));
    }

    #[test]
    fn suggests_from_after_signaling_protocol_name() {
        let input = "CREATE SIGNALING PROTOCOL binance_style ";
        let suggestions = suggest_create_signaling_protocol(input, input.len());

        assert!(suggestions.contains(&"FROM".to_string()));
    }

    #[test]
    fn suggests_wire_formats_after_from() {
        let input = "CREATE SIGNALING PROTOCOL binance_style FROM ";
        let suggestions = suggest_create_signaling_protocol(input, input.len());

        assert!(suggestions.contains(&"JSON".to_string()));
        assert!(suggestions.contains(&"YAML".to_string()));
        assert!(suggestions.contains(&"TOML".to_string()));
        assert!(suggestions.contains(&"XML".to_string()));
        assert!(suggestions.contains(&"CBOR".to_string()));
        assert!(suggestions.contains(&"RAW".to_string()));
        assert!(suggestions.contains(&"PROTOBUF".to_string()));
        assert!(!suggestions.contains(&"WIRE".to_string()));
        assert!(!suggestions.contains(&"AVRO".to_string()));
    }

    #[test]
    fn suggests_using_after_from_protobuf() {
        let input = "CREATE SIGNALING PROTOCOL proto_handshake FROM PROTOBUF ";
        let suggestions = suggest_create_signaling_protocol(input, input.len());

        assert!(suggestions.contains(&"USING".to_string()));
        assert!(!suggestions.contains(&"ON".to_string()));
    }

    #[test]
    fn suggests_send_message_after_protobuf_config() {
        let input = "CREATE SIGNALING PROTOCOL proto_handshake FROM PROTOBUF USING RESOURCE \
                     proto_bundle CONFIG {'file' = 'signaling.proto'} ";
        let suggestions = suggest_create_signaling_protocol(input, input.len());

        assert!(suggestions.contains(&"SEND".to_string()));
        assert!(!suggestions.contains(&"WAIT".to_string()));
    }

    #[test]
    fn suggests_jaq_program_after_send_jaq() {
        let input = "CREATE SIGNALING PROTOCOL binance_style FROM JSON ON CONNECT SEND JAQ ";
        let suggestions = suggest_create_signaling_protocol(input, input.len());

        assert!(suggestions.contains(&"jaq_program".to_string()));
    }

    #[test]
    fn suggests_jaq_matcher_after_wait_jaq() {
        let input = "CREATE SIGNALING PROTOCOL binance_style FROM JSON ON CONNECT SEND JAQ '{id: \
                     1}' WAIT JAQ ";
        let suggestions = suggest_create_signaling_protocol(input, input.len());

        assert!(suggestions.contains(&"jaq_matcher".to_string()));
    }

    #[test]
    fn suggests_fail_and_timeout_after_wait_matchers() {
        let input = "CREATE SIGNALING PROTOCOL binance_style FROM JSON ON CONNECT SEND JAQ '{id: \
                     1}' WAIT JAQ '.id == 1' ";
        let suggestions = suggest_create_signaling_protocol(input, input.len());

        assert!(suggestions.contains(&"FAIL".to_string()));
        assert!(suggestions.contains(&"TIMEOUT".to_string()));
    }

    #[test]
    fn suggests_capture_and_a_following_phase_after_a_single_matcher() {
        let input = "CREATE SIGNALING PROTOCOL binance_style FROM JSON ON CONNECT SEND JAQ '{id: \
                     1}' WAIT JAQ '.id == 1' ";
        let suggestions = suggest_create_signaling_protocol(input, input.len());

        assert!(suggestions.contains(&"CAPTURE".to_string()));
        assert!(suggestions.contains(&"SEND".to_string()));
        assert!(suggestions.contains(&"WAIT".to_string()));
    }

    #[test]
    fn does_not_suggest_capture_after_a_matcher_list() {
        let input = "CREATE SIGNALING PROTOCOL binance_style FROM JSON ON CONNECT SEND JAQ '{id: \
                     1}' WAIT JAQ '.id == 1', '.id == 2' ";
        let suggestions = suggest_create_signaling_protocol(input, input.len());

        assert!(!suggestions.contains(&"CAPTURE".to_string()));
        assert!(suggestions.contains(&"TIMEOUT".to_string()));
    }

    #[test]
    fn suggests_a_capture_program_after_capture() {
        let input = "CREATE SIGNALING PROTOCOL binance_style FROM JSON ON CONNECT SEND JAQ '{id: \
                     1}' WAIT JAQ '.id == 1' CAPTURE ";
        let suggestions = suggest_create_signaling_protocol(input, input.len());

        assert!(suggestions.contains(&"jaq_capture".to_string()));
    }

    #[test]
    fn suggests_only_timeout_after_fail_matchers() {
        let input = "CREATE SIGNALING PROTOCOL binance_style FROM JSON ON CONNECT SEND JAQ '{id: \
                     1}' WAIT JAQ '.id == 1' FAIL JAQ '.error' ";
        let suggestions = suggest_create_signaling_protocol(input, input.len());

        assert!(suggestions.contains(&"TIMEOUT".to_string()));
        assert!(!suggestions.contains(&"FAIL".to_string()));
    }
}
