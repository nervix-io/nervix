//! Layer: language.
//!
//! - **Owns.** The one lexer of NSPL: the tokens, keywords and spans every NSPL grammar reads, a
//!   statement and each expression it embeds alike, and the standalone expression readers.
//! - **Depends on.** Chumsky's parser primitives.
//! - **Must not know.** Grammars, Models, registry state or runtime execution.

use std::str::FromStr;

use chumsky::prelude::*;
use strum::{AsRefStr, EnumIter, EnumString, IntoStaticStr};

pub type LexError<'src> = Rich<'src, char>;
pub type Span = SimpleSpan<usize>;

#[derive(Debug, Clone, PartialEq)]
pub struct SpannedToken {
    pub token: Token,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Word(Word),
    StringLiteral(String),
    NumberLiteral(String),
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    LParen,
    RParen,
    Comma,
    Semicolon,
    DoubleColon,
    Colon,
    Dot,
    /// `-`: the minus of an expression, and the joint of a hyphenated name such as a hostname.
    Hyphen,
    Eq,
    NotEq,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Plus,
    Star,
    Slash,
    Percent,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Word {
    KnownWord {
        iden: Identifier,
        raw: String,
    },
    UnknownWord(String),
    /// A name written between backticks, such as `` `end` `` or `` `a-b` ``, holding the text
    /// between them. It is never a keyword, whatever it spells, and the name it stands for checks
    /// that text by its own rule.
    Quoted(String),
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter, EnumString, AsRefStr, IntoStaticStr,
)]
#[strum(ascii_case_insensitive, serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum Identifier {
    Create,
    Delete,
    Add,
    Alter,
    Drop,
    Rename,
    Cordon,
    Uncordon,
    Drain,
    Rebind,
    Relocate,
    Relocation,
    Use,
    List,
    Attach,
    Detach,
    Begin,
    Start,
    Stop,
    Describe,
    Lookup,
    Upload,
    Backup,
    Restore,
    Resume,
    Existing,
    Dry,
    Run,
    Resource,
    Resources,
    Show,
    If,
    Exists,
    Cluster,
    Status,
    Node,
    Version,
    Paced,
    Unpaced,
    Domain,
    Domains,
    Clock,
    Transaction,
    Transactions,
    Operation,
    Placement,
    Placements,
    Rank,
    Require,
    Prefer,
    Suggest,
    Preferences,
    Follow,
    Onto,
    Colocation,
    Separation,
    Neutral,
    User,
    Password,
    Period,
    Skew,
    Intersection,
    At,
    Now,
    Time,
    Rate,
    Timestamp,
    Session,
    Subscription,
    Vhost,
    Endpoint,
    Signaling,
    Protocol,
    Generator,
    Inferencer,
    Wasm,
    Reingestor,
    Reorderer,
    On,
    Connect,
    Message,
    Input,
    Branch,
    Branches,
    General,
    Global,
    Error,
    Ignore,
    Log,
    Rejected,
    Preserve,
    Reset,
    In,
    Sensitive,
    Branched,
    Unbranched,
    Path,
    Method,
    Without,
    Body,
    Http,
    Websockets,
    KafkaBroker,
    Addresses,
    With,
    Materialized,
    State,
    Pause,
    Source,
    Offsets,
    Last,
    Tls,
    Jaq,
    Transformations,
    Ingestion,
    Emitting,
    Strict,
    Loose,
    Json,
    Yaml,
    Toml,
    Xml,
    Avro,
    Cbor,
    Raw,
    Protobuf,
    Wire,
    Schema,
    Field,
    Codec,
    Ingestor,
    Ingestors,
    Into,
    Relay,
    Route,
    Processor,
    Window,
    Junction,
    Deduplicator,
    Correlator,
    Decode,
    Using,
    From,
    Left,
    Right,
    Rfc3339,
    File,
    Inputs,
    InnerInput,
    InnerOutput,
    Dense,
    Tensor,
    Hash,
    Key,
    Kafka,
    Pulsar,
    Clickhouse,
    Postgres,
    Mysql,
    Mongodb,
    S3,
    Gcs,
    AzureBlob,
    Iceberg,
    IcebergRest,
    Prometheus,
    Mqtt,
    Nats,
    Rabbitmq,
    Redis,
    Pubsub,
    Zeromq,
    Sqs,
    Sentry,
    Syslog,
    Otel,
    Logs,
    Traces,
    Metric,
    Attributes,
    Scope,
    Gauge,
    Sum,
    Histogram,
    Unit,
    Description,
    Monotonic,
    Delta,
    Cumulative,
    Collect,
    Collection,
    Broker,
    Capacity,
    Ttl,
    Topic,
    Subject,
    Queue,
    Channel,
    Offset,
    Consumer,
    Group,
    Instances,
    Evict,
    Lru,
    Clean,
    Persistent,
    Qos,
    Mode,
    Quiesce,
    Suspend,
    Buffer,
    Overflow,
    Oldest,
    Newest,
    Reject,
    After,
    Ack,
    NoAck,
    Jetstream,
    Parallel,
    Sequential,
    Single,
    Batch,
    Fifo,
    Dynamic,
    Sample,
    Blocking,
    Dropping,
    Flush,
    Commit,
    Revert,
    Immediate,
    Retry,
    Policy,
    Backoff,
    Max,
    Fuel,
    Memory,
    Min,
    Query,
    Every,
    Each,
    For,
    Set,
    Replace,
    Inherit,
    Except,
    Leak,
    Invoke,
    Output,
    Attached,
    Detached,
    Processed,
    By,
    SlidingWindow,
    Size,
    Pool,
    Step,
    Width,
    Duration,
    Filter,
    Where,
    And,
    Or,
    Not,
    True,
    False,
    Case,
    When,
    Then,
    Else,
    End,
    Between,
    Is,
    Distinct,
    TryCast,
    JsonValue,
    TryJsonValue,
    JsonExists,
    Deduplicate,
    Match,
    Correlate,
    First,
    All,
    Earliest,
    Latest,
    Correlation,
    Conflict,
    Send,
    Wait,
    Fail,
    Capture,
    Accept,
    Data,
    Format,
    Text,
    Required,
    Skip,
    Default,
    Emitter,
    Encode,
    Emit,
    Insert,
    To,
    Do,
    Update,
    Nothing,
    Timeout,
    Parse,
    As,
    Messages,
    Oneof,
    Client,
    Type,
    Mount,
    Config,
    Table,
    Catalog,
    Same,
    Location,
    Values,
    Optional,
    String,
    Number,
    Integer,
    Object,
    Array,
    Vec,
    Boolean,
    Null,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    Record,
    Enum,
    Map,
    Fixed,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    Bool,
    Datetime,
    F32,
    F64,
    Udf,
    Udfs,
    Args,
    Returns,
    Volatile,
    Code,
    #[strum(serialize = "ROTO_0_13")]
    Roto0_13,
}

impl Identifier {
    /// Whether an expression reserves this keyword, which spells one of its operators, literals or
    /// forms, or a clause of a route construction. Where a keyword could stand, such a word names a
    /// field or a function only between backticks; after a scope's `.` and after `udf::`, where no
    /// keyword stands, it is the name as written.
    ///
    /// Every other keyword is an ordinary name inside an expression: `max(input.readings)`,
    /// `first(...)`, `output.total`, `input.status` and a bare `to` read the statement keywords they
    /// spell as names.
    pub fn is_expression_keyword(self) -> bool {
        matches!(
            self,
            Self::Where
                | Self::Set
                | Self::Inherit
                | Self::All
                | Self::Except
                | Self::Leak
                | Self::Sensitive
                | Self::Invoke
                | Self::As
                | Self::TryCast
                | Self::JsonValue
                | Self::TryJsonValue
                | Self::JsonExists
                | Self::And
                | Self::Or
                | Self::Not
                | Self::True
                | Self::False
                | Self::Null
                | Self::If
                | Self::Case
                | Self::When
                | Self::Then
                | Self::Else
                | Self::End
                | Self::In
                | Self::Between
                | Self::Is
                | Self::Distinct
                | Self::From
                | Self::Udf
        )
    }
}

fn classify_word(raw: &str) -> Word {
    match Identifier::from_str(raw).ok() {
        Some(iden) => Word::KnownWord {
            iden,
            raw: raw.to_string(),
        },
        None => Word::UnknownWord(raw.to_string()),
    }
}

fn ws<'src>() -> impl Parser<'src, &'src str, (), extra::Err<LexError<'src>>> + Clone {
    let spaces = any()
        .filter(|c: &char| c.is_whitespace())
        .repeated()
        .at_least(1)
        .ignored();

    let line_comment = just("//")
        .then(any().filter(|c: &char| *c != '\n').repeated())
        .then(just('\n').or_not())
        .ignored();

    choice((spaces, line_comment)).repeated().ignored()
}

/// A string literal, read verbatim: `'text'`, `"text"`, or dollar-quoted `$$text$$` and
/// `$tag$text$tag$`, whose tag is letters, digits and underscores. A quoted string holds anything
/// but its own quote and a line break, and a dollar-quoted string anything but its closing
/// delimiter. No escape sequence is interpreted, so a backslash is an ordinary character.
///
/// This is the one reading of a string literal, wherever a statement or a standalone expression
/// writes one.
fn string_literal<'src>() -> impl Parser<'src, &'src str, String, extra::Err<LexError<'src>>> + Clone
{
    let single_string = just('\'')
        .ignore_then(
            any()
                .filter(|c: &char| *c != '\'' && *c != '\n')
                .repeated()
                .to_slice(),
        )
        .then_ignore(just('\''));

    let double_string = just('"')
        .ignore_then(
            any()
                .filter(|c: &char| *c != '"' && *c != '\n')
                .repeated()
                .to_slice(),
        )
        .then_ignore(just('"'));

    let dollar_string = custom(|input| {
        let start = input.cursor();
        if input.peek() != Some('$') {
            return Err(Rich::custom(
                input.span_since(&start),
                "expected dollar-quoted string",
            ));
        }
        input.skip();

        let mut tag = String::new();
        loop {
            match input.next() {
                Some('$') => break,
                Some(ch) if ch.is_ascii_alphanumeric() || ch == '_' => tag.push(ch),
                Some(_) => {
                    return Err(Rich::custom(
                        input.span_since(&start),
                        "dollar-quote tag must contain only letters, digits, or underscores",
                    ));
                }
                None => {
                    return Err(Rich::custom(
                        input.span_since(&start),
                        "unterminated dollar-quote delimiter",
                    ));
                }
            }
        }

        let delimiter = format!("${tag}$");
        let mut value = String::new();
        loop {
            match input.next() {
                Some(ch) => {
                    value.push(ch);
                    if let Some(body) = value.strip_suffix(delimiter.as_str()) {
                        let body_length = body.len();
                        value.truncate(body_length);
                        return Ok(value);
                    }
                }
                None => {
                    return Err(Rich::custom(
                        input.span_since(&start),
                        format!("unterminated dollar-quoted string; expected {delimiter}"),
                    ));
                }
            }
        }
    });

    choice((
        dollar_string,
        choice((single_string, double_string)).map(str::to_string),
    ))
}

/// A name written between backticks: any text but a backtick or a line break.
///
/// A name that spells a reserved word, or holds a character a plain word cannot, is written this
/// way. The text is not checked here: the name it stands for checks it by its own rule, which says
/// what is wrong with it.
fn quoted_name<'src>() -> impl Parser<'src, &'src str, String, extra::Err<LexError<'src>>> + Clone {
    just('`')
        .ignore_then(
            any()
                .filter(|c: &char| *c != '`' && *c != '\n')
                .repeated()
                .to_slice(),
        )
        .then_ignore(just('`'))
        .map(str::to_string)
}

fn token<'src>() -> impl Parser<'src, &'src str, SpannedToken, extra::Err<LexError<'src>>> + Clone {
    let word = text::ascii::ident().map(|raw: &str| Token::Word(classify_word(raw)));
    let quoted = quoted_name().map(|text| Token::Word(Word::Quoted(text)));

    let number = text::int(10)
        .then(just('.').then(text::digits(10)).or_not())
        .then(
            one_of("eE")
                .then(one_of("+-").or_not())
                .then(text::digits(10))
                .or_not(),
        )
        .to_slice()
        .map(|n: &str| Token::NumberLiteral(n.to_string()));

    let string = string_literal().map(Token::StringLiteral);

    let punctuation = choice((
        just("!=").to(Token::NotEq),
        just(">=").to(Token::GtEq),
        just("<=").to(Token::LtEq),
        just("::").to(Token::DoubleColon),
        just('{').to(Token::LBrace),
        just('}').to(Token::RBrace),
        just('[').to(Token::LBracket),
        just(']').to(Token::RBracket),
        just('(').to(Token::LParen),
        just(')').to(Token::RParen),
        just(',').to(Token::Comma),
        just(';').to(Token::Semicolon),
        just(':').to(Token::Colon),
        just('.').to(Token::Dot),
        just('-').to(Token::Hyphen),
        just('=').to(Token::Eq),
        just('>').to(Token::Gt),
        just('<').to(Token::Lt),
        just('+').to(Token::Plus),
        just('*').to(Token::Star),
        just('/').to(Token::Slash),
        just('%').to(Token::Percent),
    ));

    choice((string, number, word, quoted, punctuation)).map_with(|token, e| SpannedToken {
        token,
        span: e.span(),
    })
}

pub fn lexer<'src>()
-> impl Parser<'src, &'src str, Vec<SpannedToken>, extra::Err<LexError<'src>>> + Clone {
    let ws = ws();

    token()
        .padded_by(ws.clone())
        .repeated()
        .collect::<Vec<_>>()
        .then_ignore(ws)
        .then_ignore(end())
}

pub fn lex(input: &str) -> Result<Vec<SpannedToken>, Vec<LexError<'_>>> {
    let out = lexer().parse(input);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out.into_output().unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator as _;

    use super::*;

    #[test]
    fn the_generators_spell_a_name_like_every_keyword() {
        let lexed = Identifier::iter()
            .map(|keyword| <&'static str>::from(keyword).to_ascii_lowercase())
            .collect::<std::collections::BTreeSet<_>>();
        let generated = nervix_arbitrary::KEYWORDS
            .iter()
            .map(|word| (*word).to_string())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(generated, lexed);
    }

    #[test]
    fn a_name_between_backticks_is_never_a_keyword() {
        let tokens = lex("`end` `input` `a-b` `9 lives` ``")
            .expect("quoted names lex")
            .into_iter()
            .map(|spanned| spanned.token)
            .collect::<Vec<_>>();
        assert_eq!(
            tokens,
            ["end", "input", "a-b", "9 lives", ""]
                .map(|text| Token::Word(Word::Quoted(text.to_string())))
                .to_vec()
        );
    }

    #[test]
    fn a_quoted_name_ends_at_its_closing_backtick_on_its_own_line() {
        for source in ["`end", "`a\nb`", "`a` `b"] {
            assert!(lex(source).is_err(), "{source:?} must be rejected");
        }
    }

    #[test]
    fn lexes_known_words_and_unknown_words() {
        let tokens = lex("CREATE json SCHEMA my_schema").expect("lex should succeed");

        assert_eq!(tokens.len(), 4);
        assert_eq!(
            tokens[0].token,
            Token::Word(Word::KnownWord {
                iden: Identifier::Create,
                raw: "CREATE".to_string(),
            })
        );
        assert_eq!(
            tokens[1].token,
            Token::Word(Word::KnownWord {
                iden: Identifier::Json,
                raw: "json".to_string(),
            })
        );
        assert_eq!(
            tokens[2].token,
            Token::Word(Word::KnownWord {
                iden: Identifier::Schema,
                raw: "SCHEMA".to_string(),
            })
        );
        assert_eq!(
            tokens[3].token,
            Token::Word(Word::UnknownWord("my_schema".to_string()))
        );
    }

    #[test]
    fn lexes_literals_punctuation_and_comments() {
        let input = r#"
            // comment
            ADDRESSES ("kafka-1:9092", 'kafka-2:9092');
            p99 = 99.5
        "#;

        let tokens = lex(input).expect("lex should succeed");
        let only = tokens.into_iter().map(|t| t.token).collect::<Vec<_>>();

        assert_eq!(
            only,
            vec![
                Token::Word(Word::KnownWord {
                    iden: Identifier::Addresses,
                    raw: "ADDRESSES".to_string(),
                }),
                Token::LParen,
                Token::StringLiteral("kafka-1:9092".to_string()),
                Token::Comma,
                Token::StringLiteral("kafka-2:9092".to_string()),
                Token::RParen,
                Token::Semicolon,
                Token::Word(Word::UnknownWord("p99".to_string())),
                Token::Eq,
                Token::NumberLiteral("99.5".to_string()),
            ]
        );
    }

    #[test]
    fn lexes_scientific_number_literals_as_one_token() {
        let tokens = lex("5e-324 1.7976931348623157E+308").expect("numbers should lex");
        assert_eq!(
            tokens
                .into_iter()
                .map(|token| token.token)
                .collect::<Vec<_>>(),
            vec![
                Token::NumberLiteral("5e-324".to_string()),
                Token::NumberLiteral("1.7976931348623157E+308".to_string()),
            ]
        );
    }

    #[test]
    fn lexes_verbatim_multiline_dollar_quoted_strings() {
        let source = "$roto$fn f() {\n  \"quoted\" // not an NSPL comment\n}$roto$";
        let tokens = lex(source).expect("dollar-quoted string should lex");
        assert_eq!(
            tokens
                .into_iter()
                .map(|token| token.token)
                .collect::<Vec<_>>(),
            vec![Token::StringLiteral(
                "fn f() {\n  \"quoted\" // not an NSPL comment\n}".to_string()
            )]
        );
    }

    #[test]
    fn rejects_mismatched_dollar_quote_tags() {
        assert!(lex("$roto$body$other$").is_err());
    }

    fn keywords(source: &str) -> Vec<Option<Identifier>> {
        lex(source)
            .unwrap_or_else(|errors| panic!("{source} must lex: {errors:?}"))
            .into_iter()
            .map(|spanned| match spanned.token {
                Token::Word(Word::KnownWord { iden, .. }) => Some(iden),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn lexes_the_keywords_of_expression_forms_case_insensitively() {
        assert_eq!(
            keywords(
                "if CASE when Then ELSE end in NOT Between is Distinct fRoM TRY_CAST try_cast \
                 Json_Value TRY_JSON_VALUE json_exists"
            ),
            [
                Identifier::If,
                Identifier::Case,
                Identifier::When,
                Identifier::Then,
                Identifier::Else,
                Identifier::End,
                Identifier::In,
                Identifier::Not,
                Identifier::Between,
                Identifier::Is,
                Identifier::Distinct,
                Identifier::From,
                Identifier::TryCast,
                Identifier::TryCast,
                Identifier::JsonValue,
                Identifier::TryJsonValue,
                Identifier::JsonExists,
            ]
            .map(Some)
        );
        assert_eq!(
            keywords("try cast json value from_unix input"),
            [
                None,
                None,
                Some(Identifier::Json),
                None,
                None,
                Some(Identifier::Input)
            ]
        );
    }

    #[test]
    fn an_expression_reserves_only_the_keywords_of_its_own_grammar() {
        let reserved = "WHERE SET INHERIT ALL EXCEPT LEAK SENSITIVE INVOKE AS TRY_CAST JSON_VALUE \
                        TRY_JSON_VALUE JSON_EXISTS AND OR NOT TRUE FALSE NULL IF CASE WHEN THEN \
                        ELSE END IN BETWEEN IS DISTINCT FROM UDF";
        for keyword in keywords(reserved) {
            let keyword = keyword.expect("every reserved word is a keyword");
            assert!(
                keyword.is_expression_keyword(),
                "{keyword:?} must be reserved"
            );
        }
        let names = "max min sum first last count input output message branch left right error \
                     status key time timestamp type value replace filter by method path";
        for keyword in keywords(names).into_iter().flatten() {
            assert!(
                !keyword.is_expression_keyword(),
                "{keyword:?} must name a term"
            );
        }
    }

    #[test]
    fn lexes_the_udf_qualifier_as_a_keyword_and_a_double_colon() {
        let tokens = lex("UdF::mask")
            .expect("UDF qualifier must lex")
            .into_iter()
            .map(|token| token.token)
            .collect::<Vec<_>>();
        assert_eq!(
            tokens,
            vec![
                Token::Word(Word::KnownWord {
                    iden: Identifier::Udf,
                    raw: "UdF".to_string(),
                }),
                Token::DoubleColon,
                Token::Word(Word::UnknownWord("mask".to_string())),
            ]
        );
    }

    #[test]
    fn a_number_is_one_token_only_without_spaces_around_its_dot() {
        let numbers = |source: &str| {
            lex(source)
                .unwrap_or_else(|errors| panic!("{source} must lex: {errors:?}"))
                .into_iter()
                .map(|token| token.token)
                .collect::<Vec<_>>()
        };
        let number = |raw: &str| Token::NumberLiteral(raw.to_string());
        assert_eq!(numbers("1.5"), vec![number("1.5")]);
        for source in ["1 .5", "1. 5", "1 . 5"] {
            assert_eq!(
                numbers(source),
                vec![number("1"), Token::Dot, number("5")],
                "{source}"
            );
        }
        assert_eq!(numbers("1."), vec![number("1"), Token::Dot]);
        assert_eq!(numbers(".5"), vec![Token::Dot, number("5")]);
    }

    #[test]
    fn lexes_every_backslash_sequence_verbatim() {
        let cases = [
            (r"'a\\b'", r"a\\b"),
            (r#"'a\"b'"#, r#"a\"b"#),
            (r#""a\'b""#, r"a\'b"),
            (r"'a\nb'", r"a\nb"),
            (r#""a\rb""#, r"a\rb"),
            (r"'a\tb'", r"a\tb"),
            (r"'a\0b'", r"a\0b"),
            (r#""a\x41b""#, r"a\x41b"),
            (r"'a\u{e9}b'", r"a\u{e9}b"),
            (r"'a\$b'", r"a\$b"),
            (r"'a\'", r"a\"),
            (r#""a\""#, r"a\"),
            (r"$$a\nb\$$", r"a\nb\"),
        ];
        for (source, value) in cases {
            let tokens =
                lex(source).unwrap_or_else(|errors| panic!("{source} must lex: {errors:?}"));
            assert_eq!(
                tokens
                    .into_iter()
                    .map(|token| token.token)
                    .collect::<Vec<_>>(),
                vec![Token::StringLiteral(value.to_string())],
                "{source}"
            );
        }
    }

    #[test]
    fn a_backslash_before_the_closing_quote_leaves_it_closing() {
        for source in [r"'a\'b'", r#""a\"b""#] {
            assert!(lex(source).is_err(), "{source} must be rejected");
        }
    }

    #[test]
    fn known_words_are_case_insensitive() {
        let tokens = lex("nO_aCk").expect("lex should succeed");
        assert_eq!(tokens.len(), 1);
        assert_eq!(
            tokens[0].token,
            Token::Word(Word::KnownWord {
                iden: Identifier::NoAck,
                raw: "nO_aCk".to_string(),
            })
        );
    }
}
