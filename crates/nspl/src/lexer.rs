use std::str::FromStr;

use chumsky::prelude::*;
use strum::{AsRefStr, EnumString, IntoStaticStr};

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
    Colon,
    Dot,
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
    KnownWord { iden: Identifier, raw: String },
    UnknownWord(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, AsRefStr, IntoStaticStr)]
#[strum(ascii_case_insensitive, serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum Identifier {
    Create,
    Alter,
    Drop,
    Cordon,
    Uncordon,
    Drain,
    Use,
    List,
    Start,
    Stop,
    Describe,
    Lookup,
    Upload,
    Resource,
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
    Subscribe,
    Unsubscribe,
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
    Router,
    Forwarder,
    On,
    Connect,
    Message,
    Branch,
    General,
    Global,
    Error,
    Ignore,
    Log,
    Sensitive,
    Dlq,
    Path,
    Http,
    Websockets,
    Parameterized,
    Parametrized,
    Unparameterized,
    KafkaBroker,
    Addresses,
    With,
    Materialized,
    State,
    Last,
    Tls,
    Jaq,
    Transformation,
    Transformations,
    Ingestion,
    Emitting,
    Json,
    Yaml,
    Toml,
    Xml,
    Avro,
    Cbor,
    Protobuf,
    Wire,
    Schema,
    Codec,
    Ingestor,
    Into,
    Relay,
    Processor,
    Window,
    Unifier,
    Deduplicator,
    Correlator,
    Decode,
    Using,
    From,
    Rfc3339,
    File,
    Inputs,
    Outputs,
    Hash,
    Key,
    Kafka,
    Pulsar,
    Kinesis,
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
    Clean,
    Persistent,
    Qos,
    Mode,
    Ack,
    NoAck,
    Parallel,
    Sequential,
    Batch,
    Sample,
    Blocking,
    Dropping,
    Flush,
    Commit,
    Immediate,
    Retry,
    Policy,
    Backoff,
    Max,
    Min,
    Query,
    Every,
    Each,
    Set,
    Output,
    Unset,
    Attached,
    Detached,
    Processed,
    By,
    SlidingWindow,
    Size,
    Step,
    Width,
    Duration,
    Aggregate,
    Filter,
    Where,
    Default,
    And,
    Or,
    Not,
    True,
    False,
    Deduplicate,
    Match,
    First,
    All,
    Earliest,
    Latest,
    Correlation,
    Conflict,
    Send,
    Wait,
    Body,
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

fn token<'src>() -> impl Parser<'src, &'src str, SpannedToken, extra::Err<LexError<'src>>> + Clone {
    let word = text::ascii::ident().map(|raw: &str| Token::Word(classify_word(raw)));

    let number = text::int(10)
        .then(just('.').then(text::digits(10)).or_not())
        .to_slice()
        .map(|n: &str| Token::NumberLiteral(n.to_string()));

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

    let string =
        choice((single_string, double_string)).map(|s: &str| Token::StringLiteral(s.to_string()));

    let punctuation = choice((
        just("!=").to(Token::NotEq),
        just(">=").to(Token::GtEq),
        just("<=").to(Token::LtEq),
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

    choice((string, number, word, punctuation)).map_with(|token, e| SpannedToken {
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
    use super::*;

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
