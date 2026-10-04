//! Names, string literals, durations and byte sizes as NSPL and the vocabulary hold them.

use std::{fmt::Debug, num::NonZeroU64, str::FromStr};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    BranchName, BuiltinFunctionName, ChannelName, ClientName, ClusterNodeName, CodecName,
    CollectionName, ConsumerGroupName, CorrelatorName, DeduplicatorName, DomainClockPeriod,
    DomainClockSkew, DomainName, EmitterName, EndpointName, FieldName, GeneratorName,
    InferencerName, IngestorName, JunctionName, LookupName, ModelName, PlacementName,
    PulsarSubscriptionName, QueueGroupName, QueueName, ReingestorName, RelayName, ReordererName,
    RequestedResourceVersion, ResourceName, SchemaName, SignalingProtocolName, SubjectName,
    SubscriptionName, TableName, TopicName, UdfName, UserName, VhostName, WasmProcessorName,
    WindowProcessorName, WireSchemaName,
};

use crate::{Arbitrary, Domain};

/// The longest name the vocabulary accepts, in bytes. It mirrors the bound every name type
/// enforces; a generated name past it fails to parse, loudly, the moment the two disagree.
const NAME_BYTES: u64 = 128;

/// The characters a generated name continues with after its keyword-proof head.
const NAME_TAIL: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789_";

/// Every keyword NSPL reads, statement and expression keywords alike, spelled as a name spells it:
/// in lower case. `nervix-nspl` checks this list against the one lexer's keyword set, so a keyword
/// the language adds is generated as a name as soon as it exists.
pub const KEYWORDS: [&str; 354] = [
    "create",
    "delete",
    "add",
    "alter",
    "drop",
    "rename",
    "cordon",
    "uncordon",
    "drain",
    "rebind",
    "relocate",
    "relocation",
    "use",
    "list",
    "attach",
    "detach",
    "begin",
    "start",
    "stop",
    "describe",
    "lookup",
    "upload",
    "backup",
    "restore",
    "existing",
    "dry",
    "run",
    "resource",
    "resources",
    "show",
    "if",
    "exists",
    "cluster",
    "status",
    "node",
    "version",
    "paced",
    "unpaced",
    "domain",
    "domains",
    "clock",
    "transaction",
    "transactions",
    "operation",
    "placement",
    "placements",
    "rank",
    "require",
    "prefer",
    "suggest",
    "preferences",
    "follow",
    "onto",
    "colocation",
    "separation",
    "neutral",
    "user",
    "password",
    "period",
    "skew",
    "intersection",
    "at",
    "now",
    "time",
    "rate",
    "timestamp",
    "session",
    "subscription",
    "vhost",
    "endpoint",
    "signaling",
    "protocol",
    "generator",
    "inferencer",
    "wasm",
    "reingestor",
    "reorderer",
    "on",
    "connect",
    "message",
    "input",
    "branch",
    "branches",
    "general",
    "global",
    "error",
    "ignore",
    "log",
    "rejected",
    "preserve",
    "reset",
    "in",
    "sensitive",
    "branched",
    "unbranched",
    "path",
    "method",
    "without",
    "body",
    "http",
    "websockets",
    "kafka_broker",
    "addresses",
    "with",
    "materialized",
    "state",
    "last",
    "tls",
    "jaq",
    "transformations",
    "ingestion",
    "emitting",
    "strict",
    "loose",
    "json",
    "yaml",
    "toml",
    "xml",
    "avro",
    "cbor",
    "raw",
    "protobuf",
    "wire",
    "schema",
    "field",
    "codec",
    "ingestor",
    "ingestors",
    "into",
    "relay",
    "route",
    "processor",
    "window",
    "junction",
    "deduplicator",
    "correlator",
    "decode",
    "using",
    "from",
    "left",
    "right",
    "rfc3339",
    "file",
    "inputs",
    "inner_input",
    "inner_output",
    "dense",
    "tensor",
    "hash",
    "key",
    "kafka",
    "pulsar",
    "clickhouse",
    "postgres",
    "mysql",
    "mongodb",
    "s3",
    "gcs",
    "azure_blob",
    "iceberg",
    "iceberg_rest",
    "prometheus",
    "mqtt",
    "nats",
    "rabbitmq",
    "redis",
    "pubsub",
    "zeromq",
    "sqs",
    "sentry",
    "syslog",
    "otel",
    "logs",
    "traces",
    "metric",
    "attributes",
    "scope",
    "gauge",
    "sum",
    "histogram",
    "unit",
    "description",
    "monotonic",
    "delta",
    "cumulative",
    "collect",
    "collection",
    "broker",
    "capacity",
    "ttl",
    "topic",
    "subject",
    "queue",
    "channel",
    "offset",
    "consumer",
    "group",
    "instances",
    "evict",
    "lru",
    "clean",
    "persistent",
    "qos",
    "mode",
    "quiesce",
    "suspend",
    "buffer",
    "overflow",
    "oldest",
    "newest",
    "reject",
    "after",
    "ack",
    "no_ack",
    "jetstream",
    "parallel",
    "sequential",
    "single",
    "batch",
    "fifo",
    "dynamic",
    "sample",
    "blocking",
    "dropping",
    "flush",
    "commit",
    "revert",
    "immediate",
    "retry",
    "policy",
    "backoff",
    "max",
    "fuel",
    "memory",
    "min",
    "query",
    "every",
    "each",
    "for",
    "set",
    "replace",
    "inherit",
    "except",
    "leak",
    "invoke",
    "output",
    "attached",
    "detached",
    "processed",
    "by",
    "sliding_window",
    "size",
    "pool",
    "step",
    "width",
    "duration",
    "filter",
    "where",
    "and",
    "or",
    "not",
    "true",
    "false",
    "case",
    "when",
    "then",
    "else",
    "end",
    "between",
    "is",
    "distinct",
    "try_cast",
    "json_value",
    "try_json_value",
    "json_exists",
    "deduplicate",
    "match",
    "correlate",
    "first",
    "all",
    "earliest",
    "latest",
    "correlation",
    "conflict",
    "send",
    "wait",
    "fail",
    "capture",
    "accept",
    "data",
    "format",
    "text",
    "required",
    "skip",
    "default",
    "emitter",
    "encode",
    "emit",
    "insert",
    "to",
    "do",
    "update",
    "nothing",
    "timeout",
    "parse",
    "as",
    "messages",
    "oneof",
    "client",
    "type",
    "mount",
    "config",
    "table",
    "catalog",
    "same",
    "location",
    "values",
    "optional",
    "string",
    "number",
    "integer",
    "object",
    "array",
    "vec",
    "boolean",
    "null",
    "int",
    "long",
    "float",
    "double",
    "bytes",
    "record",
    "enum",
    "map",
    "fixed",
    "u8",
    "i8",
    "u16",
    "i16",
    "u32",
    "i32",
    "u64",
    "i64",
    "bool",
    "datetime",
    "f32",
    "f64",
    "udf",
    "udfs",
    "args",
    "returns",
    "volatile",
    "code",
    "roto_0_13",
];

/// The words NSPL reserves wherever it reads a relay, so a relay is never named by one.
const RESERVED_RELAY_WORDS: [&str; 2] = ["message", "branch"];

/// A kind of name the generators build, and the keyword spellings NSPL refuses for that kind.
pub trait GeneratedName: FromStr<Err: Debug> {
    /// The keywords NSPL refuses as a name of this kind. A name in the NSPL domain never spells one;
    /// the vocabulary holds them, and so does the vocabulary domain. Every such list holds at most
    /// a handful of words.
    const REFUSED_IN_NSPL: &'static [&'static str] = &[];
}

/// Declares name kinds NSPL writes in every spelling a generated name takes, keywords included.
macro_rules! spelled_like_any_word {
    ($($name:ident),+ $(,)?) => {
        $(impl GeneratedName for $name {})+
    };
}

spelled_like_any_word!(
    BranchName,
    BuiltinFunctionName,
    ChannelName,
    ClientName,
    ClusterNodeName,
    CodecName,
    CollectionName,
    ConsumerGroupName,
    CorrelatorName,
    DeduplicatorName,
    DomainName,
    EmitterName,
    EndpointName,
    FieldName,
    GeneratorName,
    InferencerName,
    IngestorName,
    JunctionName,
    LookupName,
    ModelName,
    PlacementName,
    PulsarSubscriptionName,
    QueueGroupName,
    QueueName,
    ReingestorName,
    ReordererName,
    ResourceName,
    SchemaName,
    SignalingProtocolName,
    SubjectName,
    SubscriptionName,
    TableName,
    TopicName,
    UdfName,
    UserName,
    VhostName,
    WasmProcessorName,
    WindowProcessorName,
    WireSchemaName,
);

/// NSPL reads `message` and `branch` as the keywords of its scopes and clauses wherever it reads a
/// relay, so it refuses them as a relay's name.
impl GeneratedName for RelayName {
    const REFUSED_IN_NSPL: &'static [&'static str] = &RESERVED_RELAY_WORDS;
}

/// Pieces a generated string is assembled from. Beside plain text they hold every character the
/// NSPL lexer treats specially: both quote styles, the dollar-quote delimiters a renderer picks
/// and prefixes of them, backslashes and the escapes they could start, line breaks, a comma and
/// braces that separate configuration entries, and text outside ASCII, including a combining mark
/// and characters Unicode classes as whitespace.
const STRING_PIECES: [&str; 40] = [
    "a", "Z", "0", "9", " ", "  ", "_", "-", ".", "/", "'", "\"", "'\"", "$", "$s", "$s$", "s$",
    "$s_1$", "$roto", "$roto$", "\\", "\\n", "\\t", "\\\\", "\n", "\r", "\r\n", "\t", "\0", ",",
    "{", "}", "=", ";", "//", "é", "中文", "🎉", "e\u{301}", "\u{2028}",
];

/// Units a duration is written with, as NSPL keeps the spelling it was given.
const DURATION_UNITS: [&str; 14] = [
    "ns", "us", "ms", "s", "sec", "m", "min", "h", "hr", "d", "days", "w", "M", "y",
];

/// Units a byte size is written with.
const BYTE_SIZE_UNITS: [&str; 9] = ["B", "KB", "KiB", "MB", "MiB", "GB", "GiB", "TB", "TiB"];

impl Arbitrary<'_> {
    /// A name of kind `N`: one lower-case ASCII identifier, which may spell any NSPL keyword. In the
    /// NSPL domain it never spells a keyword NSPL refuses for that kind of name.
    pub fn name<N: GeneratedName>(&mut self) -> N {
        self.name_refusing(N::REFUSED_IN_NSPL)
    }

    /// A name of kind `N` that, in the NSPL domain, spells none of `refused` either, for a position
    /// that refuses more words than the kind does. `refused` holds at most a handful of words.
    pub fn name_refusing<N: GeneratedName>(&mut self, refused: &[&str]) -> N {
        let text = self.name_text_refusing::<N>(refused);
        N::from_str(&text).assured("a generated name is a lower-case identifier within the bound")
    }

    /// The text of a name [`Self::name_refusing`] would build. A refused keyword gains a trailing
    /// underscore, which no keyword ends with.
    pub(crate) fn name_text_refusing<N: GeneratedName>(&mut self, refused: &[&str]) -> String {
        let mut text = self.name_text();
        if self.domain == Domain::Nspl
            && (refused.contains(&text.as_str()) || N::REFUSED_IN_NSPL.contains(&text.as_str()))
        {
            text.push('_');
        }
        text
    }

    /// A name of kind `N` as an expression or a route construction writes it. Besides any name
    /// [`Self::name`] builds, it may hold what only its spelling between backticks can: a `-`, a `~`
    /// or a `.`, or a leading digit.
    pub fn expression_name<N: GeneratedName>(&mut self) -> N {
        let text = self.expression_name_text::<N>();
        N::from_str(&text).assured("a generated name holds only name characters within the bound")
    }

    /// The text of a name [`Self::expression_name`] would build.
    ///
    /// Holding such a character is one choice among eight and never the first, so a name read from
    /// bytes that ran out is still a plain identifier.
    pub(crate) fn expression_name_text<N: GeneratedName>(&mut self) -> String {
        let mut text = self.name_text_refusing::<N>(&[]);
        if self.entropy.byte() % 8 != 7 {
            return text;
        }
        let length = u64::try_from(text.len()).assured("a name's length fits in u64");
        if length >= NAME_BYTES {
            text.pop();
        }
        match self.entropy.byte() % 4 {
            0 => {
                let digit = char::from(
                    b'0'.checked_add(self.entropy.byte() % 10)
                        .assured("a digit offset below ten stays inside the ASCII digits"),
                );
                text.insert(0, digit);
            }
            1 => text.push('-'),
            2 => text.insert(0, '~'),
            _ => text.push('.'),
        }
        text
    }

    /// The text of a name [`Self::name`] would build: an NSPL keyword, one letter, a letter and an
    /// underscore followed by more, or an underscore followed by more, so the length reaches the
    /// vocabulary's bound.
    ///
    /// A keyword is one choice among four and never the first, so a name read from bytes that ran
    /// out is a single letter, the smallest name there is.
    pub fn name_text(&mut self) -> String {
        if self.entropy.byte() % 4 == 3 {
            return self.entropy.pick(KEYWORDS).to_string();
        }
        let letter = char::from(
            b'a'.checked_add(self.entropy.byte() % 26)
                .assured("a letter offset below 26 stays inside the ASCII lower-case range"),
        );
        let mut text = String::new();
        match self.entropy.byte() % 3 {
            0 => {
                text.push(letter);
                return text;
            }
            1 => {
                text.push(letter);
                text.push('_');
            }
            _ => text.push('_'),
        }
        let head = u64::try_from(text.len()).assured("a two-byte head fits in u64");
        let room = NAME_BYTES
            .checked_sub(head)
            .verified("the head is shorter than the name bound");
        let tail = self.entropy.boundary_biased(0..=room);
        for _ in 0..tail {
            let tail_count = u64::try_from(NAME_TAIL.len()).assured("a small table fits in u64");
            let chosen = self.entropy.up_to(
                tail_count
                    .checked_sub(1)
                    .assured("the tail alphabet is not empty"),
            );
            let chosen = usize::try_from(chosen).verified("an index below the table length");
            text.push(char::from(NAME_TAIL[chosen]));
        }
        text
    }

    /// Any string, assembled from pieces that stress every quoting and escaping rule and from code
    /// points drawn across the whole Unicode range.
    pub fn string(&mut self) -> String {
        let pieces = self.entropy.count(10);
        let mut text = String::new();
        for _ in 0..pieces {
            if self.entropy.flag() {
                let piece = self.entropy.pick(STRING_PIECES);
                text.push_str(piece);
            } else {
                text.push(self.code_point());
            }
        }
        text
    }

    /// A string holding at least one character.
    pub fn non_empty_string(&mut self) -> String {
        let mut text = self.string();
        if text.is_empty() {
            text.push(self.code_point());
        }
        text
    }

    /// One Unicode scalar value. Surrogate code points are not characters, so a choice among them
    /// takes the replacement character instead.
    fn code_point(&mut self) -> char {
        let value = self.entropy.between(0..=0x10_FFFF);
        let value = u32::try_from(value).verified("the range above ends below u32::MAX");
        match char::from_u32(value) {
            Some(character) => character,
            None => char::REPLACEMENT_CHARACTER,
        }
    }

    /// A duration as NSPL keeps it: a whole number and a unit, spelled as written.
    pub fn duration(&mut self) -> String {
        let count = self.entropy.boundary_biased(0..=u64::from(u32::MAX));
        let unit = self.entropy.pick(DURATION_UNITS);
        format!("{count}{unit}")
    }

    /// A byte size as NSPL keeps it: a whole number and a unit, spelled as written.
    pub fn byte_size(&mut self) -> String {
        let count = self.entropy.boundary_biased(0..=u64::from(u16::MAX));
        let unit = self.entropy.pick(BYTE_SIZE_UNITS);
        format!("{count}{unit}")
    }

    /// A byte size of at least one byte, for a bound where zero would hold nothing.
    pub fn positive_byte_size(&mut self) -> String {
        let count = self.entropy.boundary_biased(1..=u64::from(u16::MAX));
        let unit = self.entropy.pick(BYTE_SIZE_UNITS);
        format!("{count}{unit}")
    }

    /// A positive domain-clock period: any number of nanoseconds a `u64` holds.
    pub fn clock_period(&mut self) -> DomainClockPeriod {
        let nanos = self.entropy.boundary_biased(1..=u64::MAX);
        DomainClockPeriod::from_nanos(
            NonZeroU64::new(nanos).verified("the range above starts at one"),
        )
    }

    /// A domain-clock skew: any number of nanoseconds a `u64` holds, zero included.
    pub fn clock_skew(&mut self) -> DomainClockSkew {
        DomainClockSkew::from_nanos(self.entropy.any_u64())
    }

    /// A positive count a `u64` holds.
    pub fn positive_u64(&mut self) -> NonZeroU64 {
        let value = self.entropy.boundary_biased(1..=u64::MAX);
        NonZeroU64::new(value).verified("the range above starts at one")
    }

    /// A resource version a statement asks for: a number, or the latest completed version.
    pub fn requested_version(&mut self) -> RequestedResourceVersion {
        if self.entropy.flag() {
            RequestedResourceVersion::Latest
        } else {
            RequestedResourceVersion::Number(self.entropy.any_u64())
        }
    }
}
