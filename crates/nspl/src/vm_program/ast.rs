use arrow_schema::DataType;
use chumsky::span::{SimpleSpan, Spanned};

pub type Span = SimpleSpan<usize>;
pub type SpannedNode<T> = Spanned<T, Span>;
pub type SpannedExpr = SpannedNode<Expr>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldRef {
    pub relay: String,
    pub field: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InternalFieldNamespace {
    LookupHashMap,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InternalFieldRef {
    pub namespace: InternalFieldNamespace,
    pub field: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Program {
    pub filter: Option<SpannedExpr>,
    pub branch_filters: Vec<SpannedExpr>,
    pub set: Vec<(FieldRef, SpannedExpr)>,
    pub unset: Vec<FieldRef>,
    pub invoke: Vec<SpannedInvocation>,
}

impl Program {
    pub fn rewrite_unset_sources_to_destination(
        &mut self,
        source_relays: &[String],
        destination_relay: &str,
    ) {
        for field_ref in &mut self.unset {
            if source_relays
                .iter()
                .any(|source_relay| source_relay == &field_ref.relay)
            {
                field_ref.relay = destination_relay.to_string();
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Invocation {
    pub function: FunctionName,
    pub args: Vec<SpannedExpr>,
}

pub type SpannedInvocation = SpannedNode<Invocation>;

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Literal),
    FieldRef(FieldRef),
    InternalFieldRef(InternalFieldRef),
    Unary {
        op: UnaryOp,
        expr: Box<SpannedExpr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<SpannedExpr>,
        right: Box<SpannedExpr>,
    },
    Cast {
        expr: Box<SpannedExpr>,
        data_type: DataType,
    },
    Call {
        function: FunctionName,
        args: Vec<SpannedExpr>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FunctionName {
    Now,
    UuidV4,
    UuidV7,
    Lower,
    Upper,
    Trim,
    Btrim,
    Ltrim,
    Rtrim,
    Length,
    CharLength,
    BitLength,
    Ascii,
    Coalesce,
    IsNull,
    NullIf,
    Abs,
    Acos,
    Asin,
    Atan,
    Ceil,
    Cos,
    Exp,
    Floor,
    Initcap,
    Left,
    Ln,
    Log,
    Lpad,
    Md5,
    Pow,
    Repeat,
    Replace,
    Reverse,
    Right,
    Round,
    Rpad,
    SplitPart,
    Sqrt,
    Strpos,
    Substr,
    Tan,
    ToHex,
    Translate,
    Concat,
    Sum,
    Last,
    First,
    Count,
    Nth,
    Contains,
    StartsWith,
    EndsWith,
    RegexpLike,
    RegexpReplace,
    RegexpSubstr,
    LeakSensitive,
    LookupHashMap,
    ReadHeader,
    ReadHeaders,
    WriteHeader,
    Unknown(String),
}

impl FunctionName {
    pub fn parse(name: &str) -> Self {
        match name.to_ascii_lowercase().as_str() {
            "now" => Self::Now,
            "uuid_v4" => Self::UuidV4,
            "uuid_v7" => Self::UuidV7,
            "lower" => Self::Lower,
            "upper" => Self::Upper,
            "trim" => Self::Trim,
            "btrim" => Self::Btrim,
            "ltrim" => Self::Ltrim,
            "rtrim" => Self::Rtrim,
            "length" => Self::Length,
            "char_length" => Self::CharLength,
            "bit_length" => Self::BitLength,
            "ascii" => Self::Ascii,
            "coalesce" => Self::Coalesce,
            "is_null" => Self::IsNull,
            "nullif" => Self::NullIf,
            "abs" => Self::Abs,
            "acos" => Self::Acos,
            "asin" => Self::Asin,
            "atan" => Self::Atan,
            "ceil" | "ceiling" => Self::Ceil,
            "cos" => Self::Cos,
            "exp" => Self::Exp,
            "floor" => Self::Floor,
            "initcap" => Self::Initcap,
            "left" => Self::Left,
            "ln" => Self::Ln,
            "log" => Self::Log,
            "lpad" => Self::Lpad,
            "md5" => Self::Md5,
            "pow" | "power" => Self::Pow,
            "repeat" => Self::Repeat,
            "replace" => Self::Replace,
            "reverse" => Self::Reverse,
            "right" => Self::Right,
            "round" => Self::Round,
            "rpad" => Self::Rpad,
            "split_part" => Self::SplitPart,
            "sqrt" => Self::Sqrt,
            "strpos" => Self::Strpos,
            "substr" | "substring" => Self::Substr,
            "tan" => Self::Tan,
            "to_hex" => Self::ToHex,
            "translate" => Self::Translate,
            "concat" => Self::Concat,
            "sum" => Self::Sum,
            "last" => Self::Last,
            "first" => Self::First,
            "count" => Self::Count,
            "nth" => Self::Nth,
            "contains" => Self::Contains,
            "starts_with" => Self::StartsWith,
            "ends_with" => Self::EndsWith,
            "regexp_like" => Self::RegexpLike,
            "regexp_replace" => Self::RegexpReplace,
            "regexp_substr" => Self::RegexpSubstr,
            "leak_sensitive" => Self::LeakSensitive,
            "lookup_hash_map" => Self::LookupHashMap,
            "read_header" => Self::ReadHeader,
            "read_headers" => Self::ReadHeaders,
            "write_header" => Self::WriteHeader,
            _ => Self::Unknown(name.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Now => "now",
            Self::UuidV4 => "uuid_v4",
            Self::UuidV7 => "uuid_v7",
            Self::Lower => "lower",
            Self::Upper => "upper",
            Self::Trim => "trim",
            Self::Btrim => "btrim",
            Self::Ltrim => "ltrim",
            Self::Rtrim => "rtrim",
            Self::Length => "length",
            Self::CharLength => "char_length",
            Self::BitLength => "bit_length",
            Self::Ascii => "ascii",
            Self::Coalesce => "coalesce",
            Self::IsNull => "is_null",
            Self::NullIf => "nullif",
            Self::Abs => "abs",
            Self::Acos => "acos",
            Self::Asin => "asin",
            Self::Atan => "atan",
            Self::Ceil => "ceil",
            Self::Cos => "cos",
            Self::Exp => "exp",
            Self::Floor => "floor",
            Self::Initcap => "initcap",
            Self::Left => "left",
            Self::Ln => "ln",
            Self::Log => "log",
            Self::Lpad => "lpad",
            Self::Md5 => "md5",
            Self::Pow => "pow",
            Self::Repeat => "repeat",
            Self::Replace => "replace",
            Self::Reverse => "reverse",
            Self::Right => "right",
            Self::Round => "round",
            Self::Rpad => "rpad",
            Self::SplitPart => "split_part",
            Self::Sqrt => "sqrt",
            Self::Strpos => "strpos",
            Self::Substr => "substr",
            Self::Tan => "tan",
            Self::ToHex => "to_hex",
            Self::Translate => "translate",
            Self::Concat => "concat",
            Self::Sum => "sum",
            Self::Last => "last",
            Self::First => "first",
            Self::Count => "count",
            Self::Nth => "nth",
            Self::Contains => "contains",
            Self::StartsWith => "starts_with",
            Self::EndsWith => "ends_with",
            Self::RegexpLike => "regexp_like",
            Self::RegexpReplace => "regexp_replace",
            Self::RegexpSubstr => "regexp_substr",
            Self::LeakSensitive => "leak_sensitive",
            Self::LookupHashMap => "lookup_hash_map",
            Self::ReadHeader => "read_header",
            Self::ReadHeaders => "read_headers",
            Self::WriteHeader => "write_header",
            Self::Unknown(name) => name.as_str(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Int64(i64),
    Float64(f64),
    Bool(bool),
    String(String),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOp {
    Neg,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
    Eq,
    NotEq,
    Gt,
    Lt,
    GtEq,
    LtEq,
    And,
    Or,
}

pub(crate) fn spanned<T>(inner: T, span: Span) -> SpannedNode<T> {
    Spanned { inner, span }
}

pub(crate) fn merge_spans(left: &Span, right: &Span) -> Span {
    (left.start..right.end).into()
}
