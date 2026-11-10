//! Builtin name-to-lowering registration and semantic projection.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The exhaustive builtin catalog and its lowering/semantic descriptors.
//! - **Depends on.** VM function names and the semantic contracts of each lowering.
//! - **Must not know.** Registers, payload values, graph execution or scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns builtin catalog registration")
)]

use super::*;

pub fn builtin_descriptor(function: &FunctionName) -> Option<BuiltinDescriptor> {
    let lowering = match function {
        FunctionName::Now => BuiltinLowering::Now,
        FunctionName::UuidV4 => BuiltinLowering::UuidV4,
        FunctionName::UuidV7 => BuiltinLowering::UuidV7,
        FunctionName::Lower => BuiltinLowering::Lower,
        FunctionName::Upper => BuiltinLowering::Upper,
        FunctionName::Trim => BuiltinLowering::Trim,
        FunctionName::Btrim => BuiltinLowering::Btrim,
        FunctionName::Ltrim => BuiltinLowering::Ltrim,
        FunctionName::Rtrim => BuiltinLowering::Rtrim,
        FunctionName::Length => BuiltinLowering::Length,
        FunctionName::CharLength => BuiltinLowering::CharLength,
        FunctionName::BitLength => BuiltinLowering::BitLength,
        FunctionName::OctetLength => BuiltinLowering::OctetLength,
        FunctionName::Ascii => BuiltinLowering::Ascii,
        FunctionName::Coalesce => BuiltinLowering::Coalesce,
        FunctionName::IsNull => BuiltinLowering::IsNull,
        FunctionName::NullIf => BuiltinLowering::NullIf,
        FunctionName::Abs => BuiltinLowering::Abs,
        FunctionName::Acos => BuiltinLowering::Acos,
        FunctionName::Asin => BuiltinLowering::Asin,
        FunctionName::Atan => BuiltinLowering::Atan,
        FunctionName::Ceil => BuiltinLowering::Ceil,
        FunctionName::Concat => BuiltinLowering::Concat,
        FunctionName::ConcatWs => BuiltinLowering::ConcatWs,
        FunctionName::Split => BuiltinLowering::Split,
        FunctionName::Join => BuiltinLowering::Join,
        FunctionName::Like => BuiltinLowering::Like,
        FunctionName::ILike => BuiltinLowering::ILike,
        FunctionName::ContainsAny => BuiltinLowering::ContainsAny(ContainsAnyCall::Dynamic {
            capacity: crate::VmSettings::default().pattern_sets_per_batch(),
        }),
        FunctionName::NormalizeNfc => BuiltinLowering::NormalizeNfc,
        FunctionName::Array => BuiltinLowering::ArrayConstruct,
        FunctionName::Vec => BuiltinLowering::VecConstruct,
        FunctionName::Overlap => BuiltinLowering::Overlap,
        FunctionName::Slice => BuiltinLowering::Slice,
        FunctionName::Min => BuiltinLowering::ListMin,
        FunctionName::Max => BuiltinLowering::ListMax,
        FunctionName::Mean => BuiltinLowering::Mean,
        FunctionName::ListAdd => BuiltinLowering::ListArithmetic(ListArithmetic::Add),
        FunctionName::ListSub => BuiltinLowering::ListArithmetic(ListArithmetic::Subtract),
        FunctionName::ListMul => BuiltinLowering::ListArithmetic(ListArithmetic::Multiply),
        FunctionName::ListDiv => BuiltinLowering::ListArithmetic(ListArithmetic::Divide),
        FunctionName::ListAddScalar => BuiltinLowering::ListArithmetic(ListArithmetic::AddScalar),
        FunctionName::ListSubScalar => {
            BuiltinLowering::ListArithmetic(ListArithmetic::SubtractScalar)
        }
        FunctionName::ListMulScalar => {
            BuiltinLowering::ListArithmetic(ListArithmetic::MultiplyScalar)
        }
        FunctionName::ListDivScalar => {
            BuiltinLowering::ListArithmetic(ListArithmetic::DivideScalar)
        }
        FunctionName::ListAbs => BuiltinLowering::ListArithmetic(ListArithmetic::AbsoluteValue),
        FunctionName::Dot => BuiltinLowering::Dot,
        FunctionName::Distance => BuiltinLowering::Distance,
        FunctionName::Sum => BuiltinLowering::Sum,
        FunctionName::Last => BuiltinLowering::Last,
        FunctionName::First => BuiltinLowering::First,
        FunctionName::Count => BuiltinLowering::Count,
        FunctionName::Nth => BuiltinLowering::Nth,
        FunctionName::Contains => BuiltinLowering::Contains,
        FunctionName::Cos => BuiltinLowering::Cos,
        FunctionName::StartsWith => BuiltinLowering::StartsWith,
        FunctionName::EndsWith => BuiltinLowering::EndsWith,
        FunctionName::Exp => BuiltinLowering::Exp,
        FunctionName::Floor => BuiltinLowering::Floor,
        FunctionName::Initcap => BuiltinLowering::Initcap,
        FunctionName::Left => BuiltinLowering::Left,
        FunctionName::Ln => BuiltinLowering::Ln,
        FunctionName::Log => BuiltinLowering::Log,
        FunctionName::Lpad => BuiltinLowering::Lpad,
        FunctionName::Md5 => BuiltinLowering::Md5,
        FunctionName::BytesFromUtf8 => BuiltinLowering::BytesFromUtf8,
        FunctionName::BytesToUtf8 => BuiltinLowering::BytesToUtf8,
        FunctionName::Base64Encode => BuiltinLowering::Base64Encode,
        FunctionName::Base64Decode => BuiltinLowering::Base64Decode,
        FunctionName::HexEncode => BuiltinLowering::HexEncode,
        FunctionName::HexDecode => BuiltinLowering::HexDecode,
        FunctionName::Sha256 => BuiltinLowering::Sha256,
        FunctionName::Xxh3_64 => BuiltinLowering::Xxh3_64,
        FunctionName::IpFromString => BuiltinLowering::IpFromString,
        FunctionName::IsIpAddress => BuiltinLowering::IsIpAddress,
        FunctionName::IpToString => BuiltinLowering::IpToString,
        FunctionName::IpFamily => BuiltinLowering::IpFamily,
        FunctionName::IpTrunc => BuiltinLowering::IpTrunc,
        FunctionName::IpInNetwork => BuiltinLowering::IpInNetwork(NetworkSource::Argument),
        FunctionName::IpUnmap => BuiltinLowering::IpUnmap,
        FunctionName::UrlScheme => BuiltinLowering::UrlScheme,
        FunctionName::UrlHost => BuiltinLowering::UrlHost,
        FunctionName::UrlPort => BuiltinLowering::UrlPort,
        FunctionName::UrlPath => BuiltinLowering::UrlPath,
        FunctionName::UrlQuery => BuiltinLowering::UrlQuery,
        FunctionName::UrlFragment => BuiltinLowering::UrlFragment,
        FunctionName::UrlQueryValue => BuiltinLowering::UrlQueryValue,
        FunctionName::UrlQueryValues => BuiltinLowering::UrlQueryValues,
        FunctionName::UrlDecode => BuiltinLowering::UrlDecode,
        FunctionName::IsUrl => BuiltinLowering::IsUrl,
        FunctionName::Pow => BuiltinLowering::Pow,
        FunctionName::RegexpLike => {
            BuiltinLowering::Regexp(RegexpCall::reading_pattern_argument(RegexpFunction::Like))
        }
        FunctionName::RegexpReplace => BuiltinLowering::Regexp(
            RegexpCall::reading_pattern_argument(RegexpFunction::Replace),
        ),
        FunctionName::RegexpSubstr => {
            BuiltinLowering::Regexp(RegexpCall::reading_pattern_argument(RegexpFunction::Substr))
        }
        FunctionName::RegexpExtract => BuiltinLowering::Regexp(
            RegexpCall::reading_pattern_argument(RegexpFunction::Extract),
        ),
        FunctionName::Repeat => BuiltinLowering::Repeat,
        FunctionName::Replace => BuiltinLowering::Replace,
        FunctionName::Reverse => BuiltinLowering::Reverse,
        FunctionName::Right => BuiltinLowering::Right,
        FunctionName::Round => BuiltinLowering::Round,
        FunctionName::Rpad => BuiltinLowering::Rpad,
        FunctionName::SplitPart => BuiltinLowering::SplitPart,
        FunctionName::Sqrt => BuiltinLowering::Sqrt,
        FunctionName::Strpos => BuiltinLowering::Strpos,
        FunctionName::Substr => BuiltinLowering::Substr,
        FunctionName::Tan => BuiltinLowering::Tan,
        FunctionName::ToHex => BuiltinLowering::ToHex,
        FunctionName::Translate => BuiltinLowering::Translate,
        FunctionName::Sin => BuiltinLowering::Sin,
        FunctionName::Atan2 => BuiltinLowering::Atan2,
        FunctionName::Log2 => BuiltinLowering::Log2,
        FunctionName::Radians => BuiltinLowering::Radians,
        FunctionName::Degrees => BuiltinLowering::Degrees,
        FunctionName::Sign => BuiltinLowering::Sign,
        FunctionName::Trunc => BuiltinLowering::Trunc,
        FunctionName::IsNan => BuiltinLowering::IsNan,
        FunctionName::IsFinite => BuiltinLowering::IsFinite,
        FunctionName::IsInfinite => BuiltinLowering::IsInfinite,
        FunctionName::BitwiseAnd => BuiltinLowering::BitwiseAnd,
        FunctionName::BitwiseOr => BuiltinLowering::BitwiseOr,
        FunctionName::BitwiseXor => BuiltinLowering::BitwiseXor,
        FunctionName::BitwiseNot => BuiltinLowering::BitwiseNot,
        FunctionName::ShiftLeft => BuiltinLowering::ShiftLeft,
        FunctionName::ShiftRight => BuiltinLowering::ShiftRight,
        FunctionName::BitCount => BuiltinLowering::BitCount,
        FunctionName::Greatest => BuiltinLowering::Extremum(Extremum::Greatest),
        FunctionName::Least => BuiltinLowering::Extremum(Extremum::Least),
        FunctionName::Clamp => BuiltinLowering::Clamp,
        FunctionName::Datetime(function) => BuiltinLowering::Datetime(function.clone()),
        FunctionName::ListShape(function) => BuiltinLowering::ListShape(function.clone()),
        FunctionName::LeakSensitive
        | FunctionName::LookupHashMap
        | FunctionName::ReadHeader
        | FunctionName::ReadHeaders
        | FunctionName::WriteHeader
        | FunctionName::WindowAggregate(_)
        | FunctionName::Udf(_)
        | FunctionName::Unknown(_) => return None,
    };
    let semantics = builtin_semantics_for_lowering(&lowering);
    Some(BuiltinDescriptor {
        lowering,
        semantics,
    })
}
