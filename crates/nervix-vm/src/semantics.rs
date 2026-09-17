//! The semantic catalog of every expression operator, cast and builtin.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Each operation's accepted argument types, result type, volatility, dependency scope,
//!   null propagation and whether it can report a per-row error, and the value contracts, such as
//!   case mapping, floating-point classification and integer bit operations, that compile-time
//!   folding and columnar execution both apply.
//! - **Depends on.** The VM program model and Arrow data types.
//! - **Must not know.** How execution walks Arrow buffers, registers or batches, and anything about
//!   relays, branches, connectors or the registry.

use std::ops::{BitAnd, BitOr, BitXor, Not};

use arrow_schema::{DataType, TimeUnit};

use crate::{
    CompileError, RegisterType,
    program::{BinaryOp, Expr, FunctionName, SpannedExpr, UnaryOp},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Volatility {
    Immutable,
    Stable,
    Volatile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DependencyScope {
    Constant,
    ExecutionLocal,
    RowLocal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullPropagation {
    NeverNull,
    Strict,
    Custom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationSemantics {
    pub volatility: Volatility,
    pub dependency_scope: DependencyScope,
    pub has_side_effects: bool,
    pub can_error: bool,
    pub null_propagation: NullPropagation,
}

impl OperationSemantics {
    pub fn supports_common_subexpression_elimination(self) -> bool {
        !self.has_side_effects && !self.can_error && self.volatility != Volatility::Volatile
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpressionSemantics {
    pub volatility: Volatility,
    pub dependency_scope: DependencyScope,
    pub has_side_effects: bool,
    pub can_error: bool,
    pub null_propagation: NullPropagation,
}

impl ExpressionSemantics {
    pub const fn literal() -> Self {
        Self {
            volatility: Volatility::Immutable,
            dependency_scope: DependencyScope::Constant,
            has_side_effects: false,
            can_error: false,
            null_propagation: NullPropagation::NeverNull,
        }
    }

    pub const fn identifier() -> Self {
        Self {
            volatility: Volatility::Immutable,
            dependency_scope: DependencyScope::RowLocal,
            has_side_effects: false,
            can_error: false,
            null_propagation: NullPropagation::Strict,
        }
    }

    pub fn from_operation(
        operation: OperationSemantics,
        children: impl IntoIterator<Item = Self>,
    ) -> Self {
        let mut volatility = operation.volatility;
        let mut dependency_scope = operation.dependency_scope;
        let mut has_side_effects = operation.has_side_effects;
        let mut can_error = operation.can_error;
        let mut saw_child = false;
        let mut all_children_never_null = true;
        let mut any_child_custom_null = false;

        for child in children {
            saw_child = true;
            volatility = volatility.max(child.volatility);
            dependency_scope = dependency_scope.max(child.dependency_scope);
            has_side_effects |= child.has_side_effects;
            can_error |= child.can_error;
            all_children_never_null &= child.null_propagation == NullPropagation::NeverNull;
            any_child_custom_null |= child.null_propagation == NullPropagation::Custom;
        }

        let null_propagation = match operation.null_propagation {
            NullPropagation::NeverNull => NullPropagation::NeverNull,
            NullPropagation::Strict => {
                if !saw_child || all_children_never_null {
                    NullPropagation::NeverNull
                } else if any_child_custom_null {
                    NullPropagation::Custom
                } else {
                    NullPropagation::Strict
                }
            }
            NullPropagation::Custom => NullPropagation::Custom,
        };

        Self {
            volatility,
            dependency_scope,
            has_side_effects,
            can_error,
            null_propagation,
        }
    }

    pub fn supports_common_subexpression_elimination(self) -> bool {
        !self.has_side_effects && !self.can_error && self.volatility != Volatility::Volatile
    }

    pub fn supports_constant_folding(self) -> bool {
        self.supports_common_subexpression_elimination()
            && self.dependency_scope == DependencyScope::Constant
            && !self.can_error
    }
}

/// The IEEE 754 class `is_nan`, `is_finite` and `is_infinite` test a floating-point value for.
///
/// Folding a call over a literal and executing it over a column both test through
/// [`FloatClass::contains`], so a literal and a column holding the same value always classify the
/// same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloatClass {
    /// NaN, whatever its sign or payload.
    Nan,
    /// Every value that is neither NaN nor an infinity, including both zeros and subnormals.
    Finite,
    /// Positive or negative infinity.
    Infinite,
}

impl FloatClass {
    /// Whether `value` belongs to this class.
    pub fn contains<F: ClassifiedFloat>(self, value: F) -> bool {
        match self {
            Self::Nan => value.is_nan_value(),
            Self::Finite => value.is_finite_value(),
            Self::Infinite => value.is_infinite_value(),
        }
    }
}

/// A floating-point type whose values `is_nan`, `is_finite` and `is_infinite` classify.
pub trait ClassifiedFloat: Copy {
    fn is_nan_value(self) -> bool;

    fn is_finite_value(self) -> bool;

    fn is_infinite_value(self) -> bool;
}

macro_rules! classified_float {
    ($($native:ty),+ $(,)?) => {
        $(
            impl ClassifiedFloat for $native {
                fn is_nan_value(self) -> bool {
                    self.is_nan()
                }

                fn is_finite_value(self) -> bool {
                    self.is_finite()
                }

                fn is_infinite_value(self) -> bool {
                    self.is_infinite()
                }
            }
        )+
    };
}

classified_float!(f32, f64);

/// The operator `bitwise_and`, `bitwise_or` or `bitwise_xor` applies to two integers of one type.
///
/// Folding a call over literals and executing it over columns both combine values through
/// [`BitwiseOperation::apply`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitwiseOperation {
    And,
    Or,
    Xor,
}

impl BitwiseOperation {
    /// Combines the two's complement bits of `left` and `right` at their shared width.
    pub fn apply<N: IntegerBits>(self, left: N, right: N) -> N {
        match self {
            Self::And => left & right,
            Self::Or => left | right,
            Self::Xor => left ^ right,
        }
    }
}

/// An integer type whose two's complement bits `bitwise_not` and `bit_count` read at the type's own
/// width. Folding and columnar execution both use these methods.
pub trait IntegerBits:
    Copy + BitAnd<Output = Self> + BitOr<Output = Self> + BitXor<Output = Self> + Not<Output = Self>
{
    /// `bitwise_not`: every bit inverted, so an unsigned value maps to its type's maximum minus the
    /// value and a signed value `v` maps to `-v - 1`.
    fn complement(self) -> Self {
        !self
    }

    /// `bit_count`: how many bits are set. A negative value counts its sign-extended bits, so `-1`
    /// counts every bit of its type.
    fn one_bits(self) -> i64;
}

macro_rules! integer_bits {
    ($($native:ty),+ $(,)?) => {
        $(
            impl IntegerBits for $native {
                fn one_bits(self) -> i64 {
                    i64::from(self.count_ones())
                }
            }
        )+
    };
}

integer_bits!(u8, i8, u16, i16, u32, i32, u64, i64);

/// The case mapping `lower` and `upper` apply: Unicode's full case mapping, which never depends on
/// a locale. Folding a call over a literal and executing it over a column both apply this one
/// mapping, so a literal and a column holding the same text always convert to the same value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseMapping {
    Lower,
    Upper,
}

impl CaseMapping {
    /// Maps one whole value. The mapping has to see the whole value because lowercasing a sigma
    /// depends on the characters around it, so a character-by-character mapping would differ.
    pub fn apply(self, text: &str) -> String {
        match self {
            Self::Lower => text.to_lowercase(),
            Self::Upper => text.to_uppercase(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinLowering {
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
    Concat,
    Sum,
    Last,
    First,
    Count,
    Nth,
    Contains,
    Cos,
    StartsWith,
    EndsWith,
    Exp,
    Floor,
    Initcap,
    Left,
    Ln,
    Log,
    Lpad,
    Md5,
    Pow,
    RegexpLike,
    RegexpReplace,
    RegexpSubstr,
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
    Sin,
    Atan2,
    Log2,
    Radians,
    Degrees,
    Sign,
    Trunc,
    IsNan,
    IsFinite,
    IsInfinite,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    BitwiseNot,
    ShiftLeft,
    ShiftRight,
    BitCount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnaryDescriptor {
    pub semantics: OperationSemantics,
}

impl UnaryDescriptor {
    pub fn output_type(self, input_type: &DataType) -> Option<DataType> {
        match (self.semantics.can_error, input_type) {
            (true, ty) if is_signed_numeric_type(ty) => Some(input_type.clone()),
            (false, DataType::Boolean) => Some(DataType::Boolean),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinaryDescriptor {
    pub semantics: OperationSemantics,
}

impl BinaryDescriptor {
    pub fn output_type(self, left_type: &DataType, right_type: &DataType) -> Option<DataType> {
        if left_type != right_type {
            return None;
        }

        match self.semantics {
            semantics if semantics.can_error => {
                if is_numeric_type(left_type) {
                    Some(left_type.clone())
                } else {
                    None
                }
            }
            OperationSemantics {
                null_propagation: NullPropagation::Custom,
                ..
            } => match left_type {
                DataType::Boolean => Some(DataType::Boolean),
                _ => None,
            },
            _ => {
                if is_supported_type(left_type) {
                    Some(DataType::Boolean)
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CastDescriptor {
    pub semantics: OperationSemantics,
}

impl CastDescriptor {
    pub fn validate(
        self,
        input_type: &DataType,
        target_type: &DataType,
        span: impl Into<std::ops::Range<usize>>,
    ) -> Result<(), CompileError> {
        if is_supported_type(input_type) && is_supported_type(target_type) {
            Ok(())
        } else {
            Err(CompileError {
                code: "unsupported_cast",
                message: format!(
                    "casts are only implemented for supported nervix VM types, found {:?} AS {:?}",
                    input_type, target_type
                ),
                span: span.into().into(),
            })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinDescriptor {
    pub lowering: BuiltinLowering,
    pub semantics: OperationSemantics,
}

impl BuiltinDescriptor {
    pub fn output_type(
        self,
        function: &FunctionName,
        arg_types: &[DataType],
        span: impl Into<std::ops::Range<usize>>,
    ) -> Result<DataType, CompileError> {
        builtin_output_type(function, self.lowering, arg_types, span.into())
    }
}

pub const fn unary_descriptor(op: UnaryOp) -> UnaryDescriptor {
    match op {
        UnaryOp::Neg => UnaryDescriptor {
            semantics: OperationSemantics {
                volatility: Volatility::Immutable,
                dependency_scope: DependencyScope::Constant,
                has_side_effects: false,
                can_error: true,
                null_propagation: NullPropagation::Strict,
            },
        },
        UnaryOp::Not => UnaryDescriptor {
            semantics: OperationSemantics {
                volatility: Volatility::Immutable,
                dependency_scope: DependencyScope::Constant,
                has_side_effects: false,
                can_error: false,
                null_propagation: NullPropagation::Strict,
            },
        },
    }
}

pub const fn unary_op_semantics(op: UnaryOp) -> OperationSemantics {
    unary_descriptor(op).semantics
}

pub const fn binary_descriptor(op: BinaryOp) -> BinaryDescriptor {
    match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Rem => {
            BinaryDescriptor {
                semantics: OperationSemantics {
                    volatility: Volatility::Immutable,
                    dependency_scope: DependencyScope::Constant,
                    has_side_effects: false,
                    can_error: true,
                    null_propagation: NullPropagation::Strict,
                },
            }
        }
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Gt
        | BinaryOp::Lt
        | BinaryOp::GtEq
        | BinaryOp::LtEq => BinaryDescriptor {
            semantics: OperationSemantics {
                volatility: Volatility::Immutable,
                dependency_scope: DependencyScope::Constant,
                has_side_effects: false,
                can_error: false,
                null_propagation: NullPropagation::Strict,
            },
        },
        BinaryOp::And | BinaryOp::Or => BinaryDescriptor {
            semantics: OperationSemantics {
                volatility: Volatility::Immutable,
                dependency_scope: DependencyScope::Constant,
                has_side_effects: false,
                can_error: false,
                null_propagation: NullPropagation::Custom,
            },
        },
    }
}

pub const fn binary_op_semantics(op: BinaryOp) -> OperationSemantics {
    binary_descriptor(op).semantics
}

pub const fn cast_descriptor() -> CastDescriptor {
    CastDescriptor {
        semantics: OperationSemantics {
            volatility: Volatility::Immutable,
            dependency_scope: DependencyScope::Constant,
            has_side_effects: false,
            can_error: true,
            null_propagation: NullPropagation::Strict,
        },
    }
}

pub const fn cast_semantics() -> OperationSemantics {
    cast_descriptor().semantics
}

pub const fn builtin_descriptor(function: &FunctionName) -> Option<BuiltinDescriptor> {
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
        FunctionName::Pow => BuiltinLowering::Pow,
        FunctionName::RegexpLike => BuiltinLowering::RegexpLike,
        FunctionName::RegexpReplace => BuiltinLowering::RegexpReplace,
        FunctionName::RegexpSubstr => BuiltinLowering::RegexpSubstr,
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
        FunctionName::LeakSensitive
        | FunctionName::LookupHashMap
        | FunctionName::ReadHeader
        | FunctionName::ReadHeaders
        | FunctionName::WriteHeader
        | FunctionName::WindowAggregate(_)
        | FunctionName::Udf(_)
        | FunctionName::Unknown(_) => return None,
    };
    Some(BuiltinDescriptor {
        lowering,
        semantics: builtin_semantics_for_lowering(lowering),
    })
}

pub const fn builtin_semantics_for_lowering(lowering: BuiltinLowering) -> OperationSemantics {
    match lowering {
        BuiltinLowering::Now => OperationSemantics {
            volatility: Volatility::Stable,
            dependency_scope: DependencyScope::ExecutionLocal,
            has_side_effects: false,
            can_error: false,
            null_propagation: NullPropagation::NeverNull,
        },
        BuiltinLowering::UuidV4 | BuiltinLowering::UuidV7 => OperationSemantics {
            volatility: Volatility::Volatile,
            dependency_scope: DependencyScope::ExecutionLocal,
            has_side_effects: false,
            can_error: false,
            null_propagation: NullPropagation::NeverNull,
        },
        BuiltinLowering::Lower
        | BuiltinLowering::Upper
        | BuiltinLowering::Trim
        | BuiltinLowering::Btrim
        | BuiltinLowering::Ltrim
        | BuiltinLowering::Rtrim
        | BuiltinLowering::Length
        | BuiltinLowering::CharLength
        | BuiltinLowering::BitLength
        | BuiltinLowering::Ascii
        | BuiltinLowering::Contains
        | BuiltinLowering::StartsWith
        | BuiltinLowering::EndsWith
        | BuiltinLowering::Initcap
        | BuiltinLowering::Left
        | BuiltinLowering::Lpad
        | BuiltinLowering::Md5
        | BuiltinLowering::Repeat
        | BuiltinLowering::Replace
        | BuiltinLowering::Reverse
        | BuiltinLowering::Right
        | BuiltinLowering::Rpad
        | BuiltinLowering::SplitPart
        | BuiltinLowering::Strpos
        | BuiltinLowering::Substr
        | BuiltinLowering::ToHex
        | BuiltinLowering::Translate
        | BuiltinLowering::Count
        | BuiltinLowering::First
        | BuiltinLowering::Last
        | BuiltinLowering::Nth
        | BuiltinLowering::IsNan
        | BuiltinLowering::IsFinite
        | BuiltinLowering::IsInfinite
        | BuiltinLowering::BitwiseAnd
        | BuiltinLowering::BitwiseOr
        | BuiltinLowering::BitwiseXor
        | BuiltinLowering::BitwiseNot
        | BuiltinLowering::BitCount => OperationSemantics {
            volatility: Volatility::Immutable,
            dependency_scope: DependencyScope::Constant,
            has_side_effects: false,
            can_error: false,
            null_propagation: NullPropagation::Strict,
        },
        BuiltinLowering::Coalesce | BuiltinLowering::NullIf | BuiltinLowering::Concat => {
            OperationSemantics {
                volatility: Volatility::Immutable,
                dependency_scope: DependencyScope::Constant,
                has_side_effects: false,
                can_error: false,
                null_propagation: NullPropagation::Custom,
            }
        }
        BuiltinLowering::IsNull => OperationSemantics {
            volatility: Volatility::Immutable,
            dependency_scope: DependencyScope::Constant,
            has_side_effects: false,
            can_error: false,
            null_propagation: NullPropagation::NeverNull,
        },
        BuiltinLowering::Abs
        | BuiltinLowering::Acos
        | BuiltinLowering::Asin
        | BuiltinLowering::Atan
        | BuiltinLowering::Ceil
        | BuiltinLowering::Cos
        | BuiltinLowering::Exp
        | BuiltinLowering::Floor
        | BuiltinLowering::Ln
        | BuiltinLowering::Log
        | BuiltinLowering::Pow
        | BuiltinLowering::RegexpLike
        | BuiltinLowering::RegexpReplace
        | BuiltinLowering::RegexpSubstr
        | BuiltinLowering::Round
        | BuiltinLowering::Sqrt
        | BuiltinLowering::Sum
        | BuiltinLowering::Tan
        | BuiltinLowering::Sin
        | BuiltinLowering::Atan2
        | BuiltinLowering::Log2
        | BuiltinLowering::Radians
        | BuiltinLowering::Degrees
        | BuiltinLowering::Sign
        | BuiltinLowering::Trunc
        | BuiltinLowering::ShiftLeft
        | BuiltinLowering::ShiftRight => OperationSemantics {
            volatility: Volatility::Immutable,
            dependency_scope: DependencyScope::Constant,
            has_side_effects: false,
            can_error: true,
            null_propagation: NullPropagation::Strict,
        },
    }
}

pub fn builtin_function_semantics(function: &FunctionName) -> Option<OperationSemantics> {
    builtin_descriptor(function).map(|descriptor| descriptor.semantics)
}

pub fn builtin_signature(
    function: &FunctionName,
    arg_types: &[DataType],
    span: impl Into<std::ops::Range<usize>>,
) -> Result<DataType, CompileError> {
    let Some(descriptor) = builtin_descriptor(function) else {
        return Err(CompileError {
            code: "unknown_function",
            message: format!(
                "unknown function '{}' with arity {}",
                function.as_str(),
                arg_types.len()
            ),
            span: span.into().into(),
        });
    };
    descriptor.output_type(function, arg_types, span)
}

fn builtin_output_type(
    function: &FunctionName,
    lowering: BuiltinLowering,
    arg_types: &[DataType],
    span: std::ops::Range<usize>,
) -> Result<DataType, CompileError> {
    match lowering {
        BuiltinLowering::Now => {
            require_builtin_arity_exact(function, arg_types, 0, span)?;
            Ok(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some("+00:00".into()),
            ))
        }
        BuiltinLowering::UuidV4 | BuiltinLowering::UuidV7 => {
            require_builtin_arity_exact(function, arg_types, 0, span)?;
            Ok(DataType::Utf8)
        }
        BuiltinLowering::Lower
        | BuiltinLowering::Upper
        | BuiltinLowering::Trim
        | BuiltinLowering::Btrim
        | BuiltinLowering::Ltrim
        | BuiltinLowering::Rtrim
        | BuiltinLowering::Initcap
        | BuiltinLowering::Md5
        | BuiltinLowering::Reverse => {
            require_builtin_arity_exact(function, arg_types, 1, span.clone())?;
            require_utf8_arg(
                function,
                require_supported_register_type(function, &arg_types[0], span.clone())?,
                span,
            )?;
            Ok(DataType::Utf8)
        }
        BuiltinLowering::Length
        | BuiltinLowering::CharLength
        | BuiltinLowering::BitLength
        | BuiltinLowering::Ascii => {
            require_builtin_arity_exact(function, arg_types, 1, span.clone())?;
            require_utf8_arg(
                function,
                require_supported_register_type(function, &arg_types[0], span.clone())?,
                span,
            )?;
            Ok(DataType::Int64)
        }
        BuiltinLowering::Coalesce => {
            require_builtin_min_arity(function, arg_types, 1, span.clone())?;
            require_supported_register_type(function, &arg_types[0], span.clone())?;
            for arg_type in &arg_types[1..] {
                require_supported_register_type(function, arg_type, span.clone())?;
                if arg_type != &arg_types[0] {
                    return Err(CompileError {
                        code: "type_mismatch",
                        message: format!(
                            "function '{}' requires matching operand types, found {:?} and {:?}",
                            function.as_str(),
                            arg_types[0],
                            arg_type
                        ),
                        span: span.into(),
                    });
                }
            }
            Ok(arg_types[0].clone())
        }
        BuiltinLowering::IsNull => {
            require_builtin_arity_exact(function, arg_types, 1, span.clone())?;
            require_supported_register_type(function, &arg_types[0], span)?;
            Ok(DataType::Boolean)
        }
        BuiltinLowering::NullIf => {
            require_builtin_arity_exact(function, arg_types, 2, span.clone())?;
            require_supported_register_type(function, &arg_types[0], span.clone())?;
            require_supported_register_type(function, &arg_types[1], span.clone())?;
            if arg_types[0] != arg_types[1] {
                return Err(CompileError {
                    code: "type_mismatch",
                    message: format!(
                        "function '{}' requires matching operand types, found {:?} and {:?}",
                        function.as_str(),
                        arg_types[0],
                        arg_types[1]
                    ),
                    span: span.into(),
                });
            }
            Ok(arg_types[0].clone())
        }
        BuiltinLowering::Abs => {
            require_builtin_arity_exact(function, arg_types, 1, span.clone())?;
            let input = require_supported_register_type(function, &arg_types[0], span.clone())?;
            require_numeric_arg(function, input, span)?;
            Ok(input.data_type())
        }
        BuiltinLowering::Acos
        | BuiltinLowering::Asin
        | BuiltinLowering::Atan
        | BuiltinLowering::Cos
        | BuiltinLowering::Exp
        | BuiltinLowering::Ln
        | BuiltinLowering::Sqrt
        | BuiltinLowering::Tan
        | BuiltinLowering::Sin
        | BuiltinLowering::Log2
        | BuiltinLowering::Radians
        | BuiltinLowering::Degrees => {
            require_builtin_arity_exact(function, arg_types, 1, span.clone())?;
            let input = require_supported_register_type(function, &arg_types[0], span.clone())?;
            require_numeric_arg(function, input, span)?;
            Ok(DataType::Float64)
        }
        BuiltinLowering::Ceil
        | BuiltinLowering::Floor
        | BuiltinLowering::Sign
        | BuiltinLowering::Trunc => {
            require_builtin_arity_exact(function, arg_types, 1, span.clone())?;
            let input = require_supported_register_type(function, &arg_types[0], span.clone())?;
            require_numeric_arg(function, input, span)?;
            Ok(input.data_type())
        }
        BuiltinLowering::Round => match arg_types {
            [value] => {
                let input = require_supported_register_type(function, value, span.clone())?;
                require_numeric_arg(function, input, span)?;
                Ok(input.data_type())
            }
            [value, digits] => {
                let input = require_supported_register_type(function, value, span.clone())?;
                let digits = require_supported_register_type(function, digits, span.clone())?;
                require_numeric_arg(function, input, span.clone())?;
                require_integral_arg(function, digits, span)?;
                Ok(input.data_type())
            }
            _ => Err(invalid_builtin_arity_error(function, arg_types.len(), span)),
        },
        BuiltinLowering::IsNan | BuiltinLowering::IsFinite | BuiltinLowering::IsInfinite => {
            require_builtin_arity_exact(function, arg_types, 1, span.clone())?;
            let input = require_supported_register_type(function, &arg_types[0], span.clone())?;
            // No integer is NaN or infinite, so classification accepts only floats.
            if let RegisterType::Float32 | RegisterType::Float64 = input {
                Ok(DataType::Boolean)
            } else {
                Err(CompileError {
                    code: "unsupported_function",
                    message: format!(
                        "function '{}' requires floating-point input, found {input}",
                        function.as_str()
                    ),
                    span: span.into(),
                })
            }
        }
        BuiltinLowering::BitwiseAnd | BuiltinLowering::BitwiseOr | BuiltinLowering::BitwiseXor => {
            require_builtin_arity_exact(function, arg_types, 2, span.clone())?;
            let left = require_supported_register_type(function, &arg_types[0], span.clone())?;
            let right = require_supported_register_type(function, &arg_types[1], span.clone())?;
            require_integral_arg(function, left, span.clone())?;
            require_integral_arg(function, right, span.clone())?;
            if left != right {
                return Err(CompileError {
                    code: "type_mismatch",
                    message: format!(
                        "function '{}' requires matching operand types, found {:?} and {:?}",
                        function.as_str(),
                        arg_types[0],
                        arg_types[1]
                    ),
                    span: span.into(),
                });
            }
            Ok(left.data_type())
        }
        BuiltinLowering::BitwiseNot => {
            require_builtin_arity_exact(function, arg_types, 1, span.clone())?;
            let input = require_supported_register_type(function, &arg_types[0], span.clone())?;
            require_integral_arg(function, input, span)?;
            Ok(input.data_type())
        }
        BuiltinLowering::BitCount => {
            require_builtin_arity_exact(function, arg_types, 1, span.clone())?;
            let input = require_supported_register_type(function, &arg_types[0], span.clone())?;
            require_integral_arg(function, input, span)?;
            Ok(DataType::Int64)
        }
        BuiltinLowering::ShiftLeft | BuiltinLowering::ShiftRight => {
            require_builtin_arity_exact(function, arg_types, 2, span.clone())?;
            let value = require_supported_register_type(function, &arg_types[0], span.clone())?;
            let count = require_supported_register_type(function, &arg_types[1], span.clone())?;
            require_integral_arg(function, value, span.clone())?;
            require_integral_arg(function, count, span)?;
            Ok(value.data_type())
        }
        BuiltinLowering::Concat => {
            require_builtin_min_arity(function, arg_types, 1, span.clone())?;
            for arg_type in arg_types {
                let input = require_supported_register_type(function, arg_type, span.clone())?;
                require_utf8_arg(function, input, span.clone())?;
            }
            Ok(DataType::Utf8)
        }
        BuiltinLowering::Count => {
            require_builtin_arity_exact(function, arg_types, 1, span.clone())?;
            require_list_arg(function, &arg_types[0], span)?;
            Ok(DataType::Int64)
        }
        BuiltinLowering::First | BuiltinLowering::Last => {
            require_builtin_arity_exact(function, arg_types, 1, span.clone())?;
            list_element_type(function, &arg_types[0], ListElements::Scalar, span)
        }
        BuiltinLowering::Nth => {
            require_builtin_arity_exact(function, arg_types, 2, span.clone())?;
            let index = require_supported_register_type(function, &arg_types[1], span.clone())?;
            require_integral_arg(function, index, span.clone())?;
            list_element_type(function, &arg_types[0], ListElements::Scalar, span)
        }
        BuiltinLowering::Sum => {
            require_builtin_arity_exact(function, arg_types, 1, span.clone())?;
            let element =
                list_element_type(function, &arg_types[0], ListElements::Any, span.clone())?;
            let input = require_supported_register_type(function, &element, span.clone())?;
            require_numeric_arg(function, input, span)?;
            Ok(element)
        }
        BuiltinLowering::Contains
        | BuiltinLowering::StartsWith
        | BuiltinLowering::EndsWith
        | BuiltinLowering::RegexpLike => {
            require_builtin_arity_exact(function, arg_types, 2, span.clone())?;
            let left = require_supported_register_type(function, &arg_types[0], span.clone())?;
            let right = require_supported_register_type(function, &arg_types[1], span.clone())?;
            require_utf8_arg(function, left, span.clone())?;
            require_utf8_arg(function, right, span)?;
            Ok(DataType::Boolean)
        }
        BuiltinLowering::Left | BuiltinLowering::Right | BuiltinLowering::Repeat => {
            require_builtin_arity_exact(function, arg_types, 2, span.clone())?;
            let string = require_supported_register_type(function, &arg_types[0], span.clone())?;
            let count = require_supported_register_type(function, &arg_types[1], span.clone())?;
            require_utf8_arg(function, string, span.clone())?;
            require_integral_arg(function, count, span)?;
            Ok(DataType::Utf8)
        }
        BuiltinLowering::Log => match arg_types {
            [value] => {
                let input = require_supported_register_type(function, value, span.clone())?;
                require_numeric_arg(function, input, span)?;
                Ok(DataType::Float64)
            }
            [base, value] => {
                let base = require_supported_register_type(function, base, span.clone())?;
                let value = require_supported_register_type(function, value, span.clone())?;
                require_numeric_arg(function, base, span.clone())?;
                require_numeric_arg(function, value, span)?;
                Ok(DataType::Float64)
            }
            _ => Err(invalid_builtin_arity_error(function, arg_types.len(), span)),
        },
        BuiltinLowering::Lpad | BuiltinLowering::Rpad => {
            require_builtin_arity_exact(function, arg_types, 3, span.clone())?;
            let string = require_supported_register_type(function, &arg_types[0], span.clone())?;
            let length = require_supported_register_type(function, &arg_types[1], span.clone())?;
            let fill = require_supported_register_type(function, &arg_types[2], span.clone())?;
            require_utf8_arg(function, string, span.clone())?;
            require_integral_arg(function, length, span.clone())?;
            require_utf8_arg(function, fill, span)?;
            Ok(DataType::Utf8)
        }
        BuiltinLowering::Pow | BuiltinLowering::Atan2 => {
            require_builtin_arity_exact(function, arg_types, 2, span.clone())?;
            let left = require_supported_register_type(function, &arg_types[0], span.clone())?;
            let right = require_supported_register_type(function, &arg_types[1], span.clone())?;
            require_numeric_arg(function, left, span.clone())?;
            require_numeric_arg(function, right, span)?;
            Ok(DataType::Float64)
        }
        BuiltinLowering::RegexpReplace | BuiltinLowering::Replace | BuiltinLowering::Translate => {
            require_builtin_arity_exact(function, arg_types, 3, span.clone())?;
            for arg_type in arg_types {
                let input = require_supported_register_type(function, arg_type, span.clone())?;
                require_utf8_arg(function, input, span.clone())?;
            }
            Ok(DataType::Utf8)
        }
        BuiltinLowering::RegexpSubstr => {
            require_builtin_arity_exact(function, arg_types, 2, span.clone())?;
            let string = require_supported_register_type(function, &arg_types[0], span.clone())?;
            let pattern = require_supported_register_type(function, &arg_types[1], span.clone())?;
            require_utf8_arg(function, string, span.clone())?;
            require_utf8_arg(function, pattern, span)?;
            Ok(DataType::Utf8)
        }
        BuiltinLowering::SplitPart => {
            require_builtin_arity_exact(function, arg_types, 3, span.clone())?;
            let string = require_supported_register_type(function, &arg_types[0], span.clone())?;
            let delimiter = require_supported_register_type(function, &arg_types[1], span.clone())?;
            let index = require_supported_register_type(function, &arg_types[2], span.clone())?;
            require_utf8_arg(function, string, span.clone())?;
            require_utf8_arg(function, delimiter, span.clone())?;
            require_integral_arg(function, index, span)?;
            Ok(DataType::Utf8)
        }
        BuiltinLowering::Strpos => {
            require_builtin_arity_exact(function, arg_types, 2, span.clone())?;
            let string = require_supported_register_type(function, &arg_types[0], span.clone())?;
            let needle = require_supported_register_type(function, &arg_types[1], span.clone())?;
            require_utf8_arg(function, string, span.clone())?;
            require_utf8_arg(function, needle, span)?;
            Ok(DataType::Int64)
        }
        BuiltinLowering::Substr => match arg_types {
            [string, start] => {
                let string = require_supported_register_type(function, string, span.clone())?;
                let start = require_supported_register_type(function, start, span.clone())?;
                require_utf8_arg(function, string, span.clone())?;
                require_integral_arg(function, start, span)?;
                Ok(DataType::Utf8)
            }
            [string, start, length] => {
                let string = require_supported_register_type(function, string, span.clone())?;
                let start = require_supported_register_type(function, start, span.clone())?;
                let length = require_supported_register_type(function, length, span.clone())?;
                require_utf8_arg(function, string, span.clone())?;
                require_integral_arg(function, start, span.clone())?;
                require_integral_arg(function, length, span)?;
                Ok(DataType::Utf8)
            }
            _ => Err(invalid_builtin_arity_error(function, arg_types.len(), span)),
        },
        BuiltinLowering::ToHex => {
            require_builtin_arity_exact(function, arg_types, 1, span.clone())?;
            let input = require_supported_register_type(function, &arg_types[0], span.clone())?;
            require_integral_arg(function, input, span)?;
            Ok(DataType::Utf8)
        }
    }
}

pub fn binary_output_type(
    op: BinaryOp,
    left_type: &DataType,
    right_type: &DataType,
) -> Option<DataType> {
    let descriptor = binary_descriptor(op);
    let output = descriptor.output_type(left_type, right_type)?;
    if matches!(
        op,
        BinaryOp::Gt | BinaryOp::Lt | BinaryOp::GtEq | BinaryOp::LtEq
    ) && !(is_numeric_type(left_type)
        || matches!(
            left_type,
            DataType::Utf8 | DataType::Timestamp(TimeUnit::Nanosecond, Some(_))
        ))
    {
        return None;
    }
    Some(output)
}

fn is_supported_type(data_type: &DataType) -> bool {
    RegisterType::from_data_type(data_type).is_some()
}

fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::UInt8
            | DataType::Int8
            | DataType::UInt16
            | DataType::Int16
            | DataType::UInt32
            | DataType::Int32
            | DataType::UInt64
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
    )
}

fn is_signed_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
    )
}

fn is_integral_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::UInt8
            | DataType::Int8
            | DataType::UInt16
            | DataType::Int16
            | DataType::UInt32
            | DataType::Int32
            | DataType::UInt64
            | DataType::Int64
    )
}

fn invalid_builtin_arity_error(
    function: &FunctionName,
    actual: usize,
    span: std::ops::Range<usize>,
) -> CompileError {
    CompileError {
        code: "unknown_function",
        message: format!(
            "unknown function '{}' with arity {actual}",
            function.as_str()
        ),
        span: span.into(),
    }
}

fn require_builtin_arity_exact(
    function: &FunctionName,
    arg_types: &[DataType],
    expected: usize,
    span: std::ops::Range<usize>,
) -> Result<(), CompileError> {
    if arg_types.len() == expected {
        Ok(())
    } else {
        Err(invalid_builtin_arity_error(function, arg_types.len(), span))
    }
}

fn require_builtin_min_arity(
    function: &FunctionName,
    arg_types: &[DataType],
    min: usize,
    span: std::ops::Range<usize>,
) -> Result<(), CompileError> {
    if arg_types.len() >= min {
        Ok(())
    } else {
        Err(invalid_builtin_arity_error(function, arg_types.len(), span))
    }
}

fn require_supported_register_type(
    function: &FunctionName,
    data_type: &DataType,
    span: std::ops::Range<usize>,
) -> Result<RegisterType, CompileError> {
    RegisterType::from_data_type(data_type).ok_or_else(|| CompileError {
        code: "unsupported_function",
        message: format!(
            "function '{}' does not support input type {:?}",
            function.as_str(),
            data_type
        ),
        span: span.into(),
    })
}

/// Which elements an `ARRAY` or `VEC` function accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListElements {
    /// Every supported element, including a nested `ARRAY` or `VEC` value.
    Any,
    /// Scalar and datetime elements only. `first`, `last` and `nth` select one element out of the
    /// list's value column, which execution supports only for these elements, so a nested element
    /// is rejected when the statement is applied instead of failing every batch it meets.
    Scalar,
}

fn list_element_type(
    function: &FunctionName,
    data_type: &DataType,
    elements: ListElements,
    span: std::ops::Range<usize>,
) -> Result<DataType, CompileError> {
    let element = match data_type {
        DataType::List(field) | DataType::FixedSizeList(field, _) => field.data_type().clone(),
        _ => {
            return Err(CompileError {
                code: "unsupported_function",
                message: format!(
                    "function '{}' requires ARRAY or VEC input, found {:?}",
                    function.as_str(),
                    data_type
                ),
                span: span.into(),
            });
        }
    };
    let element_register = require_supported_register_type(function, &element, span.clone())?;
    if let ListElements::Scalar = elements
        && let RegisterType::Generic = element_register
    {
        return Err(CompileError {
            code: "unsupported_function",
            message: format!(
                "function '{}' requires ARRAY or VEC elements of a scalar type, found {:?}",
                function.as_str(),
                element
            ),
            span: span.into(),
        });
    }
    Ok(element)
}

fn require_list_arg(
    function: &FunctionName,
    data_type: &DataType,
    span: std::ops::Range<usize>,
) -> Result<(), CompileError> {
    list_element_type(function, data_type, ListElements::Any, span).map(|_| ())
}

fn require_utf8_arg(
    function: &FunctionName,
    input_type: RegisterType,
    span: std::ops::Range<usize>,
) -> Result<(), CompileError> {
    if input_type == RegisterType::Utf8 {
        Ok(())
    } else {
        Err(CompileError {
            code: "unsupported_function",
            message: format!(
                "function '{}' requires Utf8 input, found {input_type}",
                function.as_str()
            ),
            span: span.into(),
        })
    }
}

fn require_numeric_arg(
    function: &FunctionName,
    input_type: RegisterType,
    span: std::ops::Range<usize>,
) -> Result<(), CompileError> {
    match input_type {
        RegisterType::UInt8
        | RegisterType::Int8
        | RegisterType::UInt16
        | RegisterType::Int16
        | RegisterType::UInt32
        | RegisterType::Int32
        | RegisterType::UInt64
        | RegisterType::Int64
        | RegisterType::Float32
        | RegisterType::Float64 => Ok(()),
        _ => Err(CompileError {
            code: "unsupported_function",
            message: format!(
                "function '{}' requires numeric input, found {input_type}",
                function.as_str()
            ),
            span: span.into(),
        }),
    }
}

fn require_integral_arg(
    function: &FunctionName,
    input_type: RegisterType,
    span: std::ops::Range<usize>,
) -> Result<(), CompileError> {
    if is_integral_type(&input_type.data_type()) {
        Ok(())
    } else {
        Err(CompileError {
            code: "unsupported_function",
            message: format!(
                "function '{}' requires integer input, found {input_type}",
                function.as_str()
            ),
            span: span.into(),
        })
    }
}

pub fn expr_semantics(expr: &SpannedExpr) -> Option<ExpressionSemantics> {
    match &expr.inner {
        Expr::Literal(_) => Some(ExpressionSemantics::literal()),
        Expr::FieldRef(_) | Expr::InternalFieldRef(_) => Some(ExpressionSemantics::identifier()),
        Expr::Unary { op, expr: inner } => Some(ExpressionSemantics::from_operation(
            unary_op_semantics(*op),
            [expr_semantics(inner.as_ref())?],
        )),
        Expr::Binary { op, left, right } => Some(ExpressionSemantics::from_operation(
            binary_op_semantics(*op),
            [
                expr_semantics(left.as_ref())?,
                expr_semantics(right.as_ref())?,
            ],
        )),
        Expr::Cast { expr: inner, .. } => Some(ExpressionSemantics::from_operation(
            cast_semantics(),
            [expr_semantics(inner.as_ref())?],
        )),
        Expr::Call { function, args } => {
            let operation = if let FunctionName::WindowAggregate(_) = function {
                OperationSemantics {
                    volatility: Volatility::Stable,
                    dependency_scope: DependencyScope::ExecutionLocal,
                    has_side_effects: false,
                    can_error: true,
                    null_propagation: NullPropagation::NeverNull,
                }
            } else if let FunctionName::ReadHeader | FunctionName::ReadHeaders = function {
                OperationSemantics {
                    volatility: Volatility::Stable,
                    dependency_scope: DependencyScope::RowLocal,
                    has_side_effects: false,
                    can_error: false,
                    null_propagation: if let FunctionName::ReadHeader = function {
                        NullPropagation::Custom
                    } else {
                        NullPropagation::NeverNull
                    },
                }
            } else {
                builtin_function_semantics(function)?
            };
            Some(ExpressionSemantics::from_operation(
                operation,
                args.iter()
                    .map(expr_semantics)
                    .collect::<Option<Vec<_>>>()?,
            ))
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            let mut children = Vec::with_capacity(
                usize::from(operand.is_some())
                    + branches.len() * 2
                    + usize::from(else_result.is_some()),
            );
            if let Some(operand) = operand {
                children.push(expr_semantics(operand)?);
            }
            for branch in branches {
                children.push(expr_semantics(&branch.when)?);
                children.push(expr_semantics(&branch.result)?);
            }
            if let Some(else_result) = else_result {
                children.push(expr_semantics(else_result)?);
            }
            Some(ExpressionSemantics::from_operation(
                OperationSemantics {
                    volatility: Volatility::Immutable,
                    dependency_scope: DependencyScope::Constant,
                    has_side_effects: false,
                    can_error: false,
                    null_propagation: NullPropagation::Custom,
                },
                children,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BitwiseOperation, DependencyScope, ExpressionSemantics, FloatClass, IntegerBits,
        NullPropagation, Volatility, binary_op_semantics, builtin_function_semantics,
        cast_semantics, expr_semantics, unary_op_semantics,
    };
    use crate::program::{BinaryOp, Expr, FieldRef, FunctionName, Literal, SpannedNode, UnaryOp};

    fn spanned(inner: Expr) -> SpannedNode<Expr> {
        SpannedNode {
            inner,
            span: (0..0).into(),
        }
    }

    fn relay_ref(relay: &str, field: &str) -> Expr {
        Expr::FieldRef(FieldRef {
            relay: relay.to_string(),
            field: field.to_string(),
        })
    }

    #[test]
    fn classifies_every_supported_builtin() {
        for function in [
            FunctionName::Lower,
            FunctionName::Upper,
            FunctionName::Trim,
            FunctionName::Length,
            FunctionName::Coalesce,
            FunctionName::IsNull,
            FunctionName::NullIf,
            FunctionName::Abs,
            FunctionName::Contains,
            FunctionName::StartsWith,
            FunctionName::EndsWith,
        ] {
            assert!(builtin_function_semantics(&function).is_some());
        }

        assert!(builtin_function_semantics(&FunctionName::Unknown("rand".to_string())).is_none());
    }

    #[test]
    fn numeric_additions_fail_exactly_where_a_row_can_fail() {
        let infallible = [
            FunctionName::IsNan,
            FunctionName::IsFinite,
            FunctionName::IsInfinite,
            FunctionName::BitwiseAnd,
            FunctionName::BitwiseOr,
            FunctionName::BitwiseXor,
            FunctionName::BitwiseNot,
            FunctionName::BitCount,
        ];
        for function in infallible {
            let semantics =
                builtin_function_semantics(&function).expect("the builtin must be classified");
            assert!(!semantics.can_error, "{function:?}");
            assert_eq!(semantics.null_propagation, NullPropagation::Strict);
            assert_eq!(semantics.volatility, Volatility::Immutable);
        }
        let fallible = [
            FunctionName::Sin,
            FunctionName::Atan2,
            FunctionName::Log2,
            FunctionName::Radians,
            FunctionName::Degrees,
            FunctionName::Sign,
            FunctionName::Trunc,
            FunctionName::ShiftLeft,
            FunctionName::ShiftRight,
        ];
        for function in fallible {
            let semantics =
                builtin_function_semantics(&function).expect("the builtin must be classified");
            assert!(semantics.can_error, "{function:?}");
            assert_eq!(semantics.null_propagation, NullPropagation::Strict);
        }
        for name in [
            "sin",
            "atan2",
            "log2",
            "radians",
            "degrees",
            "sign",
            "trunc",
            "is_nan",
            "is_finite",
            "is_infinite",
            "bitwise_and",
            "bitwise_or",
            "bitwise_xor",
            "bitwise_not",
            "shift_left",
            "shift_right",
            "bit_count",
        ] {
            let function = FunctionName::parse(name);
            assert_eq!(function.as_str(), name);
            assert!(builtin_function_semantics(&function).is_some(), "{name}");
        }
    }

    #[test]
    fn value_contracts_classify_and_combine_at_the_operand_width() {
        assert!(FloatClass::Nan.contains(f32::NAN));
        assert!(!FloatClass::Nan.contains(f64::INFINITY));
        assert!(FloatClass::Finite.contains(-0.0_f64));
        assert!(FloatClass::Finite.contains(f32::from_bits(1)));
        assert!(!FloatClass::Finite.contains(f64::NEG_INFINITY));
        assert!(FloatClass::Infinite.contains(f32::NEG_INFINITY));
        assert!(!FloatClass::Infinite.contains(f64::MAX));

        assert_eq!(BitwiseOperation::And.apply(0b1100_u8, 0b1010), 0b1000);
        assert_eq!(BitwiseOperation::Or.apply(-128_i8, 1), -127);
        assert_eq!(BitwiseOperation::Xor.apply(u64::MAX, 1), u64::MAX - 1);
        assert_eq!(0_u8.complement(), u8::MAX);
        assert_eq!(i16::MIN.complement(), i16::MAX);
        assert_eq!((-1_i8).one_bits(), 8);
        assert_eq!(u32::MAX.one_bits(), 32);
        assert_eq!(0_u64.one_bits(), 0);
    }

    #[test]
    fn classifies_every_operator_and_cast() {
        for op in [UnaryOp::Neg, UnaryOp::Not] {
            let semantics = unary_op_semantics(op);
            assert_eq!(semantics.volatility, Volatility::Immutable);
        }

        for op in [
            BinaryOp::Add,
            BinaryOp::Sub,
            BinaryOp::Mul,
            BinaryOp::Div,
            BinaryOp::Rem,
            BinaryOp::Eq,
            BinaryOp::NotEq,
            BinaryOp::Gt,
            BinaryOp::Lt,
            BinaryOp::GtEq,
            BinaryOp::LtEq,
            BinaryOp::And,
            BinaryOp::Or,
        ] {
            let semantics = binary_op_semantics(op);
            assert_eq!(semantics.volatility, Volatility::Immutable);
        }

        assert_eq!(cast_semantics().volatility, Volatility::Immutable);
    }

    #[test]
    fn constant_expression_is_foldable() {
        let expr = spanned(Expr::Call {
            function: FunctionName::Lower,
            args: vec![spanned(Expr::Literal(Literal::String("ABC".to_string())))],
        });

        let semantics = expr_semantics(&expr).expect("known function must have semantics");

        assert_eq!(semantics.dependency_scope, DependencyScope::Constant);
        assert_eq!(semantics.null_propagation, NullPropagation::NeverNull);
        assert!(semantics.supports_constant_folding());
    }

    #[test]
    fn row_local_expression_can_use_cse_but_not_constant_folding() {
        let expr = spanned(Expr::Call {
            function: FunctionName::Lower,
            args: vec![spanned(relay_ref("input", "name"))],
        });

        let semantics = expr_semantics(&expr).expect("builtin expression must have semantics");

        assert_eq!(semantics.dependency_scope, DependencyScope::RowLocal);
        assert_eq!(semantics.null_propagation, NullPropagation::Strict);
        assert!(semantics.supports_common_subexpression_elimination());
        assert!(!semantics.supports_constant_folding());
    }

    #[test]
    fn custom_null_behavior_is_preserved_in_composed_expressions() {
        let expr = spanned(Expr::Call {
            function: FunctionName::Length,
            args: vec![spanned(Expr::Call {
                function: FunctionName::Coalesce,
                args: vec![
                    spanned(relay_ref("input", "primary")),
                    spanned(relay_ref("input", "fallback")),
                ],
            })],
        });

        let semantics = expr_semantics(&expr).expect("builtin expression must have semantics");

        assert_eq!(semantics.null_propagation, NullPropagation::Custom);
        assert_eq!(semantics.dependency_scope, DependencyScope::RowLocal);
        assert_eq!(semantics.volatility, Volatility::Immutable);
    }

    #[test]
    fn erroring_operations_block_constant_folding() {
        let expr = spanned(Expr::Cast {
            expr: Box::new(spanned(Expr::Literal(Literal::String("bad".to_string())))),
            data_type: arrow_schema::DataType::Int64,
        });

        let semantics = expr_semantics(&expr).expect("cast must have semantics");

        assert_eq!(
            semantics,
            ExpressionSemantics {
                volatility: Volatility::Immutable,
                dependency_scope: DependencyScope::Constant,
                has_side_effects: false,
                can_error: true,
                null_propagation: NullPropagation::NeverNull,
            }
        );
        assert!(!semantics.supports_constant_folding());
    }
}
