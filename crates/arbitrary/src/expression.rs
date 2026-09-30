//! Expressions, their literals and field references, the types they convert to, and JSON paths.

use std::num::{NonZeroU32, NonZeroUsize};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    BinaryOperator, CaseBranch, Expression, FieldReference, FieldScope, Float64Literal, JsonPath,
    JsonPathStep, Literal, MembershipOperator, ParseAsType, RangeOperator, UnaryOperator,
};

use crate::{Arbitrary, Domain};

/// How deeply a generated expression nests. Every operand below this depth is a leaf, which keeps
/// a generated expression, and the archive depth it needs, bounded.
pub const EXPRESSION_DEPTH: u8 = 4;

/// How deeply a generated collection type nests its element types.
const TYPE_DEPTH: u8 = 3;

/// The most elements a generated list inside an expression holds: call arguments, array items, a
/// membership set or the branches of a `CASE`.
const LIST_ITEMS: usize = 4;

/// Built-in functions a call names beside generated names. Several share their spelling with a
/// statement keyword. `max` is not among them: every region that ends at `MAX` stops at a call to
/// it, so NSPL cannot spell such a call there, and generating it only where it can be read would
/// make every expression depend on where it is written.
const FUNCTIONS: [&str; 15] = [
    "lower", "upper", "now", "coalesce", "abs", "concat", "length", "greatest", "least", "clamp",
    "min", "sum", "count", "first", "last",
];

/// Every scalar type a conversion names.
const SCALAR_TYPES: [ParseAsType; 14] = [
    ParseAsType::U8,
    ParseAsType::I8,
    ParseAsType::U16,
    ParseAsType::I16,
    ParseAsType::U32,
    ParseAsType::I32,
    ParseAsType::U64,
    ParseAsType::I64,
    ParseAsType::Bool,
    ParseAsType::String,
    ParseAsType::Datetime,
    ParseAsType::F32,
    ParseAsType::F64,
    ParseAsType::Bytes,
];

const BINARY_OPERATORS: [BinaryOperator; 15] = [
    BinaryOperator::Add,
    BinaryOperator::Subtract,
    BinaryOperator::Multiply,
    BinaryOperator::Divide,
    BinaryOperator::Remainder,
    BinaryOperator::Equal,
    BinaryOperator::NotEqual,
    BinaryOperator::GreaterThan,
    BinaryOperator::LessThan,
    BinaryOperator::GreaterThanOrEqual,
    BinaryOperator::LessThanOrEqual,
    BinaryOperator::And,
    BinaryOperator::Or,
    BinaryOperator::IsDistinctFrom,
    BinaryOperator::IsNotDistinctFrom,
];

/// Every form of [`Expression`], so a coverage check can ask the generator for each one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum ExpressionForm {
    Literal,
    Field,
    Unary,
    Binary,
    Cast,
    Call,
    UdfCall,
    Array,
    If,
    Case,
    Membership,
    Range,
    TryCast,
    JsonValue,
    TryJsonValue,
    JsonExists,
}

impl ExpressionForm {
    /// The form `expression` takes. The match is exhaustive, so a new form of expression does not
    /// compile until the generator is taught to build it.
    pub fn of(expression: &Expression) -> Self {
        match expression {
            Expression::Literal(_) => Self::Literal,
            Expression::Field(_) => Self::Field,
            Expression::Unary { .. } => Self::Unary,
            Expression::Binary { .. } => Self::Binary,
            Expression::Cast { .. } => Self::Cast,
            Expression::Call { .. } => Self::Call,
            Expression::UdfCall { .. } => Self::UdfCall,
            Expression::Array(_) => Self::Array,
            Expression::If { .. } => Self::If,
            Expression::Case { .. } => Self::Case,
            Expression::Membership { .. } => Self::Membership,
            Expression::Range { .. } => Self::Range,
            Expression::TryCast { .. } => Self::TryCast,
            Expression::JsonValue { .. } => Self::JsonValue,
            Expression::TryJsonValue { .. } => Self::TryJsonValue,
            Expression::JsonExists { .. } => Self::JsonExists,
        }
    }

    /// Every form, in declaration order.
    pub const ALL: [Self; 16] = [
        Self::Literal,
        Self::Field,
        Self::Unary,
        Self::Binary,
        Self::Cast,
        Self::Call,
        Self::UdfCall,
        Self::Array,
        Self::If,
        Self::Case,
        Self::Membership,
        Self::Range,
        Self::TryCast,
        Self::JsonValue,
        Self::TryJsonValue,
        Self::JsonExists,
    ];
}

/// The depth an operand of a node at `depth` is built within.
fn operand_depth(depth: u8) -> u8 {
    let Some(below) = depth.checked_sub(1) else {
        // A node already at the bottom has only leaves beneath it.
        return 0;
    };
    below
}

impl Arbitrary<'_> {
    /// An expression nested at most [`EXPRESSION_DEPTH`] deep.
    pub fn expression(&mut self) -> Expression {
        self.expression_within(EXPRESSION_DEPTH)
    }

    /// An expression of the requested form, whose operands nest at most `depth` levels below it.
    pub fn expression_of(&mut self, form: ExpressionForm, depth: u8) -> Expression {
        let below = operand_depth(depth);
        match form {
            ExpressionForm::Literal => Expression::Literal(self.literal()),
            ExpressionForm::Field => Expression::Field(self.field_reference()),
            ExpressionForm::Unary => Expression::Unary {
                operator: self
                    .entropy
                    .pick([UnaryOperator::Negate, UnaryOperator::Not]),
                expression: Box::new(self.expression_within(below)),
            },
            ExpressionForm::Binary => Expression::Binary {
                operator: self.entropy.pick(BINARY_OPERATORS),
                left: Box::new(self.expression_within(below)),
                right: Box::new(self.expression_within(below)),
            },
            ExpressionForm::Cast => Expression::Cast {
                expression: Box::new(self.expression_within(below)),
                target: self.conversion_type(),
            },
            ExpressionForm::Call => Expression::Call {
                function: self.function_name(),
                arguments: self.expression_list(below, 0),
            },
            ExpressionForm::UdfCall => Expression::UdfCall {
                function: self.name(),
                arguments: self.expression_list(below, 0),
            },
            ExpressionForm::Array => {
                let minimum = self.non_empty_in_nspl();
                Expression::Array(self.expression_list(below, minimum))
            }
            ExpressionForm::If => Expression::If {
                condition: Box::new(self.expression_within(below)),
                then_result: Box::new(self.expression_within(below)),
                else_result: Box::new(self.expression_within(below)),
            },
            ExpressionForm::Case => {
                let operand = if self.entropy.flag() {
                    Some(Box::new(self.expression_within(below)))
                } else {
                    None
                };
                let minimum = self.non_empty_in_nspl();
                let count = self.list_length(minimum);
                let mut branches = Vec::with_capacity(count);
                for _ in 0..count {
                    branches.push(CaseBranch {
                        when: self.expression_within(below),
                        result: self.expression_within(below),
                    });
                }
                let else_result = if self.entropy.flag() {
                    Some(Box::new(self.expression_within(below)))
                } else {
                    None
                };
                Expression::Case {
                    operand,
                    branches,
                    else_result,
                }
            }
            ExpressionForm::Membership => Expression::Membership {
                operator: self
                    .entropy
                    .pick([MembershipOperator::In, MembershipOperator::NotIn]),
                operand: Box::new(self.expression_within(below)),
                set: self.expression_list(below, 0),
            },
            ExpressionForm::Range => Expression::Range {
                operator: self
                    .entropy
                    .pick([RangeOperator::Between, RangeOperator::NotBetween]),
                operand: Box::new(self.expression_within(below)),
                low: Box::new(self.expression_within(below)),
                high: Box::new(self.expression_within(below)),
            },
            ExpressionForm::TryCast => Expression::TryCast {
                expression: Box::new(self.expression_within(below)),
                target: self.conversion_type(),
            },
            ExpressionForm::JsonValue => Expression::JsonValue {
                document: Box::new(self.expression_within(below)),
                path: self.json_path(),
                target: self.declared_type(),
            },
            ExpressionForm::TryJsonValue => Expression::TryJsonValue {
                document: Box::new(self.expression_within(below)),
                path: self.json_path(),
                target: self.declared_type(),
            },
            ExpressionForm::JsonExists => Expression::JsonExists {
                document: Box::new(self.expression_within(below)),
                path: self.json_path(),
            },
        }
    }

    fn expression_within(&mut self, depth: u8) -> Expression {
        if depth == 0 {
            return if self.entropy.flag() {
                Expression::Literal(self.literal())
            } else {
                Expression::Field(self.field_reference())
            };
        }
        let form = self.entropy.pick(ExpressionForm::ALL);
        self.expression_of(form, depth)
    }

    /// Up to [`LIST_ITEMS`] expressions, at least `minimum` of them.
    fn expression_list(&mut self, depth: u8, minimum: usize) -> Vec<Expression> {
        let count = self.list_length(minimum);
        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            items.push(self.expression_within(depth));
        }
        items
    }

    fn list_length(&mut self, minimum: usize) -> usize {
        let extra = LIST_ITEMS
            .checked_sub(minimum)
            .assured("a list's minimum never exceeds its bound");
        minimum
            .checked_add(self.entropy.count(extra))
            .verified("the extra count is at most the bound minus the minimum")
    }

    /// One in NSPL, which spells no empty array and no `CASE` without a `WHEN`; zero otherwise.
    fn non_empty_in_nspl(&self) -> usize {
        match self.domain {
            Domain::Nspl => 1,
            Domain::Vocabulary => 0,
        }
    }

    /// A literal of any type.
    ///
    /// In NSPL a number literal is never negative, because `-5` is the negation of `5`, and a float
    /// is finite; a negative value is built as a negation around a literal instead. The vocabulary
    /// holds any `i64` and any `f64` bit pattern, NaN payloads and negative zero included.
    pub fn literal(&mut self) -> Literal {
        match self.entropy.byte() % 5 {
            0 => Literal::I64(self.integer_literal()),
            1 => Literal::F64(self.float_literal()),
            2 => Literal::Bool(self.entropy.flag()),
            3 => Literal::String(self.string()),
            _ => Literal::Null,
        }
    }

    fn integer_literal(&mut self) -> i64 {
        match self.domain {
            Domain::Nspl => {
                let maximum = u64::try_from(i64::MAX).assured("i64::MAX is positive");
                let value = self.entropy.boundary_biased(0..=maximum);
                i64::try_from(value).verified("the range above ends at i64::MAX")
            }
            Domain::Vocabulary => self.entropy.any_i64(),
        }
    }

    fn float_literal(&mut self) -> Float64Literal {
        let value = match self.entropy.byte() % 12 {
            0 => 0.0,
            1 => 1.0,
            2 => 0.1,
            3 => f64::MIN_POSITIVE,
            4 => f64::from_bits(1),
            5 => f64::MAX,
            6 => 1e21,
            7 => 1e-7,
            8 => 123.456,
            _ => f64::from_bits(self.entropy.any_u64()),
        };
        match self.domain {
            Domain::Vocabulary => Float64Literal::new(value),
            Domain::Nspl => Float64Literal::new(Self::spellable_float(value)),
        }
    }

    /// The finite, non-negative float closest in spirit to `value`: its magnitude, with a
    /// non-finite value replaced by the largest finite one.
    fn spellable_float(value: f64) -> f64 {
        let magnitude = value.abs();
        if magnitude.is_finite() {
            magnitude
        } else {
            f64::MAX
        }
    }

    /// A reference to a field in any scope.
    pub fn field_reference(&mut self) -> FieldReference {
        let scope = match self.entropy.byte() % 11 {
            0 => FieldScope::Bare,
            1 => FieldScope::Message,
            2 => FieldScope::Input,
            3 if self.region.output_scope => FieldScope::Input,
            3 => FieldScope::Output,
            4 => FieldScope::Branch,
            5 => FieldScope::Left,
            6 if self.region.right_scope => FieldScope::Left,
            6 => FieldScope::Right,
            7 => FieldScope::RelayState { relay: self.name() },
            8 => FieldScope::Metadata,
            9 => FieldScope::PartialOutput,
            _ => FieldScope::Error,
        };
        FieldReference {
            scope,
            field: self.name(),
        }
    }

    /// The name a call invokes: a built-in function, spelled even where it is also a keyword of
    /// the statement around it, or a generated name.
    pub fn function_name(&mut self) -> nervix_models::BuiltinFunctionName {
        if self.entropy.flag() {
            let spelled = self.entropy.pick(FUNCTIONS);
            spelled
                .parse()
                .assured("every listed built-in function spells a valid name")
        } else {
            self.name()
        }
    }

    /// A type a conversion names. NSPL converts only to scalar types; the vocabulary holds any.
    pub fn conversion_type(&mut self) -> ParseAsType {
        match self.domain {
            Domain::Nspl => self.entropy.pick(SCALAR_TYPES),
            Domain::Vocabulary => self.declared_type(),
        }
    }

    /// A type a schema field or a JSON read declares: a scalar, or a list or fixed-length array of
    /// declared types nested at most [`TYPE_DEPTH`] deep.
    pub fn declared_type(&mut self) -> ParseAsType {
        self.declared_type_within(TYPE_DEPTH)
    }

    fn declared_type_within(&mut self, depth: u8) -> ParseAsType {
        if depth == 0 {
            return self.entropy.pick(SCALAR_TYPES);
        }
        let below = operand_depth(depth);
        match self.entropy.byte() % 4 {
            0 => ParseAsType::Vec {
                element: Box::new(self.declared_type_within(below)),
            },
            1 => {
                let longest = self.longest_array();
                let len = self.entropy.boundary_biased(1..=u64::from(longest));
                let len = u32::try_from(len).verified("the range above ends at a u32");
                ParseAsType::Array {
                    element: Box::new(self.declared_type_within(below)),
                    len: NonZeroU32::new(len).verified("the range above starts at one"),
                }
            }
            _ => self.entropy.pick(SCALAR_TYPES),
        }
    }

    /// The longest fixed-length array a declared type holds. NSPL reads a length only up to
    /// `i32::MAX`, the longest fixed-size list Arrow carries; the vocabulary holds any `u32`.
    fn longest_array(&self) -> u32 {
        match self.domain {
            Domain::Nspl => u32::try_from(i32::MAX).assured("i32::MAX is positive"),
            Domain::Vocabulary => u32::MAX,
        }
    }

    /// A JSON path of up to [`JsonPath::MAX_STEPS`] steps. A member is named by any string, an
    /// element by any `u32` index.
    pub fn json_path(&mut self) -> JsonPath {
        let most = NonZeroUsize::new(JsonPath::MAX_STEPS).assured("a path allows some steps");
        let count = match self.entropy.byte() % 4 {
            0 => most.get(),
            _ => self.entropy.count(6),
        };
        let mut steps = Vec::with_capacity(count);
        for _ in 0..count {
            let step = if self.entropy.flag() {
                let index = self.entropy.boundary_biased(0..=u64::from(u32::MAX));
                JsonPathStep::Element(u32::try_from(index).verified("the range ends at u32::MAX"))
            } else if self.entropy.flag() {
                JsonPathStep::Member(self.name_text())
            } else {
                JsonPathStep::Member(self.string())
            };
            steps.push(step);
        }
        JsonPath::new(steps).verified("the step count is at most the path's own bound")
    }
}
