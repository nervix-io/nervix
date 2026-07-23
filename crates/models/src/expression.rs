use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{Identifier, ParseAsType};

/// An executable NSPL expression with parser-only spans removed.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(bytecheck(bounds(__C: rkyv::validation::ArchiveContext)))]
pub enum Expression {
    Literal(Literal),
    Field(FieldReference),
    Unary {
        operator: UnaryOperator,
        #[rkyv(omit_bounds)]
        expression: Box<Self>,
    },
    Binary {
        operator: BinaryOperator,
        #[rkyv(omit_bounds)]
        left: Box<Self>,
        #[rkyv(omit_bounds)]
        right: Box<Self>,
    },
    Cast {
        #[rkyv(omit_bounds)]
        expression: Box<Self>,
        target: ParseAsType,
    },
    Call {
        function: Identifier,
        #[rkyv(omit_bounds)]
        arguments: Vec<Self>,
    },
    Array(#[rkyv(omit_bounds)] Vec<Self>),
    If {
        #[rkyv(omit_bounds)]
        condition: Box<Self>,
        #[rkyv(omit_bounds)]
        then_result: Box<Self>,
        #[rkyv(omit_bounds)]
        else_result: Box<Self>,
    },
    Case {
        #[rkyv(omit_bounds)]
        operand: Option<Box<Self>>,
        #[rkyv(omit_bounds)]
        branches: Vec<CaseBranch>,
        #[rkyv(omit_bounds)]
        else_result: Option<Box<Self>>,
    },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(bytecheck(bounds(
    __C: rkyv::validation::ArchiveContext,
    __C::Error: rkyv::rancor::Source,
)))]
pub struct CaseBranch {
    #[rkyv(omit_bounds)]
    pub when: Expression,
    #[rkyv(omit_bounds)]
    pub result: Expression,
}

impl Expression {
    pub fn visit_fields(&self, visitor: &mut impl FnMut(&FieldReference)) {
        match self {
            Self::Literal(_) => {}
            Self::Field(field) => visitor(field),
            Self::Unary { expression, .. } | Self::Cast { expression, .. } => {
                expression.visit_fields(visitor);
            }
            Self::Binary { left, right, .. } => {
                left.visit_fields(visitor);
                right.visit_fields(visitor);
            }
            Self::Call { arguments, .. } => {
                for argument in arguments {
                    argument.visit_fields(visitor);
                }
            }
            Self::Array(items) => {
                for item in items {
                    item.visit_fields(visitor);
                }
            }
            Self::If {
                condition,
                then_result,
                else_result,
            } => {
                condition.visit_fields(visitor);
                then_result.visit_fields(visitor);
                else_result.visit_fields(visitor);
            }
            Self::Case {
                operand,
                branches,
                else_result,
            } => {
                if let Some(operand) = operand {
                    operand.visit_fields(visitor);
                }
                for branch in branches {
                    branch.when.visit_fields(visitor);
                    branch.result.visit_fields(visitor);
                }
                if let Some(else_result) = else_result {
                    else_result.visit_fields(visitor);
                }
            }
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum Literal {
    I64(i64),
    F64(Float64Literal),
    Bool(bool),
    String(String),
    Null,
}

/// Bit-preserving floating-point literal representation with total equality.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(transparent)]
pub struct Float64Literal(u64);

impl Float64Literal {
    pub fn new(value: f64) -> Self {
        Self(value.to_bits())
    }

    pub fn value(self) -> f64 {
        f64::from_bits(self.0)
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct FieldReference {
    pub scope: FieldScope,
    pub field: Identifier,
}

impl FieldReference {
    pub fn bare(field: Identifier) -> Self {
        Self {
            scope: FieldScope::Bare,
            field,
        }
    }

    pub fn scoped(scope: FieldScope, field: Identifier) -> Self {
        Self { scope, field }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum FieldScope {
    Bare,
    Message,
    Input,
    Output,
    Branch,
    Left,
    Right,
    RelayState { relay: Identifier },
    Metadata,
    PartialOutput,
    Error,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum UnaryOperator {
    Negate,
    Not,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum BinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    And,
    Or,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct Assignment {
    pub target: AssignmentTarget,
    pub value: Expression,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct AssignmentTarget {
    pub scope: AssignmentTargetScope,
    pub field: Identifier,
}

impl AssignmentTarget {
    pub fn bare(field: Identifier) -> Self {
        Self {
            scope: AssignmentTargetScope::Bare,
            field,
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum AssignmentTargetScope {
    Bare,
    Message,
    Output,
    Branch,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum Inheritance {
    All,
    AllExcept(Vec<Identifier>),
    Fields(Vec<InheritedField>),
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct InheritedField {
    pub field: Identifier,
    pub leak_sensitive: bool,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct Invocation {
    pub function: Identifier,
    pub arguments: Vec<Expression>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ExternalValue {
    pub name: String,
    pub value: Expression,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Default,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct RouteConstruction {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherit: Option<Inheritance>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assignments: Vec<Assignment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub where_clause: Option<Expression>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub invocations: Vec<Invocation>,
}

impl RouteConstruction {
    pub fn is_empty(&self) -> bool {
        self.inherit.is_none()
            && self.assignments.is_empty()
            && self.where_clause.is_none()
            && self.invocations.is_empty()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum MaterializedStatePolicy {
    RequiredSkip,
    RequiredWait,
    Default(Vec<Assignment>),
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct MaterializedStateDependency {
    pub relay: Identifier,
    pub policy: MaterializedStatePolicy,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum OutputBranch {
    BranchedBy {
        branch: Identifier,
        assignments: Vec<Assignment>,
    },
    Unbranched,
}

impl OutputBranch {
    pub fn branch(&self) -> Option<&Identifier> {
        match self {
            Self::BranchedBy { branch, .. } => Some(branch),
            Self::Unbranched => None,
        }
    }

    pub fn assignments(&self) -> &[Assignment] {
        match self {
            Self::BranchedBy { assignments, .. } => assignments,
            Self::Unbranched => &[],
        }
    }
}
