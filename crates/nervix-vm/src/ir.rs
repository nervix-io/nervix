use std::{fmt, sync::Arc};

use arrow_schema::{DataType, Schema, TimeUnit};
use nervix_nspl::vm_program::{BinaryOp, FunctionName, Span, UnaryOp};

use crate::semantics::BuiltinLowering;

macro_rules! declare_register_types {
    ($($Variant:ident => $field:ident, $setter:ident, $accessor:ident, $Array:ty, $data_type:path;)+) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display)]
        pub enum RegisterType {
            $($Variant,)+
            Datetime,
            Generic,
        }

        impl RegisterType {
            pub fn from_data_type(data_type: &DataType) -> Option<Self> {
                match data_type {
                    $($data_type => Some(Self::$Variant),)+
                    DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone))
                        if timezone.as_ref() == "+00:00" || timezone.as_ref() == "UTC" =>
                    {
                        Some(Self::Datetime)
                    }
                    DataType::List(_) | DataType::FixedSizeList(_, _) => Some(Self::Generic),
                    _ => None,
                }
            }

            pub fn data_type(self) -> DataType {
                match self {
                    $(Self::$Variant => $data_type,)+
                    Self::Datetime => {
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()))
                    }
                    Self::Generic => DataType::Null,
                }
            }
        }
    };
}

with_typed_registers!(declare_register_types);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display)]
#[strum(serialize_all = "lowercase")]
pub enum RegisterSpace {
    Input,
    Temp,
    Condition,
    Output,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RegisterRef {
    pub space: RegisterSpace,
    pub ty: RegisterType,
    pub index: usize,
}

impl RegisterRef {
    pub const fn new(space: RegisterSpace, ty: RegisterType, index: usize) -> Self {
        Self { space, ty, index }
    }
}

impl fmt::Display for RegisterRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}[{}]", self.space, self.ty, self.index)
    }
}

macro_rules! declare_register_layout {
    ($($Variant:ident => $field:ident, $setter:ident, $accessor:ident, $Array:ty, $data_type:path;)+) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
        pub struct RegisterLayout {
            $(pub $field: usize,)+
            pub datetime: usize,
            pub generic: usize,
        }

        impl RegisterLayout {
            pub fn alloc(&mut self, ty: RegisterType) -> usize {
                let slot = match ty {
                    $(RegisterType::$Variant => &mut self.$field,)+
                    RegisterType::Datetime => &mut self.datetime,
                    RegisterType::Generic => &mut self.generic,
                };
                let index = *slot;
                *slot += 1;
                index
            }
        }
    };
}

with_typed_registers!(declare_register_layout);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RegisterLayouts {
    pub inputs: RegisterLayout,
    pub temps: RegisterLayout,
    pub condition: RegisterLayout,
    pub outputs: RegisterLayout,
}

impl RegisterLayouts {
    pub fn alloc(&mut self, space: RegisterSpace, ty: RegisterType) -> RegisterRef {
        let index = match space {
            RegisterSpace::Input => self.inputs.alloc(ty),
            RegisterSpace::Temp => self.temps.alloc(ty),
            RegisterSpace::Condition => self.condition.alloc(ty),
            RegisterSpace::Output => self.outputs.alloc(ty),
        };
        RegisterRef::new(space, ty, index)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    Utf8(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AssignmentFallback {
    Uninitialized(DataType),
    Register(RegisterRef),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectArm {
    pub mask: RegisterRef,
    pub value: RegisterRef,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InstructionKind {
    Move {
        dst: RegisterRef,
        input: RegisterRef,
    },
    Assign {
        dst: RegisterRef,
        input: RegisterRef,
        fallback: AssignmentFallback,
    },
    Literal {
        dst: RegisterRef,
        value: ScalarValue,
    },
    NullLiteral {
        dst: RegisterRef,
        data_type: DataType,
    },
    Uninitialized {
        dst: RegisterRef,
        data_type: DataType,
    },
    Unary {
        dst: RegisterRef,
        input: RegisterRef,
        op: UnaryOp,
    },
    Binary {
        dst: RegisterRef,
        left: RegisterRef,
        right: RegisterRef,
        op: BinaryOp,
    },
    Cast {
        dst: RegisterRef,
        input: RegisterRef,
        target: RegisterType,
    },
    Builtin {
        dst: RegisterRef,
        lowering: BuiltinLowering,
        inputs: Vec<RegisterRef>,
    },
    Inject {
        dst: RegisterRef,
        function: FunctionName,
        inputs: Vec<RegisterRef>,
        output_type: DataType,
    },
    Select {
        dst: RegisterRef,
        arms: Vec<SelectArm>,
        otherwise: RegisterRef,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Instruction {
    pub kind: InstructionKind,
    pub span: Span,
    pub error_mask: Option<RegisterRef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InputBinding {
    pub column_index: usize,
    pub reg: RegisterRef,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutputBinding {
    pub output_index: usize,
    pub name: String,
    pub reg: RegisterRef,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InvocationBinding {
    pub function: FunctionName,
    pub inputs: Vec<RegisterRef>,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct CompiledProgram {
    pub input_schema: Arc<Schema>,
    pub output_schema: Arc<Schema>,
    pub inputs: Vec<InputBinding>,
    pub instructions: Vec<Instruction>,
    pub filter: Option<RegisterRef>,
    pub outputs: Vec<OutputBinding>,
    pub invocations: Vec<InvocationBinding>,
    pub layouts: RegisterLayouts,
    pub injector: Option<triomphe::Arc<Box<dyn crate::runtime::FunctionInjector>>>,
}

#[cfg(test)]
mod tests {
    use super::{RegisterLayouts, RegisterSpace, RegisterType};

    #[test]
    fn register_layouts_allocate_per_space_and_type() {
        let mut layouts = RegisterLayouts::default();

        assert_eq!(
            layouts.alloc(RegisterSpace::Input, RegisterType::Int64),
            super::RegisterRef::new(RegisterSpace::Input, RegisterType::Int64, 0)
        );
        assert_eq!(
            layouts.alloc(RegisterSpace::Condition, RegisterType::Boolean),
            super::RegisterRef::new(RegisterSpace::Condition, RegisterType::Boolean, 0)
        );
        assert_eq!(
            layouts.alloc(RegisterSpace::Temp, RegisterType::Int64),
            super::RegisterRef::new(RegisterSpace::Temp, RegisterType::Int64, 0)
        );
        assert_eq!(
            layouts.alloc(RegisterSpace::Output, RegisterType::Utf8),
            super::RegisterRef::new(RegisterSpace::Output, RegisterType::Utf8, 0)
        );
        assert_eq!(layouts.inputs.float64, 0);
    }

    #[test]
    fn register_type_maps_to_and_from_arrow_types() {
        for register_type in [
            RegisterType::Int64,
            RegisterType::Float64,
            RegisterType::Boolean,
            RegisterType::Utf8,
        ] {
            let data_type = register_type.data_type();
            assert_eq!(
                RegisterType::from_data_type(&data_type),
                Some(register_type)
            );
        }
    }
}
