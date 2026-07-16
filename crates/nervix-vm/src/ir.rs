use std::{fmt, sync::Arc};

use arrow_schema::{DataType, Schema, TimeUnit};
use nervix_nspl::vm_program::{BinaryOp, Span, UnaryOp};

use crate::semantics::BuiltinLowering;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RegisterType {
    UInt8,
    Int8,
    UInt16,
    Int16,
    UInt32,
    Int32,
    UInt64,
    Int64,
    Float32,
    Float64,
    Boolean,
    Utf8,
    Datetime,
    Generic,
}

impl RegisterType {
    pub fn from_data_type(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::UInt8 => Some(Self::UInt8),
            DataType::Int8 => Some(Self::Int8),
            DataType::UInt16 => Some(Self::UInt16),
            DataType::Int16 => Some(Self::Int16),
            DataType::UInt32 => Some(Self::UInt32),
            DataType::Int32 => Some(Self::Int32),
            DataType::UInt64 => Some(Self::UInt64),
            DataType::Int64 => Some(Self::Int64),
            DataType::Float32 => Some(Self::Float32),
            DataType::Float64 => Some(Self::Float64),
            DataType::Boolean => Some(Self::Boolean),
            DataType::Utf8 => Some(Self::Utf8),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(tz))
                if tz.as_ref() == "+00:00" || tz.as_ref() == "UTC" =>
            {
                Some(Self::Datetime)
            }
            DataType::List(_) | DataType::FixedSizeList(_, _) => Some(Self::Generic),
            _ => None,
        }
    }

    pub fn data_type(self) -> DataType {
        match self {
            Self::UInt8 => DataType::UInt8,
            Self::Int8 => DataType::Int8,
            Self::UInt16 => DataType::UInt16,
            Self::Int16 => DataType::Int16,
            Self::UInt32 => DataType::UInt32,
            Self::Int32 => DataType::Int32,
            Self::UInt64 => DataType::UInt64,
            Self::Int64 => DataType::Int64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Boolean => DataType::Boolean,
            Self::Utf8 => DataType::Utf8,
            Self::Datetime => DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            Self::Generic => DataType::Null,
        }
    }
}

impl fmt::Display for RegisterType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UInt8 => write!(f, "UInt8"),
            Self::Int8 => write!(f, "Int8"),
            Self::UInt16 => write!(f, "UInt16"),
            Self::Int16 => write!(f, "Int16"),
            Self::UInt32 => write!(f, "UInt32"),
            Self::Int32 => write!(f, "Int32"),
            Self::UInt64 => write!(f, "UInt64"),
            Self::Int64 => write!(f, "Int64"),
            Self::Float32 => write!(f, "Float32"),
            Self::Float64 => write!(f, "Float64"),
            Self::Boolean => write!(f, "Boolean"),
            Self::Utf8 => write!(f, "Utf8"),
            Self::Datetime => write!(f, "Datetime"),
            Self::Generic => write!(f, "Generic"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RegisterSpace {
    Input,
    Temp,
    Condition,
    Output,
}

impl fmt::Display for RegisterSpace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Input => write!(f, "input"),
            Self::Temp => write!(f, "temp"),
            Self::Condition => write!(f, "condition"),
            Self::Output => write!(f, "output"),
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RegisterLayout {
    pub uint8: usize,
    pub int8: usize,
    pub uint16: usize,
    pub int16: usize,
    pub uint32: usize,
    pub int32: usize,
    pub uint64: usize,
    pub int64: usize,
    pub float32: usize,
    pub float64: usize,
    pub boolean: usize,
    pub utf8: usize,
    pub datetime: usize,
    pub generic: usize,
}

impl RegisterLayout {
    pub fn alloc(&mut self, ty: RegisterType) -> usize {
        match ty {
            RegisterType::UInt8 => {
                let index = self.uint8;
                self.uint8 += 1;
                index
            }
            RegisterType::Int8 => {
                let index = self.int8;
                self.int8 += 1;
                index
            }
            RegisterType::UInt16 => {
                let index = self.uint16;
                self.uint16 += 1;
                index
            }
            RegisterType::Int16 => {
                let index = self.int16;
                self.int16 += 1;
                index
            }
            RegisterType::UInt32 => {
                let index = self.uint32;
                self.uint32 += 1;
                index
            }
            RegisterType::Int32 => {
                let index = self.int32;
                self.int32 += 1;
                index
            }
            RegisterType::UInt64 => {
                let index = self.uint64;
                self.uint64 += 1;
                index
            }
            RegisterType::Int64 => {
                let index = self.int64;
                self.int64 += 1;
                index
            }
            RegisterType::Float32 => {
                let index = self.float32;
                self.float32 += 1;
                index
            }
            RegisterType::Float64 => {
                let index = self.float64;
                self.float64 += 1;
                index
            }
            RegisterType::Boolean => {
                let index = self.boolean;
                self.boolean += 1;
                index
            }
            RegisterType::Utf8 => {
                let index = self.utf8;
                self.utf8 += 1;
                index
            }
            RegisterType::Datetime => {
                let index = self.datetime;
                self.datetime += 1;
                index
            }
            RegisterType::Generic => {
                let index = self.generic;
                self.generic += 1;
                index
            }
        }
    }
}

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
pub enum InstructionKind {
    Move {
        dst: RegisterRef,
        input: RegisterRef,
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct Instruction {
    pub kind: InstructionKind,
    pub span: Span,
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

#[derive(Debug, Clone)]
pub struct CompiledProgram {
    pub input_schema: Arc<Schema>,
    pub output_schema: Arc<Schema>,
    pub inputs: Vec<InputBinding>,
    pub instructions: Vec<Instruction>,
    pub filter: Option<RegisterRef>,
    pub branch_filters: Vec<RegisterRef>,
    pub outputs: Vec<OutputBinding>,
    pub layouts: RegisterLayouts,
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
