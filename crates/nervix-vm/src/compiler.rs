use std::{
    collections::{BTreeMap, BTreeSet},
    hash::Hash,
    sync::Arc,
};

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use arrow_schema::{DataType, Field, Schema};
use nervix_nspl::vm_program::{
    BinaryOp, CaseArm, Expr, FieldRef, FunctionName, InternalFieldNamespace, InternalFieldRef,
    Literal, Program, Span, SpannedExpr, SpannedNode, UnaryOp, WindowAggregateFunction,
};

use crate::{
    error::CompileError,
    ir::{
        AssignmentFallback, CompiledProgram, InputBinding, Instruction, InstructionKind,
        InvocationBinding, OutputBinding, RegisterLayouts, RegisterRef, RegisterSpace,
        RegisterType, ScalarValue, SelectArm,
    },
    semantics::{
        BuiltinLowering, binary_descriptor, binary_output_type, builtin_descriptor,
        builtin_semantics_for_lowering, builtin_signature, cast_descriptor, expr_semantics,
        unary_descriptor,
    },
};

#[derive(Clone)]
struct ColumnBinding {
    data_type: DataType,
    nullable: bool,
    sensitive: bool,
    value: ColumnValue,
}

#[derive(Clone, Copy)]
enum ColumnValue {
    Initialized(RegisterRef),
    Uninitialized,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CompileNamespace {
    User(String),
    Internal(InternalFieldNamespace),
}

impl CompileNamespace {
    pub fn label(&self) -> String {
        match self {
            Self::User(namespace) => namespace.clone(),
            Self::Internal(InternalFieldNamespace::LookupHashMap) => {
                "internal LOOKUP_HASH_MAP".to_string()
            }
        }
    }

    pub fn qualified_field_name(&self, field_name: &str) -> String {
        format!("{}.{}", self.label(), field_name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum BoundFieldRef {
    User(FieldRef),
    Internal(InternalFieldRef),
}

#[derive(Debug, Clone)]
pub struct CompileBinding {
    pub namespace: CompileNamespace,
    pub schema: Arc<Schema>,
    pub sensitivity: SchemaSensitivity,
    pub readable: bool,
    pub writable: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaSensitivity {
    sensitive_fields: BTreeSet<String>,
}

impl SchemaSensitivity {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from_sensitive_fields(fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            sensitive_fields: fields.into_iter().map(Into::into).collect(),
        }
    }

    pub fn is_sensitive(&self, field_name: &str) -> bool {
        self.sensitive_fields.contains(field_name)
    }

    pub fn field_names(&self) -> impl Iterator<Item = &str> {
        self.sensitive_fields.iter().map(String::as_str)
    }

    fn validate_against_schema(&self, schema: &Schema, role: &str) -> Result<(), CompileError> {
        for field_name in self.field_names() {
            if schema.field_with_name(field_name).is_err() {
                return Err(CompileError {
                    code: "unknown_sensitive_field",
                    message: format!(
                        "{role} sensitivity marks unknown field '{field_name}' as sensitive"
                    ),
                    span: (0..0).into(),
                });
            }
        }
        Ok(())
    }
}

impl CompileBinding {
    pub fn writable(namespace: impl Into<String>, schema: Arc<Schema>) -> Self {
        Self {
            namespace: CompileNamespace::User(namespace.into()),
            schema,
            sensitivity: SchemaSensitivity::default(),
            readable: true,
            writable: true,
        }
    }

    pub fn writeonly(namespace: impl Into<String>, schema: Arc<Schema>) -> Self {
        Self {
            namespace: CompileNamespace::User(namespace.into()),
            schema,
            sensitivity: SchemaSensitivity::default(),
            readable: false,
            writable: true,
        }
    }

    pub fn readonly(namespace: impl Into<String>, schema: Arc<Schema>) -> Self {
        Self {
            namespace: CompileNamespace::User(namespace.into()),
            schema,
            sensitivity: SchemaSensitivity::default(),
            readable: true,
            writable: false,
        }
    }

    pub fn internal_readonly(namespace: InternalFieldNamespace, schema: Arc<Schema>) -> Self {
        Self {
            namespace: CompileNamespace::Internal(namespace),
            schema,
            sensitivity: SchemaSensitivity::default(),
            readable: true,
            writable: false,
        }
    }

    pub fn with_sensitivity(mut self, sensitivity: SchemaSensitivity) -> Self {
        self.sensitivity = sensitivity;
        self
    }
}

struct Compiler {
    readable_namespaces: HashSet<CompileNamespace>,
    writable_namespaces: HashSet<String>,
    default_passthrough_namespace: String,
    columns: HashMap<BoundFieldRef, ColumnBinding>,
    inputs: Vec<InputBinding>,
    instructions: Vec<Instruction>,
    layouts: RegisterLayouts,
    expr_cache: HashMap<CachedExpr, RegisterRef>,
    expr_cache_generation: usize,
    current_error_mask: Option<RegisterRef>,
    allow_header_reads: bool,
    allow_header_writes: bool,
    udf_signatures: UdfSignatures,
    injector: Option<triomphe::Arc<Box<dyn crate::runtime::FunctionInjector>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CachedExpr {
    generation: usize,
    expr: ExprKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ExprKey {
    Literal(LiteralKey),
    FieldRef(FieldRef),
    InternalFieldRef(InternalFieldRef),
    Unary {
        op: UnaryOp,
        expr: Box<ExprKey>,
    },
    Binary {
        op: BinaryOp,
        left: Box<ExprKey>,
        right: Box<ExprKey>,
    },
    Cast {
        expr: Box<ExprKey>,
        data_type: DataType,
    },
    Call {
        function: FunctionName,
        args: Vec<ExprKey>,
    },
    Case {
        operand: Option<Box<ExprKey>>,
        branches: Vec<(ExprKey, ExprKey)>,
        else_result: Option<Box<ExprKey>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum LiteralKey {
    Int64(i64),
    Float64(u64),
    Boolean(bool),
    Utf8(String),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
enum FoldedValue {
    NonNull(ScalarValue),
    Null(RegisterType),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    PassthroughByName,
    ExplicitOnly,
}

#[derive(Debug, Clone)]
pub struct CompileOptions {
    pub optimize_temp_registers: bool,
    pub output_mode: OutputMode,
    pub allow_sensitive_output: bool,
    pub allow_header_reads: bool,
    pub allow_header_writes: bool,
    pub udf_signatures: UdfSignatures,
    pub injector: Option<triomphe::Arc<Box<dyn crate::runtime::FunctionInjector>>>,
}

impl Default for CompileOptions {
    fn default() -> Self {
        Self {
            optimize_temp_registers: true,
            output_mode: OutputMode::PassthroughByName,
            allow_sensitive_output: false,
            allow_header_reads: false,
            allow_header_writes: false,
            udf_signatures: UdfSignatures::default(),
            injector: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdfParameter {
    pub data_type: DataType,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdfSignature {
    pub arguments: Vec<UdfParameter>,
    pub return_type: DataType,
    pub return_optional: bool,
    pub volatile: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UdfSignatures {
    signatures: BTreeMap<String, UdfSignature>,
}

impl UdfSignatures {
    pub fn insert(&mut self, name: impl Into<String>, signature: UdfSignature) {
        self.signatures
            .insert(name.into().to_ascii_lowercase(), signature);
    }

    pub fn get(&self, name: &str) -> Option<&UdfSignature> {
        self.signatures.get(&name.to_ascii_lowercase())
    }

    pub fn is_empty(&self) -> bool {
        self.signatures.is_empty()
    }
}

impl Compiler {
    fn udf_call_type(
        &self,
        name: &str,
        args: &[SpannedExpr],
        span: Span,
    ) -> Result<DataType, CompileError> {
        let signature = self.udf_signatures.get(name).ok_or_else(|| CompileError {
            code: "unknown_function",
            message: format!("unknown function '{name}' with arity {}", args.len()),
            span,
        })?;
        if signature.arguments.len() != args.len() {
            return Err(CompileError {
                code: "invalid_function_arity",
                message: format!(
                    "UDF '{name}' expects exactly {} arguments, found {}",
                    signature.arguments.len(),
                    args.len()
                ),
                span,
            });
        }
        for (index, (argument, parameter)) in args.iter().zip(&signature.arguments).enumerate() {
            let actual = self.infer_expr_type(argument)?;
            if actual != parameter.data_type {
                return Err(CompileError {
                    code: "type_mismatch",
                    message: format!(
                        "UDF '{name}' argument {} requires {:?}, found {:?}; cast explicitly",
                        index + 1,
                        parameter.data_type,
                        actual
                    ),
                    span: argument.span,
                });
            }
        }
        Ok(signature.return_type.clone())
    }

    fn header_values_type() -> DataType {
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)))
    }

    fn injected_header_call_type(
        &self,
        function: &FunctionName,
        args: &[SpannedExpr],
        span: Span,
    ) -> Result<DataType, CompileError> {
        if !self.allow_header_reads {
            return Err(CompileError {
                code: "unsupported_function_context",
                message: format!(
                    "function '{}' is only available to ingestors whose connector supports headers",
                    function.as_str()
                ),
                span,
            });
        }
        let [name] = args else {
            return Err(CompileError {
                code: "invalid_function_arity",
                message: format!(
                    "function '{}' expects exactly 1 argument, found {}",
                    function.as_str(),
                    args.len()
                ),
                span,
            });
        };
        let name_type = self.infer_expr_type(name)?;
        if name_type != DataType::Utf8 {
            return Err(CompileError {
                code: "type_mismatch",
                message: format!(
                    "function '{}' requires a STRING header name, found {name_type:?}",
                    function.as_str()
                ),
                span: name.span,
            });
        }
        if let FunctionName::ReadHeader = function {
            Ok(DataType::Utf8)
        } else {
            Ok(Self::header_values_type())
        }
    }

    fn injected_window_aggregate_call_type(
        &self,
        function: WindowAggregateFunction,
        args: &[SpannedExpr],
        span: Span,
    ) -> Result<DataType, CompileError> {
        if args.len() != function.expected_arity() {
            return Err(CompileError {
                code: "invalid_function_arity",
                message: format!(
                    "function '{}' expects exactly {} argument(s), found {}",
                    function.nspl_name(),
                    function.expected_arity(),
                    args.len()
                ),
                span,
            });
        }
        let input_type = self.infer_expr_type(&args[0])?;
        match function {
            WindowAggregateFunction::Count => Ok(DataType::Int64),
            WindowAggregateFunction::PercentileLinearHistogram => {
                if !input_type.is_numeric() {
                    return Err(CompileError {
                        code: "type_mismatch",
                        message: format!(
                            "function '{}' requires a numeric input, found {input_type:?}",
                            function.nspl_name()
                        ),
                        span: args[0].span,
                    });
                }
                Ok(DataType::Float64)
            }
            WindowAggregateFunction::Sum => {
                if !input_type.is_numeric() {
                    return Err(CompileError {
                        code: "type_mismatch",
                        message: format!(
                            "function '{}' requires a numeric input, found {input_type:?}",
                            function.nspl_name()
                        ),
                        span: args[0].span,
                    });
                }
                Ok(input_type)
            }
            WindowAggregateFunction::First
            | WindowAggregateFunction::Last
            | WindowAggregateFunction::Max
            | WindowAggregateFunction::Min => Ok(input_type),
        }
    }

    fn new(bindings: &[CompileBinding]) -> Result<(Self, Arc<Schema>), CompileError> {
        let mut layouts = RegisterLayouts::default();
        let mut inputs = Vec::new();
        let mut columns = HashMap::new();
        let mut input_fields = Vec::new();
        let single_binding = bindings.len() == 1;
        let mut readable_namespaces = HashSet::new();
        let mut writable_namespaces = HashSet::new();
        let mut default_output_namespace = None;
        let mut default_passthrough_namespace = None;
        let mut input_index = 0usize;

        for binding in bindings {
            binding
                .sensitivity
                .validate_against_schema(&binding.schema, "input")?;
            if binding.readable && !readable_namespaces.insert(binding.namespace.clone()) {
                return Err(CompileError {
                    code: "duplicate_namespace",
                    message: format!(
                        "input namespace '{}' is bound more than once",
                        binding.namespace.label()
                    ),
                    span: (0..0).into(),
                });
            }
            if binding.readable
                && default_passthrough_namespace.is_none()
                && let CompileNamespace::User(namespace) = &binding.namespace
            {
                default_passthrough_namespace = Some(namespace.clone());
            }
            if binding.writable {
                let CompileNamespace::User(namespace) = &binding.namespace else {
                    return Err(CompileError {
                        code: "internal_namespace_not_writable",
                        message: format!(
                            "internal namespace '{}' cannot be writable",
                            binding.namespace.label()
                        ),
                        span: (0..0).into(),
                    });
                };
                if default_output_namespace.is_none() {
                    default_output_namespace = Some(namespace.clone());
                }
                writable_namespaces.insert(namespace.clone());
            }
            if !binding.readable {
                continue;
            }
            for field in binding.schema.fields() {
                let reg = RegisterType::from_data_type(field.data_type()).map(|ty| {
                    let reg = layouts.alloc(RegisterSpace::Input, ty);
                    inputs.push(InputBinding {
                        column_index: input_index,
                        reg,
                    });
                    reg
                });
                input_fields.push(Field::new(
                    if single_binding {
                        field.name().clone()
                    } else {
                        binding.namespace.qualified_field_name(field.name())
                    },
                    field.data_type().clone(),
                    field.is_nullable(),
                ));
                columns.insert(
                    match &binding.namespace {
                        CompileNamespace::User(namespace) => BoundFieldRef::User(FieldRef {
                            relay: namespace.clone(),
                            field: field.name().clone(),
                        }),
                        CompileNamespace::Internal(namespace) => {
                            BoundFieldRef::Internal(InternalFieldRef {
                                namespace: *namespace,
                                field: field.name().clone(),
                            })
                        }
                    },
                    ColumnBinding {
                        data_type: field.data_type().clone(),
                        nullable: field.is_nullable(),
                        sensitive: binding.sensitivity.is_sensitive(field.name()),
                        value: reg.map_or(ColumnValue::Unsupported, ColumnValue::Initialized),
                    },
                );
                input_index += 1;
            }
        }

        let default_output_namespace = default_output_namespace.ok_or_else(|| CompileError {
            code: "missing_output_namespace",
            message: "at least one writable input namespace is required".to_string(),
            span: (0..0).into(),
        })?;
        let default_passthrough_namespace = default_passthrough_namespace
            .clone()
            .unwrap_or_else(|| default_output_namespace.clone());

        Ok((
            Self {
                readable_namespaces,
                writable_namespaces,
                default_passthrough_namespace,
                columns,
                inputs,
                instructions: Vec::new(),
                layouts,
                expr_cache: HashMap::new(),
                expr_cache_generation: 0,
                current_error_mask: None,
                allow_header_reads: false,
                allow_header_writes: false,
                udf_signatures: UdfSignatures::default(),
                injector: None,
            },
            Arc::new(Schema::new(input_fields)),
        ))
    }

    fn alloc_temp(&mut self, ty: RegisterType) -> RegisterRef {
        self.layouts.alloc(RegisterSpace::Temp, ty)
    }

    fn alloc_condition(&mut self, ty: RegisterType) -> RegisterRef {
        self.layouts.alloc(RegisterSpace::Condition, ty)
    }

    fn alloc_output(&mut self, ty: RegisterType) -> RegisterRef {
        self.layouts.alloc(RegisterSpace::Output, ty)
    }

    fn emit(&mut self, kind: InstructionKind, span: Span) {
        let error_mask = if instruction_can_emit_row_errors(&kind) {
            self.current_error_mask
        } else {
            None
        };
        self.instructions.push(Instruction {
            kind,
            span,
            error_mask,
        });
    }

    fn with_error_mask<T>(
        &mut self,
        mask: Option<RegisterRef>,
        compile: impl FnOnce(&mut Self) -> Result<T, CompileError>,
    ) -> Result<T, CompileError> {
        let previous = self.current_error_mask;
        self.current_error_mask = mask;
        let result = compile(self);
        self.current_error_mask = previous;
        result
    }

    fn emit_boolean_literal(&mut self, value: bool, span: Span) -> RegisterRef {
        let dst = self.alloc_temp(RegisterType::Boolean);
        self.emit(
            InstructionKind::Literal {
                dst,
                value: ScalarValue::Boolean(value),
            },
            span,
        );
        dst
    }

    fn emit_boolean_binary(
        &mut self,
        op: BinaryOp,
        left: RegisterRef,
        right: RegisterRef,
        span: Span,
    ) -> RegisterRef {
        let dst = self.alloc_temp(RegisterType::Boolean);
        self.emit(
            InstructionKind::Binary {
                dst,
                left,
                right,
                op,
            },
            span,
        );
        dst
    }

    fn emit_boolean_not(&mut self, input: RegisterRef, span: Span) -> RegisterRef {
        let dst = self.alloc_temp(RegisterType::Boolean);
        self.emit(
            InstructionKind::Unary {
                dst,
                input,
                op: UnaryOp::Not,
            },
            span,
        );
        dst
    }

    fn normalize_condition(&mut self, input: RegisterRef, span: Span) -> RegisterRef {
        let false_value = self.emit_boolean_literal(false, span);
        let dst = self.alloc_temp(RegisterType::Boolean);
        self.emit(
            InstructionKind::Builtin {
                dst,
                lowering: BuiltinLowering::Coalesce,
                inputs: vec![input, false_value],
            },
            span,
        );
        dst
    }

    fn combine_with_outer_mask(
        &mut self,
        outer: Option<RegisterRef>,
        mask: RegisterRef,
        span: Span,
    ) -> RegisterRef {
        outer.map_or(mask, |outer| {
            self.emit_boolean_binary(BinaryOp::And, outer, mask, span)
        })
    }

    fn apply_options(&mut self, options: &CompileOptions) {
        self.allow_header_reads = options.allow_header_reads;
        self.allow_header_writes = options.allow_header_writes;
        self.udf_signatures.clone_from(&options.udf_signatures);
        self.injector.clone_from(&options.injector);
    }

    fn clear_expr_cache(&mut self) {
        self.expr_cache.clear();
        self.expr_cache_generation += 1;
    }

    fn expected_readable_namespaces(&self) -> String {
        let mut names = self
            .readable_namespaces
            .iter()
            .map(CompileNamespace::label)
            .collect::<Vec<_>>();
        names.sort();
        names.join(", ")
    }

    fn expected_writable_namespaces(&self) -> String {
        let mut names = self.writable_namespaces.iter().cloned().collect::<Vec<_>>();
        names.sort();
        names.join(", ")
    }

    fn enter_set_scope(&mut self, output_schema: &Schema, output_sensitivity: &SchemaSensitivity) {
        for namespace in &self.writable_namespaces {
            self.readable_namespaces
                .insert(CompileNamespace::User(namespace.clone()));
            for field in output_schema.fields() {
                self.columns
                    .entry(BoundFieldRef::User(FieldRef {
                        relay: namespace.clone(),
                        field: field.name().clone(),
                    }))
                    .or_insert_with(|| ColumnBinding {
                        data_type: field.data_type().clone(),
                        nullable: true,
                        sensitive: output_sensitivity.is_sensitive(field.name()),
                        value: ColumnValue::Uninitialized,
                    });
            }
        }
    }

    fn update_writable_field(&mut self, field_name: &str, binding: ColumnBinding) {
        for namespace in self.writable_namespaces.iter().cloned() {
            self.columns.insert(
                BoundFieldRef::User(FieldRef {
                    relay: namespace,
                    field: field_name.to_string(),
                }),
                binding.clone(),
            );
        }
        self.clear_expr_cache();
    }

    fn materialize_uninitialized_field(
        &mut self,
        field_ref: BoundFieldRef,
        binding: ColumnBinding,
        span: Span,
    ) -> Result<RegisterRef, CompileError> {
        let ty =
            Self::register_type_for_data_type(&binding.data_type, span, "uninitialized column")?;
        let dst = self.alloc_temp(ty);
        self.emit(
            InstructionKind::Uninitialized {
                dst,
                data_type: binding.data_type.clone(),
            },
            span,
        );
        let initialized = ColumnBinding {
            nullable: true,
            value: ColumnValue::Initialized(dst),
            ..binding
        };
        if let BoundFieldRef::User(field) = &field_ref
            && self.writable_namespaces.contains(&field.relay)
        {
            self.update_writable_field(&field.field, initialized);
        } else {
            self.columns.insert(field_ref, initialized);
            self.clear_expr_cache();
        }
        Ok(dst)
    }

    fn validate_field_ref<'a>(
        &'a self,
        field_ref: &'a FieldRef,
        span: Span,
    ) -> Result<&'a ColumnBinding, CompileError> {
        let namespace = CompileNamespace::User(field_ref.relay.clone());
        if !self.readable_namespaces.contains(&namespace) {
            return Err(CompileError {
                code: "wrong_stream",
                message: format!(
                    "field reference '{}.{}' targets namespace '{}', expected one of [{}]",
                    field_ref.relay,
                    field_ref.field,
                    field_ref.relay,
                    self.expected_readable_namespaces()
                ),
                span,
            });
        }

        self.columns
            .get(&BoundFieldRef::User(field_ref.clone()))
            .ok_or_else(|| CompileError {
                code: "unknown_identifier",
                message: format!(
                    "unknown input column '{}.{}'",
                    field_ref.relay, field_ref.field
                ),
                span,
            })
    }

    fn validate_internal_field_ref<'a>(
        &'a self,
        field_ref: &'a InternalFieldRef,
        span: Span,
    ) -> Result<&'a ColumnBinding, CompileError> {
        let namespace = CompileNamespace::Internal(field_ref.namespace);
        if !self.readable_namespaces.contains(&namespace) {
            return Err(CompileError {
                code: "wrong_stream",
                message: format!(
                    "field reference '{}.{}' targets namespace '{}', expected one of [{}]",
                    namespace.label(),
                    field_ref.field,
                    namespace.label(),
                    self.expected_readable_namespaces()
                ),
                span,
            });
        }

        self.columns
            .get(&BoundFieldRef::Internal(field_ref.clone()))
            .ok_or_else(|| CompileError {
                code: "unknown_identifier",
                message: format!(
                    "unknown input column '{}.{}'",
                    namespace.label(),
                    field_ref.field
                ),
                span,
            })
    }

    fn validate_target_field_ref<'a>(
        &'a self,
        field_ref: &'a FieldRef,
        span: Span,
    ) -> Result<&'a str, CompileError> {
        if !self.writable_namespaces.contains(&field_ref.relay) {
            return Err(CompileError {
                code: "wrong_stream",
                message: format!(
                    "field reference '{}.{}' targets namespace '{}', expected one of [{}]",
                    field_ref.relay,
                    field_ref.field,
                    field_ref.relay,
                    self.expected_writable_namespaces()
                ),
                span,
            });
        }
        Ok(field_ref.field.as_str())
    }

    fn passthrough_binding_for_output_field(
        &self,
        field_name: &str,
        span: Span,
    ) -> Result<&ColumnBinding, CompileError> {
        self.columns
            .get(&BoundFieldRef::User(FieldRef {
                relay: self.default_passthrough_namespace.clone(),
                field: field_name.to_string(),
            }))
            .ok_or_else(|| CompileError {
                code: "missing_set",
                message: format!(
                    "declared output field '{field_name}' is not present in source namespace '{}' \
                     and must be assigned with SET",
                    self.default_passthrough_namespace
                ),
                span,
            })
    }

    fn register_type_for_data_type(
        data_type: &DataType,
        span: Span,
        role: &str,
    ) -> Result<RegisterType, CompileError> {
        RegisterType::from_data_type(data_type).ok_or_else(|| CompileError {
            code: "unsupported_type",
            message: format!("{role} type {data_type:?} is not supported by FILTER-MAP execution"),
            span,
        })
    }

    fn infer_expr_type(&self, expr: &SpannedExpr) -> Result<DataType, CompileError> {
        match &expr.inner {
            Expr::Literal(literal) => Ok(match literal {
                Literal::Int64(_) => DataType::Int64,
                Literal::Float64(_) => DataType::Float64,
                Literal::Bool(_) => DataType::Boolean,
                Literal::String(_) => DataType::Utf8,
                Literal::Null => {
                    return Err(CompileError {
                        code: "untyped_null",
                        message: "NULL requires a declared optional assignment target".to_string(),
                        span: expr.span,
                    });
                }
            }),
            Expr::FieldRef(field_ref) => {
                let binding = self.validate_field_ref(field_ref, expr.span)?;
                Ok(binding.data_type.clone())
            }
            Expr::InternalFieldRef(field_ref) => {
                let binding = self.validate_internal_field_ref(field_ref, expr.span)?;
                Ok(binding.data_type.clone())
            }
            Expr::Unary { op, expr: inner } => {
                let input_type = self.infer_expr_type(inner)?;
                unary_descriptor(*op)
                    .output_type(&input_type)
                    .ok_or_else(|| CompileError {
                        code: "unsupported_unary",
                        message: format!("operator {op:?} is not valid for {:?}", input_type),
                        span: expr.span,
                    })
            }
            Expr::Binary { op, left, right } => {
                let left_type = self.infer_expr_type(left)?;
                let right_type = self.infer_expr_type(right)?;

                binary_output_type(*op, &left_type, &right_type).ok_or_else(|| {
                    let (code, message) = if left_type != right_type {
                        (
                            "type_mismatch",
                            format!(
                                "binary operator {op:?} requires matching operand types, found \
                                 {:?} and {:?}",
                                left_type, right_type
                            ),
                        )
                    } else {
                        (
                            "unsupported_binary",
                            format!("operator {op:?} is not valid for {:?}", left_type),
                        )
                    };
                    CompileError {
                        code,
                        message,
                        span: expr.span,
                    }
                })
            }
            Expr::Cast {
                expr: inner,
                data_type,
            } => {
                let input_type = self.infer_expr_type(inner)?;
                cast_descriptor().validate(&input_type, data_type, expr.span)?;
                Ok(data_type.clone())
            }
            Expr::Call { function, args } => {
                if let FunctionName::LeakSensitive = function {
                    let arg = self.leak_sensitive_arg(args, expr.span)?;
                    return self.infer_expr_type(arg);
                }
                if let FunctionName::WindowAggregate(invocation) = function {
                    return self.injected_window_aggregate_call_type(
                        invocation.function,
                        args,
                        expr.span,
                    );
                }
                if let FunctionName::ReadHeader | FunctionName::ReadHeaders = function {
                    return self.injected_header_call_type(function, args, expr.span);
                }
                if let FunctionName::WriteHeader = function {
                    return Err(CompileError {
                        code: "invalid_side_effect_call",
                        message: "write_header must be a top-level call in an INVOKE clause"
                            .to_string(),
                        span: expr.span,
                    });
                }
                if let FunctionName::Udf(name) = function {
                    return self.udf_call_type(name, args, expr.span);
                }
                let arg_types = args
                    .iter()
                    .map(|arg| self.infer_expr_type(arg))
                    .collect::<Result<Vec<_>, _>>()?;
                builtin_signature(function, &arg_types, expr.span)
            }
            Expr::Case {
                operand,
                branches,
                else_result,
            } => {
                if branches.is_empty() {
                    return Err(CompileError {
                        code: "invalid_case",
                        message: "CASE requires at least one WHEN branch".to_string(),
                        span: expr.span,
                    });
                }
                let operand_type = operand
                    .as_ref()
                    .map(|operand| self.infer_expr_type(operand))
                    .transpose()?;
                let mut result_type = None;
                for branch in branches {
                    if let Some(operand_type) = &operand_type {
                        if matches!(branch.when.inner, Expr::Literal(Literal::Null)) {
                            return Err(CompileError {
                                code: "untyped_null",
                                message: "simple CASE WHEN values cannot be bare NULL".to_string(),
                                span: branch.when.span,
                            });
                        }
                        let when_type = self.infer_expr_type(&branch.when)?;
                        if &when_type != operand_type {
                            return Err(CompileError {
                                code: "type_mismatch",
                                message: format!(
                                    "simple CASE operand has type {operand_type:?}, but WHEN \
                                     value has type {when_type:?}"
                                ),
                                span: branch.when.span,
                            });
                        }
                    } else {
                        let condition_type = self.infer_expr_type(&branch.when)?;
                        if condition_type != DataType::Boolean {
                            return Err(CompileError {
                                code: "invalid_condition",
                                message: format!(
                                    "CASE WHEN condition must evaluate to Boolean, found \
                                     {condition_type:?}"
                                ),
                                span: branch.when.span,
                            });
                        }
                    }
                    if !matches!(branch.result.inner, Expr::Literal(Literal::Null)) {
                        let branch_type = self.infer_expr_type(&branch.result)?;
                        if let Some(expected) = &result_type
                            && expected != &branch_type
                        {
                            return Err(CompileError {
                                code: "type_mismatch",
                                message: format!(
                                    "CASE results must have one exact type, found {expected:?} \
                                     and {branch_type:?}"
                                ),
                                span: branch.result.span,
                            });
                        }
                        result_type.get_or_insert(branch_type);
                    }
                }
                if let Some(else_result) = else_result
                    && !matches!(else_result.inner, Expr::Literal(Literal::Null))
                {
                    let else_type = self.infer_expr_type(else_result)?;
                    if let Some(expected) = &result_type
                        && expected != &else_type
                    {
                        return Err(CompileError {
                            code: "type_mismatch",
                            message: format!(
                                "CASE results must have one exact type, found {expected:?} and \
                                 {else_type:?}"
                            ),
                            span: else_result.span,
                        });
                    }
                    result_type.get_or_insert(else_type);
                }
                let result_type = result_type.ok_or_else(|| CompileError {
                    code: "untyped_null",
                    message: "CASE with only NULL results has no inferable type".to_string(),
                    span: expr.span,
                })?;
                if RegisterType::from_data_type(&result_type) == Some(RegisterType::Generic) {
                    return Err(CompileError {
                        code: "unsupported_type",
                        message: format!(
                            "CASE result type {result_type:?} is not supported by conditional \
                             selection"
                        ),
                        span: expr.span,
                    });
                }
                Ok(result_type)
            }
        }
    }

    fn leak_sensitive_arg<'a>(
        &self,
        args: &'a [SpannedExpr],
        span: Span,
    ) -> Result<&'a SpannedExpr, CompileError> {
        let [arg] = args else {
            return Err(CompileError {
                code: "invalid_function_arity",
                message: format!(
                    "function 'leak_sensitive' expects exactly 1 argument, found {}",
                    args.len()
                ),
                span,
            });
        };
        self.infer_expr_type(arg)?;
        Ok(arg)
    }

    fn infer_assignment_expr_type(
        &self,
        expr: &SpannedExpr,
        target_type: &DataType,
    ) -> Result<DataType, CompileError> {
        if let Expr::Literal(Literal::Null) = expr.inner {
            return Ok(target_type.clone());
        }

        self.infer_expr_type(expr)
    }

    fn expr_may_be_null(&self, expr: &SpannedExpr) -> Result<bool, CompileError> {
        match &expr.inner {
            Expr::Literal(literal) => Ok(matches!(literal, Literal::Null)),
            Expr::FieldRef(field_ref) => {
                let binding = self.validate_field_ref(field_ref, expr.span)?;
                Ok(binding.nullable)
            }
            Expr::InternalFieldRef(field_ref) => {
                let binding = self.validate_internal_field_ref(field_ref, expr.span)?;
                Ok(binding.nullable)
            }
            Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => self.expr_may_be_null(expr),
            Expr::Binary { left, right, .. } => {
                Ok(self.expr_may_be_null(left)? || self.expr_may_be_null(right)?)
            }
            Expr::Call { function, args } => {
                if let FunctionName::LeakSensitive = function {
                    let arg = self.leak_sensitive_arg(args, expr.span)?;
                    return self.expr_may_be_null(arg);
                }
                if let FunctionName::IsNull
                | FunctionName::Now
                | FunctionName::UuidV4
                | FunctionName::UuidV7
                | FunctionName::ReadHeaders = function
                {
                    return Ok(false);
                }
                if let FunctionName::ReadHeader = function {
                    return Ok(true);
                }
                if let FunctionName::WindowAggregate(_) = function {
                    return Ok(false);
                }
                if let FunctionName::NullIf = function {
                    return Ok(true);
                }
                if let FunctionName::Coalesce = function {
                    let mut all_nullable = true;
                    for arg in args {
                        all_nullable &= self.expr_may_be_null(arg)?;
                    }
                    return Ok(all_nullable);
                }
                if let FunctionName::Udf(name) = function {
                    let signature = self.udf_signatures.get(name).ok_or_else(|| CompileError {
                        code: "unknown_function",
                        message: format!("unknown function '{name}' with arity {}", args.len()),
                        span: expr.span,
                    })?;
                    if signature.return_optional {
                        return Ok(true);
                    }
                    for (argument, parameter) in args.iter().zip(&signature.arguments) {
                        if !parameter.optional && self.expr_may_be_null(argument)? {
                            return Ok(true);
                        }
                    }
                    return Ok(false);
                }
                for arg in args {
                    if self.expr_may_be_null(arg)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Expr::Case {
                branches,
                else_result,
                ..
            } => {
                if else_result.is_none() {
                    return Ok(true);
                }
                for branch in branches {
                    if self.expr_may_be_null(&branch.result)? {
                        return Ok(true);
                    }
                }
                self.expr_may_be_null(
                    else_result
                        .as_ref()
                        .expect("checked CASE ELSE presence above"),
                )
            }
        }
    }

    fn expr_is_sensitive(&self, expr: &SpannedExpr) -> Result<bool, CompileError> {
        match &expr.inner {
            Expr::Literal(_) => Ok(false),
            Expr::FieldRef(field_ref) => {
                let binding = self.validate_field_ref(field_ref, expr.span)?;
                Ok(binding.sensitive)
            }
            Expr::InternalFieldRef(field_ref) => {
                let binding = self.validate_internal_field_ref(field_ref, expr.span)?;
                Ok(binding.sensitive)
            }
            Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => self.expr_is_sensitive(expr),
            Expr::Binary { left, right, .. } => {
                Ok(self.expr_is_sensitive(left)? || self.expr_is_sensitive(right)?)
            }
            Expr::Call { function, args } => {
                if let FunctionName::LeakSensitive = function {
                    self.leak_sensitive_arg(args, expr.span)?;
                    return Ok(false);
                }
                for arg in args {
                    if self.expr_is_sensitive(arg)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Expr::Case {
                operand,
                branches,
                else_result,
            } => {
                if let Some(operand) = operand
                    && self.expr_is_sensitive(operand)?
                {
                    return Ok(true);
                }
                for branch in branches {
                    if self.expr_is_sensitive(&branch.when)?
                        || self.expr_is_sensitive(&branch.result)?
                    {
                        return Ok(true);
                    }
                }
                if let Some(else_result) = else_result
                    && self.expr_is_sensitive(else_result)?
                {
                    return Ok(true);
                }
                Ok(false)
            }
        }
    }

    fn validate_assignment_expr(
        &self,
        field_name: &str,
        expr: &SpannedExpr,
        target_type: &DataType,
        target_nullable: bool,
        target_sensitive: bool,
        allow_sensitive_output: bool,
    ) -> Result<(), CompileError> {
        let expr_type = self.infer_assignment_expr_type(expr, target_type)?;
        if &expr_type != target_type {
            return Err(CompileError {
                code: "type_mismatch",
                message: format!(
                    "SET field '{field_name}' has expression type {expr_type:?}, expected \
                     declared output type {target_type:?}"
                ),
                span: expr.span,
            });
        }
        if !target_nullable && self.expr_may_be_null(expr)? {
            return Err(CompileError {
                code: "null_for_required_field",
                message: format!(
                    "SET field '{field_name}' may be null but the output field is required"
                ),
                span: expr.span,
            });
        }
        if !target_sensitive && !allow_sensitive_output && self.expr_is_sensitive(expr)? {
            return Err(CompileError {
                code: "sensitive_leak",
                message: format!(
                    "SET field '{field_name}' would store sensitive data in a non-sensitive \
                     output field; use leak_sensitive(...) to explicitly remove sensitivity"
                ),
                span: expr.span,
            });
        }
        Ok(())
    }

    fn compile_expr(&mut self, expr: &SpannedExpr) -> Result<RegisterRef, CompileError> {
        if let Expr::Call {
            function: FunctionName::Udf(name),
            args,
        } = &expr.inner
        {
            let signature = self.udf_signatures.get(name).ok_or_else(|| CompileError {
                code: "unknown_function",
                message: format!("unknown function '{name}'"),
                span: expr.span,
            })?;
            if !signature.volatile
                && !args.iter().any(|argument| {
                    expression_contains_volatile_udf(argument, &self.udf_signatures)
                })
            {
                let cache_key = CachedExpr {
                    generation: self.expr_cache_generation,
                    expr: ExprKey::from_expr(expr),
                };
                if let Some(reg) = self.expr_cache.get(&cache_key) {
                    return Ok(*reg);
                }
                let reg = self.compile_expr_uncached(expr)?;
                self.expr_cache.insert(cache_key, reg);
                return Ok(reg);
            }
            return self.compile_expr_uncached(expr);
        }
        if let Some(semantics) = expr_semantics(expr) {
            let cache_key = if semantics.supports_common_subexpression_elimination() {
                Some(CachedExpr {
                    generation: self.expr_cache_generation,
                    expr: ExprKey::from_expr(expr),
                })
            } else {
                None
            };

            if let Some(cache_key) = cache_key.as_ref()
                && let Some(reg) = self.expr_cache.get(cache_key)
            {
                return Ok(*reg);
            }

            if semantics.supports_constant_folding()
                && let Some(value) = self.try_fold_constant_expr(expr)?
                && let FoldedValue::NonNull(_) = value
            {
                let reg = self.emit_folded_value(value, expr.span);
                if let Some(cache_key) = cache_key {
                    self.expr_cache.insert(cache_key, reg);
                }
                return Ok(reg);
            }

            let reg = self.compile_expr_uncached(expr)?;
            if let Some(cache_key) = cache_key {
                self.expr_cache.insert(cache_key, reg);
            }
            return Ok(reg);
        }

        self.compile_expr_uncached(expr)
    }

    fn compile_assignment_expr(
        &mut self,
        expr: &SpannedExpr,
        target_type: &DataType,
    ) -> Result<RegisterRef, CompileError> {
        if let Expr::Literal(Literal::Null) = expr.inner {
            let ty = Self::register_type_for_data_type(target_type, expr.span, "NULL target")?;
            let dst = self.alloc_temp(ty);
            self.emit(
                InstructionKind::NullLiteral {
                    dst,
                    data_type: target_type.clone(),
                },
                expr.span,
            );
            return Ok(dst);
        }

        self.compile_expr(expr)
    }

    fn compile_expr_uncached(&mut self, expr: &SpannedExpr) -> Result<RegisterRef, CompileError> {
        match &expr.inner {
            Expr::Literal(literal) => {
                let (ty, value) = match literal {
                    Literal::Int64(value) => (RegisterType::Int64, ScalarValue::Int64(*value)),
                    Literal::Float64(value) => {
                        (RegisterType::Float64, ScalarValue::Float64(*value))
                    }
                    Literal::Bool(value) => (RegisterType::Boolean, ScalarValue::Boolean(*value)),
                    Literal::String(value) => {
                        (RegisterType::Utf8, ScalarValue::Utf8(value.clone()))
                    }
                    Literal::Null => {
                        return Err(CompileError {
                            code: "untyped_null",
                            message: "NULL requires a declared optional assignment target"
                                .to_string(),
                            span: expr.span,
                        });
                    }
                };
                let dst = self.alloc_temp(ty);
                self.emit(InstructionKind::Literal { dst, value }, expr.span);
                Ok(dst)
            }
            Expr::FieldRef(field_ref) => {
                let binding = self.validate_field_ref(field_ref, expr.span)?.clone();
                match binding.value {
                    ColumnValue::Initialized(reg) => Ok(reg),
                    ColumnValue::Uninitialized => self.materialize_uninitialized_field(
                        BoundFieldRef::User(field_ref.clone()),
                        binding,
                        expr.span,
                    ),
                    ColumnValue::Unsupported => Err(CompileError {
                        code: "unsupported_identifier",
                        message: format!(
                            "input column '{}.{}' has unsupported type {:?}",
                            field_ref.relay, field_ref.field, binding.data_type
                        ),
                        span: expr.span,
                    }),
                }
            }
            Expr::InternalFieldRef(field_ref) => {
                let binding = self
                    .validate_internal_field_ref(field_ref, expr.span)?
                    .clone();
                match binding.value {
                    ColumnValue::Initialized(reg) => Ok(reg),
                    ColumnValue::Uninitialized => self.materialize_uninitialized_field(
                        BoundFieldRef::Internal(field_ref.clone()),
                        binding,
                        expr.span,
                    ),
                    ColumnValue::Unsupported => Err(CompileError {
                        code: "unsupported_identifier",
                        message: format!(
                            "input column '{}.{}' has unsupported type {:?}",
                            CompileNamespace::Internal(field_ref.namespace).label(),
                            field_ref.field,
                            binding.data_type
                        ),
                        span: expr.span,
                    }),
                }
            }
            Expr::Unary { op, expr: inner } => {
                let input = self.compile_expr(inner)?;
                let dst = match op {
                    UnaryOp::Neg => self.alloc_temp(input.ty),
                    UnaryOp::Not => self.alloc_temp(RegisterType::Boolean),
                };
                self.emit(
                    InstructionKind::Unary {
                        dst,
                        input,
                        op: *op,
                    },
                    expr.span,
                );
                Ok(dst)
            }
            Expr::Binary { op, left, right } => {
                let left_reg = self.compile_expr(left)?;
                let right_reg = self.compile_expr(right)?;
                let output_type = RegisterType::from_data_type(&self.infer_expr_type(expr)?)
                    .expect("validated expression type must be supported");
                let dst = self.alloc_temp(output_type);
                self.emit(
                    InstructionKind::Binary {
                        dst,
                        left: left_reg,
                        right: right_reg,
                        op: *op,
                    },
                    expr.span,
                );
                Ok(dst)
            }
            Expr::Cast {
                expr: inner,
                data_type,
            } => {
                let input = self.compile_expr(inner)?;
                let target = RegisterType::from_data_type(data_type)
                    .expect("validated cast target must be supported");
                let dst = self.alloc_temp(target);
                self.emit(InstructionKind::Cast { dst, input, target }, expr.span);
                Ok(dst)
            }
            Expr::Call { function, args } => {
                if let FunctionName::LeakSensitive = function {
                    let arg = self.leak_sensitive_arg(args, expr.span)?;
                    return self.compile_expr(arg);
                }
                if let FunctionName::ReadHeader | FunctionName::ReadHeaders = function {
                    let output_type = self.injected_header_call_type(function, args, expr.span)?;
                    let inputs = args
                        .iter()
                        .map(|arg| self.compile_expr(arg))
                        .collect::<Result<Vec<_>, _>>()?;
                    let dst = self.alloc_temp(
                        RegisterType::from_data_type(&output_type)
                            .expect("validated injected output type must be supported"),
                    );
                    self.emit(
                        InstructionKind::Inject {
                            dst,
                            function: function.clone(),
                            inputs,
                            output_type,
                        },
                        expr.span,
                    );
                    return Ok(dst);
                }
                if let FunctionName::WindowAggregate(invocation) = function {
                    let output_type = self.injected_window_aggregate_call_type(
                        invocation.function,
                        args,
                        expr.span,
                    )?;
                    let dst = self.alloc_temp(
                        RegisterType::from_data_type(&output_type)
                            .expect("validated aggregate output type must be supported"),
                    );
                    self.emit(
                        InstructionKind::Inject {
                            dst,
                            function: function.clone(),
                            inputs: Vec::new(),
                            output_type,
                        },
                        expr.span,
                    );
                    return Ok(dst);
                }
                if let FunctionName::Udf(name) = function {
                    let output_type = self.udf_call_type(name, args, expr.span)?;
                    let inputs = args
                        .iter()
                        .map(|argument| self.compile_expr(argument))
                        .collect::<Result<Vec<_>, _>>()?;
                    let dst = self.alloc_temp(
                        RegisterType::from_data_type(&output_type)
                            .expect("validated UDF output type must be supported"),
                    );
                    self.emit(
                        InstructionKind::Inject {
                            dst,
                            function: function.clone(),
                            inputs,
                            output_type,
                        },
                        expr.span,
                    );
                    return Ok(dst);
                }
                let builtin = self.compile_builtin_call(function, args, expr.span)?;
                let dst = self.alloc_temp(builtin.output_type());
                self.emit(builtin.into_instruction(dst), expr.span);
                Ok(dst)
            }
            Expr::Case {
                operand,
                branches,
                else_result,
            } => {
                let result_type = self.infer_expr_type(expr)?;
                self.compile_case(
                    operand.as_deref(),
                    branches,
                    else_result.as_deref(),
                    &result_type,
                    expr.span,
                )
            }
        }
    }

    fn compile_case(
        &mut self,
        operand: Option<&SpannedExpr>,
        branches: &[CaseArm],
        else_result: Option<&SpannedExpr>,
        result_type: &DataType,
        span: Span,
    ) -> Result<RegisterRef, CompileError> {
        let outer_mask = self.current_error_mask;
        let folded_operand = operand.map(fold_constant_expr).transpose()?.flatten();
        let mut active_branches = Vec::with_capacity(branches.len());
        let mut effective_else = else_result;

        for branch in branches {
            let known_match = if operand.is_some() {
                if let Some(operand) = &folded_operand {
                    fold_constant_expr(&branch.when)?
                        .and_then(|when| fold_binary_expr(BinaryOp::Eq, operand.clone(), when))
                        .and_then(|value| match value {
                            FoldedValue::NonNull(ScalarValue::Boolean(value)) => Some(value),
                            FoldedValue::NonNull(_) | FoldedValue::Null(_) => None,
                        })
                } else {
                    None
                }
            } else {
                match &branch.when.inner {
                    Expr::Literal(Literal::Bool(value)) => Some(*value),
                    Expr::Literal(Literal::Null) => Some(false),
                    _ => fold_constant_expr(&branch.when)?.and_then(|value| match value {
                        FoldedValue::NonNull(ScalarValue::Boolean(value)) => Some(value),
                        FoldedValue::NonNull(_) | FoldedValue::Null(_) => None,
                    }),
                }
            };
            match known_match {
                Some(false) => {}
                Some(true) => {
                    effective_else = Some(&branch.result);
                    break;
                }
                None => active_branches.push(branch),
            }
        }

        if active_branches.is_empty() {
            return self.compile_case_result(effective_else, result_type, outer_mask, span);
        }

        let operand_reg = operand
            .map(|operand| self.compile_expr(operand))
            .transpose()?;
        let mut matched = None;
        let mut select_arms = Vec::with_capacity(active_branches.len());

        for branch in active_branches {
            let unmatched = matched.map(|matched| self.emit_boolean_not(matched, span));
            let eligible = unmatched
                .map(|unmatched| self.combine_with_outer_mask(outer_mask, unmatched, span));
            let condition_mask = eligible.or(outer_mask);
            let condition = self.with_error_mask(condition_mask, |compiler| {
                let when = compiler.compile_expr(&branch.when)?;
                Ok(operand_reg.map_or(when, |operand| {
                    compiler.emit_boolean_binary(BinaryOp::Eq, operand, when, branch.when.span)
                }))
            })?;
            let normalized = self.normalize_condition(condition, branch.when.span);
            let selected = unmatched.map_or(normalized, |unmatched| {
                self.emit_boolean_binary(BinaryOp::And, unmatched, normalized, span)
            });
            let selected = self.combine_with_outer_mask(outer_mask, selected, span);
            let value = self.compile_case_result(
                Some(&branch.result),
                result_type,
                Some(selected),
                branch.result.span,
            )?;
            select_arms.push(SelectArm {
                mask: normalized,
                value,
            });
            matched = Some(matched.map_or(normalized, |matched| {
                self.emit_boolean_binary(BinaryOp::Or, matched, normalized, span)
            }));
        }

        let else_mask = matched.map(|matched| {
            let unmatched = self.emit_boolean_not(matched, span);
            self.combine_with_outer_mask(outer_mask, unmatched, span)
        });
        let else_mask = else_mask.or(outer_mask);
        let otherwise = self.compile_case_result(effective_else, result_type, else_mask, span)?;
        let output_type = Self::register_type_for_data_type(result_type, span, "CASE result")?;
        let dst = self.alloc_temp(output_type);
        self.emit(
            InstructionKind::Select {
                dst,
                arms: select_arms,
                otherwise,
            },
            span,
        );
        Ok(dst)
    }

    fn compile_case_result(
        &mut self,
        result: Option<&SpannedExpr>,
        result_type: &DataType,
        error_mask: Option<RegisterRef>,
        span: Span,
    ) -> Result<RegisterRef, CompileError> {
        self.with_error_mask(error_mask, |compiler| {
            if result.is_none_or(|result| matches!(result.inner, Expr::Literal(Literal::Null))) {
                let ty = Self::register_type_for_data_type(result_type, span, "CASE NULL result")?;
                let dst = compiler.alloc_temp(ty);
                compiler.emit(
                    InstructionKind::NullLiteral {
                        dst,
                        data_type: result_type.clone(),
                    },
                    span,
                );
                Ok(dst)
            } else {
                compiler.compile_expr(result.expect("checked CASE result presence above"))
            }
        })
    }

    fn emit_folded_value(&mut self, value: FoldedValue, span: Span) -> RegisterRef {
        match value {
            FoldedValue::NonNull(value) => {
                let ty = value.register_type();
                let dst = self.alloc_temp(ty);
                self.emit(InstructionKind::Literal { dst, value }, span);
                dst
            }
            FoldedValue::Null(_) => unreachable!("null-valued constant folding is not emitted yet"),
        }
    }

    fn try_fold_constant_expr(
        &self,
        expr: &SpannedExpr,
    ) -> Result<Option<FoldedValue>, CompileError> {
        fold_constant_expr(expr)
    }

    fn compile_builtin_call(
        &mut self,
        function: &FunctionName,
        args: &[SpannedExpr],
        span: Span,
    ) -> Result<BuiltinPlan, CompileError> {
        let Some(descriptor) = builtin_descriptor(function) else {
            return Err(CompileError {
                code: "unknown_function",
                message: format!(
                    "unknown function '{}' with arity {}",
                    function.as_str(),
                    args.len()
                ),
                span,
            });
        };
        let arg_types = args
            .iter()
            .map(|arg| self.infer_expr_type(arg))
            .collect::<Result<Vec<_>, _>>()?;
        let output_type = builtin_signature(function, &arg_types, span)?;
        let output_type = RegisterType::from_data_type(&output_type)
            .expect("validated builtin output type must be supported");
        let compiled_args = args
            .iter()
            .map(|arg| self.compile_expr(arg))
            .collect::<Result<Vec<_>, _>>()?;

        BuiltinPlan::from_descriptor(descriptor.lowering, compiled_args, output_type, span)
    }

    fn compile_invocation(
        &mut self,
        invocation: &nervix_nspl::vm_program::SpannedInvocation,
    ) -> Result<InvocationBinding, CompileError> {
        if !self.allow_header_writes {
            return Err(CompileError {
                code: "unsupported_invoke_context",
                message: "INVOKE is only available to emitters".to_string(),
                span: invocation.span,
            });
        }
        if invocation.inner.function != FunctionName::WriteHeader {
            return Err(CompileError {
                code: "unsupported_invocation",
                message: format!(
                    "function '{}' cannot be used in INVOKE; expected write_header",
                    invocation.inner.function.as_str()
                ),
                span: invocation.span,
            });
        }
        let [name, value] = invocation.inner.args.as_slice() else {
            return Err(CompileError {
                code: "invalid_function_arity",
                message: format!(
                    "function 'write_header' expects exactly 2 arguments, found {}",
                    invocation.inner.args.len()
                ),
                span: invocation.span,
            });
        };
        let mut inputs = Vec::with_capacity(2);
        for (label, arg) in [("name", name), ("value", value)] {
            let data_type = self.infer_expr_type(arg)?;
            if data_type != DataType::Utf8 {
                return Err(CompileError {
                    code: "type_mismatch",
                    message: format!("write_header {label} must be STRING, found {data_type:?}"),
                    span: arg.span,
                });
            }
            if self.expr_may_be_null(arg)? {
                return Err(CompileError {
                    code: "nullable_header_argument",
                    message: format!("write_header {label} must not be nullable"),
                    span: arg.span,
                });
            }
            let compiled = self.compile_expr(arg)?;
            let input = self.alloc_condition(RegisterType::Utf8);
            self.emit_move(input, compiled, arg.span);
            inputs.push(input);
        }
        Ok(InvocationBinding {
            function: invocation.inner.function.clone(),
            inputs,
            span: invocation.span,
        })
    }

    fn emit_move(&mut self, dst: RegisterRef, input: RegisterRef, span: Span) {
        if dst != input {
            self.emit(InstructionKind::Move { dst, input }, span);
        }
    }

    fn emit_assignment(
        &mut self,
        dst: RegisterRef,
        input: RegisterRef,
        fallback: AssignmentFallback,
        span: Span,
    ) {
        self.emit(
            InstructionKind::Assign {
                dst,
                input,
                fallback,
            },
            span,
        );
    }
}

impl ExprKey {
    fn from_expr(expr: &SpannedExpr) -> Self {
        match &expr.inner {
            Expr::Literal(literal) => Self::Literal(LiteralKey::from_literal(literal)),
            Expr::FieldRef(field_ref) => Self::FieldRef(field_ref.clone()),
            Expr::InternalFieldRef(field_ref) => Self::InternalFieldRef(field_ref.clone()),
            Expr::Unary { op, expr } => Self::Unary {
                op: *op,
                expr: Box::new(Self::from_expr(expr.as_ref())),
            },
            Expr::Binary { op, left, right } => Self::Binary {
                op: *op,
                left: Box::new(Self::from_expr(left.as_ref())),
                right: Box::new(Self::from_expr(right.as_ref())),
            },
            Expr::Cast { expr, data_type } => Self::Cast {
                expr: Box::new(Self::from_expr(expr.as_ref())),
                data_type: data_type.clone(),
            },
            Expr::Call { function, args } => Self::Call {
                function: function.clone(),
                args: args.iter().map(Self::from_expr).collect(),
            },
            Expr::Case {
                operand,
                branches,
                else_result,
            } => Self::Case {
                operand: operand
                    .as_ref()
                    .map(|operand| Box::new(Self::from_expr(operand))),
                branches: branches
                    .iter()
                    .map(|branch| {
                        (
                            Self::from_expr(&branch.when),
                            Self::from_expr(&branch.result),
                        )
                    })
                    .collect(),
                else_result: else_result
                    .as_ref()
                    .map(|result| Box::new(Self::from_expr(result))),
            },
        }
    }
}

impl LiteralKey {
    fn from_literal(literal: &Literal) -> Self {
        match literal {
            Literal::Int64(value) => Self::Int64(*value),
            Literal::Float64(value) => Self::Float64(value.to_bits()),
            Literal::Bool(value) => Self::Boolean(*value),
            Literal::String(value) => Self::Utf8(value.clone()),
            Literal::Null => Self::Null,
        }
    }
}

impl ScalarValue {
    fn register_type(&self) -> RegisterType {
        match self {
            ScalarValue::Int64(_) => RegisterType::Int64,
            ScalarValue::Float64(_) => RegisterType::Float64,
            ScalarValue::Boolean(_) => RegisterType::Boolean,
            ScalarValue::Utf8(_) => RegisterType::Utf8,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct BuiltinPlan {
    lowering: BuiltinLowering,
    inputs: Vec<RegisterRef>,
    output_type: RegisterType,
}

impl BuiltinPlan {
    fn from_descriptor(
        lowering: BuiltinLowering,
        args: Vec<RegisterRef>,
        output_type: RegisterType,
        _span: Span,
    ) -> Result<Self, CompileError> {
        Ok(Self {
            lowering,
            inputs: args,
            output_type,
        })
    }

    fn output_type(&self) -> RegisterType {
        self.output_type
    }

    fn into_instruction(self, dst: RegisterRef) -> InstructionKind {
        InstructionKind::Builtin {
            dst,
            lowering: self.lowering,
            inputs: self.inputs,
        }
    }
}

fn fold_constant_expr(expr: &SpannedExpr) -> Result<Option<FoldedValue>, CompileError> {
    match &expr.inner {
        Expr::Literal(literal) => Ok(match literal {
            Literal::Int64(value) => Some(FoldedValue::NonNull(ScalarValue::Int64(*value))),
            Literal::Float64(value) => Some(FoldedValue::NonNull(ScalarValue::Float64(*value))),
            Literal::Bool(value) => Some(FoldedValue::NonNull(ScalarValue::Boolean(*value))),
            Literal::String(value) => Some(FoldedValue::NonNull(ScalarValue::Utf8(value.clone()))),
            Literal::Null => None,
        }),
        Expr::FieldRef(_) | Expr::InternalFieldRef(_) => Ok(None),
        Expr::Unary { op, expr: inner } => {
            let Some(value) = fold_constant_expr(inner.as_ref())? else {
                return Ok(None);
            };

            Ok(match (op, value) {
                (UnaryOp::Not, FoldedValue::NonNull(ScalarValue::Boolean(value))) => {
                    Some(FoldedValue::NonNull(ScalarValue::Boolean(!value)))
                }
                _ => None,
            })
        }
        Expr::Binary { op, left, right } => {
            let Some(left) = fold_constant_expr(left.as_ref())? else {
                return Ok(None);
            };
            let Some(right) = fold_constant_expr(right.as_ref())? else {
                return Ok(None);
            };
            Ok(fold_binary_expr(*op, left, right))
        }
        Expr::Cast { .. } => Ok(None),
        Expr::Call { function, args } => {
            let mut folded_args = Vec::with_capacity(args.len());
            for arg in args {
                let Some(value) = fold_constant_expr(arg)? else {
                    return Ok(None);
                };
                folded_args.push(value);
            }
            Ok(fold_builtin_call(function, &folded_args))
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            let has_operand = operand.is_some();
            let operand = operand
                .as_ref()
                .map(|operand| fold_constant_expr(operand))
                .transpose()?
                .flatten();
            if has_operand && operand.is_none() {
                return Ok(None);
            }
            for branch in branches {
                let matches = if let Some(operand) = &operand {
                    let Some(when) = fold_constant_expr(&branch.when)? else {
                        return Ok(None);
                    };
                    matches!(
                        fold_binary_expr(BinaryOp::Eq, operand.clone(), when),
                        Some(FoldedValue::NonNull(ScalarValue::Boolean(true)))
                    )
                } else {
                    let Some(condition) = fold_constant_expr(&branch.when)? else {
                        return Ok(None);
                    };
                    matches!(condition, FoldedValue::NonNull(ScalarValue::Boolean(true)))
                };
                if matches {
                    return fold_constant_expr(&branch.result);
                }
            }
            else_result
                .as_ref()
                .map(|result| fold_constant_expr(result))
                .transpose()
                .map(Option::flatten)
        }
    }
}

fn fold_binary_expr(op: BinaryOp, left: FoldedValue, right: FoldedValue) -> Option<FoldedValue> {
    match (op, left, right) {
        (
            BinaryOp::Eq,
            FoldedValue::NonNull(ScalarValue::Int64(left)),
            FoldedValue::NonNull(ScalarValue::Int64(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left == right))),
        (
            BinaryOp::Eq,
            FoldedValue::NonNull(ScalarValue::Float64(left)),
            FoldedValue::NonNull(ScalarValue::Float64(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left == right))),
        (
            BinaryOp::Eq,
            FoldedValue::NonNull(ScalarValue::Boolean(left)),
            FoldedValue::NonNull(ScalarValue::Boolean(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left == right))),
        (
            BinaryOp::Eq,
            FoldedValue::NonNull(ScalarValue::Utf8(left)),
            FoldedValue::NonNull(ScalarValue::Utf8(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left == right))),
        (
            BinaryOp::NotEq,
            FoldedValue::NonNull(ScalarValue::Int64(left)),
            FoldedValue::NonNull(ScalarValue::Int64(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left != right))),
        (
            BinaryOp::NotEq,
            FoldedValue::NonNull(ScalarValue::Float64(left)),
            FoldedValue::NonNull(ScalarValue::Float64(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left != right))),
        (
            BinaryOp::NotEq,
            FoldedValue::NonNull(ScalarValue::Boolean(left)),
            FoldedValue::NonNull(ScalarValue::Boolean(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left != right))),
        (
            BinaryOp::NotEq,
            FoldedValue::NonNull(ScalarValue::Utf8(left)),
            FoldedValue::NonNull(ScalarValue::Utf8(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left != right))),
        (
            BinaryOp::Gt,
            FoldedValue::NonNull(ScalarValue::Int64(left)),
            FoldedValue::NonNull(ScalarValue::Int64(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left > right))),
        (
            BinaryOp::Gt,
            FoldedValue::NonNull(ScalarValue::Float64(left)),
            FoldedValue::NonNull(ScalarValue::Float64(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left > right))),
        (
            BinaryOp::Gt,
            FoldedValue::NonNull(ScalarValue::Utf8(left)),
            FoldedValue::NonNull(ScalarValue::Utf8(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left > right))),
        (
            BinaryOp::Lt,
            FoldedValue::NonNull(ScalarValue::Int64(left)),
            FoldedValue::NonNull(ScalarValue::Int64(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left < right))),
        (
            BinaryOp::Lt,
            FoldedValue::NonNull(ScalarValue::Float64(left)),
            FoldedValue::NonNull(ScalarValue::Float64(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left < right))),
        (
            BinaryOp::Lt,
            FoldedValue::NonNull(ScalarValue::Utf8(left)),
            FoldedValue::NonNull(ScalarValue::Utf8(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left < right))),
        (
            BinaryOp::GtEq,
            FoldedValue::NonNull(ScalarValue::Int64(left)),
            FoldedValue::NonNull(ScalarValue::Int64(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left >= right))),
        (
            BinaryOp::GtEq,
            FoldedValue::NonNull(ScalarValue::Float64(left)),
            FoldedValue::NonNull(ScalarValue::Float64(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left >= right))),
        (
            BinaryOp::GtEq,
            FoldedValue::NonNull(ScalarValue::Utf8(left)),
            FoldedValue::NonNull(ScalarValue::Utf8(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left >= right))),
        (
            BinaryOp::LtEq,
            FoldedValue::NonNull(ScalarValue::Int64(left)),
            FoldedValue::NonNull(ScalarValue::Int64(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left <= right))),
        (
            BinaryOp::LtEq,
            FoldedValue::NonNull(ScalarValue::Float64(left)),
            FoldedValue::NonNull(ScalarValue::Float64(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left <= right))),
        (
            BinaryOp::LtEq,
            FoldedValue::NonNull(ScalarValue::Utf8(left)),
            FoldedValue::NonNull(ScalarValue::Utf8(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left <= right))),
        (
            BinaryOp::And,
            FoldedValue::NonNull(ScalarValue::Boolean(left)),
            FoldedValue::NonNull(ScalarValue::Boolean(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left && right))),
        (
            BinaryOp::Or,
            FoldedValue::NonNull(ScalarValue::Boolean(left)),
            FoldedValue::NonNull(ScalarValue::Boolean(right)),
        ) => Some(FoldedValue::NonNull(ScalarValue::Boolean(left || right))),
        _ => None,
    }
}

fn fold_builtin_call(function: &FunctionName, args: &[FoldedValue]) -> Option<FoldedValue> {
    match function {
        FunctionName::Lower => {
            let [FoldedValue::NonNull(ScalarValue::Utf8(value))] = args else {
                return None;
            };
            Some(FoldedValue::NonNull(ScalarValue::Utf8(
                value.to_lowercase(),
            )))
        }
        FunctionName::Upper => {
            let [FoldedValue::NonNull(ScalarValue::Utf8(value))] = args else {
                return None;
            };
            Some(FoldedValue::NonNull(ScalarValue::Utf8(
                value.to_uppercase(),
            )))
        }
        FunctionName::Trim => {
            let [FoldedValue::NonNull(ScalarValue::Utf8(value))] = args else {
                return None;
            };
            Some(FoldedValue::NonNull(ScalarValue::Utf8(
                value.trim().to_string(),
            )))
        }
        FunctionName::Length => {
            let [FoldedValue::NonNull(ScalarValue::Utf8(value))] = args else {
                return None;
            };
            Some(FoldedValue::NonNull(ScalarValue::Int64(
                i64::try_from(value.chars().count()).ok()?,
            )))
        }
        FunctionName::Coalesce => args.iter().find_map(|arg| match arg {
            FoldedValue::NonNull(value) => Some(FoldedValue::NonNull(value.clone())),
            FoldedValue::Null(_) => None,
        }),
        FunctionName::IsNull => {
            let [value] = args else {
                return None;
            };
            Some(FoldedValue::NonNull(ScalarValue::Boolean(matches!(
                value,
                FoldedValue::Null(_)
            ))))
        }
        FunctionName::NullIf => {
            let [left, right] = args else {
                return None;
            };
            match (left, right) {
                (
                    FoldedValue::NonNull(ScalarValue::Int64(left)),
                    FoldedValue::NonNull(ScalarValue::Int64(right)),
                ) => {
                    if left == right {
                        Some(FoldedValue::Null(RegisterType::Int64))
                    } else {
                        Some(FoldedValue::NonNull(ScalarValue::Int64(*left)))
                    }
                }
                (
                    FoldedValue::NonNull(ScalarValue::Float64(left)),
                    FoldedValue::NonNull(ScalarValue::Float64(right)),
                ) => {
                    if left == right {
                        Some(FoldedValue::Null(RegisterType::Float64))
                    } else {
                        Some(FoldedValue::NonNull(ScalarValue::Float64(*left)))
                    }
                }
                (
                    FoldedValue::NonNull(ScalarValue::Boolean(left)),
                    FoldedValue::NonNull(ScalarValue::Boolean(right)),
                ) => {
                    if left == right {
                        Some(FoldedValue::Null(RegisterType::Boolean))
                    } else {
                        Some(FoldedValue::NonNull(ScalarValue::Boolean(*left)))
                    }
                }
                (
                    FoldedValue::NonNull(ScalarValue::Utf8(left)),
                    FoldedValue::NonNull(ScalarValue::Utf8(right)),
                ) => {
                    if left == right {
                        Some(FoldedValue::Null(RegisterType::Utf8))
                    } else {
                        Some(FoldedValue::NonNull(ScalarValue::Utf8(left.clone())))
                    }
                }
                _ => None,
            }
        }
        FunctionName::LeakSensitive => {
            let [value] = args else {
                return None;
            };
            Some(value.clone())
        }
        FunctionName::Abs => None,
        FunctionName::Contains => {
            let [
                FoldedValue::NonNull(ScalarValue::Utf8(string)),
                FoldedValue::NonNull(ScalarValue::Utf8(substring)),
            ] = args
            else {
                return None;
            };
            Some(FoldedValue::NonNull(ScalarValue::Boolean(
                string.contains(substring),
            )))
        }
        FunctionName::StartsWith => {
            let [
                FoldedValue::NonNull(ScalarValue::Utf8(string)),
                FoldedValue::NonNull(ScalarValue::Utf8(prefix)),
            ] = args
            else {
                return None;
            };
            Some(FoldedValue::NonNull(ScalarValue::Boolean(
                string.starts_with(prefix),
            )))
        }
        FunctionName::EndsWith => {
            let [
                FoldedValue::NonNull(ScalarValue::Utf8(string)),
                FoldedValue::NonNull(ScalarValue::Utf8(suffix)),
            ] = args
            else {
                return None;
            };
            Some(FoldedValue::NonNull(ScalarValue::Boolean(
                string.ends_with(suffix),
            )))
        }
        FunctionName::Udf(_)
        | FunctionName::Unknown(_)
        | FunctionName::WindowAggregate(_)
        | FunctionName::LookupHashMap
        | FunctionName::ReadHeader
        | FunctionName::ReadHeaders
        | FunctionName::WriteHeader
        | FunctionName::Now
        | FunctionName::UuidV4
        | FunctionName::UuidV7
        | FunctionName::Btrim
        | FunctionName::Ltrim
        | FunctionName::Rtrim
        | FunctionName::CharLength
        | FunctionName::BitLength
        | FunctionName::Ascii
        | FunctionName::Acos
        | FunctionName::Asin
        | FunctionName::Atan
        | FunctionName::Ceil
        | FunctionName::Concat
        | FunctionName::Sum
        | FunctionName::Last
        | FunctionName::First
        | FunctionName::Count
        | FunctionName::Nth
        | FunctionName::Cos
        | FunctionName::Exp
        | FunctionName::Floor
        | FunctionName::Initcap
        | FunctionName::Left
        | FunctionName::Ln
        | FunctionName::Log
        | FunctionName::Lpad
        | FunctionName::Md5
        | FunctionName::Pow
        | FunctionName::RegexpLike
        | FunctionName::RegexpReplace
        | FunctionName::RegexpSubstr
        | FunctionName::Repeat
        | FunctionName::Replace
        | FunctionName::Reverse
        | FunctionName::Right
        | FunctionName::Round
        | FunctionName::Rpad
        | FunctionName::SplitPart
        | FunctionName::Sqrt
        | FunctionName::Strpos
        | FunctionName::Substr
        | FunctionName::Tan
        | FunctionName::ToHex
        | FunctionName::Translate => None,
    }
}

pub fn compile_program(
    program: &SpannedNode<Program>,
    schema: Arc<Schema>,
) -> Result<CompiledProgram, CompileError> {
    compile_program_for_relay(program, schema, "input")
}

pub fn compile_program_for_relay(
    program: &SpannedNode<Program>,
    schema: Arc<Schema>,
    relay_name: &str,
) -> Result<CompiledProgram, CompileError> {
    compile_program_for_bindings(
        program,
        schema.clone(),
        [CompileBinding::writable(relay_name, schema)],
    )
}

pub fn compile_program_for_relays<'a>(
    program: &SpannedNode<Program>,
    schema: Arc<Schema>,
    relay_names: impl IntoIterator<Item = &'a str>,
) -> Result<CompiledProgram, CompileError> {
    compile_program_with_options_for_relays(program, schema, relay_names, CompileOptions::default())
}

pub fn compile_program_with_options(
    program: &SpannedNode<Program>,
    schema: Arc<Schema>,
    options: CompileOptions,
) -> Result<CompiledProgram, CompileError> {
    compile_program_with_options_for_relay(program, schema, "input", options)
}

pub fn compile_program_with_options_for_relay(
    program: &SpannedNode<Program>,
    schema: Arc<Schema>,
    relay_name: &str,
    options: CompileOptions,
) -> Result<CompiledProgram, CompileError> {
    compile_program_with_options_for_bindings(
        program,
        schema.clone(),
        [CompileBinding::writable(relay_name, schema)],
        options,
    )
}

pub fn compile_program_with_options_for_relays<'a>(
    program: &SpannedNode<Program>,
    schema: Arc<Schema>,
    relay_names: impl IntoIterator<Item = &'a str>,
    options: CompileOptions,
) -> Result<CompiledProgram, CompileError> {
    let bindings = relay_names
        .into_iter()
        .map(|relay_name| CompileBinding::writable(relay_name, schema.clone()))
        .collect::<Vec<_>>();
    compile_program_with_options_for_bindings(program, schema, bindings, options)
}

pub fn compile_program_for_bindings(
    program: &SpannedNode<Program>,
    output_schema: Arc<Schema>,
    bindings: impl IntoIterator<Item = CompileBinding>,
) -> Result<CompiledProgram, CompileError> {
    compile_program_with_options_for_bindings_with_sensitivity(
        program,
        output_schema,
        SchemaSensitivity::default(),
        bindings,
        CompileOptions::default(),
    )
}

pub fn compile_program_for_bindings_with_sensitivity(
    program: &SpannedNode<Program>,
    output_schema: Arc<Schema>,
    output_sensitivity: SchemaSensitivity,
    bindings: impl IntoIterator<Item = CompileBinding>,
) -> Result<CompiledProgram, CompileError> {
    compile_program_with_options_for_bindings_with_sensitivity(
        program,
        output_schema,
        output_sensitivity,
        bindings,
        CompileOptions::default(),
    )
}

pub fn infer_set_expr_types_for_bindings(
    program: &SpannedNode<Program>,
    bindings: impl IntoIterator<Item = CompileBinding>,
) -> Result<Vec<(String, DataType, bool)>, CompileError> {
    infer_set_expr_types_for_bindings_with_udfs(program, bindings, UdfSignatures::default())
}

pub fn infer_set_expr_types_for_bindings_with_udfs(
    program: &SpannedNode<Program>,
    bindings: impl IntoIterator<Item = CompileBinding>,
    udf_signatures: UdfSignatures,
) -> Result<Vec<(String, DataType, bool)>, CompileError> {
    let bindings = bindings.into_iter().collect::<Vec<_>>();
    let (mut compiler, _input_schema) = Compiler::new(&bindings)?;
    compiler.udf_signatures = udf_signatures;
    for namespace in compiler.writable_namespaces.iter().cloned() {
        compiler
            .readable_namespaces
            .insert(CompileNamespace::User(namespace));
    }
    let mut output = Vec::with_capacity(program.inner.set.len());
    for (field_ref, expr) in &program.inner.set {
        compiler.validate_target_field_ref(field_ref, expr.span)?;
        let data_type = compiler.infer_expr_type(expr)?;
        let nullable = compiler.expr_may_be_null(expr)?;
        let sensitive = compiler.expr_is_sensitive(expr)?;
        compiler.update_writable_field(
            &field_ref.field,
            ColumnBinding {
                data_type: data_type.clone(),
                nullable,
                sensitive,
                value: ColumnValue::Unsupported,
            },
        );
        if let Some((_, output_type, output_nullable)) = output
            .iter_mut()
            .find(|(field, _, _)| field == &field_ref.field)
        {
            *output_type = data_type;
            *output_nullable = nullable;
        } else {
            output.push((field_ref.field.clone(), data_type, nullable));
        }
    }
    Ok(output)
}

pub fn compile_program_with_options_for_bindings(
    program: &SpannedNode<Program>,
    output_schema: Arc<Schema>,
    bindings: impl IntoIterator<Item = CompileBinding>,
    options: CompileOptions,
) -> Result<CompiledProgram, CompileError> {
    compile_program_with_options_for_bindings_with_sensitivity(
        program,
        output_schema,
        SchemaSensitivity::default(),
        bindings,
        options,
    )
}

pub fn compile_program_with_options_for_bindings_with_sensitivity(
    program: &SpannedNode<Program>,
    output_schema: Arc<Schema>,
    output_sensitivity: SchemaSensitivity,
    bindings: impl IntoIterator<Item = CompileBinding>,
    options: CompileOptions,
) -> Result<CompiledProgram, CompileError> {
    let bindings = bindings.into_iter().collect::<Vec<_>>();
    let (mut compiler, input_schema) = Compiler::new(&bindings)?;
    compiler.apply_options(&options);
    output_sensitivity.validate_against_schema(&output_schema, "output")?;
    let output_field_names = output_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<HashSet<_>>();

    for (field_ref, expr) in &program.inner.set {
        let name = compiler.validate_target_field_ref(field_ref, expr.span)?;
        if !output_field_names.contains(name) {
            return Err(CompileError {
                code: "unknown_set",
                message: format!("SET field '{name}' is not declared in the output schema"),
                span: expr.span,
            });
        }
    }

    compiler.enter_set_scope(&output_schema, &output_sensitivity);
    if let OutputMode::PassthroughByName = options.output_mode {
        for field in output_schema.fields() {
            if let Ok(source) = compiler
                .passthrough_binding_for_output_field(field.name(), program.span)
                .cloned()
            {
                compiler.update_writable_field(field.name(), source);
            }
        }
    }

    for (field_ref, expr) in &program.inner.set {
        let field = output_schema
            .field_with_name(&field_ref.field)
            .expect("SET target was validated against the output schema");
        let field_sensitive = output_sensitivity.is_sensitive(field.name());
        compiler.validate_assignment_expr(
            field.name(),
            expr,
            field.data_type(),
            field.is_nullable(),
            field_sensitive,
            options.allow_sensitive_output,
        )?;
        let nullable = compiler.expr_may_be_null(expr)?;
        let previous_binding = compiler.validate_field_ref(field_ref, expr.span)?.clone();
        let fallback = match previous_binding.value {
            ColumnValue::Initialized(reg) => AssignmentFallback::Register(reg),
            ColumnValue::Uninitialized => {
                AssignmentFallback::Uninitialized(previous_binding.data_type.clone())
            }
            ColumnValue::Unsupported => {
                return Err(CompileError {
                    code: "unsupported_assignment_target",
                    message: format!(
                        "SET target '{}.{}' has unsupported type {:?}",
                        field_ref.relay, field_ref.field, previous_binding.data_type
                    ),
                    span: expr.span,
                });
            }
        };
        let compiled = compiler.compile_assignment_expr(expr, field.data_type())?;
        let reg = compiler.alloc_output(compiled.ty);
        if expr_semantics(expr).is_some_and(|semantics| semantics.can_error)
            || expression_contains_udf(expr)
        {
            compiler.emit_assignment(reg, compiled, fallback, expr.span);
        } else {
            compiler.emit_move(reg, compiled, expr.span);
        }
        compiler.update_writable_field(
            field.name(),
            ColumnBinding {
                data_type: field.data_type().clone(),
                nullable,
                sensitive: field_sensitive,
                value: ColumnValue::Initialized(reg),
            },
        );
    }

    let filter = if let Some(filter_expr) = &program.inner.filter {
        let filter_type = compiler.infer_expr_type(filter_expr)?;
        if filter_type != DataType::Boolean {
            return Err(CompileError {
                code: "invalid_filter",
                message: "WHERE expression must evaluate to Boolean".to_string(),
                span: filter_expr.span,
            });
        }
        let filter_reg = compiler.alloc_condition(RegisterType::Boolean);
        let compiled = compiler.compile_expr(filter_expr)?;
        compiler.emit_move(filter_reg, compiled, filter_expr.span);
        Some(filter_reg)
    } else {
        None
    };

    let invocations = program
        .inner
        .invoke
        .iter()
        .map(|invocation| compiler.compile_invocation(invocation))
        .collect::<Result<Vec<_>, _>>()?;

    let output_namespace = compiler
        .writable_namespaces
        .iter()
        .next()
        .expect("compiler requires one writable namespace")
        .clone();
    let mut outputs = Vec::with_capacity(output_schema.fields().len());
    for (output_index, field) in output_schema.fields().iter().enumerate() {
        let binding = compiler
            .columns
            .get(&BoundFieldRef::User(FieldRef {
                relay: output_namespace.clone(),
                field: field.name().clone(),
            }))
            .expect("output fields were installed in SET scope")
            .clone();
        if binding.data_type != *field.data_type() {
            return Err(CompileError {
                code: "type_mismatch",
                message: format!(
                    "output field '{}' has expression type {:?}, expected declared output type \
                     {:?}",
                    field.name(),
                    binding.data_type,
                    field.data_type()
                ),
                span: program.span,
            });
        }
        if binding.nullable && !field.is_nullable() {
            let (code, message) = if let ColumnValue::Uninitialized = binding.value {
                (
                    "uninitialized_required_field",
                    format!(
                        "required output field '{}' remains uninitialized",
                        field.name()
                    ),
                )
            } else {
                (
                    "null_for_required_field",
                    format!(
                        "output field '{}' may be null but the output field is required",
                        field.name()
                    ),
                )
            };
            return Err(CompileError {
                code,
                message,
                span: program.span,
            });
        }
        if binding.sensitive
            && !output_sensitivity.is_sensitive(field.name())
            && !options.allow_sensitive_output
        {
            return Err(CompileError {
                code: "sensitive_leak",
                message: format!(
                    "output field '{}' would store sensitive data in a non-sensitive output \
                     field; use SET with leak_sensitive(...) to explicitly remove sensitivity",
                    field.name()
                ),
                span: program.span,
            });
        }
        let reg = match binding.value {
            ColumnValue::Initialized(reg) => reg,
            ColumnValue::Uninitialized => {
                let ty = Compiler::register_type_for_data_type(
                    field.data_type(),
                    program.span,
                    "uninitialized output",
                )?;
                let reg = compiler.alloc_output(ty);
                compiler.emit(
                    InstructionKind::Uninitialized {
                        dst: reg,
                        data_type: field.data_type().clone(),
                    },
                    program.span,
                );
                reg
            }
            ColumnValue::Unsupported => {
                return Err(CompileError {
                    code: "unsupported_passthrough",
                    message: format!(
                        "output field '{}' has unsupported type for FILTER-MAP execution",
                        field.name()
                    ),
                    span: program.span,
                });
            }
        };
        outputs.push(OutputBinding {
            output_index,
            name: field.name().clone(),
            reg,
        });
    }

    let mut branch_filters = Vec::with_capacity(program.inner.branch_filters.len());
    for filter_expr in &program.inner.branch_filters {
        let filter_type = compiler.infer_expr_type(filter_expr)?;
        if filter_type != DataType::Boolean {
            return Err(CompileError {
                code: "invalid_filter",
                message: "WHERE expression must evaluate to Boolean".to_string(),
                span: filter_expr.span,
            });
        }
        let filter_reg = compiler.alloc_condition(RegisterType::Boolean);
        let compiled = compiler.compile_expr(filter_expr)?;
        compiler.emit_move(filter_reg, compiled, filter_expr.span);
        branch_filters.push(filter_reg);
    }

    optimize_instructions(
        &mut compiler.instructions,
        &outputs,
        filter,
        &branch_filters,
    );
    if options.optimize_temp_registers {
        remap_temp_registers(&mut compiler.instructions, &mut compiler.layouts.temps);
    }

    Ok(CompiledProgram {
        input_schema,
        output_schema,
        inputs: compiler.inputs,
        instructions: compiler.instructions,
        filter,
        branch_filters,
        outputs,
        invocations,
        layouts: compiler.layouts,
        injector: compiler.injector,
    })
}

fn optimize_instructions(
    instructions: &mut Vec<Instruction>,
    outputs: &[OutputBinding],
    filter: Option<RegisterRef>,
    branch_filters: &[RegisterRef],
) {
    eliminate_redundant_moves(instructions);
    eliminate_dead_removable_temps(instructions, outputs, filter, branch_filters);
}

fn eliminate_redundant_moves(instructions: &mut Vec<Instruction>) {
    loop {
        let use_counts = collect_register_use_counts(instructions);
        let definitions = collect_register_definitions(instructions);
        let mut changed = false;
        let mut removed = vec![false; instructions.len()];

        for inst_idx in 0..instructions.len() {
            let InstructionKind::Move { dst, input } = instructions[inst_idx].kind else {
                continue;
            };

            if let Some(&definition_idx) = definitions.get(&input)
                && input.space == RegisterSpace::Temp
                && use_counts.get(&input) == Some(&1)
                && definition_idx < inst_idx
            {
                let producer = &mut instructions[definition_idx];
                rewrite_instruction_output(&mut producer.kind, dst);
                removed[inst_idx] = true;
                changed = true;
            }
        }

        *instructions = instructions
            .iter()
            .cloned()
            .enumerate()
            .filter_map(|(idx, instruction)| (!removed[idx]).then_some(instruction))
            .collect();
        if !changed {
            break;
        }
    }
}

fn expression_contains_udf(expr: &SpannedExpr) -> bool {
    match &expr.inner {
        Expr::Call { function, args } => {
            matches!(function, FunctionName::Udf(_)) || args.iter().any(expression_contains_udf)
        }
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => expression_contains_udf(expr),
        Expr::Binary { left, right, .. } => {
            expression_contains_udf(left) || expression_contains_udf(right)
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            operand.as_deref().is_some_and(expression_contains_udf)
                || branches.iter().any(|branch| {
                    expression_contains_udf(&branch.when) || expression_contains_udf(&branch.result)
                })
                || else_result.as_deref().is_some_and(expression_contains_udf)
        }
        Expr::Literal(_) | Expr::FieldRef(_) | Expr::InternalFieldRef(_) => false,
    }
}

fn expression_contains_volatile_udf(expr: &SpannedExpr, signatures: &UdfSignatures) -> bool {
    match &expr.inner {
        Expr::Call { function, args } => {
            matches!(
                function,
                FunctionName::Udf(name)
                    if signatures.get(name).is_some_and(|signature| signature.volatile)
            ) || args
                .iter()
                .any(|argument| expression_contains_volatile_udf(argument, signatures))
        }
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => {
            expression_contains_volatile_udf(expr, signatures)
        }
        Expr::Binary { left, right, .. } => {
            expression_contains_volatile_udf(left, signatures)
                || expression_contains_volatile_udf(right, signatures)
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            operand
                .as_deref()
                .is_some_and(|operand| expression_contains_volatile_udf(operand, signatures))
                || branches.iter().any(|branch| {
                    expression_contains_volatile_udf(&branch.when, signatures)
                        || expression_contains_volatile_udf(&branch.result, signatures)
                })
                || else_result
                    .as_deref()
                    .is_some_and(|result| expression_contains_volatile_udf(result, signatures))
        }
        Expr::Literal(_) | Expr::FieldRef(_) | Expr::InternalFieldRef(_) => false,
    }
}

fn eliminate_dead_removable_temps(
    instructions: &mut Vec<Instruction>,
    outputs: &[OutputBinding],
    filter: Option<RegisterRef>,
    branch_filters: &[RegisterRef],
) {
    let mut live = outputs
        .iter()
        .map(|output| output.reg)
        .collect::<HashSet<_>>();
    if let Some(filter) = filter {
        live.insert(filter);
    }
    live.extend(branch_filters.iter().copied());

    let mut retained = Vec::with_capacity(instructions.len());
    for instruction in instructions.iter().rev() {
        let output = instruction_output(&instruction.kind);
        let dead_temp = output.space == RegisterSpace::Temp && !live.contains(&output);
        if dead_temp && instruction_is_removable_if_dead(&instruction.kind) {
            continue;
        }

        live.remove(&output);
        for input in instruction_inputs(instruction) {
            live.insert(input);
        }
        retained.push(instruction.clone());
    }

    retained.reverse();
    *instructions = retained;
}

fn collect_register_use_counts(instructions: &[Instruction]) -> HashMap<RegisterRef, usize> {
    let mut counts = HashMap::new();
    for instruction in instructions {
        for input in instruction_inputs(instruction) {
            *counts.entry(input).or_default() += 1;
        }
    }
    counts
}

fn collect_register_definitions(instructions: &[Instruction]) -> HashMap<RegisterRef, usize> {
    instructions
        .iter()
        .enumerate()
        .map(|(idx, instruction)| (instruction_output(&instruction.kind), idx))
        .collect()
}

fn instruction_is_removable_if_dead(kind: &InstructionKind) -> bool {
    match kind {
        InstructionKind::Move { .. }
        | InstructionKind::Assign { .. }
        | InstructionKind::Literal { .. }
        | InstructionKind::NullLiteral { .. }
        | InstructionKind::Uninitialized { .. } => true,
        InstructionKind::Unary { op, .. } => unary_descriptor(*op)
            .semantics
            .supports_common_subexpression_elimination(),
        InstructionKind::Binary { op, .. } => binary_descriptor(*op)
            .semantics
            .supports_common_subexpression_elimination(),
        InstructionKind::Cast { .. } => cast_descriptor()
            .semantics
            .supports_common_subexpression_elimination(),
        InstructionKind::Builtin { lowering, .. } => {
            builtin_semantics_for_lowering(*lowering).supports_common_subexpression_elimination()
        }
        InstructionKind::Inject { .. } => false,
        InstructionKind::Select { .. } => true,
    }
}

fn instruction_can_emit_row_errors(kind: &InstructionKind) -> bool {
    match kind {
        InstructionKind::Unary { op, .. } => unary_descriptor(*op).semantics.can_error,
        InstructionKind::Binary { op, .. } => binary_descriptor(*op).semantics.can_error,
        InstructionKind::Cast { .. } => cast_descriptor().semantics.can_error,
        InstructionKind::Builtin { lowering, .. } => {
            builtin_semantics_for_lowering(*lowering).can_error
        }
        InstructionKind::Move { .. }
        | InstructionKind::Assign { .. }
        | InstructionKind::Literal { .. }
        | InstructionKind::NullLiteral { .. }
        | InstructionKind::Uninitialized { .. }
        | InstructionKind::Select { .. } => false,
        InstructionKind::Inject { .. } => true,
    }
}

fn remap_temp_registers(instructions: &mut [Instruction], layout: &mut crate::ir::RegisterLayout) {
    let last_uses = collect_temp_last_uses(instructions);
    let mut active = HashMap::<RegisterRef, usize>::new();
    let mut free = FreeTempSlots::default();
    let mut peak = crate::ir::RegisterLayout::default();

    for (inst_idx, instruction) in instructions.iter_mut().enumerate() {
        let logical_error_mask = instruction.error_mask;
        let inputs = instruction_inputs(instruction);
        let mut dead_inputs = HashSet::new();
        let mut deferred_dead_inputs = HashSet::new();

        for input in &inputs {
            if input.space == RegisterSpace::Temp {
                let physical_index = *active
                    .get(input)
                    .expect("temp register must be assigned before it is read");
                rewrite_temp_input(instruction, *input, physical_index);
                if last_uses.get(input) == Some(&inst_idx) {
                    if Some(*input) == logical_error_mask {
                        deferred_dead_inputs.insert(*input);
                    } else {
                        dead_inputs.insert(*input);
                    }
                }
            }
        }

        for dead in dead_inputs {
            let physical_index = active
                .remove(&dead)
                .expect("dead temp register must still be active");
            free.release(dead.ty, physical_index);
        }

        let output = instruction_output(&instruction.kind);
        if output.space == RegisterSpace::Temp {
            let physical_index = free.acquire(output.ty, &mut peak);
            active.insert(output, physical_index);
            rewrite_temp_output(&mut instruction.kind, physical_index);
        }

        for dead in deferred_dead_inputs {
            let physical_index = active
                .remove(&dead)
                .expect("dead error-mask temp register must still be active");
            free.release(dead.ty, physical_index);
        }
    }

    *layout = peak;
}

fn collect_temp_last_uses(instructions: &[Instruction]) -> HashMap<RegisterRef, usize> {
    let mut last_uses = HashMap::new();
    for (inst_idx, instruction) in instructions.iter().enumerate() {
        for input in instruction_inputs(instruction) {
            if input.space == RegisterSpace::Temp {
                last_uses.insert(input, inst_idx);
            }
        }
    }
    last_uses
}

fn instruction_inputs(instruction: &Instruction) -> Vec<RegisterRef> {
    let mut inputs = match &instruction.kind {
        InstructionKind::Move { input, .. }
        | InstructionKind::Unary { input, .. }
        | InstructionKind::Cast { input, .. } => vec![*input],
        InstructionKind::Assign {
            input, fallback, ..
        } => {
            let mut inputs = vec![*input];
            if let AssignmentFallback::Register(previous) = fallback {
                inputs.push(*previous);
            }
            inputs
        }
        InstructionKind::Literal { .. }
        | InstructionKind::NullLiteral { .. }
        | InstructionKind::Uninitialized { .. } => Vec::new(),
        InstructionKind::Binary { left, right, .. } => vec![*left, *right],
        InstructionKind::Builtin { inputs, .. } | InstructionKind::Inject { inputs, .. } => {
            inputs.clone()
        }
        InstructionKind::Select {
            arms, otherwise, ..
        } => {
            let mut inputs = Vec::with_capacity(arms.len() * 2 + 1);
            for arm in arms {
                inputs.push(arm.mask);
                inputs.push(arm.value);
            }
            inputs.push(*otherwise);
            inputs
        }
    };
    if let Some(error_mask) = instruction.error_mask {
        inputs.push(error_mask);
    }
    inputs
}

fn instruction_output(kind: &InstructionKind) -> RegisterRef {
    match kind {
        InstructionKind::Move { dst, .. }
        | InstructionKind::Assign { dst, .. }
        | InstructionKind::Literal { dst, .. }
        | InstructionKind::NullLiteral { dst, .. }
        | InstructionKind::Uninitialized { dst, .. }
        | InstructionKind::Unary { dst, .. }
        | InstructionKind::Binary { dst, .. }
        | InstructionKind::Cast { dst, .. }
        | InstructionKind::Builtin { dst, .. }
        | InstructionKind::Inject { dst, .. }
        | InstructionKind::Select { dst, .. } => *dst,
    }
}

fn rewrite_instruction_output(kind: &mut InstructionKind, dst: RegisterRef) {
    match kind {
        InstructionKind::Move { dst: output, .. }
        | InstructionKind::Assign { dst: output, .. }
        | InstructionKind::Literal { dst: output, .. }
        | InstructionKind::NullLiteral { dst: output, .. }
        | InstructionKind::Uninitialized { dst: output, .. }
        | InstructionKind::Unary { dst: output, .. }
        | InstructionKind::Binary { dst: output, .. }
        | InstructionKind::Cast { dst: output, .. }
        | InstructionKind::Builtin { dst: output, .. }
        | InstructionKind::Inject { dst: output, .. }
        | InstructionKind::Select { dst: output, .. } => *output = dst,
    }
}

fn rewrite_temp_input(instruction: &mut Instruction, from: RegisterRef, to_index: usize) {
    let rewrite = |reg: &mut RegisterRef| {
        if *reg == from {
            reg.index = to_index;
        }
    };

    match &mut instruction.kind {
        InstructionKind::Move { input, .. }
        | InstructionKind::Unary { input, .. }
        | InstructionKind::Cast { input, .. } => rewrite(input),
        InstructionKind::Assign {
            input, fallback, ..
        } => {
            rewrite(input);
            if let AssignmentFallback::Register(previous) = fallback {
                rewrite(previous);
            }
        }
        InstructionKind::Literal { .. }
        | InstructionKind::NullLiteral { .. }
        | InstructionKind::Uninitialized { .. } => {}
        InstructionKind::Binary { left, right, .. } => {
            rewrite(left);
            rewrite(right);
        }
        InstructionKind::Builtin { inputs, .. } | InstructionKind::Inject { inputs, .. } => {
            for input in inputs {
                rewrite(input);
            }
        }
        InstructionKind::Select {
            arms, otherwise, ..
        } => {
            for arm in arms {
                rewrite(&mut arm.mask);
                rewrite(&mut arm.value);
            }
            rewrite(otherwise);
        }
    }
    if let Some(error_mask) = &mut instruction.error_mask {
        rewrite(error_mask);
    }
}

fn rewrite_temp_output(kind: &mut InstructionKind, to_index: usize) {
    let output = match kind {
        InstructionKind::Move { dst, .. }
        | InstructionKind::Assign { dst, .. }
        | InstructionKind::Literal { dst, .. }
        | InstructionKind::NullLiteral { dst, .. }
        | InstructionKind::Uninitialized { dst, .. }
        | InstructionKind::Unary { dst, .. }
        | InstructionKind::Binary { dst, .. }
        | InstructionKind::Cast { dst, .. }
        | InstructionKind::Builtin { dst, .. }
        | InstructionKind::Inject { dst, .. }
        | InstructionKind::Select { dst, .. } => dst,
    };
    output.index = to_index;
}

#[derive(Default)]
struct FreeTempSlots {
    slots: HashMap<RegisterType, Vec<usize>>,
}

impl FreeTempSlots {
    fn acquire(&mut self, ty: RegisterType, peak: &mut crate::ir::RegisterLayout) -> usize {
        self.slots
            .entry(ty)
            .or_default()
            .pop()
            .unwrap_or_else(|| peak.alloc(ty))
    }

    fn release(&mut self, ty: RegisterType, index: usize) {
        self.slots.entry(ty).or_default().push(index);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use nervix_nspl::vm_program::{
        Expr, FieldRef, InternalFieldNamespace, InternalFieldRef, Program, SpannedNode,
        parse_program,
    };

    use super::*;

    fn has_builtin(compiled: &CompiledProgram, lowering: BuiltinLowering) -> bool {
        compiled.instructions.iter().any(|instruction| {
            matches!(
                instruction.kind,
                InstructionKind::Builtin {
                    lowering: builtin,
                    ..
                } if builtin == lowering
            )
        })
    }

    fn schema(fields: Vec<Field>) -> Arc<Schema> {
        Arc::new(Schema::new(fields))
    }

    fn sensitivity(fields: &[&str]) -> SchemaSensitivity {
        SchemaSensitivity::from_sensitive_fields(fields.iter().copied())
    }

    fn with_output_fields(input_schema: &Arc<Schema>, fields: Vec<Field>) -> Arc<Schema> {
        let mut output_fields = input_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        output_fields.extend(fields);
        schema(output_fields)
    }

    fn compile_program_with_output(
        program: &SpannedNode<Program>,
        input_schema: Arc<Schema>,
        output_schema: Arc<Schema>,
    ) -> Result<CompiledProgram, CompileError> {
        compile_program_with_sensitive_output(
            program,
            input_schema,
            SchemaSensitivity::default(),
            output_schema,
            SchemaSensitivity::default(),
        )
    }

    fn compile_program_with_sensitive_output(
        program: &SpannedNode<Program>,
        input_schema: Arc<Schema>,
        input_sensitivity: SchemaSensitivity,
        output_schema: Arc<Schema>,
        output_sensitivity: SchemaSensitivity,
    ) -> Result<CompiledProgram, CompileError> {
        compile_program_for_bindings_with_sensitivity(
            program,
            output_schema,
            output_sensitivity,
            [CompileBinding::writable("input", input_schema).with_sensitivity(input_sensitivity)],
        )
    }

    fn compile_program_with_output_fields(
        program: &SpannedNode<Program>,
        input_schema: Arc<Schema>,
        fields: Vec<Field>,
    ) -> Result<CompiledProgram, CompileError> {
        let output_schema = with_output_fields(&input_schema, fields);
        compile_program_with_output(program, input_schema, output_schema)
    }

    #[test]
    fn resolves_udfs_only_through_the_udf_namespace() {
        let schema = schema(vec![Field::new("value", DataType::Int64, true)]);
        let mut udf_signatures = UdfSignatures::default();
        udf_signatures.insert(
            "add_one",
            UdfSignature {
                arguments: vec![UdfParameter {
                    data_type: DataType::Int64,
                    optional: false,
                }],
                return_type: DataType::Int64,
                return_optional: false,
                volatile: false,
            },
        );
        let options = CompileOptions {
            udf_signatures,
            ..CompileOptions::default()
        };
        let qualified = parse_program("SET input.value = udf::add_one(input.value);")
            .expect("qualified call must parse");
        compile_program_with_options(&qualified, schema.clone(), options.clone())
            .expect("qualified call must resolve");

        let bare = parse_program("SET input.value = add_one(input.value);")
            .expect("bare call remains syntactically valid");
        let error = compile_program_with_options(&bare, schema, options)
            .expect_err("bare call must not resolve against the UDF catalog");
        assert_eq!(error.code, "unknown_function");
    }

    #[test]
    fn rejects_mixed_operand_types() {
        let program =
            parse_program("SET input.total = input.quantity + input.price;").expect("must parse");
        let schema = schema(vec![
            Field::new("quantity", DataType::Int64, true),
            Field::new("price", DataType::Float64, true),
        ]);

        let error = compile_program_with_output_fields(
            &program,
            schema,
            vec![Field::new("total", DataType::Int64, true)],
        )
        .expect_err("must fail");
        assert_eq!(error.code, "type_mismatch");
    }

    #[test]
    fn validates_conditional_expression_types_and_nullability() {
        let schema = schema(vec![
            Field::new("number", DataType::Int64, true),
            Field::new("kind", DataType::Utf8, true),
        ]);
        let invalid_condition =
            parse_program("SET input.result = CASE WHEN input.number THEN 1 ELSE 0 END;")
                .expect("program must parse");
        let error = compile_program_with_output_fields(
            &invalid_condition,
            schema.clone(),
            vec![Field::new("result", DataType::Int64, true)],
        )
        .expect_err("non-Boolean condition must fail");
        assert_eq!(error.code, "invalid_condition");

        let mismatched_results = parse_program(
            "SET input.result = CASE input.kind WHEN \"number\" THEN 1 ELSE \"text\" END;",
        )
        .expect("program must parse");
        let error = compile_program_with_output_fields(
            &mismatched_results,
            schema.clone(),
            vec![Field::new("result", DataType::Int64, true)],
        )
        .expect_err("mixed CASE results must fail");
        assert_eq!(error.code, "type_mismatch");

        let untyped =
            parse_program("SET input.result = CASE WHEN input.number = 0 THEN NULL ELSE NULL END;")
                .expect("program must parse");
        let error = compile_program_with_output_fields(
            &untyped,
            schema.clone(),
            vec![Field::new("result", DataType::Int64, true)],
        )
        .expect_err("all-NULL CASE must fail");
        assert_eq!(error.code, "untyped_null");

        let omitted_else =
            parse_program("SET input.result = CASE WHEN input.number = 0 THEN 1 END;")
                .expect("program must parse");
        let error = compile_program_with_output_fields(
            &omitted_else,
            schema,
            vec![Field::new("result", DataType::Int64, false)],
        )
        .expect_err("omitted CASE ELSE must require an optional output");
        assert_eq!(error.code, "null_for_required_field");
    }

    #[test]
    fn folds_constant_if_without_emitting_select() {
        let program = parse_program("SET input.result = IF TRUE THEN 1 ELSE 2 END;")
            .expect("program must parse");
        let schema = schema(vec![Field::new("source", DataType::Int64, false)]);
        let compiled = compile_program_with_output_fields(
            &program,
            schema,
            vec![Field::new("result", DataType::Int64, false)],
        )
        .expect("constant IF must compile");

        assert!(
            compiled
                .instructions
                .iter()
                .all(|instruction| !matches!(instruction.kind, InstructionKind::Select { .. }))
        );
    }

    #[test]
    fn propagates_sensitivity_from_conditional_conditions() {
        let program = parse_program("SET input.result = IF input.secret THEN 1 ELSE 0 END;")
            .expect("program must parse");
        let input_schema = schema(vec![Field::new("secret", DataType::Boolean, false)]);
        let output_schema = with_output_fields(
            &input_schema,
            vec![Field::new("result", DataType::Int64, false)],
        );
        let error = compile_program_with_sensitive_output(
            &program,
            input_schema,
            sensitivity(&["secret"]),
            output_schema,
            SchemaSensitivity::default(),
        )
        .expect_err("sensitive condition must taint the CASE result");

        assert_eq!(error.code, "sensitive_leak");
    }

    #[test]
    fn lowers_builtin_to_dedicated_instruction() {
        let program = parse_program("SET input.lowered = lower(input.name);").expect("must parse");
        let schema = schema(vec![Field::new("name", DataType::Utf8, true)]);

        let compiled = compile_program_with_output_fields(
            &program,
            schema,
            vec![Field::new("lowered", DataType::Utf8, true)],
        )
        .expect("must compile");

        assert!(has_builtin(&compiled, BuiltinLowering::Lower));
    }

    #[test]
    fn compiles_internal_lookup_hash_map_binding_without_user_namespace() {
        let program = SpannedNode {
            inner: Program {
                filter: None,
                branch_filters: Vec::new(),
                set: vec![(
                    FieldRef {
                        relay: "input".to_string(),
                        field: "enriched".to_string(),
                    },
                    SpannedNode {
                        inner: Expr::InternalFieldRef(InternalFieldRef {
                            namespace: InternalFieldNamespace::LookupHashMap,
                            field: "value_0".to_string(),
                        }),
                        span: (0..0).into(),
                    },
                )],
                invoke: Vec::new(),
            },
            span: (0..0).into(),
        };
        let input_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("enriched", DataType::Utf8, true),
        ]));
        let lookup_schema = Arc::new(Schema::new(vec![Field::new(
            "value_0",
            DataType::Utf8,
            true,
        )]));

        let compiled = compile_program_for_bindings(
            &program,
            output_schema,
            [
                CompileBinding::writable("input", input_schema),
                CompileBinding::internal_readonly(
                    InternalFieldNamespace::LookupHashMap,
                    lookup_schema,
                ),
            ],
        )
        .expect("internal lookup binding must compile");

        assert!(
            compiled
                .input_schema
                .field_with_name("internal LOOKUP_HASH_MAP.value_0")
                .is_ok()
        );
    }

    #[test]
    fn header_functions_are_contextual_and_plural_reads_are_typed_vectors() {
        let parsed = parse_program("SET input.headers = read_headers(lower(input.name))")
            .expect("program must parse");
        let input_schema = schema(vec![Field::new("name", DataType::Utf8, false)]);
        let headers_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)));
        let output_schema = with_output_fields(
            &input_schema,
            vec![Field::new("headers", headers_type.clone(), false)],
        );

        let error = compile_program_for_bindings(
            &parsed,
            output_schema.clone(),
            [CompileBinding::writable("input", input_schema.clone())],
        )
        .expect_err("header reads must be rejected outside an ingestor context");
        assert_eq!(error.code, "unsupported_function_context");

        let compiled = compile_program_with_options_for_bindings(
            &parsed,
            output_schema,
            [CompileBinding::writable("input", input_schema)],
            CompileOptions {
                allow_header_reads: true,
                ..CompileOptions::default()
            },
        )
        .expect("header reads must compile for a supported ingestor");
        assert_eq!(
            compiled
                .output_schema
                .field_with_name("headers")
                .unwrap()
                .data_type(),
            &headers_type
        );
    }

    #[test]
    fn write_header_is_only_valid_as_an_emitter_invocation() {
        let schema = schema(vec![Field::new("value", DataType::Utf8, false)]);
        let expression = parse_program("SET input.value = write_header(\"name\", input.value)")
            .expect("program must parse");
        let error = compile_program_with_options_for_bindings(
            &expression,
            schema.clone(),
            [CompileBinding::writable("input", schema.clone())],
            CompileOptions {
                allow_header_writes: true,
                ..CompileOptions::default()
            },
        )
        .expect_err("write_header must not be an expression");
        assert_eq!(error.code, "invalid_side_effect_call");

        let invocation = parse_program("INVOKE write_header(\"name\", input.value)")
            .expect("program must parse");
        let error = compile_program_for_bindings(
            &invocation,
            schema,
            [CompileBinding::writable(
                "input",
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Utf8,
                    false,
                )])),
            )],
        )
        .expect_err("INVOKE must be rejected outside an emitter context");
        assert_eq!(error.code, "unsupported_invoke_context");
    }

    #[test]
    fn rejects_set_target_missing_from_declared_output_schema() {
        let program = parse_program("SET input.extra = input.value;").expect("must parse");
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));

        let error = compile_program_for_bindings(
            &program,
            output_schema,
            [CompileBinding::writable("input", input_schema)],
        )
        .expect_err("SET target outside declared output schema must fail");

        assert_eq!(error.code, "unknown_set");
    }

    #[test]
    fn allows_declared_target_only_field_without_legacy_unset() {
        let program = parse_program("SET input.total = input.value;").expect("must parse");
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("legacy", DataType::Utf8, true),
        ]));
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "total",
            DataType::Int64,
            true,
        )]));

        let compiled = compile_program_for_bindings(
            &program,
            output_schema,
            [CompileBinding::writable("input", input_schema)],
        )
        .expect("declared computed output must compile without legacy source-drop syntax");

        assert!(compiled.output_schema.field_with_name("total").is_ok());
    }

    #[test]
    fn rejects_sensitive_field_assignment_to_non_sensitive_output() {
        let program =
            parse_program("SET input.public_value = lower(input.secret);").expect("must parse");
        let input_schema = schema(vec![Field::new("secret", DataType::Utf8, true)]);
        let output_schema = schema(vec![Field::new("public_value", DataType::Utf8, true)]);

        let error = compile_program_with_sensitive_output(
            &program,
            input_schema,
            sensitivity(&["secret"]),
            output_schema,
            SchemaSensitivity::default(),
        )
        .expect_err("sensitive expression must not flow into normal output");

        assert_eq!(error.code, "sensitive_leak");
    }

    #[test]
    fn rejects_sensitive_passthrough_to_non_sensitive_output() {
        let program = SpannedNode {
            inner: Program {
                filter: None,
                branch_filters: Vec::new(),
                set: Vec::new(),
                invoke: Vec::new(),
            },
            span: (0..0).into(),
        };
        let input_schema = schema(vec![Field::new("secret", DataType::Utf8, true)]);
        let output_schema = schema(vec![Field::new("secret", DataType::Utf8, true)]);

        let error = compile_program_with_sensitive_output(
            &program,
            input_schema,
            sensitivity(&["secret"]),
            output_schema,
            SchemaSensitivity::default(),
        )
        .expect_err("automatic sensitive passthrough into normal output must fail");

        assert_eq!(error.code, "sensitive_leak");
    }

    #[test]
    fn allows_sensitive_output_when_compile_option_permits_external_output() {
        let program = parse_program("SET input.public_value = input.secret;").expect("must parse");
        let input_schema = schema(vec![Field::new("secret", DataType::Utf8, true)]);
        let output_schema = schema(vec![Field::new("public_value", DataType::Utf8, true)]);

        compile_program_with_options_for_bindings_with_sensitivity(
            &program,
            output_schema,
            SchemaSensitivity::default(),
            [CompileBinding::writable("input", input_schema)
                .with_sensitivity(sensitivity(&["secret"]))],
            CompileOptions {
                allow_sensitive_output: true,
                ..CompileOptions::default()
            },
        )
        .expect("emitter-style external output may receive sensitive values");
    }

    #[test]
    fn allows_sensitive_output_and_explicit_leak_sensitive_downgrade() {
        let sensitive_program =
            parse_program("SET input.copy = input.secret;").expect("must parse");
        let input_schema = schema(vec![Field::new("secret", DataType::Utf8, true)]);
        let output_schema = schema(vec![Field::new("copy", DataType::Utf8, true)]);
        compile_program_with_sensitive_output(
            &sensitive_program,
            input_schema.clone(),
            sensitivity(&["secret"]),
            output_schema,
            sensitivity(&["copy"]),
        )
        .expect("sensitive value may flow into sensitive output");

        let leak_program = parse_program("SET input.public_value = leak_sensitive(input.secret);")
            .expect("must parse");
        let output_schema = schema(vec![Field::new("public_value", DataType::Utf8, true)]);
        compile_program_with_sensitive_output(
            &leak_program,
            input_schema,
            sensitivity(&["secret"]),
            output_schema,
            SchemaSensitivity::default(),
        )
        .expect("leak_sensitive explicitly removes sensitivity");
    }

    #[test]
    fn allows_null_assignment_to_declared_optional_output_field() {
        let program = parse_program("SET input.maybe = NULL;").expect("must parse");
        let input_schema = schema(Vec::<Field>::new());
        let output_schema = schema(vec![Field::new("maybe", DataType::Utf8, true)]);

        let compiled = compile_program_with_output(&program, input_schema, output_schema)
            .expect("must compile");

        assert!(compiled.instructions.iter().any(|instruction| {
            matches!(
                instruction.kind,
                InstructionKind::NullLiteral {
                    dst,
                    data_type: DataType::Utf8,
                } if dst.ty == RegisterType::Utf8
            )
        }));
    }

    #[test]
    fn rejects_null_assignment_to_required_output_field() {
        let program = parse_program("SET input.maybe = NULL;").expect("must parse");
        let input_schema = schema(Vec::<Field>::new());
        let output_schema = schema(vec![Field::new("maybe", DataType::Utf8, false)]);

        let error = compile_program_with_output(&program, input_schema, output_schema)
            .expect_err("NULL cannot assign required output field");

        assert_eq!(error.code, "null_for_required_field");
    }

    #[test]
    fn rejects_null_without_assignment_target_type() {
        let program = parse_program("WHERE NULL;").expect("must parse");
        let schema = schema(Vec::<Field>::new());

        let error = compile_program(&program, schema).expect_err("untyped NULL must fail");

        assert_eq!(error.code, "untyped_null");
    }

    #[test]
    fn lowers_upper_trim_and_length_builtins() {
        let schema = schema(vec![Field::new("name", DataType::Utf8, true)]);

        let upper_program =
            parse_program("SET input.uppered = upper(input.name);").expect("must parse");
        let upper = compile_program_with_output_fields(
            &upper_program,
            schema.clone(),
            vec![Field::new("uppered", DataType::Utf8, true)],
        )
        .expect("upper must compile");
        assert!(has_builtin(&upper, BuiltinLowering::Upper));

        let trim_program =
            parse_program("SET input.trimmed = trim(input.name);").expect("must parse");
        let trim = compile_program_with_output_fields(
            &trim_program,
            schema.clone(),
            vec![Field::new("trimmed", DataType::Utf8, true)],
        )
        .expect("trim must compile");
        assert!(has_builtin(&trim, BuiltinLowering::Trim));

        let length_program =
            parse_program("SET input.len = length(input.name);").expect("must parse");
        let length = compile_program_with_output_fields(
            &length_program,
            schema,
            vec![Field::new("len", DataType::Int64, true)],
        )
        .expect("length must compile");
        assert!(has_builtin(&length, BuiltinLowering::Length));
    }

    #[test]
    fn lowers_extended_builtins_to_dedicated_instructions() {
        let program = parse_program(
            "SET input.chosen = coalesce(input.primary, input.fallback), input.was_null = \
             is_null(input.primary), input.maybe = nullif(input.primary, input.fallback), \
             input.magnitude = abs(input.amount), input.has = contains(input.text, input.needle), \
             input.starts = starts_with(input.text, input.prefix), input.ends = \
             ends_with(input.text, input.suffix);",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("primary", DataType::Utf8, true),
            Field::new("fallback", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("needle", DataType::Utf8, true),
            Field::new("prefix", DataType::Utf8, true),
            Field::new("suffix", DataType::Utf8, true),
        ]);

        let compiled = compile_program_with_output_fields(
            &program,
            schema,
            vec![
                Field::new("chosen", DataType::Utf8, true),
                Field::new("was_null", DataType::Boolean, true),
                Field::new("maybe", DataType::Utf8, true),
                Field::new("magnitude", DataType::Int64, true),
                Field::new("has", DataType::Boolean, true),
                Field::new("starts", DataType::Boolean, true),
                Field::new("ends", DataType::Boolean, true),
            ],
        )
        .expect("must compile");

        assert!(has_builtin(&compiled, BuiltinLowering::Coalesce));
        assert!(has_builtin(&compiled, BuiltinLowering::IsNull));
        assert!(has_builtin(&compiled, BuiltinLowering::NullIf));
        assert!(has_builtin(&compiled, BuiltinLowering::Abs));
        assert!(has_builtin(&compiled, BuiltinLowering::Contains));
        assert!(has_builtin(&compiled, BuiltinLowering::StartsWith));
        assert!(has_builtin(&compiled, BuiltinLowering::EndsWith));
    }

    #[test]
    fn remaps_single_live_temp_chain_to_one_slot() {
        let program = parse_program("SET input.lowered = lower(trim(upper(input.name)));")
            .expect("must parse");
        let schema = schema(vec![Field::new("name", DataType::Utf8, true)]);

        let compiled = compile_program_with_output_fields(
            &program,
            schema,
            vec![Field::new("lowered", DataType::Utf8, true)],
        )
        .expect("must compile");

        assert_eq!(compiled.layouts.temps.utf8, 1);
        assert_eq!(compiled.layouts.temps.int64, 0);
        assert_eq!(compiled.layouts.temps.float64, 0);
        assert_eq!(compiled.layouts.temps.boolean, 0);
    }

    #[test]
    fn keeps_multiple_temp_slots_when_values_are_live_together() {
        let program = parse_program("SET input.equal = lower(input.left) = lower(input.right);")
            .expect("must parse");
        let schema = schema(vec![
            Field::new("left", DataType::Utf8, true),
            Field::new("right", DataType::Utf8, true),
        ]);

        let compiled = compile_program_with_output_fields(
            &program,
            schema,
            vec![Field::new("equal", DataType::Boolean, true)],
        )
        .expect("must compile");

        assert_eq!(compiled.layouts.temps.utf8, 2);
        assert_eq!(compiled.layouts.temps.boolean, 0);
    }

    #[test]
    fn invalidates_expression_cache_after_each_assignment() {
        let program = parse_program(
            "SET input.lowered = lower(input.name), input.normalized = lower(input.name);",
        )
        .expect("must parse");
        let schema = schema(vec![Field::new("name", DataType::Utf8, true)]);

        let compiled = compile_program_with_output_fields(
            &program,
            schema,
            vec![
                Field::new("lowered", DataType::Utf8, true),
                Field::new("normalized", DataType::Utf8, true),
            ],
        )
        .expect("must compile");

        assert_eq!(
            compiled
                .instructions
                .iter()
                .filter(|instruction| {
                    matches!(
                        instruction.kind,
                        InstructionKind::Builtin {
                            lowering: BuiltinLowering::Lower,
                            ..
                        }
                    )
                })
                .count(),
            2
        );
    }

    #[test]
    fn compiles_repeated_set_targets_in_source_order() {
        let program = parse_program("SET output.amount = 1, output.amount = output.amount + 1;")
            .expect("must parse");
        let input_schema = schema(Vec::<Field>::new());
        let output_schema = schema(vec![Field::new("amount", DataType::Int64, false)]);

        let compiled = compile_program_for_bindings(
            &program,
            output_schema,
            [
                CompileBinding::writeonly(
                    "output",
                    schema(vec![Field::new("amount", DataType::Int64, false)]),
                ),
                CompileBinding::readonly("input", input_schema),
            ],
        )
        .expect("sequential SET must compile");

        assert_eq!(
            compiled
                .instructions
                .iter()
                .filter(|instruction| {
                    matches!(
                        instruction.kind,
                        InstructionKind::Binary {
                            op: BinaryOp::Add,
                            ..
                        }
                    )
                })
                .count(),
            1
        );
    }

    #[test]
    fn rejects_required_output_that_stays_symbolically_uninitialized() {
        let program = parse_program("WHERE true;").expect("must parse");
        let output_schema = schema(vec![Field::new("amount", DataType::Int64, false)]);

        let error = compile_program_for_bindings(
            &program,
            output_schema.clone(),
            [CompileBinding::writeonly("output", output_schema)],
        )
        .expect_err("required uninitialized output must fail validation");

        assert_eq!(error.code, "uninitialized_required_field");
    }

    #[test]
    fn folds_constant_pure_expressions_into_literals() {
        let program =
            parse_program("SET input.lowered = lower(' ABC '), input.has = contains('abc', 'b');")
                .expect("must parse");
        let schema = schema(Vec::<Field>::new());

        let compiled = compile_program_with_output_fields(
            &program,
            schema,
            vec![
                Field::new("lowered", DataType::Utf8, true),
                Field::new("has", DataType::Boolean, true),
            ],
        )
        .expect("must compile");

        assert_eq!(
            compiled
                .instructions
                .iter()
                .filter(|instruction| matches!(instruction.kind, InstructionKind::Literal { .. }))
                .count(),
            2
        );
        assert!(compiled.instructions.iter().all(|instruction| {
            !matches!(
                instruction.kind,
                InstructionKind::Builtin {
                    lowering: BuiltinLowering::Lower | BuiltinLowering::Contains,
                    ..
                }
            )
        }));
    }

    #[test]
    fn keeps_repeated_erroring_expressions_separate() {
        let program = parse_program(
            "SET input.first = input.amount / input.divisor, input.second = input.amount / \
             input.divisor;",
        )
        .expect("must parse");
        let schema = schema(vec![
            Field::new("amount", DataType::Int64, true),
            Field::new("divisor", DataType::Int64, true),
        ]);

        let compiled = compile_program_with_output_fields(
            &program,
            schema,
            vec![
                Field::new("first", DataType::Int64, true),
                Field::new("second", DataType::Int64, true),
            ],
        )
        .expect("must compile");

        assert_eq!(
            compiled
                .instructions
                .iter()
                .filter(|instruction| {
                    matches!(
                        instruction.kind,
                        InstructionKind::Binary {
                            op: BinaryOp::Div,
                            ..
                        }
                    )
                })
                .count(),
            2
        );
    }

    #[test]
    fn can_disable_temp_register_remap() {
        let program = parse_program("SET input.lowered = lower(trim(upper(input.name)));")
            .expect("must parse");
        let schema = schema(vec![Field::new("name", DataType::Utf8, true)]);
        let output_schema =
            with_output_fields(&schema, vec![Field::new("lowered", DataType::Utf8, true)]);

        let optimized =
            compile_program_with_output(&program, schema.clone(), output_schema.clone())
                .expect("must compile");
        let unoptimized = compile_program_with_options_for_bindings(
            &program,
            output_schema,
            [CompileBinding::writable("input", schema)],
            CompileOptions {
                optimize_temp_registers: false,
                ..CompileOptions::default()
            },
        )
        .expect("must compile");

        assert_eq!(optimized.layouts.temps.utf8, 1);
        assert_eq!(unoptimized.layouts.temps.utf8, 3);
    }

    #[test]
    fn eliminates_single_use_moves_and_dead_pure_temps() {
        let literal = RegisterRef::new(RegisterSpace::Temp, RegisterType::Utf8, 0);
        let lowered = RegisterRef::new(RegisterSpace::Temp, RegisterType::Utf8, 1);
        let dead = RegisterRef::new(RegisterSpace::Temp, RegisterType::Utf8, 2);
        let output = RegisterRef::new(RegisterSpace::Output, RegisterType::Utf8, 0);
        let outputs = vec![OutputBinding {
            output_index: 0,
            name: "lowered".to_string(),
            reg: output,
        }];
        let mut instructions = vec![
            Instruction {
                kind: InstructionKind::Literal {
                    dst: literal,
                    value: ScalarValue::Utf8("ABC".to_string()),
                },
                span: (0..0).into(),
                error_mask: None,
            },
            Instruction {
                kind: InstructionKind::Builtin {
                    dst: lowered,
                    lowering: BuiltinLowering::Lower,
                    inputs: vec![literal],
                },
                span: (0..0).into(),
                error_mask: None,
            },
            Instruction {
                kind: InstructionKind::Move {
                    dst: output,
                    input: lowered,
                },
                span: (0..0).into(),
                error_mask: None,
            },
            Instruction {
                kind: InstructionKind::Literal {
                    dst: dead,
                    value: ScalarValue::Utf8("unused".to_string()),
                },
                span: (0..0).into(),
                error_mask: None,
            },
        ];

        optimize_instructions(&mut instructions, &outputs, None, &[]);

        assert_eq!(instructions.len(), 2);
        assert!(
            instructions
                .iter()
                .all(|instruction| { !matches!(instruction.kind, InstructionKind::Move { .. }) })
        );
        assert!(instructions.iter().any(|instruction| {
            matches!(
                instruction.kind,
                InstructionKind::Builtin {
                    dst,
                    lowering: BuiltinLowering::Lower,
                    ..
                } if dst == output
            )
        }));
        assert!(
            instructions
                .iter()
                .all(|instruction| { instruction_output(&instruction.kind) != dead })
        );
    }

    #[test]
    fn keeps_dead_erroring_temps_to_preserve_side_effects() {
        let lhs = RegisterRef::new(RegisterSpace::Input, RegisterType::Int64, 0);
        let rhs = RegisterRef::new(RegisterSpace::Input, RegisterType::Int64, 1);
        let dead = RegisterRef::new(RegisterSpace::Temp, RegisterType::Int64, 0);
        let mut instructions = vec![Instruction {
            kind: InstructionKind::Binary {
                dst: dead,
                left: lhs,
                right: rhs,
                op: BinaryOp::Div,
            },
            span: (0..0).into(),
            error_mask: None,
        }];

        optimize_instructions(&mut instructions, &[], None, &[]);

        assert_eq!(instructions.len(), 1);
        assert!(matches!(
            instructions[0].kind,
            InstructionKind::Binary {
                dst,
                op: BinaryOp::Div,
                ..
            } if dst == dead
        ));
    }

    #[test]
    fn temp_remapping_keeps_error_mask_live_through_boolean_output() {
        let mask = RegisterRef::new(RegisterSpace::Temp, RegisterType::Boolean, 0);
        let output = RegisterRef::new(RegisterSpace::Temp, RegisterType::Boolean, 1);
        let text = RegisterRef::new(RegisterSpace::Input, RegisterType::Utf8, 0);
        let pattern = RegisterRef::new(RegisterSpace::Input, RegisterType::Utf8, 1);
        let mut instructions = vec![
            Instruction {
                kind: InstructionKind::Literal {
                    dst: mask,
                    value: ScalarValue::Boolean(true),
                },
                span: (0..0).into(),
                error_mask: None,
            },
            Instruction {
                kind: InstructionKind::Builtin {
                    dst: output,
                    lowering: BuiltinLowering::RegexpLike,
                    inputs: vec![text, pattern],
                },
                span: (0..0).into(),
                error_mask: Some(mask),
            },
        ];
        let mut layout = crate::ir::RegisterLayout {
            boolean: 2,
            ..crate::ir::RegisterLayout::default()
        };

        remap_temp_registers(&mut instructions, &mut layout);

        let Instruction {
            kind: InstructionKind::Builtin { dst, .. },
            error_mask: Some(error_mask),
            ..
        } = &instructions[1]
        else {
            panic!("masked Boolean instruction must remain");
        };
        assert_ne!(*dst, *error_mask);
    }

    #[test]
    fn compiles_supported_unary_operations() {
        let program = parse_program("SET input.neg = -input.amount, input.inv = NOT input.active;")
            .expect("must parse");
        let schema = schema(vec![
            Field::new("amount", DataType::Int64, true),
            Field::new("active", DataType::Boolean, true),
        ]);

        let compiled = compile_program_with_output_fields(
            &program,
            schema,
            vec![
                Field::new("neg", DataType::Int64, true),
                Field::new("inv", DataType::Boolean, true),
            ],
        )
        .expect("must compile");

        assert!(compiled.instructions.iter().any(|instruction| {
            matches!(
                instruction.kind,
                InstructionKind::Unary {
                    op: UnaryOp::Neg,
                    ..
                }
            )
        }));
        assert!(compiled.instructions.iter().any(|instruction| {
            matches!(
                instruction.kind,
                InstructionKind::Unary {
                    op: UnaryOp::Not,
                    ..
                }
            )
        }));
    }

    #[test]
    fn rejects_unsupported_cast_source_type() {
        let program =
            parse_program("SET input.created = input.created AS INT64;").expect("must parse");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "created",
            DataType::Date32,
            true,
        )]));

        let error = compile_program(&program, schema).expect_err("must fail");
        assert_eq!(error.code, "unsupported_cast");
    }

    #[test]
    fn rejects_non_boolean_filter() {
        let program = parse_program("SET input.value = input.amount WHERE input.amount;")
            .expect("must parse");
        let schema = schema(vec![Field::new("amount", DataType::Int64, true)]);

        let error = compile_program_with_output_fields(
            &program,
            schema,
            vec![Field::new("value", DataType::Int64, true)],
        )
        .expect_err("must fail");
        assert_eq!(error.code, "invalid_filter");
    }

    #[test]
    fn rejects_non_utf8_builtin_argument() {
        let program = parse_program("SET input.bad = upper(input.amount);").expect("must parse");
        let schema = schema(vec![Field::new("amount", DataType::Int64, true)]);

        let error = compile_program_with_output_fields(
            &program,
            schema,
            vec![Field::new("bad", DataType::Utf8, true)],
        )
        .expect_err("must fail");
        assert_eq!(error.code, "unsupported_function");
        assert!(error.message.contains("requires Utf8 input"));
    }

    #[test]
    fn rejects_invalid_extended_builtin_arguments() {
        let mixed = parse_program("SET input.bad = coalesce(input.amount, input.name);")
            .expect("must parse");
        let mixed_schema = schema(vec![
            Field::new("amount", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        let mixed_error = compile_program_with_output_fields(
            &mixed,
            mixed_schema,
            vec![Field::new("bad", DataType::Utf8, true)],
        )
        .expect_err("must fail");
        assert_eq!(mixed_error.code, "type_mismatch");

        let abs = parse_program("SET input.bad = abs(input.name);").expect("must parse");
        let abs_schema = schema(vec![Field::new("name", DataType::Utf8, true)]);
        let abs_error = compile_program_with_output_fields(
            &abs,
            abs_schema,
            vec![Field::new("bad", DataType::Int64, true)],
        )
        .expect_err("must fail");
        assert_eq!(abs_error.code, "unsupported_function");
        assert!(abs_error.message.contains("requires numeric input"));

        let unknown = parse_program("SET input.bad = mystery(input.name);").expect("must parse");
        let unknown_schema = schema(vec![Field::new("name", DataType::Utf8, true)]);
        let unknown_error = compile_program_with_output_fields(
            &unknown,
            unknown_schema,
            vec![Field::new("bad", DataType::Utf8, true)],
        )
        .expect_err("must fail");
        assert_eq!(unknown_error.code, "unknown_function");
        assert!(unknown_error.message.contains("mystery"));
    }

    #[test]
    fn rejects_invalid_binary_operators_for_operand_type() {
        let bool_program =
            parse_program("SET input.bad = input.left > input.right;").expect("must parse");
        let bool_schema = schema(vec![
            Field::new("left", DataType::Boolean, true),
            Field::new("right", DataType::Boolean, true),
        ]);
        let bool_error = compile_program_with_output_fields(
            &bool_program,
            bool_schema,
            vec![Field::new("bad", DataType::Boolean, true)],
        )
        .expect_err("must fail");
        assert_eq!(bool_error.code, "unsupported_binary");

        let numeric_program =
            parse_program("SET input.bad = input.left AND input.right;").expect("must parse");
        let numeric_schema = schema(vec![
            Field::new("left", DataType::Int64, true),
            Field::new("right", DataType::Int64, true),
        ]);
        let numeric_error = compile_program_with_output_fields(
            &numeric_program,
            numeric_schema,
            vec![Field::new("bad", DataType::Boolean, true)],
        )
        .expect_err("must fail");
        assert_eq!(numeric_error.code, "unsupported_binary");
    }
}
