//! Compiling one window route's aggregate program against its input and output schemas.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The argument program that evaluates every per-row aggregate argument of a route over
//!   one input batch, the output programs that turn aggregate results into the route's fields, and
//!   the exact type every aggregate invocation yields.
//! - **Depends on.** The window lowering and the VM compiler.
//! - **Must not know.** Relays, branches, accumulator state, or when a window emits.

use std::{collections::BTreeMap, sync::Arc as StdArc};

use arrow_schema::{DataType, Field, Schema};
use error_stack::{Report, ResultExt as _};
use sorted_vec::SortedSet;
use thiserror::Error;

use super::{
    WindowAggregateExpr, WindowAggregateProgram, WindowAggregateStorageKind, WindowArguments,
    WindowLinearHistogramConfig,
};
use crate::{
    CompileBinding, CompileOptions, OutputMode, SchemaSensitivity,
    compiler::{
        compile_program_with_options_for_bindings_with_sensitivity,
        infer_set_expr_types_for_bindings_with_udfs,
    },
    ir::{CompiledProgram, InstructionKind},
    program::{
        FieldRef, FunctionName, Program, Span, SpannedExpr, SpannedNode, WindowAggregateFunction,
        WindowAggregateInvocation,
    },
};

/// The namespace the argument program writes every per-row aggregate argument into.
pub const WINDOW_ARGUMENT_NAMESPACE: &str = "window_argument";

/// The namespace aggregate arguments read the window's input rows from.
const INPUT_NAMESPACE: &str = "input";

/// One per-row aggregate argument as the argument program produces it.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowArgumentColumn {
    /// The argument program output field holding the argument.
    pub field: String,
    /// The exact type every input batch evaluates the argument to.
    pub data_type: DataType,
}

/// The schemas and namespaces one window route compiles against.
#[derive(Debug, Clone, Copy)]
pub struct WindowRouteSchemas<'a> {
    /// The schema of the input relay every aggregate argument reads.
    pub input: &'a StdArc<Schema>,
    /// Which input fields are sensitive.
    pub input_sensitivity: &'a SchemaSensitivity,
    /// Namespaces beyond `input` that the route's assignments may read.
    pub readable: &'a [CompileBinding],
    /// The schema of the route's output relay.
    pub output: &'a StdArc<Schema>,
    /// Which output fields are sensitive.
    pub output_sensitivity: &'a SchemaSensitivity,
}

/// One window route's aggregate program, compiled.
#[derive(Debug, Clone)]
pub struct CompiledWindowRoute {
    /// Evaluates every per-row argument of the route's demands over one input batch.
    pub argument_program: triomphe::Arc<CompiledProgram>,
    /// Every demand of the route, in demand order, with the argument columns it reads.
    pub demands: Vec<CompiledWindowDemand>,
    /// The compiled value of every assigned output field, in assignment order.
    pub assignments: Vec<CompiledWindowAssignment>,
    /// Every aggregate invocation the assignments inject, each with the exact type it yields.
    pub invocations: Vec<CompiledWindowInvocation>,
}

/// One aggregate demand of a route, with the argument program columns it reads.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledWindowDemand {
    pub storage: WindowAggregateStorageKind,
    /// Every function the demand's structure answers.
    pub functions: SortedSet<WindowAggregateFunction>,
    pub arguments: WindowArguments<WindowArgumentColumn>,
    pub linear_histogram: Option<WindowLinearHistogramConfig>,
}

#[derive(Debug, Clone)]
pub struct CompiledWindowAssignment {
    /// The output field the value is assigned to.
    pub field: String,
    pub value: CompiledWindowExpr,
}

#[derive(Debug, Clone)]
pub enum CompiledWindowExpr {
    /// A one-row program over the injected aggregate results.
    Scalar(triomphe::Arc<CompiledProgram>),
    /// An array literal whose items are compiled one by one.
    Array {
        items: Vec<CompiledWindowExpr>,
        /// Whether the target is a fixed-size `ARRAY` rather than a `VEC`.
        fixed_size: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompiledWindowInvocation {
    pub invocation: WindowAggregateInvocation,
    /// The exact type the invocation injects into its expression.
    pub output_type: DataType,
}

/// Every way compiling a window route fails.
#[derive(Debug, Error)]
pub enum WindowRouteCompileError {
    #[error("window aggregate arguments failed type inference")]
    ArgumentInference,
    #[error("window aggregate arguments failed to compile")]
    ArgumentCompile,
    #[error("window aggregate argument program inferred no '{field}' argument")]
    ArgumentColumn { field: String },
    #[error("window aggregate output schema is missing field '{field}'")]
    OutputField { field: String },
    #[error("window aggregate SET field '{field}' failed to compile")]
    Assignment { field: String },
    #[error("window aggregate array cannot be assigned to {data_type:?} field '{field}'")]
    ArrayTarget { data_type: DataType, field: String },
    #[error("compiled window aggregate references unknown demand {demand}")]
    UnknownDemand { demand: usize },
    #[error(
        "{} yields {output:?} but its argument evaluates to {argument:?}",
        .function.nspl_name()
    )]
    InvocationType {
        function: WindowAggregateFunction,
        output: DataType,
        argument: DataType,
    },
}

impl WindowAggregateFunction {
    /// Whether the function returns a value of the argument it reads, rather than a statistic
    /// computed from it. `ARG_MIN` and `ARG_MAX` return their first argument.
    const fn returns_argument_value(self) -> bool {
        match self {
            Self::ArgMax
            | Self::ArgMin
            | Self::First
            | Self::Last
            | Self::Max
            | Self::Min
            | Self::Sum => true,
            Self::Avg
            | Self::BoolAnd
            | Self::BoolOr
            | Self::Corr
            | Self::Count
            | Self::CountIf
            | Self::CovarPop
            | Self::CovarSamp
            | Self::PercentileLinearHistogram
            | Self::StddevPop
            | Self::StddevSamp
            | Self::VarPop
            | Self::VarSamp => false,
        }
    }
}

impl CompiledWindowRoute {
    /// Compile `program` for a route reading `schemas.input` and writing `schemas.output`.
    pub fn compile(
        program: &WindowAggregateProgram,
        schemas: WindowRouteSchemas<'_>,
        options: &CompileOptions,
    ) -> Result<Self, Report<WindowRouteCompileError>> {
        let CompiledArguments {
            program: argument_program,
            demands,
        } = CompiledArguments::compile(program, schemas.input, options)?;
        let mut bindings = vec![
            CompileBinding::readonly(INPUT_NAMESPACE, StdArc::clone(schemas.input))
                .with_sensitivity(schemas.input_sensitivity.clone()),
        ];
        bindings.extend(schemas.readable.iter().cloned());
        let mut assignments = Vec::with_capacity(program.assignments.len());
        for assignment in &program.assignments {
            let target_field = schemas
                .output
                .field_with_name(&assignment.target.field)
                .map_err(|_| {
                    Report::new(WindowRouteCompileError::OutputField {
                        field: assignment.target.field.clone(),
                    })
                })?;
            let target = AssignmentTarget {
                reference: &assignment.target,
                data_type: target_field.data_type(),
                nullable: target_field.is_nullable(),
                sensitive: schemas
                    .output_sensitivity
                    .is_sensitive(&assignment.target.field),
            };
            let value = Self::compile_expr(&assignment.value.inner, target, &bindings, options)?;
            assignments.push(CompiledWindowAssignment {
                field: assignment.target.field.clone(),
                value,
            });
        }
        let mut invocations = BTreeMap::new();
        for assignment in &assignments {
            assignment.value.collect_invocations(&mut invocations);
        }
        let mut compiled_invocations = Vec::with_capacity(invocations.len());
        for (invocation, output_type) in invocations {
            let demand = demands.get(invocation.demand_id).ok_or_else(|| {
                Report::new(WindowRouteCompileError::UnknownDemand {
                    demand: invocation.demand_id,
                })
            })?;
            let argument_type = &demand.arguments.first().data_type;
            if invocation.function.returns_argument_value() && &output_type != argument_type {
                return Err(Report::new(WindowRouteCompileError::InvocationType {
                    function: invocation.function,
                    output: output_type,
                    argument: argument_type.clone(),
                }));
            }
            compiled_invocations.push(CompiledWindowInvocation {
                invocation,
                output_type,
            });
        }
        Ok(Self {
            argument_program,
            demands,
            assignments,
            invocations: compiled_invocations,
        })
    }

    fn compile_expr(
        expr: &WindowAggregateExpr,
        target: AssignmentTarget<'_>,
        bindings: &[CompileBinding],
        options: &CompileOptions,
    ) -> Result<CompiledWindowExpr, Report<WindowRouteCompileError>> {
        match expr {
            WindowAggregateExpr::Scalar(expr) => {
                Self::compile_scalar(expr, target, bindings, options)
                    .map(CompiledWindowExpr::Scalar)
            }
            WindowAggregateExpr::Array(items) => {
                let (element, fixed_size) = match target.data_type {
                    DataType::FixedSizeList(element, _) => (element, true),
                    DataType::List(element) => (element, false),
                    other => {
                        return Err(Report::new(WindowRouteCompileError::ArrayTarget {
                            data_type: other.clone(),
                            field: target.reference.field.clone(),
                        }));
                    }
                };
                let element_target = AssignmentTarget {
                    reference: target.reference,
                    data_type: element.data_type(),
                    nullable: element.is_nullable(),
                    sensitive: target.sensitive,
                };
                let mut compiled_items = Vec::with_capacity(items.len());
                for item in items {
                    compiled_items.push(Self::compile_expr(
                        &item.inner,
                        element_target,
                        bindings,
                        options,
                    )?);
                }
                Ok(CompiledWindowExpr::Array {
                    items: compiled_items,
                    fixed_size,
                })
            }
        }
    }

    fn compile_scalar(
        expr: &SpannedExpr,
        target: AssignmentTarget<'_>,
        bindings: &[CompileBinding],
        options: &CompileOptions,
    ) -> Result<triomphe::Arc<CompiledProgram>, Report<WindowRouteCompileError>> {
        let output_schema = StdArc::new(Schema::new(vec![Field::new(
            target.reference.field.clone(),
            target.data_type.clone(),
            target.nullable,
        )]));
        let output_sensitivity = if target.sensitive {
            SchemaSensitivity::from_sensitive_fields([target.reference.field.clone()])
        } else {
            SchemaSensitivity::default()
        };
        let mut compile_bindings = bindings.to_vec();
        compile_bindings.push(CompileBinding::writeonly(
            target.reference.relay.clone(),
            StdArc::clone(&output_schema),
        ));
        let program = SpannedNode {
            inner: Program {
                filter: None,
                set: vec![(target.reference.clone(), expr.clone())],
                invoke: Vec::new(),
            },
            span: expr.span,
        };
        let compiled = compile_program_with_options_for_bindings_with_sensitivity(
            &program,
            output_schema,
            output_sensitivity,
            compile_bindings,
            explicit_output_options(options),
        )
        .change_context_lazy(|| WindowRouteCompileError::Assignment {
            field: target.reference.field.clone(),
        })?;
        Ok(triomphe::Arc::new(compiled))
    }
}

impl CompiledWindowExpr {
    /// Record every aggregate invocation this value injects, with the type it yields.
    fn collect_invocations(&self, invocations: &mut BTreeMap<WindowAggregateInvocation, DataType>) {
        match self {
            Self::Scalar(program) => {
                for instruction in &program.instructions {
                    if let InstructionKind::Inject {
                        function: FunctionName::WindowAggregate(invocation),
                        output_type,
                        ..
                    } = &instruction.kind
                    {
                        invocations
                            .entry(invocation.clone())
                            .or_insert_with(|| output_type.clone());
                    }
                }
            }
            Self::Array { items, .. } => {
                for item in items {
                    item.collect_invocations(invocations);
                }
            }
        }
    }
}

/// The program that evaluates every argument of a route's demands, and where each argument lands.
struct CompiledArguments {
    program: triomphe::Arc<CompiledProgram>,
    demands: Vec<CompiledWindowDemand>,
}

impl CompiledArguments {
    fn compile(
        program: &WindowAggregateProgram,
        input: &StdArc<Schema>,
        options: &CompileOptions,
    ) -> Result<Self, Report<WindowRouteCompileError>> {
        let span: Span = (0..0).into();
        let mut set = Vec::new();
        for demand in program.demands() {
            for (position, argument) in demand.arguments.iter().enumerate() {
                set.push((
                    FieldRef {
                        relay: WINDOW_ARGUMENT_NAMESPACE.to_string(),
                        field: argument_field_name(demand.id, position),
                    },
                    SpannedNode {
                        inner: argument.clone(),
                        span,
                    },
                ));
            }
        }
        let argument_program = SpannedNode {
            inner: Program {
                filter: None,
                set,
                invoke: Vec::new(),
            },
            span,
        };
        let inferred = infer_set_expr_types_for_bindings_with_udfs(
            &argument_program,
            [
                CompileBinding::writeonly(WINDOW_ARGUMENT_NAMESPACE, StdArc::new(Schema::empty())),
                CompileBinding::readonly(INPUT_NAMESPACE, StdArc::clone(input)),
            ],
            options.udf_signatures.clone(),
        )
        .change_context(WindowRouteCompileError::ArgumentInference)?;
        let mut types = BTreeMap::new();
        let mut fields = Vec::with_capacity(inferred.len());
        for inferred in inferred {
            fields.push(Field::new(
                inferred.field.clone(),
                inferred.data_type.clone(),
                inferred.nullable,
            ));
            types.insert(inferred.field, inferred.data_type);
        }
        let mut demands = Vec::with_capacity(program.demands().len());
        for demand in program.demands() {
            let first = argument_column(&types, demand.id, 0)?;
            let arguments = match &demand.arguments {
                WindowArguments::Single(_) => WindowArguments::Single(first),
                WindowArguments::Pair { .. } => WindowArguments::Pair {
                    first,
                    second: argument_column(&types, demand.id, 1)?,
                },
            };
            demands.push(CompiledWindowDemand {
                storage: demand.storage,
                functions: demand.functions.clone(),
                arguments,
                linear_histogram: demand.linear_histogram.clone(),
            });
        }
        let output_schema = StdArc::new(Schema::new(fields));
        let compiled = compile_program_with_options_for_bindings_with_sensitivity(
            &argument_program,
            StdArc::clone(&output_schema),
            SchemaSensitivity::default(),
            [
                CompileBinding::writeonly(WINDOW_ARGUMENT_NAMESPACE, output_schema),
                CompileBinding::readonly(INPUT_NAMESPACE, StdArc::clone(input)),
            ],
            explicit_output_options(options),
        )
        .change_context(WindowRouteCompileError::ArgumentCompile)?;
        Ok(Self {
            program: triomphe::Arc::new(compiled),
            demands,
        })
    }
}

/// The argument program column of one demand argument, `position` counting from zero.
fn argument_column(
    types: &BTreeMap<String, DataType>,
    demand: usize,
    position: usize,
) -> Result<WindowArgumentColumn, Report<WindowRouteCompileError>> {
    let field = argument_field_name(demand, position);
    let Some(data_type) = types.get(&field) else {
        return Err(Report::new(WindowRouteCompileError::ArgumentColumn {
            field,
        }));
    };
    Ok(WindowArgumentColumn {
        field,
        data_type: data_type.clone(),
    })
}

/// The output field one window value is compiled into.
#[derive(Debug, Clone, Copy)]
struct AssignmentTarget<'a> {
    reference: &'a FieldRef,
    data_type: &'a DataType,
    nullable: bool,
    sensitive: bool,
}

/// The argument program field of one demand argument, `position` counting from zero.
fn argument_field_name(demand: usize, position: usize) -> String {
    format!("demand_{demand}_{position}")
}

fn explicit_output_options(options: &CompileOptions) -> CompileOptions {
    CompileOptions {
        output_mode: OutputMode::ExplicitOnly,
        ..options.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::window::lower_window_assignments;

    fn schema(fields: &[(&str, DataType, bool)]) -> StdArc<Schema> {
        StdArc::new(Schema::new(
            fields
                .iter()
                .map(|(name, data_type, nullable)| Field::new(*name, data_type.clone(), *nullable))
                .collect::<Vec<_>>(),
        ))
    }

    fn compile(
        assignments: &str,
        input: &StdArc<Schema>,
        output: &StdArc<Schema>,
    ) -> Result<CompiledWindowRoute, Report<WindowRouteCompileError>> {
        let construction = nervix_nspl::parse_route_construction(&format!("SET {assignments}"))
            .expect("every route construction in these tests is valid NSPL");
        let program = lower_window_assignments(&construction)
            .expect("every route construction in these tests lowers");
        CompiledWindowRoute::compile(
            &program.inner,
            WindowRouteSchemas {
                input,
                input_sensitivity: &SchemaSensitivity::default(),
                readable: &[],
                output,
                output_sensitivity: &SchemaSensitivity::default(),
            },
            &CompileOptions::default(),
        )
    }

    fn failure(result: Result<CompiledWindowRoute, Report<WindowRouteCompileError>>) -> String {
        let Err(error) = result else {
            panic!("the route must fail to compile");
        };
        format!("{error:#}")
    }

    fn readings() -> StdArc<Schema> {
        schema(&[
            ("sensor", DataType::Utf8, false),
            ("value", DataType::Int64, false),
            ("optional_value", DataType::Int64, true),
            ("load", DataType::Float64, false),
            ("healthy", DataType::Boolean, false),
            (
                "tags",
                DataType::List(StdArc::new(Field::new("item", DataType::Utf8, false))),
                false,
            ),
        ])
    }

    #[test]
    fn statistics_compile_with_their_result_types_and_shared_arguments() {
        let output = schema(&[
            ("mean_value", DataType::Float64, false),
            ("spread", DataType::Float64, true),
            ("correlation", DataType::Float64, true),
            ("healthy_samples", DataType::Int64, false),
            ("all_healthy", DataType::Boolean, false),
            ("lowest_sensor", DataType::Utf8, false),
        ]);
        let route = compile(
            "mean_value = AVG(input.value), spread = VAR_SAMP(input.value), correlation = \
             CORR(input.value, input.load), healthy_samples = COUNT_IF(input.healthy), \
             all_healthy = BOOL_AND(input.healthy), lowest_sensor = ARG_MIN(input.sensor, \
             input.value)",
            &readings(),
            &output,
        )
        .expect("well-typed statistics must compile");

        assert_eq!(route.demands.len(), 4);
        assert_eq!(
            route.demands[1].arguments,
            WindowArguments::Pair {
                first: WindowArgumentColumn {
                    field: "demand_1_0".to_string(),
                    data_type: DataType::Int64,
                },
                second: WindowArgumentColumn {
                    field: "demand_1_1".to_string(),
                    data_type: DataType::Float64,
                },
            }
        );
        let types = route
            .invocations
            .iter()
            .map(|compiled| (compiled.invocation.function, compiled.output_type.clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            types,
            vec![
                (WindowAggregateFunction::Avg, DataType::Float64),
                (WindowAggregateFunction::VarSamp, DataType::Float64),
                (WindowAggregateFunction::Corr, DataType::Float64),
                (WindowAggregateFunction::BoolAnd, DataType::Boolean),
                (WindowAggregateFunction::CountIf, DataType::Int64),
                (WindowAggregateFunction::ArgMin, DataType::Utf8),
            ]
        );
    }

    #[test]
    fn statistics_that_can_be_undefined_require_optional_outputs() {
        let required = schema(&[("spread", DataType::Float64, false)]);
        for assignment in [
            "spread = VAR_SAMP(input.value)",
            "spread = STDDEV_SAMP(input.value)",
            "spread = COVAR_SAMP(input.value, input.load)",
            "spread = CORR(input.value, input.load)",
            "spread = AVG(input.optional_value)",
        ] {
            let message = failure(compile(assignment, &readings(), &required));
            assert!(
                message.contains("SET field 'spread' may be null but the output field is required"),
                "{assignment}: {message}"
            );
        }
        for assignment in [
            "spread = VAR_POP(input.value)",
            "spread = STDDEV_POP(input.value)",
            "spread = COVAR_POP(input.value, input.load)",
            "spread = AVG(input.value)",
            "spread = COALESCE(CORR(input.value, input.load), 0.0)",
        ] {
            compile(assignment, &readings(), &required)
                .unwrap_or_else(|error| panic!("{assignment} must compile: {error:#}"));
        }
        let counts = schema(&[("samples", DataType::Int64, false)]);
        for assignment in [
            "samples = COUNT(input.optional_value)",
            "samples = COUNT_IF(input.healthy)",
        ] {
            compile(assignment, &readings(), &counts)
                .unwrap_or_else(|error| panic!("{assignment} must compile: {error:#}"));
        }
        let optional_sum = schema(&[("samples", DataType::Int64, false)]);
        let message = failure(compile(
            "samples = SUM(input.optional_value)",
            &readings(),
            &optional_sum,
        ));
        assert!(message.contains("may be null but the output field is required"));
    }

    #[test]
    fn statistics_reject_arguments_of_the_wrong_type() {
        let float = schema(&[("statistic", DataType::Float64, true)]);
        let count = schema(&[("statistic", DataType::Int64, true)]);
        let text = schema(&[("statistic", DataType::Utf8, true)]);
        for (assignment, output, expected) in [
            (
                "statistic = AVG(input.sensor)",
                &float,
                "function 'AVG' requires a numeric argument",
            ),
            (
                "statistic = CORR(input.value, input.sensor)",
                &float,
                "function 'CORR' requires a numeric argument",
            ),
            (
                "statistic = COUNT_IF(input.value)",
                &count,
                "function 'COUNT_IF' requires a BOOL argument",
            ),
            (
                "statistic = ARG_MAX(input.sensor, input.tags)",
                &text,
                "function 'ARG_MAX' requires an orderable key",
            ),
            (
                "statistic = MIN(input.tags)",
                &text,
                "function 'MIN' requires an orderable argument",
            ),
        ] {
            let message = failure(compile(assignment, &readings(), output));
            assert!(message.contains(expected), "{assignment}: {message}");
        }
    }

    #[test]
    fn array_items_follow_their_element_nullability() {
        let output = schema(&[(
            "spreads",
            DataType::FixedSizeList(StdArc::new(Field::new("item", DataType::Float64, false)), 2),
            false,
        )]);
        let message = failure(compile(
            "spreads = [VAR_POP(input.value), VAR_SAMP(input.value)]",
            &readings(),
            &output,
        ));
        assert!(message.contains("may be null but the output field is required"));
        let scalar = schema(&[("spreads", DataType::Float64, false)]);
        let message = failure(compile(
            "spreads = [VAR_POP(input.value), VAR_POP(input.value)]",
            &readings(),
            &scalar,
        ));
        assert!(message.contains("window aggregate array cannot be assigned to Float64"));
    }
}
