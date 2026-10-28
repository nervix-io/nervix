//! The programs one emitter task compiles when it starts and evaluates for every batch it admits.
//!
//! Layer: data plane.
//! - **Owns.** Compiling an emitter's route, HTTP request fields, ordering group, envelope and
//!   source filters once per task, against the task's input and output schemas and its compile
//!   context, and which part of the emitter a program that does not compile belongs to.
//! - **Depends on.** The emitter's execution and start plans, its codec, and the VM compile API.
//! - **Must not know.** How the task evaluates the programs for each batch, when it publishes, or
//!   which connector it publishes through.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;

use super::*;

/// The programs one emitter task compiles when it starts, which its batch context evaluates for
/// every batch the task admits.
pub(super) struct EmitterTaskPrograms {
    pub(super) filter_map: Option<CompiledEmitterFilterMapProgram>,
    /// Absent for every emitter but an HTTP request emitter.
    pub(super) http_requests: Option<CompiledHttpRequestFields>,
    pub(super) ordering_group: Option<CompiledOrderingGroup>,
    /// Absent where the emitter writes no envelope.
    pub(super) envelope: Option<CompiledEnvelope>,
    /// The `WHERE` program of each input that declares one.
    pub(super) source_filters: HashMap<RelayName, CompiledProgramWithMaterializedInterest>,
}

impl EmitterTaskPrograms {
    /// Compiles every program `emitter` evaluates for its batches. A program that does not compile
    /// fails the start, naming the part of the emitter it belongs to.
    pub(super) fn compile(
        emitter: &EmitterExecutionPlan,
        plan: &EmitterStartPlan,
        codec: Option<&Arc<CompiledCodec>>,
        input_schema: &CompiledSchema,
        output_schema: &CompiledSchema,
        compile_context: RuntimeVmCompileContext<'_>,
    ) -> error_stack::Result<Self, EmitterStartError> {
        let filter_map = compile_emitter_filter_map_program(
            &emitter.name,
            emitter.route.as_ref(),
            RuntimeVmSchemaPair {
                input: input_schema.arrow_schema(),
                input_sensitivity: input_schema.vm_sensitivity(),
                output: output_schema.arrow_schema(),
                output_sensitivity: output_schema.vm_sensitivity(),
            },
            compile_context,
        )
        .change_context(EmitterStartError::Route)?;
        let http_requests = match &plan.connector {
            EmitterConnectorPlan::Http(HttpEmitterPlan::Request(connector)) => {
                let output = codec.map(|codec| RuntimeVmSchema {
                    schema: codec.schema().arrow_schema(),
                    sensitivity: codec.schema().vm_sensitivity(),
                });
                let compiled = CompiledHttpRequestFields::compile(
                    &emitter.name,
                    connector,
                    emitter
                        .http_request
                        .as_ref()
                        .verified("the decision gives every HTTP emitter its request program"),
                    HttpRequestSchemas {
                        input: RuntimeVmSchema {
                            schema: input_schema.arrow_schema(),
                            sensitivity: input_schema.vm_sensitivity(),
                        },
                        output,
                    },
                    compile_context,
                )
                .change_context(EmitterStartError::HttpRequests)?;
                Some(compiled)
            }
            _ => None,
        };
        let ordering_group = match emitter.ordering_group.as_ref() {
            None => None,
            Some(declared) => Some(
                CompiledOrderingGroup::compile(
                    declared,
                    &emitter.name,
                    RuntimeVmSchema {
                        schema: input_schema.arrow_schema(),
                        sensitivity: input_schema.vm_sensitivity(),
                    },
                    compile_context,
                )
                .change_context(EmitterStartError::OrderingGroup)?,
            ),
        };
        let envelope = CompiledEnvelope::for_emitter(
            &emitter.name,
            emitter.envelope.as_ref(),
            input_schema,
            output_schema,
            compile_context,
        )
        .change_context(EmitterStartError::Envelope)?;
        let mut source_filters = HashMap::default();
        for input in &emitter.inputs {
            let Some(source_filter) = input.from_where.as_ref() else {
                continue;
            };
            let program = bind_scoped_filter_program(
                &ModelName::from(&emitter.name),
                source_filter.program(),
                RuntimeVmSchema {
                    schema: input_schema.arrow_schema(),
                    sensitivity: input_schema.vm_sensitivity(),
                },
                MessageErrorOperation::SourceWhere,
                compile_context,
                RuntimeFilterScope::Source {
                    namespace: "input",
                    allow_header_reads: false,
                    allow_metadata: false,
                },
            )
            .change_context_lazy(|| EmitterStartError::SourceFilter {
                relay: input.relay.clone(),
            })?;
            source_filters.insert(input.relay.clone(), program);
        }
        Ok(Self {
            filter_map,
            http_requests,
            ordering_group,
            envelope,
            source_filters,
        })
    }
}
