//! The envelope the emitter host evaluates for every record it publishes.
//!
//! Layer: data plane.
//! - **Owns.** Compiling the values an emitter's envelope computes once, as one program over each
//!   published record, its source record and the batch's materialized state; evaluating that
//!   program over each batch the emitter admits, after its route kept and finalized the rows;
//!   carrying each row's envelope, or the reason it has none, beside the batch, held once for the
//!   payloads that publish the row to share; and the bytes they hold.
//! - **Depends on.** The emitter's envelope plan, the VM's compile and execute API, the connector
//!   contract's record envelope, and the relay batches the program runs over.
//! - **Must not know.** Which connector reads an envelope, what its transport does with it, or
//!   when the emitter publishes.
//!
//! A record emitter receives each record's envelope already evaluated, so the expressions that
//! produced it, and the reason a record has none, both stay with the host.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use error_stack::ResultExt as _;
use nervix_connector::{
    PropertyValue, RecordDestination, RecordEnvelope, RecordKey, RecordProperty,
};
use nervix_models::{EnvelopeClauseKind, EnvelopeValueType, PropertyType, Unit};
use nervix_vm::infer_set_expr_types_for_bindings_with_udfs;

use super::*;
use crate::emitter_execution_plan::{
    ENVELOPE_NAMESPACE, EmitterEnvelopePlan, EnvelopeSchemaPlan, EnvelopeTarget,
};

/// Why an emitter's envelope cannot be compiled, or why a batch's evaluated envelope cannot be
/// read, which fails the whole batch rather than one record.
#[derive(Debug, Error)]
pub(in crate::runtime) enum EmitterEnvelopeError {
    #[error("emitter '{emitter}' envelope cannot bind the materialized state it reads")]
    MaterializedState { emitter: EmitterName },
    #[error("emitter '{emitter}' envelope cannot bind the lookups it calls")]
    Lookups { emitter: EmitterName },
    #[error("emitter '{emitter}' envelope does not compile")]
    Compile { emitter: EmitterName },
    #[error("the envelope program did not evaluate {expected} for {site}")]
    ValueColumn {
        site: EnvelopeSite,
        expected: &'static str,
    },
    #[error("failed to select the source records of the published rows")]
    SelectSourceRecords,
    #[error("source relay '{relay}' has no envelope program")]
    MissingSource { relay: RelayName },
    #[error("cannot resolve the envelope source partition")]
    SourcePartition,
}

/// Where an envelope computes a value, named as a statement writes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) enum EnvelopeSite {
    /// The routing key the emitter's destination computes beside its envelope.
    RoutingKey,
    Clause(EnvelopeClauseKind),
    Property(&'static str),
}

impl From<EnvelopeTarget> for EnvelopeSite {
    fn from(target: EnvelopeTarget) -> Self {
        match target {
            EnvelopeTarget::RoutingKey => Self::RoutingKey,
            EnvelopeTarget::Clause(kind) => Self::Clause(kind),
            EnvelopeTarget::Property(declaration) => Self::Property(declaration.name),
        }
    }
}

/// A site is written as its statement writes it: `envelope TIMESTAMP`, `envelope PROPERTIES key
/// 'priority'`, or the destination's `ROUTING KEY`.
impl std::fmt::Display for EnvelopeSite {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RoutingKey => formatter.write_str("ROUTING KEY"),
            Self::Clause(kind) => write!(formatter, "envelope {}", kind.keywords()),
            Self::Property(name) => write!(formatter, "envelope PROPERTIES key '{name}'"),
        }
    }
}

/// Why one row has no envelope. The emitter rejects that row when it publishes it.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(in crate::runtime) enum EnvelopeError {
    #[error("{site} expression failed with {} at {span}", .code.as_str())]
    ExpressionFailed {
        site: EnvelopeSite,
        code: nervix_vm::ErrorCode,
        span: VmSpan,
    },
    #[error("{site} expression produced NULL")]
    Null { site: EnvelopeSite },
    #[error("envelope expressions omitted their input row")]
    OmittedInputRow,
}

/// The schemas an envelope program reads.
pub(in crate::runtime) struct EnvelopeSchemas {
    /// The source record, which envelope expressions read as `input`.
    pub(in crate::runtime) input: RuntimeVmSchema,
    /// The output the emitter's route constructs, which envelope expressions read as `output` and
    /// `message`. Absent where the emitter publishes its source records, whose `message` is the
    /// source record.
    pub(in crate::runtime) output: Option<RuntimeVmSchema>,
}

/// Where the envelope of the rows a batch publishes reads `input` from.
pub(in crate::runtime) enum EnvelopeInput<'a> {
    /// The emitter publishes its source records, so every published row is its own input.
    Published,
    /// The emitter's route constructed the published rows from these source rows, one for each
    /// published row in order.
    Source {
        records: &'a SourceRecords,
        rows: &'a [usize],
    },
}

/// One emitter's envelope, compiled once for its task.
pub(in crate::runtime) struct CompiledEnvelope {
    /// What every record carries whatever it holds: the flag and duration clauses. Every row of
    /// every batch shares it where the envelope computes no value.
    fixed: Arc<RecordEnvelope>,
    /// The values each record computes, absent where the envelope computes none.
    computed: Option<EnvelopePrograms>,
}

struct CompiledEnvelopeValues {
    program: CompiledProgramWithMaterializedInterest,
    /// What the value of each field the program writes sets, in the order it writes them.
    targets: Box<[EnvelopeTarget]>,
    /// The position in `targets` of the value whose evaluation reports each VM span.
    positions: BTreeMap<VmSpan, usize>,
}

/// The common program stays inline; branch-dependent programs are selected by their source relay.
/// Boxed slices retain the exact-capacity target allocation while leaving room for this dispatch.
enum EnvelopePrograms {
    Common(CompiledEnvelopeValues),
    Sources(Box<BTreeMap<RelayName, SourceEnvelope>>),
}

struct SourceEnvelope {
    computed: Option<CompiledEnvelopeValues>,
    partitioning: Option<RelayPartitioning>,
}

impl CompiledEnvelope {
    /// The envelope `plan` describes for the emitter `emitter`, compiled against the relay schema
    /// its rows arrive with and, where its route constructs the records it publishes, the schema of
    /// those records; absent where the emitter writes none.
    pub(in crate::runtime) fn for_emitter(
        emitter: &EmitterName,
        plan: Option<&EmitterEnvelopePlan>,
        input: &CompiledSchema,
        output: &CompiledSchema,
        context: RuntimeVmCompileContext<'_>,
    ) -> error_stack::Result<Option<Self>, EmitterEnvelopeError> {
        let Some(plan) = plan else {
            return Ok(None);
        };
        let reads_output = match &plan.computed {
            Some(computed) => computed.reads_output,
            None => false,
        };
        let output = reads_output.then(|| RuntimeVmSchema {
            schema: output.arrow_schema(),
            sensitivity: output.vm_sensitivity(),
        });
        let schemas = EnvelopeSchemas {
            input: RuntimeVmSchema {
                schema: input.arrow_schema(),
                sensitivity: input.vm_sensitivity(),
            },
            output,
        };
        if plan.sources.is_empty() {
            return Self::compile(emitter, plan, schemas, context).map(Some);
        }
        let mut sources = BTreeMap::new();
        for (relay, branching) in &plan.sources {
            let compiled = Self::compile(
                emitter,
                plan,
                EnvelopeSchemas {
                    input: schemas.input.clone(),
                    output: schemas.output.clone(),
                },
                RuntimeVmCompileContext {
                    current_branching: branching,
                    ..context
                },
            )?;
            let computed = match compiled.computed {
                Some(EnvelopePrograms::Common(values)) => Some(values),
                None => None,
                Some(EnvelopePrograms::Sources(_)) => unreachable!("compile binds one source"),
            };
            sources.insert(
                relay.clone(),
                SourceEnvelope {
                    computed,
                    partitioning: plan
                        .partition_from_branch
                        .then(|| RelayPartitioning::of(Some(branching))),
                },
            );
        }
        Ok(Some(Self {
            fixed: Arc::new(plan.fixed.clone()),
            computed: Some(EnvelopePrograms::Sources(Box::new(sources))),
        }))
    }

    /// Compiles the envelope `plan` describes for the emitter `emitter` against `schemas`.
    pub(in crate::runtime) fn compile(
        emitter: &EmitterName,
        plan: &EmitterEnvelopePlan,
        schemas: EnvelopeSchemas,
        context: RuntimeVmCompileContext<'_>,
    ) -> error_stack::Result<Self, EmitterEnvelopeError> {
        let Some(computed) = &plan.computed else {
            return Ok(Self {
                fixed: Arc::new(plan.fixed.clone()),
                computed: None,
            });
        };
        let mut positions = BTreeMap::new();
        for (position, (_, expression)) in computed.program.inner.set.iter().enumerate() {
            positions.insert(expression.span, position);
        }
        let exact_schema = match &computed.schema {
            EnvelopeSchemaPlan::Exact(schema) => Some(compile_schema(schema)),
            EnvelopeSchemaPlan::Inferred(_) => None,
        };
        let EnvelopeSchemas { input, output } = schemas;
        let mut bindings = match &exact_schema {
            Some(schema) => vec![
                VmCompileBinding::readonly("input", input.schema.clone())
                    .with_sensitivity(input.sensitivity.clone()),
                VmCompileBinding::writeonly(ENVELOPE_NAMESPACE, schema.arrow_schema()),
            ],
            None => vec![
                VmCompileBinding::readonly("input", input.schema.clone())
                    .with_sensitivity(input.sensitivity.clone()),
            ],
        };
        let message = match &output {
            Some(output) => {
                bindings.push(
                    VmCompileBinding::readonly("output", output.schema.clone())
                        .with_sensitivity(output.sensitivity.clone()),
                );
                output
            }
            None => &input,
        };
        bindings.push(
            VmCompileBinding::readonly("message", message.schema.clone())
                .with_sensitivity(message.sensitivity.clone()),
        );
        if exact_schema.is_none()
            && let Some(branch) = context.branch_binding()
        {
            bindings.push(branch);
        }
        let mut local_namespaces = HashSet::from_iter([
            "input".to_string(),
            "message".to_string(),
            "output".to_string(),
            ENVELOPE_NAMESPACE.to_string(),
        ]);
        if exact_schema.is_none() {
            local_namespaces.insert("branch".to_string());
        }
        let (materialized_bindings, materialized_interest) =
            referenced_materialized_stream_bindings(
                &computed.program,
                &local_namespaces,
                context.available_materialized_streams,
                context.current_branching,
            )
            .change_context_lazy(|| EmitterEnvelopeError::MaterializedState {
                emitter: emitter.clone(),
            })?;
        bindings.extend(materialized_bindings);
        let (parsed, pending_lookup_calls) =
            rewrite_lookup_hash_map_program(&computed.program, context.available_lookups)
                .change_context_lazy(|| EmitterEnvelopeError::Lookups {
                    emitter: emitter.clone(),
                })?;
        let (lookup_hash_maps, lookup_binding) = compile_lookup_hash_map_calls(
            pending_lookup_calls,
            ENVELOPE_NAMESPACE,
            &bindings,
            context.udfs,
        )
        .change_context_lazy(|| EmitterEnvelopeError::Lookups {
            emitter: emitter.clone(),
        })?;
        if let Some(lookup_binding) = lookup_binding {
            bindings.push(lookup_binding);
        }
        let value_schema = match (&computed.schema, exact_schema) {
            (EnvelopeSchemaPlan::Exact(_), Some(schema)) => schema,
            (EnvelopeSchemaPlan::Inferred(declarations), None) => {
                let mut inference_bindings = bindings.clone();
                inference_bindings.push(VmCompileBinding::writeonly(
                    ENVELOPE_NAMESPACE,
                    StdArc::new(arrow_schema::Schema::empty()),
                ));
                let inferred = infer_set_expr_types_for_bindings_with_udfs(
                    &parsed,
                    inference_bindings,
                    runtime_udf_signatures(context.udfs),
                )
                .change_context_lazy(|| EmitterEnvelopeError::Compile {
                    emitter: emitter.clone(),
                })?;
                let mut fields = Vec::with_capacity(declarations.len());
                for (index, (name, constraint)) in declarations.iter().enumerate() {
                    let ty = match constraint {
                        EnvelopeValueType::Exact(ty) => (*ty).into(),
                        EnvelopeValueType::Key => {
                            let field = &inferred[index];
                            if field.nullable || field.sensitive {
                                return Err(Report::new(EmitterEnvelopeError::Compile {
                                    emitter: emitter.clone(),
                                }));
                            }
                            match field.data_type {
                                ArrowDataType::Utf8 => ParseAsType::String,
                                ArrowDataType::Binary => ParseAsType::Bytes,
                                _ => {
                                    return Err(Report::new(EmitterEnvelopeError::Compile {
                                        emitter: emitter.clone(),
                                    }));
                                }
                            }
                        }
                    };
                    fields.push(nervix_models::SchemaField {
                        name: name.clone(),
                        ty,
                        optional: false,
                        sensitive: false,
                    });
                }
                let schema = compile_schema(&nervix_models::CreateSchema {
                    name: nervix_models::SchemaName::parse(ENVELOPE_NAMESPACE)
                        .assured("the envelope namespace is a schema name"),
                    fields,
                });
                bindings.push(VmCompileBinding::writeonly(
                    ENVELOPE_NAMESPACE,
                    schema.arrow_schema(),
                ));
                schema
            }
            _ => unreachable!("exact declarations have their precompiled schema"),
        };
        let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
            &parsed,
            value_schema.arrow_schema(),
            VmSchemaSensitivity::default(),
            bindings,
            context.compile_options(VmCompileOptions {
                output_mode: VmOutputMode::ExplicitOnly,
                allow_sensitive_output: false,
                ..VmCompileOptions::default()
            }),
        )
        .change_context_lazy(|| EmitterEnvelopeError::Compile {
            emitter: emitter.clone(),
        })?;
        Ok(Self {
            fixed: Arc::new(plan.fixed.clone()),
            computed: Some(EnvelopePrograms::Common(CompiledEnvelopeValues {
                program: CompiledProgramWithMaterializedInterest {
                    compiled: Arc::new(compiled),
                    materialized_interest,
                    output_namespace_input: OutputNamespaceInput::Finalized,
                    lookup_hash_maps,
                    error_sites: CompiledMessageErrorSites::new(),
                },
                targets: computed.targets.clone().into_boxed_slice(),
                positions,
            })),
        })
    }

    /// The envelope of every row `published` holds, evaluated with the execution time and
    /// materialized-state snapshot its route used.
    ///
    /// Fails only when the envelope cannot be evaluated over the batch at all. A row whose own
    /// envelope cannot be evaluated keeps its reason, and the emitter rejects that row when it
    /// publishes it.
    pub(in crate::runtime) async fn evaluate(
        &self,
        executor: &Executor,
        source_relay: &RelayName,
        published: &mut RelayRecordBatch,
        input: EnvelopeInput<'_>,
        side_inputs: &HashMap<String, RuntimeValue>,
        execution_now: Timestamp,
    ) -> Result<RowEnvelopes, PlannedGeneralFailure> {
        let (computed, partitioning) = match &self.computed {
            None => (None, None),
            Some(EnvelopePrograms::Common(computed)) => (Some(computed), None),
            Some(EnvelopePrograms::Sources(sources)) => {
                let Some(source) = sources.get(source_relay) else {
                    let error = Report::new(EmitterEnvelopeError::MissingSource {
                        relay: source_relay.clone(),
                    })
                    .change_context(PlannedGeneralError::Envelope);
                    return Err(PlannedGeneralFailure::new(
                        error,
                        std::mem::take(&mut published.acks),
                    ));
                };
                (source.computed.as_ref(), source.partitioning)
            }
        };
        let source_partition = match partitioning {
            Some(partitioning) => match partitioning.unit_of(&published.key) {
                Ok(Unit::Single) => Some(0),
                Ok(Unit::Partition(partition)) => Some(u64::from(partition)),
                Ok(Unit::Instance(_)) => {
                    unreachable!("relay partitioning never selects an instance")
                }
                Err(error) => {
                    return Err(PlannedGeneralFailure::new(
                        error
                            .change_context(EmitterEnvelopeError::SourcePartition)
                            .change_context(PlannedGeneralError::Envelope),
                        std::mem::take(&mut published.acks),
                    ));
                }
            },
            None => None,
        };
        let Some(computed) = computed else {
            let destination = source_partition.map(|partition| {
                Arc::new(RecordDestination {
                    key: None,
                    source_partition: Some(partition),
                })
            });
            return Ok(RowEnvelopes::Fixed(RowEnvelope {
                clauses: Arc::clone(&self.fixed),
                destination,
            }));
        };
        let acks = std::mem::take(&mut published.acks);
        let source = match input {
            EnvelopeInput::Published => None,
            EnvelopeInput::Source { records, rows } => match records.selected(rows) {
                Ok(selected) => Some(selected),
                Err(error) => {
                    let error = error
                        .change_context(EmitterEnvelopeError::SelectSourceRecords)
                        .change_context(PlannedGeneralError::Envelope);
                    return Err(PlannedGeneralFailure::new(error, acks));
                }
            },
        };
        let namespace_batches = match &source {
            Some(source) => vec![
                ("input", source.as_ref()),
                ("output", published.batch.as_ref()),
                ("message", published.batch.as_ref()),
            ],
            None => vec![
                ("input", published.batch.as_ref()),
                ("message", published.batch.as_ref()),
            ],
        };
        let executed = execute_filter_map_program_on_batch(
            ProgramRun {
                executor,
                now: execution_now,
            },
            &computed.program,
            FilterMapBatchInputs {
                carrier: &published.batch,
                namespace_batches: &namespace_batches,
                keys: &published.keys,
                side_inputs,
                ingest_metadata: None,
            },
            acks,
            None,
        )
        .await?;
        let row_count = published.batch.batch().num_rows();
        let ExecutedFilterMap {
            batch: evaluated,
            acks,
            selected_rows,
            ..
        } = executed;
        let rows = match computed.rows(
            &self.fixed,
            source_partition,
            &evaluated,
            &selected_rows,
            row_count,
        ) {
            Ok(rows) => rows,
            Err(error) => {
                let error = error.change_context(PlannedGeneralError::Envelope);
                return Err(PlannedGeneralFailure::new(error, acks));
            }
        };
        published.acks = acks;
        Ok(RowEnvelopes::Evaluated(rows.into_boxed_slice()))
    }
}

impl CompiledEnvelopeValues {
    /// The envelope of each of `row_count` published rows: `fixed` with the values the program
    /// evaluated for it, or why it has none.
    ///
    /// The program selects each input row it evaluated, so a row it left out has no envelope, and
    /// a row whose evaluation recorded an error fails at the first value, in written order, that
    /// failed.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the VM iterator maps selected output rows to the admitted input batch"
        )
    )]
    fn rows(
        &self,
        fixed: &RecordEnvelope,
        source_partition: Option<u64>,
        evaluated: &VmTypedBatch,
        selected_rows: &nervix_vm::RowSelection,
        row_count: usize,
    ) -> error_stack::Result<Vec<Result<RowEnvelope, EnvelopeError>>, EmitterEnvelopeError> {
        let mut output_rows = vec![None; row_count];
        for (output_row, input_row) in selected_rows.iter().enumerate() {
            if let Some(slot) = output_rows.get_mut(input_row) {
                *slot = Some(output_row);
            }
        }
        let columns = evaluated.columns();
        let mut rows = Vec::with_capacity(row_count);
        for output_row in output_rows {
            let Some(output_row) = output_row else {
                rows.push(Err(EnvelopeError::OmittedInputRow));
                continue;
            };
            if let Some(failure) = self.first_failure(evaluated, output_row) {
                rows.push(Err(failure));
                continue;
            }
            let mut envelope = fixed.clone();
            let mut destination = RecordDestination {
                key: None,
                source_partition,
            };
            let mut missing = None;
            for (target, column) in self.targets.iter().zip(columns) {
                if !target.read(column, output_row, &mut envelope, &mut destination)? {
                    missing = Some(EnvelopeError::Null {
                        site: EnvelopeSite::from(*target),
                    });
                    break;
                }
            }
            match missing {
                Some(failure) => rows.push(Err(failure)),
                None => {
                    let destination = (destination.key.is_some()
                        || destination.source_partition.is_some())
                    .then(|| Arc::new(destination));
                    rows.push(Ok(RowEnvelope {
                        clauses: Arc::new(envelope),
                        destination,
                    }));
                }
            }
        }
        Ok(rows)
    }

    /// The first value, in written order, whose evaluation failed for `row`, if one did.
    fn first_failure(&self, evaluated: &VmTypedBatch, row: usize) -> Option<EnvelopeError> {
        let mut first: Option<(usize, &nervix_vm::SideError)> = None;
        for error in evaluated.errors().row(row) {
            // A failure the program reports outside every value belongs to the first one, so the
            // row still has no envelope.
            let position = match self.positions.get(&error.span) {
                Some(position) => *position,
                None => 0,
            };
            let earlier = match first {
                Some((first_position, _)) => position < first_position,
                None => true,
            };
            if earlier {
                first = Some((position, error));
            }
        }
        let (position, error) = first?;
        let target = self
            .targets
            .get(position)
            .verified("every position names one of the program's values");
        Some(EnvelopeError::ExpressionFailed {
            site: EnvelopeSite::from(*target),
            code: error.code(),
            span: error.span,
        })
    }
}

impl EnvelopeTarget {
    /// Sets on `envelope` the value `column` holds for `row`, and answers whether it held one. A
    /// column of another type than the target's is not this program's output.
    fn read(
        self,
        column: &VmTypedArray,
        row: usize,
        envelope: &mut RecordEnvelope,
        destination: &mut RecordDestination,
    ) -> error_stack::Result<bool, EmitterEnvelopeError> {
        let wrong_column = |expected: &'static str| {
            Report::new(EmitterEnvelopeError::ValueColumn {
                site: EnvelopeSite::from(self),
                expected,
            })
        };
        // The value `column` holds for `row` through its typed `accessor`, leaving the target
        // unset where the row's value is null.
        macro_rules! value {
            ($accessor:ident, $expected:literal) => {{
                let Some(values) = column.$accessor() else {
                    return Err(wrong_column($expected));
                };
                if values.is_null(row) {
                    return Ok(false);
                }
                values.value(row)
            }};
        }
        match self {
            Self::Clause(EnvelopeClauseKind::Key) => {
                destination.key = Some(if column.as_utf8().is_some() {
                    RecordKey::String(value!(as_utf8, "STRING").to_string())
                } else {
                    RecordKey::Bytes(value!(as_binary, "BYTES").to_vec())
                });
            }
            Self::RoutingKey => {
                envelope.routing_key = Some(value!(as_utf8, "STRING").to_string());
            }
            Self::Clause(EnvelopeClauseKind::Timestamp) => {
                let nanos = value!(as_datetime, "DATETIME");
                envelope.timestamp = Some(Timestamp::from_unix_nanos(nanos));
            }
            Self::Clause(EnvelopeClauseKind::ReplyTo) => {
                envelope.reply_to = Some(value!(as_utf8, "STRING").to_string());
            }
            Self::Clause(EnvelopeClauseKind::MessageId) => {
                envelope.message_id = Some(value!(as_utf8, "STRING").to_string());
            }
            Self::Clause(EnvelopeClauseKind::DeduplicationId) => {
                envelope.deduplication_id = Some(value!(as_utf8, "STRING").to_string());
            }
            Self::Clause(
                EnvelopeClauseKind::PartitionFromBranch
                | EnvelopeClauseKind::Persistent
                | EnvelopeClauseKind::Mandatory
                | EnvelopeClauseKind::Retain
                | EnvelopeClauseKind::DeliverAfter
                | EnvelopeClauseKind::Delay,
            ) => return Err(wrong_column("no value")),
            Self::Property(declaration) => {
                let value = match declaration.ty {
                    PropertyType::U8 => PropertyValue::U8(value!(as_uint8, "U8")),
                    PropertyType::I8 => PropertyValue::I8(value!(as_int8, "I8")),
                    PropertyType::U16 => PropertyValue::U16(value!(as_uint16, "U16")),
                    PropertyType::I16 => PropertyValue::I16(value!(as_int16, "I16")),
                    PropertyType::U32 => PropertyValue::U32(value!(as_uint32, "U32")),
                    PropertyType::I32 => PropertyValue::I32(value!(as_int32, "I32")),
                    PropertyType::U64 => PropertyValue::U64(value!(as_uint64, "U64")),
                    PropertyType::I64 => PropertyValue::I64(value!(as_int64, "I64")),
                    PropertyType::Bool => PropertyValue::Bool(value!(as_boolean, "BOOL")),
                    PropertyType::String => {
                        PropertyValue::String(value!(as_utf8, "STRING").to_string())
                    }
                    PropertyType::Datetime => PropertyValue::Datetime(Timestamp::from_unix_nanos(
                        value!(as_datetime, "DATETIME"),
                    )),
                    PropertyType::Bytes => {
                        PropertyValue::Bytes(value!(as_binary, "BYTES").to_vec())
                    }
                };
                envelope.properties.push(RecordProperty {
                    name: declaration.name,
                    value,
                });
            }
        }
        Ok(true)
    }
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct RowEnvelope {
    clauses: Arc<RecordEnvelope>,
    destination: Option<Arc<RecordDestination>>,
}

impl From<Arc<RecordEnvelope>> for RowEnvelope {
    fn from(clauses: Arc<RecordEnvelope>) -> Self {
        Self {
            clauses,
            destination: None,
        }
    }
}

impl RowEnvelope {
    fn held_bytes(&self) -> u64 {
        let destination = match &self.destination {
            Some(destination) => destination.held_bytes(),
            None => 0,
        };
        self.clauses
            .held_bytes()
            .checked_add(destination)
            .assured("both values are held in memory")
    }
}

/// The envelope of every row of one emitter batch, as the host evaluated it. Each envelope is held
/// once: the payloads that publish a row share its envelope rather than copying it.
#[derive(Debug, Clone)]
pub(in crate::runtime) enum RowEnvelopes {
    /// Every row carries this one envelope, which computes no value.
    Fixed(RowEnvelope),
    /// Each row's own envelope, or why it has none.
    Evaluated(Box<[Result<RowEnvelope, EnvelopeError>]>),
}

impl RowEnvelopes {
    /// How many rows these envelopes describe, absent when they describe every row alike.
    #[cfg(test)]
    pub(in crate::runtime) fn row_count(&self) -> Option<usize> {
        match self {
            Self::Fixed(_) => None,
            Self::Evaluated(rows) => Some(rows.len()),
        }
    }

    /// The envelope row `row` is published with, or why it has none. `None` means these
    /// envelopes describe no such row.
    pub(in crate::runtime) fn envelope(
        &self,
        row: usize,
    ) -> Option<Result<&Arc<RecordEnvelope>, &EnvelopeError>> {
        match self {
            Self::Fixed(envelope) => Some(Ok(&envelope.clauses)),
            Self::Evaluated(rows) => match rows.get(row)? {
                Ok(envelope) => Some(Ok(&envelope.clauses)),
                Err(failure) => Some(Err(failure)),
            },
        }
    }

    pub(in crate::runtime) fn destination(&self, row: usize) -> Option<&Arc<RecordDestination>> {
        match self {
            Self::Fixed(envelope) => envelope.destination.as_ref(),
            Self::Evaluated(rows) => rows.get(row)?.as_ref().ok()?.destination.as_ref(),
        }
    }

    /// The bytes these envelopes hold beyond the batch they describe.
    pub(in crate::runtime) fn estimated_bytes(&self) -> u64 {
        match self {
            Self::Fixed(envelope) => envelope.held_bytes(),
            Self::Evaluated(rows) => {
                let mut bytes = 0_u64;
                for envelope in rows.iter().flatten() {
                    bytes = bytes
                        .checked_add(envelope.held_bytes())
                        .assured("every term counts bytes of an envelope this node holds");
                }
                bytes
            }
        }
    }

    /// The bytes the envelopes of `rows` hold beyond the batch they describe, as
    /// [`Self::estimated_bytes`] measures them for every row.
    pub(in crate::runtime) fn estimated_bytes_of_rows(&self, rows: &[usize]) -> u64 {
        let Self::Evaluated(envelopes) = self else {
            return self.estimated_bytes();
        };
        let mut bytes = 0_u64;
        for row in rows {
            // A row without an envelope holds only the reason it has none.
            let Some(Ok(envelope)) = envelopes.get(*row) else {
                continue;
            };
            bytes = bytes
                .checked_add(envelope.held_bytes())
                .assured("every term counts bytes of an envelope this node holds");
        }
        bytes
    }
}

#[cfg(test)]
#[path = "emitter_envelope_tests.rs"]
mod tests;
