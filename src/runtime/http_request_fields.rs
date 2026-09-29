//! The request fields an HTTP emitter evaluates for every record its route keeps.
//!
//! Layer: data plane.
//! - **Owns.** Compiling one HTTP emitter's `METHOD`, `PATH` and `write_header` invocations once,
//!   as one program over each record's original input, its finalized output and the batch's
//!   materialized state; evaluating that program for the rows the route kept, with the execution
//!   time and state snapshot the rest of the batch used; validating each row's method, then its
//!   path on the client's origin, then each header write in written order; the message error that
//!   rejects a row at the first field that failed; and what the message error of an admitted
//!   request rejected later reads: its original source record, the materialized state its batch
//!   was admitted with, and its attempted codec record.
//! - **Depends on.** The emitter's HTTP sink plan, the VM's compile and execute API, the
//!   vocabulary's HTTP request fields, and the node's planned message errors.
//! - **Must not know.** Which connector sends the requests, how their bodies are encoded, or when
//!   the emitter publishes them.

use error_stack::ResultExt as _;
use nervix_models::{
    HttpApplicationHeaders, HttpBodyMode, HttpHeaderName, HttpHeaderValue, HttpMethod, HttpOrigin,
    HttpRequestFieldError, HttpTarget,
};
use nervix_vm::SideError as VmSideError;

use super::{
    vm_compile::{RuntimeVmCompileError, RuntimeVmCompileResult},
    *,
};

/// The namespace the request program writes a record's method and path into. It is internal: the
/// program assigns it only through its two synthesized bare targets, and no field reference a user
/// writes names it.
const HTTP_REQUEST_NAMESPACE: &str = "http_request";
const METHOD_FIELD: &str = "method";
const PATH_FIELD: &str = "path";
/// Where the method and the path sit among the fields, which are evaluated in this order and then
/// every header write after them.
const METHOD_POSITION: usize = 0;
const PATH_POSITION: usize = 1;
const FIRST_HEADER_POSITION: usize = 2;

/// Why an HTTP emitter's request fields cannot be compiled, or why a batch's evaluated fields
/// cannot be read or reported, which fails the whole batch rather than one record.
#[derive(Debug, Error)]
pub(in crate::runtime) enum HttpRequestFieldsError {
    #[error("HTTP emitter '{emitter}' request fields cannot record where they fail")]
    Sites { emitter: EmitterName },
    #[error("HTTP emitter '{emitter}' request fields cannot bind the materialized state they read")]
    MaterializedState { emitter: EmitterName },
    #[error("HTTP emitter '{emitter}' request fields cannot bind the lookups they call")]
    Lookups { emitter: EmitterName },
    #[error("HTTP emitter '{emitter}' request fields do not compile")]
    Compile { emitter: EmitterName },
    #[error("the request program did not evaluate STRING method and path columns")]
    FieldColumns,
    #[error("the request program invoked '{function}' rather than write_header")]
    UnsupportedInvocation { function: String },
    #[error("a write_header invocation did not evaluate two STRING arguments")]
    HeaderWriteArguments,
    #[error("failed to select the source records of the published rows")]
    SelectSourceRecords,
    #[error("published row {row} has no source record")]
    MissingSourceRecord { row: usize },
    #[error("failed to address the source record of published row {row}")]
    SourceRecord { row: usize },
}

/// The request fields one record was admitted with. They are validated once, when the record is
/// admitted, and every attempt sends them unchanged.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct HttpRequestFields {
    pub(super) method: HttpMethod,
    pub(super) target: HttpTarget,
    pub(super) headers: HttpApplicationHeaders,
}

impl HttpRequestFields {
    /// The bytes these fields hold, which the emitter's buffer counts with the batch that carries
    /// them.
    pub(super) fn estimated_bytes(&self) -> u64 {
        let method: u64 = self.method.as_str().len().arch_into();
        let target: u64 = self.target.as_str().len().arch_into();
        let headers: u64 = self.headers.field_bytes().arch_into();
        method
            .checked_add(target)
            .assured("both lengths count bytes of values this node already holds in memory")
            .checked_add(headers)
            .assured("every term counts bytes of values this node already holds in memory")
    }
}

/// The schemas a request program reads.
pub(in crate::runtime) struct HttpRequestSchemas {
    /// The original source record, which request expressions read as `input`.
    pub(in crate::runtime) input: RuntimeVmSchema,
    /// The finalized codec record, which request expressions read as `output` and `message`.
    /// Absent for an emitter declared `WITHOUT BODY`, whose `message` is the source record.
    pub(in crate::runtime) output: Option<RuntimeVmSchema>,
}

/// One HTTP emitter's request fields, compiled once for its task.
pub(in crate::runtime) struct CompiledHttpRequestFields {
    program: CompiledProgramWithMaterializedInterest,
    origin: HttpOrigin,
    body: HttpBodyMode,
    /// Where a failure of each field is reported, in the order the fields are evaluated: the
    /// method, the path, and then every header write.
    sites: Vec<CompiledMessageErrorSite>,
    /// The position in `sites` of the field whose evaluation reports each VM span.
    positions: BTreeMap<VmSpan, usize>,
}

/// The source records of one batch, kept while its route runs so that the request fields of each
/// record the route keeps read, and report, the original input, and then with the requests it
/// admitted until they complete, so that a later rejection reports it as well.
#[derive(Clone)]
pub(in crate::runtime) struct SourceRecords {
    batch: Arc<RuntimeRecordBatch>,
    metadata: RecordMetadataColumns,
}

impl SourceRecords {
    pub(in crate::runtime) fn of(batch: &RelayRecordBatch) -> Self {
        Self {
            batch: batch.batch.clone(),
            metadata: batch.metadata.clone(),
        }
    }

    /// Row `row`, without copying its values.
    fn row(&self, row: usize) -> error_stack::Result<RuntimeRow, RuntimeSchemaError> {
        let Some(metadata) = self.metadata.row(row) else {
            return Err(Report::new(RuntimeSchemaError::RowOutOfBounds {
                row,
                rows: self.metadata.len(),
            }));
        };
        RuntimeRow::new(self.batch.clone(), row, metadata)
    }

    /// The rows `rows` names, in that order. Rows that are every row in order share this batch's
    /// columns.
    fn selected(
        &self,
        rows: &[usize],
    ) -> error_stack::Result<Arc<RuntimeRecordBatch>, RuntimeSchemaError> {
        let row_count = self.batch.batch().num_rows();
        if rows.len() == row_count && rows.iter().copied().eq(0..row_count) {
            return Ok(self.batch.clone());
        }
        Ok(Arc::new(self.batch.take(rows)?))
    }
}

/// Where the request fields of the rows a batch publishes read `input` from, which is also the
/// original source record the message error of each row reads.
#[derive(Clone)]
pub(in crate::runtime) enum HttpRequestInput {
    /// The emitter sends no body, so every row it publishes is a source record itself.
    Published,
    /// The emitter encodes finalized codec records, which its route built from these source rows,
    /// one for each published row in order.
    Source {
        records: SourceRecords,
        rows: Vec<usize>,
    },
}

impl HttpRequestInput {
    /// The original source record of row `row` of `published`, which a message error names.
    fn source_record(
        &self,
        published: &RelayRecordBatch,
        row: usize,
    ) -> error_stack::Result<RuntimeRow, HttpRequestFieldsError> {
        match self {
            Self::Published => published
                .runtime_row(row)
                .change_context(HttpRequestFieldsError::SourceRecord { row }),
            Self::Source { records, rows } => {
                let Some(source_row) = rows.get(row) else {
                    return Err(Report::new(HttpRequestFieldsError::MissingSourceRecord {
                        row,
                    }));
                };
                records
                    .row(*source_row)
                    .change_context(HttpRequestFieldsError::SourceRecord { row })
            }
        }
    }

    /// Whether the rows the emitter publishes are finalized codec records, whose attempted body
    /// an error handler reads as `partial_output`.
    fn publishes_codec_records(&self) -> bool {
        match self {
            Self::Published => false,
            Self::Source { .. } => true,
        }
    }

    /// The source records of `admitted`, the published rows a batch kept, in that order.
    fn admitted(self, admitted: &[usize]) -> error_stack::Result<Self, HttpRequestFieldsError> {
        let (records, rows) = match self {
            Self::Published => return Ok(Self::Published),
            Self::Source { records, rows } => (records, rows),
        };
        let mut admitted_rows = Vec::with_capacity(admitted.len());
        for row in admitted {
            let Some(source_row) = rows.get(*row) else {
                return Err(Report::new(HttpRequestFieldsError::MissingSourceRecord {
                    row: *row,
                }));
            };
            admitted_rows.push(*source_row);
        }
        Ok(Self::Source {
            records,
            rows: admitted_rows,
        })
    }
}

/// The rows of one batch whose request fields are all valid, and the request of each.
pub(in crate::runtime) struct AcceptedHttpRequests {
    pub(in crate::runtime) batch: RelayRecordBatch,
    pub(in crate::runtime) requests: AdmittedHttpRequests,
}

/// The requests of the rows of one admitted batch, and what the message error of a row reads when
/// its request is rejected after admission: its original source record, the materialized state
/// its batch was admitted with and, with a codec, its attempted codec record.
#[derive(Clone)]
pub(in crate::runtime) struct AdmittedHttpRequests {
    /// The request of each row, in row order.
    fields: Vec<HttpRequestFields>,
    /// The original source record of each row.
    sources: HttpRequestInput,
    materialized_state: HashMap<String, RuntimeValue>,
}

impl AdmittedHttpRequests {
    /// Requests whose rows are their own source records, admitted without materialized state.
    #[cfg(test)]
    pub(in crate::runtime) fn published(fields: Vec<HttpRequestFields>) -> Self {
        Self {
            fields,
            sources: HttpRequestInput::Published,
            materialized_state: HashMap::default(),
        }
    }

    pub(in crate::runtime) fn request_count(&self) -> usize {
        self.fields.len()
    }

    /// The request row `row` was admitted with.
    pub(in crate::runtime) fn request(&self, row: usize) -> Option<&HttpRequestFields> {
        self.fields.get(row)
    }

    /// The bytes the request fields hold, which the emitter's buffer counts with their batch.
    pub(in crate::runtime) fn estimated_bytes(&self) -> u64 {
        let mut bytes = 0_u64;
        for fields in &self.fields {
            bytes = bytes
                .checked_add(fields.estimated_bytes())
                .assured("every term counts bytes of values this node already holds in memory");
        }
        bytes
    }

    /// What the message error of row `row` of `published`, the batch these requests were admitted
    /// with, reads besides the error itself.
    pub(in crate::runtime) fn rejected_record(
        &self,
        published: &RelayRecordBatch,
        row: usize,
    ) -> error_stack::Result<RejectedRecordInput, HttpRequestFieldsError> {
        let record = self.sources.source_record(published, row)?;
        let partial_output = match self.sources.publishes_codec_records() {
            true => finalized_partial_output(&published.batch, row),
            false => None,
        };
        Ok(RejectedRecordInput {
            record,
            partial_output,
            materialized_state: self.materialized_state.clone(),
        })
    }
}

/// What evaluating one batch's request fields decided.
pub(in crate::runtime) struct PreparedHttpRequests {
    /// The rows whose every request field is valid, absent when every row failed one.
    pub(in crate::runtime) accepted: Option<AcceptedHttpRequests>,
    /// The message error of each row a failed request field rejected.
    pub(in crate::runtime) message_errors: Vec<PlannedMessageError>,
}

/// How the first failed request field of one row failed.
enum RequestFieldFailure<'a> {
    /// Its expression failed while it was evaluated.
    Evaluation(&'a VmSideError),
    /// Its value is not a valid request field.
    Invalid(HttpRequestFieldError),
}

/// The first request field one row failed, by its position among the fields.
struct FailedRequestField<'a> {
    position: usize,
    failure: RequestFieldFailure<'a>,
}

impl<'a> FailedRequestField<'a> {
    fn evaluation(position: usize, error: &'a VmSideError) -> Self {
        Self {
            position,
            failure: RequestFieldFailure::Evaluation(error),
        }
    }

    fn invalid(position: usize, error: Report<HttpRequestFieldError>) -> Self {
        Self {
            position,
            failure: RequestFieldFailure::Invalid(*error.current_context()),
        }
    }
}

/// The evaluated method, path and header writes of every row of one batch.
struct EvaluatedRequestFields<'a> {
    methods: &'a StringArray,
    paths: &'a StringArray,
    /// The name and value each header write produced for every row, in written order.
    header_writes: Vec<EvaluatedHeaderWrite<'a>>,
    /// The first expression failure of each field of each row, by the field's position.
    evaluation_failures: Vec<BTreeMap<usize, &'a VmSideError>>,
}

struct EvaluatedHeaderWrite<'a> {
    names: &'a StringArray,
    values: &'a StringArray,
}

impl<'a> EvaluatedRequestFields<'a> {
    /// The first failure of the expression of the field at `position` for row `row`, if it failed.
    fn evaluation_failure(&self, row: usize, position: usize) -> Option<&'a VmSideError> {
        let failures = self.evaluation_failures.get(row)?;
        failures.get(&position).copied()
    }
}

impl CompiledHttpRequestFields {
    /// Compiles the request fields of the HTTP emitter `emitter` against `schemas`.
    pub(in crate::runtime) fn compile(
        emitter: &EmitterName,
        sink: &HttpSinkPlan,
        request: &nervix_vm::program::SpannedNode<nervix_vm::program::Program>,
        schemas: HttpRequestSchemas,
        context: RuntimeVmCompileContext<'_>,
    ) -> error_stack::Result<Self, HttpRequestFieldsError> {
        let RequestFieldSites { sites, positions } = RequestFieldSites::of(request)
            .change_context(HttpRequestFieldsError::Sites {
                emitter: emitter.clone(),
            })?;

        let HttpRequestSchemas { input, output } = schemas;
        let request_schema = StdArc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(METHOD_FIELD, ArrowDataType::Utf8, false),
            arrow_schema::Field::new(PATH_FIELD, ArrowDataType::Utf8, false),
        ]));
        let mut bindings = vec![
            VmCompileBinding::readonly("input", input.schema.clone())
                .with_sensitivity(input.sensitivity.clone()),
            // The method and the path are only written, so neither is an input of the program.
            VmCompileBinding::writeonly(HTTP_REQUEST_NAMESPACE, request_schema.clone()),
        ];
        let body = match &output {
            Some(output) => {
                bindings.push(
                    VmCompileBinding::readonly("output", output.schema.clone())
                        .with_sensitivity(output.sensitivity.clone()),
                );
                bindings.push(
                    VmCompileBinding::readonly("message", output.schema.clone())
                        .with_sensitivity(output.sensitivity.clone()),
                );
                HttpBodyMode::Codec
            }
            None => {
                bindings.push(
                    VmCompileBinding::readonly("message", input.schema.clone())
                        .with_sensitivity(input.sensitivity.clone()),
                );
                HttpBodyMode::WithoutBody
            }
        };
        let local_namespaces = HashSet::from_iter([
            "input".to_string(),
            "message".to_string(),
            "output".to_string(),
            HTTP_REQUEST_NAMESPACE.to_string(),
        ]);
        let (materialized_bindings, materialized_interest) =
            referenced_materialized_stream_bindings(
                request,
                &local_namespaces,
                context.available_materialized_streams,
                context.current_branching,
            )
            .change_context(HttpRequestFieldsError::MaterializedState {
                emitter: emitter.clone(),
            })?;
        bindings.extend(materialized_bindings);
        let (parsed, pending_lookup_calls) =
            rewrite_lookup_hash_map_program(request, context.available_lookups).change_context(
                HttpRequestFieldsError::Lookups {
                    emitter: emitter.clone(),
                },
            )?;
        let (lookup_hash_maps, lookup_binding) = compile_lookup_hash_map_calls(
            pending_lookup_calls,
            HTTP_REQUEST_NAMESPACE,
            &bindings,
            context.udfs,
        )
        .change_context(HttpRequestFieldsError::Lookups {
            emitter: emitter.clone(),
        })?;
        if let Some(lookup_binding) = lookup_binding {
            bindings.push(lookup_binding);
        }
        let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
            &parsed,
            request_schema,
            VmSchemaSensitivity::default(),
            bindings,
            context.compile_options(VmCompileOptions {
                output_mode: VmOutputMode::ExplicitOnly,
                allow_sensitive_output: false,
                allow_header_writes: true,
                ..VmCompileOptions::default()
            }),
        )
        .map_err(|error| {
            let message = error.current_context().message.clone();
            error
                .change_context(HttpRequestFieldsError::Compile {
                    emitter: emitter.clone(),
                })
                .attach_printable(message)
        })?;
        Ok(Self {
            program: CompiledProgramWithMaterializedInterest {
                compiled: Arc::new(compiled),
                materialized_interest,
                output_namespace_input: OutputNamespaceInput::Finalized,
                lookup_hash_maps,
                error_sites: CompiledMessageErrorSites::new(),
            },
            origin: sink.origin.clone(),
            body,
            sites,
            positions,
        })
    }

    /// Evaluates the request fields of every row `published` holds, with the execution time and
    /// materialized-state snapshot its route used, and keeps the rows whose every field is valid.
    ///
    /// A row is rejected at the first field that failed, in evaluation order: the method, the
    /// path, and then each header write as it is written, so an invalid write rejects its row even
    /// when a later write replaces its value. A rejected row sends no part of its request.
    pub(in crate::runtime) async fn prepare(
        &self,
        emitter: &EmitterName,
        mut published: RelayRecordBatch,
        input: HttpRequestInput,
        side_inputs: &HashMap<String, RuntimeValue>,
        execution_now: Timestamp,
    ) -> PlannedGeneralResult<PreparedHttpRequests> {
        let acks = std::mem::take(&mut published.acks);
        let source_input = match &input {
            HttpRequestInput::Published => None,
            HttpRequestInput::Source { records, rows } => match records.selected(rows) {
                Ok(selected) => Some(selected),
                Err(error) => {
                    let error = error.change_context(HttpRequestFieldsError::SelectSourceRecords);
                    return Err(Report::new(PlannedGeneralError {
                        acks,
                        reason: format!(
                            "emitter '{}' failed to prepare its HTTP requests: {error:#}",
                            emitter.as_str()
                        ),
                    }));
                }
            },
        };
        let namespace_batches = match &source_input {
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
            "emitter",
            emitter,
            &self.program,
            FilterMapBatchInputs {
                carrier: &published.batch,
                namespace_batches: &namespace_batches,
                keys: &published.keys,
                side_inputs,
                ingest_metadata: None,
            },
            execution_now,
            acks,
            None,
        )
        .await?;
        let ExecutedFilterMap {
            batch: evaluated,
            invocations,
            mut acks,
            ..
        } = executed;
        let fields = match self.evaluated_fields(&evaluated, &invocations) {
            Ok(fields) => fields,
            Err(error) => {
                return Err(Report::new(PlannedGeneralError {
                    acks,
                    reason: format!(
                        "emitter '{}' failed to read its HTTP request fields: {error:#}",
                        emitter.as_str()
                    ),
                }));
            }
        };
        let state_snapshot = relay_state_snapshot_from_side_inputs(side_inputs);
        let row_count = published.batch.batch().num_rows();
        let mut accepted_rows = Vec::with_capacity(row_count);
        let mut requests = Vec::with_capacity(row_count);
        let mut message_errors: Vec<PlannedMessageError> = Vec::new();
        for row in 0..row_count {
            let failed = match self.row_request(&fields, row) {
                Ok(request) => {
                    accepted_rows.push(row);
                    requests.push(request);
                    continue;
                }
                Err(failed) => failed,
            };
            let source_record = match input.source_record(&published, row) {
                Ok(source_record) => source_record,
                Err(error) => {
                    return Err(batch_failure(
                        acks,
                        message_errors,
                        format!(
                            "emitter '{}' failed to report a rejected HTTP request: {error:#}",
                            emitter.as_str()
                        ),
                    ));
                }
            };
            let partial_output = match input.publishes_codec_records() {
                true => finalized_partial_output(&published.batch, row),
                false => None,
            };
            let key = match published.keys.get(row) {
                Some(key) => key.clone(),
                None => None,
            };
            let row_acks = match acks.get_mut(row) {
                Some(row_acks) => std::mem::take(row_acks),
                None => AckSet::empty(),
            };
            message_errors.push(planned_structured_message_error(
                RelayMessage {
                    key,
                    record: source_record,
                    acks: row_acks,
                },
                self.structured_error(emitter, failed, execution_now),
                partial_output,
                state_snapshot.clone(),
                execution_now,
            ));
        }
        if accepted_rows.is_empty() {
            return Ok(PreparedHttpRequests {
                accepted: None,
                message_errors,
            });
        }
        let sources = match input.admitted(&accepted_rows) {
            Ok(sources) => sources,
            Err(error) => {
                return Err(batch_failure(
                    acks,
                    message_errors,
                    format!(
                        "emitter '{}' failed to keep the source records of its HTTP requests: \
                         {error:#}",
                        emitter.as_str()
                    ),
                ));
            }
        };
        published.acks = acks;
        let batch = match published.take(&accepted_rows) {
            Ok(batch) => batch,
            Err(failure) => {
                return Err(batch_failure(
                    failure.preserved,
                    message_errors,
                    format!(
                        "emitter '{}' failed to keep the rows whose HTTP requests are valid: {:#}",
                        emitter.as_str(),
                        failure.error
                    ),
                ));
            }
        };
        Ok(PreparedHttpRequests {
            accepted: Some(AcceptedHttpRequests {
                batch,
                requests: AdmittedHttpRequests {
                    fields: requests,
                    sources,
                    materialized_state: state_snapshot,
                },
            }),
            message_errors,
        })
    }

    /// The method and path columns and the header writes the program evaluated, with the first
    /// expression failure of each field of each row.
    fn evaluated_fields<'a>(
        &self,
        evaluated: &'a VmTypedBatch,
        invocations: &'a [nervix_vm::FunctionInvocation],
    ) -> error_stack::Result<EvaluatedRequestFields<'a>, HttpRequestFieldsError> {
        let [VmTypedArray::Utf8(methods), VmTypedArray::Utf8(paths)] = evaluated.columns() else {
            return Err(Report::new(HttpRequestFieldsError::FieldColumns));
        };
        let mut header_writes = Vec::with_capacity(invocations.len());
        for invocation in invocations {
            if invocation.function != FunctionName::WriteHeader {
                return Err(Report::new(HttpRequestFieldsError::UnsupportedInvocation {
                    function: invocation.function.as_str().to_string(),
                }));
            }
            let [VmTypedArray::Utf8(names), VmTypedArray::Utf8(values)] =
                invocation.arguments.as_slice()
            else {
                return Err(Report::new(HttpRequestFieldsError::HeaderWriteArguments));
            };
            header_writes.push(EvaluatedHeaderWrite { names, values });
        }
        let row_count = evaluated.row_count();
        let mut evaluation_failures = Vec::with_capacity(row_count);
        for row in 0..row_count {
            let mut failures = BTreeMap::new();
            // The VM records a row's failures in the order it evaluates them. Inserting them
            // latest first leaves each field with its first failure.
            for error in evaluated.errors().row(row).iter().rev() {
                // A failure the program reports outside every field belongs to the first field, so
                // the row is still rejected before any field is sent.
                let position = match self.positions.get(&error.span) {
                    Some(position) => *position,
                    None => METHOD_POSITION,
                };
                failures.insert(position, error);
            }
            evaluation_failures.push(failures);
        }
        Ok(EvaluatedRequestFields {
            methods,
            paths,
            header_writes,
            evaluation_failures,
        })
    }

    /// The request of row `row`, or the first of its fields that failed.
    fn row_request<'a>(
        &self,
        fields: &EvaluatedRequestFields<'a>,
        row: usize,
    ) -> Result<HttpRequestFields, FailedRequestField<'a>> {
        if let Some(error) = fields.evaluation_failure(row, METHOD_POSITION) {
            return Err(FailedRequestField::evaluation(METHOD_POSITION, error));
        }
        let method = string_value(fields.methods, row, HttpRequestFieldError::Method)
            .map_err(|error| FailedRequestField::invalid(METHOD_POSITION, error))?;
        let method = HttpMethod::parse(method, self.body)
            .map_err(|error| FailedRequestField::invalid(METHOD_POSITION, error))?;

        if let Some(error) = fields.evaluation_failure(row, PATH_POSITION) {
            return Err(FailedRequestField::evaluation(PATH_POSITION, error));
        }
        let path = string_value(fields.paths, row, HttpRequestFieldError::Target)
            .map_err(|error| FailedRequestField::invalid(PATH_POSITION, error))?;
        let target = self
            .origin
            .target(path)
            .map_err(|error| FailedRequestField::invalid(PATH_POSITION, error))?;

        let mut headers = HttpApplicationHeaders::default();
        let mut last_position = None;
        for (index, header_write) in fields.header_writes.iter().enumerate() {
            let position = FIRST_HEADER_POSITION
                .checked_add(index)
                .assured("the header writes belong to one construction held in memory");
            if let Some(error) = fields.evaluation_failure(row, position) {
                return Err(FailedRequestField::evaluation(position, error));
            }
            let name = string_value(header_write.names, row, HttpRequestFieldError::HeaderName)
                .map_err(|error| FailedRequestField::invalid(position, error))?;
            let name = HttpHeaderName::parse(name)
                .map_err(|error| FailedRequestField::invalid(position, error))?;
            let value = string_value(header_write.values, row, HttpRequestFieldError::HeaderValue)
                .map_err(|error| FailedRequestField::invalid(position, error))?;
            let value = HttpHeaderValue::parse(value)
                .map_err(|error| FailedRequestField::invalid(position, error))?;
            headers
                .insert(name, value)
                .map_err(|error| FailedRequestField::invalid(position, error))?;
            last_position = Some(position);
        }
        // The combined bound holds once every write has replaced any earlier value of its name,
        // so exceeding it is reported at the write that completed the envelope.
        if let Some(position) = last_position {
            headers
                .validate_total()
                .map_err(|error| FailedRequestField::invalid(position, error))?;
        }
        Ok(HttpRequestFields {
            method,
            target,
            headers,
        })
    }

    /// The message error of a row whose `failed` field rejected it. It names the field and the
    /// fields its expression reads, and never quotes a value.
    fn structured_error(
        &self,
        emitter: &EmitterName,
        failed: FailedRequestField<'_>,
        execution_now: Timestamp,
    ) -> StructuredMessageError {
        let site = self.sites.get(failed.position);
        let (code, message) = match failed.failure {
            RequestFieldFailure::Evaluation(error) => (
                MessageErrorCode::Evaluation,
                format!(
                    "emitter '{}' HTTP request side error {}: {} at {}",
                    emitter.as_str(),
                    error.code().as_str(),
                    error.reason,
                    error.span
                ),
            ),
            RequestFieldFailure::Invalid(error) => (
                MessageErrorCode::Validation,
                format!(
                    "emitter '{}' cannot publish its HTTP request: {error}",
                    emitter.as_str()
                ),
            ),
        };
        let Some(site) = site else {
            return structured_message_error(
                execution_now,
                code,
                message,
                MessageErrorOperation::Publish,
                None,
                std::iter::empty(),
            );
        };
        structured_message_error(
            execution_now,
            code,
            message,
            site.operation,
            site.operation_index,
            site.fields.iter().cloned(),
        )
    }
}

/// The general error that fails a whole batch. It owns the acknowledgements of every row, including
/// those of the rows already rejected into `message_errors`.
fn batch_failure(
    mut acks: Vec<AckSet>,
    message_errors: Vec<PlannedMessageError>,
    reason: String,
) -> Report<PlannedGeneralError> {
    for planned in message_errors {
        acks.push(planned.message.acks);
    }
    Report::new(PlannedGeneralError { acks, reason })
}

/// The value of one row of a string column a request field was evaluated into, or `absent` when
/// the row holds none.
fn string_value(
    column: &StringArray,
    row: usize,
    absent: HttpRequestFieldError,
) -> Result<&str, Report<HttpRequestFieldError>> {
    if row >= column.len() || column.is_null(row) {
        return Err(Report::new(absent));
    }
    Ok(column.value(row))
}

/// Where each field of a lowered request program reports a failure.
struct RequestFieldSites {
    sites: Vec<CompiledMessageErrorSite>,
    positions: BTreeMap<VmSpan, usize>,
}

impl RequestFieldSites {
    /// The sites of `parsed`: the method and the path report the `publish` operation with their
    /// own field, and each header write reports `invoke` with its zero-based position. Each names
    /// the fields its expression reads as well.
    fn of(
        parsed: &nervix_vm::program::SpannedNode<nervix_vm::program::Program>,
    ) -> RuntimeVmCompileResult<Self> {
        let [(_, method), (_, path)] = parsed.inner.set.as_slice() else {
            return Err(Report::new(
                RuntimeVmCompileError::MessageErrorSetCountMismatch {
                    operations: 2,
                    assignments: parsed.inner.set.len(),
                },
            ));
        };
        let mut sites = Vec::with_capacity(
            parsed
                .inner
                .invoke
                .len()
                .checked_add(FIRST_HEADER_POSITION)
                .assured("the header writes belong to one construction held in memory"),
        );
        let mut positions = BTreeMap::new();
        for (field, expression) in [(METHOD_FIELD, method), (PATH_FIELD, path)] {
            let mut fields = vec![FieldPath::new(field)];
            collect_expression_field_paths(expression, &mut fields);
            positions.insert(expression.span, sites.len());
            sites.push(CompiledMessageErrorSite {
                operation: MessageErrorOperation::Publish,
                operation_index: None,
                fields: SortedSet::from_unsorted(fields),
            });
        }
        for (index, invocation) in parsed.inner.invoke.iter().enumerate() {
            let mut fields = Vec::new();
            for argument in &invocation.inner.args {
                collect_expression_field_paths(argument, &mut fields);
            }
            let operation_index = u32::try_from(index).map_err(|_| {
                Report::new(RuntimeVmCompileError::MessageErrorOperationIndexOverflow {
                    operation: MessageErrorOperation::Invoke,
                    index,
                })
            })?;
            positions.insert(invocation.span, sites.len());
            sites.push(CompiledMessageErrorSite {
                operation: MessageErrorOperation::Invoke,
                operation_index: Some(operation_index),
                fields: SortedSet::from_unsorted(fields),
            });
        }
        Ok(Self { sites, positions })
    }
}

#[cfg(test)]
mod tests {
    use nervix_connector::ResolvedClientConfig;

    use super::*;
    use crate::runtime::test_fixtures::{construction, expression, named, test_schema};

    struct Row<'a> {
        id: &'a str,
        method: &'a str,
        path: &'a str,
        header_name: &'a str,
        header_value: &'a str,
        divisor: i64,
        padding: i64,
    }

    impl Row<'_> {
        fn valid(id: &str) -> Row<'_> {
            Row {
                id,
                method: "POST",
                path: "/v1/a/../events?tag=a&tag=b",
                header_name: "X-Custom",
                header_value: "custom value",
                divisor: 4,
                padding: 0,
            }
        }
    }

    fn source_schema() -> Arc<CompiledSchema> {
        test_schema(&[
            ("id", ParseAsType::String),
            ("method", ParseAsType::String),
            ("path", ParseAsType::String),
            ("header_name", ParseAsType::String),
            ("header_value", ParseAsType::String),
            ("divisor", ParseAsType::I64),
            ("padding", ParseAsType::I64),
        ])
    }

    /// One batch of source records, each with an acknowledgement root of its own, and the
    /// completion of each root in row order.
    fn source_batch(rows: &[Row<'_>]) -> (RelayRecordBatch, Vec<AckCompletion>) {
        let mut messages = Vec::with_capacity(rows.len());
        let mut completions = Vec::with_capacity(rows.len());
        for row in rows {
            let (acks, completion) = AckSet::root();
            messages.push(RelayMessage {
                key: None,
                record: test_runtime_row([
                    ("id".to_string(), RuntimeValue::String(row.id.to_string())),
                    (
                        "method".to_string(),
                        RuntimeValue::String(row.method.to_string()),
                    ),
                    (
                        "path".to_string(),
                        RuntimeValue::String(row.path.to_string()),
                    ),
                    (
                        "header_name".to_string(),
                        RuntimeValue::String(row.header_name.to_string()),
                    ),
                    (
                        "header_value".to_string(),
                        RuntimeValue::String(row.header_value.to_string()),
                    ),
                    ("divisor".to_string(), RuntimeValue::I64(row.divisor)),
                    ("padding".to_string(), RuntimeValue::I64(row.padding)),
                ]),
                acks,
            });
            completions.push(completion);
        }
        let batch = RelayRecordBatch::from_messages(source_schema(), messages)
            .expect("the test rows match the source schema");
        (batch, completions)
    }

    struct HttpSinkTestPlan {
        sink: HttpSinkPlan,
        request: nervix_vm::program::SpannedNode<nervix_vm::program::Program>,
    }

    fn sink(method: &str, path: &str, header_writes: &str) -> HttpSinkTestPlan {
        use nervix_models::{Assignment, AssignmentTarget};

        let construction = RouteConstruction {
            assignments: vec![
                Assignment {
                    target: AssignmentTarget::bare(named("method")),
                    value: expression(method),
                },
                Assignment {
                    target: AssignmentTarget::bare(named("path")),
                    value: expression(path),
                },
            ],
            invocations: construction(header_writes).invocations,
            ..RouteConstruction::default()
        };
        let request = lower_route_construction(
            &construction,
            SemanticScopePolicy::read_write("message", HTTP_REQUEST_NAMESPACE),
        )
        .expect("the test request fields must lower");
        HttpSinkTestPlan {
            sink: HttpSinkPlan {
                client: EmitterClientSpec {
                    name: named("api"),
                    config: ResolvedClientConfig::default(),
                },
                origin: HttpOrigin::parse("https://api.example.com")
                    .expect("the test origin has an HTTPS scheme and a host"),
            },
            request,
        }
    }

    fn compile(
        plan: &HttpSinkTestPlan,
        output: Option<Arc<CompiledSchema>>,
    ) -> CompiledHttpRequestFields {
        let source = source_schema();
        let materialized = HashMap::default();
        let lookups = HashMap::default();
        let branching = ResolvedBranching::unbranched();
        let output = output.map(|output| RuntimeVmSchema {
            schema: output.arrow_schema(),
            sensitivity: output.vm_sensitivity(),
        });
        CompiledHttpRequestFields::compile(
            &named("deliver"),
            &plan.sink,
            &plan.request,
            HttpRequestSchemas {
                input: RuntimeVmSchema {
                    schema: source.arrow_schema(),
                    sensitivity: source.vm_sensitivity(),
                },
                output,
            },
            RuntimeVmCompileContext {
                available_materialized_streams: &materialized,
                available_lookups: &lookups,
                current_branching: &branching,
                udfs: None,
            },
        )
        .expect("the test request fields compile")
    }

    #[test]
    fn request_field_type_failure_retains_the_vm_compile_report() {
        let source = source_schema();
        let materialized = HashMap::default();
        let lookups = HashMap::default();
        let branching = ResolvedBranching::unbranched();
        let plan = sink(
            "input.divisor",
            "'/events'",
            "INVOKE write_header('X-Test', 'value')",
        );
        let report = CompiledHttpRequestFields::compile(
            &named("deliver"),
            &plan.sink,
            &plan.request,
            HttpRequestSchemas {
                input: RuntimeVmSchema {
                    schema: source.arrow_schema(),
                    sensitivity: source.vm_sensitivity(),
                },
                output: None,
            },
            RuntimeVmCompileContext {
                available_materialized_streams: &materialized,
                available_lookups: &lookups,
                current_branching: &branching,
                udfs: None,
            },
        )
        .err()
        .expect("an I64 method cannot satisfy the exact STRING request contract");
        assert!(matches!(
            report.current_context(),
            HttpRequestFieldsError::Compile { emitter } if emitter.as_str() == "deliver"
        ));
        let compile = report
            .downcast_ref::<nervix_vm::CompileError>()
            .expect("the HTTP request context retains the VM compile cause");
        assert!(format!("{report:?}").contains(&compile.message));
    }

    fn field_paths(error: &StructuredMessageError) -> Vec<&str> {
        error.fields.iter().map(FieldPath::as_str).collect()
    }

    /// How the message error of one rejected row reports it.
    #[derive(Debug, PartialEq)]
    struct Rejection<'a> {
        id: RuntimeValue,
        code: MessageErrorCode,
        operation: MessageErrorOperation,
        operation_index: Option<u32>,
        fields: Vec<&'a str>,
    }

    #[nervix_primitives::test]
    async fn each_row_is_rejected_at_its_first_failed_field_and_the_rest_keep_their_requests() {
        let compiled = compile(
            &sink(
                "input.method",
                "concat(input.path, repeat('a', input.padding))",
                "INVOKE write_header('X-Id', input.id), write_header(input.header_name, \
                 input.header_value), write_header('X-Ratio', coalesce(TRY_CAST(100 / \
                 input.divisor AS STRING), 'none'))",
            ),
            None,
        );
        let (batch, completions) = source_batch(&[
            Row::valid("accepted"),
            Row {
                method: "TRACE",
                ..Row::valid("method")
            },
            Row {
                path: "/a/..//evil",
                ..Row::valid("path")
            },
            Row {
                path: "/",
                padding: 8192,
                ..Row::valid("long-path")
            },
            Row {
                header_name: "Host",
                ..Row::valid("reserved")
            },
            Row {
                header_value: "a\r\nb",
                ..Row::valid("injected")
            },
            Row {
                divisor: 0,
                ..Row::valid("ratio")
            },
            Row {
                method: "TRACE",
                divisor: 0,
                ..Row::valid("method-first")
            },
            Row {
                header_name: "x-ID",
                header_value: "",
                ..Row::valid("replaced")
            },
        ]);

        let prepared = compiled
            .prepare(
                &named("deliver"),
                batch,
                HttpRequestInput::Published,
                &HashMap::default(),
                Timestamp::from_unix_nanos(7),
            )
            .await
            .expect("every row is evaluated");

        let mut rejected = Vec::with_capacity(prepared.message_errors.len());
        for planned in &prepared.message_errors {
            let id = planned
                .message
                .record
                .value("id")
                .expect("the source record has an id")
                .expect("the id is set");
            rejected.push(Rejection {
                id,
                code: planned.error.code,
                operation: planned.error.operation,
                operation_index: planned.error.operation_index,
                fields: field_paths(&planned.error),
            });
        }
        let id = |value: &str| RuntimeValue::String(value.to_string());
        assert_eq!(
            rejected,
            vec![
                Rejection {
                    id: id("method"),
                    code: MessageErrorCode::Validation,
                    operation: MessageErrorOperation::Publish,
                    operation_index: None,
                    fields: vec!["input.method", "method"],
                },
                Rejection {
                    id: id("path"),
                    code: MessageErrorCode::Validation,
                    operation: MessageErrorOperation::Publish,
                    operation_index: None,
                    fields: vec!["input.padding", "input.path", "path"],
                },
                Rejection {
                    id: id("long-path"),
                    code: MessageErrorCode::Validation,
                    operation: MessageErrorOperation::Publish,
                    operation_index: None,
                    fields: vec!["input.padding", "input.path", "path"],
                },
                Rejection {
                    id: id("reserved"),
                    code: MessageErrorCode::Validation,
                    operation: MessageErrorOperation::Invoke,
                    operation_index: Some(1),
                    fields: vec!["input.header_name", "input.header_value"],
                },
                Rejection {
                    id: id("injected"),
                    code: MessageErrorCode::Validation,
                    operation: MessageErrorOperation::Invoke,
                    operation_index: Some(1),
                    fields: vec!["input.header_name", "input.header_value"],
                },
                Rejection {
                    id: id("ratio"),
                    code: MessageErrorCode::Evaluation,
                    operation: MessageErrorOperation::Invoke,
                    operation_index: Some(2),
                    fields: vec!["input.divisor"],
                },
                Rejection {
                    id: id("method-first"),
                    code: MessageErrorCode::Validation,
                    operation: MessageErrorOperation::Publish,
                    operation_index: None,
                    fields: vec!["input.method", "method"],
                },
            ]
        );
        for planned in &prepared.message_errors {
            assert!(
                planned.partial_output.is_none(),
                "a request without a body has no partial output"
            );
            assert!(
                !planned.error.message.contains("evil") && !planned.error.message.contains('\n'),
                "a message error never quotes a value: {}",
                planned.error.message
            );
        }

        let accepted = prepared.accepted.expect("two rows have valid requests");
        assert_eq!(accepted.batch.message_count(), 2);
        let [first, replaced] = accepted.requests.fields.as_slice() else {
            panic!("two rows have valid requests");
        };
        assert_eq!(first.method.as_str(), "POST");
        assert_eq!(first.target.as_str(), "/v1/events?tag=a&tag=b");
        let headers = first
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            headers,
            vec![
                ("x-custom", "custom value"),
                ("x-id", "accepted"),
                ("x-ratio", "25"),
            ]
        );
        let replaced = replaced
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            replaced,
            vec![("x-id", ""), ("x-ratio", "25")],
            "a later write replaces an earlier one without case, and an empty value is kept"
        );
        assert!(first.estimated_bytes() > 0);
        let Ok(rejected_later) = accepted.requests.rejected_record(&accepted.batch, 1) else {
            panic!("an admitted row has its source record");
        };
        assert_eq!(
            rejected_later.record.value("id").expect("readable id"),
            Some(RuntimeValue::String("replaced".to_string())),
            "a request without a body is its own source record"
        );
        assert!(rejected_later.partial_output.is_none());

        accepted.batch.ack_success();
        for planned in prepared.message_errors {
            planned.message.acks.no_ack("rejected by the test");
        }
        for completion in completions {
            assert!(matches!(
                completion.wait().await,
                AckOutcome::Ack | AckOutcome::NoAck(_)
            ));
        }
    }

    #[nervix_primitives::test]
    async fn a_codec_request_reads_its_source_row_and_its_finalized_record() {
        let output = test_schema(&[
            ("id", ParseAsType::String),
            ("payload", ParseAsType::String),
        ]);
        let compiled = compile(
            &sink(
                "'PUT'",
                "concat('/v1/', output.payload, '/', id)",
                "INVOKE write_header('X-Source', input.method), write_header('X-Message', \
                 message.payload)",
            ),
            Some(output.clone()),
        );
        let (source, _source_completions) = source_batch(&[
            Row::valid("first"),
            Row::valid("filtered"),
            Row {
                method: "GET",
                ..Row::valid("third")
            },
        ]);
        let records = SourceRecords::of(&source);
        let mut messages = Vec::new();
        let mut completions = Vec::new();
        for (id, payload) in [("first", "FIRST"), ("third", "a#b")] {
            let (acks, completion) = AckSet::root();
            messages.push(RelayMessage {
                key: None,
                record: test_runtime_row([
                    ("id".to_string(), RuntimeValue::String(id.to_string())),
                    (
                        "payload".to_string(),
                        RuntimeValue::String(payload.to_string()),
                    ),
                ]),
                acks,
            });
            completions.push(completion);
        }
        let published = RelayRecordBatch::from_messages(output, messages)
            .expect("the finalized rows match the codec schema");

        let prepared = compiled
            .prepare(
                &named("deliver"),
                published,
                HttpRequestInput::Source {
                    records,
                    rows: vec![0, 2],
                },
                &HashMap::default(),
                Timestamp::from_unix_nanos(7),
            )
            .await
            .expect("every row is evaluated");

        let accepted = prepared
            .accepted
            .expect("the first row has a valid request");
        let [request] = accepted.requests.fields.as_slice() else {
            panic!("one row has a valid request");
        };
        assert_eq!(request.method.as_str(), "PUT");
        assert_eq!(request.target.as_str(), "/v1/FIRST/first");
        let headers = request
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(headers, vec![("x-message", "FIRST"), ("x-source", "POST")]);

        // A request rejected after admission reads the source record it was prepared from and the
        // codec record it attempted.
        let Ok(rejected_later) = accepted.requests.rejected_record(&accepted.batch, 0) else {
            panic!("the admitted row has its source record");
        };
        assert_eq!(
            rejected_later
                .record
                .value("method")
                .expect("readable method"),
            Some(RuntimeValue::String("POST".to_string())),
            "only the source record holds the method"
        );
        let attempted = rejected_later
            .partial_output
            .expect("a codec request offers its attempted body");
        assert_eq!(attempted.batch().num_rows(), 1);
        assert!(rejected_later.materialized_state.is_empty());
        let Err(missing) = accepted.requests.rejected_record(&accepted.batch, 1) else {
            panic!("the batch admitted one row");
        };
        assert!(matches!(
            missing.current_context(),
            HttpRequestFieldsError::MissingSourceRecord { row: 1 }
        ));

        let [rejected] = prepared.message_errors.as_slice() else {
            panic!("the third source row's target has a fragment");
        };
        assert_eq!(rejected.error.operation, MessageErrorOperation::Publish);
        assert_eq!(
            field_paths(&rejected.error),
            vec!["message.id", "output.payload", "path"]
        );
        assert_eq!(
            rejected.message.record.value("id").expect("readable id"),
            Some(RuntimeValue::String("third".to_string())),
            "the message error names the original source record"
        );
        let partial_output = rejected
            .partial_output
            .as_ref()
            .expect("a codec request offers its attempted body");
        assert_eq!(partial_output.batch().num_rows(), 1);
        assert!(
            partial_output
                .batch()
                .schema()
                .fields()
                .iter()
                .all(|field| field.is_nullable())
        );
        drop(completions);
    }
}
