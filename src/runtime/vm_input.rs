use super::*;

pub(super) struct VmInputProjectionSources<'a> {
    pub(super) carrier: &'a RuntimeRecordBatch,
    pub(super) namespace_batches: &'a [(&'a str, &'a RuntimeRecordBatch)],
    pub(super) strict_namespaces: &'a [&'a str],
    pub(super) keys: &'a [Option<BranchKey>],
    pub(super) side_inputs: &'a HashMap<String, RuntimeValue>,
    pub(super) ingest_metadata: Option<&'a IngestFilterMapMetadata>,
    pub(super) lookup_columns: &'a HashMap<String, VmTypedArray>,
    pub(super) uninitialized: Option<&'a VmUninitializedInput>,
}

impl<'a> VmInputProjectionSources<'a> {
    pub(super) fn namespace_batch_field<'name>(
        &self,
        qualified_name: &'name str,
    ) -> Option<(&'a RuntimeRecordBatch, &'name str)> {
        /// The longest namespace matched so far, with the batch it names and the field left after
        /// stripping it. A longer namespace always wins, so its length is compared first.
        struct NamespaceMatch<'batch, 'field> {
            namespace_len: usize,
            batch: &'batch RuntimeRecordBatch,
            field_name: &'field str,
        }

        let mut best_match: Option<NamespaceMatch<'a, 'name>> = None;
        for &(namespace, batch) in self.namespace_batches {
            let Some(suffix) = qualified_name.strip_prefix(namespace) else {
                continue;
            };
            let Some(field_name) = suffix.strip_prefix('.') else {
                continue;
            };
            match &best_match {
                Some(best) if best.namespace_len >= namespace.len() => {}
                _ => {
                    best_match = Some(NamespaceMatch {
                        namespace_len: namespace.len(),
                        batch,
                        field_name,
                    });
                }
            }
        }
        best_match.map(|best| (best.batch, best.field_name))
    }

    pub(super) fn has_strict_namespace(&self, qualified_name: &str) -> bool {
        self.strict_namespaces.iter().any(|namespace| {
            qualified_name
                .strip_prefix(namespace)
                .is_some_and(|suffix| suffix.starts_with('.'))
        })
    }
}

/// Input columns of one dispatched batch that every output route projects identically.
///
/// Branch columns, broadcast materialized values, and selected ingest-metadata columns are
/// derived from the batch alone, so a fan-out node builds each of them once rather than once per
/// route. Carrier columns already share their Arrow buffers and need no cache.
#[derive(Default)]
pub(super) struct SharedVmInputColumns {
    pub(super) columns: HashMap<SharedVmInputColumnKey, VmTypedArray>,
}

/// The identity of a shared input column. Routes may request the same name with a different Arrow
/// type or nullability, so the resolved field is part of the identity rather than the name alone.
#[derive(PartialEq, Eq, Hash)]
pub(super) struct SharedVmInputColumnKey {
    pub(super) name: String,
    pub(super) data_type: ArrowDataType,
    pub(super) nullable: bool,
}

/// Per-batch caches shared by every output route's program execution.
#[derive(Default)]
pub(super) struct SharedBatchColumns {
    pub(super) inputs: SharedVmInputColumns,
    pub(super) lookups: BTreeMap<LookupHashMapCallKey, VmTypedArray>,
}

impl SharedVmInputColumns {
    /// Returns the shared column for `field`, building it on first use.
    ///
    /// Routes may request the same name with a different Arrow type or nullability, so the
    /// resolved field is part of the identity rather than the name alone.
    pub(super) fn column(
        &mut self,
        field: &arrow_schema::Field,
        build: impl FnOnce() -> Result<VmTypedArray, String>,
    ) -> Result<VmTypedArray, String> {
        let key = SharedVmInputColumnKey {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
        };
        if let Some(column) = self.columns.get(&key) {
            return Ok(column.clone());
        }
        let column = build()?;
        self.columns.insert(key, column.clone());
        Ok(column)
    }
}

pub(super) fn project_vm_input_batch(
    schema: &StdArc<arrow_schema::Schema>,
    sources: &VmInputProjectionSources<'_>,
    mut shared: Option<&mut SharedVmInputColumns>,
) -> Result<VmTypedBatch, String> {
    let row_count = sources.carrier.batch().num_rows();
    if sources.keys.len() != row_count {
        return Err(format!(
            "branch key count {} does not match batch row count {row_count}",
            sources.keys.len()
        ));
    }
    if let Some(metadata) = sources.ingest_metadata
        && metadata.len() != row_count
    {
        return Err(format!(
            "ingest metadata count {} does not match batch row count {row_count}",
            metadata.len()
        ));
    }
    for (namespace, batch) in sources.namespace_batches {
        if batch.batch().num_rows() != row_count {
            return Err(format!(
                "namespace '{namespace}' batch row count {} does not match carrier row count \
                 {row_count}",
                batch.batch().num_rows()
            ));
        }
    }
    let carrier_schema = sources.carrier.schema();
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        columns.push(project_vm_input_column(
            field,
            sources,
            &carrier_schema,
            row_count,
            shared.as_deref_mut(),
        )?);
    }
    VmTypedBatch::try_new(schema.clone(), columns).map_err(|error| error.to_string())
}

pub(super) fn project_vm_input_column(
    field: &arrow_schema::Field,
    sources: &VmInputProjectionSources<'_>,
    carrier_schema: &StdArc<arrow_schema::Schema>,
    row_count: usize,
    shared: Option<&mut SharedVmInputColumns>,
) -> Result<VmTypedArray, String> {
    if let Some(uninitialized) = sources.uninitialized
        && uninitialized.contains(field)
    {
        return Ok(VmTypedArray::uninitialized(
            field.data_type().clone(),
            row_count,
        ));
    }
    if let Some(column) = sources.lookup_columns.get(field.name()) {
        return Ok(column.clone());
    }
    if let Ok(index) = carrier_schema.index_of(field.name()) {
        return carrier_input_column(sources.carrier, index, field);
    }
    if let Some(value) = sources.side_inputs.get(field.name()) {
        let build = || {
            runtime_values_input_column(
                std::iter::repeat_n(Some(value), row_count),
                row_count,
                field,
            )
        };
        return match shared {
            Some(shared) => shared.column(field, build),
            None => build(),
        };
    }
    if let Some((namespace, field_name)) = field.name().split_once('.') {
        if namespace == INGEST_METADATA_NAMESPACE {
            let build = || {
                if let Some(column) = sources
                    .ingest_metadata
                    .map(|metadata| metadata.field_column(field_name))
                    .transpose()?
                    .flatten()
                {
                    if column.data_type() != field.data_type() {
                        return Err(format!(
                            "ingest metadata field '{}' expected {:?}, found {:?}",
                            field.name(),
                            field.data_type(),
                            column.data_type()
                        ));
                    }
                    return VmTypedArray::try_from_array_ref(column)
                        .map_err(|error| error.to_string());
                }
                runtime_values_input_column(std::iter::repeat_n(None, row_count), row_count, field)
            };
            return match shared {
                Some(shared) => shared.column(field, build),
                None => build(),
            };
        }
        if namespace == BRANCH_NAMESPACE {
            let build = || branch_key_input_column(sources.keys, field_name, field);
            return match shared {
                Some(shared) => shared.column(field, build),
                None => build(),
            };
        }
        if let Some((batch, namespaced_field)) = sources.namespace_batch_field(field.name())
            && let Ok(index) = batch.schema().index_of(namespaced_field)
        {
            return carrier_input_column(batch, index, field);
        }
        if sources.has_strict_namespace(field.name()) {
            if field.is_nullable() {
                return runtime_values_input_column(
                    std::iter::repeat_n(None, row_count),
                    row_count,
                    field,
                );
            }
            return Err(format!(
                "FILTER-MAP input record is missing field '{}'",
                field.name()
            ));
        }
        if namespace != INGEST_METADATA_NAMESPACE
            && let Ok(index) = carrier_schema.index_of(field_name)
        {
            return carrier_input_column(sources.carrier, index, field);
        }
    }
    if field.is_nullable() {
        return runtime_values_input_column(std::iter::repeat_n(None, row_count), row_count, field);
    }
    Err(format!(
        "FILTER-MAP input record is missing field '{}'",
        field.name()
    ))
}

pub(super) fn carrier_input_column(
    carrier: &RuntimeRecordBatch,
    index: usize,
    field: &arrow_schema::Field,
) -> Result<VmTypedArray, String> {
    let column = carrier.batch().column(index);
    if column.data_type() != field.data_type() {
        return Err(format!(
            "input field '{}' expected {:?}, found carrier column type {:?}",
            field.name(),
            field.data_type(),
            column.data_type()
        ));
    }
    VmTypedArray::try_from_array_ref(column.clone()).map_err(|error| error.to_string())
}

pub(super) fn branch_key_input_column(
    keys: &[Option<BranchKey>],
    field_name: &str,
    field: &arrow_schema::Field,
) -> Result<VmTypedArray, String> {
    runtime_values_input_column(
        keys.iter()
            .map(|key| key.as_ref().and_then(|key| key.field_value(field_name))),
        keys.len(),
        field,
    )
}

pub(super) fn runtime_values_input_column<'a>(
    values: impl Iterator<Item = Option<&'a RuntimeValue>>,
    len: usize,
    field: &arrow_schema::Field,
) -> Result<VmTypedArray, String> {
    if let ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some(tz)) =
        field.data_type()
        && (tz.as_ref() == "+00:00" || tz.as_ref() == "UTC")
    {
        let nanos = values
            .map(|value| match value {
                Some(RuntimeValue::Datetime(value)) => {
                    value.timestamp_nanos_opt().map(Some).ok_or_else(|| {
                        format!(
                            "FILTER-MAP input field '{}' datetime is out of nanosecond range",
                            field.name()
                        )
                    })
                }
                Some(value) => Err(format!(
                    "FILTER-MAP input field '{}' expected {:?}, got {}",
                    field.name(),
                    field.data_type(),
                    runtime_value_type_name(value)
                )),
                None => Ok(None),
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(VmTypedArray::Datetime(
            nanos
                .into_iter()
                .collect::<arrow_array::TimestampNanosecondArray>()
                .with_timezone_utc(),
        ));
    }
    let mut builder = make_builder(field.data_type(), len);
    for value in values {
        append_filter_map_nested_value(builder.as_mut(), field.data_type(), value, field)?;
    }
    VmTypedArray::try_from_array_ref(builder.finish()).map_err(|error| error.to_string())
}

pub(super) fn relay_state_snapshot_from_side_inputs(
    side_inputs: &HashMap<String, RuntimeValue>,
) -> HashMap<String, RuntimeValue> {
    side_inputs
        .iter()
        .filter(|(name, _)| name.starts_with("relay_state."))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

pub(super) fn lookup_generated_input_field<'a>(
    program: &'a CompiledProgramWithMaterializedInterest,
    call_index: usize,
    name: &str,
) -> Option<&'a arrow_schema::Field> {
    if let Ok(index) = program.compiled.input_schema.index_of(name) {
        return Some(program.compiled.input_schema.field(index));
    }
    program.lookup_hash_maps[call_index + 1..]
        .iter()
        .find_map(|call| {
            call.key_program
                .input_schema
                .index_of(name)
                .ok()
                .map(|index| call.key_program.input_schema.field(index))
        })
}

pub(super) async fn compute_lookup_hash_map_columns(
    program: &CompiledProgramWithMaterializedInterest,
    inputs: &FilterMapBatchInputs<'_>,
    execution_now: Timestamp,
    mut shared_calls: Option<&mut BTreeMap<LookupHashMapCallKey, VmTypedArray>>,
) -> Result<HashMap<String, VmTypedArray>, String> {
    let mut lookup_columns = HashMap::new();
    if program.lookup_hash_maps.is_empty() {
        return Ok(lookup_columns);
    }
    let row_count = inputs.carrier.batch().num_rows();
    for (call_index, call) in program.lookup_hash_maps.iter().enumerate() {
        let generated_name = VmCompileNamespace::Internal(InternalFieldNamespace::LookupHashMap)
            .qualified_field_name(&call.generated_field);
        let Some(field) = lookup_generated_input_field(program, call_index, &generated_name) else {
            continue;
        };
        // Routes of one node often spell the same lookup. The generated field name is derived from
        // the call's position within its own program, so a shared result is looked up by the call's
        // identity and reinserted under this route's name. The Arrow field is part of the identity
        // because routes may resolve the same lookup at a different type or nullability.
        let shared_key = LookupHashMapCallKey {
            lookup: call.lookup.clone(),
            lookup_field: call.lookup_field.clone(),
            key_expr: call.key_expr.clone(),
        };
        if let Some(shared) = shared_calls.as_deref()
            && let Some(column) = shared.get(&shared_key)
            && column.data_type() == *field.data_type()
        {
            lookup_columns.insert(generated_name, column.clone());
            continue;
        }
        let uninitialized = VmUninitializedInput {
            fields: call
                .key_program
                .input_schema
                .fields()
                .iter()
                .filter(|field| field.name().starts_with("output."))
                .map(|field| field.name().clone())
                .collect(),
        };
        let vm_batch = project_vm_input_batch(
            &call.key_program.input_schema,
            &VmInputProjectionSources {
                carrier: inputs.carrier,
                namespace_batches: inputs.namespace_batches,
                strict_namespaces: &[],
                keys: inputs.keys,
                side_inputs: inputs.side_inputs,
                ingest_metadata: inputs.ingest_metadata,
                lookup_columns: &lookup_columns,
                uninitialized: Some(&uninitialized),
            },
            None,
        )?;
        let result = execute_program_with_selection_in_context(
            &call.key_program,
            &vm_batch,
            &VmExecutionContext {
                now: execution_now,
                injector: None,
            },
        )
        .await
        .map_err(|error| {
            format!(
                "LOOKUP_HASH_MAP key execution failed for hash map '{}' field '{}': {}",
                call.lookup.as_str(),
                call.lookup_field,
                error
            )
        })?;
        let key_column = result
            .batch
            .schema()
            .index_of(&call.generated_field)
            .ok()
            .map(|index| {
                let array = result.batch.column(index).to_array_ref();
                parse_as_type_from_arrow(array.data_type()).map(|ty| (array, ty))
            })
            .transpose()?;
        let mut row_keys: Vec<Option<String>> = vec![None; row_count];
        for (output_row, input_row) in result.selected_rows.iter().enumerate() {
            if let Some(side_error) = result.batch.errors().row(output_row).first() {
                return Err(format!(
                    "LOOKUP_HASH_MAP key side error {}: {} at {}",
                    side_error.code.as_str(),
                    side_error.message,
                    side_error.span
                ));
            }
            let Some((array, ty)) = key_column.as_ref() else {
                continue;
            };
            if let Some(value) = runtime_value_from_arrow_array(
                array.as_ref(),
                ty,
                true,
                output_row,
                &call.generated_field,
            )? {
                row_keys[input_row] = Some(value.to_key_fragment());
            }
        }
        let lookup_values = row_keys
            .iter()
            .map(|key| {
                let Some(row) = key
                    .as_deref()
                    .and_then(|key| call.lookup_runtime.entries.get(key))
                    .copied()
                else {
                    return Ok(None);
                };
                call.lookup_runtime.batch.value(row, &call.lookup_field)
            })
            .collect::<Result<Vec<_>, String>>()?;
        let column = runtime_values_input_column(
            lookup_values.iter().map(Option::as_ref),
            row_count,
            field,
        )?;
        if let Some(shared) = shared_calls.as_deref_mut() {
            shared.insert(shared_key, column.clone());
        }
        lookup_columns.insert(generated_name, column);
    }
    Ok(lookup_columns)
}

pub(super) fn vm_output_value(
    batch: &VmTypedBatch,
    row: usize,
    field_name: &str,
) -> Result<Option<RuntimeValue>, String> {
    let column_index = match batch.schema().index_of(field_name) {
        Ok(index) => index,
        Err(_) => return Ok(None),
    };
    let field = batch.schema().field(column_index);
    let array = batch.column(column_index).to_array_ref();
    runtime_value_from_arrow_array(
        array.as_ref(),
        &parse_as_type_from_arrow(field.data_type())?,
        field.is_nullable(),
        row,
        field_name,
    )
}

pub(super) fn vm_typed_batch_to_runtime_batch(
    batch: &VmTypedBatch,
) -> Result<RuntimeRecordBatch, String> {
    let record_batch = batch.to_record_batch().map_err(|error| error.to_string())?;
    RuntimeRecordBatch::from_record_batch(batch.schema().clone(), record_batch)
}

pub(super) fn vm_typed_batch_selected_rows_to_runtime_batch(
    batch: &VmTypedBatch,
    selected_rows: &[usize],
) -> Result<RuntimeRecordBatch, String> {
    if selected_rows.len() == batch.row_count() {
        return vm_typed_batch_to_runtime_batch(batch);
    }
    let selected = selected_rows.iter().copied().collect::<HashSet<_>>();
    let predicate =
        BooleanArray::from_iter((0..batch.row_count()).map(|row| Some(selected.contains(&row))));
    let columns = batch
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .map(|(column, field)| {
            let column = filter_arrow_array(column.to_array_ref().as_ref(), &predicate)
                .map_err(|error| error.to_string())?;
            if !field.is_nullable() && column.null_count() > 0 {
                return Err(format!(
                    "required output column '{}' contains null values",
                    field.name()
                ));
            }
            Ok(column)
        })
        .collect::<Result<Vec<_>, String>>()?;
    let record_batch = if columns.is_empty() {
        RecordBatch::try_new_with_options(
            batch.schema().clone(),
            columns,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(selected_rows.len())),
        )
    } else {
        RecordBatch::try_new(batch.schema().clone(), columns)
    }
    .map_err(|error| error.to_string())?;
    RuntimeRecordBatch::from_record_batch(batch.schema().clone(), record_batch)
}

pub(super) fn runtime_value_type_name(value: &RuntimeValue) -> &'static str {
    match value {
        RuntimeValue::U8(_) => "U8",
        RuntimeValue::I8(_) => "I8",
        RuntimeValue::U16(_) => "U16",
        RuntimeValue::I16(_) => "I16",
        RuntimeValue::U32(_) => "U32",
        RuntimeValue::I32(_) => "I32",
        RuntimeValue::U64(_) => "U64",
        RuntimeValue::I64(_) => "I64",
        RuntimeValue::Bool(_) => "BOOL",
        RuntimeValue::String(_) => "STRING",
        RuntimeValue::Datetime(_) => "DATETIME",
        RuntimeValue::F32(_) => "F32",
        RuntimeValue::F64(_) => "F64",
        RuntimeValue::Array(_) => "ARRAY",
        RuntimeValue::Vec(_) => "VEC",
    }
}
