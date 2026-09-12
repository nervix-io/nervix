use super::*;

pub(super) const INGEST_METADATA_NAMESPACE: &str = "metadata";

pub(super) const BRANCH_NAMESPACE: &str = "branch";

#[derive(Debug)]
pub(super) struct IngestFilterMapMetadataColumns {
    pub(super) integration_fields: RecordBatch,
    pub(super) header_names: ArrayRef,
    pub(super) header_values: ArrayRef,
}

pub(super) struct IngestHeaderRow<'a> {
    pub(super) names: &'a StringArray,
    pub(super) values: &'a StringArray,
    pub(super) start: usize,
    pub(super) end: usize,
}

impl<'a> IngestHeaderRow<'a> {
    pub(super) fn first(&self, name: &str) -> Option<&'a str> {
        (self.start..self.end)
            .find(|index| self.names.value(*index) == name)
            .map(|index| self.values.value(index))
    }

    pub(super) fn visit(&self, name: &str, mut visit: impl FnMut(&'a str)) {
        for index in self.start..self.end {
            if self.names.value(index) == name {
                visit(self.values.value(index));
            }
        }
    }
}

/// Columnar metadata for one ingest group.
///
/// The Arrow columns are shared by every filtered or single-row view. `rows` is the
/// logical-to-physical row projection, so filtering and message-error attribution do
/// not copy transport headers or rebuild integration fields from scalar values.
#[derive(Debug, Clone)]
pub(crate) struct IngestFilterMapMetadata {
    pub(super) columns: Arc<IngestFilterMapMetadataColumns>,
    pub(super) rows: Arc<Vec<usize>>,
}

/// The ingest-metadata columns a source exposes, fixed when its ingestor starts.
///
/// Every source kind maps to exactly one variant, so a builder set is never chosen from
/// the contents of a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum IngestMetadataKind {
    Kafka,
    Syslog,
    Headers,
}

impl IngestMetadataKind {
    #[cfg(test)]
    pub(super) fn for_source(source: &IngestSource) -> Self {
        match source {
            IngestSource::Kafka { .. } => Self::Kafka,
            IngestSource::Syslog { .. } => Self::Syslog,
            _ => Self::Headers,
        }
    }

    /// The `metadata` namespace this source kind exposes to programs, or `None` when it
    /// exposes transport headers only.
    pub(super) fn integration_arrow_schema(self) -> Option<StdArc<arrow_schema::Schema>> {
        match self {
            Self::Kafka => Some(StdArc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("topic", ArrowDataType::Utf8, true),
                arrow_schema::Field::new("partition", ArrowDataType::Int32, true),
                arrow_schema::Field::new("offset", ArrowDataType::Int64, true),
            ]))),
            Self::Syslog => Some(StdArc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("peer_addr", ArrowDataType::Utf8, true),
            ]))),
            Self::Headers => None,
        }
    }
}

/// Transport headers of one source message, visited in arrival order.
///
/// Connectors implement this over their own borrowed message so a group's header builders
/// are appended from the source directly, without an owned header vector per message.
pub(crate) trait IngestMessageHeaders: Send + Sync {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str));
}

/// A source message that carries no transport headers.
pub(in crate::runtime) struct NoIngestHeaders;

impl IngestMessageHeaders for NoIngestHeaders {
    fn visit(&self, _visit: &mut dyn FnMut(&str, &str)) {}
}

/// Transport headers copied out of a source message so they outlive it.
///
/// Only the paths that append after their source message is gone retain headers: the
/// quiesce buffer replays payloads long after the message was dropped, and a WebSocket
/// session carries its handshake headers into every later frame. Every other connector
/// appends from the borrowed message instead.
#[derive(Clone, Debug)]
pub(crate) struct RetainedIngestHeaders(pub(super) Vec<(String, String)>);

impl RetainedIngestHeaders {
    /// Copies the headers a source message carries right now.
    pub(crate) fn capture(headers: &dyn IngestMessageHeaders) -> Self {
        let mut retained = Vec::new();
        headers.visit(&mut |name, value| retained.push((name.to_string(), value.to_string())));
        Self(retained)
    }

    /// The headers of a source that carries none.
    pub(in crate::runtime) fn none() -> Self {
        Self(Vec::new())
    }
}

impl IngestMessageHeaders for RetainedIngestHeaders {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        for (name, value) in &self.0 {
            visit(name, value);
        }
    }
}

/// One message's ingest metadata, read from its source message.
///
/// The variant must match the kind the ingestor started with; the fields borrow the source
/// message so appending a row copies bytes into Arrow buffers and nothing else.
pub(in crate::runtime) enum IngestMetadataRow<'a> {
    Kafka {
        topic: &'a str,
        partition: i32,
        offset: i64,
        headers: &'a dyn IngestMessageHeaders,
    },
    Syslog {
        peer_addr: std::net::SocketAddr,
    },
    Headers {
        headers: &'a dyn IngestMessageHeaders,
    },
}

impl IngestMetadataRow<'_> {
    pub(super) fn kind(&self) -> IngestMetadataKind {
        match self {
            Self::Kafka { .. } => IngestMetadataKind::Kafka,
            Self::Syslog { .. } => IngestMetadataKind::Syslog,
            Self::Headers { .. } => IngestMetadataKind::Headers,
        }
    }
}

/// The `topic`, `partition` and `offset` builders of one Kafka ingest group.
pub(super) struct KafkaIntegrationBuilders {
    pub(super) topic: StringBuilder,
    pub(super) partition: Int32Builder,
    pub(super) offset: Int64Builder,
}

/// The integration-field builders of one ingest group, one variant per source kind.
///
/// The Kafka columns live behind one allocation taken when the group opens, so the variants
/// stay comparable in size.
pub(super) enum IngestIntegrationBuilders {
    Kafka(Box<KafkaIntegrationBuilders>),
    Syslog { peer_addr: StringBuilder },
    Headers,
}

/// Arrow builders for one ingest group's metadata columns.
///
/// A group opens one set with its first row, appends one row per decoded message, and
/// finishes it once when the group closes. No Arrow array, batch or builder is created per
/// message, and no one-row batch is ever concatenated.
pub(super) struct IngestMetadataBuilders {
    pub(super) kind: IngestMetadataKind,
    pub(super) integration: IngestIntegrationBuilders,
    pub(super) header_names: ListBuilder<StringBuilder>,
    pub(super) header_values: ListBuilder<StringBuilder>,
    pub(super) rows: usize,
}

// Counted per thread so a test observes only the groups it opened itself, while the rest of
// the suite exercises the same builders in parallel.
#[cfg(test)]
thread_local! {
    pub(super) static INGEST_METADATA_BUILDER_SETS_OPENED: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(super) static INGEST_METADATA_COLUMN_SETS_BUILT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

impl IngestMetadataBuilders {
    /// Opens a builder set for `kind`, sizing the integration fields for `row_bound` rows.
    ///
    /// Header builders start empty because most sources carry no headers at all.
    pub(super) fn new(kind: IngestMetadataKind, row_bound: usize) -> Self {
        #[cfg(test)]
        INGEST_METADATA_BUILDER_SETS_OPENED.with(|count| count.set(count.get() + 1));
        let integration = match kind {
            IngestMetadataKind::Kafka => {
                IngestIntegrationBuilders::Kafka(Box::new(KafkaIntegrationBuilders {
                    topic: StringBuilder::with_capacity(row_bound, 0),
                    partition: Int32Builder::with_capacity(row_bound),
                    offset: Int64Builder::with_capacity(row_bound),
                }))
            }
            IngestMetadataKind::Syslog => IngestIntegrationBuilders::Syslog {
                peer_addr: StringBuilder::with_capacity(row_bound, 0),
            },
            IngestMetadataKind::Headers => IngestIntegrationBuilders::Headers,
        };
        let item_field =
            || StdArc::new(arrow_schema::Field::new("item", ArrowDataType::Utf8, false));
        Self {
            kind,
            integration,
            header_names: ListBuilder::new(StringBuilder::new()).with_field(item_field()),
            header_values: ListBuilder::new(StringBuilder::new()).with_field(item_field()),
            rows: 0,
        }
    }

    pub(super) fn append(&mut self, row: &IngestMetadataRow<'_>) -> Result<(), String> {
        let headers = match (&mut self.integration, row) {
            (
                IngestIntegrationBuilders::Kafka(kafka),
                IngestMetadataRow::Kafka {
                    topic,
                    partition,
                    offset,
                    headers,
                },
            ) => {
                kafka.topic.append_value(topic);
                kafka.partition.append_value(*partition);
                kafka.offset.append_value(*offset);
                Some(*headers)
            }
            (
                IngestIntegrationBuilders::Syslog { peer_addr },
                IngestMetadataRow::Syslog {
                    peer_addr: source_peer_addr,
                },
            ) => {
                // The address renders straight into the builder's value buffer, so a peer
                // address does not allocate a string per message.
                use std::fmt::Write as _;
                write!(peer_addr, "{source_peer_addr}")
                    .assured("fmt::Write over an in-memory string builder has no failure mode");
                peer_addr.append_value("");
                None
            }
            (IngestIntegrationBuilders::Headers, IngestMetadataRow::Headers { headers }) => {
                Some(*headers)
            }
            (_, row) => {
                return Err(format!(
                    "ingest metadata builders for {:?} cannot append a {:?} row",
                    self.kind,
                    row.kind()
                ));
            }
        };
        if let Some(headers) = headers {
            let (names, values) = (&mut self.header_names, &mut self.header_values);
            headers.visit(&mut |name, value| {
                names.values().append_value(name);
                values.values().append_value(value);
            });
        }
        self.header_names.append(true);
        self.header_values.append(true);
        self.rows += 1;
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<IngestFilterMapMetadata, String> {
        #[cfg(test)]
        INGEST_METADATA_COLUMN_SETS_BUILT.with(|count| count.set(count.get() + 1));
        let rows = self.rows;
        let integration_columns = match &mut self.integration {
            IngestIntegrationBuilders::Kafka(kafka) => {
                let topic: ArrayRef = StdArc::new(kafka.topic.finish());
                let partition: ArrayRef = StdArc::new(kafka.partition.finish());
                let offset: ArrayRef = StdArc::new(kafka.offset.finish());
                vec![topic, partition, offset]
            }
            IngestIntegrationBuilders::Syslog { peer_addr } => {
                let peer_addr: ArrayRef = StdArc::new(peer_addr.finish());
                vec![peer_addr]
            }
            IngestIntegrationBuilders::Headers => Vec::new(),
        };
        let schema = self
            .kind
            .integration_arrow_schema()
            .unwrap_or_else(|| StdArc::new(arrow_schema::Schema::empty()));
        let integration_fields = RecordBatch::try_new_with_options(
            schema,
            integration_columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|error| error.to_string())?;
        let header_names: ArrayRef = StdArc::new(self.header_names.finish());
        let header_values: ArrayRef = StdArc::new(self.header_values.finish());
        if header_names.len() != rows || header_values.len() != rows {
            return Err(format!(
                "ingest metadata built {rows} integration rows, {} header-name rows and {} \
                 header-value rows",
                header_names.len(),
                header_values.len()
            ));
        }
        Ok(IngestFilterMapMetadata {
            columns: Arc::new(IngestFilterMapMetadataColumns {
                integration_fields,
                header_names,
                header_values,
            }),
            rows: Arc::new((0..rows).collect()),
        })
    }
}

impl IngestFilterMapMetadata {
    pub(super) fn selected_array(&self, array: &ArrayRef) -> Result<ArrayRef, String> {
        if self.rows.iter().copied().eq(0..self.rows.len()) && self.rows.len() == array.len() {
            return Ok(array.clone());
        }
        let indices = self
            .rows
            .iter()
            .map(|row| {
                if *row >= array.len() {
                    return Err(format!(
                        "ingest metadata row {row} is outside column with {} rows",
                        array.len()
                    ));
                }
                Ok::<u64, String>((*row).arch_into())
            })
            .collect::<Result<UInt64Array, _>>()?;
        take_arrow_array(array.as_ref(), &indices, None).map_err(|error| error.to_string())
    }

    pub(super) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(super) fn row(&self, row: usize) -> Option<Self> {
        self.rows.get(row).copied().map(|physical_row| Self {
            columns: self.columns.clone(),
            rows: Arc::new(vec![physical_row]),
        })
    }

    pub(super) fn select(&self, keep: &[bool]) -> Result<Self, String> {
        if keep.len() != self.len() {
            return Err(format!(
                "ingest metadata selection has {} rows for {} metadata rows",
                keep.len(),
                self.len()
            ));
        }
        Ok(Self {
            columns: self.columns.clone(),
            rows: Arc::new(
                self.rows
                    .iter()
                    .zip(keep)
                    .filter_map(|(row, keep)| keep.then_some(*row))
                    .collect(),
            ),
        })
    }

    pub(super) fn field_column(&self, name: &str) -> Result<Option<ArrayRef>, String> {
        let Ok(index) = self.columns.integration_fields.schema().index_of(name) else {
            return Ok(None);
        };
        self.selected_array(self.columns.integration_fields.column(index))
            .map(Some)
    }

    pub(super) fn first_header(&self, row: usize, name: &str) -> Option<&str> {
        self.header_row(row)?.first(name)
    }

    pub(super) fn visit_header_values(&self, row: usize, name: &str, visit: impl FnMut(&str)) {
        if let Some(headers) = self.header_row(row) {
            headers.visit(name, visit);
        }
    }

    /// The header names and values carried for `row`, or `None` when this batch carries none.
    ///
    /// Ingestion metadata is optional per connector, so a batch whose header columns are absent or
    /// hold another Arrow type simply has no headers to read, and a row past the end of the
    /// selection is not part of this view. The offsets are Arrow list offsets, which are
    /// non-negative by construction; a negative one would mean a corrupt array, and reading no
    /// headers is the only answer this view can give for it.
    pub(super) fn header_row(&self, row: usize) -> Option<IngestHeaderRow<'_>> {
        let physical_row = *self.rows.get(row)?;
        let names = self
            .columns
            .header_names
            .as_any()
            .downcast_ref::<ListArray>()?;
        let values = self
            .columns
            .header_values
            .as_any()
            .downcast_ref::<ListArray>()?;
        Some(IngestHeaderRow {
            names: names.values().as_any().downcast_ref::<StringArray>()?,
            values: values.values().as_any().downcast_ref::<StringArray>()?,
            start: usize::try_from(*names.value_offsets().get(physical_row)?).ok()?,
            end: usize::try_from(*names.value_offsets().get(physical_row + 1)?).ok()?,
        })
    }
}

#[derive(Debug)]
pub(super) struct IngestHeaderFunctionInjector {
    pub(super) metadata: Option<IngestFilterMapMetadata>,
    pub(super) row_count: usize,
}

impl IngestHeaderFunctionInjector {
    pub(super) fn from_metadata(
        metadata: Option<&IngestFilterMapMetadata>,
        row_count: usize,
    ) -> Arc<Box<dyn VmFunctionInjector>> {
        Arc::new(Box::new(Self {
            metadata: metadata.cloned(),
            row_count,
        }))
    }
}

impl VmFunctionInjector for IngestHeaderFunctionInjector {
    fn inject(
        &self,
        function: &FunctionName,
        arguments: &[VmTypedArray],
        row_count: usize,
        _span: nervix_vm::program::Span,
    ) -> Result<VmTypedArray, nervix_vm::RuntimeError> {
        let [VmTypedArray::Utf8(names)] = arguments else {
            return Err(nervix_vm::RuntimeError::InvalidBatch {
                message: format!(
                    "function '{}' requires one STRING argument",
                    function.as_str()
                ),
            });
        };
        let metadata_row_count = match self.metadata.as_ref() {
            Some(metadata) => metadata.len(),
            None => self.row_count,
        };
        if metadata_row_count != row_count || names.len() != row_count {
            return Err(nervix_vm::RuntimeError::InvalidBatch {
                message: format!(
                    "function '{}' header context has {} rows for a {row_count}-row batch",
                    function.as_str(),
                    metadata_row_count
                ),
            });
        }
        if let FunctionName::ReadHeader = function {
            let mut values = Vec::with_capacity(names.len());
            for (row, name) in names.iter().enumerate() {
                let value = if let Some(name) = name
                    && let Some(metadata) = self.metadata.as_ref()
                {
                    metadata.first_header(row, name)
                } else {
                    None
                };
                values.push(value);
            }
            return Ok(VmTypedArray::Utf8(arrow_array::StringArray::from(values)));
        }
        if let FunctionName::ReadHeaders = function {
            let field = StdArc::new(arrow_schema::Field::new("item", ArrowDataType::Utf8, false));
            let mut builder = ListBuilder::new(StringBuilder::new()).with_field(field);
            for (row, name) in names.iter().enumerate() {
                if let Some(name) = name
                    && let Some(metadata) = self.metadata.as_ref()
                {
                    metadata.visit_header_values(row, name, |value| {
                        builder.values().append_value(value);
                    });
                }
                builder.append(true);
            }
            return Ok(VmTypedArray::Generic(StdArc::new(builder.finish())));
        }
        Err(nervix_vm::RuntimeError::InvalidBatch {
            message: format!("function '{}' is not injectable", function.as_str()),
        })
    }
}

pub(super) fn ingest_source_supports_headers(source: &IngestSource) -> bool {
    matches!(
        source,
        IngestSource::Endpoint { .. }
            | IngestSource::Http { .. }
            | IngestSource::Kafka { .. }
            | IngestSource::Nats { .. }
            | IngestSource::Pulsar { .. }
            | IngestSource::RabbitMq { .. }
            | IngestSource::Sqs { .. }
    )
}

pub(super) fn emit_sink_supports_headers(sink: &EmitSink) -> bool {
    matches!(
        sink,
        EmitSink::Kafka { .. }
            | EmitSink::Pulsar { .. }
            | EmitSink::RabbitMq { .. }
            | EmitSink::Nats { .. }
            | EmitSink::Sqs { .. }
    )
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;
    use arrow_array::Array;
    use nervix_models::{
        CreateSchema, IngestSource, MessageErrorOperation, ModelKind, ModelName, ParseAsType,
        RetryPolicy, SchemaField, Timestamp,
    };
    use nonzero_ext::nonzero;
    use triomphe::Arc;

    use super::*;
    use crate::{
        runtime::ingest_group::PendingIngestGroup,
        runtime_ack::AckSet,
        runtime_schema::{RuntimeRecordBatch, RuntimeValue, compile_schema, test_runtime_row},
    };
    #[test]
    fn ingest_group_builds_one_metadata_column_set_for_all_of_its_messages() {
        let topic = "metering_events";
        let headers = TestIngestHeaders(&[("route", "primary")]);
        let schema = test_schema(&[("value", ParseAsType::I64)]);
        let mut group = PendingIngestGroup::new(IngestMetadataKind::Kafka, 8);

        INGEST_METADATA_BUILDER_SETS_OPENED.with(|count| count.set(0));
        INGEST_METADATA_COLUMN_SETS_BUILT.with(|count| count.set(0));

        for offset in 0..3i64 {
            let builder = group.record_builder(&schema);
            builder
                .append(Some(&RuntimeValue::I64(offset)))
                .and_then(|()| builder.finish_row())
                .expect("each decoded message must append into the group's record builder");
            group
                .append(
                    &[IngestMetadataRow::Kafka {
                        topic,
                        partition: 1,
                        offset,
                        headers: &headers,
                    }],
                    vec![AckSet::empty()],
                    Timestamp::from_unix_nanos(1),
                )
                .expect("each decoded message must append into the open group");
        }

        assert_eq!(
            INGEST_METADATA_BUILDER_SETS_OPENED.with(std::cell::Cell::get),
            1,
            "a group must open exactly one set of metadata builders, not one per message"
        );
        assert_eq!(
            INGEST_METADATA_COLUMN_SETS_BUILT.with(std::cell::Cell::get),
            0,
            "an open group must not build metadata columns before it closes"
        );

        let rows = group.into_rows().expect("the group must close");

        assert_eq!(
            INGEST_METADATA_COLUMN_SETS_BUILT.with(std::cell::Cell::get),
            1,
            "closing a group must build exactly one metadata column set"
        );
        assert_eq!(rows.len(), 3);
        let offsets = rows
            .metadata_rows()
            .expect("a Kafka group exposes its metadata columns")
            .field_column("offset")
            .expect("metadata projection must succeed")
            .expect("Kafka offset column must exist");
        let offsets = offsets
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("Kafka offsets must remain INT64");
        assert_eq!(offsets.values(), &[0, 1, 2]);
    }

    #[tokio::test]
    async fn kafka_ingestor_filter_map_can_read_metadata_namespace() {
        let input_schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("active", ParseAsType::Bool),
            ("amount", ParseAsType::I64),
            ("raw", ParseAsType::String),
        ]);
        let program = compile_ingestor_filter_map_program(
            &domain("default"),
            named::<ModelName>("logic_ingestor"),
            IngestMetadataKind::Kafka,
            true,
            &construction(
                "INHERIT tenant SET topic = metadata.topic, partition = metadata.partition, \
                 offset = metadata.offset WHERE metadata.offset >= 0",
            ),
            RuntimeVmSchemaPair {
                input: input_schema.arrow_schema(),
                input_sensitivity: VmSchemaSensitivity::default(),
                output: Arc::new(compile_schema(&CreateSchema {
                    name: named("metadata_output"),
                    fields: vec![
                        nervix_models::SchemaField {
                            name: named("tenant"),
                            ty: ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        },
                        nervix_models::SchemaField {
                            name: named("topic"),
                            ty: ParseAsType::String,
                            optional: true,
                            sensitive: false,
                        },
                        nervix_models::SchemaField {
                            name: named("partition"),
                            ty: ParseAsType::I32,
                            optional: true,
                            sensitive: false,
                        },
                        nervix_models::SchemaField {
                            name: named("offset"),
                            ty: ParseAsType::I64,
                            optional: true,
                            sensitive: false,
                        },
                    ],
                }))
                .arrow_schema(),
                output_sensitivity: VmSchemaSensitivity::default(),
            },
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &[],
                current_branch_schema: None,
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect("filter-map must compile")
        .expect("program must exist");

        let record = test_runtime_row([
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            ("active".to_string(), RuntimeValue::Bool(true)),
            ("amount".to_string(), RuntimeValue::I64(7)),
            ("raw".to_string(), RuntimeValue::String("meta".to_string())),
        ]);
        let metadata = ingest_metadata_for_test(
            IngestMetadataKind::Kafka,
            &[IngestMetadataRow::Kafka {
                topic: "logic_notifications_t123",
                partition: 2,
                offset: 42,
                headers: &NoIngestHeaders,
            }],
        );

        let output = execute_filter_map_for_test(
            &program,
            record,
            None,
            Some(&metadata),
            Timestamp::from_unix_nanos(1),
        )
        .await
        .expect("filter-map must execute")
        .expect("record must not be filtered out");

        assert_eq!(
            row_value(&output, "tenant"),
            Some(RuntimeValue::String("acme".to_string()))
        );
        assert_eq!(
            row_value(&output, "topic"),
            Some(RuntimeValue::String("logic_notifications_t123".to_string()))
        );
        assert_eq!(row_value(&output, "partition"), Some(RuntimeValue::I32(2)));
        assert_eq!(row_value(&output, "offset"), Some(RuntimeValue::I64(42)));
        assert!(row_value(&output, "active").is_none());
        assert!(row_value(&output, "amount").is_none());
        assert!(row_value(&output, "raw").is_none());

        let grouped_metadata = ingest_metadata_for_test(
            IngestMetadataKind::Kafka,
            &[
                IngestMetadataRow::Kafka {
                    topic: "logic_notifications_t123",
                    partition: 2,
                    offset: 42,
                    headers: &NoIngestHeaders,
                },
                IngestMetadataRow::Kafka {
                    topic: "logic_notifications_t123",
                    partition: 3,
                    offset: 43,
                    headers: &NoIngestHeaders,
                },
            ],
        );
        let offsets = grouped_metadata
            .field_column("offset")
            .expect("metadata projection must succeed")
            .expect("Kafka offset column must exist");
        let offsets = offsets
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("Kafka offsets must remain INT64");
        assert_eq!(offsets.values(), &[42, 43]);
        let selected = grouped_metadata
            .select(&[false, true])
            .expect("metadata row selection must succeed");
        let selected_offset = selected
            .field_column("offset")
            .expect("selected metadata projection must succeed")
            .expect("selected Kafka offset column must exist");
        let selected_offset = selected_offset
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("selected Kafka offset must remain INT64");
        assert_eq!(selected_offset.values(), &[43]);
    }

    #[tokio::test]
    async fn ingestor_header_functions_preserve_order_and_missing_value_semantics() {
        let input_schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("header_name", ParseAsType::String),
            ("raw", ParseAsType::String),
        ]);
        let output_schema = Arc::new(compile_schema(&CreateSchema {
            name: named("header_output"),
            fields: vec![
                SchemaField {
                    name: named("tenant"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: named("first"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: named("total"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                },
            ],
        }));
        let source = IngestSource::Kafka {
            client: named("logic_kafka"),
            topic: named("logic_notifications"),
            offset_mode: nervix_models::KafkaOffsetMode::Domain,
            instances: nonzero!(1u64),
            mode: nervix_models::KafkaIngestMode::AckSequential {
                timeout: "5s".to_string(),
                retry_policy: RetryPolicy {
                    backoff: "100ms".to_string(),
                    max_backoff: "200ms".to_string(),
                },
            },
            quiesce: nervix_models::IngestQuiesceMode::Suspend,
        };
        let program = compile_ingestor_filter_map_program(
            &domain("default"),
            named::<ModelName>("header_ingestor"),
            IngestMetadataKind::for_source(&source),
            ingest_source_supports_headers(&source),
            &construction(
                "INHERIT tenant SET first = read_header(lower(input.header_name)), total = \
                 count(read_headers(lower(input.header_name))) WHERE read_header(\"tenant\") = \
                 input.tenant AND count(read_headers(\"missing\")) = 0",
            ),
            RuntimeVmSchemaPair {
                input: input_schema.arrow_schema(),
                input_sensitivity: VmSchemaSensitivity::default(),
                output: output_schema.arrow_schema(),
                output_sensitivity: VmSchemaSensitivity::default(),
            },
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &[],
                current_branch_schema: None,
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect("header filter-map must compile")
        .expect("program must exist");
        let record = test_runtime_row([
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            (
                "header_name".to_string(),
                RuntimeValue::String("ROUTE".to_string()),
            ),
            ("raw".to_string(), RuntimeValue::String("body".to_string())),
        ]);
        let first_headers = TestIngestHeaders(&[
            ("tenant", "acme"),
            ("route", "primary"),
            ("route", "secondary"),
        ]);
        let metadata = ingest_metadata_for_test(
            IngestMetadataKind::Headers,
            &[IngestMetadataRow::Headers {
                headers: &first_headers,
            }],
        );

        let output = execute_filter_map_for_test(
            &program,
            record.clone(),
            None,
            Some(&metadata),
            Timestamp::from_unix_nanos(1),
        )
        .await
        .expect("header filter-map must execute")
        .expect("record must be selected");

        assert_eq!(
            row_value(&output, "first"),
            Some(RuntimeValue::String("primary".to_string()))
        );
        assert_eq!(row_value(&output, "total"), Some(RuntimeValue::I64(2)));

        let second_record = test_runtime_row([
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            (
                "header_name".to_string(),
                RuntimeValue::String("route".to_string()),
            ),
            (
                "raw".to_string(),
                RuntimeValue::String("second".to_string()),
            ),
        ]);
        let grouped_carrier =
            RuntimeRecordBatch::concat(&[&record.one_row_batch(), &second_record.one_row_batch()])
                .expect("grouped carrier must concatenate");
        let second_headers = TestIngestHeaders(&[("tenant", "acme"), ("route", "backup")]);
        let grouped_metadata = ingest_metadata_for_test(
            IngestMetadataKind::Headers,
            &[
                IngestMetadataRow::Headers {
                    headers: &first_headers,
                },
                IngestMetadataRow::Headers {
                    headers: &second_headers,
                },
            ],
        );
        let grouped_runtime_metadata =
            vec![record.metadata().clone(), second_record.metadata().clone()];
        let grouped_keys = vec![None, None];
        let grouped_outcomes = evaluate_filter_map_on_batch(
            ModelKind::Ingestor.as_str(),
            &named::<ModelName>("header_ingestor"),
            &program,
            FilterMapOutcomeInputs {
                carrier: &grouped_carrier,
                record_metadata: &grouped_runtime_metadata,
                keys: &grouped_keys,
                filter_map_metadata: Some(&grouped_metadata),
                side_inputs: &HashMap::default(),
            },
            Timestamp::from_unix_nanos(1),
        )
        .await
        .expect("grouped header filter-map must execute");
        let grouped_outputs = grouped_outcomes
            .into_iter()
            .map(|outcome| match outcome {
                SingleRecordFilterMapOutcome::Output(record) => record,
                SingleRecordFilterMapOutcome::Filtered => {
                    panic!("grouped header row must be selected")
                }
                SingleRecordFilterMapOutcome::MessageError { error, .. } => {
                    panic!("grouped header row failed: {}", error.message)
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            row_value(&grouped_outputs[0], "first"),
            Some(RuntimeValue::String("primary".to_string()))
        );
        assert_eq!(
            row_value(&grouped_outputs[0], "total"),
            Some(RuntimeValue::I64(2))
        );
        assert_eq!(
            row_value(&grouped_outputs[1], "first"),
            Some(RuntimeValue::String("backup".to_string()))
        );
        assert_eq!(
            row_value(&grouped_outputs[1], "total"),
            Some(RuntimeValue::I64(1))
        );

        let top_filter = compile_expression_filter_program(
            RuntimeCompileTarget {
                domain: &domain("default"),
                identifier: &named("header_ingestor"),
            },
            Some(&expression(
                "read_header(lower(input.header_name)) = \"primary\" AND \
                 count(read_headers(\"missing\")) = 0",
            )),
            RuntimeVmSchema {
                schema: input_schema.arrow_schema(),
                sensitivity: VmSchemaSensitivity::default(),
            },
            true,
            MessageErrorOperation::FilterWhere,
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &[],
                current_branch_schema: None,
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect("top FILTER WHERE must compile")
        .expect("program must exist");
        assert!(
            execute_filter_map_for_test(
                &top_filter,
                record,
                None,
                Some(&metadata),
                Timestamp::from_unix_nanos(1),
            )
            .await
            .expect("top FILTER WHERE must execute")
            .is_some()
        );
    }
}
