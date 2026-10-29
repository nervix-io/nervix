//! Typed ingestion metadata projection.
//!
//! Layer: data plane.
//! - **Owns.** Connector metadata columns and VM namespaces for accepted input batches.
//! - **Depends on.** Arrow arrays, the connector contract's metadata rows and header trait, and
//!   explicit execution timestamps.
//! - **Must not know.** Plexus parsing, source-client lifecycle or persisted control state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use arrow_buffer::BooleanBuffer;
use nervix_connector::{IngestMetadataRow, NoIngestHeaders};
use nervix_models::WebsocketsFrameType;
use strum::IntoEnumIterator as _;

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

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the caller supplies the synchronous header visitor; external Arrow access \
                      owns its data effects"
        )
    )]
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

pub(super) type IngestMetadataResult<T> = Result<T, Report<IngestMetadataError>>;

#[derive(Debug, Error)]
pub(super) enum IngestMetadataError {
    #[error(
        "WebSockets metadata capacity for {row_bound} rows with {label_bytes} bytes per frame \
         label exceeds the address space"
    )]
    WebsocketsCapacityOverflow {
        row_bound: usize,
        label_bytes: usize,
    },
    #[error("ingest metadata builders for {builders:?} cannot append a {row:?} row")]
    BuilderScopeMismatch {
        builders: IngestorMetadataScope,
        row: IngestorMetadataScope,
    },
    #[error("failed to build ingest metadata integration columns")]
    BuildIntegrationColumns,
    #[error(
        "ingest metadata built {integration_rows} integration rows, {header_name_rows} \
         header-name rows and {header_value_rows} header-value rows"
    )]
    HeaderRowCountMismatch {
        integration_rows: usize,
        header_name_rows: usize,
        header_value_rows: usize,
    },
    #[error("ingest metadata row {row} is outside column with {column_rows} rows")]
    RowOutOfBounds { row: usize, column_rows: usize },
    #[error("failed to select ingest metadata rows")]
    SelectRows,
    #[error(
        "ingest metadata selection has {selection_rows} rows for {metadata_rows} metadata rows"
    )]
    SelectionRowCountMismatch {
        selection_rows: usize,
        metadata_rows: usize,
    },
}

/// The `topic`, `partition` and `offset` builders of one Kafka ingest group.
pub(super) struct KafkaIntegrationBuilders {
    pub(super) topic: StringBuilder,
    pub(super) partition: Int32Builder,
    pub(super) offset: Int64Builder,
}

/// The integration-field builders of one ingest group, one variant per metadata scope.
///
/// The Kafka columns live behind one allocation taken when the group opens, so the variants
/// stay comparable in size.
pub(super) enum IngestIntegrationBuilders {
    Http {
        status: UInt16Builder,
        polled_at: TimestampNanosecondBuilder,
    },
    Websockets {
        frame_type: StringBuilder,
    },
    Kafka(Box<KafkaIntegrationBuilders>),
    Syslog {
        peer_addr: StringBuilder,
    },
    Headers,
}

/// Arrow builders for one ingest group's metadata columns.
///
/// A group opens one set with its first row, appends one row per decoded message, and
/// finishes it once when the group closes. No Arrow array, batch or builder is created per
/// message, and no one-row batch is ever concatenated.
pub(super) struct IngestMetadataBuilders {
    pub(super) scope: IngestorMetadataScope,
    pub(super) integration: IngestIntegrationBuilders,
    pub(super) header_names: ListBuilder<StringBuilder>,
    pub(super) header_values: ListBuilder<StringBuilder>,
    pub(super) rows: usize,
}

// Counted per thread so a test observes only the groups it opened itself, while the rest of
// the suite exercises the same builders in parallel.
#[cfg(test)]
nervix_primitives::thread_local! {
    pub(super) static INGEST_METADATA_BUILDER_SETS_OPENED: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    pub(super) static INGEST_METADATA_COLUMN_SETS_BUILT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

impl IngestMetadataBuilders {
    /// Opens a builder set for `scope`, sizing the integration fields for `row_bound` rows.
    ///
    /// Header builders start empty because most sources carry no headers at all.
    /// A frame-label capacity outside the address space returns a typed error before allocation.
    #[inline(never)]
    pub(super) fn new(
        scope: IngestorMetadataScope,
        row_bound: usize,
    ) -> IngestMetadataResult<Self> {
        let integration = match scope {
            IngestorMetadataScope::Http => IngestIntegrationBuilders::Http {
                status: UInt16Builder::with_capacity(row_bound),
                polled_at: TimestampNanosecondBuilder::with_capacity(row_bound),
            },
            IngestorMetadataScope::Websockets => {
                let mut maximum_label_bytes = 0;
                for kind in WebsocketsFrameType::iter() {
                    maximum_label_bytes = maximum_label_bytes.max(kind.as_ref().len());
                }
                let label_capacity =
                    row_bound.checked_mul(maximum_label_bytes).ok_or_else(|| {
                        Report::new(IngestMetadataError::WebsocketsCapacityOverflow {
                            row_bound,
                            label_bytes: maximum_label_bytes,
                        })
                    })?;
                IngestIntegrationBuilders::Websockets {
                    frame_type: StringBuilder::with_capacity(row_bound, label_capacity),
                }
            }
            IngestorMetadataScope::Kafka => {
                IngestIntegrationBuilders::Kafka(Box::new(KafkaIntegrationBuilders {
                    topic: StringBuilder::with_capacity(row_bound, 0),
                    partition: Int32Builder::with_capacity(row_bound),
                    offset: Int64Builder::with_capacity(row_bound),
                }))
            }
            IngestorMetadataScope::Syslog => IngestIntegrationBuilders::Syslog {
                peer_addr: StringBuilder::with_capacity(row_bound, 0),
            },
            IngestorMetadataScope::Headers => IngestIntegrationBuilders::Headers,
        };
        let item_field =
            || StdArc::new(arrow_schema::Field::new("item", ArrowDataType::Utf8, false));
        #[cfg(test)]
        INGEST_METADATA_BUILDER_SETS_OPENED.with(|count| count.set(count.get() + 1));
        Ok(Self {
            scope,
            integration,
            header_names: ListBuilder::new(StringBuilder::new()).with_field(item_field()),
            header_values: ListBuilder::new(StringBuilder::new()).with_field(item_field()),
            rows: 0,
        })
    }

    // Keep Arrow's builder machinery behind this boundary. Inlining it into the source collector
    // doubles that per-message function's machine code and measurably reduces ingest throughput.
    #[inline(never)]
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the source supplies a transport header visitor; this host \
                                   appends the bounded row metadata")
    )]
    pub(super) fn append(&mut self, row: &IngestMetadataRow<'_>) -> IngestMetadataResult<()> {
        let headers: Option<&dyn nervix_connector::IngestMessageHeaders> =
            match (&mut self.integration, row) {
                (
                    IngestIntegrationBuilders::Http { status, polled_at },
                    IngestMetadataRow::Http {
                        status: response_status,
                        polled_at: scheduled_at,
                        headers,
                    },
                ) => {
                    status.append_value(*response_status);
                    polled_at.append_value(scheduled_at.unix_nanos());
                    Some(*headers)
                }
                (
                    IngestIntegrationBuilders::Websockets { frame_type },
                    IngestMetadataRow::Websockets {
                        frame_type: kind,
                        headers,
                    },
                ) => {
                    frame_type.append_value(kind.as_ref());
                    Some(headers.as_ref())
                }
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
                    return Err(Report::new(IngestMetadataError::BuilderScopeMismatch {
                        builders: self.scope,
                        row: row.scope(),
                    }));
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

    #[inline(never)]
    pub(super) fn finish(mut self) -> IngestMetadataResult<IngestFilterMapMetadata> {
        #[cfg(test)]
        INGEST_METADATA_COLUMN_SETS_BUILT.with(|count| count.set(count.get() + 1));
        let rows = self.rows;
        let integration_columns = match &mut self.integration {
            IngestIntegrationBuilders::Http { status, polled_at } => {
                let status: ArrayRef = StdArc::new(status.finish());
                let polled_at: ArrayRef = StdArc::new(polled_at.finish().with_timezone("+00:00"));
                vec![status, polled_at]
            }
            IngestIntegrationBuilders::Websockets { frame_type } => {
                let frame_type: ArrayRef = StdArc::new(frame_type.finish());
                vec![frame_type]
            }
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
        let schema = match self.scope.arrow_schema() {
            Some(schema) => StdArc::new(schema),
            None => StdArc::new(arrow_schema::Schema::empty()),
        };
        let integration_fields = RecordBatch::try_new_with_options(
            schema,
            integration_columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|source| {
            Report::new(IngestMetadataError::BuildIntegrationColumns)
                .attach_printable(source.to_string())
        })?;
        let header_names: ArrayRef = StdArc::new(self.header_names.finish());
        let header_values: ArrayRef = StdArc::new(self.header_values.finish());
        if header_names.len() != rows || header_values.len() != rows {
            return Err(Report::new(IngestMetadataError::HeaderRowCountMismatch {
                integration_rows: rows,
                header_name_rows: header_names.len(),
                header_value_rows: header_values.len(),
            }));
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
    /// The metadata of `rows` messages that arrived without transport headers or integration
    /// fields, as every row of a client batch does.
    pub(super) fn headerless(rows: usize) -> IngestMetadataResult<Self> {
        let mut builders = IngestMetadataBuilders::new(IngestorMetadataScope::Headers, rows)?;
        let row = IngestMetadataRow::Headers {
            headers: &NoIngestHeaders,
        };
        for _ in 0..rows {
            builders.append(&row)?;
        }
        builders.finish()
    }

    pub(super) fn selected_array(&self, array: &ArrayRef) -> IngestMetadataResult<ArrayRef> {
        if self.rows.iter().copied().eq(0..self.rows.len()) && self.rows.len() == array.len() {
            return Ok(array.clone());
        }
        let indices = self
            .rows
            .iter()
            .map(|row| {
                if *row >= array.len() {
                    return Err(Report::new(IngestMetadataError::RowOutOfBounds {
                        row: *row,
                        column_rows: array.len(),
                    }));
                }
                Ok::<u64, Report<IngestMetadataError>>((*row).arch_into())
            })
            .collect::<Result<UInt64Array, _>>()?;
        take_arrow_array(array.as_ref(), &indices, None).map_err(|source| {
            Report::new(IngestMetadataError::SelectRows).attach_printable(source.to_string())
        })
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

    pub(super) fn select(&self, keep: &BooleanBuffer) -> IngestMetadataResult<Self> {
        if keep.len() != self.len() {
            return Err(Report::new(
                IngestMetadataError::SelectionRowCountMismatch {
                    selection_rows: keep.len(),
                    metadata_rows: self.len(),
                },
            ));
        }
        Ok(Self {
            columns: self.columns.clone(),
            rows: Arc::new(
                self.rows
                    .iter()
                    .enumerate()
                    .filter_map(|(index, row)| keep.value(index).then_some(*row))
                    .collect(),
            ),
        })
    }

    pub(super) fn field_column(&self, name: &str) -> IngestMetadataResult<Option<ArrayRef>> {
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
    /// Reads the headers of the messages the selected rows were decoded from. A conditional arm
    /// calls this for the rows it selects only, so each row's headers are looked up by the row's
    /// identity in the batch, which the selection names.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the caller supplies the synchronous header visitor; external Arrow access \
                      owns its data effects"
        )
    )]
    fn inject_with_context(
        &self,
        function: &FunctionName,
        arguments: &[VmTypedArray],
        rows: &nervix_vm::RowSelection,
        _span: nervix_vm::program::Span,
        _now: Timestamp,
        _prior_error_rows: nervix_vm::RowErrorMask<'_>,
    ) -> error_stack::Result<nervix_vm::InjectedResult, nervix_vm::RuntimeError> {
        let [VmTypedArray::Utf8(names)] = arguments else {
            return Err(Report::new(nervix_vm::RuntimeError::InvalidBatch {
                message: format!(
                    "function '{}' requires one STRING argument",
                    function.as_str()
                ),
            }));
        };
        let metadata_row_count = match self.metadata.as_ref() {
            Some(metadata) => metadata.len(),
            None => self.row_count,
        };
        if !rows.fits(metadata_row_count) || names.len() != rows.len() {
            return Err(Report::new(nervix_vm::RuntimeError::InvalidBatch {
                message: format!(
                    "function '{}' header context has {} rows for a call over {} rows of a batch \
                     selected as {rows:?}",
                    function.as_str(),
                    metadata_row_count,
                    names.len()
                ),
            }));
        }
        if let FunctionName::ReadHeader = function {
            let mut values = Vec::with_capacity(names.len());
            for (row, name) in rows.iter().zip(names.iter()) {
                let value = if let Some(name) = name
                    && let Some(metadata) = self.metadata.as_ref()
                {
                    metadata.first_header(row, name)
                } else {
                    None
                };
                values.push(value);
            }
            return Ok(nervix_vm::InjectedResult::success(VmTypedArray::Utf8(
                arrow_array::StringArray::from(values),
            )));
        }
        if let FunctionName::ReadHeaders = function {
            let field = StdArc::new(arrow_schema::Field::new("item", ArrowDataType::Utf8, false));
            let mut builder = ListBuilder::new(StringBuilder::new()).with_field(field);
            for (row, name) in rows.iter().zip(names.iter()) {
                if let Some(name) = name
                    && let Some(metadata) = self.metadata.as_ref()
                {
                    metadata.visit_header_values(row, name, |value| {
                        builder.values().append_value(value);
                    });
                }
                builder.append(true);
            }
            return Ok(nervix_vm::InjectedResult::success(VmTypedArray::Generic(
                StdArc::new(builder.finish()),
            )));
        }
        Err(Report::new(nervix_vm::RuntimeError::InvalidBatch {
            message: format!("function '{}' is not injectable", function.as_str()),
        }))
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use ahash::HashMap;
    use arrow_array::{Array, TimestampNanosecondArray, UInt16Array};
    use nervix_connector::NoIngestHeaders;
    use nervix_models::{
        CodecWireFormat, CreateCodec, CreateSchema, CreateWireSchema, IngestorEntity, JsonType,
        KafkaIngestor, KafkaTopicIngestor, MessageErrorOperation, ModelName, ParseAsType,
        ResolvedCodecWireFormat, RetryPolicy, SchemaField, Timestamp, WireSchemaField,
    };
    use nervix_primitives::sync::Arc;
    use nonzero_ext::nonzero;

    use super::*;
    use crate::{
        runtime::ingest_group::PendingIngestGroup,
        runtime_ack::AckSet,
        runtime_schema::{
            RuntimeRecordBatch, RuntimeValue, compile_codec, compile_schema, test_runtime_row,
        },
    };

    #[test]
    fn http_poll_metadata_keeps_status_and_scheduled_instant_in_typed_columns() {
        let instant = Timestamp::from_unix_nanos(1_234_567_890);
        let mut builders = IngestMetadataBuilders::new(IngestorMetadataScope::Http, 1)
            .expect("HTTP metadata builders open");
        builders
            .append(&IngestMetadataRow::Http {
                status: 206,
                polled_at: instant,
                headers: &NoIngestHeaders,
            })
            .expect("the HTTP row matches its builders");
        let metadata = builders.finish().expect("HTTP columns finish");
        let fields = &metadata.columns.integration_fields;
        let status = fields
            .column(0)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("status is U16");
        let polled_at = fields
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("poll time is DATETIME");
        assert_eq!(status.value(0), 206);
        assert_eq!(polled_at.value(0), instant.unix_nanos());
    }

    /// A JSON codec over a one-field `value` schema, for tests that decode payloads into a group.
    fn metering_value_codec() -> Arc<CompiledCodec> {
        let schema = Arc::new(compile_schema(&CreateSchema {
            name: named("metering_value"),
            fields: vec![SchemaField {
                name: named("value"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            }],
        }));
        compile_codec(
            &CreateCodec {
                name: named("metering_value_codec"),
                wire_format: CodecWireFormat::Json {
                    wire_schema: named("metering_value_wire"),
                },
                schema: named("metering_value"),
                encoding_rules: Vec::new(),
            },
            schema,
            ResolvedCodecWireFormat::Json(&CreateWireSchema {
                name: named("metering_value_wire"),
                strictness: Default::default(),
                fields: vec![WireSchemaField {
                    name: named("value"),
                    ty: JsonType::Integer,
                    optional: false,
                }],
            }),
        )
        .expect("the metering value codec should compile")
    }

    #[test]
    fn websockets_metadata_refuses_an_unaddressable_capacity() {
        let error = match IngestMetadataBuilders::new(IngestorMetadataScope::Websockets, usize::MAX)
        {
            Ok(_) => panic!("an unaddressable frame-label capacity must fail before allocation"),
            Err(error) => error,
        };
        assert!(matches!(
            error.current_context(),
            IngestMetadataError::WebsocketsCapacityOverflow {
                row_bound: usize::MAX,
                ..
            }
        ));
    }

    #[test]
    fn metadata_shape_failures_keep_their_typed_context() {
        let mut kafka_builders = IngestMetadataBuilders::new(IngestorMetadataScope::Kafka, 1)
            .expect("one Kafka metadata row fits");
        let mismatch = kafka_builders
            .append(&IngestMetadataRow::Headers {
                headers: &NoIngestHeaders,
            })
            .expect_err("a header row cannot be appended to Kafka metadata builders");
        assert!(matches!(
            mismatch.current_context(),
            IngestMetadataError::BuilderScopeMismatch {
                builders: IngestorMetadataScope::Kafka,
                row: IngestorMetadataScope::Headers,
            }
        ));

        kafka_builders.rows = 1;
        let integration = kafka_builders
            .finish()
            .expect_err("declared rows without integration values must fail");
        assert!(matches!(
            integration.current_context(),
            IngestMetadataError::BuildIntegrationColumns
        ));

        let mut header_builders = IngestMetadataBuilders::new(IngestorMetadataScope::Headers, 1)
            .expect("one header metadata row fits");
        header_builders
            .append(&IngestMetadataRow::Headers {
                headers: &NoIngestHeaders,
            })
            .expect("the matching metadata row should append");
        header_builders.rows = 2;
        let headers = header_builders
            .finish()
            .expect_err("header and integration row counts must agree");
        assert!(matches!(
            headers.current_context(),
            IngestMetadataError::HeaderRowCountMismatch {
                integration_rows: 2,
                header_name_rows: 1,
                header_value_rows: 1,
            }
        ));

        let mut builders = IngestMetadataBuilders::new(IngestorMetadataScope::Headers, 1)
            .expect("one header metadata row fits");
        builders
            .append(&IngestMetadataRow::Headers {
                headers: &NoIngestHeaders,
            })
            .expect("the matching metadata row should append");
        let metadata = builders.finish().expect("valid metadata should finish");
        let array: ArrayRef = StdArc::new(StringArray::from(vec!["value"]));
        let out_of_bounds = IngestFilterMapMetadata {
            columns: metadata.columns.clone(),
            rows: Arc::new(vec![1]),
        }
        .selected_array(&array)
        .expect_err("a projection cannot address a missing physical row");
        assert!(matches!(
            out_of_bounds.current_context(),
            IngestMetadataError::RowOutOfBounds {
                row: 1,
                column_rows: 1,
            }
        ));

        let selection = metadata
            .select(&BooleanBuffer::new_unset(0))
            .expect_err("selection and metadata row counts must agree");
        assert!(matches!(
            selection.current_context(),
            IngestMetadataError::SelectionRowCountMismatch {
                selection_rows: 0,
                metadata_rows: 1,
            }
        ));
    }

    #[nervix_primitives::test]
    async fn ingest_group_builds_one_metadata_column_set_for_all_of_its_messages() {
        let topic = "metering_events";
        let headers = TestIngestHeaders(&[("route", "primary")]);
        let codec = metering_value_codec();
        let mut group = PendingIngestGroup::new(IngestorMetadataScope::Kafka, 8);

        INGEST_METADATA_BUILDER_SETS_OPENED.with(|count| count.set(0));
        INGEST_METADATA_COLUMN_SETS_BUILT.with(|count| count.set(0));

        for offset in 0..3i64 {
            let payload = format!(r#"{{"value":{offset}}}"#);
            group
                .decode_payload(
                    &Executor::default(),
                    QueueAdmission::RefuseWhenFull,
                    &codec,
                    payload.as_bytes(),
                )
                .await
                .expect("each payload must decode into the group's record builder");
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
                .expect("each decoded payload must append into the open group");
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

    #[nervix_primitives::test]
    async fn kafka_ingestor_filter_map_can_read_metadata_namespace() {
        let input_schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("active", ParseAsType::Bool),
            ("amount", ParseAsType::I64),
            ("raw", ParseAsType::String),
        ]);
        let program = bind_ingestor_route_for_test(
            &named::<ModelName>("logic_ingestor"),
            IngestorMetadataScope::Kafka,
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
                current_branching: &ResolvedBranching::unbranched(),
                udfs: None,
            },
        )
        .expect("filter-map must compile");

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
            IngestorMetadataScope::Kafka,
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
            IngestorMetadataScope::Kafka,
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
            .select(&BooleanBuffer::collect_bool(2, |row| row == 1))
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

    #[test]
    fn header_functions_read_the_selected_rows_by_identity() {
        let first_headers = TestIngestHeaders(&[("route", "primary")]);
        let second_headers = TestIngestHeaders(&[("route", "secondary")]);
        let metadata = ingest_metadata_for_test(
            IngestorMetadataScope::Headers,
            &[
                IngestMetadataRow::Headers {
                    headers: &first_headers,
                },
                IngestMetadataRow::Headers {
                    headers: &second_headers,
                },
            ],
        );
        let injector = IngestHeaderFunctionInjector::from_metadata(Some(&metadata), 2);
        let span: nervix_vm::program::Span = (0..0).into();
        let names = [VmTypedArray::Utf8(arrow_array::StringArray::from(vec![
            Some("route"),
        ]))];

        let second_only = injector
            .inject_with_context(
                &FunctionName::ReadHeader,
                &names,
                &nervix_vm::RowSelection::Selected(vec![1]),
                span,
                Timestamp::from_unix_nanos(0),
                nervix_vm::RowErrorMask::none(1),
            )
            .expect("a selected row reads its own headers");
        assert_eq!(
            second_only.output,
            VmTypedArray::Utf8(arrow_array::StringArray::from(vec![Some("secondary")])),
            "the header comes from the message the selected row was decoded from"
        );

        let beyond = injector.inject_with_context(
            &FunctionName::ReadHeader,
            &names,
            &nervix_vm::RowSelection::Selected(vec![2]),
            span,
            Timestamp::from_unix_nanos(0),
            nervix_vm::RowErrorMask::none(1),
        );
        assert!(
            matches!(beyond, Err(error) if matches!(error.current_context(), nervix_vm::RuntimeError::InvalidBatch { .. })),
            "a row past the header context is refused"
        );
        let short = injector.inject_with_context(
            &FunctionName::ReadHeader,
            &names,
            &nervix_vm::RowSelection::All(1),
            span,
            Timestamp::from_unix_nanos(0),
            nervix_vm::RowErrorMask::none(1),
        );
        assert!(
            matches!(short, Err(error) if matches!(error.current_context(), nervix_vm::RuntimeError::InvalidBatch { .. })),
            "a batch of another size than the header context is refused"
        );
    }

    #[nervix_primitives::test]
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
        let source = IngestorEntity::Kafka(KafkaIngestor::Topic(KafkaTopicIngestor {
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
        }));
        let program = bind_ingestor_route_for_test(
            &named::<ModelName>("header_ingestor"),
            IngestorMetadataScope::Kafka,
            source.reads_headers(),
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
                current_branching: &ResolvedBranching::unbranched(),
                udfs: None,
            },
        )
        .expect("header filter-map must compile");
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
            IngestorMetadataScope::Headers,
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
            IngestorMetadataScope::Headers,
            &[
                IngestMetadataRow::Headers {
                    headers: &first_headers,
                },
                IngestMetadataRow::Headers {
                    headers: &second_headers,
                },
            ],
        );
        let grouped_runtime_metadata = RecordMetadataColumns::from_rows([
            record.metadata().clone(),
            second_record.metadata().clone(),
        ]);
        let grouped_keys = vec![None, None];
        let grouped_outcomes = evaluate_filter_map_on_batch(
            &Executor::default(),
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

        let top_filter = compile_scoped_filter_program(
            &named("header_ingestor"),
            Some(&expression(
                "read_header(lower(input.header_name)) = \"primary\" AND \
                 count(read_headers(\"missing\")) = 0",
            )),
            RuntimeVmSchema {
                schema: input_schema.arrow_schema(),
                sensitivity: VmSchemaSensitivity::default(),
            },
            MessageErrorOperation::FilterWhere,
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &ResolvedBranching::unbranched(),
                udfs: None,
            },
            RuntimeFilterScope::Source {
                namespace: "input",
                allow_header_reads: true,
                allow_metadata: true,
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
