use std::{ops::Range, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use nervix_wasm_protocol::{
    AckSidecar, Envelope, EnvelopeRef, OutputColumnRef, ProcessorType, ProtocolError, RoutedOutput,
};

use crate::error::GuestError;

/// One host input envelope: the Arrow IPC record batches plus the ACK sidecar.
///
/// The raw envelope bytes stay owned so a processor can stash a batch and
/// carry it inside its own saved state across snapshots.
#[derive(Debug, Clone)]
pub struct InputBatch {
    bytes: Vec<u8>,
    arrow: Range<usize>,
    acks: AckSidecar,
    batches: Vec<RecordBatch>,
}

impl InputBatch {
    /// Decodes one size-prefixed input envelope, verifying the FlatBuffer and
    /// the Arrow IPC stream eagerly.
    pub fn from_envelope_bytes(bytes: Vec<u8>) -> Result<Self, GuestError> {
        let (arrow, acks) = {
            let EnvelopeRef::Input(input) = EnvelopeRef::decode(&bytes)? else {
                return Err(GuestError::Protocol(ProtocolError::UnexpectedPayload {
                    expected: "input envelope",
                    actual: "output envelope",
                }));
            };
            let arrow_ipc = input.arrow_ipc_batch();
            let start = arrow_ipc.as_ptr() as usize - bytes.as_ptr() as usize;
            (start..start + arrow_ipc.len(), input.acks())
        };
        let reader = StreamReader::try_new(&bytes[arrow.clone()], None)?;
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            bytes,
            arrow,
            acks,
            batches,
        })
    }

    /// Complete size-prefixed envelope bytes, restorable through
    /// [`InputBatch::from_envelope_bytes`].
    pub fn envelope_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_envelope_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub fn arrow_ipc(&self) -> &[u8] {
        &self.bytes[self.arrow.clone()]
    }

    pub fn acks(&self) -> &AckSidecar {
        &self.acks
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn row_count(&self) -> u64 {
        self.batches.iter().fold(0_u64, |rows, batch| {
            rows.saturating_add(batch.num_rows() as u64)
        })
    }
}

/// Builder for one guest output envelope: a shared generated column pool plus
/// destination-aligned routed outputs.
#[derive(Debug, Default)]
pub struct OutputEnvelope {
    generated: Vec<(ArrayRef, bool)>,
    outputs: Vec<RoutedOutput>,
}

impl OutputEnvelope {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one immutable column to the shared generated pool and returns its
    /// index for [`OutputColumnRef::Generated`] references. `optional` must
    /// match the nullability of every destination field that references the
    /// column; several routes may reference the same index.
    pub fn add_generated_column(&mut self, array: ArrayRef, optional: bool) -> u32 {
        self.generated.push((array, optional));
        (self.generated.len() - 1) as u32
    }

    /// Adds one routed output whose `columns` align positionally with the
    /// destination relay schema.
    pub fn add_route(
        &mut self,
        output_relay: impl Into<String>,
        columns: Vec<OutputColumnRef>,
        acks: AckSidecar,
    ) {
        self.outputs.push(RoutedOutput {
            output_relay: output_relay.into(),
            columns,
            acks,
        });
    }

    pub(crate) fn encode(self) -> Result<Vec<u8>, GuestError> {
        let generated_arrow_ipc_batch = if self.generated.is_empty() {
            Vec::new()
        } else {
            let fields = self
                .generated
                .iter()
                .map(|(array, optional)| Field::new("", array.data_type().clone(), *optional))
                .collect::<Vec<_>>();
            let schema = Arc::new(Schema::new(fields));
            let arrays = self
                .generated
                .into_iter()
                .map(|(array, _)| array)
                .collect::<Vec<_>>();
            let batch = RecordBatch::try_new(schema.clone(), arrays)?;
            let mut ipc = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut ipc, &schema)?;
                writer.write(&batch)?;
                writer.finish()?;
            }
            ipc
        };
        Ok(Envelope::Output {
            generated_arrow_ipc_batch,
            outputs: self.outputs,
        }
        .encode())
    }
}

/// Arrow interop for the ABI schema contract.
pub trait ProcessorTypeArrow {
    /// Returns the exact Arrow data type Nervix uses for this ABI type,
    /// including timestamp unit and timezone and nested list shapes.
    fn arrow_data_type(&self) -> Result<DataType, GuestError>;
}

impl ProcessorTypeArrow for ProcessorType {
    fn arrow_data_type(&self) -> Result<DataType, GuestError> {
        Ok(match self {
            Self::U8 => DataType::UInt8,
            Self::I8 => DataType::Int8,
            Self::U16 => DataType::UInt16,
            Self::I16 => DataType::Int16,
            Self::U32 => DataType::UInt32,
            Self::I32 => DataType::Int32,
            Self::U64 => DataType::UInt64,
            Self::I64 => DataType::Int64,
            Self::Bool => DataType::Boolean,
            Self::String => DataType::Utf8,
            Self::Datetime => DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            Self::F32 => DataType::Float32,
            Self::F64 => DataType::Float64,
            Self::Array { element, len } => DataType::FixedSizeList(
                Arc::new(Field::new("item", element.arrow_data_type()?, false)),
                i32::try_from(*len).map_err(|_| GuestError::InvalidSize)?,
            ),
            Self::Vec { element } => DataType::List(Arc::new(Field::new(
                "item",
                element.arrow_data_type()?,
                false,
            ))),
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, StringArray};
    use nervix_wasm_protocol::{AckToken, OutputRow};

    use super::*;

    fn sample_arrow_ipc(values: &[i32]) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(values.to_vec()))],
        )
        .expect("batch must build");
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, &schema).expect("writer must build");
            writer.write(&batch).expect("batch must encode");
            writer.finish().expect("stream must finish");
        }
        ipc
    }

    fn sidecar() -> AckSidecar {
        AckSidecar {
            rows: vec![OutputRow {
                tokens: vec![AckToken(7)],
                source_token: Some(AckToken(7)),
            }],
            ..AckSidecar::default()
        }
    }

    #[test]
    fn input_batch_decodes_envelope_and_keeps_restorable_bytes() {
        let arrow_ipc = sample_arrow_ipc(&[1, 2, 3]);
        let encoded = Envelope::Input {
            arrow_ipc_batch: arrow_ipc.clone(),
            acks: sidecar(),
        }
        .encode();

        let input = InputBatch::from_envelope_bytes(encoded.clone()).expect("must decode");

        assert_eq!(input.arrow_ipc(), arrow_ipc.as_slice());
        assert_eq!(input.acks(), &sidecar());
        assert_eq!(input.row_count(), 3);
        assert_eq!(input.batches().len(), 1);
        assert_eq!(input.envelope_bytes(), encoded.as_slice());
        let restored =
            InputBatch::from_envelope_bytes(input.into_envelope_bytes()).expect("must restore");
        assert_eq!(restored.row_count(), 3);
    }

    #[test]
    fn input_batch_rejects_output_envelopes() {
        let encoded = Envelope::Output {
            generated_arrow_ipc_batch: Vec::new(),
            outputs: Vec::new(),
        }
        .encode();

        assert!(matches!(
            InputBatch::from_envelope_bytes(encoded),
            Err(GuestError::Protocol(_))
        ));
    }

    #[test]
    fn output_envelope_encodes_shared_generated_pool() {
        let mut output = OutputEnvelope::new();
        let bucket = output.add_generated_column(
            Arc::new(StringArray::from(vec![Some("EVEN")])) as ArrayRef,
            false,
        );
        output.add_route(
            "enriched_events",
            vec![
                OutputColumnRef::Input { column_index: 0 },
                OutputColumnRef::Generated {
                    column_index: bucket,
                },
            ],
            sidecar(),
        );
        output.add_route(
            "audit_events",
            vec![
                OutputColumnRef::Input { column_index: 0 },
                OutputColumnRef::Generated {
                    column_index: bucket,
                },
            ],
            AckSidecar::default(),
        );

        let encoded = output.encode().expect("must encode");
        let Envelope::Output {
            generated_arrow_ipc_batch,
            outputs,
        } = Envelope::decode(&encoded).expect("must decode")
        else {
            panic!("expected output envelope");
        };

        assert_eq!(outputs.len(), 2);
        assert_eq!(outputs[0].output_relay, "enriched_events");
        assert_eq!(outputs[1].output_relay, "audit_events");
        let reader = StreamReader::try_new(generated_arrow_ipc_batch.as_slice(), None)
            .expect("generated pool must decode");
        assert_eq!(reader.schema().fields().len(), 1);
        assert!(reader.schema().field(0).name().is_empty());
        assert!(!reader.schema().field(0).is_nullable());
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .expect("generated batch must decode");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn output_envelope_without_generated_columns_encodes_empty_pool() {
        let mut output = OutputEnvelope::new();
        output.add_route(
            "output_events",
            vec![OutputColumnRef::Input { column_index: 0 }],
            sidecar(),
        );

        let encoded = output.encode().expect("must encode");
        let Envelope::Output {
            generated_arrow_ipc_batch,
            ..
        } = Envelope::decode(&encoded).expect("must decode")
        else {
            panic!("expected output envelope");
        };

        assert!(generated_arrow_ipc_batch.is_empty());
    }

    #[test]
    fn processor_types_map_to_the_exact_nervix_arrow_types() {
        assert_eq!(
            ProcessorType::Datetime.arrow_data_type().expect("must map"),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()))
        );
        assert_eq!(
            ProcessorType::Array {
                element: Box::new(ProcessorType::F64),
                len: 3,
            }
            .arrow_data_type()
            .expect("must map"),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, false)), 3)
        );
        assert_eq!(
            ProcessorType::Vec {
                element: Box::new(ProcessorType::String),
            }
            .arrow_data_type()
            .expect("must map"),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)))
        );
    }
}
