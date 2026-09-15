//! Unfolding one payload through a JAQ-backed codec into the messages it holds.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The unfold limit, the zero-based position of every input value and output of a
//!   payload, and appending a payload's messages to a batch as a whole or not at all.
//! - **Depends on.** The jaq programs and payload readers, the codec's JSON decoding, and the batch
//!   builder.
//! - **Must not know.** Sources, ingest groups, acknowledgements or branches. The caller decides
//!   what a rejected payload means for its source.

use std::fmt;

use bytes::Bytes;
use serde_json::Value as JsonValue;
use tracing::trace;

use super::{
    CodecError, CompiledCodec, CompiledJaqNativeCodec, CompiledProtobufCodec,
    RuntimeRecordBatchBuilder, decode_json_value, decode_protobuf_payload, finish_decoded_row,
};
use crate::jaq_program::{CompiledJaqProgram, JaqFormatError, JaqInput, JaqProgramError};

/// The most messages one payload may unfold into.
///
/// A program with an unbounded output stream, such as `repeat` or an open-ended `range`, is stopped
/// here instead of exhausting the node. The value is the largest batch size the message histograms
/// track, so a payload at the limit is still observable as one batch.
pub(crate) const PAYLOAD_UNFOLD_LIMIT: usize = 65_536;

/// Where in a payload an unfolding failure occurred. Both ordinals are zero-based.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnfoldPosition {
    /// An input value failed as a whole: it did not parse, or the program failed on it.
    Input { input: usize },
    /// One output the program produced for an input value failed.
    Output { input: usize, output: usize },
}

impl fmt::Display for UnfoldPosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Input { input } => write!(f, "input value {input}"),
            Self::Output { input, output } => write!(f, "input value {input}, output {output}"),
        }
    }
}

/// The messages one payload unfolded into, in unfold order, before any of them joins a batch.
///
/// Unfolding runs the codec's program, which is the CPU-bound half of decoding, so a caller may run
/// it off the reactor and append the result on the task that owns the batch builder.
#[derive(Debug)]
pub(crate) struct UnfoldedPayload {
    messages: Vec<UnfoldedMessage>,
}

/// One object the program produced, with the position it came from.
#[derive(Debug)]
struct UnfoldedMessage {
    input: usize,
    output: usize,
    value: JsonValue,
}

impl UnfoldedPayload {
    /// Runs `program` over every input value in payload order and collects its outputs.
    ///
    /// Reading stops at the first failure, and a program that produces more than
    /// [`PAYLOAD_UNFOLD_LIMIT`] messages is stopped as soon as it exceeds the limit.
    fn unfold(
        codec: &CompiledCodec,
        program: &CompiledJaqProgram,
        inputs: impl Iterator<Item = Result<JaqInput, CodecError>>,
    ) -> Result<Self, CodecError> {
        let mut messages = Vec::new();
        for (input_index, input) in inputs.enumerate() {
            let input = match input {
                Ok(input) => input,
                Err(cause) => {
                    return Err(cause.at(UnfoldPosition::Input { input: input_index }));
                }
            };
            for (output_index, output) in program.outputs(input).enumerate() {
                let output = match output {
                    Ok(output) => output,
                    Err(error) => return Err(codec.evaluation_failure(input_index, &error)),
                };
                if messages.len() == PAYLOAD_UNFOLD_LIMIT {
                    return Err(CodecError::UnfoldLimit {
                        codec: codec.name.as_str().to_string(),
                        limit: PAYLOAD_UNFOLD_LIMIT,
                    });
                }
                let value = match JsonValue::try_from(output) {
                    Ok(value) => value,
                    Err(error) => {
                        let cause = CodecError::JaqTransform {
                            codec: codec.name.as_str().to_string(),
                            reason: error.to_string(),
                        };
                        return Err(cause.at(UnfoldPosition::Output {
                            input: input_index,
                            output: output_index,
                        }));
                    }
                };
                messages.push(UnfoldedMessage {
                    input: input_index,
                    output: output_index,
                    value,
                });
            }
        }
        Ok(Self { messages })
    }

    /// Appends every message to `builder`, or none of them, and answers how many it appended.
    ///
    /// A message that does not fit the schema closes the row it started and drops the rows of the
    /// payload's earlier messages, so the builder keeps exactly the rows it held before.
    pub(super) fn append_to(
        self,
        codec: &CompiledCodec,
        builder: &mut RuntimeRecordBatchBuilder,
    ) -> Result<usize, CodecError> {
        let rows_before = builder.rows();
        let appended = self.messages.len();
        for UnfoldedMessage {
            input,
            output,
            value,
        } in self.messages
        {
            let decoded = decode_json_value(codec, &value, None, builder);
            if let Err(cause) = finish_decoded_row(codec, builder, decoded) {
                builder.abandon_rows_after(rows_before);
                return Err(cause.at(UnfoldPosition::Output { input, output }));
            }
        }
        Ok(appended)
    }
}

impl CodecError {
    /// This failure, placed at `position` in the payload that was unfolding.
    fn at(self, position: UnfoldPosition) -> Self {
        Self::Unfold {
            position,
            cause: Box::new(self),
        }
    }
}

impl CompiledCodec {
    /// The failure of the ON INGESTION program on one input value.
    ///
    /// The evaluator's own message quotes the values it failed on, so the diagnostic names only the
    /// codec and the position, and the message is recorded at trace level with the rest of the
    /// payload-bearing detail.
    fn evaluation_failure(&self, input: usize, error: &JaqProgramError) -> CodecError {
        trace!(
            codec = self.name.as_str(),
            input,
            error = %error,
            "ON INGESTION program evaluation failed"
        );
        let cause = CodecError::JaqIngestionEvaluation {
            codec: self.name.as_str().to_string(),
        };
        cause.at(UnfoldPosition::Input { input })
    }
}

impl CompiledJaqNativeCodec {
    /// Unfolds a payload of this codec's format through its ON INGESTION program.
    pub(super) fn unfold(
        &self,
        codec: &CompiledCodec,
        payload: &Bytes,
    ) -> Result<UnfoldedPayload, CodecError> {
        let Some(program) = self.transformations.on_ingestion.as_deref() else {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "JAQ-native codec used for decoding must declare ON INGESTION \
                         transformation"
                    .to_string(),
            });
        };
        // A text payload that is not valid UTF-8 is malformed before its first value is read.
        let values = match self.format.read_values(payload) {
            Ok(values) => values,
            Err(error) => {
                let cause = self.read_failure(codec, error);
                return Err(cause.at(UnfoldPosition::Input { input: 0 }));
            }
        };
        let inputs = values.map(|value| value.map_err(|error| self.read_failure(codec, error)));
        UnfoldedPayload::unfold(codec, program, inputs)
    }

    fn read_failure(&self, codec: &CompiledCodec, error: JaqFormatError) -> CodecError {
        CodecError::JaqNativeDecode {
            codec: codec.name.as_str().to_string(),
            format: self.format.name(),
            reason: error.to_string(),
        }
    }
}

impl CompiledProtobufCodec {
    /// Unfolds a payload holding one message of this codec's type through its ON INGESTION program.
    pub(super) fn unfold(
        &self,
        codec: &CompiledCodec,
        payload: &[u8],
    ) -> Result<UnfoldedPayload, CodecError> {
        let Some(program) = self.transformations.on_ingestion.as_deref() else {
            return Err(CodecError::InvalidCodec {
                codec: codec.name.as_str().to_string(),
                reason: "protobuf codec used for decoding must declare ON INGESTION transformation"
                    .to_string(),
            });
        };
        let input = self.read_input(codec, payload);
        UnfoldedPayload::unfold(codec, program, std::iter::once(input))
    }

    /// Decodes the one message a protobuf payload holds into the value the program runs on.
    fn read_input(&self, codec: &CompiledCodec, payload: &[u8]) -> Result<JaqInput, CodecError> {
        let value = decode_protobuf_payload(&self.message, payload).map_err(|reason| {
            CodecError::ProtobufDecode {
                codec: codec.name.as_str().to_string(),
                reason,
            }
        })?;
        JaqInput::try_from(value).map_err(|error| CodecError::ProtobufDecode {
            codec: codec.name.as_str().to_string(),
            reason: error.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use nervix_models::{
        CodecJaqFormat, CodecJaqTransformations, CodecWireFormat, CreateCodec, CreateSchema,
        ParseAsType, ResolvedCodecWireFormat, SchemaField,
    };
    use triomphe::Arc;

    use super::*;
    use crate::runtime_schema::{
        RuntimeRecordBatch, RuntimeValue, compile_codec, compile_schema, decode_with_codec,
    };

    fn named<N>(raw: &str) -> N
    where
        N: for<'a> TryFrom<&'a str>,
        for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
    {
        N::try_from(raw).expect("valid name")
    }

    /// A codec of `format` over a one-field event schema whose ON INGESTION program is `program`.
    fn unfolding_codec(format: CodecJaqFormat, program: &str) -> Arc<CompiledCodec> {
        let schema = Arc::new(compile_schema(&CreateSchema {
            name: named("event"),
            fields: vec![SchemaField {
                name: named("user_id"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            }],
        }));
        let transformations = CodecJaqTransformations {
            on_ingestion: Some(program.to_string()),
            on_emitting: None,
        };
        compile_codec(
            &CreateCodec {
                name: named("unfolding_codec"),
                wire_format: CodecWireFormat::JaqNative {
                    format,
                    transformations: transformations.clone(),
                },
                schema: named("event"),
                encoding_rules: Vec::new(),
            },
            schema,
            ResolvedCodecWireFormat::JaqNative {
                format,
                transformations: &transformations,
            },
        )
        .expect("the unfolding codec should compile")
    }

    fn decode(
        codec: &CompiledCodec,
        builder: &mut RuntimeRecordBatchBuilder,
        payload: &[u8],
    ) -> Result<usize, CodecError> {
        decode_with_codec(codec, Cow::Borrowed(payload), builder)
    }

    fn user_ids(batch: &RuntimeRecordBatch) -> Vec<i64> {
        let mut user_ids = Vec::new();
        for row in 0..batch.batch().num_rows() {
            let value = batch
                .value(row, "user_id")
                .expect("the user id column must be readable");
            match value {
                Some(RuntimeValue::I64(user_id)) => user_ids.push(user_id),
                other => panic!("row {row} must hold an I64 user id, found {other:?}"),
            }
        }
        user_ids
    }

    #[test]
    fn unfolds_an_array_into_one_message_per_element_in_order() {
        let codec = unfolding_codec(CodecJaqFormat::Json, ".[]");
        let mut builder = codec.schema().batch_builder(3);

        let decoded = decode(
            &codec,
            &mut builder,
            br#"[{"user_id":1},{"user_id":2},{"user_id":3}]"#,
        )
        .expect("the array should unfold");

        assert_eq!(decoded, 3);
        assert_eq!(
            user_ids(&builder.finish().expect("the batch should build")),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn unfolds_every_output_of_one_value_before_the_next_value() {
        let codec = unfolding_codec(CodecJaqFormat::Json, "., {user_id: (.user_id * 10)}");
        let mut builder = codec.schema().batch_builder(4);

        let decoded = decode(&codec, &mut builder, b"{\"user_id\":1}\n{\"user_id\":2}\n")
            .expect("newline-delimited JSON should unfold");

        assert_eq!(decoded, 4);
        assert_eq!(
            user_ids(&builder.finish().expect("the batch should build")),
            vec![1, 10, 2, 20]
        );
    }

    #[test]
    fn decodes_a_payload_without_values_or_outputs_into_no_messages() {
        let codec = unfolding_codec(CodecJaqFormat::Json, ".[] | select(.keep)");
        let mut builder = codec.schema().batch_builder(1);

        assert_eq!(
            decode(&codec, &mut builder, b"").expect("an empty payload holds no value"),
            0
        );
        assert_eq!(
            decode(&codec, &mut builder, br#"[{"user_id":1,"keep":false}]"#)
                .expect("a payload the program selects nothing from should decode"),
            0
        );
        assert_eq!(builder.rows(), 0);
    }

    #[test]
    fn rejects_a_whole_payload_whose_output_does_not_fit_the_schema() {
        let codec = unfolding_codec(CodecJaqFormat::Json, ".[]");
        let mut builder = codec.schema().batch_builder(4);
        decode(&codec, &mut builder, br#"[{"user_id":1}]"#)
            .expect("the first payload should unfold");

        let error = decode(
            &codec,
            &mut builder,
            br#"[{"user_id":2},{"user_id":"confidential"},{"user_id":3}]"#,
        )
        .expect_err("an element of the wrong type must reject the whole payload");

        assert!(
            matches!(
                error,
                CodecError::Unfold {
                    position: UnfoldPosition::Output {
                        input: 0,
                        output: 1
                    },
                    ..
                }
            ),
            "{error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("codec 'unfolding_codec' failed to parse field 'user_id'"),
            "{message}"
        );
        assert!(message.ends_with("(input value 0, output 1)"), "{message}");
        assert!(!message.contains("confidential"), "{message}");
        assert_eq!(builder.rows(), 1);
        assert_eq!(
            user_ids(&builder.finish().expect("the batch should build")),
            vec![1]
        );
    }

    #[test]
    fn names_the_position_of_an_output_that_is_not_an_object() {
        let codec = unfolding_codec(CodecJaqFormat::Json, ".[]");
        let mut builder = codec.schema().batch_builder(2);

        let error = decode(&codec, &mut builder, br#"[{"user_id":1},7]"#)
            .expect_err("an output that is not an object must be rejected");

        assert_eq!(
            error.to_string(),
            "codec 'unfolding_codec' expected an object (input value 0, output 1)"
        );
        assert_eq!(builder.rows(), 0);
    }

    #[test]
    fn names_the_position_of_a_malformed_input_value() {
        let codec = unfolding_codec(CodecJaqFormat::Json, ".");
        let mut builder = codec.schema().batch_builder(2);

        let error = decode(&codec, &mut builder, b"{\"user_id\":1}\n{\"user_id\":")
            .expect_err("a truncated second value must be rejected");

        assert!(
            matches!(
                error,
                CodecError::Unfold {
                    position: UnfoldPosition::Input { input: 1 },
                    ..
                }
            ),
            "{error}"
        );
        assert_eq!(builder.rows(), 0);
    }

    #[test]
    fn reports_a_program_evaluation_error_without_the_values_it_failed_on() {
        let codec = unfolding_codec(CodecJaqFormat::Json, "{user_id: (.user_id + 1)}");
        let mut builder = codec.schema().batch_builder(2);

        let error = decode(
            &codec,
            &mut builder,
            b"{\"user_id\":1}\n{\"user_id\":\"confidential\"}",
        )
        .expect_err("adding a number to a string must fail");

        assert_eq!(
            error.to_string(),
            "codec 'unfolding_codec' ON INGESTION program evaluation failed (input value 1)"
        );
        assert_eq!(builder.rows(), 0);
    }

    #[test]
    fn unfolds_a_payload_into_exactly_the_limit() {
        let codec = unfolding_codec(CodecJaqFormat::Json, "range(65536) | {user_id: .}");
        let mut builder = codec.schema().batch_builder(PAYLOAD_UNFOLD_LIMIT);

        assert_eq!(
            decode(&codec, &mut builder, b"null").expect("a payload at the limit should unfold"),
            PAYLOAD_UNFOLD_LIMIT
        );
    }

    #[test]
    fn rejects_a_payload_that_unfolds_beyond_the_limit() {
        let codec = unfolding_codec(CodecJaqFormat::Json, "range(65537) | {user_id: .}");
        let mut builder = codec.schema().batch_builder(1);

        let error = decode(&codec, &mut builder, b"null")
            .expect_err("a payload beyond the limit must be rejected");

        assert_eq!(
            error.to_string(),
            "codec 'unfolding_codec' payload exceeds the unfold limit of 65536 messages"
        );
        assert_eq!(builder.rows(), 0);
    }

    #[test]
    fn stops_an_unbounded_program_at_the_limit() {
        let codec = unfolding_codec(CodecJaqFormat::Json, "repeat({user_id: 1})");
        let mut builder = codec.schema().batch_builder(1);

        let error = decode(&codec, &mut builder, br#"{"user_id":1}"#)
            .expect_err("an unbounded program must be stopped at the limit");

        assert!(matches!(error, CodecError::UnfoldLimit { .. }), "{error}");
        assert_eq!(builder.rows(), 0);
    }

    #[test]
    fn unfolds_the_root_element_of_a_declared_xml_document() {
        let codec = unfolding_codec(
            CodecJaqFormat::Xml,
            r#"{user_id: (.c[] | select(.t == "user_id").c[0] | tonumber)}"#,
        );
        let mut builder = codec.schema().batch_builder(1);

        let decoded = decode(
            &codec,
            &mut builder,
            b"<?xml version=\"1.0\"?>\n<event><user_id>5</user_id></event>\n",
        )
        .expect("a declared XML document should decode");

        assert_eq!(decoded, 1);
        assert_eq!(
            user_ids(&builder.finish().expect("the batch should build")),
            vec![5]
        );
    }
}
