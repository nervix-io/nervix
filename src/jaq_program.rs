//! Compiled jaq programs and the schemaless wire formats they read and write.
//!
//! Codecs and websocket signaling protocols both express boundary behavior as jaq programs over
//! self-describing payloads. This module owns compiling those programs once, running them, and
//! converting each supported format to and from the JSON values programs operate on.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The compiled program, its execution, and the conversion of every supported
//!   schemaless format to and from the JSON values a program operates on.
//! - **Depends on.** `jaq` and the wire-format crates.
//! - **Must not know.** Schemas, relays or branches. Its callers decide what a program means; this
//!   module only runs it.

use std::{fmt::Display, io, str::FromStr};

use bytes::Bytes;
use jaq_core::{
    Compiler as JaqCompiler, Ctx as JaqCtx, Filter as JaqFilter, Vars as JaqVars, data,
    load::{Arena, File, Loader},
    unwrap_valr,
};
use jaq_fmts::{
    Format as JaqFormat, read as jaq_read,
    write::{self as jaq_write, Writer as JaqWriter},
};
use jaq_json::{Num as JaqNum, Val as JaqVal};
use nervix_models::{CodecJaqFormat, SignalingWireFormat};
use serde_json::{Map as JsonMap, Value as JsonValue};
use strum::IntoStaticStr;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum JaqProgramError {
    #[error("invalid jaq program: {reason}")]
    Compile { reason: String },
    #[error("jaq program produced no output")]
    NoOutput,
    #[error("jaq program produced multiple outputs")]
    MultipleOutputs,
    #[error("jaq program evaluation failed: {reason}")]
    Eval { reason: String },
    #[error("jaq value is not valid JSON: {reason}")]
    NotJson { reason: String },
}

/// One value read from a payload, in the form a program runs on.
#[derive(Debug)]
pub struct JaqInput(JaqVal);

impl TryFrom<JsonValue> for JaqInput {
    type Error = JaqProgramError;

    fn try_from(value: JsonValue) -> Result<Self, Self::Error> {
        let value = serde_json::from_value(value).map_err(|error| JaqProgramError::Eval {
            reason: error.to_string(),
        })?;
        Ok(Self(value))
    }
}

/// One value a program produced, before it is read as JSON.
#[derive(Debug)]
pub struct JaqOutput(JaqVal);

impl JaqOutput {
    fn evaluated(output: Result<JaqVal, jaq_json::Error>) -> Result<Self, JaqProgramError> {
        match output {
            Ok(value) => Ok(Self(value)),
            Err(error) => Err(JaqProgramError::Eval {
                reason: error.to_string(),
            }),
        }
    }
}

impl TryFrom<JaqOutput> for JsonValue {
    type Error = JaqProgramError;

    fn try_from(output: JaqOutput) -> Result<Self, Self::Error> {
        jaq_value_to_json(output.0)
    }
}

/// A jaq program compiled once and reusable across payloads.
pub struct CompiledJaqProgram {
    source: String,
    filter: JaqFilter<data::JustLut<JaqVal>>,
}

impl std::fmt::Debug for CompiledJaqProgram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledJaqProgram")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl CompiledJaqProgram {
    pub fn compile(source: &str) -> Result<Self, JaqProgramError> {
        compile_filter(source, &[]).map(|filter| Self {
            source: source.to_string(),
            filter,
        })
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    /// Run the program and require exactly one output.
    pub fn run_single(&self, input: JsonValue) -> Result<JsonValue, JaqProgramError> {
        run_single(&self.filter, JaqInput::try_from(input)?, Vec::new())
    }

    /// Run the program and take its first output, if it produces any.
    ///
    /// Matchers probe payloads they were not written for, so an absent output is an ordinary
    /// answer rather than a failure.
    pub fn run_first(&self, input: JsonValue) -> Result<Option<JsonValue>, JaqProgramError> {
        run_first(&self.filter, JaqInput::try_from(input)?, Vec::new())
    }

    /// Run the program on one input value and yield its outputs in the order it produces them.
    ///
    /// Each output is produced only when it is read, so a caller that stops reading stops the
    /// program.
    pub fn outputs(
        &self,
        input: JaqInput,
    ) -> impl Iterator<Item = Result<JaqOutput, JaqProgramError>> + '_ {
        run(&self.filter, input, Vec::new())
    }
}

/// The variable through which a program reads handshake state.
pub const STATE_VAR: &str = "$state";

/// A jaq program that reads a state document through [`STATE_VAR`].
///
/// The variable must be declared at compile time and bound at run time, and the two must agree —
/// so this type owns both halves and callers only supply the state value.
pub struct StatefulJaqProgram {
    source: String,
    filter: JaqFilter<data::JustLut<JaqVal>>,
}

impl std::fmt::Debug for StatefulJaqProgram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatefulJaqProgram")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl StatefulJaqProgram {
    pub fn compile(source: &str) -> Result<Self, JaqProgramError> {
        compile_filter(source, &[STATE_VAR]).map(|filter| Self {
            source: source.to_string(),
            filter,
        })
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn run_single(
        &self,
        input: JsonValue,
        state: &JsonValue,
    ) -> Result<JsonValue, JaqProgramError> {
        run_single(&self.filter, JaqInput::try_from(input)?, self.bind(state)?)
    }

    pub fn run_first(
        &self,
        input: JsonValue,
        state: &JsonValue,
    ) -> Result<Option<JsonValue>, JaqProgramError> {
        run_first(&self.filter, JaqInput::try_from(input)?, self.bind(state)?)
    }

    fn bind(&self, state: &JsonValue) -> Result<Vec<JaqVal>, JaqProgramError> {
        let state: JaqVal =
            serde_json::from_value(state.clone()).map_err(|error| JaqProgramError::Eval {
                reason: error.to_string(),
            })?;
        Ok(vec![state])
    }
}

fn compile_filter(
    source: &str,
    global_vars: &[&str],
) -> Result<JaqFilter<data::JustLut<JaqVal>>, JaqProgramError> {
    let defs = jaq_core::defs()
        .chain(jaq_std::defs())
        .chain(jaq_json::defs());
    let funs = jaq_core::funs()
        .chain(jaq_std::funs())
        .chain(jaq_json::funs())
        .chain(jaq_fmts::funs());
    let loader = Loader::new(defs);
    let arena = Arena::default();
    let modules = loader
        .load(
            &arena,
            File {
                code: source,
                path: (),
            },
        )
        .map_err(|errors| JaqProgramError::Compile {
            reason: format!("{errors:?}"),
        })?;
    JaqCompiler::default()
        .with_funs(funs)
        .with_global_vars(global_vars.iter().copied())
        .compile(modules)
        .map_err(|errors| JaqProgramError::Compile {
            reason: format!("{errors:?}"),
        })
}

fn run<'a>(
    filter: &'a JaqFilter<data::JustLut<JaqVal>>,
    input: JaqInput,
    vars: Vec<JaqVal>,
) -> impl Iterator<Item = Result<JaqOutput, JaqProgramError>> + 'a {
    let ctx = JaqCtx::<data::JustLut<JaqVal>>::new(&filter.lut, JaqVars::new(vars));
    filter
        .id
        .run((ctx, input.0))
        .map(unwrap_valr)
        .map(JaqOutput::evaluated)
}

fn run_single(
    filter: &JaqFilter<data::JustLut<JaqVal>>,
    input: JaqInput,
    vars: Vec<JaqVal>,
) -> Result<JsonValue, JaqProgramError> {
    let mut outputs = run(filter, input, vars);
    let Some(output) = outputs.next() else {
        return Err(JaqProgramError::NoOutput);
    };
    let output = output?;
    if outputs.next().is_some() {
        return Err(JaqProgramError::MultipleOutputs);
    }
    JsonValue::try_from(output)
}

fn run_first(
    filter: &JaqFilter<data::JustLut<JaqVal>>,
    input: JaqInput,
    vars: Vec<JaqVal>,
) -> Result<Option<JsonValue>, JaqProgramError> {
    let Some(output) = run(filter, input, vars).next() else {
        return Ok(None);
    };
    let output = output?;
    JsonValue::try_from(output).map(Some)
}

#[derive(Debug, Error)]
pub enum JaqFormatError {
    #[error("failed to decode {format} payload: {reason}")]
    Decode {
        format: &'static str,
        reason: String,
    },
    #[error("failed to encode {format} payload: {reason}")]
    Encode {
        format: &'static str,
        reason: String,
    },
}

macro_rules! declare_jaq_native_formats {
    (
        common {$($Common:ident => $common_binary:literal,)+}
        signaling_only {$($SignalingOnly:ident => $signaling_binary:literal,)+}
    ) => {
        /// A self-describing wire format that jaq programs read from and write to.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, IntoStaticStr)]
        #[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
        pub enum JaqNativeFormat {
            $($Common,)+
            $($SignalingOnly,)+
        }

        impl JaqNativeFormat {
            pub fn name(self) -> &'static str {
                self.into()
            }

            /// Whether values of this format travel as binary rather than text frames.
            pub fn is_binary(self) -> bool {
                match self {
                    $(Self::$Common => $common_binary,)+
                    $(Self::$SignalingOnly => $signaling_binary,)+
                }
            }

            fn jaq_format(self) -> JaqFormat {
                match self {
                    $(Self::$Common => JaqFormat::$Common,)+
                    $(Self::$SignalingOnly => JaqFormat::$SignalingOnly,)+
                }
            }
        }

        impl From<CodecJaqFormat> for JaqNativeFormat {
            fn from(format: CodecJaqFormat) -> Self {
                match format {
                    $(CodecJaqFormat::$Common => Self::$Common,)+
                }
            }
        }

        impl TryFrom<&SignalingWireFormat> for JaqNativeFormat {
            type Error = ();

            fn try_from(format: &SignalingWireFormat) -> Result<Self, Self::Error> {
                match format {
                    $(SignalingWireFormat::$Common => Ok(Self::$Common),)+
                    $(SignalingWireFormat::$SignalingOnly => Ok(Self::$SignalingOnly),)+
                    SignalingWireFormat::Protobuf(_) => Err(()),
                }
            }
        }
    };
}

declare_jaq_native_formats! {
    common {
        Json => false,
        Yaml => false,
        Toml => false,
        Xml => false,
        Cbor => true,
    }
    signaling_only {
        Raw => false,
    }
}

impl JaqNativeFormat {
    /// Read the values `payload` holds, in payload order.
    ///
    /// `RAW` slurps the whole payload into one string rather than splitting it into lines, because
    /// a payload is one message. An `XML` payload holds only its root element, as
    /// [`JaqPayloadValues`] describes.
    pub fn read_values(self, payload: &Bytes) -> Result<JaqPayloadValues<'_>, JaqFormatError> {
        let format = self.jaq_format();
        let source = jaq_read::bytes_str(format, payload).map_err(|error| self.decode(error))?;
        let slurp = self == Self::Raw;
        Ok(JaqPayloadValues {
            format: self,
            values: jaq_read::parse(format, payload, source, slurp),
        })
    }

    /// Decode a payload into the single value it represents.
    pub fn read_single_value(self, payload: &[u8]) -> Result<JsonValue, JaqFormatError> {
        let bytes = Bytes::copy_from_slice(payload);
        let mut values = self.read_values(&bytes)?;
        let Some(value) = values.next() else {
            return Err(self.decode("payload produced no input values"));
        };
        let value = value?;
        if values.next().is_some() {
            return Err(self.decode("payload produced multiple input values"));
        }
        jaq_value_to_json(value.0).map_err(|error| self.decode(error))
    }

    /// Whether a value the reader produced is one of the payload's values.
    ///
    /// The XML reader also yields the declaration, document type, comments, and processing
    /// instructions around the root element. Only an element, which jaq reads as an object that
    /// carries its tag in `t`, is a value of the payload.
    fn holds_value(self, value: &JaqVal) -> bool {
        match self {
            Self::Xml => {
                let JaqVal::Obj(fields) = value else {
                    return false;
                };
                fields.contains_key(&JaqVal::from(String::from("t")))
            }
            Self::Json | Self::Yaml | Self::Toml | Self::Cbor | Self::Raw => true,
        }
    }

    /// The failure to read one value, described without the payload content a reader's own
    /// message can quote.
    fn read_failure(self, error: &io::Error) -> JaqFormatError {
        if let Some(source) = error.get_ref()
            && let Some(jaq_read::yaml::Error::Scalar(tag, _, span)) =
                source.downcast_ref::<jaq_read::yaml::Error>()
        {
            return self.decode(format_args!(
                "scalar at {span} is incompatible with tag {tag}"
            ));
        }
        self.decode(error)
    }

    /// Encode one value as a payload of this format.
    pub fn write_value(self, value: JsonValue) -> Result<Vec<u8>, JaqFormatError> {
        if self == Self::Raw {
            let JsonValue::String(value) = value else {
                return Err(self.encode("RAW payloads require a string value"));
            };
            return Ok(value.into_bytes());
        }
        let value: JaqVal = serde_json::from_value(value).map_err(|error| self.encode(error))?;
        let mut encoded = Vec::new();
        let writer = JaqWriter {
            format: self.jaq_format(),
            // YAML reads `{1:2}` as the key `"1:2"`, so a space after the separator is required
            // rather than cosmetic.
            pp: jaq_json::write::Pp {
                sep_space: true,
                ..Default::default()
            },
            join: true,
        };
        jaq_write::write(&mut encoded, &writer, &value).map_err(|error| self.encode(error))?;
        Ok(encoded)
    }

    fn decode(self, reason: impl Display) -> JaqFormatError {
        JaqFormatError::Decode {
            format: self.name(),
            reason: reason.to_string(),
        }
    }

    fn encode(self, reason: impl Display) -> JaqFormatError {
        JaqFormatError::Encode {
            format: self.name(),
            reason: reason.to_string(),
        }
    }
}

/// The values one payload holds, read lazily in payload order.
///
/// A value is parsed only when it is read. An `XML` payload yields its root element alone: the
/// declaration, document type, comments, and processing instructions outside it are skipped, and a
/// second root element is malformed.
pub struct JaqPayloadValues<'a> {
    format: JaqNativeFormat,
    values: Box<dyn Iterator<Item = io::Result<JaqVal>> + 'a>,
}

impl Iterator for JaqPayloadValues<'_> {
    type Item = Result<JaqInput, JaqFormatError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let value = match self.values.next()? {
                Ok(value) => value,
                Err(error) => return Some(Err(self.format.read_failure(&error))),
            };
            if self.format.holds_value(&value) {
                return Some(Ok(JaqInput(value)));
            }
        }
    }
}

fn jaq_value_to_json(value: JaqVal) -> Result<JsonValue, JaqProgramError> {
    jaq_value_to_json_inner(value).map_err(|reason| JaqProgramError::NotJson { reason })
}

fn jaq_value_to_json_inner(value: JaqVal) -> Result<JsonValue, String> {
    match value {
        JaqVal::Null => Ok(JsonValue::Null),
        JaqVal::Bool(value) => Ok(JsonValue::Bool(value)),
        JaqVal::Num(value) => jaq_num_to_json(value),
        JaqVal::BStr(_) => {
            Err("jaq output contains binary string, which is not valid JSON".to_string())
        }
        JaqVal::TStr(value) => String::from_utf8(value.to_vec())
            .map(JsonValue::String)
            .map_err(|error| error.to_string()),
        JaqVal::Arr(values) => values
            .iter()
            .cloned()
            .map(jaq_value_to_json_inner)
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Array),
        JaqVal::Obj(values) => {
            let mut object = JsonMap::new();
            for (key, value) in values.iter() {
                let key = match key {
                    JaqVal::TStr(key) => {
                        String::from_utf8(key.to_vec()).map_err(|error| error.to_string())?
                    }
                    _ => {
                        return Err("jaq output contains a non-string object key, which is not \
                                    valid JSON"
                            .to_string());
                    }
                };
                object.insert(key, jaq_value_to_json_inner(value.clone())?);
            }
            Ok(JsonValue::Object(object))
        }
    }
}

fn jaq_num_to_json(value: JaqNum) -> Result<JsonValue, String> {
    let rendered = value.to_string();
    serde_json::Number::from_str(&rendered)
        .map(JsonValue::Number)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn runs_a_program_with_a_single_output() {
        let program = CompiledJaqProgram::compile(".payload").expect("program should compile");

        assert_eq!(
            program
                .run_single(json!({"payload": {"user_id": 7}}))
                .expect("program should run"),
            json!({"user_id": 7})
        );
    }

    #[test]
    fn rejects_a_program_producing_multiple_outputs() {
        let program = CompiledJaqProgram::compile(".[]").expect("program should compile");

        assert!(matches!(
            program.run_single(json!([1, 2])),
            Err(JaqProgramError::MultipleOutputs)
        ));
    }

    #[test]
    fn rejects_a_program_producing_no_output() {
        let program = CompiledJaqProgram::compile("select(.ok)").expect("program should compile");

        assert!(matches!(
            program.run_single(json!({"ok": false})),
            Err(JaqProgramError::NoOutput)
        ));
    }

    #[test]
    fn reports_no_first_output_for_a_filtered_probe() {
        let program = CompiledJaqProgram::compile("select(.ok)").expect("program should compile");

        assert_eq!(
            program
                .run_first(json!({"ok": false}))
                .expect("probe should run"),
            None
        );
    }

    #[test]
    fn reads_state_through_the_state_variable() {
        let program =
            StatefulJaqProgram::compile("{token: $state.token, id: .id}").expect("compiles");

        assert_eq!(
            program
                .run_single(json!({"id": 1}), &json!({"token": "tok-7f3a"}))
                .expect("program should run"),
            json!({"token": "tok-7f3a", "id": 1})
        );
    }

    #[test]
    fn matches_a_frame_against_captured_state() {
        let matcher = StatefulJaqProgram::compile(".id == $state.pending").expect("compiles");

        assert_eq!(
            matcher
                .run_first(json!({"id": 7}), &json!({"pending": 7}))
                .expect("matcher should run"),
            Some(json!(true))
        );
        assert_eq!(
            matcher
                .run_first(json!({"id": 8}), &json!({"pending": 7}))
                .expect("matcher should run"),
            Some(json!(false))
        );
    }

    #[test]
    fn a_stateful_program_may_ignore_the_state_variable() {
        let program = StatefulJaqProgram::compile("{id: 1}").expect("compiles");

        assert_eq!(
            program
                .run_single(JsonValue::Null, &json!({}))
                .expect("program should run"),
            json!({"id": 1})
        );
    }

    #[test]
    fn a_stateless_program_cannot_reference_state() {
        assert!(matches!(
            CompiledJaqProgram::compile("$state.token"),
            Err(JaqProgramError::Compile { .. })
        ));
    }

    #[test]
    fn rejects_an_invalid_program() {
        assert!(matches!(
            CompiledJaqProgram::compile(".["),
            Err(JaqProgramError::Compile { .. })
        ));
    }

    #[test]
    fn round_trips_values_through_every_structured_format() {
        for format in [
            JaqNativeFormat::Json,
            JaqNativeFormat::Yaml,
            JaqNativeFormat::Toml,
            JaqNativeFormat::Cbor,
        ] {
            let value = json!({"id": 1, "name": "nervix"});
            let encoded = format
                .write_value(value.clone())
                .unwrap_or_else(|error| panic!("{} encode failed: {error}", format.name()));
            let decoded = format
                .read_single_value(&encoded)
                .unwrap_or_else(|error| panic!("{} decode failed: {error}", format.name()));

            assert_eq!(decoded, value, "{} round trip", format.name());
        }
    }

    #[test]
    fn writes_json_with_a_separator_space_and_declaration_order() {
        assert_eq!(
            String::from_utf8(
                JaqNativeFormat::Json
                    .write_value(json!({"method": "SUBSCRIBE", "id": 1}))
                    .expect("json encode should succeed")
            )
            .expect("json output is utf-8"),
            r#"{"method": "SUBSCRIBE", "id": 1}"#
        );
    }

    #[test]
    fn reads_a_whole_raw_payload_as_one_string() {
        assert_eq!(
            JaqNativeFormat::Raw
                .read_single_value(b"first\nsecond")
                .expect("raw decode should succeed"),
            json!("first\nsecond")
        );
    }

    #[test]
    fn writes_a_raw_payload_verbatim() {
        assert_eq!(
            JaqNativeFormat::Raw
                .write_value(json!("WELCOME"))
                .expect("raw encode should succeed"),
            b"WELCOME".to_vec()
        );
    }

    #[test]
    fn rejects_a_non_string_raw_payload() {
        assert!(matches!(
            JaqNativeFormat::Raw.write_value(json!({"id": 1})),
            Err(JaqFormatError::Encode { .. })
        ));
    }

    /// Reads every value of `payload` as JSON, for tests that assert on what a payload holds.
    fn read_json_values(format: JaqNativeFormat, payload: &[u8]) -> Vec<JsonValue> {
        let bytes = Bytes::copy_from_slice(payload);
        let mut values = Vec::new();
        for value in format
            .read_values(&bytes)
            .expect("the payload should be readable")
        {
            let value = value.expect("every value of the payload should parse");
            values.push(jaq_value_to_json(value.0).expect("every value should be JSON"));
        }
        values
    }

    #[test]
    fn reads_every_value_of_a_json_sequence() {
        assert_eq!(
            read_json_values(JaqNativeFormat::Json, b"{\"id\":1}\n{\"id\":2} {\"id\":3}"),
            vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})]
        );
    }

    #[test]
    fn reads_every_document_of_a_yaml_stream() {
        assert_eq!(
            read_json_values(JaqNativeFormat::Yaml, b"id: 1\n---\nid: 2\n"),
            vec![json!({"id": 1}), json!({"id": 2})]
        );
    }

    #[test]
    fn reads_every_item_of_a_cbor_sequence() {
        let mut payload = Vec::new();
        for id in 1..=2 {
            ciborium::into_writer(&json!({"id": id}), &mut payload)
                .expect("a CBOR item should encode");
        }

        assert_eq!(
            read_json_values(JaqNativeFormat::Cbor, &payload),
            vec![json!({"id": 1}), json!({"id": 2})]
        );
    }

    #[test]
    fn reads_the_root_element_of_an_xml_payload_without_its_prolog() {
        assert_eq!(
            read_json_values(
                JaqNativeFormat::Xml,
                b"<?xml version=\"1.0\"?>\n<!-- batch -->\n<order id=\"7\"/>\n<!-- end -->\n",
            ),
            vec![json!({"t": "order", "a": {"id": "7"}})]
        );
    }

    #[test]
    fn rejects_a_second_xml_root_element() {
        let bytes = Bytes::from_static(b"<first/><second/>");
        let mut values = JaqNativeFormat::Xml
            .read_values(&bytes)
            .expect("the payload should be readable");

        assert!(matches!(values.next(), Some(Ok(_))));
        assert!(matches!(
            values.next(),
            Some(Err(JaqFormatError::Decode { .. }))
        ));
    }

    #[test]
    fn reads_no_value_from_an_empty_payload_of_a_sequence_format() {
        for format in [
            JaqNativeFormat::Json,
            JaqNativeFormat::Yaml,
            JaqNativeFormat::Xml,
            JaqNativeFormat::Cbor,
        ] {
            assert_eq!(
                read_json_values(format, b""),
                Vec::<JsonValue>::new(),
                "an empty {} payload holds no value",
                format.name()
            );
        }
    }

    #[test]
    fn reads_an_empty_toml_payload_as_one_empty_document() {
        assert_eq!(
            read_json_values(JaqNativeFormat::Toml, b""),
            vec![json!({})]
        );
    }

    #[test]
    fn stops_reading_at_the_first_malformed_value() {
        let bytes = Bytes::from_static(b"{\"id\":1}\n{\"id\":");
        let mut values = JaqNativeFormat::Json
            .read_values(&bytes)
            .expect("the payload should be readable");

        assert!(matches!(values.next(), Some(Ok(_))));
        assert!(matches!(
            values.next(),
            Some(Err(JaqFormatError::Decode { .. }))
        ));
    }

    #[test]
    fn describes_a_yaml_scalar_that_conflicts_with_its_tag_without_quoting_it() {
        let bytes = Bytes::from_static(b"id: !!int confidential\n");
        let mut values = JaqNativeFormat::Yaml
            .read_values(&bytes)
            .expect("the payload should be readable");
        let Some(Err(error)) = values.next() else {
            panic!("a scalar that conflicts with its tag must fail to read");
        };
        let message = error.to_string();

        assert!(message.contains("incompatible with tag"), "{message}");
        assert!(!message.contains("confidential"), "{message}");
    }

    #[test]
    fn yields_every_output_of_a_program_in_order() {
        let program = CompiledJaqProgram::compile(".[]").expect("program should compile");
        let input = JaqInput::try_from(json!([1, 2, 3])).expect("the input should convert");
        let mut outputs = Vec::new();
        for output in program.outputs(input) {
            let output = output.expect("every output should evaluate");
            outputs.push(JsonValue::try_from(output).expect("every output should be JSON"));
        }

        assert_eq!(outputs, vec![json!(1), json!(2), json!(3)]);
    }

    #[test]
    fn yields_no_output_for_a_value_the_program_rejects() {
        let program = CompiledJaqProgram::compile("select(.ok)").expect("program should compile");
        let input = JaqInput::try_from(json!({"ok": false})).expect("the input should convert");

        assert_eq!(program.outputs(input).count(), 0);
    }

    #[test]
    fn stops_an_unbounded_program_when_its_outputs_are_no_longer_read() {
        let program = CompiledJaqProgram::compile("repeat(1)").expect("program should compile");
        let input = JaqInput::try_from(JsonValue::Null).expect("the input should convert");

        assert_eq!(program.outputs(input).take(3).count(), 3);
    }

    #[test]
    fn reports_only_cbor_as_a_binary_format() {
        assert!(JaqNativeFormat::Cbor.is_binary());
        assert!(!JaqNativeFormat::Json.is_binary());
        assert!(!JaqNativeFormat::Raw.is_binary());
    }
}
