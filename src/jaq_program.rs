//! Compiled jaq programs and the schemaless wire formats they read and write.
//!
//! Codecs and websocket signaling protocols both express boundary behavior as jaq programs over
//! self-describing payloads. This module owns compiling those programs once, running them, and
//! converting each supported format to and from the JSON values programs operate on.

use std::{fmt::Display, str::FromStr};

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
        run_single(&self.filter, input, Vec::new())
    }

    /// Run the program and take its first output, if it produces any.
    ///
    /// Matchers probe payloads they were not written for, so an absent output is an ordinary
    /// answer rather than a failure.
    pub fn run_first(&self, input: JsonValue) -> Result<Option<JsonValue>, JaqProgramError> {
        run_first(&self.filter, input, Vec::new())
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
        run_single(&self.filter, input, self.bind(state)?)
    }

    pub fn run_first(
        &self,
        input: JsonValue,
        state: &JsonValue,
    ) -> Result<Option<JsonValue>, JaqProgramError> {
        run_first(&self.filter, input, self.bind(state)?)
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
    input: JsonValue,
    vars: Vec<JaqVal>,
) -> Result<impl Iterator<Item = Result<JaqVal, jaq_json::Error>> + 'a, JaqProgramError> {
    let input: JaqVal = serde_json::from_value(input).map_err(|error| JaqProgramError::Eval {
        reason: error.to_string(),
    })?;
    let ctx = JaqCtx::<data::JustLut<JaqVal>>::new(&filter.lut, JaqVars::new(vars));
    Ok(filter.id.run((ctx, input)).map(unwrap_valr))
}

fn run_single(
    filter: &JaqFilter<data::JustLut<JaqVal>>,
    input: JsonValue,
    vars: Vec<JaqVal>,
) -> Result<JsonValue, JaqProgramError> {
    let mut outputs = run(filter, input, vars)?;
    let output = outputs
        .next()
        .ok_or(JaqProgramError::NoOutput)?
        .map_err(|error| JaqProgramError::Eval {
            reason: error.to_string(),
        })?;
    if outputs.next().is_some() {
        return Err(JaqProgramError::MultipleOutputs);
    }
    jaq_value_to_json(output)
}

fn run_first(
    filter: &JaqFilter<data::JustLut<JaqVal>>,
    input: JsonValue,
    vars: Vec<JaqVal>,
) -> Result<Option<JsonValue>, JaqProgramError> {
    let Some(output) = run(filter, input, vars)?.next() else {
        return Ok(None);
    };
    let output = output.map_err(|error| JaqProgramError::Eval {
        reason: error.to_string(),
    })?;
    jaq_value_to_json(output).map(Some)
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

/// A self-describing wire format that jaq programs read from and write to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JaqNativeFormat {
    Json,
    Yaml,
    Toml,
    Xml,
    Cbor,
    Raw,
}

impl JaqNativeFormat {
    pub fn name(self) -> &'static str {
        match self {
            Self::Json => "JSON",
            Self::Yaml => "YAML",
            Self::Toml => "TOML",
            Self::Xml => "XML",
            Self::Cbor => "CBOR",
            Self::Raw => "RAW",
        }
    }

    /// Whether values of this format travel as binary rather than text frames.
    pub fn is_binary(self) -> bool {
        matches!(self, Self::Cbor)
    }

    fn jaq_format(self) -> JaqFormat {
        match self {
            Self::Json => JaqFormat::Json,
            Self::Yaml => JaqFormat::Yaml,
            Self::Toml => JaqFormat::Toml,
            Self::Xml => JaqFormat::Xml,
            Self::Cbor => JaqFormat::Cbor,
            Self::Raw => JaqFormat::Raw,
        }
    }

    /// Decode a payload into the single value it represents.
    ///
    /// `RAW` slurps the whole payload into one string rather than splitting it into lines, because
    /// a payload is one message.
    pub fn read_single_value(self, payload: &[u8]) -> Result<JsonValue, JaqFormatError> {
        let bytes = Bytes::copy_from_slice(payload);
        let format = self.jaq_format();
        let source = jaq_read::bytes_str(format, &bytes).map_err(|error| self.decode(error))?;
        let slurp = self == Self::Raw;
        let mut values = jaq_read::parse(format, &bytes, source, slurp);
        let value = values
            .next()
            .ok_or_else(|| self.decode("payload produced no input values"))?
            .map_err(|error| self.decode(error))?;
        if values.next().is_some() {
            return Err(self.decode("payload produced multiple input values"));
        }
        jaq_value_to_json(value).map_err(|error| self.decode(error))
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

impl From<CodecJaqFormat> for JaqNativeFormat {
    fn from(format: CodecJaqFormat) -> Self {
        match format {
            CodecJaqFormat::Json => Self::Json,
            CodecJaqFormat::Yaml => Self::Yaml,
            CodecJaqFormat::Toml => Self::Toml,
            CodecJaqFormat::Xml => Self::Xml,
            CodecJaqFormat::Cbor => Self::Cbor,
        }
    }
}

impl TryFrom<&SignalingWireFormat> for JaqNativeFormat {
    type Error = ();

    fn try_from(format: &SignalingWireFormat) -> Result<Self, Self::Error> {
        match format {
            SignalingWireFormat::Json => Ok(Self::Json),
            SignalingWireFormat::Yaml => Ok(Self::Yaml),
            SignalingWireFormat::Toml => Ok(Self::Toml),
            SignalingWireFormat::Xml => Ok(Self::Xml),
            SignalingWireFormat::Cbor => Ok(Self::Cbor),
            SignalingWireFormat::Raw => Ok(Self::Raw),
            SignalingWireFormat::Protobuf(_) => Err(()),
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

    #[test]
    fn reports_only_cbor_as_a_binary_format() {
        assert!(JaqNativeFormat::Cbor.is_binary());
        assert!(!JaqNativeFormat::Json.is_binary());
        assert!(!JaqNativeFormat::Raw.is_binary());
    }
}
