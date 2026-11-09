//! Text and JSON projections of the same verified inspection envelope.

use std::fmt::Write as _;

use error_stack::Report;
use meticulous::ResultExt as _;
use serde::{
    Serialize, Serializer,
    ser::{SerializeMap as _, SerializeSeq as _},
};

use super::*;
use crate::{CellView, CellsView, RowBatchView, WireDecodeError};

impl StateInspection {
    pub fn display_text(&self) -> Result<String, Report<WireDecodeError>> {
        let mut output = format!(
            "kind: {}\nentity: {}\ndomain: {}\n",
            self.entity.node.kind.keyword_phrase(),
            self.entity.node.identifier,
            self.entity.domain
        );
        let page = match &self.result {
            StateInspectionResult::Page(page) => page,
            StateInspectionResult::Rejected(reason) => {
                writeln!(output, "inspection refused: {}", reason.as_ref())
                    .assured("writing text to a String cannot fail");
                return Ok(output);
            }
        };
        writeln!(
            output,
            "branches: {}\nrows: {}\nbytes: {}\nunits: {}",
            page.counts.branches, page.counts.rows, page.counts.bytes, page.unit_count
        )
        .assured("writing text to a String cannot fail");
        if let Some(binding) = &page.wasm_binding {
            writeln!(output, "{}", super::wasm::binding_line(binding))
                .assured("writing text to a String cannot fail");
        }
        for unit in &page.units {
            writeln!(
                output,
                "unit: {}\nowner: {}\nreplicas: {}\norigin: {} generation={} revision={} stage={}",
                match unit.unit.kind() {
                    nervix_models::UnitKind::Single => "single".to_string(),
                    _ => unit.unit.label_suffix().trim_start().to_string(),
                },
                unit.owner,
                unit.replicas
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
                unit.origin.node,
                unit.origin.generation,
                unit.origin.revision,
                unit.origin.stage.as_ref()
            )
            .assured("writing text to a String cannot fail");
            if let Some(state) = &unit.wasm {
                let mut lines = super::wasm::wasm_unit_summary_lines(state);
                lines.extend(super::wasm::entry_lines(state));
                for line in lines {
                    writeln!(output, "{line}").assured("writing text to a String cannot fail");
                }
            }
            for section in &unit.sections {
                writeln!(
                    output,
                    "section: {} omitted_rows={}",
                    section.name, section.omitted_rows
                )
                .assured("writing text to a String cannot fail");
                for frame in &section.frames {
                    let lines = frame
                        .batch()
                        .display_lines(&section.schema)
                        .map_err(|error| {
                            error.change_context(WireDecodeError::InvalidValue {
                                field: "StateInspectionSection.frames",
                                kind: "rows conforming to their schema",
                            })
                        })?;
                    for line in lines {
                        writeln!(output, "{line}").assured("writing text to a String cannot fail");
                    }
                }
            }
        }
        if let Some(continuation) = &page.continuation {
            writeln!(output, "continuation: {continuation}")
                .assured("writing text to a String cannot fail");
        }
        Ok(output)
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self)
            .assured("the inspection serializer writes only valid JSON values")
    }
}

impl Serialize for StateInspection {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut output = serializer.serialize_map(None)?;
        #[derive(Serialize)]
        struct Entity<'a> {
            domain: &'a nervix_models::DomainName,
            kind: nervix_models::ModelKind,
            name: &'a nervix_models::ModelName,
        }
        output.serialize_entry(
            "entity",
            &Entity {
                domain: &self.entity.domain,
                kind: self.entity.node.kind,
                name: &self.entity.node.identifier,
            },
        )?;
        match &self.result {
            StateInspectionResult::Page(page) => {
                output.serialize_entry("outcome", "page")?;
                output.serialize_entry("counts", &page.counts)?;
                output.serialize_entry("unit_count", &page.unit_count)?;
                output.serialize_entry("units", &page.units)?;
                output.serialize_entry("continuation", &page.continuation)?;
                if let Some(binding) = &page.wasm_binding {
                    output.serialize_entry("wasm_binding", &super::wasm::BindingJson(binding))?;
                }
            }
            StateInspectionResult::Rejected(reason) => {
                output.serialize_entry("outcome", "rejected")?;
                output.serialize_entry("rejection", reason.as_ref())?;
            }
        }
        output.end()
    }
}

impl Serialize for StateInspectionSection {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut output = serializer.serialize_map(Some(5))?;
        output.serialize_entry("name", &self.name)?;
        output.serialize_entry("schema", &SchemaJson(&self.schema))?;
        output.serialize_entry("frames", &self.frames)?;
        output.serialize_entry("omitted_rows", &self.omitted_rows)?;
        output.serialize_entry("watermarks", &self.watermarks)?;
        output.end()
    }
}

struct SchemaJson<'a>(&'a RowSchema);
impl Serialize for SchemaJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut output = serializer.serialize_map(Some(2))?;
        output.serialize_entry("fields", &self.0.fields)?;
        let branch = self.0.branch.as_ref().map(|branch| BranchJson {
            name: branch.branch(),
            fields: branch.fields(),
        });
        output.serialize_entry("branch", &branch)?;
        output.end()
    }
}

#[derive(Serialize)]
struct BranchJson<'a> {
    name: &'a nervix_models::BranchName,
    fields: &'a [nervix_models::SchemaField],
}

impl Serialize for StateRowFrame {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.batch().serialize(serializer)
    }
}

impl Serialize for RowBatchView<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut output = serializer.serialize_map(Some(2))?;
        output.serialize_entry("branch_key", &self.branch_key())?;
        struct Rows<'a>(RowBatchView<'a>);
        impl Serialize for Rows<'_> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut rows = serializer.serialize_seq(Some(self.0.len()))?;
                for row in self.0.rows() {
                    rows.serialize_element(&row)?;
                }
                rows.end()
            }
        }
        output.serialize_entry("rows", &Rows(*self))?;
        output.end()
    }
}

impl Serialize for CellsView<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut values = serializer.serialize_seq(Some(self.len()))?;
        for value in self.iter() {
            values.serialize_element(&value)?;
        }
        values.end()
    }
}

impl Serialize for CellView<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut value = serializer.serialize_map(None)?;
        macro_rules! scalar {
            ($kind:literal, $data:expr) => {{
                value.serialize_entry("type", $kind)?;
                value.serialize_entry("value", $data)?;
            }};
        }
        match self {
            CellView::Null => value.serialize_entry("type", "null")?,
            CellView::Redacted => value.serialize_entry("type", "redacted")?,
            CellView::U8(data) => scalar!("u8", data),
            CellView::I8(data) => scalar!("i8", data),
            CellView::U16(data) => scalar!("u16", data),
            CellView::I16(data) => scalar!("i16", data),
            CellView::U32(data) => scalar!("u32", data),
            CellView::I32(data) => scalar!("i32", data),
            CellView::U64(data) => scalar!("u64", data),
            CellView::I64(data) => scalar!("i64", data),
            CellView::F32(data) => {
                value.serialize_entry("type", "f32")?;
                value.serialize_entry("bits", &data.to_bits())?;
            }
            CellView::F64(data) => {
                value.serialize_entry("type", "f64")?;
                value.serialize_entry("bits", &data.to_bits())?;
            }
            CellView::Bool(data) => scalar!("bool", data),
            CellView::String(data) => scalar!("string", data),
            CellView::Bytes(data) => {
                scalar!("bytes", &base64_simd::STANDARD.encode_to_string(data))
            }
            CellView::Datetime(data) => scalar!("datetime", &data.unix_nanos()),
            CellView::List(data) => scalar!("list", data),
        }
        value.end()
    }
}
