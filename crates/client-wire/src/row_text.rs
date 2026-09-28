//! The text a client displays for a row.
//!
//! A row renders as one compact JSON object holding its present fields in field-name order. A null
//! field is left out, and a sensitive field renders as the string `"<masked>"`, because its value
//! never leaves the server. The row of a branched relay is prefixed with the concrete branch key,
//! as `key=<object> payload=<object>`, the key object holding its fields in key order. Datetimes
//! render in RFC 3339, bytes as padded standard base64, and floats as the shortest JSON number
//! that reads back as the same value, a single-precision float widened to double precision first.
//!
//! Every client that shows rows as text renders them here, so one batch reads the same in the CLI,
//! the console and any other client built on this crate. Rendering writes JSON; it never parses
//! any.

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::SchemaField;

use crate::row::{CellView, CellsView, RowBatchView, RowConformanceError, RowSchema};

/// What a sensitive field renders as.
const MASKED: &str = "<masked>";

impl RowBatchView<'_> {
    /// The display text of every row of the batch, in batch order.
    ///
    /// The batch is first held to `schema`, the one its subscription announced, so every cell is
    /// rendered under the field it belongs to.
    pub fn display_lines(
        &self,
        schema: &RowSchema,
    ) -> Result<Vec<String>, Report<RowConformanceError>> {
        self.conform(schema)?;
        let key = match (self.branch_key(), &schema.branch) {
            (Some(key), Some(branch)) => {
                let names = branch
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(index, field)| (index, json_name(field)))
                    .collect::<Vec<_>>();
                Some(render_object(key, &names))
            }
            _ => None,
        };
        let mut names = schema
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| (index, json_name(field)))
            .collect::<Vec<_>>();
        names.sort_by(|(left, _), (right, _)| {
            schema.fields[*left]
                .name
                .as_str()
                .cmp(schema.fields[*right].name.as_str())
        });
        let mut lines = Vec::with_capacity(self.len());
        for row in self.rows() {
            let payload = render_object(row, &names);
            let line = match &key {
                Some(key) => format!("key={key} payload={payload}"),
                None => payload,
            };
            lines.push(line);
        }
        Ok(lines)
    }
}

/// Renders a row or branch key using field names escaped once for the whole batch. The cells were
/// already held to the schema, so there is one per name.
fn render_object(cells: CellsView<'_>, names: &[(usize, String)]) -> String {
    let mut object = String::from("{");
    let mut first = true;
    for (index, name) in names {
        let Some(cell) = cells.get(*index) else {
            continue;
        };
        if let CellView::Null = cell {
            continue;
        }
        if !first {
            object.push(',');
        }
        first = false;
        object.push_str(name);
        object.push(':');
        push_cell(&mut object, cell);
    }
    object.push('}');
    object
}

fn json_name(field: &SchemaField) -> String {
    serde_json::to_string(field.name.as_str())
        .assured("serializing a field name to JSON has no failing case")
}

/// Appends one cell as a JSON value.
fn push_cell(output: &mut String, cell: CellView<'_>) {
    match cell {
        CellView::Null => output.push_str("null"),
        CellView::Redacted => push_json_string(output, MASKED),
        CellView::U8(value) => output.push_str(&value.to_string()),
        CellView::I8(value) => output.push_str(&value.to_string()),
        CellView::U16(value) => output.push_str(&value.to_string()),
        CellView::I16(value) => output.push_str(&value.to_string()),
        CellView::U32(value) => output.push_str(&value.to_string()),
        CellView::I32(value) => output.push_str(&value.to_string()),
        CellView::U64(value) => output.push_str(&value.to_string()),
        CellView::I64(value) => output.push_str(&value.to_string()),
        CellView::F32(value) => push_float(output, f64::from(value)),
        CellView::F64(value) => push_float(output, value),
        CellView::Bool(value) => output.push_str(if value { "true" } else { "false" }),
        CellView::String(value) => push_json_string(output, value),
        CellView::Bytes(value) => {
            let encoded = base64_simd::STANDARD.encode_to_string(value);
            push_json_string(output, &encoded);
        }
        CellView::Datetime(value) => push_json_string(output, &value.as_datetime().to_rfc3339()),
        CellView::List(elements) => {
            output.push('[');
            for (index, element) in elements.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                push_cell(output, element);
            }
            output.push(']');
        }
    }
}

/// Appends a float as the shortest JSON number that reads back as the same value. JSON has no
/// number for a NaN or an infinity, so such a value renders as the string Rust prints for it.
fn push_float(output: &mut String, value: f64) {
    match serde_json::Number::from_f64(value) {
        Some(number) => output.push_str(&number.to_string()),
        None => push_json_string(output, &value.to_string()),
    }
}

/// Appends a string as a JSON string, escaped exactly as `serde_json` escapes it.
fn push_json_string(output: &mut String, value: &str) {
    let escaped = serde_json::to_string(value)
        .assured("serializing a string slice to JSON has no failing case");
    output.push_str(&escaped);
}
