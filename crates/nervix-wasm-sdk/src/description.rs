use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{Field, Schema};
use error_stack::{Report, Result};
use nervix_wasm_protocol::{ProcessorField, ProcessorSchema, StateSection};

use crate::{envelope::ProcessorTypeArrow as _, error::GuestError};

/// What a processor describes of its state for an operator's state inspection: named sections,
/// each one table of typed rows.
///
/// Nervix treats a guest's state as sensitive, always, because only the guest knows what it holds.
/// An inspection reports each section's name, its fields and their types, and its row count, and
/// never a value. Describe what [`crate::Processor::save_state`] carries in a form an operator can
/// recognize: counters, open windows, cached entries.
#[derive(Debug, Default)]
pub struct StateDescription {
    sections: Vec<StateSection>,
}

impl StateDescription {
    /// A description of no state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one section named `name` whose rows are the positional `columns` of `fields`.
    ///
    /// Every column must have the exact Arrow type of its field's [`crate::ProcessorType`], as
    /// [`crate::ProcessorTypeArrow::arrow_data_type`] states it, every column must hold the same
    /// number of rows, and a field that is not optional must hold no null. Sections are reported in
    /// the order they are added.
    pub fn add_section(
        &mut self,
        name: impl Into<String>,
        fields: Vec<ProcessorField>,
        columns: Vec<ArrayRef>,
    ) -> Result<(), GuestError> {
        let name = name.into();
        if fields.len() != columns.len() {
            return Err(Report::new(GuestError::StateSectionShape {
                section: name,
                fields: fields.len(),
                columns: columns.len(),
            }));
        }
        let mut arrow_fields = Vec::with_capacity(fields.len());
        for (field, column) in fields.iter().zip(&columns) {
            let data_type = field.ty.arrow_data_type()?;
            if column.data_type() != &data_type {
                return Err(Report::new(GuestError::StateSectionColumnType {
                    section: name,
                    field: field.name.clone(),
                }));
            }
            if !field.optional && column.null_count() > 0 {
                return Err(Report::new(GuestError::StateSectionNulls {
                    section: name,
                    field: field.name.clone(),
                }));
            }
            arrow_fields.push(Field::new(field.name.as_str(), data_type, field.optional));
        }
        let schema = Arc::new(Schema::new(arrow_fields));
        let batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|source| Report::new(GuestError::ArrowIpc(source)))?;
        let mut arrow_ipc_batch = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut arrow_ipc_batch, &schema)
                .map_err(|source| Report::new(GuestError::ArrowIpc(source)))?;
            writer
                .write(&batch)
                .map_err(|source| Report::new(GuestError::ArrowIpc(source)))?;
            writer
                .finish()
                .map_err(|source| Report::new(GuestError::ArrowIpc(source)))?;
        }
        self.sections.push(StateSection {
            schema: ProcessorSchema { name, fields },
            arrow_ipc_batch,
        });
        Ok(())
    }

    /// The `StateDescription` message this description is written as, or `None` when it describes
    /// no state, which the ABI reports by writing nothing.
    pub(crate) fn encode(self) -> Option<Vec<u8>> {
        if self.sections.is_empty() {
            return None;
        }
        Some(
            nervix_wasm_protocol::StateDescription {
                sections: self.sections,
            }
            .encode(),
        )
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, StringArray, UInt64Array};
    use arrow_ipc::reader::StreamReader;
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_wasm_protocol::ProcessorType;

    use super::*;

    fn field(name: &str, ty: ProcessorType, optional: bool) -> ProcessorField {
        ProcessorField {
            name: name.to_string(),
            ty,
            optional,
        }
    }

    #[test]
    fn a_description_of_no_state_writes_nothing() {
        assert!(StateDescription::new().encode().is_none());
    }

    #[test]
    fn sections_are_written_in_order_with_their_fields_and_one_batch_of_rows() {
        let mut description = StateDescription::new();
        description
            .add_section(
                "progress",
                vec![field("processed_rows", ProcessorType::U64, false)],
                vec![Arc::new(UInt64Array::from(vec![7]))],
            )
            .assured("a matching column describes the section");
        description
            .add_section(
                "tenants",
                vec![
                    field("tenant", ProcessorType::String, false),
                    field("note", ProcessorType::String, true),
                ],
                vec![
                    Arc::new(StringArray::from(vec!["alpha", "beta"])),
                    Arc::new(StringArray::from(vec![Some("first"), None])),
                ],
            )
            .assured("matching columns describe the section");

        let encoded = description
            .encode()
            .assured("a described state writes a message");
        let decoded = nervix_wasm_protocol::StateDescription::decode(&encoded)
            .assured("the SDK writes a valid state description");

        let names = decoded
            .sections
            .iter()
            .map(|section| section.schema.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["progress", "tenants"]);
        assert_eq!(
            decoded.sections[1].schema.fields,
            vec![
                field("tenant", ProcessorType::String, false),
                field("note", ProcessorType::String, true),
            ]
        );
        let reader = StreamReader::try_new(decoded.sections[1].arrow_ipc_batch.as_slice(), None)
            .assured("the section's rows are one Arrow IPC stream");
        let batches = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .assured("the section's batch decodes");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn a_section_refuses_columns_that_do_not_match_its_fields() {
        let mut description = StateDescription::new();
        let shape = description
            .add_section(
                "progress",
                vec![field("processed_rows", ProcessorType::U64, false)],
                Vec::new(),
            )
            .expect_err("a field without a column is refused");
        assert!(matches!(
            shape.current_context(),
            GuestError::StateSectionShape {
                fields: 1,
                columns: 0,
                ..
            }
        ));

        let typed = description
            .add_section(
                "progress",
                vec![field("processed_rows", ProcessorType::U64, false)],
                vec![Arc::new(Int32Array::from(vec![7]))],
            )
            .expect_err("a column of another type is refused");
        assert!(matches!(
            typed.current_context(),
            GuestError::StateSectionColumnType { .. }
        ));

        let nulls = description
            .add_section(
                "tenants",
                vec![field("tenant", ProcessorType::String, false)],
                vec![Arc::new(StringArray::from(vec![Some("alpha"), None]))],
            )
            .expect_err("a null in a required field is refused");
        assert!(matches!(
            nulls.current_context(),
            GuestError::StateSectionNulls { .. }
        ));

        assert!(
            description.encode().is_none(),
            "refused sections add nothing to the description"
        );
    }
}
