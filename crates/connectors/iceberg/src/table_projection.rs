//! The check an Iceberg sink makes, when it opens, that its table can take the mapped columns.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Matching the mapped columns to the columns of the table's current schema by name
//!   and Iceberg type, and refusing a table that cannot take them.
//! - **Depends on.** Arrow schemas, the `iceberg` crate's schema and its conversion from Arrow, the
//!   sink's start errors, and `error-stack`.
//! - **Must not know.** Staging, catalogs, commits, acknowledgements, or the host.

#![cfg_attr(
    nervix_lint,
    nervix::input(
        plans,
        reason = "the sink checks the mapped columns its validated plan names"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use ::iceberg::spec::Type;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{SinkStartError, SinkStartResult};

use super::{ICEBERG, IcebergSink};

impl IcebergSink {
    /// Match the staged columns to the table by name. The generated field IDs describe the
    /// staged file, not the table, so nested types are compared without those IDs.
    pub(super) fn validate_table_schema(
        staged: &arrow_schema::Schema,
        table: &::iceberg::spec::Schema,
    ) -> SinkStartResult<()> {
        let mapped = ::iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(staged)
            .change_context(SinkStartError::InvalidConfiguration { sink: ICEBERG })
            .attach_printable("mapped Iceberg schema cannot be written")?;
        for column in mapped.as_struct().fields() {
            let Some(target) = table.as_struct().field_by_name(&column.name) else {
                return Err(
                    Report::new(SinkStartError::InvalidConfiguration { sink: ICEBERG })
                        .attach_printable(format!(
                            "mapped column '{}' does not exist in the Iceberg table",
                            column.name
                        )),
                );
            };
            if !Self::iceberg_types_match(&column.field_type, &target.field_type) {
                return Err(
                    Report::new(SinkStartError::InvalidConfiguration { sink: ICEBERG })
                        .attach_printable(format!(
                            "mapped column '{}' has Iceberg type {}, but the table requires {}",
                            column.name, column.field_type, target.field_type
                        )),
                );
            }
        }
        Ok(())
    }

    fn iceberg_types_match(mapped: &Type, table: &Type) -> bool {
        match (mapped, table) {
            (Type::Primitive(mapped), Type::Primitive(table)) => mapped == table,
            (Type::List(mapped), Type::List(table)) => Self::iceberg_types_match(
                &mapped.element_field.field_type,
                &table.element_field.field_type,
            ),
            (Type::Struct(mapped), Type::Struct(table)) => {
                if mapped.fields().len() != table.fields().len() {
                    return false;
                }
                for field in mapped.fields() {
                    let Some(target) = table.field_by_name(&field.name) else {
                        return false;
                    };
                    if !Self::iceberg_types_match(&field.field_type, &target.field_type) {
                        return false;
                    }
                }
                true
            }
            (Type::Map(mapped), Type::Map(table)) => {
                Self::iceberg_types_match(&mapped.key_field.field_type, &table.key_field.field_type)
                    && Self::iceberg_types_match(
                        &mapped.value_field.field_type,
                        &table.value_field.field_type,
                    )
            }
            _ => false,
        }
    }
}
