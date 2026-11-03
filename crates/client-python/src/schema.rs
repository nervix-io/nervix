//! The schema a subscription's rows, or an endpoint's batches, follow, and the fields a host
//! expects an endpoint to have.
//!
//! - **Owns.** Reading a schema's fields, the levels of each field's type and its branch, and
//!   describing the fields a host expects one type level at a time.
//! - **Depends on.** The host session runtime's schema, field types and field descriptions.
//! - **Must not know.** Where a schema came from or what the server compares fields with.

use nervix_client_host as host;
use nervix_models::{ParseAsType, SchemaField};
use pyo3::prelude::*;

use crate::{
    enums::{FieldType, Part},
    errors,
};

/// One level of a field's type: its type, and the element count of a fixed-size list, which is
/// zero for every other level.
#[pyclass(frozen, eq, hash, module = "nervix_client")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Level {
    field_type: FieldType,
    length: u32,
}

#[pymethods]
impl Level {
    #[new]
    #[pyo3(signature = (field_type, length = 0))]
    fn new(field_type: FieldType, length: u32) -> Self {
        Self { field_type, length }
    }

    #[getter(r#type)]
    fn field_type(&self) -> FieldType {
        self.field_type
    }

    #[getter]
    fn length(&self) -> u32 {
        self.length
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let field_type = Bound::new(py, self.field_type)?;
        let field_type = field_type.as_any().repr()?;
        Ok(format!("Level({field_type}, {})", self.length))
    }
}

impl Level {
    /// The level `ty` names.
    fn of(ty: &ParseAsType) -> Self {
        let length = match ty {
            ParseAsType::Array { len, .. } => len.get(),
            _ => 0,
        };
        Self {
            field_type: FieldType::from(host::FieldType::from(ty)),
            length,
        }
    }
}

/// One field of a schema.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct Field {
    field: SchemaField,
}

#[pymethods]
impl Field {
    #[getter]
    fn name(&self) -> &str {
        self.field.name.as_str()
    }

    /// The type of the field's own level: its scalar, or the kind of list it is.
    #[getter(r#type)]
    fn field_type(&self) -> FieldType {
        FieldType::from(host::FieldType::from(&self.field.ty))
    }

    /// The levels of the field's type, from the field's own type down to its innermost scalar.
    #[getter]
    fn levels(&self) -> Vec<Level> {
        let mut levels = Vec::new();
        let mut ty = &self.field.ty;
        loop {
            levels.push(Level::of(ty));
            match ty {
                ParseAsType::Array { element, .. } | ParseAsType::Vec { element } => ty = element,
                _ => return levels,
            }
        }
    }

    #[getter]
    fn nullable(&self) -> bool {
        self.field.optional
    }

    #[getter]
    fn sensitive(&self) -> bool {
        self.field.sensitive
    }

    fn __repr__(&self) -> String {
        format!("Field({:?})", self.field.name.as_str())
    }
}

/// The schema a subscription's rows, or an endpoint's batches, follow.
#[pyclass(frozen, module = "nervix_client")]
pub(crate) struct Schema {
    schema: host::Schema,
}

impl Schema {
    pub(crate) fn new(schema: host::Schema) -> Self {
        Self { schema }
    }

    pub(crate) fn host(&self) -> &host::Schema {
        &self.schema
    }

    fn part(&self, part: Part) -> Vec<Field> {
        self.schema
            .fields(host::Part::from(part))
            .iter()
            .map(|field| Field {
                field: field.clone(),
            })
            .collect()
    }
}

#[pymethods]
impl Schema {
    /// The fields of every row.
    #[getter]
    fn fields(&self) -> Vec<Field> {
        self.part(Part::Rows)
    }

    /// The fields of the branch key, which an unbranched relay's rows do not have.
    #[getter]
    fn key_fields(&self) -> Vec<Field> {
        self.part(Part::BranchKey)
    }

    /// The name of the branch the rows belong to, or `None` for an unbranched relay.
    #[getter]
    fn branch(&self) -> Option<&str> {
        self.schema.branch()
    }
}

/// The fields a host expects an endpoint to have, described one type level at a time.
///
/// A field starts with its outermost type, and each list around an element is followed by that
/// element, until a scalar ends the field.
#[pyclass(module = "nervix_client")]
pub(crate) struct Fields {
    fields: host::Fields,
}

impl Fields {
    pub(crate) fn host(&self) -> &host::Fields {
        &self.fields
    }
}

#[pymethods]
impl Fields {
    #[new]
    fn new() -> Self {
        Self {
            fields: host::Fields::default(),
        }
    }

    /// Starts a field whose type begins with `field_type`. Only a fixed-size list has a length.
    #[pyo3(signature = (name, field_type, length = 0, *, nullable = false, sensitive = false))]
    fn add(
        &mut self,
        py: Python<'_>,
        name: &str,
        field_type: FieldType,
        length: u32,
        nullable: bool,
        sensitive: bool,
    ) -> PyResult<()> {
        let added = self.fields.add(
            name,
            host::FieldType::from(field_type),
            length,
            nullable,
            sensitive,
        );
        match added {
            Ok(()) => Ok(()),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }

    /// Adds the element of the innermost list of the last field.
    #[pyo3(signature = (field_type, length = 0))]
    fn element(&mut self, py: Python<'_>, field_type: FieldType, length: u32) -> PyResult<()> {
        let added = self
            .fields
            .element(host::FieldType::from(field_type), length);
        match added {
            Ok(()) => Ok(()),
            Err(failure) => Err(errors::raise(py, failure)),
        }
    }
}

/// Adds the schema and field description classes to the module.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Schema>()?;
    module.add_class::<Field>()?;
    module.add_class::<Level>()?;
    module.add_class::<Fields>()?;
    Ok(())
}
