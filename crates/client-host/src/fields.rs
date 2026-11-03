//! The fields a host expects an endpoint to have.
//!
//! - **Owns.** Building a field list one type level at a time from the field types a host names,
//!   and refusing a field whose type is not complete or cannot be a Nervix type.
//! - **Depends on.** The vocabulary's schema fields, types and names, and the field types a host
//!   names.
//! - **Must not know.** Which endpoint the fields are for, or what the server compares them with.
//!
//! A host cannot pass a nested type as one value, so a field starts with its outermost type and
//! each list around an element is followed by that element, until a scalar ends the field.

use std::num::NonZeroU32;

use nervix_models::{FieldName, ParseAsType, SchemaField};

use crate::{
    failure::Failure,
    schema::{FieldType, TypeKind},
};

/// The most list levels one field may nest before its scalar: far more than any schema declares,
/// and few enough that every recursion over the type stays shallow.
const MAX_LIST_LEVELS: usize = 32;

/// The largest element count of a fixed-size list: an Arrow fixed-size list counts its elements
/// in an `i32`.
const MAX_FIXED_LIST_LENGTH: u32 = i32::MAX.cast_unsigned();

/// A field list a host is building.
#[derive(Debug, Default)]
pub struct Fields {
    fields: Vec<FieldDraft>,
}

/// One field as far as the host has described it.
#[derive(Debug)]
struct FieldDraft {
    name: FieldName,
    nullable: bool,
    sensitive: bool,
    /// The lists around the field's element, from the outside in.
    lists: Vec<ListLevel>,
    /// The scalar the innermost list holds, or the field's own type when it is no list. The field
    /// is complete once it has one.
    element: Option<ParseAsType>,
}

/// One list level of a field's type.
#[derive(Debug, Clone, Copy)]
enum ListLevel {
    List,
    FixedList(NonZeroU32),
}

/// One type level a host named, before it is placed in a field.
enum Level {
    List(ListLevel),
    Scalar(ParseAsType),
}

impl Level {
    /// The level a host's type and length name. Only a fixed-size list has a length.
    fn from_host(field_type: i32, length: u32) -> Result<Self, Failure> {
        match FieldType::from_host(field_type)?.kind() {
            TypeKind::List => {
                if length != 0 {
                    return Err(Failure::invalid_argument(
                        "length",
                        "must be zero for a LIST level",
                    ));
                }
                Ok(Self::List(ListLevel::List))
            }
            TypeKind::FixedList => {
                let Some(length) = NonZeroU32::new(length) else {
                    return Err(Failure::invalid_argument(
                        "length",
                        "must be positive for a FIXED_LIST level",
                    ));
                };
                if length.get() > MAX_FIXED_LIST_LENGTH {
                    return Err(Failure::invalid_argument(
                        "length",
                        "exceeds the element count an Arrow fixed-size list holds",
                    ));
                }
                Ok(Self::List(ListLevel::FixedList(length)))
            }
            TypeKind::Scalar(scalar) => {
                if length != 0 {
                    return Err(Failure::invalid_argument(
                        "length",
                        "must be zero for a scalar level",
                    ));
                }
                Ok(Self::Scalar(scalar))
            }
        }
    }
}

impl FieldDraft {
    fn place(&mut self, level: Level) -> Result<(), Failure> {
        match level {
            Level::Scalar(scalar) => {
                self.element = Some(scalar);
                Ok(())
            }
            Level::List(list) => {
                if self.lists.len() >= MAX_LIST_LEVELS {
                    return Err(Failure::invalid_argument(
                        "type",
                        &format!(
                            "nests field '{}' in more than {MAX_LIST_LEVELS} lists",
                            self.name.as_str()
                        ),
                    ));
                }
                self.lists.push(list);
                Ok(())
            }
        }
    }

    fn schema_field(&self) -> Result<SchemaField, Failure> {
        let Some(element) = &self.element else {
            return Err(Failure::invalid_argument(
                "fields",
                &format!(
                    "leaves the type of field '{}' without its innermost element",
                    self.name.as_str()
                ),
            ));
        };
        let mut ty = element.clone();
        for list in self.lists.iter().rev() {
            ty = match list {
                ListLevel::List => ParseAsType::Vec {
                    element: Box::new(ty),
                },
                ListLevel::FixedList(len) => ParseAsType::Array {
                    element: Box::new(ty),
                    len: *len,
                },
            };
        }
        Ok(SchemaField {
            name: self.name.clone(),
            ty,
            optional: self.nullable,
            sensitive: self.sensitive,
        })
    }
}

impl Fields {
    /// Starts a field whose type begins with `field_type`.
    pub fn add(
        &mut self,
        name: &str,
        field_type: i32,
        length: u32,
        nullable: bool,
        sensitive: bool,
    ) -> Result<(), Failure> {
        if let Some(last) = self.fields.last()
            && last.element.is_none()
        {
            return Err(Failure::invalid_argument(
                "fields",
                &format!(
                    "still needs the innermost element of field '{}'",
                    last.name.as_str()
                ),
            ));
        }
        let name = match FieldName::try_from(name) {
            Ok(name) => name,
            Err(error) => return Err(Failure::invalid_argument("name", &error.to_string())),
        };
        let mut field = FieldDraft {
            name,
            nullable,
            sensitive,
            lists: Vec::new(),
            element: None,
        };
        field.place(Level::from_host(field_type, length)?)?;
        self.fields.push(field);
        Ok(())
    }

    /// Adds the element of the innermost list of the last field.
    pub fn element(&mut self, field_type: i32, length: u32) -> Result<(), Failure> {
        let level = Level::from_host(field_type, length)?;
        let Some(last) = self.fields.last_mut() else {
            return Err(Failure::invalid_argument(
                "fields",
                "has no field whose element this could be",
            ));
        };
        if last.element.is_some() {
            return Err(Failure::invalid_argument(
                "fields",
                &format!(
                    "already ends field '{}' with its scalar",
                    last.name.as_str()
                ),
            ));
        }
        last.place(level)
    }

    /// The fields in the order they were added, each with its complete type.
    pub fn schema_fields(&self) -> Result<Vec<SchemaField>, Failure> {
        let mut fields = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            fields.push(field.schema_field()?);
        }
        Ok(fields)
    }
}
