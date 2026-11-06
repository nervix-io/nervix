//! Schemas, wire schemas and user-defined functions: the typed field lists Models declare.

use std::{collections::BTreeSet, num::NonZeroU32, str::FromStr as _};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    AvroType, CborType, CreateAvroWireSchema, CreateCborWireSchema, CreateJsonWireSchema,
    CreateSchema, CreateUdf, CreateWireSchema, FieldName, JsonType, ParseAsType, SchemaField,
    UdfArgument, UdfLanguage, UdfReturn, WireSchemaField, WireSchemaStrictness,
};

use crate::{Arbitrary, text::GeneratedName};

/// The most fields a generated schema or argument list declares.
const FIELDS: usize = 5;

/// One field of every declared type: each scalar, a list of it, and a fixed-length array of such
/// lists at the shortest and the longest length, with the optional and sensitive flags cycling
/// through their four combinations. A property that must carry every type on every run sends this
/// list, so that no type is left to its random cases.
pub fn fields_of_every_type() -> Vec<SchemaField> {
    let mut fields = Vec::new();
    for scalar in ParseAsType::scalar_variants() {
        let list = ParseAsType::Vec {
            element: Box::new(scalar.clone()),
        };
        let length = if fields.len().is_multiple_of(2) {
            NonZeroU32::MIN
        } else {
            NonZeroU32::MAX
        };
        let array = ParseAsType::Array {
            element: Box::new(list.clone()),
            len: length,
        };
        for ty in [scalar.clone(), list, array] {
            let position = fields.len();
            let name = FieldName::from_str(&format!("field_{position}"))
                .assured("a name of letters, an underscore and digits is a field name");
            fields.push(SchemaField {
                name,
                ty,
                optional: position % 2 == 1,
                sensitive: position % 4 >= 2,
            });
        }
    }
    fields
}

const JSON_TYPES: [JsonType; 5] = [
    JsonType::String,
    JsonType::Number,
    JsonType::Integer,
    JsonType::Array,
    JsonType::Boolean,
];

const CBOR_TYPES: [CborType; 6] = [
    CborType::Integer,
    CborType::Float,
    CborType::Boolean,
    CborType::Text,
    CborType::Bytes,
    CborType::Array,
];

const AVRO_PRIMITIVE_TYPES: [AvroType; 7] = [
    AvroType::Boolean,
    AvroType::Int,
    AvroType::Long,
    AvroType::Float,
    AvroType::Double,
    AvroType::Bytes,
    AvroType::String,
];

/// The most arrays a generated AVRO wire type nests around its primitive item type.
const AVRO_ARRAY_DEPTH: u8 = 3;

impl Arbitrary<'_> {
    /// Between `minimum` and `maximum` names, none repeated, in the order they were drawn.
    pub fn distinct_names<N: GeneratedName>(&mut self, minimum: usize, maximum: usize) -> Vec<N> {
        self.distinct_names_refusing(minimum, maximum, &[])
    }

    /// Between `minimum` and `maximum` names, none repeated, that in the Plexus domain spell none of
    /// `refused`, for a list whose position refuses more words than its kind of name does.
    pub fn distinct_names_refusing<N: GeneratedName>(
        &mut self,
        minimum: usize,
        maximum: usize,
        refused: &[&str],
    ) -> Vec<N> {
        self.distinct_drawn(minimum, maximum, |arbitrary| {
            arbitrary.name_text_refusing::<N>(refused)
        })
    }

    /// Between `minimum` and `maximum` names, none repeated, as an expression or a route
    /// construction writes them.
    pub fn distinct_expression_names<N: GeneratedName>(
        &mut self,
        minimum: usize,
        maximum: usize,
    ) -> Vec<N> {
        self.distinct_drawn(minimum, maximum, Self::expression_name_text::<N>)
    }

    /// Between `minimum` and `maximum` names whose text `draw` builds, none repeated, in the order
    /// they were drawn.
    fn distinct_drawn<N: GeneratedName>(
        &mut self,
        minimum: usize,
        maximum: usize,
        mut draw: impl FnMut(&mut Self) -> String,
    ) -> Vec<N> {
        let extra = maximum
            .checked_sub(minimum)
            .assured("a list's minimum never exceeds its maximum");
        let wanted = minimum
            .checked_add(self.entropy.count(extra))
            .verified("the extra count is at most the maximum minus the minimum");
        let mut seen = BTreeSet::new();
        let mut names = Vec::with_capacity(wanted);
        for _ in 0..wanted {
            let drawn = draw(self);
            let text = if seen.contains(&drawn) {
                Self::unused_name(&seen)
            } else {
                drawn
            };
            names.push(N::from_str(&text).assured("a generated name is a valid identifier"));
            seen.insert(text);
        }
        names
    }

    /// A name no earlier draw took. A repeat is rare, so a short underscore name is enough: it
    /// grows until it is unused, which a finite list reaches within as many steps as it holds.
    fn unused_name(seen: &BTreeSet<String>) -> String {
        let mut text = format!("_{}", seen.len());
        while seen.contains(&text) {
            text.push('_');
        }
        text
    }

    /// A schema of one or more distinctly named fields of any declared type.
    pub fn create_schema(&mut self) -> CreateSchema {
        let name = self.name();
        let fields = self.schema_fields(1);
        CreateSchema { name, fields }
    }

    /// At least `minimum` schema fields with distinct names.
    pub fn schema_fields(&mut self, minimum: usize) -> Vec<SchemaField> {
        let names = self.distinct_names(minimum, FIELDS);
        let mut fields = Vec::with_capacity(names.len());
        for name in names {
            fields.push(SchemaField {
                name,
                ty: self.declared_type(),
                optional: self.entropy.flag(),
                sensitive: self.entropy.flag(),
            });
        }
        fields
    }

    fn wire_strictness(&mut self) -> WireSchemaStrictness {
        self.entropy
            .pick([WireSchemaStrictness::Strict, WireSchemaStrictness::Loose])
    }

    /// A JSON wire schema in its declared vocabulary.
    pub fn json_wire_schema(&mut self) -> CreateJsonWireSchema {
        self.wire_schema(|arbitrary| arbitrary.entropy.pick(JSON_TYPES))
    }

    /// A CBOR wire schema in its declared vocabulary.
    pub fn cbor_wire_schema(&mut self) -> CreateCborWireSchema {
        self.wire_schema(|arbitrary| arbitrary.entropy.pick(CBOR_TYPES))
    }

    /// An Avro wire schema, its arrays nested at most [`AVRO_ARRAY_DEPTH`] deep.
    pub fn avro_wire_schema(&mut self) -> CreateAvroWireSchema {
        self.wire_schema(|arbitrary| arbitrary.avro_type_within(AVRO_ARRAY_DEPTH))
    }

    /// An AVRO wire type: a primitive type, or an array of an AVRO type nested at most `depth`
    /// arrays deep.
    fn avro_type_within(&mut self, depth: u8) -> AvroType {
        let Some(below) = depth.checked_sub(1) else {
            return self.entropy.pick(AVRO_PRIMITIVE_TYPES);
        };
        match self.entropy.byte() % 3 {
            0 => AvroType::array_of(self.avro_type_within(below)),
            _ => self.entropy.pick(AVRO_PRIMITIVE_TYPES),
        }
    }

    fn wire_schema<T>(&mut self, mut draw: impl FnMut(&mut Self) -> T) -> CreateWireSchema<T> {
        let name = self.name();
        let strictness = self.wire_strictness();
        let names = self.distinct_names(1, FIELDS);
        let mut fields = Vec::with_capacity(names.len());
        for field in names {
            let ty = draw(self);
            fields.push(WireSchemaField {
                name: field,
                ty,
                optional: self.entropy.flag(),
            });
        }
        CreateWireSchema {
            name,
            strictness,
            fields,
        }
    }

    /// A user-defined function with distinctly named arguments and any source code.
    pub fn create_udf(&mut self) -> CreateUdf {
        let name = self.name();
        let names = self.distinct_names(1, FIELDS);
        let mut arguments = Vec::with_capacity(names.len());
        for argument in names {
            arguments.push(UdfArgument {
                name: argument,
                ty: self.declared_type(),
                optional: self.entropy.flag(),
            });
        }
        let returns = UdfReturn {
            ty: self.declared_type(),
            optional: self.entropy.flag(),
        };
        let volatile = self.entropy.flag();
        let code = self.string();
        CreateUdf::new(
            name,
            UdfLanguage::Roto0_13,
            arguments,
            returns,
            volatile,
            code,
        )
    }
}
