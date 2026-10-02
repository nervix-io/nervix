//! Schemas, wire schemas and user-defined functions: the typed field lists Models declare.

use std::collections::BTreeSet;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    AvroType, CreateAvroWireSchema, CreateJsonWireSchema, CreateSchema, CreateUdf, JsonType,
    SchemaField, UdfArgument, UdfLanguage, UdfReturn, WireSchemaField, WireSchemaStrictness,
};

use crate::{Arbitrary, text::GeneratedName};

/// The most fields a generated schema or argument list declares.
const FIELDS: usize = 5;

const JSON_TYPES: [JsonType; 19] = [
    JsonType::String,
    JsonType::Number,
    JsonType::Integer,
    JsonType::Object,
    JsonType::Array,
    JsonType::Boolean,
    JsonType::Null,
    JsonType::U8,
    JsonType::I8,
    JsonType::U16,
    JsonType::I16,
    JsonType::U32,
    JsonType::I32,
    JsonType::U64,
    JsonType::I64,
    JsonType::Datetime,
    JsonType::F32,
    JsonType::F64,
    JsonType::Bytes,
];

const AVRO_TYPES: [AvroType; 13] = [
    AvroType::Null,
    AvroType::Boolean,
    AvroType::Int,
    AvroType::Long,
    AvroType::Float,
    AvroType::Double,
    AvroType::Bytes,
    AvroType::String,
    AvroType::Record,
    AvroType::Enum,
    AvroType::Array,
    AvroType::Map,
    AvroType::Fixed,
];

impl Arbitrary<'_> {
    /// Between `minimum` and `maximum` names, none repeated, in the order they were drawn.
    pub fn distinct_names<N: GeneratedName>(&mut self, minimum: usize, maximum: usize) -> Vec<N> {
        self.distinct_names_refusing(minimum, maximum, &[])
    }

    /// Between `minimum` and `maximum` names, none repeated, that in the NSPL domain spell none of
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

    /// A JSON wire schema, which is also the shape of a CBOR wire schema.
    pub fn json_wire_schema(&mut self) -> CreateJsonWireSchema {
        let name = self.name();
        let strictness = self.wire_strictness();
        let names = self.distinct_names(1, FIELDS);
        let mut fields = Vec::with_capacity(names.len());
        for field in names {
            fields.push(WireSchemaField {
                name: field,
                ty: self.entropy.pick(JSON_TYPES),
                optional: self.entropy.flag(),
            });
        }
        CreateJsonWireSchema {
            name,
            strictness,
            fields,
        }
    }

    /// An Avro wire schema.
    pub fn avro_wire_schema(&mut self) -> CreateAvroWireSchema {
        let name = self.name();
        let strictness = self.wire_strictness();
        let names = self.distinct_names(1, FIELDS);
        let mut fields = Vec::with_capacity(names.len());
        for field in names {
            fields.push(WireSchemaField {
                name: field,
                ty: self.entropy.pick(AVRO_TYPES),
                optional: self.entropy.flag(),
            });
        }
        CreateAvroWireSchema {
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
