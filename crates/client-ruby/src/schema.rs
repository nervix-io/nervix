//! Ruby schemas, exact field descriptions and mutable field drafts.
//!
//! Layer: edges.
//! - **Owns.** Ruby access to schema fields and type levels, and snapshots for endpoint opens.
//! - **Depends on.** The shared host schema/field owners and magnus typed data.
//! - **Must not know.** Endpoint admission, sessions or batch encoding.

use std::cell::RefCell;

use magnus::{
    DataTypeFunctions, Error, RArray, RModule, Ruby, TypedData, Value, function, method,
    prelude::*,
    scan_args::{get_kwargs, scan_args},
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_host as host;
use nervix_models::{ParseAsType, SchemaField};
use nervix_primitives::sync::Arc;

use crate::{enums, errors};

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Schema", free_immediately)]
pub(crate) struct Schema {
    pub(crate) schema: host::Schema,
}

impl Schema {
    pub(crate) fn new(schema: host::Schema) -> Self {
        Self { schema }
    }

    fn fields(&self) -> RArray {
        let ruby = Ruby::get().assured("a native Ruby method holds the GVL");
        self.part(&ruby, host::Part::Rows)
    }
    fn key_fields(&self) -> RArray {
        let ruby = Ruby::get().assured("a native Ruby method holds the GVL");
        self.part(&ruby, host::Part::BranchKey)
    }
    fn branch(&self) -> Option<&str> {
        self.schema.branch()
    }

    fn part(&self, ruby: &Ruby, part: host::Part) -> RArray {
        let mut fields = Vec::with_capacity(self.schema.fields(part).len());
        for index in 0..self.schema.fields(part).len() {
            fields.push(Field {
                schema: self.schema.clone(),
                part,
                index,
            });
        }
        ruby.ary_from_iter(fields)
    }
}

#[derive(Clone, DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Field", free_immediately)]
pub(crate) struct Field {
    schema: host::Schema,
    part: host::Part,
    index: usize,
}

impl Field {
    fn field(&self) -> &SchemaField {
        self.schema
            .field(self.part, self.index)
            .assured("only declared schema indices construct a Field")
    }
    fn name(&self) -> &str {
        self.field().name.as_str()
    }
    fn kind(&self) -> i32 {
        enums::FieldType::code(host::FieldType::from(&self.field().ty))
    }
    fn nullable(&self) -> bool {
        self.field().optional
    }
    fn sensitive(&self) -> bool {
        self.field().sensitive
    }
    fn levels(&self) -> RArray {
        let ruby = Ruby::get().assured("a native Ruby method holds the GVL");
        let count = host::TypeLevels::of(&self.field().ty).count();
        let mut levels = Vec::with_capacity(count);
        for depth in 0..count {
            levels.push(Level {
                field: self.clone(),
                depth,
            });
        }
        ruby.ary_from_iter(levels)
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Level", free_immediately)]
pub(crate) struct Level {
    field: Field,
    depth: usize,
}

impl Level {
    fn ty(&self) -> &ParseAsType {
        host::TypeLevels::of(&self.field.field().ty)
            .get(self.depth)
            .assured("only declared levels construct a Level")
    }
    fn kind(&self) -> i32 {
        enums::FieldType::code(host::FieldType::from(self.ty()))
    }
    fn length(&self) -> u32 {
        match self.ty() {
            ParseAsType::Array { len, .. } => len.get(),
            _ => 0,
        }
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Fields", free_immediately)]
pub(crate) struct Fields {
    // An in-flight open is a second owner. Mutation clones only when that snapshot is held.
    fields: RefCell<Arc<host::Fields>>,
}

impl Fields {
    fn new() -> Self {
        Self {
            fields: RefCell::new(Arc::new(host::Fields::default())),
        }
    }

    pub(crate) fn snapshot(&self) -> Result<Arc<host::Fields>, Error> {
        let fields = self
            .fields
            .try_borrow()
            .map_err(|_| errors::borrowed("field list"))?;
        Ok(fields.clone())
    }

    fn add(&self, args: &[Value]) -> Result<(), Error> {
        let args = scan_args::<(String, i32), (Option<u32>,), (), (), _, ()>(args)?;
        let (name, kind) = args.required;
        let (length,) = args.optional;
        let kwargs = get_kwargs::<_, (), (Option<bool>, Option<bool>), ()>(
            args.keywords,
            &[],
            &["nullable", "sensitive"],
        )?;
        let (nullable, sensitive) = kwargs.optional;
        let kind = host::FieldType::from_repr(kind)
            .ok_or_else(|| errors::invalid("field_type", "is not a declared field type"))?;
        let mut fields = self
            .fields
            .try_borrow_mut()
            .map_err(|_| errors::borrowed("field list"))?;
        Arc::make_mut(&mut fields)
            .add(
                &name,
                kind,
                length.unwrap_or(0),
                nullable.unwrap_or(false),
                sensitive.unwrap_or(false),
            )
            .map_err(errors::raise)
    }

    fn element(&self, args: &[Value]) -> Result<(), Error> {
        let args = scan_args::<(i32,), (Option<u32>,), (), (), (), ()>(args)?;
        let (kind,) = args.required;
        let (length,) = args.optional;
        let kind = host::FieldType::from_repr(kind)
            .ok_or_else(|| errors::invalid("field_type", "is not a declared field type"))?;
        let mut fields = self
            .fields
            .try_borrow_mut()
            .map_err(|_| errors::borrowed("field list"))?;
        Arc::make_mut(&mut fields)
            .element(kind, length.unwrap_or(0))
            .map_err(errors::raise)
    }

    fn schema(&self) -> Result<Schema, Error> {
        let fields = self.snapshot()?.schema_fields().map_err(errors::raise)?;
        Ok(Schema::new(host::Schema::of_fields(fields)))
    }
}

pub(crate) fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
    let schema = crate::class(ruby, module, "Schema")?;
    schema.define_method("fields", method!(Schema::fields, 0))?;
    schema.define_method("key_fields", method!(Schema::key_fields, 0))?;
    schema.define_method("branch", method!(Schema::branch, 0))?;
    let field = crate::class(ruby, module, "Field")?;
    field.define_method("name", method!(Field::name, 0))?;
    field.define_method("type", method!(Field::kind, 0))?;
    field.define_method("nullable", method!(Field::nullable, 0))?;
    field.define_method("sensitive", method!(Field::sensitive, 0))?;
    field.define_method("levels", method!(Field::levels, 0))?;
    let level = crate::class(ruby, module, "Level")?;
    level.define_method("type", method!(Level::kind, 0))?;
    level.define_method("length", method!(Level::length, 0))?;
    let fields = crate::class(ruby, module, "Fields")?;
    fields.define_singleton_method("new", function!(Fields::new, 0))?;
    fields.define_method("add", method!(Fields::add, -1))?;
    fields.define_method("element", method!(Fields::element, -1))?;
    fields.define_method("schema", method!(Fields::schema, 0))?;
    Ok(())
}
