//! Field descriptions through the typed field list, as a binding passes a host's description one
//! type level at a time.
//!
//! A field is described by its levels from its own type inwards, each a `FieldType` with the
//! element count of a fixed-size list, and the list yields the original field. Every scalar level
//! names its scalar back through the type's kind.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    ops::RangeInclusive,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain, Entropy};
use nervix_models::{FieldName, ParseAsType, SchemaField};

use crate::{FieldType, Fields, TypeKind, TypeLevels};

/// The most lists the field list lets one field nest.
const MAX_LISTS: u64 = 32;

/// The most elements a fixed-size list level holds: `INT32_MAX`.
const MAX_FIXED_LENGTH: u32 = i32::MAX.cast_unsigned();

/// The most fields one generated list holds.
const FIELDS: usize = 4;

/// One level of a field's type as a binding passes it: its type, and the element count of a
/// fixed-size list, which is zero for every other level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TypedLevel {
    field_type: FieldType,
    length: u32,
}

impl TypedLevel {
    fn of(ty: &ParseAsType) -> Self {
        let length = match ty {
            ParseAsType::Array { len, .. } => len.get(),
            _ => 0,
        };
        Self {
            field_type: FieldType::from(ty),
            length,
        }
    }
}

/// The levels of `ty`, from its own type inwards, as a binding passes them.
fn levels(ty: &ParseAsType) -> Vec<TypedLevel> {
    let levels = TypeLevels::of(ty);
    let mut typed = Vec::with_capacity(levels.count());
    for level in 0..levels.count() {
        let level = levels
            .get(level)
            .assured("every level below the count exists");
        typed.push(TypedLevel::of(level));
    }
    typed
}

/// Any scalar the vocabulary has.
fn scalar(entropy: &mut Entropy<'_>) -> ParseAsType {
    let scalars = ParseAsType::scalar_variants();
    let count = NonZeroUsize::new(scalars.len()).assured("the vocabulary has scalar types");
    let index = entropy.index(count);
    scalars[index].clone()
}

/// An element count from `range`, either end as likely as the rest.
fn length_within(entropy: &mut Entropy<'_>, range: RangeInclusive<u32>) -> u32 {
    let (low, high) = range.into_inner();
    let drawn = entropy.boundary_biased(u64::from(low)..=u64::from(high));
    u32::try_from(drawn).verified("a value drawn between two u32 ends fits in a u32")
}

/// A list of `element`: a variable-length list, or a fixed-size list of a few elements or of one
/// to `INT32_MAX`, either end as likely as the rest.
fn generated_list(entropy: &mut Entropy<'_>, element: ParseAsType) -> ParseAsType {
    let element = Box::new(element);
    let length = match entropy.byte() % 3 {
        0 => return ParseAsType::Vec { element },
        1 => length_within(entropy, 2..=8),
        _ => length_within(entropy, 1..=MAX_FIXED_LENGTH),
    };
    let len = NonZeroU32::new(length).verified("every element count drawn here is positive");
    ParseAsType::Array { element, len }
}

/// A field named by the whole name rule, of any scalar inside up to [`MAX_LISTS`] lists, nullable
/// or not and sensitive or not.
fn generated_field(arbitrary: &mut Arbitrary<'_>) -> SchemaField {
    let name = arbitrary.rule_name::<FieldName>();
    let entropy = arbitrary.entropy();
    let mut ty = scalar(entropy);
    for _ in 0..entropy.boundary_biased(0..=MAX_LISTS) {
        ty = generated_list(entropy, ty);
    }
    SchemaField {
        name,
        ty,
        optional: entropy.flag(),
        sensitive: entropy.flag(),
    }
}

/// Describes `field` to `list` one level at a time: its own level starts it, and every other
/// level is the element of the list before it.
fn describe(list: &mut Fields, field: &SchemaField) {
    let levels = levels(&field.ty);
    let Some((own, elements)) = levels.split_first() else {
        panic!("every type has its own level");
    };
    list.add(
        field.name.as_str(),
        own.field_type,
        own.length,
        field.optional,
        field.sensitive,
    )
    .assured("a generated field's own level starts it");
    for element in elements {
        list.element(element.field_type, element.length)
            .assured("a generated field's next level is the element of its innermost list");
    }
}

/// Asserts that every scalar level of `field` names its scalar back through its type's kind, and
/// every list level its kind of list.
fn assert_levels_name_their_types(field: &SchemaField) {
    let levels = TypeLevels::of(&field.ty);
    for level in 0..levels.count() {
        let ty = levels
            .get(level)
            .assured("every level below the count exists");
        match (ty, FieldType::from(ty).kind()) {
            (ParseAsType::Vec { .. }, TypeKind::List) => {}
            (ParseAsType::Array { .. }, TypeKind::FixedList) => {}
            (ty, TypeKind::Scalar(scalar)) => assert_eq!(&scalar, ty, "level {level}"),
            (ty, _) => panic!("level {level} of type {ty:?} names another kind"),
        }
    }
}

#[test]
fn bolero_typed_field_descriptions_round_trip() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|input: &[u8]| {
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            let count = arbitrary.entropy().count(FIELDS);
            let mut fields = Vec::with_capacity(count);
            for _ in 0..count {
                fields.push(generated_field(&mut arbitrary));
            }
            let mut list = Fields::default();
            for field in &fields {
                assert_levels_name_their_types(field);
                describe(&mut list, field);
            }
            let described = list
                .schema_fields()
                .assured("every description ends its field with a scalar");
            assert_eq!(described, fields);
        });
}
