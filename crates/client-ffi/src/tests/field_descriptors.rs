//! Field descriptions through the binding's field descriptor ABI, as a host makes and reads them.
//!
//! A host describes a field one type level at a time with `nx_fields_add` and
//! `nx_fields_element`, and reads one back with `nx_schema_field` and the level accessors as
//! `nx_type` integers. Both directions are compared with the header's own `nx_type` values, written
//! out here rather than taken from the binding's conversions, so a type the binding maps wrong in
//! both directions still fails.
//!
//! Layer: test harness.
//! - **Owns.** A host's description of a field, generators of valid and refused descriptions, and
//!   the complete equality of both ABI directions.
//! - **Depends on.** The production binding, the vocabulary's schema fields and the vocabulary
//!   test generators.
//! - **Must not know.** Sessions, endpoints, or what a server compares the fields with.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{
    num::{NonZeroU32, NonZeroUsize},
    ops::RangeInclusive,
    ptr, slice,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain, Entropy};
use nervix_models::{FieldName, ParseAsType, SchemaField};

use super::{ROWS, endpoints::HostFields, failure_kind, field, name, succeeded};
use crate::{
    FailureKind, FieldType, Schema, nx_fields_add, nx_fields_element, nx_fields_new,
    nx_schema_field, nx_schema_field_count, nx_schema_field_level, nx_schema_field_levels,
};

/// The header's `nx_type` of a FIXED_LIST level.
const FIXED_LIST: i32 = 15;

/// The header's `nx_type` of a LIST level, the last type it declares.
const LIST: i32 = 16;

/// The first integer past the types the header declares.
const PAST_LAST_TYPE: i32 = LIST + 1;

/// The most lists the header lets one field nest.
const MAX_LISTS: usize = 32;

/// The most lists a field can still nest after its first.
const MORE_LISTS: usize = MAX_LISTS - 1;

/// The most elements the header lets a FIXED_LIST level hold: `INT32_MAX`.
const MAX_FIXED_LENGTH: u32 = i32::MAX.cast_unsigned();

/// The fewest elements of a FIXED_LIST level too long for the header.
const OVERSIZED_FIXED_LENGTH: u32 = MAX_FIXED_LENGTH + 1;

/// The longest name the name rule admits, in bytes.
const MAX_NAME_BYTES: usize = 128;

/// The shortest name too long for the name rule, in bytes.
const TOO_LONG_NAME_BYTES: usize = MAX_NAME_BYTES + 1;

/// The most fields one generated list holds.
const FIELDS: usize = 4;

/// The name of a refused field whose name is not why the binding refuses it.
const REFUSED_FIELD_NAME: &[u8] = b"refused";

/// A name the name rule admits, which a refused name breaks.
const BROKEN_NAME: &[u8] = b"name";

/// The `nx_type` the header gives the outermost level of `ty`.
fn header_type(ty: &ParseAsType) -> i32 {
    match ty {
        ParseAsType::U8 => 1,
        ParseAsType::I8 => 2,
        ParseAsType::U16 => 3,
        ParseAsType::I16 => 4,
        ParseAsType::U32 => 5,
        ParseAsType::I32 => 6,
        ParseAsType::U64 => 7,
        ParseAsType::I64 => 8,
        ParseAsType::F32 => 9,
        ParseAsType::F64 => 10,
        ParseAsType::Bool => 11,
        ParseAsType::String => 12,
        ParseAsType::Bytes => 13,
        ParseAsType::Datetime => 14,
        ParseAsType::Array { .. } => FIXED_LIST,
        ParseAsType::Vec { .. } => LIST,
    }
}

/// One type level as a host passes or reads it: an `nx_type`, and the element count of a
/// FIXED_LIST level, which is zero for every other level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HostLevel {
    field_type: i32,
    length: u32,
}

impl HostLevel {
    /// The outermost level of `ty`.
    fn outermost(ty: &ParseAsType) -> Self {
        let length = match ty {
            ParseAsType::Array { len, .. } => len.get(),
            _ => 0,
        };
        Self {
            field_type: header_type(ty),
            length,
        }
    }

    /// Any level a field can have: a scalar or a list.
    fn valid(entropy: &mut Entropy<'_>) -> Self {
        if entropy.flag() {
            Self::valid_list(entropy)
        } else {
            Self::outermost(&scalar(entropy))
        }
    }

    /// Any list level a field can have.
    fn valid_list(entropy: &mut Entropy<'_>) -> Self {
        Self::outermost(&generated_list(entropy, ParseAsType::U8))
    }

    /// A level no field can have: a type that is no `nx_type`, an element count on a scalar or
    /// LIST level, or a FIXED_LIST of no elements or of more than `INT32_MAX`.
    fn invalid(entropy: &mut Entropy<'_>) -> Self {
        match entropy.byte() % 5 {
            0 => Self {
                field_type: unknown_type(entropy),
                length: length_within(entropy, 0..=u32::MAX),
            },
            1 => Self {
                field_type: header_type(&scalar(entropy)),
                length: length_within(entropy, 1..=u32::MAX),
            },
            2 => Self {
                field_type: LIST,
                length: length_within(entropy, 1..=u32::MAX),
            },
            3 => Self {
                field_type: FIXED_LIST,
                length: 0,
            },
            _ => Self {
                field_type: FIXED_LIST,
                length: length_within(entropy, OVERSIZED_FIXED_LENGTH..=u32::MAX),
            },
        }
    }
}

/// A field as a host describes or reads it: the bytes of its name, its levels from its own type
/// inwards, and its nullability and sensitivity.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HostField {
    name: Vec<u8>,
    levels: Vec<HostLevel>,
    nullable: bool,
    sensitive: bool,
}

impl HostField {
    /// How a host describes `field`, spelling its name as the vocabulary holds it.
    fn of(field: &SchemaField) -> Self {
        let mut levels = vec![HostLevel::outermost(&field.ty)];
        let mut ty = &field.ty;
        while let ParseAsType::Array { element, .. } | ParseAsType::Vec { element } = ty {
            ty = element;
            levels.push(HostLevel::outermost(ty));
        }
        Self {
            name: field.name.as_str().as_bytes().to_vec(),
            levels,
            nullable: field.optional,
            sensitive: field.sensitive,
        }
    }

    /// The same description with its name in the letter case `entropy` picks: as it is, in upper
    /// case, or letter by letter. The binding folds every spelling back to the name.
    fn respelled(&self, entropy: &mut Entropy<'_>) -> Self {
        let mut name = self.name.clone();
        match entropy.byte() % 3 {
            0 => {}
            1 => name.make_ascii_uppercase(),
            _ => {
                for byte in &mut name {
                    if entropy.flag() {
                        byte.make_ascii_uppercase();
                    }
                }
            }
        }
        Self {
            name,
            levels: self.levels.clone(),
            nullable: self.nullable,
            sensitive: self.sensitive,
        }
    }

    /// Field `index` of `schema`'s rows as a host reads it, every `nx_type` into the `int32_t` a
    /// host passes for it.
    fn read(schema: &Schema, index: usize) -> Self {
        let mut name = ptr::null();
        let mut name_len = 0;
        let mut field_type = 0_i32;
        let mut nullable = false;
        let mut sensitive = false;
        let mut count = 0;
        // SAFETY: the schema is live and every out-parameter is writable. `FieldType` is
        // `repr(i32)`, so the binding writes an `nx_type` into an `i32` as into a host's.
        unsafe {
            succeeded(nx_schema_field(
                schema,
                ROWS,
                index,
                &mut name,
                &mut name_len,
                ptr::from_mut(&mut field_type).cast::<FieldType>(),
                &mut nullable,
                &mut sensitive,
            ));
            succeeded(nx_schema_field_levels(schema, ROWS, index, &mut count));
        }
        assert!(!name.is_null(), "field {index} reads a name");
        // SAFETY: the name addresses `name_len` bytes for as long as the schema is live.
        let name = unsafe { slice::from_raw_parts(name, name_len) }.to_vec();
        let mut levels = Vec::with_capacity(count);
        for level in 0..count {
            let mut level_type = 0_i32;
            let mut length = 0;
            // SAFETY: as above.
            succeeded(unsafe {
                nx_schema_field_level(
                    schema,
                    ROWS,
                    index,
                    level,
                    ptr::from_mut(&mut level_type).cast::<FieldType>(),
                    &mut length,
                )
            });
            levels.push(HostLevel {
                field_type: level_type,
                length,
            });
        }
        match levels.first() {
            Some(outermost) => assert_eq!(
                outermost.field_type, field_type,
                "field {index} reads its own type as level 0"
            ),
            None => panic!("field {index} reads no level"),
        }
        Self {
            name,
            levels,
            nullable,
            sensitive,
        }
    }

    /// Passes level `position` of this field to `list`: the first level starts the field, and
    /// every other one is the element of the list before it.
    fn pass_level(&self, list: &HostFields, position: usize) {
        let level = self.levels[position];
        let failure = if position == 0 {
            // SAFETY: the list is live and only this thread uses it, and the name addresses its
            // length.
            unsafe {
                nx_fields_add(
                    list.0,
                    self.name.as_ptr(),
                    self.name.len(),
                    level.field_type,
                    level.length,
                    self.nullable,
                    self.sensitive,
                )
            }
        } else {
            // SAFETY: the list is live and only this thread uses it.
            unsafe { nx_fields_element(list.0, level.field_type, level.length) }
        };
        succeeded(failure);
    }

    /// Passes every level of this field to `list`.
    fn describe(&self, list: &HostFields) {
        for position in 0..self.levels.len() {
            self.pass_level(list, position);
        }
    }

    /// How far the description is once level `position` has been passed. Every level before the
    /// last is a list.
    fn progress_after(&self, position: usize) -> Progress {
        let passed = position
            .checked_add(1)
            .assured("a position indexes a level of a bounded type");
        if passed == self.levels.len() {
            Progress::Complete
        } else {
            Progress::Open { lists: passed }
        }
    }
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

/// A fixed-size list of `length` elements of `element`.
fn fixed_list(element: Box<ParseAsType>, length: u32) -> ParseAsType {
    let len = NonZeroU32::new(length).verified("every element count passed here is positive");
    ParseAsType::Array { element, len }
}

/// A list of `element`: a LIST, or a FIXED_LIST of a few elements or of 1 to `INT32_MAX`, either
/// end as likely as the rest.
fn generated_list(entropy: &mut Entropy<'_>, element: ParseAsType) -> ParseAsType {
    let element = Box::new(element);
    match entropy.byte() % 3 {
        0 => ParseAsType::Vec { element },
        1 => fixed_list(element, length_within(entropy, 2..=8)),
        _ => fixed_list(element, length_within(entropy, 1..=MAX_FIXED_LENGTH)),
    }
}

/// A field named by the whole name rule, of any scalar inside up to [`MAX_LISTS`] lists, nullable
/// or not and sensitive or not.
fn generated_field(arbitrary: &mut Arbitrary<'_>) -> SchemaField {
    let name = arbitrary.rule_name::<FieldName>();
    let entropy = arbitrary.entropy();
    let mut ty = scalar(entropy);
    let most_lists = u64::try_from(MAX_LISTS).assured("supported targets address at most 64 bits");
    let lists = entropy.boundary_biased(0..=most_lists);
    for _ in 0..lists {
        ty = generated_list(entropy, ty);
    }
    SchemaField {
        name,
        ty,
        optional: entropy.flag(),
        sensitive: entropy.flag(),
    }
}

/// Up to [`FIELDS`] generated fields.
fn generated_fields(arbitrary: &mut Arbitrary<'_>) -> Vec<SchemaField> {
    let count = arbitrary.entropy().count(FIELDS);
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        fields.push(generated_field(arbitrary));
    }
    fields
}

/// The fields a host's list holds once it has described `fields`, one call per level.
fn described(fields: &[HostField]) -> Vec<SchemaField> {
    let list = HostFields(nx_fields_new());
    for field in fields {
        field.describe(&list);
    }
    list.schema_fields()
        .assured("every description ends its field with a scalar")
}

/// Every field of `schema`'s rows as a host reads it back.
fn read(schema: &Schema) -> Vec<HostField> {
    let mut count = 0;
    // SAFETY: the schema is live and `count` is writable.
    succeeded(unsafe { nx_schema_field_count(schema, ROWS, &mut count) });
    let mut fields = Vec::with_capacity(count);
    for index in 0..count {
        fields.push(HostField::read(schema, index));
    }
    fields
}

/// Describes `fields` as the vocabulary spells their names and as `respelled` spells them, and
/// reads them back from a schema of them.
fn assert_round_trip(fields: &[SchemaField], respelled: &[HostField]) {
    let canonical: Vec<_> = fields.iter().map(HostField::of).collect();
    assert_eq!(
        described(&canonical),
        fields,
        "described as the vocabulary spells them"
    );
    assert_eq!(
        described(respelled),
        fields,
        "described in another letter case"
    );
    let schema = Schema::of_fields(fields.to_vec());
    assert_eq!(read(&schema), canonical, "read back from their schema");
}

/// Generated fields round-trip through the descriptor ABI in both directions.
fn round_trip(bytes: &[u8]) {
    let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
    let fields = generated_fields(&mut arbitrary);
    let mut respelled = Vec::with_capacity(fields.len());
    for field in &fields {
        respelled.push(HostField::of(field).respelled(arbitrary.entropy()));
    }
    assert_round_trip(&fields, &respelled);
}

/// How far a host has described its field list, which decides what the binding refuses next.
#[derive(Debug, Clone, Copy)]
enum Progress {
    /// No field has been added yet.
    Empty,
    /// The last field still needs the element of the innermost of this many lists.
    Open { lists: usize },
    /// The last field ends with its scalar.
    Complete,
}

/// The name an `nx_fields_add` call passes.
#[derive(Debug)]
enum NameArgument {
    Bytes(Vec<u8>),
    /// A null pointer, with a length the binding must not read.
    Null {
        length: usize,
    },
}

impl NameArgument {
    /// A name the name rule refuses: no bytes, a null pointer, more than [`MAX_NAME_BYTES`] bytes,
    /// a character outside the rule, or bytes that are not UTF-8.
    fn invalid(entropy: &mut Entropy<'_>) -> Self {
        let mut name = BROKEN_NAME.to_vec();
        match entropy.byte() % 5 {
            0 => Self::Bytes(Vec::new()),
            1 => Self::Null {
                length: entropy.count(MAX_NAME_BYTES),
            },
            2 => {
                let length = entropy
                    .count(MAX_NAME_BYTES)
                    .checked_add(TOO_LONG_NAME_BYTES)
                    .assured("two bounded name lengths add up within a usize");
                name.resize(length, b'a');
                Self::Bytes(name)
            }
            3 => {
                let character = entropy.pick([" ", "/", "\"", "*", "\0", "\u{e9}", "\u{1f680}"]);
                let position = entropy.count(name.len());
                let tail = name.split_off(position);
                name.extend_from_slice(character.as_bytes());
                name.extend_from_slice(&tail);
                Self::Bytes(name)
            }
            _ => {
                let byte = entropy.pick([0x80, 0xc3, 0xfe, 0xff]);
                let position = entropy.count(name.len());
                name.insert(position, byte);
                Self::Bytes(name)
            }
        }
    }

    fn pointer(&self) -> *const u8 {
        match self {
            Self::Bytes(bytes) => bytes.as_ptr(),
            Self::Null { .. } => ptr::null(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.len(),
            Self::Null { length } => *length,
        }
    }
}

/// The bytes of a name the name rule admits, as the vocabulary spells it.
fn valid_name(arbitrary: &mut Arbitrary<'_>) -> Vec<u8> {
    let name = arbitrary.rule_name::<FieldName>();
    name.as_str().as_bytes().to_vec()
}

/// An integer that is no `nx_type`: zero, a value past the last type, a negative value, or a
/// value past the types whose low byte is a type, which a read narrower than `int32_t` would take.
fn unknown_type(entropy: &mut Entropy<'_>) -> i32 {
    match entropy.byte() % 4 {
        0 => entropy.pick([0, PAST_LAST_TYPE, -1, i32::MIN, i32::MAX]),
        1 => {
            let low_byte = entropy.between(1..=16);
            let high_bytes = entropy.between(1..=0x7f_ffff);
            let shifted = high_bytes
                .checked_mul(256)
                .verified("23 bits shifted by a byte fit in a u64");
            let value = shifted
                .checked_add(low_byte)
                .verified("a byte below the shifted bits fits in a u64");
            i32::try_from(value).verified("31 bits fit in an i32")
        }
        2 => {
            let first = u64::from(PAST_LAST_TYPE.unsigned_abs());
            let value = entropy.between(first..=u64::from(i32::MAX.unsigned_abs()));
            i32::try_from(value).verified("the range ends at i32::MAX")
        }
        _ => {
            let magnitude = entropy.between(1..=u64::from(i32::MIN.unsigned_abs()));
            let magnitude = i64::try_from(magnitude).verified("2^31 fits in an i64");
            let value = magnitude.checked_neg().verified("a positive i64 negates");
            i32::try_from(value).verified("a negative magnitude of at most 2^31 fits in an i32")
        }
    }
}

/// A call the binding refuses in the progress it is made in.
#[derive(Debug)]
enum Refused {
    /// `nx_fields_add`, starting a field with this name and first level.
    Add {
        name: NameArgument,
        level: HostLevel,
        nullable: bool,
        sensitive: bool,
    },
    /// `nx_fields_element`, continuing the last field with this level.
    Element(HostLevel),
}

impl Refused {
    /// A call the binding refuses after `progress`: an invalid level or name, refused whatever
    /// the progress, or a valid call made out of order.
    fn drawn(arbitrary: &mut Arbitrary<'_>, progress: Progress) -> Self {
        match arbitrary.entropy().byte() % 4 {
            0 => {
                let name = NameArgument::Bytes(REFUSED_FIELD_NAME.to_vec());
                let level = HostLevel::invalid(arbitrary.entropy());
                Self::add(arbitrary.entropy(), name, level)
            }
            1 => Self::Element(HostLevel::invalid(arbitrary.entropy())),
            2 => {
                let name = NameArgument::invalid(arbitrary.entropy());
                let level = HostLevel::valid(arbitrary.entropy());
                Self::add(arbitrary.entropy(), name, level)
            }
            _ => Self::out_of_order(arbitrary, progress),
        }
    }

    /// A call that is valid in itself and refused only in `progress`: an element when no field is
    /// open, a list more than a field may nest, or a field before the open one has its element.
    fn out_of_order(arbitrary: &mut Arbitrary<'_>, progress: Progress) -> Self {
        let Progress::Open { lists } = progress else {
            return Self::Element(HostLevel::valid(arbitrary.entropy()));
        };
        if lists == MAX_LISTS && arbitrary.entropy().flag() {
            return Self::Element(HostLevel::valid_list(arbitrary.entropy()));
        }
        let name = NameArgument::Bytes(REFUSED_FIELD_NAME.to_vec());
        let level = HostLevel::valid(arbitrary.entropy());
        Self::add(arbitrary.entropy(), name, level)
    }

    fn add(entropy: &mut Entropy<'_>, name: NameArgument, level: HostLevel) -> Self {
        Self::Add {
            name,
            level,
            nullable: entropy.flag(),
            sensitive: entropy.flag(),
        }
    }

    /// Makes the call on `list` and checks that the binding refuses it as an invalid argument.
    fn refused_by(&self, list: &HostFields) {
        let failure = match self {
            Self::Add {
                name,
                level,
                nullable,
                sensitive,
            } => {
                // SAFETY: the list is live and only this thread uses it, and a non-null name
                // addresses its length.
                unsafe {
                    nx_fields_add(
                        list.0,
                        name.pointer(),
                        name.len(),
                        level.field_type,
                        level.length,
                        *nullable,
                        *sensitive,
                    )
                }
            }
            Self::Element(level) => {
                // SAFETY: the list is live and only this thread uses it.
                unsafe { nx_fields_element(list.0, level.field_type, level.length) }
            }
        };
        assert_eq!(
            failure_kind(failure),
            FailureKind::InvalidArgument,
            "{self:?}"
        );
    }
}

/// Makes the calls the binding refuses after `progress`: a list past the last one a field may
/// nest, once a field is open inside all of them, and then none, one or two others, rarely
/// enough that a description reaches its deepest levels before the bytes run out.
fn refuse_some(arbitrary: &mut Arbitrary<'_>, list: &HostFields, progress: Progress) {
    if let Progress::Open { lists: MAX_LISTS } = progress
        && arbitrary.entropy().flag()
    {
        Refused::Element(HostLevel::valid_list(arbitrary.entropy())).refused_by(list);
    }
    let refusals = match arbitrary.entropy().byte() % 8 {
        6 => 1,
        7 => 2,
        _ => 0,
    };
    for _ in 0..refusals {
        Refused::drawn(arbitrary, progress).refused_by(list);
    }
}

/// An index from `first` up, `first` itself and the largest index as likely as the rest.
fn index_from(entropy: &mut Entropy<'_>, first: usize) -> usize {
    let first = u64::try_from(first).assured("supported targets address at most 64 bits");
    let last = u64::try_from(usize::MAX).assured("supported targets address at most 64 bits");
    let drawn = entropy.boundary_biased(first..=last);
    usize::try_from(drawn).verified("the range ends at usize::MAX")
}

/// Checks that reading `level` of field `index`, past the field's innermost element, is refused.
fn refused_level(schema: &Schema, index: usize, level: usize) {
    let mut level_type = 0_i32;
    let mut length = 0;
    // SAFETY: the schema is live and the out-parameters are writable, as in `HostField::read`.
    let failure = unsafe {
        nx_schema_field_level(
            schema,
            ROWS,
            index,
            level,
            ptr::from_mut(&mut level_type).cast::<FieldType>(),
            &mut length,
        )
    };
    assert_eq!(
        failure_kind(failure),
        FailureKind::InvalidArgument,
        "level {level} of field {index}"
    );
}

/// Checks that every field accessor refuses `index`, past the last field.
fn refused_field(schema: &Schema, index: usize) {
    let mut name = ptr::null();
    let mut name_len = 0;
    let mut field_type = 0_i32;
    let mut nullable = false;
    let mut sensitive = false;
    let mut count = 0;
    // SAFETY: the schema is live and the out-parameters are writable, as in `HostField::read`.
    unsafe {
        assert_eq!(
            failure_kind(nx_schema_field(
                schema,
                ROWS,
                index,
                &mut name,
                &mut name_len,
                ptr::from_mut(&mut field_type).cast::<FieldType>(),
                &mut nullable,
                &mut sensitive,
            )),
            FailureKind::InvalidArgument,
            "field {index}"
        );
        assert_eq!(
            failure_kind(nx_schema_field_levels(schema, ROWS, index, &mut count)),
            FailureKind::InvalidArgument,
            "the levels of field {index}"
        );
    }
    refused_level(schema, index, 0);
}

/// Refused calls interleaved with a generated description leave it intact, an open last field
/// leaves the list incomplete, and reads past the end of a field or the fields are refused.
fn refusals(bytes: &[u8]) {
    let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
    let fields = generated_fields(&mut arbitrary);
    let list = HostFields(nx_fields_new());
    let mut progress = Progress::Empty;
    for field in &fields {
        let description = HostField::of(field);
        for position in 0..description.levels.len() {
            refuse_some(&mut arbitrary, &list, progress);
            description.pass_level(&list, position);
            progress = description.progress_after(position);
        }
    }
    refuse_some(&mut arbitrary, &list, progress);
    assert_eq!(
        list.schema_fields()
            .assured("every accepted description ends its field with a scalar"),
        fields,
        "refused calls leave the list as the accepted ones built it"
    );

    // A last field that never gets its element leaves the list without a complete type.
    let open = HostField {
        name: valid_name(&mut arbitrary),
        levels: vec![HostLevel::valid_list(arbitrary.entropy())],
        nullable: arbitrary.entropy().flag(),
        sensitive: arbitrary.entropy().flag(),
    };
    open.pass_level(&list, 0);
    for _ in 0..arbitrary.entropy().count(MORE_LISTS) {
        let level = HostLevel::valid_list(arbitrary.entropy());
        // SAFETY: the list is live and only this thread uses it.
        succeeded(unsafe { nx_fields_element(list.0, level.field_type, level.length) });
    }
    match list.schema_fields() {
        Ok(read) => panic!("a list whose last field lacks its element reads as {read:?}"),
        Err(failure) => assert_eq!(failure.kind(), FailureKind::InvalidArgument),
    }

    // Reading past the innermost element of a field, or past the last field, is refused.
    let schema = Schema::of_fields(fields.clone());
    for (index, field) in fields.iter().enumerate() {
        let levels = HostField::of(field).levels.len();
        let past = index_from(arbitrary.entropy(), levels);
        refused_level(&schema, index, past);
    }
    let past = index_from(arbitrary.entropy(), fields.len());
    refused_field(&schema, past);
}

#[test]
fn bolero_host_field_descriptions_round_trip_through_the_binding() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            round_trip(bytes);
        });
}

#[test]
fn bolero_invalid_field_descriptions_are_refused_without_changing_the_list() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            refusals(bytes);
        });
}

#[test]
fn every_scalar_and_list_shape_round_trips_at_the_header_bounds() {
    let names = ["a", "z9", "_", "-", "~", ".", "0", "a.b-c~d_e"];
    let longest = "x".repeat(MAX_NAME_BYTES);
    let mut fields = Vec::new();
    for (index, scalar) in ParseAsType::scalar_variants().iter().enumerate() {
        let mut deep = scalar.clone();
        for depth in 0..MAX_LISTS {
            let element = Box::new(deep);
            deep = match depth % 4 {
                0 => ParseAsType::Vec { element },
                1 => fixed_list(element, 1),
                2 => fixed_list(element, MAX_FIXED_LENGTH),
                _ => fixed_list(element, 3),
            };
        }
        fields.push(SchemaField {
            name: name(names[index % names.len()]),
            ty: scalar.clone(),
            optional: index % 2 == 1,
            sensitive: index % 4 >= 2,
        });
        fields.push(SchemaField {
            name: name(&longest),
            ty: deep,
            optional: index % 2 == 0,
            sensitive: index % 4 < 2,
        });
    }
    let mut respelled = Vec::with_capacity(fields.len());
    for field in &fields {
        let mut description = HostField::of(field);
        description.name.make_ascii_uppercase();
        respelled.push(description);
    }
    assert_round_trip(&fields, &respelled);
}

#[test]
fn every_invalid_description_is_refused_at_the_header_bounds() {
    let mut deep_type = ParseAsType::U8;
    for depth in 0..MAX_LISTS {
        let element = Box::new(deep_type);
        deep_type = match depth % 3 {
            0 => ParseAsType::Vec { element },
            1 => fixed_list(element, 1),
            _ => fixed_list(element, MAX_FIXED_LENGTH),
        };
    }
    let fields = vec![
        field("id", ParseAsType::U32),
        SchemaField {
            name: name("deep"),
            ty: deep_type,
            optional: true,
            sensitive: true,
        },
    ];
    let id = HostField::of(&fields[0]);
    let deep = HostField::of(&fields[1]);
    let scalar = HostLevel::outermost(&ParseAsType::U32);
    let invalid_levels = [
        HostLevel {
            field_type: 0,
            length: 0,
        },
        HostLevel {
            field_type: PAST_LAST_TYPE,
            length: 0,
        },
        HostLevel {
            field_type: -1,
            length: 0,
        },
        HostLevel {
            field_type: i32::MIN,
            length: 0,
        },
        // Its low byte is STRING.
        HostLevel {
            field_type: 0x10c,
            length: 0,
        },
        HostLevel {
            length: 1,
            ..scalar
        },
        HostLevel {
            length: u32::MAX,
            ..scalar
        },
        HostLevel {
            field_type: LIST,
            length: 1,
        },
        HostLevel {
            field_type: FIXED_LIST,
            length: 0,
        },
        HostLevel {
            field_type: FIXED_LIST,
            length: OVERSIZED_FIXED_LENGTH,
        },
        HostLevel {
            field_type: FIXED_LIST,
            length: u32::MAX,
        },
    ];
    let invalid_names = [
        NameArgument::Bytes(Vec::new()),
        NameArgument::Null { length: 4 },
        NameArgument::Bytes(vec![b'a'; TOO_LONG_NAME_BYTES]),
        NameArgument::Bytes(b"a b".to_vec()),
        NameArgument::Bytes("caf\u{e9}".as_bytes().to_vec()),
        NameArgument::Bytes(vec![b'a', 0xff]),
    ];
    let list = HostFields(nx_fields_new());

    // Nothing to continue yet, and no field can start with these names or levels.
    Refused::Element(scalar).refused_by(&list);
    for invalid in invalid_names {
        Refused::Add {
            name: invalid,
            level: scalar,
            nullable: false,
            sensitive: false,
        }
        .refused_by(&list);
    }
    for level in invalid_levels {
        Refused::Add {
            name: NameArgument::Bytes(b"x".to_vec()),
            level,
            nullable: false,
            sensitive: false,
        }
        .refused_by(&list);
        Refused::Element(level).refused_by(&list);
    }

    // A field that ends with its scalar takes no element.
    id.describe(&list);
    Refused::Element(scalar).refused_by(&list);

    // A field open inside all the lists it may nest takes no further list, no new field and no
    // invalid element, and leaves the list incomplete until its element comes.
    for position in 0..MAX_LISTS {
        deep.pass_level(&list, position);
    }
    for level in [
        HostLevel {
            field_type: LIST,
            length: 0,
        },
        HostLevel {
            field_type: FIXED_LIST,
            length: 1,
        },
    ] {
        Refused::Element(level).refused_by(&list);
    }
    Refused::Add {
        name: NameArgument::Bytes(b"late".to_vec()),
        level: scalar,
        nullable: false,
        sensitive: false,
    }
    .refused_by(&list);
    for level in invalid_levels {
        Refused::Element(level).refused_by(&list);
    }
    match list.schema_fields() {
        Ok(read) => panic!("a list whose last field lacks its element reads as {read:?}"),
        Err(failure) => assert_eq!(failure.kind(), FailureKind::InvalidArgument),
    }
    deep.pass_level(&list, MAX_LISTS);
    assert_eq!(
        list.schema_fields()
            .assured("the deep field ends with its scalar"),
        fields
    );

    let schema = Schema::of_fields(fields.clone());
    refused_level(&schema, 0, id.levels.len());
    refused_level(&schema, 1, deep.levels.len());
    refused_level(&schema, 1, usize::MAX);
    refused_field(&schema, fields.len());
    refused_field(&schema, usize::MAX);
}
