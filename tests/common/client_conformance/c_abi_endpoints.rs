//! The producer and consumer exercise of the in-process C ABI probe.
//!
//! It opens a producer and a consumer through the binding exactly as a C host would, builds every
//! batch column by column from buffers it overwrites as soon as each call returns, reads every
//! delivered column one level at a time, and prints the same report as every binding probe.

use std::{io, ptr, time::Duration};

use nervix_client_ffi::{
    Batch, BatchBuilder, Consumer, Delivery, EndpointState, FailureKind, FieldType, Fields,
    OpenRefusal, Producer, Schema, Settlement, SubmissionOutcome, SubmissionResult, WindowKind,
    nx_batch_builder_finish, nx_batch_builder_fixed, nx_batch_builder_free, nx_batch_builder_new,
    nx_batch_builder_offsets, nx_batch_builder_states, nx_batch_builder_varlen, nx_batch_cells,
    nx_batch_fixed, nx_batch_ipc, nx_batch_offsets, nx_batch_release, nx_batch_retain,
    nx_batch_row_count, nx_batch_states, nx_batch_varlen, nx_consumer_close, nx_consumer_free,
    nx_consumer_generation, nx_consumer_grant, nx_consumer_next, nx_consumer_policy,
    nx_consumer_schema, nx_consumer_state, nx_delivery_ack, nx_delivery_batch,
    nx_delivery_branch_fingerprint, nx_delivery_identity, nx_delivery_ipc, nx_delivery_members,
    nx_delivery_reference, nx_delivery_reject, nx_delivery_release, nx_delivery_retain,
    nx_delivery_retry, nx_delivery_source_relay, nx_error_free, nx_error_kind_of,
    nx_error_open_refusal, nx_fields_add, nx_fields_element, nx_fields_free, nx_fields_new,
    nx_producer_admission, nx_producer_close, nx_producer_free, nx_producer_generation,
    nx_producer_grant, nx_producer_pending, nx_producer_policy, nx_producer_rejoin,
    nx_producer_schema, nx_producer_state, nx_producer_submit, nx_producer_submit_ipc,
    nx_schema_field, nx_schema_field_count, nx_schema_field_level, nx_schema_field_levels,
    nx_schema_free, nx_session_open_ingestor, nx_session_subscribe_emitter,
    nx_submission_outcome_defect, nx_submission_outcome_failure, nx_submission_outcome_free,
    nx_submission_outcome_refusal, nx_submission_outcome_result, nx_submission_outcome_uncertainty,
};
use nervix_primitives::{thread, time::Instant};

use super::c_abi_probe::{Emit, OpenSession, Token, check, copied, expect_failure, hex};

/// How long a step waits for what the scenario or the graph produces.
const WAIT_MILLIS: u64 = 120_000;

/// The deadline of a wait that has to expire because nothing can end it sooner.
const EXPIRING_MILLIS: u64 = 200;

/// The credit the probe's producer asks for: two batches, so a third waits for one of them.
const PRODUCER_BATCHES: u32 = 2;
/// The credit the probe's consumer asks for.
const CONSUMER_BATCHES: u32 = 4;
/// The bytes every endpoint of the probe asks for, which hold one batch the emitter prepares.
const ENDPOINT_BYTES: u64 = 1_048_576;

/// The byte a probe writes over a buffer once the binding has copied it.
const SCRIBBLE: u8 = 0xaa;

/// The `NX_PART_ROWS` part of a schema.
const ROWS: i32 = 1;

/// The error a returned failure stands for, releasing it.
fn failed(failure: *mut nervix_client_ffi::Failure) -> io::Error {
    match check(failure) {
        Err(error) => error,
        Ok(()) => io::Error::other("a call reported a failure it did not return"),
    }
}

/// The value the header gives a type, as a host passes it.
fn type_code(ty: FieldType) -> i32 {
    match ty {
        FieldType::U8 => 1,
        FieldType::I8 => 2,
        FieldType::U16 => 3,
        FieldType::I16 => 4,
        FieldType::U32 => 5,
        FieldType::I32 => 6,
        FieldType::U64 => 7,
        FieldType::I64 => 8,
        FieldType::F32 => 9,
        FieldType::F64 => 10,
        FieldType::Bool => 11,
        FieldType::String => 12,
        FieldType::Bytes => 13,
        FieldType::Datetime => 14,
        FieldType::FixedList => 15,
        FieldType::List => 16,
    }
}

/// One level of a field's type, as a host names it.
#[derive(Clone, Copy)]
struct Level {
    ty: FieldType,
    length: u32,
}

impl Level {
    const fn of(ty: FieldType) -> Self {
        Self { ty, length: 0 }
    }

    const fn list() -> Self {
        Self::of(FieldType::List)
    }

    const fn fixed(length: u32) -> Self {
        Self {
            ty: FieldType::FixedList,
            length,
        }
    }
}

/// One field the probe expects an endpoint to have.
struct FieldSpec {
    name: &'static str,
    levels: &'static [Level],
    nullable: bool,
    sensitive: bool,
}

impl FieldSpec {
    const fn new(name: &'static str, levels: &'static [Level]) -> Self {
        Self {
            name,
            levels,
            nullable: false,
            sensitive: false,
        }
    }

    const fn nullable(self) -> Self {
        Self {
            nullable: true,
            ..self
        }
    }

    const fn sensitive(self) -> Self {
        Self {
            sensitive: true,
            ..self
        }
    }
}

const U32: &[Level] = &[Level::of(FieldType::U32)];
const STRING: &[Level] = &[Level::of(FieldType::String)];

/// The input schema of the probe's ingestor, in declared order.
const INPUT_FIELDS: &[FieldSpec] = &[
    FieldSpec::new("id", U32),
    FieldSpec::new("tenant", STRING),
    FieldSpec::new("u8v", &[Level::of(FieldType::U8)]),
    FieldSpec::new("i8v", &[Level::of(FieldType::I8)]),
    FieldSpec::new("u16v", &[Level::of(FieldType::U16)]),
    FieldSpec::new("i16v", &[Level::of(FieldType::I16)]),
    FieldSpec::new("u32v", U32),
    FieldSpec::new("i32v", &[Level::of(FieldType::I32)]),
    FieldSpec::new("u64v", &[Level::of(FieldType::U64)]),
    FieldSpec::new("i64v", &[Level::of(FieldType::I64)]),
    FieldSpec::new("f32v", &[Level::of(FieldType::F32)]),
    FieldSpec::new("f64v", &[Level::of(FieldType::F64)]),
    FieldSpec::new("flag", &[Level::of(FieldType::Bool)]),
    FieldSpec::new("text", STRING).nullable(),
    FieldSpec::new("raw", &[Level::of(FieldType::Bytes)]).nullable(),
    FieldSpec::new("at", &[Level::of(FieldType::Datetime)]),
    FieldSpec::new("maybe", &[Level::of(FieldType::I64)]).nullable(),
    FieldSpec::new("tags", &[Level::list(), Level::of(FieldType::String)]),
    FieldSpec::new(
        "grid",
        &[Level::fixed(2), Level::fixed(2), Level::of(FieldType::I16)],
    ),
    FieldSpec::new(
        "spans",
        &[
            Level::list(),
            Level::fixed(2),
            Level::of(FieldType::Datetime),
        ],
    )
    .nullable(),
    FieldSpec::new("secret", STRING).sensitive(),
];

/// The field the emitter adds to every output row: the row's `id` again.
const ECHO_FIELD: FieldSpec = FieldSpec::new("echo", U32);

/// The ingestor's secret as a host that forgot its sensitivity expects it.
const PUBLIC_SECRET: FieldSpec = FieldSpec::new("secret", STRING);

/// One row of an input batch, as the probe's application holds it.
#[derive(Clone)]
struct Row {
    id: u32,
    tenant: &'static str,
    u8v: u8,
    i8v: i8,
    u16v: u16,
    i16v: i16,
    u32v: u32,
    i32v: i32,
    u64v: u64,
    i64v: i64,
    f32v: u32,
    f64v: u64,
    flag: bool,
    text: Option<&'static str>,
    raw: Option<&'static [u8]>,
    at: i64,
    maybe: Option<i64>,
    tags: &'static [&'static str],
    grid: [[i16; 2]; 2],
    spans: Option<&'static [[i64; 2]]>,
    secret: &'static str,
}

impl Row {
    /// A row of `tenant` whose every other value is the zero of its type.
    fn plain(id: u32, tenant: &'static str) -> Self {
        Self {
            id,
            tenant,
            u8v: 0,
            i8v: 0,
            u16v: 0,
            i16v: 0,
            u32v: 0,
            i32v: 0,
            u64v: 0,
            i64v: 0,
            f32v: 0,
            f64v: 0,
            flag: false,
            text: None,
            raw: None,
            at: 0,
            maybe: None,
            tags: &[],
            grid: [[0; 2]; 2],
            spans: None,
            secret: "s",
        }
    }
}

/// The typed rows of the first batch: every integer width at both extremes, float sign and
/// subnormal bits, text and bytes with and without a value, lists that are empty and not, and the
/// extreme DATETIME nanoseconds.
fn typed_rows() -> Vec<Row> {
    vec![
        Row {
            id: 1,
            tenant: "acme",
            u8v: u8::MAX,
            i8v: i8::MAX,
            u16v: u16::MAX,
            i16v: i16::MAX,
            u32v: u32::MAX,
            i32v: i32::MAX,
            u64v: u64::MAX,
            i64v: i64::MAX,
            f32v: 0x7f7f_ffff,
            f64v: 0x7fef_ffff_ffff_ffff,
            flag: true,
            text: Some("h\u{e9}llo \u{4e16}\u{754c} \u{1f680}"),
            raw: Some(&[0x00, 0xff, 0xfe, 0x80, 0x00]),
            at: i64::MAX,
            maybe: None,
            tags: &["a", "", "h\u{e9}llo"],
            grid: [[1, -2], [i16::MAX, i16::MIN]],
            spans: Some(&[[i64::MIN, i64::MAX]]),
            secret: "s1",
        },
        Row {
            id: 2,
            tenant: "acme",
            u8v: 0,
            i8v: i8::MIN,
            u16v: 0,
            i16v: i16::MIN,
            u32v: 0,
            i32v: i32::MIN,
            u64v: 0,
            i64v: i64::MIN,
            f32v: 0x8000_0000,
            f64v: 0x0000_0000_0000_0001,
            flag: false,
            text: Some("a\u{0}b"),
            raw: Some(&[]),
            at: i64::MIN,
            maybe: Some(0),
            tags: &[],
            grid: [[0, 0], [0, 0]],
            spans: None,
            secret: "s2",
        },
        Row {
            id: 3,
            tenant: "acme",
            u8v: 1,
            i8v: -1,
            u16v: 1,
            i16v: -1,
            u32v: 1,
            i32v: -1,
            u64v: 9_007_199_254_740_993,
            i64v: -9_007_199_254_740_993,
            f32v: 0x3fc0_0000,
            f64v: 0x3fb9_9999_9999_999a,
            flag: true,
            text: None,
            raw: None,
            at: 1,
            maybe: Some(9_007_199_254_740_992),
            tags: &["x"],
            grid: [[-1, 1], [2, -2]],
            spans: Some(&[]),
            secret: "s3",
        },
    ]
}

/// What a host passes the binding for one column.
struct HostColumn {
    /// One state per row, when a row can be null.
    states: Option<Vec<u8>>,
    /// The offsets of each variable-length list level, by level.
    offsets: Vec<LevelOffsets>,
    /// The level the values fill, and the values.
    leaf: usize,
    values: HostValues,
}

struct LevelOffsets {
    level: usize,
    offsets: Vec<u64>,
}

enum HostValues {
    Fixed(Vec<u8>),
    Varlen { offsets: Vec<u64>, data: Vec<u8> },
}

/// The offsets of `lengths` consecutive runs, starting at zero.
fn offsets_of(lengths: impl IntoIterator<Item = usize>) -> Vec<u64> {
    let mut offsets = vec![0_u64];
    let mut end = 0_u64;
    for length in lengths {
        end += u64::try_from(length).expect("a probe list is short");
        offsets.push(end);
    }
    offsets
}

/// A string or bytes column, one optional value per cell.
fn varlen_values<'a>(values: impl IntoIterator<Item = Option<&'a [u8]>>) -> HostValues {
    let mut data = Vec::new();
    let mut lengths = Vec::new();
    for value in values {
        let bytes = value.unwrap_or_default();
        data.extend_from_slice(bytes);
        lengths.push(bytes.len());
    }
    HostValues::Varlen {
        offsets: offsets_of(lengths),
        data,
    }
}

/// One state per row: a value, or null for a row without one.
fn states_of(present: impl IntoIterator<Item = bool>) -> Vec<u8> {
    present
        .into_iter()
        .map(|present| if present { 1 } else { 2 })
        .collect()
}

fn scalar(values: Vec<u8>) -> HostColumn {
    HostColumn {
        states: None,
        offsets: Vec::new(),
        leaf: 0,
        values: HostValues::Fixed(values),
    }
}

/// The column `name` of `rows`, as a host lays it out.
fn column(name: &str, rows: &[Row]) -> io::Result<HostColumn> {
    let fixed = |bytes: &dyn Fn(&Row) -> Vec<u8>| scalar(rows.iter().flat_map(bytes).collect());
    let column = match name {
        "id" | "echo" => fixed(&|row| row.id.to_ne_bytes().to_vec()),
        "tenant" => HostColumn {
            states: None,
            offsets: Vec::new(),
            leaf: 0,
            values: varlen_values(rows.iter().map(|row| Some(row.tenant.as_bytes()))),
        },
        "u8v" => fixed(&|row| row.u8v.to_ne_bytes().to_vec()),
        "i8v" => fixed(&|row| row.i8v.to_ne_bytes().to_vec()),
        "u16v" => fixed(&|row| row.u16v.to_ne_bytes().to_vec()),
        "i16v" => fixed(&|row| row.i16v.to_ne_bytes().to_vec()),
        "u32v" => fixed(&|row| row.u32v.to_ne_bytes().to_vec()),
        "i32v" => fixed(&|row| row.i32v.to_ne_bytes().to_vec()),
        "u64v" => fixed(&|row| row.u64v.to_ne_bytes().to_vec()),
        "i64v" => fixed(&|row| row.i64v.to_ne_bytes().to_vec()),
        "f32v" => fixed(&|row| row.f32v.to_ne_bytes().to_vec()),
        "f64v" => fixed(&|row| row.f64v.to_ne_bytes().to_vec()),
        "flag" => fixed(&|row| vec![u8::from(row.flag)]),
        "at" => fixed(&|row| row.at.to_ne_bytes().to_vec()),
        "text" => HostColumn {
            states: Some(states_of(rows.iter().map(|row| row.text.is_some()))),
            offsets: Vec::new(),
            leaf: 0,
            values: varlen_values(rows.iter().map(|row| row.text.map(str::as_bytes))),
        },
        "raw" => HostColumn {
            states: Some(states_of(rows.iter().map(|row| row.raw.is_some()))),
            offsets: Vec::new(),
            leaf: 0,
            values: varlen_values(rows.iter().map(|row| row.raw)),
        },
        "maybe" => HostColumn {
            states: Some(states_of(rows.iter().map(|row| row.maybe.is_some()))),
            offsets: Vec::new(),
            leaf: 0,
            values: HostValues::Fixed(
                rows.iter()
                    .flat_map(|row| row.maybe.unwrap_or(0).to_ne_bytes())
                    .collect(),
            ),
        },
        "tags" => HostColumn {
            states: None,
            offsets: vec![LevelOffsets {
                level: 0,
                offsets: offsets_of(rows.iter().map(|row| row.tags.len())),
            }],
            leaf: 1,
            values: varlen_values(
                rows.iter()
                    .flat_map(|row| row.tags.iter().map(|tag| Some(tag.as_bytes()))),
            ),
        },
        "grid" => HostColumn {
            states: None,
            offsets: Vec::new(),
            leaf: 2,
            values: HostValues::Fixed(
                rows.iter()
                    .flat_map(|row| {
                        row.grid
                            .iter()
                            .flatten()
                            .flat_map(|cell| cell.to_ne_bytes())
                    })
                    .collect(),
            ),
        },
        "spans" => HostColumn {
            states: Some(states_of(rows.iter().map(|row| row.spans.is_some()))),
            offsets: vec![LevelOffsets {
                level: 0,
                offsets: offsets_of(rows.iter().map(|row| row.spans.unwrap_or_default().len())),
            }],
            leaf: 2,
            values: HostValues::Fixed(
                rows.iter()
                    .flat_map(|row| row.spans.unwrap_or_default().iter().flatten())
                    .flat_map(|instant| instant.to_ne_bytes())
                    .collect(),
            ),
        },
        "secret" => HostColumn {
            states: None,
            offsets: Vec::new(),
            leaf: 0,
            values: varlen_values(rows.iter().map(|row| Some(row.secret.as_bytes()))),
        },
        other => {
            return Err(io::Error::other(format!(
                "the probe holds no column named '{other}'"
            )));
        }
    };
    Ok(column)
}

/// A field list, freed when dropped.
struct OwnedFields(*mut Fields);

impl Drop for OwnedFields {
    fn drop(&mut self) {
        // SAFETY: the field list is live and released exactly once, here.
        unsafe { nx_fields_free(self.0) };
    }
}

impl OwnedFields {
    fn of<'a>(fields: impl IntoIterator<Item = &'a FieldSpec>) -> io::Result<Self> {
        let owned = Self(nx_fields_new());
        for field in fields {
            let mut levels = field.levels.iter();
            let first = levels
                .next()
                .ok_or_else(|| io::Error::other("a probe field has a type"))?;
            // SAFETY: the field list is live and used by this thread only, and the name addresses
            // its length in readable bytes.
            check(unsafe {
                nx_fields_add(
                    owned.0,
                    field.name.as_ptr(),
                    field.name.len(),
                    type_code(first.ty),
                    first.length,
                    field.nullable,
                    field.sensitive,
                )
            })?;
            for level in levels {
                // SAFETY: the field list is live and used by this thread only.
                check(unsafe { nx_fields_element(owned.0, type_code(level.ty), level.length) })?;
            }
        }
        Ok(owned)
    }
}

/// A schema, freed when dropped.
struct SchemaHandle(*mut Schema);

impl Drop for SchemaHandle {
    fn drop(&mut self) {
        // SAFETY: the schema is live and released exactly once, here.
        unsafe { nx_schema_free(self.0) };
    }
}

/// One field of a schema the binding reported.
struct ReportedField {
    name: String,
    levels: Vec<Level>,
    nullable: bool,
    sensitive: bool,
}

impl ReportedField {
    /// The field's type in NSPL's spelling, with nested fixed-size lists as dimensions.
    fn type_text(&self) -> String {
        Self::levels_text(&self.levels)
    }

    fn levels_text(levels: &[Level]) -> String {
        let Some(first) = levels.first() else {
            return String::new();
        };
        match first.ty {
            FieldType::List => format!("VEC<{}>", Self::levels_text(&levels[1..])),
            FieldType::FixedList => {
                let dimensions: Vec<String> = levels
                    .iter()
                    .take_while(|level| level.ty == FieldType::FixedList)
                    .map(|level| level.length.to_string())
                    .collect();
                let element = Self::levels_text(&levels[dimensions.len()..]);
                format!("ARRAY<{element}, {}>", dimensions.join(", "))
            }
            scalar => scalar_name(scalar).to_string(),
        }
    }

    fn line(&self) -> String {
        let nullability = if self.nullable {
            "nullable"
        } else {
            "required"
        };
        let sensitivity = if self.sensitive {
            "sensitive"
        } else {
            "public"
        };
        format!(
            "{} {} {nullability} {sensitivity}",
            self.name,
            self.type_text()
        )
    }
}

fn scalar_name(ty: FieldType) -> &'static str {
    match ty {
        FieldType::U8 => "U8",
        FieldType::I8 => "I8",
        FieldType::U16 => "U16",
        FieldType::I16 => "I16",
        FieldType::U32 => "U32",
        FieldType::I32 => "I32",
        FieldType::U64 => "U64",
        FieldType::I64 => "I64",
        FieldType::F32 => "F32",
        FieldType::F64 => "F64",
        FieldType::Bool => "BOOL",
        FieldType::String => "STRING",
        FieldType::Bytes => "BYTES",
        FieldType::Datetime => "DATETIME",
        FieldType::FixedList => "FIXED_LIST",
        FieldType::List => "LIST",
    }
}

fn width(ty: FieldType) -> io::Result<usize> {
    match ty {
        FieldType::U8 | FieldType::I8 | FieldType::Bool => Ok(1),
        FieldType::U16 | FieldType::I16 => Ok(2),
        FieldType::U32 | FieldType::I32 | FieldType::F32 => Ok(4),
        FieldType::U64 | FieldType::I64 | FieldType::F64 | FieldType::Datetime => Ok(8),
        other => Err(io::Error::other(format!("{other:?} is not fixed-width"))),
    }
}

impl SchemaHandle {
    fn fields(&self) -> io::Result<Vec<ReportedField>> {
        let rows = ROWS;
        let mut count = 0;
        // SAFETY: the schema is live and `count` is writable.
        check(unsafe { nx_schema_field_count(self.0, rows, &mut count) })?;
        let mut fields = Vec::with_capacity(count);
        for index in 0..count {
            let mut name = ptr::null();
            let mut name_len = 0;
            let mut ty = FieldType::U8;
            let mut nullable = false;
            let mut sensitive = false;
            let mut levels = 0;
            // SAFETY: the schema is live, every out-parameter is writable, and the name is copied
            // before the schema can be released.
            let name = unsafe {
                check(nx_schema_field(
                    self.0,
                    rows,
                    index,
                    &mut name,
                    &mut name_len,
                    &mut ty,
                    &mut nullable,
                    &mut sensitive,
                ))?;
                check(nx_schema_field_levels(self.0, rows, index, &mut levels))?;
                String::from_utf8(copied(name, name_len))
                    .map_err(|_| io::Error::other("a field name is not UTF-8"))?
            };
            let mut field_levels = Vec::with_capacity(levels);
            for level in 0..levels {
                let mut level_type = FieldType::U8;
                let mut length = 0;
                // SAFETY: the schema is live and the out-parameters are writable.
                check(unsafe {
                    nx_schema_field_level(self.0, rows, index, level, &mut level_type, &mut length)
                })?;
                field_levels.push(Level {
                    ty: level_type,
                    length,
                });
            }
            if field_levels.first().map(|level| level.ty) != Some(ty) {
                return Err(io::Error::other(
                    "a field's first level differs from its type",
                ));
            }
            fields.push(ReportedField {
                name,
                levels: field_levels,
                nullable,
                sensitive,
            });
        }
        Ok(fields)
    }
}

/// A batch builder, freed when dropped.
struct OwnedBuilder(*mut BatchBuilder);

impl Drop for OwnedBuilder {
    fn drop(&mut self) {
        // SAFETY: the builder is live and released exactly once, here.
        unsafe { nx_batch_builder_free(self.0) };
    }
}

/// One reference to a batch, released when dropped.
struct BatchReference(*mut Batch);

// SAFETY: a batch is immutable and its references are counted atomically, so a reference may be
// released on any thread.
unsafe impl Send for BatchReference {}

impl Drop for BatchReference {
    fn drop(&mut self) {
        // SAFETY: this reference is live and released exactly once, here.
        unsafe { nx_batch_release(self.0) };
    }
}

impl BatchReference {
    fn retain(&self) -> Self {
        // SAFETY: this reference is live, and the binding returns a new one.
        Self(unsafe { nx_batch_retain(self.0) })
    }

    /// Builds a batch of `rows` for `schema`, writing over every buffer once the binding has
    /// copied it.
    fn build(schema: &SchemaHandle, rows: &[Row]) -> io::Result<Self> {
        let mut builder = ptr::null_mut();
        // SAFETY: the schema is live and `builder` is writable.
        check(unsafe { nx_batch_builder_new(schema.0, rows.len(), &mut builder) })?;
        let builder = OwnedBuilder(builder);
        for (index, field) in schema.fields()?.iter().enumerate() {
            let mut host = column(&field.name, rows)?;
            if let Some(states) = host.states.as_mut() {
                // SAFETY: the builder is live and used by this thread only, and the states
                // address their length in readable bytes.
                check(unsafe {
                    nx_batch_builder_states(builder.0, index, states.as_ptr(), states.len())
                })?;
                states.fill(SCRIBBLE);
            }
            for level in &mut host.offsets {
                // SAFETY: as above, for the level's offsets.
                check(unsafe {
                    nx_batch_builder_offsets(
                        builder.0,
                        index,
                        level.level,
                        level.offsets.as_ptr(),
                        level.offsets.len(),
                    )
                })?;
                level.offsets.fill(u64::MAX);
            }
            match &mut host.values {
                HostValues::Fixed(values) => {
                    // SAFETY: as above, for the values.
                    check(unsafe {
                        nx_batch_builder_fixed(
                            builder.0,
                            index,
                            host.leaf,
                            values.as_ptr().cast(),
                            values.len(),
                        )
                    })?;
                    values.fill(SCRIBBLE);
                }
                HostValues::Varlen { offsets, data } => {
                    // SAFETY: as above, for the offsets and the data.
                    check(unsafe {
                        nx_batch_builder_varlen(
                            builder.0,
                            index,
                            host.leaf,
                            offsets.as_ptr(),
                            offsets.len(),
                            data.as_ptr(),
                            data.len(),
                        )
                    })?;
                    offsets.fill(u64::MAX);
                    data.fill(SCRIBBLE);
                }
            }
        }
        let mut batch = ptr::null_mut();
        // SAFETY: the builder is live and used by this thread only, and `batch` is writable.
        check(unsafe { nx_batch_builder_finish(builder.0, &mut batch) })?;
        Ok(Self(batch))
    }

    fn ipc(&self) -> io::Result<Vec<u8>> {
        let mut ipc = ptr::null();
        let mut ipc_len = 0;
        // SAFETY: the batch is live and the stream is copied before the reference is released.
        unsafe {
            check(nx_batch_ipc(self.0, &mut ipc, &mut ipc_len))?;
            Ok(copied(ipc, ipc_len))
        }
    }

    fn cells(&self, column: usize, level: usize) -> io::Result<usize> {
        let mut cells = 0;
        // SAFETY: the batch is live and `cells` is writable.
        check(unsafe { nx_batch_cells(self.0, column, level, &mut cells) })?;
        Ok(cells)
    }

    /// Reads one column in one call per level of its type.
    fn column(&self, index: usize, field: &ReportedField) -> io::Result<ColumnView> {
        let rows = self.cells(index, 0)?;
        let mut states = vec![0_u8; rows];
        // SAFETY: the batch is live and `states` holds one byte per row.
        check(unsafe { nx_batch_states(self.0, index, states.as_mut_ptr(), states.len()) })?;
        let mut lists = Vec::new();
        let innermost = field.levels.len() - 1;
        for (level, level_type) in field.levels.iter().enumerate().take(innermost) {
            match level_type.ty {
                FieldType::List => {
                    let cells = self.cells(index, level)?;
                    let mut offsets = vec![0_u64; cells + 1];
                    // SAFETY: the batch is live and `offsets` holds one entry per list and an end.
                    check(unsafe {
                        nx_batch_offsets(self.0, index, level, offsets.as_mut_ptr(), offsets.len())
                    })?;
                    lists.push(ListView::Variable(offsets));
                }
                _ => lists.push(ListView::Fixed(
                    usize::try_from(level_type.length).expect("a probe list is short"),
                )),
            }
        }
        let leaf_type = field.levels[innermost].ty;
        let cells = self.cells(index, innermost)?;
        let values = match leaf_type {
            FieldType::String | FieldType::Bytes => {
                let mut offsets = vec![0_u64; cells + 1];
                let mut data_len = 0;
                // SAFETY: the batch is live; a null data buffer asks only for the length.
                check(unsafe {
                    nx_batch_varlen(
                        self.0,
                        index,
                        innermost,
                        offsets.as_mut_ptr(),
                        offsets.len(),
                        ptr::null_mut(),
                        0,
                        &mut data_len,
                    )
                })?;
                let mut data = vec![0_u8; data_len];
                // SAFETY: the batch is live, and `offsets` and `data` hold what the level needs.
                check(unsafe {
                    nx_batch_varlen(
                        self.0,
                        index,
                        innermost,
                        offsets.as_mut_ptr(),
                        offsets.len(),
                        data.as_mut_ptr(),
                        data.len(),
                        &mut data_len,
                    )
                })?;
                LeafView::Varlen {
                    ty: leaf_type,
                    offsets,
                    data,
                }
            }
            _ => {
                let mut values = vec![0_u8; cells * width(leaf_type)?];
                // SAFETY: the batch is live and `values` holds one width per cell.
                check(unsafe {
                    nx_batch_fixed(
                        self.0,
                        index,
                        innermost,
                        values.as_mut_ptr().cast(),
                        values.len(),
                    )
                })?;
                LeafView::Fixed {
                    ty: leaf_type,
                    values,
                }
            }
        };
        Ok(ColumnView {
            states,
            lists,
            values,
        })
    }

    /// Every row of the batch rendered as a report line, with `fields` in their order.
    fn rows(&self, fields: &[ReportedField]) -> io::Result<Vec<String>> {
        // SAFETY: the batch is live.
        let count = unsafe { nx_batch_row_count(self.0) };
        let mut columns = Vec::with_capacity(fields.len());
        for (index, field) in fields.iter().enumerate() {
            columns.push(self.column(index, field)?);
        }
        let mut rows = Vec::with_capacity(count);
        for row in 0..count {
            let mut line = String::from("ROW");
            for (field, column) in fields.iter().zip(&columns) {
                line.push_str(&format!(" {}={}", field.name, column.render(row)?));
            }
            rows.push(line);
        }
        Ok(rows)
    }

    /// The `id` value of every row, read through the column accessors.
    fn ids(&self, fields: &[ReportedField]) -> io::Result<Vec<u32>> {
        let index = fields
            .iter()
            .position(|field| field.name == "id")
            .ok_or_else(|| io::Error::other("the batch has no id column"))?;
        let column = self.column(index, &fields[index])?;
        let LeafView::Fixed { values, .. } = column.values else {
            return Err(io::Error::other("the id column is not fixed-width"));
        };
        let (words, _) = values.as_chunks::<4>();
        Ok(words.iter().map(|word| u32::from_ne_bytes(*word)).collect())
    }
}

/// One column as the binding copied it out.
struct ColumnView {
    states: Vec<u8>,
    lists: Vec<ListView>,
    values: LeafView,
}

enum ListView {
    Variable(Vec<u64>),
    Fixed(usize),
}

enum LeafView {
    Fixed {
        ty: FieldType,
        values: Vec<u8>,
    },
    Varlen {
        ty: FieldType,
        offsets: Vec<u64>,
        data: Vec<u8>,
    },
}

impl ColumnView {
    fn render(&self, row: usize) -> io::Result<String> {
        match self.states.get(row) {
            Some(1) => self.render_cell(0, row),
            Some(2) => Ok("null".to_string()),
            _ => Err(io::Error::other("a row state is neither a value nor null")),
        }
    }

    fn render_cell(&self, level: usize, index: usize) -> io::Result<String> {
        let Some(list) = self.lists.get(level) else {
            return self.values.render(index);
        };
        let range = match list {
            ListView::Variable(offsets) => {
                let start = usize::try_from(offsets[index]).expect("a probe offset is small");
                let end = usize::try_from(offsets[index + 1]).expect("a probe offset is small");
                start..end
            }
            ListView::Fixed(length) => index * length..(index + 1) * length,
        };
        let mut elements = Vec::with_capacity(range.len());
        for element in range {
            elements.push(self.render_cell(level + 1, element)?);
        }
        Ok(format!("[{}]", elements.join(",")))
    }
}

impl LeafView {
    fn render(&self, index: usize) -> io::Result<String> {
        match self {
            Self::Fixed { ty, values } => {
                let width = width(*ty)?;
                let bytes = &values[index * width..(index + 1) * width];
                Ok(match ty {
                    FieldType::U8 => format!("u8:{}", bytes[0]),
                    FieldType::I8 => format!("i8:{}", i8::from_ne_bytes([bytes[0]])),
                    FieldType::Bool => format!("bool:{}", bytes[0] == 1),
                    FieldType::U16 => format!(
                        "u16:{}",
                        u16::from_ne_bytes(bytes.try_into().expect("two bytes"))
                    ),
                    FieldType::I16 => format!(
                        "i16:{}",
                        i16::from_ne_bytes(bytes.try_into().expect("two bytes"))
                    ),
                    FieldType::U32 => format!(
                        "u32:{}",
                        u32::from_ne_bytes(bytes.try_into().expect("four bytes"))
                    ),
                    FieldType::I32 => format!(
                        "i32:{}",
                        i32::from_ne_bytes(bytes.try_into().expect("four bytes"))
                    ),
                    FieldType::F32 => format!(
                        "f32:{:08x}",
                        u32::from_ne_bytes(bytes.try_into().expect("four bytes"))
                    ),
                    FieldType::U64 => format!(
                        "u64:{}",
                        u64::from_ne_bytes(bytes.try_into().expect("eight bytes"))
                    ),
                    FieldType::I64 => format!(
                        "i64:{}",
                        i64::from_ne_bytes(bytes.try_into().expect("eight bytes"))
                    ),
                    FieldType::F64 => format!(
                        "f64:{:016x}",
                        u64::from_ne_bytes(bytes.try_into().expect("eight bytes"))
                    ),
                    FieldType::Datetime => format!(
                        "datetime:{}",
                        i64::from_ne_bytes(bytes.try_into().expect("eight bytes"))
                    ),
                    other => return Err(io::Error::other(format!("{other:?} is not fixed-width"))),
                })
            }
            Self::Varlen { ty, offsets, data } => {
                let start = usize::try_from(offsets[index]).expect("a probe offset is small");
                let end = usize::try_from(offsets[index + 1]).expect("a probe offset is small");
                let prefix = match ty {
                    FieldType::String => "str",
                    _ => "bytes",
                };
                Ok(format!("{prefix}:{}", hex(&data[start..end])))
            }
        }
    }
}

/// A producer, freed when dropped.
struct OwnedProducer(*mut Producer);

impl Drop for OwnedProducer {
    fn drop(&mut self) {
        // SAFETY: the producer is live and released exactly once, here.
        unsafe { nx_producer_free(self.0) };
    }
}

/// A submission outcome, freed when dropped.
struct OwnedSubmission(*mut SubmissionOutcome);

impl Drop for OwnedSubmission {
    fn drop(&mut self) {
        // SAFETY: the outcome is live and released exactly once, here.
        unsafe { nx_submission_outcome_free(self.0) };
    }
}

impl OwnedSubmission {
    /// The outcome as the report prints it.
    fn text(&self) -> io::Result<String> {
        // SAFETY: the outcome is live, and every out-parameter is writable.
        unsafe {
            let result = nx_submission_outcome_result(self.0);
            Ok(match result {
                SubmissionResult::Completed => "completed".to_string(),
                SubmissionResult::NotAdmitted => {
                    let mut refusal = nervix_client_ffi::SubmissionRefusal::Busy;
                    check(nx_submission_outcome_refusal(self.0, &mut refusal))?;
                    let mut text = format!("not_admitted {}", snake(&format!("{refusal:?}")));
                    if let nervix_client_ffi::SubmissionRefusal::InvalidBatch = refusal {
                        let mut defect = nervix_client_ffi::BatchDefect::Malformed;
                        check(nx_submission_outcome_defect(self.0, &mut defect))?;
                        text.push_str(&format!(" {}", snake(&format!("{defect:?}"))));
                    }
                    text
                }
                SubmissionResult::ProcessingFailed => {
                    let mut failure = nervix_client_ffi::ProcessingFailure::Rejected;
                    check(nx_submission_outcome_failure(self.0, &mut failure))?;
                    format!("processing_failed {}", snake(&format!("{failure:?}")))
                }
                SubmissionResult::OutcomeUnknown => {
                    let mut cause = nervix_client_ffi::Uncertainty::Interrupted;
                    check(nx_submission_outcome_uncertainty(self.0, &mut cause))?;
                    format!("outcome_unknown {}", snake(&format!("{cause:?}")))
                }
            })
        }
    }
}

/// A Rust variant name in the report's snake case.
fn snake(name: &str) -> String {
    let mut text = String::new();
    for (index, character) in name.chars().enumerate() {
        if character.is_ascii_uppercase() {
            if index > 0 {
                text.push('_');
            }
            text.push(character.to_ascii_lowercase());
        } else {
            text.push(character);
        }
    }
    text
}

impl OwnedProducer {
    fn open(
        session: &OpenSession,
        domain: &str,
        ingestor: &str,
        fields: &OwnedFields,
        batches: u32,
        bytes: u64,
    ) -> Result<Self, *mut nervix_client_ffi::Failure> {
        let mut producer = ptr::null_mut();
        // SAFETY: the session and the fields are live, the names address their lengths, and
        // `producer` is writable.
        let failure = unsafe {
            nx_session_open_ingestor(
                session.0,
                domain.as_ptr(),
                domain.len(),
                ingestor.as_ptr(),
                ingestor.len(),
                fields.0,
                batches,
                bytes,
                ptr::null(),
                &mut producer,
            )
        };
        if failure.is_null() {
            return Ok(Self(producer));
        }
        Err(failure)
    }

    fn schema(&self) -> io::Result<SchemaHandle> {
        let mut schema = ptr::null_mut();
        // SAFETY: the producer is live and `schema` is writable.
        check(unsafe { nx_producer_schema(self.0, &mut schema) })?;
        Ok(SchemaHandle(schema))
    }

    fn submit(
        &self,
        batch: &BatchReference,
        token: Option<&Token>,
    ) -> Result<u64, *mut nervix_client_ffi::Failure> {
        let mut submission = 0;
        let cancel = token.map_or(ptr::null(), |token| token.0.cast_const());
        // SAFETY: the producer and the batch are live, the token is live or null, and
        // `submission` is writable.
        let failure = unsafe { nx_producer_submit(self.0, batch.0, cancel, &mut submission) };
        if failure.is_null() {
            return Ok(submission);
        }
        Err(failure)
    }

    fn rejoin(
        &self,
        submission: u64,
        token: Option<&Token>,
    ) -> Result<OwnedSubmission, *mut nervix_client_ffi::Failure> {
        let mut outcome = ptr::null_mut();
        let cancel = token.map_or(ptr::null(), |token| token.0.cast_const());
        // SAFETY: the producer is live, the token is live or null, and `outcome` is writable.
        let failure = unsafe { nx_producer_rejoin(self.0, submission, cancel, &mut outcome) };
        if failure.is_null() {
            return Ok(OwnedSubmission(outcome));
        }
        Err(failure)
    }

    fn outcome(&self, submission: u64) -> io::Result<String> {
        let token = Token::with_deadline(WAIT_MILLIS)?;
        match self.rejoin(submission, Some(&token)) {
            Ok(outcome) => outcome.text(),
            Err(failure) => Err(failed(failure)),
        }
    }

    /// The submissions the producer holds, with whether each has its outcome.
    fn pending(&self) -> io::Result<Vec<(u64, bool)>> {
        let mut count = 0;
        // SAFETY: the producer is live; null buffers ask only for the count.
        check(unsafe {
            nx_producer_pending(self.0, ptr::null_mut(), ptr::null_mut(), 0, &mut count)
        })?;
        let mut submissions = vec![0_u64; count];
        let mut resolved = vec![false; count];
        // SAFETY: the producer is live and both buffers hold `count` entries.
        check(unsafe {
            nx_producer_pending(
                self.0,
                submissions.as_mut_ptr(),
                resolved.as_mut_ptr(),
                count,
                &mut count,
            )
        })?;
        Ok(submissions.into_iter().zip(resolved).take(count).collect())
    }

    fn state(&self) -> EndpointState {
        // SAFETY: the producer is live.
        unsafe { nx_producer_state(self.0) }
    }

    fn close(&self) -> io::Result<()> {
        let token = Token::with_deadline(WAIT_MILLIS)?;
        // SAFETY: the producer and the token are live.
        check(unsafe { nx_producer_close(self.0, token.0) })
    }
}

/// A consumer, freed when dropped.
struct OwnedConsumer(*mut Consumer);

// SAFETY: the binding allows a consumer to be used from several threads at once.
unsafe impl Send for OwnedConsumer {}
// SAFETY: as above.
unsafe impl Sync for OwnedConsumer {}

impl Drop for OwnedConsumer {
    fn drop(&mut self) {
        // SAFETY: the consumer is live and released exactly once, here.
        unsafe { nx_consumer_free(self.0) };
    }
}

/// One reference to a delivery, released when dropped.
struct DeliveryReference(*mut Delivery);

// SAFETY: a delivery's references are counted atomically, and the binding allows it to be settled
// and released from any thread.
unsafe impl Send for DeliveryReference {}

impl Drop for DeliveryReference {
    fn drop(&mut self) {
        // SAFETY: this reference is live and released exactly once, here.
        unsafe { nx_delivery_release(self.0) };
    }
}

impl OwnedConsumer {
    fn open(
        session: &OpenSession,
        domain: &str,
        emitter: &str,
        fields: &OwnedFields,
    ) -> Result<Self, *mut nervix_client_ffi::Failure> {
        let mut consumer = ptr::null_mut();
        // SAFETY: the session and the fields are live, the names address their lengths, and
        // `consumer` is writable.
        let failure = unsafe {
            nx_session_subscribe_emitter(
                session.0,
                domain.as_ptr(),
                domain.len(),
                emitter.as_ptr(),
                emitter.len(),
                fields.0,
                CONSUMER_BATCHES,
                ENDPOINT_BYTES,
                ptr::null(),
                &mut consumer,
            )
        };
        if failure.is_null() {
            return Ok(Self(consumer));
        }
        Err(failure)
    }

    fn schema(&self) -> io::Result<SchemaHandle> {
        let mut schema = ptr::null_mut();
        // SAFETY: the consumer is live and `schema` is writable.
        check(unsafe { nx_consumer_schema(self.0, &mut schema) })?;
        Ok(SchemaHandle(schema))
    }

    fn next_with(
        &self,
        token: &Token,
    ) -> Result<DeliveryReference, *mut nervix_client_ffi::Failure> {
        let mut delivery = ptr::null_mut();
        // SAFETY: the consumer and the token are live and `delivery` is writable.
        let failure = unsafe { nx_consumer_next(self.0, token.0, &mut delivery) };
        if failure.is_null() {
            return Ok(DeliveryReference(delivery));
        }
        Err(failure)
    }

    /// The next delivery, which has to arrive within the probe's wait.
    fn next(&self) -> io::Result<DeliveryReference> {
        let token = Token::with_deadline(WAIT_MILLIS)?;
        self.next_with(&token).map_err(failed)
    }

    fn state(&self) -> EndpointState {
        // SAFETY: the consumer is live.
        unsafe { nx_consumer_state(self.0) }
    }

    fn close(&self) -> io::Result<()> {
        let token = Token::with_deadline(WAIT_MILLIS)?;
        // SAFETY: the consumer and the token are live.
        check(unsafe { nx_consumer_close(self.0, token.0) })
    }
}

impl DeliveryReference {
    fn retain(&self) -> Self {
        // SAFETY: this reference is live, and the binding returns a new one.
        Self(unsafe { nx_delivery_retain(self.0) })
    }

    fn identity(&self) -> Vec<u8> {
        let mut identity = ptr::null();
        let mut identity_len = 0;
        // SAFETY: the delivery is live and the identity is copied before it can be released.
        unsafe {
            nx_delivery_identity(self.0, &mut identity, &mut identity_len);
            copied(identity, identity_len)
        }
    }

    fn reference(&self) -> Vec<u8> {
        let mut reference = ptr::null();
        let mut reference_len = 0;
        // SAFETY: the delivery is live and the reference is copied before it can be released.
        unsafe {
            nx_delivery_reference(self.0, &mut reference, &mut reference_len);
            copied(reference, reference_len)
        }
    }

    fn ipc(&self) -> Vec<u8> {
        let mut ipc = ptr::null();
        let mut ipc_len = 0;
        // SAFETY: the delivery is live and the stream is copied before it can be released.
        unsafe {
            nx_delivery_ipc(self.0, &mut ipc, &mut ipc_len);
            copied(ipc, ipc_len)
        }
    }

    fn summary(&self) -> io::Result<String> {
        let mut relay = ptr::null();
        let mut relay_len = 0;
        let mut fingerprint = ptr::null();
        let mut fingerprint_len = 0;
        // SAFETY: the delivery is live, the out-parameters are writable, and the relay name is
        // copied before the delivery can be released.
        let (relay, branch, members) = unsafe {
            nx_delivery_source_relay(self.0, &mut relay, &mut relay_len);
            let branched =
                nx_delivery_branch_fingerprint(self.0, &mut fingerprint, &mut fingerprint_len);
            let relay = String::from_utf8(copied(relay, relay_len))
                .map_err(|_| io::Error::other("a relay name is not UTF-8"))?;
            let branch = if branched {
                fingerprint_len.to_string()
            } else {
                "none".to_string()
            };
            (relay, branch, nx_delivery_members(self.0))
        };
        Ok(format!(
            "DELIVERY relay={relay} members={members} branch={branch} identity={} reference={}",
            self.identity().len(),
            self.reference().len()
        ))
    }

    fn batch(&self) -> io::Result<BatchReference> {
        let mut batch = ptr::null_mut();
        // SAFETY: the delivery is live and `batch` is writable.
        check(unsafe { nx_delivery_batch(self.0, &mut batch) })?;
        Ok(BatchReference(batch))
    }

    fn settle(
        &self,
        settle: unsafe extern "C" fn(
            *const Delivery,
            *const nervix_client_ffi::Cancel,
            *mut Settlement,
        ) -> *mut nervix_client_ffi::Failure,
    ) -> Result<Settlement, *mut nervix_client_ffi::Failure> {
        let token = Token::with_deadline(WAIT_MILLIS).expect("a deadline within a day is valid");
        let mut settlement = Settlement::ConsumerEnded;
        // SAFETY: the delivery and the token are live and `settlement` is writable.
        let failure = unsafe { settle(self.0, token.0, &mut settlement) };
        if failure.is_null() {
            return Ok(settlement);
        }
        Err(failure)
    }

    fn ack(&self) -> Result<Settlement, *mut nervix_client_ffi::Failure> {
        self.settle(nx_delivery_ack)
    }

    fn retry(&self) -> Result<Settlement, *mut nervix_client_ffi::Failure> {
        self.settle(nx_delivery_retry)
    }

    fn reject(&self, reason: &str) -> Result<Settlement, *mut nervix_client_ffi::Failure> {
        let token = Token::with_deadline(WAIT_MILLIS).expect("a deadline within a day is valid");
        let mut settlement = Settlement::ConsumerEnded;
        // SAFETY: the delivery and the token are live, the reason addresses its length, and
        // `settlement` is writable.
        let failure = unsafe {
            nx_delivery_reject(
                self.0,
                reason.as_ptr(),
                reason.len(),
                token.0,
                &mut settlement,
            )
        };
        if failure.is_null() {
            return Ok(settlement);
        }
        Err(failure)
    }
}

/// A settlement as the report prints it.
fn settled(settlement: Result<Settlement, *mut nervix_client_ffi::Failure>) -> io::Result<String> {
    match settlement {
        Ok(settlement) => Ok(snake(&format!("{settlement:?}"))),
        Err(failure) => Err(failed(failure)),
    }
}

/// The refusal an open failed with, as the report prints it.
fn refused(failure: *mut nervix_client_ffi::Failure) -> io::Result<String> {
    // SAFETY: the failure is live and released exactly once, here.
    unsafe {
        let mut refusal = OpenRefusal::DomainNotFound;
        let refused = nx_error_open_refusal(failure, &mut refusal);
        let kind = nx_error_kind_of(failure);
        nx_error_free(failure);
        if !refused || kind != FailureKind::Rejected {
            return Err(io::Error::other(format!(
                "an open failed with {kind:?} and no refusal"
            )));
        }
        Ok(snake(&format!("{refusal:?}")).replace('_', " "))
    }
}

fn state_name(state: EndpointState) -> String {
    snake(&format!("{state:?}"))
}

/// The probe's own checks, which fail it rather than print.
struct Checks {
    failures: Vec<String>,
}

impl Checks {
    fn expect(&mut self, holds: bool, what: &str) {
        if !holds {
            self.failures.push(what.to_string());
        }
    }

    fn finish(self) -> io::Result<()> {
        if self.failures.is_empty() {
            return Ok(());
        }
        Err(io::Error::other(format!(
            "the probe's checks failed: {}",
            self.failures.join("; ")
        )))
    }
}

/// Waits until `state` no longer reads `from`, within the probe's wait.
fn wait_for_state_change(
    what: &str,
    from: EndpointState,
    state: impl Fn() -> EndpointState,
) -> io::Result<EndpointState> {
    let deadline = Instant::now() + Duration::from_millis(WAIT_MILLIS);
    loop {
        let current = state();
        if current != from {
            return Ok(current);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!("the {what} stayed {from:?}")));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

/// Publishes typed batches through `ingestor` and settles their output from `emitter`.
pub(super) fn run(
    session: &OpenSession,
    domain: &str,
    ingestor: &str,
    emitter: &str,
    emit: &mut Emit<'_>,
) -> io::Result<()> {
    let mut checks = Checks {
        failures: Vec::new(),
    };
    let input_fields = OwnedFields::of(INPUT_FIELDS)?;
    let output_fields = OwnedFields::of(INPUT_FIELDS.iter().chain([&ECHO_FIELD]))?;

    // An open whose expected fields differ from the endpoint's, here only in the secret's
    // sensitivity, is refused exactly.
    let mismatched = OwnedFields::of(INPUT_FIELDS.iter().map(|field| {
        if field.name == PUBLIC_SECRET.name {
            &PUBLIC_SECRET
        } else {
            field
        }
    }))?;
    match OwnedProducer::open(
        session,
        domain,
        ingestor,
        &mismatched,
        PRODUCER_BATCHES,
        ENDPOINT_BYTES,
    ) {
        Ok(_) => return Err(io::Error::other("an open with another schema succeeded")),
        Err(failure) => emit(&format!("REFUSED producer {}", refused(failure)?))?,
    }
    match OwnedConsumer::open(session, domain, emitter, &input_fields) {
        Ok(_) => return Err(io::Error::other("an open with another schema succeeded")),
        Err(failure) => emit(&format!("REFUSED consumer {}", refused(failure)?))?,
    }

    let consumer = OwnedConsumer::open(session, domain, emitter, &output_fields).map_err(failed)?;
    let producer = OwnedProducer::open(
        session,
        domain,
        ingestor,
        &input_fields,
        PRODUCER_BATCHES,
        ENDPOINT_BYTES,
    )
    .map_err(failed)?;
    let output_schema = consumer.schema()?;
    let input_schema = producer.schema()?;
    let output = output_schema.fields()?;
    let input = input_schema.fields()?;

    // SAFETY: the consumer is live and every out-parameter is writable.
    unsafe {
        let mut window = WindowKind::Parallel;
        let mut outstanding = 0;
        let mut ack_timeout = 0;
        let mut backoff = 0;
        let mut max_backoff = 0;
        nx_consumer_policy(
            consumer.0,
            &mut window,
            &mut outstanding,
            &mut ack_timeout,
            &mut backoff,
            &mut max_backoff,
        );
        let mut batches = 0;
        let mut bytes = 0;
        let mut max_rows = 0;
        let mut max_bytes = 0;
        nx_consumer_grant(
            consumer.0,
            &mut batches,
            &mut bytes,
            &mut max_rows,
            &mut max_bytes,
        );
        emit(&format!(
            "CONSUMER generation={} state={} window={}/{outstanding} ack_timeout={ack_timeout} \
             credit={batches}/{bytes} max={max_rows}/{max_bytes}",
            nx_consumer_generation(consumer.0),
            state_name(consumer.state()),
            snake(&format!("{window:?}")),
        ))?;
    }
    for field in &output {
        emit(&format!("OUTPUT FIELD {}", field.line()))?;
    }
    // SAFETY: the producer is live and every out-parameter is writable.
    unsafe {
        let mut window = WindowKind::Parallel;
        let mut outstanding = 0;
        let mut ack_timeout = 0;
        let mut backoff = 0;
        let mut max_backoff = 0;
        nx_producer_policy(
            producer.0,
            &mut window,
            &mut outstanding,
            &mut ack_timeout,
            &mut backoff,
            &mut max_backoff,
        );
        let mut batches = 0;
        let mut bytes = 0;
        let mut max_rows = 0;
        let mut max_bytes = 0;
        nx_producer_grant(
            producer.0,
            &mut batches,
            &mut bytes,
            &mut max_rows,
            &mut max_bytes,
        );
        checks.expect(
            max_rows > 0 && max_bytes > 0 && max_bytes <= bytes,
            "the producer's batch limits fit its grant",
        );
        emit(&format!(
            "PRODUCER generation={} state={} admission={} window={}/{outstanding} \
             ack_timeout={ack_timeout} credit={batches}/{bytes}",
            nx_producer_generation(producer.0),
            state_name(producer.state()),
            snake(&format!("{:?}", nx_producer_admission(producer.0))),
            snake(&format!("{window:?}")),
        ))?;
    }
    for field in &input {
        emit(&format!("INPUT FIELD {}", field.line()))?;
    }
    emit("OPENED")?;

    // A wait for output that nothing produces ends by its deadline, and one cancelled from
    // another thread by its token; the reads they leave behind are the consumer's.
    let expiring = Token::with_deadline(EXPIRING_MILLIS)?;
    match consumer.next_with(&expiring) {
        Ok(_) => return Err(io::Error::other("a read of no output returned a delivery")),
        Err(failure) => {
            let kind = expect_failure(failure)?;
            checks.expect(
                kind == FailureKind::Deadline,
                "an expired read reports its deadline",
            );
            emit("NEXT deadline")?;
        }
    }
    let token = Token::new();
    let cancelled = thread::scope(|scope| -> io::Result<FailureKind> {
        let waiting = scope.spawn(|| match consumer.next_with(&token) {
            Ok(_) => Err(io::Error::other("a cancelled read returned a delivery")),
            Err(failure) => expect_failure(failure),
        });
        thread::sleep(Duration::from_millis(100));
        // SAFETY: the token is live until the scope ends.
        unsafe { nervix_client_ffi::nx_cancel_trigger(token.0) };
        waiting
            .join()
            .map_err(|_| io::Error::other("the reading thread panicked"))?
    })?;
    checks.expect(
        cancelled == FailureKind::Cancelled,
        "a cancelled read reports its cancellation",
    );
    emit("NEXT cancelled")?;

    // A batch built for another schema is refused before anything is sent, and the same batch
    // written as a stream by other tooling is refused by the server.
    let rows = typed_rows();
    let other = BatchReference::build(&output_schema, &rows)?;
    match producer.submit(&other, None) {
        Ok(_) => return Err(io::Error::other("a batch of another schema was submitted")),
        Err(failure) => {
            let kind = expect_failure(failure)?;
            checks.expect(
                kind == FailureKind::InvalidArgument,
                "another schema is the host's argument",
            );
            emit("SUBMIT invalid argument")?;
        }
    }
    let foreign = other.ipc()?;
    let mut raw = 0;
    // SAFETY: the producer is live, the stream addresses its length, and `raw` is writable.
    check(unsafe {
        nx_producer_submit_ipc(
            producer.0,
            foreign.as_ptr(),
            foreign.len(),
            ptr::null(),
            &mut raw,
        )
    })?;
    emit(&format!("OUTCOME {}", producer.outcome(raw)?))?;

    // The typed batch: its outcome waits for the application's acknowledgement.
    let first_batch = BatchReference::build(&input_schema, &rows)?;
    let first = producer.submit(&first_batch, None).map_err(failed)?;
    drop(first_batch);
    emit("SUBMITTED first")?;
    let delivery = consumer.next()?;
    let summary = delivery.summary()?;
    emit(&summary)?;
    let pending = producer.pending()?;
    checks.expect(
        pending == vec![(first, false)],
        "the only submission waits for its outcome",
    );
    let expiring = Token::with_deadline(EXPIRING_MILLIS)?;
    match producer.rejoin(first, Some(&expiring)) {
        Ok(_) => {
            return Err(io::Error::other(
                "a submission completed before its output was acknowledged",
            ));
        }
        Err(failure) => {
            let kind = expect_failure(failure)?;
            checks.expect(
                kind == FailureKind::Deadline,
                "an unacknowledged submission has no outcome",
            );
            emit("PENDING first unresolved")?;
        }
    }
    let delivered = delivery.batch()?;
    let printed = delivered.rows(&output)?;
    for line in &printed {
        emit(line)?;
    }
    checks.expect(
        delivered.ipc()? == delivery.ipc(),
        "the batch borrows the stream its delivery carried",
    );

    // A retried attempt comes back with the same identity and a new reference; the first
    // reference is stale from then on.
    emit(&format!("RETRY {}", settled(delivery.retry())?))?;
    let again = consumer.next()?;
    checks.expect(
        again.identity() == delivery.identity(),
        "a retry keeps the identity",
    );
    checks.expect(
        again.reference() != delivery.reference(),
        "a retry makes a new reference",
    );
    emit("REDELIVERED same identity new reference")?;
    emit(&format!("ACK {}", settled(delivery.ack())?))?;
    drop(delivery);

    // A reference retained here outlives the first, released on another thread, and reads and
    // settles the same attempt.
    let retained = again.retain();
    let retained_batch = again.batch()?.retain();
    let stream = retained.ipc();
    let releasing = thread::spawn(move || drop(again));
    releasing
        .join()
        .map_err(|_| io::Error::other("the releasing thread panicked"))?;
    checks.expect(
        retained.ipc() == stream,
        "a retained delivery keeps its stream",
    );
    checks.expect(
        retained_batch.rows(&output)? == printed,
        "a retained batch reads the same rows",
    );
    emit(&format!("ACK {}", settled(retained.ack())?))?;
    drop(retained);
    drop(retained_batch);
    emit(&format!("OUTCOME {}", producer.outcome(first)?))?;

    // An application rejection finishes the batch through the emitter's message error policy.
    let second_batch = BatchReference::build(&input_schema, &[Row::plain(10, "beta")])?;
    let second = producer.submit(&second_batch, None).map_err(failed)?;
    emit("SUBMITTED second")?;
    let rejected = consumer.next()?;
    emit(&format!(
        "REJECT {}",
        settled(rejected.reject("application refused"))?
    ))?;
    drop(rejected);
    emit(&format!("OUTCOME {}", producer.outcome(second)?))?;

    // Two outstanding batches use up the producer's credit, so a third waits for an outcome.
    let third_batch = BatchReference::build(
        &input_schema,
        &[Row::plain(20, "acme"), Row::plain(21, "acme")],
    )?;
    let fourth_batch = BatchReference::build(
        &input_schema,
        &[Row::plain(22, "acme"), Row::plain(23, "acme")],
    )?;
    let fifth_batch = BatchReference::build(
        &input_schema,
        &[Row::plain(24, "acme"), Row::plain(25, "acme")],
    )?;
    let third = producer.submit(&third_batch, None).map_err(failed)?;
    emit("SUBMITTED third")?;
    let fourth = producer.submit(&fourth_batch, None).map_err(failed)?;
    emit("SUBMITTED fourth")?;
    let expiring = Token::with_deadline(EXPIRING_MILLIS)?;
    match producer.submit(&fifth_batch, Some(&expiring)) {
        Ok(_) => return Err(io::Error::other("a batch beyond the credit was submitted")),
        Err(failure) => {
            let kind = expect_failure(failure)?;
            checks.expect(
                kind == FailureKind::Deadline,
                "a batch beyond the credit waits",
            );
            emit("SUBMIT deadline")?;
        }
    }
    // A batch refused as busy is sent again after the ingestor's backoff, behind the batch
    // submitted after it, so the two outputs may arrive in either order; each keeps its own rows.
    let mut outputs = Vec::new();
    for _ in 0..2 {
        let output_delivery = consumer.next()?;
        outputs.push(output_delivery.batch()?.ids(&output)?);
        emit(&format!("ACK {}", settled(output_delivery.ack())?))?;
    }
    outputs.sort();
    checks.expect(
        outputs == [vec![20, 21], vec![22, 23]],
        "a multiple-row batch keeps its rows",
    );
    emit(&format!("OUTCOME {}", producer.outcome(third)?))?;
    emit(&format!("OUTCOME {}", producer.outcome(fourth)?))?;
    let fifth = producer.submit(&fifth_batch, None).map_err(failed)?;
    emit("SUBMITTED fifth")?;
    let fifth_output = consumer.next()?;
    checks.expect(
        fifth_output.batch()?.ids(&output)? == [24, 25],
        "a multiple-row batch keeps its rows",
    );
    emit(&format!("ACK {}", settled(fifth_output.ack())?))?;
    drop(fifth_output);
    emit(&format!("OUTCOME {}", producer.outcome(fifth)?))?;

    // The scenario cuts the session while one delivery is held unacknowledged.
    let extra =
        OwnedProducer::open(session, domain, ingestor, &input_fields, 1, 65_536).map_err(failed)?;
    emit("PRODUCER extra opened")?;
    let held_batch = BatchReference::build(&input_schema, &[Row::plain(30, "gamma")])?;
    let held = producer.submit(&held_batch, None).map_err(failed)?;
    emit("SUBMITTED held")?;
    let held_delivery = consumer.next()?;
    let held_identity = held_delivery.identity();
    emit("HOLDING")?;
    let waiting = Token::with_deadline(WAIT_MILLIS)?;
    match consumer.next_with(&waiting) {
        Ok(_) => {
            return Err(io::Error::other(
                "a read returned a delivery while one was held",
            ));
        }
        Err(failure) => {
            let kind = expect_failure(failure)?;
            checks.expect(
                kind == FailureKind::Interrupted,
                "a lost session interrupts the consumer",
            );
            emit("NEXT interrupted")?;
        }
    }
    match held_delivery.ack() {
        Ok(settlement) => {
            return Err(io::Error::other(format!(
                "a delivery of a lost session settled as {settlement:?}"
            )));
        }
        Err(failure) => {
            let kind = expect_failure(failure)?;
            checks.expect(
                kind == FailureKind::Rejected,
                "a delivery of a lost session expired",
            );
            emit("ACK expired")?;
        }
    }
    drop(held_delivery);
    emit(&format!("OUTCOME {}", producer.outcome(held)?))?;
    wait_for_state_change("extra producer", EndpointState::Active, || extra.state())?;
    extra.close()?;
    checks.expect(
        extra.state() == EndpointState::Closed,
        "a producer closed during reconnect is closed",
    );
    emit("CLOSED extra producer")?;
    emit("WAITING restore")?;

    // The restored consumer receives the held batch again, and the restored producer publishes.
    let redelivered = consumer.next()?;
    checks.expect(
        redelivered.identity() == held_identity,
        "the held batch keeps its identity",
    );
    emit("REDELIVERED held same identity")?;
    emit(&format!("ACK {}", settled(redelivered.ack())?))?;
    drop(redelivered);
    let sixth_batch = BatchReference::build(&input_schema, &[Row::plain(40, "gamma")])?;
    let sixth = producer.submit(&sixth_batch, None).map_err(failed)?;
    emit("SUBMITTED sixth")?;
    let sixth_output = consumer.next()?;
    emit(&format!("ACK {}", settled(sixth_output.ack())?))?;
    drop(sixth_output);
    emit(&format!("OUTCOME {}", producer.outcome(sixth)?))?;
    emit(&format!(
        "STATE producer={} extra={} consumer={}",
        state_name(producer.state()),
        state_name(extra.state()),
        state_name(consumer.state())
    ))?;
    consumer.close()?;
    producer.close()?;
    checks.expect(
        consumer.state() == EndpointState::Closed && producer.state() == EndpointState::Closed,
        "closed handles read closed",
    );
    emit("CLOSED completed")?;
    checks.finish()?;
    emit("CHECKS ok")?;
    emit("PASS")
}
