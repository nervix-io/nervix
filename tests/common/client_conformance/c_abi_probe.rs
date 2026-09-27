//! The probe of the shared Rust binding's C ABI, run inside the harness's own process.
//!
//! It calls the exported functions exactly as a C host would, through raw pointers and
//! out-parameters, and prints the same report as every other probe. It runs on a blocking thread,
//! because every call of the binding blocks its caller.

use std::{io, ptr, slice, thread, time::Duration};

use nervix_client_ffi::{
    Cancel, CellState, Event, EventKind, Execution, FailureKind, FieldType, Outcome, Part, Schema,
    Session, nx_cancel_free, nx_cancel_new, nx_cancel_trigger, nx_cancel_with_deadline,
    nx_error_execution_reference, nx_error_free, nx_error_kind_of, nx_error_message,
    nx_event_cell_varlen, nx_event_column_fixed, nx_event_column_states, nx_event_column_varlen,
    nx_event_frame, nx_event_kind_of, nx_event_release, nx_event_retain, nx_event_row_count,
    nx_event_schema, nx_event_subscription, nx_execution_free, nx_execution_reference,
    nx_outcome_diagnostic, nx_outcome_diagnostic_count, nx_outcome_disposition,
    nx_outcome_execution_reference, nx_outcome_free, nx_outcome_message, nx_outcome_schema,
    nx_outcome_subscription, nx_schema_branch, nx_schema_field, nx_schema_field_count,
    nx_schema_free, nx_session_connect, nx_session_execute, nx_session_free, nx_session_next_event,
    nx_session_prepare,
};

use super::ProbeTarget;

/// How long the probe waits for the rows it expects.
const ROWS_DEADLINE_MILLIS: u64 = 120_000;

/// A reporting sink: one call per report line.
type Emit<'a> = dyn FnMut(&str) -> io::Result<()> + 'a;

/// Turns a returned failure into an error, releasing it.
fn check(failure: *mut nervix_client_ffi::Failure) -> io::Result<()> {
    if failure.is_null() {
        return Ok(());
    }
    // SAFETY: a non-null failure is a live error the binding just returned, released once here.
    let (kind, message) = unsafe {
        let kind = nx_error_kind_of(failure);
        let mut message = ptr::null();
        let mut message_len = 0;
        nx_error_message(failure, &mut message, &mut message_len);
        let text =
            String::from_utf8_lossy(slice::from_raw_parts(message, message_len)).into_owned();
        nx_error_free(failure);
        (kind, text)
    };
    Err(io::Error::other(format!("{kind:?}: {message}")))
}

/// Reads a failure's kind and reference, releasing it, or reports that the call succeeded.
fn expect_failure(failure: *mut nervix_client_ffi::Failure) -> io::Result<FailureKind> {
    if failure.is_null() {
        return Err(io::Error::other("the call succeeded where it had to fail"));
    }
    // SAFETY: a non-null failure is a live error the binding just returned, released once here.
    unsafe {
        let kind = nx_error_kind_of(failure);
        nx_error_free(failure);
        Ok(kind)
    }
}

/// Copies a borrowed byte string the binding wrote.
///
/// # Safety
///
/// `data` addresses `len` readable bytes.
unsafe fn copied(data: *const u8, len: usize) -> Vec<u8> {
    if len == 0 {
        return Vec::new();
    }
    // SAFETY: the caller guarantees `len` readable bytes.
    unsafe { slice::from_raw_parts(data, len) }.to_vec()
}

fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

/// An open session, freed when dropped.
struct OpenSession(*mut Session);

impl Drop for OpenSession {
    fn drop(&mut self) {
        // SAFETY: the session is live and released exactly once, here.
        unsafe { nx_session_free(self.0) };
    }
}

/// A command outcome, freed when dropped.
struct OwnedOutcome(*mut Outcome);

impl Drop for OwnedOutcome {
    fn drop(&mut self) {
        // SAFETY: the outcome is live and released exactly once, here.
        unsafe { nx_outcome_free(self.0) };
    }
}

/// A schema, freed when dropped.
struct OwnedSchema(*mut Schema);

impl Drop for OwnedSchema {
    fn drop(&mut self) {
        // SAFETY: the schema is live and released exactly once, here.
        unsafe { nx_schema_free(self.0) };
    }
}

/// One reference to an event, released when dropped.
struct EventReference(*mut Event);

// SAFETY: an event is immutable and its references are counted atomically, so a reference may be
// released on any thread.
unsafe impl Send for EventReference {}

impl Drop for EventReference {
    fn drop(&mut self) {
        // SAFETY: this reference is live and released exactly once, here.
        unsafe { nx_event_release(self.0) };
    }
}

impl EventReference {
    fn retain(&self) -> Self {
        // SAFETY: this reference is live, and the binding returns a new one.
        Self(unsafe { nx_event_retain(self.0) })
    }
}

/// A cancellation token, freed when dropped.
struct Token(*mut Cancel);

// SAFETY: a token may be triggered from any thread, which is what the binding promises.
unsafe impl Send for Token {}
// SAFETY: as above; triggering takes a shared reference.
unsafe impl Sync for Token {}

impl Token {
    fn new() -> Self {
        Self(nx_cancel_new())
    }

    fn with_deadline(millis: u64) -> io::Result<Self> {
        let mut token = ptr::null_mut();
        // SAFETY: `token` is writable.
        check(unsafe { nx_cancel_with_deadline(millis, &mut token) })?;
        Ok(Self(token))
    }
}

impl Drop for Token {
    fn drop(&mut self) {
        // SAFETY: the token is live, no call waits on it any more, and it is released once, here.
        unsafe { nx_cancel_free(self.0) };
    }
}

impl OpenSession {
    fn connect(target: &ProbeTarget) -> io::Result<Self> {
        let mut session = ptr::null_mut();
        // SAFETY: every text argument addresses its length in readable bytes, and `session` is
        // writable.
        check(unsafe {
            nx_session_connect(
                target.grpc_uri.as_ptr(),
                target.grpc_uri.len(),
                target.domain.as_ptr(),
                target.domain.len(),
                target.username.as_ptr(),
                target.username.len(),
                target.password.as_ptr(),
                target.password.len(),
                ptr::null(),
                &mut session,
            )
        })?;
        Ok(Self(session))
    }

    /// Prepares and runs one command, returning its outcome.
    fn execute(&self, query: &str) -> io::Result<OwnedOutcome> {
        let mut execution: *mut Execution = ptr::null_mut();
        // SAFETY: the session is live, the query addresses its length, and `execution` is
        // writable.
        check(unsafe {
            nx_session_prepare(
                self.0,
                query.as_ptr(),
                query.len(),
                ptr::null(),
                &mut execution,
            )
        })?;
        let mut outcome = ptr::null_mut();
        // SAFETY: the session and execution are live and `outcome` is writable; the execution
        // is released once, after the call that reads it.
        let result = unsafe {
            let mut reference = ptr::null();
            let mut reference_len = 0;
            nx_execution_reference(execution, &mut reference, &mut reference_len);
            let prepared_reference = copied(reference, reference_len);
            let result = check(nx_session_execute(
                self.0,
                execution,
                ptr::null(),
                &mut outcome,
            ));
            nx_execution_free(execution);
            result.map(|()| prepared_reference)
        };
        let prepared_reference = result?;
        let outcome = OwnedOutcome(outcome);
        if let Some(reference) = outcome.execution_reference()
            && reference != prepared_reference
        {
            return Err(io::Error::other(
                "the outcome names another execution than the one prepared",
            ));
        }
        Ok(outcome)
    }

    fn next_event(&self, token: &Token) -> io::Result<EventReference> {
        let mut event = ptr::null_mut();
        // SAFETY: the session and token are live and `event` is writable.
        check(unsafe { nx_session_next_event(self.0, token.0, &mut event) })?;
        Ok(EventReference(event))
    }
}

impl OwnedOutcome {
    fn disposition(&self) -> String {
        // SAFETY: the outcome is live.
        let disposition = unsafe { nx_outcome_disposition(self.0) };
        let name = format!("{disposition:?}");
        let mut snake = String::new();
        for (index, character) in name.chars().enumerate() {
            if character.is_uppercase() && index > 0 {
                snake.push('_');
            }
            snake.push(character.to_ascii_lowercase());
        }
        snake
    }

    fn message(&self) -> String {
        let mut message = ptr::null();
        let mut message_len = 0;
        // SAFETY: the outcome is live and the out-parameters are writable.
        unsafe {
            nx_outcome_message(self.0, &mut message, &mut message_len);
            String::from_utf8_lossy(&copied(message, message_len)).into_owned()
        }
    }

    fn execution_reference(&self) -> Option<Vec<u8>> {
        let mut reference = ptr::null();
        let mut reference_len = 0;
        // SAFETY: the outcome is live and the out-parameters are writable.
        unsafe {
            if !nx_outcome_execution_reference(self.0, &mut reference, &mut reference_len) {
                return None;
            }
            Some(copied(reference, reference_len))
        }
    }

    /// The report line of a failed command: its disposition, diagnostic count and first span.
    fn error_line(&self) -> io::Result<String> {
        // SAFETY: the outcome is live.
        let count = unsafe { nx_outcome_diagnostic_count(self.0) };
        let mut span = "none".to_string();
        if count > 0 {
            let mut message = ptr::null();
            let mut message_len = 0;
            let mut has_span = false;
            let mut start = 0;
            let mut end = 0;
            // SAFETY: the outcome is live and every out-parameter is writable.
            check(unsafe {
                nx_outcome_diagnostic(
                    self.0,
                    0,
                    &mut message,
                    &mut message_len,
                    &mut has_span,
                    &mut start,
                    &mut end,
                )
            })?;
            if has_span {
                span = format!("{start}..{end}");
            }
        }
        Ok(format!(
            "ERROR {} diagnostics={count} span={span}",
            self.disposition()
        ))
    }

    fn schema(&self) -> io::Result<OwnedSchema> {
        let mut name = ptr::null();
        let mut name_len = 0;
        let mut generation = 0;
        // SAFETY: the outcome is live and the out-parameters are writable.
        let opened =
            unsafe { nx_outcome_subscription(self.0, &mut name, &mut name_len, &mut generation) };
        if !opened || generation == 0 {
            return Err(io::Error::other(
                "the subscribe command opened no subscription",
            ));
        }
        let mut schema = ptr::null_mut();
        // SAFETY: the outcome is live and `schema` is writable.
        check(unsafe { nx_outcome_schema(self.0, &mut schema) })?;
        Ok(OwnedSchema(schema))
    }
}

/// One field of a schema, as the report names it.
#[derive(Debug, Clone)]
struct Field {
    name: String,
    field_type: FieldType,
    nullable: bool,
    sensitive: bool,
}

impl Field {
    fn type_name(&self) -> &'static str {
        match self.field_type {
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

    fn line(&self, prefix: &str) -> String {
        let nullable = if self.nullable {
            "nullable"
        } else {
            "required"
        };
        let sensitive = if self.sensitive {
            "sensitive"
        } else {
            "public"
        };
        format!(
            "{prefix} {} {} {nullable} {sensitive}",
            self.name,
            self.type_name()
        )
    }
}

impl OwnedSchema {
    fn fields(&self, part: Part) -> io::Result<Vec<Field>> {
        let part = part_code(part);
        let mut count = 0;
        // SAFETY: the schema is live and `count` is writable.
        check(unsafe { nx_schema_field_count(self.0, part, &mut count) })?;
        let mut fields = Vec::with_capacity(count);
        for index in 0..count {
            let mut name = ptr::null();
            let mut name_len = 0;
            let mut field_type = FieldType::U8;
            let mut nullable = false;
            let mut sensitive = false;
            // SAFETY: the schema is live and every out-parameter is writable.
            check(unsafe {
                nx_schema_field(
                    self.0,
                    part,
                    index,
                    &mut name,
                    &mut name_len,
                    &mut field_type,
                    &mut nullable,
                    &mut sensitive,
                )
            })?;
            // SAFETY: the binding wrote a borrowed name of `name_len` bytes.
            let name = String::from_utf8_lossy(&unsafe { copied(name, name_len) }).into_owned();
            fields.push(Field {
                name,
                field_type,
                nullable,
                sensitive,
            });
        }
        Ok(fields)
    }

    fn branch(&self) -> Option<String> {
        let mut name = ptr::null();
        let mut name_len = 0;
        // SAFETY: the schema is live and the out-parameters are writable.
        unsafe {
            if !nx_schema_branch(self.0, &mut name, &mut name_len) {
                return None;
            }
            Some(String::from_utf8_lossy(&copied(name, name_len)).into_owned())
        }
    }
}

fn part_code(part: Part) -> i32 {
    match part {
        Part::Rows => 1,
        Part::BranchKey => 2,
    }
}

/// A column copied out of a batch in one call per buffer.
enum Column {
    Fixed { width: usize, values: Vec<u8> },
    Varlen { offsets: Vec<u64>, data: Vec<u8> },
}

impl EventReference {
    fn kind(&self) -> EventKind {
        // SAFETY: the event is live.
        unsafe { nx_event_kind_of(self.0) }
    }

    fn row_count(&self) -> usize {
        // SAFETY: the event is live.
        let count = unsafe { nx_event_row_count(self.0) };
        usize::try_from(count).expect("a batch's row count fits in memory")
    }

    fn frame(&self) -> io::Result<&[u8]> {
        let mut frame = ptr::null();
        let mut frame_len = 0;
        // SAFETY: the event is live and the out-parameters are writable.
        check(unsafe { nx_event_frame(self.0, &mut frame, &mut frame_len) })?;
        // SAFETY: the frame is borrowed from the event, which outlives the returned slice.
        Ok(unsafe { slice::from_raw_parts(frame, frame_len) })
    }

    fn states(&self, part: Part, column: usize, cells: usize) -> io::Result<Vec<CellState>> {
        let mut states = vec![0_u8; cells];
        // SAFETY: the event is live and `states` holds `cells` writable bytes.
        check(unsafe {
            nx_event_column_states(self.0, part_code(part), column, states.as_mut_ptr(), cells)
        })?;
        states
            .into_iter()
            .map(|state| match state {
                1 => Ok(CellState::Value),
                2 => Ok(CellState::Null),
                3 => Ok(CellState::Redacted),
                other => Err(io::Error::other(format!("unknown cell state {other}"))),
            })
            .collect()
    }

    fn column(&self, part: Part, column: usize, field: &Field, cells: usize) -> io::Result<Column> {
        let width = match field.field_type {
            FieldType::U8 | FieldType::I8 | FieldType::Bool => Some(1),
            FieldType::U16 | FieldType::I16 => Some(2),
            FieldType::U32 | FieldType::I32 | FieldType::F32 => Some(4),
            FieldType::U64 | FieldType::I64 | FieldType::F64 | FieldType::Datetime => Some(8),
            FieldType::String | FieldType::Bytes => None,
            FieldType::FixedList | FieldType::List => {
                return Err(io::Error::other("list columns are read from the frame"));
            }
        };
        let part = part_code(part);
        if let Some(width) = width {
            let mut values = vec![0_u8; cells * width];
            // SAFETY: the event is live and `values` holds its length in writable bytes.
            check(unsafe {
                nx_event_column_fixed(
                    self.0,
                    part,
                    column,
                    values.as_mut_ptr().cast(),
                    values.len(),
                )
            })?;
            return Ok(Column::Fixed { width, values });
        }
        let mut data_len = 0;
        // SAFETY: the event is live; a null `data` asks only for the length.
        check(unsafe {
            nx_event_column_varlen(
                self.0,
                part,
                column,
                ptr::null_mut(),
                0,
                ptr::null_mut(),
                0,
                &mut data_len,
            )
        })?;
        let mut offsets = vec![0_u64; cells + 1];
        // A non-null pointer even for an empty column, which a null pointer would only size.
        let mut data = vec![0_u8; data_len.max(1)];
        // SAFETY: the event is live and both buffers hold their lengths in writable entries.
        check(unsafe {
            nx_event_column_varlen(
                self.0,
                part,
                column,
                offsets.as_mut_ptr(),
                offsets.len(),
                data.as_mut_ptr(),
                data.len(),
                &mut data_len,
            )
        })?;
        data.truncate(data_len);
        Ok(Column::Varlen { offsets, data })
    }

    /// Borrows one string or bytes value without copying it.
    fn borrowed(&self, part: Part, row: usize, column: usize) -> io::Result<&[u8]> {
        let mut value = ptr::null();
        let mut value_len = 0;
        // SAFETY: the event is live and the out-parameters are writable.
        check(unsafe {
            nx_event_cell_varlen(
                self.0,
                part_code(part),
                row,
                column,
                &mut value,
                &mut value_len,
            )
        })?;
        if value_len == 0 {
            return Ok(&[]);
        }
        // SAFETY: the value is borrowed from the event's frame, which outlives the slice.
        Ok(unsafe { slice::from_raw_parts(value, value_len) })
    }

    /// Renders every cell of `part`, one list of `name=value` strings per row.
    fn render(&self, part: Part, fields: &[Field], cells: usize) -> io::Result<Vec<Vec<String>>> {
        let mut rows = vec![Vec::with_capacity(fields.len()); cells];
        for (column, field) in fields.iter().enumerate() {
            let states = self.states(part, column, cells)?;
            let values = self.column(part, column, field, cells)?;
            for (row, state) in states.iter().enumerate() {
                let rendered = match state {
                    CellState::Null => "null".to_string(),
                    CellState::Redacted => "redacted".to_string(),
                    CellState::Value => render_value(field, &values, row, self, part, column)?,
                };
                rows[row].push(format!("{}={rendered}", field.name));
            }
        }
        Ok(rows)
    }

    /// The report lines of a rows event.
    fn row_lines(&self, fields: &[Field], key_fields: &[Field]) -> io::Result<Vec<String>> {
        let count = self.row_count();
        let key = if key_fields.is_empty() {
            Vec::new()
        } else {
            self.render(Part::BranchKey, key_fields, 1)?
                .into_iter()
                .next()
                .expect("a branch key renders as one row")
        };
        let rows = self.render(Part::Rows, fields, count)?;
        Ok(rows
            .into_iter()
            .map(|cells| format!("ROW [{}] {}", key.join(" "), cells.join(" ")))
            .collect())
    }
}

fn render_value(
    field: &Field,
    column: &Column,
    row: usize,
    event: &EventReference,
    part: Part,
    index: usize,
) -> io::Result<String> {
    match column {
        Column::Fixed { width, values } => {
            let bytes = &values[row * width..(row + 1) * width];
            let rendered = match field.field_type {
                FieldType::U8 => format!("u8:{}", bytes[0]),
                FieldType::I8 => format!("i8:{}", i8::from_ne_bytes([bytes[0]])),
                FieldType::U16 => format!("u16:{}", u16::from_ne_bytes([bytes[0], bytes[1]])),
                FieldType::I16 => format!("i16:{}", i16::from_ne_bytes([bytes[0], bytes[1]])),
                FieldType::U32 => format!("u32:{}", u32::from_ne_bytes(four(bytes))),
                FieldType::I32 => format!("i32:{}", i32::from_ne_bytes(four(bytes))),
                FieldType::U64 => format!("u64:{}", u64::from_ne_bytes(eight(bytes))),
                FieldType::I64 => format!("i64:{}", i64::from_ne_bytes(eight(bytes))),
                FieldType::F32 => format!("f32:{:08x}", u32::from_ne_bytes(four(bytes))),
                FieldType::F64 => format!("f64:{:016x}", u64::from_ne_bytes(eight(bytes))),
                FieldType::Bool => format!("bool:{}", bytes[0] != 0),
                FieldType::Datetime => format!("datetime:{}", i64::from_ne_bytes(eight(bytes))),
                other => return Err(io::Error::other(format!("{other:?} is not fixed-width"))),
            };
            Ok(rendered)
        }
        Column::Varlen { offsets, data } => {
            let start = usize::try_from(offsets[row]).expect("an offset fits in memory");
            let end = usize::try_from(offsets[row + 1]).expect("an offset fits in memory");
            let copied = &data[start..end];
            let borrowed = event.borrowed(part, row, index)?;
            if copied != borrowed {
                return Err(io::Error::other(
                    "a copied value differs from the same value borrowed from the frame",
                ));
            }
            let prefix = match field.field_type {
                FieldType::String => "str",
                _ => "bytes",
            };
            Ok(format!("{prefix}:{}", hex(copied)))
        }
    }
}

fn four(bytes: &[u8]) -> [u8; 4] {
    bytes.try_into().expect("a four-byte column value")
}

fn eight(bytes: &[u8]) -> [u8; 8] {
    bytes.try_into().expect("an eight-byte column value")
}

/// Runs the probe against `target`, reporting through `emit`.
pub(super) fn run(target: &ProbeTarget, emit: &mut Emit<'_>) -> io::Result<()> {
    let session = OpenSession::connect(target)?;

    let operation = session.execute(&format!("SHOW CREATE RELAY {};", target.relay))?;
    emit(&format!("OPERATION {}", operation.disposition()))?;
    if operation.message().is_empty() {
        return Err(io::Error::other(
            "a completed SHOW CREATE RELAY returned no text",
        ));
    }

    let failed = session.execute("CREATE RELAY;")?;
    emit(&failed.error_line()?)?;

    let opened = session.execute(&format!(
        "CREATE SUBSCRIPTION {} TO {};",
        target.subscription, target.relay
    ))?;
    let schema = opened.schema()?;
    let fields = schema.fields(Part::Rows)?;
    for field in &fields {
        emit(&field.line("FIELD"))?;
    }
    let key_fields = schema.fields(Part::BranchKey)?;
    if let Some(branch) = schema.branch() {
        emit(&format!("BRANCH {branch}"))?;
    }
    for field in &key_fields {
        emit(&field.line("KEY"))?;
    }
    emit(super::SUBSCRIBED_LINE)?;

    let rows_deadline = Token::with_deadline(ROWS_DEADLINE_MILLIS)?;
    let mut rows_seen = 0;
    let mut retained = Vec::new();
    let mut reported = Vec::new();
    while rows_seen < target.rows {
        let event = session.next_event(&rows_deadline)?;
        if event.kind() != EventKind::Rows {
            return Err(io::Error::other(format!(
                "the subscription reported {:?} before its rows arrived",
                event.kind()
            )));
        }
        let mut name = ptr::null();
        let mut name_len = 0;
        let mut generation = 0;
        // SAFETY: the event is live and the out-parameters are writable.
        let subscription = unsafe {
            nx_event_subscription(event.0, &mut name, &mut name_len, &mut generation);
            copied(name, name_len)
        };
        if subscription != target.subscription.as_bytes() {
            return Err(io::Error::other("rows arrived for another subscription"));
        }
        let mut event_schema = ptr::null_mut();
        // SAFETY: the event is live and `event_schema` is writable.
        check(unsafe { nx_event_schema(event.0, &mut event_schema) })?;
        let event_schema = OwnedSchema(event_schema);
        if event_schema.fields(Part::Rows)?.len() != fields.len() {
            return Err(io::Error::other(
                "an event's schema differs from the opened schema",
            ));
        }
        let lines = event.row_lines(&fields, &key_fields)?;
        rows_seen += lines.len();
        for line in &lines {
            emit(line)?;
        }
        reported.push(lines);
        // Keep a second reference and let the first go, so the rows must survive on it alone.
        retained.push(event.retain());
        drop(event);
    }

    check_retention(&retained, &reported, &fields, &key_fields)?;
    check_cancellation(&session)?;
    emit("CHECKS ok")?;

    let closed = session.execute(&format!("DELETE SUBSCRIPTION {};", target.subscription))?;
    emit(&format!("CLOSED {}", closed.disposition()))?;
    emit("PASS")?;
    Ok(())
}

/// Rereads every retained event after its first reference is gone and its frames have been
/// shared across threads, and releases the references from another thread.
fn check_retention(
    retained: &[EventReference],
    reported: &[Vec<String>],
    fields: &[Field],
    key_fields: &[Field],
) -> io::Result<()> {
    let frames: Vec<Vec<u8>> = retained
        .iter()
        .map(|event| event.frame().map(<[u8]>::to_vec))
        .collect::<io::Result<_>>()?;
    let churn: Vec<Vec<u8>> = (0..1024).map(|size| vec![0xA5; size * 64]).collect();
    drop(churn);
    for ((event, lines), frame) in retained.iter().zip(reported).zip(&frames) {
        if event.frame()? != frame.as_slice() {
            return Err(io::Error::other(
                "a retained frame changed while it was retained",
            ));
        }
        if &event.row_lines(fields, key_fields)? != lines {
            return Err(io::Error::other(
                "a retained event reads differently than it did",
            ));
        }
    }
    let second: Vec<EventReference> = retained.iter().map(EventReference::retain).collect();
    let releaser = thread::spawn(move || drop(second));
    releaser
        .join()
        .map_err(|_| io::Error::other("releasing events on another thread panicked"))?;
    Ok(())
}

/// Cancels a wait from another thread, then lets a deadline end another, and checks the
/// session still serves requests after both.
fn check_cancellation(session: &OpenSession) -> io::Result<()> {
    let token = Token::new();
    let session_pointer = SessionPointer(session.0);
    let result = thread::scope(|scope| {
        let waiter = scope.spawn(|| {
            let session = &session_pointer;
            let token = &token;
            let mut event = ptr::null_mut();
            // SAFETY: the session outlives this scope and the token is live.
            FailurePointer(unsafe { nx_session_next_event(session.0, token.0, &mut event) })
        });
        thread::sleep(Duration::from_millis(100));
        // SAFETY: the token is live.
        unsafe { nx_cancel_trigger(token.0) };
        waiter.join()
    });
    let failure = result.map_err(|_| io::Error::other("the cancelled waiter panicked"))?;
    let kind = expect_failure(failure.0)?;
    if kind != FailureKind::Cancelled {
        return Err(io::Error::other(format!(
            "a cancelled wait failed with {kind:?}"
        )));
    }

    let deadline = Token::with_deadline(50)?;
    let mut event = ptr::null_mut();
    // SAFETY: the session and token are live and `event` is writable.
    let kind = expect_failure(unsafe { nx_session_next_event(session.0, deadline.0, &mut event) })?;
    if kind != FailureKind::Deadline {
        return Err(io::Error::other(format!(
            "an expired wait failed with {kind:?}"
        )));
    }

    let cancelled = Token::new();
    // SAFETY: the token is live.
    unsafe { nx_cancel_trigger(cancelled.0) };
    let query = "SHOW DOMAINS;";
    let mut execution = ptr::null_mut();
    // SAFETY: the session is live, the query addresses its length, and `execution` is writable.
    check(unsafe {
        nx_session_prepare(
            session.0,
            query.as_ptr(),
            query.len(),
            ptr::null(),
            &mut execution,
        )
    })?;
    let mut outcome = ptr::null_mut();
    // SAFETY: the session, execution and token are live and `outcome` is writable; the failure
    // is read before it is released, and the execution is released once.
    let named_reference = unsafe {
        let failure = nx_session_execute(session.0, execution, cancelled.0, &mut outcome);
        let mut reference = ptr::null();
        let mut reference_len = 0;
        let named = !failure.is_null()
            && nx_error_kind_of(failure) == FailureKind::Cancelled
            && nx_error_execution_reference(failure, &mut reference, &mut reference_len)
            && reference_len > 0;
        nx_error_free(failure);
        nx_execution_free(execution);
        named
    };
    if !named_reference {
        return Err(io::Error::other(
            "a cancelled command did not report Cancelled with its execution reference",
        ));
    }
    Ok(())
}

/// A session pointer shared with a scoped thread that uses it concurrently, as the binding
/// allows.
struct SessionPointer(*mut Session);

// SAFETY: the binding allows a session to be used from several threads at once.
unsafe impl Sync for SessionPointer {}

/// A failure returned on another thread.
struct FailurePointer(*mut nervix_client_ffi::Failure);

// SAFETY: a failure is plain owned data, released by whichever thread holds it.
unsafe impl Send for FailurePointer {}
