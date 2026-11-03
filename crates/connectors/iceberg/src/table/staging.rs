//! The local Arrow IPC staging files an Iceberg emitter appends the batches it stages to.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The staging file layout — the staged schema's Arrow IPC message followed by the
//!   messages of every staged batch appended after it — creating a staging file, appending staged
//!   batches where their emitter recorded the file's end and returning each batch's exact byte
//!   range, cutting a failed append back off, and reading a contiguous span of staged batches back.
//! - **Depends on.** Arrow IPC encoding and decoding, the filesystem, and `tracing`.
//! - **Must not know.** Commits, acknowledgements, the catalog, the bulk memory class, the
//!   executor's workers, or graph state.
//!
//! A staging file is an Arrow IPC stream without its end-of-stream marker, so it ends after its
//! last staged batch. Every staged batch carries the dictionaries its columns need, so a read can
//! start at any of them.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{
    fs::{File, OpenOptions},
    io::{Cursor, Read as _, Seek as _, SeekFrom},
    ops::Range,
    path::{Path, PathBuf},
};

use arch_into::ArchInto as _;
use arrow_array::RecordBatch;
use arrow_ipc::{
    reader::StreamReader,
    writer::{
        CompressionContext, DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions,
        write_message,
    },
};
use arrow_schema::{ArrowError, Schema};
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_execution::Cancelled;
use tracing::debug;

/// Why creating, appending to or reading back a staging file failed. The filesystem's, Arrow's
/// or the cancellation's own failure is beneath.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(super) enum StagingError {
    #[error("failed to create Iceberg staging file '{}'", .path.display())]
    Create { path: PathBuf },
    #[error("failed to append to Iceberg staging file '{}'", .path.display())]
    Append { path: PathBuf },
    #[error("failed to read Iceberg staging file '{}'", .path.display())]
    Read { path: PathBuf },
    #[error(
        "Iceberg staging file '{}' holds another schema than its emitter stages",
        .path.display()
    )]
    Schema { path: PathBuf },
    #[error(
        "Iceberg staging file '{}' holds {found} batches where its emitter staged {expected}",
        .path.display()
    )]
    BatchCount {
        path: PathBuf,
        expected: usize,
        found: usize,
    },
}

impl StagingError {
    /// This failure with `cause` beneath it, described by both, which is how its emitter shows it
    /// as its transient error.
    fn caused_by<C: error_stack::Context>(self, cause: C) -> Report<Self> {
        let description = format!("{self}: {cause}");
        Report::new(cause)
            .change_context(self)
            .attach_printable(description)
    }

    /// This failure, which no other failure caused, described by itself.
    fn reported(self) -> Report<Self> {
        let description = self.to_string();
        Report::new(self).attach_printable(description)
    }
}

pub(super) type StagingResult<T> = Result<T, Report<StagingError>>;

/// What one append wrote: the byte range of each staged batch, in the order they were handed
/// over, and where the file ends after the last of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Appended {
    pub(super) ranges: Vec<Range<u64>>,
    pub(super) end: u64,
}

/// The staging file one append writes to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum AppendTarget {
    /// A staging file whose schema message ends at `schema_end` and whose last staged batch ends
    /// at `end`.
    Open {
        path: PathBuf,
        schema_end: u64,
        end: u64,
    },
    /// A new staging file, which the append creates first.
    New { path: PathBuf },
}

/// What one append to an [`AppendTarget`] staged: where its file's schema message ends, and what
/// it appended after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Staged {
    pub(super) schema_end: u64,
    pub(super) appended: Appended,
}

impl AppendTarget {
    /// Appends `batches` to this staging file, creating it first when it is new. `before_batch`
    /// runs before each batch is written, as [`append`] describes. A file this append created and
    /// could not fill is removed again, so a failed write leaves no staging file behind that holds
    /// no staged batch.
    pub(super) fn append(
        &self,
        schema: &Schema,
        batches: &[RecordBatch],
        before_batch: impl FnMut() -> Result<(), Cancelled>,
    ) -> StagingResult<Staged> {
        match self {
            Self::Open {
                path,
                schema_end,
                end,
            } => {
                let appended = append(path, *end, schema, batches, before_batch)?;
                Ok(Staged {
                    schema_end: *schema_end,
                    appended,
                })
            }
            Self::New { path } => {
                let schema_end = create(path, schema)?;
                let appended = append(path, schema_end, schema, batches, before_batch);
                match appended {
                    Ok(appended) => Ok(Staged {
                        schema_end,
                        appended,
                    }),
                    Err(error) => {
                        remove_created(path);
                        Err(error)
                    }
                }
            }
        }
    }
}

/// The contiguous staged batches one read takes from one staging file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Span {
    pub(super) path: PathBuf,
    /// Where the file's schema message ends.
    pub(super) schema_end: u64,
    /// The bytes the staged batches take.
    pub(super) bytes: Range<u64>,
    /// How many staged batches those bytes hold.
    pub(super) batches: usize,
}

impl Span {
    /// The staged batches this span holds, read back as [`read_span`] describes.
    pub(super) fn read(&self, schema: &Schema) -> StagingResult<Vec<RecordBatch>> {
        read_span(
            &self.path,
            self.schema_end,
            self.bytes.clone(),
            self.batches,
            schema,
        )
    }
}

/// The Arrow IPC encoder every staging file is written with, configured as an Arrow IPC stream
/// writer is by default.
struct StagingEncoder {
    generator: IpcDataGenerator,
    options: IpcWriteOptions,
}

impl StagingEncoder {
    fn new() -> Self {
        Self {
            generator: IpcDataGenerator::default(),
            options: IpcWriteOptions::default(),
        }
    }

    /// The schema message a staging file starts with, and the tracker of the dictionary IDs that
    /// message assigns.
    fn schema(&self, schema: &Schema) -> (EncodedData, DictionaryTracker) {
        let mut dictionaries = DictionaryTracker::new(false);
        let message = self.generator.schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut dictionaries,
            &self.options,
        );
        (message, dictionaries)
    }

    /// Writes `batch` to `file` as the messages a read starting at it decodes after the schema
    /// message alone: the dictionaries its columns need, then the batch. Returns how many bytes
    /// they took.
    fn write_batch(
        &self,
        file: &mut File,
        schema: &Schema,
        batch: &RecordBatch,
    ) -> Result<u64, ArrowError> {
        // A tracker of this batch's own sends every dictionary its columns need along with it.
        let (schema_message, mut dictionaries) = self.schema(schema);
        drop(schema_message);
        let (encoded_dictionaries, encoded_batch) = self.generator.encode(
            batch,
            &mut dictionaries,
            &self.options,
            &mut CompressionContext::default(),
        )?;
        let mut written = 0_u64;
        for dictionary in encoded_dictionaries {
            let message = self.write(file, dictionary)?;
            written = written
                .checked_add(message)
                .assured("one batch's messages are bytes Arrow held in memory");
        }
        let message = self.write(file, encoded_batch)?;
        let written = written
            .checked_add(message)
            .assured("one batch's messages are bytes Arrow held in memory");
        Ok(written)
    }

    /// Writes one encoded message to `file` and returns how many bytes it took.
    fn write(&self, file: &mut File, encoded: EncodedData) -> Result<u64, ArrowError> {
        let (metadata, body) = write_message(file, encoded, &self.options)?;
        let written = metadata
            .checked_add(body)
            .assured("one message's metadata and body are bytes Arrow held in memory");
        Ok(written.arch_into())
    }
}

/// Creates the staging file at `path`, holding only `schema`'s message, and returns where that
/// message ends, which is where the file's first staged batch starts. A path that already names a
/// file is refused rather than reused, and a file whose schema message cannot be written is
/// removed again.
pub(super) fn create(path: &Path, schema: &Schema) -> StagingResult<u64> {
    let failed = || StagingError::Create {
        path: path.to_path_buf(),
    };
    let opened = OpenOptions::new().write(true).create_new(true).open(path);
    let mut file = match opened {
        Ok(file) => file,
        Err(error) => return Err(failed().caused_by(error)),
    };
    let encoder = StagingEncoder::new();
    let (message, dictionaries) = encoder.schema(schema);
    drop(dictionaries);
    match encoder.write(&mut file, message) {
        Ok(schema_end) => Ok(schema_end),
        Err(error) => {
            drop(file);
            remove_created(path);
            Err(failed().caused_by(error))
        }
    }
}

/// Removes the staging file at `path` that this staging created and could not fill, which holds no
/// staged batch. A removal that fails is logged; the failure that left the file stays the error its
/// write reports.
fn remove_created(path: &Path) {
    if let Err(error) = std::fs::remove_file(path) {
        debug!(
            path = %path.display(),
            error = %error,
            "failed to remove an Iceberg staging file its failed write created"
        );
    }
}

/// Appends `batches` to the staging file at `path` from `end`, where its emitter recorded that the
/// file's last staged batch ends, and returns each batch's byte range.
///
/// Whatever the file holds after `end` is cut off first: bytes an earlier append left behind when
/// it failed, or when its emitter stopped waiting for it, belong to no staged batch. A failure cuts
/// the file back to `end` before it returns, so the file still ends after its last staged batch;
/// should that cut fail as well, the next append cuts the rest. `before_batch` runs before each
/// batch is written, and a batch it refuses fails the append.
pub(super) fn append(
    path: &Path,
    end: u64,
    schema: &Schema,
    batches: &[RecordBatch],
    mut before_batch: impl FnMut() -> Result<(), Cancelled>,
) -> StagingResult<Appended> {
    let failed = || StagingError::Append {
        path: path.to_path_buf(),
    };
    let opened = OpenOptions::new().write(true).open(path);
    let mut file = match opened {
        Ok(file) => file,
        Err(error) => return Err(failed().caused_by(error)),
    };
    if let Err(error) = file.set_len(end) {
        return Err(failed().caused_by(error));
    }
    if let Err(error) = file.seek(SeekFrom::Start(end)) {
        return Err(failed().caused_by(error));
    }
    let encoder = StagingEncoder::new();
    let mut position = end;
    let mut ranges = Vec::with_capacity(batches.len());
    for batch in batches {
        if let Err(cancelled) = before_batch() {
            cut_back(&file, path, end);
            return Err(failed().caused_by(cancelled));
        }
        let written = match encoder.write_batch(&mut file, schema, batch) {
            Ok(written) => written,
            Err(error) => {
                cut_back(&file, path, end);
                return Err(failed().caused_by(error));
            }
        };
        let start = position;
        position = position
            .checked_add(written)
            .assured("a staging file holds fewer than u64::MAX bytes its node wrote");
        ranges.push(start..position);
    }
    Ok(Appended {
        ranges,
        end: position,
    })
}

/// Cuts the staging file at `path`, open as `file`, back to `end`, where a failed append began. A
/// cut that fails is logged: the next append cuts the file again before it writes.
fn cut_back(file: &File, path: &Path, end: u64) {
    if let Err(error) = file.set_len(end) {
        debug!(
            path = %path.display(),
            end,
            error = %error,
            "failed to cut a failed append back off an Iceberg staging file"
        );
    }
}

/// Reads back the `batches` staged batches the bytes `span` of the staging file at `path` hold,
/// decoding them with the schema message the file starts with, which ends at `schema_end`, and
/// checking that message describes `schema`.
pub(super) fn read_span(
    path: &Path,
    schema_end: u64,
    span: Range<u64>,
    batches: usize,
    schema: &Schema,
) -> StagingResult<Vec<RecordBatch>> {
    let failed = || StagingError::Read {
        path: path.to_path_buf(),
    };
    let opened = File::open(path);
    let mut file = match opened {
        Ok(file) => file,
        Err(error) => return Err(failed().caused_by(error)),
    };
    let mut schema_message = vec![0_u8; schema_end.arch_into()];
    if let Err(error) = file.read_exact(&mut schema_message) {
        return Err(failed().caused_by(error));
    }
    if let Err(error) = file.seek(SeekFrom::Start(span.start)) {
        return Err(failed().caused_by(error));
    }
    let span_bytes = span
        .end
        .checked_sub(span.start)
        .assured("a span of staged batches ends after it starts");
    let stream = Cursor::new(schema_message).chain(file.take(span_bytes));
    let reader = match StreamReader::try_new(stream, None) {
        Ok(reader) => reader,
        Err(error) => return Err(failed().caused_by(error)),
    };
    if reader.schema().as_ref() != schema {
        return Err(StagingError::Schema {
            path: path.to_path_buf(),
        }
        .reported());
    }
    let mut decoded = Vec::with_capacity(batches);
    for batch in reader {
        match batch {
            Ok(batch) => decoded.push(batch),
            Err(error) => return Err(failed().caused_by(error)),
        }
    }
    if decoded.len() != batches {
        return Err(StagingError::BatchCount {
            path: path.to_path_buf(),
            expected: batches,
            found: decoded.len(),
        }
        .reported());
    }
    Ok(decoded)
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "staging_tests.rs"]
mod tests;
