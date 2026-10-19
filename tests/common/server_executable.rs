//! The executable a server process launch runs, read in file order before the launch.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Finding the parts of a server executable the kernel maps when it runs it, reading
//!   them in file order within a launch's startup deadline, and what that read took.
//! - **Depends on.** The ELF file and program header layout, Tokio's file I/O, and the phase
//!   deadline.
//! - **Must not know.** What the executable does, how a launch spawns it, or when it is ready.
//!
//! A process does not read its executable when it starts. The kernel maps the file, and every page
//! the process first touches is read when the process faults on it: one page at a time, in the
//! order its code happens to run. A debug server executable maps hundreds of megabytes, and its
//! first start touches code all over them. When the file is not in memory and the storage under it
//! is busy, each of those reads waits for the storage on its own, and on a rotational disk each one
//! pays a seek. Reading the mapped parts in file order first turns those scattered reads into a few
//! large sequential ones, and the process then faults its pages in from memory.

use std::{collections::BTreeMap, fmt, io, io::SeekFrom, path::PathBuf, time::Duration};

use arch_into::ArchInto as _;
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_primitives::time::Instant;
use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncReadExt as _, AsyncSeekExt as _},
};

use super::phase_deadline::{BeforeDeadline, PhaseDeadline};

/// The identification an executable starts with: the ELF magic, then the 64-bit class and the
/// little-endian data encoding.
const ELF_MAGIC: [u8; 4] = [0x7f, b'E', b'L', b'F'];
const ELF_CLASS_64: u8 = 2;
const ELF_DATA_LITTLE_ENDIAN: u8 = 1;
const ELF_MAGIC_FIELD: usize = 0;
const ELF_CLASS_FIELD: usize = 4;
const ELF_DATA_FIELD: usize = 5;
/// The size of an ELF64 file header, and where it records its program header table.
const ELF_HEADER_BYTES: usize = 64;
const PROGRAM_HEADER_TABLE_OFFSET_FIELD: usize = 0x20;
const PROGRAM_HEADER_SIZE_FIELD: usize = 0x36;
const PROGRAM_HEADER_COUNT_FIELD: usize = 0x38;
/// The size of one ELF64 program header, and where it records its segment.
const PROGRAM_HEADER_BYTES: usize = 56;
const SEGMENT_TYPE_FIELD: usize = 0x00;
const SEGMENT_FILE_OFFSET_FIELD: usize = 0x08;
const SEGMENT_FILE_SIZE_FIELD: usize = 0x20;
/// The type of a segment the kernel maps into the process.
const LOADABLE_SEGMENT: u32 = 1;
/// How much of the executable one read asks for. A policy input: large enough that a mapped range
/// of hundreds of megabytes takes a few hundred reads, small enough that the deadline is checked
/// between reads that a busy disk finishes in well under a second.
const READ_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// The file a server process launch executes.
#[derive(Clone, Debug)]
pub(crate) struct ServerExecutable {
    path: PathBuf,
}

/// Why reading an executable before its launch ended without reading all of it.
#[derive(Debug, Error)]
pub(crate) enum ServerExecutableError {
    #[error("the executable {} could not be opened", .path.display())]
    Open {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("the executable {} is not a 64-bit little-endian ELF image", .path.display())]
    NotElf { path: PathBuf },
    #[error(
        "the program header table of the executable {} does not describe ranges of the file",
        .path.display()
    )]
    ProgramHeaders { path: PathBuf },
    #[error("reading the executable {} at offset {offset} failed", .path.display())]
    Read {
        path: PathBuf,
        offset: u64,
        #[source]
        source: io::Error,
    },
    #[error(
        "the executable {} ends at offset {offset}, inside a range its program headers map",
        .path.display()
    )]
    Truncated { path: PathBuf, offset: u64 },
    #[error(
        "the {budget:?} startup deadline passed before the program headers of the executable {} \
         were read",
        .path.display()
    )]
    DeadlinePassedBeforeHeaders { path: PathBuf, budget: Duration },
    #[error(
        "the {budget:?} startup deadline passed after {prepared} of the {mapped} mapped bytes of \
         the executable {} were read",
        .path.display()
    )]
    DeadlinePassed {
        path: PathBuf,
        prepared: u64,
        mapped: u64,
        budget: Duration,
    },
}

/// A part of the file that must be read whole, named by what it is for the failure when the file
/// ends inside it.
#[derive(Clone, Copy, Debug)]
enum WholePart {
    ElfHeader,
    ProgramHeaderTable,
}

/// One contiguous byte range of an executable that the kernel maps.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MappedRange {
    offset: u64,
    end: u64,
}

impl MappedRange {
    fn length(self) -> u64 {
        self.end
            .checked_sub(self.offset)
            .assured("a mapped range ends at or after its offset by construction")
    }
}

/// What reading an executable before its launch did: how many bytes it read, which are every byte
/// the executable maps, in how many separate ranges, and how long that took.
#[derive(Clone, Debug)]
pub(crate) struct PreparedExecutable {
    path: PathBuf,
    ranges: usize,
    bytes: u64,
    elapsed: Duration,
}

impl fmt::Display for PreparedExecutable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "read the {} mapped bytes of {} in {} ranges in {:.2?}",
            self.bytes,
            self.path.display(),
            self.ranges,
            self.elapsed
        )
    }
}

/// The bytes of an ELF header or program header, read by their little-endian fields.
struct LittleEndianFields<'a> {
    bytes: &'a [u8],
}

impl LittleEndianFields<'_> {
    fn field<const WIDTH: usize>(&self, at: usize) -> Option<[u8; WIDTH]> {
        let end = at.checked_add(WIDTH)?;
        let raw = self.bytes.get(at..end)?;
        raw.try_into().ok()
    }

    fn u16(&self, at: usize) -> Option<u16> {
        Some(u16::from_le_bytes(self.field(at)?))
    }

    fn u32(&self, at: usize) -> Option<u32> {
        Some(u32::from_le_bytes(self.field(at)?))
    }

    fn u64(&self, at: usize) -> Option<u64> {
        Some(u64::from_le_bytes(self.field(at)?))
    }
}

/// How far one preparation has read, for the failure that ends it.
#[derive(Debug)]
enum Progress {
    /// The program headers are not read yet, so the size of the mapped ranges is not known.
    ReadingHeaders,
    ReadingRanges {
        prepared: u64,
        mapped: u64,
    },
}

impl ServerExecutable {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Reads every range of the executable the kernel maps when it runs it, in file order, and
    /// ends at `deadline` if it has not finished by then.
    ///
    /// Every read receives only the time left before `deadline`, and no read starts once it has
    /// passed, so storage that cannot serve the executable in time ends the launch's startup at its
    /// own deadline rather than after it.
    pub(crate) async fn prepare(
        &self,
        deadline: PhaseDeadline,
    ) -> Result<PreparedExecutable, Report<ServerExecutableError>> {
        let started_at = Instant::now();
        let mut progress = Progress::ReadingHeaders;
        let opened = deadline.bound(File::open(&self.path)).await;
        let mut file = match opened {
            BeforeDeadline::Finished(Ok(file)) => file,
            BeforeDeadline::Finished(Err(source)) => {
                return Err(Report::new(ServerExecutableError::Open {
                    path: self.path.clone(),
                    source,
                }));
            }
            BeforeDeadline::Passed => return Err(self.deadline_passed(deadline, &progress)),
        };
        let ranges = self.mapped_ranges(&mut file, deadline, &progress).await?;
        let mut mapped = 0_u64;
        for range in &ranges {
            mapped = mapped
                .checked_add(range.length())
                .assured("disjoint ranges that each end within u64 have a total length within u64");
        }
        progress = Progress::ReadingRanges {
            prepared: 0,
            mapped,
        };
        let mut buffer = vec![0_u8; READ_CHUNK_BYTES];
        for range in &ranges {
            nervix_primitives::task::consume_budget().await;
            self.read_range(&mut file, *range, &mut buffer, deadline, &mut progress)
                .await?;
        }
        Ok(PreparedExecutable {
            path: self.path.clone(),
            ranges: ranges.len(),
            bytes: mapped,
            elapsed: started_at.elapsed(),
        })
    }

    /// The ranges the executable's loadable segments map, in file order, with overlapping and
    /// adjacent segments joined into one range.
    async fn mapped_ranges(
        &self,
        file: &mut File,
        deadline: PhaseDeadline,
        progress: &Progress,
    ) -> Result<Vec<MappedRange>, Report<ServerExecutableError>> {
        let mut header = [0_u8; ELF_HEADER_BYTES];
        self.read_whole(
            file,
            0,
            &mut header,
            WholePart::ElfHeader,
            deadline,
            progress,
        )
        .await?;
        let header = LittleEndianFields { bytes: &header };
        if header.field::<4>(ELF_MAGIC_FIELD) != Some(ELF_MAGIC) {
            return Err(self.not_elf());
        }
        if header.field::<1>(ELF_CLASS_FIELD) != Some([ELF_CLASS_64]) {
            return Err(self.not_elf());
        }
        if header.field::<1>(ELF_DATA_FIELD) != Some([ELF_DATA_LITTLE_ENDIAN]) {
            return Err(self.not_elf());
        }
        let Some(table_offset) = header.u64(PROGRAM_HEADER_TABLE_OFFSET_FIELD) else {
            return Err(self.program_headers());
        };
        let Some(entry_size) = header.u16(PROGRAM_HEADER_SIZE_FIELD) else {
            return Err(self.program_headers());
        };
        let Some(entry_count) = header.u16(PROGRAM_HEADER_COUNT_FIELD) else {
            return Err(self.program_headers());
        };
        let entry_size: usize = entry_size.arch_into();
        if entry_size != PROGRAM_HEADER_BYTES {
            return Err(self.program_headers());
        }
        let entry_count: usize = entry_count.arch_into();
        let table_bytes = entry_count
            .checked_mul(PROGRAM_HEADER_BYTES)
            .assured("a u16 count of 56-byte program headers fits in memory");
        let mut table = vec![0_u8; table_bytes];
        self.read_whole(
            file,
            table_offset,
            &mut table,
            WholePart::ProgramHeaderTable,
            deadline,
            progress,
        )
        .await?;

        // Each range's end by its offset, in offset order. Where two segments start at one offset
        // the longer one is kept.
        let mut segment_ends = BTreeMap::new();
        let (entries, _) = table.as_chunks::<PROGRAM_HEADER_BYTES>();
        for entry in entries {
            let entry = LittleEndianFields { bytes: entry };
            let Some(segment_type) = entry.u32(SEGMENT_TYPE_FIELD) else {
                return Err(self.program_headers());
            };
            let Some(offset) = entry.u64(SEGMENT_FILE_OFFSET_FIELD) else {
                return Err(self.program_headers());
            };
            let Some(file_size) = entry.u64(SEGMENT_FILE_SIZE_FIELD) else {
                return Err(self.program_headers());
            };
            if segment_type != LOADABLE_SEGMENT || file_size == 0 {
                continue;
            }
            let Some(end) = offset.checked_add(file_size) else {
                return Err(self.program_headers());
            };
            let longest = segment_ends.entry(offset).or_insert(end);
            if *longest < end {
                *longest = end;
            }
        }

        let mut ranges: Vec<MappedRange> = Vec::with_capacity(segment_ends.len());
        for (offset, end) in segment_ends {
            if let Some(previous) = ranges.last_mut()
                && offset <= previous.end
            {
                if previous.end < end {
                    previous.end = end;
                }
                continue;
            }
            ranges.push(MappedRange { offset, end });
        }
        Ok(ranges)
    }

    /// Reads `range` from its start to its end, a chunk at a time, before `deadline`.
    async fn read_range(
        &self,
        file: &mut File,
        range: MappedRange,
        buffer: &mut [u8],
        deadline: PhaseDeadline,
        progress: &mut Progress,
    ) -> Result<(), Report<ServerExecutableError>> {
        self.seek(file, range.offset, deadline, progress).await?;
        let chunk_limit: u64 = buffer.len().arch_into();
        let mut position = range.offset;
        while position < range.end {
            nervix_primitives::task::consume_budget().await;
            let remaining = range
                .end
                .checked_sub(position)
                .verified("the loop runs only while the position is before the range's end");
            let chunk: usize = remaining.min(chunk_limit).arch_into();
            let chunk_buffer = buffer
                .get_mut(..chunk)
                .verified("a chunk is at most the buffer's length, its limit above");
            let read = deadline.bound(file.read(chunk_buffer)).await;
            let count = match read {
                BeforeDeadline::Finished(Ok(0)) => {
                    return Err(Report::new(ServerExecutableError::Truncated {
                        path: self.path.clone(),
                        offset: position,
                    }));
                }
                BeforeDeadline::Finished(Ok(count)) => count,
                BeforeDeadline::Finished(Err(source)) => {
                    return Err(Report::new(ServerExecutableError::Read {
                        path: self.path.clone(),
                        offset: position,
                        source,
                    }));
                }
                BeforeDeadline::Passed => return Err(self.deadline_passed(deadline, progress)),
            };
            let count: u64 = count.arch_into();
            position = position
                .checked_add(count)
                .assured("a read returns at most the bytes left before the range's end, a u64");
            if let Progress::ReadingRanges { prepared, .. } = progress {
                *prepared = prepared
                    .checked_add(count)
                    .assured("the bytes read are at most the mapped bytes, whose total is a u64");
            }
        }
        Ok(())
    }

    /// Fills `bytes` from `offset` of the file before `deadline`.
    async fn read_whole(
        &self,
        file: &mut File,
        offset: u64,
        bytes: &mut [u8],
        part: WholePart,
        deadline: PhaseDeadline,
        progress: &Progress,
    ) -> Result<(), Report<ServerExecutableError>> {
        self.seek(file, offset, deadline, progress).await?;
        let read = deadline.bound(file.read_exact(bytes)).await;
        match read {
            BeforeDeadline::Finished(Ok(_)) => Ok(()),
            BeforeDeadline::Finished(Err(error))
                if error.kind() == io::ErrorKind::UnexpectedEof =>
            {
                match part {
                    WholePart::ElfHeader => Err(self.not_elf()),
                    WholePart::ProgramHeaderTable => Err(self.program_headers()),
                }
            }
            BeforeDeadline::Finished(Err(source)) => {
                Err(Report::new(ServerExecutableError::Read {
                    path: self.path.clone(),
                    offset,
                    source,
                }))
            }
            BeforeDeadline::Passed => Err(self.deadline_passed(deadline, progress)),
        }
    }

    async fn seek(
        &self,
        file: &mut File,
        offset: u64,
        deadline: PhaseDeadline,
        progress: &Progress,
    ) -> Result<(), Report<ServerExecutableError>> {
        let sought = deadline.bound(file.seek(SeekFrom::Start(offset))).await;
        match sought {
            BeforeDeadline::Finished(Ok(_)) => Ok(()),
            BeforeDeadline::Finished(Err(source)) => {
                Err(Report::new(ServerExecutableError::Read {
                    path: self.path.clone(),
                    offset,
                    source,
                }))
            }
            BeforeDeadline::Passed => Err(self.deadline_passed(deadline, progress)),
        }
    }

    fn not_elf(&self) -> Report<ServerExecutableError> {
        Report::new(ServerExecutableError::NotElf {
            path: self.path.clone(),
        })
    }

    fn program_headers(&self) -> Report<ServerExecutableError> {
        Report::new(ServerExecutableError::ProgramHeaders {
            path: self.path.clone(),
        })
    }

    fn deadline_passed(
        &self,
        deadline: PhaseDeadline,
        progress: &Progress,
    ) -> Report<ServerExecutableError> {
        let path = self.path.clone();
        let budget = deadline.budget();
        match progress {
            Progress::ReadingHeaders => {
                Report::new(ServerExecutableError::DeadlinePassedBeforeHeaders { path, budget })
            }
            Progress::ReadingRanges { prepared, mapped } => {
                Report::new(ServerExecutableError::DeadlinePassed {
                    path,
                    prepared: *prepared,
                    mapped: *mapped,
                    budget,
                })
            }
        }
    }
}
