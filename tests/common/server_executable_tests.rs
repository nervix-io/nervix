//! Regressions for reading a server executable's mapped ranges before a launch.
//!
//! Layer: test harness.
//! - **Owns.** Stand-in ELF images written to temporary files, and the checks that a preparation
//!   reads exactly the ranges an image maps, refuses what it cannot read, and ends at its
//!   deadline.
//! - **Depends on.** The server executable owner, the phase deadline, and the executable that
//!   runs this test.
//! - **Must not know.** How a launch spawns the executable or waits for its readiness.

use std::{path::PathBuf, time::Duration};

use error_stack::Report;
use meticulous::ResultExt as _;
use tempfile::TempDir;

use crate::{
    phase_deadline::PhaseDeadline,
    process_program::scenario_executable,
    server_executable::{PreparedExecutable, ServerExecutable, ServerExecutableError},
};

/// A preparation of a small stand-in image ends in milliseconds; the bound only has to hold on a
/// machine running every harness test at once.
const PREPARATION_BUDGET: Duration = Duration::from_secs(60);
const LOADABLE_SEGMENT: u32 = 1;
const NOTE_SEGMENT: u32 = 4;
/// The layout of an ELF64 file header and program header, as a stand-in image writes them.
const ELF_HEADER_BYTES: u64 = 64;
const PROGRAM_HEADER_BYTES: u16 = 56;
const ELF_IDENTIFICATION: [u8; 7] = [0x7f, b'E', b'L', b'F', 2, 1, 1];

/// One program header of a stand-in image.
#[derive(Clone, Copy)]
struct Segment {
    kind: u32,
    offset: u64,
    file_size: u64,
}

impl Segment {
    fn loadable(offset: u64, file_size: u64) -> Self {
        Self {
            kind: LOADABLE_SEGMENT,
            offset,
            file_size,
        }
    }

    fn write_into(self, entry: &mut [u8]) {
        entry.fill(0);
        entry[0..4].copy_from_slice(&self.kind.to_le_bytes());
        entry[8..16].copy_from_slice(&self.offset.to_le_bytes());
        entry[0x20..0x28].copy_from_slice(&self.file_size.to_le_bytes());
    }
}

/// A 64-bit little-endian ELF image that holds only its header, its program header table and
/// filler bytes.
struct StandInImage {
    length: usize,
    segments: Vec<Segment>,
    /// Where the header says the program header table is. A valid image keeps it right after the
    /// header.
    table_offset: u64,
    /// The size the header gives one program header. A valid image gives 56.
    entry_size: u16,
}

impl StandInImage {
    fn new(length: usize, segments: &[Segment]) -> Self {
        Self {
            length,
            segments: segments.to_vec(),
            table_offset: ELF_HEADER_BYTES,
            entry_size: PROGRAM_HEADER_BYTES,
        }
    }

    fn write(&self, directory: &TempDir, name: &str) -> PathBuf {
        let mut image = vec![0xa5_u8; self.length];
        image[..ELF_IDENTIFICATION.len()].copy_from_slice(&ELF_IDENTIFICATION);
        image[0x20..0x28].copy_from_slice(&self.table_offset.to_le_bytes());
        image[0x36..0x38].copy_from_slice(&self.entry_size.to_le_bytes());
        let count = u16::try_from(self.segments.len()).assured("a stand-in image has few segments");
        image[0x38..0x3a].copy_from_slice(&count.to_le_bytes());
        let table = &mut image[64..];
        let entries = table.chunks_exact_mut(usize::from(PROGRAM_HEADER_BYTES));
        for (entry, segment) in entries.zip(&self.segments) {
            segment.write_into(entry);
        }
        let path = directory.path().join(name);
        std::fs::write(&path, image).assured("the temporary directory accepts a stand-in image");
        path
    }
}

struct Preparation;

impl Preparation {
    async fn of(path: PathBuf) -> Result<PreparedExecutable, Report<ServerExecutableError>> {
        ServerExecutable::new(path)
            .prepare(PhaseDeadline::after(PREPARATION_BUDGET))
            .await
    }

    fn directory() -> TempDir {
        tempfile::tempdir().assured("the test host provides temporary directories")
    }
}

#[nervix_primitives::test]
async fn a_preparation_reads_each_mapped_range_once_and_nothing_else() {
    let directory = Preparation::directory();
    let image = StandInImage::new(
        0xa000,
        &[
            // Two segments that overlap, and a third that continues where the second ends, form
            // one range: 0x0000 to 0x6000.
            Segment::loadable(0, 0x3000),
            Segment::loadable(0x2000, 0x3000),
            Segment::loadable(0x5000, 0x1000),
            // A segment the kernel does not map, and a loadable segment with no file bytes.
            Segment {
                kind: NOTE_SEGMENT,
                offset: 0x7000,
                file_size: 0x800,
            },
            Segment::loadable(0x9000, 0),
            // A range of its own: 0x8000 to 0x9000. The bytes after it are never mapped.
            Segment::loadable(0x8000, 0x1000),
        ],
    );
    let path = image.write(&directory, "stand-in");

    let prepared = Preparation::of(path.clone())
        .await
        .assured("a well-formed stand-in image is read");

    let description = prepared.to_string();
    let expected = format!(
        "read the 28672 mapped bytes of {} in 2 ranges in ",
        path.display()
    );
    assert!(description.starts_with(&expected), "{description}");
}

#[nervix_primitives::test]
async fn the_server_executable_maps_less_than_its_whole_file() {
    let path = scenario_executable();
    let length = std::fs::metadata(&path)
        .assured("this test runs from that file")
        .len();

    let prepared = Preparation::of(path)
        .await
        .assured("the executable that runs this test is a readable ELF image");

    // A debug executable's debug information is never mapped, and its symbol and section tables
    // are not either, so the mapped bytes are fewer than the file's.
    let description = prepared.to_string();
    let Some(after_verb) = description.strip_prefix("read the ") else {
        panic!("a preparation's description starts with what it read: {description}");
    };
    let Some(bytes) = after_verb.split_whitespace().next() else {
        panic!("a preparation's description names the bytes it read: {description}");
    };
    let Ok(mapped) = bytes.parse::<u64>() else {
        panic!("a preparation's description counts the bytes it read: {description}");
    };
    assert!(mapped > 0, "{description}");
    assert!(mapped < length, "{description}; file length {length}");
}

#[nervix_primitives::test]
async fn a_file_that_is_not_an_elf_image_is_refused() {
    let directory = Preparation::directory();
    let script = directory.path().join("script");
    std::fs::write(&script, "#!/bin/sh\nexit 0\n").assured("the directory accepts a script");

    let Err(error) = Preparation::of(script).await else {
        panic!("a shell script is not an ELF image");
    };

    assert!(
        matches!(
            error.current_context(),
            ServerExecutableError::NotElf { .. }
        ),
        "{error:?}"
    );
}

#[nervix_primitives::test]
async fn a_file_shorter_than_an_elf_header_is_refused() {
    let directory = Preparation::directory();
    let short = directory.path().join("short");
    std::fs::write(&short, &ELF_IDENTIFICATION[..6]).assured("the directory accepts a file");

    let Err(error) = Preparation::of(short).await else {
        panic!("six bytes are no ELF header");
    };

    assert!(
        matches!(
            error.current_context(),
            ServerExecutableError::NotElf { .. }
        ),
        "{error:?}"
    );
}

#[nervix_primitives::test]
async fn a_program_header_table_outside_the_file_is_refused() {
    let directory = Preparation::directory();
    let mut image = StandInImage::new(0x2000, &[Segment::loadable(0, 0x1000)]);
    image.table_offset = 0x10_0000;
    let path = image.write(&directory, "far-table");

    let Err(error) = Preparation::of(path).await else {
        panic!("the program header table lies past the end of the file");
    };

    assert!(
        matches!(
            error.current_context(),
            ServerExecutableError::ProgramHeaders { .. }
        ),
        "{error:?}"
    );
}

#[nervix_primitives::test]
async fn program_headers_of_another_size_are_refused() {
    let directory = Preparation::directory();
    let mut image = StandInImage::new(0x2000, &[Segment::loadable(0, 0x1000)]);
    image.entry_size = 32;
    let path = image.write(&directory, "small-entries");

    let Err(error) = Preparation::of(path).await else {
        panic!("an ELF64 program header is 56 bytes");
    };

    assert!(
        matches!(
            error.current_context(),
            ServerExecutableError::ProgramHeaders { .. }
        ),
        "{error:?}"
    );
}

#[nervix_primitives::test]
async fn a_mapped_range_past_the_end_of_the_file_is_refused() {
    let directory = Preparation::directory();
    let image = StandInImage::new(0x2000, &[Segment::loadable(0x1000, 0x4000)]);
    let path = image.write(&directory, "truncated");

    let Err(error) = Preparation::of(path).await else {
        panic!("the mapped range ends 0x3000 bytes past the end of the file");
    };

    let ServerExecutableError::Truncated { offset, .. } = error.current_context() else {
        panic!("expected a truncated image: {error:?}");
    };
    assert_eq!(*offset, 0x2000);
}

#[nervix_primitives::test]
async fn a_preparation_whose_deadline_has_passed_reads_nothing() {
    let directory = Preparation::directory();
    let image = StandInImage::new(0x2000, &[Segment::loadable(0, 0x1000)]);
    let path = image.write(&directory, "late");

    let refused = ServerExecutable::new(path)
        .prepare(PhaseDeadline::after(Duration::ZERO))
        .await;

    let Err(error) = refused else {
        panic!("a preparation that starts after its deadline does not run");
    };
    let ServerExecutableError::DeadlinePassedBeforeHeaders { budget, .. } = error.current_context()
    else {
        panic!("expected the deadline to pass before the headers were read: {error:?}");
    };
    assert_eq!(*budget, Duration::ZERO);
}
