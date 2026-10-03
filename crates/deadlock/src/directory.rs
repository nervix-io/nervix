//! The directory a diagnostic process records its evidence in.
//!
//! Each process records one file, named after its process identifier and the time its run started,
//! so processes sharing a directory never write over one another's evidence. A file is replaced
//! atomically: the new evidence is written beside it, synchronized, and renamed over it, and the
//! directory is synchronized after the rename, so a reader finds either the previous evidence or
//! the new one, complete.

use std::{
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{Read as _, Write as _},
    path::{Path, PathBuf},
};

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;

use crate::{
    error::EvidenceError,
    evidence::DeadlockEvidence,
    wire::{MAX_EVIDENCE_BYTES, unix_nanos},
};

const FILE_PREFIX: &str = "deadlock-";
const FILE_EXTENSION: &str = "rkyv";
const PARTIAL_EXTENSION: &str = "partial";

/// A directory evidence files are recorded in and read from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceDirectory {
    path: PathBuf,
}

impl EvidenceDirectory {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The file `evidence`'s process records its evidence in.
    pub fn file_of(&self, evidence: &DeadlockEvidence) -> Result<PathBuf, Report<EvidenceError>> {
        let process = evidence.process();
        let started = unix_nanos(process.started_at)?;
        Ok(self.path.join(format!(
            "{FILE_PREFIX}{}-{started}.{FILE_EXTENSION}",
            process.id
        )))
    }

    /// Record `evidence` in its process's file, replacing what the file held. Returns the file.
    pub fn record(&self, evidence: &DeadlockEvidence) -> Result<PathBuf, Report<EvidenceError>> {
        let bytes = evidence.encode()?;
        let path = self.file_of(evidence)?;
        let partial = path.with_extension(PARTIAL_EXTENSION);
        let write_failed = || EvidenceError::Write { path: path.clone() };
        let mut file = File::create(&partial).change_context_lazy(write_failed)?;
        file.write_all(&bytes).change_context_lazy(write_failed)?;
        file.sync_all().change_context_lazy(write_failed)?;
        drop(file);
        fs::rename(&partial, &path).change_context_lazy(write_failed)?;
        let directory = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .change_context_lazy(write_failed)?;
        directory.sync_all().change_context_lazy(write_failed)?;
        Ok(path)
    }

    /// Every evidence file in the directory, in name order.
    pub fn files(&self) -> Result<Vec<PathBuf>, Report<EvidenceError>> {
        let list_failed = || EvidenceError::List {
            path: self.path.clone(),
        };
        let mut files = Vec::new();
        for entry in fs::read_dir(&self.path).change_context_lazy(list_failed)? {
            let path = entry.change_context_lazy(list_failed)?.path();
            if is_evidence_file(&path) {
                files.push(path);
            }
        }
        files.sort();
        Ok(files)
    }

    /// The evidence in every file of the directory, in file name order.
    pub fn read_all(&self) -> Result<Vec<DeadlockEvidence>, Report<EvidenceError>> {
        let mut evidence = Vec::new();
        for path in self.files()? {
            evidence.push(read_file(&path)?);
        }
        Ok(evidence)
    }
}

fn is_evidence_file(path: &Path) -> bool {
    let Some(name) = path.file_name() else {
        return false;
    };
    let Some(name) = name.to_str() else {
        return false;
    };
    name.starts_with(FILE_PREFIX) && path.extension() == Some(OsStr::new(FILE_EXTENSION))
}

/// The evidence in the file at `path`, read up to the largest evidence file and no further.
fn read_file(path: &Path) -> Result<DeadlockEvidence, Report<EvidenceError>> {
    let read_failed = || EvidenceError::Read {
        path: path.to_path_buf(),
    };
    let file = File::open(path).change_context_lazy(read_failed)?;
    let limit = MAX_EVIDENCE_BYTES
        .checked_add(1)
        .assured("the evidence bound is far below u64::MAX");
    let mut bytes = Vec::new();
    file.take(limit)
        .read_to_end(&mut bytes)
        .change_context_lazy(read_failed)?;
    DeadlockEvidence::decode(&bytes).change_context_lazy(read_failed)
}
