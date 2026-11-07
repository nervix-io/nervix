//! Repository tooling, outside the product layer order.
//! Owns: checked Git reads and their bounded, dynamically shared history-read workers.
//! Depends on: subprocess output and the selected blocking lock and scoped threads.
//! Must not know: compiler policy or product task scheduling.

use std::{
    path::{Path, PathBuf},
    process::{Command, Output},
};

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_primitives::{sync::blocking::Mutex, thread::scope};

use crate::error::{Error, Result};

pub(super) struct Git {
    root: PathBuf,
}

impl Git {
    pub fn new(root: &Path) -> Self {
        Self { root: root.into() }
    }

    pub fn output(&self, arguments: &[&str]) -> Result<Output> {
        Command::new("git")
            .args(arguments)
            .current_dir(&self.root)
            .output()
            .change_context(Error::Io {
                operation: "read audit Git history",
                path: self.root.clone(),
            })
    }

    pub fn text(&self, arguments: &[&str]) -> Result<String> {
        let output = self.output(arguments)?;
        if !output.status.success() {
            return Err(Report::new(Error::AuditGit {
                arguments: arguments
                    .iter()
                    .map(|argument| (*argument).into())
                    .collect(),
                status: output.status.code(),
            })
            .attach(String::from_utf8_lossy(&output.stderr).into_owned()));
        }
        Self::decode(output.stdout).change_context(Error::Io {
            operation: "decode audit Git history",
            path: self.root.clone(),
        })
    }

    /// Preserve the existing eight-read bound and dynamic dispatch, without holding the work
    /// lock during a Git wait. Every scoped worker joins before this read group returns.
    pub fn read_all<T: Sync, R: Send>(
        &self,
        items: &[T],
        read: impl Fn(&T) -> Result<R> + Sync,
    ) -> Result<Vec<R>> {
        let next = Mutex::new(0_usize);
        let groups = scope(|scope| {
            let mut workers = Vec::new();
            for _ in 0..items.len().min(8) {
                workers.push(scope.spawn(|| {
                    let mut results = Vec::new();
                    loop {
                        let index = {
                            let mut next = next.lock();
                            if *next == items.len() {
                                break;
                            }
                            let index = *next;
                            *next = next
                                .checked_add(1)
                                .verified("the next index is smaller than the slice length");
                            index
                        };
                        results.push(Indexed {
                            index,
                            value: read(&items[index]),
                        });
                    }
                    results
                }));
            }
            let mut groups = Vec::new();
            for worker in workers {
                match worker.join() {
                    Ok(group) => groups.push(group),
                    Err(panic) => std::panic::resume_unwind(panic),
                }
            }
            groups
        });
        let mut ordered: Vec<_> = std::iter::repeat_with(|| None).take(items.len()).collect();
        for group in groups {
            for entry in group {
                ordered[entry.index] = Some(entry.value);
            }
        }
        let mut results = Vec::with_capacity(items.len());
        for result in ordered {
            results.push(
                result.verified("every input index is claimed once and every worker joined")?,
            );
        }
        Ok(results)
    }

    pub fn decode(mut bytes: Vec<u8>) -> std::result::Result<String, std::string::FromUtf8Error> {
        // Git reads keep the established text-mode universal-newline contract, in place, so
        // large blame/diff output does not need another complete buffer.
        let mut carriage_return = false;
        bytes.retain_mut(|byte| {
            let omit = *byte == b'\n' && carriage_return;
            carriage_return = *byte == b'\r';
            if carriage_return {
                *byte = b'\n';
            }
            !omit
        });
        String::from_utf8(bytes)
    }

    pub fn lines(text: &str) -> impl Iterator<Item = &str> {
        text.split_terminator([
            '\n', '\u{b}', '\u{c}', '\u{1c}', '\u{1d}', '\u{1e}', '\u{85}', '\u{2028}', '\u{2029}',
        ])
    }

    pub fn is_sha(text: &str) -> bool {
        text.len() == 40
            && text
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    }
}

struct Indexed<T> {
    index: usize,
    value: T,
}
