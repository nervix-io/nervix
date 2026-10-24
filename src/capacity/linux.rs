//! The memory limit a Linux kernel reports for this process.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Reading physical memory from `/proc/meminfo`, this process's control groups from
//!   `/proc/self/cgroup`, and the memory limits of those groups and their ancestors.
//! - **Depends on.** The files the kernel serves under `/proc` and `/sys/fs/cgroup`.
//! - **Must not know.** What the node does with the limit.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{
    fs,
    path::{Path, PathBuf},
};

use tracing::debug;
use ubyte::ByteUnit;

const MEMINFO: &str = "/proc/meminfo";
const OWN_CGROUPS: &str = "/proc/self/cgroup";
const CGROUP_ROOT: &str = "/sys/fs/cgroup";
/// Where a version 1 hierarchy mounts its memory controller.
const CGROUP_V1_MEMORY_ROOT: &str = "/sys/fs/cgroup/memory";

/// The smallest of physical memory and every control-group memory limit on this process's path.
/// A source the kernel does not serve, or serves in a form this build cannot read, is skipped.
pub(super) fn memory_limit() -> Option<ByteUnit> {
    let physical = match read(Path::new(MEMINFO)) {
        Some(meminfo) => physical_memory(&meminfo),
        None => None,
    };
    let groups = match read(Path::new(OWN_CGROUPS)) {
        Some(cgroups) => memory_group_paths(&cgroups),
        None => Vec::new(),
    };
    let mut smallest = physical;
    for group in groups {
        for limit in group.limits() {
            smallest = match smallest {
                Some(current) if current <= limit => Some(current),
                _ => Some(limit),
            };
        }
    }
    smallest.map(ByteUnit::Byte)
}

fn read(path: &Path) -> Option<String> {
    match fs::read_to_string(path) {
        Ok(text) => Some(text),
        Err(error) => {
            debug!(path = %path.display(), %error, "a memory source is not available");
            None
        }
    }
}

/// `MemTotal` of `/proc/meminfo`, which the kernel reports in kibibytes.
fn physical_memory(meminfo: &str) -> Option<u64> {
    for line in meminfo.lines() {
        let Some(rest) = line.strip_prefix("MemTotal:") else {
            continue;
        };
        let mut fields = rest.split_whitespace();
        let kibibytes = fields.next()?.parse::<u64>().ok()?;
        if fields.next() != Some("kB") {
            return None;
        }
        return kibibytes.checked_mul(1024);
    }
    None
}

/// A control group of this process that the memory controller may limit.
#[derive(Debug, Clone, PartialEq, Eq)]
enum MemoryGroup {
    /// A group of the unified hierarchy, whose limits are each level's `memory.max`.
    Unified { path: PathBuf },
    /// A group of a version 1 memory hierarchy, whose limits are each level's
    /// `memory.limit_in_bytes`.
    Legacy { path: PathBuf },
}

impl MemoryGroup {
    /// Every numeric limit from this group up to its hierarchy's root.
    fn limits(&self) -> Vec<u64> {
        let (root, relative, file) = match self {
            Self::Unified { path } => (Path::new(CGROUP_ROOT), path, "memory.max"),
            Self::Legacy { path } => (
                Path::new(CGROUP_V1_MEMORY_ROOT),
                path,
                "memory.limit_in_bytes",
            ),
        };
        let mut limits = Vec::new();
        for level in relative.ancestors() {
            let relative = level.strip_prefix("/").unwrap_or(level);
            let Some(text) = read(&root.join(relative).join(file)) else {
                continue;
            };
            if let Some(limit) = group_limit(&text) {
                limits.push(limit);
            }
        }
        limits
    }
}

/// A group's memory limit, or `None` when it sets none.
fn group_limit(text: &str) -> Option<u64> {
    let value = text.trim();
    if value == "max" {
        return None;
    }
    value.parse::<u64>().ok()
}

/// The groups `/proc/self/cgroup` places this process in that the memory controller can limit.
fn memory_group_paths(cgroups: &str) -> Vec<MemoryGroup> {
    let mut groups = Vec::new();
    for line in cgroups.lines() {
        let mut fields = line.splitn(3, ':');
        let (Some(hierarchy), Some(controllers), Some(path)) =
            (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        if hierarchy == "0" && controllers.is_empty() {
            groups.push(MemoryGroup::Unified {
                path: PathBuf::from(path),
            });
            continue;
        }
        if controllers
            .split(',')
            .any(|controller| controller == "memory")
        {
            groups.push(MemoryGroup::Legacy {
                path: PathBuf::from(path),
            });
        }
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn physical_memory_reads_mem_total_in_bytes() {
        let meminfo = "MemTotal:       65808428 kB\nMemFree:         1234 kB\n";
        assert_eq!(physical_memory(meminfo), Some(65_808_428 * 1024));
        assert_eq!(physical_memory("MemFree: 1 kB\n"), None);
        assert_eq!(physical_memory("MemTotal: many kB\n"), None);
        assert_eq!(physical_memory("MemTotal: 12 MB\n"), None);
    }

    #[test]
    fn a_group_limit_is_a_number_and_max_sets_none() {
        assert_eq!(group_limit("2147483648\n"), Some(2_147_483_648));
        assert_eq!(group_limit("max\n"), None);
        assert_eq!(group_limit("unknown"), None);
    }

    #[test]
    fn memory_groups_come_from_the_unified_and_the_memory_hierarchies() {
        let cgroups = "12:cpu,cpuacct:/a\n11:memory:/docker/abc\n0::/user.slice/session.scope\n";
        assert_eq!(
            memory_group_paths(cgroups),
            vec![
                MemoryGroup::Legacy {
                    path: PathBuf::from("/docker/abc"),
                },
                MemoryGroup::Unified {
                    path: PathBuf::from("/user.slice/session.scope"),
                },
            ]
        );
        assert!(memory_group_paths("garbage\n").is_empty());
    }
}
