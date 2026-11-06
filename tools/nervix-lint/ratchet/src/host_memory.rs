//! Repository tooling, outside the product layer order.
//! Owns: observations of available host/cgroup memory and a running driver's resident size.
//! Depends on: kernel-authored procfs and cgroup-v2 files.
//! Must not know: admission policy, queue ordering or compiler diagnostics.

use std::{fs, path::Path};

use meticulous::ResultExt as _;

use crate::admission::Domain;

pub fn available(path: &Path) -> Option<u64> {
    let text = fs::read_to_string(path).ok()?;
    for line in text.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name != "MemAvailable" {
            continue;
        }
        let mut fields = value.split_whitespace();
        let amount: u64 = fields.next()?.parse().ok()?;
        if fields.next()? != "kB" {
            return None;
        }
        return amount.checked_mul(1024);
    }
    None
}

pub fn own_cgroup(membership: &Path) -> Option<String> {
    let text = fs::read_to_string(membership).ok()?;
    for line in text.lines() {
        if let Some(group) = line.strip_prefix("0::") {
            return Some(group.into());
        }
    }
    None
}

pub fn stat_value(text: &str, name: &str) -> Option<u64> {
    for line in text.lines() {
        let Some((key, value)) = line.split_once(' ') else {
            continue;
        };
        if key == name {
            return value.parse().ok();
        }
    }
    None
}

pub fn cgroup_domains(group: &str, hierarchy: &Path) -> Vec<Domain> {
    let mut domains = Vec::new();
    for current in Path::new(group).ancestors() {
        if current.parent().is_none() {
            break;
        }
        let directory = hierarchy.join(
            current
                .strip_prefix("/")
                .assured("cgroup paths are absolute in the unified hierarchy"),
        );
        let Some(left) = cgroup_available(&directory) else {
            continue;
        };
        domains.push(Domain {
            cgroup: Some(current.to_string_lossy().into_owned()),
            available_bytes: left,
        });
    }
    domains
}

fn cgroup_available(directory: &Path) -> Option<u64> {
    let limit_text = fs::read_to_string(directory.join("memory.max")).ok()?;
    let limit: u64 = limit_text.trim().parse().ok()?;
    let used_text = fs::read_to_string(directory.join("memory.current")).ok()?;
    let used: u64 = used_text.trim().parse().ok()?;
    let stats = fs::read_to_string(directory.join("memory.stat")).ok()?;
    let reclaimable = stat_value(&stats, "inactive_file").unwrap_or(0);
    let left = i128::from(limit) - i128::from(used) + i128::from(reclaimable);
    u64::try_from(left.clamp(0, i128::from(limit))).ok()
}

pub fn domains() -> Vec<Domain> {
    let mut domains = Vec::new();
    if let Some(available_bytes) = available(Path::new("/proc/meminfo")) {
        domains.push(Domain {
            cgroup: None,
            available_bytes,
        });
    }
    if let Some(group) = own_cgroup(Path::new("/proc/self/cgroup")) {
        domains.extend(cgroup_domains(&group, Path::new("/sys/fs/cgroup")));
    }
    domains
}

pub fn resident(pid: u32) -> Option<u64> {
    resident_in(pid, Path::new("/proc"))
}

pub fn resident_in(pid: u32, proc: &Path) -> Option<u64> {
    let status = fs::read_to_string(proc.join(pid.to_string()).join("status")).ok()?;
    for line in status.lines() {
        if !line.starts_with("VmRSS:") {
            continue;
        }
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() != 3 || fields[2] != "kB" {
            return None;
        }
        let amount: u64 = fields[1].parse().ok()?;
        return amount.checked_mul(1024);
    }
    None
}

pub fn oom_kills(membership: &Path, hierarchy: &Path) -> Option<u64> {
    let group = own_cgroup(membership)?;
    let events = fs::read_to_string(
        hierarchy
            .join(group.trim_start_matches('/'))
            .join("memory.events"),
    )
    .ok()?;
    stat_value(&events, "oom_kill")
}

#[cfg(test)]
mod tests;
