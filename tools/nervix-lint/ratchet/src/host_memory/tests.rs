//! Repository test harness, outside the product layer order.
//! Owns: deterministic kernel-observation fixtures for admission and OOM attribution.
//! Depends on: the production parsers and temporary procfs/cgroup-shaped files.
//! Must not know: real host memory, compiler execution or product policy.

use super::*;

#[test]
fn available_host_memory_and_unified_membership_are_exact() {
    let directory = tempfile::tempdir().assured("the memory fixture can create its directory");
    let path = directory.path().join("meminfo");
    assert_eq!(available(&path), None);
    fs::write(&path, "MemTotal: 999 kB\nMemAvailable: 123 kB\n")
        .assured("the fixture can write meminfo");
    assert_eq!(available(&path), Some(123 * 1024));
    fs::write(&path, "MemAvailable: 123 bytes\n").assured("the fixture can replace meminfo");
    assert_eq!(available(&path), None);
    fs::write(&path, "3:memory:/other\n0::/user.slice/one.scope\n")
        .assured("the fixture can write membership");
    assert_eq!(own_cgroup(&path).as_deref(), Some("/user.slice/one.scope"));
}

#[test]
fn every_limited_ancestor_is_counted_with_reclaimable_pages_and_clamped_availability() {
    let directory = tempfile::tempdir().assured("the cgroup fixture can create its hierarchy");
    for (name, limit, used, reclaimable) in [
        ("user.slice", "1000", "800", "100"),
        ("user.slice/one.scope", "500", "100", "50"),
    ] {
        let group = directory.path().join(name);
        fs::create_dir_all(&group).assured("the cgroup directory can be written");
        fs::write(group.join("memory.max"), limit).assured("the cgroup limit can be written");
        fs::write(group.join("memory.current"), used).assured("the cgroup usage can be written");
        fs::write(
            group.join("memory.stat"),
            format!("inactive_file {reclaimable}\n"),
        )
        .assured("the cgroup statistics can be written");
    }
    assert_eq!(
        cgroup_domains("/user.slice/one.scope", directory.path()),
        vec![
            Domain {
                cgroup: Some("/user.slice/one.scope".into()),
                available_bytes: 450
            },
            Domain {
                cgroup: Some("/user.slice".into()),
                available_bytes: 300
            }
        ]
    );
    let group = directory.path().join("user.slice/one.scope");
    fs::write(group.join("memory.current"), "10000").assured("the fixture can replace usage");
    assert_eq!(
        cgroup_domains("/user.slice/one.scope", directory.path())[0].available_bytes,
        0
    );
    fs::write(group.join("memory.current"), "0").assured("the fixture can replace usage");
    fs::write(group.join("memory.stat"), "inactive_file 10000\n")
        .assured("the fixture can replace statistics");
    assert_eq!(
        cgroup_domains("/user.slice/one.scope", directory.path())[0].available_bytes,
        500
    );
    fs::write(group.join("memory.max"), "max").assured("the fixture can remove its limit");
    assert_eq!(
        cgroup_domains("/user.slice/one.scope", directory.path()).len(),
        1
    );
}

#[test]
fn ended_driver_memory_is_absent_and_oom_kills_come_from_the_unified_hierarchy() {
    let directory = tempfile::tempdir().assured("the process fixture can create its directory");
    let process = directory.path().join("42");
    fs::create_dir(&process).assured("the process directory can be created");
    assert_eq!(resident_in(42, directory.path()), None);
    fs::write(process.join("status"), "Name: compiler\nVmRSS: 321 kB\n")
        .assured("the process status can be written");
    assert_eq!(resident_in(42, directory.path()), Some(321 * 1024));
    fs::write(process.join("status"), "Name: compiler\nState: Z\n")
        .assured("the process status can be replaced");
    assert_eq!(resident_in(42, directory.path()), None);
    let membership = directory.path().join("membership");
    fs::write(&membership, "4:memory:/wrong\n0::/group\n")
        .assured("the membership fixture can be written");
    fs::create_dir(directory.path().join("group")).assured("the cgroup directory can be created");
    fs::write(
        directory.path().join("group/memory.events"),
        "oom 7\noom_kill 3\n",
    )
    .assured("the event counter can be written");
    assert_eq!(oom_kills(&membership, directory.path()), Some(3));
}
