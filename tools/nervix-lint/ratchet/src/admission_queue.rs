//! Repository tooling, outside the product layer order.
//! Owns: kernel-locked admission leases and their bounded queue wait.
//! Depends on: the pure admission decision, host observations and selected clocks/sleep.
//! Must not know: Cargo configuration scheduling or compiler diagnostics.

use std::{
    fs::{self, File, OpenOptions, TryLockError},
    io,
    os::unix::fs::{DirBuilderExt as _, FileExt as _, MetadataExt as _, OpenOptionsExt as _},
    path::{Path, PathBuf},
    time::Duration,
};

use error_stack::{Report, ResultExt as _};
use nervix_approx_into::ApproxInto as _;
use nervix_primitives::{thread::sleep, time::Instant};

use crate::{
    admission::{AdmissionOutcome, AdmissionRecord, Compiled, Decision, GIB, Lease, Policy},
    error::{Error, Result},
    host_memory,
    io::unix_ns,
};

const POLL: Duration = Duration::from_secs(1);
const LOCK_RETRY: Duration = Duration::from_millis(10);
const REPORT_INTERVAL: Duration = Duration::from_secs(60);

pub fn host_directory() -> PathBuf {
    // SAFETY: getuid has no preconditions and observes this process's user identity.
    PathBuf::from(format!("/tmp/nervix-analysis-admission-{}", unsafe {
        libc::getuid()
    }))
}

pub struct Queue {
    directory: PathBuf,
    leases: PathBuf,
}

impl Queue {
    pub fn open(directory: &Path) -> Result<Self> {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(directory)
            .change_context(Error::Queue {
                path: directory.into(),
            })?;
        let metadata = fs::symlink_metadata(directory).change_context(Error::Queue {
            path: directory.into(),
        })?;
        // SAFETY: getuid has no preconditions.
        if !metadata.is_dir() || metadata.uid() != unsafe { libc::getuid() } {
            return Err(
                Report::new(io::Error::from_raw_os_error(libc::EPERM)).change_context(
                    Error::Queue {
                        path: directory.into(),
                    },
                ),
            );
        }
        let leases = directory.join("leases");
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&leases)
            .change_context(Error::Queue {
                path: leases.clone(),
            })?;
        Ok(Self {
            directory: directory.into(),
            leases,
        })
    }

    /// The returned descriptor owns this decision's lock; closing it releases the lock on all exits.
    pub fn deciding(&self, deadline: Instant) -> Result<File> {
        let path = self.directory.join("queue.lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&path)
            .change_context(Error::Queue { path: path.clone() })?;
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(file),
                Err(TryLockError::WouldBlock) => {
                    if Instant::now() >= deadline {
                        return Err(Report::new(Error::QueueLocked));
                    }
                    sleep(LOCK_RETRY);
                }
                Err(TryLockError::Error(error)) => {
                    return Err(Report::new(error).change_context(Error::Queue { path }));
                }
            }
        }
    }

    pub fn enter(self, lease: Lease) -> Result<Place> {
        let temporary = self.leases.join(format!(".{}.tmp", lease.identifier));
        let path = self.leases.join(format!("{}.json", lease.identifier));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .change_context(Error::Queue {
                path: temporary.clone(),
            })?;
        file.lock().change_context(Error::Queue {
            path: temporary.clone(),
        })?;
        let publishing = match write_lease(&file, &lease) {
            Ok(()) => {
                fs::rename(&temporary, &path).change_context(Error::Queue { path: path.clone() })
            }
            Err(error) => Err(error),
        };
        if let Err(error) = publishing {
            crate::io::remove_if_present(&temporary)?;
            return Err(error);
        }
        Ok(Place {
            queue: self,
            lease,
            file,
            path,
        })
    }

    /// Called while the decision lock is held. A held lease is read; a released lease is reclaimed.
    pub fn live(&self) -> Result<Vec<Lease>> {
        let mut paths = Vec::new();
        for item in fs::read_dir(&self.leases).change_context(Error::Queue {
            path: self.leases.clone(),
        })? {
            let path = item
                .change_context(Error::Queue {
                    path: self.leases.clone(),
                })?
                .path();
            if path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                paths.push(path);
            }
        }
        paths.sort();
        let mut leases = Vec::new();
        for path in paths {
            let file = match File::open(&path) {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(Report::new(error).change_context(Error::Queue { path })),
            };
            match file.try_lock() {
                Ok(()) => crate::io::remove_if_present(&path)?,
                Err(TryLockError::WouldBlock) => {
                    let size = file
                        .metadata()
                        .change_context(Error::Queue { path: path.clone() })?
                        .len();
                    let mut bytes = vec![
                        0;
                        usize::try_from(size)
                            .change_context(Error::Queue { path: path.clone() })?
                    ];
                    file.read_exact_at(&mut bytes, 0)
                        .change_context(Error::Queue { path: path.clone() })?;
                    match serde_json::from_slice(&bytes) {
                        Ok(lease) => leases.push(lease),
                        Err(error) => eprintln!(
                            "nervix analysis: skipping the admission lease {}: {error}",
                            path.display()
                        ),
                    }
                }
                Err(TryLockError::Error(error)) => {
                    return Err(Report::new(error).change_context(Error::Queue { path }));
                }
            }
        }
        Ok(leases)
    }
}

pub struct Place {
    queue: Queue,
    pub lease: Lease,
    file: File,
    path: PathBuf,
}

impl Place {
    pub fn name_driver(&mut self, driver_pid: u32) -> Result<()> {
        self.lease.driver_pid = Some(driver_pid);
        match self.queue.deciding(Instant::now() + POLL) {
            Ok(_decision) => write_lease(&self.file, &self.lease),
            // Retaining the whole reservation until a later decision is conservative.
            Err(report) if matches!(report.current_context(), Error::QueueLocked) => Ok(()),
            Err(report) => Err(report),
        }
    }
}

impl Drop for Place {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_file(&self.path)
            && error.kind() != io::ErrorKind::NotFound
        {
            eprintln!(
                "nervix analysis: cannot remove admission lease {}: {error}",
                self.path.display()
            );
        }
        // Closing the descriptor releases the place even if its path could not be removed.
    }
}

fn write_lease(file: &File, lease: &Lease) -> Result<()> {
    let bytes = serde_json::to_vec(lease).change_context(Error::Json {
        path: "admission lease".into(),
    })?;
    file.write_all_at(&bytes, 0).change_context(Error::Queue {
        path: "admission lease".into(),
    })?;
    file.set_len(u64::try_from(bytes.len()).change_context(Error::Queue {
        path: "admission lease".into(),
    })?)
    .change_context(Error::Queue {
        path: "admission lease".into(),
    })
}

pub struct Admission {
    pub record: AdmissionRecord,
    place: Option<Place>,
}

impl Admission {
    pub fn started(&mut self, driver_pid: u32) -> Result<()> {
        if let Some(place) = &mut self.place {
            place.name_driver(driver_pid)?;
        }
        Ok(())
    }

    pub fn admit(
        policy: &Policy,
        compiled: &Compiled,
        reservation: u64,
        configuration: &str,
        root: &Path,
    ) -> Self {
        let started = Instant::now();
        match Self::wait(policy, compiled, reservation, configuration, root, started) {
            Ok(admission) => admission,
            Err(report) => {
                let waited = rounded_seconds(started.elapsed());
                eprintln!(
                    "nervix analysis: {} ({}) in the {configuration} analysis starts without host \
                     memory admission after {waited:.0} s: its queue in {} cannot be used: \
                     {report:#}",
                    compiled.crate_name,
                    compiled.kind,
                    policy.directory.display()
                );
                // The shared compilation record's external error field is rendered at publication.
                Self {
                    record: AdmissionRecord {
                        reservation_bytes: reservation,
                        outcome: AdmissionOutcome::Unavailable,
                        waited_seconds: waited,
                        left_bytes: None,
                        holders: 0,
                        error: Some(format!("{report:#}")),
                    },
                    place: None,
                }
            }
        }
    }

    fn wait(
        policy: &Policy,
        compiled: &Compiled,
        reservation: u64,
        configuration: &str,
        root: &Path,
        started: Instant,
    ) -> Result<Self> {
        let now = unix_ns();
        let lease = Lease {
            identifier: uuid::Uuid::new_v4().simple().to_string(),
            pid: std::process::id(),
            crate_name: compiled.crate_name.clone(),
            kind: compiled.kind.clone(),
            configuration: configuration.into(),
            root: root.to_string_lossy().into_owned(),
            reservation_bytes: reservation,
            cgroup: host_memory::own_cgroup(Path::new("/proc/self/cgroup")),
            enqueued_ns: now,
            polled_ns: now,
            admitted_ns: None,
            driver_pid: None,
        };
        let mut place = Queue::open(&policy.directory)?.enter(lease)?;
        let record = place.decide_until_admitted(policy, started)?;
        Ok(Self {
            record,
            place: Some(place),
        })
    }
}

impl Place {
    fn decide_until_admitted(
        &mut self,
        policy: &Policy,
        started: Instant,
    ) -> Result<AdmissionRecord> {
        let deadline = started + Duration::from_secs_f64(policy.wait_limit_seconds);
        let mut reported = None;
        loop {
            let lock = self.queue.deciding(deadline);
            let leases;
            let decision;
            let outcome;
            match lock {
                Ok(_lock) => {
                    let now = unix_ns();
                    self.lease.polled_ns = now;
                    leases = self.queue.live()?;
                    decision = Decision::decide(
                        &self.lease,
                        &leases,
                        &host_memory::domains(),
                        host_memory::resident,
                        now,
                    );
                    outcome = if decision.admitted && decision.fits {
                        Some(AdmissionOutcome::Fit)
                    } else if decision.admitted {
                        Some(AdmissionOutcome::Alone)
                    } else if Instant::now() >= deadline {
                        Some(AdmissionOutcome::WaitLimit)
                    } else {
                        None
                    };
                    if outcome.is_some() {
                        self.lease.admitted_ns = Some(now);
                    }
                    write_lease(&self.file, &self.lease)?;
                }
                Err(report) if matches!(report.current_context(), Error::QueueLocked) => {
                    leases = Vec::new();
                    decision = Decision {
                        admitted: false,
                        fits: false,
                        ahead: 0,
                        holders: Vec::new(),
                        left_bytes: None,
                    };
                    outcome = Some(AdmissionOutcome::QueueLocked);
                    self.lease.admitted_ns = Some(unix_ns());
                    write_lease(&self.file, &self.lease)?;
                }
                Err(report) => return Err(report),
            }
            let waited = rounded_seconds(started.elapsed());
            if let Some(outcome) = outcome {
                if reported.is_some() || outcome != AdmissionOutcome::Fit {
                    eprintln!("{}", admitted(&self.lease, &decision, outcome, waited));
                }
                return Ok(AdmissionRecord {
                    reservation_bytes: self.lease.reservation_bytes,
                    outcome,
                    waited_seconds: waited,
                    left_bytes: decision.left_bytes,
                    holders: decision.holders.len(),
                    error: None,
                });
            }
            let report_now = match reported {
                None => true,
                Some(previous) => Instant::now().duration_since(previous) >= REPORT_INTERVAL,
            };
            if report_now {
                eprintln!("{}", waiting(&self.lease, &decision, waited, policy));
                reported = Some(Instant::now());
            }
            drop(leases);
            sleep(POLL);
        }
    }
}

pub fn rounded_seconds(duration: Duration) -> f64 {
    (duration.as_secs_f64() * 10.0).round_ties_even() / 10.0
}
fn gib(amount: i128) -> String {
    let amount: f64 = amount.approx_into();
    let gib: f64 = GIB.approx_into();
    format!("{:.1} GiB", amount / gib)
}

fn waiting(lease: &Lease, decision: &Decision<'_>, waited: f64, policy: &Policy) -> String {
    let mut text = format!(
        "nervix analysis: {} ({}) in the {} analysis waits for host memory admission of its {} \
         reservation",
        lease.crate_name,
        lease.kind,
        lease.configuration,
        gib(i128::from(lease.reservation_bytes))
    );
    if decision.ahead > 0 {
        text.push_str(&format!(
            " behind {} older waiting compilation{}",
            decision.ahead,
            if decision.ahead == 1 { "" } else { "s" }
        ));
    }
    if !decision.holders.is_empty() {
        let names: Vec<_> = decision
            .holders
            .iter()
            .map(|holder| holder.describe())
            .collect();
        text.push_str(&format!("; admitted: {}", names.join("; ")));
    }
    if let Some(left) = decision.left_bytes {
        text.push_str(&format!("; {} left", gib(left)));
    }
    text.push_str(&format!(
        "; waited {waited:.0} s of at most {:.0} s",
        policy.wait_limit_seconds
    ));
    text
}

fn admitted(
    lease: &Lease,
    decision: &Decision<'_>,
    outcome: AdmissionOutcome,
    waited: f64,
) -> String {
    let mut text = format!(
        "nervix analysis: {} ({}) in the {} analysis ",
        lease.crate_name, lease.kind, lease.configuration
    );
    let reservation = gib(i128::from(lease.reservation_bytes));
    match outcome {
        AdmissionOutcome::QueueLocked => {
            return text
                + &format!(
                    "starts after {waited:.0} s, its wait limit, without admission: the host's \
                     admission queue stayed locked"
                );
        }
        AdmissionOutcome::WaitLimit => text.push_str(&format!(
            "starts at its wait limit after {waited:.0} s, although its {reservation} reservation \
             does not fit"
        )),
        AdmissionOutcome::Alone => text.push_str(&format!(
            "is admitted after {waited:.0} s with no other analysis compilation admitted, \
             although its {reservation} reservation does not fit"
        )),
        _ => text.push_str(&format!(
            "is admitted after waiting {waited:.0} s; its {reservation} reservation fits"
        )),
    }
    let Some(left) = decision.left_bytes else {
        return text
            + "; this platform does not report its memory, so declared compilations run one at a \
               time";
    };
    text.push_str(&format!(
        " in the {} left beside {} admitted compilation{}",
        gib(left),
        decision.holders.len(),
        if decision.holders.len() == 1 { "" } else { "s" }
    ));
    text
}

#[cfg(test)]
mod tests;
