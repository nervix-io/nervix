//! Repository tooling, outside the product layer order.
//! Owns: the analysis driver's lifetime, inherited descriptors, signals and compilation record.
//! Depends on: OS process/signal primitives, admission and evidence publication.
//! Must not know: diagnostic policy, report reuse or product graph execution.

use std::{
    ffi::{OsStr, OsString},
    mem::MaybeUninit,
    os::unix::process::{CommandExt as _, ExitStatusExt as _},
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
};

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::time::Instant;
use nix::{
    sys::signal::{self, SigSet, SigmaskHow, Signal},
    unistd::Pid,
};

use crate::{
    admission::{Compiled, Policy},
    admission_queue::{Admission, rounded_seconds},
    error::{Error, Result},
    evidence::{Compilation, Ending},
    host_memory, io,
};

pub fn option_values(arguments: &[OsString], option: &str) -> Vec<String> {
    let mut values = Vec::new();
    for (index, argument) in arguments.iter().enumerate() {
        let argument = argument.to_string_lossy();
        if argument == option {
            if let Some(value) = arguments.get(index + 1) {
                values.push(value.to_string_lossy().into_owned());
            }
        } else if option.starts_with("--") {
            if let Some(value) = argument.strip_prefix(&format!("{option}=")) {
                values.push(value.into());
            }
        } else if let Some(value) = argument.strip_prefix(option) {
            values.push(value.into());
        }
    }
    values
}

pub struct CompilationArguments {
    pub crate_name: Option<String>,
    pub crate_types: Vec<String>,
    pub extra_filename: String,
    pub source: Option<PathBuf>,
}

impl CompilationArguments {
    pub fn parse(arguments: &[OsString]) -> Self {
        let crate_name = option_values(arguments, "--crate-name").into_iter().next();
        let mut crate_types = Vec::new();
        for value in option_values(arguments, "--crate-type") {
            crate_types.extend(value.split(',').map(str::to_owned));
        }
        let mut extra_filename = String::new();
        for value in option_values(arguments, "-C") {
            if let Some(value) = value.strip_prefix("extra-filename=") {
                extra_filename = value.into();
            }
        }
        let source = arguments
            .iter()
            .find(|argument| argument.to_string_lossy().ends_with(".rs"))
            .map(PathBuf::from);
        Self {
            crate_name,
            crate_types,
            extra_filename,
            source,
        }
    }

    fn admit(&self) -> Result<Option<Admission>> {
        let Some(name) = &self.crate_name else {
            return Ok(None);
        };
        if self.crate_types.is_empty() {
            return Ok(None);
        }
        let policy = std::env::var("NERVIX_LINT_ADMISSION").change_context(Error::Driver)?;
        let policy: Policy = serde_json::from_str(&policy).change_context(Error::Json {
            path: "NERVIX_LINT_ADMISSION".into(),
        })?;
        let compiled = Compiled::new(name.clone(), &self.crate_types);
        let Some(reservation) = policy.reservation(&compiled) else {
            return Ok(None);
        };
        let configuration =
            std::env::var("NERVIX_LINT_CONFIGURATION").change_context(Error::Driver)?;
        let root =
            std::env::var_os("NERVIX_LINT_ROOT").ok_or_else(|| Report::new(Error::Driver))?;
        Ok(Some(Admission::admit(
            &policy,
            &compiled,
            reservation,
            &configuration,
            Path::new(&root),
        )))
    }
}

struct Forwarding {
    previous: SigSet,
    waited: SigSet,
}

impl Forwarding {
    fn install() -> Result<Self> {
        let mut waited = SigSet::empty();
        for signal in [
            Signal::SIGHUP,
            Signal::SIGINT,
            Signal::SIGQUIT,
            Signal::SIGTERM,
            Signal::SIGCHLD,
        ] {
            waited.add(signal);
        }
        let previous = waited
            .thread_swap_mask(SigmaskHow::SIG_BLOCK)
            .change_context(Error::Driver)?;
        Ok(Self { previous, waited })
    }

    fn wait(&self, child: &mut std::process::Child) -> Result<ExitStatus> {
        loop {
            if let Some(status) = child.try_wait().change_context(Error::Driver)? {
                return Ok(status);
            }
            // SIGCHLD makes child completion an event, without polling, a thread or an atomic.
            let received = self.waited.wait().change_context(Error::Driver)?;
            if received != Signal::SIGCHLD {
                let pid = Pid::from_raw(
                    i32::try_from(child.id()).assured("a process ID fits pid_t on the host"),
                );
                match signal::kill(pid, received) {
                    Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
                    Err(error) => return Err(Report::new(error).change_context(Error::Driver)),
                }
            }
        }
    }
}

impl Drop for Forwarding {
    fn drop(&mut self) {
        if let Err(error) = self.previous.thread_set_mask() {
            eprintln!("nervix analysis: cannot restore the wrapper's signal mask: {error}");
        }
    }
}

pub enum Entry {
    Cargo,
    Explicit,
}

impl Entry {
    fn collect(&self, arguments: &[OsString]) -> Result<()> {
        if matches!(self, Self::Cargo) && collecting() {
            let executable = std::env::current_exe().change_context(Error::Driver)?;
            let mut collected = native_command(executable.as_os_str());
            // The explicit entry records no second bootstrap. exec preserves Cargo's PID,
            // descriptors and signal dispositions while the collector selects runtime profiles.
            let error = collected.arg("wrap").args(arguments).exec();
            return Err(Report::new(error).change_context(Error::Driver));
        }
        Ok(())
    }
}

fn collecting() -> bool {
    std::env::var_os("NERVIX_NATIVE_COVERAGE_ATTEMPT").is_some_and(|value| !value.is_empty())
}

fn native_command(executable: &OsStr) -> Command {
    let root = match std::env::var_os("NERVIX_RATCHET_ROOT") {
        Some(root) => PathBuf::from(root),
        None => Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(3)
            .assured("the crate is tools/nervix-lint/ratchet")
            .to_owned(),
    };
    let mut command = Command::new("python3");
    command
        .arg(root.join("scripts/native_coverage.py"))
        .arg("exec")
        .arg(executable);
    command
}

pub fn command(arguments: &[OsString]) -> Result<Command> {
    let driver =
        std::env::var_os("NERVIX_LINT_DRIVER").ok_or_else(|| Report::new(Error::Driver))?;
    let mut command = if collecting() {
        native_command(&driver)
    } else {
        Command::new(driver)
    };
    command.args(arguments);
    Ok(command)
}

struct CompletedDriver {
    ending: Ending,
    forwarding: Forwarding,
}

struct Driver(std::process::Child);

impl Drop for Driver {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(Some(_))) {
            return;
        }
        // Error exits must reap the driver before the earlier-created admission place drops.
        if let Err(error) = self.0.kill()
            && error.raw_os_error() != Some(libc::ESRCH)
        {
            eprintln!("nervix analysis: cannot stop the driver during wrapper cleanup: {error}");
        }
        if let Err(error) = self.0.wait() {
            eprintln!("nervix analysis: cannot reap the driver during wrapper cleanup: {error}");
        }
    }
}

/// Owns the wrapper's whole process lifetime, including its final signal mask. Pending forwarded
/// signals cannot replace an already observed driver ending between publication and process exit.
pub fn execute(arguments: &[OsString], entry: Entry) -> Result<std::convert::Infallible> {
    entry.collect(arguments)?;
    let completed = run(arguments)?;
    let _forwarding = completed.forwarding;
    let status = end_like(completed.ending)?;
    std::process::exit(status)
}

fn run(arguments: &[OsString]) -> Result<CompletedDriver> {
    let compiled = CompilationArguments::parse(arguments);
    let mut admitted = compiled.admit()?;
    let membership = Path::new("/proc/self/cgroup");
    let hierarchy = Path::new("/sys/fs/cgroup");
    let kills_before = host_memory::oom_kills(membership, hierarchy);
    let started_unix_ns = io::unix_ns();
    let started = Instant::now();
    let forwarding = Forwarding::install()?;
    let parent = i32::try_from(std::process::id()).assured("a process ID fits pid_t on the host");
    let previous = forwarding.previous;
    let mut command = command(arguments)?;
    // SAFETY: this child hook only calls async-signal-safe OS primitives; it neither allocates nor
    // locks. It restores the inherited signal mask and ties the child's lifetime to this wrapper.
    unsafe {
        command.pre_exec(move || {
            signal::sigprocmask(SigmaskHow::SIG_SETMASK, Some(&previous), None)
                .map_err(std::io::Error::from)?;
            #[cfg(target_os = "linux")]
            {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::getppid() != parent && libc::kill(libc::getpid(), libc::SIGKILL) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    let mut child = Driver(command.spawn().change_context(Error::Driver)?);
    if let Some(admission) = &mut admitted {
        admission.started(child.0.id())?;
    }
    let status = forwarding.wait(&mut child.0)?;
    let admission_record = admitted.as_ref().map(|admission| admission.record.clone());
    drop(admitted);
    let kills_after = host_memory::oom_kills(membership, hierarchy);
    let oom_kills = match (kills_before, kills_after) {
        (Some(before), Some(after)) => Some(i128::from(after) - i128::from(before)),
        _ => None,
    };
    let ending = ending(status);
    let record = Compilation {
        crate_name: compiled.crate_name,
        crate_types: compiled.crate_types,
        extra_filename: compiled.extra_filename,
        source: compiled.source,
        ending: ending.clone(),
        peak_resident_bytes: peak_resident_bytes()?,
        started_unix_ns,
        seconds: rounded_seconds(started.elapsed()),
        oom_kills,
        admission: admission_record,
    };
    let destination = match publish(&record) {
        Ok(path) => path,
        Err(error) => {
            eprintln!(
                "nervix analysis: cannot write the compilation record of {:?}: {error:#}",
                record.crate_name
            );
            None
        }
    };
    if let Ending::Signal { signal, name } = &ending {
        eprintln!(
            "nervix analysis: the compilation of {:?} was terminated by signal {signal} ({}); \
             compilation record: {destination:?}",
            record.crate_name,
            name.as_deref().unwrap_or("unnamed")
        );
    }
    Ok(CompletedDriver { ending, forwarding })
}

pub fn ending(status: ExitStatus) -> Ending {
    match status.code() {
        Some(status) => Ending::Exit { status },
        None => {
            let signal = status
                .signal()
                .assured("a reaped child either exited or was terminated by a signal");
            let name = match Signal::try_from(signal) {
                Ok(signal) => Some(signal.to_string()),
                Err(_) => None,
            };
            Ending::Signal { signal, name }
        }
    }
}

fn peak_resident_bytes() -> Result<u64> {
    let mut usage = MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage writes the provided rusage on success; it is read only after success.
    let result = unsafe { libc::getrusage(libc::RUSAGE_CHILDREN, usage.as_mut_ptr()) };
    if result != 0 {
        return Err(Report::new(std::io::Error::last_os_error()).change_context(Error::Driver));
    }
    // SAFETY: the successful getrusage initialized every field.
    let usage = unsafe { usage.assume_init() };
    let peak = u64::try_from(usage.ru_maxrss).change_context(Error::Driver)?;
    #[cfg(target_os = "macos")]
    {
        Ok(peak)
    }
    #[cfg(not(target_os = "macos"))]
    {
        peak.checked_mul(1024)
            .ok_or_else(|| Report::new(Error::Driver))
    }
}

pub fn publish(record: &Compilation) -> Result<Option<PathBuf>> {
    let reports = std::env::var_os("NERVIX_LINT_REPORTS");
    publish_in(reports.as_deref(), record)
}

fn publish_in(reports: Option<&OsStr>, record: &Compilation) -> Result<Option<PathBuf>> {
    let Some(reports) = reports else {
        return Ok(None);
    };
    let Some(name) = &record.crate_name else {
        return Ok(None);
    };
    if reports == OsStr::new("") || name.is_empty() {
        return Ok(None);
    }
    let destination = PathBuf::from(reports)
        .join("compilations")
        .join(format!("{name}{}.json", record.extra_filename));
    io::atomic_json(&destination, record)?;
    Ok(Some(destination))
}

#[cfg(test)]
mod tests;

fn end_like(ending: Ending) -> Result<i32> {
    match ending {
        Ending::Exit { status } => Ok(status),
        Ending::Signal { signal: number, .. } => {
            if let Ok(signal) = Signal::try_from(number)
                && !matches!(signal, Signal::SIGKILL | Signal::SIGSTOP)
            {
                // SAFETY: a default disposition has no callback or borrowed state.
                unsafe { signal::signal(signal, signal::SigHandler::SigDfl) }
                    .change_context(Error::Driver)?;
            }
            let mut signals = SigSet::empty();
            if let Ok(signal) = Signal::try_from(number) {
                signals.add(signal);
            }
            signals.thread_unblock().change_context(Error::Driver)?;
            // SAFETY: this process owns its PID; kill preserves the child's exact signal number.
            if unsafe { libc::kill(libc::getpid(), number) } != 0 {
                return Err(
                    Report::new(std::io::Error::last_os_error()).change_context(Error::Driver)
                );
            }
            Ok(128_i32
                .checked_add(number)
                .assured("a host signal number is below the signed exit-status bound"))
        }
    }
}
