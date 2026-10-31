//! The programs one scenario executable runs as, which the `arg0` of its process selects.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The names a scenario runs this executable under, which of them a process was given,
//!   the commands that start another process of this executable, and where the build left the
//!   tools beside it.
//! - **Depends on.** The process's own arguments and the path of its running executable.
//! - **Must not know.** What a server process does, or what a scenario observes of it.
//!
//! # A real process without a separate binary
//!
//! Scenarios run their nodes in-process, as Rust values. The few whose subject is a process itself,
//! its signals, exit status, command line or crash, need a real one, and they get it by running
//! this same executable again: it links the whole server, so no build has to provide another file.
//! `current_exe` reads the path of the running file from `/proc/self/exe`, and a new process of it
//! starts from the top of `main`, which reads `arg0` before anything else and runs the program that
//! names, as a multi-call binary does. `arg0` is the parent's to choose whatever the file is called.
//! A process given the server's name calls the entry point the `nervix-server` executable calls,
//! and clap reads the same `arg0` as the program name it reports. Any other name runs the suite.

use std::{
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
};

use meticulous::{OptionExt as _, ResultExt as _};
use tokio::process::Command;

/// The `arg0` of a process that runs as the server.
pub(crate) const SERVER_PROGRAM: &str = "nervix-server";

/// The `arg0` of the process that holds a dependency container open until it is killed.
const DEPENDENCY_LIFECYCLE_HELPER_PROGRAM: &str = "nervix-dependency-lifecycle-helper";

/// What this process of the scenario executable runs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProcessProgram {
    /// The scenario suite.
    Suite,
    /// The server, from its own command line to its exit.
    Server,
    /// The dependency lifecycle helper of the named scope.
    DependencyLifecycleHelper { scope: OsString },
}

impl ProcessProgram {
    /// The program this process was started as.
    pub(crate) fn of_this_process() -> Self {
        Self::of_arguments(std::env::args_os())
    }

    /// The program that `arguments`, whose first is `arg0`, select.
    pub(crate) fn of_arguments(mut arguments: impl Iterator<Item = OsString>) -> Self {
        let Some(name) = arguments.next() else {
            return Self::Suite;
        };
        if name == OsStr::new(SERVER_PROGRAM) {
            return Self::Server;
        }
        if name != OsStr::new(DEPENDENCY_LIFECYCLE_HELPER_PROGRAM) {
            return Self::Suite;
        }
        let scope = arguments
            .next()
            .assured("a scenario starts the helper with the scope it owns");
        Self::DependencyLifecycleHelper { scope }
    }
}

/// The path of the executable this process runs.
pub(crate) fn scenario_executable() -> PathBuf {
    std::env::current_exe().assured("Linux names a running executable in /proc/self/exe")
}

/// A command that runs this executable as the server.
pub(crate) fn server_command() -> Command {
    let mut command = Command::new(scenario_executable());
    command.arg0(SERVER_PROGRAM);
    command
}

/// A command that runs this executable as the dependency lifecycle helper of `scope`.
pub(crate) fn dependency_lifecycle_helper_command(scope: &str) -> Command {
    let mut command = Command::new(scenario_executable());
    command.arg0(DEPENDENCY_LIFECYCLE_HELPER_PROGRAM).arg(scope);
    command
}

/// The directory the build leaves the tools a scenario starts in. Cargo keeps a test executable in
/// its `deps` directory, one level below them.
pub(crate) fn build_tools_directory() -> PathBuf {
    let executable = scenario_executable();
    let deps = executable
        .parent()
        .assured("an executable's path names a directory");
    let tools: &Path = deps
        .parent()
        .assured("Cargo's deps directory lies in the profile directory");
    tools.to_path_buf()
}
