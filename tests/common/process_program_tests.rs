//! Regressions for the program a process of the scenario executable runs.
//!
//! Layer: test harness.
//! - **Owns.** The argument lists that select each program, and the commands that start one.
//! - **Depends on.** The process program owner.
//! - **Must not know.** What a server process or the lifecycle helper does.

use std::ffi::OsString;

use crate::process_program::{
    ProcessProgram, SERVER_PROGRAM, build_tools_directory, dependency_lifecycle_helper_command,
    scenario_executable, server_command,
};

fn program_of(arguments: &[&str]) -> ProcessProgram {
    ProcessProgram::of_arguments(arguments.iter().map(OsString::from))
}

#[test]
fn the_server_name_selects_the_server_whatever_arguments_follow() {
    assert_eq!(program_of(&[SERVER_PROGRAM]), ProcessProgram::Server);
    assert_eq!(
        program_of(&[SERVER_PROGRAM, "--help"]),
        ProcessProgram::Server
    );
}

#[test]
fn the_helper_name_selects_the_helper_of_the_scope_that_follows_it() {
    assert_eq!(
        program_of(&["nervix-dependency-lifecycle-helper", "lifecycle-1"]),
        ProcessProgram::DependencyLifecycleHelper {
            scope: OsString::from("lifecycle-1"),
        }
    );
}

#[test]
fn any_other_name_runs_the_suite() {
    assert_eq!(program_of(&[]), ProcessProgram::Suite);
    assert_eq!(
        program_of(&["target/debug/deps/scenarios-0123", "--tags", "@smoke"]),
        ProcessProgram::Suite
    );
    // The name is compared whole, so a path that ends in it is not the server.
    assert_eq!(
        program_of(&["target/debug/nervix-server"]),
        ProcessProgram::Suite
    );
    assert_eq!(ProcessProgram::of_this_process(), ProcessProgram::Suite);
}

#[test]
fn every_program_runs_this_executable_again() {
    let executable = scenario_executable();
    assert_eq!(server_command().as_std().get_program(), executable);
    let helper = dependency_lifecycle_helper_command("lifecycle-1");
    assert_eq!(helper.as_std().get_program(), executable);
    let arguments: Vec<_> = helper.as_std().get_args().collect();
    assert_eq!(arguments, ["lifecycle-1"]);
}

#[test]
fn the_tools_lie_one_level_above_the_executable() {
    let executable = scenario_executable();
    let tools = build_tools_directory();
    assert!(
        executable.starts_with(tools.join("deps")),
        "{} must lie in the deps directory of {}",
        executable.display(),
        tools.display()
    );
}
