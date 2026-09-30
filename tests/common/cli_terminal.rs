//! A `nervix-cli` REPL that a scenario types into through a pseudo-terminal, the way an operator
//! uses it.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One pseudo-terminal, the CLI process attached to its slave side, the working
//!   directory that holds the REPL's history file, the task that plays the terminal on the master
//!   side, and the bounded record of what the terminal displayed.
//! - **Depends on.** Tokio's processes and descriptor readiness, and `nix` for the pseudo-terminal.
//! - **Must not know.** Scenario state, nodes, NSPL, or what the CLI prints.
//!
//! # Playing the terminal
//!
//! The REPL reads keys in raw mode and asks where its cursor is whenever it draws a prompt or
//! repaints its input, so it cannot run on pipes. The fixture opens a pseudo-terminal, hands its
//! slave side to the CLI as stdin, stdout and stderr, and plays the terminal on the master side from
//! one task: it records what the CLI writes, answers every cursor position request (`ESC [ 6 n`)
//! with the top left corner, and types the keys a scenario sends. Only that task touches the
//! master, so a typed line and a position report never interleave.
//!
//! The REPL prints the events it queued, such as server notices and subscription rows, when it
//! draws its next prompt. A wait for displayed text therefore presses Enter between looks, which on
//! an empty line draws a new prompt and submits nothing.
//!
//! # Bounds
//!
//! The record keeps the newest [`DISPLAY_RETAINED_BYTES`] bytes the CLI wrote. A failed wait quotes
//! the newest [`TRANSCRIPT_LINES`] lines of it as text: control sequences removed, and a run of
//! identical lines, such as the prompts its own Enter presses draw, shown once with its count. The
//! terminal task ends when the CLI's side of the terminal closes. Dropping the fixture aborts the
//! task and kills the CLI, so neither outlives the scenario that started them.

use std::{
    io,
    os::fd::OwnedFd,
    path::Path,
    process::{ExitStatus, Stdio},
    sync::Arc as StdArc,
    time::Duration,
};

use meticulous::OptionExt as _;
use nervix_primitives::{
    sync::{blocking::Mutex, mpsc},
    task::AbortOnDropHandle,
    time::Instant,
};
use nix::{
    fcntl::{FcntlArg, FdFlag, OFlag, fcntl},
    pty::{Winsize, openpty},
    sys::termios::Termios,
};
use tempfile::TempDir;
use tokio::{io::unix::AsyncFd, process::Child};

/// The newest bytes of terminal output a fixture retains for its assertions.
const DISPLAY_RETAINED_BYTES: usize = 256 * 1024;

/// The newest lines of the transcript a failed wait quotes.
const TRANSCRIPT_LINES: usize = 40;

/// Typed key sequences the terminal task has not written yet.
const PENDING_KEY_SEQUENCES: usize = 64;

/// How long a wait for displayed text lets the REPL draw its prompt before it looks again.
const PROMPT_INTERVAL: Duration = Duration::from_millis(250);

/// The terminal's size, which the REPL lays its prompt out in.
const WINDOW: Winsize = Winsize {
    ws_row: 40,
    ws_col: 120,
    ws_xpixel: 0,
    ws_ypixel: 0,
};

/// What the REPL writes to ask where the cursor is.
const CURSOR_POSITION_REQUEST: &[u8] = b"\x1b[6n";

/// The answer to every request: the top left corner.
const CURSOR_POSITION_REPORT: &[u8] = b"\x1b[1;1R";

/// The bytes at the end of one read that can begin a request the next read completes.
const CARRIED_REQUEST_BYTES: usize = CURSOR_POSITION_REQUEST.len() - 1;

/// The CLI's side of the terminal closed, so it reads no more keys.
#[derive(Debug, thiserror::Error)]
#[error("the CLI's terminal closed")]
pub(crate) struct TerminalClosed;

/// Why a wait for displayed text ended without it.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DisplayWaitError {
    #[error(transparent)]
    Closed(#[from] TerminalClosed),
    #[error("the terminal did not display it within {within:?}")]
    Timeout { within: Duration },
}

/// Why a wait for the CLI to exit ended without its exit status.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ExitWaitError {
    #[error("waiting for the CLI to exit failed")]
    Wait(#[source] io::Error),
    #[error("the CLI did not exit within {within:?}")]
    Timeout { within: Duration },
}

/// A running REPL and the terminal it runs on.
pub(crate) struct CliTerminal {
    keys: mpsc::Sender<Vec<u8>>,
    display: StdArc<Mutex<TerminalDisplay>>,
    _player: AbortOnDropHandle<()>,
    cli: Child,
    _working_directory: TempDir,
}

impl CliTerminal {
    /// Starts `binary` with `arguments` on a new pseudo-terminal, in a working directory of its
    /// own.
    pub(crate) fn start(binary: &Path, arguments: &[&str]) -> io::Result<Self> {
        let terminal = openpty(&WINDOW, None::<&Termios>)?;
        // Another scenario may start a process at any moment, and it must not inherit this
        // terminal.
        fcntl(&terminal.master, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC))?;
        fcntl(&terminal.slave, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC))?;
        let status = OFlag::from_bits_truncate(fcntl(&terminal.master, FcntlArg::F_GETFL)?);
        fcntl(
            &terminal.master,
            FcntlArg::F_SETFL(status | OFlag::O_NONBLOCK),
        )?;
        let working_directory = tempfile::Builder::new()
            .prefix("nervix-cli-repl-")
            .tempdir()?;
        // The command keeps this process's copies of the slave side until it is dropped at the end
        // of the block, and the terminal task sees the CLI's side close only once no copy is left.
        let cli = {
            let mut command = tokio::process::Command::new(binary);
            command
                .args(arguments)
                .current_dir(working_directory.path())
                .env("TERM", "xterm-256color")
                .stdin(Stdio::from(terminal.slave.try_clone()?))
                .stdout(Stdio::from(terminal.slave.try_clone()?))
                .stderr(Stdio::from(terminal.slave))
                .kill_on_drop(true);
            command.spawn()?
        };
        let master = AsyncFd::new(terminal.master)?;
        let (keys, typed) = mpsc::channel(PENDING_KEY_SEQUENCES);
        let display = StdArc::new(Mutex::new(TerminalDisplay::default()));
        let player = nervix_primitives::task::spawn(play_terminal(master, typed, display.clone()));
        Ok(Self {
            keys,
            display,
            _player: AbortOnDropHandle::new(player),
            cli,
            _working_directory: working_directory,
        })
    }

    /// Types `line` and presses Enter.
    pub(crate) async fn type_line(&self, line: &str) -> Result<(), TerminalClosed> {
        let mut keys = line.as_bytes().to_vec();
        keys.push(b'\r');
        self.keys.send(keys).await.map_err(|_| TerminalClosed)
    }

    /// Waits until the terminal has displayed `expected`, pressing Enter between looks so the REPL
    /// draws a prompt and prints the events it queued.
    pub(crate) async fn wait_for_display(
        &self,
        expected: &str,
        within: Duration,
    ) -> Result<(), DisplayWaitError> {
        let deadline = Instant::now() + within;
        loop {
            nervix_primitives::task::consume_budget().await;
            if self.display.lock().contains(expected) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(DisplayWaitError::Timeout { within });
            }
            self.type_line("").await?;
            nervix_primitives::time::sleep(PROMPT_INTERVAL).await;
        }
    }

    /// Waits for the CLI to exit, and says how it ended.
    pub(crate) async fn wait_for_exit(
        &mut self,
        within: Duration,
    ) -> Result<ExitStatus, ExitWaitError> {
        match nervix_primitives::time::timeout(within, self.cli.wait()).await {
            Ok(Ok(status)) => Ok(status),
            Ok(Err(error)) => Err(ExitWaitError::Wait(error)),
            Err(_) => Err(ExitWaitError::Timeout { within }),
        }
    }

    /// The newest lines the terminal displayed, for a failure message.
    pub(crate) fn transcript(&self) -> String {
        self.display.lock().transcript()
    }
}

/// What the terminal displayed, newest last, bounded by [`DISPLAY_RETAINED_BYTES`].
#[derive(Default)]
struct TerminalDisplay {
    retained: Vec<u8>,
}

impl TerminalDisplay {
    fn record(&mut self, output: &[u8]) {
        self.retained.extend_from_slice(output);
        if let Some(excess) = self.retained.len().checked_sub(DISPLAY_RETAINED_BYTES) {
            self.retained.drain(..excess);
        }
    }

    fn contains(&self, expected: &str) -> bool {
        String::from_utf8_lossy(&self.retained).contains(expected)
    }

    /// The newest [`TRANSCRIPT_LINES`] lines as text, one per line: control sequences and carriage
    /// returns removed, blank lines dropped, and a run of identical lines shown once with its count.
    fn transcript(&self) -> String {
        let text = without_control_sequences(&String::from_utf8_lossy(&self.retained));
        let mut runs: Vec<LineRun<'_>> = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Some(last) = runs.last_mut()
                && last.line == line
            {
                last.count += 1;
                continue;
            }
            runs.push(LineRun { line, count: 1 });
        }
        // Saturation is the meaning: with fewer lines than a transcript holds, it starts at the
        // first one.
        let first = runs.len().saturating_sub(TRANSCRIPT_LINES);
        let mut transcript = String::new();
        for run in runs
            .get(first..)
            .verified("the first line is at most the line count")
        {
            transcript.push_str("\n    ");
            transcript.push_str(run.line);
            if run.count > 1 {
                transcript.push_str(&format!(" (x{})", run.count));
            }
        }
        transcript
    }
}

/// One line of a transcript and how many times in a row the terminal displayed it.
struct LineRun<'text> {
    line: &'text str,
    count: usize,
}

/// `text` without its escape sequences and carriage returns. A control sequence runs from
/// `ESC [` through its final byte, and any other escape is `ESC` and the one character after it.
fn without_control_sequences(text: &str) -> String {
    let mut plain = String::with_capacity(text.len());
    let mut characters = text.chars();
    while let Some(character) = characters.next() {
        match character {
            '\u{1b}' => {
                let Some(introducer) = characters.next() else {
                    break;
                };
                if introducer == '[' {
                    for sequence in characters.by_ref() {
                        if ('@'..='~').contains(&sequence) {
                            break;
                        }
                    }
                }
            }
            '\r' => {}
            other => plain.push(other),
        }
    }
    plain
}

/// Finds the cursor position requests in the terminal's output, including one split across two
/// reads.
#[derive(Default)]
struct CursorRequests {
    carried: Vec<u8>,
}

impl CursorRequests {
    /// How many requests `output` completes.
    fn completed_by(&mut self, output: &[u8]) -> usize {
        self.carried.extend_from_slice(output);
        let requests = self
            .carried
            .windows(CURSOR_POSITION_REQUEST.len())
            .filter(|window| *window == CURSOR_POSITION_REQUEST)
            .count();
        // A request starts with the only escape byte it contains, so the bytes kept here can begin
        // a request the next read completes and never one this read already counted.
        if let Some(counted) = self.carried.len().checked_sub(CARRIED_REQUEST_BYTES) {
            self.carried.drain(..counted);
        }
        requests
    }
}

/// Plays the terminal on the master side until the CLI's side closes or the fixture is dropped:
/// records what the CLI writes, answers its cursor position requests, and types the keys a
/// scenario sends.
async fn play_terminal(
    master: AsyncFd<OwnedFd>,
    mut keys: mpsc::Receiver<Vec<u8>>,
    display: StdArc<Mutex<TerminalDisplay>>,
) {
    let mut requests = CursorRequests::default();
    loop {
        nervix_primitives::task::consume_budget().await;
        nervix_primitives::select! {
            output = read_output(&master) => {
                let Some(output) = output else {
                    return;
                };
                display.lock().record(&output);
                for _ in 0..requests.completed_by(&output) {
                    if write_keys(&master, CURSOR_POSITION_REPORT).await.is_err() {
                        return;
                    }
                }
            }
            typed = keys.recv() => {
                let Some(typed) = typed else {
                    return;
                };
                if write_keys(&master, &typed).await.is_err() {
                    return;
                }
            }
        }
    }
}

/// Reads what the CLI wrote next, or `None` once its side of the terminal closed.
async fn read_output(master: &AsyncFd<OwnedFd>) -> Option<Vec<u8>> {
    let mut buffer = [0_u8; 4096];
    loop {
        nervix_primitives::task::consume_budget().await;
        let Ok(mut ready) = master.readable().await else {
            return None;
        };
        let read = ready.try_io(|descriptor| {
            nix::unistd::read(descriptor.get_ref(), &mut buffer).map_err(io::Error::from)
        });
        match read {
            // The readiness was stale; wait for the next.
            Err(_) => {}
            // Linux reports a closed slave side as an I/O error rather than an end of file.
            Ok(Ok(0) | Err(_)) => return None,
            Ok(Ok(count)) => {
                let output = buffer
                    .get(..count)
                    .verified("a read fills at most the buffer it was given");
                return Some(output.to_vec());
            }
        }
    }
}

/// Writes `keys` to the CLI's input.
async fn write_keys(master: &AsyncFd<OwnedFd>, keys: &[u8]) -> io::Result<()> {
    let mut remaining = keys;
    while !remaining.is_empty() {
        nervix_primitives::task::consume_budget().await;
        let mut ready = master.writable().await?;
        let written = ready.try_io(|descriptor| {
            nix::unistd::write(descriptor.get_ref(), remaining).map_err(io::Error::from)
        });
        let written = match written {
            // The readiness was stale; wait for the next.
            Err(_) => continue,
            Ok(written) => written?,
        };
        remaining = remaining
            .get(written..)
            .verified("a write takes at most the bytes it was given");
    }
    Ok(())
}
