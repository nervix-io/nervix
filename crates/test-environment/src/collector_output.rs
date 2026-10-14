//! The OpenTelemetry Collector's output, followed for as long as the suite runs its container.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** The follower of one Collector container's standard output and standard error, the
//!   transcript of everything the two printed, and the waits that look for text in it.
//! - **Depends on.** Testcontainers' log streams, Tokio's buffered reads, `memchr`'s substring
//!   search, and the primitive boundary's watch channel, timer and tasks.
//! - **Must not know.** Nervix, OTLP, or what the text a wait looks for means.
//!
//! # Following
//!
//! The Docker logs API answers a read with the output as it stood when the read began, and the
//! Collector's output grows with every record the suite exports to it. Reading the whole output
//! again for every look made each look slower as the suite went on, and a look that began just
//! before a record arrived ended without it, possibly after the wait's bound had passed. The
//! follower instead streams both outputs from the container's start and appends each chunk to the
//! transcript as Docker delivers it. A wait looks only at what arrived since its previous look, and
//! every append wakes it, so a record is seen as soon as the follower has read it.
//!
//! # Bounds
//!
//! The transcript keeps at most [`MAX_FOLLOWED_BYTES`] of each output. An output that would grow
//! past it ends as full, and the follower stops reading it. A wait searches each printed byte once,
//! plus, at each later look, the bytes one fewer than its text before the end of its previous look,
//! where an occurrence that look could not complete may start. Stopping aborts the follower and
//! waits for its task to end.

use std::{fmt, io, pin::Pin, time::Duration};

use meticulous::OptionExt as _;
use nervix_primitives::{sync::watch, task::JoinHandle, time::timeout};
use nervix_recovery::Discarded as _;
use testcontainers::{ContainerAsync, Image};
use tokio::io::{AsyncBufRead, AsyncBufReadExt as _};

/// The most bytes the transcript keeps of each output. A policy input: far more than a suite run
/// prints, small enough that a runaway Collector cannot exhaust the test process's memory.
pub const MAX_FOLLOWED_BYTES: usize = 1 << 30;

/// The most lines naming a scenario that a report of missed text keeps. A policy input: the
/// Collector prints a few dozen lines for each record, so this keeps the scenario's latest several
/// records.
pub const MISSED_TEXT_NAMING_LINES: usize = 200;

/// One followed output, as Testcontainers streams it.
type OutputReader = Pin<Box<dyn AsyncBufRead + Send>>;

/// One of the Collector's two outputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stream {
    Stdout,
    Stderr,
}

/// How the follower stopped reading one output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputEnd {
    /// Docker ended the output, as it does when the container stops.
    Closed,
    /// Reading the output failed.
    Failed {
        kind: io::ErrorKind,
        message: String,
    },
    /// The output reached [`MAX_FOLLOWED_BYTES`].
    Full,
}

/// What one output printed, and how the follower stopped reading it.
#[derive(Default)]
struct FollowedOutput {
    printed: Vec<u8>,
    end: Option<OutputEnd>,
}

impl FollowedOutput {
    /// Appends `chunk` unless it would grow the output past `limit`, which ends the output as full.
    /// Returns whether the follower keeps reading the output.
    fn append(&mut self, chunk: &[u8], limit: usize) -> bool {
        let grown = self.printed.len().checked_add(chunk.len());
        match grown {
            Some(grown) if grown <= limit => {
                self.printed.extend_from_slice(chunk);
                true
            }
            _ => {
                self.end = Some(OutputEnd::Full);
                false
            }
        }
    }

    /// Whether `text` occurs in this output where a wait that had looked at its first `looked`
    /// bytes could not yet have seen it, and marks the whole output looked at.
    fn find_since(&self, text: &[u8], looked: &mut usize) -> bool {
        // The empty text occurs in every output.
        let Some(reach) = text.len().checked_sub(1) else {
            return true;
        };
        // An occurrence the previous look could not complete ends after that look, so it starts
        // at most one byte fewer than the text before the look's end, and never before the
        // output's start.
        let overlap = reach.min(*looked);
        let from = looked
            .checked_sub(overlap)
            .assured("the overlap is at most the bytes already looked at");
        let unseen = self
            .printed
            .get(from..)
            .assured("a wait looks only at bytes already printed, and an output never shrinks");
        *looked = self.printed.len();
        memchr::memmem::find(unseen, text).is_some()
    }
}

/// Everything the Collector printed so far, by output.
#[derive(Default)]
struct Transcript {
    stdout: FollowedOutput,
    stderr: FollowedOutput,
}

impl Transcript {
    fn output_mut(&mut self, stream: Stream) -> &mut FollowedOutput {
        match stream {
            Stream::Stdout => &mut self.stdout,
            Stream::Stderr => &mut self.stderr,
        }
    }

    /// Whether either output holds `text` where `looked` had not seen it yet, marking both outputs
    /// looked at.
    fn find_since(&self, text: &[u8], looked: &mut Looked) -> bool {
        let in_stdout = self.stdout.find_since(text, &mut looked.stdout);
        let in_stderr = self.stderr.find_since(text, &mut looked.stderr);
        in_stdout || in_stderr
    }
}

/// How many bytes of each output one wait has looked at.
#[derive(Debug, Default)]
struct Looked {
    stdout: usize,
    stderr: usize,
}

/// What a wait for text in the Collector's output ended with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectorWait {
    /// An output holds the text.
    Found,
    /// The wait's bound passed before either output held the text.
    Elapsed,
    /// The follower stopped before either output held the text, so no more output arrives.
    FollowerStopped,
}

/// How far the follower has read one output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputProgress {
    pub bytes: usize,
    pub end: Option<OutputEnd>,
}

impl OutputProgress {
    fn of(output: &FollowedOutput) -> Self {
        Self {
            bytes: output.printed.len(),
            end: output.end.clone(),
        }
    }
}

impl fmt::Display for OutputProgress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} bytes, ", self.bytes)?;
        match &self.end {
            None => formatter.write_str("still followed"),
            Some(OutputEnd::Closed) => formatter.write_str("closed"),
            Some(OutputEnd::Failed { kind, message }) => {
                write!(formatter, "failed ({kind}): {message}")
            }
            Some(OutputEnd::Full) => formatter.write_str("full"),
        }
    }
}

/// How far the follower has read each of the Collector's outputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectorProgress {
    pub stdout: OutputProgress,
    pub stderr: OutputProgress,
}

impl fmt::Display for CollectorProgress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "standard output {}; standard error {}",
            self.stdout, self.stderr
        )
    }
}

/// What the followed output shows about text a wait did not find.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissedText {
    /// Whether the text arrived after the wait ended.
    pub arrived_since: bool,
    /// How far the follower has read each output.
    pub progress: CollectorProgress,
    /// The name the report looked for, such as a scenario's test identifier.
    pub name: String,
    /// The latest lines that name it, oldest first, at most [`MISSED_TEXT_NAMING_LINES`].
    pub naming: Vec<String>,
}

impl fmt::Display for MissedText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let arrival = match self.arrived_since {
            true => "the text arrived after the wait ended",
            false => "the text has not arrived",
        };
        write!(
            formatter,
            "{arrival}; followed {}; the latest Collector lines naming {}: {:?}",
            self.progress, self.name, self.naming
        )
    }
}

/// The Collector's output as one scenario observes it. Every clone observes the same transcript.
#[derive(Clone)]
pub struct CollectorOutput {
    transcript: watch::Receiver<Transcript>,
}

impl CollectorOutput {
    /// Waits at most `within` for either output to hold `text`, counting what the Collector
    /// printed before the wait began.
    pub async fn wait_for(&self, text: &str, within: Duration) -> CollectorWait {
        let watched = timeout(within, self.watch_for(text.as_bytes())).await;
        match watched {
            Ok(wait) => wait,
            Err(_) => CollectorWait::Elapsed,
        }
    }

    async fn watch_for(&self, text: &[u8]) -> CollectorWait {
        let mut transcript = self.transcript.clone();
        let mut looked = Looked::default();
        loop {
            nervix_primitives::task::consume_budget().await;
            // The look marks the transcript seen before it searches, so an append during the
            // search wakes the next look.
            let found = transcript.borrow_and_update().find_since(text, &mut looked);
            if found {
                return CollectorWait::Found;
            }
            if transcript.changed().await.is_err() {
                return CollectorWait::FollowerStopped;
            }
        }
    }

    /// Everything the Collector printed so far: its standard output, then its standard error.
    pub fn text(&self) -> String {
        let transcript = self.transcript.borrow();
        let mut text = String::from_utf8_lossy(&transcript.stdout.printed).into_owned();
        text.push_str(&String::from_utf8_lossy(&transcript.stderr.printed));
        text
    }

    /// How much of each output the follower has read, and how it stopped reading it.
    pub fn progress(&self) -> CollectorProgress {
        let transcript = self.transcript.borrow();
        CollectorProgress {
            stdout: OutputProgress::of(&transcript.stdout),
            stderr: OutputProgress::of(&transcript.stderr),
        }
    }

    /// What the output shows about `text`, which a wait did not find: whether it arrived since,
    /// how far the follower has read, and the latest lines that name `name`.
    pub fn missed(&self, text: &str, name: &str) -> MissedText {
        let progress = self.progress();
        let printed = self.text();
        let arrived_since = printed.contains(text);
        let mut naming = Vec::new();
        for line in printed.lines() {
            if line.contains(name) {
                naming.push(line);
            }
        }
        let kept = naming.len().min(MISSED_TEXT_NAMING_LINES);
        let earlier = naming
            .len()
            .checked_sub(kept)
            .assured("a report keeps at most every naming line");
        let latest = naming.split_off(earlier);
        MissedText {
            arrived_since,
            progress,
            name: name.to_string(),
            naming: latest.into_iter().map(str::to_string).collect(),
        }
    }
}

impl fmt::Debug for CollectorOutput {
    /// The progress alone: the transcript can hold far more than a diagnostic should print.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CollectorOutput")
            .field("progress", &self.progress())
            .finish()
    }
}

/// The task that follows the Collector's outputs. The environment that started the Collector owns
/// it and stops it before the Collector's teardown; dropping it aborts the task.
pub struct CollectorFollower {
    output: CollectorOutput,
    task: Option<JoinHandle<()>>,
}

impl CollectorFollower {
    /// Follows `container`'s standard output and standard error from the container's start.
    pub fn start<I: Image>(container: &ContainerAsync<I>) -> Self {
        Self::follow_readers(
            container.stdout(true),
            container.stderr(true),
            MAX_FOLLOWED_BYTES,
        )
    }

    /// Follows the two outputs `stdout` and `stderr` deliver, keeping at most `limit` bytes of
    /// each.
    fn follow_readers(stdout: OutputReader, stderr: OutputReader, limit: usize) -> Self {
        let (transcript, observed) = watch::channel(Transcript::default());
        let task = nervix_primitives::task::spawn(Self::follow(stdout, stderr, limit, transcript));
        Self {
            output: CollectorOutput {
                transcript: observed,
            },
            task: Some(task),
        }
    }

    /// The output this follower appends to.
    pub fn output(&self) -> CollectorOutput {
        self.output.clone()
    }

    /// Stops following and waits for the follower's task to end.
    pub async fn stop(mut self) {
        let task = self
            .task
            .take()
            .assured("only stopping or dropping a follower takes its task, and both consume it");
        task.abort();
        task.await
            .discarded("an aborted follower ends with a cancellation");
    }

    async fn follow(
        stdout: OutputReader,
        stderr: OutputReader,
        limit: usize,
        transcript: watch::Sender<Transcript>,
    ) {
        tokio::join!(
            Self::follow_output(stdout, Stream::Stdout, limit, &transcript),
            Self::follow_output(stderr, Stream::Stderr, limit, &transcript),
        );
    }

    async fn follow_output(
        mut reader: OutputReader,
        stream: Stream,
        limit: usize,
        transcript: &watch::Sender<Transcript>,
    ) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let chunk = match reader.fill_buf().await {
                Ok(chunk) => chunk,
                Err(error) => {
                    let end = OutputEnd::Failed {
                        kind: error.kind(),
                        message: error.to_string(),
                    };
                    transcript.send_modify(|followed| followed.output_mut(stream).end = Some(end));
                    return;
                }
            };
            if chunk.is_empty() {
                transcript.send_modify(|followed| {
                    followed.output_mut(stream).end = Some(OutputEnd::Closed);
                });
                return;
            }
            let length = chunk.len();
            let mut keeps_reading = false;
            transcript.send_modify(|followed| {
                keeps_reading = followed.output_mut(stream).append(chunk, limit);
            });
            if !keeps_reading {
                return;
            }
            reader.consume(length);
        }
    }
}

impl fmt::Debug for CollectorFollower {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CollectorFollower")
            .field("output", &self.output)
            .field("following", &self.task.is_some())
            .finish()
    }
}

impl Drop for CollectorFollower {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Cursor},
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use nervix_primitives::sync::watch;
    use tokio::io::{AsyncRead, BufReader, ReadBuf};

    use super::{
        CollectorFollower, CollectorOutput, CollectorProgress, CollectorWait, FollowedOutput,
        MISSED_TEXT_NAMING_LINES, OutputEnd, OutputProgress, OutputReader, Stream, Transcript,
    };

    /// A bound these waits never reach: each ends on what the transcript holds.
    const UNREACHED: Duration = Duration::from_secs(3_600);

    /// A log stream whose every read fails, as one does when Docker drops its connection.
    struct FailingReader;

    impl FailingReader {
        fn error() -> io::Error {
            io::Error::new(io::ErrorKind::BrokenPipe, "docker went away")
        }
    }

    impl AsyncRead for FailingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Err(Self::error()))
        }
    }

    /// A log stream that delivers `printed` and then ends, as Docker's does when the container stops.
    fn ending_reader(printed: &str) -> OutputReader {
        Box::pin(Cursor::new(printed.as_bytes().to_vec()))
    }

    fn progress(bytes: usize, end: Option<OutputEnd>) -> OutputProgress {
        OutputProgress { bytes, end }
    }

    fn followed() -> (watch::Sender<Transcript>, CollectorOutput) {
        let (transcript, observed) = watch::channel(Transcript::default());
        let output = CollectorOutput {
            transcript: observed,
        };
        (transcript, output)
    }

    fn print(transcript: &watch::Sender<Transcript>, stream: Stream, chunk: &str) {
        transcript.send_modify(|followed| {
            assert!(
                followed
                    .output_mut(stream)
                    .append(chunk.as_bytes(), usize::MAX)
            );
        });
    }

    #[test]
    fn a_look_finds_text_that_began_before_its_previous_look_ended() {
        let mut output = FollowedOutput::default();
        let mut looked = 0;
        assert!(output.append(b"Body: Str(otel-", usize::MAX));
        assert!(!output.find_since(b"otel-log-1", &mut looked));
        assert!(output.append(b"log-1)\n", usize::MAX));
        assert!(output.find_since(b"otel-log-1", &mut looked));
    }

    #[test]
    fn a_look_searches_only_what_its_previous_look_could_not_have_seen() {
        let mut output = FollowedOutput::default();
        let mut looked = 0;
        assert!(output.append(b"Body: Str(otel-log-1)\n", usize::MAX));
        assert!(output.find_since(b"otel-log-1", &mut looked));
        assert!(output.append(b"Body: Str(otel-log-2)\n", usize::MAX));
        assert!(!output.find_since(b"otel-log-1", &mut looked));
        assert_eq!(looked, output.printed.len());
    }

    #[test]
    fn a_first_look_searches_the_whole_output() {
        let mut output = FollowedOutput::default();
        let mut looked = 0;
        assert!(output.append(
            b"Body: Str(otel-log-1)\nBody: Str(otel-log-2)\n",
            usize::MAX
        ));
        assert!(output.find_since(b"otel-log-1", &mut looked));
    }

    #[test]
    fn every_output_holds_the_empty_text() {
        let output = FollowedOutput::default();
        let mut looked = 0;
        assert!(output.find_since(b"", &mut looked));
    }

    #[test]
    fn an_output_that_would_pass_its_limit_ends_full() {
        let mut output = FollowedOutput::default();
        assert!(output.append(b"1234", 6));
        assert!(!output.append(b"567", 6));
        assert_eq!(output.printed, b"1234");
        assert_eq!(output.end, Some(OutputEnd::Full));
    }

    #[nervix_primitives::test]
    async fn a_wait_finds_text_printed_before_it_began() {
        let (transcript, output) = followed();
        print(&transcript, Stream::Stderr, "Body: Str(otel-log-1)\n");
        let wait = output.wait_for("otel-log-1", UNREACHED).await;
        assert_eq!(wait, CollectorWait::Found);
    }

    #[nervix_primitives::test]
    async fn a_wait_finds_text_printed_while_it_waits() {
        let (transcript, output) = followed();
        print(&transcript, Stream::Stderr, "Body: Str(unrelated)\n");
        // The join polls the wait first, so it looks once and waits before the text is printed.
        let printer = async {
            print(&transcript, Stream::Stderr, "Body: Str(otel-log-1)\n");
        };
        let (wait, ()) = tokio::join!(output.wait_for("otel-log-1", UNREACHED), printer);
        assert_eq!(wait, CollectorWait::Found);
    }

    #[nervix_primitives::test]
    async fn a_wait_finds_text_on_either_output() {
        let (transcript, output) = followed();
        print(&transcript, Stream::Stdout, "starting\n");
        print(&transcript, Stream::Stderr, "Body: Str(otel-log-1)\n");
        assert_eq!(
            output.wait_for("starting", UNREACHED).await,
            CollectorWait::Found
        );
        assert_eq!(
            output.wait_for("otel-log-1", UNREACHED).await,
            CollectorWait::Found
        );
    }

    #[nervix_primitives::test]
    async fn a_wait_ends_when_the_follower_stops() {
        let (transcript, output) = followed();
        print(&transcript, Stream::Stderr, "Body: Str(unrelated)\n");
        drop(transcript);
        let wait = output.wait_for("otel-log-1", UNREACHED).await;
        assert_eq!(wait, CollectorWait::FollowerStopped);
    }

    #[nervix_primitives::test]
    async fn a_wait_elapses_when_no_output_holds_the_text() {
        let (transcript, output) = followed();
        print(&transcript, Stream::Stderr, "Body: Str(unrelated)\n");
        let wait = output
            .wait_for("otel-log-1", Duration::from_millis(10))
            .await;
        assert_eq!(wait, CollectorWait::Elapsed);
        drop(transcript);
    }

    #[nervix_primitives::test]
    async fn the_follower_reads_each_output_until_it_ends() {
        let follower = CollectorFollower::follow_readers(
            ending_reader("starting\n"),
            ending_reader("Body: Str(otel-log-1)\n"),
            usize::MAX,
        );
        let output = follower.output();
        assert_eq!(
            output.wait_for("otel-log-1", UNREACHED).await,
            CollectorWait::Found
        );
        assert_eq!(
            output.wait_for("absent", UNREACHED).await,
            CollectorWait::FollowerStopped
        );
        let expected = CollectorProgress {
            stdout: progress(9, Some(OutputEnd::Closed)),
            stderr: progress(22, Some(OutputEnd::Closed)),
        };
        assert_eq!(output.progress(), expected);
        assert_eq!(
            output.progress().to_string(),
            "standard output 9 bytes, closed; standard error 22 bytes, closed"
        );
        follower.stop().await;
    }

    #[nervix_primitives::test]
    async fn an_output_whose_read_fails_ends_with_the_failure() {
        let follower = CollectorFollower::follow_readers(
            ending_reader(""),
            Box::pin(BufReader::new(FailingReader)),
            usize::MAX,
        );
        let output = follower.output();
        assert_eq!(
            output.wait_for("otel-log-1", UNREACHED).await,
            CollectorWait::FollowerStopped
        );
        let failed = OutputEnd::Failed {
            kind: io::ErrorKind::BrokenPipe,
            message: "docker went away".to_string(),
        };
        assert_eq!(output.progress().stderr, progress(0, Some(failed)));
        assert_eq!(
            output.progress().stderr.to_string(),
            "0 bytes, failed (broken pipe): docker went away"
        );
        follower.stop().await;
    }

    #[nervix_primitives::test]
    async fn an_output_past_the_limit_ends_full() {
        let follower =
            CollectorFollower::follow_readers(ending_reader(""), ending_reader("0123456789"), 4);
        let output = follower.output();
        assert_eq!(
            output.wait_for("0123", UNREACHED).await,
            CollectorWait::FollowerStopped
        );
        assert_eq!(output.progress().stderr, progress(0, Some(OutputEnd::Full)));
        assert_eq!(output.progress().stderr.to_string(), "0 bytes, full");
        follower.stop().await;
    }

    #[nervix_primitives::test]
    async fn stopping_the_follower_ends_its_waits() {
        // The writers stay open, so neither output ends on its own.
        let (_stdout_writer, stdout) = tokio::io::duplex(64);
        let (_stderr_writer, stderr) = tokio::io::duplex(64);
        let follower = CollectorFollower::follow_readers(
            Box::pin(BufReader::new(stdout)),
            Box::pin(BufReader::new(stderr)),
            usize::MAX,
        );
        let output = follower.output();
        assert_eq!(
            format!("{follower:?}"),
            "CollectorFollower { output: CollectorOutput { progress: CollectorProgress { stdout: \
             OutputProgress { bytes: 0, end: None }, stderr: OutputProgress { bytes: 0, end: None \
             } } }, following: true }"
        );
        follower.stop().await;
        assert_eq!(
            output.wait_for("otel-log-1", UNREACHED).await,
            CollectorWait::FollowerStopped
        );
        assert_eq!(
            output.progress().to_string(),
            "standard output 0 bytes, still followed; standard error 0 bytes, still followed"
        );
    }

    #[nervix_primitives::test]
    async fn dropping_the_follower_ends_its_waits() {
        // The writers stay open, so neither output ends on its own.
        let (_stdout_writer, stdout) = tokio::io::duplex(64);
        let (_stderr_writer, stderr) = tokio::io::duplex(64);
        let follower = CollectorFollower::follow_readers(
            Box::pin(BufReader::new(stdout)),
            Box::pin(BufReader::new(stderr)),
            usize::MAX,
        );
        let output = follower.output();
        drop(follower);
        assert_eq!(
            output.wait_for("otel-log-1", UNREACHED).await,
            CollectorWait::FollowerStopped
        );
    }

    #[nervix_primitives::test]
    async fn a_report_of_missed_text_keeps_the_latest_lines_naming_the_scenario() {
        let (transcript, output) = followed();
        for index in 0..MISSED_TEXT_NAMING_LINES {
            let line = format!("     -> deployment.environment.name: Str(t1) #{index}\n");
            print(&transcript, Stream::Stderr, &line);
        }
        print(&transcript, Stream::Stderr, "Body: Str(otel-log-t2)\n");
        print(&transcript, Stream::Stderr, "Body: Str(otel-log-t1)\n");
        let missed = output.missed("otel-log-t1", "t1");
        assert!(missed.arrived_since);
        assert_eq!(missed.naming.len(), MISSED_TEXT_NAMING_LINES);
        assert_eq!(
            missed.naming.first().map(String::as_str),
            Some("     -> deployment.environment.name: Str(t1) #1")
        );
        assert_eq!(
            missed.naming.last().map(String::as_str),
            Some("Body: Str(otel-log-t1)")
        );
    }

    #[nervix_primitives::test]
    async fn a_report_of_missed_text_says_what_the_output_holds() {
        let (transcript, output) = followed();
        print(&transcript, Stream::Stderr, "Body: Str(unrelated)\n");
        print(
            &transcript,
            Stream::Stderr,
            "     -> service.name: Str(t1)\n",
        );
        let missed = output.missed("otel-log-t1", "t1");
        assert!(!missed.arrived_since);
        assert_eq!(
            missed.to_string(),
            "the text has not arrived; followed standard output 0 bytes, still followed; standard \
             error 51 bytes, still followed; the latest Collector lines naming t1: [\"     -> \
             service.name: Str(t1)\"]"
        );
        print(&transcript, Stream::Stderr, "Body: Str(otel-log-t1)\n");
        let arrived = output.missed("otel-log-t1", "t1");
        assert!(
            arrived
                .to_string()
                .starts_with("the text arrived after the wait ended; ")
        );
    }

    #[nervix_primitives::test]
    async fn the_text_is_standard_output_then_standard_error() {
        let (transcript, output) = followed();
        print(&transcript, Stream::Stderr, "second\n");
        print(&transcript, Stream::Stdout, "first\n");
        assert_eq!(output.text(), "first\nsecond\n");
        let progress = output.progress();
        assert_eq!(progress.stdout.bytes, 6);
        assert_eq!(progress.stderr.bytes, 7);
        assert_eq!(progress.stderr.end, None);
    }
}
