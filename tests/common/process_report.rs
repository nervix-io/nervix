//! The report a `nervix-server` process writes when its `main` ends with an error.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Finding the report in what one launch of a process wrote, and reading the report's
//!   outermost context without the escape sequences its rendering emphasises text with.
//! - **Depends on.** How a Rust program whose `main` returns an error reports it: `Error: ` and the
//!   error's debug rendering, which for an `error-stack` report starts with its outermost context.
//! - **Must not know.** What a context means. The node-startup classification decides that.
//!
//! A node running as a process of its own says why its startup ended only through what it writes
//! and how it exits. The report is the one place that names the application error it ended with.

/// What a program writes before the debug rendering of the error its `main` returned.
const REPORT_PREFIX: &str = "Error: ";
/// The character that starts every escape sequence a rendering emphasises text with.
const ESCAPE: char = '\u{1b}';
/// The character that follows [`ESCAPE`] in a control sequence, such as the select graphic
/// rendition sequences `error-stack` writes around a context.
const CONTROL_SEQUENCE_INTRODUCER: char = '[';

/// The outermost context of the last report `log` holds, without the escape sequences it is
/// rendered with, or nothing when `log` holds no report.
///
/// A launch's process writes at most one report, as it exits, after anything else it wrote. Every
/// other line of the rendering starts with the tree characters `error-stack` draws, so only the
/// report's first line starts with the prefix.
pub(crate) fn reported_context(log: &str) -> Option<String> {
    let mut report = None;
    for line in log.lines() {
        if let Some(rendered) = line.strip_prefix(REPORT_PREFIX) {
            report = Some(rendered);
        }
    }
    let rendered = report?;
    Some(without_control_sequences(rendered))
}

/// `rendered` without its control sequences. Each one is [`ESCAPE`], the introducer, any
/// parameter and intermediate characters, and one final character from `@` to `~`. An escape
/// character that starts no control sequence is dropped on its own.
fn without_control_sequences(rendered: &str) -> String {
    let mut plain = String::with_capacity(rendered.len());
    let mut characters = rendered.chars().peekable();
    while let Some(character) = characters.next() {
        if character != ESCAPE {
            plain.push(character);
            continue;
        }
        if characters
            .next_if_eq(&CONTROL_SEQUENCE_INTRODUCER)
            .is_none()
        {
            continue;
        }
        for sequence_character in characters.by_ref() {
            if ('@'..='~').contains(&sequence_character) {
                break;
            }
        }
    }
    plain
}
