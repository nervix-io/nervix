//! Canonical formatting for NSPL source files.
//!
//! Formatting parses a file into statements, renders each one canonically, and reproduces the
//! comments and blank lines that parsing discards. The result is verified by reparsing before it
//! is returned, so a rendering defect surfaces as a refusal rather than as a rewritten file.

pub mod diagnostics;
pub mod document;

use document::{Gap, GapItem};
use nervix_models::CanonicalNsplError;
use nervix_nspl::{
    client_statement::{ClientStatement, parse_client_statement_sources, parse_client_statements},
    schema::ParseFromSourceError,
};
use thiserror::Error;

/// Why a source could not be formatted.
#[derive(Debug, Error)]
pub enum FormatError {
    #[error("the source could not be parsed")]
    Parse(#[from] ParseFromSourceError),
    #[error("the statement at line {line} could not be rendered: {source}")]
    Render {
        line: usize,
        #[source]
        source: CanonicalNsplError,
    },
    /// The formatted output did not reparse to the statements it came from.
    ///
    /// This is always a defect in the formatter, never in the input.
    #[error("formatting changed the meaning of the statement at line {line}; this is a defect")]
    Verification { line: usize },
}

/// Formats NSPL source into its canonical form.
pub fn format_source(input: &str) -> Result<String, FormatError> {
    let normalized = input.replace("\r\n", "\n");
    let formatted = render(&normalized)?;
    verify(&normalized, &formatted)?;
    Ok(formatted)
}

/// Reports whether `input` is already in canonical form.
pub fn is_formatted(input: &str) -> Result<bool, FormatError> {
    Ok(format_source(input)? == input)
}

fn render(input: &str) -> Result<String, FormatError> {
    let statements = parse_client_statement_sources(input)?;
    // Parsing above already lexed this input, so lexing cannot fail here.
    let tokens = nervix_nspl::lex(input).expect("input lexed while it was parsed");

    let mut lines: Vec<String> = Vec::new();
    let mut previous_end = 0usize;

    for (index, parsed) in statements.iter().enumerate() {
        let gap_text = &input[previous_end..parsed.span.start];
        let gap = if index == 0 {
            Gap::parse_leading(gap_text).without_leading_blank()
        } else {
            Gap::parse(gap_text)
        };
        append_gap(&mut lines, gap);

        // A statement whose body holds a comment is emitted exactly as written: the formatter
        // will not guess where the comment belongs.
        if document::contains_interior_comment(input, &parsed.span, &tokens) {
            lines.extend(parsed.source(input).lines().map(str::to_string));
        } else {
            let line = line_of(input, parsed.span.start);
            let rendered = parsed
                .statement
                .to_canonical_nspl()
                .map_err(|source| FormatError::Render { line, source })?;
            lines.extend(rendered.lines().map(str::to_string));
        }

        previous_end = parsed.span.end;
    }

    let tail = format!("{}\n", &input[previous_end..]);
    let trailing = if statements.is_empty() {
        Gap::parse_leading(&tail).without_leading_blank()
    } else {
        Gap::parse(&tail)
    };
    append_gap(&mut lines, trailing.without_trailing_blank());

    if lines.is_empty() {
        return Ok(String::new());
    }

    let mut out = lines.join("\n");
    out.push('\n');
    Ok(out)
}

/// Appends a gap's comments and separators, attaching a trailing comment to the preceding line.
fn append_gap(lines: &mut Vec<String>, gap: Gap) {
    if let Some(comment) = gap.trailing_comment {
        match lines.last_mut() {
            Some(last) => {
                last.push(' ');
                last.push_str(&comment);
            }
            None => lines.push(comment),
        }
    }

    for item in gap.items {
        match item {
            GapItem::Blank => lines.push(String::new()),
            GapItem::Comment(comment) => lines.push(comment),
        }
    }
}

/// Confirms the output parses back to exactly the statements the input held.
fn verify(input: &str, formatted: &str) -> Result<(), FormatError> {
    let before = parse_client_statements(input)?;
    let after = parse_client_statements(formatted)?;

    if before.len() != after.len() {
        return Err(FormatError::Verification { line: 1 });
    }

    for (index, (before, after)) in before.iter().zip(after.iter()).enumerate() {
        if before != after {
            let line = statement_line(input, index);
            return Err(FormatError::Verification { line });
        }
    }

    Ok(())
}

fn statement_line(input: &str, index: usize) -> usize {
    parse_client_statement_sources(input)
        .ok()
        .and_then(|statements| statements.get(index).map(|s| line_of(input, s.span.start)))
        .unwrap_or(1)
}

/// The 1-based line number containing byte `offset`.
fn line_of(input: &str, offset: usize) -> usize {
    input[..offset]
        .bytes()
        .filter(|byte| *byte == b'\n')
        .count()
        + 1
}

/// Renders a single statement, exposed so callers can format text that is not a whole file.
pub fn render_statement(statement: &ClientStatement) -> Result<String, CanonicalNsplError> {
    statement.to_canonical_nspl()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_file_formats_to_nothing() {
        assert_eq!(format_source("").expect("must format"), "");
    }

    #[test]
    fn a_file_of_only_comments_keeps_them() {
        assert_eq!(
            format_source("// header\n// more\n").expect("must format"),
            "// header\n// more\n"
        );
    }

    #[test]
    fn a_missing_trailing_newline_is_added() {
        assert_eq!(
            format_source("USE demo;").expect("must format"),
            "USE demo;\n"
        );
    }

    #[test]
    fn statements_are_normalized_and_placed_on_their_own_lines() {
        assert_eq!(
            format_source("use    demo  ;begin;").expect("must format"),
            "USE demo;\nBEGIN;\n"
        );
    }

    #[test]
    fn carriage_returns_are_normalized() {
        assert_eq!(
            format_source("USE demo;\r\nBEGIN;\r\n").expect("must format"),
            "USE demo;\nBEGIN;\n"
        );
    }

    #[test]
    fn a_single_blank_line_between_statements_is_kept() {
        assert_eq!(
            format_source("USE demo;\n\nBEGIN;\n").expect("must format"),
            "USE demo;\n\nBEGIN;\n"
        );
    }

    #[test]
    fn adjacent_statements_stay_adjacent() {
        assert_eq!(
            format_source("USE demo;\nBEGIN;\n").expect("must format"),
            "USE demo;\nBEGIN;\n"
        );
    }

    #[test]
    fn repeated_blank_lines_collapse_to_one() {
        assert_eq!(
            format_source("USE demo;\n\n\n\nBEGIN;\n").expect("must format"),
            "USE demo;\n\nBEGIN;\n"
        );
    }

    #[test]
    fn comments_between_statements_are_preserved_in_place() {
        let input = "// header\n\nUSE demo;\n\n// why we begin\nBEGIN;\n\n// trailing note\n";
        assert_eq!(format_source(input).expect("must format"), input);
    }

    #[test]
    fn a_comment_after_the_last_statement_is_preserved() {
        let input = "USE demo;\n\n// done\n";
        assert_eq!(format_source(input).expect("must format"), input);
    }

    #[test]
    fn a_comment_trailing_a_statement_stays_on_its_line() {
        let input = "USE demo; // pick the domain\nBEGIN;\n";
        assert_eq!(
            format_source(input).expect("must format"),
            "USE demo; // pick the domain\nBEGIN;\n"
        );
    }

    #[test]
    fn a_statement_holding_a_comment_is_left_exactly_as_written() {
        let input = "USE    demo;\n\nCREATE RELAY orders // keep me\n  SCHEMA order UNBRANCHED \
                     CAPACITY 1;\n";
        let formatted = format_source(input).expect("must format");

        assert!(
            formatted.starts_with("USE demo;\n"),
            "the neighbour must be formatted: {formatted}"
        );
        assert!(
            formatted
                .contains("CREATE RELAY orders // keep me\n  SCHEMA order UNBRANCHED CAPACITY 1;"),
            "the commented statement must be verbatim: {formatted}"
        );
    }

    #[test]
    fn formatting_is_idempotent() {
        let input = "// header\n\nuse   demo;\n\n// note\nbegin;\ncommit;\n\n// tail\n";
        let once = format_source(input).expect("must format");
        let twice = format_source(&once).expect("must format");
        assert_eq!(once, twice);
    }

    #[test]
    fn an_unparseable_file_is_reported() {
        let error = format_source("CREATE RELAY;").expect_err("must fail");
        assert!(matches!(error, FormatError::Parse(_)));
    }
}
