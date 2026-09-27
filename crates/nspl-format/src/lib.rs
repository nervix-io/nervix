//! Canonical formatting for NSPL source files.
//!
//! Formatting parses a file into statements, renders each one canonically, and reproduces the
//! comments and blank lines that parsing discards. The result is verified by reparsing before it
//! is returned, so a rendering defect surfaces as a refusal rather than as a rewritten file.
//!
//! Layer: edges.
//!
//! - **Owns.** Canonical rendering of a source file, the comments and blank lines parsing discards,
//!   and the reparse that turns a rendering defect into a refusal.
//! - **Depends on.** The language layer — an edge may name the parser — and the vocabulary's
//!   canonical rendering of a Model.
//! - **Must not know.** The server. Formatting is source in, source out.

pub mod diagnostics;
pub mod document;

use document::{Gap, GapItem};
use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_models::CanonicalNsplError;
use nervix_nspl::client_statement::{
    ClientStatement, parse_client_statement_sources, parse_client_statements,
};
use thiserror::Error;

/// Why a source could not be formatted.
///
/// Each variant is the formatter's context over the report of the owner that refused: the
/// language's [`ParseFromSourceError`](nervix_nspl::schema::ParseFromSourceError) beneath `Parse`,
/// the vocabulary's [`CanonicalNsplError`] beneath `Render`. A caller that renders the refusal
/// reads that typed cause from the report instead of from a copy held here.
#[derive(Debug, Error)]
pub enum FormatError {
    /// The source did not lex or parse; the language's report beneath locates every diagnostic.
    #[error("the source could not be parsed")]
    Parse,
    /// A statement has no canonical spelling; the vocabulary's report beneath says why.
    #[error("the statement at line {line} could not be rendered")]
    Render { line: usize },
    /// The formatted output did not reparse to the statements it came from.
    ///
    /// This is always a defect in the formatter, never in the input. When the output did not parse
    /// at all, the language's report beneath locates the failure in the formatted text.
    #[error("formatting changed the meaning of the statement at line {line}; this is a defect")]
    Verification { line: usize },
}

/// Formats NSPL source into its canonical form.
pub fn format_source(input: &str) -> error_stack::Result<String, FormatError> {
    let normalized = input.replace("\r\n", "\n");
    let formatted = render(&normalized)?;
    verify(&normalized, &formatted)?;
    Ok(formatted)
}

/// Reports whether `input` is already in canonical form.
pub fn is_formatted(input: &str) -> error_stack::Result<bool, FormatError> {
    Ok(format_source(input)? == input)
}

fn render(input: &str) -> error_stack::Result<String, FormatError> {
    let statements = parse_client_statement_sources(input).change_context(FormatError::Parse)?;
    // Parsing above already lexed this input, so lexing cannot fail here.
    let tokens = nervix_nspl::lex(input).verified("parsing above lexed this same input");

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
                .change_context(FormatError::Render { line })?;
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
///
/// Output that does not parse at all is a rendering defect like any other, not a fault in the
/// input. Neither it nor a changed statement count points at one statement, so both are reported at
/// the first line.
fn verify(input: &str, formatted: &str) -> error_stack::Result<(), FormatError> {
    let before = parse_client_statements(input).change_context(FormatError::Parse)?;
    let after =
        parse_client_statements(formatted).change_context(FormatError::Verification { line: 1 })?;

    if before.len() != after.len() {
        return Err(Report::new(FormatError::Verification { line: 1 }));
    }

    for (index, (before, after)) in before.iter().zip(after.iter()).enumerate() {
        if before != after {
            let line = statement_line(input, index);
            return Err(Report::new(FormatError::Verification { line }));
        }
    }

    Ok(())
}

fn statement_line(input: &str, index: usize) -> usize {
    let Ok(statements) = parse_client_statement_sources(input) else {
        return 1;
    };
    let Some(statement) = statements.get(index) else {
        return 1;
    };
    line_of(input, statement.span.start)
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
pub fn render_statement(
    statement: &ClientStatement,
) -> error_stack::Result<String, CanonicalNsplError> {
    statement.to_canonical_nspl()
}

#[cfg(test)]
mod tests {
    use nervix_nspl::schema::ParseFromSourceError;

    use super::*;

    #[test]
    fn a_single_statement_renders_in_canonical_form() {
        let statement = nervix_nspl::client_statement::parse_client_statement("use    demo  ;")
            .expect("must parse");
        assert_eq!(
            render_statement(&statement).expect("must render"),
            "USE demo;"
        );
    }

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
        assert!(matches!(error.current_context(), FormatError::Parse));

        let rejection = error
            .downcast_ref::<ParseFromSourceError>()
            .expect("the parser's report stays beneath the formatter's context");
        let ParseFromSourceError::Parse { diagnostics, .. } = rejection else {
            panic!("the statement lexes, so parsing rejects it: {rejection:?}");
        };
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].span, 12..12);
        assert!(
            diagnostics[0].message.starts_with("expected relay_name"),
            "{}",
            diagnostics[0].message
        );
    }

    #[test]
    fn an_unlexable_file_is_reported_at_the_lex_stage() {
        let error = format_source("USE 'demo;").expect_err("must fail");
        assert!(matches!(error.current_context(), FormatError::Parse));

        let rejection = error
            .downcast_ref::<ParseFromSourceError>()
            .expect("the lexer's report stays beneath the formatter's context");
        assert!(
            matches!(rejection, ParseFromSourceError::Lex { .. }),
            "an unterminated string is a lex failure: {rejection:?}"
        );
        assert!(!rejection.diagnostics().is_empty());
    }

    #[test]
    fn verification_rejects_output_that_changes_a_statement() {
        let error = verify("USE demo;\nBEGIN;\n", "USE demo;\nCOMMIT;\n").expect_err("must refuse");
        assert!(matches!(
            error.current_context(),
            FormatError::Verification { line: 2 }
        ));
    }

    #[test]
    fn verification_rejects_output_that_changes_the_statement_count() {
        let error = verify("USE demo;\n", "USE demo;\nBEGIN;\n").expect_err("must refuse");
        assert!(matches!(
            error.current_context(),
            FormatError::Verification { line: 1 }
        ));
    }

    #[test]
    fn verification_reports_unparseable_output_as_a_defect() {
        let error = verify("USE demo;\n", "USE;\n").expect_err("must refuse");
        assert!(matches!(
            error.current_context(),
            FormatError::Verification { line: 1 }
        ));
        assert!(
            error.contains::<ParseFromSourceError>(),
            "the reparse failure stays beneath the defect: {error:?}"
        );
    }
}
