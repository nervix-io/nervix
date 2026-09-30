//! Formatting keeps the meaning of every document and reaches its canonical form in one pass.
//!
//! Layer: test harness.
//!
//! - **Owns.** The Bolero properties over whole documents: generated statements laid out with
//!   blank lines, comments and either line ending, and edited text that may no longer be NSPL.
//! - **Depends on.** The formatter, the client grammar it verifies with, and the generators of
//!   `nervix-arbitrary`.
//! - **Must not know.** Sessions, clusters or runtime execution.

use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::Statement;
use nervix_nspl::client_statement::{ClientStatement, parse_client_statements};

use crate::{FormatError, format_source, is_formatted};

/// A generated document and the comments written into it, in order.
struct Document {
    text: String,
    comments: Vec<String>,
}

/// Comment text a document is written with. None holds a line break; one holds a `//` of its own.
const COMMENT_TEXT: [&str; 6] = ["note", "", "keep me", "// nested", "é 🎉", "$s$ 'x' \"y\""];

fn statement(arbitrary: &mut Arbitrary<'_>) -> ClientStatement {
    match arbitrary.entropy().byte() % 6 {
        0 => ClientStatement::UseDomain(arbitrary.name()),
        1 => ClientStatement::BeginTransaction,
        2 => ClientStatement::CommitTransaction,
        _ => match arbitrary.statement() {
            Statement::UploadResource(upload) => ClientStatement::UploadResource(upload),
            server => ClientStatement::Server(server),
        },
    }
}

/// Canonical statements laid out as a person might write them: separated by blank lines and
/// whole-line comments, a comment trailing a statement, and every line ending in either `\n` or
/// `\r\n`, which reaches line breaks inside a statement's literals too.
fn document(arbitrary: &mut Arbitrary<'_>) -> Document {
    let count = arbitrary.entropy().count(4);
    let mut text = String::new();
    let mut comments = Vec::new();
    for _ in 0..count {
        let blank_lines = arbitrary.entropy().count(2);
        for _ in 0..blank_lines {
            text.push('\n');
        }
        if arbitrary.entropy().flag() {
            let comment = format!("// {}", arbitrary.entropy().pick(COMMENT_TEXT));
            text.push_str(&comment);
            text.push('\n');
            comments.push(comment);
        }
        let generated = statement(arbitrary);
        let rendered = generated
            .to_canonical_nspl()
            .unwrap_or_else(|error| panic!("{generated:?} must render: {error:?}"));
        text.push_str(&rendered);
        if arbitrary.entropy().flag() {
            let comment = format!("// {}", arbitrary.entropy().pick(COMMENT_TEXT));
            text.push(' ');
            text.push_str(&comment);
            comments.push(comment);
        }
        text.push('\n');
    }
    if arbitrary.entropy().flag() {
        text = text.replace('\n', "\r\n");
    }
    Document { text, comments }
}

/// Formats `text` and checks what a formatter guarantees: the statements it reads back are the
/// ones that went in, formatting its own output changes nothing, and every comment survives.
fn assert_formatting_keeps_meaning(text: &str, comments: &[String]) {
    let formatted =
        format_source(text).unwrap_or_else(|error| panic!("{text:?} must format: {error:?}"));
    let before = parse_client_statements(text)
        .unwrap_or_else(|error| panic!("{text:?} must parse: {error:?}"));
    let after = parse_client_statements(&formatted)
        .unwrap_or_else(|error| panic!("{formatted:?} must parse: {error:?}"));
    assert_eq!(
        after, before,
        "formatting {text:?} as {formatted:?} changed its statements"
    );
    let again =
        format_source(&formatted).unwrap_or_else(|error| panic!("{formatted:?}: {error:?}"));
    assert_eq!(
        again, formatted,
        "formatting is not idempotent for {text:?}"
    );
    assert!(
        is_formatted(&formatted).unwrap_or_else(|error| panic!("{formatted:?}: {error:?}")),
        "{formatted:?} is not reported as formatted"
    );
    // Each comment keeps its line and its order; formatting only drops its trailing whitespace.
    let mut lines = formatted.lines();
    for comment in comments {
        let comment = comment.trim_end();
        let trailing = format!(" {comment}");
        let found = lines
            .by_ref()
            .any(|line| line == comment || line.ends_with(&trailing));
        assert!(
            found,
            "comment {comment:?} was lost or moved formatting {text:?}: {formatted:?}"
        );
    }
}

#[test]
fn bolero_formatted_documents_keep_their_statements() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Nspl);
            let Document { text, comments } = document(&mut arbitrary);
            assert_formatting_keeps_meaning(&text, &comments);
        });
}

/// Characters an edit inserts or substitutes: the delimiters, quotes, comment markers and line
/// endings a document is read by, digits, and characters outside ASCII.
const EDIT_CHARACTERS: [char; 20] = [
    ' ', '\n', '\r', ';', ',', '(', ')', '{', '}', '\'', '"', '$', '/', '.', '=', '-', '0', '9',
    'a', 'é',
];

#[test]
fn bolero_formatter_text_is_formatted_or_rejected() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Nspl);
            let Document { text, .. } = document(&mut arbitrary);
            let mut characters = text.chars().collect::<Vec<_>>();
            let edits = arbitrary.entropy().boundary_biased(1..=3);
            for _ in 0..edits {
                let length = u64::try_from(characters.len()).expect("a length fits in u64");
                let at = usize::try_from(arbitrary.entropy().up_to(length))
                    .expect("the draw ends at a length");
                match arbitrary.entropy().byte() % 3 {
                    0 if at < characters.len() => {
                        characters.remove(at);
                    }
                    1 => characters.insert(at, arbitrary.entropy().pick(EDIT_CHARACTERS)),
                    _ if at < characters.len() => {
                        characters[at] = arbitrary.entropy().pick(EDIT_CHARACTERS);
                    }
                    _ => characters.push(arbitrary.entropy().pick(EDIT_CHARACTERS)),
                }
            }
            let edited = characters.into_iter().collect::<String>();
            match format_source(&edited) {
                Ok(_) => assert_formatting_keeps_meaning(&edited, &[]),
                // Text that parses always renders and reparses; a refusal of any other kind is a
                // defect in the formatter, never in the input.
                Err(report) => assert!(
                    matches!(report.current_context(), FormatError::Parse),
                    "{edited:?} was refused as a formatter defect: {report:?}"
                ),
            }
        });
}
