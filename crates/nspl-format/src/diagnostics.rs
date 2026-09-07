//! Rendering of parse failures as annotated source frames.

use ariadne::{Color, Label, Report, ReportKind, Source};
use nervix_nspl::schema::{Diagnostic, ParseFromSourceError};

/// Writes an annotated frame for `error` to standard error.
///
/// `origin` labels the frame, so a caret points into the file the reader named on the command
/// line rather than into anonymous text.
pub fn report(origin: &str, error: &ParseFromSourceError) {
    /// A parse failure reduced to what the frame renders: the stage that failed, the source it
    /// failed in, and the spans to point at.
    struct RenderedFailure<'error> {
        kind: &'static str,
        source: &'error str,
        diagnostics: &'error [Diagnostic],
    }

    let failure = match error {
        ParseFromSourceError::Lex {
            source,
            diagnostics,
        } => RenderedFailure {
            kind: "lex error",
            source,
            diagnostics,
        },
        ParseFromSourceError::Parse {
            source,
            diagnostics,
        } => RenderedFailure {
            kind: "parse error",
            source,
            diagnostics,
        },
    };

    let offset = failure
        .diagnostics
        .first()
        .map_or(0, |first| first.span.start);
    let mut builder =
        Report::build(ReportKind::Error, (origin, offset..offset)).with_message(failure.kind);

    for diagnostic in failure.diagnostics {
        builder = builder.with_label(
            Label::new((origin, diagnostic.span.clone()))
                .with_message(&diagnostic.message)
                .with_color(Color::Red),
        );
    }

    let _ = builder
        .finish()
        .eprint((origin, Source::from(failure.source)));
}
