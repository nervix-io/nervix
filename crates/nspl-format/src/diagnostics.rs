//! Rendering of parse failures as annotated source frames.

use ariadne::{Color, Label, Report, ReportKind, Source};
use nervix_nspl::schema::ParseFromSourceError;

/// Writes an annotated frame for `error` to standard error.
///
/// `origin` labels the frame, so a caret points into the file the reader named on the command
/// line rather than into anonymous text.
pub fn report(origin: &str, error: &ParseFromSourceError) {
    let (kind, source, diagnostics) = match error {
        ParseFromSourceError::Lex {
            source,
            diagnostics,
        } => ("lex error", source, diagnostics),
        ParseFromSourceError::Parse {
            source,
            diagnostics,
        } => ("parse error", source, diagnostics),
    };

    let offset = diagnostics.first().map_or(0, |first| first.span.start);
    let mut builder = Report::build(ReportKind::Error, (origin, offset..offset)).with_message(kind);

    for diagnostic in diagnostics {
        builder = builder.with_label(
            Label::new((origin, diagnostic.span.clone()))
                .with_message(&diagnostic.message)
                .with_color(Color::Red),
        );
    }

    let _ = builder
        .finish()
        .eprint((origin, Source::from(source.as_str())));
}
