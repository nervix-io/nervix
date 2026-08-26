//! Recovery of the parts of a source file that parsing discards.
//!
//! NSPL's lexer drops `//` comments, so they survive only as the text between token spans. This
//! module reads those gaps: it recovers comment blocks, counts the blank lines that separate
//! statements, and decides which statements carry a comment inside their own body.

use std::ops::Range;

use nervix_nspl::SpannedToken;

/// A piece of the text between two statements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GapItem {
    /// One or more blank lines, collapsed to a single separator.
    Blank,
    /// A whole-line comment, with its original indentation.
    Comment(String),
}

/// The text between two statements, split into the parts the formatter reproduces.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Gap {
    /// A comment that trailed the previous statement on its own line.
    pub trailing_comment: Option<String>,
    /// Whole lines of comments and blank separators, in order.
    pub items: Vec<GapItem>,
}

impl Gap {
    /// Splits the text between two statements into a trailing comment and whole lines.
    ///
    /// The first line of `gap` is the remainder of the line the previous statement ended on, and
    /// the last is the indentation of the line the next statement starts on. Neither is a line of
    /// its own, so both are handled separately from the lines in between.
    pub fn parse(gap: &str) -> Self {
        let mut segments = gap.split('\n');
        let same_line = segments.next().unwrap_or_default();
        let trailing_comment = comment_of(same_line).map(str::to_string);

        let mut lines: Vec<&str> = segments.collect();
        // The final segment is the indentation preceding the next statement, not a line.
        lines.pop();

        Self {
            trailing_comment,
            items: items_of(lines),
        }
    }

    /// Splits the text before the first statement.
    ///
    /// Nothing precedes it, so every segment is a line of its own rather than the remainder of a
    /// line some earlier statement ended on.
    pub fn parse_leading(gap: &str) -> Self {
        let mut lines: Vec<&str> = gap.split('\n').collect();
        lines.pop();

        Self {
            trailing_comment: None,
            items: items_of(lines),
        }
    }

    /// Drops a separator at the start, used where a blank line would open the file.
    pub fn without_leading_blank(mut self) -> Self {
        if self.items.first() == Some(&GapItem::Blank) {
            self.items.remove(0);
        }
        self
    }

    /// Drops a separator at the end, used where a blank line would close the file.
    pub fn without_trailing_blank(mut self) -> Self {
        if self.items.last() == Some(&GapItem::Blank) {
            self.items.pop();
        }
        self
    }
}

/// Classifies whole lines of gap text, collapsing runs of blank lines.
fn items_of(lines: Vec<&str>) -> Vec<GapItem> {
    let mut items = Vec::new();
    for line in lines {
        match comment_of(line) {
            Some(_) => items.push(GapItem::Comment(line.trim_end().to_string())),
            // Collapse a run of blank lines into a single separator.
            None if line.trim().is_empty() && items.last() != Some(&GapItem::Blank) => {
                items.push(GapItem::Blank);
            }
            // Anything else is either a blank line already accounted for, or a non-comment line,
            // which cannot occur between statements: every token belongs to a statement.
            None => {}
        }
    }
    items
}

/// Returns the comment starting on `line`, if the line has one outside a string literal.
///
/// Callers only pass text from between token spans, which no string literal can span, so a bare
/// search for the marker is accurate here.
fn comment_of(line: &str) -> Option<&str> {
    line.find("//").map(|start| &line[start..])
}

/// Reports whether `span` encloses a comment between two of its own tokens.
///
/// The check is span-driven rather than textual because `//` occurs inside ordinary values --
/// `'mqtt://…'`, `'s3://…'` -- and a textual scan would call almost every client statement
/// commented.
pub fn contains_interior_comment(
    input: &str,
    span: &Range<usize>,
    tokens: &[SpannedToken],
) -> bool {
    let interior: Vec<&SpannedToken> = tokens
        .iter()
        .filter(|token| token.span.start >= span.start && token.span.end <= span.end)
        .collect();

    interior.windows(2).any(|pair| {
        let between = &input[pair[0].span.end..pair[1].span.start];
        comment_of(between).is_some()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_leading_comment_is_a_line_not_a_trailing_comment() {
        let gap = Gap::parse_leading("// header\n\n");
        assert_eq!(gap.trailing_comment, None);
        assert_eq!(
            gap.items,
            vec![GapItem::Comment("// header".to_string()), GapItem::Blank]
        );
    }

    #[test]
    fn a_gap_of_one_newline_separates_without_a_blank_line() {
        assert_eq!(Gap::parse("\n"), Gap::default());
    }

    #[test]
    fn a_gap_of_two_newlines_keeps_one_blank_line() {
        assert_eq!(Gap::parse("\n\n").items, vec![GapItem::Blank]);
    }

    #[test]
    fn repeated_blank_lines_collapse_to_one() {
        assert_eq!(Gap::parse("\n\n\n\n\n").items, vec![GapItem::Blank]);
    }

    #[test]
    fn comment_lines_keep_their_order_and_indentation() {
        let gap = Gap::parse("\n// banner\n//   detail\n\n");
        assert_eq!(
            gap.items,
            vec![
                GapItem::Comment("// banner".to_string()),
                GapItem::Comment("//   detail".to_string()),
                GapItem::Blank,
            ]
        );
    }

    #[test]
    fn a_blank_line_before_a_comment_block_is_kept() {
        let gap = Gap::parse("\n\n// banner\n");
        assert_eq!(
            gap.items,
            vec![GapItem::Blank, GapItem::Comment("// banner".to_string())]
        );
    }

    #[test]
    fn a_comment_after_the_statement_stays_on_its_line() {
        let gap = Gap::parse(" // done\n");
        assert_eq!(gap.trailing_comment, Some("// done".to_string()));
        assert!(gap.items.is_empty());
    }

    #[test]
    fn trailing_whitespace_is_dropped_from_comment_lines() {
        let gap = Gap::parse("\n// banner   \n");
        assert_eq!(gap.items, vec![GapItem::Comment("// banner".to_string())]);
    }

    #[test]
    fn a_url_in_a_statement_is_not_an_interior_comment() {
        let input = "CREATE CLIENT c TYPE MQTT CONFIG { 'addr' = 'mqtt://127.0.0.1:1883' };";
        let tokens = nervix_nspl::lex(input).expect("must lex");
        assert!(!contains_interior_comment(
            input,
            &(0..input.len()),
            &tokens
        ));
    }

    #[test]
    fn a_comment_between_two_tokens_is_an_interior_comment() {
        let input = "CREATE RELAY orders // why\n  SCHEMA order UNBRANCHED CAPACITY 1;";
        let tokens = nervix_nspl::lex(input).expect("must lex");
        assert!(contains_interior_comment(input, &(0..input.len()), &tokens));
    }
}
