//! How canonical NSPL writes the name of a field, a UDF or a function inside an expression.
//!
//! Layer: vocabulary.
//! - **Owns.** The spelling canonical NSPL gives such a name at each position an expression or a
//!   route construction writes one, and the words it writes only between backticks there.
//! - **Depends on.** Nothing but the name's text.
//! - **Must not know.** The parser that reads the spelling back.
//!
//! A name is written bare, as it is, wherever it reads back as that name, and between backticks
//! everywhere else. The name rule admits text a plain NSPL word cannot hold, such as `-`, `~`, `.` or
//! a leading digit, and an expression reserves the words of its own operators, literals and forms
//! wherever a keyword could stand. Every other word, a statement keyword such as `to`, `on` or `by`
//! included, is an ordinary name inside an expression.

use std::borrow::Cow;

/// Where an expression or a route construction writes a name, which decides the words that cannot
/// stand bare there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamePosition {
    /// After a scope's `.`, as in `input.<field>`, or after `udf::`. No keyword stands there, so
    /// every plain word is the name as written.
    Qualified,
    /// The name of a call or of an invocation, followed by the `(` of its arguments. A word the
    /// expression grammar reserves would read as its own keyword.
    Called,
    /// A bare field, the field a `SET` or `DEFAULT` assignment writes, or a field `INHERIT` lists.
    /// A reserved word would read as its own keyword, and inside an `ALTER` a comma before a bare
    /// operation keyword begins the next operation.
    Bare,
}

impl NamePosition {
    /// The canonical NSPL spelling of `name` at this position: the name as it is where it reads back
    /// as that name, and the name between backticks otherwise.
    pub fn spell(self, name: &str) -> Cow<'_, str> {
        if self.reads_bare(name) {
            Cow::Borrowed(name)
        } else {
            Cow::Owned(format!("`{name}`"))
        }
    }

    /// Whether `name`, written as it is at this position, reads back as that name.
    fn reads_bare(self, name: &str) -> bool {
        if !is_plain_word(name) {
            return false;
        }
        match self {
            Self::Qualified => true,
            Self::Called => !is_reserved_word(name),
            Self::Bare => !is_reserved_word(name) && !is_alter_operation_word(name),
        }
    }
}

/// Whether `name` lexes as one plain word: a letter or an underscore, then letters, digits and
/// underscores. A name is lower case, so no upper-case letter is considered.
fn is_plain_word(name: &str) -> bool {
    let mut characters = name.chars();
    let Some(first) = characters.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() && first != '_' {
        return false;
    }
    characters.all(|character| {
        character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
    })
}

/// Whether an expression reserves `word`: it reads it as the keyword of one of its operators,
/// literals or forms, or of a route construction's clauses, wherever a keyword may stand.
fn is_reserved_word(word: &str) -> bool {
    matches!(
        word,
        "where"
            | "set"
            | "inherit"
            | "all"
            | "except"
            | "leak"
            | "sensitive"
            | "invoke"
            | "as"
            | "try_cast"
            | "json_value"
            | "try_json_value"
            | "json_exists"
            | "and"
            | "or"
            | "not"
            | "true"
            | "false"
            | "null"
            | "if"
            | "case"
            | "when"
            | "then"
            | "else"
            | "end"
            | "in"
            | "between"
            | "is"
            | "distinct"
            | "from"
            | "udf"
    )
}

/// Whether `word`, written bare right after a comma inside an `ALTER`, begins the next operation
/// instead of naming a field. `SET` begins an operation too, and is already reserved.
fn is_alter_operation_word(word: &str) -> bool {
    matches!(word, "add" | "drop" | "alter" | "replace" | "rename")
}

#[cfg(test)]
mod tests {
    use super::NamePosition;

    #[test]
    fn a_plain_word_no_position_reserves_is_written_bare() {
        for name in [
            "status", "to", "on", "by", "max", "path", "output", "_", "a1_b",
        ] {
            for position in [
                NamePosition::Qualified,
                NamePosition::Called,
                NamePosition::Bare,
            ] {
                assert_eq!(position.spell(name), name, "{name} at {position:?}");
            }
        }
    }

    #[test]
    fn a_reserved_word_is_quoted_wherever_a_keyword_may_stand() {
        for name in ["end", "from", "in", "null", "set", "udf"] {
            assert_eq!(NamePosition::Qualified.spell(name), name);
            assert_eq!(NamePosition::Called.spell(name), format!("`{name}`"));
            assert_eq!(NamePosition::Bare.spell(name), format!("`{name}`"));
        }
    }

    #[test]
    fn an_alter_operation_word_is_quoted_only_as_a_bare_name() {
        for name in ["add", "drop", "alter", "replace", "rename"] {
            assert_eq!(NamePosition::Qualified.spell(name), name);
            assert_eq!(NamePosition::Called.spell(name), name);
            assert_eq!(NamePosition::Bare.spell(name), format!("`{name}`"));
        }
    }

    #[test]
    fn a_name_no_plain_word_can_hold_is_quoted_everywhere() {
        for name in ["a-b", "~x", "a.b", "9lives", "user-id"] {
            for position in [
                NamePosition::Qualified,
                NamePosition::Called,
                NamePosition::Bare,
            ] {
                assert_eq!(position.spell(name), format!("`{name}`"), "{position:?}");
            }
        }
    }
}
