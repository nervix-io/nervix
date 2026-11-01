//! The names an external system gives the entities a connector reads: topics, queues, channels,
//! subjects, consumer groups and subscriptions.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The one shape every external name shares: construction that checks the text against
//!   its entity's rule and keeps it exactly as written, comparison, display, serde and the archived
//!   form, and the typed reasons a text is not a name of one kind.
//! - **Depends on.** Nothing in Nervix.
//! - **Must not know.** Which connector or entity a name belongs to, how a connector sends it, or
//!   how Plexus quotes it. Each connector's module declares its names with `external_names!` and
//!   writes the rule each one follows.
//!
//! An external name is not a Nervix name. A Nervix name is an identifier Nervix normalises to lower
//! case; an external name is whatever the external system calls the entity, case and punctuation
//! included, so Plexus writes it as a string literal and a connector sends exactly that text.
//!
//! Decoding is construction too: the serde and archived forms hold the text as written, and their
//! decoders refuse text the rule refuses rather than producing a name no rule ever checked.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use thiserror::Error;

/// Why a text is not the external name of one kind of entity.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "external name construction returns pure validation refusals"
    )
)]
pub enum ExternalNameError {
    #[error("must not be empty")]
    Empty,
    #[error("is {actual} bytes long, longer than the {max} bytes allowed")]
    TooLong { max: usize, actual: usize },
    #[error("holds '{ch}', which it may not hold")]
    InvalidChar { ch: char },
    #[error("may not be '{name}'")]
    Reserved { name: &'static str },
    #[error("holds nothing before its '{suffix}' suffix")]
    EmptyBeforeSuffix { suffix: &'static str },
    #[error("holds an empty level separated by '{separator}'")]
    EmptyLevel { separator: char },
    #[error("names {actual} levels, but at most {max} are allowed")]
    TooManyLevels { max: usize, actual: usize },
    #[error("holds the wildcard '{wildcard}' inside a level instead of as the whole level")]
    WildcardInsideLevel { wildcard: char },
    #[error("holds the wildcard '{wildcard}' before its last level")]
    WildcardBeforeLastLevel { wildcard: char },
    #[error("begins with the reserved level '{level}'")]
    ReservedFirstLevel { level: &'static str },
    #[error("names the domain '{domain}', which is not one of {}", .known.join(", "))]
    UnknownDomain {
        domain: String,
        known: &'static [&'static str],
    },
    #[error(
        "names {levels} levels after its domain, where a qualified name names {}",
        .expected.join("/")
    )]
    QualifiedLevels {
        levels: usize,
        expected: &'static [&'static str],
    },
    #[error(
        "holds '{separator}' without naming its domain, one of {}",
        .known.join(", ")
    )]
    LevelsWithoutDomain {
        separator: char,
        known: &'static [&'static str],
    },
}

/// The characters most rules refuse: control characters, which no external system expects in a
/// name and no Plexus string literal shows legibly.
pub(crate) fn refuse_control_characters(text: &str) -> Result<(), ExternalNameError> {
    match text.chars().find(|ch| ch.is_control()) {
        Some(ch) => Err(ExternalNameError::InvalidChar { ch }),
        None => Ok(()),
    }
}

/// The checks every rule starts with: a name is not empty and fits its bound.
pub(crate) fn require_length(text: &str, max: usize) -> Result<(), ExternalNameError> {
    if text.is_empty() {
        return Err(ExternalNameError::Empty);
    }
    if text.len() > max {
        return Err(ExternalNameError::TooLong {
            max,
            actual: text.len(),
        });
    }
    Ok(())
}

/// Declares external name types, each checked by its rule.
///
/// A rule is a function from the text as written to whether it is a name of that kind. The type
/// keeps the text exactly as written, so equality, ordering and display are the text's own. Each
/// type also declares the label completion offers where a name of its kind is expected and one
/// name its rule admits.
macro_rules! external_names {
    (
        $(
            $(#[$doc:meta])*
            pub struct $Name:ident => $rule:path, label $label:literal, example $example:literal;
        )+
    ) => {
        $(
            $(#[$doc])*
            #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, ::serde::Serialize)]
            #[serde(transparent)]
            #[cfg_attr(
                nervix_lint,
                nervix::value(name, reason = "immutable validated external entity name")
            )]
            pub struct $Name(String);

            impl $Name {
                /// The label completion offers where a name of this kind is expected, and one name
                /// its rule admits.
                pub const DECLARATION: $crate::ExternalNameDeclaration =
                    $crate::ExternalNameDeclaration {
                        label: $label,
                        example: $example,
                    };

                /// Checks `raw` against this kind's rule and keeps it exactly as written.
                pub fn parse(
                    raw: &str,
                ) -> ::error_stack::Result<Self, $crate::ExternalNameError> {
                    $rule(raw).map_err(::error_stack::Report::new)?;
                    Ok(Self(raw.to_string()))
                }

                #[inline]
                pub fn as_str(&self) -> &str {
                    &self.0
                }
            }

            impl $crate::ExternalName for $Name {
                const DECLARATION: $crate::ExternalNameDeclaration = $Name::DECLARATION;

                fn parse(raw: &str) -> ::error_stack::Result<Self, $crate::ExternalNameError> {
                    $Name::parse(raw)
                }
            }

            impl ::std::fmt::Debug for $Name {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    f.debug_tuple(stringify!($Name)).field(&self.0).finish()
                }
            }

            impl ::std::fmt::Display for $Name {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    f.write_str(&self.0)
                }
            }

            impl AsRef<str> for $Name {
                #[inline]
                fn as_ref(&self) -> &str {
                    &self.0
                }
            }

            impl TryFrom<&str> for $Name {
                type Error = $crate::ExternalNameError;

                fn try_from(value: &str) -> Result<Self, Self::Error> {
                    $rule(value)?;
                    Ok(Self(value.to_string()))
                }
            }

            impl TryFrom<String> for $Name {
                type Error = $crate::ExternalNameError;

                fn try_from(value: String) -> Result<Self, Self::Error> {
                    $rule(&value)?;
                    Ok(Self(value))
                }
            }

            impl ::std::str::FromStr for $Name {
                type Err = $crate::ExternalNameError;

                fn from_str(value: &str) -> Result<Self, Self::Err> {
                    Self::try_from(value)
                }
            }

            impl<'de> ::serde::Deserialize<'de> for $Name {
                fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
                where
                    D: ::serde::Deserializer<'de>,
                {
                    let raw = String::deserialize(deserializer)?;
                    Self::try_from(raw.as_str()).map_err(<D::Error as ::serde::de::Error>::custom)
                }
            }

            /// The archived form is the name's text as an inline string, the same bytes a `String`
            /// archives to.
            impl ::rkyv::Archive for $Name {
                type Archived = ::rkyv::string::ArchivedString;
                type Resolver = ::rkyv::string::StringResolver;

                fn resolve(&self, resolver: Self::Resolver, out: ::rkyv::Place<Self::Archived>) {
                    ::rkyv::string::ArchivedString::resolve_from_str(&self.0, resolver, out);
                }
            }

            impl<S> ::rkyv::Serialize<S> for $Name
            where
                S: ::rkyv::rancor::Fallible + ?Sized,
                S::Error: ::rkyv::rancor::Source,
                str: ::rkyv::SerializeUnsized<S>,
            {
                fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
                    ::rkyv::string::ArchivedString::serialize_from_str(&self.0, serializer)
                }
            }

            impl<D> ::rkyv::Deserialize<$Name, D> for ::rkyv::string::ArchivedString
            where
                D: ::rkyv::rancor::Fallible + ?Sized,
                D::Error: ::rkyv::rancor::Source,
            {
                fn deserialize(&self, _: &mut D) -> Result<$Name, D::Error> {
                    $Name::try_from(self.as_str())
                        .map_err(<D::Error as ::rkyv::rancor::Source>::new)
                }
            }
        )+
    };
}

pub(crate) use external_names;

/// The identifier alphabet and 128-byte language bound of external table and collection paths.
/// Unlike a Nervix identity, the external text keeps its original case.
pub(crate) fn external_identifier(text: &str) -> Result<(), ExternalNameError> {
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "the Plexus table and collection locator grammar fixes the name at 1–128 \
                      ASCII bytes, as specified in docs/src/emitters.md"
        )
    )]
    const MAX_IDENTIFIER_BYTES: usize = 128;
    require_length(text, MAX_IDENTIFIER_BYTES)?;
    if let Some(ch) = text
        .chars()
        .find(|ch| !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '~' | '.')))
    {
        return Err(ExternalNameError::InvalidChar { ch });
    }
    Ok(())
}

/// A table, or a schema or database and its table: one or two nonempty parts separated by `.`,
/// each on the external identifier alphabet, together within its 128-byte bound.
pub(crate) fn qualified_table(text: &str) -> Result<(), ExternalNameError> {
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "a Plexus table locator names a table or its schema and table"
        )
    )]
    const MAX_PARTS: usize = 2;
    external_identifier(text)?;
    if text.split('.').any(str::is_empty) {
        return Err(ExternalNameError::EmptyLevel { separator: '.' });
    }
    let parts = text.split('.').count();
    if parts > MAX_PARTS {
        return Err(ExternalNameError::TooManyLevels {
            max: MAX_PARTS,
            actual: parts,
        });
    }
    Ok(())
}
