//! Incomplete browser state for one Roto function declaration.
//!
//! Layer: edges.
//!
//! - **Owns.** Ordered typed arguments, return type, language, volatility, and verbatim source
//!   until the browser draft becomes the current UDF Model.
//! - **Depends on.** Shared schema type drafts and the semantic UDF Model.
//! - **Must not know.** Roto compilation, test execution, or registry application.

use std::collections::BTreeSet;

use error_stack::Report;
use nervix_models::{CreateUdf, FieldName, UdfArgument, UdfLanguage, UdfName, UdfReturn};
use thiserror::Error;

use super::schema_draft::SchemaTypeDraft;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct UdfArgumentDraft {
    pub(super) name: String,
    pub(super) ty: SchemaTypeDraft,
    pub(super) optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct UdfDraft {
    pub(super) name: String,
    pub(super) if_not_exists: bool,
    pub(super) language: UdfLanguage,
    pub(super) arguments: Vec<UdfArgumentDraft>,
    pub(super) returns: SchemaTypeDraft,
    pub(super) return_optional: bool,
    pub(super) volatile: bool,
    pub(super) code: String,
}

impl Default for UdfDraft {
    fn default() -> Self {
        Self {
            name: String::new(),
            if_not_exists: false,
            language: UdfLanguage::Roto0_13,
            arguments: Vec::new(),
            returns: SchemaTypeDraft::default(),
            return_optional: false,
            volatile: false,
            code: String::new(),
        }
    }
}

impl UdfDraft {
    pub(super) fn move_argument_up(&mut self, index: usize) {
        if index > 0 && index < self.arguments.len() {
            self.arguments.swap(index, index - 1);
        }
    }

    pub(super) fn move_argument_down(&mut self, index: usize) {
        if let Some(next) = index.checked_add(1)
            && next < self.arguments.len()
        {
            self.arguments.swap(index, next);
        }
    }

    pub(super) fn build(&self) -> error_stack::Result<CreateUdf, UdfDraftError> {
        let name =
            UdfName::parse(self.name.trim()).map_err(|_| Report::new(UdfDraftError::Name))?;
        if !(1..=8).contains(&self.arguments.len()) {
            return Err(Report::new(UdfDraftError::ArgumentCount));
        }
        let mut arguments = Vec::with_capacity(self.arguments.len());
        let mut seen = BTreeSet::new();
        for (index, argument) in self.arguments.iter().enumerate() {
            let position = index + 1;
            let name = FieldName::parse(argument.name.trim())
                .map_err(|_| Report::new(UdfDraftError::ArgumentName { position }))?;
            if !seen.insert(name.clone()) {
                return Err(Report::new(UdfDraftError::DuplicateArgument { name }));
            }
            let ty = argument
                .ty
                .build(position)
                .map_err(|error| error.change_context(UdfDraftError::ArgumentType { position }))?;
            arguments.push(UdfArgument {
                name,
                ty,
                optional: argument.optional,
            });
        }
        let ty = self
            .returns
            .build(0)
            .map_err(|error| error.change_context(UdfDraftError::ReturnType))?;
        if self.code.trim().is_empty() {
            return Err(Report::new(UdfDraftError::CodeRequired));
        }
        if self.code.len() > 64 * 1024 {
            return Err(Report::new(UdfDraftError::CodeTooLarge));
        }
        Ok(CreateUdf::new(
            name,
            self.language,
            arguments,
            UdfReturn {
                ty,
                optional: self.return_optional,
            },
            self.volatile,
            self.code.clone(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum UdfDraftError {
    #[error("UDF name is invalid")]
    Name,
    #[error("Add between one and eight typed arguments")]
    ArgumentCount,
    #[error("Argument {position} needs a valid name")]
    ArgumentName { position: usize },
    #[error("Argument {position} needs a valid type")]
    ArgumentType { position: usize },
    #[error("Argument {name} is declared more than once")]
    DuplicateArgument { name: FieldName },
    #[error("Choose a return type")]
    ReturnType,
    #[error("Enter the Roto source, including the named function and any tests")]
    CodeRequired,
    #[error("Roto source must be at most 64 KiB")]
    CodeTooLarge,
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{CreateStatement, Model, ParseAsType, Statement};
    use nervix_nspl::client_statement::{ClientStatement, parse_client_statement};

    use super::{UdfArgumentDraft, UdfDraft, UdfDraftError};

    #[test]
    fn source_quotes_tests_and_exact_signature_round_trip_through_current_model() {
        let code = "fn reflect(value: StringColumn) -> StringColumn { value }\n// \
                    \"$roto$\"\ntest accepts { accept }\n";
        let mut draft = UdfDraft {
            name: "reflect".to_string(),
            code: code.to_string(),
            volatile: true,
            return_optional: true,
            ..UdfDraft::default()
        };
        assert_eq!(
            draft
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(UdfDraftError::ArgumentCount)
        );
        let mut argument = UdfArgumentDraft {
            name: "value".to_string(),
            optional: true,
            ..UdfArgumentDraft::default()
        };
        argument.ty.scalar = Some(ParseAsType::String);
        draft.arguments.push(argument);
        draft.returns.scalar = Some(ParseAsType::String);
        let udf = draft.build().assured("complete UDF signature");
        assert_eq!(udf.code, code);
        assert!(udf.has_valid_code_hash());
        let statement = Statement::Create(CreateStatement::new(Box::new(Model::Udf(udf)), false));
        let source = statement.to_canonical_nspl().assured("UDF renders");
        assert!(source.contains("$roto_1$"));
        assert!(source.contains("VOLATILE"));
        assert_eq!(
            parse_client_statement(&source).assured("UDF parses"),
            ClientStatement::Server(statement)
        );
    }

    #[test]
    fn reordering_arguments_changes_the_declared_signature_order() {
        let mut draft = UdfDraft {
            name: "combine".to_string(),
            code: "fn combine(first: I64Column, second: I64Column) -> I64Column { first }"
                .to_string(),
            ..UdfDraft::default()
        };
        for name in ["first", "second"] {
            let mut argument = UdfArgumentDraft {
                name: name.to_string(),
                ..UdfArgumentDraft::default()
            };
            argument.ty.scalar = Some(ParseAsType::I64);
            draft.arguments.push(argument);
        }
        draft.returns.scalar = Some(ParseAsType::I64);
        draft.move_argument_up(1);
        let reordered = draft.build().assured("both arguments remain typed");
        assert_eq!(reordered.arguments[0].name.as_str(), "second");
        assert_eq!(reordered.arguments[1].name.as_str(), "first");
        draft.move_argument_down(0);
        assert_eq!(
            draft
                .build()
                .assured("original order is restored")
                .arguments[0]
                .name
                .as_str(),
            "first"
        );
    }
}
