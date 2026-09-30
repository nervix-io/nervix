//! Ordered output-route drafts for a visual ingestor.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser-local route construction, branch keys, flush and message error selections,
//!   and conversion of each complete route to the current semantic Model.
//! - **Depends on.** Typed route Models, expression parsing, and selected browser references.
//! - **Must not know.** Relay execution, registry state, or connector acknowledgements.

use error_stack::{Report, ResultExt as _};
use nervix_models::{
    Assignment, AssignmentTarget, BranchName, BuiltinFunctionName, FieldName, FlushPolicy,
    Inheritance, InheritedField, Invocation, MessageErrorPolicy, ModelKind, NodeRef, OutputBranch,
    ProcessorOutput, RelayName, RouteConstruction,
};
use nervix_nspl::parse_expression;
use thiserror::Error;

use super::SelectedReference;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct AssignmentDraft {
    pub(super) field: Option<SelectedReference<FieldName>>,
    pub(super) expression: String,
}

impl AssignmentDraft {
    pub(super) fn selected(field: FieldName) -> Self {
        Self {
            field: Some(SelectedReference::chosen(field)),
            expression: String::new(),
        }
    }

    pub(super) fn build(&self) -> error_stack::Result<Assignment, IngestRouteDraftError> {
        let field = match &self.field {
            Some(field) => field
                .current_name()
                .ok_or_else(|| Report::new(IngestRouteDraftError::FieldChanged))?,
            None => return Err(Report::new(IngestRouteDraftError::Field)),
        };
        let value = parse_expression(self.expression.trim())
            .change_context(IngestRouteDraftError::Expression)?;
        Ok(Assignment {
            target: AssignmentTarget::bare(field.clone()),
            value,
        })
    }

    pub(super) fn invalidate(&mut self) {
        if let Some(field) = &mut self.field {
            field.invalidate();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct InvocationDraft {
    pub(super) function: String,
    pub(super) arguments: Vec<String>,
}

impl InvocationDraft {
    fn build(&self) -> error_stack::Result<Invocation, IngestRouteDraftError> {
        let function = BuiltinFunctionName::parse(self.function.trim())
            .map_err(|_| Report::new(IngestRouteDraftError::InvocationFunction))?;
        let mut arguments = Vec::new();
        for argument in &self.arguments {
            arguments.push(
                parse_expression(argument.trim())
                    .change_context(IngestRouteDraftError::InvocationArgument)?,
            );
        }
        Ok(Invocation {
            function,
            arguments,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) enum InheritDraft {
    #[default]
    Unselected,
    None,
    All,
    AllExcept(Vec<SelectedReference<FieldName>>),
    Fields(Vec<InheritedFieldDraft>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct InheritedFieldDraft {
    pub(super) field: SelectedReference<FieldName>,
    pub(super) leak_sensitive: bool,
}

impl InheritDraft {
    pub(super) fn key(&self) -> &'static str {
        match self {
            Self::Unselected => "unselected",
            Self::None => "none",
            Self::All => "all",
            Self::AllExcept(_) => "all-except",
            Self::Fields(_) => "fields",
        }
    }

    pub(super) fn choose(&mut self, key: &str) {
        *self = match key {
            "none" => Self::None,
            "all" => Self::All,
            "all-except" => Self::AllExcept(Vec::new()),
            "fields" => Self::Fields(Vec::new()),
            _ => return,
        };
    }

    pub(super) fn add_field(&mut self, field: FieldName) {
        match self {
            Self::AllExcept(fields) => fields.push(SelectedReference::chosen(field)),
            Self::Fields(fields) => fields.push(InheritedFieldDraft {
                field: SelectedReference::chosen(field),
                leak_sensitive: false,
            }),
            Self::Unselected | Self::None | Self::All => {}
        }
    }

    pub(super) fn invalidate(&mut self) {
        match self {
            Self::AllExcept(fields) => {
                for field in fields {
                    field.invalidate();
                }
            }
            Self::Fields(fields) => {
                for field in fields {
                    field.field.invalidate();
                }
            }
            Self::Unselected | Self::None | Self::All => {}
        }
    }

    fn build(&self) -> error_stack::Result<Option<Inheritance>, IngestRouteDraftError> {
        match self {
            Self::Unselected => Err(Report::new(IngestRouteDraftError::Inheritance)),
            Self::None => Ok(None),
            Self::All => Ok(Some(Inheritance::All)),
            Self::AllExcept(fields) => {
                if fields.is_empty() {
                    return Err(Report::new(IngestRouteDraftError::InheritedFields));
                }
                let mut selected = Vec::new();
                for field in fields {
                    selected.push(
                        field
                            .current_name()
                            .ok_or_else(|| Report::new(IngestRouteDraftError::FieldChanged))?
                            .clone(),
                    );
                }
                Ok(Some(Inheritance::AllExcept(selected)))
            }
            Self::Fields(fields) => {
                if fields.is_empty() {
                    return Err(Report::new(IngestRouteDraftError::InheritedFields));
                }
                let mut selected = Vec::new();
                for field in fields {
                    selected.push(InheritedField {
                        field: field
                            .field
                            .current_name()
                            .ok_or_else(|| Report::new(IngestRouteDraftError::FieldChanged))?
                            .clone(),
                        leak_sensitive: field.leak_sensitive,
                    });
                }
                Ok(Some(Inheritance::Fields(selected)))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) enum RouteBranchDraft {
    #[default]
    Unselected,
    Preserve,
    Unbranched,
    Branched {
        branch: Option<SelectedReference<BranchName>>,
        assignments: Vec<AssignmentDraft>,
    },
}

impl RouteBranchDraft {
    pub(super) fn choose_preserve(&mut self) {
        *self = Self::Preserve;
    }

    pub(super) fn choose_unbranched(&mut self) {
        *self = Self::Unbranched;
    }

    pub(super) fn choose_branched(&mut self) {
        if !matches!(self, Self::Branched { .. }) {
            *self = Self::Branched {
                branch: None,
                assignments: Vec::new(),
            };
        }
    }

    pub(super) fn select_branch(&mut self, node: &NodeRef) {
        if node.kind != ModelKind::Branch {
            return;
        }
        if let Self::Branched {
            branch,
            assignments,
        } = self
        {
            let selected = BranchName::from(&node.identifier);
            if branch.as_ref().and_then(SelectedReference::current_name) != Some(&selected) {
                for assignment in assignments {
                    assignment.invalidate();
                }
            }
            *branch = Some(SelectedReference::chosen(selected));
        }
    }

    pub(super) fn current_branch(&self) -> Option<&BranchName> {
        let Self::Branched {
            branch: Some(branch),
            ..
        } = self
        else {
            return None;
        };
        branch.current_name()
    }

    pub(super) fn add_assignment(&mut self, field: FieldName) {
        if let Self::Branched { assignments, .. } = self {
            assignments.push(AssignmentDraft::selected(field));
        }
    }

    pub(super) fn invalidate(&mut self) {
        if let Self::Branched {
            branch,
            assignments,
        } = self
        {
            if let Some(branch) = branch {
                branch.invalidate();
            }
            for assignment in assignments {
                assignment.invalidate();
            }
        }
    }

    fn build(&self) -> error_stack::Result<OutputBranch, IngestRouteDraftError> {
        match self {
            Self::Unselected | Self::Preserve => Err(Report::new(IngestRouteDraftError::Branching)),
            Self::Unbranched => Ok(OutputBranch::Unbranched),
            Self::Branched {
                branch,
                assignments,
            } => {
                let branch = match branch {
                    Some(branch) => branch
                        .current_name()
                        .ok_or_else(|| Report::new(IngestRouteDraftError::BranchChanged))?,
                    None => return Err(Report::new(IngestRouteDraftError::Branch)),
                };
                let mut built = Vec::new();
                for assignment in assignments {
                    built.push(assignment.build()?);
                }
                Ok(OutputBranch::BranchedBy {
                    branch: branch.clone(),
                    assignments: built,
                })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) enum FlushDraft {
    #[default]
    Unselected,
    Immediate,
    Each {
        interval: String,
        max_batch_size: String,
    },
}

impl FlushDraft {
    fn build(&self) -> error_stack::Result<FlushPolicy, IngestRouteDraftError> {
        match self {
            Self::Unselected => Err(Report::new(IngestRouteDraftError::Flush)),
            Self::Immediate => Ok(FlushPolicy::Immediate),
            Self::Each {
                interval,
                max_batch_size,
            } => {
                if interval.trim().is_empty() {
                    return Err(Report::new(IngestRouteDraftError::FlushInterval));
                }
                if max_batch_size.trim().is_empty() {
                    return Err(Report::new(IngestRouteDraftError::FlushSize));
                }
                Ok(FlushPolicy::Each {
                    interval: interval.trim().to_string(),
                    max_batch_size: max_batch_size.trim().to_string(),
                })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) enum MessageErrorDraft {
    #[default]
    Unselected,
    Ignore,
    Log,
    SendTo {
        relay: Option<SelectedReference<RelayName>>,
        assignments: Vec<AssignmentDraft>,
    },
}

impl MessageErrorDraft {
    pub(super) fn select_relay(&mut self, node: &NodeRef) {
        if node.kind != ModelKind::Relay {
            return;
        }
        if let Self::SendTo { relay, assignments } = self {
            let selected = RelayName::from(&node.identifier);
            if relay.as_ref().and_then(SelectedReference::current_name) != Some(&selected) {
                for assignment in assignments {
                    assignment.invalidate();
                }
            }
            *relay = Some(SelectedReference::chosen(selected));
        }
    }

    pub(super) fn current_relay(&self) -> Option<&RelayName> {
        let Self::SendTo {
            relay: Some(relay), ..
        } = self
        else {
            return None;
        };
        relay.current_name()
    }

    pub(super) fn invalidate(&mut self) {
        if let Self::SendTo { relay, assignments } = self {
            if let Some(relay) = relay {
                relay.invalidate();
            }
            for assignment in assignments {
                assignment.invalidate();
            }
        }
    }

    fn build(&self) -> error_stack::Result<MessageErrorPolicy, IngestRouteDraftError> {
        match self {
            Self::Unselected => Err(Report::new(IngestRouteDraftError::MessageError)),
            Self::Ignore => Ok(MessageErrorPolicy::Ignore),
            Self::Log => Ok(MessageErrorPolicy::Log),
            Self::SendTo { relay, assignments } => {
                let relay = match relay {
                    Some(relay) => relay
                        .current_name()
                        .ok_or_else(|| Report::new(IngestRouteDraftError::ErrorRelayChanged))?,
                    None => return Err(Report::new(IngestRouteDraftError::ErrorRelay)),
                };
                let mut built = Vec::new();
                for assignment in assignments {
                    built.push(assignment.build()?);
                }
                Ok(MessageErrorPolicy::Dlq {
                    relay: relay.clone(),
                    assignments: built,
                })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct IngestRouteDraft {
    pub(super) relay: Option<SelectedReference<RelayName>>,
    pub(super) inherit: InheritDraft,
    pub(super) assignments: Vec<AssignmentDraft>,
    pub(super) where_clause: String,
    pub(super) invocations: Vec<InvocationDraft>,
    pub(super) branch: RouteBranchDraft,
    pub(super) flush: FlushDraft,
    pub(super) message_error: MessageErrorDraft,
}

impl IngestRouteDraft {
    pub(super) fn select_relay(&mut self, node: &NodeRef) {
        if node.kind != ModelKind::Relay {
            return;
        }
        let selected = RelayName::from(&node.identifier);
        if self.current_relay() != Some(&selected) {
            for assignment in &mut self.assignments {
                assignment.invalidate();
            }
        }
        self.relay = Some(SelectedReference::chosen(selected));
    }

    pub(super) fn current_relay(&self) -> Option<&RelayName> {
        self.relay
            .as_ref()
            .and_then(SelectedReference::current_name)
    }

    pub(super) fn add_assignment(&mut self, field: FieldName) {
        self.assignments.push(AssignmentDraft::selected(field));
    }

    pub(super) fn invalidate_references(&mut self) {
        if let Some(relay) = &mut self.relay {
            relay.invalidate();
        }
        self.inherit.invalidate();
        for assignment in &mut self.assignments {
            assignment.invalidate();
        }
        self.branch.invalidate();
        self.message_error.invalidate();
    }

    pub(super) fn build(&self) -> error_stack::Result<ProcessorOutput, IngestRouteDraftError> {
        self.build_with_branch(Some(self.branch.build()?))
    }

    pub(super) fn build_preserving(
        &self,
    ) -> error_stack::Result<ProcessorOutput, IngestRouteDraftError> {
        self.build_with_branch(None)
    }

    pub(super) fn build_reingestor(
        &self,
    ) -> error_stack::Result<ProcessorOutput, IngestRouteDraftError> {
        let branch = if let RouteBranchDraft::Preserve = self.branch {
            None
        } else {
            Some(self.branch.build()?)
        };
        self.build_with_branch(branch)
    }

    fn build_with_branch(
        &self,
        branch: Option<OutputBranch>,
    ) -> error_stack::Result<ProcessorOutput, IngestRouteDraftError> {
        let relay = match &self.relay {
            Some(relay) => relay
                .current_name()
                .ok_or_else(|| Report::new(IngestRouteDraftError::RelayChanged))?,
            None => return Err(Report::new(IngestRouteDraftError::Relay)),
        };
        let mut assignments = Vec::new();
        for assignment in &self.assignments {
            assignments.push(assignment.build()?);
        }
        let where_clause = if self.where_clause.trim().is_empty() {
            None
        } else {
            Some(
                parse_expression(self.where_clause.trim())
                    .change_context(IngestRouteDraftError::WhereClause)?,
            )
        };
        let mut invocations = Vec::new();
        for invocation in &self.invocations {
            invocations.push(invocation.build()?);
        }
        Ok(ProcessorOutput {
            relay: relay.clone(),
            construction: RouteConstruction {
                inherit: self.inherit.build()?,
                assignments,
                where_clause,
                invocations,
            },
            branch,
            flush_policy: Some(self.flush.build()?),
            message_error_policy: self.message_error.build()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum IngestRouteDraftError {
    #[error("Choose an output relay")]
    Relay,
    #[error("The selected output relay belongs to a changed context; select it again")]
    RelayChanged,
    #[error("Choose a route inheritance mode")]
    Inheritance,
    #[error("Choose at least one field for the selected inheritance mode")]
    InheritedFields,
    #[error("Choose a destination field")]
    Field,
    #[error("A selected field belongs to a changed context; select it again")]
    FieldChanged,
    #[error("Enter a valid assignment expression")]
    Expression,
    #[error("Enter a valid route WHERE expression")]
    WhereClause,
    #[error("Enter a valid invocation function name")]
    InvocationFunction,
    #[error("Enter a valid invocation argument expression")]
    InvocationArgument,
    #[error("Choose UNBRANCHED or a named branch")]
    Branching,
    #[error("Choose a branch")]
    Branch,
    #[error("The selected branch belongs to a changed context; select it again")]
    BranchChanged,
    #[error("Choose FLUSH IMMEDIATE or FLUSH EACH")]
    Flush,
    #[error("Enter the FLUSH EACH interval")]
    FlushInterval,
    #[error("Enter the FLUSH EACH maximum batch size")]
    FlushSize,
    #[error("Choose an ON MESSAGE ERROR policy")]
    MessageError,
    #[error("Choose an error relay")]
    ErrorRelay,
    #[error("The selected error relay belongs to a changed context; select it again")]
    ErrorRelayChanged,
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{
        BinaryOperator, BranchName, Expression, FieldName, FieldReference, FieldScope, FlushPolicy,
        Literal, MessageErrorPolicy, ModelKind, ModelName, NodeRef, OutputBranch,
    };

    use super::{
        AssignmentDraft, FlushDraft, IngestRouteDraft, InheritDraft, InvocationDraft,
        MessageErrorDraft, RouteBranchDraft,
    };

    fn node(kind: ModelKind, name: &str) -> NodeRef {
        NodeRef::new(
            kind,
            ModelName::parse(name).assured("test model name is valid"),
        )
    }

    fn string(value: &str) -> Expression {
        Expression::Literal(Literal::String(value.to_string()))
    }

    #[test]
    fn route_build_preserves_its_branch_assignments_flush_and_error_destination() {
        let mut route = IngestRouteDraft::default();
        route.select_relay(&node(ModelKind::Relay, "out"));
        route.inherit = InheritDraft::All;
        route.add_assignment(FieldName::parse("message").assured("valid field"));
        route.assignments[0].expression = "message.message".into();
        route.where_clause = "message.message != ''".into();
        route.branch.choose_branched();
        route
            .branch
            .select_branch(&node(ModelKind::Branch, "by_user"));
        route
            .branch
            .add_assignment(FieldName::parse("user").assured("valid field"));
        if let RouteBranchDraft::Branched { assignments, .. } = &mut route.branch {
            assignments[0].expression = "message.message".into();
        }
        route.flush = FlushDraft::Each {
            interval: "1s".into(),
            max_batch_size: "1MiB".into(),
        };
        route.message_error = MessageErrorDraft::SendTo {
            relay: None,
            assignments: Vec::new(),
        };
        route
            .message_error
            .select_relay(&node(ModelKind::Relay, "errors"));
        if let MessageErrorDraft::SendTo { assignments, .. } = &mut route.message_error {
            assignments.push(AssignmentDraft::selected(
                FieldName::parse("reason").assured("valid field"),
            ));
            assignments[0].expression = "error.message".into();
        }

        let output = route.build().assured("complete route builds");
        assert_eq!(output.relay.as_str(), "out");
        assert_eq!(output.construction.assignments.len(), 1);
        assert_eq!(
            output.flush_policy,
            Some(FlushPolicy::Each {
                interval: "1s".into(),
                max_batch_size: "1MiB".into()
            })
        );
        assert!(
            matches!(output.branch, Some(OutputBranch::BranchedBy { ref branch, ref assignments })
            if branch == &BranchName::parse("by_user").assured("valid branch") && assignments.len() == 1)
        );
        assert!(
            matches!(output.message_error_policy, MessageErrorPolicy::Dlq { ref assignments, .. }
            if assignments.len() == 1)
        );
    }

    #[test]
    fn route_expressions_read_a_backslash_in_a_string_literal_verbatim() {
        let mut route = IngestRouteDraft::default();
        route.select_relay(&node(ModelKind::Relay, "out"));
        route.inherit = InheritDraft::None;
        route.add_assignment(FieldName::parse("message").assured("valid field"));
        route.assignments[0].expression = r"'a\nb'".into();
        route.where_clause = r#"message.message != "c\td""#.into();
        route.invocations.push(InvocationDraft {
            function: "notify".into(),
            arguments: vec![r"'e\\f'".into()],
        });
        route.branch.choose_unbranched();
        route.flush = FlushDraft::Immediate;
        route.message_error = MessageErrorDraft::Log;

        let construction = route.build().assured("complete route builds").construction;
        assert_eq!(construction.assignments[0].value, string(r"a\nb"));
        assert_eq!(
            construction.where_clause,
            Some(Expression::Binary {
                operator: BinaryOperator::NotEqual,
                left: Box::new(Expression::Field(FieldReference::scoped(
                    FieldScope::Message,
                    FieldName::parse("message").assured("valid field"),
                ))),
                right: Box::new(string(r"c\td")),
            })
        );
        assert_eq!(construction.invocations[0].arguments, [string(r"e\\f")]);
    }

    #[test]
    fn route_requires_explicit_branch_flush_and_message_policy() {
        let mut route = IngestRouteDraft::default();
        route.select_relay(&node(ModelKind::Relay, "out"));
        route.inherit = InheritDraft::None;
        assert!(route.build().is_err());
        route.branch.choose_unbranched();
        assert!(route.build().is_err());
        route.flush = FlushDraft::Immediate;
        assert!(route.build().is_err());
        route.message_error = MessageErrorDraft::Log;
        assert!(route.build().is_ok());
        route.invalidate_references();
        assert!(route.build().is_err());
    }
}
