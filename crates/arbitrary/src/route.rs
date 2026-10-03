//! What processors share: their inputs, routes, route construction, materialized state, branch
//! declarations, flush cadences and error policies.

use std::num::NonZeroUsize;

use meticulous::OptionExt as _;
use nervix_models::{
    AckMode, Assignment, AssignmentTarget, AssignmentTargetScope, BranchSelection, ErrorPolicies,
    Expression, FieldName, FlushPolicy, GeneralErrorPolicy, Inheritance, InheritedField,
    InputCollectPolicy, Invocation, MaterializedStateDependency, MaterializedStatePolicy,
    MessageErrorPolicy, OutputBranch, ProcessorInputWhere, ProcessorInputs, ProcessorOutput,
    ProcessorOutputs, RelayName, RouteConstruction,
};

use crate::Arbitrary;

/// The most items a generated list of routes, inputs, assignments or dependencies holds.
const ITEMS: usize = 3;

/// Which construction clauses a route may write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteShape {
    /// A route that transforms its input: it may inherit fields, assign, filter and invoke.
    Transforming,
    /// A route that builds its output from nothing: at least one assignment and an optional
    /// filter.
    SetOnly,
    /// A construction that filters and invokes but builds no fields, as an emitter sending no
    /// body writes.
    FilterAndInvoke,
    /// A construction that only filters, as an emitter whose sink maps its own values writes.
    FilterOnly,
}

/// Whether a route releases its output on a flush cadence of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteFlush {
    /// Every route declares `FLUSH EACH` or `FLUSH IMMEDIATE`.
    Required,
    /// The node owns its emission cadence, so no route declares one.
    Absent,
}

/// Where a route's branch identity comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteBranch {
    /// Each route constructs its own outgoing branch, or declares itself unbranched.
    PerRoute,
    /// The node declares one branch for every route, so no route names one.
    NodeWide,
}

impl Arbitrary<'_> {
    /// Between one and three routes of the given shape, flush rule and branch source, in order.
    pub fn processor_outputs(
        &mut self,
        shape: RouteShape,
        flush: RouteFlush,
        branch: RouteBranch,
    ) -> ProcessorOutputs {
        let count = self
            .entropy
            .positive_count(NonZeroUsize::new(ITEMS).assured("a node writes at least one route"));
        let mut routes = Vec::with_capacity(count);
        for _ in 0..count {
            routes.push(self.processor_output(shape, flush, branch));
        }
        ProcessorOutputs::new(routes)
    }

    fn processor_output(
        &mut self,
        shape: RouteShape,
        flush: RouteFlush,
        branch: RouteBranch,
    ) -> ProcessorOutput {
        let relay = self.name();
        let construction = self.route_construction(shape);
        let branch = match branch {
            RouteBranch::NodeWide => None,
            RouteBranch::PerRoute => Some(self.output_branch()),
        };
        let flush_policy = match flush {
            RouteFlush::Required => Some(self.flush_policy()),
            RouteFlush::Absent => None,
        };
        ProcessorOutput {
            relay,
            construction,
            flush_policy,
            message_error_policy: self.message_error_policy(),
            branch,
        }
    }

    /// The construction a route writes, in the order its clauses are written.
    pub fn route_construction(&mut self, shape: RouteShape) -> RouteConstruction {
        match shape {
            RouteShape::SetOnly => RouteConstruction {
                inherit: None,
                assignments: self.assignments(1, true),
                where_clause: self.optional_expression(),
                invocations: Vec::new(),
            },
            RouteShape::FilterAndInvoke => RouteConstruction {
                inherit: None,
                assignments: Vec::new(),
                where_clause: self.optional_expression(),
                invocations: self.invocations(),
            },
            RouteShape::FilterOnly => RouteConstruction {
                inherit: None,
                assignments: Vec::new(),
                where_clause: self.optional_expression(),
                invocations: Vec::new(),
            },
            RouteShape::Transforming => {
                let inherit = if self.entropy.flag() {
                    Some(self.inheritance())
                } else {
                    None
                };
                let assignments = self.assignments(0, true);
                let where_clause = self.optional_expression();
                let invocations = self.invocations();
                RouteConstruction {
                    inherit,
                    assignments,
                    where_clause,
                    invocations,
                }
            }
        }
    }

    fn invocations(&mut self) -> Vec<Invocation> {
        let count = self.entropy.count(ITEMS);
        let mut invocations = Vec::with_capacity(count);
        for _ in 0..count {
            let function = self.function_name();
            let arguments = self.expression_arguments();
            invocations.push(Invocation {
                function,
                arguments,
            });
        }
        invocations
    }

    fn expression_arguments(&mut self) -> Vec<Expression> {
        let count = self.entropy.count(ITEMS);
        let mut arguments = Vec::with_capacity(count);
        for _ in 0..count {
            arguments.push(self.expression());
        }
        arguments
    }

    fn inheritance(&mut self) -> Inheritance {
        match self.entropy.byte() % 3 {
            0 => Inheritance::All,
            1 => Inheritance::AllExcept(self.distinct_names::<FieldName>(1, ITEMS)),
            _ => {
                let fields = self.distinct_names::<FieldName>(1, ITEMS);
                let mut inherited = Vec::with_capacity(fields.len());
                for field in fields {
                    inherited.push(InheritedField {
                        field,
                        leak_sensitive: self.entropy.flag(),
                    });
                }
                Inheritance::Fields(inherited)
            }
        }
    }

    /// At least `minimum` assignments. A scoped assignment targets a message, output or branch
    /// field; where `scoped` is false every target is a bare field.
    pub fn assignments(&mut self, minimum: usize, scoped: bool) -> Vec<Assignment> {
        let extra = ITEMS
            .checked_sub(minimum)
            .assured("an assignment list's minimum is within its bound");
        let count = minimum
            .checked_add(self.entropy.count(extra))
            .verified("the extra count is at most the bound minus the minimum");
        let mut assignments = Vec::with_capacity(count);
        for _ in 0..count {
            let scope = if scoped {
                self.entropy.pick([
                    AssignmentTargetScope::Bare,
                    AssignmentTargetScope::Message,
                    AssignmentTargetScope::Output,
                    AssignmentTargetScope::Branch,
                ])
            } else {
                AssignmentTargetScope::Bare
            };
            let target = AssignmentTarget {
                scope,
                field: self.name(),
            };
            assignments.push(Assignment {
                target,
                value: self.expression(),
            });
        }
        assignments
    }

    /// An expression, or none.
    pub fn optional_expression(&mut self) -> Option<Expression> {
        if self.entropy.flag() {
            Some(self.expression())
        } else {
            None
        }
    }

    fn output_branch(&mut self) -> OutputBranch {
        if self.entropy.flag() {
            OutputBranch::BranchedBy {
                branch: self.name(),
                assignments: self.assignments(0, false),
            }
        } else {
            OutputBranch::Unbranched
        }
    }

    /// A flush cadence: immediate, or on an interval with a batch bound.
    pub fn flush_policy(&mut self) -> FlushPolicy {
        if self.entropy.flag() {
            FlushPolicy::Immediate
        } else {
            FlushPolicy::Each {
                interval: self.duration(),
                max_batch_size: self.byte_size(),
            }
        }
    }

    /// What a route does with a message whose processing failed.
    pub fn message_error_policy(&mut self) -> MessageErrorPolicy {
        match self.entropy.byte() % 3 {
            0 => MessageErrorPolicy::Ignore,
            1 => MessageErrorPolicy::Log,
            _ => MessageErrorPolicy::Dlq {
                relay: self.name(),
                assignments: self.assignments(1, false),
            },
        }
    }

    /// What a node does with a failure outside any one message.
    pub fn general_error_policy(&mut self) -> GeneralErrorPolicy {
        self.entropy
            .pick([GeneralErrorPolicy::Ignore, GeneralErrorPolicy::Log])
    }

    /// The message and general error policies of an emitter.
    pub fn error_policies(&mut self) -> ErrorPolicies {
        ErrorPolicies {
            message: self.message_error_policy(),
            general: self.general_error_policy(),
        }
    }

    /// A node-wide branch declaration: a named branch, or none.
    pub fn branch_selection(&mut self) -> BranchSelection {
        if self.entropy.flag() {
            BranchSelection::branched_by(self.name())
        } else {
            BranchSelection::unbranched()
        }
    }

    /// Whether a node's acknowledgements attach to its input's or stand alone.
    pub fn ack_mode(&mut self) -> AckMode {
        self.entropy.pick([AckMode::Attached, AckMode::Detached])
    }

    /// One or more input relays, each read once and filtered by at most one condition of its own,
    /// optionally collected into batches.
    pub fn processor_inputs(&mut self, collect: bool) -> ProcessorInputs {
        let from = self.distinct_names::<RelayName>(1, ITEMS);
        let mut conditions = Vec::new();
        for relay in &from {
            if self.entropy.flag() {
                conditions.push(ProcessorInputWhere {
                    relay: relay.clone(),
                    where_clause: self.expression(),
                });
            }
        }
        let inputs = ProcessorInputs::new(from, conditions);
        if collect && self.entropy.flag() {
            let max_batch_size = if self.entropy.flag() {
                Some(self.byte_size())
            } else {
                None
            };
            inputs.with_collect_policy(self.duration(), max_batch_size)
        } else {
            inputs
        }
    }

    /// The materialized state a node reads, each relay declared once, in declaration order.
    pub fn materialized_state(&mut self) -> Vec<MaterializedStateDependency> {
        let relays = self.distinct_names::<RelayName>(0, ITEMS);
        let mut dependencies = Vec::with_capacity(relays.len());
        for relay in relays {
            dependencies.push(self.materialized_dependency(relay));
        }
        dependencies
    }

    /// A dependency on the materialized state of `relay`, under any policy.
    pub fn materialized_dependency(&mut self, relay: RelayName) -> MaterializedStateDependency {
        let policy = match self.entropy.byte() % 3 {
            0 => MaterializedStatePolicy::RequiredSkip,
            1 => MaterializedStatePolicy::RequiredWait,
            _ => MaterializedStatePolicy::Default(self.assignments(1, false)),
        };
        MaterializedStateDependency { relay, policy }
    }

    /// A collection cadence written on its own, as a correlator side declares one.
    pub fn collect_policy(&mut self) -> InputCollectPolicy {
        let max_batch_size = if self.entropy.flag() {
            Some(self.byte_size())
        } else {
            None
        };
        InputCollectPolicy {
            collect_for: self.duration(),
            max_batch_size,
        }
    }
}
